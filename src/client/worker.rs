use std::{collections::HashMap, sync::Arc, time::{Duration, Instant}};

use log::{debug, error, info};
use nix::libc::stat;
use prost::Message as _;
use tokio::{task::JoinSet, time::sleep};

use crate::{config::ClientConfig, proto::{client::{self, ProtoClientReply, ProtoClientRequest}, rpc::ProtoPayload}, rpc::client::PinnedClient, utils::channel::{make_channel, Receiver, Sender}};
use crate::rpc::MessageRef;
use super::{logger::ClientWorkerStat, workload_generators::{Executor, PerWorkerWorkloadGenerator}};

pub struct ClientWorker<Gen: PerWorkerWorkloadGenerator> {
    config: ClientConfig,
    client: PinnedClient,
    generator: Gen,
    id: usize,
    stat_tx: Sender<ClientWorkerStat>,
}

#[derive(Debug, Clone)]
struct CheckerTask {
    start_time: Instant,
    wait_from: String,
    id: u64,
}

enum CheckerResponse {
    TryAgain(CheckerTask, Option<Vec<String>> /* New Node list */, Option<usize> /* New Leader id */),
    Success(u64 /* id */)
}


struct OutstandingRequest {
    id: u64,
    payload: Vec<u8>,
    executor_mode: Executor,
    last_sent_to: String,
    start_time: Instant
}

impl OutstandingRequest {
    pub fn get_checker_task(&self) -> CheckerTask {
        CheckerTask {
            start_time: self.start_time.clone(),
            wait_from: self.last_sent_to.clone(),
            id: self.id,
        }
    }

    pub fn default() -> Self {
        Self {
            id: 0,
            payload: Vec::new(),
            last_sent_to: String::new(),
            start_time: Instant::now(),
            executor_mode: Executor::Any,
        }
    }
}


impl<Gen: PerWorkerWorkloadGenerator + Send + Sync + 'static> ClientWorker<Gen> {
    pub fn new(
        config: ClientConfig,
        client: PinnedClient,
        generator: Gen,
        id: usize,
        stat_tx: Sender<ClientWorkerStat>,
    ) -> Self {
        Self {
            config,
            client,
            generator,
            id,
            stat_tx,
        }
    }

    pub async fn launch(mut worker: Self, js: &mut JoinSet<()>) {
        // This will act as a semaphore.
        // Anytime the checker task processes a reply successfully, it sends a `Success` message to the generator task.
        // The generator waits to receive this message before sending the next request.
        // However, if the generator task receives a `TryAgain` message, it will send the same request again. 
        let max_outstanding_requests = worker.config.workload_config.max_concurrent_requests;
        let (backpressure_tx, backpressure_rx) = make_channel(max_outstanding_requests);

        // This is to let the checker task know about new requests.
        let (generator_tx, generator_rx) = make_channel(max_outstanding_requests);

        let _client = worker.client.clone();
        let _stat_tx = worker.stat_tx.clone();
        let _backpressure_tx = backpressure_tx.clone();

        let id = worker.id;
        js.spawn(async move {
            // Fill the backpressure channel with `Success` messages.
            // So that the generator task can start sending requests.
            for _ in 0..max_outstanding_requests {
                backpressure_tx.send(CheckerResponse::Success(0)).await.unwrap();
            }

            // Can't let the checker_task consume this worker (or lock it for indefinite time).
            Self::checker_task(backpressure_tx, generator_rx, _client, _stat_tx, id).await;
        });

        js.spawn(async move {
            worker.generator_task(generator_tx, backpressure_rx, _backpressure_tx).await;
        });

    }

    async fn checker_task(backpressure_tx: Sender<CheckerResponse>, generator_rx: Receiver<CheckerTask>, client: PinnedClient, stat_tx: Sender<ClientWorkerStat>, id: usize) {
        let mut waiting_for_byz_response = HashMap::<u64, CheckerTask>::new();
        let mut out_of_order_byz_response = HashMap::<u64, Instant>::new();
        loop {
            match generator_rx.recv().await {
                Some(req) => {
                    // This is a new request.
                    if let Some(byz_resp_time) = out_of_order_byz_response.remove(&req.id) {
                        // Got the response before, Nice!
                        if byz_resp_time > req.start_time {
                            let latency = byz_resp_time - req.start_time;
                            let _ = stat_tx.send(ClientWorkerStat::ByzCommitLatency(latency)).await;
                        } else {
                            error!("Byzantine response received before the request was sent. This is a bug.");
                        }
                    } else {
                        waiting_for_byz_response.insert(req.id, req.clone());
                    }
                    let _ = stat_tx.send(ClientWorkerStat::ByzCommitPending(id, waiting_for_byz_response.len())).await;
                    
                    // We will wait for the response.
                    let res = PinnedClient::await_reply(&client, &req.wait_from).await;
                    if res.is_err() {
                        // We need to try again.
                        let _ = backpressure_tx.send(CheckerResponse::TryAgain(req, None, None)).await;
                        continue;
                    }
                    let msg = res.unwrap();
                    let sz = msg.as_ref().1;
                    let resp = ProtoClientReply::decode(&msg.as_ref().0.as_slice()[..sz]);
                    if resp.is_err() {
                        // We need to try again.
                        let _ = backpressure_tx.send(CheckerResponse::TryAgain(req, None, None)).await;
                        continue;
                    }

                    let resp = resp.unwrap();

                    match resp.reply {
                        Some(client::proto_client_reply::Reply::Receipt(receipt)) => {
                            let _ = backpressure_tx.send(CheckerResponse::Success(req.id)).await;
                            let _ = stat_tx.send(ClientWorkerStat::CrashCommitLatency(req.start_time.elapsed())).await;

                            for byz_resp in receipt.byz_responses.iter() {
                                if let Some(task) = waiting_for_byz_response.remove(&byz_resp.client_tag) {
                                    let _ = stat_tx.send(ClientWorkerStat::ByzCommitLatency(task.start_time.elapsed())).await;
                                } else {
                                    out_of_order_byz_response.insert(byz_resp.client_tag, Instant::now());
                                }
                            }
                        },
                        Some(client::proto_client_reply::Reply::TryAgain(_try_again)) => {
                            sleep(Duration::from_secs(1)).await;
                            let _ = backpressure_tx.send(CheckerResponse::TryAgain(req, None, None)).await;
                        },
                        Some(client::proto_client_reply::Reply::TentativeReceipt(_tentative_receipt)) => {
                            // We treat tentative receipt as a success.
                            let _ = backpressure_tx.send(CheckerResponse::Success(req.id)).await;
                            let _ = stat_tx.send(ClientWorkerStat::CrashCommitLatency(req.start_time.elapsed())).await;

                        },
                        Some(client::proto_client_reply::Reply::Leader(leader)) => {
                            sleep(Duration::from_secs(1)).await;
                            // We need to try again but with the leader reset.
                            let curr_leader = leader.name;
                            let node_list = crate::config::NodeInfo::deserialize(&leader.serialized_node_infos);
                            let new_leader_id = node_list.nodes.iter().position(|e| e.0.eq(&curr_leader));
                            if new_leader_id.is_none() {
                                // Malformed!
                                let _ = backpressure_tx.send(CheckerResponse::TryAgain(req, None, None)).await;
                                continue;
                            }
                            let mut config = client.0.config.get();
                            let config = Arc::make_mut(&mut config);
                            for (k, v) in node_list.nodes.iter() {
                                config.net_config.nodes.insert(k.clone(), v.clone());
                            }
                            client.0.config.set(config.clone()); 
                            
                            info!("Leader changed to {}", curr_leader);

                            let node_list = node_list.nodes.keys().map(|e| e.clone()).collect::<Vec<_>>();
                            let _ = backpressure_tx.send(CheckerResponse::TryAgain(req, Some(node_list), new_leader_id)).await;
                        },
                        
                        None => {
                            // We need to try again.
                            let _ = backpressure_tx.send(CheckerResponse::TryAgain(req, None, None)).await;
                        },
                    }


                    // TODO: check response  
                },
                None => {
                    break;
                }
                
            }
        }
    }

    async fn generator_task(&mut self, generator_tx: Sender<CheckerTask>, backpressure_rx: Receiver<CheckerResponse>, backpressure_tx: Sender<CheckerResponse>) {
        let mut outstanding_requests = HashMap::<u64, OutstandingRequest>::new();

        let mut total_requests = 0;
        let max_requests = self.config.workload_config.num_requests;
        let mut node_list = self.config.net_config.nodes.keys().map(|e| e.clone()).collect::<Vec<_>>();
        let mut curr_leader_id = 0;
        let mut curr_round_robin_id = 0;

        let my_name = self.config.net_config.name.clone();

        sleep(Duration::from_secs(1)).await;

        while total_requests < max_requests {
            // Wait for the checker task to give a go-ahead.
            match backpressure_rx.recv().await {
                Some(CheckerResponse::Success(id)) => {
                    // Remove the request from the outstanding requests if possible.
                    outstanding_requests.remove(&id);
                    // We can send a new request.
                    let payload = self.generator.next();
                    let mut req = OutstandingRequest::default();
                    req.id = (total_requests + 1) as u64;
                    req.executor_mode = payload.executor;
                    let client_request = ProtoPayload {
                        message: Some(crate::proto::rpc::proto_payload::Message::ClientRequest(ProtoClientRequest {
                            tx: Some(payload.tx),
                            origin: my_name.clone(),
                            sig: vec![0u8; 1],
                            client_tag: (total_requests + 1) as u64,
                        }))
                    };

                    req.payload = client_request.encode_to_vec();
                    

                    self.send_request(&mut req, &node_list, &mut curr_leader_id, &mut curr_round_robin_id, &mut outstanding_requests).await;

                    generator_tx.send(req.get_checker_task()).await.unwrap();
                    outstanding_requests.insert(req.id, req);
                    total_requests += 1;
                },
                Some(CheckerResponse::TryAgain(task, node_vec, leader)) => {
                    // We need to send the same request again.
                    if let Some(_leader) = leader {
                        PinnedClient::drop_all_connections(&self.client).await;
                        curr_leader_id = _leader;
                    }

                    if let Some(_node_list) = node_vec {
                        node_list = _node_list;
                    }

                    let req = outstanding_requests.remove(&task.id);
                    if req.is_none() {
                        // Skip silently; try to create a new request for this
                        backpressure_tx.send(CheckerResponse::Success(0)).await.unwrap();
                        continue;
                    }
                    let mut req = req.unwrap();
                    self.send_request(&mut req, &node_list, &mut curr_leader_id, &mut curr_round_robin_id, &mut outstanding_requests).await;
                    generator_tx.send(req.get_checker_task()).await.unwrap();
                },
                None => {
                    break;
                }
            }
        }
    }

    /// Sets the req.last_sent_to.
    /// Increments the curr_round_robin_id if req.executor_mode is Any.
    /// Sends the request to the appropriate node.
    /// This will NOT add the requst to outstanding_requests. It only clears outstanding requests if the leader changes due to an error.
    async fn send_request(&self, req: &mut OutstandingRequest, node_list: &Vec<String>, curr_leader_id: &mut usize, curr_round_robin_id: &mut usize, outstanding_requests: &mut HashMap<u64, OutstandingRequest>) {
        let buf = &req.payload;
        let sz = buf.len();
        loop {
            let res = match req.executor_mode {
                Executor::Leader => {
                    let curr_leader = &node_list[*curr_leader_id];
                    req.last_sent_to = curr_leader.clone();

                    
                    PinnedClient::send(
                        &self.client,
                        &curr_leader,
                        MessageRef(buf, sz, &crate::rpc::SenderType::Anon),
                    ).await
                },
                Executor::Any => {
                    let recv_node = &node_list[(*curr_round_robin_id) % node_list.len()];
                    *curr_round_robin_id = *curr_round_robin_id + 1;
                    req.last_sent_to = recv_node.clone();
                    PinnedClient::send(
                        &self.client,
                        recv_node,
                        MessageRef(&buf, buf.len(), &crate::rpc::SenderType::Anon),
                    ).await
                },
            };

            if res.is_err() {
                debug!("Error: {:?}", res);
                *curr_leader_id = (*curr_leader_id + 1) % node_list.len();
                outstanding_requests.clear(); // Clear it out because I am not going to listen on that socket again
                // info!("Retrying with leader {} Backoff: {} ms: Error: {:?}", curr_leader, current_backoff_ms, res);
                // backoff!(*current_backoff_ms);
                continue;
            }
            break;
        }
    }
}
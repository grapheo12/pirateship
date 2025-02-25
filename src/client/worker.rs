use std::{collections::HashMap, sync::Arc, time::Instant};

use indexmap::IndexMap;
use log::info;
use prost::Message as _;
use tokio::{sync::Mutex, task::JoinHandle};

use crate::{config::ClientConfig, proto::{client::{self, ProtoClientRequest}, rpc::ProtoPayload}, rpc::client::PinnedClient, utils::channel::{make_channel, Receiver, Sender}};
use crate::rpc::MessageRef;
use super::{logger::ClientWorkerStat, workload_generators::{Executor, PerWorkerWorkloadGenerator}};

struct ClientWorker<'a, Gen: PerWorkerWorkloadGenerator + 'a> {
    config: ClientConfig,
    client: PinnedClient,
    generator: Gen,
    id: usize,
    stat_tx: Sender<ClientWorkerStat>,

    phantom: std::marker::PhantomData<&'a Gen>,
}

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


const MAX_OUTSTANDING_REQUESTS: usize = 1000;

impl<'a, Gen: PerWorkerWorkloadGenerator + Send + Sync + 'a> ClientWorker<'a, Gen> {
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

            phantom: std::marker::PhantomData,
        }
    }

    pub async fn launch(worker: &'static Arc<Mutex<Self>>) -> (JoinHandle<()>, JoinHandle<()>) {
        // This will act as a semaphore.
        // Anytime the checker task processes a reply successfully, it sends a `Success` message to the generator task.
        // The generator waits to receive this message before sending the next request.
        // However, if the generator task receives a `TryAgain` message, it will send the same request again. 
        let (backpressure_tx, backpressure_rx) = make_channel(MAX_OUTSTANDING_REQUESTS);

        // This is to let the checker task know about new requests.
        let (generator_tx, generator_rx) = make_channel(MAX_OUTSTANDING_REQUESTS);

        let _client = {
            let worker = worker.lock().await;
            worker.client.clone()
        };

        let checker_handle = tokio::spawn(async move {
            // Fill the backpressure channel with `Success` messages.
            // So that the generator task can start sending requests.
            for _ in 0..MAX_OUTSTANDING_REQUESTS {
                backpressure_tx.send(CheckerResponse::Success(0)).await.unwrap();
            }

            // Can't let the checker_task consume this worker (or lock it for indefinite time).
            Self::checker_task(backpressure_tx, generator_rx, _client).await;
        });

        let _worker = worker.clone();
        let generator_handle = tokio::spawn(async move {
            let mut worker = _worker.lock().await;
            worker.generator_task(generator_tx, backpressure_rx).await;
        });

        (checker_handle, generator_handle)

    }

    async fn checker_task(backpressure_tx: Sender<CheckerResponse>, generator_rx: Receiver<CheckerTask>, client: PinnedClient) {
        loop {
            match generator_rx.recv().await {
                Some(req) => {
                    // This is a new request.
                    // We can process it.
                    // So, we send a `Success` message to the generator task.
                    
                    backpressure_tx.send(CheckerResponse::Success(req.id)).await.unwrap();
                },
                None => {
                    break;
                }
                
            }
        }
    }

    async fn generator_task(&mut self, generator_tx: Sender<CheckerTask>, backpressure_rx: Receiver<CheckerResponse>) {
        let mut outstanding_requests = HashMap::<u64, OutstandingRequest>::new();

        let mut total_requests = 0;
        let max_requests = self.config.workload_config.num_requests;
        let mut node_list = self.config.net_config.nodes.keys().map(|e| e.clone()).collect::<Vec<_>>();
        let mut curr_leader_id = 0;
        let mut curr_round_robin_id = 0;

        let my_name = self.config.net_config.name.clone();

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
                        curr_leader_id = _leader;
                    }

                    if let Some(_node_list) = node_vec {
                        node_list = _node_list;
                    }

                    let mut req = outstanding_requests.remove(&task.id).unwrap();
                    // Todo: Actually fire the request.
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

                    
                    PinnedClient::send_buffered(
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
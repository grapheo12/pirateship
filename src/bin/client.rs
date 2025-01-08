// Copyright (c) Shubham Mishra. All rights reserved.
// Licensed under the MIT License.

use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use log::{debug, error, info, trace, warn};
use pft::{
    config::{default_log4rs_config, ClientConfig}, consensus::utils::get_f_plus_one_send_list, crypto::{cmp_hash, KeyStore}, proto::{
        client::{self, ProtoByzPollRequest, ProtoClientReply, ProtoClientRequest}, rpc::ProtoPayload
    }, rpc::{
        client::{Client, PinnedClient},
        MessageRef, PinnedMessage,
    }, utils::workload_generators::{BlankWorkloadGenerator, Executor, KVReadWriteUniformGenerator, KVReadWriteYCSBGenerator, MockSQLGenerator, PerWorkerWorkloadGenerator}
};
use prost::Message;
use rand::{distributions::WeightedIndex, prelude::*};
use rand_chacha::ChaCha20Rng;
use core::error;
use std::{collections::HashMap, env, fs, io, path, pin::Pin, sync::{Arc, Mutex}, time::Duration};
use tokio::{sync::mpsc, task::JoinSet, time::sleep};
use std::time::Instant;

#[global_allocator]
static ALLOC: snmalloc_rs::SnMalloc = snmalloc_rs::SnMalloc;


fn process_args() -> ClientConfig {
    macro_rules! usage_str {
        () => {
            "\x1b[31;1mUsage: {} path/to/config.json\x1b[0m"
        };
    }

    let args: Vec<_> = env::args().collect();

    if args.len() != 2 {
        panic!(usage_str!(), args[0]);
    }

    let cfg_path = path::Path::new(args[1].as_str());
    if !cfg_path.exists() {
        panic!(usage_str!(), args[0]);
    }

    let cfg_contents = fs::read_to_string(cfg_path).expect("Invalid file path");

    ClientConfig::deserialize(&cfg_contents)
}

const MAX_BACKOFF_MS: u64 = 1000;
const MIN_BACKOFF_MS: u64 = 150;

macro_rules! backoff {
    ($current_backoff_ms: expr) => {
        sleep(Duration::from_millis($current_backoff_ms)).await;
        $current_backoff_ms *= 2;
        if $current_backoff_ms > MAX_BACKOFF_MS {
            $current_backoff_ms = MAX_BACKOFF_MS;
        }
    };
}

macro_rules! reset_backoff {
    ($current_backoff_ms: expr) => {
        $current_backoff_ms = MIN_BACKOFF_MS;
    };
}

const MAX_OUTSTANDING_REQUESTS: usize = 256;

async fn check_response(
    idx: usize, client: &PinnedClient, num_requests: usize, config: &mut ClientConfig,
    byz_resp: &Arc<Pin<Box<Mutex<HashMap<(u64, u64), Instant>>>>>,
    msg: PinnedMessage,
    current_backoff_ms: &mut u64,
    node_list: &mut Vec<String>,
    outstanding_requests: &mut Vec<(u64, Vec<u8>, Instant)>,
    curr_leader: &mut String,
    workload_generator: &mut Box<dyn PerWorkerWorkloadGenerator>,
    sample_item: &[(bool, i32); 2],
    weight_dist: &WeightedIndex<i32>,
    rng: &mut ChaCha20Rng
) -> Result<u64, u64> {
    let sz = msg.as_ref().1;
    let resp = ProtoClientReply::decode(&msg.as_ref().0.as_slice()[..sz]).unwrap();
    if let None = resp.reply {
        error!("Empty reply");
        return Ok(0);
    }

    let is_outstanding = outstanding_requests.iter().any(|e| e.0 == resp.client_tag);
    if !is_outstanding {
        warn!("Was not waiting for this request anymore: {}:{}", idx, resp.client_tag);
        return Ok(0);
    }

    let client_tag = resp.client_tag;
    let (_, _, start) = outstanding_requests.iter().find(|e| e.0 == resp.client_tag).unwrap();


    let resp = match resp.reply.unwrap() {
        pft::proto::client::proto_client_reply::Reply::Receipt(r) => {
            reset_backoff!(*current_backoff_ms);
            r
        },
        pft::proto::client::proto_client_reply::Reply::TryAgain(try_again_msg) => {
            let node_infos = pft::config::NodeInfo::deserialize(&try_again_msg.serialized_node_infos);
            for (k, v) in node_infos.nodes.iter() {
                config.net_config.nodes.insert(k.clone(), v.clone());
            }
            client.0.config.set(Box::new(config.fill_missing()));
            *node_list = config.net_config.nodes.keys().map(|e| e.clone()).collect::<Vec<_>>();
            debug!("New Net Info: {:#?}", config.net_config.nodes);
            backoff!(*current_backoff_ms);
            return Err(client_tag);
        },
        pft::proto::client::proto_client_reply::Reply::Leader(l) => {
            reset_backoff!(*current_backoff_ms);
            let node_infos = pft::config::NodeInfo::deserialize(&l.serialized_node_infos);
            for (k, v) in node_infos.nodes.iter() {
                config.net_config.nodes.insert(k.clone(), v.clone());
            }
            client.0.config.set(Box::new(config.fill_missing()));
            *node_list = config.net_config.nodes.keys().map(|e| e.clone()).collect::<Vec<_>>();
            debug!("New Net Info: {:#?}", config.net_config.nodes);
            if *curr_leader != l.name {
                trace!("Switching leader: {} --> {}", curr_leader, l.name);
                // sleep(Duration::from_millis(10)).await; // Rachel: You fell A-SLEEP?!
                
                // Drop the connection from the old leader.
                // This is required as one process is generally allowed ~1024 open connections.
                // If ~700 threads have open connections to the leader
                // and the connections to the old leader are not closed,
                // within 2 views, the process will run out of file descriptors.
                // The OS will reset connections.
                
                // However, doing a drop here is not that efficient, why?
                // Because sometime node1 changes view where node2 is leader,
                // but node1 is still leader for node2 and
                // the curr_leader will ping pong until node2 changes view.
                
                PinnedClient::drop_connection(&client, &curr_leader).await;


                *curr_leader = l.name.clone();
                outstanding_requests.clear();
            }
            return Err(client_tag);
        },
        pft::proto::client::proto_client_reply::Reply::TentativeReceipt(r) => {
            debug!("Got tentative receipt: {:?}", r);
            backoff!(*current_backoff_ms);
            
            return Err(client_tag);
            // @todo: Wait to see if my txn gets committed in the tentative block.
        },
    };

    if !workload_generator.check_result(&resp.results) {
        error!("Unexpected Transaction result!");
    }

    
    // let should_log = curr_leader == "node1";
    let should_log = sample_item[weight_dist.sample(rng)].0;
    
    if should_log {
        info!("Client Id: {}, Msg Id: {}, Block num: {}, Tx num: {}, Latency: {} us, Current Leader: {}",
            idx, client_tag, resp.block_n, resp.tx_n,
            start.elapsed().as_micros(), curr_leader
        );
    }

    {

        let mut byz_resp = byz_resp.lock().unwrap();
        if resp.await_byz_response {
            byz_resp.insert((resp.block_n, resp.tx_n), *start);
        }

        for br in &resp.byz_responses {
            let _start = byz_resp.remove(&(br.block_n, br.tx_n));
            if let Some(_start) = _start {
                info!("Client Id: {}, Block num: {}, Tx num: {}, Byz Latency: {} us",
                    idx, br.block_n, br.tx_n,
                    _start.elapsed().as_micros()
                );
            }
        }
    }

    outstanding_requests.retain(|e| e.0 != client_tag);
    Ok(client_tag)
}

async fn propose_new_request(
    client: &PinnedClient,
    config: &mut ClientConfig,
    current_backoff_ms: &mut u64,
    node_list: &mut Vec<String>,
    outstanding_requests: &mut Vec<(u64, Vec<u8>, Instant)>,
    curr_leader: &mut String,
    workload_generator: &mut Box<dyn PerWorkerWorkloadGenerator>,
    any_node_rr: &mut usize,
    leader_rr: &mut usize,
    client_tag: u64
) {
    let work = workload_generator.next();
    let executor = work.executor;
    let client_req = ProtoClientRequest {
        tx: Some(work.tx),
        // tx: None,
        origin: config.net_config.name.clone(),
        // sig: vec![0u8; SIGNATURE_LENGTH],
        sig: vec![0u8; 1],
        client_tag,
    };

    let rpc_msg_body = ProtoPayload {
        message: Some(
            pft::proto::rpc::proto_payload::Message::ClientRequest(client_req),
        ),
    };

    let mut buf = Vec::new();
    rpc_msg_body.encode(&mut buf).expect("Protobuf error");

    let start = Instant::now();
    loop {
        let res = match &executor {
            Executor::Leader => {
                PinnedClient::send_buffered(
                    &client,
                    &curr_leader,
                    MessageRef(&buf, buf.len(), &pft::rpc::SenderType::Anon),
                ).await
            },
            Executor::Any => {
                let recv_node = &node_list[(*any_node_rr) % node_list.len()];
                *any_node_rr += 1;
                PinnedClient::send(
                    &client,
                    recv_node,
                    MessageRef(&buf, buf.len(), &pft::rpc::SenderType::Anon),
                ).await
            },
        };

        if res.is_err() {
            *leader_rr = (*leader_rr + 1) % config.net_config.nodes.len();
            *curr_leader = config.net_config.nodes.keys().into_iter().collect::<Vec<_>>()[*leader_rr].clone();
            outstanding_requests.clear(); // Clear it out because I am not going to listen on that socket again
            info!("Retrying with leader {} Backoff: {} ms: Error: {:?}", curr_leader, current_backoff_ms, res);
            backoff!(*current_backoff_ms);
            continue;
        }
        break;
    }

    outstanding_requests.push((client_tag, buf, start));
}

async fn client_runner(idx: usize, client: &PinnedClient, num_requests: usize, config: ClientConfig, byz_resp: Arc<Pin<Box<Mutex<HashMap<(u64, u64), Instant>>>>>) -> io::Result<()> {    
    let mut config = config.clone();
    let mut leader_rr = 0;
    let mut any_node_rr = 0;
    let mut node_list = config.net_config.nodes.keys().map(|e| e.clone()).collect::<Vec<_>>();
    let mut curr_leader = node_list[leader_rr].clone();
    let mut i = 0;

    let mut rng: ChaCha20Rng = ChaCha20Rng::seed_from_u64(42 + idx as u64);
    let sample_item: [(bool, i32); 2] = [(true, 1), (false, 499)];


    let weight_dist: WeightedIndex<i32> = WeightedIndex::new(sample_item.iter().map(|(_, weight)| weight)).unwrap();

    let mut workload_generator: Box<dyn PerWorkerWorkloadGenerator> = match &config.workload_config.request_config {
        pft::config::RequestConfig::Blanks => Box::new(BlankWorkloadGenerator{}),
        pft::config::RequestConfig::KVReadWriteUniform(config) => Box::new(KVReadWriteUniformGenerator::new(config)),
        pft::config::RequestConfig::KVReadWriteYCSB(_config) => Box::new(KVReadWriteYCSBGenerator::new(_config, idx, config.workload_config.num_clients)),
        pft::config::RequestConfig::MockSQL() => Box::new(MockSQLGenerator::new()),
    };

    let mut current_backoff_ms = MAX_BACKOFF_MS;

    sleep(Duration::from_millis(current_backoff_ms)).await;
    reset_backoff!(current_backoff_ms);

    let mut outstanding_requests: Vec<(u64, Vec<u8>, Instant)>  = Vec::new();

    while i < num_requests || outstanding_requests.len() > 0 {
        // Propose new request.
        if i < num_requests && outstanding_requests.len() < MAX_OUTSTANDING_REQUESTS {
            // info!("Client Id: {} Proposing request: {}", idx, i);
            propose_new_request(client, &mut config, &mut current_backoff_ms, &mut node_list, &mut outstanding_requests, &mut curr_leader, &mut workload_generator, &mut any_node_rr, &mut leader_rr, i as u64).await;
            i += 1;
        }

        // Check on old requests

        while let Ok(msg) = PinnedClient::try_await_reply(client, &curr_leader).await {
            let res = check_response(idx, client, num_requests, &mut config, &byz_resp, msg, &mut current_backoff_ms, &mut node_list, &mut outstanding_requests, &mut curr_leader, &mut workload_generator, &sample_item, &weight_dist, &mut rng).await;
            // info!("Client Id: {}, Response for {:?}", idx, res);
            if let Err(j) = res {
                let (_, buf, _) = outstanding_requests.iter().find(|e| e.0 == j).unwrap();

                let res = PinnedClient::send(
                    &client,
                    &curr_leader,
                    MessageRef(&buf, buf.len(), &pft::rpc::SenderType::Anon),
                ).await;

                if res.is_err() {
                    leader_rr = (leader_rr + 1) % config.net_config.nodes.len();
                    curr_leader = config.net_config.nodes.keys().into_iter().collect::<Vec<_>>()[leader_rr].clone();
                    outstanding_requests.clear(); // Clear it out because I am not going to listen on that socket again
                    info!("Retrying with leader {} Backoff: {} ms: Error: {:?}", curr_leader, current_backoff_ms, res);
                    backoff!(current_backoff_ms);
                }
            }
        }

        // Force check on old requests
        if outstanding_requests.len() >= MAX_OUTSTANDING_REQUESTS {
            PinnedClient::force_flush(client, &curr_leader).await.expect("Should be able to flush");
            while outstanding_requests.len() > 0 {
                if let Ok(msg) = PinnedClient::await_reply(client, &curr_leader).await {
                    let res = check_response(idx, client, num_requests, &mut config, &byz_resp, msg, &mut current_backoff_ms, &mut node_list, &mut outstanding_requests, &mut curr_leader, &mut workload_generator, &sample_item, &weight_dist, &mut rng).await;
                    // info!("Client Id: {}, Response for {:?} (force)", idx, res);
                    if let Err(j) = res {
                        if let Some((_, buf, _)) = outstanding_requests.iter().find(|e| e.0 == j) {
                            let res = PinnedClient::send(
                                &client,
                                &curr_leader,
                                MessageRef(&buf, buf.len(), &pft::rpc::SenderType::Anon),
                            ).await;
            
                            if res.is_err() {
                                leader_rr = (leader_rr + 1) % config.net_config.nodes.len();
                                curr_leader = config.net_config.nodes.keys().into_iter().collect::<Vec<_>>()[leader_rr].clone();
                                outstanding_requests.clear(); // Clear it out because I am not going to listen on that socket again
                                info!("Retrying with leader {} Backoff: {} ms: Error: {:?}", curr_leader, current_backoff_ms, res);
                                backoff!(current_backoff_ms);
                            }
                        }
        
                    }
                }
            }
        }
        // loop {          // Retry loop
        //     // info!("Client {}: Sending request", idx);
        //     if i % 2 == 0 {
        //         let _ = match &executor {
        //             Executor::Leader => {
        //                 PinnedClient::send(
        //                     &client,
        //                     &curr_leader,
        //                     MessageRef(&buf, buf.len(), &pft::rpc::SenderType::Anon),
        //                 ).await
        //             },
        //             Executor::Any => {
        //                 let recv_node = &node_list[any_node_rr % node_list.len()];
        //                 any_node_rr += 1;
        //                 PinnedClient::send(
        //                     &client,
        //                     recv_node,
        //                     MessageRef(&buf, buf.len(), &pft::rpc::SenderType::Anon),
        //                 ).await
        //             },
        //         };

        //         break;
        //     }

        //     let msg = match &executor {
        //         Executor::Leader => {
        //             PinnedClient::send_and_await_reply(
        //                 &client,
        //                 &curr_leader,
        //                 MessageRef(&buf, buf.len(), &pft::rpc::SenderType::Anon),
        //             ).await
        //         },
        //         Executor::Any => {
        //             let recv_node = &node_list[any_node_rr % node_list.len()];
        //             any_node_rr += 1;
        //             PinnedClient::send_and_await_reply(
        //                 &client,
        //                 recv_node,
        //                 MessageRef(&buf, buf.len(), &pft::rpc::SenderType::Anon),
        //             ).await
        //         },
        //     };
            
            
        //     // info!("Client {}: Received response", idx);


        //     if let Err(e) = msg {
        //         leader_rr = (leader_rr + 1) % config.net_config.nodes.len();
        //         curr_leader = config.net_config.nodes.keys().into_iter().collect::<Vec<_>>()[leader_rr].clone();
        //         info!("Retrying with leader {} Backoff: {} ms: Error: {}", curr_leader, current_backoff_ms, e);
        //         backoff!(current_backoff_ms);
        //         continue;
        //     }

        //     let msg = msg.unwrap();
            
        //     let sz = msg.as_ref().1;
        //     let resp = ProtoClientReply::decode(&msg.as_ref().0.as_slice()[..sz]).unwrap();
        //     if let None = resp.reply {
        //         error!("Empty reply");
        //         continue;
        //     }
        //     if i as u64 != resp.client_tag {
        //         error!("Tag mismatch: {} != {}", i, resp.client_tag);
        //     }
        //     let resp = match resp.reply.unwrap() {
        //         pft::proto::client::proto_client_reply::Reply::Receipt(r) => {
        //             reset_backoff!(current_backoff_ms);
        //             r
        //         },
        //         pft::proto::client::proto_client_reply::Reply::TryAgain(try_again_msg) => {
        //             let node_infos = pft::config::NodeInfo::deserialize(&try_again_msg.serialized_node_infos);
        //             for (k, v) in node_infos.nodes.iter() {
        //                 config.net_config.nodes.insert(k.clone(), v.clone());
        //             }
        //             client.0.config.set(Box::new(config.fill_missing()));
        //             node_list = config.net_config.nodes.keys().map(|e| e.clone()).collect::<Vec<_>>();
        //             debug!("New Net Info: {:#?}", config.net_config.nodes);
        //             backoff!(current_backoff_ms);
        //             continue;
        //         },
        //         pft::proto::client::proto_client_reply::Reply::Leader(l) => {
        //             reset_backoff!(current_backoff_ms);
        //             let node_infos = pft::config::NodeInfo::deserialize(&l.serialized_node_infos);
        //             for (k, v) in node_infos.nodes.iter() {
        //                 config.net_config.nodes.insert(k.clone(), v.clone());
        //             }
        //             client.0.config.set(Box::new(config.fill_missing()));
        //             node_list = config.net_config.nodes.keys().map(|e| e.clone()).collect::<Vec<_>>();
        //             debug!("New Net Info: {:#?}", config.net_config.nodes);
        //             if curr_leader != l.name {
        //                 trace!("Switching leader: {} --> {}", curr_leader, l.name);
        //                 // sleep(Duration::from_millis(10)).await; // Rachel: You fell A-SLEEP?!
                        
        //                 // Drop the connection from the old leader.
        //                 // This is required as one process is generally allowed ~1024 open connections.
        //                 // If ~700 threads have open connections to the leader
        //                 // and the connections to the old leader are not closed,
        //                 // within 2 views, the process will run out of file descriptors.
        //                 // The OS will reset connections.
                        
        //                 // However, doing a drop here is not that efficient, why?
        //                 // Because sometime node1 changes view where node2 is leader,
        //                 // but node1 is still leader for node2 and
        //                 // the curr_leader will ping pong until node2 changes view.
                        
        //                 PinnedClient::drop_connection(&client, &curr_leader).await;
    
    
        //                 curr_leader = l.name.clone();
        //             }
        //             continue;
        //         },
        //         pft::proto::client::proto_client_reply::Reply::TentativeReceipt(r) => {
        //             debug!("Got tentative receipt: {:?}", r);
        //             backoff!(current_backoff_ms);
                    
        //             continue;
        //             // @todo: Wait to see if my txn gets committed in the tentative block.
        //         },
        //     };

        //     if !workload_generator.check_result(&resp.results) {
        //         error!("Unexpected Transaction result!");
        //     }

            
        //     // let should_log = curr_leader == "node1";
        //     let should_log = sample_item[weight_dist.sample(&mut rng)].0;
            
        //     if should_log {
        //         info!("Client Id: {}, Msg Id: {}, Block num: {}, Tx num: {}, Latency: {} us, Current Leader: {}",
        //             idx, i, resp.block_n, resp.tx_n,
        //             start.elapsed().as_micros(), curr_leader
        //         );
        //     }

        //     {

        //         let mut byz_resp = byz_resp.lock().unwrap();
        //         if resp.await_byz_response {
        //             byz_resp.insert((resp.block_n, resp.tx_n), start);
        //         }

        //         for br in &resp.byz_responses {
        //             let _start = byz_resp.remove(&(br.block_n, br.tx_n));
        //             if let Some(_start) = _start {
        //                 info!("Client Id: {}, Block num: {}, Tx num: {}, Byz Latency: {} us",
        //                     idx, br.block_n, br.tx_n,
        //                     _start.elapsed().as_micros()
        //                 );
        //             }
        //         }
        //     }


        //     break;
        // }

    }

    info!("All transactions done");

    Ok(())
}


// async fn byz_poll_worker(idx: usize, client: &PinnedClient, config: &ClientConfig, mut byz_poll_rx: UnboundedReceiver<(u64, u64, Instant)>)  -> io::Result<()> {
//     // TODO: In retry loop, reuse already received messages.
//     // Send list will change when reconfiguration happens.

//     let mut rng = ChaCha20Rng::from_entropy();
//     let mut req_buf = Vec::new();
//     while byz_poll_rx.recv_many(&mut req_buf, 1).await > 0 {
//         loop { // Retry loop
//             let send_list = get_f_plus_one_send_list(config, &mut rng);
//             let (block_n,tx_n, start) = req_buf[0];
//             info!("Client Id: {}, Block num: {}, Tx num: {}, Byz Latency: {} us",
//                 idx, block_n, tx_n,
//                 start.elapsed().as_micros()
//             );
//             break;
//             let req = ProtoPayload {
//                 message: Some(pft::proto::rpc::proto_payload::Message::ByzPollRequest(
//                     ProtoByzPollRequest { block_n, tx_n }
//                 ))
//             };
    
//             let v = req.encode_to_vec();
//             let vlen = v.len();
//             let msg = PinnedMessage::from(v, vlen, pft::rpc::SenderType::Anon);
    
//             let res = PinnedClient::broadcast_and_await_reply(client, &send_list, &msg).await;
//             match res {
//                 Ok(_) => {
//                     info!("Client Id: {}, Block num: {}, Tx num: {}, Byz Latency: {} us",
//                         idx, block_n, tx_n,
//                         start.elapsed().as_micros()
//                     );
//                 },
//                 Err(e) => {
//                     error!("Error in Byz poll: {}", e);
//                     continue;
//                 },
//             }

//             // Check if responses match.
//             let res = res.unwrap();
//             let msg = res[0].as_ref().0;
//             let sz = res[0].as_ref().1;

//             let cmp_reply = ProtoClientReply::decode(&msg.as_slice()[..sz]).unwrap();
//             let cmp_hsh = match cmp_reply.reply {
//                 Some(client::proto_client_reply::Reply::Receipt(receipt)) => {
//                     receipt.req_digest.clone()
//                 },
//                 _ => {
//                     error!("Malformed response!");
//                     continue;
//                 },
//             };

//             let mut malformed_responses = false;

//             for _r in &res {
//                 let msg = res[0].as_ref().0;
//                 let sz = res[0].as_ref().1;

//                 let cmp_reply = ProtoClientReply::decode(&msg.as_slice()[..sz]).unwrap();
//                 let chk_hsh = match cmp_reply.reply {
//                     Some(client::proto_client_reply::Reply::Receipt(receipt)) => {
//                         receipt.req_digest.clone()
//                     },
//                     _ => {
//                         error!("Malformed response!");
//                         malformed_responses = true;
//                         break;
//                     },
//                 };

//                 if !cmp_hash(&chk_hsh, &cmp_hsh) {
//                     error!("Mismatched hash");
//                     malformed_responses = true;
//                     break;
//                 }
//             }

//             if malformed_responses {
//                 continue;
//             }

//             break;
//         }

//         req_buf.clear();
//     }
//     Ok(())
// }

// const NUM_BYZ_POLLERS: usize = 4;

#[tokio::main(flavor = "multi_thread", worker_threads = 8)]
async fn main() -> io::Result<()> {
    log4rs::init_config(default_log4rs_config()).unwrap();
    let config = process_args();
    let mut keys = KeyStore::empty();
    keys.priv_key = KeyStore::get_privkeys(&config.rpc_config.signing_priv_key_path);

    let resp_store = Arc::new(Box::pin(Mutex::new(HashMap::new())));
    
    let mut client_handles = JoinSet::new();
    // let mut tx_vec = Vec::new(); 
    // for i in 0..NUM_BYZ_POLLERS {
    //     let (tx, rx) = mpsc::unbounded_channel();
    //     let client_config = config.clone();
    //     let net_config = client_config.fill_missing();
    //     let c = Client::new(&net_config, &keys).into();
    //     client_handles.spawn(async move {
    //         byz_poll_worker(i, &c, &client_config, rx).await
    //     });
    //     tx_vec.push(tx);
    // }

    for i in 0..config.workload_config.num_clients {
        // let tx_id = i % NUM_BYZ_POLLERS;
        // let tx = tx_vec[tx_id].clone();
        let client_config = config.clone();
        let net_config = client_config.fill_missing();
        let c = Client::new(&net_config, &keys, true, i as u64).into();
        // let _tx = tx.clone();
        let _resp_store = resp_store.clone();
        client_handles.spawn(async move { client_runner(i, &c, config.workload_config.num_requests, client_config.clone(), _resp_store).await });
    }


    while let Some(_) = client_handles.join_next().await {}

    Ok(())
}

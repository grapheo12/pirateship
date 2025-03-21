// Copyright (c) Shubham Mishra. All rights reserved.
// Licensed under the MIT License.

use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use log::{debug, error, info, trace};
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

async fn client_runner(idx: usize, client: &PinnedClient, num_requests: usize, config: ClientConfig, byz_resp: Arc<Pin<Box<Mutex<HashMap<(u64, u64), Instant>>>>>) -> io::Result<()> {    
    let mut config = config.clone();
    let mut leader_rr = 0;
    let mut any_node_rr = 0;
    let mut node_list = config.net_config.nodes.keys().map(|e| e.clone()).collect::<Vec<_>>();
    let mut curr_leader = node_list[leader_rr].clone();
    let mut i = 0;

    let mut rng = ChaCha20Rng::seed_from_u64(42 + idx as u64);
    let sample_item = [(true, 1), (false, 499)];


    let weight_dist = WeightedIndex::new(sample_item.iter().map(|(_, weight)| weight)).unwrap();

    let mut workload_generator: Box<dyn PerWorkerWorkloadGenerator> = match &config.workload_config.request_config {
        pft::config::RequestConfig::Blanks => Box::new(BlankWorkloadGenerator{}),
        pft::config::RequestConfig::KVReadWriteUniform(config) => Box::new(KVReadWriteUniformGenerator::new(config)),
        pft::config::RequestConfig::KVReadWriteYCSB(_config) => Box::new(KVReadWriteYCSBGenerator::new(_config, idx, config.workload_config.num_clients)),
        pft::config::RequestConfig::MockSQL() => Box::new(MockSQLGenerator::new()),
    };

    let mut current_backoff_ms = MAX_BACKOFF_MS;

    sleep(Duration::from_millis(current_backoff_ms)).await;
    reset_backoff!(current_backoff_ms);

    while i < num_requests {
        let work = workload_generator.next();
        let executor = work.executor;
        let client_req = ProtoClientRequest {
            tx: Some(work.tx),
            // tx: None,
            origin: config.net_config.name.clone(),
            // sig: vec![0u8; SIGNATURE_LENGTH],
            sig: vec![0u8; 1]
        };

        let rpc_msg_body = ProtoPayload {
            message: Some(
                pft::proto::rpc::proto_payload::Message::ClientRequest(client_req),
            ),
        };

        let mut buf = Vec::new();
        rpc_msg_body.encode(&mut buf).expect("Protobuf error");

        let start = Instant::now();
        loop {          // Retry loop
            // info!("Client {}: Sending request", idx);

            let msg = match &executor {
                Executor::Leader => {
                    PinnedClient::send_and_await_reply(
                        &client,
                        &curr_leader,
                        MessageRef(&buf, buf.len(), &pft::rpc::SenderType::Anon),
                    ).await
                },
                Executor::Any => {
                    let recv_node = &node_list[any_node_rr % node_list.len()];
                    any_node_rr += 1;
                    PinnedClient::send_and_await_reply(
                        &client,
                        recv_node,
                        MessageRef(&buf, buf.len(), &pft::rpc::SenderType::Anon),
                    ).await
                },
            };
            
            
            // info!("Client {}: Received response", idx);


            if let Err(e) = msg {
                leader_rr = (leader_rr + 1) % config.net_config.nodes.len();
                curr_leader = config.net_config.nodes.keys().into_iter().collect::<Vec<_>>()[leader_rr].clone();
                info!("Retrying with leader {} Backoff: {} ms: Error: {}", curr_leader, current_backoff_ms, e);
                backoff!(current_backoff_ms);
                continue;
            }

            let msg = msg.unwrap();
            
            let sz = msg.as_ref().1;
            let resp = ProtoClientReply::decode(&msg.as_ref().0.as_slice()[..sz]).unwrap();
            if let None = resp.reply {
                continue;
            }
            let resp = match resp.reply.unwrap() {
                pft::proto::client::proto_client_reply::Reply::Receipt(r) => {
                    reset_backoff!(current_backoff_ms);
                    r
                },
                pft::proto::client::proto_client_reply::Reply::TryAgain(try_again_msg) => {
                    let node_infos = pft::config::NodeInfo::deserialize(&try_again_msg.serialized_node_infos);
                    for (k, v) in node_infos.nodes.iter() {
                        config.net_config.nodes.insert(k.clone(), v.clone());
                    }
                    client.0.config.set(Box::new(config.fill_missing()));
                    node_list = config.net_config.nodes.keys().map(|e| e.clone()).collect::<Vec<_>>();
                    debug!("New Net Info: {:#?}", config.net_config.nodes);
                    backoff!(current_backoff_ms);
                    continue;
                },
                pft::proto::client::proto_client_reply::Reply::Leader(l) => {
                    reset_backoff!(current_backoff_ms);
                    let node_infos = pft::config::NodeInfo::deserialize(&l.serialized_node_infos);
                    for (k, v) in node_infos.nodes.iter() {
                        config.net_config.nodes.insert(k.clone(), v.clone());
                    }
                    client.0.config.set(Box::new(config.fill_missing()));
                    node_list = config.net_config.nodes.keys().map(|e| e.clone()).collect::<Vec<_>>();
                    debug!("New Net Info: {:#?}", config.net_config.nodes);
                    if curr_leader != l.name {
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
    
    
                        curr_leader = l.name.clone();
                    }
                    continue;
                },
                pft::proto::client::proto_client_reply::Reply::TentativeReceipt(r) => {
                    debug!("Got tentative receipt: {:?}", r);
                    backoff!(current_backoff_ms);
                    
                    continue;
                    // @todo: Wait to see if my txn gets committed in the tentative block.
                },
            };

            if !workload_generator.check_result(&resp.results) {
                error!("Unexpected Transaction result!");
            }

            
            // let should_log = curr_leader == "node1";
            let should_log = sample_item[weight_dist.sample(&mut rng)].0;
            
            if should_log {
                info!("Client Id: {}, Msg Id: {}, Block num: {}, Tx num: {}, Latency: {} us, Current Leader: {}",
                    idx, i, resp.block_n, resp.tx_n,
                    start.elapsed().as_micros(), curr_leader
                );
            }

            {

                let mut byz_resp = byz_resp.lock().unwrap();
                if resp.await_byz_response {
                    byz_resp.insert((resp.block_n, resp.tx_n), start);
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


            break;
        }

        i += 1;
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

#[tokio::main(flavor = "multi_thread", worker_threads = 50)]
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
        let c = Client::new(&net_config, &keys).into();
        // let _tx = tx.clone();
        let _resp_store = resp_store.clone();
        client_handles.spawn(async move { client_runner(i, &c, config.workload_config.num_requests, client_config.clone(), _resp_store).await });
    }


    while let Some(_) = client_handles.join_next().await {}

    Ok(())
}

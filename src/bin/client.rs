// Copyright (c) Shubham Mishra. All rights reserved.
// Licensed under the MIT License.

use log::{debug, info, trace};
use pft::{
    config::{default_log4rs_config, ClientConfig}, proto::{
        client::{ProtoClientReply, ProtoClientRequest},
        rpc::ProtoPayload,
        execution::ProtoTransaction
    }, crypto::KeyStore, rpc::{
        client::{Client, PinnedClient},
        MessageRef,
    }
};
use prost::Message;
use rand::{distributions::WeightedIndex, prelude::*};
use rand_chacha::ChaCha20Rng;
use std::{env, fs, io, path};
use tokio::task::JoinSet;
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

async fn client_runner(idx: usize, client: &PinnedClient, num_requests: usize, config: ClientConfig) -> io::Result<()> {    
    let mut config = config.clone();
    let mut curr_leader = String::from("node1");
    let mut i = 0;

    let mut rng = ChaCha20Rng::seed_from_u64(42);
    let sample_item = [(true, 1), (false, 999)];


    let weight_dist = WeightedIndex::new(sample_item.iter().map(|(_, weight)| weight)).unwrap();

    while i < num_requests {
        let client_req = ProtoClientRequest {
            tx: Some(ProtoTransaction{
                on_receive: None,
                // on_crash_commit: Some(ProtoTransactionPhase {
                //     ops: vec![ProtoTransactionOp {
                //         op_type: pft::proto::execution::ProtoTransactionOpType::Write.into(),
                //         operands: vec![
                //             format!("crash_commit_{}", i).into_bytes(),
                //             format!("Tx:{}:{}", idx, i).into_bytes()
                //         ],
                //         // operands: Vec::new(),
                //     }],
                // }),
                on_crash_commit: None,
                on_byzantine_commit: None,
                is_reconfiguration: false,
            }),
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
            let msg = PinnedClient::send_and_await_reply(
                &client,
                &curr_leader,
                MessageRef(&buf, buf.len(), &pft::rpc::SenderType::Anon),
            )
            .await
            .unwrap();
            
            let sz = msg.as_ref().1;
            let resp = ProtoClientReply::decode(&msg.as_ref().0.as_slice()[..sz]).unwrap();
            if let None = resp.reply {
                continue;
            }
            let resp = match resp.reply.unwrap() {
                pft::proto::client::proto_client_reply::Reply::Receipt(r) => r,
                pft::proto::client::proto_client_reply::Reply::TryAgain(try_again_msg) => {
                    let node_infos = pft::config::NodeInfo::deserialize(&try_again_msg.serialized_node_infos);
                    for (k, v) in node_infos.nodes.iter() {
                        config.net_config.nodes.insert(k.clone(), v.clone());
                    }
                    client.0.config.set(Box::new(config.fill_missing()));
                    debug!("New Net Info: {:#?}", config.net_config.nodes);
                    continue;
                },
                pft::proto::client::proto_client_reply::Reply::Leader(l) => {
                    let node_infos = pft::config::NodeInfo::deserialize(&l.serialized_node_infos);
                    for (k, v) in node_infos.nodes.iter() {
                        config.net_config.nodes.insert(k.clone(), v.clone());
                    }
                    client.0.config.set(Box::new(config.fill_missing()));
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
                    continue;
                    // @todo: Wait to see if my txn gets committed in the tentative block.
                },
            };

            let should_log = sample_item[weight_dist.sample(&mut rng)].0;

            if should_log {
                info!("Client Id: {}, Msg Id: {}, Block num: {}, Tx num: {}, Latency: {} us, Current Leader: {}",
                    idx, i, resp.block_n, resp.tx_n,
                    start.elapsed().as_micros(), curr_leader
                );
            }
            break;
        }

        i += 1;
    }

    info!("All transactions done");

    Ok(())
}


#[tokio::main(flavor = "multi_thread", worker_threads = 50)]
async fn main() -> io::Result<()> {
    log4rs::init_config(default_log4rs_config()).unwrap();
    let config = process_args();
    let mut keys = KeyStore::empty();
    keys.priv_key = KeyStore::get_privkeys(&config.rpc_config.signing_priv_key_path);

    let mut client_handles = JoinSet::new();
    for i in 0..config.workload_config.num_clients {
        let client_config = config.clone();
        let net_config = client_config.fill_missing();
        let c = Client::new(&net_config, &keys).into();
        client_handles.spawn(async move { client_runner(i, &c, config.workload_config.num_requests, client_config.clone()).await });
    }

    while let Some(_) = client_handles.join_next().await {}

    Ok(())
}

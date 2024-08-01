use hex::ToHex;
use log::{debug, info, warn};
use pft::{
    config::{default_log4rs_config, ClientConfig},
    consensus::proto::{
        client::{ProtoClientReply, ProtoClientRequest},
        rpc::{self, ProtoPayload},
    },
    crypto::KeyStore,
    rpc::{
        client::{Client, PinnedClient},
        MessageRef,
    },
};
use prost::Message;
use std::{env, fs, io, path, time::Duration};
use tokio::{task::JoinSet, time::sleep};
use std::time::Instant;

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

async fn client_runner(idx: usize, client: &PinnedClient, num_requests: usize) -> io::Result<()> {    
    let mut curr_leader = String::from("node1");
    let mut i = 0;
    while i < num_requests {
        let client_req = ProtoClientRequest {
            tx: format!("Tx:{}:{}", idx, i).into_bytes(),
            // sig: vec![0u8; SIGNATURE_LENGTH],
            sig: vec![0u8; 1]
        };

        let rpc_msg_body = ProtoPayload {
            message: Some(
                pft::consensus::proto::rpc::proto_payload::Message::ClientRequest(client_req),
            ),
        };

        let mut buf = Vec::new();
        rpc_msg_body.encode(&mut buf).expect("Protobuf error");

        let start = Instant::now();
        
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
            pft::consensus::proto::client::proto_client_reply::Reply::Receipt(r) => r,
            pft::consensus::proto::client::proto_client_reply::Reply::TryAgain(_) => {
                continue;
            },
            pft::consensus::proto::client::proto_client_reply::Reply::Leader(l) => {
                if curr_leader != l.name {
                    info!("Switching leader: {} --> {}", curr_leader, l.name);
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
            pft::consensus::proto::client::proto_client_reply::Reply::TentativeReceipt(r) => {
                warn!("Got tentative receipt: {:?}", r);
                continue;
                // @todo: Wait to see if my txn gets committed in the tentative block.
            },
        };


        
        if resp.block_n % 1000 == 0 {
            info!("Client Id: {}, Msg Id: {}, Block num: {}, Tx num: {}, Latency: {} us, Current Leader: {}",
                idx, i, resp.block_n, resp.tx_n,
                start.elapsed().as_micros(), curr_leader
            );
        }
        // } else {
        //     debug!("Sending message: {} Reply: {} {} Time: {} us",
        //         format!("Tx:{}:{}", idx, i),
        //         msg.as_ref().0.encode_hex::<String>(), msg.as_ref().1,
        //         start.elapsed().as_micros()
        //     );
        // }

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
        let c = Client::new(&config.fill_missing(), &keys).into();
        client_handles.spawn(async move { client_runner(i, &c, config.workload_config.num_requests).await });
    }

    while let Some(_) = client_handles.join_next().await {}

    Ok(())
}

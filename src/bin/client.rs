use hex::ToHex;
use log::{debug, info};
use pft::{
    config::{default_log4rs_config, ClientConfig},
    consensus::proto::{
        client::ProtoClientRequest,
        rpc::{self, ProtoPayload},
    },
    crypto::KeyStore,
    rpc::{
        client::{Client, PinnedClient},
        MessageRef,
    },
};
use prost::Message;
use std::{env, fs, io, path};
use tokio::task::JoinSet;
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
    for i in 0..num_requests {
        let client_req = ProtoClientRequest {
            tx: format!("Tx:{}:{}", idx, i).into_bytes(),
            // sig: vec![0u8; SIGNATURE_LENGTH],
            sig: vec![0u8; 1]
        };

        let rpc_msg_body = ProtoPayload {
            rpc_type: rpc::RpcType::OneWay.into(),
            rpc_seq_num: i as u64,
            message: Some(
                pft::consensus::proto::rpc::proto_payload::Message::ClientRequest(client_req),
            ),
        };

        let mut buf = Vec::new();
        rpc_msg_body.encode(&mut buf).expect("Protobuf error");

        let start = Instant::now();
        let msg = PinnedClient::send_and_await_reply(
            &client,
            &String::from("node1"),
            MessageRef(&buf, buf.len(), &pft::rpc::SenderType::Anon),
        )
        .await
        .unwrap();

        if i % 1000 == 0 {
            info!("Client Id: {}, Msg Id: {}, Latency: {} us",
                idx, i,
                start.elapsed().as_micros()
            );
        } else {
            debug!("Sending message: {} Reply: {} {} Time: {} us",
                format!("Tx:{}:{}", idx, i),
                msg.as_ref().0.encode_hex::<String>(), msg.as_ref().1,
                start.elapsed().as_micros()
            );
        }
    }

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

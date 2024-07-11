use ed25519_dalek::SIGNATURE_LENGTH;
use log::{debug, info};
use pft::{
    config::ClientConfig,
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
use tokio::{task::JoinSet, time::Instant};

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

const NUM_REQUESTS: u64 = 10000000;

async fn client_runner(idx: usize, client: &PinnedClient) -> io::Result<()> {
    for i in 1..NUM_REQUESTS {
        let start = Instant::now();
        while start.elapsed().as_micros() < 8 {}

        let client_req = ProtoClientRequest {
            tx: format!("Tx:{}:{}", idx, i).into_bytes(),
            sig: vec![0u8; SIGNATURE_LENGTH],
        };

        let rpc_msg_body = ProtoPayload {
            rpc_type: rpc::RpcType::OneWay.into(),
            rpc_seq_num: i,
            message: Some(
                pft::consensus::proto::rpc::proto_payload::Message::ClientRequest(client_req),
            ),
        };

        if i % 1000 == 0 {
            info!("Sending message: {}", format!("Tx:{}:{}", idx, i));
        } else {
            debug!("Sending message: {}", format!("Tx:{}:{}", idx, i));
        }

        // let start = Instant::now();
        let mut buf = Vec::new();
        rpc_msg_body.encode(&mut buf).expect("Protobuf error");
        // info!("Serialize time: {} us", start.elapsed().as_micros());

        // let start = Instant::now();
        PinnedClient::reliable_send(
            &client,
            &String::from("node1"),
            MessageRef(&buf, buf.len(), &pft::rpc::SenderType::Anon),
        )
        .await
        .expect("Should have been able to send!!");
        // info!("Sending time: {} us", start.elapsed().as_micros());
    }

    Ok(())
}

const NUM_CLIENTS: usize = 14;

#[tokio::main]
async fn main() -> io::Result<()> {
    colog::init();
    let config = process_args();
    let mut keys = KeyStore::empty();
    keys.priv_key = KeyStore::get_privkeys(&config.rpc_config.signing_priv_key_path);

    let mut client_handles = JoinSet::new();
    for i in 0..NUM_CLIENTS {
        let c = Client::new(&config.fill_missing(), &keys).into();
        client_handles.spawn(async move { client_runner(i, &c).await });
    }

    while let Some(_) = client_handles.join_next().await {}
    // @todo: Receiving client reply on the same socket.

    Ok(())
}

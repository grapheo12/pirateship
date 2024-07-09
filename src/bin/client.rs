use std::{env, fs, io, path, time::Duration};

use ed25519_dalek::SIGNATURE_LENGTH;
use log::debug;
use pft::{config::ClientConfig, consensus::proto::{client::ProtoClientRequest, rpc::{self, ProtoPayload}}, crypto::KeyStore, rpc::{client::{Client, PinnedClient}, MessageRef}};
use tokio::time::sleep;
use prost::Message;

fn process_args() -> ClientConfig {
    macro_rules! usage_str {() => ("\x1b[31;1mUsage: {} path/to/config.json\x1b[0m");}

    let args: Vec<_> = env::args().collect();

    if args.len() != 2 {
        panic!(usage_str!(), args[0]);
    }

    let cfg_path = path::Path::new(args[1].as_str());
    if !cfg_path.exists() {
        panic!(usage_str!(), args[0]);
    }

    let cfg_contents = fs::read_to_string(cfg_path)
        .expect("Invalid file path");

    ClientConfig::deserialize(&cfg_contents)
}

#[tokio::main]
async fn main() -> io::Result<()> {
    colog::init();
    let config = process_args();
    let mut keys = KeyStore::empty();
    keys.priv_key = KeyStore::get_privkeys(&config.rpc_config.signing_priv_key_path);
    let client = Client::new(&config.fill_missing(), &keys).into();

    for i in 0..10 {
        sleep(Duration::from_millis(100)).await;

        let client_req = ProtoClientRequest { 
            tx: format!("Tx:{}", i).into_bytes(),
            sig: vec![0u8; SIGNATURE_LENGTH]
        };

        let rpc_msg_body = ProtoPayload { 
            rpc_type: rpc::RpcType::OneWay.into(), 
            rpc_seq_num: i, 
            message: Some(pft::consensus::proto::rpc::proto_payload::Message::ClientRequest(client_req))
        };

        let mut buf = Vec::new();
        rpc_msg_body.encode(&mut buf)?;

        debug!("Sending message: {:?}", buf);

        PinnedClient::reliable_send(
            &client, 
            &String::from("node1"), 
            MessageRef(&buf, buf.len(), &pft::rpc::SenderType::Anon)
        ).await?;

    }

    // @todo: Receiving client reply on the same socket.

    Ok(())
}

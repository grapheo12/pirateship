pub mod config;
pub mod rpc;
pub mod crypto;
pub mod consensus;

use config::Config;
use crypto::KeyStore;
use log::info;
use rpc::{client::{Client, PinnedClient}, MessageRef, server::Server};
use tokio::time::sleep;
use std::{env, fs, io, path, sync::{Arc, Mutex}, time::Duration};

/// Fetch json config file from command line path.
/// Panic if not found or parsed properly.
fn process_args() -> Config {
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

    Config::deserialize(&cfg_contents)
}

fn msg_handler(_ctx: Arc<Mutex<()>>, buf: MessageRef) -> bool {
    info!("Received message: {}", std::str::from_utf8(&buf).unwrap_or("Parsing error"));
    false
}

#[tokio::main]
async fn main() -> io::Result<()> {
    colog::init();
    let config = process_args();
    info!("Starting {}", config.net_config.name);
    let keys = KeyStore::new(
        &config.rpc_config.allowed_keylist_path,
        &config.rpc_config.signing_priv_key_path);
    let server = Arc::new(Server::new(&config, msg_handler, &keys));

    let server_handle = tokio::spawn(async move {
        let _ = Server::run(server, Arc::new(Mutex::new(()))).await;
    });

    let client = Client::new(&config, &keys).into();
    let data = String::from("Hello world!\n");
    let data = data.into_bytes();
    sleep(Duration::from_millis(100)).await;
    info!("Sending test message to self!");
    let _ = PinnedClient::send(&client.clone(), &config.net_config.name, MessageRef::from(&data, data.len(), &rpc::SenderType::Anon)).await;
    info!("Send done!");
    sleep(Duration::from_millis(100)).await;
    let _ = PinnedClient::send(&client.clone(), &config.net_config.name, MessageRef::from(&data, data.len(), &rpc::SenderType::Anon)).await;
    info!("Send done twice!");
    PinnedClient::reliable_send(&client.clone(), &config.net_config.name, MessageRef::from(&data, data.len(), &rpc::SenderType::Anon)).await?;
    info!("Reliable send!");
    let _ = tokio::join!(server_handle);
    Ok(())
}

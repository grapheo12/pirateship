use std::{fs, io::Error, path, sync::Arc, time::Duration};

use log::info;
use tokio::time::sleep;

use crate::{config::Config, crypto::KeyStore, rpc::{client::Client, server::Server}};

fn process_args() -> Config {
    let cfg_path = path::Path::new("configs/node1.json");
    if !cfg_path.exists() {
        panic!("Node configs not generated!");
    }

    let cfg_contents = fs::read_to_string(cfg_path)
        .expect("Invalid file path");

    Config::deserialize(&cfg_contents)
}

fn mock_msg_handler(buf: &[u8]) -> bool {
    info!("Received message: {}", std::str::from_utf8(buf).unwrap_or("Parsing error"));
    false
}

async fn run_body(server: &Arc<Server>, client: &Arc<Client>, config: &Config) -> Result<(), Error> {
    let server = server.clone();
    let server_handle = tokio::spawn(async move {
        let _ = Server::run(server).await;
    });
    let data = String::from("Hello world!\n");
    sleep(Duration::from_millis(100)).await;
    info!("Sending test message to self!");
    let _ = Client::send(&client.clone(), &config.net_config.name, data.as_bytes()).await
        .expect("First send should have passed!");
    info!("Send done!");
    sleep(Duration::from_millis(100)).await;
    let _ = Client::send(&client.clone(), &config.net_config.name, data.as_bytes()).await
        .expect_err("Second send should have failed!");
    info!("Send done twice!");
    Client::reliable_send(&client.clone(), &config.net_config.name, data.as_bytes()).await
        .expect("Reliable send should have passed");
    info!("Reliable send!");
    sleep(Duration::from_millis(100)).await;
    server_handle.abort();
    sleep(Duration::from_millis(100)).await;
    Client::reliable_send(&client.clone(), &config.net_config.name, data.as_bytes()).await
        .expect_err("Reliable send should fail after server abort");
    let _ = tokio::join!(server_handle);
    Ok(())
}
#[tokio::test]
async fn test_authenticated_client_server(){
    colog::init();
    let config = process_args();
    info!("Starting {}", config.net_config.name);
    let keys = KeyStore::new(&config.rpc_config.allowed_keylist_path, &config.rpc_config.signing_priv_key_path);
    let server = Arc::new(Server::new(&config.net_config, mock_msg_handler, &keys));
    let client = Arc::new(Client::new(&config.net_config, &keys));
    run_body(&server, &client, &config).await.unwrap();

    let server = Arc::new(Server::new(&config.net_config, mock_msg_handler, &keys));
    run_body(&server, &client, &config).await.unwrap();

    let server = Arc::new(Server::new(&config.net_config, mock_msg_handler, &keys));
    run_body(&server, &client, &config).await.unwrap();
}

#[tokio::test]
async fn test_unauthenticated_client_server(){
    colog::init();
    let config = process_args();
    info!("Starting {}", config.net_config.name);
    let server = Arc::new(Server::new_unauthenticated(&config.net_config, mock_msg_handler));
    let client = Arc::new(Client::new_unauthenticated(&config.net_config));
    run_body(&server, &client, &config).await.unwrap();
    
    let server = Arc::new(Server::new_unauthenticated(&config.net_config, mock_msg_handler));
    run_body(&server, &client, &config).await.unwrap();

    let server = Arc::new(Server::new_unauthenticated(&config.net_config, mock_msg_handler));
    run_body(&server, &client, &config).await.unwrap();
}

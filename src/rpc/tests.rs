use std::{fs, path, sync::Arc, time::Duration};

use log::info;
use tokio::time::sleep;

use crate::{config::Config, rpc::{client::Client, server::Server}};

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

#[tokio::test]
async fn test_unauthenticated_client_server(){
    colog::init();
    let config = process_args();
    info!("Starting {}", config.net_config.name);
    let server = Arc::new(Server::new(&config.net_config, mock_msg_handler));

    let server_handle = tokio::spawn(async move {
        let _ = Server::run(server).await;
    });

    let client = Arc::new(Client::new(&config.net_config));
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
    
    // Second launch
    let server = Arc::new(Server::new(&config.net_config, mock_msg_handler));
    let server_handle = tokio::spawn(async move {
        let _ = Server::run(server).await;
    });
    sleep(Duration::from_millis(100)).await;
    Client::reliable_send(&client.clone(), &config.net_config.name, data.as_bytes()).await
        .expect("Reliable send should succeed after server reboot.");

    sleep(Duration::from_millis(100)).await;
    server_handle.abort();
    let _ = tokio::join!(server_handle);
}

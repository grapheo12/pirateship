use std::{
    fs,
    io::Error,
    path,
    pin::Pin,
    sync::{Arc, Mutex},
    time::Duration,
};

use bytes::Bytes;
use log::info;
use tokio::time::sleep;

use crate::{
    config::Config,
    crypto::KeyStore,
    rpc::{client::Client, server::Server, PinnedMessage},
};

use super::{auth::HandshakeResponse, client::PinnedClient, MessageRef};

fn process_args(i: i32) -> Config {
    let _p = format!("configs/node{i}.json");
    let cfg_path = path::Path::new(&_p);
    if !cfg_path.exists() {
        panic!("Node configs not generated!");
    }

    let cfg_contents = fs::read_to_string(cfg_path).expect("Invalid file path");

    Config::deserialize(&cfg_contents)
}

fn mock_msg_handler(_ctx: &(), buf: MessageRef) -> bool {
    info!(
        "Received message: {}",
        std::str::from_utf8(&buf).unwrap_or("Parsing error")
    );
    false
}

async fn run_body(
    server: &Arc<Server<()>>,
    client: &PinnedClient,
    config: &Config,
) -> Result<(), Error> {
    let server = server.clone();
    let server_handle = tokio::spawn(async move {
        let _ = Server::<()>::run(server, ()).await;
    });
    let data = String::from("Hello world!\n");
    let data = data.into_bytes();
    sleep(Duration::from_millis(100)).await;
    info!("Sending test message to self!");
    let _ = PinnedClient::send(
        &client.clone(),
        &config.net_config.name,
        MessageRef::from(&data, data.len(), &super::SenderType::Anon),
    )
    .await
    .expect("First send should have passed!");
    info!("Send done!");
    sleep(Duration::from_millis(100)).await;
    let _ = PinnedClient::send(
        &client.clone(),
        &config.net_config.name,
        MessageRef::from(&data, data.len(), &super::SenderType::Anon),
    )
    .await
    .expect_err("Second send should have failed!");
    info!("Send done twice!");
    PinnedClient::reliable_send(
        &client.clone(),
        &config.net_config.name,
        MessageRef::from(&data, data.len(), &super::SenderType::Anon),
    )
    .await
    .expect("Reliable send should have passed");
    info!("Reliable send!");
    sleep(Duration::from_millis(100)).await;
    server_handle.abort();
    sleep(Duration::from_millis(100)).await;
    PinnedClient::reliable_send(
        &client.clone(),
        &config.net_config.name,
        MessageRef::from(&data, data.len(), &super::SenderType::Anon),
    )
    .await
    .expect_err("Reliable send should fail after server abort");
    let _ = tokio::join!(server_handle);
    Ok(())
}
#[tokio::test]
async fn test_authenticated_client_server() {
    colog::init();
    let config = process_args(1);
    info!("Starting {}", config.net_config.name);
    let keys = KeyStore::new(
        &config.rpc_config.allowed_keylist_path,
        &config.rpc_config.signing_priv_key_path,
    );
    let server = Arc::new(Server::new(&config, mock_msg_handler, &keys));
    let client = Client::new(&config, &keys).into();
    run_body(&server, &client, &config).await.unwrap();

    let server = Arc::new(Server::new(&config, mock_msg_handler, &keys));
    run_body(&server, &client, &config).await.unwrap();

    let server = Arc::new(Server::new(&config, mock_msg_handler, &keys));
    run_body(&server, &client, &config).await.unwrap();
}

#[tokio::test]
async fn test_unauthenticated_client_server() {
    colog::init();
    let config = process_args(1);
    info!("Starting {}", config.net_config.name);
    let server = Arc::new(Server::new_unauthenticated(&config, mock_msg_handler));
    let client = Client::new_unauthenticated(&config).into();
    run_body(&server, &client, &config).await.unwrap();

    let server = Arc::new(Server::new_unauthenticated(&config, mock_msg_handler));
    run_body(&server, &client, &config).await.unwrap();

    let server = Arc::new(Server::new_unauthenticated(&config, mock_msg_handler));
    run_body(&server, &client, &config).await.unwrap();
}

struct ServerCtx(i32);

fn drop_after_n(ctx: &Arc<Mutex<Pin<Box<ServerCtx>>>>, m: MessageRef) -> bool {
    let mut _ctx = ctx.lock().unwrap();
    _ctx.0 -= 1;
    info!(
        "{:?} said: {}",
        m.sender(),
        std::str::from_utf8(&m).unwrap_or("Parsing error")
    );

    if _ctx.0 <= 0 {
        return false;
    }
    true
}

#[tokio::test]
async fn test_3_node_bcast() {
    colog::init();
    let config1 = process_args(1);
    let config2 = process_args(2);
    let config3 = process_args(3);
    let keys1 = KeyStore::new(
        &config1.rpc_config.allowed_keylist_path,
        &config1.rpc_config.signing_priv_key_path,
    );
    let keys2 = KeyStore::new(
        &config2.rpc_config.allowed_keylist_path,
        &config2.rpc_config.signing_priv_key_path,
    );
    let keys3 = KeyStore::new(
        &config3.rpc_config.allowed_keylist_path,
        &config3.rpc_config.signing_priv_key_path,
    );
    let ctx1 = Arc::new(Mutex::new(Box::pin(ServerCtx(3))));
    let ctx2 = Arc::new(Mutex::new(Box::pin(ServerCtx(1))));
    let ctx3 = Arc::new(Mutex::new(Box::pin(ServerCtx(2))));
    let server1 = Arc::new(Server::new(&config1, drop_after_n, &keys1));
    let server2 = Arc::new(Server::new(&config2, drop_after_n, &keys2));
    let server3 = Arc::new(Server::new(&config3, drop_after_n, &keys3));

    let server_handle1 = tokio::spawn(async move {
        let _ = Server::run(server1, ctx1).await;
    });
    let server_handle2 = tokio::spawn(async move {
        let _ = Server::run(server2, ctx2).await;
    });
    let server_handle3 = tokio::spawn(async move {
        let _ = Server::run(server3, ctx3).await;
    });

    let client = Client::new(&config1, &keys1).into();
    let names = vec![
        String::from("node1"),
        String::from("node2"),
        String::from("node3"),
    ];
    let data = String::from("HelloWorld!!\n");
    let data = data.into_bytes();
    let sz = data.len();
    let data = PinnedMessage::from(data, sz, super::SenderType::Anon);
    PinnedClient::broadcast(&client, &names, &data)
        .await
        .expect("Broadcast should complete with 3 nodes!");
    sleep(Duration::from_millis(100)).await;
    server_handle1.abort();
    let _ = tokio::join!(server_handle1);
    sleep(Duration::from_millis(1000)).await;
    PinnedClient::broadcast(&client, &names, &data)
        .await
        .expect("Broadcast should complete with 2 nodes!");
    sleep(Duration::from_millis(100)).await;
    server_handle2.abort();
    let _ = tokio::join!(server_handle2);

    PinnedClient::broadcast(&client, &names, &data)
        .await
        .expect("There are not enough nodes!");

    sleep(Duration::from_millis(100)).await;
    server_handle3.abort();
    let _ = tokio::join!(server_handle3);
}

#[test]
pub fn test_auth_serde() {
    let h = HandshakeResponse {
        name: String::from("node1"),
        signature: Bytes::from(Vec::from(
            b"1234567812345678123456781234567812345678123456781234567812345678",
        )),
    };

    let resp_buf = h.serialize();

    println!("{:?}", resp_buf);

    let resp = match HandshakeResponse::deserialize(&resp_buf) {
        Ok(r) => r,
        Err(e) => panic!("{}", e),
    };

    println!("{:?}", resp);

    if !(resp.name == h.name && resp.signature == h.signature) {
        panic!("Field mismatch: {:?} vs {:?}", h, resp);
    }
}

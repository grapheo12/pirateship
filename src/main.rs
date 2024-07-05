pub mod config;
pub mod rpc;
pub mod crypto;

use config::Config;
use log::info;
use rpc::server::Server;
use std::{env, fs, io, path, sync::Arc};

// Fetch json config file from command line path.
// Panic if not found or parsed properly.
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

fn msg_handler(buf: &[u8]) -> bool {
    info!("Received message{:#?}", buf);
    false
}

#[tokio::main]
async fn main() -> io::Result<()> {
    colog::init();
    let config = process_args();
    info!("Starting {}", config.net_config.name);
    let server = Arc::new(Server::new(&config.net_config, msg_handler));

    let server_handle = tokio::spawn(async move {
        let _ = Server::run(server).await;
    });

    let _ = tokio::join!(server_handle);
    Ok(())
}

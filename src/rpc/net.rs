use std::{collections::HashMap, io, ops::{Deref, DerefMut}, sync::{Arc, Mutex}};

use crate::config::NetConfig;
use tokio::net::{TcpListener, TcpStream};
use log::{info, warn};

pub struct RemoteState {
    socket: TcpStream,
    session_key: String,
    addr: core::net::SocketAddr
}

pub struct Server {
    pub config: NetConfig,
    pub tls_pub_key: String,
    pub tls_priv_key: String,
    pub node_socket_map: Arc<Mutex<HashMap<String, RemoteState>>>
}

impl Server {
    pub fn new(net_cfg: &NetConfig) -> Server {
        Server {
            config: net_cfg.clone(),
            node_socket_map: Arc::new(Mutex::new(HashMap::new())),
            tls_priv_key: String::from(""),
            tls_pub_key: String::from(""),

        }
    }

    pub async fn init(self: Arc<Self>) -> io::Result<()> {
        let server_addr = &self.config.addr;
        info!("Listening on {}", server_addr);
        let listener = TcpListener::bind(server_addr).await?;
    
        loop {
            match listener.accept().await {
                Ok((socket, addr)) => {
                    // TODO: Simulate TLS handshake.
                    // This also establishes who the node is.
                    let r_name = String::from("node0");
                    let r_st = RemoteState {
                        socket, addr,
                        session_key: String::from("")
                    };

                    let mut node_socket_map = self.node_socket_map.lock().unwrap();
                    node_socket_map.insert(r_name, r_st);
                },
    
                Err(e) => {
                    warn!("{}", e);
                    break;
                }
            }
        }
        Ok(())
    }
}

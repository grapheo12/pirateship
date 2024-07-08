use std::collections::HashMap;
use serde::{Deserialize, Serialize};
use serde_json::Result;

#[cfg(test)]
mod tests;

#[derive(Serialize, Deserialize, Clone)]
pub struct NodeNetInfo {
    pub addr: String,
    pub domain: String
}

#[derive(Serialize, Deserialize, Clone)]
pub struct NetConfig {
    pub name: String,
    pub addr: String,
    pub tls_cert_path: String,
    pub tls_key_path: String,
    pub tls_root_ca_cert_path: String,
    pub nodes: HashMap<String, NodeNetInfo>,
    pub client_max_retry: i32
}

#[derive(Serialize, Deserialize, Clone)]
pub struct RpcConfig {
    pub allowed_keylist_path: String,
    pub signing_priv_key_path: String,
    pub recv_buffer_size: u32,
    pub channel_depth: u32
}

#[derive(Serialize, Deserialize, Clone)]
pub struct Config {
    pub net_config: NetConfig,
    pub rpc_config: RpcConfig
}

impl Config {
    pub fn serialize(self: &Self) -> String  {
        serde_json::to_string_pretty(self).expect("Invalid NetConfig")
    }

    pub fn deserialize(s: &String) -> Config {
        let res: Result<Config> = serde_json::from_str(s.as_str());
        res.expect("Invalid JSON config")
    }
}

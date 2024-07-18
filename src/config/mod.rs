use serde::{Deserialize, Serialize};
use serde_json::Result;
use std::collections::HashMap;

#[cfg(test)]
mod tests;


/// Default config for log4rs;
mod log4rs;
pub use log4rs::*;

#[derive(Serialize, Deserialize, Clone)]
pub struct NodeNetInfo {
    pub addr: String,
    pub domain: String,
}

#[derive(Serialize, Deserialize, Clone)]
pub struct NetConfig {
    pub name: String,
    pub addr: String,
    pub tls_cert_path: String,
    pub tls_key_path: String,
    pub tls_root_ca_cert_path: String,
    pub nodes: HashMap<String, NodeNetInfo>,
    pub client_max_retry: i32,
}

#[derive(Serialize, Deserialize, Clone)]
pub struct RpcConfig {
    pub allowed_keylist_path: String,
    pub signing_priv_key_path: String,
    pub recv_buffer_size: u32,
    pub channel_depth: u32,
}

#[derive(Serialize, Deserialize, Clone)]
pub struct ConsensusConfig {
    pub node_list: Vec<String>, // This better be in the same order in all nodes.
    pub quorum_diversity_k: u64,
    pub stats_report_secs: u64,
    pub max_backlog_batch_size: usize,
    pub signature_max_delay_ms: u64,
    pub signature_max_delay_blocks: u64
}

#[derive(Serialize, Deserialize, Clone)]
pub struct Config {
    pub net_config: NetConfig,
    pub rpc_config: RpcConfig,
    pub consensus_config: ConsensusConfig,
}

#[derive(Serialize, Deserialize, Clone)]
pub struct ClientNetConfig {
    pub name: String,
    pub tls_root_ca_cert_path: String,
    pub nodes: HashMap<String, NodeNetInfo>,
    pub client_max_retry: i32,
}

#[derive(Serialize, Deserialize, Clone)]
pub struct ClientRpcConfig {
    pub signing_priv_key_path: String,

}

#[derive(Serialize, Deserialize, Clone)]
pub struct WorkloadConfig {
    pub num_clients: usize,
    pub num_requests: usize
}

#[derive(Serialize, Deserialize, Clone)]
pub struct ClientConfig {
    pub net_config: ClientNetConfig,
    pub rpc_config: ClientRpcConfig,
    pub workload_config: WorkloadConfig
}

impl Config {
    pub fn serialize(self: &Self) -> String {
        serde_json::to_string_pretty(self).expect("Invalid Config")
    }

    pub fn deserialize(s: &String) -> Config {
        let res: Result<Config> = serde_json::from_str(s.as_str());
        res.expect("Invalid JSON config")
    }
}

impl ClientConfig {
    pub fn serialize(self: &Self) -> String {
        serde_json::to_string_pretty(self).expect("Invalid Config")
    }

    pub fn deserialize(s: &String) -> ClientConfig {
        let res: Result<ClientConfig> = serde_json::from_str(s.as_str());
        res.expect("Invalid JSON config")
    }

    pub fn fill_missing(&self) -> Config {
        Config {
            net_config: NetConfig {
                name: self.net_config.name.clone(),
                addr: String::from(""),
                tls_cert_path: String::from(""),
                tls_key_path: String::from(""),
                tls_root_ca_cert_path: self.net_config.tls_root_ca_cert_path.clone(),
                nodes: self.net_config.nodes.clone(),
                client_max_retry: self.net_config.client_max_retry,
            },
            rpc_config: RpcConfig {
                allowed_keylist_path: String::from(""),
                signing_priv_key_path: self.rpc_config.signing_priv_key_path.clone(),
                recv_buffer_size: 0,
                channel_depth: 0,
            },
            consensus_config: ConsensusConfig {
                node_list: Vec::new(),
                quorum_diversity_k: 0,
                stats_report_secs: 1,
                max_backlog_batch_size: 1,
                signature_max_delay_blocks: 128,
                signature_max_delay_ms: 100
            },
        }
    }
}

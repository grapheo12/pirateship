use crossbeam::atomic::AtomicCell;
use serde::{Deserialize, Serialize};
use serde_json::Result;
use std::{collections::HashMap, sync::Arc};

#[cfg(test)]
mod tests;


/// Default config for log4rs;
mod log4rs;
pub use log4rs::*;

use crate::utils::AtomicStruct;

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct NodeNetInfo {
    pub addr: String,
    pub domain: String,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct NetConfig {
    pub name: String,
    pub addr: String,
    pub tls_cert_path: String,
    pub tls_key_path: String,
    pub tls_root_ca_cert_path: String,
    pub nodes: HashMap<String, NodeNetInfo>,
    pub client_max_retry: i32,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct RpcConfig {
    pub allowed_keylist_path: String,
    pub signing_priv_key_path: String,
    pub recv_buffer_size: u32,
    pub channel_depth: u32,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct ConsensusConfig {
    pub node_list: Vec<String>, // This better be in the same order in all nodes.
    pub learner_list: Vec<String>,
    pub quorum_diversity_k: usize,
    pub max_backlog_batch_size: usize,
    pub signature_max_delay_ms: u64,
    pub view_timeout_ms: u64,
    pub signature_max_delay_blocks: u64,
    pub vote_processing_workers: u16
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct AppConfig {
    pub logger_stats_report_ms: u64,         // This is only for the logger app
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Config {
    pub net_config: NetConfig,
    pub rpc_config: RpcConfig,
    pub consensus_config: ConsensusConfig,
    pub app_config: AppConfig,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct ClientNetConfig {
    pub name: String,
    pub tls_root_ca_cert_path: String,
    pub nodes: HashMap<String, NodeNetInfo>,
    pub client_max_retry: i32,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct ClientRpcConfig {
    pub signing_priv_key_path: String,

}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct WorkloadConfig {
    pub num_clients: usize,
    pub num_requests: usize
}

#[derive(Serialize, Deserialize, Clone, Debug)]
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
                learner_list: Vec::new(),
                quorum_diversity_k: 0,
                max_backlog_batch_size: 1,
                signature_max_delay_blocks: 128,
                signature_max_delay_ms: 100,
                view_timeout_ms: 150,
                vote_processing_workers: 128
            },
            app_config: AppConfig {
                logger_stats_report_ms: 100,
            }
        }
    }
}

pub type AtomicConfig = AtomicStruct<Config>;
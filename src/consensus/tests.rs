// Copyright (c) Shubham Mishra. All rights reserved.
// Licensed under the MIT License.

use std::{collections::{HashMap, HashSet}, fs::exists};

use ed25519_dalek::{SigningKey, SECRET_KEY_LENGTH};

use crate::{config::{AppConfig, Config, ConsensusConfig, EvilConfig, FileStorageConfig, NetConfig, NodeNetInfo, RocksDBConfig, RpcConfig}, crypto::KeyStore, proto::{consensus::{ProtoBlock, ProtoTransactionList}, execution::ProtoTransaction}};

use super::log::{Log, LogEntry};

fn gen_config() -> Config {
    let mut net_config = NetConfig {
        name: "node1".to_string(),
        addr: "0.0.0.0:3001".to_string(),
        tls_cert_path: String::from("blah"),
        tls_key_path: String::from("blah"),
        tls_root_ca_cert_path: String::from("blah"),
        nodes: HashMap::new(),
        client_max_retry: 10,
    };

    for n in 0..5 {
        let mut name = "node".to_string();
        name.push_str(n.to_string().as_str());
        let mut addr = "127.0.0.1:300".to_string();
        addr.push_str(n.to_string().as_str());
        net_config.nodes.insert(
            name,
            NodeNetInfo {
                addr: addr.to_owned(),
                domain: String::from("blah.com"),
            },
        );
    }

    let rpc_config = RpcConfig {
        allowed_keylist_path: String::from("blah/blah"),
        signing_priv_key_path: String::from("blah/blah"),
        recv_buffer_size: (1 << 15),
        channel_depth: 32,
    };

    let consensus_config = ConsensusConfig {
        node_list: vec![
            String::from("node1"),
            String::from("node2"),
            String::from("node3"),
        ],
        learner_list: vec![String::from("node4"), String::from("node5")],
        quorum_diversity_k: 3,
        max_backlog_batch_size: 1000,
        signature_max_delay_blocks: 128,
        signature_max_delay_ms: 100,
        vote_processing_workers: 128,
        view_timeout_ms: 150,
        batch_max_delay_ms: 10,

        #[cfg(feature = "storage")]
        // log_storage_config: crate::config::StorageConfig::FileStorage(FileStorageConfig::default()),
        log_storage_config: crate::config::StorageConfig::RocksDB(RocksDBConfig::default()),

        #[cfg(feature = "platforms")]
        liveness_u: 1,
    };

    let app_config = AppConfig {
        logger_stats_report_ms: 100,
    };

    let evil_config = EvilConfig {
        simulate_byzantine_behavior: false,
        byzantine_start_block: 0,
    };

    let config = Config {
        net_config,
        rpc_config,
        consensus_config,
        app_config,

        #[cfg(feature = "evil")]
        evil_config,
    };

    config
}


macro_rules! log_push_next {
    ( $log:expr, $n:expr, $parent:expr ) => {
        let tx_list = ProtoTransactionList {
            tx_list: vec![ProtoTransaction {
                on_crash_commit: None,
                on_byzantine_commit: None,
                on_receive: None,
                is_reconfiguration: false,
            }; 1000]
        };
        $log.push(LogEntry::new(
            ProtoBlock {
                tx: Some(crate::proto::consensus::proto_block::Tx::TxList(tx_list)),
                n: $n,
                parent: $parent.clone(),
                view: 1,
                qc: Vec::new(),
                fork_validation: Vec::new(),
                view_is_stable: true,
                config_num: 1,
                sig: None
            })).unwrap();
        $n += 1;
        $parent = $log.last_hash();
    };

    ( $log:expr, $n:expr, $parent:expr, $keys:expr ) => {
        let tx_list = ProtoTransactionList {
            tx_list: vec![]
        };
        $log.push_and_sign(LogEntry::new(
            ProtoBlock {
                tx: Some(crate::proto::consensus::proto_block::Tx::TxList(tx_list)),
                n: $n,
                parent: $parent.clone(),
                view: 1,
                qc: Vec::new(),
                fork_validation: Vec::new(),
                view_is_stable: true,
                config_num: 1,
                sig: None
            }), $keys).unwrap();
        $n += 1;
        $parent = $log.last_hash();
    };
}
/// Test plan:
/// (Push 100 then push 1 signed) * 100
/// GC till 9000
/// Then try to truncate.
pub fn test_log_plan() {
    let config = gen_config();
    if exists("/tmp/testdb").unwrap() {
        std::fs::remove_dir_all("/tmp/testdb").unwrap();
    }
    let mut log = Log::new(config);
    let mut n = 1;
    let mut parent = log.hash_at_n(0).unwrap();
    let mut keys = KeyStore::empty();
    let secret_key: [u8; SECRET_KEY_LENGTH] = [
        0xca, 0xfe, 0xba, 0xbe,
        0xca, 0xfe, 0xba, 0xbe,
        0xca, 0xfe, 0xba, 0xbe,
        0xca, 0xfe, 0xba, 0xbe,
        0xca, 0xfe, 0xba, 0xbe,
        0xca, 0xfe, 0xba, 0xbe,
        0xca, 0xfe, 0xba, 0xbe,
        0xca, 0xfe, 0xba, 0xbe,
    ];
    keys.priv_key = SigningKey::from_bytes(&secret_key);

    for _ in 0..100 {
        for _ in 0..100 {
            log_push_next!(log, n, parent);
        }
        log_push_next!(log, n, parent, &keys);
    }

    #[cfg(feature = "storage")]
    log.garbage_collect_upto(9000);

    let block = log.get(9001).unwrap();
    assert!(block.block.n == 9001);

    let block = log.get(9000).unwrap();
    assert!(block.block.n == 9000);

    let block = log.get(8999).unwrap();
    assert!(block.block.n == 8999);

    let block = log.get(8800).unwrap();
    assert!(block.block.n == 8800);

    let block = log.get(1).unwrap();
    assert!(block.block.n == 1);

    assert!(log.truncate(9010).unwrap() == 9010);

    #[cfg(feature = "storage")]
    assert!(log.truncate(9000).is_err());




}

#[test]
fn test_log() {
    test_log_plan();
}
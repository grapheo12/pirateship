use std::collections::HashMap;

use gluesql::sled_storage::sled::transaction::TransactionResult;
use nix::libc::qos_class_t;
use serde::{Serialize, Deserialize};

use crate::{config::AtomicConfig, consensus_v2::app::AppEngine};
use crate::config::{AppConfig, ClientConfig, ClientNetConfig, ClientRpcConfig, Config, ConsensusConfig, EvilConfig, KVReadWriteUniform, NetConfig, NodeNetInfo, RocksDBConfig, RpcConfig, WorkloadConfig};

use crate::crypto::CachedBlock;
use crate::proto::execution::{
    ProtoTransaction, ProtoTransactionPhase, ProtoTransactionOp, ProtoTransactionOpType,
    ProtoTransactionResult, ProtoTransactionOpResult,
};

use crate::proto::consensus::{
    ProtoBlock, ProtoQuorumCertificate, ProtoForkValidation, ProtoVote, ProtoSignatureArrayEntry,
    ProtoNameWithSignature, proto_block::Sig, DefferedSignature,
};

#[derive(std::fmt:: Debug, Clone, Serialize, Deserialize)]
pub struct KVSState {
    pub ci_state: HashMap<Vec<u8>, Vec<(u64, Vec<u8>) /* versions */>>,
    pub bci_state: HashMap<Vec<u8>, Vec<u8>>,
}

pub struct KVSAppEngine {
    config: AtomicConfig,
    pub last_ci: u64,
    pub last_bci: u64,
    quit_signal: bool,
    state: KVSState,
    
}

impl AppEngine for KVSAppEngine {
    type State = KVSState;

    fn new(config: AtomicConfig) -> Self {
        Self {
            config,
            last_ci: 0,
            last_bci: 0,
            quit_signal: false,
            state: KVSState {
                ci_state: HashMap::new(),
                bci_state: HashMap::new(),
            },
        }
    }

    fn handle_crash_commit(&mut self, blocks: Vec<crate::crypto::CachedBlock>) -> Vec<Vec<crate::proto::execution::ProtoTransactionResult>> {
        // iterate through eac txn in each block
        // see if that txn on_crash_commit is not none
        //     iterate through all ops in order, see ops in protobuf
        //     for every op you have a result
        //     each txn gives a txn result. Transaction has many ops, prototransaction has many op results
            //if op type is write --> add to ci_state, if read dont add to ci_state

        //the return value is a list of lists containing transaction results for each block
        let mut block_count = 0;
        let mut txn_count = 0;

        let mut final_result: Vec<Vec<crate::proto::execution::ProtoTransactionResult>> = Vec::new();

        for block in blocks.iter() {
            let proto_block: &ProtoBlock = &block.block;
            let mut block_result: Vec<crate::proto::execution::ProtoTransactionResult> = Vec::new(); 
            for tx in proto_block.tx_list.iter() {
                let ops: &_ = match &tx.on_crash_commit {
                    Some(ops) => &ops.ops,
                    None => continue,
                };

                let mut txn_result = ProtoTransactionResult {
                    result: Vec::new(),
                };

                for op in ops.iter() {
                    if op.operands.len() != 2 {
                        continue;
                    }
                    let key = &op.operands[0];
                    let val = &op.operands[1];
                    
                    if let Some(op_type) = ProtoTransactionOpType::from_i32(op.op_type) {
                        if op_type == ProtoTransactionOpType::Write {
                            if self.state.ci_state.contains_key(key) {
                                self.state.ci_state.get_mut(key).unwrap().push((proto_block.n, val.clone()));
                            } else {
                                self.state.ci_state.insert(key.clone(), vec![(proto_block.n, val.clone())]);
                            }
                        }
                    } else {
                        println!("Invalid operation type: {}", op.op_type);
                        continue;
                    }

                    txn_result.result.push(ProtoTransactionOpResult {
                        success: true,
                        values: vec![],
                    });
                }
                block_result.push(txn_result);
                //test
                txn_count += 1;
            }
            final_result.push(block_result);

            //test
            block_count += 1;
        }
        println!("block count:{}", block_count);
        println!("transaction count{}", txn_count);
        return final_result;
    }

    fn handle_byz_commit(&mut self, blocks: Vec<crate::crypto::CachedBlock>) -> Vec<Vec<crate::proto::client::ProtoByzResponse>> {
        
        todo!()
    }

    fn handle_rollback(&mut self, rolled_back_blocks: u64) {
        todo!()
    }

    fn handle_unlogged_request(&mut self, request: crate::proto::execution::ProtoTransaction) -> crate::proto::execution::ProtoTransactionResult {
        //read requests
        //see how to handle crash commits + read requests
        //and then byz commit and rollback
        todo!()
    }

    fn get_current_state(&self) -> Self::State {
        //clone of state
        todo!()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::AtomicConfig;

    #[test]
    fn test_setup() {
        let engine = setup_engine();
        println!("{}", engine.last_bci);
        println!("{}", engine.last_ci);
        println!("{}", engine.quit_signal);
    }

    #[test]
    fn test_crash_commit() {
        let mut engine: KVSAppEngine = setup_engine();
        let blocks = vec![
            create_dummy_cached_block(),
            create_dummy_cached_block(), // Add multiple blocks for testing
        ];

        let result: Vec<Vec<crate::proto::execution::ProtoTransactionResult>> = engine.handle_crash_commit(blocks);
        dbg!(&engine.state.ci_state);
        dbg!(&result);
        println!("{}", engine.last_ci)
    }

    

    fn setup_engine() -> KVSAppEngine {
        let mut net_config = NetConfig {
            name: "node1".to_string(),
            addr: "0.0.0.0:3001".to_string(),
            tls_cert_path: String::from("blah"),
            tls_key_path: String::from("blah"),
            tls_root_ca_cert_path: String::from("blah"),
            nodes: HashMap::new(),
            client_max_retry: 10,
        };

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
            log_storage_config: crate::config::StorageConfig::RocksDB(RocksDBConfig::default()),
    
            #[cfg(feature = "platforms")]
            liveness_u: 1,
        };
    
        let app_config = AppConfig {
            logger_stats_report_ms: 100,
            checkpoint_interval_ms: 60000,
        };
    
        let evil_config = EvilConfig {
            simulate_byzantine_behavior: true,
            byzantine_start_block: 20000,
        };
    
        let config = Config {
            net_config,
            rpc_config,
            consensus_config,
            app_config,
        };
    
        let atomic_config = AtomicConfig::new(config);

        KVSAppEngine::new(atomic_config)
    }

// Function to generate a dummy CachedBlock
fn create_dummy_cached_block() -> CachedBlock {
    // Create a dummy transaction operation
    let dummy_op = ProtoTransactionOp {
        op_type: ProtoTransactionOpType::Write as i32, // Example transaction type
        operands: vec![vec![1], vec![4]] // Example byte operands
    };

    // Create a dummy transaction phase
    let dummy_phase = ProtoTransactionPhase {
        ops: vec![dummy_op.clone()],
    };

    // Create a dummy transaction
    let dummy_transaction = ProtoTransaction {
        on_receive: Some(dummy_phase.clone()),
        on_crash_commit: Some(dummy_phase.clone()),
        on_byzantine_commit: None, // Example: No Byzantine commit
        is_reconfiguration: false,
    };

    // Create a dummy quorum certificate
    let dummy_qc = ProtoQuorumCertificate {
        digest: vec![0xaa, 0xbb, 0xcc], // Example block hash
        n: 1,
        sig: vec![ProtoNameWithSignature {
            name: "Validator1".to_string(),
            sig: vec![0xde, 0xad, 0xbe, 0xef], // Example signature
        }],
        view: 1,
    };

    // Create a dummy fork validation entry
    let dummy_fork_validation = ProtoForkValidation {
        view: 3,
        fork_hash: vec![0xba, 0xad, 0xf0, 0x0d], // Example fork hash
        fork_sig: vec![0xca, 0xfe, 0xba, 0xbe], // Example fork signature
        fork_len: 2,
        fork_last_qc: Some(dummy_qc.clone()),
        name: "ForkValidator".to_string(),
    };

    // Create a dummy vote
    let dummy_vote = ProtoVote {
        sig_array: vec![ProtoSignatureArrayEntry {
            n: 10,
            sig: vec![0xbe, 0xef, 0xfa, 0xce], // Example signature bytes
        }],
        fork_digest: vec![0x12, 0x34, 0x56, 0x78], // Example fork digest
        n: 10,
        view: 4,
        config_num: 1,
    };

    // Create a dummy ProtoBlock with transactions
    let dummy_proto_block = ProtoBlock {
        tx_list: vec![dummy_transaction.clone(), dummy_transaction.clone()], // Transactions
        n: 42, // Example sequence number
        parent: vec![0xde, 0xad, 0xbe, 0xef], // Example parent hash
        view: 2,
        qc: vec![dummy_qc], // Quorum certificates
        fork_validation: vec![dummy_fork_validation], // Fork validation
        view_is_stable: true,
        config_num: 1,
        sig: Some(Sig::NoSig(DefferedSignature {})), // Example: No proposer signature
    };

    let block_ser = vec![0; 100]; // Dummy serialized block (100 bytes of zeroes)
    let block_hash = vec![0xaa; 32]; // Dummy hash (32 bytes of `0xaa`)

    return CachedBlock::new(dummy_proto_block, block_ser, block_hash);
    }
}

    

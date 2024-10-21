use rand_chacha::ChaCha20Rng;
use rand::{distributions::{Uniform, WeightedIndex}, prelude::*};

use crate::{config::KVReadWriteUniform, proto::{client::ProtoTransactionReceipt, execution::{ProtoTransaction, ProtoTransactionOp, ProtoTransactionOpType, ProtoTransactionPhase, ProtoTransactionResult}}};

pub trait PerWorkerWorkloadGenerator: Send {
    fn next(&mut self) -> ProtoTransaction;
    fn check_result(&self, result: &Option<ProtoTransactionResult>) -> bool;
}

pub struct BlankWorkloadGenerator { }

impl PerWorkerWorkloadGenerator for BlankWorkloadGenerator {
    fn next(&mut self) -> ProtoTransaction {
        ProtoTransaction{
            on_receive: None,
            // on_crash_commit: Some(ProtoTransactionPhase {
            //     ops: vec![ProtoTransactionOp {
            //         op_type: pft::proto::execution::ProtoTransactionOpType::Write.into(),
            //         operands: vec![
            //             format!("crash_commit_{}", i).into_bytes(),
            //             format!("Tx:{}:{}", idx, i).into_bytes()
            //         ],
            //         // operands: Vec::new(),
            //     }],
            // }),
            on_crash_commit: None,
            on_byzantine_commit: None,
            is_reconfiguration: false,
        }
    }
    
    fn check_result(&self, _result: &Option<ProtoTransactionResult>) -> bool {
        true
    }
}

#[derive(Clone)]
enum TxOpType {
    Read,
    WriteCrash,
    WriteByz
}

pub struct KVReadWriteUniformGenerator { 
    config: KVReadWriteUniform,
    rng: ChaCha20Rng,
    sample_item: [(TxOpType, i32); 3],
    weight_dist: WeightedIndex<i32>,
    uniform_dist: Uniform<usize>,
    last_request_type: TxOpType,
}

impl KVReadWriteUniformGenerator {
    pub fn new(config: &KVReadWriteUniform) -> KVReadWriteUniformGenerator {
        let rng = ChaCha20Rng::seed_from_u64(210);
        let sample_item = [
            (TxOpType::Read, (config.read_ratio * 1000.0) as i32),
            (TxOpType::WriteByz, (config.write_byz_commit_ratio * 1000.0) as i32),
            (TxOpType::WriteCrash, ((1.0 - config.write_byz_commit_ratio - config.read_ratio) * 1000.0) as i32),
        ];
        for item in &sample_item {
            if item.1 < 0 || item.1 > 1000 {
                panic!("Invalid config");
            }
        }

        let weight_dist = WeightedIndex::new(sample_item.iter().map(|(_, weight)| weight)).unwrap();

        let uniform_dist = Uniform::new(0, config.num_keys);
        KVReadWriteUniformGenerator {
            config: config.clone(),
            rng,
            sample_item,
            weight_dist,
            uniform_dist,
            last_request_type: TxOpType::Read
        }
    
    }

    fn get_next_key(&mut self) -> Vec<u8> {
        let key_num = self.rng.sample(self.uniform_dist);
        let key = String::from("key:") + &key_num.to_string();
        key.into()
    }
}

impl PerWorkerWorkloadGenerator for KVReadWriteUniformGenerator {
    fn next(&mut self) -> ProtoTransaction {
        let next_op = &self.sample_item[self.weight_dist.sample(&mut self.rng)].0;
        self.last_request_type = next_op.clone();

        let mut ret = ProtoTransaction{
            on_receive: None,
            on_crash_commit: None,
            on_byzantine_commit: None,
            is_reconfiguration: false,
        };
        
        match next_op {
            TxOpType::Read => {
                let key = self.get_next_key();
                ret.on_receive = Some(ProtoTransactionPhase {
                    ops: vec![ProtoTransactionOp {
                        op_type: ProtoTransactionOpType::Read.into(),
                        operands: vec![key] 
                    }]
                });
            },
            TxOpType::WriteCrash => {
                let key = self.get_next_key();
                let val = vec![0u8; self.config.val_size];
                ret.on_crash_commit = Some(ProtoTransactionPhase {
                    ops: vec![ProtoTransactionOp {
                        op_type: ProtoTransactionOpType::Write.into(),
                        operands: vec![key, val]
                    }]
                })
            },
            TxOpType::WriteByz => {
                let key = self.get_next_key();
                let val = vec![0u8; self.config.val_size];
                ret.on_byzantine_commit = Some(ProtoTransactionPhase {
                    ops: vec![ProtoTransactionOp {
                        op_type: ProtoTransactionOpType::Write.into(),
                        operands: vec![key, val]
                    }]
                })
            },
        }


        ret      
    }
    
    fn check_result(&self, result: &Option<ProtoTransactionResult>) -> bool {
        if let TxOpType::Read = self.last_request_type {
            if result.is_none() || result.as_ref().unwrap().result.len() == 0 {
                return false;
            }
            
            return true;
        }

        true

    }
}


pub struct MockSQLGenerator { 
    pub query_num: usize
}

impl MockSQLGenerator {
    pub fn new() -> Self {
        Self {
            query_num: 0
        }
    }
}

impl PerWorkerWorkloadGenerator for MockSQLGenerator {
    fn next(&mut self) -> ProtoTransaction {
        let query = match self.query_num {
            0 => String::from("CREATE TABLE foo(id INT PRIMARY KEY, num INT);"),
            1 => String::from("INSERT INTO foo VALUES (1, 1);"),
            n => {
                match n % 2 {
                    0 => format!("UPDATE foo SET num = num + 1;"),
                    1 => format!("SELECT * FROM foo;"),
                    _ => panic!("Unreachable")
                }
            }
        };
        self.query_num += 1;

        let query_vec = query.as_bytes().to_vec();

        ProtoTransaction{
            on_receive: None,
            on_crash_commit: Some(ProtoTransactionPhase {
                ops: vec![ProtoTransactionOp {
                    op_type: ProtoTransactionOpType::Custom.into(),
                    operands: vec![query_vec],
                }],
            }),
            on_byzantine_commit: None,
            is_reconfiguration: false,
        }
    }
    
    fn check_result(&self, _result: &Option<ProtoTransactionResult>) -> bool {
        true
    }
}
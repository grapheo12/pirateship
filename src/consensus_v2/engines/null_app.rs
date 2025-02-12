use serde::{Serialize, Deserialize};

use crate::{consensus_v2::app::AppEngine, proto::execution::{ProtoTransactionOpResult, ProtoTransactionPhase, ProtoTransactionResult}};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NullApp;

impl AppEngine  for NullApp {
    type State = Self;

    fn new(_config: crate::config::AtomicConfig) -> Self {
        Self{}
    }

    fn handle_crash_commit(&mut self, blocks: Vec<crate::crypto::CachedBlock>) -> Vec<Vec<crate::proto::execution::ProtoTransactionResult>> {
        blocks.iter().map(|block| {
            block.block.tx_list.iter().map(|tx| {
                ProtoTransactionResult {
                    result: tx.on_crash_commit.as_ref().unwrap_or(&ProtoTransactionPhase::default())
                        .ops.iter().map(|_| ProtoTransactionOpResult {
                            success: true,
                            values: vec![],
                        }).collect(),
                }
            }).collect()
        }).collect()
    }

    fn handle_byz_commit(&mut self, blocks: Vec<crate::crypto::CachedBlock>) -> Vec<Vec<crate::proto::execution::ProtoTransactionResult>> {
        blocks.iter().map(|block| {
            block.block.tx_list.iter().map(|tx| {
                ProtoTransactionResult {
                    result: tx.on_crash_commit.as_ref().unwrap_or(&ProtoTransactionPhase::default())
                        .ops.iter().map(|_| ProtoTransactionOpResult {
                            success: true,
                            values: vec![],
                        }).collect(),
                }
            }).collect()
        }).collect()
    }

    fn handle_rollback(&mut self, _num_rolled_back_blocks: u64) {
        // Nothing to do
    }

    fn handle_unlogged_request(&mut self, request: crate::proto::execution::ProtoTransaction) -> crate::proto::execution::ProtoTransactionResult {
        ProtoTransactionResult {
            result: request.on_receive.as_ref().unwrap_or(&ProtoTransactionPhase::default())
                .ops.iter().map(|_| ProtoTransactionOpResult {
                    success: true,
                    values: vec![],
                }).collect(),
        }
    }

    fn get_current_state(&self) -> Self::State {
        self.clone()
    }
}


use std::collections::HashMap;

use serde::{Serialize, Deserialize};

use crate::{config::AtomicConfig, consensus_v2::app::AppEngine};

#[derive(std::fmt::Display, Debug, Clone, Serialize, Deserialize)]
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
        todo!()
    }

    fn handle_crash_commit(&mut self, blocks: Vec<crate::crypto::CachedBlock>) -> Vec<Vec<crate::proto::execution::ProtoTransactionResult>> {
        todo!()
    }

    fn handle_byz_commit(&mut self, blocks: Vec<crate::crypto::CachedBlock>) -> Vec<Vec<crate::proto::execution::ProtoTransactionResult>> {
        todo!()
    }

    fn handle_rollback(&mut self, rolled_back_blocks: Vec<crate::crypto::CachedBlock>) {
        todo!()
    }

    fn handle_unlogged_request(&mut self, request: crate::proto::execution::ProtoTransaction) -> crate::proto::execution::ProtoTransactionResult {
        todo!()
    }

    fn get_current_state(&self) -> Self::State {
        todo!()
    }
}
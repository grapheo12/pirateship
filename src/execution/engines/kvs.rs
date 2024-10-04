// Copyright (c) Shubham Mishra. All rights reserved.
// Licensed under the MIT License.

use std::{collections::HashMap, ops::Deref, pin::Pin, sync::{atomic::{AtomicBool, AtomicU64}, Arc}};

use indexmap::IndexMap;

use crate::{consensus::handler::PinnedServerContext, execution::Engine};

pub struct KVStoreEngine {
    pub ctx: PinnedServerContext,
    pub last_ci: AtomicU64,
    pub last_bci: AtomicU64,
    pub quit_signal: AtomicBool,

    pub ci_state: IndexMap<String, (u64, String)>,
    pub bci_state: HashMap<String, String>
}

#[derive(Clone)]
pub struct PinnedKVStoreEngine(Arc<Pin<Box<KVStoreEngine>>>);

impl Deref for PinnedKVStoreEngine {
    type Target = KVStoreEngine;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl KVStoreEngine {
    pub fn new(ctx: PinnedServerContext) -> Self {
        Self {
            ctx,
            last_ci: AtomicU64::new(0),
            last_bci: AtomicU64::new(0),
            quit_signal: AtomicBool::new(false),
            ci_state: IndexMap::new(),
            bci_state: HashMap::new()
        }
    }
}

impl Engine for PinnedKVStoreEngine {
    fn new(ctx: PinnedServerContext) -> Self {
        Self(Arc::new(Box::pin(KVStoreEngine::new(ctx))))
    }

    /// @todo:
    /// Application logic:
    /// Run all rollbacks first
    /// Then insert all writes from ci-ed blocks to ci_state.
    /// Then insert all writes from bci-ed blocks to bci_state.
    /// Iterate through ci_state, popping results and inserting them in bci_state if seq_num is <= bci
    /// (Stop at first > bci entry, it is an IndexMap)
    /// 
    /// Rollback logic: Physical Undo + Logical Redo
    /// No WAL here. So on rollback, delete ci_state.
    /// Then from bci + 1 to new ci, reapply all writes.
    /// 
    /// Read logic: Acquire locks in same order.
    /// fork -> ci_state -> bci_state. Helps avoid deadlocks.
    /// First check key in ci_state, if not found, check bci_state.
    async fn run(&self) {
    }

    fn signal_quit(&self) {
        // Nothing to do here.
    }

    fn signal_crash_commit(&self, ci: u64) {
        
    }

    fn signal_byzantine_commit(&self, bci: u64) {
        todo!()
    }

    fn signal_rollback(&self, ci: u64) {
        todo!()
    }

    fn get_unlogged_execution_result(&self, request: crate::proto::execution::ProtoTransactionPhase) -> crate::proto::execution::ProtoTransactionResult {
        todo!()
    }
}
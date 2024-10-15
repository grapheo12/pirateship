// Copyright (c) Shubham Mishra. All rights reserved.
// Licensed under the MIT License.

use std::{collections::HashMap, ops::Deref, pin::Pin, sync::{atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering}, Arc, Mutex, MutexGuard}};

use log::info;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};

use crate::{consensus::{handler::PinnedServerContext, log::Log}, execution::Engine, proto::execution::{ProtoTransactionOpResult, ProtoTransactionResult}};

enum Event {
    CiUpd(u64),
    BciUpd(u64),
    Rback(u64)
}

pub struct KVStoreEngine {
    pub ctx: PinnedServerContext,
    pub last_ci: AtomicU64,
    pub last_bci: AtomicU64,
    quit_signal: AtomicBool,

    pub ci_state: Mutex<HashMap<Vec<u8>, Vec<(u64, Vec<u8>) /* versions */>>>,
    pub bci_state: Mutex<HashMap<Vec<u8>, Vec<u8>>>,

    event_chan: (UnboundedSender<Event>, Mutex<UnboundedReceiver<Event>>),

    pub num_crash_committed_writes: AtomicUsize,
    pub num_byz_committed_writes: AtomicUsize,
    pub num_reads: AtomicUsize,
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
        let chan = unbounded_channel();
        Self {
            ctx,
            last_ci: AtomicU64::new(0),
            last_bci: AtomicU64::new(0),
            quit_signal: AtomicBool::new(false),
            ci_state: Mutex::new(HashMap::new()),
            bci_state: Mutex::new(HashMap::new()),
            event_chan: (chan.0, Mutex::new(chan.1)),
            num_crash_committed_writes: AtomicUsize::new(0),
            num_byz_committed_writes: AtomicUsize::new(0),
            num_reads: AtomicUsize::new(0),
        }
    }

    fn execute_ops(&self,
        fork: &tokio::sync::MutexGuard<Log>,
        ci_state: &mut MutexGuard<HashMap<Vec<u8>, Vec<(u64, Vec<u8>)>>>,
        bci_state: &mut MutexGuard<HashMap<Vec<u8>, Vec<u8>>>,
        event_recv: &mut MutexGuard<UnboundedReceiver<Event>>
    ) {
        while let Ok(msg) = event_recv.try_recv() {
            match msg {
                Event::CiUpd(ci) => self.execute_crash_commit(ci, fork, ci_state, bci_state),
                Event::BciUpd(bci) => self.execute_byz_commit(bci, fork, ci_state, bci_state),
                Event::Rback(ci) => self.execute_rollback(ci, fork, ci_state, bci_state),
            }
        }
    }

    fn execute_crash_commit(&self, ci: u64,
        fork: &tokio::sync::MutexGuard<Log>,
        ci_state: &mut MutexGuard<HashMap<Vec<u8>, Vec<(u64, Vec<u8>)>>>,
        _bci_state: &mut MutexGuard<HashMap<Vec<u8>, Vec<u8>>>,
    ) {
        let old_ci = self.last_ci.load(Ordering::SeqCst);
        if old_ci >= ci {
            return;
        }

        for pos in (old_ci + 1)..(ci + 1) {
            let block = &fork.get(pos).unwrap().block;
            for tx in &block.tx {
                let ops = match &tx.on_crash_commit {
                    Some(ops) => &ops.ops,
                    None => continue
                };

                for op in ops {
                    match op.op_type() {
                        crate::proto::execution::ProtoTransactionOpType::Write => {
                            let num_crash_writes = self.num_crash_committed_writes.fetch_add(1, Ordering::SeqCst) + 1;
                            if num_crash_writes % 1000 == 0 {
                                info!("Num Crash Committed Write Requests: {}; Num Keys only crash committed: {}", num_crash_writes, ci_state.len());
                            }
                            // Sanity check
                            // Format (key, val)
                            if op.operands.len() != 2 {
                                continue;
                            }

                            let key = &op.operands[0];
                            let val = &op.operands[1];

                            if ci_state.contains_key(key) {
                                ci_state.get_mut(key).unwrap().push((pos, val.clone()));
                            } else {
                                ci_state.insert(key.clone(), vec![(pos, val.clone())]);
                            }
                        },

                        _ => {
                            // Treat as Noop

                        }
                    }
                }
            }

        }

        self.last_ci.store(ci, Ordering::SeqCst);


    }

    fn execute_byz_commit(&self, bci: u64,
        fork: &tokio::sync::MutexGuard<Log>,
        ci_state: &mut MutexGuard<HashMap<Vec<u8>, Vec<(u64, Vec<u8>)>>>,
        bci_state: &mut MutexGuard<HashMap<Vec<u8>, Vec<u8>>>,
    ) {
        let old_bci = self.last_bci.load(Ordering::SeqCst);
        if old_bci >= bci {
            return;
        }

        // First execute the on_byz_commit transaction phases.s
        for pos in (old_bci + 1)..(bci + 1) {
            let block = &fork.get(pos).unwrap().block;
            for tx in &block.tx {
                let ops = match &tx.on_byzantine_commit {
                    Some(ops) => &ops.ops,
                    None => continue
                };

                for op in ops {
                    match op.op_type() {
                        crate::proto::execution::ProtoTransactionOpType::Write => {
                            let num_byz_writes = self.num_byz_committed_writes.fetch_add(1, Ordering::SeqCst) + 1;
                            if num_byz_writes % 1000 == 0 {
                                info!("Num Byz Committed Write Requests: {}; Num keys byz committed: {}", num_byz_writes, bci_state.len());
                            }
                            // Sanity check
                            // Format (key, val)
                            if op.operands.len() != 2 {
                                continue;
                            }

                            let key = &op.operands[0];
                            let val = &op.operands[1];

                            bci_state.insert(key.clone(), val.clone());
                        },

                        _ => {
                            // Treat as Noop

                        }
                    }
                }
            }

        }

        // Then move all Byz committed entries from ci_state to bci_state.
        for (key, val_versions) in ci_state.iter_mut() {
            for (pos, val) in &(*val_versions) {
                if *pos <= bci {
                    bci_state.insert(key.clone(), val.clone());
                }
            }

            val_versions.retain(|v| v.0 > bci);
        }
        ci_state.retain(|_, v| v.len() > 0);

        self.last_bci.store(bci, Ordering::SeqCst);

    }

    fn execute_rollback(&self, ci: u64,
        _fork: &tokio::sync::MutexGuard<Log>,
        ci_state: &mut MutexGuard<HashMap<Vec<u8>, Vec<(u64, Vec<u8>)>>>,
        _bci_state: &mut MutexGuard<HashMap<Vec<u8>, Vec<u8>>>,
    ) {
        // ci is our new commit index now.
        let old_ci = self.last_ci.load(Ordering::SeqCst);
        if old_ci <= ci {
            return;
        }

        // Remove all versions in ci_state that are bigger than ci
        for (_k, v) in ci_state.iter_mut() {
            v.retain(|(pos, _)| *pos <= ci);
        }

        ci_state.retain(|_, v| v.len() > 0);
    }

    fn execute_read(&self, key: &Vec<u8>,
        ci_state: &MutexGuard<HashMap<Vec<u8>, Vec<(u64, Vec<u8>)>>>,
        bci_state: &MutexGuard<HashMap<Vec<u8>, Vec<u8>>>,
    ) -> ProtoTransactionOpResult
    {
        let num_reads = self.num_reads.fetch_add(1, Ordering::SeqCst) + 1;
        if num_reads % 1000 == 0 {
            info!("Num Read Requests: {}", num_reads);
        }
        // First find in ci_state
        let ci_res = ci_state.get(key);
        match ci_res {
            Some(v) => {
                // Invariant: v is sorted by ci
                // Invariant: v.len() > 0
                let res = &v.last().unwrap().1;
                return ProtoTransactionOpResult {
                    success: true,
                    values: vec![res.clone()],
                }
            },
            None => {
                // Need to check in bci_state.
            },
        };

        let bci_res = bci_state.get(key);
        match bci_res {
            Some(v) => ProtoTransactionOpResult {
                success: true,
                values: vec![v.clone()],
            },
            None => ProtoTransactionOpResult {
                success: false,
                values: vec![]
            },
        }
    }
}

impl Engine for PinnedKVStoreEngine {
    fn new(ctx: PinnedServerContext) -> Self {
        Self(Arc::new(Box::pin(KVStoreEngine::new(ctx))))
    }

    /// Application logic:
    /// Run all rollbacks first
    /// Then insert all writes from ci-ed blocks to ci_state.
    /// Then insert all writes from bci-ed blocks to bci_state.
    /// Iterate through ci_state, popping results and inserting them in bci_state if seq_num is <= bci
    /// (Stop at first > bci entry, it is an IndexMap)
    /// 
    /// Rollback logic: Logical undo
    /// Remove rollbacked versions from ci_state.
    /// 
    /// Read logic: Acquire locks in same order.
    /// fork -> ci_state -> bci_state -> event_chan receiver. Helps avoid deadlocks.
    /// First check key in ci_state, if not found, check bci_state.
    async fn run(&self) {
        while !self.quit_signal.load(Ordering::SeqCst) {
            let fork = self.ctx.state.fork.lock().await;
            let mut ci_state = self.ci_state.lock().unwrap();
            let mut bci_state = self.bci_state.lock().unwrap();
            let mut event_recv = self.event_chan.1.lock().unwrap();
            self.execute_ops(&fork, &mut ci_state, &mut bci_state, &mut event_recv);
        }
    }

    fn signal_quit(&self) {
        self.quit_signal.store(true, Ordering::SeqCst);
    }

    fn signal_crash_commit(&self, ci: u64) {
        self.event_chan.0.send(Event::CiUpd(ci)).unwrap();
    }

    fn signal_byzantine_commit(&self, bci: u64) {
        self.event_chan.0.send(Event::BciUpd(bci)).unwrap();
    }

    fn signal_rollback(&self, ci: u64) {
        self.event_chan.0.send(Event::Rback(ci)).unwrap();
    }

    fn get_unlogged_execution_result(&self, request: crate::proto::execution::ProtoTransactionPhase) -> crate::proto::execution::ProtoTransactionResult {
        // Read requests only
        let ci_state = self.ci_state.lock().unwrap();
        let bci_state = self.bci_state.lock().unwrap();

        let mut result = ProtoTransactionResult {
            result: Vec::new(),
        };
        for op in &request.ops {
            match op.op_type() {
                crate::proto::execution::ProtoTransactionOpType::Read => {
                    // Sanity check: 1 operand only
                    if op.operands.len() != 1 {
                        continue;
                    }

                    let key = &op.operands[0];
                    let read_result = self.execute_read(key, &ci_state, &bci_state);
                    result.result.push(read_result);
                },
                _ => {
                    // Treat as Noop
                }
            }
        }

        result
    }
}
// Copyright (c) Shubham Mishra. All rights reserved.
// Licensed under the MIT License.

use std::{collections::HashMap, ops::Deref, pin::Pin, sync::{atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering}, Arc, Mutex, MutexGuard}, time::Duration};

use hex::ToHex;
use log::{info, trace, warn};
use prost::Message;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};

use crate::{config::NodeInfo, consensus::{handler::PinnedServerContext, log::Log, timer::ResettableTimer}, crypto::hash, execution::Engine, proto::{client::{ProtoClientReply, ProtoTryAgain}, execution::{ProtoTransactionOpResult, ProtoTransactionResult}}, rpc::PinnedMessage};

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

    event_chan: (UnboundedSender<Event>, tokio::sync::Mutex<UnboundedReceiver<Event>>),

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
            event_chan: (chan.0, tokio::sync::Mutex::new(chan.1)),
            num_crash_committed_writes: AtomicUsize::new(0),
            num_byz_committed_writes: AtomicUsize::new(0),
            num_reads: AtomicUsize::new(0),
        }
    }

    fn execute_ops(&self,
        fork: &mut tokio::sync::MutexGuard<Log>,
        ci_state: &mut MutexGuard<HashMap<Vec<u8>, Vec<(u64, Vec<u8>)>>>,
        bci_state: &mut MutexGuard<HashMap<Vec<u8>, Vec<u8>>>,
        msg: &Event
    ) {
        match msg {
            Event::CiUpd(ci) => self.execute_crash_commit(*ci, fork, ci_state, bci_state),
            Event::BciUpd(bci) => self.execute_byz_commit(*bci, fork, ci_state, bci_state),
            Event::Rback(ci) => self.execute_rollback(*ci, fork, ci_state, bci_state),
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
                            if num_crash_writes % 10000 == 0 {
                                trace!("Num Crash Committed Write Requests: {}; Num Keys only crash committed: {}", num_crash_writes, ci_state.len());
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
        fork: &mut tokio::sync::MutexGuard<Log>,
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
                            if num_byz_writes % 10000 == 0 {
                                trace!("Num Byz Committed Write Requests: {}; Num keys byz committed: {}; BCI: {}",
                                    num_byz_writes, bci_state.len(), self.last_bci.load(Ordering::SeqCst));
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


        // Garbage collection
        #[cfg(feature = "storage")]
        {
            if !self.ctx.i_am_leader.load(Ordering::SeqCst){
                self.ctx.client_replied_bci.store(bci, Ordering::SeqCst);
            }
            let replied_upto = self.ctx.client_replied_bci.load(Ordering::SeqCst);
            if replied_upto > 0 {
                fork.garbage_collect_upto(replied_upto);
            }
        }

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
        if num_reads % 10000 == 0 {
            trace!("Num Read Requests: {}", num_reads);
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
        let mut event_recv = self.event_chan.1.lock().await;
        let mut event = None;
        let mut tick = false;
        let stat_timer = ResettableTimer::new(Duration::from_millis(
            self.ctx.config.get().app_config.logger_stats_report_ms
        ));
        let stat_timer_handle = stat_timer.run().await;


        while !self.quit_signal.load(Ordering::SeqCst) {
            tokio::select! {
                _e = event_recv.recv() => {
                    event = _e
                },
                _tick = stat_timer.wait() => {
                    tick = _tick
                }
            }

            if event.is_none() && !tick {
                break;
            }

            let mut fork = self.ctx.state.fork.lock().await;
            let lack_pend = self.ctx.client_ack_pending.lock().await;
            let byz_qc_pending = self.ctx.state.byz_qc_pending.lock().await;
            let mut ci_state = self.ci_state.lock().unwrap();
            let mut bci_state = self.bci_state.lock().unwrap();
            
            if let Some(_e) = &event {
                // Execute ops from event.
                self.execute_ops(&mut fork, &mut ci_state, &mut bci_state, _e);
            } else {
                // Log stats
        
                info!("fork.last = {}, fork.last_qc = {}, commit_index = {}, byz_commit_index = {}, pending_acks = {}, pending_qcs = {} num_crash_committed_txs = {}, num_byz_committed_txs = {}, fork.last_hash = {}, total_client_request = {}, view = {}, view_is_stable = {}, i_am_leader: {}",
                    fork.last(), fork.last_qc(),
                    self.ctx.state.commit_index.load(Ordering::SeqCst),
                    self.ctx.state.byz_commit_index.load(Ordering::SeqCst),
                    lack_pend.len(),
                    byz_qc_pending.len(),
                    self.ctx.state.num_committed_txs.load(Ordering::SeqCst),
                    self.ctx.state.num_byz_committed_txs.load(Ordering::SeqCst),
                    fork.last_hash().encode_hex::<String>(),
                    self.ctx.total_client_requests.load(Ordering::SeqCst),
                    self.ctx.state.view.load(Ordering::SeqCst),
                    self.ctx.view_is_stable.load(Ordering::SeqCst),
                    self.ctx.i_am_leader.load(Ordering::SeqCst)
                );


                #[cfg(feature = "storage")]
                info!("Storage GC Hi watermark: {}", fork.gc_hiwm());

                info!("Blocks forced to supermajority: {}", self.ctx.total_blocks_forced_supermajority.load(Ordering::SeqCst));
                info!("Force signed blocks: {}", self.ctx.total_forced_signed_blocks.load(Ordering::SeqCst));

                info!("Crash Committed Keys: {}; Byz Committed Keys: {}", ci_state.len(), bci_state.len());

                info!("num_reads = {}", self.num_reads.load(Ordering::SeqCst));
            }

            // Reset for the next iteration
            event = None;
            tick = false;
        }

        let _ = tokio::join!(stat_timer_handle);
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
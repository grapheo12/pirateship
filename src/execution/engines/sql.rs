// Copyright (c) Shubham Mishra. All rights reserved.
// Licensed under the MIT License.

use std::{collections::HashMap, ops::Deref, pin::Pin, sync::{atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering}, Arc, Mutex, MutexGuard}};

use futures::FutureExt;
use gluesql::prelude::{Glue, SharedMemoryStorage};
use log::{error, info, warn};
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};

use crate::{consensus::{handler::PinnedServerContext, log::Log}, execution::Engine, proto::execution::{ProtoTransactionOpResult, ProtoTransactionResult}};

enum Event {
    CiUpd(u64),
    BciUpd(u64),
    Rback(u64)
}

pub struct SQLEngine {
    pub ctx: PinnedServerContext,
    pub last_ci: AtomicU64,
    pub last_bci: AtomicU64,
    quit_signal: AtomicBool,

    glue: tokio::sync::Mutex<Glue<SharedMemoryStorage>>,

    event_chan: (UnboundedSender<Event>, tokio::sync::Mutex<UnboundedReceiver<Event>>),

    pub num_queries: AtomicUsize
}

#[derive(Clone)]
pub struct PinnedSQLEngine(Arc<Pin<Box<SQLEngine>>>);

impl Deref for PinnedSQLEngine {
    type Target = SQLEngine;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl SQLEngine {
    pub fn new(ctx: PinnedServerContext) -> Self {
        let chan = unbounded_channel();
        let storage = SharedMemoryStorage::new();
        Self {
            ctx,
            last_ci: AtomicU64::new(0),
            last_bci: AtomicU64::new(0),
            quit_signal: AtomicBool::new(false),
            glue: tokio::sync::Mutex::new(Glue::new(storage)),
            event_chan: (chan.0, tokio::sync::Mutex::new(chan.1)),
            num_queries: AtomicUsize::new(0),
        }
    }

    async fn execute_ops<'a>(&self,
        fork: &'a tokio::sync::MutexGuard<'a, Log>,
        mut event_recv: tokio::sync::MutexGuard<'a, UnboundedReceiver<Event>>
    ) {
        while let Ok(msg) = event_recv.try_recv() {
            match msg {
                Event::CiUpd(ci) => self.execute_crash_commit(ci, fork).await,
                Event::BciUpd(bci) => self.execute_byz_commit(bci, fork).await,
                Event::Rback(ci) => self.execute_rollback(ci, fork).await,
            }
        }
    }

    async fn execute_crash_commit<'a>(&self, ci: u64,
        fork: &'a tokio::sync::MutexGuard<'a, Log>,
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
                        crate::proto::execution::ProtoTransactionOpType::Custom => {
                            let num_queries = self.num_queries.fetch_add(1, Ordering::SeqCst) + 1;
                            // Sanity check
                            // Format (query)
                            if op.operands.len() != 1 {
                                continue;
                            }
                            
                            let q = &op.operands[0];
                            let q = String::from_utf8(q.clone()).unwrap();
                            let mut glue = self.glue.lock().await;
                            if num_queries % 1000 == 0 {
                                info!("Num Queries: {}, Current Query: {}", num_queries, q);
                            }
                            
                            let res = glue.execute(&q).now_or_never();
                            match res {
                                Some(r) => {
                                    if let Err(e) = r {
                                        error!("SQL Error: {}", e);
                                    }
                                },
                                None => {
                                    error!("Query did not execute!");
                                },
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

    async fn execute_byz_commit<'a>(&self, bci: u64,
        fork: &'a tokio::sync::MutexGuard<'a, Log>,
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
                        crate::proto::execution::ProtoTransactionOpType::Custom => {
                            let num_queries = self.num_queries.fetch_add(1, Ordering::SeqCst) + 1;
                            // Sanity check
                            // Format (query)
                            if op.operands.len() != 1 {
                                continue;
                            }
                            
                            let q = &op.operands[0];
                            let q = String::from_utf8(q.clone()).unwrap();
                            let mut glue = self.glue.lock().await;
                            if num_queries % 1000 == 0 {
                                info!("Num Queries: {}, Current Query: {}", num_queries, q);
                            }
                            
                            let res = glue.execute(&q).now_or_never();

                        },

                        _ => {
                            // Treat as Noop

                        }
                    }
                }
            }

        }

        self.last_bci.store(bci, Ordering::SeqCst);

    }

    async fn execute_rollback<'a>(&self, ci: u64,
        _fork: &'a tokio::sync::MutexGuard<'a, Log>,
    ) {
        error!("Rollbacks unsupported");
    }

}

impl Engine for PinnedSQLEngine {
    fn new(ctx: PinnedServerContext) -> Self {
        Self(Arc::new(Box::pin(SQLEngine::new(ctx))))
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
            let event_recv = self.event_chan.1.lock().await;
            self.execute_ops(&fork, event_recv).await;
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
        warn!("All queries must go through consensus");

        ProtoTransactionResult {
            result: Vec::new(),
        }
    }
}
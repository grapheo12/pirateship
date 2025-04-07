// Copyright (c) Shubham Mishra. All rights reserved.
// Licensed under the MIT License.

use std::{collections::HashMap, ops::Deref, pin::Pin, sync::{atomic::{AtomicBool, AtomicI64, AtomicU64, AtomicUsize, Ordering}, Arc, Mutex, MutexGuard}};

use futures::FutureExt;
use gluesql::prelude::{Glue, SharedMemoryStorage};
use log::{error, info, warn};
use prost::Message;
use tokio::{sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender}, task::yield_now};

use crate::{config::NodeInfo, consensus::{handler::PinnedServerContext, log::Log}, crypto::hash, execution::Engine, get_tx_list, proto::{client::{ProtoClientReply, ProtoTransactionReceipt, ProtoTryAgain}, execution::{ProtoTransactionOpResult, ProtoTransactionResult}}, rpc::PinnedMessage};

enum Event {
    CiUpd(u64),
    BciUpd(u64),
    Rback(u64)
}

pub struct SQLEngine {
    pub ctx: PinnedServerContext,
    pub last_ci: AtomicU64,
    pub last_executed_tx_id: AtomicI64,
    pub last_bci: AtomicU64,

    pub results: tokio::sync::Mutex<HashMap<(u64, u64), ProtoTransactionResult>>,

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
            last_executed_tx_id: AtomicI64::new(-1),
            results: tokio::sync::Mutex::new(HashMap::new()),
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
        if let Ok(msg) = event_recv.try_recv() {
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
            let mut txn = 0;
            self.last_executed_tx_id.store(-1, Ordering::SeqCst);
            for tx in get_tx_list!(block) {
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
                                    if let Err(e) = &r {
                                        error!("SQL Error: {}", e);
                                    }
                                    let mut res_store = self.results.lock().await;
                                    
                                    let final_res = ProtoTransactionResult {
                                        result: r.unwrap().iter().map(|payload| {
                                            let ser = serde_json::to_string(payload).unwrap();
                                            ProtoTransactionOpResult {
                                                success: true,
                                                values: vec![ser.into()],
                                            }
                                        }).collect(),
                                    };
                                    
                                    res_store.insert((pos, txn), final_res);
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

                self.last_executed_tx_id.fetch_add(1, Ordering::SeqCst);
                txn += 1;
            }

        }

        self.last_ci.store(ci, Ordering::SeqCst);

        #[cfg(feature = "reply_from_app")]
        {

            let mut del_list = Vec::new();
            let mut lack_pend = self.ctx.client_ack_pending.lock().await;
            for ((bn, txn), chan) in lack_pend.iter() {
                
                if *bn <= ci {
                    let entry = fork.get(*bn).unwrap();
                    let response = if entry.block.tx.len() <= *txn {
                        if self.ctx.i_am_leader.load(Ordering::SeqCst) {
                            warn!("Missing transaction as a leader!");
                        }
                        if entry.block.view_is_stable {
                            warn!("Missing transaction in stable view!");
                        }
        
                        let node_infos = NodeInfo {
                            nodes: self.ctx.config.get().net_config.nodes.clone(),
                        };
        
                        ProtoClientReply {
                            reply: Some(
                                crate::proto::client::proto_client_reply::Reply::TryAgain(
                                    ProtoTryAgain{ serialized_node_infos: node_infos.serialize() }
                            )),
                        }
                    }else {
                        let h = hash(&entry.block.tx[*txn].encode_to_vec());
                        
                        let mut res_store = self.results.lock().await;
                        let res = res_store.remove(&((*bn) as u64, (*txn) as u64));
        
                        ProtoClientReply {
                            reply: Some(
                                crate::proto::client::proto_client_reply::Reply::Receipt(
                                    ProtoTransactionReceipt {
                                        req_digest: h,
                                        block_n: (*bn) as u64,
                                        tx_n: (*txn) as u64,
                                        results: res,
                                    },
                            )),
                        }
                    };
        
                    let v = response.encode_to_vec();
                    let vlen = v.len();
        
                    let msg = PinnedMessage::from(v, vlen, crate::rpc::SenderType::Anon);
        
                    let mut profile = chan.1.clone();
                    profile.register("Init Sending Client Response");
                    if *bn % 1000 == 0 {
                        profile.should_print = true;
                        profile.prefix = String::from(format!("Block: {}, Txn: {}", *bn, *txn));
                    }
                    let _ = chan.0.send((msg, profile));
                    del_list.push((*bn, *txn));
                }
            }
            for d in del_list {
                lack_pend.remove(&d);
            }
        }

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
            for tx in get_tx_list!(block) {
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
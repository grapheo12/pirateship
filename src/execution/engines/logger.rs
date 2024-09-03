// Copyright (c) Shubham Mishra. All rights reserved.
// Licensed under the MIT License.

use std::{ops::Deref, pin::Pin, sync::{atomic::{AtomicBool, AtomicU64, Ordering}, Arc}, time::Duration};

use hex::ToHex;
use log::info;
use tokio::{sync::{mpsc::{self, UnboundedReceiver, UnboundedSender}, Mutex}, time::sleep};

use crate::{consensus::handler::PinnedServerContext, execution::Engine, proto::execution::{ProtoTransactionPhase, ProtoTransactionResult}};

pub struct LoggerEngine {
    pub ctx: PinnedServerContext,
    pub ci_chan: (UnboundedSender<u64>, Mutex<UnboundedReceiver<u64>>),
    pub last_logged_ci: AtomicU64,
    pub bci_chan: (UnboundedSender<u64>, Mutex<UnboundedReceiver<u64>>),
    pub last_logged_bci: AtomicU64,
    pub rback_chan: (UnboundedSender<u64>, Mutex<UnboundedReceiver<u64>>),
    pub quit_signal: AtomicBool,
}

#[derive(Clone)]
pub struct PinnedLoggerEngine(Arc<Pin<Box<LoggerEngine>>>);

impl LoggerEngine {
    pub fn new(ctx: PinnedServerContext) -> Self {
        let ci_chan = mpsc::unbounded_channel();
        let bci_chan = mpsc::unbounded_channel();
        let rback_chan = mpsc::unbounded_channel();
        Self {
            ctx,
            ci_chan: (ci_chan.0, Mutex::new(ci_chan.1)),
            bci_chan: (bci_chan.0, Mutex::new(bci_chan.1)),
            rback_chan: (rback_chan.0, Mutex::new(rback_chan.1)),
            quit_signal: AtomicBool::new(false),
            last_logged_ci: AtomicU64::new(0),
            last_logged_bci: AtomicU64::new(0),
        }
    }
}

impl Deref for PinnedLoggerEngine {
    type Target = LoggerEngine;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}


impl Engine for PinnedLoggerEngine {
    fn new(ctx: PinnedServerContext) -> Self {
        Self(Arc::new(Box::pin(LoggerEngine::new(ctx))))
    }


    async fn run(&self) {
        while !self.quit_signal.load(Ordering::SeqCst) {
            sleep(Duration::from_millis(self.ctx.config.get().app_config.logger_stats_report_ms)).await;
            self.log_stats().await;
        }
    }

    fn signal_quit(&self) {
        // Do nothing here.
    }

    fn signal_crash_commit(&self, ci: u64) {
        let ci = (ci / 1000) * 1000;
        if ci > self.last_logged_ci.load(Ordering::SeqCst) {
            self.last_logged_ci.store(ci, Ordering::SeqCst);
            self.ci_chan.0.send(ci).unwrap();
        }
    }

    fn signal_byzantine_commit(&self, bci: u64) {
        let bci = (bci / 1000) * 1000;
        if bci > self.last_logged_bci.load(Ordering::SeqCst) {
            self.last_logged_bci.store(bci, Ordering::SeqCst);
            self.bci_chan.0.send(bci).unwrap();
        }
    }

    fn signal_rollback(&self, ci: u64) {
        self.rback_chan.0.send(ci).unwrap();
    }

    fn get_unlogged_execution_result(&self, _request: ProtoTransactionPhase) -> ProtoTransactionResult {
        // This app doesn't have any readonly execution.
        ProtoTransactionResult::default()
    }
}

impl PinnedLoggerEngine {
    async fn log_stats(&self) {
        let fork = self.ctx.state.fork.lock().await;
        
        {
            let lack_pend = self.ctx.client_ack_pending.lock().await;
            let byz_qc_pending = self.ctx.state.byz_qc_pending.lock().await;
    
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
        }

        let mut rback_chan = self.rback_chan.1.lock().await;
        let mut ci_chan = self.ci_chan.1.lock().await;
        let mut bci_chan = self.bci_chan.1.lock().await;


        while let Ok(ci) = rback_chan.try_recv() {
            info!("rolled back commit_index = {}, hash = {}", ci, fork.hash_at_n(ci).unwrap().encode_hex::<String>());
        };

        while let Ok(ci) = ci_chan.try_recv() {
            info!("commit_index = {}, hash = {}", ci, fork.hash_at_n(ci).unwrap().encode_hex::<String>());
        };

        while let Ok(bci) = bci_chan.try_recv() {
            info!("byz_commit_index = {}, hash = {}", bci, fork.hash_at_n(bci).unwrap().encode_hex::<String>());
        };
    }

}
use std::{ops::Deref, pin::Pin, sync::{atomic::{AtomicBool, Ordering}, Arc}, time::Duration};

use hex::ToHex;
use log::info;
use tokio::{sync::{mpsc::{self, UnboundedReceiver, UnboundedSender}, Mutex}, time::sleep};

use crate::{consensus::handler::PinnedServerContext, execution::Engine, proto::execution::{ProtoTransactionPhase, ProtoTransactionResult}};

pub struct LoggerEngine {
    pub ctx: PinnedServerContext,
    pub ci_chan: (UnboundedSender<u64>, Mutex<UnboundedReceiver<u64>>),
    pub bci_chan: (UnboundedSender<u64>, Mutex<UnboundedReceiver<u64>>),
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
            sleep(Duration::from_secs(1)).await;
            self.log_stats().await;
        }
    }

    fn signal_quit(&self) {
        // Do nothing here.
    }

    fn signal_crash_commit(&self, ci: u64) {
        self.ci_chan.0.send(ci).unwrap();
    }

    fn signal_byzantine_commit(&self, bci: u64) {
        self.bci_chan.0.send(bci).unwrap();
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
    
            info!("fork.last = {}, fork.last_qc = {}, commit_index = {}, byz_commit_index = {}, pending_acks = {}, pending_qcs = {} num_txs = {}, fork.last_hash = {}, total_client_request = {}, view = {}, view_is_stable = {}, i_am_leader: {}",
                fork.last(), fork.last_qc(),
                self.ctx.state.commit_index.load(Ordering::SeqCst),
                self.ctx.state.byz_commit_index.load(Ordering::SeqCst),
                lack_pend.len(),
                byz_qc_pending.len(),
                self.ctx.state.num_committed_txs.load(Ordering::SeqCst),
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
            if ci % 1000 == 0 {
                info!("rolled back commit_index = {}, hash = {}", ci, fork.hash_at_n(ci).unwrap().encode_hex::<String>());
            }
        };

        while let Ok(ci) = ci_chan.try_recv() {
            if ci % 1000 == 0 {
                info!("commit_index = {}, hash = {}", ci, fork.hash_at_n(ci).unwrap().encode_hex::<String>());
            }
        };

        while let Ok(bci) = bci_chan.try_recv() {
            if bci % 1000 == 0 {
                info!("byz_commit_index = {}, hash = {}", bci, fork.hash_at_n(bci).unwrap().encode_hex::<String>());
            }
        };
    }

}
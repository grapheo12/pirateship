use std::{collections::{HashMap, VecDeque}, pin::Pin, sync::Arc, time::Duration};

use hex::ToHex;
use log::info;
use serde::{de::DeserializeOwned, Serialize};
use tokio::sync::{oneshot, Mutex};

use crate::{config::AtomicConfig, crypto::{CachedBlock, HashType, DIGEST_LENGTH}, proto::execution::{ProtoTransaction, ProtoTransactionResult}, utils::channel::{Receiver, Sender}};

use super::{staging::ClientReplyCommand, timer::ResettableTimer};


pub enum AppCommand {
    NewRequestBatch(u64 /* length of new batch of request */, HashType /* hash of the last block */),
    CrashCommit(Vec<CachedBlock> /* all blocks from old_ci + 1 to new_ci */),
    ByzCommit(Vec<CachedBlock> /* all blocks from old_bci + 1 to new_bci */),
    Rollback(u64 /* new last block */)
}

pub trait AppEngine {
    type State: std::fmt::Debug + Clone + Serialize + DeserializeOwned;

    fn new(config: AtomicConfig) -> Self;
    fn handle_crash_commit(&mut self, blocks: Vec<CachedBlock>) -> Vec<Vec<ProtoTransactionResult>>;
    fn handle_byz_commit(&mut self, blocks: Vec<CachedBlock>) -> Vec<Vec<ProtoTransactionResult>>;
    fn handle_rollback(&mut self, rolled_back_blocks: Vec<CachedBlock>);
    fn handle_unlogged_request(&mut self, request: ProtoTransaction) -> ProtoTransactionResult;
    fn get_current_state(&self) -> Self::State;
}

struct LogStats {
    ci: u64,
    bci: u64,
    view: u64,
    i_am_leader: bool,
    view_is_stable: bool,
    last_n: u64,
    last_qc: u64,
    last_hash: HashType,
    total_requests: u64,
    total_crash_committed_txs: u64,
    total_byz_committed_txs: u64,
    total_unlogged_txs: u64,
}

impl LogStats {
    fn new() -> Self {
        Self {
            ci: 0,
            bci: 0,
            view: 0,
            i_am_leader: false,
            view_is_stable: false,
            last_n: 0,
            last_qc: 0,
            last_hash: vec![0u8; DIGEST_LENGTH],
            total_requests: 0,
            total_crash_committed_txs: 0,
            total_byz_committed_txs: 0,
            total_unlogged_txs: 0,
        }
    }

    fn print(&self) {
        info!("fork.last = {}, fork.last_qc = {}, commit_index = {}, byz_commit_index = {}, pending_acks = {}, pending_qcs = {} num_crash_committed_txs = {}, num_byz_committed_txs = {}, fork.last_hash = {}, total_client_request = {}, view = {}, view_is_stable = {}, i_am_leader: {}",
            self.last_n,
            self.last_qc,
            self.ci,
            self.bci,
            self.total_requests - (self.total_crash_committed_txs + self.total_unlogged_txs),
            self.total_crash_committed_txs - self.total_byz_committed_txs,
            self.total_crash_committed_txs,
            self.total_byz_committed_txs,
            self.last_hash.encode_hex::<String>(),
            self.total_requests,
            self.view,
            self.view_is_stable,
            self.i_am_leader
        );
    }
}

struct Application<E: AppEngine> {
    config: AtomicConfig,

    engine: E,
    log: VecDeque<CachedBlock>,
    stats: LogStats,

    staging_rx: Receiver<AppCommand>,
    unlogged_rx: Receiver<(ProtoTransaction, oneshot::Sender<ProtoTransactionResult>)>,

    client_reply_tx: Sender<ClientReplyCommand>,

    checkpoint_timer: Arc<Pin<Box<ResettableTimer>>>,
    log_timer: Arc<Pin<Box<ResettableTimer>>>,
}


impl<E: AppEngine> Application<E> {
    pub fn new(
        config: AtomicConfig,
        staging_rx: Receiver<AppCommand>, unlogged_rx: Receiver<(ProtoTransaction, oneshot::Sender<ProtoTransactionResult>)>,
        client_reply_tx: Sender<ClientReplyCommand>) -> Self {
        let checkpoint_timer = ResettableTimer::new(Duration::from_millis(config.get().app_config.checkpoint_interval_ms));
        let log_timer = ResettableTimer::new(Duration::from_millis(config.get().app_config.logger_stats_report_ms));
        let engine = E::new(config.clone());
        Self {
            config,
            engine,
            log: VecDeque::new(),
            stats: LogStats::new(),
            staging_rx,
            unlogged_rx,
            client_reply_tx,
            checkpoint_timer,
            log_timer,
        }
    }

    pub async fn run(application: Arc<Mutex<Self>>) {
        let mut application = application.lock().await;

        let log_timer_handle = application.log_timer.run().await;
        let checkpoint_timer_handle = application.checkpoint_timer.run().await;

        while let Ok(_) = application.worker().await {

        }

        log_timer_handle.abort();
        checkpoint_timer_handle.abort();
    }

    async fn worker(&mut self) -> Result<(), ()> {
        tokio::select! {
            cmd = self.staging_rx.recv() => {
                if cmd.is_none() {
                    return Err(());
                }
                self.handle_staging_command(cmd.unwrap()).await;
            },
            req = self.unlogged_rx.recv() => {
                if req.is_none() {
                    return Err(());
                }

                let (req, reply_tx) = req.unwrap();

                self.handle_unlogged_request(req, reply_tx).await;
            },
            _ = self.checkpoint_timer.wait() => {
                self.checkpoint().await;
            },
            _ = self.log_timer.wait() => {
                self.log_stats().await;
            }
        }

        Ok(())
    }

    /// This is used to compute throughput.
    async fn log_stats(&mut self) {
        self.stats.print();
    }

    async fn checkpoint(&mut self) {
        let state = self.engine.get_current_state();
        // TODO: Decide on checkpointing strategy

        info!("Current state checkpoint: {:?}", state);
    }

    async fn handle_unlogged_request(&mut self, request: ProtoTransaction, reply_tx: oneshot::Sender<ProtoTransactionResult>) {
        let result = self.engine.handle_unlogged_request(request);
        self.stats.total_requests += 1;
        self.stats.total_unlogged_txs += 1;

        reply_tx.send(result).unwrap();
    }


    async fn handle_staging_command(&mut self, cmd: AppCommand) {
        match cmd {
            AppCommand::NewRequestBatch(batch_size, hash) => {
                self.stats.total_requests += batch_size;
                self.stats.last_hash = hash;
            }
            AppCommand::CrashCommit(blocks) => {
                let block_ns = blocks.iter().map(|block| block.block.n).collect::<Vec<_>>();
                let results = self.engine.handle_crash_commit(blocks);
                self.stats.total_crash_committed_txs += results.len() as u64;

                assert_eq!(block_ns.len(), results.len());

                let result_map = block_ns.into_iter().zip(results.into_iter()).collect();
                self.client_reply_tx.send(ClientReplyCommand::CrashCommitAck(result_map)).await.unwrap();
            },
            AppCommand::ByzCommit(blocks) => {
                let block_ns = blocks.iter().map(|block| block.block.n).collect::<Vec<_>>();
                let results = self.engine.handle_byz_commit(blocks);
                self.stats.total_byz_committed_txs += results.len() as u64;

                assert_eq!(block_ns.len(), results.len());

                let result_map = block_ns.into_iter().zip(results.into_iter()).collect();
                self.client_reply_tx.send(ClientReplyCommand::CrashCommitAck(result_map)).await.unwrap();
            },
            AppCommand::Rollback(new_last_block) => {
                let rolled_back_blocks = self.log.drain((new_last_block + 1) as usize..).collect();
                self.engine.handle_rollback(rolled_back_blocks);

                assert!(new_last_block >= self.stats.bci);
                if self.stats.ci > new_last_block {
                    self.stats.ci = new_last_block;
                }
            }
        }
    }
}
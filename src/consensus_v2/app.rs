use std::collections::VecDeque;

use serde::{de::DeserializeOwned, Serialize};

use crate::{config::AtomicConfig, crypto::CachedBlock, proto::execution::{ProtoTransaction, ProtoTransactionResult}, utils::channel::{Receiver, Sender}};

use super::staging::ClientReplyCommand;

pub enum AppCommand {
    CrashCommit(u64 /* ci */, Vec<CachedBlock> /* all blocks from old_ci + 1 to new_ci */),
    ByzCommit(u64 /* bci */, Vec<CachedBlock> /* all blocks from old_bci + 1 to new_bci */),
    Rollback(u64 /* new last block */)
}

pub trait AppEngine {
    type State: Clone + Serialize + DeserializeOwned;

    fn new(config: AtomicConfig) -> Self;
    fn handle_crash_commit(&mut self, ci: u64, blocks: Vec<CachedBlock>) -> Vec<Vec<ProtoTransactionResult>>;
    fn handle_byz_commit(&mut self, bci: u64, blocks: Vec<CachedBlock>) -> Vec<Vec<ProtoTransactionResult>>;
    fn handle_rollback(&mut self, new_last_block: u64, rolled_back_blocks: Vec<CachedBlock>);
    fn handle_unlogged_request(&mut self, request: ProtoTransaction) -> ProtoTransactionResult;
    fn get_current_state(&self) -> Self::State;
}

struct Application<E: AppEngine> {
    config: AtomicConfig,

    engine: E,
    log: VecDeque<CachedBlock>,

    staging_rx: Receiver<AppCommand>,
    unlogged_rx: Receiver<ProtoTransaction>,

    client_reply_tx: Sender<ClientReplyCommand>,
}
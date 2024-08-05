use std::future::Future;

use crate::{consensus::handler::PinnedServerContext, proto::execution::{ProtoTransactionPhase, ProtoTransactionResult}};

#[allow(async_fn_in_trait)]
pub trait Engine {
    fn new(ctx: PinnedServerContext) -> Self;

    /// Start the execution engine, used with a spawn.
    fn run(&self) -> impl Future<Output = ()> + Send;

    /// Signal to kill the execution engine.
    fn signal_quit(&self);

    /// Signal to notify of a crash committed block.
    /// The main execution thread can then execute the block.
    fn signal_crash_commit(&self, ci: u64);

    /// Signal to notify of a byzantine committed block.
    /// The main execution thread can then perform the on_byzantine_commit part of the execution.
    fn signal_byzantine_commit(&self, bci: u64);

    /// Signal to notify that the crash commit index is rolled back to ci.
    /// The main execution thread can then rollback the state to the given commit index.
    fn signal_rollback(&self, ci: u64);

    /// Execute a transaction without including it in the log.
    /// Only applicable for read-only transactions.
    fn get_unlogged_execution_result(&self, request: ProtoTransactionPhase) -> ProtoTransactionResult;

    // The write transactions will only get a receipt from the consensus protocol.
    // There is no need (for now) to send a separate execution result.
    // It can be retrived by making a read request.

}

pub mod engines;
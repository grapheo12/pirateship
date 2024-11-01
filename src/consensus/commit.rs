// Copyright (c) Shubham Mishra. All rights reserved.
// Licensed under the MIT License.

use hex::ToHex;
use log::{debug, error, info, trace, warn};
use prost::Message;
use core::num;
use std::{collections::HashMap, sync::atomic::Ordering};
use tokio::sync::MutexGuard;

use crate::{
    config::NodeInfo,
    consensus::{
        client_reply::{bulk_register_byz_response, do_reply_transaction_receipt}, handler::PinnedServerContext, log::Log,
        reconfiguration::maybe_execute_reconfiguration_transaction,
    },
    crypto::hash,
    proto::{
        client::{ProtoClientReply, ProtoTransactionReceipt, ProtoTryAgain},
        consensus::ProtoFork,
        execution::ProtoTransactionResult,
    },
    rpc::{
        client::PinnedClient,
        server::{LatencyProfile, MsgAckChan},
        PinnedMessage,
    },
};

use super::utils::get_all_nodes_num;

/// Rollback such that the commit index is at max (n - 1)
pub fn maybe_rollback<Engine>(
    ctx: &PinnedServerContext,
    engine: &Engine,
    overwriting_fork: &ProtoFork,
    _fork: &MutexGuard<Log>,
) where
    Engine: crate::execution::Engine,
{
    if overwriting_fork.blocks.len() == 0 {
        return;
    }

    let n = overwriting_fork.blocks[0].n;

    if n == 0 {
        // This is invalid. No block has n == 0
    }

    let ci = ctx.state.commit_index.load(Ordering::SeqCst);
    if ci <= n - 1 {
        return;
    }
    // let rollbacked_fork = fork.serialize_range(n - 1, ci);
    // info!("\nRollbacked fork: {}\nOverwriting fork: {}", __display_protofork(&rollbacked_fork), __display_protofork(&overwriting_fork));

    ctx.state.commit_index.store(n - 1, Ordering::SeqCst);
    engine.signal_rollback(n - 1);
    warn!("Commit index rolled back from {} to {}", ci, n - 1);

    let bci = ctx.state.byz_commit_index.load(Ordering::SeqCst);
    if bci > n - 1 {
        // This should not be happening, EVER!
        ctx.state.byz_commit_index.store(n - 1, Ordering::SeqCst);
        error!(
            "Invariant violation: Byzantine commit index rolled back from {} to {}",
            bci,
            n - 1
        );
    }
}

pub async fn do_byzantine_commit<'a, Engine>(
    ctx: &PinnedServerContext,
    client: &PinnedClient,
    engine: &Engine,
    fork: &'a MutexGuard<'a, Log>,
    updated_bci: u64,
) where
    Engine: crate::execution::Engine,
{
    let old_bci = ctx.state.byz_commit_index.load(Ordering::SeqCst);
    trace!(
        "Updating byzantine_commit_index {} --> {}",
        old_bci,
        updated_bci
    );

    if updated_bci > fork.last() {
        error!(
            "Invariant violation: Byzantine commit index {} higher than fork.last() = {}",
            updated_bci,
            fork.last()
        );
    }
    ctx.state
        .byz_commit_index
        .store(updated_bci, Ordering::SeqCst);
    do_commit(ctx, client, engine, fork, updated_bci).await;

    engine.signal_byzantine_commit(updated_bci);
    for bn in (old_bci + 1)..(updated_bci + 1) {
        let entry = fork.get(bn).unwrap();
        for _tx in &entry.block.tx {
            if !_tx.is_reconfiguration {
                continue;
            }
            if _tx.on_byzantine_commit.is_none() {
                continue;
            }
            info!(
                "Reconfiguration transaction found in block: {}. Doing Byz commit",
                bn
            );
            // The byz commit phase of reconf tx is executed async.
            let _ = ctx.reconf_channel.0.send(_tx.clone());
        }

        ctx.state
            .num_byz_committed_txs
            .fetch_add(entry.block.tx.len(), Ordering::SeqCst);
    }

    #[cfg(not(feature = "reply_from_app"))]
    {
        // do_reply_byz_poll(ctx, fork, updated_bci, |_, _| ProtoTransactionResult::default()).await;
        #[cfg(feature = "storage")]
        {
            let gc_hiwm = fork.gc_hiwm();
            let replied_bci = ctx.client_replied_bci.load(Ordering::SeqCst);

            if replied_bci < gc_hiwm {
                ctx.client_replied_bci.store(gc_hiwm, Ordering::SeqCst);
            }
        }
        
        bulk_register_byz_response(ctx, updated_bci, fork);
    }

}

/// Only returns false if there is an invariant violation.
/// There was no 2-chain QC found.
async fn maybe_byzantine_commit_with_n_and_view<'a, Engine>(
    ctx: &PinnedServerContext,
    client: &PinnedClient,
    engine: &Engine,
    fork: &'a MutexGuard<'a, Log>,
    n: u64,
    view: u64,
) -> bool
where
    Engine: crate::execution::Engine,
{
    // 2-chain commit rule.

    // The first block of a view gets a QC immediately.
    // But that QC doesn't byzantine commit the last qc of old view.
    // The 2-chain rule only pertains to QCs proposed in the same view.
    // Old view blocks are indirectly byz committed.

    if n == 0 {
        return true;
    }

    let block_qcs = &fork.get(n).unwrap().block.qc;
    let old_bci = ctx.state.byz_commit_index.load(Ordering::SeqCst);
    let mut updated_bci = old_bci;
    if block_qcs.len() == 0 && updated_bci > 0 {
        trace!("Invariant violation: No QC found!");
        return false;
    }
    for qc in block_qcs {
        if qc.n > updated_bci && qc.view == view {
            updated_bci = qc.n;
        }
    }
    if updated_bci > old_bci {
        do_byzantine_commit(ctx, client, engine, fork, updated_bci).await;
    }

    true
}

pub async fn maybe_byzantine_commit_by_fast_path<'a, Engine>(
    ctx: &PinnedServerContext,
    client: &PinnedClient,
    engine: &Engine,
    fork: &'a MutexGuard<'a, Log>
)
where
    Engine: crate::execution::Engine,
{
    let last_block_with_qc = fork.last_block_with_qc();
    if last_block_with_qc == 0 {
        return;
    }
    let qcs = &fork.get(last_block_with_qc).unwrap().block.qc;

    for qc in qcs {
        let num_votes = qc.sig.len() as u64;
        if get_all_nodes_num(ctx) > num_votes {
            continue;
        }
    
        let my_view = ctx.state.view.load(Ordering::SeqCst);
        let last_qc_view = fork.last_qc_view();
        if my_view != last_qc_view {
            continue;
        }
    
        let old_bci = ctx.state.byz_commit_index.load(Ordering::SeqCst);
        let updated_bci = qc.n;
        
        if updated_bci > old_bci {
            if updated_bci % 1000 == 1 {
                info!("Byzantine commit by fast path: {}", updated_bci);
            }
            do_byzantine_commit(ctx, client, engine, fork, updated_bci).await;
        }        
    }
}

/// Return true if bci was updated.
pub async fn maybe_byzantine_commit<'a, Engine>(
    ctx: &PinnedServerContext,
    client: &PinnedClient,
    engine: &Engine,
    fork: &'a MutexGuard<'a, Log>,
) -> bool
where
    Engine: crate::execution::Engine,
{

    let old_bci = ctx.state.byz_commit_index.load(Ordering::SeqCst);

    #[cfg(feature = "fast_path")]
    maybe_byzantine_commit_by_fast_path(ctx, client, engine, fork).await;

    // Check all QCs formed during this view.
    // Since the last_qc need not have link to another qc,
    // due pipelined proposals.

    let last_qc_view = fork.last_qc_view();
    let mut check_qc = fork.last_qc();


    while !maybe_byzantine_commit_with_n_and_view(ctx, client, engine, fork, check_qc, last_qc_view)
        .await
    {
        if check_qc == 0 {
            break;
        }
        check_qc -= 1;
        trace!("Checking lower QCs: {}", check_qc);
        // view doesn't change from last_qc_view due to commit condition.
    }


    let new_bci = ctx.state.byz_commit_index.load(Ordering::SeqCst);

    new_bci > old_bci
}

pub async fn do_commit<'a, Engine>(
    ctx: &PinnedServerContext,
    client: &PinnedClient,
    engine: &Engine,
    fork: &'a MutexGuard<'a, Log>,
    n: u64,
) where
    Engine: crate::execution::Engine,
{
    let ci = ctx.state.commit_index.load(Ordering::SeqCst);
    if fork.last() < n {
        error!(
            "Invariant violation: Committing a block that doesn't exist! new ci {}, fork.last() {}",
            n,
            fork.last()
        );
        return;
    }
    if n <= ci {
        return;
    }

    ctx.state.commit_index.store(n, Ordering::SeqCst);
    engine.signal_crash_commit(n);

    for bn in (ci + 1)..(n + 1) {
        let entry = fork.get(bn).unwrap();
        for _tx in &entry.block.tx {
            if !_tx.is_reconfiguration {
                continue;
            }
            if _tx.on_crash_commit.is_none() {
                continue;
            }
            info!("Reconfiguration transaction found in block: {}", bn);
            match maybe_execute_reconfiguration_transaction(ctx, client, _tx, false) {
                Ok(_did_reconf) => {
                    info!("Reconfiguration transaction executed successfully!");
                }
                Err(e) => {
                    warn!("Error executing reconfiguration transaction: {:?}", e);
                }
            }
        }
    }

    #[cfg(feature = "no_pipeline")]
    ctx.should_progress.add_permits(1);

    for i in (ci + 1)..(n + 1) {
        let num_txs = match fork.get(i) {
            Ok(entry) => entry.block.tx.len(),
            Err(_) => {
                break;
            }
        };
        ctx.state
            .num_committed_txs
            .fetch_add(num_txs, Ordering::SeqCst);
    }

    #[cfg(not(feature = "reply_from_app"))]
    {
        do_reply_transaction_receipt(ctx, fork, n, |_, _| {
            ProtoTransactionResult::default()
        })
        .await;
    }

    // Every thousandth block is added in ping_counters.
    {
        let mut lpings = ctx.ping_counters.lock().unwrap();
        let mut del_pings = Vec::new();
        for (_n, start) in lpings.iter() {
            if *_n <= n {
                trace!(
                    "Fork index: {} Vote quorum latency: {} us",
                    *_n,
                    start.elapsed().as_micros()
                );
                del_pings.push(*_n);
            }
        }
        for _n in del_pings {
            lpings.remove(&_n);
        }
    }

    debug!(
        "New Commit Index: {}, Fork Digest: {} Tx: {}, num_txs: {}",
        ctx.state.commit_index.load(Ordering::SeqCst),
        fork.last_hash().encode_hex::<String>(),
        String::from_utf8(fork.last_hash()).unwrap(),
        ctx.state.num_committed_txs.load(Ordering::SeqCst)
    );
}

// Copyright (c) Shubham Mishra. All rights reserved.
// Licensed under the Apache 2.0 License.

use hex::ToHex;
use log::{debug, error, info, trace, warn};
use prost::Message;
use std::{
    collections::HashMap, sync::atomic::Ordering
};
use tokio::sync::MutexGuard;

use crate::{
    consensus::{
        handler::PinnedServerContext, log::Log, reconfiguration::maybe_execute_reconfiguration_transaction
    }, crypto::hash,
    proto::{
        client::{
            ProtoClientReply, ProtoTransactionReceipt, ProtoTryAgain
        }, consensus::ProtoFork, execution::ProtoTransactionResult
    },
    rpc::{
        client::PinnedClient, server::{LatencyProfile, MsgAckChan}, PinnedMessage
    }
};

/// Rollback such that the commit index is at max (n - 1)
pub fn maybe_rollback<Engine>(ctx: &PinnedServerContext, engine: &Engine, overwriting_fork: &ProtoFork, _fork: &MutexGuard<Log>)
where Engine: crate::execution::Engine
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
        error!("Invariant violation: Byzantine commit index rolled back from {} to {}", bci, n - 1);
    }
}

pub fn do_byzantine_commit<Engine>(
    ctx: &PinnedServerContext, client: &PinnedClient, engine: &Engine,
    fork: &MutexGuard<Log>, updated_bci: u64,
    lack_pend: &mut MutexGuard<HashMap<(u64, usize), (MsgAckChan, LatencyProfile)>>
) where Engine: crate::execution::Engine
{
    let old_bci = ctx.state.byz_commit_index.load(Ordering::SeqCst);
    trace!(
        "Updating byzantine_commit_index {} --> {}",
        old_bci,
        updated_bci
    );

    if updated_bci > fork.last() {
        error!("Invariant violation: Byzantine commit index {} higher than fork.last() = {}", updated_bci, fork.last());
    }
    ctx.state.byz_commit_index.store(updated_bci, Ordering::SeqCst);
    do_commit(ctx, client, engine, fork, lack_pend, updated_bci);
    
    engine.signal_byzantine_commit(updated_bci);
    for bn in (old_bci + 1)..(updated_bci + 1) {
        let entry = fork.get(bn).unwrap();
        for _tx in &entry.block.tx {
            if !_tx.is_reconfiguration {
                continue;
            }
            

            // The byz commit phase of reconf tx is executed async.
            let _ = ctx.reconf_channel.0.send(_tx.clone());
        }

        ctx.state.num_byz_committed_txs.fetch_add(entry.block.tx.len(), Ordering::SeqCst);
    }

}


/// Only returns false if there is an invariant violation.
/// There was no 2-chain QC found.
fn maybe_byzantine_commit_with_n_and_view<Engine>(
    ctx: &PinnedServerContext, client: &PinnedClient, engine: &Engine,
    fork: &MutexGuard<Log>, n: u64, view: u64,
    lack_pend: &mut MutexGuard<HashMap<(u64, usize), (MsgAckChan, LatencyProfile)>>
) -> bool 
where Engine: crate::execution::Engine
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
        do_byzantine_commit(ctx, client, engine, fork, updated_bci, lack_pend);
    }

    true
}

/// Return true if bci was updated.
pub fn maybe_byzantine_commit<Engine>(
    ctx: &PinnedServerContext, client: &PinnedClient, engine: &Engine, fork: &MutexGuard<Log>,
    lack_pend: &mut MutexGuard<HashMap<(u64, usize), (MsgAckChan, LatencyProfile)>>) -> bool
where Engine: crate::execution::Engine
{
    // Check all QCs formed during this view.
    // Since the last_qc need not have link to another qc,
    // due pipelined proposals.

    let last_qc_view = fork.last_qc_view();
    let mut check_qc = fork.last_qc();

    let old_bci = ctx.state.byz_commit_index.load(Ordering::SeqCst);
    while !maybe_byzantine_commit_with_n_and_view(ctx, client, engine, fork, check_qc, last_qc_view, lack_pend) {
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

pub fn do_commit<Engine>(
    ctx: &PinnedServerContext, client: &PinnedClient, engine: &Engine,
    fork: &MutexGuard<Log>,
    lack_pend: &mut MutexGuard<HashMap<(u64, usize), (MsgAckChan, LatencyProfile)>>,
    n: u64,
) where Engine: crate::execution::Engine 
{
    let ci = ctx.state.commit_index.load(Ordering::SeqCst);
    if fork.last() < n {
        error!("Invariant violation: Committing a block that doesn't exist! new ci {}, fork.last() {}", n, fork.last());
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

    let mut del_list = Vec::new();
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

    for ((bn, txn), chan) in lack_pend.iter() {
        if *bn <= n {
            let entry = fork.get(*bn).unwrap();
            let response = if entry.block.tx.len() <= *txn {
                if ctx.i_am_leader.load(Ordering::SeqCst) {
                    warn!("Missing transaction as a leader!");
                }
                if entry.block.view_is_stable {
                    warn!("Missing transaction in stable view!");
                }

                ProtoClientReply {
                    reply: Some(
                        crate::proto::client::proto_client_reply::Reply::TryAgain(
                            ProtoTryAgain{ }
                    )),
                }
            }else {
                let h = hash(&entry.block.tx[*txn].encode_to_vec());
    
                ProtoClientReply {
                    reply: Some(
                        crate::proto::client::proto_client_reply::Reply::Receipt(
                            ProtoTransactionReceipt {
                                req_digest: h,
                                block_n: (*bn) as u64,
                                tx_n: (*txn) as u64,
                                results: Some(ProtoTransactionResult::default()),
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
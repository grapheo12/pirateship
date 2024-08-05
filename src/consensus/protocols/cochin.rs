use byteorder::{BigEndian, WriteBytesExt};
use ed25519_dalek::SIGNATURE_LENGTH;
use hex::ToHex;
use indexmap::IndexMap;
use log::{debug, error, info, trace, warn};
use prost::Message;
use sha2::digest::InvalidBufferSize;
use core::hash;
use std::{
    collections::{HashMap, HashSet}, io::{BufWriter, Error, ErrorKind, Write}, ops::Deref, sync::atomic::Ordering, time::{Duration, Instant}
};
use tokio::{
    join,
    sync::{mpsc::{self, UnboundedSender}, MutexGuard},
    time::sleep,
};

use crate::{
    consensus::{
        self,
        handler::{ForwardedMessage, ForwardedMessageWithAckChan, PinnedServerContext},
        leader_rotation::get_current_leader,
        log::{Log, LogEntry},
        timer::ResettableTimer,
    }, crypto::{cmp_hash, hash, KeyStore, DIGEST_LENGTH}, execution::Engine, proto::{
        checkpoint::{ProtoBackFillRequest, ProtoBackFillResponse}, client::{
            ProtoClientReply, ProtoClientRequest, ProtoCurrentLeader, ProtoTentativeReceipt, ProtoTransactionReceipt, ProtoTryAgain
        }, consensus::{
            proto_block::Sig, DefferedSignature, ProtoAppendEntries, ProtoBlock, ProtoFork,
            ProtoForkValidation, ProtoQuorumCertificate, ProtoSignatureArrayEntry,
            ProtoViewChange, ProtoVote,
        }, execution::{ProtoTransaction, ProtoTransactionOp, ProtoTransactionPhase, ProtoTransactionResult}, rpc::{self, ProtoPayload}
    }, rpc::{
        client::PinnedClient,
        server::{LatencyProfile, MsgAckChan},
        PinnedMessage,
    }
};

pub fn get_leader_str(ctx: &PinnedServerContext) -> String {
    get_leader_str_for_view(ctx, ctx.state.view.load(Ordering::SeqCst))
}

pub fn get_leader_str_for_view(ctx: &PinnedServerContext, view: u64) -> String {
    ctx.config.consensus_config.node_list
        [get_current_leader(ctx.config.consensus_config.node_list.len() as u64, view)]
    .clone()
}

fn get_node_num(ctx: &PinnedServerContext) -> u64 {
    let mut i = 0;
    for name in &ctx.config.consensus_config.node_list {
        if name.eq(&ctx.config.net_config.name) {
            return i;
        }
        i += 1;
    }

    0
}

fn get_majority_num(ctx: &PinnedServerContext) -> u64 {
    let n = ctx.config.consensus_config.node_list.len() as u64;
    n / 2 + 1
}

fn get_super_majority_num(ctx: &PinnedServerContext) -> u64 {
    let n = ctx.config.consensus_config.node_list.len() as u64;
    2 * (n / 3) + 1
}

fn get_everyone_except_me(my_name: &String, node_list: &Vec<String>) -> Vec<String> {
    node_list
        .iter()
        .map(|n| n.clone())
        .filter(|name| !name.eq(my_name))
        .collect()
}

pub async fn create_vote_for_blocks(
    ctx: PinnedServerContext,
    seq_nums: &Vec<u64>,
) -> Result<ProtoVote, Error> {
    // let mut fork = ctx.state.fork.lock().await;
    let fork = ctx.state.fork.try_lock();
    let mut fork = if let Err(e) = fork {
        debug!("create_vote_for_blocks: Fork is locked, waiting for it to be unlocked: {}", e);
        let fork = ctx.state.fork.lock().await;
        debug!("create_vote_for_blocks: Fork locked");
        fork  
    }else{
        debug!("create_vote_for_blocks: Fork locked");
        fork.unwrap()
    };
    let view = ctx.state.view.load(Ordering::SeqCst);
    let mut byz_qc_pending = ctx.state.byz_qc_pending.lock().await;

    let mut vote_sigs = Vec::new();
    let mut max_n = 0;

    let mut atleast_one_sig_block = false;

    for n in seq_nums {
        let n = *n;
        if n > fork.last() {
            continue;
        }

        if n > max_n {
            max_n = n;
        }

        if fork.get(n)?.has_signature() {
            atleast_one_sig_block = true;
            let sig = fork.signature_at_n(n, &ctx.keys).to_vec();

            // This is just caching the signature.
            fork.inc_qc_sig_unverified(&ctx.config.net_config.name, &sig, n)?;

            // Add the block to byz_qc_pending,
            // along with all other blocks for which I have sent signature before,
            // but haven't seen a QC
            byz_qc_pending.insert(n);
        }

        fork.inc_replication_vote(&ctx.config.net_config.name, n)?;
    }

    // Resend signatures for QCs I did not get yet.
    // This is safer than creating QCs with votes to higher blocks.
    if atleast_one_sig_block {
        // Invariant: Only signature blocks get signature votes.
        for n in byz_qc_pending.iter() {
            if *n > fork.last() {
                continue;
            }
            if let Some(sig) = fork.get(*n)?.qc_sigs.get(&ctx.config.net_config.name) {
                vote_sigs.push(ProtoSignatureArrayEntry {
                    n: *n,
                    sig: sig.to_vec(),
                });
            }
        }
    }

    Ok(ProtoVote {
        sig_array: vote_sigs,
        fork_digest: fork.hash_at_n(max_n).unwrap_or(vec![0u8; DIGEST_LENGTH]),
        n: max_n,
        view,
        is_nack: false,
    })
}

pub fn do_commit<Engine>(
    ctx: &PinnedServerContext, engine: &Engine,
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

fn __hash_tx_list(tx: &Vec<ProtoTransaction>) -> Vec<u8> {
    let mut buf = Vec::new();
    for t in tx {
        buf.extend(t.encode_to_vec());
    }
    hash(&buf)
}

fn __hash_qc_list(qc: &Vec<ProtoQuorumCertificate>) -> Vec<u8> {
    let mut buf = Vec::new();
    for q in qc {
        buf.extend(q.encode_to_vec());
    }
    hash(&buf)
}

fn __display_protofork(f: &ProtoFork) -> String {
    let mut s = String::from("ProtoFork { blocks: [ ");
    for b in &f.blocks {
        let _s = format!("ProtoBlock {{ View: {} n: {} Tx: {} QC: {} }},",
            b.view, b.n, __hash_tx_list(&b.tx).encode_hex::<String>(), __hash_qc_list(&b.qc).encode_hex::<String>());

        s += &_s;
    }
    s += " ] }";

    s

}
/// Rollback such that the commit index is at max (n - 1)
pub fn maybe_rollback<Engine>(ctx: &PinnedServerContext, engine: &Engine, overwriting_fork: &ProtoFork, fork: &MutexGuard<Log>)
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

    // @todo: Send rollback signal to the application above.
}

pub fn do_create_qcs(
    ctx: &PinnedServerContext,
    fork: &mut MutexGuard<Log>,
    next_qc_list: &mut MutexGuard<IndexMap<(u64, u64), ProtoQuorumCertificate>>,
    byz_qc_pending: &mut MutexGuard<HashSet<u64>>,
    qcs: &Vec<u64>,
) {
    for n in qcs {
        // It is already done.
        if *n <= fork.last_qc() {
            continue;
        }
        let view = ctx.state.view.load(Ordering::SeqCst);

        let qc = match fork.get_qc_at_n(*n, view) {
            Ok(qc) => qc,
            Err(_) => {
                continue;
            }
        };

        next_qc_list.insert((qc.n, qc.view), qc);
        byz_qc_pending.remove(n);

        if *n % 1000 == 0 {
            trace!("QC formed for index: {}", n);
        } else {
            debug!("QC formed for index: {}", n);
        }
    }
}

pub async fn do_process_vote<Engine>(
    ctx: PinnedServerContext, engine: &Engine,
    vote: &ProtoVote,
    sender: &String,
    majority: u64,
    super_majority: u64,
) -> Result<(), Error>
where Engine: crate::execution::Engine
{
    // let mut fork = ctx.state.fork.lock().await;
    let fork = ctx.state.fork.try_lock();
    let mut fork = if let Err(e) = fork {
        debug!("do_process_vote: Fork is locked, waiting for it to be unlocked: {}", e);
        let fork = ctx.state.fork.lock().await;
        debug!("do_process_vote: Fork locked");
        fork  
    }else{
        debug!("do_process_vote: Fork locked");
        fork.unwrap()
    };

    if vote.view < ctx.state.view.load(Ordering::SeqCst) {
        warn!("Vote for older view! Rejected");
        return Ok(());
    }
    if vote.n > fork.last() {
        warn!("Vote({}) higher than fork.last() = {}", vote.n, fork.last());
        return Ok(());
    }
    if !cmp_hash(&fork.hash_at_n(vote.n).unwrap(), &vote.fork_digest) {
        warn!("Wrong digest, skipping vote");
        return Ok(());
    }

    let majority = if ctx.view_is_stable.load(Ordering::SeqCst) {
        majority
    }else {
        super_majority
    };

    debug!("Processing vote: {:?} {} {}", vote, majority, super_majority);


    let mut qcs = Vec::new();

    for vote_sig in &vote.sig_array {
        if vote_sig.n > fork.last() {
            continue;
        }
        // Try to increase the signature after verifying against my own fork.
        match fork.inc_qc_sig(sender, &vote_sig.sig, vote_sig.n, &ctx.keys) {
            Ok(total_sigs) => {
                if !ctx.view_is_stable.load(Ordering::SeqCst) {
                    info!("Vote count: {}", total_sigs);
                }
                if total_sigs >= super_majority {
                    qcs.push(vote_sig.n);
                    debug!("Creating QC for {}", vote_sig.n);
                    if vote_sig.n == fork.last() && !ctx.view_is_stable.load(Ordering::SeqCst)
                        && fork.get(vote_sig.n).unwrap().block.fork_validation.len() >= super_majority as usize
                    {
                        // This was a view change message for which we got QC.
                        // View is stabilised hence.
                        ctx.view_is_stable.store(true, Ordering::SeqCst);
                        info!("View stabilised!");
                    }
                }
            }
            Err(e) => {
                warn!("Signature error: {}", e);
            }
        }
    }

    {
        let mut byz_qc_pending = ctx.state.byz_qc_pending.lock().await;
        let mut next_qc_list = ctx.state.next_qc_list.lock().await;
        do_create_qcs(
            &ctx,
            &mut fork,
            &mut next_qc_list,
            &mut byz_qc_pending,
            &qcs,
        );
    }

    let ci = ctx.state.commit_index.load(Ordering::SeqCst);
    let mut updated_ci = ci;

    // Quorum Diversity logic:
    // If the block is signed, is in byz_qc_pending AND |byz_qc_pending| >= k
    // Wait for supermajority, else, wait for majority
    // for crash commit
    let mut qd_should_wait_supermajority = false;
    // Since followers are sending signatures for older unqc-ed signed blocks with the current blocks anyway
    // This is a monotonic condition.
    // The first signed block I encounter that is in byz_qc_pending and |byz_qc_pending| >= k
    // I flip this to true. This should hold the crash commit for all block (signed or unsigned) after this block.
    // Once this signed block has supermajority, the other unsigned blocks will be committed back because they already got majority.

    // A vote at n is considered a vote at 1..n
    // So we should increase replication count for anything >= ci
    for i in (ci + 1)..(vote.n + 1) {
        if i > fork.last() {
            warn!("Vote({}) higher than fork.last() = {}", i, fork.last());
            break;
        }

        let mut __flipped = false;
        let mut __byz_qc_pending_len = 0;
        if fork.get(i).unwrap().has_signature() {
            let byz_qc_pending = ctx.state.byz_qc_pending.lock().await;
            if byz_qc_pending.contains(&i)
                && byz_qc_pending.len() >= ctx.config.consensus_config.quorum_diversity_k
            {
                qd_should_wait_supermajority = true;
                __flipped = true;
                __byz_qc_pending_len = byz_qc_pending.len();
            }
        }

        if __flipped {
            // Trying to avoid printing stuff while holding a lock (although I am holding a lock on fork :-))
            trace!(
                "Waiting for super_majority due to quorum diversity. |byz_qc_pending| = {}",
                __byz_qc_pending_len
            );
        }

        if qd_should_wait_supermajority {
            if fork.inc_replication_vote(sender, i)? >= super_majority {
                updated_ci = i;
            }
        } else {
            if fork.inc_replication_vote(sender, i)? >= majority {
                updated_ci = i;
            }
        }
    }

    {
        let mut lack_pend = ctx.client_ack_pending.lock().await;

        do_commit(&ctx, engine, &mut fork, &mut lack_pend, updated_ci);
    }

    Ok(())
}

/// Only returns false if there is an invariant violation.
/// There was no 2-chain QC found.
fn maybe_byzantine_commit_with_n_and_view<Engine>(
    ctx: &PinnedServerContext, engine: &Engine,
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
    let mut updated_bci = ctx.state.byz_commit_index.load(Ordering::SeqCst);
    if block_qcs.len() == 0 && updated_bci > 0 {
        trace!("Invariant violation: No QC found!");
        return false;
    }
    for qc in block_qcs {
        if qc.n > updated_bci && qc.view == view {
            updated_bci = qc.n;
        }
    }

    if updated_bci > ctx.state.byz_commit_index.load(Ordering::SeqCst) {
        
        trace!(
            "Updating byzantine_commit_index {} --> {}",
            ctx.state.byz_commit_index.load(Ordering::SeqCst),
            updated_bci
        );

        if updated_bci > fork.last() {
            error!("Invariant violation: Byzantine commit index {} higher than fork.last() = {}", updated_bci, fork.last());
        }
        ctx.state.byz_commit_index.store(updated_bci, Ordering::SeqCst);
        engine.signal_byzantine_commit(updated_bci);

        do_commit(ctx, engine, fork, lack_pend, updated_bci);
    }

    true
}

pub fn maybe_byzantine_commit<Engine>(
    ctx: &PinnedServerContext, engine: &Engine, fork: &MutexGuard<Log>,
    lack_pend: &mut MutexGuard<HashMap<(u64, usize), (MsgAckChan, LatencyProfile)>>)
where Engine: crate::execution::Engine
{
    // Check all QCs formed during this view.
    // Since the last_qc need not have link to another qc,
    // due pipelined proposals.

    let last_qc_view = fork.last_qc_view();
    let mut check_qc = fork.last_qc();

    while !maybe_byzantine_commit_with_n_and_view(ctx, engine, fork, check_qc, last_qc_view, lack_pend) {
        if check_qc == 0 {
            break;
        }
        check_qc -= 1;
        trace!("Checking lower QCs: {}", check_qc);
        // view doesn't change from last_qc_view due to commit condition.
    }

}

/// Split the given fork into two sequences: [..last New Leader msg] [Last new leader msg + 1..]
/// Verify all previous NewLeader messages wrt the proposer and fork choice rule.
/// If it is verified, fork.overwrite(ret.0) will not violate GlobalLock()
/// Once overwrite is done, it is safe to push/verify_and_push the blocks in ret.1.
pub async fn maybe_verify_view_change_sequence(ctx: &PinnedServerContext, f: &ProtoFork, super_majority: u64) -> Result<(ProtoFork, ProtoFork), Error> {
    let mut i = (f.blocks.len() - 1) as i64;
    let mut split_point = None;

    while i >= 0 {
        if !f.blocks[i as usize].view_is_stable {
            // This the signal that it is a New Leader message
            let mut valid_forks = 0;
            let mut max_qc_seen = 0;

            let mut subrule1_eligible_forks = HashMap::new();

            for (j, fork_validation) in f.blocks[i as usize].fork_validation.iter().enumerate() {
                // Is this a valid fork?
                // Check the signature on the fork.
                let last_qc = match &fork_validation.fork_last_qc {
                    Some(qc) => qc.n,
                    None => 0
                };
                let chk = verify_view_change_msg_raw(
                    &fork_validation.fork_hash,
                    fork_validation.view,
                    fork_validation.fork_len, last_qc,
                    &fork_validation.fork_sig, &ctx.keys,
                    &fork_validation.name
                );
                if !chk {
                    continue;
                }
                valid_forks += 1;
                let fork_stat = ForkStat {
                    last_view: fork_validation.view,
                    last_n: fork_validation.fork_len,
                    last_qc,
                    name: fork_validation.name.clone(),
                };

                if last_qc > max_qc_seen {
                    max_qc_seen = last_qc;
                    subrule1_eligible_forks.clear();
                    subrule1_eligible_forks.insert(j, fork_stat);
                }else if last_qc == max_qc_seen {
                    subrule1_eligible_forks.insert(j, fork_stat);
                }
            
            }

            if valid_forks < super_majority {
                return Err(Error::new(ErrorKind::InvalidData, "New Leader message with invalid fork information"))
            }

            let mut max_qc_selected = 0;
            for b in &f.blocks {
                if b.qc.len() > 0 {
                    for qc in &b.qc {
                        if qc.n > max_qc_selected {
                            max_qc_selected = qc.n;
                        }
                    }
                }
            }

            // SubRule1
            if max_qc_selected < max_qc_seen {
                return Err(Error::new(ErrorKind::InvalidData, "New Leader message with invalid max qc; SubRule1 violated"))
            }

            let selected_fork_size = f.blocks.len() - 1;
            let selected_fork_last_view = f.blocks[selected_fork_size].view;
            let selected_fork_size = f.blocks[selected_fork_size].n;
            for (_j, fork_stat) in &subrule1_eligible_forks {
                // Subrule2
                if selected_fork_last_view < fork_stat.last_view {
                    return Err(Error::new(ErrorKind::InvalidData, "New Leader message with invalid view; SubRule2 violated"))
                }
            }

            let subrule2_eligible_forks: HashMap<usize, ForkStat> = subrule1_eligible_forks.into_iter().filter(|(_j, fork_stat)| {
                selected_fork_size == fork_stat.last_n
            }).collect();

            for (_j, fork_stat) in subrule2_eligible_forks {
                // SubRule3
                if selected_fork_size < fork_stat.last_n {
                    return Err(Error::new(ErrorKind::InvalidData, "New Leader message with invalid length; SubRule3 violated"))
                }
            }

            if split_point.is_none() {
                split_point = Some((i + 1) as usize);
            }
        }

        i -= 1;
    }

    if split_point.is_none() {
        // There were no NewLeader messages
        return Ok((ProtoFork { blocks: Vec::new() }, f.clone()));
    }

    if split_point.unwrap() == f.blocks.len() {
        return Ok((f.clone(), ProtoFork { blocks: Vec::new() }));
    }

    let (overwrite_blocks, view_lock_blocks) = f.blocks.split_at(split_point.unwrap());
    Ok((ProtoFork { blocks: overwrite_blocks.to_vec() }, ProtoFork { blocks: view_lock_blocks.to_vec() }))
}

pub async fn maybe_backfill_fork_till_last_match<'a>(ctx: &PinnedServerContext, client: &PinnedClient, f: &ProtoFork, fork: &MutexGuard<'a, Log>, sender: &String) -> ProtoFork {
    let start = fork.last() + 1;
    let end = f.blocks[0].n - 1;
    if start > end {
        return f.clone();
    }

    maybe_backfill_fork(ctx, client, f, fork, sender, start, end).await

    
}


pub async fn maybe_backfill_fork<'a>(ctx: &PinnedServerContext, client: &PinnedClient, f: &ProtoFork, fork: &MutexGuard<'a, Log>, sender: &String, block_start: u64, block_end: u64) -> ProtoFork {
    // Currently, just backfill if the current log is lagging behind.
    if f.blocks.len() == 0 {
        return f.clone();
    }

    if f.blocks[0].n <= fork.last() + 1{
        return f.clone();
    }

    
    let backfill_req = ProtoBackFillRequest {
        block_start,
        block_end
    };
    info!("Backfilling fork from {} {:?}", sender, backfill_req);

    let mut buf = Vec::new();
    let rpc_msg_body = ProtoPayload {
        message: Some(crate::proto::rpc::proto_payload::Message::BackfillRequest(backfill_req)),
    };
    let backfill_resp = if let Ok(_) = rpc_msg_body.encode(&mut buf) {
        let sz = buf.len();
        let request = PinnedMessage::from(buf, sz, crate::rpc::SenderType::Anon);
        let resp = PinnedClient::send_and_await_reply(client, sender, request.as_ref()).await;
        if let Err(e) = resp {
            warn!("Error backfilling: {}", e);
            return f.clone();
        }

        let resp = resp.unwrap();
        let resp = resp.as_ref();
        let body = match ProtoPayload::decode(&resp.0.as_slice()[0..resp.1]) {
            Ok(b) => b,
            Err(e) => {
                warn!("Parsing problem: {}", e.to_string());
                debug!("Original message: {:?} {:?}", &resp.0, &resp.1);
                return f.clone();
            }
        };

        let backfill_resp = if let Some(crate::proto::rpc::proto_payload::Message::BackfillResponse(r)) = body.message {
            r
        }else{
            warn!("Invalid backfill response");
            return f.clone();
        };

        backfill_resp
    } else {
        error!("Error encoding backfill request");
        return f.clone();
    };

    // No sanity checking here. The log push/overwrite will fail if the fork is bad.

    if let None = backfill_resp.fork {
        return f.clone();
    }
    
    let mut res_fork = backfill_resp.fork.unwrap();
    res_fork.blocks.extend(f.blocks.clone());
    info!("Backfilled fork range from {} to {}", f.blocks[0].n, f.blocks[f.blocks.len()-1].n);

    res_fork

}


pub async fn do_push_append_entries_to_fork<Engine>(
    ctx: PinnedServerContext, engine: &Engine,
    client: PinnedClient,
    ae: &ProtoAppendEntries,
    sender: &String,
    super_majority: u64
) -> (
    u64,      /* last_n */
    u64,      /* updated_last_n */
    Vec<u64>, /* Sequence numbers */
    bool      /* Should update ci */
) where Engine: crate::execution::Engine
{
    // let mut fork = ctx.state.fork.lock().await;
    let fork = ctx.state.fork.try_lock();
    let mut fork = if let Err(e) = fork {
        debug!("do_push_append_entries_to_fork: Fork is locked, waiting for it to be unlocked: {}", e);
        let fork = ctx.state.fork.lock().await;
        debug!("do_push_append_entries_to_fork: Fork locked");
        fork  
    }else{
        debug!("do_push_append_entries_to_fork: Fork locked");
        fork.unwrap()
    };
    let last_n = fork.last();
    let last_qc = fork.last_qc();
    let mut updated_last_n = last_n;
    let mut seq_nums = Vec::new();

    if ae.view < ctx.state.view.load(Ordering::SeqCst) {
        trace!("Message from older view! Rejected; Sent by: {} Is New leader? {} Msg view: {} Current View {}",
            sender, !ae.fork.as_ref().unwrap().blocks[0].view_is_stable, ae.view, ctx.state.view.load(Ordering::SeqCst));
        return (last_n, last_n, seq_nums, false);
    }else if ae.view > ctx.state.view.load(Ordering::SeqCst) {
        ctx.state.view.store(ae.view, Ordering::SeqCst);
        if get_leader_str(&ctx) == ctx.config.net_config.name {
            ctx.i_am_leader.store(true, Ordering::SeqCst);
            // Don't think this can happen again.
        }else{
            ctx.i_am_leader.store(false, Ordering::SeqCst);
        }
        info!("View fast forwarded to {}! stable? {}", ae.view, ae.view_is_stable);
        // Since moving to new view, I can no longer send proper commit responses to clients.
        // Send tentative replies to everyone.
        do_reply_all_with_tentative_receipt(&ctx).await;
        // ctx.view_timer.reset();
    }else{
        trace!("AppendEntries for view {} stable? {} sender {}", ae.view, ae.view_is_stable, sender);
    }
    // @todo: Backfilling!

    if !sender.eq(&get_leader_str(&ctx)) {
        // Can't accept blocks from non-leader.
        warn!("Non-leader {} trying to send blocks", sender);
        return (last_n, updated_last_n, seq_nums, false);
    }

    if let Some(f) = &ae.fork {
        if !ae.view_is_stable {
            info!("Got New leader message for view {}!", ae.view);
        }
        let f = maybe_backfill_fork_till_last_match(&ctx, &client, f, &fork, sender).await;
        let res = maybe_verify_view_change_sequence(&ctx, &f, super_majority).await;
        
        if let Err(e) = res {
            warn!("Verification error: {}", e);
            return (last_n, updated_last_n, seq_nums, false);
        }
        let (overwrite_blocks, view_lock_blocks) = res.unwrap();

        if overwrite_blocks.blocks.len() > 0 {
            trace!("Untrimmed fork start: {}", overwrite_blocks.blocks[0].n);

            // If there is no equivocation, there will be no need to rollback the fork.
            // However, the view change message comes with a sizeable backlog which will cause unnecessary fork overwrites.
            // So, we trim the matching prefix.
            // After trimming, the only thing that should remain is the last New Leader message.
            let overwrite_blocks = fork.trim_matching_prefix(overwrite_blocks);
            if overwrite_blocks.blocks.len() > 0 {
                trace!("Trimmed fork start: {}", overwrite_blocks.blocks[0].n);
                maybe_rollback(&ctx, engine, &overwrite_blocks, &fork);
                let overwrite_res = fork.overwrite(&overwrite_blocks);
                match overwrite_res {
                    Ok(n) => {
                        info!("Overwritten to n = {}. Digest = {}", n, fork.last_hash().encode_hex::<String>());
                        seq_nums.push(n);
                    },
                    Err(e) => {
                        error!("{}", e);
                    },
                }
            }
        }
        
        for b in view_lock_blocks.blocks {
            trace!("Inserting block {} with {} txs", b.n, b.tx.len());
            let entry = LogEntry::new(b.clone());

            let res = if entry.has_signature() {
                fork.verify_and_push(entry, &ctx.keys, &get_leader_str_for_view(&ctx, b.view))
            } else {
                fork.push(entry)
            };
            match res {
                Ok(_n) => {
                    seq_nums.push(_n);
                }
                Err(e) => {
                    warn!("Error appending block: {} seq_num: {}", e, b.n);
                    continue;
                }
            }
        }
        debug!("Pushing complete!");
        updated_last_n = fork.last();
    }

    let old_stable = ctx.view_is_stable.load(Ordering::SeqCst);
    ctx.view_is_stable.store(ae.view_is_stable, Ordering::SeqCst);
    let new_stable = ctx.view_is_stable.load(Ordering::SeqCst);
    if new_stable && !old_stable {
        info!("View stabilised.");
    }



    if fork.last_qc() > last_qc {
        // If the last_qc progressed forward, need to clean up byz_qc_pending
        let last_qc = fork.last_qc();
        let mut byz_qc_pending = ctx.state.byz_qc_pending.lock().await;
        byz_qc_pending.retain(|&n| n > last_qc);

        // Also the byzantine commit index may move
        maybe_byzantine_commit(&ctx, engine, &fork, &mut ctx.client_ack_pending.lock().await);
    }

    (last_n, updated_last_n, seq_nums, true)
}

pub async fn do_reply_vote(
    ctx: PinnedServerContext,
    client: PinnedClient,
    vote: ProtoVote,
    reply_to: &String,
) -> Result<(), Error> {
    let vote_n = vote.n;
    let rpc_msg_body = ProtoPayload {
        message: Some(crate::proto::rpc::proto_payload::Message::Vote(vote)),
    };

    let mut buf = Vec::new();
    if let Ok(_) = rpc_msg_body.encode(&mut buf) {
        // let reply = MessageRef(&buf, buf.len(), &crate::rpc::SenderType::Anon);
        let sz = buf.len();
        let reply = PinnedMessage::from(buf, sz, crate::rpc::SenderType::Anon);
        let mut profile = LatencyProfile::new();
        if vote_n % 1000 == 0 {
            profile.should_print = true;
            profile.prefix = String::from(format!("Vote for block {}", vote_n));
        }
        let _ =
            PinnedClient::broadcast(&client, &vec![reply_to.clone()], &reply, &mut profile).await;
    }
    debug!("Sent vote");

    Ok(())
}

pub async fn maybe_backfill_fork_till_prefix_match(ctx: PinnedServerContext, client: PinnedClient, vc: &ProtoViewChange, sender: &String) -> Option<ProtoViewChange> {
    let fork = ctx.state.fork.lock().await;
    let mut vc_fork = vc.fork.clone().unwrap();
    vc_fork = maybe_backfill_fork_till_last_match(&ctx, &client, &vc_fork, &fork, sender).await;
    
    while vc_fork.blocks[0].n > 0 {
        let my_parent_hash = match fork.hash_at_n(vc_fork.blocks[0].n - 1) {
            Some(h) => h,
            None => {
                return None; // This is clearly an error!
            }
        };
    
        if cmp_hash(&vc_fork.blocks[0].parent, &my_parent_hash) {
            break;
        }

        let start = if vc_fork.blocks[0].n >= 10 {
            vc_fork.blocks[0].n - 10
        }else {
            0
        };

    
        vc_fork = maybe_backfill_fork(&ctx, &client, &vc_fork, &fork, sender,
            start,   // Fetch 10 blocks at a time. A better approach is to bin search first.
            vc_fork.blocks[0].n - 1).await;
    }

    Some(ProtoViewChange {
        fork: Some(vc_fork),
        view: vc.view,
        fork_sig: vc.fork_sig.clone(),
        fork_len: vc.fork_len,
        fork_last_qc: vc.fork_last_qc.clone(),
    })
}

pub async fn do_process_view_change<Engine>(
    ctx: PinnedServerContext, engine: &Engine,
    client: PinnedClient,
    vc: &ProtoViewChange,
    sender: &String,
    super_majority: u64,
) where Engine: crate::execution::Engine
{
    if vc.view < ctx.state.view.load(Ordering::SeqCst)
        || (vc.view == ctx.state.view.load(Ordering::SeqCst)
            && ctx.view_is_stable.load(Ordering::SeqCst))
    {
        return; // View Change for older views.
    }

    if vc.fork.is_none() {
        return;
    }

    if !ctx.config.net_config.name
        .eq(&get_leader_str_for_view(&ctx, vc.view))
    {
        // I am not the leader for this message's intended view
        // @todo: Pacemaker: Use this as a signal to do view change
        return;
    }

    // Check the signature on the fork.
    if !verify_view_change_msg(vc, &ctx.keys, sender) {
        return;
    } 

    // Check if fork's first block points to a block I already have.
    let vc = if vc.fork_len > 0 {
        if vc.fork.is_none() || vc.fork.as_ref().unwrap().blocks.len() == 0 {
            return;
        }
        match maybe_backfill_fork_till_prefix_match(ctx.clone(), client.clone(), vc, sender).await {
            Some(vc) => vc,
            None => {
                return;
            }
        }
    } else {
        vc.clone()
    };


    let mut fork_buf = ctx.state.fork_buffer.lock().await;
    if !fork_buf.contains_key(&vc.view) {
        fork_buf.insert(vc.view, HashMap::new());
    }
    fork_buf
        .get_mut(&vc.view)
        .unwrap()
        .insert(sender.clone(), vc.clone());

    let total_forks = fork_buf.get(&vc.view).unwrap().len();

    if total_forks >= super_majority as usize {
        // Got 2f + 1 view change messages.
        // Need to update view and take over as leader.

        // Get the set of forks, `F` (as in pseudocode)
        let fork_set = fork_buf.get(&vc.view).unwrap().clone();
        // Delete all buffers up till this view.
        fork_buf.retain(|&n, _| n > vc.view);

        drop(fork_buf);

        do_init_new_leader(ctx, engine, client, vc.view, fork_set, super_majority).await;
    }
}

async fn force_noop(ctx: &PinnedServerContext) {
    let client_tx = ctx.client_queue.0.clone();
    
    let client_req = ProtoClientRequest {
        tx: Some(ProtoTransaction {
            on_receive: None,
            on_crash_commit: None,
            on_byzantine_commit: Some(ProtoTransactionPhase {
                ops: vec![ProtoTransactionOp {
                    op_type: crate::proto::execution::ProtoTransactionOpType::Noop.into(),
                    operands: vec![format!("view_start_noop:{}", ctx.state.view.load(Ordering::SeqCst)).into_bytes()]
                }],
            }),
        }),
        // sig: vec![0u8; SIGNATURE_LENGTH],
        sig: vec![0u8; 1],
        origin: ctx.config.net_config.name.clone(),
    };

    let request = crate::proto::rpc::proto_payload::Message::ClientRequest(client_req);
    let profile = LatencyProfile::new();

    let _ = client_tx.send((request, ctx.config.net_config.name.clone(), ctx.__client_black_hole_channel.0.clone(), profile));

    
}

pub async fn do_init_new_leader<Engine>(
    ctx: PinnedServerContext, engine: &Engine,
    client: PinnedClient,
    view: u64,
    fork_set: HashMap<String, ProtoViewChange>,
    super_majority: u64,
) where Engine: crate::execution::Engine
{
    if ctx.state.view.load(Ordering::SeqCst) > view
        || (ctx.state.view.load(Ordering::SeqCst) == view
            && ctx.view_is_stable.load(Ordering::SeqCst))
        || fork_set.len() < super_majority as usize
        || ctx.config.net_config.name != get_leader_str_for_view(&ctx, view)
    {
        // Precondition check
        return;
    }

    // Stop accepting client messages
    ctx.view_is_stable.store(false, Ordering::SeqCst);

    // Increase view
    ctx.state.view.store(view, Ordering::SeqCst);
    // ctx.view_timer.reset();
    ctx.i_am_leader.store(true, Ordering::SeqCst);

    info!("Trying to gain stable leadership for view {}", view);
    let mut profile = LatencyProfile::new();

    // Choose fork
    let (mut chosen_fork, chosen_fork_stat) = fork_choice_rule_get(&fork_set, &ctx.config.net_config.name);
    // Backfill the fork, such that the overwrite doesn't fail.
    profile.register("Fork Choice Rule done");
    
    
    // Overwrite local fork with chosen fork
    let fork = ctx.state.fork.try_lock();
    let mut fork = if let Err(e) = fork {
        info!("do_init_new_leader: Fork is locked, waiting for it to be unlocked: {}", e);
        let fork = ctx.state.fork.lock().await;
        info!("do_init_new_leader: Fork locked");
        fork  
    }else{
        info!("do_init_new_leader: Fork locked");
        fork.unwrap()
    };
    
    // The fork is already backfilled till my fork.last().
    // This happened in the do_process_view_change function.
    
    // if chosen_fork_stat.name != ctx.config.net_config.name {
    //     chosen_fork = maybe_backfill_fork(&ctx, &client, &chosen_fork, &fork, &chosen_fork_stat.name).await;
    // }
    
    let (last_n, last_hash) = if chosen_fork_stat.name != ctx.config.net_config.name {
        if chosen_fork.blocks.len() == 0 {
            // No blocks in the chosen fork.
            let last_n = fork.last();
            let last_hash = fork.last_hash();
            (last_n, last_hash)
        } else {
            trace!("Untrimmed fork start: {}", chosen_fork.blocks[0].n);
            let trimmed_fork = fork.trim_matching_prefix(chosen_fork.clone());
            if trimmed_fork.blocks.len() > 0 {
                trace!("Trimmed fork start: {}", trimmed_fork.blocks[0].n);
        
                let last_n = if trimmed_fork.blocks.len() > 0 {
                    maybe_rollback(&ctx, engine, &trimmed_fork, &fork);
                    match fork.overwrite(&trimmed_fork){
                        Ok(n) => n,
                        Err(e) => {
                            error!("Error overwriting fork: {}", e);
                            return;
                        }
                    }
                } else {
                    fork.last()
                };
                let last_hash = fork.last_hash();
                (last_n, last_hash)
            } else {
                let last_n = fork.last();
                let last_hash = fork.last_hash();
                (last_n, last_hash)
            }
        }
        
    } else {
        // Don't need to overwrite if I chose my own fork.
        debug!("I chose my own fork!");
        let last_n = fork.last();
        let last_hash = fork.last_hash();
        (last_n, last_hash)
    };

    {
        let mut byz_qc_pending = ctx.state.byz_qc_pending.lock().await;
        byz_qc_pending.clear();
        // We'll cause the next two blocks to be 2f + 1 voted anyway.
    }

    info!("Fork Overwritten, new last_n = {}, last_hash = {}", last_n, last_hash.encode_hex::<String>());
    profile.register("Fork Overwrite done");
    let mut chosen_fork_last_qc = None;
    // Broadcast fork and wait for responses.
    let mut block = ProtoBlock {
        tx: Vec::new(),
        n: last_n + 1,
        parent: last_hash,
        view,
        qc: Vec::new(),
        fork_validation: fork_set
            .iter()
            .map(|(k, v)| {
                let mut buf = Vec::new();
                if v.fork.is_some() && v.fork.as_ref().unwrap().blocks.len() > 0 {
                    let block = v.fork.as_ref().unwrap().blocks.len();
                    let block = &v.fork.as_ref().unwrap().blocks[block-1];
                    let _ = block.encode(&mut buf);
                }
                let __hash = hash(&buf);
                if k.eq(&chosen_fork_stat.name) {
                    chosen_fork_last_qc = v.fork_last_qc.clone();
                }
                ProtoForkValidation {
                    name: k.clone(),
                    view,
                    fork_hash: __hash,
                    fork_sig: v.fork_sig.clone(),
                    fork_len: v.fork_len,
                    fork_last_qc: v.fork_last_qc.clone(),
                }
            }).collect(),
        view_is_stable: false,
        sig: Some(crate::proto::consensus::proto_block::Sig::NoSig(
            DefferedSignature {},
        )),
    };
    if chosen_fork_stat.last_qc > 0 && chosen_fork_last_qc.is_some() {
        block.qc = vec![chosen_fork_last_qc.unwrap()];
    }
    profile.register("Block creation done!");
    info!("Block created");
    let entry = LogEntry::new(block);

    info!("push and sign");
    match fork.push_and_sign(entry, &ctx.keys) {
        Ok(_) => {}
        Err(e) => {
            error!("Error pushing block: {}", e);
            return;
        }
    }
    info!("push and sign done");
    profile.register("Block push done");

    chosen_fork.blocks.push(fork.get(fork.last()).unwrap().block.clone());


    let ae = ProtoAppendEntries {
        fork: Some(chosen_fork),
        commit_index: ctx.state.commit_index.load(Ordering::SeqCst),
        view: ctx.state.view.load(Ordering::SeqCst),
        view_is_stable: ctx.view_is_stable.load(Ordering::SeqCst)
    };
    debug!("AE has signed block? {}", fork.get(fork.last()).unwrap().has_signature());
    profile.register("AE creation done");

    let send_list = get_everyone_except_me(
        &ctx.config.net_config.name,
        &ctx.config.consensus_config.node_list,
    );

    let block_n = ae.fork.as_ref().unwrap().blocks.len();
    let block_n = ae.fork.as_ref().unwrap().blocks[block_n - 1].n;

    drop(fork); // create_vote_for_blocks takes lock on fork
    info!("fork dropped");
    let my_vote = create_vote_for_blocks(ctx.clone(), &vec![block_n])
        .await
        .unwrap();
    info!("vote created");
    broadcast_append_entries(ctx.clone(), client.clone(), ae, &send_list, profile.clone())
    .await
    .unwrap();
    info!("broadcast done");
    profile.register("Broadcast done");
    do_process_vote(
        ctx.clone(), engine,
        &my_vote,
        &ctx.config.net_config.name,
        super_majority, // Forcing to wait for 2f + 1
        super_majority,
    )
    .await
    .unwrap();
    profile.register("Self processing done");
    info!("self processing done");

    profile.should_print = true;
    profile.prefix = String::from("New leader message");

    profile.force_print();

    // Force a dummy message to be sequenced.
    info!("forcing noop");
    force_noop(&ctx).await;
    info!("forcing noop done");

    // If I get AppendEntries or NewLeader from higher view during this time,
    // need to update view again and become a follower.
}

#[derive(Clone, Debug)]
struct ForkStat {
    last_view: u64,
    last_n: u64,
    last_qc: u64,
    // last_signed_block: u64,
    name: String
}

fn fork_choice_rule_get(
    fork_set: &HashMap<String, ProtoViewChange>,
    my_name: &String,
) -> (ProtoFork, ForkStat) {

    let mut chk_stats = HashMap::<String, ForkStat>::new();
    for (name, vc) in fork_set {
        let fork = vc.fork.as_ref().unwrap();
        let fork_len = fork.blocks.len();
        if fork_len == 0 {
            chk_stats.insert(
                name.clone(),
                ForkStat {
                    last_n: 0,
                    last_qc: 0,
                    last_view: 0,
                    // last_signed_block: 0,
                    name: name.clone()
                },
            );
            continue;
        }
        let last_view = vc.view;
        let last_n = vc.fork_len;

        let last_qc = match &vc.fork_last_qc {
            Some(qc) => qc.n,
            None => 0,
        };
        chk_stats.insert(
            name.clone(),
            ForkStat {
                last_n,
                last_qc,
                last_view,
                // last_signed_block,
                name: name.clone()
            },
        );
    }

    // SubRule1: last_qc.n as high as possible.
    let mut max_last_qc = 0;
    for (_name, fork) in &chk_stats {
        if fork.last_qc > max_last_qc {
            max_last_qc = fork.last_qc;
        }
    }

    chk_stats.retain(|_k, v| v.last_qc == max_last_qc);

    // SubRule 2: last_view as high as possible
    let mut max_last_view = 0;
    for (_name, fork) in &chk_stats {
        if fork.last_view > max_last_view {
            max_last_view = fork.last_view;
        }
    }

    chk_stats.retain(|_k, v| v.last_view == max_last_view);

    // SubRule 3: last_n as high as possible
    let mut max_last_n = 0;
    for (_name, fork) in &chk_stats {
        if fork.last_n > max_last_n {
            max_last_n = fork.last_n;
        }
    }

    chk_stats.retain(|_k, v| v.last_n == max_last_n);

    // Now I'm free to choose whichever fork is remaining.
    // Prefer my fork over others.
    if chk_stats.contains_key(my_name) {
        return (
            fork_set.get(my_name).unwrap().fork.as_ref().unwrap().clone(),
            chk_stats.get(my_name).unwrap().clone()
        );
    }

    // If not, choose any.
    let mut chosen_fork = None;
    let mut chosen_fork_stat = None;
    for (name, _fork) in &chk_stats {
        chosen_fork = Some(fork_set.get(name).unwrap().fork.as_ref().unwrap().clone());
        chosen_fork_stat = Some(_fork.clone());
    }

    (chosen_fork.unwrap(), chosen_fork_stat.unwrap())
}

pub async fn do_process_backfill_request(ctx: PinnedServerContext, ack_tx: &mut UnboundedSender<(PinnedMessage, LatencyProfile)>, bfr: &ProtoBackFillRequest, sender: &String) {
    let block_start = bfr.block_start;
    let block_end = bfr.block_end;

    let mut profile = LatencyProfile::new();
    let fork = ctx.state.fork.try_lock();
    let fork = match fork {
        Ok(f) => {
            debug!("do_process_backfill_request: Fork locked");
            f
        },
        Err(e) => {
            debug!("do_process_backfill_request: Fork is locked, waiting for it to be unlocked: {}", e);
            let fork = ctx.state.fork.lock().await;
            debug!("do_process_backfill_request: Fork locked");
            fork  
        }
    };
    let resp_fork = fork.serialize_range(block_start, block_end);
    profile.register("Backfill done");

    let response = ProtoBackFillResponse {
        fork: Some(resp_fork),
    };
    let rpc_msg_body = ProtoPayload {
        message: Some(crate::proto::rpc::proto_payload::Message::BackfillResponse(response)),
    };
    let mut buf = Vec::new();
    rpc_msg_body.encode(&mut buf).unwrap();
    let sz = buf.len();
    let reply = PinnedMessage::from(buf, sz, crate::rpc::SenderType::Anon);
    ack_tx.send((reply, profile)).unwrap();




}

pub async fn process_node_request<Engine>(
    ctx: &PinnedServerContext, engine: &Engine,
    client: &PinnedClient,
    majority: u64,
    super_majority: u64,
    ms: &mut ForwardedMessageWithAckChan,
) -> Result<(), Error>
where Engine: crate::execution::Engine
{
    let (msg, sender, ack_tx, profile) = ms;
    let _sender = sender.clone();
    match &msg {
        crate::proto::rpc::proto_payload::Message::AppendEntries(ae) => {
            profile.register("AE chan wait");
            let (last_n, updated_last_n, seq_nums, should_update_ci) =
                do_push_append_entries_to_fork(ctx.clone(), engine, client.clone(), ae, sender, super_majority).await;
            profile.register("Fork push");

            if updated_last_n > last_n {
                // New block has been added. Vote for the last one.
                let vote = create_vote_for_blocks(ctx.clone(), &seq_nums).await?;
                profile.register("Prepare vote");
                do_reply_vote(ctx.clone(), client.clone(), vote, &sender).await?;
                profile.register("Sent vote");

                if updated_last_n % 1000 == 0 {
                    profile.should_print = true;
                    profile.prefix = String::from(format!("Block: {}", updated_last_n));
                    profile.print();
                }
            }

            let ci = ctx.state.commit_index.load(Ordering::SeqCst);
            let new_ci = ae.commit_index;
            if should_update_ci && new_ci > ci {
                // let mut fork = ctx.state.fork.lock().await;
                let fork = ctx.state.fork.try_lock();
                let mut fork = if let Err(e) = fork {
                    debug!("process_node_request: Fork is locked, waiting for it to be unlocked: {}", e);
                    let fork = ctx.state.fork.lock().await;
                    debug!("process_node_request: Fork locked");
                    fork  
                }else{
                    debug!("process_node_request: Fork locked");
                    fork.unwrap()
                };
                let mut lack_pend = ctx.client_ack_pending.lock().await;
                // Followers should not have pending client requests.
                // But this same interface is good for refactoring.
                do_commit(ctx, engine, &mut fork, &mut lack_pend, new_ci);
            }
        }
        crate::proto::rpc::proto_payload::Message::Vote(v) => {
            profile.register("Vote chan wait");
            let _ = do_process_vote(ctx.clone(), engine, v, sender, majority, super_majority).await;
            profile.register("Vote process");
        }
        crate::proto::rpc::proto_payload::Message::ViewChange(vc) => {
            profile.register("View Change chan wait");
            let _ = do_process_view_change(ctx.clone(), engine, client.clone(), vc, sender, super_majority)
                .await;
            profile.register("View change process");
        },
        crate::proto::rpc::proto_payload::Message::BackfillRequest(bfr) => {
            profile.register("Backfill Request chan wait");
            do_process_backfill_request(ctx.clone(), ack_tx, bfr, sender).await;
            profile.register("Backfill Request process");
        },
        _ => {}
    }

    Ok(())
}

async fn do_reply_all_with_tentative_receipt(ctx: &PinnedServerContext) {
    let mut lack_pend = ctx.client_ack_pending.lock().await;

    for ((bn, txn), (chan, profile)) in lack_pend.iter_mut() {
        profile.register("Tentative chan wait");
        let response = ProtoClientReply {
            reply: Some(
                crate::proto::client::proto_client_reply::Reply::TentativeReceipt(
                    ProtoTentativeReceipt {
                        block_n: (*bn) as u64,
                        tx_n: (*txn) as u64,
                    },
                ),
            ),
        };

        let v = response.encode_to_vec();
        let vlen = v.len();

        let msg = PinnedMessage::from(v, vlen, crate::rpc::SenderType::Anon);

        profile.register("Init Sending Client Response");
        let _ = chan.send((msg, profile.to_owned()));
    }
    info!("Sent tentative responses for {} requests", lack_pend.len());
    lack_pend.clear();
}

fn sign_view_change_msg(vc: &mut ProtoViewChange, keys: &KeyStore, fork: &MutexGuard<Log>) {
    // Precondition: A correctly generated vc message will have fork.last() in its fork.
    //              OR have fork_len == 0
    let mut buf = BufWriter::new(vec![0u8; 32 + 8 + 8 + 8]);
    // 32 for hash, 8 for view, 8 for last_n, 8 for last_qc
    buf.write(&fork.last_hash());
    buf.write_u64::<BigEndian>(vc.view);
    buf.write_u64::<BigEndian>(vc.fork_len);
    buf.write_u64::<BigEndian>(match &vc.fork_last_qc {
        Some(qc) => qc.n,
        None => 0,
    });

    let buf = buf.into_inner().unwrap();
    let sig = keys.sign(&buf);

    vc.fork_sig = sig.to_vec();
}

fn verify_view_change_msg_raw(last_hash: &Vec<u8>, view: u64, fork_len: u64, last_qc: u64, sig: &Vec<u8>, keys: &KeyStore, sender: &String) -> bool {
    if sig.len() != SIGNATURE_LENGTH {
        return false;
    }
    
    let mut buf = BufWriter::new(vec![0u8; 32 + 8 + 8 + 8]);
    // 32 for hash, 8 for view, 8 for last_n, 8 for last_qc
    buf.write(last_hash);
    buf.write_u64::<BigEndian>(view);
    buf.write_u64::<BigEndian>(fork_len);
    buf.write_u64::<BigEndian>(last_qc);

    let buf = buf.into_inner().unwrap();
    keys.verify(sender, &sig.clone().try_into().unwrap(), &buf)
}

fn verify_view_change_msg(vc: &ProtoViewChange, keys: &KeyStore, sender: &String) -> bool {
    if vc.fork_len > 0 && 
    (vc.fork.is_none() || vc.fork.as_ref().unwrap().blocks.len() == 0) {
        return false;
    }
    let mut enc_buf = Vec::new();
    if vc.fork.is_some() && vc.fork.as_ref().unwrap().blocks.len() > 0 {
        if let Err(e) = vc.fork.as_ref().unwrap().blocks[vc.fork.as_ref().unwrap().blocks.len() - 1].encode(&mut enc_buf) {
            warn!("{}", e);
            return false;
        }
    }
    let hash_last = hash(&enc_buf);

    let last_qc = match &vc.fork_last_qc {
        Some(qc) => qc.n,
        None => 0,
    };

    verify_view_change_msg_raw(&hash_last, vc.view, vc.fork_len, last_qc, &vc.fork_sig, keys, sender)
}

async fn do_init_view_change<Engine>(
    ctx: &PinnedServerContext, engine: &Engine,
    client: &PinnedClient,
    super_majority: u64,
) -> Result<(), Error>
where Engine: crate::execution::Engine
{
    // Stop accepting new client requests, immediately.
    ctx.view_is_stable.store(false, Ordering::SeqCst);
    ctx.i_am_leader.store(false, Ordering::SeqCst);

    // Send tentative replies to all inflight client requests.
    do_reply_all_with_tentative_receipt(ctx).await;

    // Increase view
    let view = ctx.state.view.fetch_add(1, Ordering::SeqCst) + 1; // Fetch_add returns the old val
    let leader = get_leader_str(ctx);
    if leader == ctx.config.net_config.name {
        // I am the leader
        ctx.i_am_leader.store(true, Ordering::SeqCst);
    }else{
        ctx.i_am_leader.store(false, Ordering::SeqCst);
    }

    warn!("Moved to new view {} with leader {}", view, leader);

    // Send everything from bci onwards.
    // This is equivalent to sending the sending all preprepare and prepare messages in PBFT
    let fork = ctx.state.fork.lock().await;
    let ci = ctx.state.commit_index.load(Ordering::SeqCst);
    info!("Sending everything from {} to {}", ci, fork.last());
    let fork_from_ci = fork.serialize_from_n(ci);
    // These are all the unconfirmed blocks that the next leader may or may not have.
    // So we must send them otherwise there is a data loss.
    // It doesn't violate any guarantees, but clients will have to repropose.
    

    let mut vc_msg = ProtoViewChange {
        view,
        fork: Some(fork_from_ci),
        fork_sig: vec![0u8; SIGNATURE_LENGTH],
        fork_len: fork.last(),
        fork_last_qc: match fork.get_last_qc() {
            Ok(qc) => Some(qc),
            Err(_) => None,
        },
    };

    sign_view_change_msg(&mut vc_msg, &ctx.keys, &fork);


    if leader == ctx.config.net_config.name {
        // I won't send the message to myself.
        // @todo: Pacemaker: broadcast this
        drop(fork);
        do_process_view_change(
            ctx.clone(), engine,
            client.clone(),
            &vc_msg,
            &ctx.config.net_config.name,
            super_majority,
        )
        .await;
        return Ok(());
    }

    let mut buf = Vec::new();

    let rpc_msg_body = ProtoPayload {
        message: Some(crate::proto::rpc::proto_payload::Message::ViewChange(
            vc_msg,
        )),
    };

    rpc_msg_body.encode(&mut buf)?;
    let sz = buf.len();
    let bcast_msg = PinnedMessage::from(buf, sz, crate::rpc::SenderType::Anon);
    info!("View Change msg size: {}", sz);
    let mut profile = LatencyProfile::new();
    profile.prefix = String::from(format!("View change to {}", leader));
    profile.should_print = true;
    if let Err(e) =
        PinnedClient::broadcast(client, &vec![leader.clone()], &bcast_msg, &mut profile).await
    {
        error!("Could not broadcast ViewChange: {}", e);
    }

    Ok(())
}

pub async fn create_and_push_block<Engine>(
    ctx: PinnedServerContext, engine: &Engine,
    reqs: &mut Vec<ForwardedMessageWithAckChan>,
    should_sign: bool,
) -> Result<(ProtoAppendEntries, LatencyProfile), Error>
where Engine: crate::execution::Engine
{
    let fork = ctx.state.fork.try_lock();
    let mut fork = match fork {
        Ok(f) => {
            debug!("create_and_push_block: Fork locked");
            f
        },
        Err(e) => {
            debug!("create_and_push_block: Fork is locked, waiting for it to be unlocked: {}", e);
            ctx.state.fork.lock().await
        }
    };

    let mut tx = Vec::new();
    let block_n = fork.last() + 1;
    let mut profile = LatencyProfile::new();

    {
        let mut lack_pend = ctx.client_ack_pending.lock().await;
        for (ms, _sender, chan, profile) in reqs {
            profile.register("Client channel recv");

            if let crate::proto::rpc::proto_payload::Message::ClientRequest(req) = ms {
                if req.tx.is_some() {
                    tx.push(req.tx.clone().unwrap());
                    lack_pend.insert((block_n, tx.len() - 1), (chan.clone(), profile.to_owned()));
                }
                
            }
        }
    }

    let __view = ctx.state.view.load(Ordering::SeqCst);
    let __view_is_stable = ctx.view_is_stable.load(Ordering::SeqCst);

    if get_leader_str_for_view(&ctx, __view) != ctx.config.net_config.name {
        warn!("I am not the leader for view {}", __view);
        return Err(Error::new(ErrorKind::Other, "Not the leader"));
    }

    let mut block = ProtoBlock {
        tx,
        n: block_n,
        parent: fork.last_hash(),
        view: __view,
        qc: Vec::new(),
        sig: Some(Sig::NoSig(DefferedSignature {})),
        fork_validation: Vec::new(),
        view_is_stable: true,
    };

    if should_sign {
        // Include the next qc list.
        let mut next_qc_list = ctx.state.next_qc_list.lock().await;
        block.qc = next_qc_list.iter().map(|(_, v)| v.clone()).collect();
        next_qc_list.clear();
    }

    let entry = LogEntry::new(block);
    let __qc_trace: Vec<(u64, u64)> = entry.block.qc.iter().map(|x| (x.n, x.view)).collect();

    let res = match should_sign {
        true => fork.push_and_sign(entry, &ctx.keys),
        false => fork.push(entry),
    };

    match res {
        Ok(n) => {
            debug!("Client message sequenced at {} {}", n, block_n);
            if n % 1000 == 0 {
                let mut lpings = ctx.ping_counters.lock().unwrap();
                lpings.insert(n, Instant::now());
            }
            profile.register("Block create");

            maybe_byzantine_commit(&ctx, engine, &fork, &mut ctx.client_ack_pending.lock().await);

            trace!("QC link: {} --> {:?}", n, __qc_trace);
        }
        Err(e) => {
            warn!("Error processing client request: {}", e);
            return Err(e);
        }
    }

    let ae = ProtoAppendEntries {
        fork: Some(ProtoFork {
            blocks: vec![fork.get(fork.last()).unwrap().block.clone()],
        }),
        commit_index: ctx.state.commit_index.load(Ordering::SeqCst),
        view: __view,
        view_is_stable: __view_is_stable,
    };

    Ok((ae, profile))
}

pub async fn broadcast_append_entries(
    ctx: PinnedServerContext,
    client: PinnedClient,
    ae: ProtoAppendEntries,
    send_list: &Vec<String>,
    mut profile: LatencyProfile,
) -> Result<(), Error> {
    if send_list.len() == 0 {
        return Ok(());
    }

    let mut buf = Vec::new();
    let block_n = ae.fork.as_ref().unwrap().blocks.len();
    let block_n = ae.fork.as_ref().unwrap().blocks[block_n - 1].n;

    let rpc_msg_body = ProtoPayload {
        message: Some(crate::proto::rpc::proto_payload::Message::AppendEntries(ae)),
    };

    rpc_msg_body.encode(&mut buf)?;
    let sz = buf.len();
    let bcast_msg = PinnedMessage::from(buf, sz, crate::rpc::SenderType::Anon);
    if block_n % 1000 == 0 {
        profile.should_print = true;
        profile.prefix = String::from(format!("AppendEntries Block {}", block_n));
    }
    let start_bcast = Instant::now();
    let _ = PinnedClient::broadcast(&client, &send_list, &bcast_msg, &mut profile).await;
    if block_n % 1000 == 0 {
        trace!(
            "AppendEntries Block: {}, Broadcast time: {} us",
            block_n,
            start_bcast.elapsed().as_micros()
        );
    }

    Ok(())
}

pub fn do_add_block_to_byz_qc_pending(
    byz_qc_pending: &mut MutexGuard<HashSet<u64>>,
    ae: &ProtoAppendEntries,
) {
    let blocks: &Vec<ProtoBlock> = ae.fork.as_ref().unwrap().blocks.as_ref();
    for b in blocks {
        byz_qc_pending.insert(b.n);
    }
}

pub async fn do_append_entries<Engine>(
    ctx: PinnedServerContext, engine: &Engine,
    client: PinnedClient,
    reqs: &mut Vec<ForwardedMessageWithAckChan>,
    should_sign: bool,
    send_list: &Vec<String>,
    majority: u64,
    super_majority: u64,
) -> Result<(), Error>
where Engine: crate::execution::Engine
{
    // Create the block, holding a lock on the fork state.
    let (ae, profile) = create_and_push_block(ctx.clone(), engine, reqs, should_sign).await?;

    // Add this block to byz_qc_pending, if it is a signed block
    if should_sign {
        let mut byz_qc_pending = ctx.state.byz_qc_pending.lock().await;
        do_add_block_to_byz_qc_pending(&mut byz_qc_pending, &ae);
    }

    // Vote for self; necessary since the network subsystem doesn't send my message to me.
    let block_n = ae.fork.as_ref().unwrap().blocks.len();
    let block_n = ae.fork.as_ref().unwrap().blocks[block_n - 1].n;
    let my_vote = create_vote_for_blocks(ctx.clone(), &vec![block_n]).await?;
    do_process_vote(
        ctx.clone(), engine,
        &my_vote,
        &ctx.config.net_config.name,
        majority,
        super_majority,
    )
    .await?;

    // Lock on the fork is not kept when broadcasting.
    broadcast_append_entries(ctx, client, ae, send_list, profile).await?;

    Ok(())
}

pub async fn bulk_reply_to_client(reqs: &Vec<ForwardedMessageWithAckChan>, msg: PinnedMessage) {
    for (_, _, chan, profile) in reqs {
        chan.send((msg.clone(), profile.clone())).unwrap();
    }
}

pub async fn do_respond_with_try_again(reqs: &Vec<ForwardedMessageWithAckChan>) {
    let try_again = ProtoClientReply {
        reply: Some(
            crate::proto::client::proto_client_reply::Reply::TryAgain(ProtoTryAgain {}),
        ),
    };

    let mut buf = Vec::new();
    try_again.encode(&mut buf).unwrap();
    let sz = buf.len();
    let msg = PinnedMessage::from(buf, sz, crate::rpc::SenderType::Anon);

    bulk_reply_to_client(reqs, msg).await;
}

pub async fn do_respond_with_current_leader(
    ctx: &PinnedServerContext,
    reqs: &Vec<ForwardedMessageWithAckChan>,
) {
    let leader = ProtoClientReply {
        reply: Some(crate::proto::client::proto_client_reply::Reply::Leader(
            ProtoCurrentLeader {
                name: get_leader_str(ctx),
            },
        )),
    };

    let mut buf = Vec::new();
    leader.encode(&mut buf).unwrap();
    let sz = buf.len();
    let msg = PinnedMessage::from(buf, sz, crate::rpc::SenderType::Anon);

    bulk_reply_to_client(reqs, msg).await;
}

pub async fn handle_client_messages<Engine>(
    ctx: PinnedServerContext,
    client: PinnedClient,
    engine: Engine
) -> Result<(), Error> 
where 
    Engine: crate::execution::Engine + Clone + Send + Sync + 'static
{
    let mut client_rx = ctx.0.client_queue.1.lock().await;
    let mut curr_client_req = Vec::new();
    let mut curr_client_req_num = 0;
    let mut signature_timer_tick = false;

    let majority = get_majority_num(&ctx);
    let super_majority = get_super_majority_num(&ctx);
    let send_list = get_everyone_except_me(
        &ctx.config.net_config.name,
        &ctx.config.consensus_config.node_list,
    );

    // Signed block logic: Either this timer expires.
    // Or the number of blocks crosses signature_max_delay_blocks.
    // In the later case, reset the timer.
    let signature_timer = ResettableTimer::new(Duration::from_millis(
        ctx.config.consensus_config.signature_max_delay_ms,
    ));

    let signature_timer_handle = signature_timer.run().await;

    let mut pending_signatures = 0;


    loop {
        tokio::select! {
            biased;
            n_ = client_rx.recv_many(&mut curr_client_req, ctx.config.consensus_config.max_backlog_batch_size) => {
                curr_client_req_num = n_;
            },
            tick = signature_timer.wait() => {
                signature_timer_tick = tick;
            }
        }

        if curr_client_req_num == 0 && signature_timer_tick == false {
            // Channels are all closed.
            break;
        }

        ctx.total_client_requests.fetch_add(curr_client_req_num, Ordering::SeqCst);
        trace!("Client handler: {} client requests", ctx.total_client_requests.load(Ordering::SeqCst));
        
        if !ctx.i_am_leader.load(Ordering::SeqCst) {
            do_respond_with_current_leader(&ctx, &curr_client_req).await;
            // Reset for next iteration
            curr_client_req.clear();
            curr_client_req_num = 0;
            signature_timer_tick = false;
            continue;
        }

        if !ctx.view_is_stable.load(Ordering::SeqCst) {
            do_respond_with_try_again(&curr_client_req).await;
            // @todo: Backoff here.
            // Reset for next iteration
            curr_client_req.clear();
            curr_client_req_num = 0;
            signature_timer_tick = false;
            continue;
        }

        // @todo: Reply to read requests (even when I am not the leader).

        
        // Ok I am the leader.
        pending_signatures += 1;
        let mut should_sig = signature_timer_tick    // Either I am running this body because of signature timeout.
            || (pending_signatures >= ctx.config.consensus_config.signature_max_delay_blocks);
        // Or I actually got some transactions and I really need to sign

        match do_append_entries(
            ctx.clone(), &engine.clone(), client.clone(),
            &mut curr_client_req, should_sig,
            &send_list, majority, super_majority,
        ).await {
            Ok(_) => {}
            Err(e) => {
                warn!("Error doing append entries {}", e);
                do_respond_with_current_leader(&ctx, &curr_client_req).await;
                should_sig = false;
            }
        };

        if should_sig {
            pending_signatures = 0;
            signature_timer.reset();
        }

        // Reset for next iteration
        curr_client_req.clear();
        curr_client_req_num = 0;
        signature_timer_tick = false;
    }

    warn!("Client handler dying!");

    let _ = join!(signature_timer_handle);
    Ok(())
}

pub async fn handle_node_messages<Engine>(
    ctx: PinnedServerContext,
    client: PinnedClient,
    engine: Engine
) -> Result<(), Error>
    where Engine: crate::execution::Engine + Clone + Send + Sync + 'static
{
    let view_timer_handle = ctx.view_timer.run().await;
    let majority = get_majority_num(&ctx);
    let super_majority = get_super_majority_num(&ctx);

    // Spawn all vote processing workers.
    // This thread will also act as one.
    let mut vote_worker_chans = Vec::new();
    let mut vote_worker_rr_cnt = 1u16;
    for _ in 1..ctx.config.consensus_config.vote_processing_workers {
        // 0th index is for this thread
        let (tx, mut rx) = mpsc::unbounded_channel();
        vote_worker_chans.push(tx);
        let ctx = ctx.clone();
        let client = client.clone();
        let engine = engine.clone();
        tokio::spawn(async move {
            loop {
                let msg = rx.recv().await;
                if let None = msg {
                    break;
                }
                let mut msg = msg.unwrap();
                let _ =
                    process_node_request(&ctx, &engine, &client, majority, super_majority, &mut msg).await;
            }
        });
    }

    let mut node_rx = ctx.0.node_queue.1.lock().await;
    let send_list = get_everyone_except_me(
        &ctx.config.net_config.name,
        &ctx.config.consensus_config.node_list,
    );

    debug!(
        "Leader: {}, Send List: {:?}",
        ctx.i_am_leader.load(Ordering::SeqCst),
        &send_list
    );

    let mut curr_node_req = Vec::new();
    let mut view_timer_tick = false;
    let mut node_req_num = 0;

    let mut intended_view = 0;

    // Start with a view change
    ctx.view_timer.fire_now().await;

    loop {
        tokio::select! {
            biased;
            tick = ctx.view_timer.wait() => {
                view_timer_tick = tick;
            }
            node_req_num_ = node_rx.recv_many(&mut curr_node_req, (majority - 1) as usize) => node_req_num = node_req_num_,
        }

        if view_timer_tick == false && node_req_num == 0 {
            warn!("Consensus node dying!");
            break; // Select failed because both channels were closed!
        }

        if view_timer_tick {
            info!("Timer fired");
            intended_view += 1;
            if intended_view > ctx.state.view.load(Ordering::SeqCst) {
                if let Err(e) = do_init_view_change(&ctx, &engine, &client, super_majority).await {
                    error!("Error initiating view change: {}", e);
                }
            }
        }

        while node_req_num > 0 {
            // @todo: Really need a VecDeque::pop_front() here. But this suffices for now.
            let mut req = curr_node_req.remove(0);
            node_req_num -= 1;
            // AppendEntries should be processed by a single thread.
            // Only votes can be safely processed by multiple threads.
            if let crate::proto::rpc::proto_payload::Message::Vote(_) = req.0 {
                let rr_cnt =
                    vote_worker_rr_cnt % ctx.config.consensus_config.vote_processing_workers;
                vote_worker_rr_cnt += 1;
                if rr_cnt == 0 {
                    // Let this thread process it.
                    if let Err(e) = process_node_request(&ctx, &engine, &client, majority, super_majority, &mut req).await {
                        error!("Error processing vote: {}", e);
                    }
                } else {
                    // Push it to a worker
                    let _ = vote_worker_chans[(rr_cnt - 1) as usize].send(req);
                }
            } else {
                if let Err(e) = process_node_request(&ctx, &engine, &client, majority, super_majority, &mut req).await {
                    error!("Error processing node request: {}", e);
                }
            }
        }

        // Reset for the next iteration
        curr_node_req.clear();
        node_req_num = 0;
        view_timer_tick = false;
    }

    let _ = join!(view_timer_handle);
    Ok(())
}

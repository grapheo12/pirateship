// Copyright (c) Shubham Mishra. All rights reserved.
// Licensed under the MIT License.

use hex::ToHex;
use indexmap::IndexMap;
use log::{debug, error, info, trace, warn};
use prost::Message;
use std::{
    collections::HashSet, io::{Error, ErrorKind}, sync::atomic::Ordering, time::Instant
};
use tokio::sync::MutexGuard;

use crate::{
    consensus::{
        backfill::maybe_backfill_fork_till_last_match, handler::{ForwardedMessageWithAckChan, PinnedServerContext}, log::{Log, LogEntry}, reconfiguration::decide_my_lifecycle_stage
    }, crypto::{cmp_hash, DIGEST_LENGTH}, proto::{
        consensus::{
            proto_block::Sig, DefferedSignature, ProtoAppendEntries, ProtoBlock, ProtoFork, ProtoQuorumCertificate, ProtoSignatureArrayEntry, ProtoVote,
        }, rpc::ProtoPayload
    }, rpc::{
        client::PinnedClient,
        server::LatencyProfile,
        PinnedMessage,
    }
};

use crate::consensus::commit::*;
use crate::consensus::view_change::*;
use crate::consensus::utils::*;

use super::reconfiguration::fast_forward_config;

pub async fn create_vote_for_blocks(
    ctx: PinnedServerContext,
    seq_nums: &Vec<u64>,
) -> Result<ProtoVote, Error> {
    // let mut fork = ctx.state.fork.lock().await;
    let fork = ctx.state.fork.try_lock();
    let _cfg = ctx.config.get();
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

        #[cfg(not(feature = "no_qc"))]
        {
            if fork.get(n)?.has_signature() {
                atleast_one_sig_block = true;
                let sig = fork.signature_at_n(n, &ctx.keys.get()).to_vec();
    
                // This is just caching the signature.
                fork.inc_qc_sig_unverified(&_cfg.net_config.name, &sig, n)?;
    
                // Add the block to byz_qc_pending,
                // along with all other blocks for which I have sent signature before,
                // but haven't seen a QC
                byz_qc_pending.insert(n);
            }
        }

        fork.inc_replication_vote(&_cfg.net_config.name, n)?;
    }

    // Resend signatures for QCs I did not get yet.
    // This is safer than creating QCs with votes to higher blocks.
    #[cfg(not(feature = "no_qc"))]
    {
        if atleast_one_sig_block {
            // Invariant: Only signature blocks get signature votes.
            for n in byz_qc_pending.iter() {
                if *n > fork.last() {
                    continue;
                }
                if let Some(sig) = fork.get(*n)?.qc_sigs.get(&_cfg.net_config.name) {
                    vote_sigs.push(ProtoSignatureArrayEntry {
                        n: *n,
                        sig: sig.to_vec(),
                    });
                }
            }
        }
    }

    Ok(ProtoVote {
        sig_array: vote_sigs,
        fork_digest: fork.hash_at_n(max_n).unwrap_or(vec![0u8; DIGEST_LENGTH]),
        n: max_n,
        view,
        config_num: ctx.state.config_num.load(Ordering::SeqCst),
    })
}

pub fn do_create_qcs(
    ctx: &PinnedServerContext,
    fork: &mut MutexGuard<Log>,
    next_qc_list: &mut MutexGuard<IndexMap<(u64, u64), ProtoQuorumCertificate>>,
    byz_qc_pending: &mut MutexGuard<HashSet<u64>>,
    qcs: &Vec<u64>,
    fast_path_qcs: &Vec<u64>,
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

        // Invariant: qc.view is the same as the view of the block which carries it.

        next_qc_list.insert((qc.n, qc.view), qc);
        byz_qc_pending.remove(n);

        if *n % 1000 == 0 {
            trace!("QC formed for index: {}", n);
        } else {
            debug!("QC formed for index: {}", n);
        }
    }

    // Fast path QCs can only be formed once, since it takes ALL votes to create one
    // Once disseminated, fast path QCs fo byzantine commit.
    // So for deduplication (in case a vote is sent twice), only need to compare with bci.

    let bci = ctx.state.byz_commit_index.load(Ordering::SeqCst);
    for n in fast_path_qcs {
        if *n <= bci {
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
        info!("Fast path qc for {}", n);
    }
}

pub async fn do_process_vote<Engine>(
    ctx: PinnedServerContext, client: PinnedClient, engine: &Engine,
    vote: &ProtoVote,
    sender: &String,
    majority: u64,
    super_majority: u64,
    old_super_majority: u64
) -> Result<(), Error>
where Engine: crate::execution::Engine
{
    // let mut fork = ctx.state.fork.lock().await;
    let _cfg = ctx.config.get();
    let _keys = ctx.keys.get();
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

    if vote.view != ctx.state.view.load(Ordering::SeqCst) {
        trace!("Vote for mismatched view {} (my view {})! Rejected", vote.view, ctx.state.view.load(Ordering::SeqCst));
        return Ok(());
    }

    let member_list = &ctx.config.get().consensus_config.node_list;
    if !member_list.contains(sender) {
        if !ctx.view_is_stable.load(Ordering::SeqCst) {
            // If view is not stable, then votes from old_full_nodes allowed.
            let old_full_nodes = &ctx.old_full_nodes.get();
            if !old_full_nodes.contains(sender) {
                warn!("Vote from non-member {} {:?}! Rejected", sender, member_list);
                return Ok(());
            }
        } else {
            warn!("Vote from non-member {} {:?}! Rejected", sender, member_list);
            return Ok(());
        }
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
    let mut fast_path_qcs = Vec::new();

    for vote_sig in &vote.sig_array {
        if vote_sig.n > fork.last() {
            continue;
        }
        // Try to increase the signature after verifying against my own fork.
        match fork.inc_qc_sig(sender, &vote_sig.sig, vote_sig.n, &_keys) {
            Ok(_total_sigs) => {
                let sigs = &fork.get(vote_sig.n).unwrap().qc_sigs;
                let fullnode_sigs = sigs.iter().filter(|(signer, _)| {
                    _cfg.consensus_config.node_list.contains(signer)
                }).count();
                let _old_full_nodes = ctx.old_full_nodes.get();
                let oldfullnode_sigs = sigs.iter().filter(|(signer, _)| {
                   _old_full_nodes.contains(signer)
                }).count();

                if !ctx.view_is_stable.load(Ordering::SeqCst) {
                    // Fast path can't be done if the view is not stable.
                    info!("For fast path: {} {}", fullnode_sigs, get_all_nodes_num(&ctx));
                    if fullnode_sigs == get_all_nodes_num(&ctx) as usize {
                        fast_path_qcs.push(vote_sig.n);
                    }
                }
                if fullnode_sigs >= super_majority as usize
                && (old_super_majority == 0 || oldfullnode_sigs >= old_super_majority as usize)
                {
                    qcs.push(vote_sig.n);
                    debug!("Creating QC for {}", vote_sig.n);
                    if vote_sig.n == fork.last() && !ctx.view_is_stable.load(Ordering::SeqCst)
                        && fork.get(vote_sig.n).unwrap().block.fork_validation.len() >= super_majority as usize
                    {
                        // This was a view change message for which we got QC.
                        // View is stabilised hence.
                        ctx.view_is_stable.store(true, Ordering::SeqCst);
                        info!("View stabilised!");
                        let mut fork_buf = ctx.state.fork_buffer.lock().await;
                        fork_buf.retain(|&v, _| v < vote.view);

                        // Remove all old_full_nodes.
                        ctx.old_full_nodes.set(Box::new(Vec::new()));
                        let lifecycle_stage = decide_my_lifecycle_stage(&ctx, false);
                        ctx.lifecycle_stage.store(lifecycle_stage as i8, Ordering::SeqCst);
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
            &fast_path_qcs,
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

            #[cfg(not(feature = "quorum_diversity"))]
            let k = 1;

            #[cfg(feature = "quorum_diversity")]
            let k = _cfg.consensus_config.quorum_diversity_k;

            if byz_qc_pending.contains(&i)
                && byz_qc_pending.len() >= k
            {
                // This code path is never triggered if no_qc or never_sign is enabled.
                // Since byz_qc_pending is always empty.

                qd_should_wait_supermajority = true;
                __flipped = true;
                __byz_qc_pending_len = byz_qc_pending.len();
            }
        }

        if __flipped {
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
        do_commit(&ctx, &client, engine, &mut fork, updated_ci).await;
    }

    Ok(())
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
    let ae = if ae.config_num < ctx.state.config_num.load(Ordering::SeqCst) {
        trace!("Message from older config! Rejected; Sent by: {} Is New leader? {} Msg config: {} Current config {}",
            sender, !ae.fork.as_ref().unwrap().blocks[0].view_is_stable, ae.config_num, ctx.state.config_num.load(Ordering::SeqCst));
        
        let fork = ctx.state.fork.lock().await;
        let last_n = fork.last();
        let seq_nums = Vec::new();

        return (last_n, last_n, seq_nums, false);
    } else if ae.config_num > ctx.state.config_num.load(Ordering::SeqCst) {
        // Need to fast forward to the new config.
        // Otherwise the View Change may not be verified.
        info!("Fast forwarding to config {}", ae.config_num);

        // This will lock on fork, let's ensure fork is not locked by the caller here.
        &fast_forward_config(&ctx, &client, engine, ae, sender).await
    } else {
        debug!("Same config block: {}", ae.config_num);
        ae
    };

    // At this point, the config matches that of the ae.
    // ae.fork ONLY contains blocks in this config.
    // So we can now process as if no reconfiguration happened.

    // let mut fork = ctx.state.fork.lock().await;
    let _cfg = ctx.config.get();
    let _keys = ctx.keys.get();

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
        if ctx.last_stable_view.load(Ordering::SeqCst) <= ae.view {
            info!("I timed out due to network partition; Now going back to view {} from {}",
                ctx.state.view.load(Ordering::SeqCst), ctx.last_stable_view.load(Ordering::SeqCst));
            ctx.state.view.store(ctx.last_stable_view.load(Ordering::SeqCst), Ordering::SeqCst);
        } else {
            trace!("Message from older view! Rejected; Sent by: {} Is New leader? {} Msg view: {} Current View {}",
                sender, !ae.fork.as_ref().unwrap().blocks[0].view_is_stable, ae.view, ctx.state.view.load(Ordering::SeqCst));
            return (last_n, last_n, seq_nums, false);
        }
    }
    if ae.view > ctx.state.view.load(Ordering::SeqCst) {
        ctx.state.view.store(ae.view, Ordering::SeqCst);
        if get_leader_str(&ctx) == _cfg.net_config.name {
            ctx.i_am_leader.store(true, Ordering::SeqCst);
            // Don't think this can happen again.
        }else{
            ctx.i_am_leader.store(false, Ordering::SeqCst);
        }
        info!("View fast forwarded to {}! stable? {}", ae.view, ae.view_is_stable);
        // Since moving to new view, I can no longer send proper commit responses to clients.
        // Send tentative replies to everyone (if I am the leader in the old view).
        do_reply_all_with_tentative_receipt(&ctx).await;
        // ctx.view_timer.reset();


        // @todo: RESET VIEW TO OLD VALUE IF FORK COULD NOT BE VERIFIED
    }else{
        trace!("AppendEntries for view {} stable? {} sender {}", ae.view, ae.view_is_stable, sender);
    }

    if ae.view_is_stable && ae.view > ctx.last_stable_view.load(Ordering::SeqCst) {
        ctx.last_stable_view.store(ae.view, Ordering::SeqCst);
    }

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
                fork.verify_and_push(entry, &_keys, &get_leader_str_for_view(&ctx, b.view))
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
    let new_stable = ae.view_is_stable;
    if new_stable && !old_stable {
        info!("View stabilised.");
        
        let mut fork_buf = ctx.state.fork_buffer.lock().await;
        fork_buf.retain(|&v, _| v < ae.view);
        
        // View is stable. So all OldFullNodes become learners.
        ctx.old_full_nodes.set(Box::new(Vec::new()));
        let lifecycle_stage = decide_my_lifecycle_stage(&ctx, false);
        ctx.lifecycle_stage.store(lifecycle_stage as i8, Ordering::SeqCst);
    }
    ctx.view_is_stable.store(ae.view_is_stable, Ordering::SeqCst);



    if fork.last_qc() > last_qc {
        // If the last_qc progressed forward, need to clean up byz_qc_pending
        let last_qc = fork.last_qc();
        let mut byz_qc_pending = ctx.state.byz_qc_pending.lock().await;
        byz_qc_pending.retain(|&n| n > last_qc);

        // Also the byzantine commit index may move
        let did_byz_commit = maybe_byzantine_commit(&ctx, &client, engine, &fork).await;

        if did_byz_commit {
            // Pacemaker logic: Reset the view timer.
            ctx.view_timer.reset();
        }
    }

    (last_n, updated_last_n, seq_nums, true)
}

pub async fn do_reply_vote(
    _ctx: PinnedServerContext,
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

pub async fn create_and_push_block<Engine>(
    ctx: PinnedServerContext, client: PinnedClient, engine: &Engine,
    reqs: &mut Vec<ForwardedMessageWithAckChan>,
    should_sign: bool,
) -> Result<(ProtoAppendEntries, LatencyProfile), Error>
where Engine: crate::execution::Engine
{
    let _cfg = ctx.config.get();
    let _keys = ctx.keys.get();

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

    if get_leader_str_for_view(&ctx, __view) != _cfg.net_config.name {
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
        config_num: ctx.state.config_num.load(Ordering::SeqCst),
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
        true => fork.push_and_sign(entry, &_keys),
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

            let did_byz_commit = maybe_byzantine_commit(&ctx, &client, engine, &fork).await;

            if did_byz_commit {
                // Pacemaker logic: Reset the view timer.
                ctx.view_timer.reset();
            }

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
        config_num: ctx.state.config_num.load(Ordering::SeqCst),
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

    // Also send to all learners
    let _ = PinnedClient::broadcast(&client, &ctx.config.get().consensus_config.learner_list, &bcast_msg, &mut profile).await;



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
    old_super_majority: u64,
) -> Result<(), Error>
where Engine: crate::execution::Engine
{
    // Create the block, holding a lock on the fork state.
    let _cfg = ctx.config.get();
    let (ae, profile) = create_and_push_block(ctx.clone(), client.clone(), engine, reqs, should_sign).await?;

    // Add this block to byz_qc_pending, if it is a signed block
    #[cfg(not(feature = "no_qc"))]
    {
        if should_sign {
            let mut byz_qc_pending = ctx.state.byz_qc_pending.lock().await;
            do_add_block_to_byz_qc_pending(&mut byz_qc_pending, &ae);
        }
    }

    // Vote for self; necessary since the network subsystem doesn't send my message to me.
    let block_n = ae.fork.as_ref().unwrap().blocks.len();
    let block_n = ae.fork.as_ref().unwrap().blocks[block_n - 1].n;
    let my_vote = create_vote_for_blocks(ctx.clone(), &vec![block_n]).await?;
    do_process_vote(
        ctx.clone(), client.clone(), engine,
        &my_vote,
        &_cfg.net_config.name,
        majority,
        super_majority,
        old_super_majority
    )
    .await?;

    // Lock on the fork is not kept when broadcasting.
    broadcast_append_entries(ctx, client, ae, send_list, profile).await?;

    Ok(())
}

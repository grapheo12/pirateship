use byteorder::{BigEndian, WriteBytesExt};
use ed25519_dalek::SIGNATURE_LENGTH;
use hex::ToHex;
use log::{debug, error, info, trace, warn};
use prost::Message;
use std::{
    collections::HashMap, io::{BufWriter, Error, ErrorKind, Write}, sync::atomic::Ordering
};
use tokio::sync::MutexGuard;

use crate::{
    consensus::{
        handler::PinnedServerContext,
        log::{Log, LogEntry},
    }, crypto::{hash, KeyStore},
    proto::{
        client::{
            ProtoClientReply, ProtoClientRequest, ProtoTentativeReceipt
        }, consensus::{
            DefferedSignature, ProtoAppendEntries, ProtoBlock, ProtoFork,
            ProtoForkValidation,
            ProtoViewChange,
        }, execution::ProtoTransaction, rpc::ProtoPayload
    },
    rpc::{
        client::PinnedClient,
        server::LatencyProfile,
        PinnedMessage,
    }
};

use crate::consensus::backfill::*;
use crate::consensus::commit::*;
use crate::consensus::steady_state::*;
use crate::consensus::utils::*;

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

fn sign_view_change_msg(vc: &mut ProtoViewChange, keys: &KeyStore, fork: &MutexGuard<Log>) {
    // Precondition: A correctly generated vc message will have fork.last() in its fork.
    //              OR have fork_len == 0
    let mut buf = BufWriter::new(vec![0u8; 32 + 8 + 8 + 8]);
    // 32 for hash, 8 for view, 8 for last_n, 8 for last_qc
    let _ = buf.write(&fork.last_hash());
    let _ = buf.write_u64::<BigEndian>(vc.view);
    let _ = buf.write_u64::<BigEndian>(vc.fork_len);
    let _ = buf.write_u64::<BigEndian>(match &vc.fork_last_qc {
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
    let _ = buf.write(last_hash);
    let _ = buf.write_u64::<BigEndian>(view);
    let _ = buf.write_u64::<BigEndian>(fork_len);
    let _ = buf.write_u64::<BigEndian>(last_qc);

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

pub async fn do_reply_all_with_tentative_receipt(ctx: &PinnedServerContext) {
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

pub async fn do_init_view_change<Engine>(
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
    let entry = LogEntry::new(block);

    match fork.push_and_sign(entry, &ctx.keys) {
        Ok(_) => {}
        Err(e) => {
            error!("Error pushing block: {}", e);
            return;
        }
    }
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

    let my_vote = create_vote_for_blocks(ctx.clone(), &vec![block_n])
        .await
        .unwrap();

    broadcast_append_entries(ctx.clone(), client.clone(), ae, &send_list, profile.clone())
    .await
    .unwrap();

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


    profile.should_print = true;
    profile.prefix = String::from("New leader message");

    profile.force_print();

    // Force a dummy message to be sequenced.
    force_noop(&ctx).await;

    // If I get AppendEntries or NewLeader from higher view during this time,
    // need to update view again and become a follower.
}

async fn force_noop(ctx: &PinnedServerContext) {
    let client_tx = ctx.client_queue.0.clone();
    
    let client_req = ProtoClientRequest {
        tx: Some(ProtoTransaction {
            on_receive: None,
            on_crash_commit: None,
            on_byzantine_commit: None,
        }),
        // sig: vec![0u8; SIGNATURE_LENGTH],
        sig: vec![0u8; 1],
        origin: ctx.config.net_config.name.clone(),
    };

    let request = crate::proto::rpc::proto_payload::Message::ClientRequest(client_req);
    let profile = LatencyProfile::new();

    let _ = client_tx.send((request, ctx.config.net_config.name.clone(), ctx.__client_black_hole_channel.0.clone(), profile));

    
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
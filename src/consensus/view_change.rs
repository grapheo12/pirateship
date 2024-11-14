// Copyright (c) Shubham Mishra. All rights reserved.
// Licensed under the MIT License.

use byteorder::{BigEndian, WriteBytesExt};
use ed25519_dalek::SIGNATURE_LENGTH;
use hex::ToHex;
use log::{debug, error, info, trace, warn};
use prost::Message;
use std::{
    collections::{HashMap, HashSet}, io::{BufWriter, Error, ErrorKind, Write}, sync::atomic::Ordering
};
use tokio::sync::MutexGuard;

use crate::{
    consensus::{
        handler::{LifecycleStage, PinnedServerContext},
        log::{Log, LogEntry}, reconfiguration::fast_forward_config_from_vc,
    }, crypto::{cmp_hash, hash, KeyStore},
    proto::{
        client::{
            ProtoClientReply, ProtoClientRequest, ProtoTentativeReceipt
        }, consensus::{
            DefferedSignature, ProtoAppendEntries, ProtoBlock, ProtoFork, ProtoForkValidation, ProtoQuorumCertificate, ProtoViewChange
        }, execution::ProtoTransaction, rpc::ProtoPayload
    },
    rpc::{
        client::PinnedClient,
        server::LatencyProfile,
        PinnedMessage,
    }, utils::AgnosticRef
};

use crate::consensus::backfill::*;
use crate::consensus::commit::*;
use crate::consensus::steady_state::*;
use crate::consensus::utils::*;

#[derive(Clone, Debug)]
struct ForkStat {
    last_view: u64,
    last_n: u64,
    last_qc_view: u64,
    last_qc_n: u64,
    fork_suffix_after_last_qc_n: ProtoFork,
    // last_signed_block: u64,
    name: String
}

fn __hash_block(b: &ProtoBlock) -> Vec<u8> {
    hash(&b.encode_to_vec())
}

/// Take the highest QC.n (since this is called after subrule1, highest QC from each fork will be of the highest possible view)
/// See if |forks sharing that QC| >= f + 1, otherwise, nobody could've gone fast path.
/// For all positions after the highest QC.n
///     See if one block has support of >= (f + 1) forks,
///     but doesn't conflict with another block that has >= (f + 1) forks
/// Take the fork for which this signed block is the highest.
/// (There should be at least (f + 1) that match this.)
/// Drop everything else.
fn fork_choice_filter_fast_path(
    ctx: &PinnedServerContext,
    fork_set: &mut HashMap<String, ForkStat>
) {
    let _fork_set = & *fork_set;
    let mut highest_qc_n = 0;
    for (_, stat) in _fork_set {
        if highest_qc_n < stat.last_qc_n {
            highest_qc_n = stat.last_qc_n;
        }
    }

    let matching_qc_forks = _fork_set.iter()
        .map(|e| e.1.last_qc_n)
        .reduce(|acc, e| {
            if e == highest_qc_n {
                acc + 1
            } else {
                acc
            }
        }
    );

    let f_plus_one = get_f_plus_one_num(ctx);

    if matching_qc_forks.is_none() || matching_qc_forks.unwrap() < f_plus_one {
        return;
    }

    fork_set.retain(|_, v| {
        v.last_qc_n == highest_qc_n
    });

    let _fork_set = & *fork_set;
    let start_point = _fork_set.iter()
    .map(|(_, v)| {
        if v.fork_suffix_after_last_qc_n.blocks.len() == 0 {
            highest_qc_n
        } else {
            v.fork_suffix_after_last_qc_n.blocks.last().as_ref().unwrap().n
        }
    })
    .reduce(|acc, e| if acc > e { e } else { acc })
    .unwrap();

    if start_point == highest_qc_n {
        return;
    }

    for n in (highest_qc_n + 1..(start_point + 1)).rev() {
        let mut supports: HashMap<Vec<u8>, HashSet<String>> = HashMap::new();
        for (name, stat) in _fork_set {
            let blk: Vec<_> = stat.fork_suffix_after_last_qc_n.blocks.iter().filter(|e| {
                e.n == n
            }).collect();
            if blk.len() == 0 {
                continue;
            }
            let blk = blk[0];
            let blk_hsh = __hash_block(&blk);

            if !supports.contains_key(&blk_hsh) {
                supports.insert(blk_hsh.clone(), HashSet::new());
            }
            supports.get_mut(&blk_hsh).unwrap().insert(name.clone());
        }

        // How many blocks have >= (f + 1) support?
        let possible_fp_blocks: HashMap<_, _> = 
            supports.iter().filter(|(_, v)| v.len() as u64 >= f_plus_one)
            .collect();

        if possible_fp_blocks.len() == 0 {
            // This position couldn't have been fast path committed
            continue;
        } else if possible_fp_blocks.len() > 1 {
            // Somebody is definitely lying.
            // There can't be any fast path commit here.
            continue;
        } else {
            // This may have been committed by fast path.
            // Due to hash chaining, we don't have to check more.
            let fp_support_names: &HashSet<String> = possible_fp_blocks.into_values()
                .collect::<Vec<_>>()[0];
            fork_set.retain(|k, _| {
                fp_support_names.contains(k)
            });

            return;
        }
            
    }
}

fn check_for_equivocation(fork_set: &HashMap<String, ProtoViewChange>) {
    // Highest common block_n
    let mut max_n = 0;
    let mut max_fork_name = String::new();
    fork_set.iter().for_each(|(k, v)| {
        if v.fork.is_none() || v.fork.as_ref().unwrap().blocks.len() == 0 {
            return;
        }
        let _n = v.fork.as_ref().unwrap().blocks.last().unwrap().n;
        if _n > max_n {
            max_n = _n;
            max_fork_name = k.clone();
        }
    });

    if max_n == 0 {
        return;
    }

    let chk_hsh = hash(&fork_set.get(&max_fork_name).unwrap().fork.as_ref().unwrap().blocks.last().unwrap().encode_to_vec());
    fork_set.iter().for_each(|(_k, v)| {
        if v.fork.is_none() || v.fork.as_ref().unwrap().blocks.len() == 0 {
            return;
        }
        for blk in &v.fork.as_ref().unwrap().blocks {
            if blk.n != max_n {
                continue;
            }

            let hsh = hash(&blk.encode_to_vec());

            if !cmp_hash(&chk_hsh, &hsh) {
                warn!("Equivocation detected!");
            }
        }
    });


}

fn fork_choice_rule_get(
    ctx: &PinnedServerContext,
    fork_set: &HashMap<String, ProtoViewChange>,
    my_name: &String,
) -> (ProtoFork, ForkStat) {
    // Sanity check: warn if there was equivocation!
    // check_for_equivocation(fork_set);

    let mut chk_stats = HashMap::<String, ForkStat>::new();
    for (name, vc) in fork_set {
        let fork = vc.fork.as_ref().unwrap();
        let fork_len = fork.blocks.len();
        if fork_len == 0 {
            chk_stats.insert(
                name.clone(),
                ForkStat {
                    last_n: 0,
                    last_qc_view: 0,
                    last_view: 0,
                    last_qc_n: 0,
                    fork_suffix_after_last_qc_n: ProtoFork::default(),
                    // last_signed_block: 0,
                    name: name.clone()
                },
            );
            continue;
        }
        let last_view = vc.view;
        let last_n = vc.fork_len;

        let (last_qc_view, last_qc_n, fork_suffix_after_last_qc_n) = match &vc.fork_last_qc {
            Some(qc) => {
                let mut suffix = vc.fork.as_ref().unwrap().clone();
                suffix.blocks.retain(|e| e.n > qc.n);
                (qc.view, qc.n, suffix)
            },
            None => (0, 0, ProtoFork::default()),
        };
        chk_stats.insert(
            name.clone(),
            ForkStat {
                last_n,
                last_qc_view,
                last_view,
                last_qc_n,
                fork_suffix_after_last_qc_n,
                // last_signed_block,
                name: name.clone()
            },
        );
    }

    // SubRule1: last_qc.view as high as possible.
    let mut max_last_qc_view = 0;
    for (_name, fork) in &chk_stats {
        if fork.last_qc_view > max_last_qc_view {
            max_last_qc_view = fork.last_qc_view;
        }
    }

    chk_stats.retain(|_k, v| v.last_qc_view == max_last_qc_view);

    // SubRule for fastpath
    #[cfg(feature = "fast_path")]
    fork_choice_filter_fast_path(ctx, &mut chk_stats);

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

fn sign_view_change_msg(vc: &mut ProtoViewChange, keys: &KeyStore, fork_last_hash: &Vec<u8>) {
    // Precondition: A correctly generated vc message will have fork.last() in its fork.
    //              OR have fork_len == 0
    let mut buf = BufWriter::new(vec![0u8; 32 + 8 + 8 + 8]);
    // 32 for hash, 8 for view, 8 for last_n, 8 for last_qc_view
    let _ = buf.write(fork_last_hash);
    let _ = buf.write_u64::<BigEndian>(vc.view);
    let _ = buf.write_u64::<BigEndian>(vc.fork_len);
    let _ = buf.write_u64::<BigEndian>(match &vc.fork_last_qc {
        Some(qc) => qc.view,
        None => 0,
    });

    let buf = buf.into_inner().unwrap();
    let sig = keys.sign(&buf);

    vc.fork_sig = sig.to_vec();
}

fn verify_view_change_msg_raw(last_hash: &Vec<u8>, view: u64, fork_len: u64, last_qc_view: u64, sig: &Vec<u8>, keys: &KeyStore, sender: &String) -> bool {
    if sig.len() != SIGNATURE_LENGTH {
        return false;
    }
    
    let mut buf = BufWriter::new(vec![0u8; 32 + 8 + 8 + 8]);
    // 32 for hash, 8 for view, 8 for last_n, 8 for last_qc
    let _ = buf.write(last_hash);
    let _ = buf.write_u64::<BigEndian>(view);
    let _ = buf.write_u64::<BigEndian>(fork_len);
    let _ = buf.write_u64::<BigEndian>(last_qc_view);

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

    let last_qc_view = match &vc.fork_last_qc {
        Some(qc) => qc.view,
        None => 0,
    };

    verify_view_change_msg_raw(&hash_last, vc.view, vc.fork_len, last_qc_view, &vc.fork_sig, keys, sender)
}

pub async fn do_reply_all_with_tentative_receipt(ctx: &PinnedServerContext) {
    let mut lack_pend = ctx.client_ack_pending.lock().await;

    for ((bn, txn), (chan, profile, _sender)) in lack_pend.iter_mut() {
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

    #[cfg(feature = "no_pipeline")]
    {
        if ctx.should_progress.available_permits() == 0 {
            ctx.should_progress.add_permits(1);
        }
    }
}

pub async fn do_init_view_change<Engine>(
    ctx: &PinnedServerContext, engine: &Engine,
    client: &PinnedClient,
    super_majority: u64,
    old_super_majority: u64,
) -> Result<(), Error>
where Engine: crate::execution::Engine
{
    // Stop accepting new client requests, immediately.
    ctx.view_is_stable.store(false, Ordering::SeqCst);
    ctx.i_am_leader.store(false, Ordering::SeqCst);

    // Send tentative replies to all inflight client requests.
    do_reply_all_with_tentative_receipt(ctx).await;

    // Increase view
    let _cfg = ctx.config.get();
    let _keys = ctx.keys.get();

    let view = ctx.state.view.fetch_add(1, Ordering::SeqCst) + 1; // Fetch_add returns the old val
    let leader = get_leader_str(ctx);
    if leader == _cfg.net_config.name {
        // I am the leader
        ctx.i_am_leader.store(true, Ordering::SeqCst);
    }else{
        ctx.i_am_leader.store(false, Ordering::SeqCst);
    }

    warn!("Moved to new view {} with leader {}", view, leader);

    // Send everything from bci onwards.
    // If more of the fork is needed, the next leader will backfill.
    let (fork_from_ci, fork_last, fork_last_qc, fork_last_hash) = {
        let fork = ctx.state.fork.lock().await;
        let bci = ctx.state.byz_commit_index.load(Ordering::SeqCst);
        info!("Sending everything from {} to {}", bci, fork.last());
        (fork.serialize_from_n(bci), fork.last(), fork.get_last_qc(), fork.last_hash())
    };
    // These are all the unconfirmed blocks that the next leader may or may not have.
    // So we must send them otherwise there is a data loss.
    // It doesn't violate any guarantees, but clients will have to repropose.
    

    let mut vc_msg = ProtoViewChange {
        view,
        fork: Some(fork_from_ci),
        fork_sig: vec![0u8; SIGNATURE_LENGTH],
        fork_len: fork_last,
        fork_last_qc: match fork_last_qc {
            Ok(qc) => Some(qc),
            Err(_) => None,
        },
        config_num: ctx.state.config_num.load(Ordering::SeqCst),
    };

    sign_view_change_msg(&mut vc_msg, &_keys, &fork_last_hash);


    if leader == _cfg.net_config.name {
        do_process_view_change(
            ctx.clone(), engine,
            client.clone(),
            &vc_msg,
            &_cfg.net_config.name,
            super_majority,
            old_super_majority,
        ).await;
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
    info!("Send List: {:?}", ctx.send_list.get());
    if let Err(e) =
        PinnedClient::broadcast(client, &ctx.send_list.get(), &bcast_msg, &mut profile).await
    {
        error!("Could not broadcast ViewChange: {}", e);
    }

    let old_full_nodes = ctx.old_full_nodes.get();
    let old_full_nodes = get_everyone_except_me(&_cfg.net_config.name, &old_full_nodes);
    info!("Broadcasting ViewChange to Old Full Nodes: {:?}", old_full_nodes);
    if old_full_nodes.len() > 0 {
        if let Err(e) =
        PinnedClient::broadcast(client, &old_full_nodes, &bcast_msg, &mut profile).await
        {
            error!("Could not broadcast ViewChange to Old Full Nodes: {}", e);
        }
    }


    Ok(())
}

pub async fn do_process_view_change<Engine>(
    ctx: PinnedServerContext, engine: &Engine,
    client: PinnedClient,
    vc: &ProtoViewChange,
    sender: &String,
    super_majority: u64,
    old_super_majority: u64,
) where Engine: crate::execution::Engine
{
    let vc = if vc.config_num < ctx.state.config_num.load(Ordering::SeqCst) {
        warn!("VC from older config({}) [My config = {}]. Rejected.", vc.config_num, ctx.state.config_num.load(Ordering::SeqCst));
        return;
    } else if vc.config_num > ctx.state.config_num.load(Ordering::SeqCst) {
        // Need to fast forward the config.
        info!("Received VC from newer config({}) [My config = {}]. Fast forwarding.", vc.config_num, ctx.state.config_num.load(Ordering::SeqCst));
        &fast_forward_config_from_vc(&ctx, &client, engine, vc, sender).await
    } else {
        vc
    };

    // At this point, I have fast forwarded to have the same config num as the VC.
    if ctx.state.config_num.load(Ordering::SeqCst) != vc.config_num {
        error!("Config mismatch after fast forward. My config = {}, VC config = {}", ctx.state.config_num.load(Ordering::SeqCst), vc.config_num);
        return;
    }

    // Don't process view change messages if I am not a FullNode or OldFullNode in the newest config.
    let lifecycle_stage = ctx.lifecycle_stage.load(Ordering::SeqCst);
    if lifecycle_stage != LifecycleStage::FullNode as i8 && lifecycle_stage != LifecycleStage::OldFullNode as i8 {
        warn!("Not a FullNode or OldFullNode in the newest config. Ignoring view change message.");
        return;
    }

    let _cfg = ctx.config.get();
    let _keys = ctx.keys.get();

    if vc.view < ctx.state.view.load(Ordering::SeqCst)
        || (vc.view == ctx.state.view.load(Ordering::SeqCst)
            && ctx.view_is_stable.load(Ordering::SeqCst))
    {
        return; // View Change for older views.
    }

    if vc.fork.is_none() {
        return;
    }

    info!("Processing view change for view {} from {}", vc.view, sender);

    // Check the signature on the fork.
    if !verify_view_change_msg(vc, &_keys, sender) {
        return;
    }

    if !_cfg.net_config.name
        .eq(&get_leader_str_for_view(&ctx, vc.view))
    {
        // I am not the leader for this message's intended view
        info!("Not the leader for view {}, using this message for pacemaker", vc.view);
    }


    // Check if fork's first block points to a block I already have.
    let vc = if vc.fork_len > 0
    && _cfg.net_config.name.eq(&get_leader_str_for_view(&ctx, vc.view)) {
        // No need to backfill if it is just for pacemaker
        if vc.fork.is_none() || vc.fork.as_ref().unwrap().blocks.len() == 0 {
            return;
        }

        match maybe_backfill_fork_till_prefix_match(ctx.clone(), client.clone(), vc, sender).await {
            Some(vc) => {
                AgnosticRef::from(vc)
            },
            None => {
                return;
            }
        }

    } else {
        AgnosticRef::from(vc)
        
    };


    let mut fork_buf = ctx.state.fork_buffer.lock().await;
    if !fork_buf.contains_key(&vc.view) {
        fork_buf.insert(vc.view, HashMap::new());
    }
    fork_buf
        .get_mut(&vc.view)
        .unwrap()
        .insert(sender.clone(), vc.clone());

    let _cfg = ctx.config.get();
    let _old_full_nodes = ctx.old_full_nodes.get();
    let total_fullnode_forks = fork_buf.get(&vc.view).unwrap()
        .iter().filter(| (sender, _) | {
            _cfg.consensus_config.node_list.contains(&sender)
        }).count();

    let total_oldfullnode_forks = fork_buf.get(&vc.view).unwrap()
    .iter().filter(| (sender, _) | {
        _old_full_nodes.contains(&sender)
    }).count();

    let f_new_plus_one = get_f_plus_one_num(&ctx) as usize;
    let f_old_plus_one = get_old_f_plus_one_num(&ctx) as usize;


    // Pacemaker logic: If I have f + 1 messages, I should initiate my own view change.
    let my_view = ctx.state.view.load(Ordering::SeqCst);
    if vc.view > my_view
    && (total_fullnode_forks >= f_new_plus_one
        || (old_super_majority > 0 && total_oldfullnode_forks >= f_old_plus_one)) 
    && ctx.intended_view.load(Ordering::SeqCst) < vc.view
    {
        // Pacemaker logic: Let's try to increase our bci as much as possible from the QCs in these view change messages.
        let old_bci = ctx.state.byz_commit_index.load(Ordering::SeqCst);
        let mut updated_bci = old_bci;
        let fork = ctx.state.fork.lock().await;
        for (_sender, vc) in fork_buf.get(&vc.view).unwrap() {
            if vc.fork_last_qc.is_some() {
                let qc = vc.fork_last_qc.as_ref().unwrap();
                let mut i = qc.n;
                if i > fork.last() {
                    i = fork.last();
                }

                while i > old_bci {
                    let entry = fork.get(i).unwrap();
                    if entry.block.qc.len() > 0 {
                        // Get the highest qc.n 
                        let mut highest_qc: Option<&ProtoQuorumCertificate> = None;
                        for _qc in &entry.block.qc {
                            if highest_qc.is_none() || _qc.n > highest_qc.as_ref().unwrap().n {
                                highest_qc = Some(_qc);
                            }
                        }

                        let highest_qc = highest_qc.unwrap();
                        if highest_qc.view == qc.view {
                            let __updated_bci = highest_qc.n;
                            if __updated_bci > updated_bci {
                                updated_bci = __updated_bci;
                            }
                            break;
                        }

                    }
                    i -= 1;
                }
            }
        }

        if updated_bci > old_bci {
            info!("Pacemaker: Updating bci from {} to {}", old_bci, updated_bci);
            do_byzantine_commit(&ctx, &client, engine, &fork, updated_bci).await;
        }
        
        if vc.view > ctx.last_stable_view.load(Ordering::SeqCst) {
            if vc.view != my_view + 1 {
                info!("Lagging too far behind or have gone forward. Coming back to view {} from {}", vc.view, ctx.state.view.load(Ordering::SeqCst));
                ctx.state.view.store(vc.view - 1, Ordering::SeqCst);
                ctx.intended_view.store(vc.view - 1, Ordering::SeqCst);
            }
            // This will increment the view and broadcast the view change message
            // But sometime in the future.
            // if vc.view >= my_view + 2 {
            //     ctx.state.view.store(vc.view - 1, Ordering::SeqCst);
            // }
            ctx.view_timer.fire_now().await;
    
            info!("my_view = {}, ctx.intended_view = {}, vc.view = {}", my_view, ctx.intended_view.load(Ordering::SeqCst), vc.view);
    
            // Reset the timer so that we don't fire it again unncecessarily.
            ctx.view_timer.reset();
        }

        // if vc.view >= my_view + 2, we may need to fire the timer again and again until we catch up.  
    }

    if (total_fullnode_forks >= super_majority as usize
        || (old_super_majority > 0 && total_oldfullnode_forks >= old_super_majority as usize))
    && get_leader_str_for_view(&ctx, vc.view).eq(&ctx.config.get().net_config.name)
    {
        // Got 2f + 1 view change messages.
        // Need to update view and take over as leader.

        // Get the set of forks, `F` (as in pseudocode)
        let fork_set = fork_buf.get(&vc.view).unwrap().clone();
        // Delete all buffers up till this view.
        fork_buf.retain(|&n, _| n > vc.view);

        drop(fork_buf);

        do_init_new_leader(ctx, engine, client, vc.view, fork_set, super_majority, old_super_majority).await;
    }
}

pub async fn do_init_new_leader<Engine>(
    ctx: PinnedServerContext, engine: &Engine,
    client: PinnedClient,
    view: u64,
    fork_set: HashMap<String, ProtoViewChange>,
    super_majority: u64,
    old_super_majority: u64,
) where Engine: crate::execution::Engine
{
    let _cfg = ctx.config.get();
    let _keys = ctx.keys.get();

    if ctx.state.view.load(Ordering::SeqCst) > view
        || (ctx.state.view.load(Ordering::SeqCst) == view
            && ctx.view_is_stable.load(Ordering::SeqCst))
        || fork_set.len() < super_majority as usize
        || _cfg.net_config.name != get_leader_str_for_view(&ctx, view)
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
    let (mut chosen_fork, chosen_fork_stat) = fork_choice_rule_get(&ctx, &fork_set, &_cfg.net_config.name);
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
    
    let (last_n, last_hash) = if chosen_fork_stat.name != _cfg.net_config.name {
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
        let mut next_qc_list = ctx.state.next_qc_list.lock().await;
        byz_qc_pending.clear();
        next_qc_list.clear(); // Invariant: qc.view must be same as the view of the block.

        // We'll cause the next block to be 2f + 1 voted anyway.
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
        config_num: ctx.state.config_num.load(Ordering::SeqCst),
        sig: Some(crate::proto::consensus::proto_block::Sig::NoSig(
            DefferedSignature {},
        )),
    };
    if chosen_fork_stat.last_qc_view > 0 && chosen_fork_last_qc.is_some() {
        block.qc = vec![chosen_fork_last_qc.unwrap()];
    }
    profile.register("Block creation done!");
    let entry = LogEntry::new(block);

    match fork.push_and_sign(entry, &_keys) {
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
        view_is_stable: ctx.view_is_stable.load(Ordering::SeqCst),
        config_num: ctx.state.config_num.load(Ordering::SeqCst),
    };
    debug!("AE has signed block? {}", fork.get(fork.last()).unwrap().has_signature());
    profile.register("AE creation done");

    let send_list = ctx.send_list.get();

    let block_n = ae.fork.as_ref().unwrap().blocks.len();
    let block_n = ae.fork.as_ref().unwrap().blocks[block_n - 1].n;

    drop(fork); // create_vote_for_blocks takes lock on fork

    let my_vote = create_vote_for_blocks(ctx.clone(), &vec![block_n])
        .await.unwrap();

    broadcast_append_entries(ctx.clone(), client.clone(), ae, &send_list, profile.clone())
        .await.unwrap();

    profile.register("Broadcast done");
    do_process_vote(
        ctx.clone(), client.clone(), engine,
        &my_vote,
        &_cfg.net_config.name,
        super_majority, // Forcing to wait for 2f + 1
        super_majority,
        old_super_majority,
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

pub async fn force_noop(ctx: &PinnedServerContext) {
    // We need to make sure that the client queue is not blocked
    // due to semaphore wait of the last view.
    #[cfg(feature = "no_pipeline")]
    {
        if ctx.should_progress.available_permits() == 0 {
            ctx.should_progress.add_permits(1);
        }
    }
    let _cfg = ctx.config.get();
    let client_tx = ctx.client_queue.0.clone();
    
    let client_req = ProtoClientRequest {
        tx: Some(ProtoTransaction {
            on_receive: None,
            on_crash_commit: None,
            on_byzantine_commit: None,
            is_reconfiguration: false,
        }),
        // sig: vec![0u8; SIGNATURE_LENGTH],
        sig: vec![0u8; 1],
        origin: _cfg.net_config.name.clone(),
    };

    let request = crate::proto::rpc::proto_payload::Message::ClientRequest(client_req);
    let profile = LatencyProfile::new();

    let _ = client_tx.send((request, _cfg.net_config.name.clone(), ctx.__client_black_hole_channel.0.clone(), profile));

    
}


async fn verify_fast_path_chosen_fork<'a>(
    ctx: &PinnedServerContext,
    f: &ProtoFork,
    my_fork: &'a MutexGuard<'a, Log>
) -> bool {
    // Check if this fork tries to overwrite a byz committed block.
    // If fast path is disabled, this can never happen
    // But with fast path enabled, we can commit something by fast path
    // and then the new leader may try to overwrite it (maliciously.)

    // TODO: Need a more comprehensive check. This is a necessary check but not sufficient.
    // If the old leader did not replicate the fast path QC to sufficient nodes before dying,
    // other (honest) nodes may not know about the commit and happily accept an overwrite.

    let bci = ctx.state.byz_commit_index.load(Ordering::SeqCst);
    if f.blocks.last().is_none() {
        // My fork matches exactly.
        return true;
    }

    if f.blocks.last().as_ref().unwrap().n < bci {
        return false;
    }

    for block in &f.blocks {
        if block.n > bci {
            break;
        }

        // This block MUST match with my fork
        let my_hsh = my_fork.hash_at_n(block.n).unwrap();
        let f_hsh = __hash_block(block);

        if !cmp_hash(&my_hsh, &f_hsh) {
            return false;
        }
    }

    true
}

/// Split the given fork into two sequences: [..last New Leader msg] [Last new leader msg + 1..]
/// Verify all previous NewLeader messages wrt the proposer and fork choice rule.
/// If it is verified, fork.overwrite(ret.0) will not violate GlobalLock()
/// Once overwrite is done, it is safe to push/verify_and_push the blocks in ret.1.
pub async fn maybe_verify_view_change_sequence<'a>(
    ctx: &PinnedServerContext,
    f: &ProtoFork,
    super_majority: u64,
    my_fork: &'a MutexGuard<'a, Log>
) -> Result<(ProtoFork, ProtoFork), Error> {
    let mut split_point = None;
    let _keys = ctx.keys.get();
    
    let mut i = f.blocks.len() as i64 - 1;
    while i >= 0 {
        if !f.blocks[i as usize].view_is_stable {
            // This the signal that it is a New Leader message
            let mut valid_forks = 0;
            let mut max_qc_view_seen = 0;

            let mut subrule1_eligible_forks = HashMap::new();

            for (j, fork_validation) in f.blocks[i as usize].fork_validation.iter().enumerate() {
                // Is this a valid fork?
                // Check the signature on the fork.
                let (last_qc_view, last_qc_n, fork_suffix_after_last_qc_n) = match &fork_validation.fork_last_qc {
                    Some(qc) => {
                        let suffix = ProtoFork::default();
                        (qc.view, qc.n, suffix)
                    },
                    None => (0, 0, ProtoFork::default())
                };
                let chk = verify_view_change_msg_raw(
                    &fork_validation.fork_hash,
                    fork_validation.view,
                    fork_validation.fork_len, last_qc_view,
                    &fork_validation.fork_sig, &_keys,
                    &fork_validation.name
                );
                if !chk {
                    continue;
                }
                valid_forks += 1;
                let fork_stat = ForkStat {
                    last_view: fork_validation.view,
                    last_n: fork_validation.fork_len,
                    last_qc_view, last_qc_n, fork_suffix_after_last_qc_n,
                    name: fork_validation.name.clone(),
                };

                if last_qc_view > max_qc_view_seen {
                    max_qc_view_seen = last_qc_view;
                    subrule1_eligible_forks.clear();
                    subrule1_eligible_forks.insert(j, fork_stat);
                }else if last_qc_view == max_qc_view_seen {
                    subrule1_eligible_forks.insert(j, fork_stat);
                }
            
            }

            if valid_forks < super_majority {
                return Err(Error::new(ErrorKind::InvalidData, "New Leader message with invalid fork information"))
            }

            let mut max_qc_view_selected = 0;
            for b in &f.blocks {
                if b.qc.len() > 0 {
                    for qc in &b.qc {
                        if qc.view > max_qc_view_selected {
                            max_qc_view_selected = qc.view;
                        }
                    }
                }
            }

            // SubRule1
            if max_qc_view_selected < max_qc_view_seen {
                return Err(Error::new(ErrorKind::InvalidData, "New Leader message with invalid max qc; SubRule1 violated"))
            }

            // Subrule fastpath
            #[cfg(feature = "fast_path")]
            if !verify_fast_path_chosen_fork(ctx, f, my_fork).await {
                return Err(Error::new(ErrorKind::InvalidData, "New Leader message with invalid chosen fork; violates fast path"));
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

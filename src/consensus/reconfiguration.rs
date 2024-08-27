// Copyright (c) Shubham Mishra. All rights reserved.
// Licensed under the Apache 2.0 License.

use core::str;
use std::{sync::atomic::Ordering, io::Error, str::FromStr, sync::Arc};

use ed25519_dalek::{pkcs8::DecodePublicKey, VerifyingKey, PUBLIC_KEY_LENGTH};
use log::{error, info, warn};
use nix::sys::signal;
use nix::sys::signal::Signal::SIGINT;
use nix::unistd::Pid;

use crate::{config::{Config, NodeNetInfo}, consensus::utils::get_everyone_except_me, crypto::KeyStore, proto::{consensus::{ProtoAppendEntries, ProtoViewChange}, execution::{ProtoTransaction, ProtoTransactionOp}}, rpc::client::PinnedClient};

use super::{backfill::maybe_backfill_fork_till_last_match, commit::{do_byzantine_commit, maybe_byzantine_commit}, handler::{LifecycleStage, PinnedServerContext}, view_change::do_reply_all_with_tentative_receipt};

/// Gracefully shut down the node
pub async fn do_graceful_shutdown() {
    info!("Attempting graceful shutdown");
    // The following works only on UNIX systems.
    let pid = Pid::this();
    match signal::kill(pid, SIGINT) {
        Ok(_) => {
            info!("Sent SIGINT to self. Shutting down.");
        },
        Err(e) => {
            error!("Failed to send SIGINT to self. Error: {:?}", e);
            std::process::exit(0);
        }
    }
}

fn insert_net_config(new_cfg: &mut Box<Config>, name: &String, info: &NodeNetInfo) {
    new_cfg.net_config.nodes.insert(name.clone(), info.clone());
}

fn insert_learner(new_cfg: &mut Box<Config>, name: &String) {
    new_cfg.consensus_config.learner_list.push(name.clone());
}

fn remove_learner(new_cfg: &mut Box<Config>, name: &String) -> bool {
    let old_len = new_cfg.consensus_config.learner_list.len();
    new_cfg.consensus_config.learner_list.retain(|n| !n.eq(name));
    let new_len = new_cfg.consensus_config.learner_list.len();
    old_len > new_len
}

fn insert_node(new_cfg: &mut Box<Config>, name: &String) {
    new_cfg.consensus_config.node_list.push(name.clone());
}

fn remove_node(new_cfg: &mut Box<Config>, name: &String) -> bool {
    let old_len = new_cfg.consensus_config.node_list.len();
    new_cfg.consensus_config.node_list.retain(|n| !n.eq(name));
    let new_len = new_cfg.consensus_config.node_list.len();
    old_len > new_len
}

fn insert_old_full_node(ctx: &PinnedServerContext, name: &String) {
    let mut __old_full_nodes = ctx.old_full_nodes.get();
    let old_full_nodes = Arc::make_mut(&mut __old_full_nodes);

    old_full_nodes.push(name.clone());

    ctx.old_full_nodes.set(old_full_nodes.clone());
}

fn insert_pub_key(new_keys: &mut Box<KeyStore>, name: &String, pub_key: &VerifyingKey) {
    new_keys.pub_keys.insert(name.clone(), pub_key.clone());
}

#[derive(Clone, Debug)]
pub struct LearnerInfo {
    pub name: String,
    pub info: NodeNetInfo,
    pub pub_key: VerifyingKey,
}

impl FromStr for LearnerInfo {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let parts: Vec<&str> = s.split(" ").collect();
        if parts.len() != 4 {
            return Err("Invalid LearnerInfo".to_string());
        }

        let name = parts[0];
        let addr = parts[1];
        let domain = parts[2];
        let key_pem = String::from("-----BEGIN PUBLIC KEY-----\n")
            + parts[3]
            + "\n-----END PUBLIC KEY-----\n";

        let pub_key =
            VerifyingKey::from_public_key_pem(&key_pem).expect("Invalid PEM format");

        Ok(LearnerInfo {
            name: name.to_string(),
            info: NodeNetInfo {
                addr: addr.to_string(),
                domain: domain.to_string(),
            },
            pub_key,
        })
    }
}

/// Adding a learner == adding net config info + adding to learner list + adding public key
pub fn do_add_learners(ctx: &PinnedServerContext, client: &PinnedClient, learners: &Vec<LearnerInfo>) {
    let mut _cfg = ctx.config.get();
    let new_cfg = Arc::make_mut(&mut _cfg);

    let mut _keys = ctx.keys.get();
    let new_keys = Arc::make_mut(&mut _keys);

    for learner in learners {
        insert_net_config(new_cfg, &learner.name, &learner.info);
        insert_learner(new_cfg, &learner.name);
        insert_pub_key(new_keys, &learner.name, &learner.pub_key);
    }

    ctx.config.set(new_cfg.clone());
    client.0.config.set(new_cfg.clone());
    ctx.keys.set(new_keys.clone());
    client.0.key_store.set(new_keys.clone());

    ctx.__should_server_update_keys.store(true, Ordering::Release);
}

/// Removing a learner == removing from learner list (net config and public key are not removed)
pub fn do_delete_learners(ctx: &PinnedServerContext, names: &Vec<String>) {
    let mut _cfg = ctx.config.get();
    let new_cfg = Arc::make_mut(&mut _cfg);

    for name in names {
        remove_learner(new_cfg, name);
    }

    ctx.config.set(new_cfg.clone());
}

/// Upgrading a learner to a node == removing from learner list + adding to node list
pub fn do_upgrade_learners_to_node(ctx: &PinnedServerContext, names: &Vec<String>) {
    let mut _cfg = ctx.config.get();
    let new_cfg = Arc::make_mut(&mut _cfg);

    for name in names {
        if !remove_learner(new_cfg, name) {
            warn!("Node {} is not a learner", name);
            return;
        }
        insert_node(new_cfg, name);
    }

    ctx.config.set(new_cfg.clone());
}

/// Downgrading a node to a learner == removing from node list + adding to learner list
pub fn do_downgrade_nodes_to_learner(ctx: &PinnedServerContext, names: &Vec<String>) {
    let mut _cfg = ctx.config.get();
    let new_cfg = Arc::make_mut(&mut _cfg);

    for name in names {
        if !remove_node(new_cfg, name) {
            warn!("Node {} is not a node", name);
            return;
        }
        insert_old_full_node(ctx, name);
        insert_learner(new_cfg, name);
    }

    ctx.config.set(new_cfg.clone());
}

/*
 * Node lifecycle: Added as Learner --> Upgraded to Node ---> Downgraded to Learner --> Removed
 * Except the initial nodes, they directly start as a node
*/

fn parse_add_learner(op: &ProtoTransactionOp) -> Result<LearnerInfo, Error> {
    /*
        Op format: [name, addr, domain, pub_key] (4)
    */

    if op.operands.len() != 4 {
        return Err(Error::new(std::io::ErrorKind::InvalidData, "AddLearner: Invalid number of operands"));
    }

    let name = str::from_utf8(&op.operands[0]);
    if let Err(_) = name {
        return Err(Error::new(std::io::ErrorKind::InvalidData, "AddLearner: Invalid name"));
    }

    let addr = str::from_utf8(&op.operands[1]);
    if let Err(_) = addr {
        return Err(Error::new(std::io::ErrorKind::InvalidData, "AddLearner: Invalid addr"));
    }

    let domain = str::from_utf8(&op.operands[2]);
    if let Err(_) = domain {
        return Err(Error::new(std::io::ErrorKind::InvalidData, "AddLearner: Invalid domain"));
    }

    let pub_key: &[u8; PUBLIC_KEY_LENGTH] = match op.operands[3].as_slice().try_into() {
        Ok(key) => key,
        Err(_) => return Err(Error::new(std::io::ErrorKind::InvalidData, "AddLearner: Invalid pub_key")),
    };

    let pub_key = VerifyingKey::from_bytes(pub_key);
    if let Err(_) = pub_key {
        return Err(Error::new(std::io::ErrorKind::InvalidData, "AddLearner: Invalid pub_key"));
    }

    Ok(LearnerInfo {
        name: name.unwrap().to_string(),
        info: NodeNetInfo {
            addr: addr.unwrap().to_string(),
            domain: domain.unwrap().to_string(),
        },
        pub_key: pub_key.unwrap(),
    })
}

pub fn serialize_add_learner(learners: &LearnerInfo) -> ProtoTransactionOp {
    ProtoTransactionOp {
        op_type: crate::proto::execution::ProtoTransactionOpType::AddLearner.into(),
        operands: vec![
            learners.name.as_bytes().to_vec(),
            learners.info.addr.as_bytes().to_vec(),
            learners.info.domain.as_bytes().to_vec(),
            learners.pub_key.to_bytes().to_vec(),
        ],
    }
} 

fn parse_del_learner(op: &ProtoTransactionOp) -> Result<String, Error> {
    /*
        Op format: [name] (1)
    */
    if op.operands.len() != 1 {
        return Err(Error::new(std::io::ErrorKind::InvalidData, "DelLearner: Invalid number of operands"));
    }

    let name = str::from_utf8(&op.operands[0]);
    if let Err(_) = name {
        return Err(Error::new(std::io::ErrorKind::InvalidData, "DelLearner: Invalid name"));
    }

    Ok(name.unwrap().to_string())
}

pub fn serialize_del_learner(name: &String) -> ProtoTransactionOp {
    ProtoTransactionOp {
        op_type: crate::proto::execution::ProtoTransactionOpType::DelLearner.into(),
        operands: vec![name.as_bytes().to_vec()],
    }
}

pub fn serialize_upgrade_fullnode(name: &String) -> ProtoTransactionOp {
    ProtoTransactionOp {
        op_type: crate::proto::execution::ProtoTransactionOpType::UpgradeFullNode.into(),
        operands: vec![name.as_bytes().to_vec()],
    }
}

pub fn serialize_downgrade_fullnode(name: &String) -> ProtoTransactionOp {
    ProtoTransactionOp {
        op_type: crate::proto::execution::ProtoTransactionOpType::DowngradeFullNode.into(),
        operands: vec![name.as_bytes().to_vec()],
    }
}


fn parse_full_node_op(op: &ProtoTransactionOp) -> Result<String, Error> {
    /*
        Op format: [name] (1)
    */
    if op.operands.len() != 1 {
        return Err(Error::new(std::io::ErrorKind::InvalidData, "Full Node op: Invalid number of operands"));
    }

    let name = str::from_utf8(&op.operands[0]);
    if let Err(_) = name {
        return Err(Error::new(std::io::ErrorKind::InvalidData, "Full Node op: Invalid name"));
    }

    Ok(name.unwrap().to_string())
}

fn is_valid_reconfiguration(ctx: &PinnedServerContext, to_upgrade: &Vec<String>, to_downgrade: &Vec<String>, to_remove: &Vec<String>) -> bool {
    // // Only swapping allowed for now.
    // // Conditions: len(to_upgrade) == len(to_downgrade) (ie, net change == 0)
    // // len(new_cfg intersection old_cfg) >= supermajority

    // if to_upgrade.len() != to_downgrade.len() {
    //     return false;
    // }
    // let _cfg = ctx.config.get();
    // let old_node_list = _cfg.consensus_config.node_list.clone();
    // let mut new_node_list = old_node_list.clone();
    // new_node_list.retain(|n| !to_downgrade.contains(n));
    // new_node_list.extend(to_upgrade.clone());

    // let supermajority = crate::consensus::utils::get_super_majority_num(&ctx);

    // let intersection = old_node_list.iter().filter(|n| new_node_list.contains(n)).count() as u64;
    // if intersection < supermajority {
    //     return false;
    // }
    

    // Arbitrary reconfiguration allowed.
    // But upgrade/downgrade must not be mixed with deletion.
    if to_remove.len() > 0 && (to_upgrade.len() > 0 || to_downgrade.len() > 0) {
        return false;
    }

    true
}

/// `on_byz_commit` controls whether to execute the byz_commit phase or the crash_commit phase
/// Returns true if there was some configuration change, ie, upgrade/downgrade.
/// Addition and deletion of learners do not count as configuration change.
/// Otherwise, returns false.
pub fn maybe_execute_reconfiguration_transaction(ctx: &PinnedServerContext, client: &PinnedClient, tx: &ProtoTransaction, on_byz_commit: bool) -> Result<bool, Error>{
    // Sanity check: Cannot be both on_crash_commit and on_byz_commit
    if tx.on_crash_commit.is_some() && tx.on_byzantine_commit.is_some() {
        return Err(Error::new(std::io::ErrorKind::InvalidData, "Cannot be both on_crash_commit and on_byz_commit"));
    }

    let mut ret = false;

    if !on_byz_commit {
        if let Some(phase) = &tx.on_crash_commit {
            let mut to_add = Vec::new();    
            for op in &phase.ops {
                match op.op_type() {
                    crate::proto::execution::ProtoTransactionOpType::AddLearner => {
                        let learner_info = parse_add_learner(&op)?;
                        to_add.push(learner_info);
                    },
                    _ => { /* skip */ },
                }
            }

            do_add_learners(ctx, client, &to_add);
            if to_add.len() > 0 {
                let lifecycle_stage = decide_my_lifecycle_stage(ctx, false);
                info!("Lifecycle stage: {:?}", lifecycle_stage);
                ctx.lifecycle_stage.store(lifecycle_stage as i8, std::sync::atomic::Ordering::SeqCst);
                info!("Learners: {:?},\nKeyStore: {:#?}", ctx.config.get().consensus_config.learner_list, ctx.keys.get().pub_keys);
            }
        }
    } else {
        if let Some(phase) = &tx.on_byzantine_commit {
            let mut to_upgrade = Vec::new();
            let mut to_downgrade = Vec::new();
            let mut to_remove = Vec::new();
    
            for op in &phase.ops {
                match op.op_type() {
                    crate::proto::execution::ProtoTransactionOpType::UpgradeFullNode => {
                        let name = parse_full_node_op(&op)?;
                        to_upgrade.push(name);
                    },
                    crate::proto::execution::ProtoTransactionOpType::DowngradeFullNode => {
                        let name = parse_full_node_op(&op)?;
                        to_downgrade.push(name);
                    },
                    crate::proto::execution::ProtoTransactionOpType::DelLearner => {
                        let name = parse_del_learner(&op)?;
                        to_remove.push(name);
                    },
                    _ => { /* skip */ },
                }
            }
    
            
            if !is_valid_reconfiguration(&ctx, &to_upgrade, &to_downgrade, &to_remove) {
                return Err(Error::new(std::io::ErrorKind::InvalidData, "Invalid reconfiguration"));
            }

            if to_upgrade.len() > 0 || to_downgrade.len() > 0 {
                ret = true;
            }
            
            do_delete_learners(ctx, &to_remove);
            do_upgrade_learners_to_node(ctx, &to_upgrade);
            do_downgrade_nodes_to_learner(ctx, &to_downgrade);

            if ret || to_remove.len() > 0 {
                let lifecycle_stage = decide_my_lifecycle_stage(ctx, false);
                info!("Lifecycle stage: {:?}", lifecycle_stage);
                info!("Config Num: {},\nConfig: {:#?},\nOld Full Nodes: {:?}", ctx.state.config_num.load(Ordering::SeqCst), ctx.config.get(), &ctx.old_full_nodes.get());
                ctx.lifecycle_stage.store(lifecycle_stage as i8, std::sync::atomic::Ordering::SeqCst);

                let _cfg = ctx.config.get();
                ctx.send_list.set(Box::new(get_everyone_except_me(&_cfg.net_config.name, &_cfg.consensus_config.node_list)));
            }
        }
    }

    Ok(ret)
}

pub fn decide_my_lifecycle_stage(ctx: &PinnedServerContext, life_is_starting: bool) -> LifecycleStage {
    let mut lifecycle_stage = LifecycleStage::Dormant;
    let _cfg = ctx.config.get();
    // Am I a learner?
    if _cfg.consensus_config.learner_list.contains(&_cfg.net_config.name) {
        lifecycle_stage = LifecycleStage::Learner;
    }
    
    // Am I a full node?
    if _cfg.consensus_config.node_list.contains(&_cfg.net_config.name) {
        lifecycle_stage = LifecycleStage::FullNode;
    }

    // Am I an old full node?
    if ctx.old_full_nodes.get().contains(&_cfg.net_config.name) {
        lifecycle_stage = LifecycleStage::OldFullNode;
    }

    // I am not in any list. If life is starting, leave it as dormant.
    // Otherwise I am dead.

    if !life_is_starting && lifecycle_stage == LifecycleStage::Dormant {
        lifecycle_stage = LifecycleStage::Dead;
    }

    lifecycle_stage
}


pub async fn reconfiguration_worker(ctx: PinnedServerContext, client: PinnedClient) {
    let mut reconf_rx = ctx.reconf_channel.1.lock().await;
    let mut intended_config_num = 0;
    while let Some(tx) = reconf_rx.recv().await {
        // Only execute if view is stable
        while !ctx.view_is_stable.load(Ordering::SeqCst) {
            // Spin-wait. Isn't supposed to busy wait for long.
        }

        match maybe_execute_reconfiguration_transaction(&ctx, &client, &tx, true) {
            Ok(did_reconf) => {
                info!("Reconfiguration transaction executed successfully!");
                if did_reconf {
                    intended_config_num += 1;

                    // Config changes can happen proactively, ie, I am participating in stabilising the next config.
                    // Or it can happen passively, ie, by learning successful config change happenend in the past.
                    // In the later case, there is no need to trigger a view change.
                    if ctx.state.config_num.load(Ordering::SeqCst) < intended_config_num {
                        ctx.state.config_num.store(intended_config_num, Ordering::SeqCst);
                        // View Change on config update.
                        let my_view = ctx.state.view.load(Ordering::SeqCst);
                        ctx.view_timer.fire_now().await;
                        info!("my_view = {}, ctx.intended_view = {}", my_view, ctx.intended_view.load(Ordering::SeqCst));

                        // Reset the timer so that we don't fire it again unncecessarily.
                        ctx.view_timer.reset();
                
                    }
                }
            }
            Err(e) => {
                warn!("Error executing reconfiguration transaction: {:?}", e);
            }
        }

    }
}


pub async fn fast_forward_config<Engine>(
    ctx: &PinnedServerContext, client: &PinnedClient, engine: &Engine,
    ae: &ProtoAppendEntries, sender: &String
) -> ProtoAppendEntries
where Engine: crate::execution::Engine
{
    if ctx.state.config_num.load(Ordering::SeqCst) >= ae.config_num {
        return ae.clone();
    }
    let curr_config_num = ctx.state.config_num.load(Ordering::SeqCst);
    // This will make sure that executing the reconf tx only changes my config.
    // But doesn't trigger a view change.
    ctx.state.config_num.store(ae.config_num, Ordering::SeqCst);

    // To be on the safe side, let's flush the pipeline.
    do_reply_all_with_tentative_receipt(ctx).await;
    if let None = ae.fork {
        error!("Empty AppendEntries fork");
        return ae.clone();
    }

    let mut fork = ctx.state.fork.lock().await;
    let f = ae.fork.as_ref().unwrap();
    let mut f = maybe_backfill_fork_till_last_match(&ctx, &client, f, &fork, sender).await;

    // Indexes in fork where the config changes.
    let mut config_change_idx = Vec::new();
    let mut last_qc = 0;
    let mut last_qc_view = 0;
    for (idx, block) in f.blocks.iter().enumerate() {
        if !block.view_is_stable // So it is a NewLeader msg
        && block.config_num != curr_config_num {
            config_change_idx.push(idx);
        }

        for qc in &block.qc {
            info!("fast_forward: qc.n = {}", qc.n);
            if qc.n > last_qc {
                last_qc = qc.n;
                last_qc_view = qc.view;
            }
        }
    }

    // @todo: Verify the QC on the NewLeader msgs such that they contain the signatures from both old and new configs.

    // Find the last byz committed entry.
    let old_bci = ctx.state.byz_commit_index.load(Ordering::SeqCst);
    let mut updated_bci = old_bci;
    for block in &f.blocks {
        if block.n <= last_qc {
            // Find QC in this block.

            // @todo: Verify the QCs.
            for qc in &block.qc {
                info!("fast_forward: qc on qc check: qc.n = {}", qc.n);

                if qc.n > updated_bci && qc.view == last_qc_view {
                    updated_bci = qc.n;
                }
            }
        }
    }

    info!("fast_forward: Updated bci: {}", updated_bci);
    // Overwrite till the byz committed entry.
    let mut bci_fork = f.clone();
    bci_fork.blocks.retain(|b| b.n <= updated_bci);
    f.blocks.retain(|b| b.n > updated_bci);

    let overwrite_res = fork.overwrite(&bci_fork);
    if let Err(e) = overwrite_res {
        error!("Error overwriting fork: {:?}", e);
        return ae.clone();
    }

    // Check qc on qc again in the fork after overwriting.
    for n in (updated_bci + 1)..(fork.last() + 1) {
        let block = &fork.get(n).unwrap().block;
        for qc in &block.qc {
            info!("fast_forward: qc on qc check: qc.n = {}", qc.n);

            if qc.n > updated_bci && qc.view == last_qc_view {
                updated_bci = qc.n;
            }
        }
    }

    if updated_bci <= old_bci {
        return ae.clone();
    }

    // Now byzantine commit everything till the last byz committed entry.
    // This hopefully bring us to the newest config.
    let mut lack_pend = ctx.client_ack_pending.lock().await;
    do_byzantine_commit(ctx, client, engine, &fork, updated_bci, &mut lack_pend);

    let len = f.blocks.len();
    ProtoAppendEntries {
        fork: Some(f),
        commit_index: ctx.state.commit_index.load(Ordering::SeqCst),
        view: ae.view,
        view_is_stable: ae.view_is_stable,
        config_num: ae.config_num,
    }
}

pub async fn fast_forward_config_from_vc<Engine>(
    ctx: &PinnedServerContext, client: &PinnedClient, engine: &Engine,
    vc: &ProtoViewChange, sender: &String
) -> ProtoViewChange
where Engine: crate::execution::Engine
{
    if ctx.state.config_num.load(Ordering::SeqCst) >= vc.config_num {
        return vc.clone();
    }
    let curr_config_num = ctx.state.config_num.load(Ordering::SeqCst);

    // To be on the safe side, let's flush the pipeline.
    do_reply_all_with_tentative_receipt(ctx).await;
    if let None = vc.fork {
        error!("Empty AppendEntries fork");
        return vc.clone();
    }

    let mut fork = ctx.state.fork.lock().await;
    let f = vc.fork.as_ref().unwrap();
    let mut f = maybe_backfill_fork_till_last_match(&ctx, &client, f, &fork, sender).await;

    // Indexes in fork where the config changes.
    let mut config_change_idx = Vec::new();
    let mut last_qc = 0;
    let mut last_qc_view = 0;
    for (idx, block) in f.blocks.iter().enumerate() {
        if !block.view_is_stable // So it is a NewLeader msg
        && block.config_num != curr_config_num {
            config_change_idx.push(idx);
        }

        for qc in &block.qc {
            info!("fast_forward: qc.n = {}", qc.n);
            if qc.n > last_qc {
                last_qc = qc.n;
                last_qc_view = qc.view;
            }
        }
    }

    // @todo: Verify the QC on the NewLeader msgs such that they contain the signatures from both old and new configs.

    // Find the last byz committed entry.
    let old_bci = ctx.state.byz_commit_index.load(Ordering::SeqCst);
    let mut updated_bci = old_bci;
    for block in &f.blocks {
        if block.n <= last_qc {
            // Find QC in this block.

            // @todo: Verify the QCs.
            for qc in &block.qc {
                info!("fast_forward: qc on qc check: qc.n = {}", qc.n);

                if qc.n > updated_bci && qc.view == last_qc_view {
                    updated_bci = qc.n;
                }
            }
        }
    }

    info!("fast_forward: Updated bci: {}", updated_bci);
    // Overwrite till the byz committed entry.
    let mut bci_fork = f.clone();
    bci_fork.blocks.retain(|b| b.n <= updated_bci);
    f.blocks.retain(|b| b.n > updated_bci);

    let overwrite_res = fork.overwrite(&bci_fork);
    if let Err(e) = overwrite_res {
        error!("Error overwriting fork: {:?}", e);
        return vc.clone();
    }

    // Check qc on qc again in the fork after overwriting.
    for n in (updated_bci + 1)..(fork.last() + 1) {
        let block = &fork.get(n).unwrap().block;
        for qc in &block.qc {
            info!("fast_forward: qc on qc check: qc.n = {}", qc.n);

            if qc.n > updated_bci && qc.view == last_qc_view {
                updated_bci = qc.n;
            }
        }
    }

    if updated_bci <= old_bci {
        return vc.clone();
    }

    // Now byzantine commit everything till the last byz committed entry.
    // This hopefully bring us to the newest config.
    let mut lack_pend = ctx.client_ack_pending.lock().await;
    do_byzantine_commit(ctx, client, engine, &fork, updated_bci, &mut lack_pend);

    let len = f.blocks.len();
    ProtoViewChange {
        view: vc.view,
        fork: Some(f),
        fork_sig: vc.fork_sig.clone(),
        fork_len: len as u64,
        fork_last_qc: vc.fork_last_qc.clone(),
        config_num: vc.config_num,
    }
}
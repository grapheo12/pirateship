use core::str;
use std::{io::Error, sync::Arc};

use ed25519_dalek::{VerifyingKey, PUBLIC_KEY_LENGTH};
use log::{error, info, warn};
use nix::sys::signal;
use nix::sys::signal::Signal::SIGINT;
use nix::unistd::Pid;

use crate::{config::{Config, NodeNetInfo}, crypto::KeyStore, proto::execution::{ProtoTransaction, ProtoTransactionOp}};

use super::handler::{LifecycleStage, PinnedServerContext};

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

fn insert_pub_key(new_keys: &mut Box<KeyStore>, name: &String, pub_key: &VerifyingKey) {
    new_keys.pub_keys.insert(name.clone(), pub_key.clone());
}

pub struct LearnerInfo {
    pub name: String,
    pub info: NodeNetInfo,
    pub pub_key: VerifyingKey,
}

/// Adding a learner == adding net config info + adding to learner list + adding public key
pub fn do_add_learners(ctx: &PinnedServerContext, learners: &Vec<LearnerInfo>) {
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
    ctx.keys.set(new_keys.clone());
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

fn is_valid_reconfiguration(ctx: &PinnedServerContext, to_upgrade: &Vec<String>, to_downgrade: &Vec<String>) -> bool {
    // Only swapping allowed for now.
    // Conditions: len(to_upgrade) == len(to_downgrade) (ie, net change == 0)
    // len(new_cfg intersection old_cfg) >= supermajority

    if to_upgrade.len() != to_downgrade.len() {
        return false;
    }
    let _cfg = ctx.config.get();
    let old_node_list = _cfg.consensus_config.node_list.clone();
    let mut new_node_list = old_node_list.clone();
    new_node_list.retain(|n| !to_downgrade.contains(n));
    new_node_list.extend(to_upgrade.clone());

    let supermajority = crate::consensus::utils::get_super_majority_num(&ctx);

    let intersection = old_node_list.iter().filter(|n| new_node_list.contains(n)).count() as u64;
    if intersection < supermajority {
        return false;
    }
    
    true
}

/// `on_byz_commit` controls whether to execute the byz_commit phase or the crash_commit phase
/// Returns true if there was some configuration change.#
/// Otherwise, returns false.
pub fn maybe_execute_reconfiguration_transaction(ctx: &PinnedServerContext, tx: &ProtoTransaction, on_byz_commit: bool) -> Result<bool, Error>{
    // Sanity check: Cannot be both on_crash_commit and on_byz_commit
    if tx.on_crash_commit.is_some() && tx.on_byzantine_commit.is_some() {
        return Err(Error::new(std::io::ErrorKind::InvalidData, "Cannot be both on_crash_commit and on_byz_commit"));
    }

    let mut ret = false;

    if !on_byz_commit {
        if let Some(phase) = &tx.on_crash_commit {
            let mut to_add = Vec::new();
            let mut to_remove = Vec::new();
    
            for op in &phase.ops {
                match op.op_type() {
                    crate::proto::execution::ProtoTransactionOpType::AddLearner => {
                        let learner_info = parse_add_learner(&op)?;
                        to_add.push(learner_info);
                    },
                    crate::proto::execution::ProtoTransactionOpType::DelLearner => {
                        let name = parse_del_learner(&op)?;
                        to_remove.push(name);
                    },
                    _ => { /* skip */ },
                }
            }

            if to_add.len() > 0 || to_remove.len() > 0 {
                ret = true;
            }
    
            do_add_learners(ctx, &to_add);
            do_delete_learners(ctx, &to_remove);

            if ret {
                let lifecycle_stage = decide_my_lifecycle_stage(ctx, false);
                info!("Lifecycle stage: {:?}", lifecycle_stage);
                ctx.lifecycle_stage.store(lifecycle_stage as i8, std::sync::atomic::Ordering::SeqCst);
            }
        }
    } else {

        if let Some(phase) = &tx.on_byzantine_commit {
            let mut to_upgrade = Vec::new();
            let mut to_downgrade = Vec::new();
    
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
                    _ => { /* skip */ },
                }
            }
    
            
            if !is_valid_reconfiguration(&ctx, &to_upgrade, &to_downgrade) {
                return Err(Error::new(std::io::ErrorKind::InvalidData, "Invalid reconfiguration"));
            }

            if to_upgrade.len() > 0 || to_downgrade.len() > 0 {
                ret = true;
            }

            do_upgrade_learners_to_node(ctx, &to_upgrade);
            do_downgrade_nodes_to_learner(ctx, &to_downgrade);

            if ret {
                let lifecycle_stage = decide_my_lifecycle_stage(ctx, false);
                info!("Lifecycle stage: {:?}", lifecycle_stage);
                ctx.lifecycle_stage.store(lifecycle_stage as i8, std::sync::atomic::Ordering::SeqCst);
            }
        }
    }



    Ok(ret)
}

pub fn decide_my_lifecycle_stage(ctx: &PinnedServerContext, life_is_starting: bool) -> LifecycleStage {
    let mut lifecycle_stage = LifecycleStage::Dormant;
    let _cfg = ctx.config.get();
    // Am I a learner in the initial config.
    if _cfg.consensus_config.learner_list.contains(&_cfg.net_config.name) {
        lifecycle_stage = LifecycleStage::Learner;
    }
    
    // Am I a full node in the initial config.
    if _cfg.consensus_config.node_list.contains(&_cfg.net_config.name) {
        lifecycle_stage = LifecycleStage::FullNode;
    }

    if !life_is_starting && lifecycle_stage == LifecycleStage::Dormant {
        lifecycle_stage = LifecycleStage::Dead;
    }

    lifecycle_stage
}
// Copyright (c) Shubham Mishra. All rights reserved.
// Licensed under the MIT License.

use hex::ToHex;
use prost::Message;
use rand_chacha::ChaCha20Rng;
use std::sync::atomic::Ordering;

use crate::{
    config::ClientConfig, consensus::{
        handler::PinnedServerContext,
        leader_rotation::get_current_leader,
    }, crypto::hash, proto::{
        consensus::{
            ProtoFork, ProtoQuorumCertificate,
        }, execution::ProtoTransaction
    }
};


pub fn get_leader_str(ctx: &PinnedServerContext) -> String {
    get_leader_str_for_view(ctx, ctx.state.view.load(Ordering::SeqCst))
}

pub fn get_leader_str_for_view(ctx: &PinnedServerContext, view: u64) -> String {
    let _cfg = ctx.config.get();
    _cfg.consensus_config.node_list
        [get_current_leader(_cfg.consensus_config.node_list.len() as u64, view)]
    .clone()
}

pub fn get_node_num(ctx: &PinnedServerContext) -> u64 {
    let _cfg = ctx.config.get();
    let mut i = 0;
    for name in &_cfg.consensus_config.node_list {
        if name.eq(&_cfg.net_config.name) {
            return i;
        }
        i += 1;
    }

    0
}

pub fn get_majority_num(ctx: &PinnedServerContext) -> u64 {
    let _cfg = ctx.config.get();
    let n = _cfg.consensus_config.node_list.len() as u64;
    n / 2 + 1
}

pub fn get_super_majority_num(ctx: &PinnedServerContext) -> u64 {
    let _cfg = ctx.config.get();
    let n = _cfg.consensus_config.node_list.len() as u64;
    n - (n / 3)
}

pub fn get_old_super_majority_num(ctx: &PinnedServerContext) -> u64 {
    let old_full_nodes = ctx.old_full_nodes.get();
    let n = old_full_nodes.len() as u64;
    n - (n / 3)
}

pub fn get_f_plus_one_num(ctx: &PinnedServerContext) -> u64 {
    let _cfg = ctx.config.get();
    let n = _cfg.consensus_config.node_list.len() as u64;
    (n / 3) + 1
}

pub fn get_f_plus_one_send_list(config: &ClientConfig, rng: &mut ChaCha20Rng) -> Vec<String> {
    let n = config.net_config.nodes.len();
    let f_plus_one = (n / 3) + 1;

    let mut rand_f_plus_one = Vec::new();
    let rand_idx = rand::seq::index::sample(rng, config.net_config.nodes.len(), f_plus_one)
        .into_vec();
    let mut i = 0;

    config.net_config.nodes.iter().for_each(|(k, _)| {
        if rand_idx.contains(&i) {
            rand_f_plus_one.push(k.clone());
        }
        i += 1;
    });

    rand_f_plus_one
}

pub fn get_old_f_plus_one_num(ctx: &PinnedServerContext) -> u64 {
    let old_full_nodes = ctx.old_full_nodes.get();
    let n = old_full_nodes.len() as u64;
    (n / 3) + 1
}

pub fn get_everyone_except_me(my_name: &String, node_list: &Vec<String>) -> Vec<String> {
    node_list
        .iter()
        .map(|n| n.clone())
        .filter(|name| !name.eq(my_name))
        .collect()
}


pub fn __hash_tx_list(tx: &Vec<ProtoTransaction>) -> Vec<u8> {
    let mut buf = Vec::new();
    for t in tx {
        buf.extend(t.encode_to_vec());
    }
    hash(&buf)
}

pub fn __hash_qc_list(qc: &Vec<ProtoQuorumCertificate>) -> Vec<u8> {
    let mut buf = Vec::new();
    for q in qc {
        buf.extend(q.encode_to_vec());
    }
    hash(&buf)
}

pub fn __display_protofork(f: &ProtoFork) -> String {
    let mut s = String::from("ProtoFork { blocks: [ ");
    for b in &f.blocks {
        let _s = format!("ProtoBlock {{ View: {} n: {} Tx: {} QC: {} }},",
            b.view, b.n, __hash_tx_list(&b.tx).encode_hex::<String>(), __hash_qc_list(&b.qc).encode_hex::<String>());

        s += &_s;
    }
    s += " ] }";

    s

}
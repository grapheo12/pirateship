use hex::ToHex;
use prost::Message;
use std::sync::atomic::Ordering;

use crate::{
    consensus::{
        handler::PinnedServerContext,
        leader_rotation::get_current_leader,
    }, crypto::hash,
    proto::{
        consensus::{
            ProtoFork, ProtoQuorumCertificate,
        }, execution::ProtoTransaction
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

pub fn get_node_num(ctx: &PinnedServerContext) -> u64 {
    let mut i = 0;
    for name in &ctx.config.consensus_config.node_list {
        if name.eq(&ctx.config.net_config.name) {
            return i;
        }
        i += 1;
    }

    0
}

pub fn get_majority_num(ctx: &PinnedServerContext) -> u64 {
    let n = ctx.config.consensus_config.node_list.len() as u64;
    n / 2 + 1
}

pub fn get_super_majority_num(ctx: &PinnedServerContext) -> u64 {
    let n = ctx.config.consensus_config.node_list.len() as u64;
    2 * (n / 3) + 1
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
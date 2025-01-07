// Copyright (c) Shubham Mishra. All rights reserved.
// Licensed under the MIT License.

use std::sync::atomic::Ordering;

use gluesql::core::sqlparser::keywords::PROGRAM;
use log::{error, info, trace, warn};
use prost::Message;
use tokio::{fs::read, sync::MutexGuard};

use crate::{
    config::NodeInfo, consensus::handler::{ForwardedMessageWithAckChan, PinnedServerContext}, crypto::{hash, DIGEST_LENGTH}, get_tx_list, proto::{client::{
            proto_client_reply::Reply, ProtoByzResponse, ProtoClientReply, ProtoCurrentLeader, ProtoTransactionReceipt, ProtoTryAgain
        }, execution::ProtoTransactionResult}, rpc::PinnedMessage
};

use crate::consensus::utils::*;

use super::log::Log;

pub async fn bulk_reply_to_client(reqs: &Vec<ForwardedMessageWithAckChan>, reply: Option<Reply>) {
    for (req, _, chan, profile) in reqs {
        if let crate::proto::rpc::proto_payload::Message::ClientRequest(req) = req {
            let client_tag = req.client_tag;
            let reply_body = ProtoClientReply {
                reply: reply.clone(),
                client_tag
            };

            let mut buf = Vec::new();
            reply_body.encode(&mut buf).unwrap();
            let sz = buf.len();
            let msg = PinnedMessage::from(buf, sz, crate::rpc::SenderType::Anon);

            
            let _ = chan.send((msg.clone(), profile.clone())).await;
        }
    }
}

pub async fn do_respond_with_try_again(reqs: &Vec<ForwardedMessageWithAckChan>, node_infos: NodeInfo) {
    let reply = crate::proto::client::proto_client_reply::Reply::TryAgain(ProtoTryAgain {
        serialized_node_infos: node_infos.serialize(),
    });
    // let try_again = ProtoClientReply {
    //     reply: Some(
    //         crate::proto::client::proto_client_reply::Reply::TryAgain(ProtoTryAgain {
    //             serialized_node_infos: node_infos.serialize(),
    //         }),
    //     ),
    // };

    // let mut buf = Vec::new();
    // try_again.encode(&mut buf).unwrap();
    // let sz = buf.len();
    // let msg = PinnedMessage::from(buf, sz, crate::rpc::SenderType::Anon);

    bulk_reply_to_client(reqs, Some(reply)).await;
}

pub async fn do_respond_with_current_leader(
    ctx: &PinnedServerContext,
    reqs: &Vec<ForwardedMessageWithAckChan>,
) {
    let node_infos = NodeInfo {
        nodes: ctx.config.get().net_config.nodes.clone(),
    };
    let leader = crate::proto::client::proto_client_reply::Reply::Leader(
        ProtoCurrentLeader {
            name: get_leader_str(ctx),
            serialized_node_infos: node_infos.serialize(),
        },
    );
    // let leader = ProtoClientReply {
    //     reply: Some(crate::proto::client::proto_client_reply::Reply::Leader(
    //         ProtoCurrentLeader {
    //             name: get_leader_str(ctx),
    //             serialized_node_infos: node_infos.serialize(),
    //         },
    //     )),
    // };

    // let mut buf = Vec::new();
    // leader.encode(&mut buf).unwrap();
    // let sz = buf.len();
    // let msg = PinnedMessage::from(buf, sz, crate::rpc::SenderType::Anon);

    bulk_reply_to_client(&reqs, Some(leader)).await;
}

pub async fn do_respond_to_read_requests<Engine>(
    _ctx: &PinnedServerContext,
    engine: &Engine,
    reqs: &mut Vec<ForwardedMessageWithAckChan>
) where 
    Engine: crate::execution::Engine + Clone + Send + Sync + 'static
{
    let mut outgoing = Vec::new();
    reqs.retain(|req| {
        match &req.0 {
            crate::proto::rpc::proto_payload::Message::ClientRequest(proto_client_request) => {
                if proto_client_request.tx.is_some() {
                    let on_recv = &proto_client_request.tx.as_ref().unwrap().on_receive;
                    if on_recv.is_none() {
                        // Not a read request.
                        return true;
                    }

                    // Read request.
                    // Execute and ...
                    let read_req = on_recv.clone().unwrap();
                    let result = engine.get_unlogged_execution_result(read_req);
                    let receipt = ProtoClientReply {
                        reply: Some(
                            crate::proto::client::proto_client_reply::Reply::Receipt(
                                ProtoTransactionReceipt {
                                    req_digest: vec![0u8; DIGEST_LENGTH],
                                    block_n: 0,
                                    tx_n: 0,
                                    results: Some(result),
                                    await_byz_response: false,
                                    byz_responses: vec![]
                                },
                        )),
                        client_tag: proto_client_request.client_tag
                    };

                    // ... reply back to client.
                    let v = receipt.encode_to_vec();
                    let vlen = v.len();
        
                    let msg = PinnedMessage::from(v, vlen, crate::rpc::SenderType::Anon);
                    
                    outgoing.push((req.2.clone(), msg, req.3.clone()));
                    // let _ = req.2.send((msg, req.3.clone())).await;
        
                    // Do not include this request for creating a block
                    return false;
                }

                return true;
            },
            _ => {
                return false;
            }
        }
    });

    for (chan, msg, profile) in outgoing.drain(..) {
        let _ = chan.send((msg, profile)).await;
    }
}

pub async fn do_reply_transaction_receipt<'a, F>(
    ctx: &PinnedServerContext,
    fork: &'a MutexGuard<'a, Log>,
    n: u64,     // ci
    result_getter: F
) where F: Fn(u64 /* bn */, usize /* txn */) -> ProtoTransactionResult
{
    let mut lack_pend = ctx.client_ack_pending.lock().await;
    let mut outgoing = Vec::new();
    lack_pend.retain(|(bn, txn), chan| {
        if *bn > n {
            return true;
        }
        let client_tag = chan.3;
        trace!("Replying tx receipt for {}, gc_hiwm {}", *bn, fork.gc_hiwm());
        let entry = fork.get(*bn).unwrap();
        let response = if get_tx_list!(entry.block).len() <= *txn {
            if ctx.i_am_leader.load(Ordering::SeqCst) {
                warn!("Missing transaction as a leader!");
            }
            if entry.block.view_is_stable {
                warn!("Missing transaction in stable view!");
            }

            let node_infos = NodeInfo {
                nodes: ctx.config.get().net_config.nodes.clone(),
            };

            ProtoClientReply {
                reply: Some(
                    crate::proto::client::proto_client_reply::Reply::TryAgain(
                        ProtoTryAgain{ serialized_node_infos: node_infos.serialize() }
                )),
                client_tag
            }
        }else {
            let h = hash(&get_tx_list!(entry.block)[*txn].encode_to_vec());
            let byz_responses = get_all_byz_responses(ctx, &chan.2);
            let await_byz_response = should_await_byz_response(*bn, *txn);
            if await_byz_response {
                register_tx_with_client(ctx, &chan.2, *bn, *txn);
            }
            ProtoClientReply {
                reply: Some(
                    crate::proto::client::proto_client_reply::Reply::Receipt(
                        ProtoTransactionReceipt {
                            req_digest: h,
                            block_n: (*bn) as u64,
                            tx_n: (*txn) as u64,
                            results: Some(result_getter(*bn, *txn)),
                            await_byz_response,
                            byz_responses,
                        },
                )),
                client_tag
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
        outgoing.push((chan.clone(), msg, profile));
        false
        // let send_res = chan.0.send((msg, profile));
        // match send_res {
        //     Ok(_) => false,
        //     Err(e) => {
        //         error!("Error sending response: {}", e);
        //         true
        //     },
        // }
    });
    for (chan, msg, profile) in outgoing.drain(..) {
        chan.0.send((msg, profile)).await;
    }
}

pub fn register_byz_response(ctx: &PinnedServerContext, client_name: &String, resp: ProtoByzResponse) {
    let mut q = ctx.client_byz_ack_pending.lock().unwrap();
    if !q.contains_key(client_name) {
        q.insert(client_name.clone(), Vec::new());
    }
    q.get_mut(client_name).unwrap().push(resp);
}

pub fn get_all_byz_responses(ctx: &PinnedServerContext, client_name: &String) -> Vec<ProtoByzResponse> {
    let mut q = ctx.client_byz_ack_pending.lock().unwrap();
    if !q.contains_key(client_name) {
        return vec![];
    }
    let ret = q.get(client_name).unwrap().clone();
    q.get_mut(client_name).unwrap().clear();

    ret
}

pub fn should_await_byz_response(bn: u64, txn: usize) -> bool {
    bn > 0 && ((bn % 173 == 1 && txn == 0) || (bn % 229 == 5 && txn == 400))
}

pub fn register_tx_with_client(ctx: &PinnedServerContext, client_name: &String, bn: u64, txn: usize) {
    if !ctx.i_am_leader.load(Ordering::SeqCst) {
        return;
    }

    let mut m = ctx.client_tx_map.lock().unwrap();
    m.insert((bn, txn), client_name.clone());
}

pub fn pop_client_for_tx(ctx: &PinnedServerContext, bn: u64, txn: usize) -> Option<String> {
    if !ctx.i_am_leader.load(Ordering::SeqCst) {
        return None;
    }
    let mut m = ctx.client_tx_map.lock().unwrap();
    m.remove(&(bn, txn))
}

pub fn bulk_register_byz_response(ctx: &PinnedServerContext, updated_bci: u64, fork: &MutexGuard<Log>) {
    if !ctx.i_am_leader.load(Ordering::SeqCst) {
        return;
    }


    let old_bci = ctx.client_replied_bci.load(Ordering::SeqCst);
    if old_bci >= updated_bci {
        return;
    }

    for bn in (old_bci + 1)..(updated_bci + 1) {
        trace!("Registering byz reply for {}, gc_hiwm {}", bn, fork.gc_hiwm());
        let entry = &fork.get(bn).unwrap();
        for txn in 0..get_tx_list!(entry.block).len() {
            let client_name = pop_client_for_tx(ctx, bn, txn);
            if let None = client_name {
                continue;
            }
            let client_name = client_name.unwrap();
            register_byz_response(ctx, &client_name, ProtoByzResponse {
                block_n: bn,
                tx_n: txn as u64,
            });
        }
    }

    ctx.client_replied_bci.store(updated_bci, Ordering::SeqCst);
}
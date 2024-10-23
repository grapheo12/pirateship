// Copyright (c) Shubham Mishra. All rights reserved.
// Licensed under the MIT License.

use std::sync::atomic::Ordering;

use log::{error, info, warn};
use prost::Message;
use tokio::{fs::read, sync::MutexGuard};

use crate::{
    config::NodeInfo, consensus::handler::{ForwardedMessageWithAckChan, PinnedServerContext}, crypto::{hash, DIGEST_LENGTH}, proto::{client::{
            ProtoClientReply, ProtoCurrentLeader, ProtoTransactionReceipt, ProtoTryAgain
        }, execution::ProtoTransactionResult}, rpc::PinnedMessage
};

use crate::consensus::utils::*;

use super::log::Log;

pub async fn bulk_reply_to_client(reqs: &Vec<ForwardedMessageWithAckChan>, msg: PinnedMessage) {
    for (_, _, chan, profile) in reqs {
        chan.send((msg.clone(), profile.clone())).unwrap();
    }
}

pub async fn do_respond_with_try_again(reqs: &Vec<ForwardedMessageWithAckChan>, node_infos: NodeInfo) {
    let try_again = ProtoClientReply {
        reply: Some(
            crate::proto::client::proto_client_reply::Reply::TryAgain(ProtoTryAgain {
                serialized_node_infos: node_infos.serialize(),
            }),
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
    let node_infos = NodeInfo {
        nodes: ctx.config.get().net_config.nodes.clone(),
    };
    let leader = ProtoClientReply {
        reply: Some(crate::proto::client::proto_client_reply::Reply::Leader(
            ProtoCurrentLeader {
                name: get_leader_str(ctx),
                serialized_node_infos: node_infos.serialize(),
            },
        )),
    };

    let mut buf = Vec::new();
    leader.encode(&mut buf).unwrap();
    let sz = buf.len();
    let msg = PinnedMessage::from(buf, sz, crate::rpc::SenderType::Anon);

    bulk_reply_to_client(reqs, msg).await;
}

pub async fn do_respond_to_read_requests<Engine>(
    _ctx: &PinnedServerContext,
    engine: &Engine,
    reqs: &mut Vec<ForwardedMessageWithAckChan>
) where 
    Engine: crate::execution::Engine + Clone + Send + Sync + 'static
{
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
                                },
                        )),
                    };

                    // ... reply back to client.
                    let v = receipt.encode_to_vec();
                    let vlen = v.len();
        
                    let msg = PinnedMessage::from(v, vlen, crate::rpc::SenderType::Anon);
                    let _ = req.2.send((msg, req.3.clone()));
        
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
}

pub async fn do_reply_transaction_receipt<'a, F>(
    ctx: &PinnedServerContext,
    fork: &'a MutexGuard<'a, Log>,
    n: u64,     // ci
    result_getter: F
) where F: Fn(u64 /* bn */, usize /* txn */) -> ProtoTransactionResult
{
    let mut lack_pend = ctx.client_ack_pending.lock().await;
    lack_pend.retain(|(bn, txn), chan| {
        if *bn > n {
            return true;
        }

        let entry = fork.get(*bn).unwrap();
        let response = if entry.block.tx.len() <= *txn {
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
                            results: Some(result_getter(*bn, *txn)),
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
        let send_res = chan.0.send((msg, profile));
        match send_res {
            Ok(_) => false,
            Err(e) => {
                error!("Error sending response: {}", e);
                true
            },
        }
    });
}

pub async fn do_reply_byz_poll<'a, F>(
    ctx: &PinnedServerContext,
    fork: &'a MutexGuard<'a, Log>,
    n: u64,     // bci
    result_getter: F
) where F: Fn(u64 /* bn */, usize /* txn */) -> ProtoTransactionResult
{
    let mut lbyz_ackpend = ctx.client_byz_ack_pending.lock().await;
    lbyz_ackpend.retain(|(bn, txn), chan| {
        if *bn > n {
            return true;
        }

        let entry = fork.get(*bn).unwrap();
        let h = hash(&entry.block.tx[*txn].encode_to_vec());
        let response = ProtoClientReply {
            reply: Some(
                crate::proto::client::proto_client_reply::Reply::Receipt(
                    ProtoTransactionReceipt {
                        req_digest: h,
                        block_n: (*bn) as u64,
                        tx_n: (*txn) as u64,
                        results: Some(result_getter(*bn, *txn)),
                    },
            )),
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
        let send_res = chan.0.send((msg, profile));
        match send_res {
            Ok(_) => false,
            Err(e) => {
                error!("Error sending response: {}", e);
                true
            },
        }
    });
}
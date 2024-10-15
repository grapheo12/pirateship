// Copyright (c) Shubham Mishra. All rights reserved.
// Licensed under the MIT License.

use prost::Message;
use tokio::fs::read;

use crate::{
    config::NodeInfo, consensus::handler::{ForwardedMessageWithAckChan, PinnedServerContext}, crypto::DIGEST_LENGTH, proto::client::{
            ProtoClientReply, ProtoCurrentLeader, ProtoTransactionReceipt, ProtoTryAgain
        }, rpc::PinnedMessage
};

use crate::consensus::utils::*;

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
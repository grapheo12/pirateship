// Copyright (c) Shubham Mishra. All rights reserved.
// Licensed under the MIT License.

use prost::Message;

use crate::{
    config::NodeInfo, consensus::handler::{ForwardedMessageWithAckChan, PinnedServerContext}, proto::client::{
            ProtoClientReply, ProtoCurrentLeader, ProtoTryAgain
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


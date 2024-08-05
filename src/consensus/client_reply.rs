use prost::Message;

use crate::{
    consensus::handler::{ForwardedMessageWithAckChan, PinnedServerContext}, proto::client::{
            ProtoClientReply, ProtoCurrentLeader, ProtoTryAgain
        }, rpc::PinnedMessage
};

use crate::consensus::utils::*;

pub async fn bulk_reply_to_client(reqs: &Vec<ForwardedMessageWithAckChan>, msg: PinnedMessage) {
    for (_, _, chan, profile) in reqs {
        chan.send((msg.clone(), profile.clone())).unwrap();
    }
}

pub async fn do_respond_with_try_again(reqs: &Vec<ForwardedMessageWithAckChan>) {
    let try_again = ProtoClientReply {
        reply: Some(
            crate::proto::client::proto_client_reply::Reply::TryAgain(ProtoTryAgain {}),
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
    let leader = ProtoClientReply {
        reply: Some(crate::proto::client::proto_client_reply::Reply::Leader(
            ProtoCurrentLeader {
                name: get_leader_str(ctx),
            },
        )),
    };

    let mut buf = Vec::new();
    leader.encode(&mut buf).unwrap();
    let sz = buf.len();
    let msg = PinnedMessage::from(buf, sz, crate::rpc::SenderType::Anon);

    bulk_reply_to_client(reqs, msg).await;
}


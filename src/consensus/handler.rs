use std::{
    collections::{HashMap, HashSet}, io::{Error, ErrorKind}, ops::Deref, pin::Pin, sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc,
    }
};

use hex::ToHex;
use log::{debug, info, warn};
use prost::Message;
use tokio::sync::{mpsc, Mutex};

use crate::{config::Config, crypto::DIGEST_LENGTH, rpc::{MessageRef, PinnedMessage}};

use super::{
    leader_rotation::get_current_leader,
    log::Log,
    proto::{
        consensus::{ProtoBlock, ProtoQuorumCertificate, ProtoVote},
        rpc::{self, ProtoPayload},
    },
};

/// @todo: This doesn't have to be here. Unncessary Mutexes.
/// This can be private to the protocols. More flexibility that way.
#[derive(Debug)]
pub struct ConsensusState {
    pub fork: Mutex<Log>,
    pub view: AtomicU64,
    pub commit_index: AtomicU64,
    pub byz_commit_index: AtomicU64,
    pub byz_qc_pending: Mutex<HashMap<ProtoBlock, HashSet<(String, ProtoVote)>>>,
    pub byz_commit_pending: Mutex<HashMap<ProtoQuorumCertificate, HashSet<(String, ProtoVote)>>>,
    pub next_qc_list: Mutex<Vec<ProtoQuorumCertificate>>,
}

impl ConsensusState {
    fn new() -> ConsensusState {
        ConsensusState {
            fork: Mutex::new(Log::new()),
            view: AtomicU64::new(0),
            commit_index: AtomicU64::new(0),
            byz_commit_index: AtomicU64::new(0),
            byz_qc_pending: Mutex::new(HashMap::new()),
            byz_commit_pending: Mutex::new(HashMap::new()),
            next_qc_list: Mutex::new(Vec::new()),
        }
    }
}
pub type ForwardedMessage = (rpc::proto_payload::Message, String);

/// Keeps track of the rpc sequence numbers ONLY when I am the leader.
/// For fast quorum replies: if seq_num < last_fast_quorum_request, drop message.
/// For diverse quorum replies: if seq_num < last_diverse_quorum - k, drop message,
/// where k is the quorum diversity constant.
/// When I regain leadership, I should restart from here.
pub struct ServerContext {
    pub config: Config,
    pub last_fast_quorum_request: AtomicU64,
    pub last_diverse_quorum_request: AtomicU64,
    pub i_am_leader: AtomicBool,
    pub node_queue: (
        mpsc::UnboundedSender<ForwardedMessage>,
        Mutex<mpsc::UnboundedReceiver<ForwardedMessage>>,
    ),
    pub client_queue: (
        mpsc::UnboundedSender<ForwardedMessage>,
        Mutex<mpsc::UnboundedReceiver<ForwardedMessage>>,
    ),
    pub state: ConsensusState, // @todo better code structure such
}

#[derive(Clone)]
pub struct PinnedServerContext(pub Arc<Pin<Box<ServerContext>>>);

impl PinnedServerContext {
    pub fn new(cfg: &Config) -> PinnedServerContext {
        let node_ch = mpsc::unbounded_channel();
        let client_ch = mpsc::unbounded_channel();
        PinnedServerContext(Arc::new(Box::pin(ServerContext {
            config: cfg.clone(),
            last_fast_quorum_request: AtomicU64::new(1),
            last_diverse_quorum_request: AtomicU64::new(1),
            i_am_leader: AtomicBool::new(false),
            node_queue: (node_ch.0, Mutex::new(node_ch.1)),
            client_queue: (client_ch.0, Mutex::new(client_ch.1)),
            state: ConsensusState::new(),
        })))
    }
}

impl Deref for PinnedServerContext {
    type Target = ServerContext;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

// impl DerefMut for PinnedServerContext {
//     fn deref_mut(&mut self) -> &mut Self::Target {
//         self.0.as_mut().get_mut()
//     }
// }

/// This should be a very short running function.
/// No blocking and/or locking allowed.
/// The job is to filter old messages quickly and send them on the channel.
/// The real consensus handler is a separate green thread that consumes these messages.
pub fn consensus_rpc_handler<'a>(ctx: &PinnedServerContext, m: MessageRef<'a>) -> Result<Option<PinnedMessage>, Error> {
    let mut sender = String::from("");
    match m.2 {
        crate::rpc::SenderType::Anon => {
            return Err(Error::new(ErrorKind::InvalidData, "unauthenticated message")); // Anonymous replies shouldn't come here
        }
        crate::rpc::SenderType::Auth(name) => {
            sender = name.to_string();
        }
    }
    let body = match ProtoPayload::decode(&m.0.as_slice()[0..m.1]) {
        Ok(b) => b,
        Err(e) => {
            warn!("Parsing problem: {} ... Dropping connection", e.to_string());
            debug!("Original message: {:?}", &m.0.as_slice()[0..m.1]);
            return Err(Error::new(ErrorKind::InvalidData, e));
        }
    };

    let msg = match &body.message {
        Some(m) => m,
        None => {
            warn!("Nil message");
            return Ok(None);
        }
    };

    if !ctx.i_am_leader.load(Ordering::SeqCst) {
        match &msg {
            rpc::proto_payload::Message::ViewChange(_msg) => {
                if _msg.view < ctx.state.view.load(Ordering::SeqCst) {
                    return Ok(None); // Old view message
                }
            }
            rpc::proto_payload::Message::AppendEntries(_msg) => {
                if _msg.view < ctx.state.view.load(Ordering::SeqCst) {
                    return Ok(None); // Old view message
                }

                match body.rpc_type() {
                    rpc::RpcType::FastQuorumRequest => {
                        ctx.last_fast_quorum_request
                            .store(body.rpc_seq_num, Ordering::SeqCst);
                    }
                    rpc::RpcType::DiverseQuorumRequest => {
                        ctx.last_diverse_quorum_request
                            .store(body.rpc_seq_num, Ordering::SeqCst);
                    }
                    _ => {}
                }

                if sender
                    != ctx.config.consensus_config.node_list[get_current_leader(
                        ctx.config.consensus_config.node_list.len() as u64,
                        _msg.view,
                    )]
                {
                    return Ok(None); // This leader is not supposed to send message with this view.
                }
            }
            rpc::proto_payload::Message::NewLeader(_msg) => {
                if _msg.view < ctx.state.view.load(Ordering::SeqCst) {
                    return Ok(None); // Old view message
                }
            }
            rpc::proto_payload::Message::NewLeaderOk(_) => {
                // I am not leader, these messages shouldn't appear to me now.
                return Ok(None);
            }
            rpc::proto_payload::Message::Vote(_) => {
                // I am not leader, these messages shouldn't appear to me now.
                return Ok(None);
            }
            rpc::proto_payload::Message::ClientRequest(_) => {
                let msg = (body.message.unwrap(), sender);

                match ctx.client_queue.0.send(msg.clone()) {
                    // Does this make a double copy?
                    Ok(_) => {}
                    Err(e) => match e {
                        _ => {
                            return Err(Error::new(ErrorKind::OutOfMemory, "Channel error"));
                        }
                    },
                };

                // Send ack for mempool inclusion.
                let mut buf = Vec::new();
                msg.0.encode(&mut buf);
                let sig = crate::crypto::hash(&buf);  // @todo: This could be a signature. Bring the keystore here
                info!("Replying: {}", sig.encode_hex::<String>());
                return Ok(Some(PinnedMessage::from(
                    sig,
                    DIGEST_LENGTH,
                    crate::rpc::SenderType::Auth(ctx.config.net_config.name.clone())
                )));
            }
        }
    } else {
        // I am the leader
        if body.rpc_type() == rpc::RpcType::DiverseQuorumReply
            && body.rpc_seq_num
                < ctx.last_diverse_quorum_request.load(Ordering::Relaxed)
                    - ctx.config.consensus_config.quorum_diversity_k
        {
            return Ok(None); // Old message
        }

        if body.rpc_type() == rpc::RpcType::FastQuorumReply
            && body.rpc_seq_num < ctx.last_fast_quorum_request.load(Ordering::Relaxed)
        {
            return Ok(None); // Old message
        }

        match &msg {
            rpc::proto_payload::Message::ViewChange(_msg) => {
                if _msg.view < ctx.state.view.load(Ordering::SeqCst) {
                    return Ok(None); // Old view message
                }
            }
            rpc::proto_payload::Message::AppendEntries(_) => {
                return Ok(None); // I will not respond to other's AppendEntries while I am leader.
            }
            rpc::proto_payload::Message::NewLeader(_msg) => {
                if _msg.view < ctx.state.view.load(Ordering::SeqCst) {
                    return Ok(None); // Old view message
                }
            }
            rpc::proto_payload::Message::NewLeaderOk(_msg) => {
                if _msg.view < ctx.state.view.load(Ordering::SeqCst) {
                    return Ok(None); // Old view message
                }
            }
            rpc::proto_payload::Message::Vote(_msg) => {
                if _msg.view < ctx.state.view.load(Ordering::SeqCst) {
                    return Ok(None); // Old view message
                }
            }
            rpc::proto_payload::Message::ClientRequest(_) => {
                let msg = (body.message.unwrap(), sender);

                match ctx.client_queue.0.send(msg.clone()) {
                    // Does this make a double copy?
                    Ok(_) => {}
                    Err(e) => match e {
                        _ => {
                            return Err(Error::new(ErrorKind::OutOfMemory, "Channel error"));
                        }
                    },
                };

                // Send ack for mempool inclusion.
                let mut buf = Vec::new();
                msg.0.encode(&mut buf);
                let sig = crate::crypto::hash(&buf);  // @todo: This could be a signature. Bring the keystore here
                return Ok(Some(PinnedMessage::from(
                    sig,
                    DIGEST_LENGTH,
                    crate::rpc::SenderType::Auth(ctx.config.net_config.name.clone()
                ))));
            }
        }
    }

    // If code reaches here, it should be processed by the consensus algorithm.
    // Can be used for load shedding here.
    let msg = (body.message.unwrap(), sender);

    match ctx.node_queue.0.send(msg.clone()) {
        // Does this make a double copy?
        Ok(_) => {}
        Err(e) => match e {
            _ => {
                return Err(Error::new(ErrorKind::OutOfMemory, "Channel error"));
            }
        },
    };

    Ok(None)
}

use std::{collections::{HashMap, HashSet}, ops::Deref, pin::Pin, sync::{atomic::{AtomicBool, AtomicU64, Ordering}, Arc}};

use log::warn;
use prost::Message;
use tokio::sync::mpsc;

use crate::{config::Config, rpc::MessageRef};

use super::{leader_rotation::get_current_leader, proto::{consensus::{ProtoBlock, ProtoFork, ProtoQuorumCertificate, ProtoVote}, rpc::{self, ProtoPayload}}};

#[derive(Debug, Clone)]
pub struct ConsensusState {
    pub fork: ProtoFork,
    pub view: u64,
    pub commit_index: u64,
    pub byz_commit_index: u64,
    pub byz_qc_pending: HashMap<ProtoBlock, HashSet<(String, ProtoVote)>>,
    pub byz_commit_pending: HashMap<ProtoQuorumCertificate, HashSet<(String, ProtoVote)>>,
    pub next_qc_list: Vec<ProtoQuorumCertificate>
}

impl ConsensusState {
    fn new() -> ConsensusState {
        ConsensusState {
            fork: ProtoFork{
                blocks: Vec::new(),         // Block sequence numbers start from 1.
            },
            view: 1,
            commit_index: 0,
            byz_commit_index: 0,
            byz_qc_pending: HashMap::new(),
            byz_commit_pending: HashMap::new(),
            next_qc_list: Vec::new()
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
    pub queue: (mpsc::Sender<ForwardedMessage>, mpsc::Receiver<ForwardedMessage>),
    pub state: ConsensusState   // @todo: Mutex here
}

#[derive(Clone)]
pub struct PinnedServerContext(Arc<Pin<Box<ServerContext>>>);


impl PinnedServerContext {
    pub fn new(cfg: &Config) -> PinnedServerContext {
        PinnedServerContext(Arc::new(Box::pin(ServerContext{
            config: cfg.clone(),
            last_fast_quorum_request: AtomicU64::new(1),
            last_diverse_quorum_request: AtomicU64::new(1),
            i_am_leader: AtomicBool::new(false),
            queue: mpsc::channel(cfg.rpc_config.channel_depth as usize),
            state: ConsensusState::new()
        })))
    }
}

impl Deref for PinnedServerContext {
    type Target = ServerContext;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

/// This should be a very short running function.
/// No blocking and/or locking allowed.
/// The job is to filter old messages quickly and send them on the channel.
/// The real consensus handler is a separate green thread that consumes these messages.
pub async fn consensus_rpc_handler<'a>(ctx: &PinnedServerContext, m: MessageRef<'a>) -> bool {
    let ctx = ctx.clone();
    let mut sender = String::from("");
    match m.2 {
        crate::rpc::SenderType::Anon => {
            return false;       // Anonymous replies shouldn't come here
        },
        crate::rpc::SenderType::Auth(name) => {
            sender = name.to_string();
        }
    }
    let body = match ProtoPayload::decode(m.as_slice()){
        Ok(b) => b,
        Err(e) => {
            warn!("Parsing problem: {}... Dropping connection", e.to_string());
            return false;
        },
    };

    let msg = match body.message {
        Some(m) => m,
        None => {
            warn!("Nil message");
            return true;
        }
    };

    if !ctx.i_am_leader.load(Ordering::SeqCst) {
        match msg {
            rpc::proto_payload::Message::ViewChange(_msg) => {
                if _msg.view < ctx.state.view {
                    return true;    // Old view message
                }
            },
            rpc::proto_payload::Message::AppendEntries(_msg) => {
                if _msg.view < ctx.state.view {
                    return true;    // Old view message
                }

                if sender != ctx.node_list[get_current_leader(ctx.node_list.len(), _msg.view)]{
                    return true;    // This leader is not supposed to send message with this view.
                }
            },
            rpc::proto_payload::Message::NewLeader(_msg) => {
                if _msg.view < ctx.state.view {
                    return true;    // Old view message
                }
            },
            rpc::proto_payload::Message::NewLeaderOk(_) => {
                // I am not leader, these messages shouldn't appear to me now.
                return true;
            },
            rpc::proto_payload::Message::Vote(_) => {
                // I am not leader, these messages shouldn't appear to me now.
                return true;
            }
        }
    }else{
        // I am the leader
        match msg {
            rpc::proto_payload::Message::ViewChange(_msg) => {
                if _msg.view < ctx.state.view {
                    return true;    // Old view message
                }
            },
            rpc::proto_payload::Message::AppendEntries(_) => {
                return true;        // I will not respond to other's AppendEntries while I am leader.
            },
            rpc::proto_payload::Message::NewLeader(_msg) => {
                if _msg.view < ctx.state.view {
                    return true;    // Old view message
                }
            },
            rpc::proto_payload::Message::NewLeaderOk(_msg) => {
                if _msg.view < ctx.state.view {
                    return true;    // Old view message
                }
            },
            rpc::proto_payload::Message::Vote(_msg) => {
                if _msg.view < ctx.state.view {
                    return true;    // Old view message
                }

                // @todo: Handle what happens if the view is larger.
                // Should that code be here? Or on the other side of channel?
            },
        }
    }

    // If code reaches here, it should be processed by the consensus algorithm.
    // Can be used for load shedding here.
    match ctx.queue.0.send((msg, sender)).await {
        Ok(_) => true,
        Err(e) => {
            warn!("Sending on channel failed: {:?}", e);
            true
        },
    }

    // @todo: Server's message handler should be made async.
    // @todo: Include Client transaction entry as a separate message type. 
}










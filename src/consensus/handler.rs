use std::{
    collections::{BTreeMap, HashMap, HashSet},
    io::{Error, ErrorKind},
    ops::Deref,
    pin::Pin,
    sync::{
        atomic::{AtomicBool, AtomicU64, AtomicUsize},
        Arc,
    },
};

use indexmap::IndexMap;
use log::{debug, warn};
use prost::Message;
use std::time::Instant;
use tokio::sync::{mpsc, Mutex};

use crate::{
    config::Config,
    crypto::KeyStore,
    rpc::{
        server::{LatencyProfile, MsgAckChan, RespType},
        MessageRef,
    },
};

use super::{
    log::Log,
    proto::{
        consensus::{ProtoFork, ProtoQuorumCertificate, ProtoViewChange},
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
    pub num_committed_txs: AtomicUsize,
    pub byz_commit_index: AtomicU64,
    pub byz_qc_pending: Mutex<HashSet<u64>>,
    pub next_qc_list: Mutex<IndexMap<(u64, u64), ProtoQuorumCertificate>>,
    pub fork_buffer: Mutex<BTreeMap<u64, HashMap<String, ProtoViewChange>>>
}

impl ConsensusState {
    fn new() -> ConsensusState {
        ConsensusState {
            fork: Mutex::new(Log::new()),
            view: AtomicU64::new(0),
            commit_index: AtomicU64::new(0),
            num_committed_txs: AtomicUsize::new(0),
            byz_commit_index: AtomicU64::new(0),
            byz_qc_pending: Mutex::new(HashSet::new()),
            next_qc_list: Mutex::new(IndexMap::new()),
            fork_buffer: Mutex::new(BTreeMap::new())
        }
    }
}

pub type ForwardedMessage = (rpc::proto_payload::Message, String, LatencyProfile);
pub type ForwardedMessageWithAckChan = (
    rpc::proto_payload::Message,
    String,
    MsgAckChan,
    LatencyProfile,
);

pub struct ServerContext {
    pub config: Config,
    pub i_am_leader: AtomicBool,
    pub view_is_stable: AtomicBool,
    pub node_queue: (
        mpsc::UnboundedSender<ForwardedMessage>,
        Mutex<mpsc::UnboundedReceiver<ForwardedMessage>>,
    ),
    pub client_queue: (
        mpsc::UnboundedSender<ForwardedMessageWithAckChan>,
        Mutex<mpsc::UnboundedReceiver<ForwardedMessageWithAckChan>>,
    ),
    pub state: ConsensusState,
    pub client_ack_pending: Mutex<
        HashMap<
            (u64, usize), // (block_id, tx_id)
            (MsgAckChan, LatencyProfile),
        >,
    >,
    pub ping_counters: std::sync::Mutex<HashMap<u64, Instant>>,
    pub keys: KeyStore,
}

#[derive(Clone)]
pub struct PinnedServerContext(pub Arc<Pin<Box<ServerContext>>>);

impl PinnedServerContext {
    pub fn new(cfg: &Config, keys: &KeyStore) -> PinnedServerContext {
        let node_ch = mpsc::unbounded_channel();
        let client_ch = mpsc::unbounded_channel();
        PinnedServerContext(Arc::new(Box::pin(ServerContext {
            config: cfg.clone(),
            i_am_leader: AtomicBool::new(false),
            view_is_stable: AtomicBool::new(false),
            node_queue: (node_ch.0, Mutex::new(node_ch.1)),
            client_queue: (client_ch.0, Mutex::new(client_ch.1)),
            state: ConsensusState::new(),
            client_ack_pending: Mutex::new(HashMap::new()),
            ping_counters: std::sync::Mutex::new(HashMap::new()),
            keys: keys.clone(),
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
pub fn consensus_rpc_handler<'a>(
    ctx: &PinnedServerContext,
    m: MessageRef<'a>,
    ack_tx: MsgAckChan,
) -> Result<RespType, Error> {
    let profile = LatencyProfile::new();
    let sender = match m.2 {
        crate::rpc::SenderType::Anon => {
            return Err(Error::new(
                ErrorKind::InvalidData,
                "unauthenticated message",
            )); // Anonymous replies shouldn't come here
        }
        crate::rpc::SenderType::Auth(name) => name.to_string(),
    };
    let body = match ProtoPayload::decode(&m.0.as_slice()[0..m.1]) {
        Ok(b) => b,
        Err(e) => {
            warn!("Parsing problem: {} ... Dropping connection", e.to_string());
            debug!("Original message: {:?} {:?}", &m.0, &m.1);
            return Err(Error::new(ErrorKind::InvalidData, e));
        }
    };

    let msg = match &body.message {
        Some(m) => m,
        None => {
            warn!("Nil message");
            return Ok(RespType::NoResp);
        }
    };

    match &msg {
        rpc::proto_payload::Message::ClientRequest(_) => {
            let msg = (body.message.unwrap(), sender, ack_tx, profile);
            if let Err(_) = ctx.client_queue.0.send(msg) {
                return Err(Error::new(ErrorKind::OutOfMemory, "Channel error"));
            }
            return Ok(RespType::RespAndTrack);
        }
        _ => {
            let msg = (body.message.unwrap(), sender, profile);
            if let Err(_) = ctx.node_queue.0.send(msg) {
                return Err(Error::new(ErrorKind::OutOfMemory, "Channel error"));
            }

            return Ok(RespType::NoResp);
        }
    }
}

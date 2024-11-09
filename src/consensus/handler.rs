// Copyright (c) Shubham Mishra. All rights reserved.
// Licensed under the MIT License.

use std::{
    collections::{BTreeMap, HashMap, HashSet},
    io::{Error, ErrorKind},
    ops::Deref,
    pin::Pin,
    sync::{
        atomic::{AtomicBool, AtomicI8, AtomicU64, AtomicUsize, Ordering},
        Arc,
    }, time::Duration,
};

use indexmap::IndexMap;
use log::{debug, error, info, trace, warn};
use prost::Message;
use rustls::crypto::hash::Hash;
use std::time::Instant;
use tokio::{join, sync::{mpsc, Mutex, Semaphore}};

use crate::{
    config::{AtomicConfig, Config, NodeInfo}, crypto::{AtomicKeyStore, KeyStore}, proto::{client::{ProtoByzPollRequest, ProtoByzResponse, ProtoClientRequest}, execution::ProtoTransaction, rpc::proto_payload}, rpc::{
        client::PinnedClient, server::{GetServerKeys, LatencyProfile, MsgAckChan, RespType}, MessageRef, PinnedMessage
    }, utils::AtomicStruct
};

use super::{
    super::proto::{
        consensus::{ProtoQuorumCertificate, ProtoViewChange},
        rpc::{self, ProtoPayload},
    }, backfill::*, client_reply::*, commit::*, log::Log, reconfiguration::{decide_my_lifecycle_stage, do_graceful_shutdown}, steady_state::*, timer::{RandomResettableTimer, ResettableTimer}, utils::*, view_change::*
};

/// @todo: This doesn't have to be here. Unncessary Mutexes.
/// This can be private to the protocols. More flexibility that way.
#[derive(Debug)]
pub struct ConsensusState {
    pub fork: Mutex<Log>,
    pub view: AtomicU64,
    pub config_num: AtomicU64,
    pub commit_index: AtomicU64,
    pub num_committed_txs: AtomicUsize,
    pub num_byz_committed_txs: AtomicUsize,
    pub byz_commit_index: AtomicU64,
    pub byz_qc_pending: Mutex<HashSet<u64>>,
    pub next_qc_list: Mutex<IndexMap<(u64, u64), ProtoQuorumCertificate>>,
    pub fork_buffer: Mutex<BTreeMap<u64, HashMap<String, ProtoViewChange>>>,
}

impl ConsensusState {
    fn new(config: Config) -> ConsensusState {
        ConsensusState {
            fork: Mutex::new(Log::new(config)),

            #[cfg(feature = "view_change")]
            view: AtomicU64::new(0),
            #[cfg(not(feature = "view_change"))]
            view: AtomicU64::new(1),

            config_num: AtomicU64::new(0),
            commit_index: AtomicU64::new(0),
            num_committed_txs: AtomicUsize::new(0),
            num_byz_committed_txs: AtomicUsize::new(0),
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LifecycleStage {
    Dormant = 1,
    Learner = 2,
    FullNode = 3,
    OldFullNode = 4,
    Dead = 5,
}

type AtomicVec = AtomicStruct<Vec<String>>;

pub struct ServerContext {
    pub config: AtomicConfig,
    /// In all configurations, send_list = config.consensus_config.node_list - {me}
    pub send_list: AtomicVec,
    pub old_full_nodes: AtomicVec,
    pub i_am_leader: AtomicBool,
    pub view_is_stable: AtomicBool,
    pub last_stable_view: AtomicU64,

    pub lifecycle_stage: AtomicI8,
    pub node_queue: (
        mpsc::UnboundedSender<ForwardedMessageWithAckChan>,
        Mutex<mpsc::UnboundedReceiver<ForwardedMessageWithAckChan>>,
    ),
    pub client_queue: (
        mpsc::UnboundedSender<ForwardedMessageWithAckChan>,
        Mutex<mpsc::UnboundedReceiver<ForwardedMessageWithAckChan>>,
    ),
    pub state: ConsensusState,
    pub client_ack_pending: Mutex<
        HashMap<
            (u64, usize), // (block_id, tx_id)
            (MsgAckChan, LatencyProfile, String), // (msg chan, latency, sender)
        >,
    >,

    pub client_byz_ack_pending: std::sync::Mutex<
        HashMap<
            String,
            Vec<ProtoByzResponse>
        >,
    >,
    pub client_tx_map: std::sync::Mutex<
        HashMap<
            (u64, usize),
            String
        >
    >,
    pub client_replied_bci: AtomicU64,

    pub ping_counters: std::sync::Mutex<HashMap<u64, Instant>>,
    pub keys: AtomicKeyStore,

    /// The default flow for client requests is to send a reply when committed.
    /// For Noop blocks, there is no such client waiting,
    /// so we send the reply to a black hole.
    pub __client_black_hole_channel: (
        mpsc::UnboundedSender<(PinnedMessage, LatencyProfile)>,
        Mutex<mpsc::UnboundedReceiver<(PinnedMessage, LatencyProfile)>>,
    ),

    pub __should_server_update_keys: AtomicBool,

    pub reconf_channel: (
        mpsc::UnboundedSender<ProtoTransaction>,
        Mutex<mpsc::UnboundedReceiver<ProtoTransaction>>,
    ),

    pub view_timer: Arc<Pin<Box<RandomResettableTimer>>>,

    /// Last view that was fast forwarded due to pacemaker.
    pub intended_view: AtomicU64,

    pub total_client_requests: AtomicUsize,

    pub should_progress: Semaphore,

    pub total_blocks_forced_supermajority: AtomicUsize,
}

#[derive(Clone)]
pub struct PinnedServerContext(pub Arc<Pin<Box<ServerContext>>>);

impl PinnedServerContext {
    pub fn new(cfg: &Config, keys: &KeyStore) -> PinnedServerContext {
        let node_ch = mpsc::unbounded_channel();
        let client_ch = mpsc::unbounded_channel();
        let black_hole_ch = mpsc::unbounded_channel();
        let reconf_channel = mpsc::unbounded_channel();
        let send_list = get_everyone_except_me(&cfg.net_config.name, &cfg.consensus_config.node_list);


        let ctx = PinnedServerContext(Arc::new(Box::pin(ServerContext {
            config: AtomicConfig::new(cfg.clone()),
            send_list: AtomicVec::new(send_list),
            old_full_nodes: AtomicVec::new(Vec::new()),
            i_am_leader: AtomicBool::new(false),
            lifecycle_stage: AtomicI8::new(LifecycleStage::Dormant as i8),

            #[cfg(feature = "view_change")]
            view_is_stable: AtomicBool::new(false),
            #[cfg(not(feature = "view_change"))]
            view_is_stable: AtomicBool::new(true),

            #[cfg(feature = "view_change")]
            last_stable_view: AtomicU64::new(0),
            #[cfg(not(feature = "view_change"))]
            last_stable_view: AtomicU64::new(1),
            
            node_queue: (node_ch.0, Mutex::new(node_ch.1)),
            client_queue: (client_ch.0, Mutex::new(client_ch.1)),
            state: ConsensusState::new(cfg.clone()),
            client_ack_pending: Mutex::new(HashMap::new()),
            client_byz_ack_pending: std::sync::Mutex::new(HashMap::new()),
            client_replied_bci: AtomicU64::new(0),
            client_tx_map: std::sync::Mutex::new(HashMap::new()),
            ping_counters: std::sync::Mutex::new(HashMap::new()),
            keys: AtomicKeyStore::new(keys.clone()),
            __client_black_hole_channel: (black_hole_ch.0, Mutex::new(black_hole_ch.1)),
            __should_server_update_keys: AtomicBool::new(false),
            reconf_channel: (reconf_channel.0, Mutex::new(reconf_channel.1)),
            view_timer: RandomResettableTimer::new(Duration::from_millis(cfg.consensus_config.view_timeout_ms), Duration::from_millis(cfg.consensus_config.view_timeout_ms / 2)),
            intended_view: AtomicU64::new(0),
            total_client_requests: AtomicUsize::new(0),
            should_progress: Semaphore::new(1),
            total_blocks_forced_supermajority: AtomicUsize::new(0),
        })));
        let lifecycle_stage = decide_my_lifecycle_stage(&ctx, true);
        ctx.lifecycle_stage.store(lifecycle_stage as i8, Ordering::SeqCst);
        info!("Initial lifecycle stage: {:?}", lifecycle_stage);
        
        ctx
    }
}

impl Deref for PinnedServerContext {
    type Target = ServerContext;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl GetServerKeys for PinnedServerContext {
    fn get_server_keys(&self) -> Arc<Box<KeyStore>> {
        info!("Keys: {:?}", self.keys.get().pub_keys);
        self.keys.get()
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
        rpc::proto_payload::Message::ClientRequest(client_req) => {
            let ret = if client_req.tx.as_ref().is_some() && client_req.tx.as_ref().unwrap().is_reconfiguration {
                Ok(RespType::RespAndTrackAndReconf)
            } else {
                Ok(RespType::RespAndTrack)
            };

            let msg = (body.message.unwrap(), sender, ack_tx, profile);
            if let Err(_) = ctx.client_queue.0.send(msg) {
                return Err(Error::new(ErrorKind::OutOfMemory, "Channel error"));
            }

            return ret;
        }
        rpc::proto_payload::Message::BackfillRequest(_) => {
            let msg = (body.message.unwrap(), sender, ack_tx, profile);
            if let Err(_) = ctx.node_queue.0.send(msg) {
                return Err(Error::new(ErrorKind::OutOfMemory, "Channel error"));
            }

            return Ok(RespType::RespAndTrack);
        }
        _ => {
            let msg = (body.message.unwrap(), sender, ack_tx, profile);
            if let Err(_) = ctx.node_queue.0.send(msg) {
                return Err(Error::new(ErrorKind::OutOfMemory, "Channel error"));
            }

            if let Ok(_) = ctx.__should_server_update_keys.compare_exchange(true, false, Ordering::Acquire, Ordering::Relaxed) {
                return Ok(RespType::NoRespAndReconf);
            }

            return Ok(RespType::NoResp);
        }
    }
}





pub async fn process_node_request<Engine>(
    ctx: &PinnedServerContext, engine: &Engine,
    client: &PinnedClient,
    majority: u64,
    super_majority: u64,
    old_super_majority: u64,
    ms: &mut ForwardedMessageWithAckChan,
) -> Result<(), Error>
where Engine: crate::execution::Engine
{
    let (msg, sender, ack_tx, profile) = ms;
    let _sender = sender.clone();
    match &msg {
        crate::proto::rpc::proto_payload::Message::AppendEntries(ae) => {
            profile.register("AE chan wait");
            let (last_n, updated_last_n, seq_nums, should_update_ci) =
                do_push_append_entries_to_fork(ctx.clone(), engine, client.clone(), ae, sender, super_majority).await;
            profile.register("Fork push");

            if updated_last_n > last_n {
                // New block has been added. Vote for the last one.
                let stage = ctx.lifecycle_stage.load(Ordering::SeqCst);
                if stage == LifecycleStage::FullNode as i8 || stage == LifecycleStage::OldFullNode as i8 {
                    // Only full nodes vote.
                    let vote = create_vote_for_blocks(ctx.clone(), &seq_nums).await?;
                    profile.register("Prepare vote");
                    do_reply_vote(ctx.clone(), client.clone(), vote, &sender).await?;
                    profile.register("Sent vote");
    
                    if updated_last_n % 1000 == 0 {
                        profile.should_print = true;
                        profile.prefix = String::from(format!("Block: {}", updated_last_n));
                        profile.print();
                    }
                }
            }

            let ci = ctx.state.commit_index.load(Ordering::SeqCst);
            let new_ci = ae.commit_index;
            if should_update_ci && new_ci > ci {
                // let mut fork = ctx.state.fork.lock().await;
                let fork = ctx.state.fork.try_lock();
                let mut fork = if let Err(e) = fork {
                    debug!("process_node_request: Fork is locked, waiting for it to be unlocked: {}", e);
                    let fork = ctx.state.fork.lock().await;
                    debug!("process_node_request: Fork locked");
                    fork  
                }else{
                    debug!("process_node_request: Fork locked");
                    fork.unwrap()
                };
                // Followers should not have pending client requests.
                // But this same interface is good for refactoring.
                do_commit(ctx, client, engine, &mut fork, new_ci).await;
            }
        }
        crate::proto::rpc::proto_payload::Message::Vote(v) => {
            profile.register("Vote chan wait");
            let _ = do_process_vote(ctx.clone(), client.clone(), engine, v, sender, majority, super_majority, old_super_majority).await;
            profile.register("Vote process");
        }
        crate::proto::rpc::proto_payload::Message::ViewChange(vc) => {
            profile.register("View Change chan wait");
            let _ = do_process_view_change(ctx.clone(), engine, client.clone(), vc, sender, super_majority, old_super_majority)
                .await;
            profile.register("View change process");
        },
        crate::proto::rpc::proto_payload::Message::BackfillRequest(bfr) => {
            profile.register("Backfill Request chan wait");
            do_process_backfill_request(ctx.clone(), ack_tx, bfr, sender).await;
            profile.register("Backfill Request process");
        },
        _ => {}
    }

    // if ctx.lifecycle_stage.load(Ordering::SeqCst) == LifecycleStage::Dead as i8 {
    //     do_graceful_shutdown().await;
    // }

    Ok(())
}



pub async fn handle_client_messages<Engine>(
    ctx: PinnedServerContext,
    client: PinnedClient,
    engine: Engine
) -> Result<(), Error> 
where 
    Engine: crate::execution::Engine + Clone + Send + Sync + 'static
{
    let mut client_rx = ctx.0.client_queue.1.lock().await;
    let mut curr_client_req = Vec::new();
    let mut curr_client_req_num = 0;
    let mut signature_timer_tick = false;

    let majority = get_majority_num(&ctx);
    let super_majority = get_super_majority_num(&ctx);
    let cfg = ctx.config.get();

    // Signed block logic: Either this timer expires.
    // Or the number of blocks crosses signature_max_delay_blocks.
    // In the later case, reset the timer.
    let signature_timer = ResettableTimer::new(Duration::from_millis(
        cfg.consensus_config.signature_max_delay_ms,
    ));

    let signature_timer_handle = signature_timer.run().await;

    let mut pending_signatures = 0;

    #[cfg(not(feature = "view_change"))]
    {
        if get_leader_str(&ctx) == ctx.config.get().net_config.name {
            ctx.i_am_leader.store(true, Ordering::SeqCst);
        }
    }


    loop {
        let cfg = ctx.config.get();
        tokio::select! {
            biased;
            n_ = client_rx.recv_many(&mut curr_client_req, cfg.consensus_config.max_backlog_batch_size) => {
                curr_client_req_num = n_;
            },
            tick = signature_timer.wait() => {
                signature_timer_tick = tick;
            }
        }

        if curr_client_req_num == 0 && signature_timer_tick == false {
            // Channels are all closed.
            break;
        }

        ctx.total_client_requests.fetch_add(curr_client_req_num, Ordering::SeqCst);
        trace!("Client handler: {} client requests", ctx.total_client_requests.load(Ordering::SeqCst));
        
        if !ctx.view_is_stable.load(Ordering::SeqCst) {
            do_respond_with_try_again(&curr_client_req, NodeInfo {
                nodes: ctx.config.get().net_config.nodes.clone(),
            }).await;
            // @todo: Backoff here.
            // Reset for next iteration
            curr_client_req.clear();
            curr_client_req_num = 0;
            signature_timer_tick = false;
            continue;
        }
        
        // Respond to read requests: Any replica can reply.
        // Removes read-only requests from curr_client_req.
        do_respond_to_read_requests(&ctx, &engine, &mut curr_client_req).await;
        // if curr_client_req.len() == 0 {
        //     curr_client_req_num = 0;
        //     signature_timer_tick = false;
        //     continue;
        // }

        if !ctx.i_am_leader.load(Ordering::SeqCst) {
            if curr_client_req.len() > 0 {
                do_respond_with_current_leader(&ctx, &curr_client_req).await;
            }
            // Reset for next iteration
            curr_client_req.clear();
            curr_client_req_num = 0;
            signature_timer_tick = false;
            continue;
        }



        let cfg = ctx.config.get();
        // Ok I am the leader.
        pending_signatures += 1;
        #[cfg(feature = "always_sign")]
        let mut should_sig = true;
        #[cfg(feature = "never_sign")]
        let mut should_sig = false;
        #[cfg(feature = "dynamic_sign")]
        let mut should_sig = signature_timer_tick    // Either I am running this body because of signature timeout.
            || (pending_signatures >= cfg.consensus_config.signature_max_delay_blocks);
        // Or I actually got some transactions and I really need to sign

        if signature_timer_tick && curr_client_req.len() == 0 {
            trace!("Blank heartbeat");
            force_noop(&ctx).await;
            curr_client_req_num = 0;
            signature_timer.reset();
            continue;
        }


        // Semaphore will be released in `do_commit` when a commit happens.
        #[cfg(feature = "no_pipeline")]
        ctx.should_progress.acquire().await.unwrap().forget();

        trace!("AppendEntries with {} entries", curr_client_req.len());
        match do_append_entries(
            ctx.clone(), &engine.clone(), client.clone(),
            &mut curr_client_req, should_sig,
            &ctx.send_list.get(), majority, super_majority, 0 // View is stable; so no OldFullNode
        ).await {
            Ok(_) => {}
            Err(e) => {
                warn!("Error doing append entries {}", e);
                do_respond_with_current_leader(&ctx, &curr_client_req).await;
                should_sig = false;
            }
        };

        if should_sig {
            pending_signatures = 0;
            signature_timer.reset();
        }

        // Reset for next iteration
        curr_client_req.clear();
        curr_client_req_num = 0;
        signature_timer_tick = false;
    }

    warn!("Client handler dying!");

    let _ = join!(signature_timer_handle);
    Ok(())
}

pub async fn handle_node_messages<Engine>(
    ctx: PinnedServerContext,
    client: PinnedClient,
    engine: Engine
) -> Result<(), Error>
    where Engine: crate::execution::Engine + Clone + Send + Sync + 'static
{
    let view_timer_handle = ctx.view_timer.run().await;

    // Spawn all vote processing workers.
    // This thread will also act as one.
    let mut vote_worker_chans = Vec::new();
    let mut vote_worker_rr_cnt = 1u16;
    let cfg = ctx.config.get();
    for _ in 1..cfg.consensus_config.vote_processing_workers {
        // 0th index is for this thread
        let (tx, mut rx) = mpsc::unbounded_channel();
        vote_worker_chans.push(tx);
        let ctx = ctx.clone();
        let client = client.clone();
        let engine = engine.clone();
        tokio::spawn(async move {
            loop {
                let msg = rx.recv().await;
                if let None = msg {
                    break;
                }
                let mut msg = msg.unwrap();
                let majority = get_majority_num(&ctx);
                let super_majority = get_super_majority_num(&ctx);
                let old_super_majority = get_old_super_majority_num(&ctx);
                let _ =
                    process_node_request(&ctx, &engine, &client, majority, super_majority, old_super_majority, &mut msg).await;
            }
        });
    }

    let mut node_rx = ctx.0.node_queue.1.lock().await;
    let _cfg = ctx.config.get();

    debug!(
        "Leader: {}, Send List: {:?}",
        ctx.i_am_leader.load(Ordering::SeqCst),
        &ctx.send_list.get()
    );

    let mut curr_node_req = Vec::new();
    let mut view_timer_tick = false;
    let mut node_req_num = 0;

    // Start with a view change
    ctx.view_timer.fire_now().await;    
    let mut majority = get_majority_num(&ctx);

    loop {
        tokio::select! {
            biased;
            tick = ctx.view_timer.wait() => {
                view_timer_tick = tick;
            }
            node_req_num_ = node_rx.recv_many(&mut curr_node_req, (majority - 1) as usize) => node_req_num = node_req_num_,
        }

        if view_timer_tick == false && node_req_num == 0 {
            warn!("Consensus node dying!");
            break; // Select failed because both channels were closed!
        }

        let stage = ctx.lifecycle_stage.load(Ordering::SeqCst);
        if stage == LifecycleStage::Dead as i8 {
            do_graceful_shutdown().await;
            break;
        }

        majority = get_majority_num(&ctx);
        let super_majority = get_super_majority_num(&ctx);
        let old_super_majority = get_old_super_majority_num(&ctx);


        #[cfg(feature = "view_change")]
        {
            // If Dormant or Learner, don't act on view timer.
            let stage = ctx.lifecycle_stage.load(Ordering::SeqCst);
            if stage == LifecycleStage::Dormant as i8 || stage == LifecycleStage::Learner as i8 {
                // Do nothing
            }else {
                if view_timer_tick {
                    info!("Timer fired");
                    PinnedClient::drop_all_connections(&client);
                    ctx.intended_view.fetch_add(1, Ordering::SeqCst);
                    if ctx.intended_view.load(Ordering::SeqCst) > ctx.state.view.load(Ordering::SeqCst) {
                        if let Err(e) = do_init_view_change(&ctx, &engine, &client, super_majority, old_super_majority).await {
                            error!("Error initiating view change: {}", e);
                        }
                    }
                }
            }
        }

        while node_req_num > 0 {
            // @todo: Really need a VecDeque::pop_front() here. But this suffices for now.
            let mut req = curr_node_req.remove(0);
            node_req_num -= 1;

            // If we are in the Dormant or Learner stage, only process AppendEntries or ViewChange. (No votes)
            if stage == LifecycleStage::Dormant as i8 || stage == LifecycleStage::Learner as i8 {
                if let crate::proto::rpc::proto_payload::Message::AppendEntries(_) = req.0 {
                    // If I am Dormant, by this msg, I become a learner.
                    if stage == LifecycleStage::Dormant as i8 {
                        info!("Lifecycle stage: Dormant -> Learner");
                        ctx.lifecycle_stage.store(LifecycleStage::Learner as i8, Ordering::SeqCst);
                    }
                    if let Err(e) = process_node_request(&ctx, &engine, &client, majority, super_majority, old_super_majority, &mut req).await {
                        error!("Error processing append entries: {}", e);
                    }
                }
                if let crate::proto::rpc::proto_payload::Message::ViewChange(_) = req.0 {
                    // If I am Dormant, by this msg, I become a learner.
                    if stage == LifecycleStage::Dormant as i8 {
                        info!("Lifecycle stage: Dormant -> Learner");
                        ctx.lifecycle_stage.store(LifecycleStage::Learner as i8, Ordering::SeqCst);
                    }
                    if let Err(e) = process_node_request(&ctx, &engine, &client, majority, super_majority, old_super_majority, &mut req).await {
                        error!("Error processing append entries: {}", e);
                    }
                }
                
                continue;
            }

            // AppendEntries should be processed by a single thread.
            // Only votes can be safely processed by multiple threads.
            if let crate::proto::rpc::proto_payload::Message::Vote(_) = req.0 {
                let cfg = ctx.config.get();
                let rr_cnt =
                    vote_worker_rr_cnt % cfg.consensus_config.vote_processing_workers;
                vote_worker_rr_cnt += 1;
                if rr_cnt == 0 {
                    // Let this thread process it.
                    if let Err(e) = process_node_request(&ctx, &engine, &client, majority, super_majority, old_super_majority, &mut req).await {
                        error!("Error processing vote: {}", e);
                    }
                } else {
                    // Push it to a worker
                    let _ = vote_worker_chans[(rr_cnt - 1) as usize].send(req);
                }
            } else {
                if let Err(e) = process_node_request(&ctx, &engine, &client, majority, super_majority, old_super_majority, &mut req).await {
                    error!("Error processing node request: {}", e);
                }
            }
        }

        // Reset for the next iteration
        curr_node_req.clear();
        node_req_num = 0;
        view_timer_tick = false;
    }

    let _ = join!(view_timer_handle);
    Ok(())
}
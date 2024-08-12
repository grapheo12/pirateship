use std::{
    collections::{BTreeMap, HashMap, HashSet},
    io::{Error, ErrorKind},
    ops::Deref,
    pin::Pin,
    sync::{
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
        Arc,
    }, time::Duration,
};

use indexmap::IndexMap;
use log::{debug, error, info, trace, warn};
use prost::Message;
use std::time::Instant;
use tokio::{join, sync::{mpsc, Mutex, Semaphore}};

use crate::{
    config::Config, crypto::KeyStore, rpc::{
        client::PinnedClient, server::{LatencyProfile, MsgAckChan, RespType}, MessageRef, PinnedMessage
    }
};

use super::{
    log::Log,
    super::proto::{
        consensus::{ProtoQuorumCertificate, ProtoViewChange},
        rpc::{self, ProtoPayload},
    }, timer::ResettableTimer,
    utils::*,
    client_reply::*,
    commit::*,
    steady_state::*,
    view_change::*,
    backfill::*
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

            #[cfg(feature = "view_change")]
            view: AtomicU64::new(0),
            #[cfg(not(feature = "view_change"))]
            view: AtomicU64::new(1),

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
            (MsgAckChan, LatencyProfile),
        >,
    >,
    pub ping_counters: std::sync::Mutex<HashMap<u64, Instant>>,
    pub keys: KeyStore,

    /// The default flow for client requests is to send a reply when committed.
    /// For Noop blocks, there is no such client waiting,
    /// so we send the reply to a black hole.
    pub __client_black_hole_channel: (
        mpsc::UnboundedSender<(PinnedMessage, LatencyProfile)>,
        Mutex<mpsc::UnboundedReceiver<(PinnedMessage, LatencyProfile)>>,
    ),

    pub view_timer: Arc<Pin<Box<ResettableTimer>>>,
    pub total_client_requests: AtomicUsize,

    pub should_progress: Semaphore
}

#[derive(Clone)]
pub struct PinnedServerContext(pub Arc<Pin<Box<ServerContext>>>);

impl PinnedServerContext {
    pub fn new(cfg: &Config, keys: &KeyStore) -> PinnedServerContext {
        let node_ch = mpsc::unbounded_channel();
        let client_ch = mpsc::unbounded_channel();
        let black_hole_ch = mpsc::unbounded_channel();

        PinnedServerContext(Arc::new(Box::pin(ServerContext {
            config: cfg.clone(),
            i_am_leader: AtomicBool::new(false),

            #[cfg(feature = "view_change")]
            view_is_stable: AtomicBool::new(false),
            #[cfg(not(feature = "view_change"))]
            view_is_stable: AtomicBool::new(true),
            
            node_queue: (node_ch.0, Mutex::new(node_ch.1)),
            client_queue: (client_ch.0, Mutex::new(client_ch.1)),
            state: ConsensusState::new(),
            client_ack_pending: Mutex::new(HashMap::new()),
            ping_counters: std::sync::Mutex::new(HashMap::new()),
            keys: keys.clone(),
            __client_black_hole_channel: (black_hole_ch.0, Mutex::new(black_hole_ch.1)),
            view_timer: ResettableTimer::new(Duration::from_millis(cfg.consensus_config.view_timeout_ms)),
            total_client_requests: AtomicUsize::new(0),
            should_progress: Semaphore::new(1),
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

            return Ok(RespType::NoResp);
        }
    }
}





pub async fn process_node_request<Engine>(
    ctx: &PinnedServerContext, engine: &Engine,
    client: &PinnedClient,
    majority: u64,
    super_majority: u64,
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
                let mut lack_pend = ctx.client_ack_pending.lock().await;
                // Followers should not have pending client requests.
                // But this same interface is good for refactoring.
                do_commit(ctx, engine, &mut fork, &mut lack_pend, new_ci);
            }
        }
        crate::proto::rpc::proto_payload::Message::Vote(v) => {
            profile.register("Vote chan wait");
            let _ = do_process_vote(ctx.clone(), engine, v, sender, majority, super_majority).await;
            profile.register("Vote process");
        }
        crate::proto::rpc::proto_payload::Message::ViewChange(vc) => {
            profile.register("View Change chan wait");
            let _ = do_process_view_change(ctx.clone(), engine, client.clone(), vc, sender, super_majority)
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
    let send_list = get_everyone_except_me(
        &ctx.config.net_config.name,
        &ctx.config.consensus_config.node_list,
    );

    // Signed block logic: Either this timer expires.
    // Or the number of blocks crosses signature_max_delay_blocks.
    // In the later case, reset the timer.
    let signature_timer = ResettableTimer::new(Duration::from_millis(
        ctx.config.consensus_config.signature_max_delay_ms,
    ));

    let signature_timer_handle = signature_timer.run().await;

    let mut pending_signatures = 0;

    #[cfg(not(feature = "view_change"))]
    {
        if get_leader_str(&ctx) == ctx.config.net_config.name {
            ctx.i_am_leader.store(true, Ordering::SeqCst);
        }
    }


    loop {
        tokio::select! {
            biased;
            n_ = client_rx.recv_many(&mut curr_client_req, ctx.config.consensus_config.max_backlog_batch_size) => {
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
        
        if !ctx.i_am_leader.load(Ordering::SeqCst) {
            do_respond_with_current_leader(&ctx, &curr_client_req).await;
            // Reset for next iteration
            curr_client_req.clear();
            curr_client_req_num = 0;
            signature_timer_tick = false;
            continue;
        }

        if !ctx.view_is_stable.load(Ordering::SeqCst) {
            do_respond_with_try_again(&curr_client_req).await;
            // @todo: Backoff here.
            // Reset for next iteration
            curr_client_req.clear();
            curr_client_req_num = 0;
            signature_timer_tick = false;
            continue;
        }

        // @todo: Reply to read requests (even when I am not the leader).

        
        // Ok I am the leader.
        pending_signatures += 1;
        #[cfg(feature = "always_sign")]
        let mut should_sig = true;
        #[cfg(feature = "never_sign")]
        let mut should_sig = false;
        #[cfg(feature = "dynamic_sign")]
        let mut should_sig = signature_timer_tick    // Either I am running this body because of signature timeout.
            || (pending_signatures >= ctx.config.consensus_config.signature_max_delay_blocks);
        // Or I actually got some transactions and I really need to sign

        // Semaphore will be released in `do_commit` when a commit happens.
        #[cfg(feature = "no_pipeline")]
        ctx.should_progress.acquire().await.unwrap().forget();

        match do_append_entries(
            ctx.clone(), &engine.clone(), client.clone(),
            &mut curr_client_req, should_sig,
            &send_list, majority, super_majority,
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
    let majority = get_majority_num(&ctx);
    let super_majority = get_super_majority_num(&ctx);

    // Spawn all vote processing workers.
    // This thread will also act as one.
    let mut vote_worker_chans = Vec::new();
    let mut vote_worker_rr_cnt = 1u16;
    for _ in 1..ctx.config.consensus_config.vote_processing_workers {
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
                let _ =
                    process_node_request(&ctx, &engine, &client, majority, super_majority, &mut msg).await;
            }
        });
    }

    let mut node_rx = ctx.0.node_queue.1.lock().await;
    let send_list = get_everyone_except_me(
        &ctx.config.net_config.name,
        &ctx.config.consensus_config.node_list,
    );

    debug!(
        "Leader: {}, Send List: {:?}",
        ctx.i_am_leader.load(Ordering::SeqCst),
        &send_list
    );

    let mut curr_node_req = Vec::new();
    let mut view_timer_tick = false;
    let mut node_req_num = 0;

    let mut intended_view = 0;

    // Start with a view change
    ctx.view_timer.fire_now().await;

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

        #[cfg(feature = "view_change")]
        {
            if view_timer_tick {
                info!("Timer fired");
                intended_view += 1;
                if intended_view > ctx.state.view.load(Ordering::SeqCst) {
                    if let Err(e) = do_init_view_change(&ctx, &engine, &client, super_majority).await {
                        error!("Error initiating view change: {}", e);
                    }
                }
            }
        }

        while node_req_num > 0 {
            // @todo: Really need a VecDeque::pop_front() here. But this suffices for now.
            let mut req = curr_node_req.remove(0);
            node_req_num -= 1;
            // AppendEntries should be processed by a single thread.
            // Only votes can be safely processed by multiple threads.
            if let crate::proto::rpc::proto_payload::Message::Vote(_) = req.0 {
                let rr_cnt =
                    vote_worker_rr_cnt % ctx.config.consensus_config.vote_processing_workers;
                vote_worker_rr_cnt += 1;
                if rr_cnt == 0 {
                    // Let this thread process it.
                    if let Err(e) = process_node_request(&ctx, &engine, &client, majority, super_majority, &mut req).await {
                        error!("Error processing vote: {}", e);
                    }
                } else {
                    // Push it to a worker
                    let _ = vote_worker_chans[(rr_cnt - 1) as usize].send(req);
                }
            } else {
                if let Err(e) = process_node_request(&ctx, &engine, &client, majority, super_majority, &mut req).await {
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

use hex::ToHex;
use log::{debug, info, warn};
use prost::Message;
use std::{
    collections::{HashMap, HashSet},
    fmt::format,
    io::{Error, ErrorKind},
    sync::atomic::Ordering,
    time::{Duration, Instant},
};
use tokio::{
    sync::mpsc::{self, Sender},
    sync::MutexGuard,
    time::sleep,
};

use crate::{
    consensus::{
        self,
        handler::{ForwardedMessage, ForwardedMessageWithAckChan, PinnedServerContext},
        leader_rotation::get_current_leader,
        log::{Log, LogEntry},
        proto::{
            client::ProtoClientReply,
            consensus::{
                proto_block::Sig, DefferedSignature, ProtoAppendEntries, ProtoBlock, ProtoFork,
                ProtoSignatureArrayEntry, ProtoVote,
            },
            rpc::{self, proto_payload, ProtoPayload},
        },
        timer::ResettableTimer,
    },
    crypto::{cmp_hash, hash, DIGEST_LENGTH},
    rpc::{
        client::PinnedClient,
        server::{LatencyProfile, MsgAckChan},
        MessageRef, PinnedMessage,
    },
};

pub fn get_leader_str(ctx: &PinnedServerContext) -> String {
    ctx.config.consensus_config.node_list
        [get_current_leader(ctx.config.consensus_config.node_list.len() as u64, 1)]
    .clone()
}

fn get_node_num(ctx: &PinnedServerContext) -> u64 {
    let mut i = 0;
    for name in &ctx.config.consensus_config.node_list {
        if name.eq(&ctx.config.net_config.name) {
            return i;
        }
        i += 1;
    }

    0
}

fn get_majority_num(ctx: &PinnedServerContext) -> u64 {
    let n = ctx.config.consensus_config.node_list.len() as u64;
    n / 2 + 1
}

fn get_super_majority_num(ctx: &PinnedServerContext) -> u64 {
    let n = ctx.config.consensus_config.node_list.len() as u64;
    2 * (n / 3) + 1
}

fn get_everyone_except_me(my_name: &String, node_list: &Vec<String>) -> Vec<String> {
    node_list
        .iter()
        .map(|n| n.clone())
        .filter(|name| !name.eq(my_name))
        .collect()
}

pub async fn create_vote_for_blocks(
    ctx: PinnedServerContext,
    seq_nums: &Vec<u64>,
) -> Result<ProtoVote, Error> {
    let mut fork = ctx.state.fork.lock().await;
    let mut vote_sigs = Vec::new();
    let mut max_n = 0;
    for n in seq_nums {
        let n = *n;
        if n > fork.last() {
            continue;
        }

        if n > max_n {
            max_n = n;
        }

        if fork.get(n)?.has_signature() {
            let sig = fork.signature_at_n(n, &ctx.keys).to_vec();
            vote_sigs.push(ProtoSignatureArrayEntry { n, sig });
        }

        fork.inc_replication_vote(&ctx.config.net_config.name, n)?;
    }

    Ok(ProtoVote {
        sig_array: vote_sigs,
        fork_digest: fork.hash_at_n(max_n).unwrap_or(vec![0u8; DIGEST_LENGTH]),
        n: max_n,
        view: 1,
    })
}

pub fn do_commit(
    ctx: &PinnedServerContext,
    fork: &mut MutexGuard<Log>,
    lack_pend: &mut MutexGuard<HashMap<(u64, usize), (MsgAckChan, LatencyProfile)>>,
    n: u64,
    ci: u64,
) {
    if n <= ci {
        return;
    }

    ctx.state.commit_index.store(n, Ordering::SeqCst);
    let mut del_list = Vec::new();
    for ((bn, txn), chan) in lack_pend.iter() {
        if *bn <= n {
            let h = hash(&fork.get(*bn).unwrap().block.tx[*txn]);

            let response = ProtoClientReply {
                req_digest: h,
                block_n: (*bn) as u64,
                tx_n: (*txn) as u64,
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
            let _ = chan.0.send((msg, profile));
            del_list.push((*bn, *txn));
        }
    }
    for d in del_list {
        lack_pend.remove(&d);
    }
}

pub fn do_create_qcs(_ctx: &PinnedServerContext, _fork: &mut MutexGuard<Log>, qcs: &Vec<u64>) {
    for n in qcs {
        if n % 1000 == 0 {
            info!("QC formed for index: {}", n);
        } else {
            debug!("QC formed for index: {}", n);
        }
    }

    // @todo
}

pub async fn do_process_vote(
    ctx: PinnedServerContext,
    vote: &ProtoVote,
    sender: &String,
    majority: u64,
    super_majority: u64,
) -> Result<(), Error> {
    let mut fork = ctx.state.fork.lock().await;
    let ci = ctx.state.commit_index.load(Ordering::SeqCst);
    let mut updated_ci = ci;

    // A vote at n is considered a vote at 1..n
    // So we should increase replication count for anything >= ci
    for i in (ci + 1)..(vote.n + 1) {
        if i > fork.last() {
            break;
        }
        if fork.inc_replication_vote(sender, i)? >= majority {
            updated_ci = i;
        }
    }

    let mut qcs = Vec::new();

    for vote_sig in &vote.sig_array {
        if vote_sig.n > fork.last() {
            continue;
        }
        // Try to increase the signature after verifying against my own fork.
        match fork.inc_qc_sig(sender, &vote_sig.sig, vote_sig.n, &ctx.keys) {
            Ok(total_sigs) => {
                if total_sigs >= super_majority {
                    qcs.push(vote_sig.n);
                }
            }
            Err(e) => {
                warn!("Signature error: {}", e);
            }
        }
    }

    {
        let mut lack_pend = ctx.client_ack_pending.lock().await;
        do_commit(&ctx, &mut fork, &mut lack_pend, updated_ci, ci);
    }

    do_create_qcs(&ctx, &mut fork, &qcs);

    Ok(())
}

pub async fn process_node_request(
    ctx: &PinnedServerContext,
    client: &PinnedClient,
    node_num: u64,
    // num_txs: &mut usize,
    majority: u64,
    super_majority: u64,
    // accepting_client_requests: &mut bool,
    ms: &mut ForwardedMessage,
    ms_batch_size: usize,
) -> Result<(), Error> {
    let (msg, sender, profile) = ms;
    let _sender = sender.clone();
    match &msg {
        crate::consensus::proto::rpc::proto_payload::Message::AppendEntries(ae) => {
            profile.register("AE chan wait");
            if !ctx.i_am_leader.load(Ordering::SeqCst) {
                if !_sender.eq(&get_leader_str(&ctx)) {
                    return Ok(());
                }
                // Only take action on this if I am follower
                let mut vote_sigs = Vec::new();

                if let Some(f) = &ae.fork {
                    let (last_n, updated_last_n, digest) = {
                        let mut fork = ctx.state.fork.lock().await;
                        let last_n = fork.last();
                        for b in &f.blocks {
                            debug!("Inserting block {} with {} txs", b.n, b.tx.len());
                            let mut entry = LogEntry::new(b.clone());
                            entry.replication_votes.insert(get_leader_str(&ctx));

                            let res = if entry.has_signature() {
                                let res =
                                    fork.verify_and_push(entry, &ctx.keys, &get_leader_str(&ctx));
                                if let Ok(_n) = res {
                                    let vote_sig = fork.last_signature(&ctx.keys);
                                    vote_sigs.push(ProtoSignatureArrayEntry {
                                        n: _n,
                                        sig: vote_sig.into(),
                                    });
                                }
                                res
                            } else {
                                fork.push(entry)
                            };
                            if let Err(e) = res {
                                warn!("Error appending block: {}", e);
                                continue;
                            }
                            debug!("Pushing complete!");
                        }
                        let updated_last_n = fork.last();

                        (last_n, updated_last_n, fork.last_hash())
                    };
                    profile.register("Fork push");

                    if updated_last_n > last_n {
                        // New block has been added. Vote for the last one.
                        let vote = ProtoVote {
                            sig_array: vote_sigs,
                            fork_digest: digest,
                            view: 1,
                            n: updated_last_n,
                        };
                        debug!("Voted for {}", vote.n);

                        let rpc_msg_body = ProtoPayload {
                            rpc_type: rpc::RpcType::FastQuorumReply.into(),
                            rpc_seq_num: ctx.last_fast_quorum_request.load(Ordering::SeqCst),
                            message: Some(consensus::proto::rpc::proto_payload::Message::Vote(
                                vote,
                            )),
                        };

                        profile.register("Prepare vote");

                        let mut buf = Vec::new();
                        if let Ok(_) = rpc_msg_body.encode(&mut buf) {
                            // let reply = MessageRef(&buf, buf.len(), &crate::rpc::SenderType::Anon);
                            let sz = buf.len();
                            let reply = PinnedMessage::from(buf, sz, crate::rpc::SenderType::Anon);
                            let mut profile = LatencyProfile::new();
                            if updated_last_n % 1000 == 0 {
                                profile.should_print = true;
                                profile.prefix =
                                    String::from(format!("Vote for block {}", updated_last_n));
                            }
                            let _ = PinnedClient::broadcast(
                                &client,
                                &vec![_sender.clone()],
                                &reply,
                                &mut profile,
                            )
                            .await;
                        }
                        debug!("Sent vote");
                        profile.register("Sent vote");

                        if updated_last_n % 1000 == 0 {
                            profile.should_print = true;
                            profile.prefix = String::from(format!("Block: {}", updated_last_n));
                            profile.print();
                        }
                    }
                }

                let fork = ctx.state.fork.lock().await;
                let ci = ctx.state.commit_index.load(Ordering::SeqCst);
                if ae.commit_index > ci {
                    ctx.state
                        .commit_index
                        .store(ae.commit_index, Ordering::SeqCst);

                    ctx.state.num_committed_txs.fetch_add(
                        fork.get(ae.commit_index).unwrap().block.tx.len(),
                        Ordering::SeqCst,
                    );
                    {
                        let mut lack_pend = ctx.client_ack_pending.lock().await;
                        let mut del_list = Vec::new();
                        for ((bn, txn), mut chan) in lack_pend.iter() {
                            if *bn <= ae.commit_index {
                                let h = hash(&fork.get(*bn).unwrap().block.tx[*txn]);

                                let response = ProtoClientReply {
                                    req_digest: h,
                                    block_n: (*bn) as u64,
                                    tx_n: (*txn) as u64,
                                };

                                let v = response.encode_to_vec();
                                let vlen = v.len();

                                let msg =
                                    PinnedMessage::from(v, vlen, crate::rpc::SenderType::Anon);

                                let mut profile = chan.1.clone();
                                profile.register("Init Sending Client Response");
                                if *bn % 1000 == 0 {
                                    profile.should_print = true;
                                    profile.prefix =
                                        String::from(format!("Block: {}, Txn: {}", *bn, *txn));
                                }
                                let _ = chan.0.send((msg, profile));
                                del_list.push((*bn, *txn));
                            }
                        }
                        for d in del_list {
                            lack_pend.remove(&d);
                        }
                    }
                    debug!(
                        "New Commit Index: {}, Fork Digest: {} Tx: {}, num_txs: {}",
                        ctx.state.commit_index.load(Ordering::SeqCst),
                        fork.last_hash().encode_hex::<String>(),
                        String::from_utf8(fork.get(ae.commit_index).unwrap().block.tx[0].clone())
                            .unwrap(),
                        ctx.state.num_committed_txs.load(Ordering::SeqCst)
                    );
                }
            }
        }
        crate::consensus::proto::rpc::proto_payload::Message::Vote(v) => {
            profile.register("Vote chan wait");
            let mut fork = ctx.state.fork.lock().await;
            if v.n > fork.last() {
                return Ok(()); // todo: Actually this is an error.
            }
            let chk_hsh = fork.hash_at_n(v.n);

            // Check all signatures in the vote.
            let mut all_sig_verified = true;
            'check_votes: for vote_sig in &v.sig_array {
                if vote_sig.n > fork.last() {
                    continue;
                }

                match fork.inc_qc_sig(&_sender, &vote_sig.sig, vote_sig.n, &ctx.keys) {
                    Err(e) => {
                        warn!("Vote signature error: {}", e);
                        all_sig_verified = false;
                        break 'check_votes;
                    }
                    Ok(total_qc_votes) => {
                        if total_qc_votes >= super_majority {
                            if vote_sig.n % 1000 == 0 {
                                info!("QC formed for index: {}", vote_sig.n);
                            } else {
                                debug!("QC formed for index: {}", vote_sig.n);
                            }
                            // Move from byz_qc_pending to next_qc_list and byz_commit_pending
                        }
                    }
                }
            }

            profile.register("Vote signature checks");
            if v.n % 1000 == 0 {
                profile.should_print = true;
                profile.prefix = String::from(format!("Vote for Block {} from {}", v.n, _sender));
                profile.print();
            }

            if chk_hsh.is_some() && cmp_hash(&chk_hsh.unwrap(), &v.fork_digest) && all_sig_verified
            {
                let _l = v.n;
                let _f = ctx.state.commit_index.load(Ordering::SeqCst) + 1;
                for i in _f..(_l + 1) {
                    let total_votes = fork.inc_replication_vote(&_sender, i).unwrap();
                    if total_votes >= majority {
                        let ci = ctx.state.commit_index.load(Ordering::SeqCst);
                        if ci < v.n && v.n <= fork.last()
                        // This is just a sanity check
                        {
                            ctx.state.commit_index.store(i, Ordering::SeqCst);
                            ctx.state
                                .num_committed_txs
                                .fetch_add(fork.get(i).unwrap().block.tx.len(), Ordering::SeqCst);
                            debug!(
                                "New Commit Index: {}, Fork Digest: {} Tx: {}, num_txs: {}, vote_batch_size: {}",
                                ctx.state.commit_index.load(Ordering::SeqCst),
                                v.fork_digest.encode_hex::<String>(),
                                String::from_utf8(fork.get(i).unwrap().block.tx[0].clone())
                                    .unwrap(),
                                ctx.state.num_committed_txs.load(Ordering::SeqCst),
                                ms_batch_size
                            );

                            // *accepting_client_requests = true;
                            ctx.last_fast_quorum_request.fetch_add(1, Ordering::SeqCst);
                            if i % 1000 == 0 {
                                let mut lpings = ctx.ping_counters.lock().unwrap();
                                if let Some(start) = lpings.remove(&i) {
                                    info!(
                                        "Fork index: {} Vote quorum latency: {} us",
                                        v.n,
                                        start.elapsed().as_micros()
                                    )
                                }
                            }

                            {
                                let mut lack_pend = ctx.client_ack_pending.lock().await;
                                let mut del_list = Vec::new();
                                for ((bn, txn), chan) in lack_pend.iter() {
                                    if *bn <= i {
                                        let hash_time = Instant::now();
                                        let h = hash(&fork.get(*bn).unwrap().block.tx[*txn]);
                                        if *bn % 1000 == 0 {
                                            info!(
                                                "Hash time: {} us",
                                                hash_time.elapsed().as_micros()
                                            );
                                        }
                                        let response = ProtoClientReply {
                                            req_digest: h,
                                            block_n: (*bn) as u64,
                                            tx_n: (*txn) as u64,
                                        };

                                        let v = response.encode_to_vec();
                                        let vlen = v.len();
                                        let msg = PinnedMessage::from(
                                            v,
                                            vlen,
                                            crate::rpc::SenderType::Anon,
                                        );

                                        let mut profile = chan.1.clone();
                                        profile.register("Init Sending Client Response");
                                        if *bn % 1000 == 0 {
                                            profile.should_print = true;
                                            profile.prefix = String::from(format!(
                                                "Block: {}, Txn: {}",
                                                *bn, *txn
                                            ));
                                        }

                                        let _r = chan.0.send((msg, profile));

                                        del_list.push((*bn, *txn));
                                    }
                                }
                                for d in del_list {
                                    lack_pend.remove(&d);
                                }
                            }
                        }
                    }
                }
            } else {
                info!("Bad vote received!");
            }
        }
        _ => {}
    }

    Ok(())
}

async fn handle_timeout(_ctx: &PinnedServerContext) -> Result<(), Error> {
    // if ctx.config.net_config.name == get_leader_str(&ctx) {
    //     ctx.i_am_leader.store(true, Ordering::SeqCst);
    // }else{
    //     ctx.i_am_leader.store(false, Ordering::SeqCst);
    // }

    // ctx.state.view.fetch_add(1, Ordering::SeqCst);
    // info!("Current pending acks: {}", _ctx.client_ack_pending.lock().await.len());
    Ok(())
}

pub async fn report_stats(ctx: &PinnedServerContext) -> Result<(), Error> {
    loop {
        sleep(Duration::from_secs(
            ctx.config.consensus_config.stats_report_secs,
        ))
        .await;
        {
            let fork = ctx.state.fork.lock().await;
            let lack_pend = ctx.client_ack_pending.lock().await;

            info!("fork.last = {}, commit_index = {}, byz_commit_index = {}, pending_acks = {}, num_txs = {}, fork.last_hash = {}",
                  fork.last(),
                  ctx.state.commit_index.load(Ordering::SeqCst),
                  ctx.state.byz_commit_index.load(Ordering::SeqCst),
                  lack_pend.len(),
                  ctx.state.num_committed_txs.load(Ordering::SeqCst),
                  fork.last_hash().encode_hex::<String>()
            );
        }
    }
}

async fn view_timer(tx: Sender<bool>, timeout: Duration) -> Result<(), Error> {
    loop {
        sleep(timeout).await;
        let _ = tx.send(true).await;
    }
}

pub async fn create_and_push_block(
    ctx: PinnedServerContext,
    reqs: &mut Vec<ForwardedMessageWithAckChan>,
    should_sign: bool,
) -> Result<(ProtoAppendEntries, LatencyProfile), Error> {
    let mut fork = ctx.state.fork.lock().await;

    let mut tx = Vec::new();
    let block_n = fork.last() + 1;
    let mut profile = LatencyProfile::new();

    {
        let mut lack_pend = ctx.client_ack_pending.lock().await;
        for (ms, _sender, chan, profile) in reqs {
            profile.register("Client channel recv");

            if let crate::consensus::proto::rpc::proto_payload::Message::ClientRequest(req) = ms {
                tx.push(req.tx.clone());
                lack_pend.insert((block_n, tx.len() - 1), (chan.clone(), profile.to_owned()));
            }
        }
    }

    let block = ProtoBlock {
        tx,
        n: block_n,
        parent: fork.last_hash(),
        view: 1,
        qc: Vec::new(),
        sig: Some(Sig::NoSig(DefferedSignature {})),
    };

    let entry = LogEntry::new(block);

    // entry
    //     .replication_votes
    //     .insert(ctx.config.net_config.name.clone());

    let res = match should_sign {
        true => {
            // pending_signatures = 0;
            // signature_timer.reset();
            fork.push_and_sign(entry, &ctx.keys)
        }
        false => fork.push(entry),
    };

    match res {
        Ok(n) => {
            debug!("Client message sequenced at {} {}", n, block_n);
            if n % 1000 == 0 {
                let mut lpings = ctx.ping_counters.lock().unwrap();
                lpings.insert(n, Instant::now());
            }
            profile.register("Block create");
        }
        Err(e) => {
            warn!("Error processing client request: {}", e);
            return Err(e);
        }
    }

    let ae = ProtoAppendEntries {
        fork: Some(ProtoFork {
            blocks: vec![fork.get(fork.last()).unwrap().block.clone()],
        }),
        commit_index: ctx.state.commit_index.load(Ordering::SeqCst),
        byz_commit_index: 0,
        view: 1,
    };

    Ok((ae, profile))
}

pub async fn broadcast_append_entries(
    ctx: PinnedServerContext,
    client: PinnedClient,
    ae: ProtoAppendEntries,
    send_list: &Vec<String>,
    mut profile: LatencyProfile,
) -> Result<(), Error> {
    if send_list.len() == 0 {
        return Ok(());
    }

    let mut buf = Vec::new();
    let block_n = ae.fork.as_ref().unwrap().blocks.len();
    let block_n = ae.fork.as_ref().unwrap().blocks[block_n - 1].n;

    let rpc_msg_body = ProtoPayload {
        rpc_type: rpc::RpcType::FastQuorumRequest.into(),
        rpc_seq_num: ctx.last_fast_quorum_request.load(Ordering::SeqCst),
        message: Some(consensus::proto::rpc::proto_payload::Message::AppendEntries(ae)),
    };

    rpc_msg_body.encode(&mut buf)?;
    let sz = buf.len();
    let bcast_msg = PinnedMessage::from(buf, sz, crate::rpc::SenderType::Anon);
    if block_n % 1000 == 0 {
        profile.should_print = true;
        profile.prefix = String::from(format!("AppendEntries Block {}", block_n));
    }
    let start_bcast = Instant::now();
    let _ = PinnedClient::broadcast(&client, &send_list, &bcast_msg, &mut profile).await;
    if block_n % 1000 == 0 {
        info!(
            "AppendEntries Block: {}, Broadcast time: {} us",
            block_n,
            start_bcast.elapsed().as_micros()
        );
    }

    Ok(())
}

pub async fn do_append_entries(
    ctx: PinnedServerContext,
    client: PinnedClient,
    reqs: &mut Vec<ForwardedMessageWithAckChan>,
    should_sign: bool,
    send_list: &Vec<String>,
    majority: u64,
    super_majority: u64,
) -> Result<(), Error> {
    // Create the block, holding a lock on the fork state.
    let (ae, profile) = create_and_push_block(ctx.clone(), reqs, should_sign).await?;

    // Vote for self; necessary since the network subsystem doesn't send my message to me.
    let block_n = ae.fork.as_ref().unwrap().blocks.len();
    let block_n = ae.fork.as_ref().unwrap().blocks[block_n - 1].n;
    let my_vote = create_vote_for_blocks(ctx.clone(), &vec![block_n]).await?;
    do_process_vote(
        ctx.clone(),
        &my_vote,
        &ctx.config.net_config.name,
        majority,
        super_majority,
    )
    .await?;

    // Lock on the fork is not kept when broadcasting.
    broadcast_append_entries(ctx, client, ae, send_list, profile).await?;

    Ok(())
}

pub async fn handle_client_messages(
    ctx: PinnedServerContext,
    client: PinnedClient,
) -> Result<(), Error> {
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
    let mut signature_timer = ResettableTimer::new(Duration::from_millis(
        ctx.config.consensus_config.signature_max_delay_ms,
    ));

    let mut pending_signatures = 0;

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

        if !ctx.i_am_leader.load(Ordering::SeqCst) {
            // todo:: Change this to reply to client with the current leader.
            curr_client_req.clear();
            continue;
        }

        pending_signatures += 1;
        let should_sig = signature_timer_tick    // Either I am running this body because of signature timeout.
            || (pending_signatures >= ctx.config.consensus_config.signature_max_delay_blocks);
        // Or I actually got some transactions and I really need to sign

        // Ok I am the leader.

        do_append_entries(
            ctx.clone(),
            client.clone(),
            &mut curr_client_req,
            should_sig,
            &send_list,
            majority,
            super_majority
        )
        .await?;

        if curr_client_req_num == 0 && signature_timer_tick == false {
            // Channels are all closed.
            return Ok(());
        }
        curr_client_req.clear();
        curr_client_req_num = 0;
        signature_timer_tick = false;
    }
}

pub async fn handle_node_messages(
    ctx: PinnedServerContext,
    client: PinnedClient,
) -> Result<(), Error> {
    let (timer_tx, mut timer_rx) = mpsc::channel(1);
    let timer_handle = tokio::spawn(async move {
        let _ = view_timer(timer_tx, Duration::from_secs(1)).await;
    });

    let mut node_rx = ctx.0.node_queue.1.lock().await;
    let majority = get_majority_num(&ctx);
    let super_majority = get_super_majority_num(&ctx);
    let send_list = get_everyone_except_me(
        &ctx.config.net_config.name,
        &ctx.config.consensus_config.node_list,
    );

    debug!(
        "Leader: {}, Send List: {:?}",
        ctx.i_am_leader.load(Ordering::SeqCst),
        &send_list
    );

    // let mut accepting_client_requests = true;
    let mut curr_node_req = Vec::new();
    let mut curr_timer_val = false;
    let mut node_req_num = 0;

    // let mut num_txs = 0;

    loop {
        tokio::select! {
            biased;
            v = timer_rx.recv() => { curr_timer_val = v.unwrap(); }
            node_req_num_ = node_rx.recv_many(&mut curr_node_req, (majority - 1) as usize) => node_req_num = node_req_num_,
        }

        if curr_timer_val {
            handle_timeout(&ctx).await?;
        }

        if node_req_num > 0 {
            for req in &mut curr_node_req {
                process_node_request(
                    &ctx,
                    &client,
                    get_node_num(&ctx),
                    // &mut num_txs,
                    majority.clone(),
                    super_majority.clone(),
                    // &mut accepting_client_requests,
                    req,
                    node_req_num,
                )
                .await?;
            }
        }

        if curr_timer_val == false && node_req_num == 0 {
            warn!("Consensus node dying!");
            break; // Select failed because both channels were closed!
        }

        // Reset for the next iteration
        curr_node_req.clear();
        node_req_num = 0;
        curr_timer_val = false;
    }
    timer_handle.abort();
    let _ = tokio::join!(timer_handle);
    Ok(())
}

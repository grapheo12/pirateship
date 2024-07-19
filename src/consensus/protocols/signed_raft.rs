use std::{collections::HashSet, io::Error, sync::atomic::Ordering, time::{Duration, Instant}};
use hex::ToHex;
use log::{debug, info, warn};
use prost::Message;
use tokio::{sync::mpsc::{self, Sender}, time::sleep};

use crate::{
    consensus::{
        self,
        handler::PinnedServerContext,
        leader_rotation::get_current_leader,
        log::LogEntry,
        proto::{
            consensus::{
                proto_block::Sig, DefferedSignature, ProtoAppendEntries, ProtoBlock, ProtoFork,
                ProtoVote,
            },
            rpc::{self, proto_payload, ProtoPayload},
        }, timer::ResettableTimer,
    },
    crypto::{cmp_hash, hash, DIGEST_LENGTH},
    rpc::{client::PinnedClient, MessageRef, PinnedMessage},
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

fn get_everyone_except_me(my_name: &String, node_list: &Vec<String>) -> Vec<String> {
    node_list
        .iter()
        .map(|n| n.clone())
        .filter(|name| !name.eq(my_name))
        .collect()
}

async fn process_node_request(
    ctx: &PinnedServerContext,
    client: &PinnedClient,
    node_num: u64,
    // num_txs: &mut usize,
    majority: u64,
    // accepting_client_requests: &mut bool,
    ms: &(proto_payload::Message, String),
    ms_batch_size: usize,
) -> Result<(), Error> {
    let (msg, _sender) = ms;
    match &msg {
        crate::consensus::proto::rpc::proto_payload::Message::AppendEntries(ae) => {
            if !ctx.i_am_leader.load(Ordering::SeqCst) {
                if !_sender.eq(&get_leader_str(&ctx)) {
                    return Ok(());
                }
                // Only take action on this if I am follower

                if let Some(f) = &ae.fork {
                    let mut fork = ctx.state.fork.lock().await;
                    let last_n = fork.last();
                    for b in &f.blocks {
                        debug!("Inserting block {} with {} txs", b.n, b.tx.len());
                        let mut entry = LogEntry {
                            block: b.clone(),
                            replication_votes: HashSet::new(),
                        };
                        entry.replication_votes.insert(get_leader_str(&ctx));

                        let res = if entry.has_signature() {
                            fork.verify_and_push(entry, &ctx.keys, &get_leader_str(&ctx))
                        }else{
                            fork.push(entry)
                        };
                        if let Err(e) = res {
                            warn!("Error appending block: {}", e);
                            continue;
                        }
                        debug!("Pushing complete!");
                    }
                    if fork.last() > last_n {
                        // New block has been added. Vote for the last one.
                        let vote = ProtoVote {
                            sig_array: Vec::new(),
                            fork_digest: fork.last_hash(),
                            view: 1,
                            n: fork.last(),
                        };
                        debug!("Voted for {}", vote.n);

                        let rpc_msg_body = ProtoPayload {
                            rpc_type: rpc::RpcType::FastQuorumReply.into(),
                            rpc_seq_num: ctx.last_fast_quorum_request.load(Ordering::SeqCst),
                            message: Some(consensus::proto::rpc::proto_payload::Message::Vote(
                                vote,
                            )),
                        };

                        let mut buf = Vec::new();
                        if let Ok(_) = rpc_msg_body.encode(&mut buf) {
                            let reply = MessageRef(&buf, buf.len(), &crate::rpc::SenderType::Anon);
                            let _ = PinnedClient::reliable_send(&client, &_sender, reply).await;
                        }
                        debug!("Sent vote");
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
                        Ordering::SeqCst);
                    {
                        let mut lack_pend = ctx.client_ack_pending.lock().await;
                        let mut del_list = Vec::new();
                        for ((bn, txn), chan) in lack_pend.iter() {
                            if *bn <= ae.commit_index {
                                let h = hash(&fork.get(*bn).unwrap().block.tx[*txn]);
                                let msg = PinnedMessage::from(h, DIGEST_LENGTH, 
                                    crate::rpc::SenderType::Anon);
                                let _ = chan.send(msg).await;
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
                        String::from_utf8(
                            fork.get(ae.commit_index).unwrap().block.tx[0].clone()
                        )
                        .unwrap(),
                        ctx.state.num_committed_txs.load(Ordering::SeqCst)
                    );
                }
            }
        }
        crate::consensus::proto::rpc::proto_payload::Message::Vote(v) => {
            let mut fork = ctx.state.fork.lock().await;
            let chk_hsh = fork.hash_at_n(v.n);
            
            if chk_hsh.is_some() && cmp_hash(&chk_hsh.unwrap(), &v.fork_digest) {
                let _l = v.n;
                let _f = ctx.state.commit_index.load(Ordering::SeqCst) + 1;
                for i in _f..(_l + 1){
                    let total_votes = fork.inc_replication_vote(_sender, i).unwrap();
                    if total_votes >= majority {
                        let ci = ctx.state.commit_index.load(Ordering::SeqCst);
                        if ci < v.n && v.n <= fork.last()
                        // This is just a sanity check
                        {
                            ctx.state.commit_index.store(i, Ordering::SeqCst);
                            ctx.state.num_committed_txs.fetch_add(fork.get(i).unwrap().block.tx.len(), Ordering::SeqCst);
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
                            if v.n % 1000 == 0 {
                                let mut lpings = ctx.ping_counters.lock().unwrap();
                                if let Some(start) = lpings.remove(&v.n){
                                    info!("Fork index: {} Vote quorum latency: {} us", v.n, start.elapsed().as_micros())
                                }
                            }

                            {
                                let mut lack_pend = ctx.client_ack_pending.lock().await;
                                let mut del_list = Vec::new();
                                for ((bn, txn), chan) in lack_pend.iter() {
                                    if *bn <= i {
                                        let h = hash(&fork.get(*bn).unwrap().block.tx[*txn]);
                                        let msg = PinnedMessage::from(h, DIGEST_LENGTH, 
                                            crate::rpc::SenderType::Anon);
                                        let _r = chan.send(msg).await;
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
            }else{
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
        sleep(Duration::from_secs(ctx.config.consensus_config.stats_report_secs)).await;
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



pub async fn handle_client_messages(ctx: PinnedServerContext, client: PinnedClient) -> Result<(), Error> {
    let mut client_rx = ctx.0.client_queue.1.lock().await;
    let mut curr_client_req = Vec::new();
    let mut curr_client_req_num = 0;
    let mut signature_timer_tick = false;

    let majority = get_majority_num(&ctx);
    let send_list = get_everyone_except_me(
        &ctx.config.net_config.name,
        &ctx.config.consensus_config.node_list,
    );

    // Signed block logic: Either this timer expires.
    // Or the number of blocks crosses signature_max_delay_blocks.
    // In the later case, reset the timer.
    let mut signature_timer = ResettableTimer::new(
        Duration::from_millis(ctx.config.consensus_config.signature_max_delay_ms)
    );

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

        // Ok I am the leader.
        let mut tx = Vec::new();
        let block_n = {
            let fork = ctx.state.fork.lock().await;
            fork.last() + 1
        };
        if curr_client_req_num > 0 { // If I am here due to timeout, don't worry about adding txs.
            let mut lack_pend = ctx.client_ack_pending.lock().await;
            for (ms, _sender, chan) in &curr_client_req {
                if let crate::consensus::proto::rpc::proto_payload::Message::ClientRequest(req) = ms
                {
                    tx.push(req.tx.clone());
                    lack_pend.insert((block_n, tx.len() - 1), chan.clone());
                }
            }
        }

        pending_signatures += 1;
        let should_sig = signature_timer_tick    // Either I am running this body because of signature timeout.
            || (pending_signatures >= ctx.config.consensus_config.signature_max_delay_blocks);
            // Or I actually got some transactions and I really need to sign
        
        let mut fork = ctx.state.fork.lock().await;
        let block = ProtoBlock {
            tx,
            n: block_n,
            parent: fork.last_hash(),
            view: 1,
            qc: Vec::new(),
            sig: Some(Sig::NoSig(DefferedSignature {})),
        };

        let mut entry = LogEntry {
            block,
            replication_votes: HashSet::new(),
        };

        entry
            .replication_votes
            .insert(ctx.config.net_config.name.clone());

        let res = match should_sig {
            true => {
                pending_signatures = 0;
                signature_timer.reset();   
                fork.push_and_sign(entry, &ctx.keys)
            },
            false => fork.push(entry),
        };

        match res {
            Ok(n) => {
                debug!("Client message sequenced at {} {}", n, block_n);
                if n % 1000 == 0{
                    let mut lpings = ctx.ping_counters.lock().unwrap();
                    lpings.insert(n, Instant::now());
                }
            }
            Err(e) => {
                warn!("Error processing client request: {}", e);
            }
        }

        if majority == 1 {
            let ci = ctx.state.commit_index.load(Ordering::SeqCst);
            if ci < fork.last() {
                ctx.state.commit_index.store(fork.last(), Ordering::SeqCst);
                {
                    let mut lack_pend = ctx.client_ack_pending.lock().await;
                    let mut del_list = Vec::new();
                    for ((bn, txn), chan) in lack_pend.iter() {
                        if *bn <= fork.last() {
                            let h = hash(&fork.get(*bn).unwrap().block.tx[*txn]);
                            let msg = PinnedMessage::from(h, DIGEST_LENGTH, 
                                crate::rpc::SenderType::Anon);
                            let _ = chan.send(msg).await;
                            del_list.push((*bn, *txn));
                        }
                    }
                    for d in del_list {
                        lack_pend.remove(&d);
                    }

                }
                ctx.state.num_committed_txs.fetch_add(
                    fork.get(fork.last()).unwrap().block.tx.len(),
                    Ordering::SeqCst);
                debug!(
                    "New Commit Index: {}, Fork Digest: {} Tx: {} num_txs: {}",
                    ctx.state.commit_index.load(Ordering::SeqCst),
                    fork.last_hash().encode_hex::<String>(),
                    String::from_utf8(
                        fork.get(fork.last()).unwrap().block.tx[0].clone()
                    )
                    .unwrap(),
                    ctx.state.num_committed_txs.load(Ordering::SeqCst)
                );
            }
        } else {
            // accepting_client_requests = false; // Finish replicating this request before processing the next.

            let ae = ProtoAppendEntries {
                fork: Some(ProtoFork {
                    blocks: vec![fork.get(fork.last()).unwrap().block.clone()],
                }),
                commit_index: ctx.state.commit_index.load(Ordering::SeqCst),
                byz_commit_index: 0,
                view: 1,
            };

            // ctx.last_fast_quorum_request.fetch_add(1, Ordering::SeqCst);

            let rpc_msg_body = ProtoPayload {
                rpc_type: rpc::RpcType::FastQuorumRequest.into(),
                rpc_seq_num: ctx.last_fast_quorum_request.load(Ordering::SeqCst),
                message: Some(
                    consensus::proto::rpc::proto_payload::Message::AppendEntries(ae),
                ),
            };

            let mut buf = Vec::new();
            if let Ok(_) = rpc_msg_body.encode(&mut buf) {
                let sz = buf.len();
                let bcast_msg = PinnedMessage::from(buf, sz, crate::rpc::SenderType::Anon);

                let start_bcast = Instant::now();
                let _ = PinnedClient::broadcast(&client, &send_list, &bcast_msg).await;
                debug!("Broadcast time: {} us", start_bcast.elapsed().as_micros());
            }
        }

        curr_client_req.clear();
        curr_client_req_num = 0;
        signature_timer_tick = false;
    }

}

pub async fn handle_node_messages(ctx: PinnedServerContext, client: PinnedClient) -> Result<(), Error> {
    let (timer_tx, mut timer_rx) = mpsc::channel(1);
    let timer_handle = tokio::spawn(async move {
        let _ = view_timer(timer_tx, Duration::from_secs(1)).await;
    });

    let mut node_rx = ctx.0.node_queue.1.lock().await;
    let majority = get_majority_num(&ctx);
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
            for req in &curr_node_req {
                process_node_request(
                    &ctx,
                    &client,
                    get_node_num(&ctx),
                    // &mut num_txs,
                    majority.clone(),
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

use std::{collections::HashSet, io::Error, sync::atomic::Ordering};

use hex::ToHex;
use log::{debug, info, warn};
use prost::Message;

use tokio::time::Instant;

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
        },
    },
    crypto::cmp_hash,
    rpc::{client::PinnedClient, MessageRef, PinnedMessage},
};

fn get_leader_str(ctx: &PinnedServerContext) -> String {
    ctx.config.consensus_config.node_list
        [get_current_leader(ctx.config.consensus_config.node_list.len() as u64, 1)]
    .clone()
}

fn get_node_num(ctx: &PinnedServerContext) -> u64 {
    let mut i = 0;
    for name in &ctx.config.consensus_config.node_list {
        if name.eq(&ctx.config.net_config.name){
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
    num_txs: &mut usize,
    majority: u64,
    accepting_client_requests: &mut bool,
    ms: &(proto_payload::Message, String),
    ms_batch_size: usize
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
                        let entry = LogEntry {
                            block: b.clone(),
                            replication_votes: HashSet::new(),
                        };
                        if let Err(_) = fork.push(entry) {
                            continue;
                        }
                    }
                    if fork.last() > last_n {
                        // New block has been added. Vote for the last one.
                        let vote = ProtoVote {
                            sig_array: Vec::new(),
                            fork_digest: fork.last_hash(),
                            view: 1,
                            n: fork.last(),
                        };

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
                    }
                }

                let fork = ctx.state.fork.lock().await;
                if ae.commit_index > ctx.state.commit_index.load(Ordering::SeqCst) {
                    ctx.state
                        .commit_index
                        .store(ae.commit_index, Ordering::SeqCst);

                    *num_txs += fork.get(ae.commit_index).unwrap().block.tx.len();

                    if ae.commit_index % 1000 == node_num {

                        info!(
                            "New Commit Index: {}, Fork Digest: {} Tx: {}, num_txs: {}",
                            ctx.state.commit_index.load(Ordering::SeqCst),
                            fork.last_hash().encode_hex::<String>(),
                            String::from_utf8(fork.get(ae.commit_index).unwrap().block.tx[0].clone())
                                .unwrap(),
                            *num_txs
                        );
                    }
                }
            }
        }
        crate::consensus::proto::rpc::proto_payload::Message::Vote(v) => {
            let mut fork = ctx.state.fork.lock().await;
            if cmp_hash(&fork.last_hash(), &v.fork_digest) {
                let _l = v.n;
                let total_votes = fork.inc_replication_vote(_sender, _l).unwrap();
                if total_votes >= majority {
                    let ci = ctx.state.commit_index.load(Ordering::SeqCst);
                    if ci < v.n && v.n <= fork.last()
                    // This is just a sanity check
                    {
                        ctx.state.commit_index.store(v.n, Ordering::SeqCst);
                        *num_txs += fork.get(v.n).unwrap().block.tx.len();
                        if v.n % 1000 == 0 {
                            info!(
                                "New Commit Index: {}, Fork Digest: {} Tx: {}, num_txs: {}, vote_batch_size: {}",
                                ctx.state.commit_index.load(Ordering::SeqCst),
                                v.fork_digest.encode_hex::<String>(),
                                String::from_utf8(fork.get(v.n).unwrap().block.tx[0].clone())
                                    .unwrap(),
                                num_txs,
                                ms_batch_size
                            );
                        }

                        *accepting_client_requests = true;
                    }
                }
            }
        }
        _ => {}
    }

    Ok(())
}
pub async fn algorithm(ctx: PinnedServerContext, client: PinnedClient) -> Result<(), Error> {
    ctx.state.view.store(1, Ordering::SeqCst); // Never changes for this algorithm
    if ctx.config.net_config.name == get_leader_str(&ctx) {
        ctx.i_am_leader.store(true, Ordering::SeqCst);
    }
    let mut node_rx = ctx.0.node_queue.1.lock().await;
    let mut client_rx = ctx.0.client_queue.1.lock().await;
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

    let mut accepting_client_requests = true;
    let mut curr_node_req = Vec::new();
    let mut curr_client_req = Vec::new();
    let mut client_req_num = 0;
    let mut node_req_num = 0;

    let mut num_txs = 0;

    loop {
        if ctx.i_am_leader.load(Ordering::SeqCst) && accepting_client_requests {
            tokio::select! {
                biased;
                node_req_num_ = node_rx.recv_many(&mut curr_node_req, (majority - 1) as usize) => node_req_num = node_req_num_,
                client_req_num_ = client_rx.recv_many(&mut curr_client_req, 1000) => client_req_num = client_req_num_
            }
        } else {
            node_req_num = node_rx
                .recv_many(&mut curr_node_req, (majority - 1) as usize)
                .await;
        }

        if node_req_num > 0 {
            for req in &curr_node_req {
                process_node_request(
                    &ctx,
                    &client,
                    get_node_num(&ctx),
                    &mut num_txs,
                    majority.clone(),
                    &mut accepting_client_requests,
                    req, node_req_num
                )
                .await?;
            }
        }

        if client_req_num > 0 {
            let mut tx = Vec::new();
            for (ms, _sender) in &curr_client_req {
                if let crate::consensus::proto::rpc::proto_payload::Message::ClientRequest(req) = ms
                {
                    tx.push(req.tx.clone());
                }
            }

            if tx.len() > 0 {
                let mut fork = ctx.state.fork.lock().await;
                let block = ProtoBlock {
                    tx,
                    n: fork.last() + 1,
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

                match fork.push(entry) {
                    Ok(n) => {
                        if n % 1000 == 0 {
                            info!("Client message sequenced at {}", n);
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
                        num_txs += fork.get(fork.last()).unwrap().block.tx.len();
                        if fork.last() % 1000 == 0 {
                            info!(
                                "New Commit Index: {}, Fork Digest: {} Tx: {} num_txs: {}",
                                ctx.state.commit_index.load(Ordering::SeqCst),
                                fork.last_hash().encode_hex::<String>(),
                                String::from_utf8(
                                    fork.get(fork.last()).unwrap().block.tx[0].clone()
                                )
                                .unwrap(),
                                num_txs
                            );
                        }
                    }
                } else {
                    accepting_client_requests = false; // Finish replicating this request before processing the next.

                    let ae = ProtoAppendEntries {
                        fork: Some(ProtoFork {
                            blocks: vec![fork.get(fork.last()).unwrap().block.clone()],
                        }),
                        commit_index: ctx.state.commit_index.load(Ordering::SeqCst),
                        byz_commit_index: 0,
                        view: 1,
                    };

                    ctx.last_fast_quorum_request.fetch_add(1, Ordering::SeqCst);

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

                        // let start_bcast = Instant::now();
                        let _ = PinnedClient::broadcast(&client, &send_list, &bcast_msg).await;
                        // info!("Broadcast time: {} us", start_bcast.elapsed().as_micros());
                    }
                }
            }
        }

        if node_req_num == 0 && client_req_num == 0 {
            warn!("Consensus node dying!");
            break; // Select failed because both channels were closed!
        }

        // Reset for the next iteration
        curr_node_req.clear();
        curr_client_req.clear();
        client_req_num = 0;
        node_req_num = 0;
    }

    Ok(())
}

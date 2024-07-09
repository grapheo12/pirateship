use std::{collections::HashSet, io::Error, sync::atomic::Ordering};

use log::{info, warn};
use prost::Message;
use hex::ToHex;

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
            rpc::{self, ProtoPayload},
        },
    }, crypto::cmp_hash, rpc::{client::PinnedClient, MessageRef, PinnedMessage}
};

fn get_leader_str(ctx: &PinnedServerContext) -> String {
    ctx.config.consensus_config.node_list
        [get_current_leader(ctx.config.consensus_config.node_list.len() as u64, 1)]
    .clone()
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

pub async fn algorithm(ctx: PinnedServerContext, client: PinnedClient) -> Result<(), Error> {
    ctx.state.view.store(1, Ordering::SeqCst);    // Never changes for this algorithm
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


    let mut accepting_client_requests = true;
    let mut curr_node_req = None;
    let mut curr_client_req = None;
    loop {
        if ctx.i_am_leader.load(Ordering::SeqCst) && accepting_client_requests {
            tokio::select! {
                node_req = node_rx.recv() => curr_node_req = node_req,
                client_req = client_rx.recv() => curr_client_req = client_req
            }
        } else {
            curr_node_req = node_rx.recv().await;
        }

        if let Some(ms) = &curr_node_req {
            let (msg, _sender) = ms;
            match &msg {
                crate::consensus::proto::rpc::proto_payload::Message::AppendEntries(ae) => {
                    if !ctx.i_am_leader.load(Ordering::SeqCst) {
                        if !_sender.eq(&get_leader_str(&ctx)) {
                            continue;
                        }
                        // Only take action on this if I am follower
                        if ae.commit_index > ctx.state.commit_index.load(Ordering::SeqCst) {
                            ctx.state
                                .commit_index
                                .store(ae.commit_index, Ordering::SeqCst);
                        }

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
                                };

                                let rpc_msg_body = ProtoPayload {
                                    rpc_type: rpc::RpcType::FastQuorumReply.into(),
                                    rpc_seq_num: ctx.last_fast_quorum_request.load(Ordering::SeqCst),
                                    message: Some(consensus::proto::rpc::proto_payload::Message::Vote(vote)),
                                };

                                let mut buf = Vec::new();
                                if let Ok(_) = rpc_msg_body.encode(&mut buf) {
                                    let reply =
                                        MessageRef(&buf, buf.len(), &crate::rpc::SenderType::Anon);
                                    let _ =
                                        PinnedClient::reliable_send(&client, &_sender, reply).await;
                                }
                            }
                        }
                    }
                }
                crate::consensus::proto::rpc::proto_payload::Message::Vote(v) => {
                    let mut fork = ctx.state.fork.lock().await;
                    if cmp_hash(&fork.last_hash(), &v.fork_digest){
                        let _l = fork.last();
                        let total_votes = fork.inc_replication_vote(_sender, _l)
                            .unwrap();
                        if total_votes >= majority {
                            ctx.state.commit_index.store(fork.last(), Ordering::SeqCst);
                            info!("New Commit Index: {}, Tx Hash: {}",
                                ctx.state.commit_index.load(Ordering::SeqCst),
                                 v.fork_digest.encode_hex::<String>()
                            );
                        }
                    }
                },
                _ => {}
            }
        }

        if let Some(ms) = &curr_client_req {
            let (msg, _sender) = ms;
            match &msg {
                crate::consensus::proto::rpc::proto_payload::Message::ClientRequest(req) => {
                    let mut fork = ctx.state.fork.lock().await;
                    let block = ProtoBlock {
                        tx: vec![req.tx.clone()],
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
                            info!("Client message sequenced at {}", n);
                        }
                        Err(e) => {
                            warn!("Error processing client request: {}", e);
                        }
                    }

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
                        let _ = PinnedClient::broadcast(&client, &send_list, &bcast_msg, majority as i32);
                    }
                }
                _ => {}
            }
        }

        if curr_node_req.is_none() && curr_client_req.is_none() {
            warn!("Consensus node dying!");
            break;          // Select failed because both channels were closed!
        }

        // Reset for the next iteration
        curr_node_req = None;
        curr_client_req = None;
    }

    Ok(())
}

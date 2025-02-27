use std::{collections::HashMap, sync::Arc};

use log::{info, warn};
use prost::Message as _;
use tokio::{sync::{oneshot, Mutex}, task::JoinSet};

use crate::{config::AtomicConfig, crypto::HashType, proto::{client::{ProtoByzResponse, ProtoClientReply, ProtoTransactionReceipt}, execution::ProtoTransactionResult, rpc::ProtoPayload}, rpc::{server::LatencyProfile, PinnedMessage}, utils::channel::{Receiver, Sender}};

use super::batch_proposal::MsgAckChanWithTag;

pub enum ClientReplyCommand {
    CancelAllRequests,
    CrashCommitAck(HashMap<HashType, (u64, Vec<ProtoTransactionResult>)>),
    ByzCommitAck(HashMap<HashType, (u64, Vec<ProtoByzResponse>)>),
}

enum ReplyProcessorCommand {
    CrashCommit(u64 /* block_n */, u64 /* tx_n */, HashType, ProtoTransactionResult /* result */, MsgAckChanWithTag, Vec<ProtoByzResponse>),
    ByzCommit(u64 /* block_n */, u64 /* tx_n */, ProtoTransactionResult /* result */, MsgAckChanWithTag),
}
pub struct ClientReplyHandler {
    config: AtomicConfig,

    batch_rx: Receiver<(oneshot::Receiver<HashType>, Vec<MsgAckChanWithTag>)>,
    reply_command_rx: Receiver<ClientReplyCommand>,

    reply_map: HashMap<HashType, Vec<MsgAckChanWithTag>>,
    byz_reply_map: HashMap<HashType, Vec<(u64, String)>>,

    crash_commit_reply_buf: HashMap<HashType, (u64, Vec<ProtoTransactionResult>)>,
    byz_commit_reply_buf: HashMap<HashType, (u64, Vec<ProtoByzResponse>)>,

    byz_response_store: HashMap<String /* Sender */, Vec<ProtoByzResponse>>,

    reply_processors: JoinSet<()>,
    reply_processor_queue: (async_channel::Sender<ReplyProcessorCommand>, async_channel::Receiver<ReplyProcessorCommand>),
}

impl ClientReplyHandler {
    pub fn new(
        config: AtomicConfig,
        batch_rx: Receiver<(oneshot::Receiver<HashType>, Vec<MsgAckChanWithTag>)>,
        reply_command_rx: Receiver<ClientReplyCommand>,
    ) -> Self {
        let _chan_depth = config.get().rpc_config.channel_depth as usize;
        Self {
            config,
            batch_rx,
            reply_command_rx,
            reply_map: HashMap::new(),
            byz_reply_map: HashMap::new(),
            crash_commit_reply_buf: HashMap::new(),
            byz_commit_reply_buf: HashMap::new(),
            reply_processors: JoinSet::new(),
            reply_processor_queue: async_channel::bounded(_chan_depth),
            byz_response_store: HashMap::new(),
        }
    }

    pub async fn run(client_reply_handler: Arc<Mutex<Self>>) {
        let mut client_reply_handler = client_reply_handler.lock().await;
        for _ in 0..20 {
            let rx = client_reply_handler.reply_processor_queue.1.clone();
            client_reply_handler.reply_processors.spawn(async move {
                while let Ok(cmd) = rx.recv().await {
                    match cmd {
                        ReplyProcessorCommand::CrashCommit(block_n, tx_n, hsh, reply, (reply_chan, client_tag, _), byz_responses) => {
                            let reply = ProtoClientReply {
                                reply: Some(
                                    crate::proto::client::proto_client_reply::Reply::Receipt(
                                        ProtoTransactionReceipt {
                                            req_digest: hsh,
                                            block_n,
                                            tx_n,
                                            results: Some(reply),
                                            await_byz_response: true,
                                            byz_responses,
                                        },
                                )),
                                client_tag
                            };
                
                            let reply_ser = reply.encode_to_vec();
                            let _sz = reply_ser.len();
                            let reply_msg = PinnedMessage::from(reply_ser, _sz, crate::rpc::SenderType::Anon);
                            let latency_profile = LatencyProfile::new();
                            
                            let _ = reply_chan.send((reply_msg, latency_profile)).await;
                        },
                        ReplyProcessorCommand::ByzCommit(_, _, result, sender) => {

                        },
                    }
                }
            });
        }
        
        loop {
            if let Err(_) = client_reply_handler.worker().await {
                break;
            }
        }
    }

    async fn worker(&mut self) -> Result<(), ()> {
        tokio::select! {
            batch = self.batch_rx.recv() => {
                if batch.is_none() {
                    return Ok(());
                }

                let (batch_hash_chan, reply_vec) = batch.unwrap();
                let batch_hash = batch_hash_chan.await.unwrap();

                self.byz_reply_map.insert(batch_hash.clone(), reply_vec.iter().map(|(_, client_tag, sender)| (*client_tag, sender.clone())).collect());
                self.reply_map.insert(batch_hash.clone(), reply_vec);

                self.maybe_clear_reply_buf(batch_hash).await;
            },
            cmd = self.reply_command_rx.recv() => {
                if cmd.is_none() {
                    return Ok(());
                }

                let cmd = cmd.unwrap();

                self.handle_reply_command(cmd).await;
            },
        }
        Ok(())
    }

    async fn do_crash_commit_reply(&mut self, reply_sender_vec: Vec<MsgAckChanWithTag>, hash: HashType, n: u64, reply_vec: Vec<ProtoTransactionResult>) {
        assert_eq!(reply_sender_vec.len(), reply_vec.len());
        for (tx_n, ((reply_chan, client_tag, sender), reply)) in reply_sender_vec.into_iter().zip(reply_vec.into_iter()).enumerate() {
            let byz_responses = self.byz_response_store.remove(&sender).unwrap_or_default();
            
            self.reply_processor_queue.0.send(ReplyProcessorCommand::CrashCommit(n, tx_n as u64, hash.clone(), reply, (reply_chan, client_tag, sender), byz_responses)).await.unwrap();
        }
    }

    async fn do_byz_commit_reply(&mut self, reply_sender_vec: Vec<(u64, String)>, hash: HashType, n: u64, reply_vec: Vec<ProtoByzResponse>) {
        assert_eq!(reply_sender_vec.len(), reply_vec.len());
        for (tx_n, ((client_tag, sender), mut reply)) in reply_sender_vec.into_iter().zip(reply_vec.into_iter()).enumerate() {
            reply.client_tag = client_tag;
            match self.byz_response_store.get_mut(&sender) {
                Some(byz_responses) => {
                    byz_responses.push(reply);
                },
                None => {
                    self.byz_response_store.insert(sender, vec![reply]);
                }
            }
        }
    }

    async fn handle_reply_command(&mut self, cmd: ClientReplyCommand) {
        match cmd {
            ClientReplyCommand::CancelAllRequests => {
                self.reply_map.clear();
            },
            ClientReplyCommand::CrashCommitAck(crash_commit_ack) => {
                for (hash, (n, reply_vec)) in crash_commit_ack {
                    if let Some(reply_sender_vec) = self.reply_map.remove(&hash) {
                        self.do_crash_commit_reply(reply_sender_vec, hash, n, reply_vec).await;
                    } else {
                        // We received the reply before the request. Store it for later.
                        self.crash_commit_reply_buf.insert(hash, (n, reply_vec));
                    }
                }
            },
            ClientReplyCommand::ByzCommitAck(byz_commit_ack) => {
                for (hash, (n, reply_vec)) in byz_commit_ack {
                    if let Some(reply_sender_vec) = self.byz_reply_map.remove(&hash) {
                        self.do_byz_commit_reply(reply_sender_vec, hash, n, reply_vec).await;
                    } else {
                        self.byz_commit_reply_buf.insert(hash, (n, reply_vec));
                    }
                }
            },
        }
    }

    async fn maybe_clear_reply_buf(&mut self, batch_hash: HashType) {
        
        // Byz register must happen first. Otherwise when crash commit piggybacks the byz commit reply, it will be too late.
        if let Some((n, reply_vec)) = self.byz_commit_reply_buf.remove(&batch_hash) {
            if let Some(reply_sender_vec) = self.byz_reply_map.remove(&batch_hash) {
                self.do_byz_commit_reply(reply_sender_vec, batch_hash.clone(), n, reply_vec).await;
            }
        }

        if let Some((n, reply_vec)) = self.crash_commit_reply_buf.remove(&batch_hash) {
            if let Some(reply_sender_vec) = self.reply_map.remove(&batch_hash) {
                self.do_crash_commit_reply(reply_sender_vec, batch_hash.clone(), n, reply_vec).await;
            }
        }

    }
}


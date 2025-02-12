use std::{collections::HashMap, sync::Arc};

use log::error;
use prost::Message as _;
use tokio::sync::{oneshot, Mutex};

use crate::{config::AtomicConfig, crypto::HashType, proto::{client::{ProtoClientReply, ProtoTransactionReceipt}, execution::ProtoTransactionResult, rpc::ProtoPayload}, rpc::{server::LatencyProfile, PinnedMessage}, utils::channel::Receiver};

use super::batch_proposal::MsgAckChanWithTag;

pub enum ClientReplyCommand {
    CancelAllRequests,
    CrashCommitAck(HashMap<HashType, (u64, Vec<ProtoTransactionResult>)>),
    ByzCommitAck(HashMap<HashType, (u64, Vec<ProtoTransactionResult>)>),
}
pub struct ClientReplyHandler {
    config: AtomicConfig,

    batch_rx: Receiver<(oneshot::Receiver<HashType>, Vec<MsgAckChanWithTag>)>,
    reply_command_rx: Receiver<ClientReplyCommand>,

    reply_map: HashMap<HashType, Vec<MsgAckChanWithTag>>,

    crash_commit_reply_buf: HashMap<HashType, (u64, Vec<ProtoTransactionResult>)>,
    byz_commit_reply_buf: HashMap<HashType, (u64, Vec<ProtoTransactionResult>)>
}

impl ClientReplyHandler {
    pub fn new(
        config: AtomicConfig,
        batch_rx: Receiver<(oneshot::Receiver<HashType>, Vec<MsgAckChanWithTag>)>,
        reply_command_rx: Receiver<ClientReplyCommand>,
    ) -> Self {
        Self {
            config,
            batch_rx,
            reply_command_rx,
            reply_map: HashMap::new(),
            crash_commit_reply_buf: HashMap::new(),
            byz_commit_reply_buf: HashMap::new(),
        }
    }

    pub async fn run(client_reply_handler: Arc<Mutex<Self>>) {
        let mut client_reply_handler = client_reply_handler.lock().await;
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
        for (tx_n, ((reply_chan, client_tag), reply)) in reply_sender_vec.into_iter().zip(reply_vec.into_iter()).enumerate() {
            let reply = ProtoClientReply {
                reply: Some(
                    crate::proto::client::proto_client_reply::Reply::Receipt(
                        ProtoTransactionReceipt {
                            req_digest: hash.clone(),
                            block_n: n,
                            tx_n: tx_n as u64,
                            results: Some(reply),
                            await_byz_response: false,
                            byz_responses: vec![],
                        },
                )),
                client_tag
            };

            let reply_ser = reply.encode_to_vec();
            let _sz = reply_ser.len();
            let reply_msg = PinnedMessage::from(reply_ser, _sz, crate::rpc::SenderType::Anon);
            let latency_profile = LatencyProfile::new();
            
            let _ = reply_chan.send((reply_msg, latency_profile)).await;
        }
    }

    async fn do_byz_commit_reply(&mut self, reply_sender_vec: Vec<MsgAckChanWithTag>, hash: HashType, n: u64, reply_vec: Vec<ProtoTransactionResult>) {
        assert_eq!(reply_sender_vec.len(), reply_vec.len());
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
                        self.crash_commit_reply_buf.insert(hash, (n, reply_vec));
                    }
                }
            },
            ClientReplyCommand::ByzCommitAck(byz_commit_ack) => {
                for (hash, (n, reply_vec)) in byz_commit_ack {
                    if let Some(reply_sender_vec) = self.reply_map.remove(&hash) {
                        self.do_byz_commit_reply(reply_sender_vec, hash, n, reply_vec).await;
                    } else {
                        self.byz_commit_reply_buf.insert(hash, (n, reply_vec));
                    }
                }
            },
        }
    }

    async fn maybe_clear_reply_buf(&mut self, batch_hash: HashType) {
        if let Some((n, reply_vec)) = self.crash_commit_reply_buf.remove(&batch_hash) {
            if let Some(reply_sender_vec) = self.reply_map.remove(&batch_hash) {
                self.do_crash_commit_reply(reply_sender_vec, batch_hash.clone(), n, reply_vec).await;
            }
        }

        if let Some((n, reply_vec)) = self.byz_commit_reply_buf.remove(&batch_hash) {
            if let Some(reply_sender_vec) = self.reply_map.remove(&batch_hash) {
                self.do_byz_commit_reply(reply_sender_vec, batch_hash, n, reply_vec).await;
            }
        }
    }
}


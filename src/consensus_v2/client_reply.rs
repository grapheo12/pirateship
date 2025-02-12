use std::{collections::HashMap, sync::Arc};

use tokio::sync::{oneshot, Mutex};

use crate::{config::AtomicConfig, crypto::HashType, proto::execution::ProtoTransactionResult, utils::channel::Receiver};

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

                self.reply_map.insert(batch_hash, reply_vec);
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


    async fn handle_reply_command(&mut self, cmd: ClientReplyCommand) {
        match cmd {
            ClientReplyCommand::CancelAllRequests => {
                self.reply_map.clear();
            },
            ClientReplyCommand::CrashCommitAck(crash_commit_ack) => {
                for (hash, (n, reply_vec)) in crash_commit_ack {
                    if let Some(reply_sender_vec) = self.reply_map.remove(&hash) {
                        assert_eq!(reply_sender_vec.len(), reply_vec.len());
                    }
                }
            },
            ClientReplyCommand::ByzCommitAck(byz_commit_ack) => {
                for (hash, (n, reply_vec)) in byz_commit_ack {
                    if let Some(reply_sender_vec) = self.reply_map.remove(&hash) {
                        assert_eq!(reply_sender_vec.len(), reply_vec.len());
                    }
                }
            },
        }
    }
}


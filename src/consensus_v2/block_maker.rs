use std::{pin::Pin, sync::Arc, time::Duration};

use tokio::sync::{oneshot, Mutex};
use crate::utils::channel::{Sender, Receiver};

use crate::{config::AtomicConfig, consensus::timer::ResettableTimer, crypto::{hash, CachedBlock, CryptoServiceConnector, HashType}, proto::consensus::{proto_block::Tx, DefferedSignature, ProtoBlock, ProtoForkValidation, ProtoQuorumCertificate, ProtoTransactionList}};

use super::batch_proposal::{MsgAckChanWithTag, RawBatch};

pub enum BlockMakerControlCommand {
    NewUnstableView(u64 /* view num */, u64 /* config num */),       // View changed to a new view, it is not stable, so don't propose new blocks.
    ViewStabilised(u64 /* view num */, u64 /* config num */),        // View is stable now, if I am the leader in this view, propose new blocks.
    NewViewMessage(u64 /* view num */, u64 /* config num */, Vec<ProtoForkValidation>, HashType /* new parent hash */, u64 /* new seq num */),  // Change view to unstable, use ProtoForkValidation to propose a new view message.
}


pub struct BlockMaker {
    config: AtomicConfig,
    control_command_rx: Receiver<BlockMakerControlCommand>,
    
    batch_rx: Receiver<(RawBatch, Vec<MsgAckChanWithTag>)>,
    
    signature_timer: Arc<Pin<Box<ResettableTimer>>>,
    
    qc_rx: Receiver<ProtoQuorumCertificate>,
    current_qc_list: Vec<ProtoQuorumCertificate>,

    block_broadcaster_tx: Sender<oneshot::Receiver<CachedBlock>>, // Last-ditch effort to parallelize hashing and signing of blocks, shouldn't matter.
    client_reply_tx: Sender<(HashType, Vec<MsgAckChanWithTag>)>,

    crypto: CryptoServiceConnector,
    parent_hash: HashType,
    seq_num: u64,
    view: u64,
    config_num: u64,
    view_is_stable: bool,
    force_sign_next_batch: bool,
    last_signed_seq_num: u64,
}

impl BlockMaker {
    pub fn new(
        config: AtomicConfig,
        control_command_rx: Receiver<BlockMakerControlCommand>,
        batch_rx: Receiver<(RawBatch, Vec<MsgAckChanWithTag>)>,
        qc_rx: Receiver<ProtoQuorumCertificate>,
        block_broadcaster_tx: Sender<oneshot::Receiver<CachedBlock>>,
        client_reply_tx: Sender<(HashType, Vec<MsgAckChanWithTag>)>,
        crypto: CryptoServiceConnector,
    ) -> Self {

        let signature_timer = ResettableTimer::new(
            Duration::from_millis(config.get().consensus_config.signature_max_delay_ms)
        );

        let parent = vec![];
        let parent_hash = hash(&parent); // Can't use crypto here as it is async.

        Self {
            config,
            control_command_rx,
            batch_rx,
            signature_timer,
            qc_rx,
            current_qc_list: Vec::new(),
            block_broadcaster_tx,
            client_reply_tx,
            crypto,
            parent_hash,
            seq_num: 1,
            view: 1,
            config_num: 1,
            view_is_stable: true,       // TODO: Start with stable for now, we'll change it later.
            force_sign_next_batch: false,
            last_signed_seq_num: 0,
        }
    }

    pub async fn run(block_maker: Arc<Mutex<Self>>) {
        let mut block_maker = block_maker.lock().await;
        let signature_timer_handle = block_maker.signature_timer.run().await;

        loop {
            if let Err(_) = block_maker.worker().await {
                break;
            }
        }

        signature_timer_handle.abort();
    }

    fn i_am_leader(&self) -> bool {
        let config = self.config.get();
        let leader = config.consensus_config.get_leader_for_view(self.view);
        leader == config.net_config.name
    }

    async fn worker(&mut self) -> Result<(), ()> {
        let listen_for_new_batch = 
            self.view_is_stable && self.i_am_leader();

        if listen_for_new_batch {
            tokio::select! {
                _batch_and_client_reply = self.batch_rx.recv() => {
                    if let Some(_) = _batch_and_client_reply {
                        let (batch, client_reply) = _batch_and_client_reply.unwrap();
                        self.handle_new_batch(batch, client_reply, vec![]).await;
                    }
                },
                _tick = self.signature_timer.wait() => {
                    self.force_sign_next_batch = true;
                },
                _cmd = self.control_command_rx.recv() => {
                    self.handle_control_command(_cmd).await;
                },
                _qc = self.qc_rx.recv() => {
                    self.add_qc(_qc).await;
                }
            }
        } else {
            tokio::select! {
                _cmd = self.control_command_rx.recv() => {
                    self.handle_control_command(_cmd).await;
                },
                _qc = self.qc_rx.recv() => {
                    self.add_qc(_qc).await;
                }
            }
        }
 
        Ok(())
    }

    async fn handle_new_batch(&mut self, batch: RawBatch, replies: Vec<MsgAckChanWithTag>, fork_validation: Vec<ProtoForkValidation>) {
        let n = self.seq_num;
        self.seq_num += 1;
        let config = self.config.get();
        
        let must_sign = self.force_sign_next_batch ||
            (n - self.last_signed_seq_num) > config.consensus_config.signature_max_delay_blocks;

        if must_sign {
            self.last_signed_seq_num = n;
            self.force_sign_next_batch = false;
        }

        let qc_list = self.current_qc_list.drain(..).collect();

        let parent = self.parent_hash.clone();
        let block = ProtoBlock {
            n,
            parent,
            view: self.view,
            qc: qc_list,
            fork_validation,
            view_is_stable: self.view_is_stable,
            config_num: self.config_num,
            tx: Some(crate::proto::consensus::proto_block::Tx::TxList(ProtoTransactionList {
                tx_list: batch,
            })),
            sig: Some(crate::proto::consensus::proto_block::Sig::NoSig(DefferedSignature{})),
        };

        let (block_rx, hash_rx) = self.crypto.prepare_block(block, must_sign).await;
        self.parent_hash = hash_rx.await.unwrap();

        self.client_reply_tx.send((self.parent_hash.clone(), replies)).await
            .expect("Should be able to send client_reply_tx");
        self.block_broadcaster_tx.send(block_rx).await
            .expect("Should be able to send block_broadcaster_tx");
    }

    async fn add_qc(&mut self, qc: Option<ProtoQuorumCertificate>) {
        if qc.is_none() {
            return;
        }
        let qc = qc.unwrap();
        // Invariant: All qc.view in self.current_qc_list == self.view
        if qc.view != self.view {
            return;
        }

        self.current_qc_list.push(qc);
    }

    async fn handle_control_command(&mut self, cmd: Option<BlockMakerControlCommand>) {
        if cmd.is_none() {
            return;
        }
        let cmd = cmd.unwrap();

        match cmd {
            // Follow the changes, no questions asked!
            BlockMakerControlCommand::NewUnstableView(v, c) => {
                self.view = v;
                self.config_num = c;
                self.view_is_stable = false;
                self.current_qc_list.retain(|e| e.view == self.view);
            },
            BlockMakerControlCommand::ViewStabilised(v, c) => {
                self.view = v;
                self.config_num = c;
                self.view_is_stable = true;
                self.current_qc_list.retain(|e| e.view == self.view);
            },
            BlockMakerControlCommand::NewViewMessage(v, c, fork_validation, new_parent_hash, new_seq_num) => {
                self.view = v;
                self.config_num = c;
                self.view_is_stable = false;
                self.current_qc_list.retain(|e| e.view == self.view);

                // Rest is only applicable if I am the leader.
                if !self.i_am_leader() {
                    return;
                }

                self.seq_num = new_seq_num;     // This may not be monotonic due to rollbacks.
                self.parent_hash = new_parent_hash;

                // Now the NEXT block (ie new_seq_num + 1) is going to be for NewView.
                self.handle_new_batch(RawBatch::new(), vec![], fork_validation).await;
            },
        }
    }


}
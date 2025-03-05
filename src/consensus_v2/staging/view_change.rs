use std::{collections::{HashMap, HashSet}, sync::atomic::fence};

use log::info;
use prost::Message as _;

use crate::{
    consensus_v2::{
        block_broadcaster::BlockBroadcasterCommand, block_sequencer::BlockSequencerControlCommand, client_reply::ClientReplyCommand, fork_receiver::ForkReceiverCommand, pacemaker::PacemakerCommand
    }, crypto::{default_hash, CachedBlock, HashType}, proto::{consensus::{HalfSerializedBlock, ProtoFork, ProtoForkValidation, ProtoQuorumCertificate, ProtoViewChange}, rpc::ProtoPayload}, rpc::{client::PinnedClient, server::LatencyProfile, PinnedMessage, SenderType}, utils::{deserialize_proto_block, get_parent_hash_in_proto_block_ser}
};

use super::{CachedBlockWithVotes, Staging};

impl Staging {
    pub(super) async fn process_view_change_message(
        &mut self,
        cmd: PacemakerCommand,
    ) -> Result<(), ()> {
        match cmd {
            PacemakerCommand::UpdateView(view_num, config_num) => {
                self.update_view(view_num, config_num).await;
            }
            PacemakerCommand::NewViewForks(view_num, config_num, forks) => {
                self.propose_new_view(view_num, config_num, forks).await;
            }
            _ => {
                unreachable!("Invalid message from pacemaker");
            }
        }
        Ok(())
    }

    pub(super) async fn update_view(&mut self, view_num: u64, config_num: u64) {
        if self.view >= view_num || self.config_num >= config_num {
            return;
        }

        self.view = view_num;
        self.config_num = config_num;
        self.view_is_stable = false;

        let vc_msg = self.create_my_vc_msg().await;

        let (payload, vc_msg) = { // Jumping through hoops to avoid cloning.
            let payload = ProtoPayload {
                message: Some(crate::proto::rpc::proto_payload::Message::ViewChange(vc_msg)),
            };
            let buf = payload.encode_to_vec();
            let sz = buf.len();
            let msg = PinnedMessage::from(buf, sz, SenderType::Anon);

            if let Some(crate::proto::rpc::proto_payload::Message::ViewChange(vc_msg)) = payload.message {
                (msg, vc_msg)
            } else {
                unreachable!();
            }
        };

        
        // Memory fence to prevent reordering.
        fence(std::sync::atomic::Ordering::SeqCst);
        // Once the signal to update view is sent,
        // all blocks that are already in the queue must be dropped by staging.
        // To guarantee this, even in the face of compiler optimizations,
        // we must ensure that the view update signal is sent after the fence.
        // (I realize there is already a data dependency here, so may not be necessary :-))


        // Send the signal to everywhere
        self.block_sequencer_command_tx
            .send(BlockSequencerControlCommand::NewUnstableView(
                self.view,
                self.config_num,
            ))
            .await
            .unwrap();
        self.client_reply_tx.send(ClientReplyCommand::CancelAllRequests).await.unwrap();
        self.fork_receiver_command_tx
            .send(ForkReceiverCommand::UpdateView(self.view, self.config_num))
            .await
            .unwrap();
        self.pacemaker_tx
            .send(PacemakerCommand::MyViewJumped(self.view, self.config_num, vc_msg))
            .await
            .unwrap();



        // Broadcast the view change message
        let bcast_names = self.get_everyone_except_me();
        let _ = PinnedClient::broadcast(
            &self.client, &bcast_names, 
            &payload, &mut LatencyProfile::new()
        ).await;
        

    }

    async fn propose_new_view(
        &mut self,
        view_num: u64,
        config_num: u64,
        forks: HashMap<SenderType, ProtoViewChange>,
    ) {

        // With view_is_stable == false, the sequencer will not produce any new blocks.
        // The staging `process_block_as_leader/follower` will reject all blocks in the queue,
        // until it is completely drained.
        // The next block must come from sending NewViewMessage to the sequencer.

        if view_num != self.view || config_num != self.config_num {
            return;
        }

        if self.view_is_stable || !self.i_am_leader() {
            return;
        }

        
        // Choose a fork to propose.
        let (mut chosen_fork, fork_validation) = self.fork_choice_rule_get(&forks).await;

        let retain_n = self.check_byz_commit_invariant(&chosen_fork)
            .expect("Invariant <ByzCommit> violated");

        // Clean up pending_blocks
        self.pending_blocks.retain(|b| b.block.block.n <= retain_n);
        if self.ci > retain_n {
            self.ci = retain_n;
            // Signal rollback.
            self.app_tx.send(crate::consensus_v2::app::AppCommand::Rollback(self.ci)).await.unwrap();
        }

        // Install the chosen fork in pending_blocks.
        chosen_fork.retain(|b| b.block.n > retain_n);
        self.pending_blocks.extend(chosen_fork.into_iter().map(|block| {
            CachedBlockWithVotes {
                block,
                vote_sigs: HashMap::new(),
                replication_set: HashSet::new(),
            }
        }));

        // Send the chosen fork to sequencer.
        let new_last_n = self.pending_blocks.back().map_or(0, |b| b.block.block.n);
        let new_parent_hash = self.pending_blocks.back().map_or(default_hash(), |b| b.block.block_hash.clone());
        self.block_sequencer_command_tx.send(
            BlockSequencerControlCommand::NewViewMessage(self.view, self.config_num, fork_validation, new_parent_hash, new_last_n)
        ).await.unwrap();

    }

    pub(super) async fn maybe_stabilize_view(&mut self, qc: &ProtoQuorumCertificate) {
        // The new view message must be the VERY LAST block in pending now.
        // The QC present here must on that last block.
        let last_n = self.pending_blocks.back().as_ref().unwrap().block.block.n;
        if qc.n < last_n {
            return;
        }

        if qc.view != self.view {
            return;
        }

        // If the QC is on the last block, we can now stabilize the view.
        self.view_is_stable = true;

        // Send the signal to sequencer to produce new blocks.
        self.block_sequencer_command_tx
            .send(BlockSequencerControlCommand::ViewStabilised(
                self.view,
                self.config_num,
            ))
            .await
            .unwrap();

        info!("View {} stabilized", self.view);
    }

    async fn create_my_vc_msg(&mut self) -> ProtoViewChange {
        let my_fork = ProtoFork {
            serialized_blocks: self
                .pending_blocks
                .iter()
                .map(|b| HalfSerializedBlock {
                    n: b.block.block.n,
                    view: b.block.block.view,
                    view_is_stable: b.block.block.view_is_stable,
                    config_num: b.block.block.config_num,
                    serialized_body: b.block.block_ser.clone(),
                })
                .collect(),
        };
        let fork_len = my_fork.serialized_blocks.len() as u64;
        let my_fork_ser = my_fork.encode_to_vec();
        let my_fork_sig = self.crypto.sign(&my_fork_ser).await;

        ProtoViewChange {
            view: self.view,
            fork: Some(my_fork),
            fork_sig: my_fork_sig.to_vec(),
            fork_len,
            fork_last_qc: self.last_qc.clone(),
            config_num: self.config_num,
        }

    }

    fn get_everyone_except_me(&self) -> Vec<String> {
        let my_name = &self.config.get().net_config.name;
        self.config.get().consensus_config.node_list
            .iter().filter(|name| !((*name).eq(my_name)))
            .map(|name| name.clone())
            .collect()
    }


    /// Advance the bci from all the forks received.
    /// Returns the chosen fork (as Vec<CachedBlock>) that the leader is supposed to install.
    /// Also returns the validation messages that the leader must send to the followers.
    async fn fork_choice_rule_get(&self, forks: &HashMap<SenderType, ProtoViewChange>) -> (Vec<CachedBlock>, Vec<ProtoForkValidation>) {
        // TODO::::: This is just a placeholder.
        let my_name = &SenderType::Auth(self.config.get().net_config.name.clone(), 0);
        let chosen_fork = self.convert_to_cached_blocks(forks[my_name].fork.as_ref().unwrap()).await;
        let fork_validation = Vec::new();

        (chosen_fork, fork_validation)
    }

    async fn convert_to_cached_blocks(&mut self, fork: &ProtoFork) -> Vec<CachedBlock> {
        let mut ret = Vec::new();
        
        let block_futs = self.crypto.prepare_for_rebroadcast(fork.serialized_blocks.clone()).await;
        self.block_broadcaster_command_tx.send(
            BlockBroadcasterCommand::NextAEForkPrefix(block_futs)
        ).await.unwrap();

        ret
    }


    /// Invariant <ByzCommit>:
    /// Remember that pending_blocks.first() is the first block that's not byz committed.
    /// So it's parent must be byz committed. Let's call it `curr_parent`.
    /// This leaves with 2 cases:
    /// - Chosen fork starts with some seq num n <= bci, then `curr_parent` must be part of the fork.
    /// - Chosen fork starts with some seq num n > bci, then the parent of the first block must be either in pending_blocks or be `curr_parent`.
    /// (To ensure this, the pacemaker must have Nacked until there was a fork history match).
    /// On success, returns the max seq num to retain in pending_blocks.
    fn check_byz_commit_invariant(&self, chosen_fork: &Vec<CachedBlock>) -> Result<u64, ()> {
        if self.bci == 0 {
            // Nothing to do here.
            return Ok(0);
        }

        let curr_parent = self.curr_parent_for_pending.as_ref().unwrap();

        if chosen_fork.len() == 0 {
            // Retain everything in pending_blocks.
            let last_n = self.pending_blocks.back().map_or(0, |b| b.block.block.n);
            return Ok(last_n);
        }

        let first_block = &chosen_fork[0];
        if first_block.block.n <= self.bci {
            // 1st case
            if !chosen_fork.iter().any(|b| b.block_hash == curr_parent.block_hash) {
                return Err(());
            } else {
                // Wipe everything from pending_blocks.
                return Ok(self.bci);
            }
        } else {
            let parent_hash = get_parent_hash_in_proto_block_ser(&first_block.block_ser).unwrap();
            // 2nd case
            if first_block.block.n == self.bci + 1 {
                if curr_parent.block_hash != parent_hash {
                    return Err(());
                } else {
                    // Wipe everything from pending_blocks.
                    return Ok(self.bci);
                }
            } else { // first_block.block.n > self.bci + 1
                let find_n_in_pending = first_block.block.n - 1;
                if !self.pending_blocks.iter().any(|b| 
                    b.block.block.n == find_n_in_pending
                    && b.block.block_hash == parent_hash
                ) {
                    return Err(());
                } else {
                    // Wipe everything from pending_blocks before first_block.block.n
                    return Ok(find_n_in_pending);
                }
            }
        }

    }
}

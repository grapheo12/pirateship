use std::collections::{HashMap, HashSet};
use async_recursion::async_recursion;
use futures::future::try_join_all;
use log::{debug, error, trace, warn};
use prost::Message;
use tokio::sync::oneshot;

use crate::{
    crypto::CachedBlock,
    proto::{
        consensus::{
            proto_block::Sig, ProtoNameWithSignature, ProtoQuorumCertificate,
            ProtoSignatureArrayEntry, ProtoVote,
        },
        rpc::ProtoPayload,
    },
    rpc::{client::PinnedClient, PinnedMessage, SenderType},
    utils::StorageAck
};

use super::{
    super::{
        app::AppCommand,
        block_broadcaster::BlockBroadcasterCommand,
        block_sequencer::BlockSequencerControlCommand,
        client_reply::ClientReplyCommand,
        fork_receiver::{AppendEntriesStats, ForkReceiverCommand},
    },
    CachedBlockWithVotes, Staging,
};

impl Staging {
    pub(super) fn i_am_leader(&self) -> bool {
        let config = self.config.get();
        let leader = config.consensus_config.get_leader_for_view(self.view);
        leader == config.net_config.name
    }

    fn perf_register_block(&self, block: &CachedBlock) {
        #[cfg(feature = "perf")]
        if let Some(Sig::ProposerSig(_)) = block.block.sig {
            self.leader_perf_counter_signed
                .borrow_mut()
                .register_new_entry(block.block.n);
        } else {
            self.leader_perf_counter_unsigned
                .borrow_mut()
                .register_new_entry(block.block.n);
        }
    }

    fn perf_deregister_block(&self, block: &CachedBlock) {
        #[cfg(feature = "perf")]
        if let Some(Sig::ProposerSig(_)) = block.block.sig {
            self.leader_perf_counter_signed
                .borrow_mut()
                .deregister_entry(&block.block.n);
        } else {
            self.leader_perf_counter_unsigned
                .borrow_mut()
                .deregister_entry(&block.block.n);
        }
    }

    #[cfg(feature = "perf")]
    fn perf_add_event(
        &self,
        block: &CachedBlock,
        event: &str,
    ) -> (bool /* signed */, u64 /* block_n */) {
        if let Some(Sig::ProposerSig(_)) = block.block.sig {
            self.leader_perf_counter_signed
                .borrow_mut()
                .new_event(event, &block.block.n);
            (true, block.block.n)
        } else {
            self.leader_perf_counter_unsigned
                .borrow_mut()
                .new_event(event, &block.block.n);
            (false, block.block.n)
        }
    }

    #[cfg(not(feature = "perf"))]
    fn perf_add_event(&self, _block: &CachedBlock, _event: &str) {}

    fn perf_add_event_from_perf_stats(&self, signed: bool, block_n: u64, event: &str) {
        #[cfg(feature = "perf")]
        if signed {
            self.leader_perf_counter_signed
                .borrow_mut()
                .new_event(event, &block_n);
        } else {
            self.leader_perf_counter_unsigned
                .borrow_mut()
                .new_event(event, &block_n);
        }
    }

    fn crash_commit_threshold(&self) -> usize {
        let n = self.config.get().consensus_config.node_list.len();

        // Majority
        n / 2 + 1
    }

    fn byzantine_commit_threshold(&self) -> usize {
        // TODO: Change this to u + r + 1
        self.byzantine_liveness_threshold()
    }

    fn byzantine_liveness_threshold(&self) -> usize {
        let config = self.config.get();
        let n = config.consensus_config.node_list.len();
        let u = config.consensus_config.liveness_u as usize;

        assert!(n >= u);

        n - u
    }

    fn byzantine_fast_path_threshold(&self) -> usize {
        // All nodes
        self.config.get().consensus_config.node_list.len()
    }

    fn check_continuity(&self, block: &CachedBlock) -> bool {
        match self.pending_blocks.back() {
            Some(b) => {
                let res = b.block.block.n + 1 == block.block.n
                    && block.block.parent.eq(&b.block.block_hash);

                if !res {
                    let hash_match = block.block.parent.eq(&b.block.block_hash);
                    error!(
                        "Current last block: {} Incoming block: {} Hash link match: {}",
                        b.block.block.n, block.block.n, hash_match
                    );
                }

                res
            }
            None => block.block.n == self.bci + 1,
        }
    }

    pub(super) async fn handle_view_change_timer_tick(&mut self) -> Result<(), ()> {
        self.update_view(self.view + 1, self.config_num).await;
        Ok(())
    }

    async fn vote_on_last_block_for_self(
        &mut self,
        storage_ack: oneshot::Receiver<StorageAck>,
    ) -> Result<(), ()> {
        let name = self.config.get().net_config.name.clone();

        let last_block = match self.pending_blocks.back() {
            Some(b) => b,
            None => return Err(()),
        };

        // Wait for it to be stored.
        // Invariant: I vote => I stored
        let _ = storage_ack.await.unwrap();

        self.perf_add_event(&last_block.block, "Storage");

        let mut vote = ProtoVote {
            sig_array: Vec::with_capacity(1),
            fork_digest: last_block.block.block_hash.clone(),
            n: last_block.block.block.n,
            view: self.view,
            config_num: self.config_num,
        };

        // If this block is signed, need a signature for the vote.
        if let Some(Sig::ProposerSig(_)) = last_block.block.block.sig {
            let vote_sig = self.crypto.sign(&last_block.block.block_hash).await;

            vote.sig_array.push(ProtoSignatureArrayEntry {
                n: last_block.block.block.n,
                sig: vote_sig.to_vec(),
            });
        }

        self.perf_add_event(&last_block.block, "Vote to Self");

        self.process_vote(name, vote).await
    }

    async fn send_vote_on_last_block_to_leader(
        &mut self,
        storage_ack: oneshot::Receiver<StorageAck>,
    ) -> Result<(), ()> {
        let last_block = match self.pending_blocks.back() {
            Some(b) => b,
            None => return Err(()),
        };

        // Wait for it to be stored.
        // Invariant: I vote => I stored
        let _ = storage_ack.await.unwrap();

        // I will resend all the signatures in pending_blocks that I have not received a QC for.
        // But only if the last block was signed.
        let sig_array = if let Some(Sig::ProposerSig(_)) = last_block.block.block.sig {
            self.pending_signatures
                .iter()
                .map(|(_, sig)| sig.clone())
                .collect()
        } else {
            Vec::new()
        };

        let mut vote = ProtoVote {
            sig_array,
            fork_digest: last_block.block.block_hash.clone(),
            n: last_block.block.block.n,
            view: self.view,
            config_num: self.config_num,
        };

        // If this block is signed, need a signature for the vote.
        if let Some(Sig::ProposerSig(_)) = last_block.block.block.sig {
            let vote_sig = self.crypto.sign(&last_block.block.block_hash).await;
            let sig_entry = ProtoSignatureArrayEntry {
                n: last_block.block.block.n,
                sig: vote_sig.to_vec(),
            };
            vote.sig_array.push(sig_entry.clone());

            self.pending_signatures
                .push_back((last_block.block.block.n, sig_entry));
        }

        let leader = self
            .config
            .get()
            .consensus_config
            .get_leader_for_view(self.view);

        let rpc = ProtoPayload {
            message: Some(crate::proto::rpc::proto_payload::Message::Vote(vote)),
        };
        let data = rpc.encode_to_vec();
        let sz = data.len();
        let data = PinnedMessage::from(data, sz, SenderType::Anon);

        PinnedClient::send(&self.client, &leader, data.as_ref())
            .await
            .unwrap();

        trace!("Sent vote to {} for {}", leader, last_block.block.block.n);

        Ok(())
    }

    #[async_recursion]
    pub(super) async fn process_block_as_leader(
        &mut self,
        block: CachedBlock,
        storage_ack: oneshot::Receiver<StorageAck>,
        ae_stats: AppendEntriesStats,
    ) -> Result<(), ()> {
        if ae_stats.view < self.view {
            // Do not accept anything from a lower view.
            return Ok(());
        } else if block.block.view == self.view {
            if !self.view_is_stable && block.block.view_is_stable {
                // Notify upstream stages of view stabilization
                self.block_sequencer_command_tx
                    .send(BlockSequencerControlCommand::ViewStabilised(
                        self.view,
                        self.config_num,
                    ))
                    .await
                    .unwrap();
            }
            self.view_is_stable = block.block.view_is_stable;
            // Invariant <ViewLock>: Within the same view, the log must be append-only.
            if !self.check_continuity(&block) {
                warn!("Continuity broken");
                return Ok(());
            }
        } else {
            // If from a higher view, this must mean I am no longer the leader in the cluster.
            // Precondition: The blocks I am receiving have been verified earlier,
            // So the sequence of new view messages have been verified before reaching here.

            // Jump to the new view
            self.view = ae_stats.view;

            self.view_is_stable = block.block.view_is_stable;
            self.config_num = ae_stats.config_num;

            // Notify upstream stages of view change
            self.block_sequencer_command_tx
                .send(BlockSequencerControlCommand::NewUnstableView(
                    self.view,
                    self.config_num,
                ))
                .await
                .unwrap();
            self.fork_receiver_command_tx
                .send(ForkReceiverCommand::UpdateView(self.view, self.config_num))
                .await
                .unwrap();

            // Flush the pending queue and cancel client requests.
            let old_pending_len = self.pending_blocks.len();
            self.pending_blocks
                .retain(|e| e.block.block.n < block.block.n);
            let new_pending_len = self.pending_blocks.len();
            if new_pending_len < old_pending_len {
                // Signal a rollback
            }
            self.pending_signatures.clear();
            self.client_reply_tx
                .send(ClientReplyCommand::CancelAllRequests)
                .await
                .unwrap();

            // Ready to accept the block normally.
            if self.i_am_leader() {
                return self
                    .process_block_as_leader(block, storage_ack, ae_stats)
                    .await;
            } else {
                return self
                    .process_block_as_follower(block, storage_ack, ae_stats)
                    .await;
            }
        }

        self.perf_register_block(&block);

        // Postcondition here: block.view == self.view && check_continuity() == true && i_am_leader
        let block_with_votes = CachedBlockWithVotes {
            block,
            vote_sigs: HashMap::new(),
            replication_set: HashSet::new(),
        };

        self.pending_blocks.push_back(block_with_votes);

        self.perf_add_event(
            &self.pending_blocks.iter().last().unwrap().block,
            "Push to Pending",
        );

        // Now vote for self

        self.vote_on_last_block_for_self(storage_ack).await?;

        Ok(())
    }

    /// This has a lot of similarities with process_block_as_leader.
    #[async_recursion]
    pub(super) async fn process_block_as_follower(
        &mut self,
        block: CachedBlock,
        storage_ack: oneshot::Receiver<StorageAck>,
        ae_stats: AppendEntriesStats,
    ) -> Result<(), ()> {
        if ae_stats.view < self.view {
            // Do not accept anything from a lower view.
            return Ok(());
        } else if ae_stats.view == self.view {
            if !self.view_is_stable && block.block.view_is_stable {
                // Notify upstream stages of view stabilization
                self.block_sequencer_command_tx
                    .send(BlockSequencerControlCommand::ViewStabilised(
                        self.view,
                        self.config_num,
                    ))
                    .await
                    .unwrap();
            }
            self.view_is_stable = block.block.view_is_stable;
            // Invariant <ViewLock>: Within the same view, the log must be append-only.
            if !self.check_continuity(&block) {
                warn!("Continuity broken");
                return Ok(());
            }
        } else {
            // If from a higher view, this may mean I am now the leader in the cluster.
            // Precondition: The blocks I am receiving have been verified earlier,
            // So the sequence of new view messages have been verified before reaching here.

            // Jump to the new view
            self.view = ae_stats.view;

            self.view_is_stable = block.block.view_is_stable;
            self.config_num = block.block.config_num;

            // Notify upstream stages of view change
            self.block_sequencer_command_tx
                .send(BlockSequencerControlCommand::NewUnstableView(
                    self.view,
                    self.config_num,
                ))
                .await
                .unwrap();
            self.fork_receiver_command_tx
                .send(ForkReceiverCommand::UpdateView(self.view, self.config_num))
                .await
                .unwrap();

            // Flush the pending queue and cancel client requests.
            self.pending_blocks.clear();
            self.pending_signatures.clear();
            self.client_reply_tx
                .send(ClientReplyCommand::CancelAllRequests)
                .await
                .unwrap();

            // Ready to accept the block normally.
            if self.i_am_leader() {
                return self
                    .process_block_as_leader(block, storage_ack, ae_stats)
                    .await;
            } else {
                return self
                    .process_block_as_follower(block, storage_ack, ae_stats)
                    .await;
            }
        }

        // Postcondition here: block.view == self.view && check_continuity() == true && !i_am_leader
        let block_with_votes = CachedBlockWithVotes {
            block,
            vote_sigs: HashMap::new(),
            replication_set: HashSet::new(),
        };
        self.pending_blocks.push_back(block_with_votes);

        // Now crash commit blindly
        self.do_crash_commit(self.ci, ae_stats.ci).await;

        let mut qc_list = self
            .pending_blocks
            .iter()
            .last()
            .unwrap()
            .block
            .block
            .qc
            .iter()
            .map(|e| e.clone())
            .collect::<Vec<_>>();

        for qc in qc_list.drain(..) {
            if !self.view_is_stable {
                // Try to see if this QC can stabilize the view.
                self.maybe_stabilize_view(&qc).await;
            }
            
            self.maybe_byzantine_commit(qc).await?;
        }

        // Reply vote to the leader.
        self.send_vote_on_last_block_to_leader(storage_ack).await?;

        Ok(())
    }

    pub(super) async fn verify_and_process_vote(&mut self, sender: String, vote: ProtoVote) -> Result<(), ()> {
        debug!("Got vote on {} from {}", vote.n, sender);
        let mut verify_futs = Vec::new();
        for sig in &vote.sig_array {
            let found_block = self
                .pending_blocks
                .binary_search_by(|b| b.block.block.n.cmp(&sig.n));

            match found_block {
                Ok(idx) => {
                    let block = &self.pending_blocks[idx];
                    let _sig = sig.sig.clone().try_into();
                    match _sig {
                        Ok(_sig) => {
                            verify_futs.push(
                                self.crypto
                                    .verify_nonblocking(
                                        block.block.block_hash.clone(),
                                        sender.clone(),
                                        _sig,
                                    )
                                    .await,
                            );
                        }
                        Err(_) => {
                            warn!("Malformed signature in vote");
                            return Ok(());
                        }
                    }
                }
                Err(_) => {
                    // This is a vote for a block I have byz committed.
                    continue;
                }
            }
        }

        let results = try_join_all(verify_futs).await.unwrap();
        if !results.iter().all(|e| *e) {
            return Ok(());
        }

        self.process_vote(sender, vote).await
    }

    /// Precondition: The vote has been cryptographically verified to be from sender.
    async fn process_vote(&mut self, sender: String, mut vote: ProtoVote) -> Result<(), ()> {
        if self.view != vote.view {
            return Ok(());
        }

        if self.pending_blocks.len() == 0 {
            return Ok(());
        }

        let first_n = self.pending_blocks.front().unwrap().block.block.n;
        let last_n = self.pending_blocks.back().unwrap().block.block.n;

        if !(first_n <= vote.n && vote.n <= last_n) {
            return Ok(());
        }
        // Vote for a block is a vote on all its ancestors.
        for block in self.pending_blocks.iter_mut() {
            if block.block.block.n <= vote.n {
                block.replication_set.insert(sender.clone());
            }

            if let Some(Sig::ProposerSig(_)) = block.block.block.sig {
                // If this block is signed, the sig array may have a signature for it.
                vote.sig_array.retain(|e| {
                    if e.n != block.block.block.n {
                        true
                    } else {
                        block.vote_sigs.insert(sender.clone(), e.clone());
                        false
                    }
                });
            }
        }

        self.maybe_crash_commit().await?;

        self.maybe_create_qcs().await?;

        Ok(())
    }

    /// Crash commit blindly; checking is left on the caller.
    async fn do_crash_commit(&mut self, old_ci: u64, new_ci: u64) {
        if new_ci <= old_ci {
            return;
        }
        self.ci = new_ci;
        // Notify
        self.block_broadcaster_command_tx
            .send(BlockBroadcasterCommand::UpdateCI(new_ci))
            .await
            .unwrap();

        let blocks = self
            .pending_blocks
            .iter()
            .filter(|e| e.block.block.n > old_ci && e.block.block.n <= new_ci)
            .map(|e| e.block.clone())
            .collect::<Vec<_>>();

        #[cfg(feature = "perf")]
        let mut block_perf_stats = Vec::new();
        #[cfg(feature = "perf")]
        for b in &blocks {
            block_perf_stats.push(self.perf_add_event(&b, "Crash Commit"));
        }
        self.app_tx
            .send(AppCommand::CrashCommit(blocks))
            .await
            .unwrap();

        #[cfg(feature = "perf")]
        for (signed, block_n) in block_perf_stats {
            self.perf_add_event_from_perf_stats(signed, block_n, "Send Crash Commit to App");
        }
    }

    async fn maybe_crash_commit(&mut self) -> Result<(), ()> {
        let old_ci = self.ci;

        let n_num_tx = self
            .pending_blocks
            .iter()
            .map(|e| (e.block.block.n, e.block.block.tx_list.len()))
            .collect::<Vec<_>>();
        trace!("Pending blocks: {:?}", n_num_tx);
        for block in self.pending_blocks.iter() {
            if block.block.block.n <= self.ci {
                continue;
            }

            if block.replication_set.len() >= self.crash_commit_threshold() {
                self.ci = block.block.block.n;
            }
        }
        let new_ci = self.ci;

        self.do_crash_commit(old_ci, new_ci).await;

        Ok(())
    }

    async fn maybe_create_qcs(&mut self) -> Result<(), ()> {
        let mut qcs = Vec::new();

        for block in &self.pending_blocks {
            if block.vote_sigs.len() >= self.byzantine_commit_threshold() {
                let qc = ProtoQuorumCertificate {
                    n: block.block.block.n,
                    view: self.view,
                    sig: block
                        .vote_sigs
                        .iter()
                        .map(|(k, v)| ProtoNameWithSignature {
                            name: k.clone(),
                            sig: v.sig.clone(),
                        })
                        .collect(),
                    digest: block.block.block_hash.clone(),
                };
                qcs.push(qc);

            }
        }

        for qc in qcs.drain(..) {
            if !self.view_is_stable {
                // Try to see if this QC can stabilize the view.
                self.maybe_stabilize_view(&qc).await;
            }
            
            // This send needs to be non-blocking.
            // Hence can't avoid using an unbounded channel.
            // Otherwise: block_broadcaster_tx -> staging_tx -> qc_tx forms a cycle since BlockSequencer consumes from qc_rx.
            // Had there been another thread consuming qc_rx, this would not have been a problem since there will always be an enabled consumer.
            // But since this thread will block on block_broadcaster_tx.send, it will not be able to consume from qc_rx.
            // Once the queues are saturated, the system will deadlock.
            let _ = self.qc_tx.send(qc.clone());
            self.maybe_byzantine_commit(qc).await?;
        }

        Ok(())
    }

    /// Will push to pending_qcs and notify block_broadcaster to attach this qc.
    /// Then will try to byzantine commit.
    /// Precondition: All qcs in self.pending_qcs has same view as qc.view.
    /// Fast path rule:
    /// If qc_n has votes from all nodes, then qc.n is byz committed.
    /// Slow path rule:
    /// If \E (block_n, qc2) in self.pending_qcs: qc.n >= block_n then qc2.n is byz committed.
    /// This is PBFT/Jolteon-style 2-hop rule.
    /// If this happens then all QCs in self.pending_qcs such that block_n <= new_bci is removed.
    async fn maybe_byzantine_commit(
        &mut self,
        incoming_qc: ProtoQuorumCertificate,
    ) -> Result<(), ()> {
        // Reset view timer. Getting a QC signals that byzantine progress can still be made.
        if self.view <= incoming_qc.view /* no old */
            && self.last_qc.as_ref().map(|e| e.n).unwrap_or(0) < incoming_qc.n /* dedup */
        {
            self.view_change_timer.reset();
            self.maybe_update_last_qc(&incoming_qc);
        }

        // Clean out the pending_signatures
        self.pending_signatures.retain(|(n, _)| *n > incoming_qc.n);
        let old_bci = self.bci;

        // Fast path: All votes rule; only applicable if view is stable already.
        let new_bci_fast_path = if self.view_is_stable &&
        incoming_qc.sig.len() >= self.byzantine_fast_path_threshold() {
            incoming_qc.n
            // old_bci
        } else {
            old_bci
        };

        // Slow path: 2-hop rule
        let new_bci_slow_path = self
            .pending_blocks
            .iter()
            .rev()
            .filter(|b| b.block.block.n <= incoming_qc.n) // The blocks pointed by this QC (and all its ancestors)
            .map(|b| b.block.block.qc.iter().map(|qc| qc.n)) // Collect all the QCs in those blocks
            .flatten()
            .max()
            .unwrap_or(old_bci); // All such qc.n must be byz committed, so new_bci = max(all such qc.n)

        let new_bci = new_bci_fast_path.max(new_bci_slow_path);

        self.do_byzantine_commit(old_bci, new_bci).await;
        Ok(())
    }

    async fn do_byzantine_commit(&mut self, old_bci: u64, new_bci: u64) {
        if new_bci <= old_bci {
            return;
        }

        self.bci = new_bci;

        // Invariant: All blocks in pending_blocks is in order.
        let mut byz_blocks = Vec::new();

        while let Some(block) = self.pending_blocks.front() {
            if block.block.block.n > new_bci {
                break;
            }

            let block = self.pending_blocks.pop_front().unwrap().block;
            self.perf_add_event(&block, "Byz Commit");

            if block.block.n == new_bci {
                self.curr_parent_for_pending = Some(block.clone());
            }

            self.perf_deregister_block(&block);
            byz_blocks.push(block);
        }

        let _ = self.app_tx.send(AppCommand::ByzCommit(byz_blocks)).await;
    }

    fn maybe_update_last_qc(&mut self, qc: &ProtoQuorumCertificate) {
        if qc.n > self.last_qc.as_ref().map(|e| e.n).unwrap_or(0) {
            self.last_qc = Some(qc.clone());
        }
    }
}

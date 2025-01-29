use std::{collections::{HashMap, HashSet, VecDeque}, pin::Pin, sync::Arc, time::Duration};

use async_recursion::async_recursion;
use log::{debug, error, warn};
use tokio::sync::Mutex;

use crate::{config::AtomicConfig, consensus::timer::ResettableTimer, crypto::{CachedBlock, CryptoServiceConnector}, proto::consensus::{proto_block::Sig, ProtoQuorumCertificate, ProtoSignatureArrayEntry, ProtoViewChange, ProtoVote}, rpc::client::PinnedClient, utils::channel::{Receiver, Sender}};

use super::{block_broadcaster::BlockBroadcasterCommand, block_sequencer::BlockSequencerControlCommand};

pub enum AppCommand {
    CrashCommit(u64 /* ci */, Vec<CachedBlock> /* all blocks from old_ci + 1 to new_ci */)
}

pub enum ClientReplyCommand {
    CancelAllRequests
}


struct CachedBlockWithVotes {
    block: CachedBlock,
    
    vote_sigs: HashMap<String, ProtoSignatureArrayEntry>,
    replication_set: HashSet<String>
}

pub type VoteWithSender = (String /* Sender */, ProtoVote);
pub type QCWithBlockN = (u64 /* Block the QC was attached to */, ProtoQuorumCertificate);

/// This is where all the consensus decisions are made.
/// Feeds in blocks from block_broadcaster
/// Waits for vote / Sends vote.
/// Whatever makes it to this stage must have been properly verified before.
/// So this stage will not bother about checking cryptographic validity.
pub struct Staging {
    config: AtomicConfig,
    client: PinnedClient,
    crypto: CryptoServiceConnector,

    ci: u64,
    bci: u64,
    view: u64,
    view_is_stable: bool,
    config_num: u64,
    

    /// Invariant: pending_blocks.len() == 0 || bci == pending_blocks.front().n - 1
    pending_blocks: VecDeque<CachedBlockWithVotes>,

    /// Stores "1-hop" QCs, if adding a new QC creates QC on QC, it is removed and bci is incremented.
    pending_qcs: VecDeque<QCWithBlockN>,

    view_change_timer: Arc<Pin<Box<ResettableTimer>>>,

    block_rx: Receiver<CachedBlock>,
    vote_rx: Receiver<VoteWithSender>,
    view_change_rx: Receiver<ProtoViewChange>,

    client_reply_tx: Sender<ClientReplyCommand>,
    app_tx: Sender<AppCommand>,
    block_broadcaster_command_tx: Sender<BlockBroadcasterCommand>,
    block_sequencer_command_tx: Sender<BlockSequencerControlCommand>,
    qc_tx: Sender<ProtoQuorumCertificate>,
}


impl Staging {
    pub fn new(
        config: AtomicConfig, client: PinnedClient, crypto: CryptoServiceConnector,
        block_rx: Receiver<CachedBlock>, vote_rx: Receiver<VoteWithSender>, view_change_rx: Receiver<ProtoViewChange>,
        client_reply_tx: Sender<ClientReplyCommand>, app_tx: Sender<AppCommand>,
        block_broadcaster_command_tx: Sender<BlockBroadcasterCommand>,
        block_sequencer_command_tx: Sender<BlockSequencerControlCommand>,
        qc_tx: Sender<ProtoQuorumCertificate>,
    ) -> Self {
        let _config = config.get();
        let _chan_depth = _config.rpc_config.channel_depth as usize;
        let view_timeout = Duration::from_millis(_config.consensus_config.view_timeout_ms);
        let view_change_timer = ResettableTimer::new(view_timeout);
        
        Self {
            config,
            client,
            crypto,
            ci: 0,
            bci: 0,
            view: 1, // TODO: For now
            view_is_stable: true, // TODO: For now
            config_num: 1,
            pending_blocks: VecDeque::with_capacity(_chan_depth),
            pending_qcs: VecDeque::with_capacity(_chan_depth),
            view_change_timer,
            block_rx,
            vote_rx,
            view_change_rx,
            client_reply_tx,
            app_tx,
            block_broadcaster_command_tx,
            block_sequencer_command_tx,
            qc_tx
        }
    }

    pub async fn run(staging: Arc<Mutex<Self>>) {
        let mut staging = staging.lock().await;
        let view_timer_handle = staging.view_change_timer.run().await;
        loop {
            if let Err(_) = staging.worker().await {
                break;
            }
        }

        view_timer_handle.abort();
    }

    fn i_am_leader(&self) -> bool {
        let config = self.config.get();
        let leader = config.consensus_config.get_leader_for_view(self.view);
        leader == config.net_config.name
    }

    async fn worker(&mut self) -> Result<(), ()> {
        let i_am_leader = self.i_am_leader();

        tokio::select! {
            _tick = self.view_change_timer.wait() => {
                self.handle_view_change_timer_tick().await?;
            },
            block = self.block_rx.recv() => {
                if block.is_none() {
                    return Err(())
                }
                let block = block.unwrap();
                debug!("Got {}", block.block.n);
                if i_am_leader {
                    self.process_block_as_leader(block).await?;
                } else {
                    self.process_block_as_follower(block).await?;
                }
            },
            vote = self.vote_rx.recv() => {
                if vote.is_none() {
                    return Err(())
                }
                let vote = vote.unwrap();
                if i_am_leader {
                    self.process_vote(vote.0, vote.1).await?;
                } else {
                    warn!("Received vote while being a follower");
                }
            },
            vc_msg = self.view_change_rx.recv() => {
                if vc_msg.is_none() {
                    return Err(())
                }
                let vc_msg = vc_msg.unwrap();
                self.process_view_change_message(vc_msg).await?;
            }
        }

        Ok(())
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

    fn check_continuity(&self, block: &CachedBlock) -> bool {
        match self.pending_blocks.back() {
            Some(b) => {
                let res = b.block.block.n + 1 == block.block.n && block.block.parent.eq(&b.block.block_hash);

                if !res {
                    let hash_match = block.block.parent.eq(&b.block.block_hash);
                    error!("Current last block: {} Incoming block: {} Hash link match: {}", b.block.block.n, block.block.n, hash_match);
                }

                res
            },
            None => block.block.n == self.bci + 1,
        }
    }


    async fn handle_view_change_timer_tick(&mut self) -> Result<(), ()> {
        Ok(())
    }

    async fn vote_on_last_block_for_self(&mut self) -> Result<(), ()> {
        let name = self.config.get().net_config.name.clone();

        let last_block = match self.pending_blocks.back() {
            Some(b) => b,
            None => return Err(()),
        };


        let mut vote = ProtoVote { 
            sig_array: Vec::with_capacity(1),
            fork_digest: last_block.block.block_hash.clone(),
            n: last_block.block.block.n,
            view: self.view,
            config_num: self.config_num
        };

        // If this block is signed, need a signature for the vote.
        if let Some(Sig::ProposerSig(_)) = last_block.block.block.sig {
            let vote_sig = self.crypto.sign(&last_block.block.block_hash).await;

            vote.sig_array.push(ProtoSignatureArrayEntry {
                n: last_block.block.block.n,
                sig: vote_sig.to_vec(),
            });
        }

        self.process_vote(name, vote).await
    }

    #[async_recursion]
    async fn process_block_as_leader(&mut self, block: CachedBlock) -> Result<(), ()> {
        if block.block.view < self.view {
            // Do not accept anything from a lower view.
            return Ok(());
        } else if block.block.view == self.view {
            // Within the same view, the log must be append-only.
            if !self.check_continuity(&block) {
                println!("Continuity broken");
                return Ok(());
            }
        } else {
            // If from a higher view, this must mean I am no longer the leader in the cluster.
            // Precondition: The blocks I am receiving have been verified earlier,
            // So the sequence of new view messages have been verified before reaching here.
            

            // Jump to the new view
            self.view = block.block.view;
            self.view_is_stable = block.block.view_is_stable;
            self.config_num = block.block.config_num;

            // Flush the pending queue and cancel client requests.
            self.pending_blocks.clear();
            self.pending_qcs.clear();
            self.client_reply_tx.send(ClientReplyCommand::CancelAllRequests).await.unwrap();
            
            // Ready to accept the block normally.
            if self.i_am_leader() {
                return self.process_block_as_leader(block).await;
            } else {
                return self.process_block_as_follower(block).await;
            }
        }

        // Postcondition here: block.view == self.view && check_continuity() == true && i_am_leader
        let block_with_votes = CachedBlockWithVotes {
            block,
            vote_sigs: HashMap::new(),
            replication_set: HashSet::new()
        };

        self.pending_blocks.push_back(block_with_votes);

        // Now vote for self
        self.vote_on_last_block_for_self().await?;


        Ok(())
    }

    async fn process_block_as_follower(&mut self, block: CachedBlock) -> Result<(), ()> {
        println!("process_block_as_follower");
        Ok(())
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

    async fn maybe_crash_commit(&mut self) -> Result<(), ()> {
        let old_ci = self.ci;
        for block in self.pending_blocks.iter() {
            if block.block.block.n <= self.ci {
                continue;
            }

            if block.replication_set.len() >= self.crash_commit_threshold() {
                self.ci = block.block.block.n;
            }
        }
        let new_ci = self.ci;

        if new_ci > old_ci {
            // Notify
            self.block_broadcaster_command_tx.send(BlockBroadcasterCommand::UpdateCI(new_ci)).await.unwrap();

            let blocks = self.pending_blocks.iter()
                .filter(|e| e.block.block.n > old_ci && e.block.block.n <= new_ci)
                .map(|e| e.block.clone())
                .collect();
            self.app_tx.send(AppCommand::CrashCommit(new_ci, blocks)).await.unwrap();
        }

        Ok(())
    }

    async fn maybe_create_qcs(&mut self) -> Result<(), ()> {

        Ok(())
    }

    /// Precondition: All qcs in self.pending_qcs has same view as qc.view.
    /// If \E (block_n, qc2) in self.pending_qcs: qc.n >= block_n then qc2.n is byz committed.
    /// This is PBFT/Jolteon-style 2-hop rule.
    /// If this happens then all QCs in self.pending_qcs such that block_n <= new_bci is removed.
    async fn maybe_byzantine_commit(&mut self, qc: ProtoQuorumCertificate) -> Result<(), ()> {
        // TODO::::
        Ok(())
    }

    async fn process_view_change_message(&mut self, vc_msg: ProtoViewChange) -> Result<(), ()> {
        Ok(())
    }
}

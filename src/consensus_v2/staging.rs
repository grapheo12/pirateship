use std::{collections::VecDeque, pin::Pin, sync::Arc, time::Duration};

use tokio::sync::Mutex;

use crate::{config::AtomicConfig, consensus::timer::ResettableTimer, crypto::CachedBlock, proto::consensus::{ProtoViewChange, ProtoVote}, rpc::client::PinnedClient, utils::channel::{Receiver, Sender}};

use super::{block_broadcaster::BlockBroadcasterCommand, block_sequencer::BlockSequencerControlCommand};

pub struct AppCommand; // TODO

pub struct ClientReplyCommand; // TODO

/// This is where all the consensus decisions are made.
/// Feeds in blocks from block_broadcaster
/// Waits for vote / Sends vote.
/// Whatever makes it to this stage must have been properly verified before.
/// So this stage will not bother about checking cryptographic validity.
pub struct Staging {
    config: AtomicConfig,
    client: PinnedClient,

    ci: u64,
    bci: u64,
    view: u64,
    view_is_stable: bool,
    config_num: u64,

    pending_blocks: VecDeque<CachedBlock>,
    view_change_timer: Arc<Pin<Box<ResettableTimer>>>,

    block_rx: Receiver<CachedBlock>,
    vote_rx: Receiver<ProtoVote>,
    view_change_rx: Receiver<ProtoViewChange>,

    client_reply_tx: Sender<ClientReplyCommand>,
    app_tx: Sender<AppCommand>,
    block_broadcaster_command_tx: Sender<BlockBroadcasterCommand>,
    block_sequencer_command_tx: Sender<BlockSequencerControlCommand>,
}


impl Staging {
    pub fn new(
        config: AtomicConfig, client: PinnedClient,
        block_rx: Receiver<CachedBlock>, vote_rx: Receiver<ProtoVote>, view_change_rx: Receiver<ProtoViewChange>,
        client_reply_tx: Sender<ClientReplyCommand>, app_tx: Sender<AppCommand>,
        block_broadcaster_command_tx: Sender<BlockBroadcasterCommand>,
        block_sequencer_command_tx: Sender<BlockSequencerControlCommand>,
    ) -> Self {
        let _config = config.get();
        let _chan_depth = _config.rpc_config.channel_depth as usize;
        let view_timeout = Duration::from_millis(_config.consensus_config.view_timeout_ms);
        let view_change_timer = ResettableTimer::new(view_timeout);
        
        Self {
            config,
            client,
            ci: 0,
            bci: 0,
            view: 1, // TODO: For now
            view_is_stable: true, // TODO: For now
            config_num: 1,
            pending_blocks: VecDeque::with_capacity(_chan_depth),
            view_change_timer,
            block_rx,
            vote_rx,
            view_change_rx,
            client_reply_tx,
            app_tx,
            block_broadcaster_command_tx,
            block_sequencer_command_tx,
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

    async fn worker(&mut self) -> Result<(), ()> {
        Ok(())
    }
}

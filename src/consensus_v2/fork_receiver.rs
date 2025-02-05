use std::sync::Arc;

use tokio::sync::Mutex;

use crate::{config::AtomicConfig, crypto::{AtomicKeyStore, CachedBlock, CryptoServiceConnector}, proto::consensus::ProtoAppendEntries, rpc::client::PinnedClient, utils::channel::{Receiver, Sender}};


pub enum ForkReceiverCommand {
    UpdateView(u64 /* view num */, u64 /* config num */),
}

/// Receives AppendEntries from other nodes in the network.
/// Verifies the view change and config change sequence in the sent fork.
/// If the fork is valid, it sends the block to staging.
/// If not, this will just drop the message. No further processing/NACKing is done.
/// This WILL NOT CHECK if fork in AppendEntry and my own fork have an intersection or not.
pub struct ForkReceiver {
    config: AtomicConfig,
    crypto: CryptoServiceConnector,

    view: u64,
    config_num: u64,

    fork_rx: Receiver<ProtoAppendEntries>,
    command_rx: Receiver<ForkReceiverCommand>,
    broadcaster_tx: Sender<Vec<CachedBlock>>,
}


impl ForkReceiver {
    pub fn new(
        config: AtomicConfig,
        crypto: CryptoServiceConnector,
        fork_rx: Receiver<ProtoAppendEntries>,
        command_rx: Receiver<ForkReceiverCommand>,
        broadcaster_tx: Sender<Vec<CachedBlock>>,
    ) -> Self {
        Self {
            config,
            crypto,
            view: 1,
            config_num: 1,
            fork_rx,
            command_rx,
            broadcaster_tx,
        }
    }


    pub async fn run(fork_receiver: Arc<Mutex<Self>>) {
        let mut fork_receiver = fork_receiver.lock().await;

        loop {
            if let Err(_) = fork_receiver.worker().await {
                break;
            }
        }
    }

    async fn worker(&mut self) -> Result<(), ()> {
        tokio::select! {
            ae = self.fork_rx.recv() => {
                if let Some(ae) = ae {
                    self.process_fork(ae).await;
                }
            },
            cmd = self.command_rx.recv() => {
                if let Some(ForkReceiverCommand::UpdateView(view, config_num)) = cmd {
                    self.view = view;
                    self.config_num = config_num;
                }
            }
        }

        Ok(())
    }

    async fn process_fork(&self, ae: ProtoAppendEntries) {
        let fork = match &ae.fork {
            Some(f) => f,
            None => return,
        };

        if ae.view < self.view {
            return;
        }

        
    }
}
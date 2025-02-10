use std::{collections::VecDeque, io::Error, sync::Arc};

use log::warn;
use tokio::sync::{Mutex, oneshot};

use crate::{config::AtomicConfig, crypto::{CachedBlock, CryptoServiceConnector}, proto::consensus::{HalfSerializedBlock, ProtoAppendEntries}, utils::channel::{Receiver, Sender}};


pub enum ForkReceiverCommand {
    UpdateView(u64 /* view num */, u64 /* config num */), // Also acts as a Ack for MultiPartFork
    MultipartNack(usize /* delete these many parts from the multipart buffer */)
}

/// Shortcut stats for AppendEntries
#[derive(Debug, Clone)]
pub struct AppendEntriesStats {
    pub view: u64,
    pub config_num: u64,
    pub sender: String,
    pub ci: u64,
}

pub struct MultipartFork {
    pub fork_future: Vec<   // vector of ...
        Option <
            oneshot::Receiver<  // futures that will return ...
                Result<CachedBlock, Error> // a block or an error
            >
        > // The option is just to make it easier to remove the future from the vector
    >,
    pub remaining_parts: usize, // How many other such MultipartForks are there?
    pub ae_stats: AppendEntriesStats,
    
}

impl MultipartFork {
    pub async fn await_all(&mut self) -> Vec<Result<CachedBlock, Error>> {
        let mut results = Vec::with_capacity(self.fork_future.len());
        for future in self.fork_future.iter_mut() {
            results.push(future.take().unwrap()
                .await.unwrap());
        }
        results
    }
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

    fork_rx: Receiver<(ProtoAppendEntries, String /* Sender */)>,
    command_rx: Receiver<ForkReceiverCommand>,
    broadcaster_tx: Sender<MultipartFork>,

    multipart_buffer: VecDeque<(Vec<HalfSerializedBlock>, AppendEntriesStats)>,

    // Invariant <blocked_on_multipart>: multipart_buffer contains only parts from one AppendEntries.
    // If multipart_buffer is empty, blocked_on_multipart must be false.
    // If blocked_on_multipart is true, multipart_buffer must not be empty
    // AND no new AppendEntries will be processed.
    blocked_on_multipart: bool,
}


impl ForkReceiver {
    pub fn new(
        config: AtomicConfig,
        crypto: CryptoServiceConnector,
        fork_rx: Receiver<(ProtoAppendEntries, String)>,
        command_rx: Receiver<ForkReceiverCommand>,
        broadcaster_tx: Sender<MultipartFork>,
    ) -> Self {
        Self {
            config,
            crypto,
            view: 1,
            config_num: 1,
            fork_rx,
            command_rx,
            broadcaster_tx,
            multipart_buffer: VecDeque::new(),
            blocked_on_multipart: false,
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
        if self.blocked_on_multipart {
            let cmd = self.command_rx.recv().await.unwrap();
            self.handle_command(cmd).await;
        } else {
            tokio::select! {
                ae_sender = self.fork_rx.recv() => {
                    if let Some((ae, sender)) = ae_sender {
                        self.process_fork(ae, sender).await;
                    }
                },
                cmd = self.command_rx.recv() => {
                    if let Some(cmd) = cmd {
                        self.handle_command(cmd).await;
                    }
                }
            }
        }

        Ok(())
    }

    fn get_leader_for_view(&self, view: u64) -> String {
        self.config.get().consensus_config.get_leader_for_view(view)
    }

    async fn process_fork(&mut self, mut ae: ProtoAppendEntries, sender: String) {
        let fork = match &mut ae.fork {
            Some(f) => f,
            None => return,
        };

        if ae.view < self.view || ae.config_num < self.config_num {
            return;
        }

        if ae.config_num == self.config_num {
            // If in the same config, I can check if the sender is supposed to send this AE.
            let leader = self.get_leader_for_view(ae.view);
            if leader != sender {
                return;
            }
        }


        // Not going to check for ViewLock here.
        // Staging takes care of that.


        if ae.view > self.view {
            // The first block of each view from self.view+1..=ae.view must have view_is_stable = false
            let mut test_view = self.view;
            for block in &fork.serialized_blocks {
                if block.view > test_view {
                    if block.view_is_stable {
                        // TODO: NACK the AppendEntries; ask to backfill.
                        // The first block of the new view must have view_is_stable = false
                    }

                    test_view = block.view;
                }
            }

            if test_view < ae.view {
                warn!("Malformed AppendEntries: Missing blocks for view {} in AppendEntries", ae.view);
                // TODO: NAck the AppendEntries; ask to backfill.
            }
        }



        // The fork will have the following general structure
        // (view1, config1) <- (view1, config1) <- ... <- (view1, config1)
        // <- (view2, config1, <New View block with view_is_stable = false>)
        // <- ... <- (ViewN, config2, <New View block with view_is_stable = false>) <- (viewN, config2) ...

        // Since keystore will change with config change (or before that, but definitely not after),
        // we can only verify blocks with same config + the first (unstable) block of the next config, in parallel.
        // Once we verify and forward that to staging,
        // we must wait until staging updates its view and config.
        let mut parts = Vec::new();
        let mut curr_part = Some(Vec::new());
        let mut curr_config = self.config_num;

        for block in fork.serialized_blocks.drain(..) {
            if block.config_num == curr_config {
                curr_part.as_mut().unwrap().push(block);
            } else {
                curr_config = block.config_num;
                // First block of the new config must have view_is_stable = false
                if block.view_is_stable {
                    warn!("Invalid block in AppendEntries: First block for config {} has view_is_stable = true", curr_config);
                    
                    // TODO: NACK the AppendEntries; ask to backfill.
                    
                    return;
                }
                curr_part.as_mut().unwrap().push(block);
                if let Some(part) = curr_part.take() {
                    parts.push(part);
                }

                curr_part = Some(Vec::new());
            }
        }

        if let Some(part) = curr_part.take() {
            if part.len() > 0 {
                parts.push(part);
            }
        }

        if parts.len() == 0 {
            // Unreachable
            return;
        }

        let first_part = parts.remove(0);
        let multipart_fut = self.crypto.prepare_fork(first_part, parts.len(), AppendEntriesStats {
            view: ae.view,
            config_num: ae.config_num,
            sender: sender.clone(),
            ci: ae.commit_index,
        }).await;
        self.broadcaster_tx.send(multipart_fut).await.unwrap();

        if parts.len() > 0 {
            assert_eq!(self.multipart_buffer.len(), 0); // Due to the Invariant <blocked_on_multipart>
            self.multipart_buffer.extend(parts.iter().map(|part| (part.clone(), AppendEntriesStats {
                view: ae.view,
                config_num: ae.config_num,
                sender: sender.clone(),
                ci: ae.commit_index,
            })));
            self.blocked_on_multipart = true;
        }


    }

    async fn handle_command(&mut self, cmd: ForkReceiverCommand) {
        match cmd {
            ForkReceiverCommand::UpdateView(view, config_num) => {
                self.view = view;
                let config_is_updating = self.config_num < config_num;
                self.config_num = config_num;

                if config_is_updating && self.multipart_buffer.len() > 0 {
                    // Forward the next multipart buffer
                    let (part, ae_stats) = self.multipart_buffer.pop_front().unwrap();
                    
                    // If this is the last part, then the sender must be the leader for this view in this config.
                    let maybe_legit =
                    if self.multipart_buffer.len() == 0 {
                        let leader = self.get_leader_for_view(self.view);
                        if leader == ae_stats.sender {
                            true
                        } else {
                            false
                        }
                    } else {
                        true
                    };

                    if maybe_legit {
                        let multipart_fut = self.crypto.prepare_fork(part, self.multipart_buffer.len(), ae_stats).await;
                        self.broadcaster_tx.send(multipart_fut).await.unwrap();
                    }

                }

                if self.multipart_buffer.len() == 0 {
                    self.blocked_on_multipart = false;
                }
            },
            ForkReceiverCommand::MultipartNack(n) => {
                assert_eq!(n, self.multipart_buffer.len()); // Due to Invariant <blocked_on_multipart>
                for _ in 0..n {
                    self.multipart_buffer.pop_front();
                }

                if self.multipart_buffer.len() == 0 {
                    self.blocked_on_multipart = false;
                }
            }
        }
    }
}
use std::{cell::RefCell, io::{Error, ErrorKind}, sync::Arc};

use log::{debug, error, info, trace};
use prost::Message;
use tokio::sync::{oneshot, Mutex};

use crate::{config::AtomicConfig, crypto::{AtomicKeyStore, CachedBlock}, proto::{consensus::{HalfSerializedBlock, ProtoAppendEntries, ProtoFork}, rpc::ProtoPayload}, rpc::{client::{Client, PinnedClient}, server::LatencyProfile, PinnedMessage, SenderType}, utils::{channel::{Receiver, Sender}, PerfCounter, StorageAck, StorageServiceConnector}};

use super::{app::AppCommand, fork_receiver::{AppendEntriesStats, ForkReceiverCommand, MultipartFork}};

pub enum BlockBroadcasterCommand {
    UpdateCI(u64),
    NextAEForkPrefix(Vec<oneshot::Receiver<Result<CachedBlock, Error>>>),
}

pub struct BlockBroadcaster {
    config: AtomicConfig,
    ci: u64,
    fork_prefix_buffer: Vec<CachedBlock>,
    
    // Input ports
    my_block_rx: Receiver<(u64, oneshot::Receiver<CachedBlock>)>,
    other_block_rx: Receiver<MultipartFork>,
    control_command_rx: Receiver<BlockBroadcasterCommand>,
    
    // Output ports
    storage: StorageServiceConnector,
    client: PinnedClient,
    staging_tx: Sender<(CachedBlock, oneshot::Receiver<StorageAck>, AppendEntriesStats)>,

    // Command ports
    fork_receiver_command_tx: Sender<ForkReceiverCommand>,
    app_command_tx: Sender<AppCommand>,

    // Perf Counters
    my_block_perf_counter: RefCell<PerfCounter<u64>>

}

impl BlockBroadcaster {
    pub fn new(
        config: AtomicConfig,
        client: PinnedClient,
        my_block_rx: Receiver<(u64, oneshot::Receiver<CachedBlock>)>,
        other_block_rx: Receiver<MultipartFork>,
        control_command_rx: Receiver<BlockBroadcasterCommand>,
        storage: StorageServiceConnector,
        staging_tx: Sender<(CachedBlock, oneshot::Receiver<StorageAck>, AppendEntriesStats)>,
        fork_receiver_command_tx: Sender<ForkReceiverCommand>,
        app_command_tx: Sender<AppCommand>,
    ) -> Self {

        let my_block_event_order = vec![
            "Retrieve prepared block",
            "Store block",
            "Forward block to logserver",
            "Forward block to staging",
            "Serialize",
            "Forward block to other nodes",
        ];

        let my_block_perf_counter = RefCell::new(PerfCounter::new("BlockBroadcasterMyBlock", &my_block_event_order));
        
        Self {
            config,
            ci: 0,
            fork_prefix_buffer: Vec::new(),
            my_block_rx,
            other_block_rx,
            control_command_rx,
            storage,
            client,
            staging_tx,
            fork_receiver_command_tx,
            app_command_tx,
            my_block_perf_counter,
        }
    }

    pub async fn run(block_broadcaster: Arc<Mutex<Self>>) {
        let mut block_broadcaster = block_broadcaster.lock().await;

        let mut total_work = 0;
        loop {
            if let Err(_e) = block_broadcaster.worker().await {
                break;
            }

            total_work += 1;
            if total_work % 1000 == 0 {
                block_broadcaster.my_block_perf_counter.borrow().log_aggregate();
            }

        }

        info!("Broadcasting worker exited.");
    }

    fn perf_register(&mut self, entry: u64) {
        #[cfg(feature = "perf")]
        self.my_block_perf_counter.borrow_mut().register_new_entry(entry);
    }

    fn perf_add_event(&mut self, entry: u64, event: &str) {
        #[cfg(feature = "perf")]
        self.my_block_perf_counter.borrow_mut().new_event(event, &entry);
    }

    fn perf_deregister(&mut self, entry: u64) {
        #[cfg(feature = "perf")]
        self.my_block_perf_counter.borrow_mut().deregister_entry(&entry);
    }

    async fn worker(&mut self) -> Result<(), Error> {
        // This worker doesn't care about views and configs.
        // Its only job is to store and forward.
        // If it is my block, forward to {all other nodes, logserver and staging}.
        // It it is not my block, forward to {logserver and staging}.

        // Invariant: Anything that outputs from Block broadcaster is stored on disk.

        // Logserver and staging will take care of hash-chaining logic and everything else.
        tokio::select! {
            block = self.my_block_rx.recv() => {
                if block.is_none() {
                    return Err(Error::new(ErrorKind::BrokenPipe, "my_block_rx channel closed"));
                }
                let block = block.unwrap();
                let __n = block.0;
                // info!("Expecting {}", __n);

                let perf_entry = block.0;
                self.perf_register(perf_entry);
                let block = block.1.await;
                self.perf_add_event(perf_entry, "Retrieve prepared block");
                if block.is_err() {
                    error!("Failed to get block {} {:?}", __n, block);
                    return Ok(());
                }
                self.process_my_block(block.unwrap()).await?;

                trace!("Processed block {}", __n);
            },

            block_vec = self.other_block_rx.recv() => {
                if block_vec.is_none() {
                    return Err(Error::new(ErrorKind::BrokenPipe, "other_block_rx channel closed"));
                }
                let blocks = block_vec.unwrap();
                // info!("Processing other block");
                self.process_other_block(blocks).await?;
                // info!("Processed other block");
            },

            cmd = self.control_command_rx.recv() => {
                if cmd.is_none() {
                    return Err(Error::new(ErrorKind::BrokenPipe, "control_command_rx channel closed"));
                }
                // info!("Processing control command");
                self.handle_control_command(cmd.unwrap()).await?;
                // info!("Processed control command");
            }
        }

        Ok(())
    }

    fn get_everyone_except_me(&self) -> Vec<String> {
        let config = self.config.get();
        let me = &config.net_config.name;
        let mut node_list = config.consensus_config.node_list
            .iter().filter(|e| *e != me).map(|e| e.clone())
            .collect::<Vec<_>>();

        node_list.extend(
            config.consensus_config.learner_list.iter().map(|e| e.clone())
        );

        node_list
    }

    async fn handle_control_command(&mut self, cmd: BlockBroadcasterCommand) -> Result<(), Error> {
        match cmd {
            BlockBroadcasterCommand::UpdateCI(ci) => self.ci = ci,
            BlockBroadcasterCommand::NextAEForkPrefix(blocks) => {
                for block in blocks {
                    let block = block.await.unwrap().expect("Failed to get block");
                    self.fork_prefix_buffer.push(block);
                }
            }
        }

        Ok(())
    }

    async fn store_and_forward_internally(&mut self, block: &CachedBlock, ae_stats: AppendEntriesStats) -> Result<(), Error> {
        let perf_entry = block.block.n;
        
        // Store
        let storage_ack = self.storage.put_block(block).await;
        self.perf_add_event(perf_entry, "Store block");
        // info!("Stored {}", block.block.n);
    
        // Forward
        self.perf_add_event(perf_entry, "Forward block to logserver");

        // info!("Sending {}", block.block.n);
        self.staging_tx.send((block.clone(), storage_ack, ae_stats)).await.unwrap();
        // info!("Sent {}", block.block.n);
        self.perf_add_event(perf_entry, "Forward block to staging");

        Ok(())
    }

    async fn process_my_block(&mut self, block: CachedBlock) -> Result<(), Error> {
        debug!("Processing {}", block.block.n);
        let perf_entry = block.block.n;

        let (view, view_is_stable, config_num) = (block.block.view, block.block.view_is_stable, block.block.config_num);
        // First forward all blocks that were in the fork prefix buffer.
        let mut ae_fork = Vec::new();

        for block in self.fork_prefix_buffer.drain(..) {
            ae_fork.push(block);
        }
        ae_fork.push(block.clone());

        if ae_fork.len() > 1 {
            trace!("AE: {:?}", ae_fork);
        }

        for block in &ae_fork {
            self.store_and_forward_internally(&block, AppendEntriesStats {
                view,
                view_is_stable: block.block.view_is_stable,
                config_num,
                sender: self.config.get().net_config.name.clone(),
                ci: self.ci,
            }).await?;
        }  
        // Forward to app for stats.
        self.app_command_tx.send(AppCommand::NewRequestBatch(block.block.n, view, view_is_stable, true, block.block.tx_list.len(), block.block_hash.clone())).await.unwrap();
        // Forward to other nodes. Involves copies and serialization so done last.

        let names = self.get_everyone_except_me();
        let append_entry = ProtoAppendEntries {
            fork: Some(ProtoFork {
                serialized_blocks: ae_fork.drain(..).map(|block| HalfSerializedBlock { 
                    n: block.block.n,
                    view: block.block.view,
                    view_is_stable: block.block.view_is_stable,
                    config_num: block.block.config_num,
                    serialized_body: block.block_ser.clone(), 
                }).collect(),
            }),
            commit_index: self.ci,
            view,
            view_is_stable,
            config_num,
        };
        // let data = bincode::serialize(&append_entry).unwrap();
        // let data = bitcode::encode(&append_entry);
        let rpc = ProtoPayload {
            message: Some(crate::proto::rpc::proto_payload::Message::AppendEntries(append_entry)),
        };
        let data = rpc.encode_to_vec();
        self.perf_add_event(perf_entry, "Serialize");
        let sz = data.len();
        let data = PinnedMessage::from(data, sz, SenderType::Anon);
        let mut profile = LatencyProfile::new();
        let _res = PinnedClient::broadcast(&self.client, &names, &data, &mut profile, self.get_byzantine_broadcast_threshold()).await;
        self.perf_add_event(perf_entry, "Forward block to other nodes");

        self.perf_deregister(perf_entry);

        Ok(())

    }

    fn get_byzantine_broadcast_threshold(&self) -> usize {
        let config = self.config.get();
        let node_list_len = config.consensus_config.node_list.len();
        if node_list_len <= config.consensus_config.liveness_u as usize {
            return 0;
        }
        let byzantine_threshold = node_list_len - config.consensus_config.liveness_u as usize;
        byzantine_threshold - 1
    }

    async fn process_other_block(&mut self, mut blocks: MultipartFork) -> Result<(), Error> {
        let _blocks = blocks.await_all().await;
        // info!("Await all finished!");
        let num_parts = blocks.remaining_parts;

        for block in &_blocks {
            if let Err(e) = block {
                error!("This multipart fork is corrupted, I have no use for the remaining parts. {:?}", e);
                let _ = self.fork_receiver_command_tx.send(ForkReceiverCommand::MultipartNack(num_parts)).await;

                return Ok(());
            }
        }

        let (view, view_is_stable) = (blocks.ae_stats.view, blocks.ae_stats.view_is_stable);
        for block in _blocks {
            let block = block.unwrap();
            // info!("Processing {}", block.block.n);
            self.store_and_forward_internally(&block, blocks.ae_stats.clone()).await?;

            // Forward to app for stats.
            self.app_command_tx.send(AppCommand::NewRequestBatch(block.block.n, view, view_is_stable, false, block.block.tx_list.len(), block.block_hash.clone())).await.unwrap();

        }

        Ok(())
    } 


}
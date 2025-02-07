use std::{io::{Error, ErrorKind}, sync::Arc};

use log::debug;
use prost::Message;
use tokio::sync::{oneshot, Mutex};

use crate::{config::AtomicConfig, crypto::{AtomicKeyStore, CachedBlock}, proto::{consensus::{HalfSerializedBlock, ProtoAppendEntries, ProtoFork}, rpc::ProtoPayload}, rpc::{client::{Client, PinnedClient}, server::LatencyProfile, PinnedMessage, SenderType}, utils::{channel::{Receiver, Sender}, StorageAck, StorageServiceConnector}};

use super::fork_receiver::{AppendEntriesStats, ForkReceiverCommand, MultipartFork};

pub enum BlockBroadcasterCommand {
    UpdateCI(u64)
}

pub struct BlockBroadcaster {
    config: AtomicConfig,
    ci: u64,
    
    // Input ports
    my_block_rx: Receiver<(u64, oneshot::Receiver<CachedBlock>)>,
    other_block_rx: Receiver<MultipartFork>,
    control_command_rx: Receiver<BlockBroadcasterCommand>,
    
    // Output ports
    storage: StorageServiceConnector,
    client: PinnedClient,
    staging_tx: Sender<(CachedBlock, oneshot::Receiver<StorageAck>, AppendEntriesStats)>,
    logserver_tx: Sender<CachedBlock>,

    // Command ports
    fork_receiver_command_tx: Sender<ForkReceiverCommand>,

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
        logserver_tx: Sender<CachedBlock>,
        fork_receiver_command_tx: Sender<ForkReceiverCommand>,
    ) -> Self {
        
        Self {
            config,
            ci: 0,
            my_block_rx,
            other_block_rx,
            control_command_rx,
            storage,
            client,
            staging_tx,
            logserver_tx,
            fork_receiver_command_tx,
        }
    }

    pub async fn run(block_broadcaster: Arc<Mutex<Self>>) {
        let mut block_broadcaster = block_broadcaster.lock().await;

        loop {
            if let Err(_) = block_broadcaster.worker().await {
                break;
            }
        }
    }

    pub async fn worker(&mut self) -> Result<(), Error> {
        // This worker doesn't care about views and configs.
        // Its only job is to store and forward.
        // If it is my block, forward to {all other nodes, logserver and staging}.
        // It it is not my block, forward to {logserver and staging}.

        // Invariant: Anything that outputs from Block broadcaster is stored on disk.

        // Logserver and staging will take care of hash-chaining logic and everything else.

        tokio::select! {
            block = self.my_block_rx.recv() => {
                if block.is_none() {
                    return Err(Error::new(ErrorKind::BrokenPipe, "channel closed"));
                }
                let block = block.unwrap();
                debug!("Expecting {}", block.0);
                let block = block.1.await;
                self.process_my_block(block.unwrap()).await?;

            },

            block_vec = self.other_block_rx.recv() => {
                if block_vec.is_none() {
                    return Err(Error::new(ErrorKind::BrokenPipe, "channel closed"));
                }
                let blocks = block_vec.unwrap();
                self.process_other_block(blocks).await?;
            },

            cmd = self.control_command_rx.recv() => {
                if cmd.is_none() {
                    return Err(Error::new(ErrorKind::BrokenPipe, "channel closed"));
                }
                self.handle_control_command(cmd.unwrap()).await?;
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
        }

        Ok(())
    }

    async fn store_and_forward_internally(&mut self, block: &CachedBlock, ae_stats: AppendEntriesStats) -> Result<(), Error> {
        // Store
        let storage_ack = self.storage.put_block(block).await;
    
    
        // Forward
        // Unfortunate cloning.
        self.logserver_tx.send(block.clone()).await.unwrap();

        debug!("Sending {}", block.block.n);
        self.staging_tx.send((block.clone(), storage_ack, ae_stats)).await.unwrap();

        Ok(())
    }

    async fn process_my_block(&mut self, block: CachedBlock) -> Result<(), Error> {
        debug!("Processing {}", block.block.n);
        let (view, view_is_stable, config_num) = (block.block.view, block.block.view_is_stable, block.block.config_num);
        self.store_and_forward_internally(&block, AppendEntriesStats {
            view,
            config_num,
            sender: self.config.get().net_config.name.clone(),
            ci: self.ci,
        }).await?;


        // Forward to other nodes. Involves copies and serialization so done last.

        let names = self.get_everyone_except_me();
        let append_entry = ProtoAppendEntries {
            fork: Some(ProtoFork {
                serialized_blocks: vec![HalfSerializedBlock { 
                    n: block.block.n,
                    view: block.block.view,
                    view_is_stable: block.block.view_is_stable,
                    config_num: block.block.config_num,
                    serialized_body: block.block_ser 
                }],
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
        let sz = data.len();
        let data = PinnedMessage::from(data, sz, SenderType::Anon);
        let mut profile = LatencyProfile::new();
        PinnedClient::broadcast(&self.client, &names, &data, &mut profile).await?;

        Ok(())

    }

    async fn process_other_block(&mut self, mut blocks: MultipartFork) -> Result<(), Error> {
        let _blocks = blocks.await_all().await;
        let num_parts = blocks.remaining_parts;

        for block in &_blocks {
            if let Err(_) = block {
                // This multipart fork is corrupted, I have no use for the remaining parts.
                let _ = self.fork_receiver_command_tx.send(ForkReceiverCommand::MultipartNack(num_parts)).await;

                return Ok(());
            }
        }

        for block in _blocks {
            let block = block.unwrap();
            debug!("Processing {}", block.block.n);
            self.store_and_forward_internally(&block, blocks.ae_stats.clone()).await?;
        }

        Ok(())
    }


}
use std::sync::Arc;

use tokio::sync::Mutex;

use crate::{config::AtomicConfig, crypto::{AtomicKeyStore, CachedBlock}, rpc::client::{Client, PinnedClient}, utils::{channel::{Receiver, Sender}, StorageServiceConnector}};


pub enum BlockBroadcasterControlCommand {
    NewView(u64)            // Need to signal advent of new view, so I can adjust who the leader is.
}

pub struct BlockBroadcaster {
    config: AtomicConfig,
    view: u64,
    
    // Input ports
    my_block_rx: Receiver<CachedBlock>,
    other_block_rx: Receiver<CachedBlock>,
    control_command_rx: Receiver<BlockBroadcasterControlCommand>,
    
    // Output ports
    storage: StorageServiceConnector,
    client: PinnedClient,
    staging_tx: Sender<CachedBlock>,
    logserver_tx: Sender<CachedBlock>,

}

impl BlockBroadcaster {
    pub fn new(
        config: AtomicConfig,
        client: PinnedClient,
        my_block_rx: Receiver<CachedBlock>, other_block_rx: Receiver<CachedBlock>,
        control_command_rx: Receiver<BlockBroadcasterControlCommand>,
        storage: StorageServiceConnector,
        staging_tx: Sender<CachedBlock>,
        logserver_tx: Sender<CachedBlock>,
    ) -> Self {
        
        Self {
            config,
            view: 1,
            my_block_rx,
            other_block_rx,
            control_command_rx,
            storage,
            client,
            staging_tx,
            logserver_tx,
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

    pub async fn worker(&mut self) -> Result<(), ()> {
        // let new_batch_tx = 
        Ok(())
    }
}
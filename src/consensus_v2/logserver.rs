use std::{collections::VecDeque, sync::Arc};

use tokio::sync::Mutex;

use crate::{config::AtomicConfig, crypto::CachedBlock, proto::checkpoint::ProtoBackfillNack, utils::{channel::Receiver, StorageServiceConnector}};

pub struct LogServer {
    config: AtomicConfig,

    logserver_rx: Receiver<CachedBlock>,
    backfill_request_rx: Receiver<ProtoBackfillNack>,
    gc_rx: Receiver<u64>,


    storage: StorageServiceConnector,
    log: VecDeque<CachedBlock>
}

impl LogServer {
    pub fn new(config: AtomicConfig, logserver_rx: Receiver<CachedBlock>, backfill_request_rx: Receiver<ProtoBackfillNack>, gc_rx: Receiver<u64>, storage: StorageServiceConnector) -> Self {
        LogServer {
            config,
            logserver_rx,
            backfill_request_rx,
            gc_rx,
            storage,
            log: VecDeque::new()
        }
    }

    pub async fn run(logserver: Arc<Mutex<Self>>) {
        let mut logserver = logserver.lock().await;
        loop {
            if let Err(_) = logserver.worker().await {
                break;
            }
        }
    }


    async fn worker(&mut self) -> Result<(), ()> {
        tokio::select! {
            biased;
            block = self.logserver_rx.recv() => {
                if let Some(block) = block {
                    self.log.push_back(block);
                }
            },

            gc_req = self.gc_rx.recv() => {
                if let Some(gc_req) = gc_req {
                    self.log.retain(|block| block.block.n > gc_req);
                }
            },

            backfill_req = self.backfill_request_rx.recv() => {
                if let Some(backfill_req) = backfill_req {
                    self.respond_backfill(backfill_req).await?;
                }
            }
        }

        Ok(())
    }


    async fn get_block(&mut self, n: u64) -> Option<CachedBlock> {
        let last_n = self.log.back()?.block.n;
        if n == 0 || n > last_n {
            return None;
        }

        let first_n = self.log.front()?.block.n;
        if n < first_n {
            return self.get_gced_block(n).await;
        }

        let block_idx = self.log.binary_search_by(|e| e.block.n.cmp(&n)).ok()?;
        let block = self.log[block_idx].clone();

        Some(block)
    }

    async fn get_gced_block(&mut self, n: u64) -> Option<CachedBlock> {
        // TODO: Fill up block from storage.
        // This would be extremely slow if called repeatedly.
        // Traverse parent hash pointers to get the required hash.
        None
    }

    async fn respond_backfill(&mut self, backfill_req: ProtoBackfillNack) -> Result<(), ()> {
        // TODO: Fill up the response with the requested blocks. Send over the network.
        Ok(())
    }
}
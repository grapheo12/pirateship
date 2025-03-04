use std::{collections::{BTreeMap, HashMap, VecDeque}, sync::Arc};

use log::warn;
use prost::Message as _;
use tokio::sync::Mutex;

use crate::{config::AtomicConfig, crypto::CachedBlock, proto::{checkpoint::{proto_backfill_nack::Origin, ProtoBackfillNack, ProtoBlockHint}, consensus::{HalfSerializedBlock, ProtoAppendEntries, ProtoFork, ProtoViewChange}, rpc::{proto_payload::Message, ProtoPayload}}, rpc::{client::PinnedClient, MessageRef, PinnedMessage}, utils::{channel::Receiver, StorageServiceConnector}};


/// Deletes older blocks in favor of newer ones.
/// If the cache is full, and the block being put() has a lower n than the oldest block in the cache,
/// it is a Noop.
/// Since reading GC blocks always forms the pattern of (read parent hash) -> (fetch block) -> (read parent hash) -> ...
/// There is no need to adjust the position of the block in the cache.
struct ReadCache {
    cache: BTreeMap<u64, CachedBlock>,
    working_set_size: usize,
}

impl ReadCache {
    pub fn new(working_set_size: usize) -> Self {
        if working_set_size == 0 {
            panic!("Working set size cannot be 0");
        }
        ReadCache {
            cache: BTreeMap::new(),
            working_set_size
        }
    }


    /// Return vals:
    /// - Ok(block) if the block is in the cache.
    /// - Err(block) block with the least n higher than the requested block, if the block is not in the cache.
    /// - Err(None) if the cache is just empty.
    pub fn get(&mut self, n: u64) -> Result<CachedBlock, Option<CachedBlock>> {
        if self.cache.is_empty() {
            return Err(None);
        }

        let block = self.cache.get(&n).cloned();
        if let Some(block) = block {
            return Ok(block);
        }

        let next_block = self.cache.range(n..).next()
            .unwrap().1.clone();
        Err(Some(next_block))
    }

    pub fn put(&mut self, block: CachedBlock) {
        if self.cache.len() >= self.working_set_size
            && block.block.n < *self.cache.first_entry().unwrap().key() {
            // Don't put this in the cache.
            return;
        }
        if self.cache.len() >= self.working_set_size {
            self.cache.first_entry().unwrap().remove();
        }

        self.cache.insert(block.block.n, block);
    }
}

pub struct LogServer {
    config: AtomicConfig,
    client: PinnedClient,

    logserver_rx: Receiver<CachedBlock>,
    backfill_request_rx: Receiver<ProtoBackfillNack>,
    gc_rx: Receiver<u64>,


    storage: StorageServiceConnector,
    log: VecDeque<CachedBlock>,

    /// LFU read cache for GCed blocks.
    read_cache: ReadCache,
}

const LOGSERVER_READ_CACHE_WSS: usize = 100;

impl LogServer {
    pub fn new(config: AtomicConfig, client: PinnedClient, logserver_rx: Receiver<CachedBlock>, backfill_request_rx: Receiver<ProtoBackfillNack>, gc_rx: Receiver<u64>, storage: StorageServiceConnector) -> Self {
        LogServer {
            config,
            client,
            logserver_rx,
            backfill_request_rx,
            gc_rx,
            storage,
            log: VecDeque::new(),
            read_cache: ReadCache::new(LOGSERVER_READ_CACHE_WSS),
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
        let first_n = self.log.front()?.block.n;
        if n >= first_n {
            return None; // The block is not GCed.
        }

        // Search in the read cache.
        let starting_point = match self.read_cache.get(n) {
            Ok(block) => {
                return Some(block);
            },
            Err(Some(block)) => {
                block
            },
            Err(None) => {
                // Get the first block in the log.
                self.log.front()?.clone()
            }
        };

        // Fetch the block from the storage.
        let mut ret = starting_point;
        while ret.block.n > n {
            let parent_hash = &ret.block.parent;
            let block = self.storage.get_block(parent_hash).await
                .expect("Failed to get block from storage");
            self.read_cache.put(block.clone());
            ret = block;
        }

        Some(ret)
    }

    async fn respond_backfill(&mut self, backfill_req: ProtoBackfillNack) -> Result<(), ()> {
        let sender = backfill_req.reply_name;
        let hints = backfill_req.hints;
        let existing_fork = match &backfill_req.origin {
            Some(Origin::Ae(ae)) => {
                match ae.fork.as_ref() {
                    Some(fork) => fork,
                    None => {
                        warn!("Malformed request");
                        return Ok(());
                    }
                }
            },

            Some(Origin::Vc(vc)) => {
                match vc.fork.as_ref() {
                    Some(fork) => fork,
                    None => {
                        warn!("Malformed request");
                        return Ok(());
                    }
                }
            },

            None => {
                warn!("Malformed request");
                return Ok(());
            }
        };

        let last_n = match existing_fork.serialized_blocks.last() {
            Some(block) => block.n,
            None => match self.log.back() {
                Some(block) => block.block.n,
                None => 0,
            },
        };

        let first_n = backfill_req.last_index_needed;

        let new_fork = self.fill_fork(first_n, last_n, hints).await;

        let payload = match backfill_req.origin.unwrap() {
            Origin::Ae(ae) => {
                ProtoPayload {
                    message: Some(Message::AppendEntries(ProtoAppendEntries {
                        fork: Some(new_fork),
                        ..ae
                    }))
                }
            },

            Origin::Vc(vc) => {
                ProtoPayload {
                    message: Some(Message::ViewChange(ProtoViewChange {
                        fork: Some(new_fork),
                        ..vc
                    }))
                }
            }
        };

        // Send the payload to the sender.
        let buf = payload.encode_to_vec();

        let _ = PinnedClient::send(&self.client, &sender,
            MessageRef(&buf, buf.len(), &crate::rpc::SenderType::Anon)
        ).await;


        Ok(())
    }

    /// Returns a fork that contains blocks from `first_n` to `last_n` (both inclusive).
    /// During the process, if one of my blocks matches in hints, we stop.
    async fn fill_fork(&mut self, first_n: u64, last_n: u64, mut hints: Vec<ProtoBlockHint>) -> ProtoFork {
        if last_n < first_n {
            panic!("Invalid range");
        }
        
        let hint_map = hints.drain(..).map(|hint| (hint.block_n, hint.digest)).collect::<HashMap<_, _>>();
        
        let mut fork_queue = VecDeque::with_capacity((last_n - first_n + 1) as usize);

        for i in (first_n..=last_n).rev() {
            let block = match self.get_block(i).await {
                Some(block) => block,
                None => {
                    warn!("Block {} not found", i);
                    break;
                }
            };

            let hint = hint_map.get(&i);
            if let Some(hint) = hint {
                if hint.eq(&block.block_hash) {
                    break;
                }
            }

            fork_queue.push_front(block);
        }

        ProtoFork {
            serialized_blocks: fork_queue.into_iter()
                .map(|block| HalfSerializedBlock {
                    n: block.block.n,
                    view: block.block.view,
                    view_is_stable: block.block.view_is_stable,
                    config_num: block.block.config_num,
                    serialized_body: block.block_ser.clone(),
                }).collect(),
        }
    }
}
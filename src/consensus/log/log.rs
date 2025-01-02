use std::{io::{Error, ErrorKind}, ops::Deref, pin::Pin, sync::Arc};

use futures::future::join_all;
use tokio::{sync::{mpsc::{self, Receiver, Sender}, oneshot, Mutex}, task::JoinSet};

use crate::proto::consensus::ProtoBlock;

use super::partition::{LogPartition, LogPartitionConfig};


pub struct LogConfig {
    pub partition_total: u64,
    pub req_channel_depth: usize,
}

#[derive(Debug)]
pub enum LogPartitionResponse {
    Push(Result<u64, Error>),
    LastN(Option<u64>),
}

#[derive(Debug)]
pub enum LogPartitionRequestType {
    Push(ProtoBlock),
    LastN,
}

pub struct LogPartitionRequest {
    pub resp_chan: oneshot::Sender<LogPartitionResponse>,
    pub req: LogPartitionRequestType,
}

pub struct Log {
    pub config: LogConfig,
    pub req_tx: Vec<Sender<LogPartitionRequest>>,
    pub req_rx: Vec<Mutex<Receiver<LogPartitionRequest>>>,
}

pub struct PinnedLog(pub Arc<Pin<Box<Log>>>, JoinSet<()>);

impl Log {
    pub fn new(config: LogConfig) -> Self {
        if config.partition_total == 0 {
            panic!("Invalid config");
        }

        let mut req_tx = Vec::new();
        let mut req_rx = Vec::new();


        for _ in 0..config.partition_total {
            let (tx, rx) = mpsc::channel(config.req_channel_depth);
            req_tx.push(tx);
            req_rx.push(Mutex::new(rx));
        }

        Self {
            config,
            req_tx,
            req_rx,
        }
    }
}

impl PinnedLog {
    pub fn new(config: LogConfig) -> Self {
        Self(Arc::new(Box::pin(Log::new(config))), JoinSet::new())
    }
}

impl Deref for PinnedLog {
    type Target = Log;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl PinnedLog {
    async fn partition_worker(ctx: Arc<Pin<Box<Log>>>, idx: u64) {
        let mut rx = ctx.req_rx[idx as usize].lock().await;
        let mut log_partition = LogPartition::new(LogPartitionConfig {
            partition_id: idx,
            partition_total: ctx.config.partition_total,
        });

        while let Some(msg) = rx.recv().await {
            let resp = match msg.req {
                LogPartitionRequestType::Push(block) => LogPartitionResponse::Push(log_partition.push(block)),
                LogPartitionRequestType::LastN => LogPartitionResponse::LastN(log_partition.last_n()),
            };

            let _ = msg.resp_chan.send(resp);
        }

    }

    pub async fn init(&mut self) {
        for idx in 0..self.config.partition_total {
            let ctx = self.0.clone();
            self.1.spawn(async move {
                PinnedLog::partition_worker(ctx, idx).await;
            });
        }
    }

    pub fn teardown(&mut self) {
        self.1.abort_all();
    }

    fn get_partition_id(&self, n: u64) -> u64 {
        n % self.config.partition_total - 1
    }

    async fn submit_request(&self, req: LogPartitionRequestType, idx: u64) -> LogPartitionResponse {
        let (tx, rx) = oneshot::channel();
        let req = LogPartitionRequest {
            resp_chan: tx,
            req,
        };

        let tx = self.req_tx[idx as usize].clone();
        let _ = tx.send(req).await;

        rx.await.expect("Killed awaiting response")
    }
}

impl Drop for PinnedLog {
    fn drop(&mut self) {
        self.teardown();
    }
}

impl PinnedLog {
    pub async fn push(&self, block: ProtoBlock) -> Result<u64, Error> {
        let idx = self.get_partition_id(block.n);
        let req = LogPartitionRequestType::Push(block);
        let resp = self.submit_request(req, idx).await;

        if let LogPartitionResponse::Push(_r) = resp {
            return _r
        }

        Err(Error::new(ErrorKind::InvalidData, "UNREACHABLE"))
    }

    pub async fn last_n(&self) -> u64 {
        let futs = (0..self.config.partition_total)
            .map(|idx| self.submit_request(LogPartitionRequestType::LastN, idx));
        let res = join_all(futs).await;

        let mut last_n = 0;
        for _r in res {
            if let LogPartitionResponse::LastN(Some(n)) = _r {
                if n > last_n {
                    last_n = n;
                }
            }
        }

        last_n
    }
}

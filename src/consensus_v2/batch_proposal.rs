use std::{io::Error, pin::Pin, sync::Arc, time::Duration};

use std::io::ErrorKind;
use log::warn;
use crate::utils::channel::{Sender, Receiver};
use tokio::sync::Mutex;

use crate::{config::AtomicConfig, consensus::timer::ResettableTimer, proto::execution::ProtoTransaction, rpc::server::MsgAckChan};

pub type RawBatch = Vec<ProtoTransaction>;

pub type MsgAckChanWithTag = (MsgAckChan, u64);
pub type TxWithAckChanTag = (Option<ProtoTransaction>, MsgAckChanWithTag);

pub struct BatchProposer {
    config: AtomicConfig,

    batch_proposer_rx: Receiver<TxWithAckChanTag>,
    block_maker_tx: Sender<(RawBatch, Vec<MsgAckChanWithTag>)>,

    current_raw_batch: RawBatch,
    current_reply_vec: Vec<MsgAckChanWithTag>,
    batch_timer: Arc<Pin<Box<ResettableTimer>>>,
}

impl BatchProposer {
    pub fn new(
        config: AtomicConfig,
        batch_proposer_rx: Receiver<TxWithAckChanTag>,
        block_maker_tx: Sender<(RawBatch, Vec<MsgAckChanWithTag>)>,
    ) -> Self {
        let batch_timer = ResettableTimer::new(
            Duration::from_millis(config.get().consensus_config.batch_max_delay_ms)
        );

        let max_batch_size = config.get().consensus_config.max_backlog_batch_size;

        Self {
            config,
            batch_proposer_rx, block_maker_tx,
            current_raw_batch: RawBatch::with_capacity(max_batch_size),
            batch_timer,
            current_reply_vec: Vec::with_capacity(max_batch_size),
        }
    }

    pub async fn run(batch_proposer: Arc<Mutex<Self>>) {
        let mut batch_proposer = batch_proposer.lock().await;
        let batch_timer_handle = batch_proposer.batch_timer.run().await;

        loop {
            if let Err(_) = batch_proposer.worker().await {
                break;
            }
        }

        batch_timer_handle.abort();
    }

    pub async fn worker(&mut self) -> Result<(), Error> {
        let mut new_tx = None;
        let mut batch_timer_tick = false;
        
        tokio::select! {
            biased;
            _new_tx = self.batch_proposer_rx.recv() => {
                new_tx = _new_tx;
            },
            _tick = self.batch_timer.wait() => {
                batch_timer_tick = _tick;
            }
        }

        if new_tx.is_none() && !batch_timer_tick {
            return Err(Error::new(
                ErrorKind::BrokenPipe, "Channels not working correctly"
            ));
        }

        
        if new_tx.is_some() {
            // TODO: Filter read-only transactions that do not need to go through consensus.
            // Forward them directly to execution.

            let new_tx = new_tx.unwrap();
            if new_tx.0.is_none() {
                warn!("Malformed transaction");
                self.register_reply_malformed(new_tx.1).await;
                return Ok(());
            }

            let ack_chan = new_tx.1;
            let new_tx = new_tx.0.unwrap();

            self.current_raw_batch.push(new_tx);
            self.current_reply_vec.push(ack_chan);
        }

        let max_batch_size = self.config.get().consensus_config.max_backlog_batch_size;

        if self.current_raw_batch.len() >= max_batch_size || batch_timer_tick {
            self.propose_new_batch().await;
        }

        Ok(())
    }

    pub async fn register_reply_malformed(&self, ack_chan: MsgAckChanWithTag) {
        // TODO
    }

    pub async fn propose_new_batch(&mut self) {
        let batch = self.current_raw_batch.drain(..).collect();
        let reply_chans = self.current_reply_vec.drain(..).collect();
        self.block_maker_tx.send((batch, reply_chans)).await
            .expect("Could not push a new block");
        self.batch_timer.reset();
    }

}
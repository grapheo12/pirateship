use std::{collections::HashMap, sync::Arc};

use prost::Message;
use tokio::sync::{oneshot, Mutex};

use crate::{config::AtomicConfig, crypto::CryptoServiceConnector, proto::{checkpoint::{ProtoBackfillNack, ProtoBlockHint}, consensus::ProtoViewChange, rpc::ProtoPayload}, rpc::{client::PinnedClient, MessageRef, SenderType}, utils::{channel::{Receiver, Sender}, get_parent_hash_in_proto_block_ser}};

use super::logserver::LogServerQuery;


pub enum PacemakerCommand {
    /// Pacemaker usese it to poke the staging to change views.
    UpdateView(u64 /* new view num */, u64 /* config num */),

    /// Staging uses it to notify when it jumps to new view.
    MyViewJumped(u64 /* new view num */, u64 /* config num */, ProtoViewChange),

    /// Only for pacemaker use.
    NewViewForks(u64 /* new view num */, u64 /* config num */, HashMap<SenderType, ProtoViewChange> /* View change messages */),

    /// Only for staging use to notify about new bci.
    UpdateBCI(u64 /* new bci */),
}

pub struct Pacemaker {
    config: AtomicConfig,
    client: PinnedClient,
    crypto: CryptoServiceConnector,

    view_change_rx: Receiver<(ProtoViewChange, SenderType /* Sender */)>,
    staging_tx: Sender<PacemakerCommand>, // To send pacemaker commands to staging
    staging_rx: Receiver<PacemakerCommand>, // To receive info about new views from staging
    logserver_query_tx: Sender<LogServerQuery>,


    view_num: u64,
    config_num: u64,
    last_view_notified: u64,
    last_new_viewed_view: u64,
    bci: u64,
    vc_buffer: HashMap<(u64, u64), HashMap<SenderType, ProtoViewChange>>,
}

macro_rules! ask_logserver {
    ($me:expr, $query:expr, $($args:expr),+) => {
        {
            let (tx, rx) = oneshot::channel();
            $me.logserver_query_tx.send($query($($args),+, tx)).await.unwrap();
            rx.await.unwrap()
        }
    };
}

impl Pacemaker {
    pub fn new(
        config: AtomicConfig,
        client: PinnedClient,
        crypto: CryptoServiceConnector,
        view_change_rx: Receiver<(ProtoViewChange, SenderType)>,
        staging_tx: Sender<PacemakerCommand>,
        staging_rx: Receiver<PacemakerCommand>,
        logserver_query_tx: Sender<LogServerQuery>,
    ) -> Self {
        Pacemaker {
            config,
            client,
            crypto,
            view_change_rx,
            staging_tx,
            staging_rx,
            logserver_query_tx,
            view_num: 0,
            last_view_notified: 0,
            last_new_viewed_view: 0,
            config_num: 0,
            bci: 0,
            vc_buffer: HashMap::new(),
        }
    }

    fn pacemaker_view_update_threshold(&self) -> usize {
        let config = self.config.get();
        let n = config.consensus_config.node_list.len();
        let u = config.consensus_config.liveness_u as usize;

        // TODO: Change it to be explicitly r_safe + 1.
        n - 2 * u
    }

    fn new_view_bcast_threshold(&self) -> usize {
        let config = self.config.get();
        let n = config.consensus_config.node_list.len();
        let u = config.consensus_config.liveness_u as usize;

        // If I am the leader, New view after (N - u) view change messages.
        n - u
    }

    pub async fn run(pacemaker: Arc<Mutex<Self>>) {
        let mut pacemaker = pacemaker.lock().await;
        loop {
            if let Err(_) = pacemaker.worker().await {
                break;
            }
        }
    }

    async fn worker(&mut self) -> Result<(), ()> {
        tokio::select! {
            biased;
            vc = self.view_change_rx.recv() => {
                if let Some((vc, sender)) = vc {
                    self.handle_view_change(vc, sender).await?;
                }
            },
            cmd = self.staging_rx.recv() => {
                if let Some(PacemakerCommand::MyViewJumped(view_num, config_num, vc)) = cmd {
                    self.handle_my_view_jump(view_num, config_num, vc).await?;
                }

                else if let Some(PacemakerCommand::UpdateBCI(bci)) = cmd {
                    self.bci = bci;
                }
            },
        }

        Ok(())
    }


    async fn handle_view_change(&mut self, vc: ProtoViewChange, sender: SenderType) -> Result<(), ()> {
        let (_view_update_thresh, _new_view_thresh) = (self.pacemaker_view_update_threshold(), self.new_view_bcast_threshold());
        
        // Drop if from older view / config.
        if vc.view < self.view_num || vc.config_num < self.config_num {
            return Ok(());
        }
        
        // Verify the signature on the view change message
        // TODO


        // Check if the provided fork points back to a hash we have in our log.
        let (fork_parent_n, fork_parent_hash) = match &vc.fork {
            Some(fork) if fork.serialized_blocks.len() > 0 => {
                let first_block = &fork.serialized_blocks[0];
                let parent_hash = get_parent_hash_in_proto_block_ser(&first_block.serialized_body).unwrap();
                (first_block.n, parent_hash)
            },
            _ => {
                (0, Vec::new())
            }
        };
        let hash_match = ask_logserver!(self, LogServerQuery::CheckHash, fork_parent_n, fork_parent_hash);


        // If not, and the parent it points to is bcied, drop.
        if !hash_match && fork_parent_n <= self.bci {
            return Ok(());
        }

        // If not, and the parent is not bcied, we must send a Nack to the sender.
        // In hopes that they resend the view change but with a better filled up fork.
        if !hash_match {
            let hints = ask_logserver!(self, LogServerQuery::GetHints, self.bci);
            self.send_nack(sender, vc, hints).await?;
            return Ok(());
        }

        // Buffer it.
        let key = (vc.view, vc.config_num);
        let vc_buffer = self.vc_buffer.entry(key).or_insert(HashMap::new());
        
        let (_view, _config) = (vc.view, vc.config_num);

        vc_buffer.insert(sender, vc);
        
        
        // If it has a enough view change messages, send it to staging.
        if vc_buffer.len() >= _view_update_thresh && _view > self.last_view_notified {
            self.staging_tx.send(PacemakerCommand::UpdateView(_view, _config)).await.unwrap();
            self.last_view_notified = _view;
        }

        if vc_buffer.len() >= _new_view_thresh && _view > self.last_new_viewed_view {
            let vc_buffer = self.vc_buffer.remove(&(_view, _config)).unwrap();
            self.staging_tx.send(PacemakerCommand::NewViewForks(_view, _config, vc_buffer)).await.unwrap();
            self.last_new_viewed_view = _view;
        }
        Ok(())
    }

    async fn send_nack(&self, sender: SenderType, vc: ProtoViewChange, hints: Vec<ProtoBlockHint>) -> Result<(), ()> {
        let nack = ProtoBackfillNack {
            hints,
            last_index_needed: self.bci,
            reply_name: self.config.get().net_config.name.clone(),
            origin: Some(crate::proto::checkpoint::proto_backfill_nack::Origin::Vc(vc)),
        };
        let payload = ProtoPayload {
            message: Some(crate::proto::rpc::proto_payload::Message::BackfillNack(nack)),
        };

        let buf = payload.encode_to_vec();
        let sz = buf.len();
        let (sender, _) = sender.to_name_and_sub_id();

        PinnedClient::send(&self.client, &sender,
            MessageRef(&buf, sz, &SenderType::Anon)
        ).await.unwrap();
        Ok(())
    }

    async fn handle_my_view_jump(&mut self, view_num: u64, config_num: u64, vc: ProtoViewChange) -> Result<(), ()> {
        let key = (view_num, config_num);
        let my_name = self.config.get().net_config.name.clone();
        let sender = SenderType::Auth(my_name, 0);
        self.vc_buffer.retain(|(view, config), _| {
            *view >= view_num && *config >= config_num
        });

        let vc_buffer = self.vc_buffer.entry(key).or_insert(HashMap::new());
        vc_buffer.insert(sender, vc);
        Ok(())
    }
}
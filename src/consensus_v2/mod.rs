mod batch_proposal;
mod block_sequencer;
mod block_broadcaster;
mod staging;
pub mod fork_receiver;
mod timer;
pub mod app;
pub mod engines;
pub mod client_reply;

#[cfg(test)]
mod tests;

use std::{io::{Error, ErrorKind}, ops::Deref, pin::Pin, sync::Arc};

use app::{AppEngine, Application};
use batch_proposal::{BatchProposer, TxWithAckChanTag};
use block_broadcaster::BlockBroadcaster;
use block_sequencer::BlockSequencer;
use client_reply::ClientReplyHandler;
use fork_receiver::ForkReceiver;
use log::{debug, info, warn};
use prost::Message;
use staging::{Staging, VoteWithSender};
use tokio::{sync::{mpsc::unbounded_channel, Mutex}, task::JoinSet};
use crate::{proto::consensus::{ProtoAppendEntries, ProtoViewChange}, rpc::client::Client, utils::{channel::{make_channel, Sender}, RocksDBStorageEngine, StorageService}};

use crate::{config::{AtomicConfig, Config}, crypto::{AtomicKeyStore, CryptoService, KeyStore}, proto::rpc::ProtoPayload, rpc::{server::{MsgAckChan, RespType, Server, ServerContextType}, MessageRef}};

pub struct ConsensusServerContext {
    config: AtomicConfig,
    keystore: AtomicKeyStore,
    batch_proposal_tx: Sender<TxWithAckChanTag>,
    fork_receiver_tx: Sender<(ProtoAppendEntries, String)>,
    vote_receiver_tx: Sender<VoteWithSender>,
    view_change_receiver_tx: Sender<(ProtoViewChange, String)>,
}


#[derive(Clone)]
pub struct PinnedConsensusServerContext(pub Arc<Pin<Box<ConsensusServerContext>>>);

impl PinnedConsensusServerContext {
    pub fn new(
        config: AtomicConfig, keystore: AtomicKeyStore,
        batch_proposal_tx: Sender<TxWithAckChanTag>,
        fork_receiver_tx: Sender<(ProtoAppendEntries, String)>,
        vote_receiver_tx: Sender<VoteWithSender>,
        view_change_receiver_tx: Sender<(ProtoViewChange, String)>,

    ) -> Self {
        Self(Arc::new(Box::pin(ConsensusServerContext {
            config, keystore, batch_proposal_tx,
            fork_receiver_tx, vote_receiver_tx, view_change_receiver_tx,
        })))
    }
}

impl Deref for PinnedConsensusServerContext {
    type Target = ConsensusServerContext;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}


impl ServerContextType for PinnedConsensusServerContext {
    fn get_server_keys(&self) -> std::sync::Arc<Box<crate::crypto::KeyStore>> {
        self.keystore.get()
    }

    async fn handle_rpc(&self, m: MessageRef<'_>, ack_chan: MsgAckChan) -> Result<RespType, Error> {
        let sender = match m.2 {
            crate::rpc::SenderType::Anon => {
                return Err(Error::new(
                    ErrorKind::InvalidData,
                    "unauthenticated message",
                )); // Anonymous replies shouldn't come here
            }
            crate::rpc::SenderType::Auth(name) => name.to_string(),
        };
        let body = match ProtoPayload::decode(&m.0.as_slice()[0..m.1]) {
            Ok(b) => b,
            Err(e) => {
                warn!("Parsing problem: {} ... Dropping connection", e.to_string());
                debug!("Original message: {:?} {:?}", &m.0, &m.1);
                return Err(Error::new(ErrorKind::InvalidData, e));
            }
        };

        if sender.contains("client") {
            info!("Got client msg");
        }

    
        let msg = match body.message {
            Some(m) => m,
            None => {
                warn!("Nil message: {}", m.1);
                return Ok(RespType::NoResp);
            }
        };

        match msg {
            crate::proto::rpc::proto_payload::Message::ViewChange(proto_view_change) => {
                self.view_change_receiver_tx.send((proto_view_change, sender)).await
                    .expect("Channel send error");
                return Ok(RespType::NoResp);
            },
            crate::proto::rpc::proto_payload::Message::AppendEntries(proto_append_entries) => {
                self.fork_receiver_tx.send((proto_append_entries, sender)).await
                    .expect("Channel send error");
                return Ok(RespType::NoResp);
            },
            crate::proto::rpc::proto_payload::Message::Vote(proto_vote) => {
                self.vote_receiver_tx.send((sender, proto_vote)).await
                    .expect("Channel send error");
                return Ok(RespType::NoResp);
            },
            crate::proto::rpc::proto_payload::Message::ClientRequest(proto_client_request) => {
                let client_tag = proto_client_request.client_tag;
                self.batch_proposal_tx.send((proto_client_request.tx, (ack_chan, client_tag, sender))).await
                    .expect("Channel send error");

                return Ok(RespType::Resp);
            },
            crate::proto::rpc::proto_payload::Message::BackfillRequest(proto_back_fill_request) => {},
            crate::proto::rpc::proto_payload::Message::BackfillResponse(proto_back_fill_response) => {},
        }



        Ok(RespType::NoResp)
    }
}

pub struct ConsensusNode<E: AppEngine + Send + Sync + 'static> {
    config: AtomicConfig,
    keystore: AtomicKeyStore,

    server: Arc<Server<PinnedConsensusServerContext>>,
    storage: Arc<Mutex<StorageService<RocksDBStorageEngine>>>,
    crypto: CryptoService,


    /// This will be owned by the task that runs batch_proposer
    /// So the lock will be taken exactly ONCE and held forever.
    batch_proposer: Arc<Mutex<BatchProposer>>,
    block_sequencer: Arc<Mutex<BlockSequencer>>,
    block_broadcaster: Arc<Mutex<BlockBroadcaster>>,
    staging: Arc<Mutex<Staging>>,
    fork_receiver: Arc<Mutex<ForkReceiver>>,
    app: Arc<Mutex<Application<'static, E>>>,
    client_reply: Arc<Mutex<ClientReplyHandler>>,


    /// TODO: When all wiring is done, this will be empty.
    __sink_handles: JoinSet<()>,
}

const CRYPTO_NUM_TASKS: usize = 4;

impl<E: AppEngine + Send + Sync> ConsensusNode<E> {
    pub fn new(config: Config) -> Self {
        let _chan_depth = config.rpc_config.channel_depth as usize;

        let key_store = KeyStore::new(
            &config.rpc_config.allowed_keylist_path,
            &config.rpc_config.signing_priv_key_path,
        );
        let config = AtomicConfig::new(config);
        let keystore = AtomicKeyStore::new(key_store);
        let mut crypto = CryptoService::new(CRYPTO_NUM_TASKS, keystore.clone(), config.clone());
        crypto.run();
        let storage_config = &config.get().consensus_config.log_storage_config;
        let mut storage = match storage_config {
            rocksdb_config @ crate::config::StorageConfig::RocksDB(_) => {
                let _db = RocksDBStorageEngine::new(rocksdb_config.clone());
                StorageService::new(_db, _chan_depth)
            },
            crate::config::StorageConfig::FileStorage(_) => {
                panic!("File storage not supported!");
            },
        };

        let client = Client::new_atomic(config.clone(), keystore.clone(), false, 0);
        let staging_client = Client::new_atomic(config.clone(), keystore.clone(), false, 0);

        let (batch_proposer_tx, batch_proposer_rx) = make_channel(_chan_depth);

        let (block_maker_tx, block_maker_rx) = make_channel(_chan_depth);
        let (control_command_tx, control_command_rx) = make_channel(_chan_depth);
        let (qc_tx, qc_rx) = unbounded_channel();
        let (block_broadcaster_tx, block_broadcaster_rx) = make_channel(_chan_depth);
        let (other_block_tx, other_block_rx) = make_channel(_chan_depth);
        let (client_reply_tx, client_reply_rx) = make_channel(_chan_depth);
        let (client_reply_command_tx, client_reply_command_rx) = make_channel(_chan_depth);
        let (broadcaster_control_command_tx, broadcaster_control_command_rx) = make_channel(_chan_depth);
        let (staging_tx, staging_rx) = make_channel(_chan_depth);
        let (logserver_tx, logserver_rx) = make_channel(_chan_depth);
        let (vote_tx, vote_rx) = make_channel(_chan_depth);
        let (view_change_tx, view_change_rx) = make_channel(_chan_depth);
        let (app_tx, app_rx) = make_channel(_chan_depth);
        let (fork_receiver_command_tx, fork_receiver_command_rx) = make_channel(_chan_depth);
        let (fork_tx, fork_rx) = make_channel(_chan_depth);
        let (unlogged_tx, unlogged_rx) = make_channel(_chan_depth);

        let block_maker_crypto = crypto.get_connector();
        let block_broadcaster_crypto = crypto.get_connector();
        let block_broadcaster_storage = storage.get_connector(block_broadcaster_crypto);
        let staging_crypto = crypto.get_connector();
        let fork_receiver_crypto = crypto.get_connector();

        let ctx = PinnedConsensusServerContext::new(config.clone(), keystore.clone(), batch_proposer_tx, fork_tx, vote_tx, view_change_tx);
        let batch_proposer = BatchProposer::new(config.clone(), batch_proposer_rx, block_maker_tx, app_tx.clone());
        let block_sequencer = BlockSequencer::new(config.clone(), control_command_rx, block_maker_rx, qc_rx, block_broadcaster_tx, client_reply_tx, block_maker_crypto);
        let block_broadcaster = BlockBroadcaster::new(config.clone(), client.into(), block_broadcaster_rx, other_block_rx, broadcaster_control_command_rx, block_broadcaster_storage, staging_tx, logserver_tx, fork_receiver_command_tx.clone(), app_tx.clone());
        let staging = Staging::new(config.clone(), staging_client.into(), staging_crypto, staging_rx, vote_rx, view_change_rx, client_reply_command_tx.clone(), app_tx, broadcaster_control_command_tx, control_command_tx, fork_receiver_command_tx, qc_tx);
        let fork_receiver = ForkReceiver::new(config.clone(), fork_receiver_crypto, fork_rx, fork_receiver_command_rx, other_block_tx);
        let app = Application::new(config.clone(), app_rx, unlogged_rx, client_reply_command_tx);
        let client_reply = ClientReplyHandler::new(config.clone(), client_reply_rx, client_reply_command_rx);
        let mut handles = JoinSet::new();
        
    
        handles.spawn(async move {
            let _tx = unlogged_tx.clone();

            while let Some(_) = logserver_rx.recv().await {
                // Sink
            }
        });




        Self {
            config: config.clone(),
            keystore: keystore.clone(),
            server: Arc::new(Server::new_atomic(config.clone(), ctx, keystore.clone())),
            batch_proposer: Arc::new(Mutex::new(batch_proposer)),
            block_sequencer: Arc::new(Mutex::new(block_sequencer)),
            block_broadcaster: Arc::new(Mutex::new(block_broadcaster)),
            staging: Arc::new(Mutex::new(staging)),
            fork_receiver: Arc::new(Mutex::new(fork_receiver)),
            client_reply: Arc::new(Mutex::new(client_reply)),

            crypto,
            storage: Arc::new(Mutex::new(storage)),
            __sink_handles: handles,

            app: Arc::new(Mutex::new(app)),
        }
    }

    pub async fn run(&mut self) -> JoinSet<()> {
        let server = self.server.clone();
        let batch_proposer = self.batch_proposer.clone();
        let block_maker = self.block_sequencer.clone();
        let storage = self.storage.clone();
        let block_broadcaster = self.block_broadcaster.clone();
        let staging = self.staging.clone();
        let app = self.app.clone();
        let client_reply = self.client_reply.clone();
        let fork_receiver = self.fork_receiver.clone();

        let mut handles = JoinSet::new();

        handles.spawn(async move {
            let mut storage = storage.lock().await;
            storage.run().await;
        });

        handles.spawn(async move {
            let _ = Server::<PinnedConsensusServerContext>::run(server).await;
        });

        handles.spawn(async move {
            BatchProposer::run(batch_proposer).await;
        });

        handles.spawn(async move {
            BlockSequencer::run(block_maker).await;
        });

        handles.spawn(async move {
            BlockBroadcaster::run(block_broadcaster).await;
        });
    
        handles.spawn(async move {
            Staging::run(staging).await;
        });

        handles.spawn(async move {
            info!("Booting up application");
            Application::run(app).await;
        });

        handles.spawn(async move {
            ClientReplyHandler::run(client_reply).await;
        });

        handles.spawn(async move {
            ForkReceiver::run(fork_receiver).await;
        });
    
        handles
    }
}
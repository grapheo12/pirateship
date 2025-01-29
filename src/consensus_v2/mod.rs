mod batch_proposal;
mod block_sequencer;
mod block_broadcaster;
mod staging;

#[allow(unused_variables)]
#[cfg(test)]
mod tests;

use std::{io::{Error, ErrorKind}, ops::Deref, pin::Pin, sync::Arc};

use batch_proposal::{BatchProposer, MsgAckChanWithTag, TxWithAckChanTag};
use block_sequencer::BlockSequencer;
use log::{debug, warn};
use prost::Message;
use tokio::{sync::Mutex, task::JoinSet};
use crate::utils::channel::{Sender, make_channel};

use crate::{config::{AtomicConfig, Config}, crypto::{AtomicKeyStore, CryptoService, KeyStore}, proto::{execution::ProtoTransaction, rpc::ProtoPayload}, rpc::{server::{LatencyProfile, MsgAckChan, RespType, Server, ServerContextType}, MessageRef, PinnedMessage}};

pub struct ConsensusServerContext {
    config: AtomicConfig,
    keystore: AtomicKeyStore,
    batch_proposal_tx: Sender<TxWithAckChanTag>,
}


#[derive(Clone)]
pub struct PinnedConsensusServerContext(pub Arc<Pin<Box<ConsensusServerContext>>>);

impl PinnedConsensusServerContext {
    pub fn new(config: AtomicConfig, keystore: AtomicKeyStore, batch_proposal_tx: Sender<(Option<ProtoTransaction>, MsgAckChanWithTag)>) -> Self {
        Self(Arc::new(Box::pin(ConsensusServerContext {
            config, keystore, batch_proposal_tx
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

    
        let msg = match body.message {
            Some(m) => m,
            None => {
                warn!("Nil message: {}", m.1);
                return Ok(RespType::NoResp);
            }
        };

        match msg {
            crate::proto::rpc::proto_payload::Message::ViewChange(proto_view_change) => {},
            crate::proto::rpc::proto_payload::Message::AppendEntries(proto_append_entries) => {},
            crate::proto::rpc::proto_payload::Message::Vote(proto_vote) => {},
            crate::proto::rpc::proto_payload::Message::ClientRequest(proto_client_request) => {
                let client_tag = proto_client_request.client_tag;
                self.batch_proposal_tx.send((proto_client_request.tx, (ack_chan, client_tag))).await
                    .expect("Channel send error");

                return Ok(RespType::Resp);
            },
            crate::proto::rpc::proto_payload::Message::BackfillRequest(proto_back_fill_request) => {},
            crate::proto::rpc::proto_payload::Message::BackfillResponse(proto_back_fill_response) => {},
        }



        Ok(RespType::NoResp)
    }
}

pub struct ConsensusNode {
    config: AtomicConfig,
    keystore: AtomicKeyStore,

    server: Arc<Server<PinnedConsensusServerContext>>,

    /// This will be owned by the task that runs batch_proposer
    /// So the lock will be taken exactly ONCE and held forever.
    batch_proposer: Arc<Mutex<BatchProposer>>,

    block_maker: Arc<Mutex<BlockSequencer>>,

    crypto: CryptoService,

}

const CRYPTO_NUM_TASKS: usize = 4;

impl ConsensusNode {
    pub fn new(config: Config) -> Self {
        let _chan_depth = config.rpc_config.channel_depth as usize;

        let key_store = KeyStore::new(
            &config.rpc_config.allowed_keylist_path,
            &config.rpc_config.signing_priv_key_path,
        );
        let keystore = AtomicKeyStore::new(key_store);
        let crypto = CryptoService::new(CRYPTO_NUM_TASKS, keystore.clone());
        let config = AtomicConfig::new(config);


        let (batch_proposer_tx, batch_proposer_rx) = make_channel(_chan_depth);
        let (block_maker_tx, block_maker_rx) = make_channel(_chan_depth);
        let (control_command_tx, control_command_rx) = make_channel(_chan_depth);
        let (qc_tx, qc_rx) = make_channel(_chan_depth);
        let (block_broadcaster_tx, block_broadcaster_rx) = make_channel(_chan_depth);
        let (client_reply_tx, client_reply_rx) = make_channel(_chan_depth);

        let block_maker_crypto = crypto.get_connector();

        let ctx = PinnedConsensusServerContext::new(config.clone(), keystore.clone(), batch_proposer_tx);
        let batch_proposer = BatchProposer::new(config.clone(), batch_proposer_rx, block_maker_tx);
        let block_maker = BlockSequencer::new(config.clone(), control_command_rx, block_maker_rx, qc_rx, block_broadcaster_tx, client_reply_tx, block_maker_crypto);
        
        Self {
            config: config.clone(),
            keystore: keystore.clone(),
            server: Arc::new(Server::new_atomic(config.clone(), ctx, keystore.clone())),
            batch_proposer: Arc::new(Mutex::new(batch_proposer)),
            block_maker: Arc::new(Mutex::new(block_maker)),
            crypto,
        }
    }

    pub async fn run(&mut self) -> JoinSet<()> {
        let server = self.server.clone();
        let batch_proposer = self.batch_proposer.clone();
        let block_maker = self.block_maker.clone();

        let mut handles = JoinSet::new();
        handles.spawn(async move {
            let _ = Server::<PinnedConsensusServerContext>::run(server).await;
        });

        self.crypto.run();

        handles.spawn(async move {
            BatchProposer::run(batch_proposer).await;
        });

        handles.spawn(async move {
            BlockSequencer::run(block_maker).await;
        });

        handles
    }
}
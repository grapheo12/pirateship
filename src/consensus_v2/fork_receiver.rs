use crate::{config::AtomicConfig, crypto::{AtomicKeyStore, CachedBlock}, proto::consensus::ProtoAppendEntries, rpc::client::PinnedClient, utils::channel::Receiver};

pub struct ForkReceiver {
    config: AtomicConfig,
    key_store: AtomicKeyStore,
    client: PinnedClient,
    view: u64,
    config_num: u64,

    fork_rx: Receiver<ProtoAppendEntries>,
    broadcaster_tx: Sender<CachedBlock>,
    
}
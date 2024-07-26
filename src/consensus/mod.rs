use std::sync::{atomic::Ordering, Arc};

use handler::{consensus_rpc_handler, PinnedServerContext};
use protocols::get_leader_str;
use tokio::task::JoinSet;

use crate::{
    config::Config,
    crypto::KeyStore,
    rpc::{
        client::{Client, PinnedClient},
        server::Server,
    },
};

pub mod handler;
pub mod leader_rotation;
pub mod log;
pub mod protocols;
pub mod timer;

pub mod proto {
    pub mod consensus {
        include!(concat!(env!("OUT_DIR"), "/proto.consensus.rs"));
    }
    pub mod client {
        include!(concat!(env!("OUT_DIR"), "/proto.client.rs"));
    }
    pub mod rpc {
        include!(concat!(env!("OUT_DIR"), "/proto.rpc.rs"));
    }
}

/// Wrapper around rpc::{Server, Client} with this PinnedServerContext and consensus_rpc_handler
/// One should use this to spawn a new node, instead of creating rpc::{Server, Client} separately.
pub struct ConsensusNode {
    pub server: Arc<Server<PinnedServerContext>>,
    pub client: PinnedClient,
    pub ctx: PinnedServerContext,
}

impl ConsensusNode {
    pub fn new(config: &Config) -> ConsensusNode {
        let key_store = KeyStore::new(
            &config.rpc_config.allowed_keylist_path,
            &config.rpc_config.signing_priv_key_path,
        );
        ConsensusNode {
            server: Arc::new(Server::new(config, consensus_rpc_handler, &key_store)),
            client: Client::new(config, &key_store).into(),
            ctx: PinnedServerContext::new(config, &key_store),
        }
    }

    pub fn run(node: Arc<Self>) -> JoinSet<()> {
        // These are just increasing ref counts.
        // It is pointing to the same server instance.
        let mut js = JoinSet::new();
        let node1 = node.clone();
        let node2 = node.clone();
        let node3 = node.clone();
        let node4 = node.clone();
        js.spawn(async move {
            let _ = Server::<PinnedServerContext>::run(node1.server.clone(), node1.ctx.clone())
                .await;
        });
        js.spawn(async move {
            let _ = protocols::report_stats(&node2.ctx).await;
        });
        js.spawn(async move {
            let _ = protocols::handle_node_messages(node3.ctx.clone(), node3.client.clone()).await;
        });
        js.spawn(async move {
            let _ = protocols::handle_client_messages(node4.ctx.clone(), node4.client.clone()).await;
        });

        js
    }
}

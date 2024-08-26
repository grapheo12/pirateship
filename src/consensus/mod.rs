// Copyright (c) Shubham Mishra. All rights reserved.
// Licensed under the Apache 2.0 License.

use std::sync::Arc;

use handler::{consensus_rpc_handler, PinnedServerContext, handle_client_messages, handle_node_messages};
use reconfiguration::reconfiguration_worker;
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
pub mod timer;
pub mod view_change;
pub mod backfill;
pub mod commit;
pub mod steady_state;
pub mod utils;
pub mod client_reply;
pub mod reconfiguration;


/// Wrapper around rpc::{Server, Client} with this PinnedServerContext and consensus_rpc_handler
/// One should use this to spawn a new node, instead of creating rpc::{Server, Client} separately.
pub struct ConsensusNode<Engine>
where
    Engine: crate::execution::Engine + Clone + Send + Sync + 'static,
{
    pub server: Arc<Server<PinnedServerContext>>,
    pub client: PinnedClient,
    pub ctx: PinnedServerContext,
    pub engine: Engine,
}

impl<Engine> ConsensusNode<Engine> 
where 
    Engine: crate::execution::Engine + Clone + Send + Sync + 'static
{
    pub fn new(config: &Config) -> ConsensusNode<Engine> {
        let key_store = KeyStore::new(
            &config.rpc_config.allowed_keylist_path,
            &config.rpc_config.signing_priv_key_path,
        );
        
        let ctx = PinnedServerContext::new(config, &key_store);
        ConsensusNode{
            server: Arc::new(Server::new(config, consensus_rpc_handler, &key_store)),
            client: Client::new(config, &key_store).into(),
            ctx: ctx.clone(),
            engine: Engine::new(ctx.clone()),
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
        let node5 = node.clone();

        js.spawn(async move {
            let _ = Server::<PinnedServerContext>::run(node1.server.clone(), node1.ctx.clone())
                .await;
        });
        js.spawn(async move {
            let _ = node2.engine.run().await;
        });
        js.spawn(async move {
            let _ = handle_node_messages(node3.ctx.clone(), node3.client.clone(), node3.engine.clone()).await;
        });
        js.spawn(async move {
            let _ = handle_client_messages(node4.ctx.clone(), node4.client.clone(), node4.engine.clone()).await;
        });
        js.spawn(async move {
            let _ = reconfiguration_worker(node5.ctx.clone(), node5.client.clone()).await;
        });

        js
    }
}

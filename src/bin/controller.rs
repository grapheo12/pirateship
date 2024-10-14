// Copyright (c) Shubham Mishra. All rights reserved.
// Licensed under the MIT License.

use std::{fs, io, path, str::FromStr};

use clap::{arg, command, Arg, ArgAction};
use log::{debug, info, trace};
use pft::{
    config::{default_log4rs_config, ClientConfig},
    consensus::reconfiguration::{serialize_add_learner, serialize_del_learner, serialize_downgrade_fullnode, serialize_upgrade_fullnode, LearnerInfo},
    crypto::KeyStore,
    proto::{client::{ProtoClientReply, ProtoClientRequest}, execution::{ProtoTransaction, ProtoTransactionOp, ProtoTransactionPhase}, rpc::ProtoPayload}, rpc::{client::{Client, PinnedClient}, MessageRef},
};
use prost::Message;

#[global_allocator]
static ALLOC: snmalloc_rs::SnMalloc = snmalloc_rs::SnMalloc;


#[derive(Debug)]
struct Config {
    client_config_path: String,
    add_learner: Vec<LearnerInfo>,
    del_learner: Vec<String>,
    upgrade_fullnode: Vec<String>,
    downgrade_fullnode: Vec<String>,
}

fn parse_args() -> Config {
    let matches = command!()
        .arg(arg!(<client_config_path> "Path to client config"))
        .arg(
            Arg::new("add_learner")
                .long("add_learner")
                .action(ArgAction::Append)
                .value_parser(LearnerInfo::from_str)
                .help("Add learners with learner_info of the form 'name addr domain pub_key'")
                .required(false),
        )
        .arg(
            Arg::new("del_learner")
                .long("del_learner")
                .action(ArgAction::Append)
                .help("Delete learners with given names")
                .required(false),
        )
        .arg(
            Arg::new("upgrade_fullnode")
                .long("upgrade_fullnode")
                .action(ArgAction::Append)
                .help("Upgrade to full nodes given names")
                .required(false),
        )
        .arg(
            Arg::new("downgrade_fullnode")
                .long("downgrade_fullnode")
                .action(ArgAction::Append)
                .help("Downgrade to learners given names")
                .required(false),
        )
        .get_matches();

    let add_learner = matches
        .get_many::<LearnerInfo>("add_learner")
        .unwrap_or_default()
        .cloned()
        .collect::<Vec<_>>();
    let del_learner = matches
        .get_many::<String>("del_learner")
        .unwrap_or_default()
        .cloned()
        .collect::<Vec<_>>();
    let upgrade_fullnode = matches
        .get_many::<String>("upgrade_fullnode")
        .unwrap_or_default()
        .cloned()
        .collect::<Vec<_>>();
    let downgrade_fullnode = matches
        .get_many::<String>("downgrade_fullnode")
        .unwrap_or_default()
        .cloned()
        .collect::<Vec<_>>();

    Config {
        client_config_path: matches
            .get_one::<String>("client_config_path")
            .unwrap()
            .to_string(),
        add_learner,
        del_learner,
        upgrade_fullnode,
        downgrade_fullnode,
    }
}

#[tokio::main]
async fn main() -> io::Result<()> {
    log4rs::init_config(default_log4rs_config()).unwrap();

    let config = parse_args();
    let cfg_path = path::Path::new(config.client_config_path.as_str());
    if !cfg_path.exists() {
        panic!("{} does not exist", config.client_config_path);
    }

    let cfg_contents = fs::read_to_string(cfg_path).expect("Invalid file path");
    let mut client_cfg = ClientConfig::deserialize(&cfg_contents);
    let mut keys = KeyStore::empty();
    keys.priv_key = KeyStore::get_privkeys(&client_cfg.rpc_config.signing_priv_key_path);

    let add_learner_op: Vec<ProtoTransactionOp> = config.add_learner.iter().map(|info| {
        serialize_add_learner(info)
    }).collect();

    info!("Add learner op: {:?}", add_learner_op);

    let del_learner_op: Vec<ProtoTransactionOp> = config.del_learner.iter().map(|name| {
        serialize_del_learner(name)
    }).collect();

    info!("Del learner op: {:?}", del_learner_op);

    let upgrade_fullnode_op: Vec<ProtoTransactionOp> = config.upgrade_fullnode.iter().map(|name| {
        serialize_upgrade_fullnode(name)
    }).collect();

    info!("Upgrade fullnode op: {:?}", upgrade_fullnode_op);


    let downgrade_fullnode_op: Vec<ProtoTransactionOp> = config.downgrade_fullnode.iter().map(|name| {
        serialize_downgrade_fullnode(name)
    }).collect();

    info!("Downgrade fullnode op: {:?}", downgrade_fullnode_op);

    let crash_commit_ops: Vec<ProtoTransactionOp> = add_learner_op.into_iter()
        .collect();

    let on_crash_commit = if crash_commit_ops.is_empty() {
        None
    } else {
        Some(ProtoTransactionPhase {
            ops: crash_commit_ops,
        })
    };

    let byz_commit_ops: Vec<ProtoTransactionOp> = del_learner_op.into_iter()
        .chain(upgrade_fullnode_op.into_iter())
        .chain(downgrade_fullnode_op.into_iter())
        .collect();

    let on_byzantine_commit = if byz_commit_ops.is_empty() {
        None
    } else {
        Some(ProtoTransactionPhase {
            ops: byz_commit_ops,
        })
    };


    let client_req = ProtoClientRequest {
        tx: Some(ProtoTransaction {
            on_receive: None,
            on_crash_commit,
            on_byzantine_commit,
            is_reconfiguration: true,
        }),
        origin: client_cfg.net_config.name.clone(),
        sig: vec![0u8; 1],
    };

    let rpc_msg_body = ProtoPayload {
        message: Some(
            pft::proto::rpc::proto_payload::Message::ClientRequest(client_req),
        ),
    };

    let mut buf = Vec::new();
    rpc_msg_body.encode(&mut buf).expect("Protobuf error");

    let client = Client::new(&client_cfg.fill_missing(), &keys);
    let client = client.into();
    let mut curr_leader = String::from("node1");

    loop {
        let msg = PinnedClient::send_and_await_reply(
            &client,
            &curr_leader,
            MessageRef(&buf, buf.len(), &pft::rpc::SenderType::Anon),
        )
        .await
        .unwrap();

        let sz = msg.as_ref().1;
        let resp = ProtoClientReply::decode(&msg.as_ref().0.as_slice()[..sz]).unwrap();
        if let None = resp.reply {
            continue;
        }
        let resp = match resp.reply.unwrap() {
            pft::proto::client::proto_client_reply::Reply::Receipt(r) => r,
            pft::proto::client::proto_client_reply::Reply::TryAgain(_) => {
                continue;
            },
            pft::proto::client::proto_client_reply::Reply::Leader(l) => {
                if curr_leader != l.name {
                    trace!("Switching leader: {} --> {}", curr_leader, l.name);
                    PinnedClient::drop_connection(&client, &curr_leader);    
                    curr_leader = l.name.clone();
                }
                continue;
            },
            pft::proto::client::proto_client_reply::Reply::TentativeReceipt(r) => {
                debug!("Got tentative receipt: {:?}", r);
                break;
            },
        };

        info!("{:#?}", resp);
        break;
    }

    // Update my own config file
    for info in config.add_learner.iter() {
        client_cfg.net_config.nodes.insert(info.name.clone(), info.info.clone());
    }

    let cfg_str = serde_json::to_string_pretty(&client_cfg).expect("Invalid Config");
    fs::write(cfg_path, cfg_str).expect("Unable to write file");


    Ok(())
}

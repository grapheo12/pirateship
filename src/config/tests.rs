use std::collections::HashMap;

use crate::config::{Config, ConsensusConfig, NetConfig, NodeNetInfo, RpcConfig};


#[test]
fn test_nodeconfig_serialize() {
    let mut net_config = NetConfig{
        name: "node1".to_string(),
        addr: "0.0.0.0:3001".to_string(),
        tls_cert_path: String::from("blah"),
        tls_key_path: String::from("blah"),
        tls_root_ca_cert_path: String::from("blah"),
        nodes: HashMap::new(),
        client_max_retry: 10
    };

    for n in 0..5 {
        let mut name = "node".to_string();
        name.push_str(n.to_string().as_str());
        let mut addr = "127.0.0.1:300".to_string();
        addr.push_str(n.to_string().as_str());
        net_config.nodes.insert(name, NodeNetInfo{addr: addr.to_owned(), domain: String::from("blah.com")});

    }

    let rpc_config = RpcConfig {
        allowed_keylist_path: String::from("blah/blah"),
        signing_priv_key_path: String::from("blah/blah"),
        recv_buffer_size: (1 << 15),
        channel_depth: 32
    };

    let consensus_config = ConsensusConfig {
        node_list: vec![String::from("node1"), String::from("node2"), String::from("node3")],
        quorum_diversity_k: 3
    };

    let config = Config{net_config, rpc_config, consensus_config};

    let s = config.serialize();
    println!("{}", s);

    let config2 = Config::deserialize(&s);
    let nc2 = config2.net_config;
    assert!(config.net_config.name.eq(&nc2.name));
    assert!(config.net_config.addr.eq(&nc2.addr));
    for (name, node_config) in config.net_config.nodes.into_iter() {
        let opt = nc2.nodes.get(&name);
        assert!(opt.is_some());
        let val = opt.unwrap();
        assert!(val.addr.eq(&node_config.addr));
    }

}
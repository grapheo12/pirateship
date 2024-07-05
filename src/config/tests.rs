use std::collections::HashMap;

use crate::config::{NetConfig, NodeNetInfo, Config};


#[test]
fn test_nodeconfig_serialize() {
    let mut net_config = NetConfig{
        name: "node1".to_string(),
        addr: "0.0.0.0:3001".to_string(),
        tls_cert_path: String::from("blah"),
        tls_key_path: String::from("blah"),
        tls_root_ca_cert_path: String::from("blah"),
        nodes: HashMap::new()
    };

    for n in 0..5 {
        let mut name = "node".to_string();
        name.push_str(n.to_string().as_str());
        let mut addr = "127.0.0.1:300".to_string();
        addr.push_str(n.to_string().as_str());
        net_config.nodes.insert(name, NodeNetInfo{addr: addr.to_owned()});

    }

    let config = Config{net_config};

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
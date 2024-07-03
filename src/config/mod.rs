use std::collections::HashMap;
use serde::{Deserialize, Serialize};
use serde_json::Result;
mod tests;

#[derive(Serialize, Deserialize)]
pub struct NodeNetInfo {
    pub addr: String,
}

#[derive(Serialize, Deserialize)]
pub struct NetConfig {
    pub name: String,
    pub addr: String,
    pub nodes: HashMap<String, NodeNetInfo>
}

impl NetConfig {
    pub fn serialize(self: &Self) -> String  {
        serde_json::to_string_pretty(self).expect("Invalid NetConfig")
    }

    pub fn deserialize(s: &String) -> NetConfig {
        let res: Result<NetConfig> = serde_json::from_str(s.as_str());
        res.expect("Invalid JSON config")
    }
}
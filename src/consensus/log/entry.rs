use prost::Message;
use std::collections::{HashMap, HashSet};

use ed25519_dalek::SIGNATURE_LENGTH;

use crate::{crypto::hash, proto::consensus::{proto_block::Sig, ProtoBlock}};


#[derive(Clone, Debug)]
pub struct LogEntry {
    pub block: ProtoBlock,
    pub replication_votes: HashSet<String>,
    pub block_hash: Vec<u8>,

    /// Accumulate signatures in the leader.
    /// Used as signature cache in the followers.
    pub qc_sigs: HashMap<String, [u8; SIGNATURE_LENGTH]>,
}

impl LogEntry {
    pub fn new(block: ProtoBlock) -> LogEntry {
        let mut buf = Vec::new();
        block.encode(&mut buf).unwrap();
        
        LogEntry {
            block,
            replication_votes: HashSet::new(),
            qc_sigs: HashMap::new(),
            block_hash: hash(&buf)
        }
    }

    pub fn has_signature(&self) -> bool {
        if self.block.sig.is_none() {
            return false;
        }

        match self.block.sig.clone().unwrap() {
            Sig::NoSig(_) => false,
            Sig::ProposerSig(_) => true,
        }
    }
}
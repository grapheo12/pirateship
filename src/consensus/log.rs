use std::{collections::HashSet, io::{Error, ErrorKind}};

use prost::Message;

use crate::crypto::{hash, cmp_hash, DIGEST_LENGTH};

use super::proto::consensus::ProtoBlock;


#[derive(Clone, Debug)]
pub struct LogEntry {
    pub block: ProtoBlock,
    pub replication_votes: HashSet<String>
}

#[derive(Clone, Debug)]
pub struct Log {
    entries: Vec<LogEntry>,
    last_qc: u64
}

impl Log {
    pub fn new() -> Log {
        Log {
            entries: Vec::new(),
            last_qc: 0
        }
    }

    /// The block index is 1-based.
    /// 0 is reserved for the genesis (or null) block.
    pub fn last(&self) -> u64 {
        self.entries.len() as u64
    }

    pub fn last_qc(&self) -> u64 {
        self.last_qc
    }

    /// Returns last() on success.
    pub fn push(&mut self, entry: LogEntry) -> Result<u64, Error> {
        for qc in &entry.block.qc {
            if qc.n > self.last_qc && qc.n <= self.last() {
                self.last_qc = qc.n
            }
        }
        if entry.block.n != self.last() + 1 {
            return Err(Error::new(ErrorKind::InvalidInput, "Missing intermediate blocks!"));
        }
        if !cmp_hash(&entry.block.parent, &self.last_hash()) {
            return Err(Error::new(ErrorKind::InvalidInput, "Hash link violation"));
        }
        self.entries.push(entry);
        Ok(self.last())
    }

    pub fn get(&self, n: u64) -> Result<&LogEntry, Error> {
        if n > self.last() || n == 0 {
            return Err(Error::new(ErrorKind::InvalidInput, "Out of bounds"));
        }
        Ok(self.entries.get((n - 1) as usize).unwrap())
    }

    /// Returns current vote size
    pub fn inc_replication_vote(&mut self, name: &String, n: u64) -> Result<u64, Error> {
        if n > self.last() {
            return Err(Error::new(ErrorKind::InvalidInput, "Vote for missing block!"));
        }

        let idx = n - 1;        // Index is 1-based
        let entry = self.entries.get_mut(idx as usize).unwrap();
        entry.replication_votes.insert(name.clone());

        Ok(entry.replication_votes.len() as u64)
    }

    pub fn last_hash(&self) -> Vec<u8> {
        if self.last() == 0 {
            return vec![0u8; DIGEST_LENGTH];
        }

        let mut buf = Vec::new();
        self.entries[(self.last() - 1) as usize].block.encode(&mut buf).unwrap();

        hash(&buf)
    }

}
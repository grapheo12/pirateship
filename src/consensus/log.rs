// Copyright (c) Shubham Mishra. All rights reserved.
// Licensed under the MIT License.

use std::{
    collections::{HashMap, HashSet}, io::{Error, ErrorKind}, sync::Arc
};

#[cfg(feature = "storage")]
use std::collections::VecDeque;

use ed25519_dalek::SIGNATURE_LENGTH;
use log::{error, info, warn};
use prost::Message;

use crate::{config::Config, crypto::{cmp_hash, hash, KeyStore}, utils::{AgnosticRef, FileStorageEngine, RocksDBStorageEngine, StorageEngine}};

use super::super::proto::consensus::{
    proto_block::Sig, DefferedSignature, ProtoBlock, ProtoFork, ProtoNameWithSignature,
    ProtoQuorumCertificate,
};

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
        let mut buf = Vec::with_capacity(block.encoded_len());
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

#[derive(Clone, Debug)]
pub struct Log {
    #[cfg(feature = "storage")]
    entries: VecDeque<LogEntry>,

    #[cfg(feature = "storage")]
    storage_engine: Arc<Box<dyn StorageEngine>>,

    /// High Watermark for garbage collection
    #[cfg(feature = "storage")]
    gc_hiwm: u64,

    #[cfg(not(feature = "storage"))]
    entries: Vec<LogEntry>,

    /// Highest QC.n seen so far
    last_qc: u64,
    /// The block that contains the qc for `last_qc`.
    last_block_with_qc: u64,
    /// View of the qc with qc.n == last_qc.
    /// This is strictly not neccessary to track,
    /// but helps in quickly checking the 2-chain rule.
    last_qc_view: u64,
}

impl Log {
    pub fn new(config: Config) -> Log {
        
        Log {
            #[cfg(feature = "storage")]
            entries: VecDeque::new(),

            #[cfg(feature = "storage")]
            storage_engine: Arc::new({
                // Only RocksDB supported for now.
                let mut storage: Box<dyn StorageEngine> = match config.consensus_config.log_storage_config {
                    crate::config::StorageConfig::RocksDB(_) => {
                        Box::new(RocksDBStorageEngine::new(config.consensus_config.log_storage_config.clone()))
                    },
                    crate::config::StorageConfig::FileStorage(_) => {
                        Box::new(FileStorageEngine::new(config.consensus_config.log_storage_config.clone()))
                    },
                };
                storage.init();
                storage
            }),

            #[cfg(feature = "storage")]
            gc_hiwm: 0,

            #[cfg(not(feature = "storage"))]
            entries: Vec::new(),

            last_qc: 0,
            last_block_with_qc: 0,
            last_qc_view: 0,
        }
    }

    /// The block index is 1-based.
    /// 0 is reserved for the genesis (or null) block.
    pub fn last(&self) -> u64 {
        #[cfg(feature = "storage")]
        return (self.entries.len() as u64) + self.gc_hiwm;

        #[cfg(not(feature = "storage"))]
        return self.entries.len() as u64;

    }

    pub fn last_qc(&self) -> u64 {
        self.last_qc
    }

    pub fn last_qc_view(&self) -> u64 {
        self.last_qc_view
    }

    pub fn last_block_with_qc(&self) -> u64 {
        self.last_block_with_qc
    }

    /// Returns last() on success.
    /// Never overwrites, so always follows ViewLock and GlobalLock.
    pub fn push(&mut self, entry: LogEntry) -> Result<u64, Error> {
        if entry.block.n != self.last() + 1 {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                "Missing intermediate blocks!",
            ));
        }
        if !cmp_hash(&entry.block.parent, &self.last_hash()) {
            return Err(Error::new(ErrorKind::InvalidInput, "Hash link violation"));
        }
        for qc in &entry.block.qc {
            if qc.n > self.last_qc && qc.n <= self.last() {
                self.last_qc = qc.n;
                self.last_qc_view = qc.view;
            }
            self.last_block_with_qc = entry.block.n;
        }

        #[cfg(feature = "storage")]
        {
            let mut buf = Vec::new();
            entry.block.encode(&mut buf).unwrap();
            self.entries.push_back(entry);
            let hsh = self.last_hash();


            let res = self.storage_engine.put_block(&buf, &hsh);

            if let Err(e) = res {
                return Err(e)
            }
        }

        #[cfg(not(feature = "storage"))]
        self.entries.push(entry);

        Ok(self.last())
    }

    /// This is a slow function.
    /// This should be not be called in the critical path,
    /// except for when a slow node asks for old data.
    /// Be careful what you Garbage Collect.
    /// Blocks < byz_commit_index should be ok, given all of their transactions have been executed,
    /// So there is no need to bring up that block again.
    #[cfg(feature = "storage")]
    pub fn get_gc_block(&self, n: u64) -> Result<LogEntry, Error> {
        warn!("Get GC Block called! {}", n);
        if n > self.gc_hiwm {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                "Block is not GCed yet",
            ));
        }

        if self.entries.len() == 0 {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                "Empty log should not have been GCed at all",
            ));
        }

        let mut fetch_hash = self.entries.front().as_ref().unwrap().block.parent.clone();
        loop {
            let ser_block = self.storage_engine.get_block(&fetch_hash);
            match ser_block {
                Ok(ser_block) => {
                    let res = ProtoBlock::decode(ser_block.as_slice());

                    match res {
                        Ok(block) => {
                            if block.n == n {
                                return Ok(LogEntry::new(block));
                            }
                            // Go back one more block
                            if block.n == 1 {
                                return Err(Error::new(
                                    ErrorKind::InvalidInput,
                                    format!("Block doesn't exist {} {} {}", block.n, n, self.entries.front().as_ref().unwrap().block.n),
                                ));
                            }
                            fetch_hash = block.parent.clone();
                            
                        },
                        Err(e) => {
                            return Err(e.into());
                        },
                    }

                },
                Err(e) => {
                    return Err(e);
                },
            }
        }

    }

    pub fn get(&self, n: u64) -> Result<AgnosticRef<LogEntry>, Error> {
        if n > self.last() || n == 0 {
            return Err(Error::new(ErrorKind::InvalidInput, format!("Out of bounds {}, last() = {}", n, self.last())));
        }

        #[cfg(feature = "storage")]
        if n <= self.gc_hiwm {
            let res = self.get_gc_block(n);
            match res {
                Ok(entry) => {
                    return Ok(AgnosticRef::from(entry))
                },
                Err(e) => {
                    return Err(e)
                },
            }
        }

        #[cfg(feature = "storage")]
        return Ok(AgnosticRef::from(
            self.entries.get((n - self.gc_hiwm - 1) as usize).unwrap()
        ));


        #[cfg(not(feature = "storage"))]
        Ok(AgnosticRef::from(
            self.entries.get((n - 1) as usize).unwrap()
        ))
    }

    /// Returns current vote size
    pub fn inc_replication_vote(&mut self, name: &String, n: u64) -> Result<u64, Error> {
        if n > self.last() {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                "Vote for missing block!",
            ));
        }

        #[cfg(feature = "storage")]
        if n <= self.gc_hiwm {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                "Vote for GCed block!",
            ));
        }

        #[cfg(feature = "storage")]
        let idx = n - self.gc_hiwm - 1; // Index is 1-based

        #[cfg(not(feature = "storage"))]
        let idx = n - 1;

        let entry = self.entries.get_mut(idx as usize).unwrap();
        entry.replication_votes.insert(name.clone());

        Ok(entry.replication_votes.len() as u64)
    }

    pub fn inc_qc_sig_unverified(
        &mut self,
        name: &String,
        sig: &Vec<u8>,
        n: u64,
    ) -> Result<u64, Error> {
        if n > self.last() {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                "Vote for missing block!",
            ));
        }

        #[cfg(feature = "storage")]
        if n <= self.gc_hiwm {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                "Vote for GCed block!",
            ));
        }

        #[cfg(feature = "storage")]
        let idx = n - self.gc_hiwm - 1; // Index is 1-based

        #[cfg(not(feature = "storage"))]
        let idx = n - 1;

        let entry = self.entries.get_mut(idx as usize).unwrap();
        entry
            .qc_sigs
            .insert(name.clone(), sig.as_slice().try_into().unwrap());

        Ok(entry.qc_sigs.len() as u64)
    }

    /// Increase the signature for these entry.
    /// Checks the correctness against current fork
    pub fn inc_qc_sig(
        &mut self,
        name: &String,
        sig: &Vec<u8>,
        n: u64,
        keys: &KeyStore,
    ) -> Result<u64, Error> {
        if n > self.last() {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                "Vote for missing block!",
            ));
        }

        #[cfg(feature = "storage")]
        if n <= self.gc_hiwm {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                "Vote for GCed block!",
            ));
        }

        if !self.verify_signature_at_n(n, sig, name, keys) {
            return Err(Error::new(ErrorKind::InvalidInput, "Invalid signature"));
        }

        #[cfg(feature = "storage")]
        let idx = n - self.gc_hiwm - 1; // Index is 1-based

        #[cfg(not(feature = "storage"))]
        let idx = n - 1;

        let entry = self.entries.get_mut(idx as usize).unwrap();
        entry
            .qc_sigs
            .insert(name.clone(), sig.as_slice().try_into().unwrap());

        Ok(entry.qc_sigs.len() as u64)
    }

    pub fn get_qc_at_n(&self, n: u64, curr_view: u64) -> Result<ProtoQuorumCertificate, Error> {
        let sig_map = &self.get(n)?.qc_sigs;
        Ok(ProtoQuorumCertificate {
            digest: self.hash_at_n(n).unwrap(),
            n,
            sig: sig_map
                .into_iter()
                .map(|(k, v)| ProtoNameWithSignature {
                    name: k.clone(),
                    sig: v.to_vec(),
                })
                .collect(),
            view: curr_view
        })
    }

    pub fn verify_qc_at_n(
        &self,
        n: u64,
        qc: &ProtoQuorumCertificate,
        supermajority: u64,
        keys: &KeyStore,
    ) -> Result<(), Error> {
        if n > self.last() || n == 0 {
            return Err(Error::new(ErrorKind::InvalidData, "Wrong index"));
        }
        if !cmp_hash(&self.hash_at_n(n).unwrap(), &qc.digest) {
            return Err(Error::new(ErrorKind::InvalidData, "Hash mismatch"));
        }

        let mut matching_sigs = 0;

        for ProtoNameWithSignature { name, sig } in &qc.sig {
            let sig: [u8; SIGNATURE_LENGTH] = match sig.as_slice().try_into() {
                Ok(s) => s,
                Err(_) => {
                    continue;
                }
            };
            if keys.verify(name, &sig, &qc.digest) {
                matching_sigs += 1;
            }
            // @todo: Not implementing the mechanism of having signature on a later block,
            // with witnesses to that block
        }

        if matching_sigs < supermajority {
            return Err(Error::new(
                ErrorKind::InvalidData,
                "Not enough matching signatures",
            ));
        }

        Ok(())
    }

    pub fn hash_at_n(&self, n: u64) -> Option<Vec<u8>> {
        if n > self.last() {
            return None;
        }

        // let mut buf = Vec::new();

        // if n > 0 {
        //     self.get(n).unwrap()
        //         .block
        //         .encode(&mut buf)
        //         .unwrap();
        // }

        // Some(hash(&buf))

        if n == 0 {
            let buf = Vec::new();
            Some(hash(&buf))
        } else {
            Some(self.get(n).unwrap().block_hash.clone())
        }
    }

    pub fn last_hash(&self) -> Vec<u8> {
        self.hash_at_n(self.last()).unwrap()
    }

    /// entry is an unsigned.
    /// push while entry.block.sig is set to empty.
    /// Sign the last_hash(), then add the signature back.
    /// Pretend this as an atomic operation.
    /// If an entry needs to be signed, use this fn instead of push and manually signing.
    pub fn push_and_sign(&mut self, entry: LogEntry, keys: &KeyStore) -> Result<u64, Error> {
        let mut entry = entry;
        entry.block.sig = Some(Sig::NoSig(DefferedSignature {}));
        let n = self.push(entry)?;
        let sig = keys.sign(&self.last_hash());
        let len = self.entries.len();
        self.entries[len - 1].block.sig = Some(Sig::ProposerSig(sig.to_vec()));

        let mut buf = Vec::new();
        self.entries[len - 1].block.encode(&mut buf).unwrap();
        self.entries[len - 1].block_hash.copy_from_slice(&hash(&buf));

        Ok(n)
    }

    /// This the counterpart of push_and_sign
    /// Verify the signature the same way it was created.
    /// Again, use this in favor of push, if block is signed.
    pub fn verify_and_push(
        &mut self,
        entry: LogEntry,
        keys: &KeyStore,
        proposer: &String,
    ) -> Result<u64, Error> {
        let mut entry = entry;
        let sig_opt = entry.block.sig.clone();
        if sig_opt.is_none() {
            return Err(Error::new(ErrorKind::InvalidData, "No signature"));
        }
        let sig = match sig_opt.clone().unwrap() {
            Sig::NoSig(_) => {
                return Err(Error::new(ErrorKind::InvalidData, "Blank signature"));
            }
            Sig::ProposerSig(psig) => {
                let _sig: &[u8; SIGNATURE_LENGTH] = match psig.as_slice().try_into() {
                    Ok(_s) => _s,
                    Err(_) => {
                        return Err(Error::new(ErrorKind::InvalidData, "Malformed signature"));
                    }
                };
                _sig.clone()
            }
        };

        // This is how the signature was created.
        entry.block.sig = Some(Sig::NoSig(DefferedSignature {}));

        // This is essentially the last_hash logic.
        let mut buf = Vec::new();
        entry.block.encode(&mut buf).unwrap();
        let hash_without_sig = hash(&buf);

        if !keys.verify(proposer, &sig, &hash_without_sig) {
            return Err(Error::new(ErrorKind::InvalidData, "Signature not verified"));
        }

        // Check the QCs attached.
        for qc in &entry.block.qc {
            for sig in &qc.sig {
                if sig.sig.len() != SIGNATURE_LENGTH {
                    return Err(Error::new(ErrorKind::InvalidData, "Malformed QC signature"));
                }
                
                if !keys.verify(&sig.name, &sig.sig.clone().try_into().unwrap(), &qc.digest) {
                    return Err(Error::new(ErrorKind::InvalidData, "QC signature not verified"));
                }
            }
        }

        entry.block.sig = sig_opt;

        self.push(entry) // Push the ORIGINAL entry
    }

    /// Signature on the hash of the nth block.
    /// This includes the proposer's signature, if it exists.
    /// This will NOT be over the same hash that created proposer's signature.
    pub fn signature_at_n(&self, n: u64, keys: &KeyStore) -> [u8; SIGNATURE_LENGTH] {
        let hsh = self.hash_at_n(n).unwrap();
        keys.sign(&hsh)
    }

    /// Signature on the hash of the last block.
    /// This includes the proposer's signature, if it exists.
    /// This will NOT be over the same hash that created proposer's signature.
    pub fn last_signature(&self, keys: &KeyStore) -> [u8; SIGNATURE_LENGTH] {
        self.signature_at_n(self.last(), keys)
    }

    pub fn verify_signature_at_n(
        &self,
        n: u64,
        sig: &Vec<u8>,
        name: &String,
        keys: &KeyStore,
    ) -> bool {
        if sig.len() != SIGNATURE_LENGTH {
            return false;
        }

        keys.verify(
            name,
            sig.as_slice().try_into().unwrap(),
            &self.hash_at_n(n).unwrap(),
        )
    }

    pub fn serialize_range(&self, start: u64, end: u64) -> ProtoFork {
        let mut fork = ProtoFork { blocks: Vec::new() };

        for i in start..(end + 1) {
            if i == 0 {
                continue;
            }
            if i > self.last() {
                break;
            }
            let block = self.get(i).unwrap().block.clone();
            fork.blocks.push(block);
        }

        fork
    }

    pub fn serialize_from_n(&self, n: u64) -> ProtoFork {
        self.serialize_range(n, self.last())
    }

    /// Truncate log such that `last() == n`
    pub fn truncate(&mut self, n: u64) -> Result<u64, Error> {
        #[cfg(feature = "storage")]
        if n <= self.gc_hiwm && n > 0 {
            return Err(Error::new(
                ErrorKind::InvalidInput, 
                "Invariant violated: Garbage collected blocks should not be truncated."
            ))
        }
        self.entries.retain(|e| {
            e.block.n <= n
        });

        // Reset last_qc.
        let mut i = (self.entries.len() - 1) as i64;
        while i >= 0 {
            if self.entries[i as usize].block.qc.len() > 0 {
                let mut last_qc = 0;
                let mut last_qc_view = 0;
                for qc in &self.entries[i as usize].block.qc {
                    if qc.n > last_qc {
                        last_qc = qc.n;
                        last_qc_view = qc.view;
                    }
                }

                self.last_qc = last_qc;
                self.last_qc_view = last_qc_view;
                self.last_block_with_qc = self.entries[i as usize].block.n;
                break;
            }
            i -= 1;
        }

        if i < 0 {
            self.last_qc = 0;
            self.last_qc_view = 0;
        }
        Ok(self.last())
    }

    /// Overwrites local fork with the given `fork`.
    /// `fork` must be continuous and must extend a prefix of the local fork.
    /// Doesn't check signatures, make sure yiu validate the signatures first.
    pub fn overwrite(&mut self, fork: &ProtoFork) -> Result<u64, Error> {
        if fork.blocks.len() == 0 {
            return Ok(self.last());
        }

        // Assume fork.block is sorted by n

        // Only overwrite if:
        // First block extends a prefix of me.
        info!("Overwriting fork starts at {}. Local fork ends at {}", fork.blocks[0].n, self.last());

        let ok = if fork.blocks[0].n <= self.last() + 1 {
            if fork.blocks[0].n == 1 {
                true
            } else if fork.blocks[0].n == 0 {
                warn!("Faulty fork");
                false // This is just for sanity check, n == 0 is an invalid block
            } else {
                let n = fork.blocks[0].n;
                let desired_parent_hash = self.hash_at_n(n - 1).unwrap();
                cmp_hash(&desired_parent_hash, &fork.blocks[0].parent)
            }
        } else {
            warn!("Fork missing blocks");
            false
        };

        if !ok {
            return Err(Error::new(ErrorKind::InvalidData, "Fork violates hash chain"))
        }

        // Only overwrite if:
        // fork is continuous.
        let mut i = 0;
        while i < fork.blocks.len() - 1 {
            if fork.blocks[i].n + 1 != fork.blocks[i + 1].n {
                return Err(Error::new(ErrorKind::InvalidData, "Fork has holes"))
            }
            i += 1;
        }

        // Truncate local fork.
        self.truncate(fork.blocks[0].n - 1).unwrap(); // This should not fail!!
        
        for block in &fork.blocks {
            let entry = LogEntry::new(block.clone());
            
            self.push(entry).unwrap();      // This should all pass after the truncation. 
        }



        Ok(self.last())
    }

    pub fn trim_matching_prefix(&self, f: ProtoFork) -> ProtoFork {
        // Find how far I match with given fork
        let idx = f.blocks.binary_search_by(|b| {
            if b.n > self.last() {
                std::cmp::Ordering::Greater
            } else {
                let h = self.hash_at_n(b.n).unwrap();
                let h2 = hash(&b.encode_to_vec());
                if cmp_hash(&h, &h2) {
                    std::cmp::Ordering::Less        // Need upper_bound like behavior
                }else{
                    std::cmp::Ordering::Greater
                }
            }
        }).unwrap_err();

        if idx > 0 && idx < f.blocks.len() {
            let b = f.blocks.get(idx).unwrap();
            if b.n <= self.last() {
                let h = self.hash_at_n(b.n).unwrap();
                let h2 = hash(&b.encode_to_vec());
                assert!(!cmp_hash(&h, &h2));
            }
        }

        let idx = idx - 1;
        if idx > 0 && idx < f.blocks.len() {
            let b = f.blocks.get(idx).unwrap();
            if b.n <= self.last() {
                let h = self.hash_at_n(b.n).unwrap();
                let h2 = hash(&b.encode_to_vec());
                assert!(cmp_hash(&h, &h2));
            }
            ProtoFork {
                blocks: f.blocks.into_iter().skip(idx+1).collect()
            }
        }else{
            f
        }


    }

    pub fn get_last_qc(&self) -> Result<ProtoQuorumCertificate, Error> {
        let block = &self.get(self.last_block_with_qc())?.block;

        for qc in &block.qc {
            if qc.n == self.last_qc() {
                return Ok(qc.clone());
            }
        }

        error!("Invariant violation: QC not found");
        Err(Error::new(ErrorKind::InvalidData, "QC not found"))
    }

    /// Delete the in memory representation of the blocks <= n
    /// Must preserve the last ever entry in the log.
    #[cfg(feature = "storage")]
    pub fn garbage_collect_upto(&mut self, n: u64) {
        let n = if n >= self.last() {
            self.last() - 1
        } else {
            n
        };

        if self.gc_hiwm >= n {
            return
        }


        let mut write_batch = Vec::new();
        while self.entries.len() > 1 {
            if self.entries.front().as_ref().unwrap().block.n <= n {
                let entry = self.entries.pop_front().unwrap();
                let block_hsh = self.entries.front().as_ref().unwrap().block.parent.clone();
                let mut block_ser = Vec::new();
                entry.block.encode(&mut block_ser).unwrap();
                write_batch.push((block_ser, block_hsh));
            } else {
                break;
            }
        }

        self.storage_engine.put_multiple_blocks(&write_batch).unwrap();

        self.gc_hiwm = n;
    }

    pub fn gc_hiwm(&self) -> u64 {
        #[cfg(feature = "storage")]
        return self.gc_hiwm;

        #[cfg(not(feature = "storage"))]
        return 0;
    }

}

impl Drop for Log {
    fn drop(&mut self) {
        #[cfg(feature = "storage")]
        self.storage_engine.destroy();
    }
}

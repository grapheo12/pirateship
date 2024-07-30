use std::{
    collections::{HashMap, HashSet},
    io::{Error, ErrorKind},
};

use ed25519_dalek::SIGNATURE_LENGTH;
use log::{info, warn};
use prost::Message;

use crate::crypto::{cmp_hash, hash, KeyStore};

use super::proto::consensus::{
    proto_block::Sig, DefferedSignature, ProtoBlock, ProtoFork, ProtoNameWithSignature,
    ProtoQuorumCertificate,
};

#[derive(Clone, Debug)]
pub struct LogEntry {
    pub block: ProtoBlock,
    pub replication_votes: HashSet<String>,

    /// Accumulate signatures in the leader.
    /// Used as signature cache in the followers.
    pub qc_sigs: HashMap<String, [u8; SIGNATURE_LENGTH]>,
}

impl LogEntry {
    pub fn new(block: ProtoBlock) -> LogEntry {
        LogEntry {
            block,
            replication_votes: HashSet::new(),
            qc_sigs: HashMap::new(),
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
    entries: Vec<LogEntry>,

    /// Highest QC.n seen so far
    last_qc: u64,
}

impl Log {
    pub fn new() -> Log {
        Log {
            entries: Vec::new(),
            last_qc: 0,
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
                self.last_qc = qc.n
            }
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
            return Err(Error::new(
                ErrorKind::InvalidInput,
                "Vote for missing block!",
            ));
        }

        let idx = n - 1; // Index is 1-based
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

        let idx = n - 1; // Index is 1-based
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

        if !self.verify_signature_at_n(n, sig, name, keys) {
            return Err(Error::new(ErrorKind::InvalidInput, "Invalid signature"));
        }

        let idx = n - 1; // Index is 1-based
        let entry = self.entries.get_mut(idx as usize).unwrap();
        entry
            .qc_sigs
            .insert(name.clone(), sig.as_slice().try_into().unwrap());

        Ok(entry.qc_sigs.len() as u64)
    }

    pub fn get_qc_at_n(&self, n: u64) -> Result<ProtoQuorumCertificate, Error> {
        let sig_map = self.get(n)?.qc_sigs.clone();
        Ok(ProtoQuorumCertificate {
            digest: self.hash_at_n(n).unwrap(),
            n,
            sig: sig_map
                .into_iter()
                .map(|(k, v)| ProtoNameWithSignature {
                    name: k,
                    sig: v.to_vec(),
                })
                .collect(),
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

        let mut buf = Vec::new();

        if n > 0 {
            self.entries[(n - 1) as usize]
                .block
                .encode(&mut buf)
                .unwrap();
        }

        Some(hash(&buf))
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

    pub fn serialize_from_n(&self, n: u64) -> ProtoFork {
        let mut fork = ProtoFork { blocks: Vec::new() };

        for i in n..(self.last() + 1) {
            if i == 0 {
                continue;
            }
            let block = self.get(i).unwrap().block.clone();
            fork.blocks.push(block);
        }

        fork
    }

    /// Truncate log such that `last() == n`
    pub fn truncate(&mut self, n: u64) {
        self.entries.truncate(n as usize);
        // Reset last_qc.
        let mut i = (self.last() - 1) as i64;
        while i >= 0 {
            if self.entries[i as usize].block.qc.len() > 0 {
                let mut last_qc = 0;
                for qc in &self.entries[i as usize].block.qc {
                    if qc.n > last_qc {
                        last_qc = qc.n;
                    }
                }

                self.last_qc = last_qc;
                break;
            }
            i -= 1;
        }
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
        self.truncate(fork.blocks[0].n - 1);
        for block in &fork.blocks {
            let entry = LogEntry {
                block: block.clone(),
                replication_votes: HashSet::new(),
                qc_sigs: HashMap::new()
            };
            
            self.push(entry).unwrap();      // This should all pass after the truncation. 
        }



        Ok(self.last())
    }
}

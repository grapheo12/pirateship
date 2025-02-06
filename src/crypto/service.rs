use std::io::{BufReader, Error, ErrorKind};

use ed25519_dalek::SIGNATURE_LENGTH;
use log::trace;
use nix::libc::PARENB;
use prost::Message;
use rand::{thread_rng, Rng};
use sha2::{Digest, Sha256};
use tokio::{sync::{mpsc::{channel, Receiver, Sender}, oneshot}, task::JoinSet};

use crate::{consensus_v2::fork_receiver::MultipartFork, crypto::DIGEST_LENGTH, proto::consensus::{HalfSerializedBlock, ProtoBlock}, utils::{deserialize_proto_block, serialize_proto_block_nascent, update_parent_hash_in_proto_block_ser, update_signature_in_proto_block_ser}};

use super::{hash, AtomicKeyStore, HashType, KeyStore};

#[derive(Clone, Debug)]
pub struct CachedBlock {
    pub block: ProtoBlock,
    pub block_ser: Vec<u8>,
    pub block_hash: HashType,
}

pub enum FutureHash {
    None,
    Immediate(HashType),
    Future(oneshot::Receiver<HashType>),
}

impl FutureHash {
    pub fn take(&mut self) -> Self {
        std::mem::replace(self, FutureHash::None)
    }
}

fn hash_proto_block_ser(data: &[u8]) -> HashType {
    let mut hasher = Sha256::new();
    hasher.update(&data[DIGEST_LENGTH+SIGNATURE_LENGTH..]);
    hasher.update(&data[SIGNATURE_LENGTH..SIGNATURE_LENGTH+DIGEST_LENGTH]);
    hasher.update(&data[..SIGNATURE_LENGTH]);
    hasher.finalize().to_vec()
}

enum CryptoServiceCommand {
    Hash(Vec<u8>, oneshot::Sender<Vec<u8>>),
    Sign(Vec<u8>, oneshot::Sender<[u8; SIGNATURE_LENGTH]>),
    Verify(Vec<u8> /* data */, String /* Signer name */, [u8; SIGNATURE_LENGTH] /* Signature */, oneshot::Sender<bool>),
    ChangeKeyStore(KeyStore, oneshot::Sender<()>),
    PrepareBlock(ProtoBlock, oneshot::Sender<CachedBlock>, oneshot::Sender<HashType>, oneshot::Sender<HashType>, bool /* must_sign */, FutureHash),

    // Takes the output of StorageService and converts it to CachedBlock.
    CheckBlockSer(HashType, oneshot::Receiver<Result<Vec<u8>, Error>>, oneshot::Sender<Result<CachedBlock, Error>>),
    
    // Deserializes and verifies block serialization
    VerifyBlockSer(Vec<u8>, oneshot::Sender<Result<CachedBlock, Error>>),
    
    Die
}

pub struct CryptoService {
    num_tasks: usize,
    keystore: AtomicKeyStore,
    handles: JoinSet<()>,
    cmd_txs: Vec<Sender<CryptoServiceCommand>>,
}

/// Every task that wants to use CryptoService must use it through this Connector.
/// The connector must be created and passed down to the tasks by the main thread (that created the crypto service)
pub struct CryptoServiceConnector {
    cmd_txs: Vec<Sender<CryptoServiceCommand>>,
    round_robin: usize,
    num_tasks: usize,
}

impl CryptoService {
    pub fn new(num_tasks: usize, keystore: AtomicKeyStore) -> Self {
        assert!(num_tasks > 0);
        Self { num_tasks, keystore, handles: JoinSet::new(), cmd_txs: Vec::with_capacity(num_tasks) }
    }

    async fn worker(keystore: AtomicKeyStore, mut cmd_rx: Receiver<CryptoServiceCommand>, worker_id: usize) {
        let mut total_work = 0;
        while let Some(cmd) = cmd_rx.recv().await {
            total_work += 1usize;

            if total_work % 1000 == 0 {
                trace!("Crypto service worker {} total work: {}", worker_id, total_work);
            }
            match cmd {
                CryptoServiceCommand::Hash(data, res_tx) => {
                    let _ = res_tx.send(hash(&data));
                },
                CryptoServiceCommand::Sign(data, res_tx) => {
                    let _ = res_tx.send(keystore.get().sign(&data));
                },
                CryptoServiceCommand::Verify(data, signer, signature, res_tx) => {
                    let _ = res_tx.send(keystore.get().verify(&signer, &signature, &data));
                },
                CryptoServiceCommand::ChangeKeyStore(key_store, res_tx) => {
                    keystore.set(Box::new(key_store));
                    let _ = res_tx.send(());
                },
                CryptoServiceCommand::Die => {
                    break;
                },
                CryptoServiceCommand::PrepareBlock(proto_block, block_tx, hash_tx, hash_tx2, must_sign, parent_hash_rx) => {
                    // let mut buf = bincode::serialize(&proto_block).unwrap();
                    // let mut buf = bitcode::encode(&proto_block);
                    // let mut buf = proto_block.encode_to_vec();

                    let mut buf = serialize_proto_block_nascent(&proto_block).unwrap();
                    let mut hasher = Sha256::new();
                    hasher.update(&buf[DIGEST_LENGTH+SIGNATURE_LENGTH..]);

                    let parent = match parent_hash_rx {
                        FutureHash::None => vec![0u8; DIGEST_LENGTH],
                        FutureHash::Immediate(val) => val,
                        FutureHash::Future(receiver) => receiver.await.unwrap(),
                    };
                    update_parent_hash_in_proto_block_ser(&mut buf, &parent);
                    let mut block = proto_block;
                    block.parent = parent;
                    hasher.update(&buf[SIGNATURE_LENGTH..SIGNATURE_LENGTH+DIGEST_LENGTH]);
                    if must_sign {
                        // Signature is on the (parent_hash || block) part of the serialized block.
                        let partial_hsh = hash(&buf[SIGNATURE_LENGTH..]);
                        let keystore = keystore.get();
                        let sig = keystore.sign(&partial_hsh);
                        block.sig = Some(crate::proto::consensus::proto_block::Sig::ProposerSig(sig.to_vec()));
                        update_signature_in_proto_block_ser(&mut buf, &sig);
                    }

                    hasher.update(&buf[..SIGNATURE_LENGTH]);

                    let hsh = hasher.finalize().to_vec();

                    let _ = hash_tx.send(hsh.clone());
                    let _ = hash_tx2.send(hsh.clone());
                    let _ = block_tx.send(CachedBlock {
                        block,
                        block_ser: buf,
                        block_hash: hsh
                    });
                },
                CryptoServiceCommand::CheckBlockSer(hsh, ser_rx, block_tx) => {
                    let res = ser_rx.await.unwrap();
                    if let Err(e) = res {
                        block_tx.send(Err(e)).unwrap();
                        continue;
                    }
                    let block_ser = res.unwrap();

                    let chk_hsh = hash_proto_block_ser(&block_ser);
                    if !chk_hsh.eq(&hsh) {
                        block_tx.send(Err(Error::new(ErrorKind::InvalidData, "Invalid hash"))).unwrap();
                        continue;
                    }

                    let block = deserialize_proto_block(block_ser.as_ref());
                    match block {
                        Ok(block) => {
                            block_tx.send(Ok(CachedBlock {
                                block,
                                block_ser,
                                block_hash: hsh,
                            })).unwrap();
                        },
                        Err(_) => {
                            block_tx.send(Err(Error::new(ErrorKind::InvalidData, "Decode error"))).unwrap();
                        },
                    };
                },
                CryptoServiceCommand::VerifyBlockSer(block_ser, block_tx) => {
                    let block = deserialize_proto_block(block_ser.as_ref());
                    let hsh = hash_proto_block_ser(&block_ser);

                    // TODO: Verify signature
                    // TODO: Verify each QuorumCertificate attached.
                    // TODO: If view_is_stable = False, verify ProtoForkValidation
                    match block {
                        Ok(block) => {
                            block_tx.send(Ok(CachedBlock {
                                block,
                                block_ser,
                                block_hash: hsh,
                            })).unwrap();
                        },
                        Err(_) => {
                            block_tx.send(Err(Error::new(ErrorKind::InvalidData, "Decode error"))).unwrap();
                        },
                    };
                }
            }
        }

        trace!("Crypto service worker {} done. Total work: {}", worker_id, total_work);
    }

    pub fn run(&mut self) {
        for i in 0..self.num_tasks {
            let (tx, rx) = channel(2048);
            self.cmd_txs.push(tx);
            let key_store = self.keystore.clone();
            self.handles.spawn(async move {
                Self::worker(key_store, rx, i).await;
            });
        }
    }

    pub fn get_connector(&self) -> CryptoServiceConnector {
        CryptoServiceConnector {
            cmd_txs: self.cmd_txs.iter().map(|e| e.clone()).collect(),
            round_robin: thread_rng().gen(),
            num_tasks: self.num_tasks
        }
    }
}

macro_rules! dispatch_cmd {
    ($self: expr, $cmd: expr, $($args: expr),+) => {
        {
            let (tx, rx) = oneshot::channel();
            $self.dispatch($cmd($($args),+, tx)).await;
    
            match rx.await {
                Ok(ret) => ret,
                Err(e) => panic!("Crypto service error: {}", e),
            }
        }
    };
}

impl CryptoServiceConnector {
    async fn dispatch(&mut self, cmd: CryptoServiceCommand) {
        if let Err(e) = self.cmd_txs[self.round_robin % self.num_tasks].send(cmd).await {
            panic!("Crypto service failed: {}", e);
        }
        self.round_robin += 1;
    }

    pub async fn kill(&mut self) {
        for _ in 0..self.num_tasks {
            let cmd = CryptoServiceCommand::Die;
            self.dispatch(cmd).await;
        }
    }

    pub async fn hash(&mut self, data: &Vec<u8>) -> Vec<u8> {   
        dispatch_cmd!(self, CryptoServiceCommand::Hash, data.clone())
    }

    pub async fn sign(&mut self, data: &Vec<u8>) -> [u8; SIGNATURE_LENGTH] {
        dispatch_cmd!(self, CryptoServiceCommand::Sign, data.clone())
    }

    pub async fn verify(&mut self, data: &Vec<u8>, signer: &String, signature: &[u8; SIGNATURE_LENGTH]) -> bool {
        dispatch_cmd!(self, CryptoServiceCommand::Verify, data.clone(), signer.clone(), signature.clone())
    }

    pub async fn change_key_store(&mut self, key_store: KeyStore) {
        dispatch_cmd!(self, CryptoServiceCommand::ChangeKeyStore, key_store);
    }

    pub async fn prepare_block(&mut self, block: ProtoBlock, must_sign: bool, parent_hash_rx: FutureHash) -> (oneshot::Receiver<CachedBlock>, oneshot::Receiver<HashType>, oneshot::Receiver<HashType>) {
        let (block_tx, block_rx) = oneshot::channel();
        let (hash_tx, hash_rx) = oneshot::channel();
        let (hash_tx2, hash_rx2) = oneshot::channel();
        self.dispatch(CryptoServiceCommand::PrepareBlock(block, block_tx, hash_tx, hash_tx2, must_sign, parent_hash_rx)).await;

        (block_rx, hash_rx, hash_rx2)
    }

    pub async fn check_block(&mut self, hsh: HashType, ser_rx: oneshot::Receiver<Result<Vec<u8>, Error>>) -> Result<CachedBlock, Error> {
        dispatch_cmd!(self, CryptoServiceCommand::CheckBlockSer, hsh, ser_rx)
    }

    pub async fn prepare_fork(&mut self, mut part: Vec<HalfSerializedBlock>, remaining_parts: usize) -> MultipartFork {
        
        MultipartFork {
            fork_future: part.drain(..).map(|e| {
                let (tx, rx) = oneshot::channel();
                self.dispatch(CryptoServiceCommand::VerifyBlockSer(e.serialized_body, tx));
                Some(rx)
            }).collect(),
            remaining_parts
        }
    }


}
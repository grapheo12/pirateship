use std::io::{BufReader, Error, ErrorKind};

use ed25519_dalek::SIGNATURE_LENGTH;
use prost::Message;
use tokio::{sync::{mpsc::{channel, Receiver, Sender}, oneshot}, task::JoinSet};

use crate::proto::consensus::ProtoBlock;

use super::{hash, AtomicKeyStore, HashType, KeyStore};

#[derive(Clone)]
pub struct CachedBlock {
    pub block: ProtoBlock,
    pub block_ser: Vec<u8>,
    pub block_hash: HashType,
}

enum CryptoServiceCommand {
    Hash(Vec<u8>, oneshot::Sender<Vec<u8>>),
    Sign(Vec<u8>, oneshot::Sender<[u8; SIGNATURE_LENGTH]>),
    Verify(Vec<u8> /* data */, String /* Signer name */, [u8; SIGNATURE_LENGTH] /* Signature */, oneshot::Sender<bool>),
    ChangeKeyStore(KeyStore, oneshot::Sender<()>),
    PrepareBlock(ProtoBlock, oneshot::Sender<CachedBlock>, oneshot::Sender<HashType>, bool /* must_sign */),

    // Takes the output of StorageService and converts it to CachedBlock.
    CheckBlockSer(HashType, oneshot::Receiver<Result<Vec<u8>, Error>>, oneshot::Sender<Result<CachedBlock, Error>>),
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

    async fn worker(keystore: AtomicKeyStore, mut cmd_rx: Receiver<CryptoServiceCommand>) {
        while let Some(cmd) = cmd_rx.recv().await {
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
                CryptoServiceCommand::PrepareBlock(proto_block, block_tx, hash_tx, must_sign) => {
                    let mut buf = proto_block.encode_to_vec();
                    let mut hsh = hash(&buf);
                    let mut block = proto_block;
                    if must_sign {
                        let keystore = keystore.get();
                        let sig = keystore.sign(&hsh);
                        block.sig = Some(crate::proto::consensus::proto_block::Sig::ProposerSig(sig.to_vec()));

                        buf = block.encode_to_vec();
                        hsh = hash(&buf);
                    }

                    hash_tx.send(hsh.clone());
                    block_tx.send(CachedBlock {
                        block,
                        block_ser: buf,
                        block_hash: hsh
                    });
                },
                CryptoServiceCommand::CheckBlockSer(hsh, ser_rx, block_tx) => {
                    let res = ser_rx.await.unwrap();
                    if let Err(e) = res {
                        block_tx.send(Err(e));
                        continue;
                    }
                    let block_ser = res.unwrap();

                    let chk_hsh = hash(&block_ser);
                    if !chk_hsh.eq(&hsh) {
                        block_tx.send(Err(Error::new(ErrorKind::InvalidData, "Invalid hash")));
                        continue;
                    }

                    let block = ProtoBlock::decode(block_ser.as_ref());
                    match block {
                        Ok(block) => {
                            block_tx.send(Ok(CachedBlock {
                                block,
                                block_ser,
                                block_hash: hsh,
                            }));
                        },
                        Err(_) => {
                            block_tx.send(Err(Error::new(ErrorKind::InvalidData, "Decode error")));
                        },
                    };
                },
            }
        }
    }

    pub fn run(&mut self) {
        for _ in 0..self.num_tasks {
            let (tx, rx) = channel(2048);
            self.cmd_txs.push(tx);
            let key_store = self.keystore.clone();
            self.handles.spawn(async move {
                Self::worker(key_store, rx).await;
            });
        }
    }

    pub fn get_connector(&self) -> CryptoServiceConnector {
        CryptoServiceConnector {
            cmd_txs: self.cmd_txs.iter().map(|e| e.clone()).collect(),
            round_robin: 0,
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

    pub async fn prepare_block(&mut self, block: ProtoBlock, must_sign: bool) -> (oneshot::Receiver<CachedBlock>, oneshot::Receiver<HashType>) {
        let (block_tx, block_rx) = oneshot::channel();
        let (hash_tx, hash_rx) = oneshot::channel();
        self.dispatch(CryptoServiceCommand::PrepareBlock(block, block_tx, hash_tx, must_sign)).await;

        (block_rx, hash_rx)
    }

    pub async fn check_block(&mut self, hsh: HashType, ser_rx: oneshot::Receiver<Result<Vec<u8>, Error>>) -> Result<CachedBlock, Error> {
        dispatch_cmd!(self, CryptoServiceCommand::CheckBlockSer, hsh, ser_rx)
    }


}
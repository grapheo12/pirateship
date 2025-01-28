use std::io::Error;

use tokio::sync::oneshot;

use crate::crypto::{CachedBlock, CryptoServiceConnector, HashType};

use super::{channel::{make_channel, Receiver, Sender}, StorageEngine};


enum StorageServiceCommand {
    Put(HashType /* key */, Vec<u8> /* val */, oneshot::Sender<Result<(), Error>>),
    Get(HashType /* key */, oneshot::Sender<Result<Vec<u8>, Error>>)
}

pub struct StorageService<S: StorageEngine> {
    db: S,

    cmd_rx: Receiver<StorageServiceCommand>,
    cmd_tx: Sender<StorageServiceCommand>
}


pub struct StorageServiceConnector {
    cmd_tx: Sender<StorageServiceCommand>,
    crypto: CryptoServiceConnector,
}


impl<S: StorageEngine> StorageService<S> {
    pub fn new(db: S, buffer_size: usize) -> Self {
        let (cmd_tx, cmd_rx) = make_channel(buffer_size);
        Self { db, cmd_rx, cmd_tx }
    }

    pub fn get_connector(&self, crypto: CryptoServiceConnector) -> StorageServiceConnector {
        StorageServiceConnector {
            cmd_tx: self.cmd_tx.clone(),
            crypto
        }
    }

    pub async fn run(&mut self) {
        self.db.init();
        while let Some(cmd) = self.cmd_rx.recv().await {
            match cmd {
                StorageServiceCommand::Put(key, val, ok_chan) => {
                    let res = self.db.put_block(&val, &key);
                    ok_chan.send(res);
                },
                StorageServiceCommand::Get(key, val_chan) => {
                    let res = self.db.get_block(&key);
                    val_chan.send(res);
                },
            }
        }
        self.db.destroy();
    }
}

impl StorageServiceConnector {
    pub async fn get_block(&mut self, block_hash: &HashType) -> Result<CachedBlock, Error> {
        let (tx, rx) = oneshot::channel();
        self.cmd_tx.send(StorageServiceCommand::Get(block_hash.clone(), tx));

        // Can't trust Disk to not have changed.
        self.crypto.check_block(block_hash.clone(), rx).await
    }

    pub async fn put_block(&self, block: &CachedBlock) -> Result<(), Error> {
        let (tx, rx) = oneshot::channel();
        self.cmd_tx.send(StorageServiceCommand::Put(block.block_hash.clone(), block.block_ser.clone(), tx));

        rx.await.unwrap()
    }
}
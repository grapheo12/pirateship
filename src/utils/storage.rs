use crate::config::StorageConfig;
use std::{fmt::Debug, io::Error};

pub trait StorageEngine: Debug + Sync + Send {
    fn init(&mut self);
    fn destroy(&mut self);

    /// Can't trust the storage to handle anything more than block hashes
    fn put_block(&mut self, block_ser: &Vec<u8>, block_hash: &Vec<u8>) -> Result<(), Error>;
    fn get_block(&self, block_hash: &Vec<u8>) -> Result<Vec<u8>, Error>;
}

#[derive(Debug)]
pub struct RocksDBStorageEngine {

}

impl RocksDBStorageEngine {
    pub fn new(config: StorageConfig) -> RocksDBStorageEngine {
        RocksDBStorageEngine {  }
    }
}

impl StorageEngine for RocksDBStorageEngine {
    fn init(&mut self) {
        todo!()
    }

    fn destroy(&mut self) {
        todo!()
    }

    fn put_block(&mut self, block_ser: &Vec<u8>, block_hash: &Vec<u8>) -> Result<(), Error> {
        todo!()
    }

    fn get_block(&self, block_hash: &Vec<u8>) -> Result<Vec<u8>, Error> {
        todo!()
    }
}
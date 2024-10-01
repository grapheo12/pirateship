use rand_chacha::rand_core::block;
use rocksdb::{Options, DB};

use crate::config::{RocksDBConfig, StorageConfig};
use std::{fmt::Debug, io::{Error, ErrorKind}};

pub trait StorageEngine: Debug + Sync + Send {
    fn init(&mut self);
    fn destroy(&mut self);

    /// Can't trust the storage to handle anything more than block hashes
    fn put_block(&mut self, block_ser: &Vec<u8>, block_hash: &Vec<u8>) -> Result<(), Error>;
    fn get_block(&self, block_hash: &Vec<u8>) -> Result<Vec<u8>, Error>;
}

#[derive(Debug)]
pub struct RocksDBStorageEngine {
    pub config: RocksDBConfig,
    pub db: DB
}

impl RocksDBStorageEngine {
    pub fn new(config: StorageConfig) -> RocksDBStorageEngine {
        if let StorageConfig::RocksDB(config) = config {
            let mut opts = Options::default();
            opts.create_if_missing(true);
            opts.set_write_buffer_size(config.write_buffer_size);
            let path = config.db_path.clone();
            RocksDBStorageEngine {
                config,
                db: DB::open(&opts, path).unwrap(),
            }
        } else {
            panic!("Wrong config")
        }
    }
}

impl StorageEngine for RocksDBStorageEngine {
    fn init(&mut self) {
        // This does nothing for RocksDBStorageEngine, since it is already created when new() is called.
    }

    fn destroy(&mut self) {
        let _ = self.db.flush();
    }

    fn put_block(&mut self, block_ser: &Vec<u8>, block_hash: &Vec<u8>) -> Result<(), Error> {
        let res = self.db.put(block_hash, block_ser);
        match res {
            Ok(_) => {
                return Ok(())
            },
            Err(e) => {
                return Err(Error::new(ErrorKind::BrokenPipe, e))
            },
        }
    }

    fn get_block(&self, block_hash: &Vec<u8>) -> Result<Vec<u8>, Error> {
        let res = self.db.get(block_hash);
        match res {
            Ok(val) => {
                match val {
                    Some(val) => {
                        return Ok(val)
                    },
                    None => {
                        Err(Error::new(ErrorKind::InvalidInput, "Key not found"))
                    },
                }
            },
            Err(e) => {
                return Err(Error::new(ErrorKind::InvalidInput, e))
            },
        }
    }
}
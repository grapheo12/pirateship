use std::{fmt::Debug, fs::{create_dir, exists}, io::Error, sync::Mutex};

use indexmap::IndexMap;

use crate::config::{FileStorageConfig, StorageConfig};

use super::StorageEngine;
use hex::ToHex;

pub struct FileStorageEngine {
    pub config: FileStorageConfig,
    memtable: Mutex<IndexMap<Vec<u8>, Vec<u8>>>,
}

impl FileStorageEngine {
    pub fn new(config: StorageConfig) -> Self {
        match config {
            StorageConfig::FileStorage(file_storage_config) => {
                return Self {
                    config: file_storage_config,
                    memtable: Mutex::new(IndexMap::new()),
                }
            },

            _ => panic!("Invalid config")
            
        }
    }

    pub fn compact(&self, entry_cnt: usize) {
        let mut cnt = entry_cnt;
        let mut mtable = self.memtable.lock().unwrap();
        mtable.retain(|k, v| {
            if cnt == 0 {
                return true;
            }
            cnt -= 1;

            self.persist(k, v).unwrap();

            return false;
        });

    }

    pub fn persist(&self, block_hash: &Vec<u8>, block_ser: &Vec<u8>) -> Result<(), Error> {
        let fname = self.hash_to_fname(block_hash);
        self.write_file(&fname, block_ser)?;
        Ok(())
    }

    fn hash_to_fname(&self, block_hash: &Vec<u8>) -> String {
        let path = block_hash.encode_hex::<String>();
        let short_path = path[..2].to_string();
        let sep = "/";      // Very UNIXy

        let dir_path = self.config.db_path.clone() + sep + &short_path;
        self.create_dir_if_not_exists(&dir_path);

        let path = dir_path + sep + &path;
        path
    }

    fn write_file(&self, path: &String, contents: &Vec<u8>) -> Result<(), Error> {
        std::fs::write(path, contents)?;
        Ok(())

    }

    fn read_file(&self, path: &String) -> Result<Vec<u8>, Error> {
        let res = std::fs::read(path)?;
        Ok(res)
    }

    fn create_dir_if_not_exists(&self, dir_path: &String) {
        // mkdir -p
        let exists_res = exists(dir_path);
        if let Err(e) = exists_res {
            panic!("{}", e);
        }

        if !exists_res.unwrap() {
            let res = create_dir(dir_path);
            if let Err(e) = res {
                panic!("{}", e);
            }
        }
    }

}

impl Debug for FileStorageEngine {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FileStorageEngine").field("config", &self.config).field("memtable", &self.memtable).finish()
    }
}

impl StorageEngine for FileStorageEngine {
    fn init(&mut self) {
        // mkdir -p db_path
        self.create_dir_if_not_exists(&self.config.db_path);
    }

    fn destroy(&self) {
        let n = {
            let mtable = self.memtable.lock().unwrap();
            mtable.len()
        };

        self.compact(n);
    }

    fn put_block(&self, block_ser: &Vec<u8>, block_hash: &Vec<u8>) -> Result<(), std::io::Error> {
        // Insert to memtable
        let n = {
            let mut mtable = self.memtable.lock().unwrap();
            mtable.insert(block_hash.clone(), block_ser.clone());
            mtable.len()
        };

        if n > self.config.memtable_size {
            // Clean half the memtable
            self.compact(n / 2);
        }

        Ok(())


    }

    fn get_block(&self, block_hash: &Vec<u8>) -> Result<Vec<u8>, std::io::Error> {
        // Search in memtable
        let mtable = self.memtable.lock().unwrap();
        let res = mtable.get(block_hash);
        if let Some(ser) = res {
            return Ok(ser.clone());
        }

        let path = self.hash_to_fname(block_hash);
        self.read_file(&path)
    }
}
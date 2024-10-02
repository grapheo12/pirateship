// Copyright (c) Shubham Mishra. All rights reserved.
// Licensed under the MIT License.

use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct RocksDBConfig {
    pub db_path: String,
    pub wal_enabled: bool,
    pub write_buffer_size: usize,
}

impl Default for RocksDBConfig {
    fn default() -> Self {
        Self {
            db_path: String::from("/tmp/testdb"),
            wal_enabled: false,
            write_buffer_size: 32768
        }
    }
}


#[derive(Serialize, Deserialize, Clone, Debug)]
pub enum StorageConfig {
    RocksDB(RocksDBConfig),
}
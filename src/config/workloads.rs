use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct KVReadWriteUniform {
    pub num_keys: usize,
    pub val_size: usize,
    pub read_ratio: f64,
    pub write_byz_commit_ratio: f64
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub enum RequestConfig {
    Blanks,
    KVReadWriteUniform(KVReadWriteUniform),
    KVReadWriteYCSB(), /* Todo */
    MockSQL(),
}

use crate::proto::execution::{ProtoTransaction, ProtoTransactionResult};

pub trait PerWorkerWorkloadGenerator: Send {
    fn next(&mut self) -> ProtoTransaction;
    fn check_result(&self, result: &Option<ProtoTransactionResult>) -> bool;
}

mod blanks;
pub use blanks::*;

mod kv_uniform;
pub use kv_uniform::*;

mod kv_ycsb;
pub use kv_ycsb::*;

mod mocksql;
pub use mocksql::*;
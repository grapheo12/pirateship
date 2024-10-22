use crate::proto::execution::{ProtoTransaction, ProtoTransactionResult};

use super::PerWorkerWorkloadGenerator;

pub struct BlankWorkloadGenerator { }

impl PerWorkerWorkloadGenerator for BlankWorkloadGenerator {
    fn next(&mut self) -> ProtoTransaction {
        ProtoTransaction{
            on_receive: None,
            // on_crash_commit: Some(ProtoTransactionPhase {
            //     ops: vec![ProtoTransactionOp {
            //         op_type: pft::proto::execution::ProtoTransactionOpType::Write.into(),
            //         operands: vec![
            //             format!("crash_commit_{}", i).into_bytes(),
            //             format!("Tx:{}:{}", idx, i).into_bytes()
            //         ],
            //         // operands: Vec::new(),
            //     }],
            // }),
            on_crash_commit: None,
            on_byzantine_commit: None,
            is_reconfiguration: false,
        }
    }
    
    fn check_result(&self, _result: &Option<ProtoTransactionResult>) -> bool {
        true
    }
}
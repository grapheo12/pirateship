use crate::proto::execution::{ProtoTransaction, ProtoTransactionOp, ProtoTransactionPhase, ProtoTransactionResult};

use super::{PerWorkerWorkloadGenerator, WorkloadUnit, Executor};

pub struct BlankWorkloadGenerator { }

impl PerWorkerWorkloadGenerator for BlankWorkloadGenerator {
    fn next(&mut self) -> WorkloadUnit {
        WorkloadUnit {
            tx: ProtoTransaction{
                on_receive: None,
                on_crash_commit: Some(ProtoTransactionPhase {
                    ops: vec![ProtoTransactionOp {
                        op_type: crate::proto::execution::ProtoTransactionOpType::Noop.into(),
                        operands: vec![vec![2u8; 478]],
                        // operands: vec![vec![2u8; 0]],
                    }; 1],
                }),
                on_byzantine_commit: None,
                is_reconfiguration: false,
            },
            executor: Executor::Leader
        }
    }
    
    fn check_result(&mut self, _result: &Option<ProtoTransactionResult>) -> bool {
        true
    }
}
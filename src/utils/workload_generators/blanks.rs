use crate::{proto::execution::{ProtoTransaction, ProtoTransactionOp, ProtoTransactionPhase, ProtoTransactionResult}, utils::workload_generators::Executor};

use super::{PerWorkerWorkloadGenerator, WorkloadUnit};

pub struct BlankWorkloadGenerator { }

impl PerWorkerWorkloadGenerator for BlankWorkloadGenerator {
    fn next(&mut self) -> WorkloadUnit {
        WorkloadUnit {
            tx: ProtoTransaction{
                on_receive: None,
                // on_crash_commit: None,
                on_crash_commit: Some(ProtoTransactionPhase {
                    ops: vec![ProtoTransactionOp {
                        op_type: crate::proto::execution::ProtoTransactionOpType::Noop.into(),
                        operands: vec![vec![2u8; 512]],
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
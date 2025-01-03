use crate::{consensus::log::{LogConfig, PinnedLog}, proto::consensus::ProtoBlock};

use super::partition::{LogPartition, LogPartitionConfig, OutOfOrderEntry};

#[test]
pub fn test_partition_push() {
    let config = LogPartitionConfig {
        partition_id: 1,
        partition_total: 3,
    };

    let mut log_partition = LogPartition::new(config.clone());

    assert_eq!(log_partition.last_n(), None);

    let mut block = ProtoBlock {
        tx: vec![],
        n: 1,
        parent: vec![],
        view: 1,
        qc: vec![],
        fork_validation: vec![],
        view_is_stable: true,
        config_num: 1,
        sig: None,
    };

    log_partition.push_unchecked(block.clone()).expect_err("Block_n shouldn't match");
    

    block.n += 1;
    log_partition.push_unchecked(block.clone()).expect("Should be able to push this");

    block.n += 2 * config.partition_total;
    log_partition.push_unchecked(block.clone()).expect_err("Should be out of order error");

    log_partition.push_unchecked(block.clone()).expect_err("Should be out of order error");

    block.n -= config.partition_total;
    assert_eq!(log_partition.push_unchecked(block.clone()).unwrap(), block.n + config.partition_total);

    assert_eq!(log_partition.last_n(), Some(block.n + config.partition_total));

    assert_eq!(log_partition.entries.len(), 3);


}

#[test]
pub fn test_out_of_order_entries() {
    let config = LogPartitionConfig {
        partition_id: 0,
        partition_total: 1,
    };

    let mut log_partition = LogPartition::new(config.clone());

    let mut block = ProtoBlock {
        tx: vec![],
        n: 1,
        parent: vec![],
        view: 1,
        qc: vec![],
        fork_validation: vec![],
        view_is_stable: true,
        config_num: 1,
        sig: None,
    };
    log_partition.out_of_order_entries.push(OutOfOrderEntry::new(block.clone()));

    block.n = 5;
    log_partition.out_of_order_entries.push(OutOfOrderEntry::new(block.clone()));


    block.n = 3;
    log_partition.out_of_order_entries.push(OutOfOrderEntry::new(block.clone()));


    block.n = 4;
    log_partition.out_of_order_entries.push(OutOfOrderEntry::new(block.clone()));

    block.n = 4;
    log_partition.out_of_order_entries.push(OutOfOrderEntry::new(block.clone()));

    assert_eq!(log_partition.out_of_order_entries.pop().unwrap().0.0, 1);
    assert_eq!(log_partition.out_of_order_entries.pop().unwrap().0.0, 3);
    assert_eq!(log_partition.out_of_order_entries.pop().unwrap().0.0, 4);
    assert_eq!(log_partition.out_of_order_entries.pop().unwrap().0.0, 4);
    assert_eq!(log_partition.out_of_order_entries.pop().unwrap().0.0, 5);



}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
pub async fn test_log() {
    let config = LogConfig {
        partition_total: 8,
        req_channel_depth: 64
    };


    let mut log = PinnedLog::new(config);

    log.init().await;

    assert_eq!(log.last_n().await, 0);

    let mut block = ProtoBlock {
        tx: vec![],
        n: 1,
        parent: vec![],
        view: 1,
        qc: vec![],
        fork_validation: vec![],
        view_is_stable: true,
        config_num: 1,
        sig: None,
    };

    log.push(block.clone()).await.expect("Should pass");
    

    block.n += 1;
    log.push(block.clone()).await.expect("Should pass");


    assert_eq!(log.last_n().await, block.n);

        
}
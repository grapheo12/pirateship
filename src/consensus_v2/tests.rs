use std::{sync::Arc, thread, time::{Duration, Instant}};

use rand::{thread_rng, Rng as _};
use tokio::{sync::Mutex, task::JoinSet};
use crate::{consensus_v2::{block_broadcaster::BlockBroadcaster, staging::Staging}, rpc::client::Client, utils::{channel::{make_channel, Sender}, RocksDBStorageEngine, StorageService}};

use crate::{config::{AtomicConfig, Config}, consensus_v2::{batch_proposal::BatchProposer, block_sequencer::BlockSequencer}, crypto::{AtomicKeyStore, CryptoService, KeyStore}, proto::execution::{ProtoTransaction, ProtoTransactionOp, ProtoTransactionPhase}};

use super::batch_proposal::{MsgAckChanWithTag, TxWithAckChanTag};

const TEST_CRYPTO_NUM_TASKS: usize = 6;
const MAX_TXS: usize = 2_000_000;
const MAX_CLIENTS: usize = 10;
const TEST_RATE: f64 = 500_000.0;
const PAYLOAD_SIZE: usize = 512;

#[global_allocator]
static ALLOC: snmalloc_rs::SnMalloc = snmalloc_rs::SnMalloc; 

async fn load(batch_proposer_tx: Sender<TxWithAckChanTag>, req_per_sec: f64) {
    let sleep_time = Duration::from_secs_f64(1.0f64 / req_per_sec);
    let mut last_fire_time = Instant::now();
    let start = Instant::now();
    let transaction = ProtoTransaction {
        on_receive: None,
        on_crash_commit: Some(ProtoTransactionPhase {
            ops: vec![ProtoTransactionOp {
                op_type: crate::proto::execution::ProtoTransactionOpType::Noop.into(),
                operands: vec![vec![rand::random(); PAYLOAD_SIZE]],
            }; 1],
        }),
        on_byzantine_commit: None,
        is_reconfiguration: false,
    };
    for tag in 0..MAX_TXS {
        while last_fire_time.elapsed() < sleep_time {
            // Sleep is not that accurate. So busy wait
        }


        let (tx, _rx) = tokio::sync::mpsc::channel(1);

        let ack_chan_with_tag: MsgAckChanWithTag = (tx, tag as u64);

        batch_proposer_tx.send((Some(transaction.clone()), ack_chan_with_tag)).await.unwrap();

        last_fire_time = Instant::now();
    }

    let total_time = start.elapsed().as_secs_f64();
    let input_rate = MAX_TXS as f64 / total_time;

    println!("Input rate: {} req/s", input_rate);
}


#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn test_batch_proposal() {
    // Generate configs first
    let cfg_path = "configs/node1_config.json";
    let cfg_contents = std::fs::read_to_string(cfg_path).expect("Invalid file path");

    let config = Config::deserialize(&cfg_contents);
    let _chan_depth = config.rpc_config.channel_depth as usize;
    let key_store = KeyStore::new(&config.rpc_config.allowed_keylist_path, &config.rpc_config.signing_priv_key_path);

    let config = AtomicConfig::new(config);
    let keystore = AtomicKeyStore::new(key_store);
    println!("num tasks: {}", TEST_CRYPTO_NUM_TASKS);
    let mut crypto = CryptoService::new(TEST_CRYPTO_NUM_TASKS, keystore.clone());
    crypto.run();

    let (batch_proposer_tx, batch_proposer_rx) = make_channel(_chan_depth);
    let (block_maker_tx, mut block_maker_rx) = make_channel(_chan_depth);

    let batch_proposer = Arc::new(Mutex::new(BatchProposer::new(config.clone(), batch_proposer_rx, block_maker_tx)));
    // let block_maker = Arc::new(Mutex::new(BlockMaker::new(config.clone(), control_command_rx, block_maker_rx, qc_rx, block_broadcaster_tx, client_reply_tx, block_maker_crypto)));

    let mut handles = JoinSet::new();

    handles.spawn(async move {
        BatchProposer::run(batch_proposer).await;
        println!("Batch proposer quits");
    });


    for _ in 0..MAX_CLIENTS {
        let __tx = batch_proposer_tx.clone();
        handles.spawn(async move {
            load(__tx, TEST_RATE).await;
        });
    }

    let mut total_txs_output = 0;
    let mut last_block = 0;
    let mut underfull_batches = 0;

    let start = Instant::now();
    while let Some(block) = block_maker_rx.recv().await {
        last_block += 1;

        let block_sz = block.0.len();

        if block_sz < config.get().consensus_config.max_backlog_batch_size {
            underfull_batches += 1;
        }

        total_txs_output += block_sz;

        if total_txs_output >= MAX_CLIENTS * MAX_TXS {
            break;
        }
    }
    let total_time = start.elapsed().as_secs_f64();
    let throughput = total_txs_output as f64 / total_time;
    println!("Throughput: {} req/s", throughput);
    println!("Total blocks: {}", last_block);
    println!("Underfull blocks: {}", underfull_batches);

    if underfull_batches > 10 {
        panic!("Too many underfull batches");
    }


    handles.shutdown().await;
}


#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn test_block_sequencer() {
    // Generate configs first
    let cfg_path = "configs/node1_config.json";
    let cfg_contents = std::fs::read_to_string(cfg_path).expect("Invalid file path");

    let config = Config::deserialize(&cfg_contents);
    let _chan_depth = config.rpc_config.channel_depth as usize;
    let key_store = KeyStore::new(&config.rpc_config.allowed_keylist_path, &config.rpc_config.signing_priv_key_path);

    let config = AtomicConfig::new(config);
    let keystore = AtomicKeyStore::new(key_store);
    println!("num tasks: {}", TEST_CRYPTO_NUM_TASKS);

    let mut crypto = CryptoService::new(TEST_CRYPTO_NUM_TASKS, keystore.clone());
    crypto.run();

    let (batch_proposer_tx, batch_proposer_rx) = make_channel(_chan_depth);
    let (block_maker_tx, block_maker_rx) = make_channel(_chan_depth);
    let (control_command_tx, control_command_rx) = make_channel(_chan_depth);
    let (qc_tx, qc_rx) = make_channel(_chan_depth);
    let (block_broadcaster_tx, mut block_broadcaster_rx) = make_channel(_chan_depth);
    let (client_reply_tx, mut client_reply_rx) = make_channel(_chan_depth);

    let block_maker_crypto = crypto.get_connector();

    let batch_proposer = Arc::new(Mutex::new(BatchProposer::new(config.clone(), batch_proposer_rx, block_maker_tx)));
    let block_maker = Arc::new(Mutex::new(BlockSequencer::new(config.clone(), control_command_rx, block_maker_rx, qc_rx, block_broadcaster_tx, client_reply_tx, block_maker_crypto)));

    let mut handles = JoinSet::new();

    handles.spawn(async move {
        BatchProposer::run(batch_proposer).await;
        println!("Batch proposer quits");
    });

    handles.spawn(async move {
        while let Some(_) = client_reply_rx.recv().await {
            // Sink
        }
    });


    handles.spawn(async move {
        BlockSequencer::run(block_maker).await;
        println!("Block maker quits");
    });


    for _ in 0..MAX_CLIENTS {
        let __tx = batch_proposer_tx.clone();
        handles.spawn(async move {
            load(__tx, TEST_RATE).await;
        });
    }

    let mut total_txs_output = 0;
    let mut last_block = 0;
    let mut underfull_batches = 0;
    let mut signed_blocks = 0;

    let start = Instant::now();
    while let Some(block) = block_broadcaster_rx.recv().await {
        let block = block.1.await.unwrap();

        if block.block.n != last_block + 1 {
            panic!("Monotonicity broken!");
        }
        last_block += 1;

        let block_sz = match block.block.tx.unwrap() {
            crate::proto::consensus::proto_block::Tx::TxList(proto_transaction_list) => {
                proto_transaction_list.tx_list.len()
            },
            crate::proto::consensus::proto_block::Tx::TxListHash(vec) => {
                vec.len()
            },
        };

        if block_sz < config.get().consensus_config.max_backlog_batch_size {
            underfull_batches += 1;
        }

        total_txs_output += block_sz;

        if let Some(crate::proto::consensus::proto_block::Sig::ProposerSig(_)) = block.block.sig {
            signed_blocks += 1;
        }


        if total_txs_output >= MAX_CLIENTS * MAX_TXS {
            break;
        }
    }
    let total_time = start.elapsed().as_secs_f64();
    let throughput = total_txs_output as f64 / total_time;
    println!("Throughput: {} req/s", throughput);
    println!("Total blocks: {}", last_block);
    println!("Underfull blocks: {}", underfull_batches);
    println!("Signed blocks: {}", signed_blocks);

    if underfull_batches > 10 {
        panic!("Too many underfull batches");
    }

    let expected_signed_blocks = (last_block / config.get().consensus_config.signature_max_delay_blocks) as i32;

    if signed_blocks < expected_signed_blocks - 15 || signed_blocks > expected_signed_blocks + 15 {
        panic!("Too much or too few signed blocks");
    }


    handles.shutdown().await;
}


#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn test_block_broadcaster() {
    // Generate configs first
    let cfg_path = "configs/node1_config.json";
    let cfg_contents = std::fs::read_to_string(cfg_path).expect("Invalid file path");

    let config = Config::deserialize(&cfg_contents);
    let _chan_depth = config.rpc_config.channel_depth as usize;
    let key_store = KeyStore::new(&config.rpc_config.allowed_keylist_path, &config.rpc_config.signing_priv_key_path);

    let config = AtomicConfig::new(config);
    let keystore = AtomicKeyStore::new(key_store);
    let mut crypto = CryptoService::new(TEST_CRYPTO_NUM_TASKS, keystore.clone());
    crypto.run();

    let client = Client::new_atomic(config.clone(), keystore.clone(), false, 0);

    let storage_config = &config.get().consensus_config.log_storage_config;
    let (mut storage, storage_path) = match storage_config {
        rocksdb_config @ crate::config::StorageConfig::RocksDB(_inner) => {
            let _db = RocksDBStorageEngine::new(rocksdb_config.clone());
            (StorageService::new(_db, _chan_depth), _inner.db_path.clone())
        },
        crate::config::StorageConfig::FileStorage(_) => {
            panic!("File storage not supported!");
        },
    };

    
    let (batch_proposer_tx, batch_proposer_rx) = make_channel(_chan_depth);
    let (block_maker_tx, block_maker_rx) = make_channel(_chan_depth);
    let (control_command_tx, control_command_rx) = make_channel(_chan_depth);
    let (qc_tx, qc_rx) = make_channel(_chan_depth);
    let (block_broadcaster_tx, block_broadcaster_rx) = make_channel(_chan_depth);
    let (other_block_tx, other_block_rx) = make_channel(_chan_depth);
    let (client_reply_tx, mut client_reply_rx) = make_channel(_chan_depth);
    let (broadcaster_control_command_tx, broadcaster_control_command_rx) = make_channel(_chan_depth);
    let (staging_tx, mut staging_rx) = make_channel(_chan_depth);
    let (logserver_tx, mut logserver_rx) = make_channel(_chan_depth);
    
    let block_maker_crypto = crypto.get_connector();
    let block_broadcaster_crypto = crypto.get_connector();
    let block_broadcaster_storage = storage.get_connector(block_broadcaster_crypto);
    
    let batch_proposer = Arc::new(Mutex::new(BatchProposer::new(config.clone(), batch_proposer_rx, block_maker_tx)));
    let block_maker = Arc::new(Mutex::new(BlockSequencer::new(config.clone(), control_command_rx, block_maker_rx, qc_rx, block_broadcaster_tx, client_reply_tx, block_maker_crypto)));
    let block_broadcaster = Arc::new(Mutex::new(BlockBroadcaster::new(config.clone(), client.into(), block_broadcaster_rx, other_block_rx, broadcaster_control_command_rx, block_broadcaster_storage, staging_tx, logserver_tx)));
    
    let mut handles = JoinSet::new();
    
    handles.spawn(async move {
        storage.run().await;
    });

    handles.spawn(async move {
        BatchProposer::run(batch_proposer).await;
        println!("Batch proposer quits");
    });

    handles.spawn(async move {
        while let Some(_) = client_reply_rx.recv().await {
            // Sink
        }
    });

    handles.spawn(async move {
        while let Some(_) = logserver_rx.recv().await {
            // Sink
        }
    });


    handles.spawn(async move {
        BlockSequencer::run(block_maker).await;
        println!("Block maker quits");
    });

    handles.spawn(async move {
        BlockBroadcaster::run(block_broadcaster).await;
        println!("Block broadcaster quits");
    });


    for _ in 0..MAX_CLIENTS {
        let __tx = batch_proposer_tx.clone();
        handles.spawn(async move {
            load(__tx, TEST_RATE).await;
        });
    }

    let mut total_txs_output = 0;
    let mut last_block = 0;
    let mut underfull_batches = 0;
    let mut signed_blocks = 0;

    let start = Instant::now();
    while let Some((block, _storage_ack)) = staging_rx.recv().await {
        if block.block.n != last_block + 1 {
            panic!("Monotonicity broken!");
        }
        last_block += 1;

        let block_sz = match block.block.tx.unwrap() {
            crate::proto::consensus::proto_block::Tx::TxList(proto_transaction_list) => {
                proto_transaction_list.tx_list.len()
            },
            crate::proto::consensus::proto_block::Tx::TxListHash(vec) => {
                vec.len()
            },
        };

        if block_sz < config.get().consensus_config.max_backlog_batch_size {
            underfull_batches += 1;
        }

        total_txs_output += block_sz;

        if let Some(crate::proto::consensus::proto_block::Sig::ProposerSig(_)) = block.block.sig {
            signed_blocks += 1;
        }


        if total_txs_output >= MAX_CLIENTS * MAX_TXS {
            break;
        }
    }
    let total_time = start.elapsed().as_secs_f64();
    let throughput = total_txs_output as f64 / total_time;
    println!("Throughput: {} req/s", throughput);
    println!("Total blocks: {}", last_block);
    println!("Underfull blocks: {}", underfull_batches);
    println!("Signed blocks: {}", signed_blocks);

    if underfull_batches > 10 {
        panic!("Too many underfull batches");
    }

    let expected_signed_blocks = (last_block / config.get().consensus_config.signature_max_delay_blocks) as i32;

    if signed_blocks < expected_signed_blocks - 15 || signed_blocks > expected_signed_blocks + 15 {
        panic!("Too much or too few signed blocks");
    }


    handles.shutdown().await;
    std::fs::remove_dir_all(&storage_path).unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn test_staging() {
    // Generate configs first
    let cfg_path = "configs/node1_config.json";
    let cfg_contents = std::fs::read_to_string(cfg_path).expect("Invalid file path");

    let config = Config::deserialize(&cfg_contents);
    let _chan_depth = config.rpc_config.channel_depth as usize;
    let key_store = KeyStore::new(&config.rpc_config.allowed_keylist_path, &config.rpc_config.signing_priv_key_path);

    let config = AtomicConfig::new(config);
    let keystore = AtomicKeyStore::new(key_store);
    let mut crypto = CryptoService::new(TEST_CRYPTO_NUM_TASKS, keystore.clone());
    crypto.run();

    let client = Client::new_atomic(config.clone(), keystore.clone(), false, 0);
    let staging_client = Client::new_atomic(config.clone(), keystore.clone(), false, 0);

    let storage_config = &config.get().consensus_config.log_storage_config;
    let (mut storage, storage_path) = match storage_config {
        rocksdb_config @ crate::config::StorageConfig::RocksDB(_inner) => {
            let _db = RocksDBStorageEngine::new(rocksdb_config.clone());
            (StorageService::new(_db, _chan_depth), _inner.db_path.clone())
        },
        crate::config::StorageConfig::FileStorage(_) => {
            panic!("File storage not supported!");
        },
    };

    
    let (batch_proposer_tx, batch_proposer_rx) = make_channel(_chan_depth);
    let (block_maker_tx, block_maker_rx) = make_channel(_chan_depth);
    let (control_command_tx, control_command_rx) = make_channel(_chan_depth);
    let (qc_tx, qc_rx) = make_channel(_chan_depth);
    let (block_broadcaster_tx, block_broadcaster_rx) = make_channel(_chan_depth);
    let (other_block_tx, other_block_rx) = make_channel(_chan_depth);
    let (client_reply_tx, mut client_reply_rx) = make_channel(_chan_depth);
    let (client_reply_command_tx, mut client_reply_command_rx) = make_channel(_chan_depth);
    let (broadcaster_control_command_tx, broadcaster_control_command_rx) = make_channel(_chan_depth);
    let (staging_tx, staging_rx) = make_channel(_chan_depth);
    let (logserver_tx, mut logserver_rx) = make_channel(_chan_depth);
    let (vote_tx, vote_rx) = make_channel(_chan_depth);
    let (view_change_tx, view_change_rx) = make_channel(_chan_depth);
    let (app_tx, mut app_rx) = make_channel(_chan_depth);

    let block_maker_crypto = crypto.get_connector();
    let block_broadcaster_crypto = crypto.get_connector();
    let block_broadcaster_storage = storage.get_connector(block_broadcaster_crypto);
    let staging_crypto = crypto.get_connector();
    
    let batch_proposer = Arc::new(Mutex::new(BatchProposer::new(config.clone(), batch_proposer_rx, block_maker_tx)));
    let block_maker = Arc::new(Mutex::new(BlockSequencer::new(config.clone(), control_command_rx, block_maker_rx, qc_rx, block_broadcaster_tx, client_reply_tx, block_maker_crypto)));
    let block_broadcaster = Arc::new(Mutex::new(BlockBroadcaster::new(config.clone(), client.into(), block_broadcaster_rx, other_block_rx, broadcaster_control_command_rx, block_broadcaster_storage, staging_tx, logserver_tx)));
    let staging = Arc::new(Mutex::new(Staging::new(config.clone(), staging_client.into(), staging_crypto, staging_rx, vote_rx, view_change_rx, client_reply_command_tx, app_tx, broadcaster_control_command_tx, control_command_tx, qc_tx)));
    let mut handles = JoinSet::new();
    
    handles.spawn(async move {
        storage.run().await;
    });

    handles.spawn(async move {
        BatchProposer::run(batch_proposer).await;
        println!("Batch proposer quits");
    });

    handles.spawn(async move {
        while let Some(_) = client_reply_rx.recv().await {
            // Sink
        }
    });

    handles.spawn(async move {
        while let Some(_) = client_reply_command_rx.recv().await {
            // Sink
        }
    });

    handles.spawn(async move {
        while let Some(_) = logserver_rx.recv().await {
            // Sink
        }
    });


    handles.spawn(async move {
        BlockSequencer::run(block_maker).await;
        println!("Block maker quits");
    });

    handles.spawn(async move {
        BlockBroadcaster::run(block_broadcaster).await;
        println!("Block broadcaster quits");
    });

    handles.spawn(async move {
        Staging::run(staging).await;
        println!("Staging quits");
    });


    for _ in 0..MAX_CLIENTS {
        let __tx = batch_proposer_tx.clone();
        handles.spawn(async move {
            load(__tx, TEST_RATE).await;
        });
    }

    let mut total_txs_output = 0;
    let mut last_block = 0;
    let mut underfull_batches = 0;
    let mut signed_blocks = 0;

    let start = Instant::now();
    'main: while let Some(cmd) = app_rx.recv().await {
        match cmd {
            crate::consensus_v2::staging::AppCommand::CrashCommit(ci, committed_blocks) => {
                if committed_blocks.last().unwrap().block.n != ci {
                    panic!("Last block should have same n as ci");
                }
                
                for block in committed_blocks {
                    if block.block.n != last_block + 1 {
                        panic!("Monotonicity broken!");
                    }
                    last_block += 1;
            
                    let block_sz = match block.block.tx.unwrap() {
                        crate::proto::consensus::proto_block::Tx::TxList(proto_transaction_list) => {
                            proto_transaction_list.tx_list.len()
                        },
                        crate::proto::consensus::proto_block::Tx::TxListHash(vec) => {
                            vec.len()
                        },
                    };
            
                    if block_sz < config.get().consensus_config.max_backlog_batch_size {
                        underfull_batches += 1;
                    }
            
                    total_txs_output += block_sz;
            
                    if let Some(crate::proto::consensus::proto_block::Sig::ProposerSig(_)) = block.block.sig {
                        signed_blocks += 1;
                    }
            
            
                    if total_txs_output >= MAX_CLIENTS * MAX_TXS {
                        break 'main;
                    }
                }
            },
        }
    }
    let total_time = start.elapsed().as_secs_f64();
    let throughput = total_txs_output as f64 / total_time;
    println!("Throughput: {} req/s", throughput);
    println!("Total blocks: {}", last_block);
    println!("Underfull blocks: {}", underfull_batches);
    println!("Signed blocks: {}", signed_blocks);

    if underfull_batches > 10 {
        panic!("Too many underfull batches");
    }

    let expected_signed_blocks = (last_block / config.get().consensus_config.signature_max_delay_blocks) as i32;

    if signed_blocks < expected_signed_blocks - 15 || signed_blocks > expected_signed_blocks + 15 {
        panic!("Too much or too few signed blocks");
    }

    println!("Errors appearing after this are inconsequential");
    handles.shutdown().await;

    std::fs::remove_dir_all(&storage_path).unwrap();
}
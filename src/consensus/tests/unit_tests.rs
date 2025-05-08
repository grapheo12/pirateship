use std::{sync::{atomic::AtomicUsize, Arc}, time::{Duration, Instant}};
use core_affinity::CoreId;
use tokio::{sync::{mpsc::unbounded_channel, Mutex}, task::JoinSet};
use crate::{consensus_v2::{app::Application, block_broadcaster::BlockBroadcaster, engines::null_app::NullApp, staging::Staging}, rpc::{client::Client, SenderType}, utils::{channel::{make_channel, Receiver, Sender}, RocksDBStorageEngine, StorageService}};
use crate::{config::{AtomicConfig, Config}, consensus_v2::{batch_proposal::BatchProposer, block_sequencer::BlockSequencer}, crypto::{AtomicKeyStore, CryptoService, KeyStore}, proto::execution::{ProtoTransaction, ProtoTransactionOp, ProtoTransactionPhase}};

use super::super::batch_proposal::{MsgAckChanWithTag, TxWithAckChanTag};

const TEST_CRYPTO_NUM_TASKS: usize = 3;
const MAX_TXS: usize = 5_00_000;
const TEST_RATE: f64 = 300_000.0;
const PAYLOAD_SIZE: usize = 512;

macro_rules! pinned_runtime {
    ($fn_name: expr) => {
        // Generate configs first
        let cfg_path = "configs/node1_config.json";
        let cfg_contents = std::fs::read_to_string(cfg_path).expect("Invalid file path");

        let config = Config::deserialize(&cfg_contents);
        let _chan_depth = config.rpc_config.channel_depth as usize;
        
        colog::init();
        let num_cores = num_cpus::get();
        let id = Arc::new(std::sync::Mutex::new(AtomicUsize::new(0)));
        let (batch_proposer_tx, batch_proposer_rx) = make_channel(_chan_depth);

        let num_consensus_cores = num_cores / 2;
        let num_client_cores = num_cores - num_consensus_cores;

        let id1 = id.clone();
        let client_rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(num_client_cores)
            .enable_all()
            .on_thread_start(move || {
                let id = id1.lock().unwrap();
                let _ = core_affinity::set_for_current(CoreId{ id: id.fetch_add(1, std::sync::atomic::Ordering::SeqCst) % num_cores});
            })
            .build()
            .unwrap();

        // Pin thread to core
        let consensus_rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(num_consensus_cores)
            .enable_all()
            .on_thread_start(move || {
                let id = id.lock().unwrap();
                let _ = core_affinity::set_for_current(CoreId{ id: id.fetch_add(1, std::sync::atomic::Ordering::SeqCst) % num_cores});
            })
            .build()
            .unwrap();

        client_rt.spawn(client_runner(batch_proposer_tx, num_client_cores));

        consensus_rt.block_on($fn_name(config, batch_proposer_rx, num_client_cores));
    };
}

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

        let ack_chan_with_tag: MsgAckChanWithTag = (tx, tag as u64, SenderType::Auth("client".to_string(), 0));

        batch_proposer_tx.send((Some(transaction.clone()), ack_chan_with_tag)).await.unwrap();

        last_fire_time = Instant::now();
    }

    let total_time = start.elapsed().as_secs_f64();
    let input_rate = MAX_TXS as f64 / total_time;

    println!("Input rate: {} req/s", input_rate);
}

async fn client_runner(batch_proposer_tx: Sender<TxWithAckChanTag>, num_clients: usize) {
    for _ in 0..num_clients {
        let __tx = batch_proposer_tx.clone();
        tokio::spawn(async move {
            load(__tx, TEST_RATE).await;
        });
    }

    let __tx = batch_proposer_tx.clone();
    tokio::spawn(async move {
        __tx.send((None, (tokio::sync::mpsc::channel(1).0, 0, SenderType::Auth("client".to_string(), 0)))).await.unwrap();
        // This is just so we don't drop the channel prematurely.
        loop {
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    });

}

#[test]
fn test_batch_proposal() {
    pinned_runtime!(_test_batch_proposal);
}

async fn _test_batch_proposal(config: Config, batch_proposer_rx: Receiver<TxWithAckChanTag>, num_clients: usize) {
    let _chan_depth = config.rpc_config.channel_depth as usize;
    let key_store = KeyStore::new(&config.rpc_config.allowed_keylist_path, &config.rpc_config.signing_priv_key_path);

    let config = AtomicConfig::new(config);
    let keystore = AtomicKeyStore::new(key_store);
    println!("num tasks: {}", TEST_CRYPTO_NUM_TASKS);
    let mut crypto = CryptoService::new(TEST_CRYPTO_NUM_TASKS, keystore.clone(), config.clone());
    crypto.run();

    let (block_maker_tx, mut block_maker_rx) = make_channel(_chan_depth);
    let (app_tx, mut app_rx) = make_channel(_chan_depth);
    let batch_proposer = Arc::new(Mutex::new(BatchProposer::new(config.clone(), batch_proposer_rx, block_maker_tx, app_tx)));
    // let block_maker = Arc::new(Mutex::new(BlockMaker::new(config.clone(), control_command_rx, block_maker_rx, qc_rx, block_broadcaster_tx, client_reply_tx, block_maker_crypto)));

    let mut handles = JoinSet::new();

    handles.spawn(async move {
        BatchProposer::run(batch_proposer).await;
        println!("Batch proposer quits");
    });

    handles.spawn(async move {
        while let Some(_) = app_rx.recv().await {
            // Sink
        }
    });

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

        if total_txs_output >= num_clients * MAX_TXS {
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

#[test]
fn test_block_sequencer() {
    pinned_runtime!(_test_block_sequencer);
}

async fn _test_block_sequencer(config: Config, batch_proposer_rx: Receiver<TxWithAckChanTag>, num_clients: usize) {
    // Generate configs first
    let cfg_path = "configs/node1_config.json";
    let cfg_contents = std::fs::read_to_string(cfg_path).expect("Invalid file path");

    let config = Config::deserialize(&cfg_contents);
    let _chan_depth = config.rpc_config.channel_depth as usize;
    let key_store = KeyStore::new(&config.rpc_config.allowed_keylist_path, &config.rpc_config.signing_priv_key_path);

    let config = AtomicConfig::new(config);
    let keystore = AtomicKeyStore::new(key_store);
    println!("num tasks: {}", TEST_CRYPTO_NUM_TASKS);

    let mut crypto = CryptoService::new(TEST_CRYPTO_NUM_TASKS, keystore.clone(), config.clone());
    crypto.run();

    let (block_maker_tx, block_maker_rx) = make_channel(_chan_depth);
    let (control_command_tx, control_command_rx) = make_channel(_chan_depth);
    let (qc_tx, qc_rx) = unbounded_channel();
    let (block_broadcaster_tx, mut block_broadcaster_rx) = make_channel(_chan_depth);
    let (client_reply_tx, mut client_reply_rx) = make_channel(_chan_depth);
    let (app_tx, mut app_rx) = make_channel(_chan_depth);
    let block_maker_crypto = crypto.get_connector();

    let batch_proposer = Arc::new(Mutex::new(BatchProposer::new(config.clone(), batch_proposer_rx, block_maker_tx, app_tx)));
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
        while let Some(_) = app_rx.recv().await {
            // Sink
        }
    });


    handles.spawn(async move {
        BlockSequencer::run(block_maker).await;
        println!("Block maker quits");
    });

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

        let block_sz = block.block.tx_list.len();

        if block_sz < config.get().consensus_config.max_backlog_batch_size {
            underfull_batches += 1;
        }

        total_txs_output += block_sz;

        if let Some(crate::proto::consensus::proto_block::Sig::ProposerSig(_)) = block.block.sig {
            signed_blocks += 1;
        }


        if total_txs_output >= num_clients * MAX_TXS {
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


#[test]
fn test_block_broadcaster() {
    pinned_runtime!(_test_block_broadcaster);
}

async fn _test_block_broadcaster(config: Config, batch_proposer_rx: Receiver<TxWithAckChanTag>, num_clients: usize) {
    // Generate configs first
    let cfg_path = "configs/node1_config.json";
    let cfg_contents = std::fs::read_to_string(cfg_path).expect("Invalid file path");

    let config = Config::deserialize(&cfg_contents);
    let _chan_depth = config.rpc_config.channel_depth as usize;
    let key_store = KeyStore::new(&config.rpc_config.allowed_keylist_path, &config.rpc_config.signing_priv_key_path);

    let config = AtomicConfig::new(config);
    let keystore = AtomicKeyStore::new(key_store);
    let mut crypto = CryptoService::new(TEST_CRYPTO_NUM_TASKS, keystore.clone(), config.clone());
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

    
    let (block_maker_tx, block_maker_rx) = make_channel(_chan_depth);
    let (control_command_tx, control_command_rx) = make_channel(_chan_depth);
    let (qc_tx, qc_rx) = unbounded_channel();
    let (block_broadcaster_tx, block_broadcaster_rx) = make_channel(_chan_depth);
    let (other_block_tx, other_block_rx) = make_channel(_chan_depth);
    let (client_reply_tx, mut client_reply_rx) = make_channel(_chan_depth);
    let (broadcaster_control_command_tx, broadcaster_control_command_rx) = make_channel(_chan_depth);
    let (staging_tx, mut staging_rx) = make_channel(_chan_depth);
    let (logserver_tx, mut logserver_rx) = make_channel(_chan_depth);
    let (fork_receiver_command_tx, mut fork_receiver_command_rx) = make_channel(_chan_depth);
    let (app_tx, mut app_rx) = make_channel(_chan_depth);

    let block_maker_crypto = crypto.get_connector();
    let block_broadcaster_crypto = crypto.get_connector();
    let block_broadcaster_storage = storage.get_connector(block_broadcaster_crypto);
    
    let batch_proposer = Arc::new(Mutex::new(BatchProposer::new(config.clone(), batch_proposer_rx, block_maker_tx, app_tx.clone())));
    let block_maker = Arc::new(Mutex::new(BlockSequencer::new(config.clone(), control_command_rx, block_maker_rx, qc_rx, block_broadcaster_tx, client_reply_tx, block_maker_crypto)));
    let block_broadcaster = Arc::new(Mutex::new(BlockBroadcaster::new(config.clone(), client.into(), block_broadcaster_rx, other_block_rx, broadcaster_control_command_rx, block_broadcaster_storage, staging_tx, logserver_tx, fork_receiver_command_tx, app_tx)));
    
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
        while let Some(_) = fork_receiver_command_rx.recv().await {
            // Sink
        }
    });

    handles.spawn(async move {
        while let Some(_) = app_rx.recv().await {
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

    let mut total_txs_output = 0;
    let mut last_block = 0;
    let mut underfull_batches = 0;
    let mut signed_blocks = 0;

    let start = Instant::now();
    while let Some((block, _storage_ack, _)) = staging_rx.recv().await {
        if block.block.n != last_block + 1 {
            panic!("Monotonicity broken!");
        }

        last_block += 1;

        let block_sz = block.block.tx_list.len();

        if block_sz < config.get().consensus_config.max_backlog_batch_size {
            underfull_batches += 1;
        }

        total_txs_output += block_sz;

        if let Some(crate::proto::consensus::proto_block::Sig::ProposerSig(_)) = block.block.sig {
            signed_blocks += 1;
        }


        if total_txs_output >= num_clients * MAX_TXS {
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

#[test]
fn test_staging() {
    pinned_runtime!(_test_staging);
}
async fn _test_staging(config: Config, batch_proposer_rx: Receiver<TxWithAckChanTag>, num_clients: usize) {
    // Generate configs first
    let cfg_path = "configs/node1_config.json";
    let cfg_contents = std::fs::read_to_string(cfg_path).expect("Invalid file path");

    let config = Config::deserialize(&cfg_contents);
    let _chan_depth = config.rpc_config.channel_depth as usize;
    let key_store = KeyStore::new(&config.rpc_config.allowed_keylist_path, &config.rpc_config.signing_priv_key_path);

    let config = AtomicConfig::new(config);
    let keystore = AtomicKeyStore::new(key_store);
    let mut crypto = CryptoService::new(TEST_CRYPTO_NUM_TASKS, keystore.clone(), config.clone());
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

    
    let (block_maker_tx, block_maker_rx) = make_channel(_chan_depth);
    let (control_command_tx, control_command_rx) = make_channel(_chan_depth);
    let (qc_tx, qc_rx) = unbounded_channel();
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
    let (fork_receiver_command_tx, mut fork_receiver_command_rx) = make_channel(_chan_depth);

    let block_maker_crypto = crypto.get_connector();
    let block_broadcaster_crypto = crypto.get_connector();
    let block_broadcaster_storage = storage.get_connector(block_broadcaster_crypto);
    let staging_crypto = crypto.get_connector();
    
    let batch_proposer = Arc::new(Mutex::new(BatchProposer::new(config.clone(), batch_proposer_rx, block_maker_tx, app_tx.clone())));
    let block_maker = Arc::new(Mutex::new(BlockSequencer::new(config.clone(), control_command_rx, block_maker_rx, qc_rx, block_broadcaster_tx, client_reply_tx, block_maker_crypto)));
    let block_broadcaster = Arc::new(Mutex::new(BlockBroadcaster::new(config.clone(), client.into(), block_broadcaster_rx, other_block_rx, broadcaster_control_command_rx, block_broadcaster_storage, staging_tx, logserver_tx, fork_receiver_command_tx.clone(), app_tx.clone())));
    let staging = Arc::new(Mutex::new(Staging::new(config.clone(), staging_client.into(), staging_crypto, staging_rx, vote_rx, view_change_rx, client_reply_command_tx, app_tx, broadcaster_control_command_tx, control_command_tx, fork_receiver_command_tx, qc_tx)));
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
        while let Some(_) = fork_receiver_command_rx.recv().await {
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


    let mut total_txs_output = 0;
    let mut total_byz_commit = 0;
    let mut last_block = 0;
    let mut underfull_batches = 0;
    let mut signed_blocks = 0;

    let start = Instant::now();
    'main: while let Some(cmd) = app_rx.recv().await {
        match cmd {
            crate::consensus_v2::app::AppCommand::CrashCommit(committed_blocks) => {
                for block in committed_blocks {
                    // println!("Block: {}", block.block.n);
                    if block.block.n != last_block + 1 {
                        panic!("Monotonicity broken!");
                    }
                    last_block += 1;
            
                    let block_sz = block.block.tx_list.len();
            
                    if block_sz < config.get().consensus_config.max_backlog_batch_size {
                        underfull_batches += 1;
                    }
            
                    total_txs_output += block_sz;
            
                    if let Some(crate::proto::consensus::proto_block::Sig::ProposerSig(_)) = block.block.sig {
                        signed_blocks += 1;
                    }
            
            
                    if total_txs_output >= num_clients * MAX_TXS {
                        break 'main;
                    }
                }
            },

            crate::consensus_v2::app::AppCommand::ByzCommit(committed_blocks) => {
                // println!("Byz commit batch size: {}", committed_blocks.len());
                total_byz_commit += committed_blocks.iter()
                    .map(|block| block.block.tx_list.len())
                    .sum::<usize>();
            },

            _ => {

            }
        }
    }
    let total_time = start.elapsed().as_secs_f64();
    let throughput = total_txs_output as f64 / total_time;
    let byz_throughput = total_byz_commit as f64 / total_time;
    println!("Crash Throughput: {} req/s", throughput);
    println!("Byz Throughput: {} req/s", byz_throughput);
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


#[test]
fn test_null_app() {
    pinned_runtime!(_test_null_app);
}


async fn _test_null_app(config: Config, batch_proposer_rx: Receiver<TxWithAckChanTag>, num_clients: usize) {
    // Generate configs first
    let cfg_path = "configs/node1_config.json";
    let cfg_contents = std::fs::read_to_string(cfg_path).expect("Invalid file path");

    let config = Config::deserialize(&cfg_contents);
    let _chan_depth = config.rpc_config.channel_depth as usize;
    let key_store = KeyStore::new(&config.rpc_config.allowed_keylist_path, &config.rpc_config.signing_priv_key_path);

    let config = AtomicConfig::new(config);
    let keystore = AtomicKeyStore::new(key_store);
    let mut crypto = CryptoService::new(TEST_CRYPTO_NUM_TASKS, keystore.clone(), config.clone());
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


    let (block_maker_tx, block_maker_rx) = make_channel(_chan_depth);
    let (control_command_tx, control_command_rx) = make_channel(_chan_depth);
    let (qc_tx, qc_rx) = unbounded_channel();
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
    let (fork_receiver_command_tx, mut fork_receiver_command_rx) = make_channel(_chan_depth);
    let (unlogged_tx, unlogged_rx) = make_channel(_chan_depth);

    let block_maker_crypto = crypto.get_connector();
    let block_broadcaster_crypto = crypto.get_connector();
    let block_broadcaster_storage = storage.get_connector(block_broadcaster_crypto);
    let staging_crypto = crypto.get_connector();

    let batch_proposer = Arc::new(Mutex::new(BatchProposer::new(config.clone(), batch_proposer_rx, block_maker_tx, app_tx.clone())));
    let block_maker = Arc::new(Mutex::new(BlockSequencer::new(config.clone(), control_command_rx, block_maker_rx, qc_rx, block_broadcaster_tx, client_reply_tx, block_maker_crypto)));
    let block_broadcaster = Arc::new(Mutex::new(BlockBroadcaster::new(config.clone(), client.into(), block_broadcaster_rx, other_block_rx, broadcaster_control_command_rx, block_broadcaster_storage, staging_tx, logserver_tx, fork_receiver_command_tx.clone(), app_tx.clone())));
    let staging = Arc::new(Mutex::new(Staging::new(config.clone(), staging_client.into(), staging_crypto, staging_rx, vote_rx, view_change_rx, client_reply_command_tx.clone(), app_tx, broadcaster_control_command_tx, control_command_tx, fork_receiver_command_tx, qc_tx)));
    let app = Arc::new(Mutex::new(Application::<NullApp>::new(config.clone(), app_rx, unlogged_rx, client_reply_command_tx)));
    
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
        while let Some(_) = fork_receiver_command_rx.recv().await {
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

    handles.spawn(async move {
        Application::<NullApp>::run(app).await;
        println!("Application quits");
    });

    let mut total_txs_output = 0;
    let mut total_byz_commit = 0;
    let mut last_block = 0;
    let mut underfull_batches = 0;

    let start = Instant::now();
    'main: while let Some(cmd) = client_reply_command_rx.recv().await {
        match cmd {
            crate::consensus_v2::client_reply::ClientReplyCommand::CancelAllRequests => {},
            crate::consensus_v2::client_reply::ClientReplyCommand::CrashCommitAck(mut committed_block_results) => {
                
                for (_, (n, block_results)) in committed_block_results.drain() {
                    // println!("Block: {}", block.block.n);
                    if n != last_block + 1 {
                        panic!("Monotonicity broken! {} {}", n, last_block);
                    }
                    last_block += 1;

                    let block_sz = block_results.len();
            
                    if block_sz < config.get().consensus_config.max_backlog_batch_size {
                        underfull_batches += 1;
                    }
            
                    total_txs_output += block_sz;
            
                    if total_txs_output >= num_clients * MAX_TXS {
                        break 'main;
                    }
                }
            },
            crate::consensus_v2::client_reply::ClientReplyCommand::ByzCommitAck(committed_block_results) => {
                total_byz_commit += committed_block_results.iter()
                    .map(|(_, (_, results))| results.len())
                    .sum::<usize>();
            },
        }
    }
    let total_time = start.elapsed().as_secs_f64();
    let throughput = total_txs_output as f64 / total_time;
    let byz_throughput = total_byz_commit as f64 / total_time;
    println!("Crash Throughput: {} req/s", throughput);
    println!("Byz Throughput: {} req/s", byz_throughput);
    println!("Total blocks: {}", last_block);
    println!("Underfull blocks: {}", underfull_batches);

    if underfull_batches > 10 {
        panic!("Too many underfull batches");
    }

    println!("Errors appearing after this are inconsequential");
    std::fs::remove_dir_all(&storage_path).unwrap();
}


#[tokio::test]
async fn test_logserver() {
    // TODO
}
use pft::{config::Config, consensus};
use tokio::runtime;
use std::{env, fs, io, path, sync::{atomic::AtomicUsize, Arc, Mutex}};
use std::io::Write;

#[global_allocator]
static ALLOC: snmalloc_rs::SnMalloc = snmalloc_rs::SnMalloc;

/// Fetch json config file from command line path.
/// Panic if not found or parsed properly.
fn process_args() -> Config {
    macro_rules! usage_str {
        () => {
            "\x1b[31;1mUsage: {} path/to/config.json\x1b[0m"
        };
    }

    let args: Vec<_> = env::args().collect();

    if args.len() != 2 {
        panic!(usage_str!(), args[0]);
    }

    let cfg_path = path::Path::new(args[1].as_str());
    if !cfg_path.exists() {
        panic!(usage_str!(), args[0]);
    }

    let cfg_contents = fs::read_to_string(cfg_path).expect("Invalid file path");

    Config::deserialize(&cfg_contents)
}

async fn run_main(cfg: Config) -> io::Result<()> {
    let node = Arc::new(consensus::ConsensusNode::new(&cfg));
    let mut handles = consensus::ConsensusNode::run(node);

    while let Some(_) = handles.join_next().await {

    }
    Ok(())
}

const NUM_THREADS: usize = 32;

fn main() {
    colog::init();
    let cfg = process_args();

    let core_ids = 
        Arc::new(Mutex::new(Box::pin(core_affinity::get_core_ids().unwrap())));

    let start_idx = cfg.consensus_config.node_list.iter().position(|r| r.eq(&cfg.net_config.name)).unwrap();
    let mut num_threads = NUM_THREADS;
    {
        let _num_cores = core_ids.lock().unwrap().len();
        if _num_cores < num_threads {
            num_threads = _num_cores;
        }
    }

    let start_idx = start_idx * num_threads;
    
    let i = Box::pin(AtomicUsize::new(0));
    let runtime = runtime::Builder::new_multi_thread()
        .enable_all()
        .worker_threads(NUM_THREADS)
        .on_thread_start(move || {
            let _cids = core_ids.clone();
            let lcores = _cids.lock().unwrap();
            let id = (start_idx + i.fetch_add(1, std::sync::atomic::Ordering::SeqCst)) % lcores.len();
            // let res = core_affinity::set_for_current(lcores[id]);
            
            // if res {
            //     println!("Thread pinned to core {:?}", id);
            // }else{
            //     println!("Thread pinning to core {:?} failed", id);
            // }

            std::io::stdout().flush()
                .unwrap();
        })
        .build()
        .unwrap();

    let _ = runtime.block_on(run_main(cfg));
}

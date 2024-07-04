pub mod config;
use config::Config;
use std::{env, fs, path};


// Fetch json config file from command line path.
// Panic if not found or parsed properly.
fn process_args() -> Config {
    macro_rules! usage_str {() => ("\x1b[31;1mUsage: {} path/to/config.json\x1b[0m");}

    let args: Vec<_> = env::args().collect();

    if args.len() != 2 {
        panic!(usage_str!(), args[0]);
    }

    let cfg_path = path::Path::new(args[1].as_str());
    if !cfg_path.exists() {
        panic!(usage_str!(), args[0]);
    }

    let cfg_contents = fs::read_to_string(cfg_path)
        .expect("Invalid file path");

    Config::deserialize(&cfg_contents)
}
fn main() {
    let config = process_args();
    println!("{}", config.serialize());
}

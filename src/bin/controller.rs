use std::str::FromStr;

use clap::{arg, command, Arg, ArgAction};
use pft::consensus::reconfiguration::LearnerInfo;

#[derive(Debug)]
struct Config {
    client_config_path: String,
    add_learner: Vec<LearnerInfo>,
    del_learner: Vec<String>,
    upgrade_fullnode: Vec<String>,
    downgrade_fullnode: Vec<String>,
}

fn parse_args() -> Config {
    let matches = command!()
        .arg(arg!(<client_config_path> "Path to client config"))
        .arg(Arg::new("add_learner")
            .long("add_learner")
            .action(ArgAction::Append)
            .value_parser(LearnerInfo::from_str)
            .help("Add learners with learner_info of the form 'name addr domain pub_key'")
            .required(false)
        )
        .arg(Arg::new("del_learner")
            .long("del_learner")
            .action(ArgAction::Append)
            .help("Delete learners with given names")
            .required(false)
        )
        .arg(Arg::new("upgrade_fullnode")
            .long("upgrade_fullnode")
            .action(ArgAction::Append)
            .help("Upgrade to full nodes given names")
            .required(false)
        )
        .arg(Arg::new("downgrade_fullnode")
            .long("downgrade_fullnode")
            .action(ArgAction::Append)
            .help("Downgrade to learners given names")
            .required(false)
        )
        .get_matches();

    let add_learner = matches.get_many::<LearnerInfo>("add_learner")
        .unwrap_or_default().cloned().collect::<Vec<_>>();
    let del_learner = matches.get_many::<String>("del_learner")
        .unwrap_or_default().cloned().collect::<Vec<_>>();
    let upgrade_fullnode = matches.get_many::<String>("upgrade_fullnode")
        .unwrap_or_default().cloned().collect::<Vec<_>>();
    let downgrade_fullnode = matches.get_many::<String>("downgrade_fullnode")
        .unwrap_or_default().cloned().collect::<Vec<_>>();

    Config {
        client_config_path: matches.get_one::<String>("client_config_path").unwrap().to_string(),
        add_learner,
        del_learner,
        upgrade_fullnode,
        downgrade_fullnode
    }

}

fn main() {
    let cfg = parse_args();
    println!("Hello, world! {:#?}", cfg);
}
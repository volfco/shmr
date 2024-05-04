use std::collections::HashMap;
use std::io::ErrorKind;
// Goal. Create enough of a FUSE filesystem to be able to list the contents of a directory, show that hello.txt exists, and read the contents of hello.txt.
//       this can all be virtual. doesn't need to actually touch the filesystem.
use clap::Parser;
use fuser::MountOption;
use log::{error, LevelFilter};
use serde::{Deserialize, Serialize};
use shmr::fuse::Shmr;
use shmr::storage::Engine;
use std::path::PathBuf;

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    #[arg(short, long)]
    verbosity: u8,

    #[arg(short, long)]
    config: PathBuf,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct FuseConfig {
    mount_dir: PathBuf,
    pools: HashMap<String, HashMap<String, PathBuf>>,
    metadata_dir: PathBuf,
    write_pool: String,
}

fn main() {
    let args = Args::parse();

    let log_level = match args.verbosity {
        0 => LevelFilter::Error,
        1 => LevelFilter::Warn,
        2 => LevelFilter::Info,
        3 => LevelFilter::Debug,
        _ => LevelFilter::Trace,
    };
    env_logger::builder()
        .format_timestamp_nanos()
        .filter_level(log_level)
        .init();

    // load config file
    let config = std::fs::read_to_string(&args.config).expect("could not read config file");
    let config: FuseConfig = serde_yaml::from_str(&config).expect("could not parse config file");

    let options = vec![MountOption::FSName("fuser".to_string())];

    let mount = config.mount_dir.clone();
    //
    // // check if there is already something mounted at the mount point
    let engine = Engine::new(config.write_pool.clone(), config.pools.clone());
    let fs = Shmr::open(config.metadata_dir, engine).unwrap();
    let result = fuser::mount2(fs, mount, &options);
    if let Err(e) = result {
        // Return a special error code for permission denied, which usually indicates that
        // "user_allow_other" is missing from /etc/fuse.conf
        if e.kind() == ErrorKind::PermissionDenied {
            error!("{}", e.to_string());
            std::process::exit(2);
        }
    }
    //
    // // TODO unmount the filesystem when the program exits
}

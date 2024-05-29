use clap::Parser;
use fuser::MountOption;
use log::{error, LevelFilter};
use shmr2::config::ShmrFsConfig;
use shmr2::ShmrFs;
use std::io::ErrorKind;
use std::path::PathBuf;

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    #[arg(short, long, default_value_t = 1)]
    verbosity: u8,

    #[arg(short, long)]
    config: PathBuf,
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
        .filter(Some("shmr"), log_level)
        .filter(Some("fuser"), log_level)
        // .filter_level(log_level)
        .init();

    let config = std::fs::read_to_string(&args.config).expect("could not read config file");
    let config: ShmrFsConfig = serde_yaml::from_str(&config).expect("could not parse config file");

    let options = vec![MountOption::FSName("fuser".to_string())];

    let mount = config.mount_dir.clone();
    let fs = ShmrFs::new(config).unwrap();
    let result = fuser::mount2(fs, mount, &options);
    if let Err(e) = result {
        // Return a special error code for permission denied, which usually indicates that
        // "user_allow_other" is missing from /etc/fuse.conf
        if e.kind() == ErrorKind::PermissionDenied {
            error!("{}", e.to_string());
            std::process::exit(2);
        }
    }
}

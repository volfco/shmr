use std::io::ErrorKind;
// Goal. Create enough of a FUSE filesystem to be able to list the contents of a directory, show that hello.txt exists, and read the contents of hello.txt.
//       this can all be virtual. doesn't need to actually touch the filesystem.
use clap::Parser;
use std::path::PathBuf;
use fuser::MountOption;
use log::{error, LevelFilter};
use serde::{Serialize, Deserialize};
use shmr::fsdb::FsDB;
use shmr::fuse::Shmr;

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
  #[arg(short, long)]
  verbosity: u8,

  #[arg(short, long)]
  config: PathBuf,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PoolConfig {
  // TODO For now, we're going to assume everything is just a single path
  /// Pool Name
  name: String,
  /// Pool Paths
  paths: Vec<PathBuf>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
struct FuseConfig {
  mount_dir: PathBuf,
  pools: Vec<PoolConfig>,
  metadata_dir: PathBuf,
  write_pool: String
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
  //
  let fs = Shmr {
    fs_db: FsDB::open(&config.metadata_dir).unwrap(),
  };
  let result = fuser::mount2(
    fs,
    mount,
    &options,
  );
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

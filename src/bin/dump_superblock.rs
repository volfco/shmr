// use std::io::ErrorKind;
// // Goal. Create enough of a FUSE filesystem to be able to list the contents of a directory, show that hello.txt exists, and read the contents of hello.txt.
// //       this can all be virtual. doesn't need to actually touch the filesystem.
// use clap::Parser;
// use std::path::PathBuf;
// use fuser::MountOption;
// use log::{error, LevelFilter};
// use shmr::{ConfigStub, ShmrFilesystem};
//
// #[derive(Parser, Debug)]
// #[command(version, about, long_about = None)]
// struct Args {
//   #[arg(short, long)]
//   verbosity: u8,
//
//   #[arg(short, long)]
//   config: PathBuf,
// }
//
// fn main() {
//   let args = Args::parse();
//
//   let log_level = match args.verbosity {
//     0 => LevelFilter::Error,
//     1 => LevelFilter::Warn,
//     2 => LevelFilter::Info,
//     3 => LevelFilter::Debug,
//     _ => LevelFilter::Trace,
//   };
//   env_logger::builder()
//     .format_timestamp_nanos()
//     .filter_level(log_level)
//     .init();
//
//   // load config file
//   let config = std::fs::read_to_string(&args.config).expect("could not read config file");
//   let config: ConfigStub = serde_yaml::from_str(&config).expect("could not parse config file");
//
//   let mut options = vec![MountOption::FSName("fuser".to_string())];
//
//   let mount = config.mount.clone();
//
//   let fs = ShmrFilesystem::init(config).unwrap();
// }
fn main() {}

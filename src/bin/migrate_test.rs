// migrate_test --inode INODE NUM --topology 3,2 --pool POOL_NAME

use std::collections::HashMap;
use std::mem;
use std::path::PathBuf;
use clap::Parser;
use log::{info, warn};
use serde::{Deserialize, Serialize};
use shmr::fsdb::FsDB;
use shmr::fuse::InodeDescriptor;
use shmr::storage::StorageBlock;

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
  #[arg(short, long)]
  config: PathBuf,

  /// Inode
  #[arg(short, long)]
  inode: u64,

  // /// Topology of the StorageBlock
  // #[arg(short, long)]
  // topology: String,

  /// New Pool to write Erasure Encoded Blocks too
  #[arg(short, long)]
  pool: String
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct FuseConfig {
  mount_dir: PathBuf,
  pools: HashMap<String, HashMap<String, PathBuf>>,
  metadata_dir: PathBuf,
  write_pool: String,
}

fn main() {
  env_logger::init();

  let args = Args::parse();

  // load config file
  let config = std::fs::read_to_string(&args.config).expect("could not read config file");
  let config: FuseConfig = serde_yaml::from_str(&config).expect("could not parse config file");

  // let parts = args.topology.split(",").collect::<Vec<String>>();

  let fs_db = FsDB::open(&config.metadata_dir).unwrap();

  let pool_map = (config.pools.clone(), config.write_pool.clone());
  let mut inode_descriptor = fs_db.read_descriptor(args.inode).unwrap();

  let mut to_delete = vec![];

  match inode_descriptor {
    InodeDescriptor::File(ref mut vf) => {

      for idx in 0..vf.blocks.len() {

        if !vf.blocks[idx].is_single() {
          warn!("block {} is not single, skipping", idx);
          continue;
        }

        // create a new block
        let new = StorageBlock::init_ec(config.write_pool.as_str(), &pool_map, (3,2), vf.blocks[idx].size());
        new.create(&pool_map).unwrap();
        info!("created new block to replace block {} ", idx);

        // create a buffer for the block
        let mut buf: Vec<u8> = vec![0; vf.blocks[idx].size()];
        let read = vf.blocks[idx].read(&pool_map, 0, &mut buf).unwrap();

        info!("read {} bytes from old block", read);

        // write the buf into the new block
        let write = new.write(&pool_map, 0, &buf).unwrap();
        info!("wrote {} bytes to new block", write);

        // replace and push the old Block to the to_delete vec
        to_delete.push(mem::replace(&mut vf.blocks[idx], new));
      }

    }
    InodeDescriptor::Directory(_) => {}
    InodeDescriptor::Symlink => {}
  }

  // write the updated descriptor back to the database
  fs_db.write_descriptor(args.inode, &inode_descriptor).unwrap();

  // // delete the old blocks
  // for sb in to_delete {
  //   sb.delete();
  // }
}
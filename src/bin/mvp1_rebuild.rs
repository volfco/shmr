// Read in a .shmr file and reconstruct any missing shards
extern crate reed_solomon_erasure;

use log::{debug, info, warn, error};
use reed_solomon_erasure::galois_8::ReedSolomon;
// or use the following for Galois 2^16 backend
// use reed_solomon_erasure::galois_16::ReedSolomon;
use std::fs::File;
use std::io::{Read, Write};
use std::path::PathBuf;
use std::time::Instant;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum Topology {
    /// Mirror (n copies)
    Mirror(u8),
    /// Reed-Solomon (data, parity)
    ReedSolomon(usize, usize),
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct FileTopology {
    pub uuid: String,
    pub name: Vec<u8>,
    pub size: usize,
    pub topology: Topology,

    /// Block Size, in bytes
    pub block_size: u32,
    /// Blocks
    pub blocks: Vec<BlockTopology>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct BlockTopology {
    pub block: usize,
    pub hash: Vec<u8>,
    pub size: usize,
    pub layout: (u8, u8),
    pub shards: Vec<PathBuf>,
}

fn main() {
    env_logger::init();
    
    let start_time = Instant::now();
    let file_layout_handle = File::open("test.shmr").unwrap();

    let file_layout: FileTopology = serde_yaml::from_reader(file_layout_handle).unwrap();
    for block in file_layout.blocks {
        let shard_size = (block.size as f32 / block.layout.0 as f32).ceil() as usize;
        let mut shard_file_states = Vec::new();
        let mut shards: Vec<Option<Vec<u8>>> = block.shards.iter().map(|path| {

            // check if the file exists. if not, return a zeroed buffer
            if !path.exists() {
                shard_file_states.push(false);
                warn!("File {:?} does not exist", path);
                return None;
            }

            debug!("Reading file {:?}", path);
            shard_file_states.push(true);

            let mut file = File::open(path).unwrap();
            let mut buffer = Vec::with_capacity(shard_size);
            file.read_to_end(&mut buffer).unwrap();

            Some(buffer)
        }).collect();

        let r = ReedSolomon::new(block.layout.0.into(), block.layout.1.into()).unwrap();

        // check if the first three shards are Some
        if shards.iter().take(block.layout.0 as usize).all(|x| x.is_some()) {
            info!("[block {}] all shards are present", block.block);
        } else {
            warn!("[block {}] missing shards. attempting to rebuild", block.block);
            
            if let Err(e) = r.reconstruct(&mut shards) {
                error!("[block {}] failed to rebuild: {:?}", block.block, e);
            }
        }

        // Convert back to normal shard arrangement
        let result: Vec<_> = shards.into_iter().flatten().collect();

        if let Err(e) = r.verify(&result) {
            error!("[block {}] failed to verify: {:?}", block.block, e);
            continue;
        }

        // write any missing shards
        for (i, state) in shard_file_states.iter().enumerate() {
            if !state {
                info!("[block {}] writing missing shard {}", block.block, i);
                let mut file = File::create(&block.shards[i]).unwrap();
                file.write_all(&result[i]).unwrap();
            }
        }
    }

    let duration = start_time.elapsed();
    println!("Total duration: {:?}", duration);
}
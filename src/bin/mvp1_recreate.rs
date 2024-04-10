extern crate reed_solomon_erasure;

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

fn main () {
    let start_time = Instant::now();

    let file_layout_handle = File::open("test.shmr").unwrap();

    let file_layout: FileTopology = serde_yaml::from_reader(file_layout_handle).unwrap();

    let mut output = File::create("output.bin").unwrap();

    for block in file_layout.blocks {
        let shard_size = (block.size as f32 / block.layout.0 as f32).ceil() as usize;

        // because we know the layout of the shards, we only have to read in the first three shards. 
        // the last two shards are parity shards. we don't need to read them in if all the data shards are intact.
        // ... but eventually we will be doing the loading in parallel. So it's not a big deal.        

        let mut shards: Vec<Option<Vec<u8>>> = block.shards.iter().map(|path| {

            // check if the file exists. if not, return a zeroed buffer
            if !path.exists() {
                println!("File {:?} does not exist", path);
                return None;
            }

            println!("Reading file {:?}", path);

            let mut file = File::open(path).unwrap();
            let mut buffer = Vec::with_capacity(shard_size);
            file.read_to_end(&mut buffer).unwrap();

            Some(buffer)
        }).collect();

        let r = ReedSolomon::new(block.layout.0.into(), block.layout.1.into()).unwrap();

        // check if the first three shards are Some
        if shards.iter().take(block.layout.0 as usize).all(|x| x.is_some()) {
            println!("All data shards are intact. No need to reconstruct.");
        } else {
            print!("Some data shards are missing. Reconstructing...");
            
            let start = Instant::now();

            r.reconstruct(&mut shards).unwrap();
    
            let duration = start.elapsed();

            print!("done. took {:?}", duration);
        }

        // truncate the result vector to just the data shards
        shards.truncate(block.layout.0.into());

        // create a buffer the size of the n data shards
        let mut buffer = vec![];

        for shard in shards {
            let shard = shard.unwrap();
            buffer.write_all(&shard).unwrap();
        }

        // we need to do this, because we might have had to padd the last shard with zeros to make it the same size as the others
        // because Reed-Solomon requires all shards to be the same size.
        // TODO this kinda cucks performance, as additional space needs to be allocated
        buffer.truncate(block.size);      

        output.write_all(&buffer).unwrap();
        
    }

    let duration = start_time.elapsed();
    println!("Total duration: {:?}", duration);
}
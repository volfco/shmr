#[macro_use(shards)]
extern crate reed_solomon_erasure;

use reed_solomon_erasure::galois_8::ReedSolomon;
// or use the following for Galois 2^16 backend
// use reed_solomon_erasure::galois_16::ReedSolomon;
use std::fs::File;
use std::io::{Read, Write};
use std::time::Instant;
use std::path::PathBuf;
use std::vec;
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

    let data_shards = 3;
    let parity_shards = 2;

    // eventually, these will be mounts sorted by free space (and other attributes)
    let targets = vec![PathBuf::from("dir1"), PathBuf::from("dir2"), PathBuf::from("dir3"), PathBuf::from("dir4"), PathBuf::from("dir5")];

    if targets.len() != (data_shards + parity_shards) {
        panic!("The number of targets must be equal to the sum of data shards and parity shards");
    }

    let r = ReedSolomon::new(data_shards, parity_shards).unwrap(); // 3 data shards, 2 parity shards

    let mut file = File::open("test.bin").unwrap();
    let mut buffer = vec![0; 1024 * 1024]; // 1 MB buffer

    let shard_size = (buffer.len() as f32 / data_shards as f32).ceil() as usize;

    let mut i = 0;
    let mut block_layout: Vec<BlockTopology> = Vec::new();
    while let Ok(n) = file.read(&mut buffer) {  
        if n == 0 {
            break;
        }

        // only read the amount of data that was read into the buffer
        let buf_content = &buffer[..n]; 

        // creating the 2d array of shards
        let mut shards = buf_content.chunks(shard_size).map(|x| {
            // pad with zeroes if it's not the right size
            // this is because the last shard might be a bit smaller than the rest
            let mut r = x.to_vec();
            if r.len() < shard_size {
                r.resize(shard_size, 0);
            }
            r
        }).collect::<Vec<_>>();

        // add empty parity shards
        for _ in 0..parity_shards {
            shards.push(vec![0; shard_size]);
        }

        let start = Instant::now();

        r.encode(&mut shards).unwrap();

        let duration = start.elapsed();
        // println!("Encoding duration: {:?}", duration);

        let mut shard_paths = Vec::new();

        for (q, shard) in shards.iter().enumerate() {

            // for now, just assume that the target vec contains the exact directory we want to write to
            let path = targets.get(q).expect("Invalid target index").join(format!("block_{}_shard_{}.bin", i, q));

            shard_paths.push(path.clone());

            let mut file = File::create(&path).unwrap();
            file.write_all(shard).unwrap();
        }

        block_layout.push(BlockTopology {
            block: i,
            hash: vec![0; 32],
            size: n,
            layout: (data_shards as u8, parity_shards as u8),
            shards: shard_paths,
        });

        i += 1;
    }

    let file_topology = FileTopology {
        uuid: "1234".to_string(),
        name: "test.bin".as_bytes().to_vec(),
        topology: Topology::ReedSolomon(data_shards, parity_shards),
        block_size: 1024 * 1024,
        blocks: block_layout,
        size: file.metadata().unwrap().len() as usize,
    };

    let shmr_data = serde_json::to_string(&file_topology).unwrap();
    let mut shmr_file = File::create("test.shmr").unwrap();
    shmr_file.write_all(shmr_data.as_bytes()).unwrap();

    let duration = start_time.elapsed();
    println!("Total duration: {:?}", duration);
}
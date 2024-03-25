#[macro_use(shards)]
extern crate reed_solomon_erasure;

use reed_solomon_erasure::galois_8::ReedSolomon;
// or use the following for Galois 2^16 backend
// use reed_solomon_erasure::galois_16::ReedSolomon;
use std::fs::File;
use std::io::{self, Read, Write};
use std::time::Instant;

fn main () {
    let data_shards = 3;
    let parity_shards = 2;

    let r = ReedSolomon::new(data_shards, parity_shards).unwrap(); // 3 data shards, 2 parity shards

    let mut file = File::open("test.bin").unwrap();
    let mut buffer = vec![0; 1024 * 1024]; // 1 MB buffer

    let shard_size = (buffer.len() as f32 / data_shards as f32).ceil() as usize;

    let mut i = 0;
    while let Ok(n) = file.read(&mut buffer) {  
        if n == 0 {
            break;
        }

        let mut shards = buffer.chunks(shard_size).map(|x| {
            let mut r = x.to_vec();
            if r.len() < shard_size {
                r.resize(shard_size, 0);
            }
            r
        }).collect::<Vec<_>>();

        // add in space for the parity shards
        for _ in 0..parity_shards {
            let mut shard = Vec::new();
            shard.resize(shard_size, 0);

            shards.push(shard);
        }

        let start = Instant::now();

        r.encode(&mut shards).unwrap();

        let duration = start.elapsed();
        println!("Encoding duration: {:?}", duration);

        for (q, shard) in shards.iter().enumerate() {
            let mut file = File::create(format!("block_{}_shard_{}.bin", i, q)).unwrap();
            file.write_all(shard).unwrap();
        }

        i += 1;
    }
}
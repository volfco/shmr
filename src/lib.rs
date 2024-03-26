use std::path::PathBuf;

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

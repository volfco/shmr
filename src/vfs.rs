use std::collections::BTreeMap;
use std::fmt::Display;
use std::fs::File;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use log::trace;
use serde::{Deserialize, Serialize};
use crate::ShmrError;
use crate::storage::{IOEngine, PoolMap};
use crate::vpf::VirtualPathBuf;

#[derive(Clone, Debug, PartialEq)]
pub enum BlockTopology {
  /// Single Shard
  Single,
  /// Mirrored Shards, with n mirrors
  Mirror(usize),
  /// Erasure Encoded. (Version, Data Shards, Parity Shards)
  Erasure(u8, u8, u8)
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct VirtualFile {
  /// File Size, in bytes
  pub size: usize,
  /// Chunk Size, in bytes. This is the "block size" for the file
  pub chunk_size: usize,
  /// Chunk Map, mapping each chunk of the file to it's underlying StorageBlock. Vec index is the
  /// chunk number
  /// [chunk index] => (Storage Block, Offset)
  chunk_map: Vec<(usize, usize)>,

  /// List of VirtualBlock, in order, that make up the file
  pub blocks: Vec<VirtualBlock>,
}
impl Default for crate::file::VirtualFile {
  fn default() -> Self {
    Self::new()
  }
}
impl VirtualFile {
  pub fn new() -> Self {
    VirtualFile {
      size: 0,
      chunk_size: 4096,
      chunk_map: vec![],
      blocks: vec![],
    }
  }

  /// Return the known filesize
  pub fn size(&self) -> u64 {
    self.size as u64
  }

  /// Return the number of chunks in the Chunk Map
  pub fn chunks(&self) -> u64 {
    self.chunk_map.len() as u64
  }

  /// Allocate a new StorageBlock then extend the chunk map
  fn allocate_block(&mut self, engine: &IOEngine) -> anyhow::Result<(), ShmrError> {
    // TODO Improve the logic when selecting which pool to select from
    let sb = VirtualBlock::init_single(engine)?;
    sb.create(engine)?;

    // extend the chunk map
    let block_idx = self.blocks.len();
    for i in 0..(sb.size() / self.chunk_size) {
      self.chunk_map.push((block_idx, self.chunk_size * i));
    }

    self.blocks.push(sb);
    Ok(())
  }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct VirtualBlock {
  /// StorageBlock UUID
  uuid: uuid::Uuid,

  /// Size of the StorageBlock.
  /// This is a fixed size, and represents the maximum amount of data that can be stored in this
  /// block. On-disk size might be a bit larger (or smaller) than this value.
  size: usize,

  /// Shards that make up this block.
  shards: Vec<VirtualPathBuf>,

  /// Layout of this StorageBlock
  topology: BlockTopology,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct VirtualPath {
  /// Drive Pool
  pub pool: String,
  /// Specific drive in the Drive Pool
  pub bucket: String,
  /// Filename in the Bucket
  pub filename: String,
  // #[with(Skip)]
  // resolved_path: Option<(PathBuf, PathBuf)>
}
impl VirtualPath {
  /// Return the (Filename, Directory) for the file.
  /// It's inverted to avoid needing to create a copy of the directory name before joining the filename
  pub fn resolve(&self, map: &PoolMap) -> Result<(PathBuf, PathBuf), ShmrError> {
    let pool_map = map.get(&self.pool).ok_or(ShmrError::InvalidPoolId)?;

    let mut path_buf = pool_map
      .get(&self.bucket)
      .ok_or(ShmrError::InvalidBucketId)?
      .path();

    path_buf.push(&self.filename[0..2]); // first two characters of the filename
    path_buf.push(&self.filename[2..4]); // next two characters of the filename

    let result = (path_buf.join(&self.filename), path_buf);
    trace!(
            "Resolved path for {:?} to (file: {:?}, dir: {:?})",
            self,
            result.0,
            result.1
        );

    Ok(result)
  }
}
impl Display for crate::vpf::VirtualPathBuf {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    write!(f, "{}({}):{}", self.pool, self.bucket, self.filename)
  }
}
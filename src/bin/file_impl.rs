// Goal: Run this to read in a file, and then create a VirtualFile from it.
//       Then step through moving it from a single buffer file, to one with multiple StorageBlocks.
//       Then do some I/O operations on it.

use std::collections::HashMap;
use std::ops::{Range, RangeBounds, RangeInclusive};
use std::path::PathBuf;
use anyhow::Result;
use log::{debug, info, trace};
use shmr::storage::StorageBlock;

pub struct VirtualFile {
  base_dir: PathBuf,

  /// File size, in bytes
  size: u64,

  /// List of StorageBlocks, in order, that make up the file
  blocks: Vec<StorageBlock>,

  block_size: u64,

}
impl VirtualFile {
  /// path is the file location, on disk
  /// block_size is the size of each block, in bytes
  pub fn open(path: impl Into<PathBuf>, block_size: u64) -> Self {
    let path = path.into();
    // create the directory at path if it doesn't exist
    std::fs::create_dir_all(path.join("blocks")).unwrap();

    todo!()
  }

  fn pool_map(&self) -> HashMap<String, PathBuf> {
    /// TODO This is dirty. Bad.
    let mut map = HashMap::new();
    map.insert("default".to_string(), self.base_dir.join("blocks"));
    map
  }
  fn calculate_block_range(&self, offset: u64, buf_len: u64) -> RangeInclusive<u64> {
    let starting_block = match offset > 0 {
      true => offset / self.block_size,
      false => 0
    };
    let mut ending_block = starting_block + (buf_len / self.block_size);


    starting_block..=ending_block
  }

  pub fn read(&self, offset: u64, buf: &mut [u8]) -> Result<usize> {
    todo!()
  }

  pub fn write(&mut self, offset: u64, buf: &[u8]) -> Result<usize> {
    // I'm switching back and forth between needing zero indexed data and non-zero indexed data :/
    let block_range = self.calculate_block_range(offset, buf.len() as u64);
    trace!("writing {} bytes to offset {}. write blocks: {:?}", buf.len(), offset, &block_range);

    let mut written = 0;

    for block_idx in block_range.clone() {
      if self.blocks.get(block_idx as usize).is_none() {
        let pool = self.pool_map();
        let sb = StorageBlock::init_single(&pool)?;
        sb.create(&pool)?;
        self.blocks.push(sb);
      }

      let block = self.blocks.get(block_idx as usize).unwrap();

      let block_offset = (block_idx * self.block_size) - offset;
      let mut block_end = block_offset + self.block_size;
      if block_end > buf.len() as u64 {
        block_end = buf.len() as u64;
      }
      written += block.write(&self.pool_map(), block_offset as usize, &buf[written..(block_end as usize)])?;
    }
    Ok(written)
  }
}

fn main() {

}



#[cfg(test)]
mod tests {
  use shmr::random_data;
  use super::*;

  #[test]
  fn test_virtual_file_write_one_block() {
    let mut vf = VirtualFile {
      base_dir: PathBuf::from("/tmp"),
      size: 0,
      blocks: vec![],
      block_size: 1024,
    };

    let buffer = vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9];
    let result = vf.write(0, &buffer);
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), 10);

    // read it back
    let mut read_buffer = vec![0; 10];
    let result = vf.read(0, &mut read_buffer);
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), 10);
    assert_eq!(read_buffer, buffer);
  }

  #[test]
  fn test_virtual_file_write_two_blocks() {
    let mut vf = VirtualFile {
      base_dir: PathBuf::from("/tmp"),
      size: 0,
      blocks: vec![],
      block_size: 1024,
    };

    let buffer = random_data(1999);
    let result = vf.write(0, &buffer);
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), 1999);

    // read it back
    let mut read_buffer = vec![0; 1999];
    let result = vf.read(0, &mut read_buffer);
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), 1999);
    assert_eq!(read_buffer, buffer);
  }
  #[test]
  fn test_virtual_file_write_lots_of_blocks_1() {
    let mut vf = VirtualFile {
      base_dir: PathBuf::from("/tmp"),
      size: 0,
      blocks: vec![],
      block_size: 4096,
    };

    let buffer = random_data(199990);
    let result = vf.write(0, &buffer);
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), 199990);

    // read it back
    let mut read_buffer = vec![0; 199990];
    let result = vf.read(0, &mut read_buffer);
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), 199990);
    assert_eq!(read_buffer, buffer);
  }
}
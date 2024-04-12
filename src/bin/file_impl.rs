// Goal: Run this to read in a file, and then create a VirtualFile from it.
//       Then step through moving it from a single buffer file, to one with multiple StorageBlocks.
//       Then do some I/O operations on it.

use std::collections::HashMap;
use std::ops::{Range, RangeBounds, RangeInclusive};
use std::path::PathBuf;
use anyhow::Result;
use log::{debug, trace};
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
    let mut ending_block = starting_block + (buf_len / self.block_size) - 1;


    starting_block..=ending_block
  }

  pub fn read(&self, offset: u64, buf: &mut [u8]) -> Result<usize> {
    todo!()
  }

  pub fn write(&mut self, offset: u64, buf: &[u8]) -> Result<usize> {
    // I'm switching back and forth between needing zero indexed data and non zero indexed data :/
    // WHY Did I make this a Range?
    let block_range = self.calculate_block_range(offset, buf.len() as u64);
    trace!("writing {} bytes to offset {}. write blocks: {:?}", buf.len(), offset, &block_range);

    if block_range.end() > &(self.blocks.len() as u64) {
      let mut new_blocks = block_range.end() - self.blocks.len() as u64;

      if self.blocks.is_empty() {
        // this is why zero is stupid
        new_blocks += 1;
      }

      debug!("Adding {} new blocks to VirtualFile", new_blocks);
      let pool = self.pool_map();
      for _ in 0..new_blocks {
        let sb = StorageBlock::init_single(&pool)?;
        sb.create(&pool)?;

        self.blocks.push(sb);
      }
    }

    let mut written = 0;
    let mut remaining_offset = offset;
    for i in block_range.clone() {

      let buf_start = written;
      let mut buf_end = written + self.block_size as usize;
      // cuz nobody wants an out-of-bounds error
      if buf_end > buf.len() {
        buf_end = buf.len();
      }

      written += self.blocks[i as usize].write(&self.pool_map(), remaining_offset as usize, &buf[buf_start..buf_end])?;
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

  // #[test]
  // fn test_calculate_block_range() {
  //   let vf = VirtualFile {
  //     base_dir: PathBuf::from("/tmp"),
  //     size: 0,
  //     blocks: vec![],
  //     block_size: 1024,
  //   };
  //
  //   let range = vf.calculate_block_range(0, 512);
  //   assert_eq!(range, 0..1);
  //
  //   let range = vf.calculate_block_range(1024, 1024);
  //   assert_eq!(range, 1..2);
  //
  //   let range = vf.calculate_block_range(1024, 2048);
  //   assert_eq!(range, 1..3);
  //
  //   let range = vf.calculate_block_range(1024, 1024 * 1024);
  //   assert_eq!(range, 1..1025);
  // }

  // #[test]
  // fn test_virtual_file_write_smol() {
  //   let mut vf = VirtualFile {
  //     base_dir: PathBuf::from("/tmp"),
  //     size: 0,
  //     blocks: vec![],
  //     block_size: 1024,
  //   };
  //
  //   let buffer = vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9];
  //   let result = vf.write(0, &buffer);
  //   assert!(result.is_ok());
  //   assert_eq!(result.unwrap(), 10);
  // }

  #[test]
  fn test_virtual_file_write_small() {
    env_logger::init();

    let mut vf = VirtualFile {
      base_dir: PathBuf::from("/tmp"),
      size: 0,
      blocks: vec![],
      block_size: 1024,
    };

    let buffer = random_data(2048);
    let result = vf.write(0, &buffer);
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), 2048);
  }
}
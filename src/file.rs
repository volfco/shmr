use std::cmp;
// Goal: Run this to read in a file, and then create a VirtualFile from it.
//       Then step through moving it from a single buffer file, to one with multiple StorageBlocks.
//       Then do some I/O operations on it.
use crate::storage::{PoolMap, StorageBlock};
use anyhow::Result;
use log::{debug, info, trace, warn};
use rkyv::{Archive, Deserialize, Serialize};
use std::collections::HashMap;
use std::ops::{RangeInclusive};
use std::path::PathBuf;

pub trait StoragePoolMap {
    fn get(&self) -> HashMap<String, PathBuf>;
    /// Return the New Write Path, which is where new blocks should be written to
    fn new_write(&self) -> Result<HashMap<String, PathBuf>>;
}

#[derive(Debug, Archive, Serialize, Deserialize, Clone, PartialEq)]
#[archive(compare(PartialEq), check_bytes)]
pub struct VirtualFile {
    /// File size, in bytes
    size: usize,

    /// List of StorageBlocks, in order, that make up the file
    blocks: Vec<StorageBlock>,

    /// Size of each block, in bytes
    /// Used to calculate the block number to send I/O operations to. Additionally, used to size
    /// erasure encoded shards.
    block_size: usize,

    cursor: usize
}
impl VirtualFile {
    pub fn new(block_size: usize) -> Self {
        VirtualFile {
            size: 0,
            blocks: vec![],
            block_size,
            cursor: 0,
        }
    }

    fn calculate_block_range(&self, buf_len: usize) -> RangeInclusive<usize> {
        RangeInclusive::new(
            cmp::max(0, self.cursor / self.block_size),
            (self.cursor + buf_len) / self.block_size
        )
    }


    pub fn write(&mut self, pool: &PoolMap, buf: &[u8]) -> Result<usize> {
        let bf = buf.len();
        if bf == 0 {
            trace!("write request buf len == 0. nothing to do");
            return Ok(0);
        }
        // calculate the range of blocks we want to write to
        let block_range = self.calculate_block_range(bf);
        debug!("writing {} bytes. cursor: {}. computed block range: {:?}", bf, self.cursor, &block_range);

        let mut written = 0;
        let mut block_offset = self.cursor % self.block_size;

        // loop over the blocks covered in this write call, and make sure we have blocks allocated
        // to cover the data in this write
        for block_idx in block_range {
            // if the block we want to write to doesn't exist, create it
            if self.blocks.get(block_idx).is_none() {
                let sb = StorageBlock::init_single(pool.1.as_str(), pool)?;
                sb.create(pool)?;
                self.blocks.push(sb);
            }

            let block = self.blocks.get_mut(block_idx).unwrap();

            let buf_start = written;
            let buf_end = cmp::min(bf, written + self.block_size) - block_offset;

            written += block.write(pool, block_offset, &buf[buf_start..buf_end])?;

            // reset the block cursor
            block_offset = 0;
        }

        Ok(written)
    }

    /// Read `size` bytes from VirtualFile, starting at the given offset, into the buffer.
    pub fn read(&self, pool: &PoolMap, buf: &mut [u8]) -> Result<usize> {
        let block_range = self.calculate_block_range(buf.len());

        warn!("cursor: {}. computed block range: {:?}", self.cursor, &block_range);

        let mut read = 0;
        let mut block_offset = self.cursor % self.block_size;
        for block_idx in block_range {
            let block = self.blocks.get(block_idx).unwrap();
            let buf_start = read;
            let buf_end = cmp::max(buf.len(), read + self.block_size) - block_offset;
            info!("reading from block {} at {}. {} - {}", block_idx, block_offset, buf_start, buf_end);
            read += block.read(pool, block_offset, &mut buf[buf_start..])?;

            block_offset = 0;
        }


        Ok(read)
    }

}


#[cfg(test)]
mod tests {
    use super::*;
    use crate::random_data;
    use crate::tests::get_pool;

    #[test]
    fn sanity_check_block_range() {
        let mut vf = VirtualFile::new(1024);
        assert_eq!(vf.calculate_block_range(200), 0..=0);
        assert_eq!(vf.calculate_block_range(1024), 0..=1);

        // set the cursor to 1024
        vf.cursor = 1024;
        assert_eq!(vf.calculate_block_range(2000), 1..=2);
    }

    #[test]
    fn test_virtual_file_write_one_block() {
        let pool_map: PoolMap = get_pool();

        let mut vf = VirtualFile {
            size: 0,
            blocks: vec![],
            block_size: 1024,
            cursor: 0,
        };

        let buffer = vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9];
        let result = vf.write(&pool_map, &buffer);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 10);

        // read it back
        let mut read_buffer = [0; 10];
        let result = vf.read(&pool_map, &mut read_buffer);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 10);
        assert_eq!(read_buffer, buffer.as_slice());
    }

    #[test]
    fn test_virtual_file_write_two_blocks() {
        let pool_map: PoolMap = get_pool();

        let mut vf = VirtualFile {
            size: 0,
            cursor: 0,
            blocks: vec![],
            block_size: 128,
        };

        let buffer = random_data(200);
        let result = vf.write(&pool_map,  &buffer);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 200);


        // read it back
        let mut read_buffer = [0; 200];
        let result = vf.read(&pool_map, &mut read_buffer);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 200);
        assert_eq!(read_buffer, buffer.as_slice());
    }

    #[test]
    fn test_virtual_file_two_block_offset_read() {
        let pool_map: PoolMap = get_pool();

        let mut vf = VirtualFile {
            size: 0,
            cursor: 0,
            blocks: vec![],
            block_size: 128,
        };

        let buffer = random_data(200);
        let result = vf.write(&pool_map,  &buffer);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 200);

        // read back block 0
        let mut nb = [0; 200];
        vf.blocks[0].read(&pool_map, 0, &mut nb).unwrap();
        assert_eq!(buffer[0..128], nb[0..128]);

        // read back block 1
        let mut nb = [0; 200];
        vf.blocks[1].read(&pool_map, 0, &mut nb).unwrap();
        assert_eq!(buffer[128..200], nb[0..72]);

        // read it back
        let mut read_buffer = [0; 100];
        vf.cursor = 100;
        let result = vf.read(&pool_map, &mut read_buffer);
        assert!(result.is_ok());
        // assert_eq!(result.unwrap(), 100);
        assert_eq!(read_buffer, buffer.as_slice()[100..200]);
    }
    //
    // #[test]
    // fn test_virtual_file_write_size() {
    //     let pool_map: PoolMap = get_pool();
    //
    //     let mut vf = VirtualFile {
    //         size: 0,
    //         blocks: vec![],
    //         block_size: 128,
    //         cursor: 0,
    //     };
    //
    //     let buffer = random_data(200);
    //     let result = vf.write(&pool_map, &buffer);
    //     assert!(result.is_ok());
    //     assert_eq!(result.unwrap(), 200);
    //
    //     assert_eq!(vf.size, 200);
    // }


    #[test]
    fn test_virtual_file_write_lots_of_blocks_1() {
        let pool_map: PoolMap = get_pool();

        let mut vf = VirtualFile {
            size: 0,
            blocks: vec![],
            block_size: 4096,
            cursor: 0
        };

        let buffer = random_data(199990);
        let result = vf.write(&pool_map,  &buffer);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 199990);

        // read it back
        let mut read_buffer = [0; 199990];
        let result = vf.read(&pool_map, &mut read_buffer);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 199990);
        assert_eq!(read_buffer, buffer.as_slice());
    }
}

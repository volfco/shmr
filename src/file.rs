use std::cmp;
// Goal: Run this to read in a file, and then create a VirtualFile from it.
//       Then step through moving it from a single buffer file, to one with multiple StorageBlocks.
//       Then do some I/O operations on it.
use crate::storage::{PoolMap, StorageBlock};
use anyhow::Result;
use log::{debug, info, trace, warn};
use rkyv::{Archive, Deserialize, Serialize};
use std::collections::HashMap;
use std::ops::{Range, RangeInclusive};
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
    pub fn read(&self, pool: &PoolMap, offset: u64, buf: &mut [u8]) -> Result<usize> {


        let block_range = self.calculate_block_range(offset, bf as u64);

        warn!("reading blocks: {:?} with offset {}", &block_range, offset);

        let mut read = 0;
        for block_idx in block_range {
            let block = self.blocks.get(block_idx as usize).unwrap();

            let block_offset = cmp::max(0, offset as i64 - (block_idx * self.block_size) as i64) as usize;

            trace!(
                "reading block {} from offset {}",
                block_idx,
                block_offset
            );
            // only pass the remaining buffer to the block read, so the lower functions
            // can figure out how much to read
            read += block.read(pool, block_offset, &mut buf[read..])?;
        }

        Ok(read)
    }

}


#[cfg(test)]
mod tests {
    use super::*;
    use crate::random_data;
    use crate::tests::get_pool;
    use std::path::Path;

    #[test]
    fn sanity_check_block_range() {
        let vf = VirtualFile::new(1024);
        assert_eq!(vf.calculate_block_range(0, 512), 0..=0);
        assert_eq!(vf.calculate_block_range(0, 2000), 0..=1);
        assert_eq!(vf.calculate_block_range(1024, 900), 1..=1);
        assert_eq!(vf.calculate_block_range(1024, 1024), 1..=2);

        assert_eq!(vf.calculate_block_range(1024, 2048), 1..=3);

        let vf = VirtualFile::new(128);
        assert_eq!(vf.calculate_block_range(100, 100), 0..=1);
    }

    #[test]
    fn test_virtual_file_write_one_block() {
        let temp_dir = Path::new("/tmp");
        let pool_map: PoolMap = get_pool();

        let mut vf = VirtualFile {
            size: 0,
            blocks: vec![],
            block_size: 1024,
        };

        let buffer = vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9];
        let result = vf.write(&pool_map, 0, &buffer);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 10);

        // read it back
        let mut read_buffer = [0; 10];
        let result = vf.read(&pool_map, 0, &mut read_buffer);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 10);
        assert_eq!(read_buffer, buffer.as_slice());
    }

    #[test]
    fn test_virtual_file_write_two_blocks() {
        let temp_dir = Path::new("/tmp");
        let pool_map: PoolMap = get_pool();

        let mut vf = VirtualFile {
            size: 0,
            blocks: vec![],
            block_size: 128,
        };

        let buffer = random_data(200);
        let result = vf.write(&pool_map, 0, &buffer);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 200);


        // read it back
        let mut read_buffer = [0; 200];
        let result = vf.read(&pool_map, 0, &mut read_buffer);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 200);
        assert_eq!(read_buffer, buffer.as_slice());
    }

    #[test]
    fn test_virtual_file_two_block_offset_read() {
        env_logger::init();
        let temp_dir = Path::new("/tmp");
        let pool_map: PoolMap = get_pool();

        let mut vf = VirtualFile {
            size: 0,
            blocks: vec![],
            block_size: 128,
        };

        let buffer = random_data(200);
        let result = vf.write(&pool_map, 0, &buffer);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 200);


        // read it back
        let mut read_buffer = [0; 100];
        let result = vf.read(&pool_map, 100, &mut read_buffer);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 100);
        assert_eq!(read_buffer, buffer.as_slice()[100..200]);
    }

    #[test]
    fn test_virtual_file_write_size() {
        env_logger::init();
        let temp_dir = Path::new("/tmp");
        let pool_map: PoolMap = get_pool();

        let mut vf = VirtualFile {
            size: 0,
            blocks: vec![],
            block_size: 128,
        };

        let buffer = random_data(200);
        let result = vf.write(&pool_map, 0, &buffer);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 200);

        assert_eq!(vf.size, 200);
    }


    // #[test]
    // fn test_virtual_file_write_lots_of_blocks_1() {
    //     let temp_dir = Path::new("/tmp");
    //     let pool_map: PoolMap = get_pool();
    //
    //     let mut vf = VirtualFile {
    //         size: 0,
    //         blocks: vec![],
    //         block_size: 4096,
    //     };
    //
    //     let buffer = random_data(199990);
    //     let result = vf.write(&pool_map, 0, &buffer);
    //     assert!(result.is_ok());
    //     assert_eq!(result.unwrap(), 199990);
    //
    //     // read it back
    //     let mut read_buffer = vec![];
    //     let result = vf.read(&pool_map, 0, &mut read_buffer);
    //     assert!(result.is_ok());
    //     assert_eq!(result.unwrap(), 199990);
    //     assert_eq!(read_buffer, buffer);
    // }

    // TODO more tests around file operations

    // // test writing the virtualfile to disk, and then read it back as a new object
    // #[test]
    // fn test_virtual_file_save_to_disk() {
    //     let base_dir = "/tmp".to_string();
    //     let temp_dir = Path::new("/tmp");
    //     let pool_map: PoolMap = get_pool();
    //
    //     let mut vf = VirtualFile {
    //         size: 0,
    //         blocks: vec![],
    //         block_size: 4096,
    //     };
    //
    //     let buffer = random_data(199990);
    //     let result = vf.write(&pool_map, 0, &buffer);
    //     assert!(result.is_ok());
    //     assert_eq!(result.unwrap(), 199990);
    //
    //     let result = vf.save_to_disk();
    //     assert!(result.is_ok());
    //
    //     // read it back
    //     let metadata_file = PathBuf::from(&base_dir).join("topology.shmr-v0");
    //     let file = std::fs::File::open(&metadata_file).unwrap();
    //     let mut reader = std::io::BufReader::new(file);
    //     let mut buf = vec![];
    //     reader.read_to_end(&mut buf).unwrap();
    //
    //     let new_vf = rkyv::from_bytes::<VirtualFile>(&buf).unwrap();
    //     assert_eq!(vf, new_vf);
    //
    //     // initialize a new VirtualFile from disk
    //     let nvf = VirtualFile::open(&base_dir);
    //     assert!(nvf.is_ok());
    //     let nvf = nvf.unwrap();
    //     let mut buf1 = vec![0];
    //     let mut buf2 = vec![0];
    //
    //     let _ = nvf.read(&pool_map, 0, &mut buf1);
    //     let _ = vf.read(&pool_map, 0, &mut buf2);
    //
    //     assert_eq!(buf1, buf2);
    // }
}

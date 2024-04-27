use std::cmp;
// Goal: Run this to read in a file, and then create a VirtualFile from it.
//       Then step through moving it from a single buffer file, to one with multiple StorageBlocks.
//       Then do some I/O operations on it.
use crate::storage::{PoolMap, StorageBlock};
use anyhow::{bail, Result};
use log::{debug, error, trace};
use rkyv::{Archive, Deserialize, Serialize};
use std::collections::HashMap;
use std::ops::{Range, RangeInclusive};
use std::path::PathBuf;
use tokio::io::AsyncReadExt;

/// Default Chunk Size. Also used as the inode block size for FUSE stuffs
///
/// It is undefined behavior if this doesn't cleanly mathphs with the other defaults
const DEFAULT_CHUNK_SIZE: usize = 4096;

pub trait StoragePoolMap {
    fn get(&self) -> HashMap<String, PathBuf>;
    /// Return the New Write Path, which is where new blocks should be written to
    fn new_write(&self) -> Result<HashMap<String, PathBuf>>;
}

#[derive(Debug, Archive, Serialize, Deserialize, Clone, PartialEq)]
#[archive(compare(PartialEq), check_bytes)]
pub struct VirtualFile {
    pub size: usize,
    pub chunk_size: usize,
    /// Chunk Map, mapping each chunk of the file to it's underlying StorageBlock
    chunk_map: Vec<(usize, usize)>,

    /// List of StorageBlocks, in order, that make up the file
    blocks: Vec<StorageBlock>
}
impl VirtualFile {
    pub fn new() -> Self {
        VirtualFile {
            size: 0,
            chunk_size: 4096,
            chunk_map: vec![],
            blocks: vec![]
        }
    }

    // fn calculate_block_range(&self, buf_len: usize) -> RangeInclusive<usize> {
    //     RangeInclusive::new(
    //         cmp::max(0, self.cursor / self.block_size),
    //         (self.cursor + buf_len) / self.block_size
    //     )
    // }

    fn allocate_block(&mut self, pool: &PoolMap) -> Result<()> {
        let sb = StorageBlock::init_single(pool.1.as_str(), pool)?;
        sb.create(pool)?;

        // add the new storageblock chunk mapping
        let block_idx = self.blocks.len();
        for i in 0..(sb.size() / self.chunk_size) {
            self.chunk_map.push((block_idx, self.chunk_size * i));
        }

        self.blocks.push(sb);

        Ok(())
    }

    pub fn write(&mut self, pool: &PoolMap, pos: usize, buf: &[u8]) -> Result<usize> {
        let bf = buf.len();
        if bf == 0 {
            trace!("write request buf len == 0. nothing to do");
            return Ok(0);
        }

        if pos % self.chunk_size != 0 {
            panic!("cursor position does not align with block size")
        }

        let range = (pos / self.chunk_size)..=((pos + bf) / self.chunk_size);

        let mut written = 0;
        for chunk_idx in range {
            // allocate a new block if we're out of space to write the chunk
            if self.chunk_map.is_empty() || self.chunk_map.len() < chunk_idx {
                self.allocate_block(&pool)?;
            }
            let (block_idx, block_pos) = self.chunk_map.get(chunk_idx).expect(format!("chunk_idx: {}. chunk_map len: {}", chunk_idx, self.chunk_map.len()).as_str());

            let buf_end = cmp::min(written + self.chunk_size, bf);
            written += self.blocks[*block_idx].write(&pool, *block_pos, &buf[written..buf_end])?;

        }

        // size is the largest (offset + written buffer)
        self.size = cmp::max(self.size, pos + bf);


        Ok(written)
    }

    /// Read `size` bytes from VirtualFile, starting at the given offset, into the buffer.
    pub fn read(&self, pool: &PoolMap, pos: usize, buf: &mut [u8]) -> Result<usize> {
        let bf = buf.len();
        if bf == 0 {
            trace!("write request buf len == 0. nothing to do");
            return Ok(0);
        }

        if pos > self.size {
            bail!("EOF")
        }

        let mut read = 0;
        let pos_chk = pos / self.chunk_size;
        let buf_chk = bf / self.chunk_size;

        debug!("reading {} bytes at offset {}. chunk range {}..={}", bf, pos, pos_chk, pos_chk + buf_chk);

        for idx in pos_chk..=pos_chk+buf_chk {
            let (block_idx, block_pos) = self.chunk_map.get(idx).expect("wtf bro");

            read += &self.blocks[*block_idx].read(&pool, *block_pos, &mut buf[read..])?;
        }

        Ok(read)
    }
}

#[cfg(test)]
mod tests {
    use log::error;
    use super::*;
    use crate::random_data;
    use crate::tests::get_pool;

    #[test]
    fn test_virtual_file_write_one_block() {
        let pool_map: PoolMap = get_pool();

        let mut vf = VirtualFile {
            size: 0,
            chunk_size: 16,
            chunk_map: vec![],
            blocks: vec![],
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
        let pool_map: PoolMap = get_pool();

        let mut vf = VirtualFile {
            size: 0,
            chunk_size: 128,
            chunk_map: vec![],
            blocks: vec![],
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

    // #[test]
    // fn test_virtual_file_two_block_offset_read() {
    //     let pool_map: PoolMap = get_pool();
    //
    //     let mut vf = VirtualFile {
    //         size: 0,
    //         chunk_size: 128,
    //         chunk_map: vec![],
    //         blocks: vec![],
    //     };
    //
    //     let buffer = random_data(200);
    //     let result = vf.write(&pool_map, 0, &buffer);
    //     assert!(result.is_ok());
    //     assert_eq!(result.unwrap(), 200);
    //
    //     // read back block 0
    //     let mut nb = [0; 200];
    //     vf.blocks[0].read(&pool_map, 0, &mut nb).unwrap();
    //     assert_eq!(buffer[0..128], nb[0..128]);
    //
    //     // read back block 1
    //     let mut nb = [0; 200];
    //     vf.blocks[1].read(&pool_map, 0, &mut nb).unwrap();
    //     assert_eq!(buffer[128..200], nb[0..72]);
    //
    //     // read it back
    //     let mut read_buffer = [0; 100];
    //     let result = vf.read(&pool_map, 0, &mut read_buffer);
    //     assert!(result.is_ok());
    //     // assert_eq!(result.unwrap(), 100);
    //     assert_eq!(read_buffer, buffer.as_slice()[100..200]);
    // }

    #[test]
    fn test_virtual_file_write_size() {
        let pool_map: PoolMap = get_pool();

        let mut vf = VirtualFile {
            size: 0,
            chunk_size: 128,
            chunk_map: vec![],
            blocks: vec![],
        };

        let buffer = random_data(200);
        let result = vf.write(&pool_map, 0, &buffer);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 200);

        assert_eq!(vf.size, 200);
    }

    #[test]
    fn test_virtual_file_write_lots_of_blocks_1() {
        let pool_map: PoolMap = get_pool();

        let mut vf = VirtualFile {
            size: 0,
            chunk_size: 128,
            chunk_map: vec![],
            blocks: vec![],
        };

        let buffer = random_data(199990);
        let result = vf.write(&pool_map, 0, &buffer);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 199990);

        // read it back
        let mut read_buffer = [0; 199990];
        let result = vf.read(&pool_map, 0, &mut read_buffer);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 199990);
        assert_eq!(read_buffer, buffer.as_slice());
    }
}

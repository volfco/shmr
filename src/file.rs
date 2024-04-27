use std::cmp;
// Goal: Run this to read in a file, and then create a VirtualFile from it.
//       Then step through moving it from a single buffer file, to one with multiple StorageBlocks.
//       Then do some I/O operations on it.
use crate::storage::{PoolMap, StorageBlock};
use anyhow::{bail, Result};
use log::trace;
use rkyv::{Archive, Deserialize, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;

/// Default Chunk Size. Also used as the inode block size for FUSE stuffs
///
/// It is undefined behavior if this doesn't cleanly mathphs with the other defaults
pub(crate) const DEFAULT_CHUNK_SIZE: usize = 4096;

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
impl Default for VirtualFile {
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
            blocks: vec![]
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
    fn allocate_block(&mut self, pool: &PoolMap) -> Result<()> {
        // TODO Improve the logic when selecting which pool to select from
        let sb = StorageBlock::init_single(pool.1.as_str(), pool)?;
        sb.create(pool)?;

        // extend the chunk map
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

        let mut written = 0;
        for chunk_idx in (pos / self.chunk_size)..=((pos + bf) / self.chunk_size) {
            // allocate a new block if we're out of space to write the chunk
            if self.chunk_map.is_empty() || self.chunk_map.len() < chunk_idx {
                self.allocate_block(pool)?;
            }
            let (block_idx, block_pos) = self.chunk_map.get(chunk_idx).unwrap_or_else(|| panic!("chunk_idx: {}. chunk_map len: {}", chunk_idx, self.chunk_map.len()) );

            let buf_end = cmp::min(written + self.chunk_size, bf);
            written += self.blocks[*block_idx].write(pool, *block_pos, &buf[written..buf_end])?;
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

        let mut read = 0;  // amount read
        let mut chk_pos = pos % self.chunk_size;  // initial chunk offset
        let pos_chk = pos / self.chunk_size;
        let buf_chk = bf / self.chunk_size + (chk_pos != 0) as usize;  // got this nifty solution off Reddit. Thanks /u/HeroicKatora

        let chunk_range = pos_chk..=pos_chk+buf_chk;

        for idx in chunk_range {
            let (block_idx, block_pos) = self.chunk_map.get(idx).expect("wtf bro");

            // we might get a read that starts in the middle a chunk
            // in that case, just move the block cursor forward the amount of the chunk offset
            let read_pos = *block_pos + chk_pos;
            let read_amt = cmp::min(bf, read + self.chunk_size - chk_pos);

            trace!("reading {} bytes from chunk {} (mapping to block_idx:{} / block_pos:{} / read_pos:{})", read_amt, idx, block_idx, block_pos, read_pos);

            read += &self.blocks[*block_idx].read(pool, read_pos, &mut buf[read..read_amt])?;

            chk_pos = 0;
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

    #[test]
    fn test_virtual_file_offset_read() {
        let pool_map: PoolMap = get_pool();

        let mut vf = VirtualFile {
            size: 0,
            chunk_size: 16,
            chunk_map: vec![],
            blocks: vec![],
        };

        let buffer = random_data(40);
        let result = vf.write(&pool_map, 0, &buffer);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 40);

        // 192, 92, 37, 64, 198, 38, 220, 129, 178, 52, 71, 6, 67, 8, 48, 134, 35, 13, 161, 210, 198, 129, 44, 38, 215, 245, 17, 87, 120, 125, 185, 65, 138, 218, 230, 29, 125, 189, 113, 50

        //                                                                     35, 13, 161, 210, 198, 129, 44, 38, 215, 245, 17, 87, 120, 125, 185, 65, 138, 218, 230, 29
        //                                                                                       198, 129, 44, 38, 215, 245, 17, 87, 120, 125, 185, 65, 138, 218, 230, 29, 125, 189, 113, 50

        // read it back
        let mut read_buffer = [0; 20];
        let result = vf.read(&pool_map, 20, &mut read_buffer);
        assert!(result.is_ok());
        // assert_eq!(result.unwrap(), 100);
        assert_eq!(read_buffer, buffer.as_slice()[20..40]);
    }

    #[test]
    fn test_virtual_file_two_block_offset_read() {
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
        //
        // // read back block 0
        // let mut nb = [0; 200];
        // vf.blocks[0].read(&pool_map, 0, &mut nb).unwrap();
        // assert_eq!(buffer[0..128], nb[0..128]);
        //
        // // read back block 1
        // let mut nb = [0; 200];
        // vf.blocks[1].read(&pool_map, 0, &mut nb).unwrap();
        // assert_eq!(buffer[128..200], nb[0..72]);

        // read it back
        let mut read_buffer = [0; 100];
        let result = vf.read(&pool_map, 100, &mut read_buffer);
        assert!(result.is_ok());
        // assert_eq!(result.unwrap(), 100);
        assert_eq!(read_buffer, buffer.as_slice()[100..200]);
    }
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

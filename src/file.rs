use std::cmp;
// Goal: Run this to read in a file, and then create a VirtualFile from it.
//       Then step through moving it from a single buffer file, to one with multiple StorageBlocks.
//       Then do some I/O operations on it.
use crate::storage::{Engine, StorageBlock};
use anyhow::{Result};
use log::trace;
use serde::{Serialize, Deserialize};
use std::collections::HashMap;
use std::path::PathBuf;
use crate::ShmrError;

/// Default Chunk Size. Also used as the inode block size for FUSE stuffs
///
/// It is undefined behavior if this doesn't cleanly mathphs with the other defaults
pub(crate) const DEFAULT_CHUNK_SIZE: usize = 4096;

pub trait StoragePoolMap {
    fn get(&self) -> HashMap<String, PathBuf>;
    /// Return the New Write Path, which is where new blocks should be written to
    fn new_write(&self) -> Result<HashMap<String, PathBuf>>;
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub struct VirtualFile {
    pub size: usize,
    pub chunk_size: usize,
    /// Chunk Map, mapping each chunk of the file to it's underlying StorageBlock
    chunk_map: Vec<(usize, usize)>,

    /// List of StorageBlocks, in order, that make up the file
    pub blocks: Vec<StorageBlock>
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
    fn allocate_block(&mut self, engine: &Engine) -> Result<(), ShmrError> {
        // TODO Improve the logic when selecting which pool to select from
        let sb = StorageBlock::init_single(&engine.write_pool, &engine.pools)?;
        sb.create(engine)?;

        // extend the chunk map
        let block_idx = self.blocks.len();
        for i in 0..(sb.size() / self.chunk_size) {
            self.chunk_map.push((block_idx, self.chunk_size * i));
        }

        self.blocks.push(sb);
        Ok(())
    }

    pub fn write(&mut self, engine: &Engine, pos: usize, buf: &[u8]) -> Result<usize, ShmrError> {
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
            if self.chunk_map.is_empty() || self.chunk_map.len() <= chunk_idx {
                self.allocate_block(engine)?;
            }
            let (block_idx, block_pos) = self.chunk_map.get(chunk_idx).unwrap_or_else(|| panic!("chunk_idx: {}. chunk_map len: {}", chunk_idx, self.chunk_map.len()) );

            let buf_end = cmp::min(written + self.chunk_size, bf);
            written += self.blocks[*block_idx].write(engine, *block_pos, &buf[written..buf_end])?;
        }

        // size is the largest (offset + written buffer)
        self.size = cmp::max(self.size, pos + bf);

        Ok(written)
    }

    /// Read `size` bytes from VirtualFile, starting at the given offset, into the buffer.
    pub fn read(&self, engine: &Engine, pos: usize, buf: &mut [u8]) -> Result<usize, ShmrError> {
        let bf = buf.len();
        if bf == 0 {
            trace!("write request buf len == 0. nothing to do");
            return Ok(0);
        }

        if pos > self.size {
            return Err(ShmrError::EndOfFile)
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

            read += &self.blocks[*block_idx].read(engine, read_pos, &mut buf[read..read_amt])?;

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
    use crate::storage::Engine;

    #[test]
    fn test_virtual_file_write_one_block() {
        let engine: Engine = Engine::new("test_pool".to_string(), get_pool());

        let mut vf = crate::file::VirtualFile {
            size: 0,
            chunk_size: 16,
            chunk_map: vec![],
            blocks: vec![],
        };

        let buffer = vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9];
        let result = vf.write(&engine, 0, &buffer);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 10);

        // read it back
        let mut read_buffer = [0; 10];
        let result = vf.read(&engine, 0, &mut read_buffer);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 10);
        assert_eq!(read_buffer, buffer.as_slice());
    }

    #[test]
    fn test_virtual_file_write_two_blocks() {
        let engine: Engine = Engine::new("test_pool".to_string(), get_pool());

        let mut vf = VirtualFile::new();

        let buffer = random_data(200);
        let result = vf.write(&engine, 0, &buffer);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 200);


        // read it back
        let mut read_buffer = [0; 200];
        let result = vf.read(&engine, 0, &mut read_buffer);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 200);
        assert_eq!(read_buffer, buffer.as_slice());
    }

    #[test]
    fn test_virtual_file_offset_read() {
        let engine: Engine = Engine::new("test_pool".to_string(), get_pool());

        let mut vf = VirtualFile::new();

        let buffer = random_data(40);
        let result = vf.write(&engine, 0, &buffer);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 40);

        // read it back
        let mut read_buffer = [0; 20];
        let result = vf.read(&engine, 20, &mut read_buffer);
        assert!(result.is_ok());
        assert_eq!(read_buffer, buffer.as_slice()[20..40]);
    }

    #[test]
    fn test_virtual_file_two_block_offset_read() {
        let engine: Engine = Engine::new("test_pool".to_string(), get_pool());

        let mut vf = VirtualFile::new();

        let buffer = random_data(200);
        let result = vf.write(&engine, 0, &buffer);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 200);

        // read it back
        let mut read_buffer = [0; 100];
        let result = vf.read(&engine, 100, &mut read_buffer);
        assert!(result.is_ok());
        assert_eq!(read_buffer, buffer.as_slice()[100..200]);
    }
    #[test]
    fn test_virtual_file_write_size() {
        let engine: Engine = Engine::new("test_pool".to_string(), get_pool());

        let mut vf = VirtualFile::new();

        let buffer = random_data(200);
        let result = vf.write(&engine, 0, &buffer);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 200);

        assert_eq!(vf.size, 200);
    }

    #[test]
    fn test_virtual_file_write_lots_of_blocks_1() {
        let engine: Engine = Engine::new("test_pool".to_string(), get_pool());

        let mut vf = VirtualFile::new();

        let buffer = random_data(199990);
        let result = vf.write(&engine, 0, &buffer);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 199990);

        // read it back
        let mut read_buffer = vec![0; 199990];
        let result = vf.read(&engine, 0, read_buffer.as_mut_slice());
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 199990);
        assert_eq!(read_buffer, buffer);
    }
}
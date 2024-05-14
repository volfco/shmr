pub mod path;
pub mod block;

use std::cmp;
use log::{debug, trace};
use serde::{Deserialize, Serialize};
use crate::ShmrError;
use crate::PoolMap;
use crate::vfs::block::{BlockTopology, VIRTUAL_BLOCK_DEFAULT_SIZE, VirtualBlock};


/// Represents a virtual file.
///
/// The `VirtualFile` struct is used to store information about a file in a virtual storage system.
///
/// # Fields
///
/// - `size`: The size of the file in bytes.
///
/// - `chunk_size`: The size of each chunk of the file in bytes. This is also referred to as the "block size".
///
/// - `chunk_map`: A vector that maps each chunk of the file to its underlying `StorageBlock`. The index of
///                the vector represents the chunk number, and the value is a tuple of the storage block index
///                and the offset within the block.
///
/// - `blocks`: A vector of `VirtualBlock` objects that make up the file. The blocks are stored in the order
///             in which they appear in the file.
///
/// - `pool_map`: An optional `PoolMap` object that represents the mapping of data blocks to storage pools.
///               This field is skipped during serialization and deserialization.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct VirtualFile {
    /// File Size, in bytes
    pub size: usize,
    /// Chunk Size, in bytes. This is the "block size" for the file
    pub chunk_size: usize,
    /// Chunk Map, mapping each chunk of the file to it's underlying StorageBlock. Vec index is the
    /// chunk number
    /// [chunk index] => (Storage Block, Offset)
    pub(crate) chunk_map: Vec<(usize, usize)>,

    /// List of VirtualBlock, in order, that make up the file
    pub blocks: Vec<VirtualBlock>,

    #[serde(skip)]
    pool_map: Option<PoolMap>
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
            blocks: vec![],
            pool_map: None,
        }
    }

    pub fn populate(&mut self, pool_map: PoolMap)  {
        for block in self.blocks.iter_mut() {
            block.populate(pool_map.clone());
        }
        self.pool_map = Some(pool_map);
    }

    /// Flush & Unloads storage blocks from memory
    pub fn unload(&self) -> Result<(), ShmrError> {
        for block in self.blocks.iter() {
            block.drop_buffer()?;
        }
        Ok(())
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
    fn allocate_block(&mut self) -> Result<(), ShmrError> {
        let pools = match &self.pool_map {
            None => panic!("pool_map has not been populated. Unable to perform operation."),
            Some(pools) => pools
        };
        let block = VirtualBlock::new(pools, VIRTUAL_BLOCK_DEFAULT_SIZE, BlockTopology::Single)?;

        // extend the chunk map
        let block_idx = self.blocks.len();
        for i in 0..(block.size() / self.chunk_size) {
            self.chunk_map.push((block_idx, self.chunk_size * i));
        }

        self.blocks.push(block);
        Ok(())
    }

    pub fn read(&self, pos: usize, buf: &mut [u8]) -> Result<usize, ShmrError> {
        let bf = buf.len();
        if bf == 0 {
            trace!("write request buf len == 0. nothing to do");
            return Ok(0);
        }

        if pos > self.size {
            return Err(ShmrError::EndOfFile);
        }

        if self.pool_map.is_none() {
            // because it has not been populated, we can assume that the underlying Blocks have not been as well
            panic!("pool_map has not been populated. Unable to perform operation.")
        };

        // if the chunk map is empty, there's nothing to read... and the logic below falls apart
        if self.chunk_map.is_empty() {
            return Ok(0);
        }

        let mut read = 0; // amount read
        let mut chk_pos = pos % self.chunk_size; // initial chunk offset
        let pos_chk = pos / self.chunk_size;
        let buf_chk = bf / self.chunk_size + (chk_pos != 0) as usize; // got this nifty solution off Reddit. Thanks /u/HeroicKatora

        let chunk_range = pos_chk..=pos_chk + buf_chk;

        for idx in chunk_range {
            let (block_idx, block_pos) = self.chunk_map.get(idx).expect("there's nothing here") ;

            // we might get a read that starts in the middle a chunk
            // in that case, just move the block cursor forward the amount of the chunk offset
            let read_pos = *block_pos + chk_pos;
            let read_amt = cmp::min(bf, read + self.chunk_size - chk_pos);

            trace!("reading {} bytes from chunk {} (mapping to block_idx:{} / block_pos:{} / read_pos:{})", read_amt, idx, block_idx, block_pos, read_pos);

            read += &self.blocks[*block_idx].read(read_pos, &mut buf[read..read_amt])?;
            // just open up the buffer, write a copy of the block. Then... it should just work. NO. We need to start the background thread and populate the Pool Map
            // because we're holding a reference to the object. So even if the virtualfile is dropped, the block will still be in memory.
            // then when we want to evict, we can just unload the block from memory.
            // just need to make sure that we delete unloaded/stale blocks from the cache so we don't leak memory.


            chk_pos = 0;
        }

        Ok(read)
    }

    pub fn write(&mut self, pos: usize, buf: &[u8]) -> Result<usize, ShmrError> {
        let bf = buf.len();
        if bf == 0 {
            trace!("write request buf len == 0. nothing to do");
            return Ok(0);
        }

        if pos % self.chunk_size != 0 {
            panic!("cursor position does not align with block size")
        }

        if self.pool_map.is_none() {
            // because it has not been populated, we can assume that the underlying Blocks have not been as well
            panic!("pool_map has not been populated. Unable to perform operation.")
        };

        let mut written = 0;
        for chunk_idx in (pos / self.chunk_size)..=((pos + bf) / self.chunk_size) {
            // allocate a new block if we're out of space to write the chunk
            if self.chunk_map.is_empty() || self.chunk_map.len() <= chunk_idx {
                self.allocate_block()?;
            }
            let (block_idx, block_pos) = self.chunk_map.get(chunk_idx).unwrap_or_else(|| {
                panic!(
                    "chunk_idx: {}. chunk_map len: {}",
                    chunk_idx,
                    self.chunk_map.len()
                )
            });

            let buf_end = cmp::min(written + self.chunk_size, bf);
            written += self.blocks[*block_idx].write(*block_pos, &buf[written..buf_end])?;
        }

        // size is the largest (offset + written buffer)
        self.size = cmp::max(self.size, pos + bf);

        Ok(written)
    }
}

#[cfg(test)]
mod tests {
    use crate::random_data;
    use crate::tests::get_pool;
    use crate::vfs::VirtualFile;

    #[test]
    fn test_virtual_file() {
        env_logger::init();
        let pools = get_pool();

        let mut vf = VirtualFile::new();
        vf.populate(pools.clone());
        let data = random_data(8192);

        let written = vf.write(0, &data);
        assert_eq!(written.unwrap(), 8192);

        assert_eq!(vf.size(), 8192);
        assert_eq!(vf.blocks.len(), 1);

        let mut buf = vec![0u8; 8192];
        let read = vf.read(0, &mut buf);
        assert_eq!(read.unwrap(), 8192);

        assert_eq!(buf, data);
    }

    #[test]
    fn test_virtual_file_2mb() {
        let pools = get_pool();
        let amt = 1024 * 1024 * 2;

        let mut vf = VirtualFile::new();
        vf.populate(pools.clone());

        let data = random_data(amt);

        let written = vf.write(0, &data);
        assert_eq!(written.unwrap(), amt);

        // assert_eq!(vf.size(), amt);

        assert_eq!(vf.blocks.len(), 3);
    }

}
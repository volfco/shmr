pub mod block;
pub mod path;
use crate::config::ShmrFsConfig;
use crate::iostat::{IOTracker, METRIC_VFS_IO_OPERATION, METRIC_VFS_IO_OPERATION_DURATION};
use crate::vfs::block::{BlockTopology, VirtualBlock};
use crate::vfs::path::VIRTUAL_BLOCK_DEFAULT_SIZE;
use crate::{ShmrError, VFS_DEFAULT_BLOCK_SIZE};
use log::{debug, trace, warn};
use rayon::iter::{IntoParallelIterator, ParallelIterator};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::Instant;
use std::{cmp, mem};
use metrics::{counter, histogram};

fn calculate_shard_size(length: u64, data_shards: u8) -> usize {
    (length as f32 / data_shards as f32).ceil() as usize
}

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
/// - `blocks`: A vector of `VirtualBlock` objects that make up the file. The blocks are stored in the order
///             in which they appear in the file.
///
/// - `pool_map`: An optional `PoolMap` object that represents the mapping of data blocks to storage pools.
///               This field is skipped during serialization and deserialization.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct VirtualFile {
    /// Inode.
    /// Only used to generate Block Filenames
    pub ino: u64,

    /// File Size, in bytes
    pub size: u64,

    /// Chunk Size, in bytes. This is the "block size" for the file
    pub chunk_size: u64,

    /// List of VirtualBlock, in order, that make up the file
    pub blocks: Vec<VirtualBlock>,

    pub block_size: u64,

    #[serde(skip)]
    config: Option<Arc<ShmrFsConfig>>,

    #[serde(skip)]
    io_stat: IOTracker,
}
impl Default for VirtualFile {
    fn default() -> Self {
        Self::new()
    }
}
impl VirtualFile {
    pub fn new() -> Self {
        VirtualFile {
            ino: 0,
            size: 0,
            chunk_size: VFS_DEFAULT_BLOCK_SIZE,
            blocks: vec![],
            block_size: VIRTUAL_BLOCK_DEFAULT_SIZE,
            config: None,
            io_stat: IOTracker::new(),
        }
    }

    pub fn new_with(ino: u64, size: u64) -> Self {
        let mut vf = VirtualFile::new();
        vf.ino = ino;
        vf.size = size;
        vf
    }

    pub fn populate(&mut self, config: Arc<ShmrFsConfig>) {
        for block in self.blocks.iter_mut() {
            block.populate(config.clone());
        }
        self.config = Some(config);
    }

    /// Sync the data on all blocks
    pub fn sync_data(&self, force: bool) -> Result<(), ShmrError> {
        // attempt flush all blocks in parallel
        let results: Vec<Result<(), ShmrError>> = (&self.blocks)
            .into_par_iter()
            .map(|block| block.sync_data(force))
            .collect();
        // then run over the results; bubbling up any errors
        // I guess this is safer, as we attempt to flush every block before passing along errors
        for block in results {
            block?;
        }
        Ok(())
    }

    /// Drop the Block Buffers. sync_data is called on each block before the buffer is dropped.
    pub fn drop_buffers(&self) -> Result<(), ShmrError> {
        for block in self.blocks.iter() {
            block.drop_buffer()?;
        }
        Ok(())
    }

    /// Allocate a new StorageBlock then extend the chunk map
    fn allocate_block(&mut self) -> Result<(), ShmrError> {
        let pools = match &self.config {
            None => panic!("pool_map has not been populated. Unable to perform operation."),
            Some(pools) => pools,
        };
        let block_number = self.blocks.len();
        let next = block_number as u64 + 1;
        let block = VirtualBlock::create(
            self.ino,
            next,
            pools.clone(),
            self.block_size,
            BlockTopology::Single,
        )?;

        self.blocks.push(block);
        Ok(())
    }

    pub fn iostat(&self) -> (Instant, usize, usize) {
        self.io_stat.read()
    }

    pub fn read(&self, pos: u64, buf: &mut [u8]) -> Result<usize, ShmrError> {
        if self.config.is_none() {
            // because it has not been populated, we can assume that the underlying Blocks have not been as well
            panic!("pool_map has not been populated. Unable to perform operation.")
        };
        counter!(METRIC_VFS_IO_OPERATION, "operation" => "read", "inode" => self.ino.to_string()).increment(1);
        let histogram = histogram!(METRIC_VFS_IO_OPERATION_DURATION, "operation" => "read", "inode" => self.ino.to_string());
        let start = Instant::now();

        let buf_len = buf.len() as u64;
        if buf_len == 0 || self.size == 0 {
            return Ok(0);
        }

        if pos > self.size {
            return Err(ShmrError::EndOfFile);
        }

        let start_chunk = pos / self.chunk_size;
        let end_chunk = (buf_len / self.chunk_size) + start_chunk;

        let mut read: usize = 0; // amount read
        for chunk_idx in start_chunk..=end_chunk {
            let block_idx = (chunk_idx * self.chunk_size) / self.block_size;
            let block_pos = (chunk_idx * self.chunk_size) % self.block_size;

            let buf_end = cmp::min(read as u64 + self.chunk_size, buf_len) as usize;

            trace!(
                "reading {} bytes to chunk {} (mapping to block_idx:{} / block_pos:{})",
                buf_end - read,
                chunk_idx,
                block_idx,
                block_pos
            );

            read += self.blocks[block_idx as usize].read(block_pos, &mut buf[read..buf_end])?;
        }

        self.io_stat.inc_read();
        histogram.record(start.elapsed());
        Ok(read)
    }

    pub fn write(&mut self, pos: u64, buf: &[u8]) -> Result<usize, ShmrError> {
        if self.config.is_none() {
            // because it has not been populated, we can assume that the underlying Blocks have not been as well
            panic!("pool_map has not been populated. Unable to perform operation.")
        };
        counter!(METRIC_VFS_IO_OPERATION, "operation" => "write", "inode" => self.ino.to_string()).increment(1);
        let histogram = histogram!(METRIC_VFS_IO_OPERATION_DURATION, "operation" => "write", "inode" => self.ino.to_string());
        let start = Instant::now();

        let buf_len = buf.len() as u64;
        if buf_len == 0 {
            trace!("write request buf len == 0. nothing to do");
            return Ok(0);
        }

        // The torrent client writes 1 byte at a random position, which may or may not align to the
        // chunk size.
        //
        // so... we just need to make sure pos aligns to the start of the block and the remainder gets down to block_pos

        let start_chunk = pos / self.chunk_size;
        let end_chunk = (buf_len / self.chunk_size) + start_chunk;
        let chk_per_blk = self.block_size / self.chunk_size;

        let mut written: usize = 0;
        for chunk_idx in start_chunk..=end_chunk {
            // allocate blocks until we have enough blocks for this chunk
            while (self.blocks.len() as u64 * chk_per_blk) <= chunk_idx {
                self.allocate_block()?;
            }

            let block_idx = (chunk_idx * self.chunk_size) / self.block_size;
            let block_pos = (chunk_idx * self.chunk_size) % self.block_size;

            let buf_end = cmp::min(written as u64 + self.chunk_size, buf_len) as usize;

            trace!(
                "writing {} bytes to chunk {} (mapping to block_idx:{} / block_pos:{})",
                buf_end - written,
                chunk_idx,
                block_idx,
                block_pos
            );

            written += self.blocks[block_idx as usize].write(block_pos, &buf[written..buf_end])?;
        }

        // size is the largest (offset + written buffer)
        self.size = cmp::max(self.size, pos + buf_len);

        self.io_stat.inc_write();
        histogram.record(start.elapsed());
        Ok(written)
    }

    pub fn replace_block(
        &mut self,
        block_idx: usize,
        new_block: VirtualBlock,
    ) -> Result<(), ShmrError> {
        if block_idx >= self.blocks.len() {
            warn!("Requested Block Index is greater than total blocks");
            return Err(ShmrError::BlockIndexOutOfBounds);
        }
        let old_block = self.blocks.get_mut(block_idx).unwrap();

        // TODO Maybe we can improve performance by taking the old block's buffer and moving it to the new one. Would require the buffer to be up to date
        let mut block_buffer = vec![0u8; old_block.size as usize];

        debug!("reading contents of block {} into buffer", block_idx);
        old_block.read(0, &mut block_buffer)?;

        debug!("writing contents of buffer to new block");
        new_block.write(0, &block_buffer)?;
        new_block.sync_data(true)?;

        // replace the old and new blocks in the File
        let _old_block = mem::replace(old_block, new_block);

        // TODO flag the old block for deletion

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::config::{random_data, Bucket, ShmrFsConfig};
    use crate::iostat::IOTracker;
    use crate::vfs::path::VIRTUAL_BLOCK_DEFAULT_SIZE;
    use crate::vfs::VirtualFile;
    use crate::VFS_DEFAULT_BLOCK_SIZE;
    use bytesize::ByteSize;
    use std::collections::{BTreeMap, HashMap};
    use std::io::Read;
    use std::path::PathBuf;
    use std::sync::Arc;

    fn gen_virtual_file() -> VirtualFile {
        let mut buckets = HashMap::new();
        buckets.insert(
            "bucket1".to_string(),
            Bucket {
                path: PathBuf::from("/tmp"),
                capacity: 999,
                available: 999,
                priority: Default::default(),
            },
        );

        let mut pools = BTreeMap::new();
        pools.insert("test_pool".to_string(), buckets);

        VirtualFile {
            ino: rand::random(),
            size: 0,
            chunk_size: VFS_DEFAULT_BLOCK_SIZE,
            blocks: Vec::new(),
            block_size: VIRTUAL_BLOCK_DEFAULT_SIZE,
            config: Some(Arc::new(ShmrFsConfig {
                metadata_dir: Default::default(),
                mount_dir: Default::default(),
                pools,
                write_pool: "test_pool".to_string(),
                block_size: ByteSize(1024 * 1024),
            })),
            io_stat: IOTracker::default(),
        }
    }

    #[test]
    fn test_virtual_file_1() {
        let mut vf = gen_virtual_file();
        let data = random_data(7000);

        let written = vf.write(0, &data);
        assert_eq!(written.unwrap(), 7000);

        assert_eq!(vf.size, 7000);
        assert_eq!(vf.blocks.len(), 1);

        for i in 0..vf.blocks.len() {
            let mut buf = vec![0u8; vf.chunk_size as usize];
            let read = vf.blocks[i].read(0, &mut buf);
            assert_eq!(read.unwrap(), vf.chunk_size as usize);

            // Get corresponding data shard from original input data
            let shard_start = i * vf.chunk_size as usize;
            let shard_end = std::cmp::min(shard_start + vf.chunk_size as usize, data.len());

            assert_eq!(buf, &data[shard_start..shard_end]);
        }

        let mut buf = vec![0u8; 7000];
        let read = vf.read(0, &mut buf);
        assert_eq!(read.unwrap(), 7000);

        assert_eq!(buf, data);
    }
    #[test]
    fn test_virtual_file_2_4_mb() {
        env_logger::init();

        let mut vf = gen_virtual_file();
        let data = random_data(2 * 1024 * 1024);

        let _written = vf.write(0, &data);

        assert_eq!(vf.size, 2 * 1024 * 1024);
        assert_eq!(vf.blocks.len(), 3);

        assert!(vf.sync_data(true).is_ok());

        // verify the contents of the first block match the data
        let mut buf = vec![0u8; 1024 * 1024];
        let block1_shard = &vf.blocks[0].shards[0].resolve(&vf.config.unwrap()).unwrap();
        let mut file = std::fs::File::open(block1_shard.0.as_path()).unwrap();
        assert!(file.read_exact(&mut buf).is_ok());
        assert_eq!(buf, data[..1024 * 1024])
    }
}

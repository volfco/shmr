use crate::vpf::VirtualPathBuf;
use crate::{random_string, ShmrError};
use log::{debug, error, trace, warn};
use rand::Rng;
use reed_solomon_erasure::galois_8::ReedSolomon;
use serde::{Deserialize, Serialize};
use std::cmp;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, Write};
use std::path::PathBuf;

/// Represent a Single StorageBlock, the basic component of a VirtualFile.
///
/// For Single & Mirror types, the I/O operations are directly passed to the backing files.
/// For ReedSolomon types, the block size is fixed on creation.
///
/// TODO Improve Filename Generation
#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub enum StorageBlock {
    /// Single Backing File
    Single(usize, VirtualPathBuf),
    /// The contents of the StorageBlock are written to all files configured
    Mirror(usize, Vec<VirtualPathBuf>),
    ReedSolomon {
        /// Version of the Block. For now, only 1 is used
        version: u8,
        /// Data and Parity Block Configuration
        topology: (u8, u8),
        shards: Vec<VirtualPathBuf>,
        /// StorageBlock Size.
        /// Does not represent size on disk, which might be slightly larger due to padding
        size: usize,
    },
}
impl StorageBlock {
    /// Select a Bucket from the given Pool
    fn select_bucket(pool: &str, map: &PoolMap) -> String {
        // TODO eventually we will want to be smart about how something from the pool is selected...
        //      for now, random!
        let mut rng = rand::thread_rng();

        let buckets = map.get(pool).unwrap();

        let candidate = buckets.keys().nth(rng.gen_range(0..buckets.len())).unwrap();
        trace!("selected bucket {} from pool {}", candidate, pool);

        candidate.clone()
    }
    fn calculate_shard_size(length: usize, data_shards: usize) -> usize {
        (length as f32 / data_shards as f32).ceil() as usize
    }

    /// Initialize a Single StorageBlock
    pub fn init_single(pool: &str, pools: &PoolMap) -> Result<Self, ShmrError> {
        Ok(StorageBlock::Single(
            DEFAULT_STORAGE_BLOCK_SIZE,
            VirtualPathBuf {
                pool: pool.to_string(),
                bucket: Self::select_bucket(pool, pools),
                filename: format!("{}.dat", random_string()),
            },
        ))
    }

    /// Initialize a Mirror StorageBlock, with n number of copies
    pub fn init_mirror(pool: &str, map: &PoolMap, copies: usize) -> Result<Self, ShmrError> {
        let copies = (0..copies)
            .map(|_| VirtualPathBuf {
                pool: pool.to_string(),
                bucket: Self::select_bucket(pool, map),
                filename: format!("{}.dat", random_string()),
            })
            .collect();

        Ok(StorageBlock::Mirror(DEFAULT_STORAGE_BLOCK_SIZE, copies))
    }

    pub fn init_ec(pool: &str, map: &PoolMap, topology: (u8, u8), size: usize) -> Self {
        let shards = (0..topology.0 + topology.1)
            .map(|_| VirtualPathBuf {
                pool: pool.to_string(),
                bucket: Self::select_bucket(pool, map),
                filename: format!("{}.dat", random_string()),
            })
            .collect();

        StorageBlock::ReedSolomon {
            version: 1,
            topology,
            shards,
            size,
        }
    }

    pub fn is_single(&self) -> bool {
        matches!(self, StorageBlock::Single(_, _))
    }

    pub fn is_mirror(&self) -> bool {
        matches!(self, StorageBlock::Mirror(_, _))
    }

    pub fn is_ec(&self) -> bool {
        matches!(self, StorageBlock::ReedSolomon { .. })
    }

    /// Create the storage block, creating the necessary directories and files
    pub fn create(&self, engine: &IOEngine) -> Result<(), ShmrError> {
        match self {
            StorageBlock::Single(_, path) => engine.create(path),
            StorageBlock::Mirror(_, copies) => {
                for path in copies {
                    engine.create(path)?;
                }
                Ok(())
            }
            StorageBlock::ReedSolomon { shards, .. } => {
                for shard in shards {
                    engine.create(shard)?;
                }
                Ok(())
            }
        }
    }

    /// Read the contents of the StorageBlock into the given buffer starting at the given offset
    pub fn read(&self, engine: &IOEngine, offset: usize, buf: &mut [u8]) -> Result<usize, ShmrError> {
        if buf.is_empty() {
            // warn!("empty buffer passed to read function");
            return Ok(0);
        }
        match self {
            StorageBlock::Single(_, path) => {
                // path.read(map, offset, buf)
                engine.read(path, offset, buf)
            }
            StorageBlock::Mirror(_, copies) => {
                // TODO Read in parallel, and return the first successful read
                for path in copies {
                    match engine.read(path, offset, buf) {
                        Ok(result) => return Ok(result),
                        Err(e) => error!("Error reading from path: {:?}", e),
                    }
                }
                panic!("Failed to read from any of the mirror shards")
            }
            StorageBlock::ReedSolomon {
                topology,
                shards,
                size,
                ..
            } => {
                let r = ReedSolomon::new(topology.0.into(), topology.1.into())?;

                let ec_data = erasure::read(
                    &r,
                    engine,
                    shards,
                    StorageBlock::calculate_shard_size(*size, r.data_shard_count()),
                )?;
                let mut write = 0;
                for slot in 0..cmp::min(buf.len(), ec_data.len()) {
                    buf[slot] = ec_data[offset + slot];
                    write += 1;
                }
                Ok(write)
            }
        }
    }

    /// Write the contents of the buffer to the StorageBlock at the given offset
    pub fn write(&self, engine: &IOEngine, offset: usize, buf: &[u8]) -> Result<usize, ShmrError> {
        if buf.is_empty() {
            // warn!("empty buffer passed to read function");
            return Ok(0);
        }
        debug!("writing {} bytes at offset {}", buf.len(), offset);
        match self {
            StorageBlock::Single(size, path) => {
                if offset > *size {
                    return Err(ShmrError::OutOfSpace);
                }
                engine.write(path, offset, buf)
            }
            StorageBlock::Mirror(size, copies) => {
                if offset > *size {
                    return Err(ShmrError::OutOfSpace);
                }
                // TODO Write in parallel, wait for all to finish
                // Write to all the sync shards
                let mut written = 0;
                let mut i = 0;
                for path in copies {
                    written += engine.write(path, offset, buf)?;
                    i += 1;
                }
                Ok(written / i)
            }
            StorageBlock::ReedSolomon {
                topology,
                shards,
                size,
                ..
            } => {
                // TODO We need a way to keep the shard in memory
                if offset > *size {
                    return Err(ShmrError::OutOfSpace);
                }
                let r = ReedSolomon::new(topology.0.into(), topology.1.into())?;

                let shard_size = StorageBlock::calculate_shard_size(*size, r.data_shard_count());

                let mut data = erasure::read(&r, engine, shards, shard_size)?;

                // update the buffer
                data[offset..buf.len()].copy_from_slice(buf);

                Ok(erasure::write(&r, engine, shards, shard_size, data)?)
            }
        }
    }

    pub fn verify(&self, engine: &IOEngine) -> Result<bool, ShmrError> {
        match self {
            StorageBlock::Single(_, path) => Ok(path.exists(&engine.pools)),
            StorageBlock::Mirror(_, copies) => {
                Ok(copies.iter().all(|path| path.exists(&engine.pools))
                    && hash::compare(&engine.pools, copies))
            }
            StorageBlock::ReedSolomon {
                shards,
                topology,
                size,
                ..
            } => {
                let r = ReedSolomon::new(topology.0.into(), topology.1.into())?;

                // verify all shards exist
                for s in shards {
                    if !s.exists(&engine.pools) {
                        warn!("Shard does not exist: {:?}", s);
                        return Ok(false);
                    }
                }

                let shards = erasure::read_ec_shards(
                    engine,
                    shards,
                    &StorageBlock::calculate_shard_size(*size, r.data_shard_count()),
                );

                // assert that all shards are Some
                if shards.iter().any(|s| s.is_none()) {
                    return Ok(false);
                }

                let shards = shards
                    .iter()
                    .map(|s| s.as_ref().unwrap())
                    .collect::<Vec<_>>();

                Ok(r.verify(&shards)?)
            }
        }
    }

    pub fn size(&self) -> usize {
        match self {
            StorageBlock::Single(size, _) => *size,
            StorageBlock::Mirror(size, _) => *size,
            StorageBlock::ReedSolomon { size, .. } => *size,
        }
    }

    pub fn reconstruct(&self, _pool_map: &PoolMap) -> Result<(), ShmrError> {
        todo!()
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use crate::random_data;
    use crate::tests::get_pool;

    #[test]
    fn test_single_storage_block() {
        let filename = random_string();

        let engine: IOEngine = IOEngine::new("test_pool".to_string(), get_pool());

        let sb = StorageBlock::Single(
            DEFAULT_STORAGE_BLOCK_SIZE,
            VirtualPathBuf {
                pool: "test_pool".to_string(),
                bucket: "bucket1".to_string(),
                filename,
            },
        );

        // create it
        let create = sb.create(&engine);
        assert!(create.is_ok());

        // attempt to write to it
        let offset: usize = 0;
        let mut buf: Vec<u8> = vec![1, 3, 2, 4, 6, 5, 8, 7, 0, 9];

        let result = sb.write(&engine, offset, &mut buf);
        assert_eq!(result.unwrap(), buf.len());

        // read the data back
        if let StorageBlock::Single(_, path) = sb {
            assert!(path.exists(&engine.pools));

            let mut read_buffer = [0; 10];
            let result = engine.read(&path, offset, &mut read_buffer);

            assert!(result.is_ok());
            assert_eq!(read_buffer, buf.as_slice());
        }
    }

    #[test]
    fn test_mirror_storage_block() {
        let filename1 = random_string();
        let filename2 = random_string();

        let engine: IOEngine = IOEngine::new("test_pool".to_string(), get_pool());

        let sb = StorageBlock::Mirror(
            DEFAULT_STORAGE_BLOCK_SIZE,
            vec![
                VirtualPathBuf {
                    pool: "test_pool".to_string(),
                    bucket: "bucket1".to_string(),
                    filename: filename1,
                },
                VirtualPathBuf {
                    pool: "test_pool".to_string(),
                    bucket: "bucket1".to_string(),
                    filename: filename2,
                },
            ],
        );

        // create the mirror block
        let create = sb.create(&engine);
        assert!(create.is_ok());

        let mirrors_exist = match &sb {
            StorageBlock::Mirror(_, copies) => copies.iter().all(|path| path.exists(&engine.pools)),
            _ => false,
        };

        assert!(mirrors_exist, "Not all mirror shards exist on disk");

        // write some data to the mirror
        let offset: usize = 0;
        let buf: Vec<u8> = vec![1, 3, 2, 4, 6, 5, 8, 7, 0, 9];
        let op = sb.write(&engine, offset, &buf);
        assert!(op.is_ok());
        assert_eq!(op.unwrap(), buf.len());

        // read the data back
        let mut tbuf = [0; 10];
        let read = sb.read(&engine, offset, &mut tbuf);
        assert!(read.is_ok());
        assert_eq!(tbuf, buf.as_slice());

        if let StorageBlock::Mirror(_, copies) = sb {
            let mut buf1 = [0; 10];
            let mut buf2 = [0; 10];

            let read1 = engine.read(&copies[0], offset, &mut buf1);
            assert!(read1.is_ok());

            let read2 = engine.read(&copies[1], offset, &mut buf2);
            assert!(read2.is_ok());

            assert_eq!(buf2, buf1);
            assert_eq!(buf1, buf.as_slice());
            assert_eq!(buf2, buf.as_slice());
        }
    }

    #[test]
    fn test_init_ec() {
        let shard_size = 1024 * 1024 * 1; // 1MB

        let engine: IOEngine = IOEngine::new("test_pool".to_string(), get_pool());

        let ec_block = StorageBlock::init_ec(&engine.write_pool, &engine.pools, (3, 2), shard_size);
        let valid = match ec_block {
            StorageBlock::Single(..) => false,
            StorageBlock::Mirror { .. } => false,
            StorageBlock::ReedSolomon {
                version,
                topology,
                shards,
                size,
            } => {
                assert_eq!(version, 1);
                assert_eq!(topology, (3, 2));
                assert_eq!(shards.len(), 5);
                assert_eq!(size, 1024 * 1024 * 1);

                true
            }
        };

        assert!(valid, "Invalid EC block returned");
    }

    #[test]
    fn test_ec_storage_block() {
        let shard_size = 1024 * 1024 * 1; // 1MB

        let engine: IOEngine = IOEngine::new("test_pool".to_string(), get_pool());

        let ec_block = StorageBlock::init_ec(&engine.write_pool, &engine.pools, (3, 2), shard_size);

        // create the ec block
        let create = ec_block.create(&engine);
        assert!(create.is_ok());

        let ec_shards_exist = match &ec_block {
            StorageBlock::ReedSolomon { shards, .. } => {
                shards.iter().all(|path| path.exists(&engine.pools))
            }
            _ => false,
        };
        assert!(ec_shards_exist, "Not all ec shards exist on disk");

        // read the first 10 bytes to make sure it's empty
        let mut tbuf = [0; 10];
        let read = ec_block.read(&engine, 0, &mut tbuf);
        assert!(read.is_ok(), "Error reading: {:?}", read);
        assert_eq!(tbuf[0..10], [0; 10]);
    }

    #[test]
    fn test_ec_block_write_on_disk_data() {
        let data = random_data((1024 * 1024 * 1) + (1024 * 512));

        let engine: IOEngine = IOEngine::new("test_pool".to_string(), get_pool());

        let ec_block = StorageBlock::init_ec(&engine.write_pool, &engine.pools, (3, 2), data.len());

        // create the ec block
        let create = ec_block.create(&engine);
        assert!(create.is_ok());

        let write = ec_block.write(&engine, 0, &data);
        assert!(write.is_ok());

        if let StorageBlock::ReedSolomon { shards, size, .. } = ec_block {
            let shard_size = StorageBlock::calculate_shard_size(size, 3);

            let mut tbuf = [0; 524288];
            let read = engine.read(&shards[0], 0, &mut tbuf);
            assert!(read.is_ok());

            // the data from the shard should match
            assert_eq!(tbuf, data[..shard_size]);

            let mut tbuf = [0; 524288];
            let read = engine.read(&shards[1], 0, &mut tbuf);
            assert!(read.is_ok());

            assert_eq!(data[shard_size..2 * shard_size], tbuf);
        } else {
            panic!("Invalid block type");
        }
    }
}
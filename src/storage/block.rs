use crate::storage::{erasure, hash, IOEngine, PoolMap, DEFAULT_STORAGE_BLOCK_SIZE};
use crate::vpf::{VirtualPathBuf, VPB_DEFAULT_FILE_EXT};
use crate::ShmrError;
use log::{debug, error, warn};
use reed_solomon_erasure::galois_8::ReedSolomon;
use serde::{Deserialize, Serialize};
use std::cmp;
use std::fmt::Display;

/// Represent a Single StorageBlock, the basic component of a VirtualFile.
///
/// Stateless Struct. This has no running state
/// TODO Improve Filename Generation
#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub enum StorageBlock {
    /// Single Backing File
    /// TODO Can this just be merged into Mirror. So it's a Mirror with one Shard
    Single {
        uuid: uuid::Uuid,
        size: usize,
        shard: VirtualPathBuf,
    },
    /// The contents of the StorageBlock are written to all files configured
    Mirror {
        uuid: uuid::Uuid,
        size: usize,
        shards: Vec<VirtualPathBuf>,
    },
    ReedSolomon {
        uuid: uuid::Uuid,
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
    fn calculate_shard_size(length: usize, data_shards: usize) -> usize {
        (length as f32 / data_shards as f32).ceil() as usize
    }

    /// Initialize a Single StorageBlock
    pub fn init_single(engine: &IOEngine) -> Result<Self, ShmrError> {
        let pool = engine.write_pool.clone();
        let mut buckets = engine.select_buckets(&pool, 1)?;
        let uuid = uuid::Uuid::new_v4();

        Ok(StorageBlock::Single {
            uuid: Default::default(),
            size: DEFAULT_STORAGE_BLOCK_SIZE,
            shard: VirtualPathBuf {
                pool,
                bucket: buckets.pop().unwrap(),
                filename: format!("{}_single.{}", uuid, VPB_DEFAULT_FILE_EXT),
            },
        })
    }

    /// Initialize a Mirror StorageBlock, with n number of copies
    pub fn init_mirror(engine: &IOEngine, n: usize) -> Result<Self, ShmrError> {
        let pool = engine.write_pool.clone();
        let mut buckets = engine.select_buckets(&pool, n)?;
        let mut shards = vec![];
        let uuid = uuid::Uuid::new_v4();

        for i in 0..n {
            let vpf = VirtualPathBuf {
                pool: pool.clone(),
                bucket: buckets.pop().unwrap(),
                filename: format!("{}_mirror_{}_{}.{}", uuid, i, n, VPB_DEFAULT_FILE_EXT),
            };
            shards.push(vpf);
        }

        Ok(StorageBlock::Mirror {
            uuid,
            size: DEFAULT_STORAGE_BLOCK_SIZE,
            shards,
        })
    }

    pub fn init_ec(engine: &IOEngine, topology: (u8, u8), size: usize) -> Result<Self, ShmrError> {
        let pool = engine.write_pool.clone();
        let mut buckets = engine.select_buckets(&pool, (topology.0 + topology.1) as usize)?;
        let mut shards = vec![];
        let uuid = uuid::Uuid::new_v4();

        // the baked in assumption is that EC Shards are always ordered by DATA then PARITY
        for i in 0..topology.0 {
            let vpf = VirtualPathBuf {
                pool: pool.clone(),
                bucket: buckets.pop().unwrap(),
                filename: format!(
                    "{}_ec_{}_{}_d{}.{}",
                    uuid, topology.0, topology.1, i, VPB_DEFAULT_FILE_EXT
                ),
            };
            shards.push(vpf);
        }
        for i in 0..topology.1 {
            let vpf = VirtualPathBuf {
                pool: pool.clone(),
                bucket: buckets.pop().unwrap(),
                filename: format!(
                    "{}_ec_{}_{}_p{}.{}",
                    uuid, topology.0, topology.1, i, VPB_DEFAULT_FILE_EXT
                ),
            };
            shards.push(vpf);
        }

        Ok(StorageBlock::ReedSolomon {
            version: 1,
            uuid,
            topology,
            shards,
            size,
        })
    }

    pub fn is_single(&self) -> bool {
        matches!(self, StorageBlock::Single { .. })
    }

    pub fn is_mirror(&self) -> bool {
        matches!(self, StorageBlock::Mirror { .. })
    }

    pub fn is_ec(&self) -> bool {
        matches!(self, StorageBlock::ReedSolomon { .. })
    }

    /// Create the storage block, creating the necessary directories and files
    pub fn create(&self, engine: &IOEngine) -> Result<(), ShmrError> {
        match self {
            StorageBlock::Single { shard, .. } => engine.create(shard),
            StorageBlock::Mirror { shards, .. } => {
                for path in shards {
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
    pub fn read(
        &self,
        engine: &IOEngine,
        offset: usize,
        buf: &mut [u8],
    ) -> Result<usize, ShmrError> {
        if buf.is_empty() {
            // warn!("empty buffer passed to read function");
            return Ok(0);
        }
        match self {
            StorageBlock::Single { shard, .. } => engine.read(shard, offset, buf),
            StorageBlock::Mirror { shards, .. } => {
                // TODO Read in parallel, and return the first successful read
                for path in shards {
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
            StorageBlock::Single { size, shard, .. } => {
                if offset > *size {
                    return Err(ShmrError::OutOfSpace);
                }
                engine.write(shard, offset, buf)
            }
            StorageBlock::Mirror { shards, size, .. } => {
                if offset > *size {
                    return Err(ShmrError::OutOfSpace);
                }
                // TODO Write in parallel, wait for all to finish
                // Write to all the sync shards
                let mut written = 0;
                let mut i = 0;
                for path in shards {
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
            StorageBlock::Single { shard, .. } => Ok(shard.exists(&engine.pools)),
            StorageBlock::Mirror { shards, .. } => {
                Ok(shards.iter().all(|path| path.exists(&engine.pools))
                    && hash::compare(&engine.pools, shards))
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
            StorageBlock::Single { size, .. } => *size,
            StorageBlock::Mirror { size, .. } => *size,
            StorageBlock::ReedSolomon { size, .. } => *size,
        }
    }

    pub fn reconstruct(&self, _pool_map: &PoolMap) -> Result<(), ShmrError> {
        todo!()
    }
}
impl Display for StorageBlock {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            // TODO Add UUID to display output
            StorageBlock::Single { shard, .. } => write!(f, "Single({})", shard),
            StorageBlock::Mirror { shards, .. } => write!(f, "Mirror({:?})", shards),
            StorageBlock::ReedSolomon {
                version,
                topology,
                shards,
                size,
                ..
            } => {
                write!(
                    f,
                    "ReedSolomon(version: {}, topology: {:?}, shards: {:?}, size: {})",
                    version, topology, shards, size
                )
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tests::get_pool;
    use crate::{random_data, random_string};

    #[test]
    fn test_single_storage_block() {
        let filename = random_string();

        let engine: IOEngine = IOEngine::new("test_pool".to_string(), get_pool());

        let sb = StorageBlock::init_single(&engine).unwrap();

        // create it
        let create = sb.create(&engine);
        assert!(create.is_ok());

        // attempt to write to it
        let offset: usize = 0;
        let mut buf: Vec<u8> = vec![1, 3, 2, 4, 6, 5, 8, 7, 0, 9];

        let result = sb.write(&engine, offset, &mut buf);
        assert_eq!(result.unwrap(), buf.len());

        // read the data back
        if let StorageBlock::Single { shard, .. } = sb {
            assert!(shard.exists(&engine.pools));

            let mut read_buffer = [0; 10];
            let result = engine.read(&shard, offset, &mut read_buffer);

            assert!(result.is_ok());
            assert_eq!(read_buffer, buf.as_slice());
        }
    }

    #[test]
    fn test_mirror_storage_block() {
        let filename1 = random_string();
        let filename2 = random_string();

        let engine: IOEngine = IOEngine::new("test_pool".to_string(), get_pool());

        let sb = StorageBlock::init_mirror(&engine, 2).unwrap();

        // create the mirror block
        let create = sb.create(&engine);
        assert!(create.is_ok());

        let mirrors_exist = match &sb {
            StorageBlock::Mirror { shards, .. } => {
                shards.iter().all(|path| path.exists(&engine.pools))
            }
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

        if let StorageBlock::Mirror { shards, .. } = sb {
            let mut buf1 = [0; 10];
            let mut buf2 = [0; 10];

            let read1 = engine.read(&shards[0], offset, &mut buf1);
            assert!(read1.is_ok());

            let read2 = engine.read(&shards[1], offset, &mut buf2);
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

        let ec_block = StorageBlock::init_ec(&engine, (3, 2), shard_size).unwrap();
        let valid = match ec_block {
            StorageBlock::Single { .. } => false,
            StorageBlock::Mirror { .. } => false,
            StorageBlock::ReedSolomon {
                version,
                topology,
                shards,
                size,
                ..
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

        let ec_block = StorageBlock::init_ec(&engine, (3, 2), shard_size).unwrap();

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

        let ec_block = StorageBlock::init_ec(&engine, (3, 2), data.len()).unwrap();

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

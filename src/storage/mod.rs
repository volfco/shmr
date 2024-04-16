use crate::random_string;
use crate::vpf::VirtualPathBuf;
use anyhow::Result;
use log::{debug, trace, warn};
use rand::Rng;
use reed_solomon_erasure::galois_8::ReedSolomon;
use rkyv::{Archive, Deserialize, Serialize};
use std::collections::HashMap;
use std::io::Write;
use std::path::PathBuf;

pub mod erasure;
mod hash;
pub mod ops;

/// Represent a Single StorageBlock, the basic component of a VirtualFile.
///
/// For Single & Mirror types, the I/O operations are directly passed to the backing files.
/// For ReedSolomon types, the block size is fixed on creation.
///
/// TODO Improve Filename Generation
#[derive(Debug, Archive, Serialize, Deserialize, Clone, PartialEq)]
#[archive(compare(PartialEq), check_bytes)]
pub enum StorageBlock {
    /// Single Backing File
    Single(VirtualPathBuf),
    /// The contents of the StorageBlock are written to all files configured
    Mirror(Vec<VirtualPathBuf>),
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
    fn select_pool(pool: &HashMap<String, PathBuf>) -> &String {
        // TODO eventually we will want to be smart about how something from the pool is selected...
        //      for now, random!
        let mut rng = rand::thread_rng();
        let candidate = pool.keys().nth(rng.gen_range(0..pool.len())).unwrap();
        trace!("Selected pool: {:?}", candidate);

        candidate
    }
    fn calculate_shard_size(length: usize, data_shards: usize) -> usize {
        (length as f32 / data_shards as f32).ceil() as usize
    }

    /// Initialize a Single StorageBlock
    pub fn init_single(pool: &HashMap<String, PathBuf>) -> Result<Self> {
        Ok(StorageBlock::Single(VirtualPathBuf {
            pool: Self::select_pool(pool).clone(),
            filename: format!("{}.dat", random_string()),
        }))
    }

    /// Initialize a Mirror StorageBlock, with n number of copies
    pub fn init_mirror(pool: &HashMap<String, PathBuf>, copies: usize) -> Result<Self> {
        let copies = (0..copies)
            .map(|_| VirtualPathBuf {
                pool: Self::select_pool(pool).clone(),
                filename: format!("{}.dat", random_string()),
            })
            .collect();

        Ok(StorageBlock::Mirror(copies))
    }

    pub fn init_ec(pool: &HashMap<String, PathBuf>, topology: (u8, u8), size: usize) -> Self {
        let shards = (0..topology.0 + topology.1)
            .map(|_| VirtualPathBuf {
                pool: Self::select_pool(pool).clone(),
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

    /// Create the storage block, creating the necessary directories and files
    pub fn create(&self, pool_map: &HashMap<String, PathBuf>) -> Result<()> {
        match self {
            StorageBlock::Single(path) => path.create(pool_map),
            StorageBlock::Mirror(copies) => {
                for path in copies {
                    path.create(pool_map)?;
                }
                Ok(())
            }
            StorageBlock::ReedSolomon { shards, .. } => {
                for shard in shards {
                    shard.create(pool_map)?;
                }
                Ok(())
            }
        }
    }

    /// Read the contents of the StorageBlock into the given buffer starting at the given offset
    pub fn read(
        &self,
        pool_map: &HashMap<String, PathBuf>,
        offset: usize,
        buf: &mut Vec<u8>,
    ) -> Result<usize> {
        match self {
            StorageBlock::Single(path) => path.read(pool_map, offset, buf),
            StorageBlock::Mirror(copies) => {
                // TODO Read in paralle, and return the first successful read
                for path in copies {
                    match path.read(pool_map, offset, buf) {
                        Ok(result) => return Ok(result),
                        Err(e) => eprintln!("Error reading from path: {:?}", e),
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
                    pool_map,
                    shards,
                    StorageBlock::calculate_shard_size(*size, r.data_shard_count()),
                )?;

                Ok(buf.write(&ec_data[offset..*size])?)
            }
        }
    }

    /// Write the contents of the buffer to the StorageBlock at the given offset
    pub fn write(
        &self,
        pool_map: &HashMap<String, PathBuf>,
        offset: usize,
        buf: &[u8],
    ) -> Result<usize> {
        debug!("writing {} bytes at offset {}", buf.len(), offset);
        match self {
            StorageBlock::Single(path) => path.write(pool_map, offset, buf),
            StorageBlock::Mirror(copies) => {
                // TODO Write in parallel, wait for all to finish
                // Write to all the sync shards
                let mut written = 0;
                let mut i = 0;
                for path in copies {
                    written += path.write(pool_map, offset, buf)?;
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
                let r = ReedSolomon::new(topology.0.into(), topology.1.into())?;

                let shard_size = StorageBlock::calculate_shard_size(*size, r.data_shard_count());

                let mut data = erasure::read(&r, pool_map, shards, shard_size)?;

                // update the buffer
                data[offset..buf.len()].copy_from_slice(buf);

                Ok(erasure::write(&r, pool_map, shards, shard_size, data)?)
            }
        }
    }

    pub fn verify(&self, pool_map: &HashMap<String, PathBuf>) -> Result<bool> {
        match self {
            StorageBlock::Single(path) => Ok(path.exists(pool_map)),
            StorageBlock::Mirror(copies) => {
                Ok(copies.iter().all(|path| path.exists(pool_map))
                    && hash::compare(pool_map, copies))
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
                    if !s.exists(pool_map) {
                        warn!("Shard does not exist: {:?}", s);
                        return Ok(false);
                    }
                }

                let shards = erasure::read_ec_shards(
                    pool_map,
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

    pub fn reconstruct(&self, _pool_map: &HashMap<String, PathBuf>) -> Result<()> {
        todo!()
    }
}

#[cfg(test)]
mod tests {
    use crate::storage::StorageBlock;
    use crate::vpf::VirtualPathBuf;
    use crate::{random_data, random_string};
    use std::collections::HashMap;
    use std::path::{Path, PathBuf};

    // TODO Add tests for verifying the data

    #[test]
    fn test_single_storage_block() {
        let temp_dir = Path::new("/tmp");
        let filename = random_string();

        let mut pool_map: HashMap<String, PathBuf> = HashMap::new();
        pool_map.insert("test_pool".to_string(), temp_dir.to_path_buf());

        let sb = StorageBlock::Single(VirtualPathBuf {
            pool: "test_pool".to_string(),
            filename,
        });

        // create it
        let create = sb.create(&pool_map);
        assert!(create.is_ok());

        // attempt to write to it
        let offset: usize = 0;
        let mut buf: Vec<u8> = vec![1, 3, 2, 4, 6, 5, 8, 7, 0, 9];

        let result = sb.write(&pool_map, offset, &mut buf);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), buf.len());

        // read the data back
        if let StorageBlock::Single(path) = sb {
            assert!(path.exists(&pool_map));

            let mut read_buffer = vec![];
            let result = path.read(&pool_map, offset, &mut read_buffer);

            assert!(result.is_ok());
            assert_eq!(read_buffer, buf);
        }
    }

    #[test]
    fn test_mirror_storage_block() {
        let temp_dir = Path::new("/tmp");
        let filename1 = random_string();
        let filename2 = random_string();

        let mut pool_map: HashMap<String, PathBuf> = HashMap::new();
        pool_map.insert("test_pool".to_string(), temp_dir.to_path_buf());

        let sb = StorageBlock::Mirror(vec![
            VirtualPathBuf {
                pool: "test_pool".to_string(),
                filename: filename1,
            },
            VirtualPathBuf {
                pool: "test_pool".to_string(),
                filename: filename2,
            },
        ]);

        // create the mirror block
        let create = sb.create(&pool_map);
        assert!(create.is_ok());

        let mirrors_exist = match &sb {
            StorageBlock::Mirror(copies) => copies.iter().all(|path| path.exists(&pool_map)),
            _ => false,
        };

        assert!(mirrors_exist, "Not all mirror shards exist on disk");

        // write some data to the mirror
        let offset: usize = 0;
        let buf: Vec<u8> = vec![1, 3, 2, 4, 6, 5, 8, 7, 0, 9];
        let op = sb.write(&pool_map, offset, &buf);
        assert!(op.is_ok());
        assert_eq!(op.unwrap(), buf.len());

        // read the data back
        let mut tbuf = vec![];
        let read = sb.read(&pool_map, offset, &mut tbuf);
        assert!(read.is_ok());
        assert_eq!(tbuf, buf);

        if let StorageBlock::Mirror(copies) = sb {
            let mut buf1 = vec![];
            let mut buf2 = vec![];

            let read1 = copies[0].read(&pool_map, offset, &mut buf1);
            assert!(read1.is_ok());

            let read2 = copies[1].read(&pool_map, offset, &mut buf2);
            assert!(read2.is_ok());

            assert_eq!(buf2, buf1);
            assert_eq!(buf1, buf);
            assert_eq!(buf2, buf);
        }
    }

    #[test]
    fn test_init_ec() {
        let shard_size = 1024 * 1024 * 1; // 1MB

        let mut pool_map: HashMap<String, PathBuf> = HashMap::new();
        pool_map.insert("test_pool".to_string(), PathBuf::from("/tmp"));

        let ec_block = StorageBlock::init_ec(&pool_map, (3, 2), shard_size);
        let valid = match ec_block {
            StorageBlock::Single(_) => false,
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

        let mut pool_map: HashMap<String, PathBuf> = HashMap::new();
        pool_map.insert("test_pool".to_string(), PathBuf::from("/tmp"));

        let ec_block = StorageBlock::init_ec(&pool_map, (3, 2), shard_size);

        // create the ec block
        let create = ec_block.create(&pool_map);
        assert!(create.is_ok());

        let ec_shards_exist = match &ec_block {
            StorageBlock::ReedSolomon { shards, .. } => {
                shards.iter().all(|path| path.exists(&pool_map))
            }
            _ => false,
        };
        assert!(ec_shards_exist, "Not all ec shards exist on disk");

        // read the first 10 bytes to make sure it's empty
        let mut tbuf = vec![];
        let read = ec_block.read(&pool_map, 0, &mut tbuf);
        assert!(read.is_ok(), "Error reading: {:?}", read);
        assert_eq!(tbuf[0..10], vec![0; 10]);
    }

    #[test]
    fn test_ec_block_write_on_disk_data() {
        let data = random_data((1024 * 1024 * 1) + (1024 * 512));

        let mut pool_map: HashMap<String, PathBuf> = HashMap::new();
        pool_map.insert("test_pool".to_string(), PathBuf::from("/tmp"));

        let ec_block = StorageBlock::init_ec(&pool_map, (3, 2), data.len());

        // create the ec block
        let create = ec_block.create(&pool_map);
        assert!(create.is_ok());

        let write = ec_block.write(&pool_map, 0, &data);
        assert!(write.is_ok());
        // assert_eq!(write.unwrap(), data.len());

        if let StorageBlock::ReedSolomon { shards, size, .. } = ec_block {
            let shard_size = StorageBlock::calculate_shard_size(size, 3);

            let mut tbuf = vec![];
            let read = shards[0].read(&pool_map, 0, &mut tbuf);
            assert!(read.is_ok());

            // the data from the shard should match
            assert_eq!(tbuf, data[..shard_size]);

            let mut tbuf = vec![];
            let read = shards[1].read(&pool_map, 0, &mut tbuf);
            assert!(read.is_ok());

            assert_eq!(data[shard_size..2 * shard_size], tbuf);
        } else {
            panic!("Invalid block type");
        }
    }
}

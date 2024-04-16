use std::collections::HashMap;
use std::io::Write;
use std::path::PathBuf;
use log::debug;
use rand::Rng;
use reed_solomon_erasure::galois_8::ReedSolomon;
use crate::vpf::VirtualPathBuf;
use rkyv::{Archive, Deserialize, Serialize};
use anyhow::Result;
use crate::random_string;

pub mod erasure;
mod hash;
pub mod ops;

#[derive(Debug, Archive, Serialize, Deserialize, Clone, PartialEq)]
#[archive(
  compare(PartialEq),
  check_bytes,
)]
pub enum StorageBlock {
  /// We're just passing writes through to the underlying file
  Single(VirtualPathBuf),
  Mirror {
    sync: Vec<VirtualPathBuf>,
    r#async: Vec<VirtualPathBuf>
  },
  ReedSolomon {
    version: u8,
    /// Data and Parity Blocks
    topology: (u8, u8),
    shards: Vec<VirtualPathBuf>,
    /// StorageBlock Size.
    /// Does not represent size on disk, which might be slightly larger due to padding
    size: usize
  }
}
impl StorageBlock {

  fn select_pool(pool: &HashMap<String, PathBuf>) -> &String {
    // TODO eventually we will want to be smart about how something from the pool is selected...
    //      for now, random!
    let mut rng = rand::thread_rng();
    let candidate = pool.keys().nth(rng.gen_range(0..pool.len())).unwrap();
    debug!("Selected pool: {:?}", candidate);

    candidate
  }

  pub fn init_single(pool: &HashMap<String, PathBuf>) -> Result<Self> {
    Ok(StorageBlock::Single(VirtualPathBuf {
      pool: Self::select_pool(pool).clone(),
      filename: format!("{}.dat", random_string())
    }))
  }

  pub fn init_ec(pool: &HashMap<String, PathBuf>, topology: (u8, u8), size: usize) -> Self {
    let shards = (0..topology.0 + topology.1).map(|_| VirtualPathBuf {
      pool: Self::select_pool(pool).clone(),
      filename: format!("{}.dat", random_string())
    }).collect();

    StorageBlock::ReedSolomon {
      version: 1,
      topology,
      shards,
      size
    }
  }

  pub fn calculate_shard_size(length: usize, data_shards: usize) -> usize {
    (length as f32 / data_shards as f32).ceil() as usize
  }

  /// Create the storage block, creating the necessary directories and files
  pub fn create(&self, pool_map: &HashMap<String, PathBuf>) -> Result<()> {
    match self {
      StorageBlock::Single(path) => path.create(pool_map),
      StorageBlock::Mirror { sync, r#async } => {
        for path in sync.iter().chain(r#async.iter()) {
          path.create(pool_map)?;
        }
        // TODO: Spawn the async writes into a thread so they continue in the background
        Ok(())
      },
      StorageBlock::ReedSolomon { shards, .. } => {
        for shard in shards {
          shard.create(pool_map)?;
        }
        Ok(())
      }
    }
  }

  pub fn read(&self, pool_map: &HashMap<String, PathBuf>, offset: usize, buf: &mut Vec<u8>) -> Result<usize> {
    match self {
      StorageBlock::Single(path) => path.read(pool_map, offset, buf),
      StorageBlock::Mirror { sync, .. } => {
        // TODO Eventually, reads will be issued to all shards and the first one to return will be
        //      used... and eventually the results will be compared if they both arrive quick enough

        // for now, just try and read from the sync disks in order
        for path in sync {
          match path.read(pool_map, offset, buf) {
            Ok(result) => return Ok(result),
            Err(e) => eprintln!("Error reading from path: {:?}", e)
          }
        }
        panic!("Failed to read from any of the mirror shards")
      },
      StorageBlock::ReedSolomon { topology, shards, size, .. } => {
        let r = ReedSolomon::new(topology.0.into(), topology.1.into())?;

        // read the shards
        let ec_shards = erasure::read_ec_shards(&r, pool_map, shards, &StorageBlock::calculate_shard_size(size.clone(), r.data_shard_count()))?;
        let mut flattened = vec![];
        for shard in ec_shards[0..r.data_shard_count()].iter() {
          flattened.write_all(&shard)?;
        }

        Ok(buf.write(&flattened[offset..size.clone()])?)
      }
    }
  }

  pub fn write(&self, pool_map: &HashMap<String, PathBuf>, offset: usize, buf: &[u8]) -> Result<usize> {
    debug!("writing {} bytes at offset {}", buf.len(), offset);
    match self {
      StorageBlock::Single(path) => path.write(pool_map, offset, buf),
      StorageBlock::Mirror { sync, r#async } => {
        // Write to all the sync shards
        let mut written = 0;
        let mut i = 0;
        for path in sync.iter().chain(r#async.iter()) {
          written += path.write(pool_map, offset, buf)?;
          i += 1;
        }
        // TODO: Spawn the async writes into a thread so they continue in the background
        Ok(written / i)
      },
      StorageBlock::ReedSolomon { topology, shards, size, .. } => {
        let r = ReedSolomon::new(topology.0.into(), topology.1.into())?;

        let shard_size = StorageBlock::calculate_shard_size(size.clone(), r.data_shard_count());

        let mut data = erasure::read(&r, pool_map, shards, shard_size)?;

        // update the buffer
        data[offset..buf.len()].copy_from_slice(buf);

        Ok(erasure::write(&r, pool_map, shards, shard_size,  data)?)
      }
    }
  }

  pub fn verify(&self, pool_map: &HashMap<String, PathBuf>) -> Result<bool> {
    match self {
      StorageBlock::Single(path) => Ok(path.exists(pool_map)),
      StorageBlock::Mirror { sync, r#async } => {
        let sync_exist = sync.iter().all(|path| path.exists(pool_map));
        let async_exist = r#async.iter().all(|path| path.exists(pool_map));

        // TODO: Compare the async shards as well

        Ok(sync_exist && async_exist && hash::compare(pool_map, sync))
      },
      StorageBlock::ReedSolomon { shards, topology, size, .. } => {
        // let r = ReedSolomon::new(topology.0.into(), topology.1.into())?;
        //
        // let shards: Vec<Vec<u8>> = shards.iter().map(|path| {
        //   let mut buffer = vec![0; *shard_size];
        //   if let Err(e) = path.read(pool_map, 0, &mut buffer) {
        //     println!("Error reading shard: {:?}", e);
        //   }
        //   // if we were unable to read the file, or the file had nothing in it
        //   if buffer.len() != *shard_size {
        //     buffer = vec![0; *shard_size];
        //   }
        //
        //   buffer
        // }).collect();
        //
        // Ok(r.verify(&shards)?)
        todo!()
      }
    }
  }
}

#[cfg(test)]
mod tests {
  use std::collections::HashMap;
  use std::path::{Path, PathBuf};
  use log::debug;
  use crate::{random_data, random_string};
  use crate::storage::StorageBlock;
  use crate::vpf::VirtualPathBuf;

  #[test]
  fn test_single_storage_block() {
    let temp_dir = Path::new("/tmp");
    let filename = random_string();

    let mut pool_map: HashMap<String, PathBuf> = HashMap::new();
    pool_map.insert("test_pool".to_string(), temp_dir.to_path_buf());

    let sb = StorageBlock::Single(VirtualPathBuf {
      pool: "test_pool".to_string(),
      filename
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

    let sb = StorageBlock::Mirror {
      sync: vec![VirtualPathBuf {
        pool: "test_pool".to_string(),
        filename: filename1
      }, VirtualPathBuf {
        pool: "test_pool".to_string(),
        filename: filename2
      }],
      r#async: vec![]
    };

    // create the mirror block
    let create = sb.create(&pool_map);
    assert!(create.is_ok());

    let mirrors_exist = match &sb {
      StorageBlock::Mirror { sync, r#async } => {
        sync.iter().all(|path| path.exists(&pool_map)) && r#async.iter().all(|path| path.exists(&pool_map))
      },
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

    if let StorageBlock::Mirror { sync, .. } = sb {

      let mut buf1 = vec![];
      let mut buf2 = vec![];

      let read1 = sync[0].read(&pool_map, offset, &mut buf1);
      assert!(read1.is_ok());

      let read2 = sync[1].read(&pool_map, offset, &mut buf2);
      assert!(read2.is_ok());


      assert_eq!(buf2, buf1);
      assert_eq!(buf1, buf);
      assert_eq!(buf2, buf);
    }
  }

  #[test]
  fn test_init_ec() {
    let shard_size = 1024 * 1024 * 1;  // 1MB

    let mut pool_map: HashMap<String, PathBuf> = HashMap::new();
    pool_map.insert("test_pool".to_string(), PathBuf::from("/tmp"));

    let ec_block = StorageBlock::init_ec(&pool_map, (3, 2), shard_size);
    let valid = match ec_block {
      StorageBlock::Single(_) => false,
      StorageBlock::Mirror { .. } => false,
      StorageBlock::ReedSolomon { version, topology, shards, size } => {
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
    let shard_size = 1024 * 1024 * 1;  // 1MB

    let mut pool_map: HashMap<String, PathBuf> = HashMap::new();
    pool_map.insert("test_pool".to_string(), PathBuf::from("/tmp"));

    let ec_block = StorageBlock::init_ec(&pool_map, (3, 2), shard_size);

    // create the ec block
    let create = ec_block.create(&pool_map);
    assert!(create.is_ok());

    let ec_shards_exist = match &ec_block {
      StorageBlock::ReedSolomon { shards, .. } => {
        shards.iter().all(|path| path.exists(&pool_map))
      },
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

      assert_eq!(data[shard_size..2*shard_size], tbuf);

    } else {
      panic!("Invalid block type");
    }
  }

}
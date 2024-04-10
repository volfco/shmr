use std::collections::HashMap;
use std::io::Write;
use std::path::PathBuf;
use log::error;
use reed_solomon_erasure::galois_8::ReedSolomon;
use crate::vpf::VirtualPathBuf;

pub mod erasure;

pub type StorageBlockMap = HashMap<u64, StorageBlock>;

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
    /// Size of each shard, used for file layout stuff
    shard_size: usize,
    /// Size of the Content stored in the block
    size: usize,
  }
}
impl StorageBlock {
  pub fn calculate_shard_size(length: usize, data_shards: u8) -> usize {
    (length as f32 / data_shards as f32).ceil() as usize
  }

  /// Create the storage block, creating the necessary directories and files
  pub fn create(&self, pool_map: &HashMap<String, PathBuf>) -> anyhow::Result<()> {
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

  pub fn read(&self, pool_map: &HashMap<String, PathBuf>, offset: usize, mut buf: &mut [u8]) -> anyhow::Result<usize> {
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
      StorageBlock::ReedSolomon { topology, shards, shard_size, .. } => {
        let r = ReedSolomon::new(topology.0.into(), topology.1.into())?;

        // read the shards
        let ec_shards = erasure::read_ec_shards(&r, pool_map, shards, shard_size)?;

        // flatten the vec into a single buffer
        let mut buffer = vec![];
        for shard in ec_shards {
          buffer.write_all(&shard)?;
        }

        Ok(buf.write(&buffer[offset..])?)
      }
    }
  }

  pub fn write(&self, pool_map: &HashMap<String, PathBuf>, offset: usize, buf: &[u8]) -> anyhow::Result<usize> {
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
      StorageBlock::ReedSolomon { topology, shards, shard_size, .. } => {
        let r = ReedSolomon::new(topology.0.into(), topology.1.into())?;

        // read the shards
        let mut ec_shards = erasure::read_ec_shards(&r, pool_map, shards, shard_size)?;

        // write the buffer to the shards
        if erasure::update_ec_shards(&mut ec_shards, offset, buf)? > topology.0 {
          panic!("Too many shards updated");
        }

        // calculate the new parity shards and write all ec_shards to disk
        erasure::write_ec_shards(&r, pool_map, shards, ec_shards)?;

        Ok(buf.len()) // TODO this most likely will need to be fixed
      }
    }
  }

  pub fn verify(&self, pool_map: &HashMap<String, PathBuf>) -> anyhow::Result<bool> {
    match self {
      StorageBlock::Single(path) => Ok(path.exists(pool_map)),
      StorageBlock::Mirror { sync, r#async } => {
        let sync_exist = sync.iter().all(|path| path.exists(pool_map));
        let async_exist = r#async.iter().all(|path| path.exists(pool_map));

        // TODO Hash the files and compare

        Ok(sync_exist && async_exist)
      },
      StorageBlock::ReedSolomon { shards, topology, shard_size, .. } => {
        let r = ReedSolomon::new(topology.0.into(), topology.1.into())?;

        let mut shards: Vec<Option<Vec<u8>>> = shards.iter().map(|path| {
          let mut buffer = Vec::new();
          if let Err(e) = path.read(pool_map, 0, &mut buffer) {
            return None;
          }
          Some(buffer)
        }).collect();

        Ok(r.verify(&mut shards)?)

      }
    }
  }
}

#[cfg(test)]
mod tests {
  use crate::tests::*;

  use std::collections::HashMap;
  use std::path::{Path, PathBuf};
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

      let mut read_buffer = vec![0; 10];
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
    let mut tbuf = vec![0; 10];
    let read = sb.read(&pool_map, offset, &mut tbuf);
    assert!(read.is_ok());
    assert_eq!(tbuf, buf);

    if let StorageBlock::Mirror { sync, .. } = sb {

      let mut buf1 = vec![0; 10];
      let mut buf2 = vec![0; 10];

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
  fn test_ec_storage_block() {
    init();

    let temp_dir = Path::new("/tmp");
    let filename1 = random_string();
    let filename2 = random_string();
    let filename3 = random_string();
    let filename4 = random_string();
    let filename5 = random_string();

    let mut pool_map: HashMap<String, PathBuf> = HashMap::new();
    pool_map.insert("test_pool".to_string(), temp_dir.to_path_buf());

    let buf = random_data(1024 * 1024 * 10);
    let shard_size = 1024 * 1024 * 4; // 50 bytes per shard, so the 3rd shard should be mostly empty

    let shards = vec![VirtualPathBuf {
      pool: "test_pool".to_string(),
      filename: filename1
    }, VirtualPathBuf {
      pool: "test_pool".to_string(),
      filename: filename2
    }, VirtualPathBuf {
      pool: "test_pool".to_string(),
      filename: filename3
    }, VirtualPathBuf {
      pool: "test_pool".to_string(),
      filename: filename4
    }, VirtualPathBuf {
      pool: "test_pool".to_string(),
      filename: filename5
    }];

    let sb = StorageBlock::ReedSolomon {
      version: 1,
      topology: (3, 2), // 3 data shards, 1 parity shard
      shards: shards.clone(),
      shard_size,
      size: buf.len(),
    };

    // create the ec block
    let create = sb.create(&pool_map);
    assert!(create.is_ok());

    let ec_shards_exist = match &sb {
      StorageBlock::ReedSolomon { shards, .. } => {
        shards.iter().all(|path| path.exists(&pool_map))
      },
      _ => false,
    };
    assert!(ec_shards_exist, "Not all ec shards exist on disk");

    // read the first 10 bytes to make sure it's empty
    let mut tbuf = vec![0; 10];
    let read = sb.read(&pool_map, 0, &mut tbuf);
    assert!(read.is_ok(), "Error reading: {:?}", read);
    assert_eq!(read.unwrap(), 10);
    assert_eq!(tbuf, vec![0; 10]);

    // write stuff
    let write = sb.write(&pool_map, 0, &buf);
    assert!(write.is_ok(), "Error writing: {:?}", write);
    assert_eq!(write.unwrap(), buf.len());

    // now, delete two shards
    let rand1 = rand::random::<usize>() % shards.len();
    let rand2 = rand::random::<usize>() % shards.len();

    let _ = shards[rand1].delete(&pool_map);
    let _ = shards[rand2].delete(&pool_map);

    // read the data back
    let mut read_buffer = vec![0; buf.len()];
    let read = sb.read(&pool_map, 0, &mut read_buffer);
    assert!(read.is_ok(), "Error reading: {:?}", read);
    assert_eq!(read.unwrap(), buf.len());

  }

}
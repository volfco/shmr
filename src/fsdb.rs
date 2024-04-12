// Filesystem database
// The General idea is that we have two databases. One is the "superblock" that contains the
// metadata of the filesystem, and the other is just for storing information on the underlying storage blocks.
use anyhow::{bail, Result};
use std::path::{Path};
use log::{debug};
use rkyv::Deserialize;
use sled::Db;
use crate::storage::StorageBlock;
// use fuser::{FileAttr, FUSE_ROOT_ID};
// use crate::fuse::time_now;

// const SUPERBLOCK_DB_NAME: &str = "superblock";
const STORAGE_BLOCK_DB_NAME: &str = "blockdb";

/// Database to store storage block information.
pub struct StorageBlockDB {
  db: Db,
}
impl StorageBlockDB {
  pub fn open(path: &Path) -> Result<Self> {
    let db = sled::open(path.join(STORAGE_BLOCK_DB_NAME))?;
    Ok(Self { db })
  }

  /// Lookup & Read the given Block ID.
  /// Returns an Error when the block is not found or cannot be deserialized.
  pub fn read(&self, id: u64) -> Result<StorageBlock> {
    let entry = self.db.get(id.to_be_bytes())?;
    if entry.is_none() {
      bail!("StorageBlock {} not found", &id);
    }

    let binding = entry.unwrap();
    let archived = rkyv::check_archived_root::<StorageBlock>(&binding).unwrap();
    let result: StorageBlock = match archived.deserialize(&mut rkyv::Infallible) {
      Ok(storage_block) => storage_block,
      Err(e) => {
        // this should never happen, but you can never say never
        bail!("Failed to deserialize FileAttributes: {:?}", e);
      }
    };

    Ok(result)
  }

  /// Create a new StorageBlock entry in the database, returning the block's ID
  pub fn create(&self, descriptor: &StorageBlock) -> Result<u64> {
    let new_id = self.db.generate_id()?;
    debug!("Creating new block with ID {}", new_id);

    let pos = rkyv::to_bytes::<_, 256>(descriptor)?;
    self.db.insert(new_id.to_be_bytes(), &pos[..])?;

    Ok(new_id)
  }

  /// Update the StorageBlock entry in the database
  pub fn update(&self, id: u64, descriptor: &StorageBlock) -> Result<()> {
    let pos = rkyv::to_bytes::<_, 256>(descriptor)?;
    self.db.insert(id.to_be_bytes(), &pos[..])?;
    Ok(())
  }
}

pub struct Superblock {
  #[allow(dead_code)]
  db: Db,
}
impl Superblock {
  pub fn open(_config: ()) -> Result<Self> {
    todo!()
    // let path = config.workspace.join("superblock.db");

    // debug!("Opening superblock at {:?}", &path);
    // let db = sled::open(path)?;

    // let supa = Self {
    //   db,
    // };

    // check if the 0 inode exists. if not, we're on a fresh filesystem and we need to do some basic initialization
    // if !supa.inode_exists(&FUSE_ROOT_ID)? {
    //   info!("Initializing filesystem");

      // create the root inode
      // supa.inode_update(&FUSE_ROOT_ID, &Inode {
      //   ino: FUSE_ROOT_ID,
      //   size: 0,
      //   blocks: 0,
      //   atime: time_now(),
      //   mtime: time_now(),
      //   ctime: crate::time_now(),
      //   crtime: crate::time_now(),
      //   kind: IFileType::Directory,
      //   perm: 0o755,
      //   nlink: 1,
      //   uid: 0,
      //   gid: 0,
      //   rdev: 0,
      //   blksize: 0,
      //   flags: 0,
      // }).unwrap(); // todo this is fucky
      //
      // supa.directory_create(&FUSE_ROOT_ID, &FUSE_ROOT_ID)?;
    // }
    // Ok(supa)
  }
}
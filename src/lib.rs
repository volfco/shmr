extern crate core;

use crate::config::{ShmrError, ShmrFsConfig};
use crate::db::inode::InodeDB;
use crate::db::FileDB;
use crate::fuse::cache::FileCacheStrategy;
use crate::vfs::VirtualFile;
use dashmap::DashMap;
use log::{info, warn};
use r2d2::Pool;
use r2d2_sqlite::SqliteConnectionManager;
use rand::Rng;
use std::sync::{Arc, Mutex};

pub mod config;
mod db;
mod dbus;
mod fuse;
mod iostat;
mod vfs;
mod worker;

#[derive(Clone, Debug)]
pub struct ShmrFs {
    db: Pool<SqliteConnectionManager>,
    config: ShmrFsConfig,

    inode_db: InodeDB,
    file_db: FileDB,

    /// File Handle Map.
    /// Maps the fh (key) to the inode (value).
    /// Used to map FUSE File Handles to their associated Inode. This is used for eviction selection
    file_handles: Arc<DashMap<u64, u64>>,

    /// File Block Cache
    /// Stores VirtualBlocks, that may or may not be buffered, for quick access.
    file_cache: Arc<DashMap<u64, VirtualFile>>,

    file_cache_strategy: Arc<Mutex<FileCacheStrategy>>,
}
impl ShmrFs {
    pub fn new(config: ShmrFsConfig) -> Result<Self, ShmrError> {
        let db_path = config.metadata_dir.join("../metadata/shmr.sqlite");
        let db = Pool::new(SqliteConnectionManager::file(db_path)).unwrap();

        Ok(Self {
            inode_db: InodeDB::open(db.clone()),
            file_db: FileDB::open(db.clone()),
            file_handles: Arc::new(Default::default()),
            file_cache: Arc::new(Default::default()),
            db,
            config,
            file_cache_strategy: Arc::new(Mutex::new(FileCacheStrategy::ReadCachePriority)),
        })
    }

    /// Generate a File Handle. Guaranteed to not overlap with existing entries.
    fn gen_fh(&self) -> u64 {
        let mut rng = rand::thread_rng();

        loop {
            let fh = rng.gen::<u64>();
            if !self.file_handles.contains_key(&fh) {
                break fh;
            }
        }
    }

    /// Release the File Handle, and maybe fully flush and close the Inode
    fn release_fh(&self, fh: u64) -> Result<(), ShmrError> {
        let handle = self.file_handles.remove(&fh);

        if handle.is_none() {
            warn!("attempted to release a non-existent filehandle ({}).", fh);
            return Ok(());
        }

        let (_, inode) = handle.unwrap();

        let mut vf = self.file_cache.get_mut(&inode).unwrap();
        vf.sync_data()?;

        // scan for any other filehandles that point to the inode
        let existing = self.file_handles.iter().find(|r| r.value() == &inode);
        if existing.is_none() {
            info!(
                "Inode {} has no additional open FileHandles. Doing Something...",
                inode
            );

            // sync all stuff
            vf.sync_data()?;

            // disable the buffer which will leave the file loaded, but drop all the buffers
            vf.disable_buffer()?;
        }

        Ok(())
    }
}

#[cfg(test)]
pub mod tests {
    use crate::config::{Bucket, ShmrFsConfig};
    use std::collections::HashMap;
    use std::path::PathBuf;

    pub fn get_shmr_config() -> ShmrFsConfig {
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

        let mut pools = HashMap::new();
        pools.insert("test_pool".to_string(), buckets);

        ShmrFsConfig {
            metadata_dir: Default::default(),
            mount_dir: Default::default(),
            pools,
            write_pool: "test_pool".to_string(),
        }
    }
}

extern crate core;

use crate::config::{ShmrError, ShmrFsConfig};
use crate::db::inode::InodeDB;
use crate::db::file::FileDB;
use crate::tasks::flush::{FlushMaster, FLUSH_MASTER_DEFAULT_RUN_INTERVAL};
use crate::tasks::WorkerThread;
use crate::vfs::VirtualFile;
use dashmap::DashMap;
use log::warn;
use rand::Rng;
use std::sync::Arc;
use crate::db::get_connection;

pub mod config;
mod db;
mod dbus;
mod fuse;
mod iostat;
pub mod tasks;
mod vfs;

#[derive(Clone, Debug)]
pub struct ShmrFs {
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

    // file_cache_strategy: Arc<Mutex<FileCacheStrategy>>,

    tasks: ShmrFsTasks,
}
impl ShmrFs {
    pub fn new(config: ShmrFsConfig) -> Result<Self, ShmrError> {
        let db = get_connection(&config);
        let file_cache = Arc::new(DashMap::new());
        let inode_db = InodeDB::open(db.clone());
        let file_db = FileDB::open(db.clone());

        let tasks = ShmrFsTasks {
            flusher: WorkerThread::new(FlushMaster::new(file_db.clone(), file_cache.clone()))
                .interval(FLUSH_MASTER_DEFAULT_RUN_INTERVAL),
        };
        tasks.start();

        Ok(Self {
            inode_db,
            file_db,
            tasks,
            file_cache,
            config,
            // file_cache_strategy: Arc::new(Mutex::new(FileCacheStrategy::Read)),
            file_handles: Arc::new(Default::default()),
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
        match self.file_handles.remove(&fh) {
            None => warn!("attempted to release a non-existent filehandle ({}).", fh),
            Some(inner) => {
                let vf = self.file_cache.get(&inner.1).unwrap();
                vf.sync_data()?;
            }
        }
        Ok(())
    }
}

#[derive(Clone, Debug)]
struct ShmrFsTasks {
    flusher: WorkerThread<FlushMaster>,
}
impl ShmrFsTasks {
    fn start(&self) {
        self.flusher.spawn();
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

extern crate core;

use crate::config::{ShmrError, ShmrFsConfig};
use crate::databunny::{CompressionMethod, DataBunny};
// use crate::tasks::flush::FlushMaster;
// use crate::tasks::WorkerThread;
use crate::types::SuperblockEntry;
use dashmap::DashMap;
use rand::Rng;
use rlimit::Resource;
use std::sync::Arc;

pub mod config;
mod databunny;
mod dbus;
mod fuse;
mod iostat;
pub mod tasks;
mod types;
mod vfs;

pub const VFS_DEFAULT_BLOCK_SIZE: u64 = 4096;

#[derive(Clone, Debug)]
pub struct ShmrFs {
    pub config: ShmrFsConfig,

    superblock: DataBunny<u64, SuperblockEntry>,

    /// File Handle Map.
    /// Maps the fh (key) to the inode (value).
    /// Used to map FUSE File Handles to their associated Inode. This is used for eviction selection
    file_handles: Arc<DashMap<u64, u64>>,
}
impl ShmrFs {
    pub fn new(config: ShmrFsConfig) -> Result<Self, ShmrError> {
        // set limits
        Resource::NOFILE
            .set(102400, 409600)
            .expect("unable to set NOFILE limits");

        let shmr = Self {
            superblock: DataBunny::open(&config.metadata_dir, CompressionMethod::Zstd(20)).unwrap(),
            config,
            // file_cache_strategy: Arc::new(Mutex::new(FileCacheStrategy::Read)),
            file_handles: Arc::new(Default::default()),
        };

        let dbus_shmr = shmr.clone();
        std::thread::spawn(move || dbus::dbus_server(dbus_shmr));

        Ok(shmr)
    }

    /// Generate Inode Number
    fn gen_ino(&self) -> u64 {
        self.superblock.last_key() + 1
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
    fn release_fh(&self, _fh: u64) -> Result<(), ShmrError> {
        Ok(())
    }

    fn check_access(&self, inode: u64, uid: u32, gid: u32, access_mask: i32) -> bool {
        self.superblock.has(&inode)
            && self
                .superblock
                .get(&inode)
                .unwrap()
                .unwrap()
                .inode
                .check_access(uid, gid, access_mask)
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

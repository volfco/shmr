#![feature(lazy_cell)]

use std::collections::HashMap;
use rand::Rng;
use std::io::Error;
use std::path::PathBuf;
use log::debug;
use serde::{Deserialize, Serialize};
use sysinfo::Disks;

pub mod fsdb;
pub mod kernel;
pub mod vfs;

pub mod fuse;
pub mod iotracker;

/// Pool -> Bucket -> Base Directory
#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub struct PoolMap {
    write_pool: String,
    entries: HashMap<String, HashMap<String, Bucket>>
}
impl PoolMap {
    pub fn write_pool(&self) -> String {
        self.write_pool.clone()
    }
    /// Select n Buckets from the given Pool and return the names.
    pub fn select_buckets(&self, count: usize) -> Result<Vec<String>, ShmrError> {
        // TODO This function is ugly and could be refactored

        // for now, we are going to return the pools in order of Priority & Free Space Percentage
        let mut possible_buckets: Vec<(&String, &Bucket)> = self
            .entries
            .get(&self.write_pool)
            .ok_or(ShmrError::InvalidPoolId)?
            .iter()
            .filter(|(_, bucket)| bucket.priority > BucketPriority::IGNORE)
            .collect();

        possible_buckets.sort_by(|a, b| {
            a.1.priority
                .partial_cmp(&b.1.priority)
                .unwrap()
                .then(a.1.available.partial_cmp(&b.1.available).unwrap())
        });

        // if we are requesting more buckets

        // if the number of entries in the possible buckets to return is less than the requested
        // count... just duplicate the list until it has more than the requested amount.
        // This way we will still get even-ish distribution
        let copy = possible_buckets.clone();
        while possible_buckets.len() < count {
            possible_buckets.extend(copy.iter().cloned());
        }

        #[allow(suspicious_double_ref_op)]
        let selected_buckets: Vec<String> = possible_buckets
            .iter()
            .take(count)
            .map(|(bucket_name, _)| bucket_name.clone().clone())
            .collect();

        debug!("selected the following buckets: {:?}", &selected_buckets);

        Ok(selected_buckets)
    }

    pub fn get(&self, pool: &str) -> Option<&HashMap<String, Bucket>> {
        self.entries.get(pool)
    }
}


#[derive(Serialize, Deserialize, Ord, PartialOrd, Eq, PartialEq, Debug, Clone)]
pub enum BucketPriority {
    /// Prefer this bucket
    PRIORITIZE = 4,
    /// No Preference
    NORMAL = 3,
    /// Deprioritize this bucket.
    DEPRIORITIZE = 2,
    /// Ignore this Bucket. No new data will be written, but existing data will not be touched.
    IGNORE = 1,
    /// Actively Ignore this Bucket and Evacuate Data
    EVACUATE = 0,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Bucket {
    path: PathBuf,
    /// Total Space in the Bucket
    capacity: u64,
    /// Used Disk Space
    available: u64,
    /// Bucket Priority
    priority: BucketPriority,
}
impl Bucket {
    pub fn new(path: PathBuf) -> Self {
        Bucket {
            path,
            capacity: 0,
            available: 0,
            priority: BucketPriority::NORMAL,
        }
    }

    /// Given a list of [`sysinfo::Disks`], update this Bucket
    pub fn update(&mut self, disks: &Disks) {
        for disk in disks.list() {
            if disk.mount_point() == self.path.as_path() {
                self.capacity = disk.total_space();
                self.available = disk.available_space();

                // there is only ever going to be one disk in the system that we're looking for
                break;
            }
        }
    }

    pub fn path(&self) -> PathBuf {
        self.path.clone()
    }
}

pub fn build_poolmap(write_pool: String, paths: HashMap<String, HashMap<String, PathBuf>>) -> PoolMap {
    let entries = paths
        .into_iter()
        .map(|(pool_name, buckets)| {
            let pool_buckets = buckets
                .into_iter()
                .map(|(bucket_name, path)| (bucket_name, Bucket::new(path)))
                .collect();
            (pool_name, pool_buckets)
        })
        .collect();

    PoolMap {
        write_pool,
        entries,
    }
}

#[derive(Debug)]
pub enum ShmrError {
    InvalidPoolId,
    InvalidBucketId,
    OutOfSpace,
    EndOfFile,
    FsError(Error),
    EcError(reed_solomon_erasure::Error),
    ShardOpened,
    ShardMissing,
}
impl From<Error> for ShmrError {
    fn from(value: Error) -> Self {
        Self::FsError(value)
    }
}
impl From<reed_solomon_erasure::Error> for ShmrError {
    fn from(value: reed_solomon_erasure::Error) -> Self {
        Self::EcError(value)
    }
}

// just some helper functions for now
pub fn random_string() -> String {
    let mut rng = rand::thread_rng();
    let s: String = (0..14).map(|_| rng.gen_range(0..9).to_string()).collect();
    s
}

pub fn random_data(size: usize) -> Vec<u8> {
    let mut rng = rand::thread_rng();
    (0..size).map(|_| rng.gen_range(0..255)).collect()
}

#[cfg(test)]
pub mod tests {
    use std::collections::HashMap;
    use std::path::PathBuf;
    use crate::{build_poolmap, PoolMap};

    pub fn init() {
        let _ = env_logger::builder().is_test(true).try_init();
    }

    pub fn get_pool() -> PoolMap {
        let mut buckets = HashMap::new();
        buckets.insert("bucket1".to_string(), PathBuf::from("/tmp"));

        let mut pool_map = HashMap::new();
        pool_map.insert("test_pool".to_string(), buckets);


        build_poolmap("test_pool".to_string(), pool_map)
    }
}

// const FILE_HANDLE_READ_BIT: u64 = 1 << 63;
// const FILE_HANDLE_WRITE_BIT: u64 = 1 << 62;
//
// #[derive(Debug, Clone, Serialize, Deserialize)]
// pub struct ConfigStub {
//     pub mount: String,
//     pub workspace: PathBuf,
//   pub archives: HashMap<String, PathBuf>,
// }
//
// pub struct ShmrFilesystem {
//     config: ConfigStub,
//     superblock: Superblock,
//
//     atime: bool,
//     pub(crate) current_fh: AtomicU64,
//
// }
// impl ShmrFilesystem {
//     pub fn init(config: ConfigStub) -> Result<Self> {
//       debug!("initializing filesystem");
//         let superblock = Superblock::open(&config)?;
//         Ok(Self {
//             config,
//             superblock,
//             atime: false,
//             current_fh: Default::default(),
//         })
//     }
//
//   fn allocate_next_file_handle(&self, read: bool, write: bool) -> u64 {
//     let mut fh = self.current_fh.fetch_add(1, Ordering::SeqCst);
//     // Assert that we haven't run out of file handles
//     assert!(fh < FILE_HANDLE_READ_BIT.min(FILE_HANDLE_WRITE_BIT));
//     if read {
//       fh |= FILE_HANDLE_READ_BIT;
//     }
//     if write {
//       fh |= FILE_HANDLE_WRITE_BIT;
//     }
//
//     fh
//   }
// }
//
//
// pub fn time_now() -> (i64, u32) {
//     let now = SystemTime::now();
//     let since_the_epoch = now.duration_since(UNIX_EPOCH).expect("Time went backwards");
//     (since_the_epoch.as_secs() as i64, since_the_epoch.subsec_nanos())
//   }

use bytesize::ByteSize;
use log::debug;
use rand::Rng;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};
use std::io::Error;
use std::ops::Deref;
use std::path::PathBuf;
use sysinfo::Disks;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum MetadataFormat {
    Yaml,
    BrotliYaml,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShmrFsConfig {
    /// Metadata Directory location. Where the sqlite databases are stored
    pub metadata_dir: PathBuf,

    // pub metdata_format: MetadataFormat,
    /// Directory where the FUSE Filesystem will be mounted
    pub mount_dir: PathBuf,

    /// Pool -> Bucket Map
    pub pools: BTreeMap<String, HashMap<String, Bucket>>,

    /// Write Pool
    pub write_pool: String,

    pub block_size: ByteSize,

    /// Prometheus Remote Write Endpoint. "http://127.0.0.1:9091/metrics/job/example"
    pub prometheus_endpoint: Option<String>,

    pub prometheus_username: Option<String>,
    pub prometheus_password: Option<String>,
}
impl ShmrFsConfig {
    pub fn has_pool(&self, pool: &str) -> bool {
        self.pools.contains_key(pool)
    }

    /// Select n Buckets from the given pool
    pub fn select_buckets(&self, pool: &str, count: usize) -> Result<Vec<String>, ShmrError> {
        // TODO This function is ugly and could be refactored
        // TODO Eliminate un-needed clone operations
        // for now, we are going to return the pools in order of Priority & Free Space Percentage
        let mut possible_buckets: Vec<(&String, &Bucket)> = self
            .pools
            .get(pool)
            .ok_or(ShmrError::InvalidPoolId)?
            .iter()
            .filter(|(_, bucket)| bucket.priority > BucketPriority::Ignore)
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
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CacheConfig {
    max_memory: ByteSize,
}

#[derive(Serialize, Deserialize, Ord, PartialOrd, Eq, PartialEq, Debug, Clone, Default)]
pub enum BucketPriority {
    /// Prefer this bucket
    Prioritize = 4,
    /// No Preference
    #[default]
    Normal = 3,
    /// Deprioritize this bucket.
    Deprioritize = 2,
    /// Ignore this Bucket. No new data will be written, but existing data will not be touched.
    Ignore = 1,
    /// Actively Ignore this Bucket and Evacuate Data
    Evacuate = 0,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Bucket {
    /// Bucket Directory Path
    pub path: PathBuf,

    /// Total Space in the Bucket
    #[serde(skip)]
    pub capacity: u64,

    /// Used Disk Space
    #[serde(skip)]
    pub available: u64,

    /// Bucket Priority
    #[serde(default)]
    pub priority: BucketPriority,
}
impl Bucket {
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
impl Deref for Bucket {
    type Target = PathBuf;

    fn deref(&self) -> &Self::Target {
        &self.path
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
    InvalidInodeType(String),
    InodeNotExist,
    BlockIndexOutOfBounds,
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

use crate::vpf::VirtualPathBuf;
use crate::ShmrError;
use bytesize::ByteSize;
use log::{debug, trace};
use std::cmp::PartialEq;
use std::collections::{BTreeMap, HashMap};
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, Write};
use std::path::PathBuf;
use std::sync::{Arc, Mutex, RwLock};
use std::thread::JoinHandle;
use sysinfo::Disks;

pub mod block;
pub mod buffer;
pub mod erasure;
pub mod hash;
pub mod ops;

#[allow(clippy::identity_op)]
const DEFAULT_STORAGE_BLOCK_SIZE: usize = 1024 * 1024 * 1; // 1MB

/// Pool -> Bucket -> Base Directory
pub type PoolMap = HashMap<String, HashMap<String, Bucket>>;
/// Central Management of Open File Handles and resolution of VirtualPathBufs
///
/// TODO Implement some sort of background flush mechanism
/// TODO Implement a LRU Cache for the handles
pub struct IOEngine {
    /// Write Pool Name
    pub(crate) write_pool: String,

    /// Map of Pools
    pub(crate) pools: PoolMap,

    /// HashMap of VirtualPathBuf entries, and the associated File Handle + PathBuf
    handles: Arc<RwLock<BTreeMap<VirtualPathBuf, (Arc<Mutex<File>>, PathBuf)>>>,

    run_state: Arc<Mutex<bool>>,

    /// JoinHandle of the Background Thread
    worker_handle: Arc<Mutex<Option<JoinHandle<()>>>>,
}

impl IOEngine {
    // TODO Periodic flush, like FsDB2. <--
    // TODO Implement a LRU Cache for the handles
    pub fn new(write_pool: String, pools: PoolMap) -> Self {
        IOEngine {
            write_pool,
            pools,
            handles: Arc::new(RwLock::new(BTreeMap::new())),
            run_state: Arc::new(Mutex::new(true)),
            worker_handle: Arc::new(Mutex::new(None)),
        }
    }

    fn open_handle(&self, vpf: &VirtualPathBuf) -> Result<(), ShmrError> {
        let path = vpf.resolve_path(&self.pools)?;

        let handle = OpenOptions::new().read(true).write(true).open(&path)?;

        let mut wh = self.handles.write().unwrap();
        if wh.contains_key(vpf) {
            return Ok(());
        }

        wh.insert(vpf.clone(), (Arc::new(Mutex::new(handle)), path));

        Ok(())
    }

    pub fn create(&self, vpf: &VirtualPathBuf) -> Result<(), ShmrError> {
        let full_path = vpf.resolve(&self.pools)?;

        // ensure the directory exists
        if !full_path.1.exists() {
            trace!("creating directory: {:?}", &full_path.1);
            std::fs::create_dir_all(&full_path.1)?;
        }

        trace!("creating file {:?}", &full_path.0);

        let file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&full_path.0)?;

        // close the handle
        drop(file);

        // reopen it, caching the handle
        self.open_handle(vpf)?;

        Ok(())
    }

    pub fn delete(&self, vpf: &VirtualPathBuf) -> Result<(), ShmrError> {
        let full_path = vpf.resolve(&self.pools)?;

        if full_path.0.exists() {
            std::fs::remove_file(&full_path.0)?;
        }

        Ok(())
    }

    pub fn read(
        &self,
        vpf: &VirtualPathBuf,
        offset: usize,
        buf: &mut [u8],
    ) -> Result<usize, ShmrError> {
        {
            let handles_handle = self.handles.read().unwrap();
            if !handles_handle.contains_key(vpf) {
                drop(handles_handle);
                self.open_handle(vpf)?;
            }
        }
        let binding = self.handles.read().unwrap();
        let entry = binding.get(vpf).unwrap();
        let mut file_handle = entry.0.lock().unwrap();

        file_handle.seek(std::io::SeekFrom::Start(offset as u64))?;
        let read = file_handle.read(buf)?;
        debug!("Read {} bytes to {:?} at offset {}", read, entry.1, offset);
        Ok(read)
    }

    pub fn write(
        &self,
        vpf: &VirtualPathBuf,
        offset: usize,
        buf: &[u8],
    ) -> Result<usize, ShmrError> {
        {
            let handles_handle = self.handles.read().unwrap();
            if !handles_handle.contains_key(vpf) {
                drop(handles_handle);
                self.open_handle(vpf)?;
            }
        }
        let binding = self.handles.read().unwrap();
        let entry = binding.get(vpf).unwrap();
        let mut file_handle = entry.0.lock().unwrap();

        file_handle.seek(std::io::SeekFrom::Start(offset as u64))?;
        let written = file_handle.write(buf)?;
        debug!(
            "Wrote {} bytes to {:?} at offset {}",
            written, entry.1, offset
        );
        Ok(written)
    }

    pub fn flush_all(&self) {
        todo!()
        // for (_vpf, handle) in (self.handles.clone()).into_iter() {
        //     handle.0.sync_all().unwrap();
        // }
    }

    pub fn flush(&self, vpb: &VirtualPathBuf) -> Result<(), ShmrError> {
        let handle = self.handles.read().unwrap();
        let fhl = handle.get(vpb);

        let fhl = fhl.unwrap();

        let mut inner_handle = fhl.0.lock().unwrap();
        inner_handle.flush()?;

        Ok(())
    }

    /// Select n Buckets from the given Pool and return the names.
    pub fn select_buckets(&self, pool: &str, count: usize) -> Result<Vec<String>, ShmrError> {
        // TODO This function is ugly and could be refactored

        // for now, we are going to return the pools in order of Priority & Free Space Percentage
        let mut possible_buckets: Vec<(&String, &Bucket)> = self
            .pools
            .get(pool)
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

        let selected_buckets: Vec<String> = possible_buckets
            .iter()
            .take(count)
            .map(|(bucket_name, _)| bucket_name.clone().clone())
            .collect();

        Ok(selected_buckets)
    }
}

#[derive(Ord, PartialOrd, Eq, PartialEq, Clone, Copy, Debug)]
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

#[derive(Clone, Debug)]
pub struct Bucket {
    path: PathBuf,
    /// Total Space in the Bucket
    capacity: ByteSize,
    /// Used Disk Space
    available: ByteSize,
    /// Bucket Priority
    priority: BucketPriority,
}
impl Bucket {
    pub fn new(path: PathBuf) -> Self {
        Bucket {
            path,
            capacity: ByteSize::b(0),
            available: ByteSize::b(0),
            priority: BucketPriority::NORMAL,
        }
    }

    /// Given a list of [`sysinfo::Disks`], update this Bucket
    pub fn update(&mut self, disks: &Disks) {
        for disk in disks.list() {
            if disk.mount_point() == self.path.as_path() {
                self.capacity = ByteSize::b(disk.total_space());
                self.available = ByteSize::b(disk.available_space());

                // there is only ever going to be one disk in the system that we're looking for
                break;
            }
        }
    }

    pub fn path(&self) -> PathBuf {
        self.path.clone()
    }
}

pub fn build_poolmap(paths: HashMap<String, HashMap<String, PathBuf>>) -> PoolMap {
    paths
        .into_iter()
        .map(|(pool_name, buckets)| {
            let pool_buckets = buckets
                .into_iter()
                .map(|(bucket_name, path)| (bucket_name, Bucket::new(path)))
                .collect();
            (pool_name, pool_buckets)
        })
        .collect()
}

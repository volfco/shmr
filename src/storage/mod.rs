use crate::vpf::VirtualPathBuf;
use crate::ShmrError;
use log::{debug, trace};
use std::collections::{BTreeMap, HashMap};
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, Write};
use std::path::PathBuf;
use std::sync::{Arc, Mutex, RwLock};
use std::thread::JoinHandle;

pub mod erasure;
pub mod hash;
pub mod ops;
pub mod buffer;
pub mod block;


const DEFAULT_STORAGE_BLOCK_SIZE: usize = 1024 * 1024 * 1; // 1MB

pub type PoolMap = HashMap<String, HashMap<String, PathBuf>>;

/// Central Management of Open File Handles and resolution of VirtualPathBufs
///
/// TODO Implement some sort of background flush mechanism
/// TODO Implement a LRU Cache for the handles
pub struct IOEngine {
    /// Write Pool Name
    pub(crate) write_pool: String,
    
    /// Map of Pools
    pub(crate) pools: PoolMap,

    /// HashMap of VirtualPathBuf entries, and the associated File Handle + FilePath
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
            worker_handle: Arc::new(Mutex::new(None))
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
}



// /// Run Interval for the FileCacheManager Worker thread, which
// pub const FILE_CACHE_MANAGER_SWEEP_INTERVAL: usize = 500; // ms
// pub const FILE_CACHE_MANAGER_LOW_WATERMARK_RATIO: f32 = 0.69420; //

use std::sync::Arc;
use dashmap::DashMap;
use crate::config::ShmrFsConfig;
use crate::tasks::WorkerTask;
use crate::vfs::VirtualFile;

#[derive(Clone, Debug)]
pub struct CacheWorker {
    config: ShmrFsConfig,
    file_cache: Arc<DashMap<u64, VirtualFile>>,
}

impl WorkerTask for CacheWorker {
    fn execute(&self) {



        todo!()
    }
}
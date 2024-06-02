use crate::FileDB;
use crate::tasks::WorkerTask;
use crate::vfs::VirtualFile;
use dashmap::DashMap;
use log::error;
use std::sync::Arc;
use std::time::Duration;

pub const FLUSH_MASTER_DEFAULT_RUN_INTERVAL: Duration = Duration::from_secs(1);

/// Iterate over all files in the File Cache, and call sync_data on each
#[derive(Clone, Debug)]
pub struct FlushMaster {
    file_db: FileDB,
    file_cache: Arc<DashMap<u64, VirtualFile>>,
}
impl FlushMaster {
    pub fn new(file_db: FileDB, file_cache: Arc<DashMap<u64, VirtualFile>>) -> Self {
        FlushMaster {
            file_db,
            file_cache,
        }
    }
}
impl WorkerTask for FlushMaster {
    fn execute(&self) {
        for entry in self.file_cache.iter() {
            if let Err(e) = entry.sync_data() {
                error!("[{}] error occurred during data sync. {:?}", entry.ino, e);
            }

            if let Err(e) = self.file_db.save_virtual_file(&entry) {
                error!(
                    "[{}] error occurred during metadata sync. {:?}",
                    entry.ino, e
                );
            }
        }
    }
}

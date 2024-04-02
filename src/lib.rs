#![feature(trivial_bounds)]
pub mod topology;
pub mod fuse;

use std::time::UNIX_EPOCH;
use std::time::SystemTime;
use std::path::PathBuf;

use anyhow::Result;
use std::str;
use std::sync::atomic::{AtomicU64, Ordering};
use log::debug;
use serde::{Deserialize, Serialize};
use crate::topology::Superblock;

const FILE_HANDLE_READ_BIT: u64 = 1 << 63;
const FILE_HANDLE_WRITE_BIT: u64 = 1 << 62;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConfigStub {
    pub mount: String,
    pub workspace: PathBuf,
}

pub struct ShmrFilesystem {
    config: ConfigStub,
    superblock: Superblock,

    atime: bool,
    pub(crate) current_fh: AtomicU64,

}
impl ShmrFilesystem {
    pub fn init(config: ConfigStub) -> Result<Self> {
      debug!("initializing filesystem");
        let superblock = Superblock::open(&config)?;
        Ok(Self {
            config,
            superblock,
            atime: false,
            current_fh: Default::default(),
        })
    }

  fn allocate_next_file_handle(&self, read: bool, write: bool) -> u64 {
    let mut fh = self.current_fh.fetch_add(1, Ordering::SeqCst);
    // Assert that we haven't run out of file handles
    assert!(fh < FILE_HANDLE_READ_BIT.min(FILE_HANDLE_WRITE_BIT));
    if read {
      fh |= FILE_HANDLE_READ_BIT;
    }
    if write {
      fh |= FILE_HANDLE_WRITE_BIT;
    }

    fh
  }
}


pub fn time_now() -> (i64, u32) {
    let now = SystemTime::now();
    let since_the_epoch = now.duration_since(UNIX_EPOCH).expect("Time went backwards");
    (since_the_epoch.as_secs() as i64, since_the_epoch.subsec_nanos())
  }
  
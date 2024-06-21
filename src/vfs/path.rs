use crate::config::{ShmrError, ShmrFsConfig};
use log::trace;
use serde::{Deserialize, Serialize};
use std::fmt::Display;
use std::fs::OpenOptions;
use std::path::PathBuf;

#[allow(clippy::identity_op)]
pub const VIRTUAL_BLOCK_DEFAULT_SIZE: u64 = 1024 * 1024 * 1;

pub const VP_DEFAULT_FILE_EXT: &str = "bin";

#[derive(Serialize, Deserialize, Ord, PartialOrd, Debug, Clone, Eq, PartialEq)]
pub struct VirtualPath {
    /// Drive Pool
    pub pool: String,
    /// Specific drive in the Drive Pool
    pub bucket: String,
    /// Filename in the Bucket
    pub filename: String,
}
impl VirtualPath {
    /// Return the (Filename, Directory) for the file.
    /// It's inverted to avoid needing to create a copy of the directory name before joining the filename
    pub fn resolve(&self, map: &ShmrFsConfig) -> Result<(PathBuf, PathBuf), ShmrError> {
        trace!("resolving {:?}", &self);
        let pool_map = map.pools.get(&self.pool).ok_or(ShmrError::InvalidPoolId)?;

        let mut path_buf = pool_map
            .get(&self.bucket)
            .ok_or(ShmrError::InvalidBucketId)?
            .path();

        path_buf.push(&self.filename[0..2]); // first two characters of the filename
        path_buf.push(&self.filename[2..4]); // next two characters of the filename

        let result = (path_buf.join(&self.filename), path_buf);
        trace!(
            "Resolved path.rs for {:?} to (file: {:?}, dir: {:?})",
            self,
            result.0,
            result.1
        );

        Ok(result)
    }

    pub fn create(&self, map: &ShmrFsConfig) -> Result<(), ShmrError> {
        let full_path = self.resolve(map)?;

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

        Ok(())
    }
}
impl Display for VirtualPath {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}({}):{}", self.pool, self.bucket, self.filename)
    }
}

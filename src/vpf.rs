use crate::storage::PoolMap;
use anyhow::Context;
use log::{debug, error, trace};
use rkyv::{Archive, Deserialize, Serialize};
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, Write};
use std::path::PathBuf;
// use rkyv::with::Skip;

/// VirtualPathBuf is like PathBuf, but the location of the pool is not stored in the path itself.
/// Instead, it is provided as a parameter during operations.
///
/// There is no need for a constructor here because this has no state. I guess? idk
#[derive(Debug, Archive, Serialize, Deserialize, Clone, PartialEq)]
#[archive(compare(PartialEq), check_bytes)]
pub struct VirtualPathBuf {
    pub pool: String,
    pub bucket: String,
    pub filename: String,
    // #[with(Skip)]
    // resolved_path: Option<(PathBuf, PathBuf)>
}
impl VirtualPathBuf {
    // pub fn resolve(&self, map: &PoolMap) -> Self {
    //     let mut new = self.clone();
    //     new.resolved_path = Some(self.get_path(map).unwrap());
    //     new
    // }
    /// Return the (Filename, Directory) for the file.
    /// It's inverted to avoid needing to create a copy of the directory name before joining the filename
    fn get_path(&self, map: &PoolMap) -> anyhow::Result<(PathBuf, PathBuf)> {
        // // TODO This is fucking dumb. Fix
        // if self.resolved_path.is_some() {
        //     return Ok(self.resolved_path.clone().unwrap());
        // }

        let pool_map = map
            .0
            .get(&self.pool)
            .ok_or(anyhow::anyhow!("Invalid pool id"))?;

        let mut path_buf = pool_map
            .get(&self.bucket)
            .ok_or(anyhow::anyhow!("Invalid bucket id"))?
            .clone();

        path_buf.push(&self.filename[0..2]); // first two characters of the filename
        path_buf.push(&self.filename[2..4]); // next two characters of the filename

        let result = (path_buf.join(&self.filename), path_buf);
        trace!(
            "Resolved path for {:?} to (file: {:?}, dir: {:?})",
            self, result.0, result.1
        );

        Ok(result)
    }

    pub fn resolve_path(&self, map: &PoolMap) -> anyhow::Result<PathBuf> {
        let full_path = self.get_path(map)?;
        Ok(full_path.1.join(full_path.0))
    }

    pub fn exists(&self, map: &PoolMap) -> bool {
        match self.resolve_path(map) {
            Ok(path) => path.exists(),
            Err(e) => {
                error!("Error resolving path: {:?}", e);
                false
            }
        }
    }

    pub fn create(&self, map: &PoolMap) -> anyhow::Result<()> {
        let full_path = self.get_path(map)?;

        // ensure the directory exists
        if !full_path.1.exists() {
            trace!("creating directory: {:?}", &full_path.1);
            std::fs::create_dir_all(&full_path.1)
                .context(format!("creating {:?}", &full_path.1))?;
        }

        trace!("creating file {:?}", &full_path.0);

        let file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&full_path.0)
            .context(format!("opening {:?}", &full_path.0))?;

        file.sync_all()?;

        Ok(())
    }

    pub fn read(&self, map: &PoolMap, offset: usize, buf: &mut [u8]) -> anyhow::Result<usize> {
        let full_path = self.get_path(map)?;

        let mut file = File::open(&full_path.0).context(format!("opening {:?}", &full_path.0))?;
        file.seek(std::io::SeekFrom::Start(offset as u64))?;
        let read = file.read(buf)?;

        debug!(
            "Read {} bytes from {:?} at offset {}",
            read, full_path.0, offset
        );
        Ok(read)
    }

    pub fn write(&self, map: &PoolMap, offset: usize, buf: &[u8]) -> anyhow::Result<usize> {
        let full_path = self.get_path(map)?;

        let mut file = OpenOptions::new()
            .write(true)
            // .truncate(true)
            .open(&full_path.0)
            .context("unable to open file")?;

        file.seek(std::io::SeekFrom::Start(offset as u64))?;
        let written = file.write(buf)?;

        // ensure that we sync the data to disk
        file.sync_all()?;

        debug!(
            "Wrote {} bytes to {:?} at offset {}",
            written, full_path.0, offset
        );
        Ok(written)
    }

    pub fn delete(&self, map: &PoolMap) -> anyhow::Result<()> {
        let full_path = self.get_path(map)?;

        std::fs::remove_file(&full_path.0).context(format!("removing {:?}", &full_path.0))?;

        debug!("Deleted file path: {:?}", full_path.0);
        Ok(())
    }
}


#[cfg(test)]
mod tests {
    use super::VirtualPathBuf;
    use crate::random_string;
    use crate::tests::get_pool;

    #[test]
    fn test_virtual_path_buf_create() {
        let pool_map = get_pool();

        let path = VirtualPathBuf {
            pool: "test_pool".to_string(),
            bucket: "bucket1".to_string(),
            filename: "test_file".to_string(),
        };

        let result = path.create(&pool_map);
        assert!(result.is_ok());
    }

    #[test]
    fn test_virtual_path_buf_write() {
        let filename = random_string();
        let pool_map = get_pool();

        let path = VirtualPathBuf {
            pool: "test_pool".to_string(),
            bucket: "bucket1".to_string(),
            filename,
        };

        // ensure the file exists before writing
        let result = path.create(&pool_map);
        assert!(result.is_ok());

        // write to the file
        let buffer = vec![0, 1, 2];
        let result = path.write(&pool_map, 0, &buffer);
        assert!(result.is_ok());

        // read the data back
        let mut read_buffer = [0; 3];
        let result = path.read(&pool_map, 0, &mut read_buffer);
        assert!(result.is_ok());
        assert_eq!(read_buffer, buffer.as_slice());
    }

    #[test]
    fn test_virtual_path_buf_delete() {
        let filename = random_string();
        let pool_map = get_pool();

        let path = VirtualPathBuf {
            pool: "test_pool".to_string(),
            bucket: "bucket1".to_string(),
            filename,
        };

        // ensure the file exists before writing
        let result = path.create(&pool_map);
        assert!(result.is_ok());

        let result = path.delete(&pool_map);
        assert!(result.is_ok());

        // resolve the path to make sure the data is not there
        let result = path.resolve_path(&pool_map);
        assert!(result.is_ok());

        assert_eq!(result.unwrap().exists(), false);
    }
}

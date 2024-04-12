use anyhow::Context;
use log::{debug, error, trace};
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, Write};
use std::path::PathBuf;
use rkyv::{Archive, Deserialize, Serialize};

/// VirtualPathBuf is like PathBuf, but the location of the pool is not stored in the path itself.
/// Instead, it is provided as a parameter during operations.
///
/// There is no need for a constructor here because this has no state. I guess? idk
#[derive(Debug, Archive, Serialize, Deserialize, Clone, PartialEq)]
#[archive(
  compare(PartialEq),
  check_bytes,
)]
pub struct VirtualPathBuf {
    /// Storage Pool ID, in UUID format
    pub pool: String,
    /// Path to the file location in the Pool
    pub filename: String,
}
impl VirtualPathBuf {
    /// Return the (Filename, Directory) for the file.
    /// It's inverted to avoid needing to create a copy of the directory name before joining the filename
    fn get_path(&self, pool_map: &HashMap<String, PathBuf>) -> anyhow::Result<(PathBuf, PathBuf)> {
        let mut path_buf = pool_map
            .get(&self.pool)
            .ok_or(anyhow::anyhow!("Invalid pool id"))?
            .clone();

        path_buf.push(&self.filename[0..2]); // first two characters of the filename
        path_buf.push(&self.filename[2..4]); // next two characters of the filename

        let result = (path_buf.join(&self.filename), path_buf);
        // trace!(
        //     "Resolved path for {:?} to (file: {:?}, dir: {:?})",
        //     self, result.0, result.1
        // );

        Ok(result)
    }

    pub fn resolve_path(&self, pool_map: &HashMap<String, PathBuf>) -> anyhow::Result<PathBuf> {
        let full_path = self.get_path(pool_map)?;
        Ok(full_path.1.join(full_path.0))
    }

    pub fn exists(&self, pool_map: &HashMap<String, PathBuf>) -> bool {
        match self.resolve_path(pool_map) {
            Ok(path) => path.exists(),
            Err(e) => {
                error!("Error resolving path: {:?}", e);
                false
            },
        }
    }

    pub fn create(&self, pool_map: &HashMap<String, PathBuf>) -> anyhow::Result<()> {
        let full_path = self.get_path(pool_map)?;
        let file_path = full_path.1.join(full_path.0);

        // ensure the directory exists
        if !full_path.1.exists() {
            trace!("creating directory: {:?}", &full_path.1);
            std::fs::create_dir_all(&full_path.1)
                .context(format!("creating {:?}", &full_path.1))?;
        }

        trace!("creating file {:?}", &file_path);

        let file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&file_path)
            .context(format!("opening {:?}", &file_path))?;

        file.sync_all()?;

        Ok(())
    }

    pub fn read(
        &self,
        pool_map: &HashMap<String, PathBuf>,
        offset: usize,
        buf: &mut Vec<u8>,
    ) -> anyhow::Result<usize> {
        let full_path = self.get_path(pool_map)?;
        let file_path = full_path.1.join(full_path.0);

        let mut file = File::open(&file_path).context(format!("opening {:?}", &file_path))?;
        file.seek(std::io::SeekFrom::Start(offset as u64))?;
        let read = file.read_to_end(buf)?;

        debug!("Read {} bytes from {:?} at offset {}", read, file_path, offset);
        Ok(read)
    }

    pub fn write(
        &self,
        pool_map: &HashMap<String, PathBuf>,
        offset: usize,
        buf: &[u8],
    ) -> anyhow::Result<usize> {
        let full_path = self.get_path(pool_map)?;
        let file_path = full_path.1.join(full_path.0);

        let mut file = OpenOptions::new()
            .write(true)
            // .truncate(true)
            .open(&file_path)
            .context(format!("opening {:?}", &file_path))?;

        file.seek(std::io::SeekFrom::Start(offset as u64))?;
        let written = file.write(buf)?;

        // ensure that we sync the data to disk
        file.sync_all()?;

        debug!("Wrote {} bytes to {:?} at offset {}", written, file_path, offset);
        Ok(written)
    }

    pub fn delete(&self, pool_map: &HashMap<String, PathBuf>) -> anyhow::Result<()> {
        let full_path = self.get_path(pool_map)?;
        let file_path = full_path.1.join(full_path.0);

        std::fs::remove_file(&file_path).context(format!("removing {:?}", &file_path))?;

        debug!("Deleted file path: {:?}", file_path);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::VirtualPathBuf;
    use log::error;
    use std::collections::HashMap;
    use std::path::{Path, PathBuf};
    use crate::random_string;

    #[test]
    fn test_virtual_path_buf_create() {
        let temp_dir = Path::new("/tmp");

        let mut pool_map: HashMap<String, PathBuf> = HashMap::new();
        pool_map.insert("test_pool".to_string(), temp_dir.to_path_buf());

        let path = VirtualPathBuf {
            pool: "test_pool".to_string(),
            filename: "test_file".to_string(),
        };

        let result = path.create(&pool_map);
        error!("{:?}", result);
        assert!(result.is_ok());
    }

    #[test]
    fn test_virtual_path_buf_write() {
        let filename = random_string();

        let mut pool_map: HashMap<String, PathBuf> = HashMap::new();
        let temp_dir = Path::new("/tmp");

        pool_map.insert("test_pool".to_string(), temp_dir.to_path_buf());

        let path = VirtualPathBuf {
            pool: "test_pool".to_string(),
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
        let mut read_buffer = vec![0; 3];
        let result = path.read(&pool_map, 0, &mut read_buffer);
        assert!(result.is_ok());
        assert_eq!(read_buffer, buffer);
    }

    #[test]
    fn test_virtual_path_buf_delete() {
        let temp_dir = Path::new("/tmp");

        let filename = random_string();

        let mut pool_map: HashMap<String, PathBuf> = HashMap::new();
        pool_map.insert("test_pool".to_string(), temp_dir.to_path_buf());

        let path = VirtualPathBuf {
            pool: "test_pool".to_string(),
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

use crate::storage::PoolMap;
use log::{error, trace};
use std::path::PathBuf;
use serde::{Deserialize, Serialize};
use crate::ShmrError;
// use rkyv::with::Skip;

/// VirtualPathBuf is like PathBuf, but the location of the pool is not stored in the path itself.
/// Instead, it is provided as a parameter during operations.
///
/// There is no need for a constructor here because this has no state. I guess? idk
#[derive(Serialize, Deserialize, PartialEq, Hash, Ord, PartialOrd, Eq, Debug, Clone)]
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
    pub fn resolve(&self, map: &PoolMap) -> Result<(PathBuf, PathBuf), ShmrError> {
        let pool_map = map
            .get(&self.pool)
            .ok_or(ShmrError::InvalidPoolId)?;

        let mut path_buf = pool_map
            .get(&self.bucket)
            .ok_or(ShmrError::InvalidBucketId)?
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

    pub fn resolve_path(&self, map: &PoolMap) -> Result<PathBuf, ShmrError> {
        let full_path = self.resolve(map)?;
        Ok(full_path.0)
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

}

//
// #[cfg(test)]
// mod tests {
//     use super::VirtualPathBuf;
//     use crate::random_string;
//     use crate::tests::get_pool;
//
//     #[test]
//     fn test_virtual_path_buf_create() {
//         let pool_map = get_pool();
//
//         let path = VirtualPathBuf {
//             pool: "test_pool".to_string(),
//             bucket: "bucket1".to_string(),
//             filename: "test_file".to_string(),
//         };
//
//         let result = path.create(&pool_map);
//         assert!(result.is_ok());
//     }
//
//     #[test]
//     fn test_virtual_path_buf_write() {
//         let filename = random_string();
//         let pool_map = get_pool();
//
//         let path = VirtualPathBuf {
//             pool: "test_pool".to_string(),
//             bucket: "bucket1".to_string(),
//             filename,
//         };
//
//         // ensure the file exists before writing
//         let result = path.create(&pool_map);
//         assert!(result.is_ok());
//
//         // write to the file
//         let buffer = vec![0, 1, 2];
//         let result = path.write(&pool_map, 0, &buffer);
//         assert!(result.is_ok());
//
//         // read the data back
//         let mut read_buffer = [0; 3];
//         let result = path.read(&pool_map, 0, &mut read_buffer);
//         assert!(result.is_ok());
//         assert_eq!(read_buffer, buffer.as_slice());
//     }
//
//     #[test]
//     fn test_virtual_path_buf_delete() {
//         let filename = random_string();
//         let pool_map = get_pool();
//
//         let path = VirtualPathBuf {
//             pool: "test_pool".to_string(),
//             bucket: "bucket1".to_string(),
//             filename,
//         };
//
//         // ensure the file exists before writing
//         let result = path.create(&pool_map);
//         assert!(result.is_ok());
//
//         let result = path.delete(&pool_map);
//         assert!(result.is_ok());
//
//         // resolve the path to make sure the data is not there
//         let result = path.resolve_path(&pool_map);
//         assert!(result.is_ok());
//
//         assert_eq!(result.unwrap().exists(), false);
//     }
//
// }

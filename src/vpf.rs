use crate::storage::PoolMap;
use crate::ShmrError;
use log::{error, trace};
use serde::{Deserialize, Serialize};
use std::fmt::Display;
use std::path::PathBuf;
// use rkyv::with::Skip;

pub const VPB_DEFAULT_FILE_EXT: &str = "bin";

/// VirtualPathBuf
#[derive(Serialize, Deserialize, PartialEq, Hash, Ord, PartialOrd, Eq, Debug, Clone)]
pub struct VirtualPathBuf {
    /// Drive Pool
    pub pool: String,
    /// Specific drive in the Drive Pool
    pub bucket: String,
    /// Filename in the Bucket
    pub filename: String,
    // #[with(Skip)]
    // resolved_path: Option<(PathBuf, PathBuf)>
}
impl VirtualPathBuf {
    /// Return the (Filename, Directory) for the file.
    /// It's inverted to avoid needing to create a copy of the directory name before joining the filename
    pub fn resolve(&self, map: &PoolMap) -> Result<(PathBuf, PathBuf), ShmrError> {
        let pool_map = map.get(&self.pool).ok_or(ShmrError::InvalidPoolId)?;

        let mut path_buf = pool_map
            .get(&self.bucket)
            .ok_or(ShmrError::InvalidBucketId)?
            .path();

        path_buf.push(&self.filename[0..2]); // first two characters of the filename
        path_buf.push(&self.filename[2..4]); // next two characters of the filename

        let result = (path_buf.join(&self.filename), path_buf);
        trace!(
            "Resolved path for {:?} to (file: {:?}, dir: {:?})",
            self,
            result.0,
            result.1
        );

        Ok(result)
    }
}
impl Display for VirtualPathBuf {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}({}):{}", self.pool, self.bucket, self.filename)
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

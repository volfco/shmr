#![feature(trivial_bounds)]
pub mod topology;
mod fuse;

use std::collections::HashMap;
use std::path::PathBuf;

use rkyv::{Archive, Deserialize, Serialize};
use std::process::Command;
use std::str;
use crate::topology::Superblock;


#[derive(Debug, Archive, Serialize, Deserialize, Clone)]
pub struct StorageVolume {
    /// UUID is the filesystem UUID 
    pub uuid: String,
    pub path: String,
}
impl StorageVolume {
    pub fn from_path(path: PathBuf) -> Self {
        let output = Command::new("xfs_admin")
            .arg("-u")
            .arg(&path)
            .output()
            .expect("Failed to execute xfs_admin");

            // lazy way to parse the output. works for now
        let uuid = str::from_utf8(&output.stdout)
            .expect("Failed to parse output")
            .trim()
            .to_string().replace("UUID = ", "");

        Self {
            uuid,
            path: path.to_str().unwrap().to_string(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct ConfigStub {
    pub mount: String,
    pub workspace: PathBuf,
    pub archives: HashMap<String, StorageVolume>,
    pub encoding: (u8, u8),
    pub drive_replacements: Vec<DriveReplacements>,
}

#[derive(Debug, Clone)]
pub struct DriveReplacements {
    pub old: Vec<String>,
    pub new: Vec<String>
}

#[derive(Debug, Clone)]
pub struct Config {
    pub stub: ConfigStub,
    pub archives: HashMap<String, String>
}

/// ShmrFilesystem
pub struct ShmrFilesystem {
    config: Config,
    superblock: Superblock,

    atime: bool

}
impl ShmrFilesystem {
}
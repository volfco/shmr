#![feature(trivial_bounds)]
pub mod topology;

use std::collections::HashMap;
use std::ffi::OsStr;
use std::path::PathBuf;

use fuser::{Filesystem, ReplyCreate, Request};
use rkyv::{Archive, Deserialize, Serialize};
use std::process::Command;
use std::str;


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

#[derive(Debug, Archive, Serialize, Deserialize, Clone)]
pub struct ConfigStub {
    pub mount: String,
    pub workspace: String,
    pub archives: HashMap<String, StorageVolume>,
    pub encoding: (u8, u8),
    pub drive_replacements: Vec<DriveReplacements>,
}

#[derive(Debug, Archive, Serialize, Deserialize, Clone)]
pub struct DriveReplacements {
    pub old: Vec<String>,
    pub new: Vec<String>
}

#[derive(Debug, Archive, Serialize, Deserialize, Clone)]
pub struct Config {
    pub stub: ConfigStub,
    pub archives: HashMap<String, String>
}

/// ShmrFilesystem
pub struct ShmrFilesystem {
    // pub config: RunningConfig,
    pub direct_io: bool,
    // pub suid_support: bool,
}

impl Filesystem for ShmrFilesystem {
    fn create(&mut self, _req: &Request<'_>, parent: u64, name: &OsStr, mode: u32, umask: u32, flags: i32, reply: ReplyCreate) {
        todo!()

        // check if the file exists
        // if not, create it with the given mode

        // open the file
    }
}
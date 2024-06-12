use crate::fuse::types::IFileType;
use fuser::FileAttr;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use crate::fuse::time_now;
use crate::vfs::VirtualFile;

fn system_time_from_time(secs: i64, nsecs: u32) -> SystemTime {
    if secs >= 0 {
        UNIX_EPOCH + Duration::new(secs as u64, nsecs)
    } else {
        UNIX_EPOCH - Duration::new((-secs) as u64, nsecs)
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct SuperblockEntry {
    pub inode: Inode,
    pub inode_descriptor: InodeDescriptor
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum InodeDescriptor {
    Directory(BTreeMap<Vec<u8>, u64>),
    File(Box<VirtualFile>),
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub struct Inode {
    /// Inode number
    pub ino: u64,
    /// Size in bytes
    pub size: u64,
    /// Size in blocks
    pub blocks: u64,
    /// Time of last access
    pub atime: (i64, u32),
    /// Time of last modification
    pub mtime: (i64, u32),
    /// Time of last change
    pub ctime: (i64, u32),
    /// Time of creation (macOS only)
    pub crtime: (i64, u32),
    /// Kind of file (directory, file, pipe, etc)
    pub kind: IFileType,
    /// Permissions
    pub perm: u16,
    /// Number of hard links
    pub nlink: u32,
    /// User id
    pub uid: u32,
    /// Group id
    pub gid: u32,
    /// Rdev
    pub rdev: u32,
    /// Block size
    pub blksize: u32,
    /// Flags (macOS only, see chflags(2))
    pub flags: u32,

    pub xattrs: BTreeMap<Vec<u8>, Vec<u8>>,
}
impl Inode {

    /// Update the Inode's metadata (ctime, mtime, nlink)
    pub fn update_metadata(&mut self, incr_nlink: i8) {
        if incr_nlink < 0 {
            self.nlink -= incr_nlink.unsigned_abs() as u32;
        } else {
            self.nlink += incr_nlink as u32;
        }
        self.mtime = time_now();
        self.ctime = time_now();
    }
    pub fn check_access(&self, uid: u32, gid: u32, mut access_mask: i32) -> bool {
        // F_OK tests for existence of file
        if access_mask == libc::F_OK {
            return true;
        }

        let file_mode = self.perm as i32;

        // root is allowed to read & write anything
        if uid == 0 {
            // root only allowed to exec if one of the X bits is set
            access_mask &= libc::X_OK;
            access_mask -= access_mask & (file_mode >> 6);
            access_mask -= access_mask & (file_mode >> 3);
            access_mask -= access_mask & file_mode;
            return access_mask == 0;
        }

        if uid == self.uid {
            access_mask -= access_mask & (file_mode >> 6);
        } else if gid == self.gid {
            // TODO we might need more indepth group checking
            access_mask -= access_mask & (file_mode >> 3);
        } else {
            access_mask -= access_mask & file_mode;
        }

        access_mask == 0
    }
    pub fn to_fileattr(&self) -> FileAttr {
        FileAttr {
            ino: self.ino,
            size: self.size,
            blocks: self.blocks,
            atime: system_time_from_time(self.atime.0, self.atime.1),
            mtime: system_time_from_time(self.mtime.0, self.mtime.1),
            ctime: system_time_from_time(self.ctime.0, self.ctime.1),
            crtime: system_time_from_time(self.crtime.0, self.crtime.1),
            kind: self.kind.clone().into(),
            perm: self.perm,
            nlink: self.nlink,
            uid: self.uid,
            gid: self.gid,
            rdev: self.rdev,
            blksize: self.blksize,
            flags: self.flags,
        }
    }
}

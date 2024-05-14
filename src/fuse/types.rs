use std::collections::BTreeMap;
use fuser::FileAttr;
use serde::{Deserialize, Serialize};
use crate::fuse::system_time_from_time;
use crate::vfs::VirtualFile;

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub enum IFileType {
    /// Named pipe (S_IFIFO)
    NamedPipe,
    /// Character device (S_IFCHR)
    CharDevice,
    /// Block device (S_IFBLK)
    BlockDevice,
    /// Directory (S_IFDIR)
    Directory,
    /// Regular file (S_IFREG)
    RegularFile,
    /// Symbolic link (S_IFLNK)
    Symlink,
    /// Unix domain socket (S_IFSOCK)
    Socket,
}
impl From<IFileType> for fuser::FileType {
    fn from(value: IFileType) -> Self {
        match value {
            IFileType::NamedPipe => fuser::FileType::NamedPipe,
            IFileType::CharDevice => fuser::FileType::CharDevice,
            IFileType::BlockDevice => fuser::FileType::BlockDevice,
            IFileType::Directory => fuser::FileType::Directory,
            IFileType::RegularFile => fuser::FileType::RegularFile,
            IFileType::Symlink => fuser::FileType::Symlink,
            IFileType::Socket => fuser::FileType::Socket,
        }
    }
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

// TODO Refactor this to use a Uuid for the File Contents then fold into Inode
#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum InodeDescriptor {
    File(VirtualFile),
    /// InodeDescriptor for a directory, where the BTreeMap is (filename -> inode)
    Directory(BTreeMap<Vec<u8>, u64>),
    Symlink,
}
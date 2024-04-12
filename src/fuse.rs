use std::collections::BTreeMap;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use fuser::FileAttr;
use rkyv::{Archive, Deserialize, Serialize};

#[allow(dead_code)]
pub fn time_now() -> (i64, u32) {
    let now = SystemTime::now();
    let since_the_epoch = now.duration_since(UNIX_EPOCH).expect("Time went backwards");
    (since_the_epoch.as_secs() as i64, since_the_epoch.subsec_nanos())
  }
fn system_time_from_time(secs: i64, nsecs: u32) -> SystemTime {
  if secs >= 0 {
    UNIX_EPOCH + Duration::new(secs as u64, nsecs)
  } else {
    UNIX_EPOCH - Duration::new((-secs) as u64, nsecs)
  }
}

#[derive(Debug, Archive, Serialize, Deserialize, Clone, PartialEq)]
#[archive(
  compare(PartialEq),
  check_bytes,
)]
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

#[derive(Debug, Archive, Serialize, Deserialize, Clone, PartialEq)]
#[archive(
  compare(PartialEq),
  check_bytes,
)]
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

  pub xattrs: BTreeMap<Vec<u8>, Vec<u8>>
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
}
impl From<Inode> for FileAttr {
  fn from(value: Inode) -> Self {
    FileAttr {
      ino: value.ino,
      size: value.size,
      blocks: value.blocks,
      atime: system_time_from_time(value.atime.0, value.atime.1),
      mtime: system_time_from_time(value.mtime.0, value.mtime.1),
      ctime: system_time_from_time(value.ctime.0, value.ctime.1),
      crtime: system_time_from_time(value.crtime.0, value.crtime.1),
      kind: value.kind.into(),
      perm: value.perm,
      nlink: value.nlink,
      uid: value.uid,
      gid: value.gid,
      rdev: value.rdev,
      blksize: value.blksize,
      flags: value.flags,
    }
  }
}

#[derive(Debug, Archive, Serialize, Deserialize, Clone, PartialEq)]
#[archive(
  compare(PartialEq),
  check_bytes,
)]
pub enum InodeDescriptor {
  File(Vec<u64>),
  /// InodeDescriptor for a directory, where the BTreeMap is (filename -> inode)
  Directory(BTreeMap<Vec<u8>, u64>),
  Symlink
}
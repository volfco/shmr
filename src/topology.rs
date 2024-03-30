use std::collections::BTreeMap;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use fuser::FileAttr;
use rkyv::{Archive, Deserialize, Serialize};
use anyhow::{Context, Result};
use libc::c_int;
use log::error;
use crate::ConfigStub;

type Inode = u64;

/// DirectoryDescriptor is used to list file entries in a directory and map them to their
/// respective inodes.
type DirectoryDescriptor = BTreeMap<Vec<u8>, Inode>;

fn system_time_from_time(secs: i64, nsecs: u32) -> SystemTime {
  if secs >= 0 {
    UNIX_EPOCH + Duration::new(secs as u64, nsecs)
  } else {
    UNIX_EPOCH - Duration::new((-secs) as u64, nsecs)
  }
}

#[derive(Debug, Clone)]
pub struct Superblock {
  pub(crate) db: sled::Db,
}

impl Superblock {
  pub fn open(config: &ConfigStub) -> Result<Self> {
    Ok(Self {
      db: sled::open(config.workspace.join("superblock.db"))?,
    })
  }
  pub fn gen_inode(&self) -> Result<Inode> {
    self.db.generate_id().context("Failed to generate inode")
  }

  pub fn create_inode(&self, descriptor: InodeDescriptor) -> Result<Inode> {
    let inode = self.gen_inode()?;
    self.update_inode(&inode, descriptor)?;
    Ok(inode)
  }
  pub fn read_inode(&self, inode: &Inode) -> Result<InodeDescriptor, c_int> {
    // todo does format have any performance implications?
    // get the InodeDescriptor from the database
    let op = match self.db.get(format!("inode-{}", inode).as_bytes()) {
      Ok(op) => op,
      Err(_) => return Err(libc::EIO), // throw EIO if there is an error reading the inode
    };

    if op.is_none() {
      return Err(libc::ENOENT);
    }

    let binding = op.unwrap();
    let archived = rkyv::check_archived_root::<InodeDescriptor>(&binding).unwrap();
    let inode_metadata: InodeDescriptor = match archived.deserialize(&mut rkyv::Infallible) {
      Ok(inode_metadata) => inode_metadata,
      Err(e) => {
        // this should never happen, but you can never say never
        error!("Failed to deserialize InodeDescriptor: {:?}", e);
        return Err(libc::EIO)
      }
    };

    Ok(inode_metadata)
  }

  pub fn update_inode(&self, inode: &Inode, descriptor: InodeDescriptor) -> Result<()> {
    let pos = rkyv::to_bytes::<_, 256>(&descriptor).context("Failed to serialize InodeDescriptor")?;

    // insert the InodeDescriptor, ignoring if anything is returned
    // at this point,
    let _ = self.db.insert(format!("inode-{}", inode).as_bytes(), &pos[..]).context("Failed to update inode")?;

    Ok(())
  }

  pub fn get_fileattr(&self, inode: &Inode) -> Result<FileAttr, c_int> {
    let desc = self.read_inode(inode)?;
    let attrs = desc.get_attributes();
    Ok(FileAttr {
      ino: *inode,
      size: 0,
      blocks: 0,
      blksize: 0,
      atime: system_time_from_time(attrs.last_accessed.0, attrs.last_accessed.1),
      mtime: system_time_from_time(attrs.last_modified.0, attrs.last_modified.1),
      ctime: system_time_from_time(attrs.last_metadata_changed.0, attrs.last_metadata_changed.1),
      crtime: SystemTime::UNIX_EPOCH,
      kind: match desc {
        InodeDescriptor::File(_, _) => fuser::FileType::RegularFile,
        InodeDescriptor::Directory(_, _) => fuser::FileType::Directory,
        InodeDescriptor::Symlink => fuser::FileType::Symlink,
      },
      perm: attrs.mode as u16,
      nlink: attrs.hardlinks,
      uid: attrs.uid,
      gid: attrs.gid,
      rdev: 0,
      flags: 0,
    })
  }
}

impl Drop for Superblock {
  fn drop(&mut self) {
    self.db.flush().unwrap();
  }
}

#[derive(Debug, Archive, Serialize, Deserialize, Clone, PartialEq)]
#[archive(
compare(PartialEq),
check_bytes,
)]
/// Describe the contents of the inode entry
pub enum InodeDescriptor {
  File(InodeAttributes, FileDescriptor),
  Directory(InodeAttributes, DirectoryDescriptor),
  Symlink,
}

impl InodeDescriptor {
  pub fn get_attributes(&self) -> &InodeAttributes {
    match self {
      InodeDescriptor::File(attrs, _) => attrs,
      InodeDescriptor::Directory(attrs, _) => attrs,
      InodeDescriptor::Symlink => panic!("Symlinks do not have attributes"),
    }
  }

  pub fn check_access(&self, uid: u32, gid: u32, mut access_mask: i32) -> bool {
    // F_OK tests for existence of file
    if access_mask == libc::F_OK {
      return true;
    }

    let parent_attrs = self.get_attributes();

    let file_mode = parent_attrs.mode as i32;

    // root is allowed to read & write anything
    if uid == 0 {
      // root only allowed to exec if one of the X bits is set
      access_mask &= libc::X_OK;
      access_mask -= access_mask & (file_mode >> 6);
      access_mask -= access_mask & (file_mode >> 3);
      access_mask -= access_mask & file_mode;
      return access_mask == 0;
    }

    if uid == parent_attrs.uid {
      access_mask -= access_mask & (file_mode >> 6);
    } else if gid == parent_attrs.gid {
      // TODO we might need more indepth group checking
      access_mask -= access_mask & (file_mode >> 3);
    } else {
      access_mask -= access_mask & file_mode;
    }

    access_mask == 0
  }
}

#[derive(Debug, Archive, Serialize, Deserialize, Clone, PartialEq)]
#[archive(
compare(PartialEq),
check_bytes,
)]
/// FileTopology describes the topology of a file. 
/// This is used to tell Shmr about how to store or reconstruct the file's data.
pub enum FileTopology {
  /// Only store one copy of the block
  Single,
  /// Mirror (n copies)
  Mirror(u8),
  /// Reed-Solomon (data, parity)
  ReedSolomon(usize, usize),
}

#[derive(Debug, Archive, Serialize, Deserialize, Clone, PartialEq)]
#[archive(
compare(PartialEq),
check_bytes,
)]
pub struct InodeAttributes {
  pub open_file_handles: u64,
  // Ref count of open file handles to this inode
  pub last_accessed: (i64, u32),
  pub last_modified: (i64, u32),
  pub last_metadata_changed: (i64, u32),

  // Permissions and special mode bits
  pub mode: u32,
  pub hardlinks: u32,
  pub uid: u32,
  pub gid: u32,
  pub xattrs: BTreeMap<Vec<u8>, Vec<u8>>,
}

impl From<&InodeAttributes> for FileAttr {
  fn from(_value: &InodeAttributes) -> Self {
    todo!()
  }
}

#[derive(Debug, Archive, Serialize, Deserialize, Clone, PartialEq)]
#[archive(
compare(PartialEq),
check_bytes,
)]
pub struct FileDescriptor {
  pub inode: Inode,

  /// File Size, in bytes
  pub size: u64,
  /// Block Size, in bytes
  pub block_size: u16,
  /// Block Topology
  pub topology: Vec<BlockTopology>,
  pub topology_type: FileTopology,

}

#[derive(Debug, Archive, Serialize, Deserialize, Clone, PartialEq)]
#[archive(
compare(PartialEq),
check_bytes,
)]
pub struct BlockTopology {
  pub block: usize,
  pub hash: Vec<u8>,
  pub size: usize,
  pub layout: (u8, u8),
  /// Shard Path is relative to the Filesystem's UUID
  pub shards: Vec<(String, String)>,
}

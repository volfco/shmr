use std::collections::{BTreeMap, HashMap};
use std::fs::OpenOptions;
use std::io::{Seek, SeekFrom, Write};
use std::path::PathBuf;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use fuser::{FileAttr, FUSE_ROOT_ID};
use rkyv::{Archive, Deserialize, Serialize};
use anyhow::{Context, Result};
use libc::c_int;
use log::{debug, error, info, trace};
use reed_solomon_erasure::galois_8::ReedSolomon;
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
pub struct FileAttributes {
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
}
impl FileAttributes {
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
impl From<FileAttributes> for FileAttr {
  fn from(value: FileAttributes) -> Self {
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

#[derive(Debug)]
pub struct Superblock {
  pub(crate) db: sled::Db,
}

impl Superblock {
  pub fn open(config: &ConfigStub) -> Result<Self> {
    let path = config.workspace.join("superblock.db");

    debug!("Opening superblock at {:?}", &path);
    let db = sled::open(path)?;

    let supa = Self {
      db,
    };

    // check if the 0 inode exists. if not, we're on a fresh filesystem and we need to do some basic initialization
    if !supa.inode_exists(&FUSE_ROOT_ID)? {
      info!("Initializing filesystem");

      // create the root inode
      supa.inode_update(&FUSE_ROOT_ID, &FileAttributes {
        ino: FUSE_ROOT_ID,
        size: 0,
        blocks: 0,
        atime: crate::time_now(),
        mtime: crate::time_now(),
        ctime: crate::time_now(),
        crtime: crate::time_now(),
        kind: IFileType::Directory,
        perm: 0o755,
        nlink: 1,
        uid: 0,
        gid: 0,
        rdev: 0,
        blksize: 0,
        flags: 0,
      }).unwrap(); // todo this is fucky

      supa.directory_create(&FUSE_ROOT_ID, &FUSE_ROOT_ID)?;
    }
    Ok(supa)
  }

  fn inode_names(&self, inode: &Inode) -> (Vec<u8>, Vec<u8>) {
    let base = format!("shmr_{}", inode).into_bytes();
    let mut topology = base.clone();
    topology.extend_from_slice(b"_topology");
    (base, topology)
  }

  pub fn gen_inode(&self) -> Result<Inode, c_int> {
    self.db.generate_id().map_err(|_e| libc::EIO)
  }

  pub fn inode_exists(&self, inode: &Inode) -> Result<bool> {
    Ok(self.db.contains_key(self.inode_names(inode).0)?)
  }

  pub fn inode_create(&self, descriptor: &FileAttributes) -> Result<Inode, c_int> {
    let inode = self.gen_inode()?;
    self.inode_update(&inode, descriptor)?;
    Ok(inode)
  }

  pub fn inode_read(&self, inode: &Inode) -> Result<FileAttributes, c_int> {
    trace!("reading inode {}", inode);

    // todo does format have any performance implications?
    // get the InodeDescriptor from the database
    let op = match self.db.get(self.inode_names(inode).0) {
      Ok(op) => op,
      Err(_) => return Err(libc::EIO), // throw EIO if there is an error reading the inode
    };

    if op.is_none() {
      return Err(libc::ENOENT);
    }

    let binding = op.unwrap();
    let archived = rkyv::check_archived_root::<FileAttributes>(&binding).unwrap();
    let inode_metadata: FileAttributes = match archived.deserialize(&mut rkyv::Infallible) {
      Ok(inode_metadata) => inode_metadata,
      Err(e) => {
        // this should never happen, but you can never say never
        error!("Failed to deserialize FileAttributes: {:?}", e);
        return Err(libc::EIO)
      }
    };

    self.inode_accessed(inode)?;

    Ok(inode_metadata)
  }

  pub fn inode_update(&self, inode: &Inode, descriptor: &FileAttributes) -> Result<(), c_int> {
    trace!("Updating inode {}. Contents: {:?}", inode, descriptor);
    let pos = rkyv::to_bytes::<_, 256>(descriptor).map_err(|_e| libc::EIO)?;
    let _ = self.db.insert(self.inode_names(inode).0, &pos[..]).map_err(|_e| libc::EIO)?;
    Ok(())
  }

  /// Delete the given Inode and it's associated data
  pub fn inode_remove(&self, inode: &Inode) -> Result<()> {
    let _ = self.db.remove(self.inode_names(inode).0).context("Failed to remove inode")?;
    let _ = self.db.remove(self.inode_names(inode).1).context("Failed to remove inode")?;
    Ok(())
  }

  /// Read Inode with Permission Check
  pub fn inode_pread(&self, inode: &Inode, uid: u32, gid: u32, access_mask: i32) -> Result<FileAttributes, c_int> {
    let inode_metadata = self.inode_read(inode)?;

    if !inode_metadata.check_access(uid, gid, access_mask) {
      return Err(libc::EACCES);
    }

    Ok(inode_metadata)
  }

  pub(crate) fn inode_modified(&self, inode: &Inode) -> Result<(), c_int> {
    let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
    let now = (now.as_secs() as i64, now.subsec_nanos());

    let mut inode_metadata = self.inode_read(inode)?;
    inode_metadata.mtime = now;
    inode_metadata.ctime = now;

    self.inode_update(inode, &inode_metadata)?;
    Ok(())
  }

  pub(crate) fn inode_accessed(&self, _inode: &Inode) -> Result<(), c_int> {
    // let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
    // let now = (now.as_secs() as i64, now.subsec_nanos());
    //
    // let mut inode_metadata = self.inode_read(inode)?;
    // inode_metadata.atime = now;
    //
    // self.inode_update(inode, &inode_metadata)?;
    Ok(())
  }

  // Directory Operations
  /// Read the contents of a directory inode
  pub fn directory_read(&self, inode: &Inode) -> Result<DirectoryDescriptor, c_int> {
    // todo add a LRU cache to reduce the number of reads on disk... maybe?
    let raw = self.db.get(self.inode_names(inode).1).map_err(|_e| libc::EIO )?;

    if raw.is_none() {
      error!("Directory not found for inode: {}", inode);
      return Err(libc::EIO);
    }

    let data = raw.unwrap();
    let archived = rkyv::check_archived_root::<DirectoryDescriptor>(&data).unwrap();
    match archived.deserialize(&mut rkyv::Infallible) {
      Ok(directory) => Ok(directory),
      Err(e) => {
        panic!("unable to deserialize DirectoryDescriptor: {:?}", e);
      }
    }
  }

  /// Create a Directory with the Given Inode
  /// Parent is specified to add the `..` entry
  pub fn directory_create(&self, parent: &Inode, inode: &Inode) -> Result<()> {
    if !self.inode_exists(inode)? {
      panic!("Attempting to create a directory with an inode that does not exist");
    }

    // populate the directory with the current directory and parent directory entries
    let mut directory: DirectoryDescriptor = BTreeMap::new();
    directory.insert(".".as_bytes().to_vec(), *inode);
    directory.insert("..".as_bytes().to_vec(), *parent);

    let pos = rkyv::to_bytes::<_, 256>(&directory).context("Failed to serialize DirectoryDescriptor")?;
    let _ = self.db.insert(self.inode_names(inode).1, &pos[..]).context("Failed to create DirectoryDescriptor")?;

    debug!("created directory descriptor for inode {} under {}", inode, parent);

    Ok(())
  }

  /// Check if the given Directory contains the given name
  pub fn directory_contains(&self, inode: &Inode, name: &Vec<u8>) -> Result<bool, c_int> {
    Ok(self.directory_read(inode)?.contains_key(name))
  }

  pub fn directory_get_entry(&self, inode: &Inode, name: &Vec<u8>) -> Result<Option<Inode>, c_int> {
    Ok(self.directory_read(inode)?.get(name).cloned())
  }

  /// Insert a new entry into the given parent directory
  pub fn directory_entry_insert(&self, parent: &Inode, name: Vec<u8>, inode: &Inode) -> Result<(), c_int> {
    debug!("Inserting entry {:?} -> {} into directory {}", name, inode, parent);
    let mut dir_contents = self.directory_read(parent)?;
    if dir_contents.contains_key(&name) {
      return Err(libc::EEXIST);
    }

    dir_contents.insert(name, *inode);

    trace!("Directory contents: {:?}", dir_contents);

    let pos = rkyv::to_bytes::<_, 256>(&dir_contents).context("Failed to serialize DirectoryDescriptor").map_err(|_e| libc::EIO)?;
    // we are updating the parent entry, not the one we just created!
    let _ = self.db.insert(self.inode_names(parent).1, &pos[..]).context("Failed to update DirectoryDescriptor").map_err(|_e| libc::EIO)?;

    // self.inode_modified(parent)?;
    let _ = self.db.flush();
    Ok(())
  }

  pub fn directory_entry_remove(&self, parent: &Inode, name: &Vec<u8>) -> Result<(), c_int> {
    let mut dir_contents = self.directory_read(parent)?;
    if !dir_contents.contains_key(name) {
      return Err(libc::ENOENT);
    }

    dir_contents.remove(name);

    let pos = rkyv::to_bytes::<_, 256>(&dir_contents).context("Failed to serialize DirectoryDescriptor").map_err(|_e| libc::EIO)?;
    let _ = self.db.insert(self.inode_names(parent).1, &pos[..]).context("Failed to update DirectoryDescriptor").map_err(|_e| libc::EIO)?;

    self.inode_modified(parent)?;

    Ok(())
  }

  // Files

  /// Initialize a FileInformation object for the given Inode with an empty Topology
  pub fn file_initialize(&self, inode: &Inode) -> Result<(), c_int> {
    if !self.inode_exists(inode).map_err(|_e| libc::EIO)? {
      panic!("Attempting to create a directory with an inode that does not exist");
    }

    let fd = FileInformation {
      inode: *inode,
      size: 0,
      block_size: 4096, // TODO This needs to be dynamic
      topology: vec![],
      topology_type: FileTopology::Empty,
    };

    let pos = rkyv::to_bytes::<_, 256>(&fd).context("Failed to serialize FileInformation").map_err(|_e| libc::EIO)?;
    let _ = self.db.insert(self.inode_names(inode).1, &pos[..]).context("Failed to create FileInformation").map_err(|_e| libc::EIO)?;

    debug!("created FileInformation for inode {}", inode);

    Ok(())
  }

  pub fn file_write(&self, inode: &Inode, offet: i64, data: &[u8]) -> Result<usize, c_int> {
    todo!()
  }

  pub fn read_file_topology(&self, inode: &Inode) -> Result<FileInformation, c_int> {
    let raw = self.db.get(self.inode_names(inode).1).map_err(|_e| libc::EIO )?;

    if raw.is_none() {
      error!("Topology not found for inode: {}", inode);
      return Err(libc::EIO);
    }

    let data = raw.unwrap();
    let archived = rkyv::check_archived_root::<FileInformation>(&data).unwrap();
    match archived.deserialize(&mut rkyv::Infallible) {
      Ok(topology) => Ok(topology),
      Err(e) => {
        panic!("unable to deserialize FileInformation: {:?}", e);
      }
    }
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
  File(InodeAttributes, FileInformation),
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


}

#[derive(Debug, Archive, Serialize, Deserialize, Clone, PartialEq)]
#[archive(
compare(PartialEq),
check_bytes,
)]
/// FileTopology describes the topology of a file.
/// This is used to tell Shmr about how to store or reconstruct the file's data.
pub enum FileTopology {
  /// Empty, zero length, File.
  /// Used to denote when a file is empty, or just a placeholder until the file is written to.
  Empty,
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
// TODO this struct name kinda sucks. rename it to something more descriptive
pub struct FileInformation {
  pub inode: Inode,

  /// File Size, in bytes
  pub size: u64,
  /// Block Size, in bytes
  pub block_size: u16,
  /// Block Topology
  pub topology: Vec<BlockTopology>,
  pub topology_type: FileTopology,
}
impl FileInformation {
  /// Read from the given offset
  // pub fn read(&self, buffer: &mut [u8], offset: i64) -> Result<usize> {
  //   // figure out which block we are starting at
  //   let starting_block = offset / self.block_size as i64;
  //   let block_offset = offset % self.block_size as i64;
  //
  //   for block in &self.topology {
  //     if block.block < starting_block as usize {
  //       continue;
  //     }
  //
  //     // read the underlying block
  //
  //
  //   }
  //
  //   todo!()
  //
  // }

  pub fn write(&mut self, config: &ConfigStub, data: &[u8], offset: i64) -> Result<usize> {
    // offset is where this data should be written in the file, i.e. where the cursor should be placed

    // write the data to the proper block for the given inode
    let block_path = config.workspace.join("blocks");

    // which block we're writing to
    let starting_block = match offset > 0 {
      true => offset / self.block_size as i64,
      false => 0
    };
    let ending_block = ((offset + data.len() as i64) / self.block_size as i64) + 1;

    debug!("Writing {} bytes to block {} ending at block {}", data.len(), starting_block, ending_block);

    for i in starting_block..ending_block {
      let mut block_offset = 0;
      if i == starting_block {
        // we only need to worry about the offset on the first block
        block_offset = offset % self.block_size as i64;
      }
      let path = block_path.join(format!("{}_{}.bin", self.inode, i));
      trace!("Writing to {:?} at offset {}", path, block_offset);

      let mut fh = OpenOptions::new().write(true).create(true).open(path)?;
      fh.seek(SeekFrom::Start(block_offset as u64))?;
      fh.write_all(data)?;

      // TODO Update the filesize
      // if data.len() + offset as usize > attrs.size as usize {
      //   attrs.size = (data.len() + offset as usize) as u64;
      // }
    }

    Ok(data.len())
  }

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
  /// Shard Path is relative to the Filesystem's UUID
  pub shards: Vec<(String, String)>,
}
impl BlockTopology {

  // /// Read each shard from the filesystem
  // pub fn read(&self, archives: HashMap<String, PathBuf>, skip_after: usize) -> Result<Vec<Option<Vec<u8>>>> {
  //   // skip_after: if the first n shards load, skip the rest
  //   let mut shards: Vec<Option<Vec<u8>>> = Vec::new();
  //
  //   let mut can_skip = true;
  //   for (i, shard) in self.shards.iter().enumerate() {
  //     let (archive_uuid, shard) = shard;
  //
  //     let archive = archives.get(archive_uuid);
  //     if archive.is_none() {
  //       error!("Archive {} not found", archive_uuid);
  //
  //       can_skip = false;
  //       continue;
  //     }
  //
  //     let path = archive.unwrap().join(shard);
  //     debug!("Reading block shard from {}", path.display());
  //
  //     match std::fs::read(&path) {
  //       Ok(raw) => shards.push(Some(raw)),
  //       Err(e) => {
  //         error!("Failed to read block shard from {}. {:?}", path.display(), e);
  //
  //         // used to track if we can skip reconstructing the block
  //         // the first n shards are data shards, the rest are parity shards
  //         // so if all the data shards load correctly, we can skip the parity shards
  //         can_skip = false;
  //
  //         shards.push(None);
  //       }
  //     }
  //
  //     if i >= (skip_after - 1) && can_skip {
  //       debug!("first {} shards loaded successfully, allowed to skip the rest", skip_after);
  //       break;
  //     }
  //   }
  //
  //   Ok(shards)
  // }
}

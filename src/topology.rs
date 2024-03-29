use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use fuser::FileAttr;
use rkyv::{Archive, Deserialize, Serialize};
type Inode = u64;

/// DirectoryDescriptor is used to list file entries in a directory and map them to their
/// respective inodes.
type DirectoryDescriptor = BTreeMap<Vec<u8>, Inode>;

#[derive(Debug, Archive, Serialize, Deserialize, Clone)]
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


#[derive(Debug, Archive, Serialize, Deserialize, Clone)]
/// Describe the contents of the inode entry
pub enum InodeDescriptor {
    File(FileDescriptor),
    Directory(DirectoryDescriptor),
    Symlink
}

#[derive(Debug, Archive, Serialize, Deserialize, Clone)]
pub struct Superblock {
    /// Inode array
    pub inodes: Vec<Option<InodeDescriptor>>,
}

#[derive(Debug, Archive, Serialize, Deserialize, Clone)]
pub struct FileDescriptor {
    pub inode: Inode,

    /// File Size, in bytes
    pub size: u64,
    /// Block Size, in bytes
    pub block_size: u16,
    /// Block Topology
    pub topology: Vec<BlockTopology>,
    pub topology_type: FileTopology,

    pub open_file_handles: u64, // Ref count of open file handles to this inode
    pub last_accessed: (i64, u32),
    pub last_modified: (i64, u32),
    pub last_metadata_changed: (i64, u32),

    // Permissions and special mode bits
    pub mode: u16,
    pub hardlinks: u32,
    pub uid: u32,
    pub gid: u32,
    pub xattrs: BTreeMap<Vec<u8>, Vec<u8>>,
}

#[derive(Debug, Archive, Serialize, Deserialize, Clone)]
pub struct BlockTopology {
    pub block: usize,
    pub hash: Vec<u8>,
    pub size: usize,
    pub layout: (u8, u8),
    /// Shard Path is relative to the Filesystem's UUID
    pub shards: Vec<(String, String)>,
}

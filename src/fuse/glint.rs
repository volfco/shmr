use uuid::Uuid;
use crate::fsdb::FsDB2;
use crate::fuse::types::{Inode, InodeDescriptor};
use crate::vfs::VirtualFile;

pub struct Glint {
    inode_db: FsDB2<u64, Inode>,
    // TODO Fold this into inode_db
    descriptor_db: FsDB2<u64, InodeDescriptor>,
    vfdb: FsDB2<Uuid, VirtualFile>
}
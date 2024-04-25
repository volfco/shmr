use crate::file::VirtualFile;
use crate::fsdb::FsDB;
use crate::storage::PoolMap;
use fuser::{
    FileAttr, FileType, Filesystem, KernelConfig, ReplyAttr, ReplyDirectory, ReplyEntry, ReplyOpen,
    ReplyWrite, Request, TimeOrNow, FUSE_ROOT_ID,
};
use libc::c_int;
use log::{debug, error, info, trace, warn};
use rkyv::{Archive, Deserialize, Serialize};
use std::collections::BTreeMap;
use std::ffi::OsStr;
use std::os::unix::prelude::OsStrExt;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

const MAX_NAME_LENGTH: u32 = 255;
const DEFAULT_BLOCK_SIZE: usize = 1024 * 1024 * 4; // 4MB

#[allow(dead_code)]
pub fn time_now() -> (i64, u32) {
    let now = SystemTime::now();
    let since_the_epoch = now.duration_since(UNIX_EPOCH).expect("Time went backwards");
    (
        since_the_epoch.as_secs() as i64,
        since_the_epoch.subsec_nanos(),
    )
}
fn system_time_from_time(secs: i64, nsecs: u32) -> SystemTime {
    if secs >= 0 {
        UNIX_EPOCH + Duration::new(secs as u64, nsecs)
    } else {
        UNIX_EPOCH - Duration::new((-secs) as u64, nsecs)
    }
}

#[derive(Debug, Archive, Serialize, Deserialize, Clone, PartialEq)]
#[archive(compare(PartialEq), check_bytes)]
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
#[archive(compare(PartialEq), check_bytes)]
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
#[archive(compare(PartialEq), check_bytes)]
pub enum InodeDescriptor {
    File(VirtualFile),
    /// InodeDescriptor for a directory, where the BTreeMap is (filename -> inode)
    Directory(BTreeMap<Vec<u8>, u64>),
    Symlink,
}

pub struct Shmr {
    pub pool_map: PoolMap,
    pub fs_db: FsDB,
}
impl Shmr {
    fn get_inode(&self, inode: u64) -> Result<Inode, c_int> {
        match self.fs_db.read_inode(inode) {
            Ok(inode) => Ok(inode),
            Err(e) => {
                error!("Failed to read inode {}: {:?}", inode, e);
                Err(libc::EIO)
            }
        }
    }
    fn check_access(&self, inode: u64, uid: u32, gid: u32, access_mask: i32) -> bool {
        match self.get_inode(inode) {
            Ok(inode) => inode.check_access(uid, gid, access_mask),
            Err(_) => false,
        }
    }

    fn get_directory_contents(&self, inode: u64) -> Result<BTreeMap<Vec<u8>, u64>, c_int> {
        match self.fs_db.read_descriptor(inode) {
            Ok(descriptor) => match descriptor {
                InodeDescriptor::Directory(contents) => Ok(contents),
                _ => {
                    error!("Inode {} is not a directory", inode);
                    Err(libc::ENOTDIR)
                }
            },
            Err(e) => {
                error!("Failed to read topology {}: {:?}", inode, e);
                Err(libc::EIO)
            }
        }
    }

    fn create(
        &mut self,
        req: &Request<'_>,
        parent: u64,
        name: &OsStr,
        mode: u32,
        umask: u32,
        rdev: u32,
    ) -> Result<Inode, c_int> {
        let file_type = mode & libc::S_IFMT;

        if file_type != libc::S_IFREG && file_type != libc::S_IFLNK && file_type != libc::S_IFDIR {
            error!(
                "Shmr::create() only supports regular files, symlinks, and directories. Got {:o}",
                mode
            );
            return Err(libc::ENOSYS);
        }

        // check if we can write to this directory
        let mut parent_inode = match self.get_inode(parent) {
            Ok(inode) => inode,
            Err(e) => {
                error!("Failed to read parent inode {}: {:?}", parent, e);
                return Err(libc::ENOENT);
            }
        };

        if !parent_inode.check_access(req.uid(), req.gid(), libc::W_OK) {
            return Err(libc::EACCES);
        }

        if !matches!(parent_inode.kind, IFileType::Directory) {
            warn!("attempted to create file under non-directory inode");
            return Err(libc::ENOTDIR);
        }

        // check if there is already an entry for the given name
        let mut directory_contents = match self.get_directory_contents(parent) {
            Ok(descriptor) => descriptor,
            Err(e) => {
                error!("Failed to read directory contents {}: {:?}", parent, e);
                return Err(libc::EIO);
            }
        };

        if directory_contents.contains_key(name.as_bytes()) {
            return Err(libc::EEXIST);
        }

        // before updating the parent inode, create the child inode first.
        // so if this fails, all we have is an orphaned inode and not a modified directory without the
        // underlying inode
        debug!("Creating child inode for parent {}", parent);
        let gid = if parent_inode.perm & libc::S_ISGID as u16 != 0 {
            parent_inode.gid
        } else {
            req.gid()
        };

        let child_inode = Inode {
            ino: self.fs_db.generate_id(),
            size: 0,
            blocks: 0,
            atime: time_now(),
            mtime: time_now(),
            ctime: time_now(),
            crtime: time_now(),
            kind: match file_type {
                libc::S_IFREG => IFileType::RegularFile,
                libc::S_IFLNK => IFileType::Symlink,
                libc::S_IFDIR => IFileType::Directory,
                _ => unreachable!(),
            },
            perm: mode as u16 & !umask as u16,
            nlink: 1,
            uid: req.uid(),
            gid,
            rdev,
            blksize: 4096,
            flags: 0,
            xattrs: BTreeMap::new(),
        };
        trace!("Creating child inode: {:?}", child_inode);
        self.fs_db
            .write_inode(child_inode.ino, &child_inode)
            .map_err(|e| {
                error!("Failed to write child inode: {:?}", e);
                libc::EIO
            })?;

        let child_descriptor = match file_type {
            libc::S_IFREG => InodeDescriptor::File(VirtualFile::new(DEFAULT_BLOCK_SIZE)),
            libc::S_IFLNK => InodeDescriptor::Symlink,
            libc::S_IFDIR => InodeDescriptor::Directory(BTreeMap::new()),
            _ => unimplemented!(),
        };
        trace!("Creating child topology: {:?}", child_descriptor);
        self.fs_db
            .write_descriptor(child_inode.ino, &child_descriptor)
            .map_err(|e| {
                error!("Failed to write child topology: {:?}", e);
                libc::EIO
            })?;

        // Update the Parent's metadata
        parent_inode.nlink += 1;
        parent_inode.mtime = time_now();
        parent_inode.ctime = time_now();
        self.fs_db
            .write_inode(parent_inode.ino, &parent_inode)
            .map_err(|e| {
                error!("Failed to write parent inode: {:?}", e);
                libc::EIO
            })?;

        // update the parent's directory contents
        directory_contents.insert(name.as_bytes().to_vec(), child_inode.ino);
        self.fs_db
            .write_descriptor(
                parent_inode.ino,
                &InodeDescriptor::Directory(directory_contents),
            )
            .map_err(|e| {
                error!("Failed to write parent topology: {:?}", e);
                libc::EIO
            })?;

        Ok(child_inode)
    }
}
impl Filesystem for Shmr {
    fn init(&mut self, _req: &Request<'_>, _config: &mut KernelConfig) -> Result<(), c_int> {
        // perform a re-index of the filesystem on init
        self.fs_db.index().map_err(|e| {
            error!("Unable to re-index FsDB. {:?}", e);
            libc::EIO
        })?;

        if !self.fs_db.check_inode(FUSE_ROOT_ID) {
            info!("inode 0 does not exist, creating root node");
            let inode = Inode {
                ino: FUSE_ROOT_ID,
                size: 0,
                blocks: 0,
                atime: time_now(),
                mtime: time_now(),
                ctime: time_now(),
                crtime: time_now(),
                kind: IFileType::Directory,
                perm: 0o777,
                nlink: 2,
                uid: 0,
                gid: 0,
                rdev: 0,
                blksize: 0,
                flags: 0,
                xattrs: BTreeMap::new(),
            };
            self.fs_db.write_inode(FUSE_ROOT_ID, &inode).map_err(|e| {
                error!("Failed to write root inode: {:?}", e);
                libc::EIO
            })?;

            let topology = InodeDescriptor::Directory({
                let mut inner = BTreeMap::new();
                inner.insert(b".".to_vec(), FUSE_ROOT_ID);
                inner
            });
            self.fs_db
                .write_descriptor(FUSE_ROOT_ID, &topology)
                .map_err(|e| {
                    error!("Failed to write root topology: {:?}", e);
                    libc::EIO
                })?;
        }
        Ok(())
    }

    /// Look up a directory entry by name and get its attributes.
    fn lookup(&mut self, req: &Request, parent: u64, name: &OsStr, reply: ReplyEntry) {
        if name.len() > MAX_NAME_LENGTH as usize {
            reply.error(libc::ENAMETOOLONG);
            return;
        }

        // this checks the inode's existence, and if we can access it
        if !self.check_access(parent, req.uid(), req.gid(), libc::X_OK) {
            reply.error(libc::EACCES);
            return;
        }

        let directory_contents = match self.get_directory_contents(parent) {
            Ok(descriptor) => descriptor,
            Err(e) => {
                error!("Failed to read directory contents {}: {:?}", parent, e);
                reply.error(libc::EIO);
                return;
            }
        };

        match directory_contents.get(name.as_bytes()) {
            Some(entry_inode) => {
                let attrs = match self.fs_db.read_inode(*entry_inode) {
                    Err(e) => {
                        error!("Unable to read inode {}: {:?}", entry_inode, e);
                        reply.error(libc::EIO);
                        return;
                    }
                    Ok(inode_attrs) => inode_attrs,
                };
                reply.entry(&Duration::new(0, 0), &attrs.into(), 0)
            }
            None => reply.error(libc::ENOENT),
        }
    }

    fn getattr(&mut self, req: &Request<'_>, ino: u64, reply: ReplyAttr) {
        trace!("FUSE({}) 'getattr' invoked for inode {}", req.unique(), ino);
        match self.get_inode(ino) {
            Ok(inode) => {
                reply.attr(&Duration::new(0, 0), &inode.into());
            }
            Err(_) => {
                reply.error(libc::ENOENT);
            }
        }
    }

    fn setattr(
        &mut self,
        req: &Request<'_>,
        ino: u64,
        mode: Option<u32>,
        uid: Option<u32>,
        gid: Option<u32>,
        size: Option<u64>,
        atime: Option<TimeOrNow>,
        mtime: Option<TimeOrNow>,
        ctime: Option<SystemTime>,
        fh: Option<u64>,
        crtime: Option<SystemTime>,
        chgtime: Option<SystemTime>,
        bkuptime: Option<SystemTime>,
        flags: Option<u32>,
        reply: ReplyAttr,
    ) {
        trace!("FUSE({}) 'setattr' invoked on inode {}", req.unique(), ino);

        let mut parent_attr = match self.get_inode(ino) {
            Ok(inode) => inode,
            Err(e) => {
                reply.error(e);
                return;
            }
        };

        if mode.is_some() {
            parent_attr.perm = mode.unwrap() as u16;
        }
        if uid.is_some() {
            parent_attr.uid = uid.unwrap();
        }
        if gid.is_some() {
            parent_attr.gid = gid.unwrap();
        }
        if size.is_some() {
            parent_attr.size = size.unwrap();
        }
        if atime.is_some() {
            match atime.unwrap() {
                TimeOrNow::SpecificTime(time) => {
                    let since_the_epoch = time
                        .duration_since(UNIX_EPOCH)
                        .expect("Time went backwards");
                    parent_attr.atime = (
                        since_the_epoch.as_secs() as i64,
                        since_the_epoch.subsec_nanos(),
                    );
                }
                TimeOrNow::Now => {
                    let (sec, nsec) = time_now();
                    parent_attr.atime = (sec, nsec);
                }
            }
        }
        if mtime.is_some() {
            match atime.unwrap() {
                TimeOrNow::SpecificTime(time) => {
                    let since_the_epoch = time
                        .duration_since(UNIX_EPOCH)
                        .expect("Time went backwards");
                    parent_attr.atime = (
                        since_the_epoch.as_secs() as i64,
                        since_the_epoch.subsec_nanos(),
                    );
                }
                TimeOrNow::Now => {
                    let (sec, nsec) = time_now();
                    parent_attr.atime = (sec, nsec);
                }
            }
        }
        if ctime.is_some() {
            let since_the_epoch = ctime
                .unwrap()
                .duration_since(UNIX_EPOCH)
                .expect("Time went backwards");
            parent_attr.ctime = (
                since_the_epoch.as_secs() as i64,
                since_the_epoch.subsec_nanos(),
            );
        }
        if crtime.is_some() {
            let since_the_epoch = crtime
                .unwrap()
                .duration_since(UNIX_EPOCH)
                .expect("Time went backwards");
            parent_attr.crtime = (
                since_the_epoch.as_secs() as i64,
                since_the_epoch.subsec_nanos(),
            );
        }
        if flags.is_some() {
            parent_attr.flags = flags.unwrap();
        }
        if fh.is_some() {
            unimplemented!("fh not implemented");
        }
        if chgtime.is_some() {
            unimplemented!("chgtime not implemented");
        }
        if bkuptime.is_some() {
            unimplemented!("bkuptime not implemented");
        }

        if let Err(e) = self.fs_db.write_inode(ino, &parent_attr) {
            error!("Failed to write inode {}: {:?}", ino, e);
            reply.error(libc::EIO);
            return;
        }

        debug!(
            "FUSE({}) 'setattr' success. inode {} updated",
            req.unique(),
            ino
        );

        reply.attr(&Duration::new(0, 0), &parent_attr.into());
    }

    /// Create a file node
    // This is called for creation of all non-directory, non-symlink nodes. If the filesystem defines a create() method, then for regular files that will be called instead.
    fn mknod(
        &mut self,
        _req: &Request<'_>,
        parent: u64,
        name: &OsStr,
        mode: u32,
        umask: u32,
        rdev: u32,
        reply: ReplyEntry,
    ) {
        match self.create(_req, parent, name, mode, umask, rdev) {
            Ok(inode) => {
                reply.entry(&Duration::new(0, 0), &inode.into(), 0);
            }
            Err(e) => {
                reply.error(e);
            }
        }
    }

    fn mkdir(
        &mut self,
        req: &Request<'_>,
        parent: u64,
        name: &OsStr,
        mode: u32,
        umask: u32,
        reply: ReplyEntry,
    ) {
        match self.create(req, parent, name, mode | libc::S_IFDIR, umask, 0) {
            Ok(inode) => {
                reply.entry(&Duration::new(0, 0), &inode.into(), 0);
            }
            Err(e) => {
                reply.error(e);
            }
        }
    }

    // /// Create a symbolic link
    // fn symlink(&mut self, _req: &Request<'_>, parent: u64, link_name: &OsStr, target: &Path, reply: ReplyEntry) {
    //     todo!()
    // }

    // Not used, as mknod is used for all non-directory, non-symlink nodes
    // fn create(&mut self, _req: &Request<'_>, parent: u64, name: &OsStr, mode: u32, umask: u32, flags: i32, reply: ReplyCreate) {
    //     todo!()
    // }

    fn write(
        &mut self,
        req: &Request<'_>,
        ino: u64,
        fh: u64,
        offset: i64,
        data: &[u8],
        _write_flags: u32,
        _flags: i32,
        _lock_owner: Option<u64>,
        reply: ReplyWrite,
    ) {
        trace!(
            "FUSE({}) 'write' invoked on inode {} for fh {} starting at offset {}. data length: {}",
            req.unique(),
            ino,
            fh,
            offset,
            data.len()
        );

        assert!(offset >= 0);
        // if !check_file_handle_write(fh) {
        //   reply.error(libc::EACCES);
        //   return;
        // }

        let file_inode = match self.fs_db.read_inode(ino) {
            Ok(inode) => inode,
            Err(e) => {
                error!("Failed to read inode {}: {:?}", ino, e);
                reply.error(libc::ENOENT);
                return;
            }
        };

        if !file_inode.check_access(req.uid(), req.gid(), libc::W_OK) {
            reply.error(libc::EACCES);
            return;
        }

        let descriptor = match self.fs_db.read_descriptor(ino) {
            Ok(descriptor) => descriptor,
            Err(e) => {
                error!("Failed to read descriptor {}: {:?}", ino, e);
                reply.error(libc::EIO);
                return;
            }
        };

        let mut vf = match descriptor {
            InodeDescriptor::File(vf) => vf,
            _ => {
                error!("Inode {} is not a file", ino);
                reply.error(libc::EISDIR);
                return;
            }
        };

        match vf.write(&self.pool_map, data) {
            Ok(amount) => {

                self.fs_db.write_descriptor(ino, &InodeDescriptor::File(vf)).unwrap();


                reply.written(amount as u32);
            }
            Err(e) => {
                error!("Failed to write data to file: {:?}", e);
                reply.error(libc::EIO);
                return;
            }
        }
    }

    /// Open a directory.
    /// Filesystem may store an arbitrary file handle (pointer, index, etc) in fh, and use this in
    /// other all other directory stream operations (readdir, releasedir, fsyncdir). Filesystem may
    /// also implement stateless directory I/O and not store anything in fh, though that makes it
    /// impossible to implement standard conforming directory stream operations in case the
    /// contents of the directory can change between opendir and releasedir.
    fn opendir(&mut self, req: &Request<'_>, ino: u64, flags: i32, reply: ReplyOpen) {
        debug!("FUSE({}) 'opendir' invoked on inode {}", req.unique(), ino);
        let (access_mask, _read, _write) = match flags & libc::O_ACCMODE {
            libc::O_RDONLY => {
                // Behavior is undefined, but most filesystems return EACCES
                if flags & libc::O_TRUNC != 0 {
                    reply.error(libc::EACCES);
                    return;
                }
                (libc::R_OK, true, false)
            }
            libc::O_WRONLY => (libc::W_OK, false, true),
            libc::O_RDWR => (libc::R_OK | libc::W_OK, true, true),
            // Exactly one access mode flag must be specified
            _ => {
                reply.error(libc::EINVAL);
                return;
            }
        };

        self.check_access(ino, req.uid(), req.gid(), access_mask);

        // TODO Track open file handles

        reply.opened(0, 0);
    }

    // Read directory.
    /// Send a buffer filled using buffer.fill(), with size not exceeding the requested size. Send
    /// an empty buffer on end of stream. fh will contain the value set by the opendir method, or
    /// will be undefined if the opendir method didn’t set any value.
    fn readdir(
        &mut self,
        req: &Request<'_>,
        inode: u64,
        _fh: u64,
        offset: i64,
        mut reply: ReplyDirectory,
    ) {
        assert!(offset >= 0);

        trace!(
            "FUSE({}) 'readdir' invoked for inode {} with offset {}",
            req.unique(),
            inode,
            offset
        );

        let entries = match self.get_directory_contents(inode) {
            Ok(tree) => tree,
            Err(e) => {
                warn!("error reading directory: {:?}", e);
                reply.error(libc::ENOENT);
                return;
            }
        };

        for (index, entry) in entries.iter().skip(offset as usize).enumerate() {
            let file_name = OsStr::from_bytes(entry.0);
            trace!(
                "FUSE({}) 'readdir' entry: {:?} -> {}",
                req.unique(),
                file_name,
                entry.1
            );
            let buffer_full: bool = reply.add(
                *entry.1,
                offset + index as i64 + 1,
                FileType::Directory,
                OsStr::from_bytes(entry.0),
            );

            if buffer_full {
                break;
            }
        }

        reply.ok();
    }
}

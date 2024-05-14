mod types;
mod magics;
mod glint;
mod cache;


use crate::fsdb::FsDB2;
use crate::{PoolMap, ShmrError};
use fuser::{
    FileAttr, FileType, Filesystem, KernelConfig, ReplyAttr, ReplyData, ReplyDirectory, ReplyEmpty,
    ReplyEntry, ReplyOpen, ReplyWrite, Request, TimeOrNow, FUSE_ROOT_ID,
};
use libc::c_int;
use log::{debug, error, info, trace, warn};
use std::collections::BTreeMap;
use std::ffi::OsStr;
use std::os::unix::prelude::OsStrExt;
use std::path::PathBuf;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use crate::fuse::types::{IFileType, Inode, InodeDescriptor};
use crate::vfs::VirtualFile;
use crate::fuse::magics::*;


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


pub struct ShmrFuse {
    pool_map: PoolMap,
    inode_db: FsDB2<u64, Inode>,
    descriptor_db: FsDB2<u64, InodeDescriptor>,
}
impl ShmrFuse {
    pub fn open(pool_map: PoolMap, path: PathBuf) -> Result<Self, ShmrError> {
        let db_path = path.join("shmr");
        Ok(Self {
            pool_map,
            inode_db: FsDB2::open(db_path.join("inode_db")).unwrap(),
            descriptor_db: FsDB2::open(db_path.join("descriptor_db")).unwrap(),
        })
    }

    fn check_access(&self, inode: u64, uid: u32, gid: u32, access_mask: i32) -> bool {
        self.inode_db.has(&inode)
            && self
            .inode_db
            .get(&inode)
            .unwrap()
            .check_access(uid, gid, access_mask)
    }

    fn create(
        &mut self,
        req: &Request<'_>,
        parent: u64,
        name: &OsStr,
        mode: u32,
        umask: u32,
        rdev: u32,
    ) -> Result<u64, c_int> {
        let file_type = mode & libc::S_IFMT;

        if file_type != libc::S_IFREG && file_type != libc::S_IFLNK && file_type != libc::S_IFDIR {
            error!(
                "Shmr::create() only supports regular files, symlinks, and directories. Got {:o}",
                mode
            );
            return Err(libc::ENOSYS);
        }

        let gid = {
            // check if we can write to this directory
            let parent_inode = match self.inode_db.get(&parent) {
                Some(inode) => inode,
                None => {
                    return Err(libc::ENOENT);
                }
            };

            if !parent_inode.check_access(req.uid(), req.gid(), libc::W_OK) {
                return Err(libc::EACCES);
            }

            {
                let descriptor = self.descriptor_db.get(&parent).unwrap();
                if let InodeDescriptor::Directory(contents) = &*descriptor {
                    if contents.contains_key(name.as_bytes()) {
                        return Err(libc::EEXIST);
                    }
                } else {
                    warn!("attempted to create file under non-directory inode");
                    return Err(libc::ENOTDIR);
                }
            }

            // before updating the parent inode, create the child inode first.
            // so if this fails, all we have is an orphaned inode and not a modified directory without the
            // underlying inode
            debug!("Creating child inode for parent {}", parent);
            if parent_inode.perm & libc::S_ISGID as u16 != 0 {
                parent_inode.gid
            } else {
                req.gid()
            }
        };

        let ino = self.inode_db.gen_id().unwrap();
        let child_inode = Inode {
            ino,
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
            blksize: DEFAULT_CHUNK_SIZE as u32,
            flags: 0,
            xattrs: BTreeMap::new(),
        };
        trace!("Creating child inode: {:?}", child_inode);
        self.inode_db.insert(ino, child_inode);

        let child_descriptor = match file_type {
            libc::S_IFREG => InodeDescriptor::File(VirtualFile::new()),
            libc::S_IFLNK => InodeDescriptor::Symlink,
            libc::S_IFDIR => InodeDescriptor::Directory(BTreeMap::new()),
            _ => unimplemented!(),
        };
        trace!("Creating child topology: {:?}", child_descriptor);
        self.descriptor_db.insert(ino, child_descriptor);

        // Update the Parent's metadata
        {
            let mut parent_write = self.inode_db.get_mut(&parent).unwrap();
            parent_write.nlink += 1;
            parent_write.mtime = time_now();
            parent_write.ctime = time_now();
        }

        // update the parent's directory contents
        {
            let mut write = self.descriptor_db.get_mut(&parent).unwrap();
            match &mut *write {
                InodeDescriptor::Directory(entries) => {
                    entries.insert(name.as_bytes().to_vec(), ino);
                }
                _ => panic!(),
            }
        }

        self.inode_db.flush_all().unwrap();
        self.descriptor_db.flush_all().unwrap();

        Ok(ino)
    }
}
impl Filesystem for ShmrFuse {
    fn init(&mut self, _req: &Request<'_>, _config: &mut KernelConfig) -> Result<(), c_int> {
        if !self.inode_db.has(&FUSE_ROOT_ID) {
            info!("inode {} does not exist, creating root node", FUSE_ROOT_ID);

            self.inode_db.insert(
                FUSE_ROOT_ID,
                Inode {
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
                },
            );

            self.descriptor_db.insert(
                FUSE_ROOT_ID,
                InodeDescriptor::Directory({
                    let mut inner = BTreeMap::new();
                    inner.insert(b".".to_vec(), FUSE_ROOT_ID);
                    inner
                }),
            );

            let _ = self.inode_db.flush_all();
            let _ = self.descriptor_db.flush_all();
        }
        Ok(())
    }

    /// Look up a directory entry by name and get its attributes.
    fn lookup(&mut self, req: &Request, parent: u64, name: &OsStr, reply: ReplyEntry) {
        trace!(
            "FUSE({}) 'lookup' invoked for parent {} with name {:?}",
            req.unique(),
            parent,
            name
        );
        if name.len() > MAX_NAME_LENGTH as usize {
            reply.error(libc::ENAMETOOLONG);
            return;
        }

        // this checks the inode's existence, and if we can access it
        if !self.check_access(parent, req.uid(), req.gid(), libc::X_OK) {
            reply.error(libc::EACCES);
            return;
        }

        let descriptor = self.descriptor_db.get(&parent).unwrap();
        if let InodeDescriptor::Directory(contents) = &*descriptor {
            match contents.get(name.as_bytes()) {
                Some(entry_inode) => {
                    let inode = self.inode_db.get(entry_inode).unwrap();
                    reply.entry(&Duration::new(0, 0), &inode.to_fileattr(), 0)
                }
                None => reply.error(libc::ENOENT),
            }
        } else {
            panic!("parent inode is not a directory")
        }
    }

    fn getattr(&mut self, req: &Request<'_>, ino: u64, reply: ReplyAttr) {
        trace!("FUSE({}) 'getattr' invoked for inode {}", req.unique(), ino);
        let inode = self.inode_db.get(&ino).unwrap();
        reply.attr(&Duration::new(0, 0), &inode.to_fileattr());
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

        let mut inode = match self.inode_db.get_mut(&ino) {
            None => {
                reply.error(libc::ENOENT);
                return;
            }
            Some(inner) => inner,
        };

        if mode.is_some() {
            inode.perm = mode.unwrap() as u16;
        }
        if uid.is_some() {
            inode.uid = uid.unwrap();
        }
        if gid.is_some() {
            inode.gid = gid.unwrap();
        }
        if size.is_some() {
            inode.size = size.unwrap();
        }
        if atime.is_some() {
            match atime.unwrap() {
                TimeOrNow::SpecificTime(time) => {
                    let since_the_epoch = time
                        .duration_since(UNIX_EPOCH)
                        .expect("Time went backwards");
                    inode.atime = (
                        since_the_epoch.as_secs() as i64,
                        since_the_epoch.subsec_nanos(),
                    );
                }
                TimeOrNow::Now => {
                    let (sec, nsec) = time_now();
                    inode.atime = (sec, nsec);
                }
            }
        }
        if mtime.is_some() {
            match atime.unwrap() {
                TimeOrNow::SpecificTime(time) => {
                    let since_the_epoch = time
                        .duration_since(UNIX_EPOCH)
                        .expect("Time went backwards");
                    inode.atime = (
                        since_the_epoch.as_secs() as i64,
                        since_the_epoch.subsec_nanos(),
                    );
                }
                TimeOrNow::Now => {
                    let (sec, nsec) = time_now();
                    inode.atime = (sec, nsec);
                }
            }
        }
        if ctime.is_some() {
            let since_the_epoch = ctime
                .unwrap()
                .duration_since(UNIX_EPOCH)
                .expect("Time went backwards");
            inode.ctime = (
                since_the_epoch.as_secs() as i64,
                since_the_epoch.subsec_nanos(),
            );
        }
        if crtime.is_some() {
            let since_the_epoch = crtime
                .unwrap()
                .duration_since(UNIX_EPOCH)
                .expect("Time went backwards");
            inode.crtime = (
                since_the_epoch.as_secs() as i64,
                since_the_epoch.subsec_nanos(),
            );
        }
        if flags.is_some() {
            inode.flags = flags.unwrap();
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

        debug!(
            "FUSE({}) 'setattr' success. inode {} updated",
            req.unique(),
            ino
        );

        reply.attr(&Duration::new(0, 0), &inode.to_fileattr());
        drop(inode);
    }

    /// Create a file node
    // This is called for creation of all non-directory, non-symlink nodes. If the filesystem defines a create() method, then for regular files that will be called instead.
    fn mknod(
        &mut self,
        req: &Request<'_>,
        parent: u64,
        name: &OsStr,
        mode: u32,
        umask: u32,
        rdev: u32,
        reply: ReplyEntry,
    ) {
        trace!(
            "FUSE({}) 'mknod' invoked for parent {} with name {:?}",
            req.unique(),
            parent,
            name
        );
        match self.create(req, parent, name, mode, umask, rdev) {
            Ok(inode) => {
                let inode = self.inode_db.get(&inode).unwrap().clone();
                reply.entry(&Duration::new(0, 0), &inode.to_fileattr(), 0);
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
        trace!(
            "FUSE({}) 'mkdir' invoked for parent {} with name {:?}",
            req.unique(),
            parent,
            name
        );
        match self.create(req, parent, name, mode | libc::S_IFDIR, umask, 0) {
            Ok(inode) => {
                let inode = self.inode_db.get(&inode).unwrap().clone();
                reply.entry(&Duration::new(0, 0), &inode.to_fileattr(), 0);
            }
            Err(e) => {
                reply.error(e);
            }
        }
    }

    fn unlink(&mut self, _req: &Request<'_>, _parent: u64, _name: &OsStr, _reply: ReplyEmpty) {
        todo!()
    }

    fn rename(
        &mut self,
        req: &Request<'_>,
        parent: u64,
        name: &OsStr,
        newparent: u64,
        newname: &OsStr,
        _flags: u32,
        reply: ReplyEmpty,
    ) {
        trace!("FUSE({}) 'rename' invoked for parent {} with name {:?} to new parent {} with new name {:?}", req.unique(), parent, name, newparent, newname);

        // TODO Clean this up
        if newparent == parent {
            let mut parent_inode = match self.inode_db.get_mut(&parent) {
                Some(inode) => inode,
                None => {
                    reply.error(libc::ENOENT);
                    return;
                }
            };
            if !parent_inode.check_access(req.uid(), req.gid(), libc::W_OK) {
                reply.error(libc::EACCES);
                return;
            }

            if let Some(mut descriptor) = self.descriptor_db.get_mut(&parent) {
                if let InodeDescriptor::Directory(contents) = &mut *descriptor {
                    // make sure the new name doesn't already exist
                    if contents.contains_key(newname.as_bytes()) {
                        warn!("attempted to rename file to a name that already exists");
                        reply.error(libc::EEXIST);
                        return;
                    }

                    let inode = contents.remove(name.as_bytes());
                    if inode.is_none() {
                        warn!("attempted to rename a non-existent file");
                        reply.error(libc::ENOENT);
                        return;
                    }

                    contents.insert(newname.as_bytes().to_vec(), inode.unwrap());
                }
            }

            parent_inode.ctime = time_now();
        } else {
            let parent_inode = match self.inode_db.get(&parent) {
                Some(inode) => inode,
                None => {
                    reply.error(libc::ENOENT);
                    return;
                }
            };
            if !parent_inode.check_access(req.uid(), req.gid(), libc::W_OK) {
                reply.error(libc::EACCES);
                return;
            }
            let new_parent_inode = match self.inode_db.get(&newparent) {
                Some(inode) => inode,
                None => {
                    warn!("attempted to move file to a non-existent inode");
                    reply.error(libc::ENOTDIR);
                    return;
                }
            };

            if parent_inode.kind != IFileType::Directory
                || new_parent_inode.kind != IFileType::Directory
            {
                warn!("attempted to move file to a non-directory inode");
                reply.error(libc::ENOTDIR);
                return;
            }

            // drop the handles for now.
            drop(parent_inode);
            drop(new_parent_inode);

            let mut parent_descriptor = match self.descriptor_db.get_mut(&parent) {
                Some(descriptor) => descriptor,
                None => {
                    error!("parent inode {} does not have a descriptor", parent);
                    reply.error(libc::ENOENT);
                    return;
                }
            };
            let mut new_parent_descriptor = match self.descriptor_db.get_mut(&newparent) {
                Some(descriptor) => descriptor,
                None => {
                    error!("new parent inode {} does not have a descriptor", newparent);
                    reply.error(libc::ENOENT);
                    return;
                }
            };
            // let's make sure that the thing we're moving out actually exists
            let target_inode = if let InodeDescriptor::Directory(contents) = &*parent_descriptor {
                match contents.get(name.as_bytes()) {
                    Some(inode) => *inode,
                    // we already know this entry exists
                    None => panic!("at the disco"),
                }
            } else {
                panic!("at the disco");
            };

            if let InodeDescriptor::Directory(contents) = &*new_parent_descriptor {
                if contents.contains_key(newname.as_bytes()) {
                    warn!("attempted to move file to a directory that already has a file with the same name");
                    reply.error(libc::EEXIST);
                    return;
                }
            }

            // remove the entry from the parent directory
            if let InodeDescriptor::Directory(contents) = &mut *parent_descriptor {
                let _ = contents.remove(name.as_bytes());
            }
            // add the entry to the new parent directory with the new name
            if let InodeDescriptor::Directory(contents) = &mut *new_parent_descriptor {
                let _ = contents.insert(newname.as_bytes().to_vec(), target_inode);
            }

            drop(parent_descriptor);
            drop(new_parent_descriptor);

            // now update the metadata of both parent inodes
            {
                let mut parent_inode = self.inode_db.get_mut(&parent).unwrap();
                let mut new_parent_inode = self.inode_db.get_mut(&newparent).unwrap();
                parent_inode.nlink -= 1;
                parent_inode.mtime = time_now();
                parent_inode.ctime = time_now();
                new_parent_inode.nlink += 1;
                new_parent_inode.mtime = time_now();
                new_parent_inode.ctime = time_now();
            }
        }

        reply.ok();
    }

    // /// Create a symbolic link
    // fn symlink(&mut self, _req: &Request<'_>, parent: u64, link_name: &OsStr, target: &Path, reply: ReplyEntry) {
    //     todo!()
    // }

    // Not used, as mknod is used for all non-directory, non-symlink nodes
    // fn create(&mut self, _req: &Request<'_>, parent: u64, name: &OsStr, mode: u32, umask: u32, flags: i32, reply: ReplyCreate) {
    //     todo!()
    // }

    fn read(
        &mut self,
        req: &Request<'_>,
        ino: u64,
        fh: u64,
        offset: i64,
        size: u32,
        _flags: i32,
        _lock_owner: Option<u64>,
        reply: ReplyData,
    ) {
        trace!(
            "FUSE({}) 'read' invoked on inode {} for fh {} starting at offset {}. read size: {}",
            req.unique(),
            ino,
            fh,
            offset,
            size
        );

        assert!(offset >= 0);
        // TODO Access Control stuff here
        // let file_inode = match self.read_inode(ino) {
        //     Ok(inode) => inode,
        //     Err(e) => {
        //         error!("Failed to read inode {}: {:?}", ino, e);
        //         reply.error(libc::ENOENT);
        //         return;
        //     }
        // };
        //
        // if !file_inode.check_access(req.uid(), req.gid(), libc::W_OK) {
        //     reply.error(libc::EACCES);
        //     return;
        // }

        let descriptor = match self.descriptor_db.get(&ino) {
            Some(descriptor) => descriptor,
            None => {
                reply.error(libc::ENOENT);
                return;
            }
        };

        let vf = match &*descriptor {
            InodeDescriptor::File(vf) => vf,
            _ => {
                error!("Inode {} is not a file", ino);
                reply.error(libc::EISDIR);
                return;
            }
        };
        // fuck u biiiiitch
        let mut vf = vf.clone();
        vf.populate(self.pool_map.clone());

        let mut buffer = vec![0; vf.chunk_size];

        // TODO this might not work because offset might be negative?
        match vf.read(offset as usize, &mut buffer) {
            Ok(_) => reply.data(&buffer),
            Err(e) => {
                error!("Failed to read data to file: {:?}", e);
                reply.error(libc::EIO)
            }
        }
    }

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

        let mut file_inode = match self.inode_db.get_mut(&ino) {
            Some(inode) => inode,
            None => {
                // error!("Failed to read inode {}: {:?}", ino, e);
                reply.error(libc::ENOENT);
                return;
            }
        };

        if !file_inode.check_access(req.uid(), req.gid(), libc::W_OK) {
            reply.error(libc::EACCES);
            return;
        }

        let mut descriptor = match self.descriptor_db.get_mut(&ino) {
            Some(descriptor) => descriptor,
            None => {
                reply.error(libc::ENOENT);
                return;
            }
        };

        let vf: &mut VirtualFile = match &mut *descriptor {
            InodeDescriptor::File(vf) => vf,
            _ => {
                error!("Inode {} is not a file", ino);
                reply.error(libc::EISDIR);
                return;
            }
        };
        vf.populate(self.pool_map.clone());

        match &mut vf.write(offset as usize, data) {
            Ok(amount) => {
                // update the inode
                file_inode.size = vf.size();
                file_inode.blocks = vf.chunks();
                file_inode.mtime = time_now();
                file_inode.atime = time_now();

                reply.written(*amount as u32);
                drop(file_inode);
            }
            Err(e) => {
                error!("Failed to write data to file: {:?}", e);
                reply.error(libc::EIO)
            }
        }
    }

    fn fsync(&mut self, req: &Request<'_>, ino: u64, fh: u64, datasync: bool, reply: ReplyEmpty) {
        debug!("FUSE({}) 'fsync' invoked on inode {}", req.unique(), ino);
        todo!()
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

    /// Read directory.
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

        let descriptor = self.descriptor_db.get(&inode).unwrap();
        if let InodeDescriptor::Directory(contents) = &*descriptor {
            for (index, entry) in contents.iter().skip(offset as usize).enumerate() {
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
        } else {
            panic!("parent inode is not a directory")
        }
    }
}

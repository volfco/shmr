pub mod cache;
mod magics;
pub mod types;

use crate::fuse::magics::*;
use crate::fuse::types::IFileType;
use crate::ShmrFs;
use fuser::{
    FileType, Filesystem, KernelConfig, ReplyAttr, ReplyData, ReplyDirectory, ReplyEmpty,
    ReplyEntry, ReplyOpen, ReplyWrite, Request, TimeOrNow, FUSE_ROOT_ID,
};
use libc::c_int;
use log::{debug, error, info, trace, warn};
use std::ffi::OsStr;
use std::os::unix::prelude::OsStrExt;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
// libc::ENOMSG

impl Filesystem for ShmrFs {
    fn init(&mut self, req: &Request<'_>, _config: &mut KernelConfig) -> Result<(), c_int> {
        match self.inode_db.check_inode(FUSE_ROOT_ID) {
            Ok(exist) => {
                if !exist {
                    info!("inode {} does not exist, creating root node", FUSE_ROOT_ID);
                    if let Err(e) = self.inode_db.create_inode(
                        Some(FUSE_ROOT_ID),
                        IFileType::Directory,
                        req.uid(),
                        req.gid(),
                        4096,
                        0,
                    ) {
                        error!("error when creating inode {}. {:?}", FUSE_ROOT_ID, e);
                        return Err(libc::ENOMSG);
                    }
                }
            }
            Err(e) => {
                error!("error when querying database. {:?}", e);
                return Err(libc::ENOMSG);
            }
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

        // check if we can access this inode
        if !self.inode_db.check_access(parent, req, libc::X_OK).unwrap() {
            reply.error(libc::EACCES);
            return;
        }

        let child_inode = match self
            .inode_db
            .get_directory_entry_inode(parent, name.to_str().unwrap())
        {
            Ok(ino) => match ino {
                Some(ino) => ino,
                None => {
                    debug!(
                        "FUSE({}) Inode {} has no entry for {}",
                        req.unique(),
                        parent,
                        name.to_str().unwrap()
                    );
                    reply.error(libc::ENOENT);
                    return;
                }
            },
            Err(err) => {
                error!(
                    "FUSE({}) Unable to get directory entry. {:?}",
                    req.unique(),
                    err
                );
                reply.error(libc::ENOMSG);
                return;
            }
        };

        match self.inode_db.to_fileattr(child_inode) {
            Ok(attrs) => reply.entry(&Duration::new(0, 0), &attrs, 0),
            Err(_) => {
                reply.error(libc::ENOENT);
            }
        }
    }

    fn getattr(&mut self, req: &Request<'_>, ino: u64, reply: ReplyAttr) {
        trace!("FUSE({}) 'getattr' invoked for inode {}", req.unique(), ino);

        if !self.inode_db.check_inode(ino).unwrap() {
            warn!(
                "FUSE({}) Inode {} does not exist or check_inode is false",
                req.unique(),
                ino
            );
            reply.error(libc::ENOENT);
            return;
        }

        match self.inode_db.to_fileattr(ino) {
            Ok(inode_attr) => reply.attr(&Duration::new(0, 0), &inode_attr),
            Err(_e) => reply.error(libc::ENOENT),
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
        _fh: Option<u64>,
        crtime: Option<SystemTime>,
        chgtime: Option<SystemTime>,
        bkuptime: Option<SystemTime>,
        flags: Option<u32>,
        reply: ReplyAttr,
    ) {
        trace!("FUSE({}) 'setattr' invoked on inode {}", req.unique(), ino);

        if let Err(e) = self.inode_db.update_inode(
            ino, mode, uid, gid, size, atime, mtime, ctime, crtime, chgtime, bkuptime, flags,
        ) {
            error!("FUSE({}) Query Error. {:?}", req.unique(), e);
            reply.error(libc::ENOMSG);
            return;
        }

        reply.attr(
            &Duration::from_secs(0),
            &self.inode_db.to_fileattr(ino).unwrap(),
        )
    }

    fn mknod(
        &mut self,
        req: &Request<'_>,
        parent: u64,
        name: &OsStr,
        mode: u32,
        _umask: u32,
        _rdev: u32,
        reply: ReplyEntry,
    ) {
        trace!(
            "FUSE({}) 'mknod' invoked for parent {} with name {:?}",
            req.unique(),
            parent,
            name
        );

        // check if directory already contains an entry for the given name
        match self
            .inode_db
            .get_directory_entry_inode(parent, name.to_str().unwrap())
        {
            Ok(entry) => {
                if entry.is_some() {
                    reply.error(libc::EEXIST);
                    return;
                }
            }
            Err(e) => {
                warn!("FUSE({}) Query Error. {:?}", req.unique(), e);
                reply.error(libc::ENOMSG);
                return;
            }
        }
        let file_type = mode & libc::S_IFMT;
        if file_type != libc::S_IFREG && file_type != libc::S_IFLNK && file_type != libc::S_IFDIR {
            // TODO
            warn!("mknod() implementation is incomplete. Only supports regular files, symlinks, and directories. Got {:o}", mode);
            reply.error(libc::ENOSYS);
            return;
        }

        let ino = match self.inode_db.create_inode(
            None,
            IFileType::from_mode(mode),
            req.uid(),
            req.gid(),
            4096,
            0,
        ) {
            Ok(ino) => ino,
            Err(e) => {
                error!("FUSE({}) Unable to create Inode. {:?}", req.unique(), e);
                reply.error(libc::EIO);
                return;
            }
        };

        // add entry to the parent directory
        if let Err(e) = self
            .inode_db
            .add_directory_entry(parent, name.to_str().unwrap(), ino)
        {
            error!(
                "FUSE({}) Unable to add entry to Inode. {:?}",
                req.unique(),
                e
            );
            reply.error(libc::EIO);
            return;
        }

        reply.entry(
            &Duration::from_secs(0),
            &self.inode_db.to_fileattr(ino).unwrap(),
            0,
        );
    }

    fn mkdir(
        &mut self,
        req: &Request<'_>,
        parent: u64,
        name: &OsStr,
        _mode: u32,
        _umask: u32,
        reply: ReplyEntry,
    ) {
        trace!(
            "FUSE({}) 'mkdir' invoked for parent {} with name {:?}",
            req.unique(),
            parent,
            name
        );

        // create the inode
        // TODO Push down mode and umask
        let new_inode = self
            .inode_db
            .create_inode(None, IFileType::Directory, req.uid(), req.gid(), 4096, 0)
            .unwrap();

        // add entry to parent
        self.inode_db
            .add_directory_entry(parent, name.to_str().unwrap(), new_inode)
            .unwrap();

        let fileattr = self.inode_db.to_fileattr(new_inode).unwrap();

        reply.entry(&Duration::new(0, 0), &fileattr, 0);
    }

    /// Rename a directory entry, possibly moving the entry from one inode to another.
    ///
    /// Process:
    ///   1. Check if we can access the old parent inode
    ///   2. Check if we can access the new parent inode
    ///   3. Check if we have permissions on the entry's target inode
    ///   4. Check if the entry exists in the old parent directory
    ///   5. Check if the new entry does not exist in the new parent directory
    ///   6. Remove the Directory Entry from the old parent
    ///   7. Add the Directory Entry, with the new name, to the new parent
    ///
    /// TODO Add error handling for operations
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

        let parent_access = self.inode_db.check_access(parent, req, 0).unwrap();
        let newparent_access = self.inode_db.check_access(newparent, req, 0).unwrap();

        // if we don't have access to either the parent, or newparent- fail the operation
        if parent_access || newparent_access {
            reply.error(libc::EACCES);
            return;
        }

        // check if the entry exists
        let old_entry = self
            .inode_db
            .get_directory_entry_inode(parent, name.to_str().unwrap())
            .unwrap();

        if old_entry.is_none() {
            reply.error(libc::ENOENT);
            return;
        }

        // check if the new entry exists
        let new_entry = self
            .inode_db
            .get_directory_entry_inode(newparent, newname.to_str().unwrap())
            .unwrap();

        if new_entry.is_some() {
            reply.error(libc::EEXIST);
            return;
        }

        // remove the entry from the old directory
        self.inode_db
            .remove_directory_entry(parent, name.to_str().unwrap())
            .unwrap();

        // add the entry to the new directory
        // old_entry contains the entry's target inode
        self.inode_db
            .add_directory_entry(newparent, newname.to_str().unwrap(), old_entry.unwrap())
            .unwrap();

        reply.ok();
    }

    fn open(&mut self, req: &Request<'_>, ino: u64, flags: i32, reply: ReplyOpen) {
        trace!(
            "FUSE({}) 'open' invoked on inode {} with flag {}",
            req.unique(),
            ino,
            flags
        );

        if !self.inode_db.is_file(ino).unwrap() {
            warn!("FUSE({}) inode {} is not a RegularFile", req.unique(), ino);
            reply.error(libc::ENOTDIR); // TODO This isn't correct
            return;
        }

        let fh = self.gen_fh();

        // associate the file handle
        let _ = self.file_handles.insert(fh, ino);

        // check to see if there is already a cache entry
        if self.file_cache.contains_key(&ino) {
            info!("FUSE({}) inode {} is already opened", req.unique(), ino);
            reply.opened(fh, 0);
            return;
        }

        let mut vf = match self.file_db.load_virtual_file(ino) {
            Err(e) => {
                error!("FUSE({}) error loading VirtualFile. {:?}", req.unique(), e);
                reply.error(libc::ENOMSG);
                return;
            }
            Ok(vf) => vf,
        };

        vf.populate(self.config.clone());

        self.file_cache.insert(ino, vf);

        debug!(
            "FUSE({}) Successfully opened inode {} with fh {}",
            req.unique(),
            ino,
            fh
        );

        reply.opened(fh, 0);
    }

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
        let offset = offset as u64;

        let vf = match self.file_cache.get(&ino) {
            Some(inner) => inner,
            None => {
                warn!("FUSE({}) Unable to perform read operation. Inode does not exist or has not been opened", req.unique());
                reply.error(libc::ENOENT);
                return;
            }
        };

        let mut buffer = vec![0; 4096];

        match vf.read(offset, &mut buffer) {
            Ok(_) => reply.data(&buffer),
            Err(e) => {
                error!(
                    "FUSE({}) Failed to read data to file: {:?}",
                    req.unique(),
                    e
                );
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
        let offset = offset as u64;

        let mut vf = match self.file_cache.get_mut(&ino) {
            Some(inner) => inner,
            None => {
                warn!("FUSE({}) Unable to perform write operation. Inode does not exist or has not been opened", req.unique());
                reply.error(libc::ENOENT);
                return;
            }
        };

        match vf.write(offset, data) {
            Ok(amt) => reply.written(amt as u32),
            Err(e) => {
                error!(
                    "FUSE({}) Failed to write data to file: {:?}",
                    req.unique(),
                    e
                );
                reply.error(libc::EIO)
            }
        }
    }

    fn release(
        &mut self,
        req: &Request<'_>,
        ino: u64,
        fh: u64,
        _flags: i32,
        _lock_owner: Option<u64>,
        flush: bool,
        reply: ReplyEmpty,
    ) {
        trace!(
            "FUSE({}) 'release' invoked for inode {} for filehandle {}. flush:{}",
            req.unique(),
            ino,
            fh,
            flush
        );

        if let Err(e) = self.release_fh(fh) {
            error!(
                "FUSE({}) Error releasing file handle: {:?}",
                req.unique(),
                e
            );
            reply.error(libc::ENOMSG);
        } else {
            debug!(
                "FUSE({}) successfully released fh {} (inode {})",
                req.unique(),
                fh,
                ino
            );
            reply.ok();
        }
    }

    fn fsync(&mut self, req: &Request<'_>, ino: u64, _fh: u64, datasync: bool, reply: ReplyEmpty) {
        trace!("FUSE({}) 'fsync' invoked on inode {}", req.unique(), ino);

        match self.file_cache.get(&ino) {
            None => {
                warn!("FUSE({}) Inode {} does not exist", req.unique(), ino);
                reply.error(libc::ENOENT); // TODO Is this the right error
            }
            Some(entry) => {
                if let Err(e) = entry.sync_data() {
                    error!("FUSE({}) Error syncing data: {:?}", req.unique(), e);
                    reply.error(libc::EIO);
                    return;
                }

                if !datasync {
                    // if datasync is false, then also sync the metadata
                    if let Err(e) = self.file_db.save_virtual_file(&entry) {
                        error!("FUSE({}) Error syncing metadata. {:?}", req.unique(), e);
                        reply.error(libc::EIO);
                        return;
                    }
                }

                reply.ok();
            }
        }
    }

    /// Open a directory.
    /// Filesystem may store an arbitrary file handle (pointer, index, etc) in fh, and use this in
    /// other all other directory stream operations (readdir, releasedir, fsyncdir). Filesystem may
    /// also implement stateless directory I/O and not store anything in fh, though that makes it
    /// impossible to implement standard conforming directory stream operations in case the
    /// contents of the directory can change between opendir and releasedir.
    fn opendir(&mut self, req: &Request<'_>, ino: u64, _flags: i32, reply: ReplyOpen) {
        debug!("FUSE({}) 'opendir' invoked on inode {}", req.unique(), ino);
        // let (access_mask, _read, _write) = match flags & libc::O_ACCMODE {
        //     libc::O_RDONLY => {
        //         // Behavior is undefined, but most filesystems return EACCES
        //         if flags & libc::O_TRUNC != 0 {
        //             reply.error(libc::EACCES);
        //             return;
        //         }
        //         (libc::R_OK, true, false)
        //     }
        //     libc::O_WRONLY => (libc::W_OK, false, true),
        //     libc::O_RDWR => (libc::R_OK | libc::W_OK, true, true),
        //     // Exactly one access mode flag must be specified
        //     _ => {
        //         reply.error(libc::EINVAL);
        //         return;
        //     }
        // };
        //
        // self.inode_db.check_access(ino, req, access_mask);

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

        let entries = self.inode_db.get_directory_entries(inode);
        if let Err(e) = entries {
            error!(
                "FUSE({}) Unable to get Directory Contents for Inode {}. {:?}",
                req.unique(),
                inode,
                e
            );
            reply.error(libc::ENOMSG);
            return;
        }

        let entries = entries.unwrap();

        for (idx, entry) in entries.iter().skip(offset as usize).enumerate() {
            let file_name = OsStr::from_bytes(entry.0.as_slice());
            trace!(
                "FUSE({}) 'readdir' entry: {:?} -> {}",
                req.unique(),
                file_name,
                entry.1
            );

            let buffer_full: bool = reply.add(
                entry.1,
                offset + idx as i64 + 1,
                FileType::Directory,
                file_name,
            );

            if buffer_full {
                break;
            }
        }

        reply.ok();
    }

    fn access(&mut self, _req: &Request<'_>, _ino: u64, _mask: i32, reply: ReplyEmpty) {
        reply.ok()
    }
}

#[allow(dead_code)]
pub fn time_now() -> (i64, u32) {
    let now = SystemTime::now();
    let since_the_epoch = now.duration_since(UNIX_EPOCH).expect("Time went backwards");
    (
        since_the_epoch.as_secs() as i64,
        since_the_epoch.subsec_nanos(),
    )
}
#[allow(dead_code)]
fn system_time_from_time(secs: i64, nsecs: u32) -> SystemTime {
    if secs >= 0 {
        UNIX_EPOCH + Duration::new(secs as u64, nsecs)
    } else {
        UNIX_EPOCH - Duration::new((-secs) as u64, nsecs)
    }
}

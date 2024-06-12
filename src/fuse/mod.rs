pub mod cache;
mod magics;
pub mod types;

use crate::fuse::magics::*;
use crate::fuse::types::IFileType;
use crate::types::{Inode, InodeDescriptor, SuperblockEntry};
use crate::{ShmrFs, VFS_DEFAULT_BLOCK_SIZE};
use fuser::{
    FileType, Filesystem, KernelConfig, ReplyAttr, ReplyData, ReplyDirectory, ReplyEmpty,
    ReplyEntry, ReplyOpen, ReplyWrite, Request, TimeOrNow, FUSE_ROOT_ID,
};
use libc::c_int;
use log::{debug, error, info, trace, warn};
use std::collections::BTreeMap;
use std::ffi::OsStr;
use std::os::unix::prelude::OsStrExt;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use crate::vfs::VirtualFile;
// libc::ENOMSG

#[cfg(target_os = "macos")]
pub type Mode = u16;

#[cfg(not(target_os = "macos"))]
pub type Mode = u32;
impl ShmrFs {
    fn is_file(&self, ino: u64) -> bool {
        let binding = self.superblock.get(&ino).unwrap();
        if binding.is_none() {
            false
        } else {
            matches!(binding.unwrap().inode_descriptor, InodeDescriptor::File(_))
        }
    }

    fn is_dir(&self, ino: u64) -> bool {
        let binding = self.superblock.get(&ino).unwrap();
        if binding.is_none() {
            false
        } else {
            matches!(binding.unwrap().inode_descriptor, InodeDescriptor::Directory(_))
        }
    }

    fn create_entry(&self,req: &Request<'_>,
                    parent: u64,
                    name: &OsStr,
                    mode: u32,
                    umask: u32,
                    rdev: u32,
                    reply: ReplyEntry) {
        let file_type = mode as Mode & libc::S_IFMT;
        if file_type != libc::S_IFREG && file_type != libc::S_IFLNK && file_type != libc::S_IFDIR {
            // TODO
            warn!("ShmrFs::create_entry() implementation is incomplete. Only supports regular files and directories. Got {:o}", mode);
            reply.error(libc::ENOSYS);
            return;
        }

        let mut parent_entry = match self.superblock.get_mut(&parent) {
            Err(err) => {
                error!("FUSE({}) Unable to load parent inode info. {:?}", req.unique(), err);
                reply.error(libc::EIO);
                return;
            }
            Ok(inner) => match inner {
                None => {
                    warn!("FUSE({}) Parent inode does not exist", req.unique());
                    reply.error(libc::ENOENT);
                    return;
                }
                Some(innerinner) => innerinner
            }
        };

        // // check if we have write access to the parent directory
        // if !parent_entry.inode.check_access(req.uid(), req.gid(), libc::W_OK) {
        //     reply.error(libc::EACCES);
        //     return;
        // }

        let parent_inode = parent_entry.inode.clone();

        if let InodeDescriptor::Directory(entries) = &mut parent_entry.inode_descriptor {
            if entries.contains_key(&name.to_str().unwrap().as_bytes().to_vec()) {
                warn!("FUSE({}) '{}' already exists in directory", req.unique(), &name.to_str().unwrap());
                reply.error(libc::EEXIST);
                return;
            }

            let ino = self.gen_ino();
            let gid = if parent_inode.perm & libc::S_ISGID as u16 != 0 {
                parent_inode.gid
            } else {
                req.gid()
            };

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
                blksize: VFS_DEFAULT_BLOCK_SIZE as u32,
                flags: 0,
                xattrs: BTreeMap::new(),
            };
            let child_descriptor = match file_type {
                libc::S_IFREG => InodeDescriptor::File(Box::new(VirtualFile::new())),
                // libc::S_IFLNK => InodeDescriptor::Symlink,
                libc::S_IFDIR => InodeDescriptor::Directory(BTreeMap::new()),
                _ => unimplemented!(),
            };

            let child_addr = child_inode.to_fileattr();
            if let Err(err) = self.superblock.insert(ino, SuperblockEntry {
                inode: child_inode,
                inode_descriptor: child_descriptor }) {
                error!("FUSE({}) Unable to update Superblock. {:?}", req.unique(), err);
                reply.error(libc::EIO);
                return;
            }

            // add the entry to the parent's directory list
            entries.insert(name.as_bytes().to_vec(), ino);
            parent_entry.inode.update_metadata(1);

            info!("FUSE({}) created inode", req.unique());
            reply.entry(
                &Duration::from_secs(0),
                &child_addr,
                0,
            );

        } else {
            warn!("FUSE({}) attempted to create file under non-directory inode", req.unique());
            reply.error(libc::ENOTDIR)
        }
    }
}
impl Filesystem for ShmrFs {
    fn init(&mut self, req: &Request<'_>, _config: &mut KernelConfig) -> Result<(), c_int> {
        if !self.superblock.has(&FUSE_ROOT_ID) {
            info!("inode {} does not exist, creating root node", FUSE_ROOT_ID);

            let superblock_entry = SuperblockEntry {
                inode: Inode {
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
                    uid: req.uid(),
                    gid: req.gid(),
                    rdev: 0,
                    blksize: 0,
                    flags: 0,
                    xattrs: BTreeMap::new(),
                },
                inode_descriptor: InodeDescriptor::Directory({
                    let mut inner = BTreeMap::new();
                    inner.insert(b".".to_vec(), FUSE_ROOT_ID);
                    inner
                }),
            };

            if let Err(e) = self.superblock.insert(FUSE_ROOT_ID, superblock_entry) {
                error!(
                    "error when inserting superblock entry for inode {}. {:?}",
                    FUSE_ROOT_ID, e
                );
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

        // this checks the inode's existence, and if we can access it
        // if !self.check_access(parent, req.uid(), req.gid(), libc::X_OK) {
        //     reply.error(libc::EACCES);
        //     return;
        // }

        // we already know this exists based on the previous check
        let entry = match self.superblock.get(&parent) {
            Err(err) => {
                error!("FUSE({}) error encountered when loading Inode {}. {:?}", req.unique(), parent, err);
                reply.error(libc::ENOMSG);
                return;
            }
            Ok(inner) => inner.unwrap()
        };

        let inode_lookup = if let InodeDescriptor::Directory(entries) = &entry.inode_descriptor {
            match entries.get(name.as_bytes()) {
                Some(entry_inode) => entry_inode,
                None => { reply.error(libc::ENOENT); return; },
            }
        } else {
            error!("FUSE({}) 'lookup' invoked on non Directory Inode", req.unique());
            reply.error(libc::ENOMSG);
            return;
        };

        match self.superblock.get(inode_lookup) {
            Err(err) => {
                error!("FUSE({}) error encountered when loading Inode {}. {:?}", req.unique(), parent, err);
                reply.error(libc::ENOMSG)
            }
            Ok(inner) => match inner {
                None => {
                    warn!("FUSE({}) error encountered when loading Inode {}. It does not exist", req.unique(), parent);
                    reply.error(libc::ENOENT)
                }
                Some(inner_inner) => {
                    reply.entry(&Duration::new(0, 0), &inner_inner.inode.to_fileattr(), 0)
                }
            }
        }
    }

    fn getattr(&mut self, req: &Request<'_>, ino: u64, reply: ReplyAttr) {
        trace!("FUSE({}) 'getattr' invoked for inode {}", req.unique(), ino);

        // if !self.check_access(ino, req.uid(), req.gid(), libc::X_OK) {
        //     reply.error(libc::EACCES);
        //     return;
        // }

        let entry = match self.superblock.get(&ino) {
            Err(err) => {
                error!("FUSE({}) error encountered when loading Inode {}. {:?}", req.unique(), ino, err);
                reply.error(libc::ENOMSG);
                return;
            }
            Ok(inner) => inner.unwrap()
        };

        reply.attr(&Duration::new(0, 0), &entry.inode.to_fileattr())
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

        // if !self.check_access(ino, req.uid(), req.gid(), libc::X_OK) {
        //     reply.error(libc::EACCES);
        //     return;
        // }
        // if !self.check_access(ino, req.uid(), req.gid(), libc::X_OK) {
        //     reply.error(libc::EACCES);
        //     return;
        // }

        let mut inode_entry = match self.superblock.get_mut(&ino) {
            Err(err) => {
                error!("FUSE({}) error encountered when loading Inode {}. {:?}", req.unique(), ino, err);
                reply.error(libc::ENOMSG);
                return;
            }
            Ok(inner) => inner.unwrap()
        };

        let inode = &mut inode_entry.inode;

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
        // if atime.is_some() {
        //     match atime.unwrap() {
        //         TimeOrNow::SpecificTime(time) => {
        //             let since_the_epoch = time
        //                 .duration_since(UNIX_EPOCH)
        //                 .expect("Time went backwards");
        //             inode.atime = (
        //                 since_the_epoch.as_secs() as i64,
        //                 since_the_epoch.subsec_nanos(),
        //             );
        //         }
        //         TimeOrNow::Now => {
        //             let (sec, nsec) = time_now();
        //             inode.atime = (sec, nsec);
        //         }
        //     }
        // }
        // if mtime.is_some() {
        //     match atime.unwrap() {
        //         TimeOrNow::SpecificTime(time) => {
        //             let since_the_epoch = time
        //                 .duration_since(UNIX_EPOCH)
        //                 .expect("Time went backwards");
        //             inode.atime = (
        //                 since_the_epoch.as_secs() as i64,
        //                 since_the_epoch.subsec_nanos(),
        //             );
        //         }
        //         TimeOrNow::Now => {
        //             let (sec, nsec) = time_now();
        //             inode.atime = (sec, nsec);
        //         }
        //     }
        // }
        // if ctime.is_some() {
        //     let since_the_epoch = ctime
        //         .unwrap()
        //         .duration_since(UNIX_EPOCH)
        //         .expect("Time went backwards");
        //     inode.ctime = (
        //         since_the_epoch.as_secs() as i64,
        //         since_the_epoch.subsec_nanos(),
        //     );
        // }
        // if crtime.is_some() {
        //     let since_the_epoch = crtime
        //         .unwrap()
        //         .duration_since(UNIX_EPOCH)
        //         .expect("Time went backwards");
        //     inode.crtime = (
        //         since_the_epoch.as_secs() as i64,
        //         since_the_epoch.subsec_nanos(),
        //     );
        // }
        // if flags.is_some() {
        //     inode.flags = flags.unwrap();
        // }
        // if fh.is_some() {
        //     unimplemented!("fh not implemented");
        // }
        // if chgtime.is_some() {
        //     unimplemented!("chgtime not implemented");
        // }
        // if bkuptime.is_some() {
        //     unimplemented!("bkuptime not implemented");
        // }

        debug!(
            "FUSE({}) 'setattr' success. inode {} updated",
            req.unique(),
            ino
        );

        reply.attr(&Duration::new(0, 0), &inode.to_fileattr());
        drop(inode_entry);
    }


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

        self.create_entry(req, parent, name, mode, umask, rdev, reply)

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
        self.create_entry(req, parent, name, mode, umask, 0, reply)
    }

    // /// Rename a directory entry, possibly moving the entry from one inode to another.
    // ///
    // /// Process:
    // ///   1. Check if we can access the old parent inode
    // ///   2. Check if we can access the new parent inode
    // ///   3. Check if we have permissions on the entry's target inode
    // ///   4. Check if the entry exists in the old parent directory
    // ///   5. Check if the new entry does not exist in the new parent directory
    // ///   6. Remove the Directory Entry from the old parent
    // ///   7. Add the Directory Entry, with the new name, to the new parent
    // ///
    // /// TODO Add error handling for operations
    // fn rename(
    //     &mut self,
    //     req: &Request<'_>,
    //     parent: u64,
    //     name: &OsStr,
    //     newparent: u64,
    //     newname: &OsStr,
    //     _flags: u32,
    //     reply: ReplyEmpty,
    // ) {
    //     trace!("FUSE({}) 'rename' invoked for parent {} with name {:?} to new parent {} with new name {:?}", req.unique(), parent, name, newparent, newname);
    //
    //     let parent_access = self.inode_db.check_access(parent, req, 0).unwrap();
    //     let newparent_access = self.inode_db.check_access(newparent, req, 0).unwrap();
    //
    //     // if we don't have access to either the parent, or newparent- fail the operation
    //     if parent_access || newparent_access {
    //         reply.error(libc::EACCES);
    //         return;
    //     }
    //
    //     // check if the entry exists
    //     let old_entry = self
    //         .inode_db
    //         .get_directory_entry_inode(parent, name.to_str().unwrap())
    //         .unwrap();
    //
    //     if old_entry.is_none() {
    //         reply.error(libc::ENOENT);
    //         return;
    //     }
    //
    //     // check if the new entry exists
    //     let new_entry = self
    //         .inode_db
    //         .get_directory_entry_inode(newparent, newname.to_str().unwrap())
    //         .unwrap();
    //
    //     if new_entry.is_some() {
    //         reply.error(libc::EEXIST);
    //         return;
    //     }
    //
    //     // remove the entry from the old directory
    //     self.inode_db
    //         .remove_directory_entry(parent, name.to_str().unwrap())
    //         .unwrap();
    //
    //     // add the entry to the new directory
    //     // old_entry contains the entry's target inode
    //     self.inode_db
    //         .add_directory_entry(newparent, newname.to_str().unwrap(), old_entry.unwrap())
    //         .unwrap();
    //
    //     reply.ok();
    // }

    fn open(&mut self, req: &Request<'_>, ino: u64, flags: i32, reply: ReplyOpen) {
        trace!(
            "FUSE({}) 'open' invoked on inode {} with flag {}",
            req.unique(),
            ino,
            flags
        );

        if !self.is_file(ino) {
            warn!("FUSE({}) inode {} is not a RegularFile", req.unique(), ino);
            reply.error(libc::ENOTDIR); // TODO This isn't correct
            return;
        }

        let fh = self.gen_fh();

        // associate the file handle
        let _ = self.file_handles.insert(fh, ino);

        // populate the VirtualFile, if not already populated
        {
            let mut binder = self.superblock.get_mut(&ino).unwrap().unwrap();
            if let InodeDescriptor::File(vf) = &mut binder.inode_descriptor {
                vf.populate(self.config.clone());
            }
        }

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

        let inode_entry = self.superblock.get(&ino).unwrap();
        if inode_entry.is_none() {
            warn!("FUSE({}) Unable to perform read operation. Inode does not exist or has not been opened", req.unique());
            reply.error(libc::ENOENT);
            return;
        }

        let mut buffer = vec![0; VFS_DEFAULT_BLOCK_SIZE as usize];

        let inode_entry = inode_entry.unwrap();
        if let InodeDescriptor::File(vf) = &inode_entry.inode_descriptor {
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
        } else {
            panic!("attempted to read from non-file inode")
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

        let inode_entry = self.superblock.get_mut(&ino).unwrap();
        if inode_entry.is_none() {
            warn!("FUSE({}) Unable to perform read operation. Inode does not exist or has not been opened", req.unique());
            reply.error(libc::ENOENT);
            return;
        }

        let mut inode_entry = inode_entry.unwrap();
        if let InodeDescriptor::File(vf) = &mut inode_entry.inode_descriptor {
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
        } else {
            panic!("attempted to write to non-file inode")
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

        // get the inode
        let inode_entry = self.superblock.get(&ino).unwrap();
        if inode_entry.is_none() {
            warn!("FUSE({}) Inode does not exist", req.unique());
            reply.error(libc::ENOENT);
            return;
        }

        let mut inode_entry = inode_entry.unwrap();
        if let InodeDescriptor::File(file) = &inode_entry.inode_descriptor {
            if let Err(e) = file.sync_data(true) {
                warn!("FUSE({}) Error occurred when attempting to sync inode {}. {:?}", req.unique(), ino, e);
            }
            // TODO take datasync into consideration
            if let Err(e) =  self.superblock.flush(&ino) {
                warn!("FUSE({}) Error occurred when attempting to sync inode {} metadata. {:?}", req.unique(), ino, e);
            }
        } else {
            reply.error(libc::ENOMSG)
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
        ino: u64,
        _fh: u64,
        offset: i64,
        mut reply: ReplyDirectory,
    ) {
        assert!(offset >= 0);

        trace!(
            "FUSE({}) 'readdir' invoked for inode {} with offset {}",
            req.unique(),
            ino,
            offset
        );

        let binding = self.superblock.get(&ino).unwrap();
        if binding.is_none() {
            reply.error(libc::ENOENT);
            return;
        }
        let entry = binding.unwrap();
        if let InodeDescriptor::Directory(entries) = &entry.inode_descriptor {

            for (idx, entry) in entries.iter().skip(offset as usize).enumerate() {
                let file_name = OsStr::from_bytes(entry.0.as_slice());
                trace!(
                    "FUSE({}) 'readdir' entry: {:?} -> {}",
                    req.unique(),
                    file_name,
                    entry.1
                );

                let buffer_full: bool = reply.add(
                    *entry.1,
                    offset + idx as i64 + 1,
                    FileType::Directory,
                    file_name,
                );

                if buffer_full {
                    break;
                }
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

use std::ffi::OsStr;
use std::os::unix::prelude::OsStrExt;
use fuser::{FileAttr, Filesystem, FileType, KernelConfig, ReplyAttr, ReplyDirectory, ReplyDirectoryPlus, ReplyEmpty, ReplyEntry, ReplyOpen, Request};
use log::{debug, info, trace, warn};
use crate::ShmrFilesystem;
use crate::topology::{FileAttributes, IFileType, InodeAttributes, InodeDescriptor};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// ShmrFilesystem

const MAX_NAME_LENGTH: u32 = 255;
const MAX_FILE_SIZE: u64 = 1024 * 1024 * 1024 * 1024;

fn time_now() -> (i64, u32) {
  let now = SystemTime::now();
  let since_the_epoch = now.duration_since(UNIX_EPOCH).expect("Time went backwards");
  (since_the_epoch.as_secs() as i64, since_the_epoch.subsec_nanos())
}


impl Filesystem for ShmrFilesystem {
  /// Initialize filesystem.
  /// Called before any other filesystem method. The kernel module connection can be configured
  /// using the KernelConfig object
  // fn init(&mut self, _req: &Request<'_>, config: &mut KernelConfig) -> Result<(), libc::c_int> {
  //   // self.superblock = Superblock::open()?;
  //   todo!()
  // }

  /// Clean up filesystem. Called on filesystem exit.
  fn destroy(&mut self) {
    todo!()
  }

  // Directory operations

  /// Look up a directory entry by name and get its attributes.
  fn lookup(&mut self, req: &Request, parent: u64, name: &OsStr, reply: ReplyEntry) {
    trace!("FUSE({}) 'lookup' invoked for inode {} name {:?}", req.unique(), parent, name);
    // given a parent inode (which is assumed a directory) and a directory entry name, return the
    // attributes of the item
    if name.len() > MAX_NAME_LENGTH as usize {
      debug!("given filename name too long. {} > {}", name.len(), MAX_NAME_LENGTH);
      reply.error(libc::ENAMETOOLONG);
      return;
    }

    let parent_inode = self.superblock.inode_pread(&parent, req.uid(), req.gid(), libc::X_OK).unwrap();

    if let IFileType::Directory = parent_inode.kind {
      // read the directory
      let contents = match self.superblock.directory_read(&parent) {
        Ok(tree) => tree,
        Err(e) => {
          warn!("error reading directory: {:?}", e);
          reply.error(libc::ENOENT);
          return;
        }
      };

      // check if the directory contains the name
      if let Some(inode) = contents.get(name.as_bytes()) {
        match self.superblock.inode_read(inode) {
          Ok(inode) => {
            let attr: FileAttr = inode.into();
            reply.entry(&Duration::new(0, 0), &attr, 0)
          },
          Err(_) => reply.error(libc::ENOENT),
        }
      } else {
        reply.error(libc::ENOENT);
      }
    }
  }

  /// Create a directory.
  fn mkdir(&mut self, req: &Request<'_>, parent: u64, name: &OsStr, mode: u32, umask: u32, reply: ReplyEntry) {
    trace!("FUSE({}) 'mkdir' invoked. parent inode: '{}'. dirname: {:?}", req.unique(), parent, name);

    // check if the user has access
    if name.len() > MAX_NAME_LENGTH as usize {
      debug!("given filename name too long. {} > {}", name.len(), MAX_NAME_LENGTH);
      reply.error(libc::ENAMETOOLONG);
      return;
    }

    let mut parent_attr = match self.superblock.inode_pread(&parent, req.uid(), req.gid(), libc::X_OK) {
      Ok(inode) => inode,
      Err(e) => {
        reply.error(e);
        return;
      }
    };

    if !matches!(parent_attr.kind, IFileType::Directory) {
      reply.error(libc::ENOTDIR);
      return;
    }

    // check if the directory already exists before we write the new child inode
    match self.superblock.directory_contains(&parent, &name.as_bytes().to_vec()) {
      Ok(true) => {
        reply.error(libc::EEXIST);
        return;
      },
      Err(e) => {
        reply.error(e);
        return;
      },
      _ => {},
    }

    // before updating the parent inode, create the child inode first.
    // so if this fails, all we have is an orphaned inode and not a modified directory without the
    // underlying inode

    let gid = if parent_attr.perm & libc::S_ISGID as u16 != 0 {
      parent_attr.gid
    } else {
      req.gid()
    };
    let attrs = FileAttributes {
      ino: self.superblock.gen_inode().unwrap(),
      size: 512,
      blksize: 512,
      blocks: 1,

      atime: time_now(),
      mtime: time_now(),
      ctime: time_now(),
      crtime: time_now(), // mac os only, which we don't care about
      kind: IFileType::Directory,

      perm: mode as u16,
      nlink: 2,  // Directories start with link count of 2, since they have a self link
      uid: req.uid(),
      gid,

      rdev: 0,
      flags: 0,
    };

    if let Err(e) = self.superblock.inode_update(&attrs.ino, &attrs) {
        reply.error(libc::ENOENT);
        return;
    }

    debug!("FUSE({}) created child inode: {:?}", req.unique(), &attrs.ino);

    // create the DirectoryDescriptor
    self.superblock.directory_create(&parent, &attrs.ino).unwrap();

    // now that the child inode has been created successfully we can update the parent directory
    parent_attr.mtime = time_now();
    // parent_attr.atime = time_now();
    parent_attr.ctime = time_now();

    // TODO Ideally this should be done in a transaction
    // update parent inode
    if let Err(e) = self.superblock.inode_update(&parent, &parent_attr) {
      debug!("error updating parent inode: {:?}", e);
      reply.error(libc::EIO);
      return;
    }

    // update the parent's directory entries to include the one we just made
    self.superblock.directory_entry_insert(&parent, name.as_bytes().to_vec(), &attrs.ino).unwrap();

    reply.entry(&Duration::new(0, 0), &attrs.into(), 0);

    debug!("FUSE({}) 'mkdir' success", req.unique());
  }

  /// Remove a directory.
  fn rmdir(&mut self, req: &Request<'_>, parent: u64, name: &OsStr, reply: ReplyEmpty) {
    trace!("FUSE({}) 'rmdir' invoked. parent inode: '{}'. dirname: {:?}", req.unique(), parent, name);
    let mut parent_attr = match self.superblock.inode_pread(&parent, req.uid(), req.gid(), libc::X_OK) {
      Ok(inode) => inode,
      Err(e) => {
        reply.error(e);
        return;
      }
    };

    if !matches!(parent_attr.kind, IFileType::Directory) {
      reply.error(libc::ENOTDIR);
      return;
    }

    // TODO check if we can write to the parent directory

    // ensure that the target of removal actually exists and is empty
    match self.superblock.directory_get_entry(&parent, &name.as_bytes().to_vec()) {
      Ok(inode) => match inode {
        // directory does not exist
        None => {
          reply.error(libc::ENOENT);
          return;
        }
        // it does exist!
        Some(inode) => match self.superblock.directory_read(&inode) {
          Ok(tree) => {
            if tree.len() > 2 {
              info!("directory not empty");
              reply.error(libc::ENOTEMPTY);
              return;
            }
            // remove the directory entry from the parent directory, orphaning the inode and inode_ds
            self.superblock.directory_entry_remove(&parent, &name.as_bytes().to_vec()).unwrap();

            // delete the directory inode and it's descriptor
            self.superblock.inode_remove(&inode).unwrap();

            reply.ok();
          }
          Err(e) => {
            reply.error(e);
            return;
          }
        },
      }
      Err(e) => {
        reply.error(e);
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
    debug!("opendir invoked on inode {}. {:?}", ino, req);
    let (access_mask, read, write) = match flags & libc::O_ACCMODE {
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

    let inode = match self.superblock.inode_pread(&ino, req.uid(), req.gid(), access_mask) {
      Ok(inode) => inode,
      Err(e) => {
        reply.error(e);
        return;
      }
    };

    // TODO track open file handles
    let fh = 0;
    // let open_flags = if self.direct_io { FOPEN_DIRECT_IO } else { 0 };
    reply.opened(self.allocate_next_file_handle(read, write), 0);
  }

  fn getattr(&mut self, req: &Request<'_>, ino: u64, reply: ReplyAttr) {
    trace!("FUSE({}) 'getattr' invoked for inode {}", req.unique(), ino);
    match self.superblock.inode_read(&ino) {
      Ok(inode) => {
        let attr: FileAttr = inode.into();
        reply.attr(&Duration::new(0, 0), &attr);
      },
      Err(_) => reply.error(libc::ENOENT),
    }
  }

  /// Read directory.
  /// Send a buffer filled using buffer.fill(), with size not exceeding the requested size. Send
  /// an empty buffer on end of stream. fh will contain the value set by the opendir method, or
  /// will be undefined if the opendir method didn’t set any value.
  fn readdir(&mut self, req: &Request<'_>, inode: u64, fh: u64, offset: i64, mut reply: ReplyDirectory) {
    assert!(offset >= 0);

    trace!("FUSE({}) 'readdir' invoked for inode {} with offset {}", req.unique(), inode, offset);

    let entries = match self.superblock.directory_read(&inode) {
      Ok(tree) => tree,
      Err(e) => {
        warn!("error reading directory: {:?}", e);
        reply.error(libc::ENOENT);
        return;
      }
    };

    for (index, entry) in entries.iter().skip(offset as usize).enumerate() {
      let file_name = OsStr::from_bytes(entry.0);
      trace!("FUSE({}) 'readdir' entry: {:?} -> {}", req.unique(), file_name, entry.1);
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
  //
  // /// Read directory.
  // /// Send a buffer filled using buffer.fill(), with size not exceeding the requested size. Send
  // /// an empty buffer on end of stream. fh will contain the value set by the opendir method, or
  // /// will be undefined if the opendir method didn’t set any value.
  // fn readdirplus(&mut self, _req: &Request<'_>, ino: u64, fh: u64, offset: i64, reply: ReplyDirectoryPlus) {
  //   todo!()
  // }
  //
  // /// Release an open directory.
  // /// For every opendir call there will be exactly one releasedir call. fh will contain the value
  // /// set by the opendir method, or will be undefined if the opendir method didn’t set any value.
  // fn releasedir(&mut self, _req: &Request<'_>, _ino: u64, _fh: u64, _flags: i32, reply: ReplyEmpty) {
  //   todo!()
  // }
  //
  // /// Synchronize directory contents.
  // /// If the datasync parameter is set, then only the directory contents should be flushed, not
  // /// the metadata. fh will contain the value set by the opendir method, or will be undefined if
  // /// the opendir method didn’t set any value.
  // fn fsyncdir(&mut self, _req: &Request<'_>, ino: u64, fh: u64, datasync: bool, reply: ReplyEmpty) {
  //   todo!()
  // }
}
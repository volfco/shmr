use std::ffi::OsStr;
use std::os::unix::prelude::OsStrExt;
use fuser::{FileAttr, Filesystem, KernelConfig, ReplyDirectory, ReplyDirectoryPlus, ReplyEmpty, ReplyEntry, ReplyOpen, Request};
use log::debug;
use crate::ShmrFilesystem;
use crate::topology::{InodeAttributes, InodeDescriptor};
use std::time::{Duration, SystemTime, UNIX_EPOCH};


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
  fn init(&mut self, _req: &Request<'_>, config: &mut KernelConfig) -> Result<(), libc::c_int> {
    todo!()
  }

  /// Clean up filesystem. Called on filesystem exit.
  fn destroy(&mut self) {
    todo!()
  }

  // Directory operations

  /// Look up a directory entry by name and get its attributes.
  fn lookup(&mut self, req: &Request, parent: u64, name: &OsStr, reply: ReplyEntry) {
    // given a parent inode (which is assumed a directory) and a directory entry name, return the
    // attributes of the item
    if name.len() > MAX_NAME_LENGTH as usize {
      debug!("given filename name too long. {} > {}", name.len(), MAX_NAME_LENGTH);
      reply.error(libc::ENAMETOOLONG);
      return;
    }

    let parent_inode = self.superblock.read_inode(&parent).unwrap();
    if !parent_inode.check_access(req.uid(), req.gid(), libc::X_OK ) {
      debug!("access denied to parent inode {}", parent);
      reply.error(libc::EACCES);
      return;
    }

    // check if the inode is a directory, if not return an ENOTDIR error
    if let InodeDescriptor::Directory(_attrs, directory) = parent_inode {
      // attempt to find the inode for the given name
      if let Some(inode) = directory.get(name.as_bytes()) {
        match self.superblock.read_inode(inode) {
          // todo populate the generation field correctly
          Ok(inode) => {
            let attr: FileAttr = inode.get_attributes().into();
            reply.entry(&Duration::new(0, 0), &attr, 0)
          },
          Err(_) => reply.error(libc::ENOENT),
        }
      } else {
        // if the inode is not found, return an ENOENT error
        reply.error(libc::ENOENT);
      }
    } else {
      reply.error(libc::ENOTDIR);
    }
  }

  /// Create a directory.
  fn mkdir(&mut self, req: &Request<'_>, parent: u64, name: &OsStr, mode: u32, umask: u32, reply: ReplyEntry) {
    // check if the user has access
    if name.len() > MAX_NAME_LENGTH as usize {
      debug!("given filename name too long. {} > {}", name.len(), MAX_NAME_LENGTH);
      reply.error(libc::ENAMETOOLONG);
      return;
    }

    // todo ensure the parent inode exists
    let mut parent_inode = match self.superblock.read_inode(&parent) {
      Ok(inode) => if !inode.check_access(req.uid(), req.gid(), libc::X_OK ) {
        debug!("access denied to parent inode {}", parent);
        reply.error(libc::EACCES);
        return;
      } else {
        inode
      }
      Err(e) => {
        debug!("error reading parent inode: {:?}", e);
        reply.error(e);
        return;
      }
    };

    match &mut parent_inode {
      InodeDescriptor::Directory(attrs, directory) => {
        if directory.get(name.as_bytes()).is_some() {
          reply.error(libc::EEXIST);
          return;
        }
        attrs.last_modified = time_now();
        attrs.last_metadata_changed = time_now();
        // if self.atime {
        //   attrs.last_accessed = time_now();
        // }
      },
      _ => {
        reply.error(libc::ENOTDIR);
        return;
      }
    }

    // before updating the parent inode, create the child inode first.
    // so if this fails, all we have is an orphaned inode and not a modified directory without the
    // underlying inode

    let attr = InodeAttributes {
      open_file_handles: 0,
      last_accessed: time_now(),
      last_modified: time_now(),
      last_metadata_changed: time_now(),
      mode,
      hardlinks: 0,
      uid: 0,
      gid: 0,
      xattrs: Default::default(),
    };
    let descriptor = InodeDescriptor::Directory(attr.clone(), Default::default());

    let file_attr = match self.superblock.create_inode(descriptor) {
      Ok(inode) => {
        self.superblock.get_fileattr(&inode).unwrap()
      },
      Err(_) => {
        reply.error(libc::ENOENT);
        return;
      },
    };

    // TODO Ideally this should be done in a transaction
    // update parent inode
    if let Err(e) = self.superblock.update_inode(&parent, parent_inode) {
      debug!("error updating parent inode: {:?}", e);
      reply.error(libc::EIO);
      return;
    }

    reply.entry(&Duration::new(0, 0), &file_attr, 0);

  }

  /// Remove a directory.
  fn rmdir(&mut self, _req: &Request<'_>, parent: u64, name: &OsStr, reply: ReplyEmpty) {
    todo!()
  }

  /// Open a directory.
  /// Filesystem may store an arbitrary file handle (pointer, index, etc) in fh, and use this in
  /// other all other directory stream operations (readdir, releasedir, fsyncdir). Filesystem may
  /// also implement stateless directory I/O and not store anything in fh, though that makes it
  /// impossible to implement standard conforming directory stream operations in case the
  /// contents of the directory can change between opendir and releasedir.
  fn opendir(&mut self, _req: &Request<'_>, _ino: u64, _flags: i32, reply: ReplyOpen) {
    todo!()
  }

  /// Read directory.
  /// Send a buffer filled using buffer.fill(), with size not exceeding the requested size. Send
  /// an empty buffer on end of stream. fh will contain the value set by the opendir method, or
  /// will be undefined if the opendir method didn’t set any value.
  fn readdir(&mut self, _req: &Request<'_>, ino: u64, fh: u64, offset: i64, reply: ReplyDirectory) {
    todo!()
  }

  /// Read directory.
  /// Send a buffer filled using buffer.fill(), with size not exceeding the requested size. Send
  /// an empty buffer on end of stream. fh will contain the value set by the opendir method, or
  /// will be undefined if the opendir method didn’t set any value.
  fn readdirplus(&mut self, _req: &Request<'_>, ino: u64, fh: u64, offset: i64, reply: ReplyDirectoryPlus) {
    todo!()
  }

  /// Release an open directory.
  /// For every opendir call there will be exactly one releasedir call. fh will contain the value
  /// set by the opendir method, or will be undefined if the opendir method didn’t set any value.
  fn releasedir(&mut self, _req: &Request<'_>, _ino: u64, _fh: u64, _flags: i32, reply: ReplyEmpty) {
    todo!()
  }

  /// Synchronize directory contents.
  /// If the datasync parameter is set, then only the directory contents should be flushed, not
  /// the metadata. fh will contain the value set by the opendir method, or will be undefined if
  /// the opendir method didn’t set any value.
  fn fsyncdir(&mut self, _req: &Request<'_>, ino: u64, fh: u64, datasync: bool, reply: ReplyEmpty) {
    todo!()
  }
}
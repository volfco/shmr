// use std::ffi::OsStr;
// use std::fs::OpenOptions;
// use std::io::{Seek, SeekFrom, Write};
// use std::os::unix::prelude::OsStrExt;
// use fuser::{FileAttr, Filesystem, FileType, ReplyAttr, ReplyCreate, ReplyDirectory, ReplyEmpty, ReplyEntry, ReplyOpen, ReplyWrite, ReplyXattr, Request, TimeOrNow};
// use log::{debug, error, info, trace, warn};
// use crate::{FILE_HANDLE_WRITE_BIT, ShmrFilesystem};
// use crate::topology::{FileAttributes, IFileType};
// use std::time::{Duration, SystemTime, UNIX_EPOCH};
//
// /// ShmrFilesystem
//
// const MAX_NAME_LENGTH: u32 = 255;
// const MAX_FILE_SIZE: u64 = 1024 * 1024 * 1024 * 1024;
//
// fn time_now() -> (i64, u32) {
//   let now = SystemTime::now();
//   let since_the_epoch = now.duration_since(UNIX_EPOCH).expect("Time went backwards");
//   (since_the_epoch.as_secs() as i64, since_the_epoch.subsec_nanos())
// }
//
// fn as_file_kind(mut mode: u32) -> IFileType {
//   mode &= libc::S_IFMT as u32;
//
//   if mode == libc::S_IFREG as u32 {
//     IFileType::RegularFile
//   } else if mode == libc::S_IFLNK as u32 {
//     IFileType::Symlink
//   } else if mode == libc::S_IFDIR as u32 {
//     IFileType::Directory
//   } else {
//     unimplemented!("{}", mode);
//   }
// }
//
// impl Filesystem for ShmrFilesystem {
//   /// Initialize filesystem.
//   /// Called before any other filesystem method. The kernel module connection can be configured
//   /// using the KernelConfig object
//   // fn init(&mut self, _req: &Request<'_>, config: &mut KernelConfig) -> Result<(), libc::c_int> {
//   //   // self.superblock = Superblock::open()?;
//   //   todo!()
//   // }
//
//   /// Clean up filesystem. Called on filesystem exit.
//   fn destroy(&mut self) {
//     todo!()
//   }
//
//   // Directory operations
//
//   /// Look up a directory entry by name and get its attributes.
//   fn lookup(&mut self, req: &Request, parent: u64, name: &OsStr, reply: ReplyEntry) {
//     trace!("FUSE({}) 'lookup' invoked for inode {} name {:?}", req.unique(), parent, name);
//     // given a parent inode (which is assumed a directory) and a directory entry name, return the
//     // attributes of the item
//     if name.len() > MAX_NAME_LENGTH as usize {
//       debug!("given filename name too long. {} > {}", name.len(), MAX_NAME_LENGTH);
//       reply.error(libc::ENAMETOOLONG);
//       return;
//     }
//
//     let parent_inode = self.superblock.inode_pread(&parent, req.uid(), req.gid(), libc::X_OK).unwrap();
//
//     if let IFileType::Directory = parent_inode.kind {
//       // read the directory
//       let contents = match self.superblock.directory_read(&parent) {
//         Ok(tree) => tree,
//         Err(e) => {
//           warn!("error reading directory: {:?}", e);
//           reply.error(libc::ENOENT);
//           return;
//         }
//       };
//
//       // check if the directory contains the name
//       if let Some(inode) = contents.get(name.as_bytes()) {
//         match self.superblock.inode_read(inode) {
//           Ok(inode) => {
//             let attr: FileAttr = inode.into();
//             reply.entry(&Duration::new(0, 0), &attr, 0)
//           },
//           Err(e) => {
//             error!("FUSE({}) 'lookup' failed. inode not found", req.unique());
//             reply.error(e)
//           },
//         }
//       } else {
//         trace!("FUSE({}) 'lookup' failed. directory does not contain given name", req.unique());
//         reply.error(libc::ENOENT);
//       }
//     }
//   }
//
//   fn getattr(&mut self, req: &Request<'_>, ino: u64, reply: ReplyAttr) {
//     trace!("FUSE({}) 'getattr' invoked for inode {}", req.unique(), ino);
//     match self.superblock.inode_read(&ino) {
//       Ok(inode) => {
//         let attr: FileAttr = inode.into();
//         reply.attr(&Duration::new(0, 0), &attr);
//       },
//       Err(_) => reply.error(libc::ENOENT),
//     }
//   }
//
//   fn setattr(&mut self, req: &Request<'_>, ino: u64, mode: Option<u32>, uid: Option<u32>, gid: Option<u32>, size: Option<u64>, atime: Option<TimeOrNow>, mtime: Option<TimeOrNow>, ctime: Option<SystemTime>, fh: Option<u64>, crtime: Option<SystemTime>, chgtime: Option<SystemTime>, bkuptime: Option<SystemTime>, flags: Option<u32>, reply: ReplyAttr) {
//     trace!("FUSE({}) 'setattr' invoked on inode {}", req.unique(), ino);
//
//     let mut parent_attr = match self.superblock.inode_read(&ino) {
//       Ok(inode) => inode,
//       Err(e) => {
//         warn!("FUST({}) 'setattr' failed. unable to read parent inode", req.unique());
//         reply.error(e);
//         return;
//       }
//     };
//
//     if mode.is_some() {
//       parent_attr.perm = mode.unwrap() as u16;
//     }
//     if uid.is_some() {
//       parent_attr.uid = uid.unwrap();
//     }
//     if gid.is_some() {
//       parent_attr.gid = gid.unwrap();
//     }
//     if size.is_some() {
//       parent_attr.size = size.unwrap();
//     }
//     if atime.is_some() {
//       match atime.unwrap() {
//         TimeOrNow::SpecificTime(time) => {
//           let since_the_epoch = time.duration_since(UNIX_EPOCH).expect("Time went backwards");
//           parent_attr.atime = (since_the_epoch.as_secs() as i64, since_the_epoch.subsec_nanos());
//         },
//         TimeOrNow::Now => {
//           let (sec, nsec) = time_now();
//           parent_attr.atime = (sec, nsec);
//         },
//       }
//     }
//     if mtime.is_some() {
//       match atime.unwrap() {
//         TimeOrNow::SpecificTime(time) => {
//           let since_the_epoch = time.duration_since(UNIX_EPOCH).expect("Time went backwards");
//           parent_attr.atime = (since_the_epoch.as_secs() as i64, since_the_epoch.subsec_nanos());
//         },
//         TimeOrNow::Now => {
//           let (sec, nsec) = time_now();
//           parent_attr.atime = (sec, nsec);
//         },
//       }
//     }
//     if ctime.is_some() {
//       let since_the_epoch = ctime.unwrap().duration_since(UNIX_EPOCH).expect("Time went backwards");
//       parent_attr.ctime = (since_the_epoch.as_secs() as i64, since_the_epoch.subsec_nanos());
//     }
//     if crtime.is_some() {
//       let since_the_epoch = crtime.unwrap().duration_since(UNIX_EPOCH).expect("Time went backwards");
//       parent_attr.crtime = (since_the_epoch.as_secs() as i64, since_the_epoch.subsec_nanos());
//     }
//     if flags.is_some() {
//       parent_attr.flags = flags.unwrap();
//     }
//     if fh.is_some() {
//       unimplemented!("fh not implemented");
//     }
//     if chgtime.is_some() {
//       unimplemented!("chgtime not implemented");
//     }
//     if bkuptime.is_some() {
//       unimplemented!("bkuptime not implemented");
//     }
//
//     if let Err(e) = self.superblock.inode_update(&ino, &parent_attr) {
//       reply.error(e);
//       return;
//     }
//
//     debug!("FUSE({}) 'setattr' success. inode {} updated", req.unique(), ino);
//
//     reply.attr(&Duration::new(0, 0), &parent_attr.into());
//
//   }
//
//   fn mknod(&mut self, req: &Request<'_>, parent: u64, name: &OsStr, mut mode: u32, umask: u32, rdev: u32, reply: ReplyEntry) {
//
//     let file_type = mode & libc::S_IFMT as u32;
//
//     if file_type != libc::S_IFREG as u32
//       && file_type != libc::S_IFLNK as u32
//       && file_type != libc::S_IFDIR as u32
//     {
//       // TODO
//       warn!("mknod() implementation is incomplete. Only supports regular files, symlinks, and directories. Got {:o}", mode);
//       reply.error(libc::ENOSYS);
//       return;
//     }
//
//     let parent_attr = match self.superblock.inode_pread(&parent, req.uid(), req.gid(), libc::X_OK) {
//       Ok(inode) => inode,
//       Err(e) => {
//         reply.error(e);
//         return;
//       }
//     };
//
//     if !matches!(parent_attr.kind, IFileType::Directory) {
//       warn!("attempted to create file under non-directory inode");
//       reply.error(libc::ENOTDIR);
//       return;
//     }
//
//     match self.superblock.directory_contains(&parent, &name.as_bytes().to_vec()) {
//       Ok(true) => {
//         reply.error(libc::EEXIST);
//         return;
//       },
//       Err(e) => {
//         reply.error(e);
//         return;
//       },
//       _ => {},
//     }
//
//     // before updating the parent inode, create the child inode first.
//     // so if this fails, all we have is an orphaned inode and not a modified directory without the
//     // underlying inode
//
//     let gid = if parent_attr.perm & libc::S_ISGID as u16 != 0 {
//       parent_attr.gid
//     } else {
//       req.gid()
//     };
//     let attrs = FileAttributes {
//       ino: self.superblock.gen_inode().unwrap(),
//       size: 512,
//       blksize: 4096,
//       blocks: 1,
//
//       atime: time_now(),
//       mtime: time_now(),
//       ctime: time_now(),
//       crtime: time_now(), // mac os only, which we don't care about
//       kind: IFileType::Directory,
//
//       perm: mode as u16,
//       nlink: 2,  // Directories start with link count of 2, since they have a self link
//       uid: req.uid(),
//       gid,
//
//       rdev: 0,
//       flags: 0,
//     };
//
//     if let Err(e) = self.superblock.inode_update(&attrs.ino, &attrs) {
//       reply.error(e);
//       return;
//     }
//
//     // create the file inode
//     let gid = if parent_attr.perm & libc::S_ISGID as u16 != 0 {
//       parent_attr.gid
//     } else {
//       req.gid()
//     };
//
//     let attrs = FileAttributes {
//       ino: self.superblock.gen_inode().unwrap(),
//       size: 0,
//       blksize: 512,
//       blocks: 0,
//
//       atime: time_now(),
//       mtime: time_now(),
//       ctime: time_now(),
//       crtime: time_now(), // mac os only, which we don't care about
//       kind: as_file_kind(mode),
//
//       perm: mode as u16,
//       nlink: 2,  // Directories start with link count of 2, since they have a self link
//       uid: req.uid(),
//       gid,
//
//       rdev: 0,
//       flags: 0,
//     };
//
//     if let Err(e) = self.superblock.inode_update(&attrs.ino, &attrs) {
//       reply.error(e);
//       return;
//     }
//     // TODO: implement flags
//     reply.entry(&Duration::new(0, 0), &attrs.into(), 0);
//   }
//
//   /// Create a directory.
//   /// TODO Merge this with `create` and `mknod` to reduce code duplication
//   fn mkdir(&mut self, req: &Request<'_>, parent: u64, name: &OsStr, mode: u32, _umask: u32, reply: ReplyEntry) {
//     trace!("FUSE({}) 'mkdir' invoked. parent inode: '{}'. dirname: {:?}", req.unique(), parent, name);
//
//     // check if the user has access
//     if name.len() > MAX_NAME_LENGTH as usize {
//       debug!("given filename name too long. {} > {}", name.len(), MAX_NAME_LENGTH);
//       reply.error(libc::ENAMETOOLONG);
//       return;
//     }
//
//     let mut parent_attr = match self.superblock.inode_pread(&parent, req.uid(), req.gid(), libc::X_OK) {
//       Ok(inode) => inode,
//       Err(e) => {
//         reply.error(e);
//         return;
//       }
//     };
//
//     if !matches!(parent_attr.kind, IFileType::Directory) {
//       reply.error(libc::ENOTDIR);
//       return;
//     }
//
//     // check if the directory already exists before we write the new child inode
//     match self.superblock.directory_contains(&parent, &name.as_bytes().to_vec()) {
//       Ok(true) => {
//         reply.error(libc::EEXIST);
//         return;
//       },
//       Err(e) => {
//         reply.error(e);
//         return;
//       },
//       _ => {},
//     }
//
//     // before updating the parent inode, create the child inode first.
//     // so if this fails, all we have is an orphaned inode and not a modified directory without the
//     // underlying inode
//
//     let gid = if parent_attr.perm & libc::S_ISGID as u16 != 0 {
//       parent_attr.gid
//     } else {
//       req.gid()
//     };
//     let attrs = FileAttributes {
//       ino: self.superblock.gen_inode().unwrap(),
//       size: 512,
//       blksize: 512,
//       blocks: 1,
//
//       atime: time_now(),
//       mtime: time_now(),
//       ctime: time_now(),
//       crtime: time_now(), // mac os only, which we don't care about
//       kind: IFileType::Directory,
//
//       perm: mode as u16,
//       nlink: 2,  // Directories start with link count of 2, since they have a self link
//       uid: req.uid(),
//       gid,
//
//       rdev: 0,
//       flags: 0,
//     };
//
//     if let Err(e) = self.superblock.inode_update(&attrs.ino, &attrs) {
//         reply.error(e);
//         return;
//     }
//
//     debug!("FUSE({}) created child inode: {:?}", req.unique(), &attrs.ino);
//
//     // create the DirectoryDescriptor
//     self.superblock.directory_create(&parent, &attrs.ino).unwrap();
//
//     // now that the child inode has been created successfully we can update the parent directory
//     parent_attr.mtime = time_now();
//     // parent_attr.atime = time_now();
//     parent_attr.ctime = time_now();
//
//     // TODO Ideally this should be done in a transaction
//     // update parent inode
//     if let Err(e) = self.superblock.inode_update(&parent, &parent_attr) {
//       debug!("error updating parent inode: {:?}", e);
//       reply.error(libc::EIO);
//       return;
//     }
//
//     // update the parent's directory entries to include the one we just made
//     self.superblock.directory_entry_insert(&parent, name.as_bytes().to_vec(), &attrs.ino).unwrap();
//
//     reply.entry(&Duration::new(0, 0), &attrs.into(), 0);
//
//     debug!("FUSE({}) 'mkdir' success", req.unique());
//   }
//
//   /// Remove a directory.
//   fn rmdir(&mut self, req: &Request<'_>, parent: u64, name: &OsStr, reply: ReplyEmpty) {
//     trace!("FUSE({}) 'rmdir' invoked. parent inode: '{}'. dirname: {:?}", req.unique(), parent, name);
//     let parent_attr = match self.superblock.inode_pread(&parent, req.uid(), req.gid(), libc::X_OK) {
//       Ok(inode) => inode,
//       Err(e) => {
//         reply.error(e);
//         return;
//       }
//     };
//
//     if !matches!(parent_attr.kind, IFileType::Directory) {
//       reply.error(libc::ENOTDIR);
//       return;
//     }
//
//     // TODO check if we can write to the parent directory
//
//     // ensure that the target of removal actually exists and is empty
//     match self.superblock.directory_get_entry(&parent, &name.as_bytes().to_vec()) {
//       Ok(inode) => match inode {
//         Some(inode) => match self.superblock.directory_read(&inode) {
//           Ok(tree) => {
//             if tree.len() > 2 {
//               info!("directory not empty");
//               reply.error(libc::ENOTEMPTY);
//               return;
//             }
//             // remove the directory entry from the parent directory, orphaning the inode and inode_ds
//             self.superblock.directory_entry_remove(&parent, &name.as_bytes().to_vec()).unwrap();
//
//             // delete the directory inode and it's descriptor
//             self.superblock.inode_remove(&inode).unwrap();
//
//             reply.ok()
//           }
//           Err(e) => {
//             reply.error(e)
//           }
//         },
//         // directory does not exist
//         None => {
//           reply.error(libc::ENOENT)
//         }
//       }
//       Err(e) => {
//         reply.error(e)
//       }
//     }
//   }
//   //
//   // /// Read directory.
//   // /// Send a buffer filled using buffer.fill(), with size not exceeding the requested size. Send
//   // /// an empty buffer on end of stream. fh will contain the value set by the opendir method, or
//   // /// will be undefined if the opendir method didn’t set any value.
//   // fn readdirplus(&mut self, _req: &Request<'_>, ino: u64, fh: u64, offset: i64, reply: ReplyDirectoryPlus) {
//   //   todo!()
//   // }
//   //
//   // /// Release an open directory.
//   // /// For every opendir call there will be exactly one releasedir call. fh will contain the value
//   // /// set by the opendir method, or will be undefined if the opendir method didn’t set any value.
//   // fn releasedir(&mut self, _req: &Request<'_>, _ino: u64, _fh: u64, _flags: i32, reply: ReplyEmpty) {
//   //   todo!()
//   // }
//   //
//   // /// Synchronize directory contents.
//   // /// If the datasync parameter is set, then only the directory contents should be flushed, not
//   // /// the metadata. fh will contain the value set by the opendir method, or will be undefined if
//   // /// the opendir method didn’t set any value.
//   // fn fsyncdir(&mut self, _req: &Request<'_>, ino: u64, fh: u64, datasync: bool, reply: ReplyEmpty) {
//   //   todo!()
//   // }
//
//   fn write(&mut self, req: &Request<'_>, ino: u64, fh: u64, offset: i64, data: &[u8], write_flags: u32, flags: i32, lock_owner: Option<u64>, reply: ReplyWrite) {
//     trace!("FUSE({}) 'write' invoked on inode {} for fh {} starting at offset {}. data length: {}", req.unique(), ino, fh, offset, data.len());
//
//     assert!(offset >= 0);
//     // if !check_file_handle_write(fh) {
//     //   reply.error(libc::EACCES);
//     //   return;
//     // }
//
//     let mut file_inode = self.superblock.inode_read(&ino).unwrap();
//
//     let mut file_ino = self.superblock.read_file_topology(&ino).unwrap();
//
//     // write the data to the proper block for the given inode
//     let block_path = self.config.workspace.join("blocks");
//
//     // which block we're writing to
//     let starting_block = match offset > 0 {
//       true => offset / file_inode.blksize as i64,
//       false => 0
//     };
//     let ending_block = ((offset + data.len() as i64) / file_inode.blksize as i64) + 1;
//
//     debug!("Writing {} bytes to block {} ending at block {}", data.len(), starting_block, ending_block);
//
//     let mut shards = vec![];
//     for i in starting_block..ending_block {
//       let mut block_offset = 0;
//       if i == starting_block {
//         // we only need to worry about the offset on the first block
//         block_offset = offset % file_inode.blksize as i64;
//       }
//       let path = block_path.join(format!("{}_{}.bin", &ino, i));
//       trace!("Writing to {:?} at offset {}", path, block_offset);
//       //
//       // let mut fh = OpenOptions::new().write(true).create(true).open(path)?;
//       // fh.seek(SeekFrom::Start(block_offset as u64))?;
//       // fh.write_all(data)?;
//
//       // TODO Update the filesize if we're writing new blocks
//       // if data.len() + offset as usize > attrs.size as usize {
//       //   attrs.size = (data.len() + offset as usize) as u64;
//       // }
//
//       shards.push((String::new(), block_path.to_str().unwrap().to_string()))
//     }
//
//   }
//
//   fn flush(&mut self, req: &Request<'_>, ino: u64, fh: u64, lock_owner: u64, reply: ReplyEmpty) {
//     trace!("FUSE({}) 'flush' invoked on inode {} for fh {}", req.unique(), ino, fh);
//
//     reply.ok();
//   }
//
//   /// Open a directory.
//   /// Filesystem may store an arbitrary file handle (pointer, index, etc) in fh, and use this in
//   /// other all other directory stream operations (readdir, releasedir, fsyncdir). Filesystem may
//   /// also implement stateless directory I/O and not store anything in fh, though that makes it
//   /// impossible to implement standard conforming directory stream operations in case the
//   /// contents of the directory can change between opendir and releasedir.
//   fn opendir(&mut self, req: &Request<'_>, ino: u64, flags: i32, reply: ReplyOpen) {
//     debug!("opendir invoked on inode {}. {:?}", ino, req);
//     let (access_mask, read, write) = match flags & libc::O_ACCMODE {
//       libc::O_RDONLY => {
//         // Behavior is undefined, but most filesystems return EACCES
//         if flags & libc::O_TRUNC != 0 {
//           reply.error(libc::EACCES);
//           return;
//         }
//         (libc::R_OK, true, false)
//       }
//       libc::O_WRONLY => (libc::W_OK, false, true),
//       libc::O_RDWR => (libc::R_OK | libc::W_OK, true, true),
//       // Exactly one access mode flag must be specified
//       _ => {
//         reply.error(libc::EINVAL);
//         return;
//       }
//     };
//
//     let inode = match self.superblock.inode_pread(&ino, req.uid(), req.gid(), access_mask) {
//       Ok(inode) => inode,
//       Err(e) => {
//         reply.error(e);
//         return;
//       }
//     };
//
//     // TODO track open file handles
//     // let open_flags = if self.direct_io { FOPEN_DIRECT_IO } else { 0 };
//     reply.opened(self.allocate_next_file_handle(read, write), 0);
//   }
//
//   /// Read directory.
//   /// Send a buffer filled using buffer.fill(), with size not exceeding the requested size. Send
//   /// an empty buffer on end of stream. fh will contain the value set by the opendir method, or
//   /// will be undefined if the opendir method didn’t set any value.
//   fn readdir(&mut self, req: &Request<'_>, inode: u64, _fh: u64, offset: i64, mut reply: ReplyDirectory) {
//     assert!(offset >= 0);
//
//     trace!("FUSE({}) 'readdir' invoked for inode {} with offset {}", req.unique(), inode, offset);
//
//     let entries = match self.superblock.directory_read(&inode) {
//       Ok(tree) => tree,
//       Err(e) => {
//         warn!("error reading directory: {:?}", e);
//         reply.error(libc::ENOENT);
//         return;
//       }
//     };
//
//     for (index, entry) in entries.iter().skip(offset as usize).enumerate() {
//       let file_name = OsStr::from_bytes(entry.0);
//       trace!("FUSE({}) 'readdir' entry: {:?} -> {}", req.unique(), file_name, entry.1);
//       let buffer_full: bool = reply.add(
//         *entry.1,
//         offset + index as i64 + 1,
//         FileType::Directory,
//         OsStr::from_bytes(entry.0),
//       );
//
//       if buffer_full {
//         break;
//       }
//     }
//
//     reply.ok();
//   }
//   fn getxattr(&mut self, req: &Request<'_>, ino: u64, name: &OsStr, size: u32, reply: ReplyXattr) {
//     reply.error(libc::ERANGE);
//   }
//   // File operations
//   fn create(&mut self, req: &Request<'_>, parent: u64, name: &OsStr, mut mode: u32, _umask: u32, flags: i32, reply: ReplyCreate) {
//     trace!("FUSE({}) 'create' invoked. creating file {:?} under inode {}", req.unique(), name, parent);
//
//     if name.len() > MAX_NAME_LENGTH as usize {
//       warn!("filename name too long. {} > {}", name.len(), MAX_NAME_LENGTH);
//       reply.error(libc::ENAMETOOLONG);
//       return;
//     }
//
//     let parent_attr = match self.superblock.inode_pread(&parent, req.uid(), req.gid(), libc::X_OK) {
//       Ok(inode) => inode,
//       Err(e) => {
//         reply.error(e);
//         return;
//       }
//     };
//
//     if !matches!(parent_attr.kind, IFileType::Directory) {
//       warn!("attempted to create file under non-directory inode");
//       reply.error(libc::ENOTDIR);
//       return;
//     }
//
//     match self.superblock.directory_contains(&parent, &name.as_bytes().to_vec()) {
//       Ok(true) => {
//         warn!("directory already contains file with name {:?}", name);
//         reply.error(libc::EEXIST);
//         return;
//       },
//       Err(e) => {
//         warn!("error checking directory for file existence. {:?}", e);
//         reply.error(e);
//         return;
//       },
//       _ => {},
//     }
//
//     let (read, write) = match flags & libc::O_ACCMODE {
//       libc::O_RDONLY => (true, false),
//       libc::O_WRONLY => (false, true),
//       libc::O_RDWR => (true, true),
//       // Exactly one access mode flag must be specified
//       _ => {
//         reply.error(libc::EINVAL);
//         return;
//       }
//     };
//
//     if req.uid() != 0 {
//       mode &= !(libc::S_ISUID | libc::S_ISGID) as u32;
//     }
//
//     // create the file inode
//     let gid = if parent_attr.perm & libc::S_ISGID as u16 != 0 {
//       parent_attr.gid
//     } else {
//       req.gid()
//     };
//
//     let attrs = FileAttributes {
//       ino: self.superblock.gen_inode().unwrap(),
//       size: 0,
//       blksize: 512,
//       blocks: 0,
//
//       atime: time_now(),
//       mtime: time_now(),
//       ctime: time_now(),
//       crtime: time_now(), // mac os only, which we don't care about
//       kind: as_file_kind(mode),
//
//       perm: mode as u16,
//       nlink: 2,  // Directories start with link count of 2, since they have a self link
//       uid: req.uid(),
//       gid,
//
//       rdev: 0,
//       flags: 0,
//     };
//
//     if let Err(e) = self.superblock.inode_update(&attrs.ino, &attrs) {
//       reply.error(e);
//       return;
//     }
//
//     debug!("FUSE({}) created child inode: {:?}", req.unique(), &attrs.ino);
//
//     match attrs.kind {
//       IFileType::Directory => {
//         // create the DirectoryDescriptor
//         self.superblock.directory_create(&parent, &attrs.ino).unwrap();
//       },
//       IFileType::RegularFile => {
//         self.superblock.file_initialize(&attrs.ino).unwrap();
//       },
//       _ => {
//         // create the file inode layout
//       }
//     }
//
//     // insert the new directory entry into the parent's directory
//     self.superblock.directory_entry_insert(&parent, name.as_bytes().to_vec(), &attrs.ino).unwrap();
//
//     reply.created(
//       &Duration::new(0, 0),
//       &attrs.into(),
//       0,
//       self.allocate_next_file_handle(read, write),
//       0,
//     );
//   }
//
// }
//
// fn check_file_handle_write(file_handle: u64) -> bool {
//   (file_handle & FILE_HANDLE_WRITE_BIT) != 0
// }
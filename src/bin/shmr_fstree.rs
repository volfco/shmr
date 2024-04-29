// shmr_fstree - show the filesystem tree and underlying file layout
// shmr_fstree --config CONFIG --inode INODE
// inode of 1 is assumed if not provided

use std::path::{Path, PathBuf};
use shmr::fsdb::FsDB;
use shmr::fuse::InodeDescriptor;

fn enter_dir(fs_db: &FsDB, ino: u64, depth: usize) {
  let descriptor = fs_db.read_descriptor(ino).unwrap();
  match descriptor {
    InodeDescriptor::Directory(vf) => {
      for entry in vf {
        if entry.1 == ino {
          continue;
        }
        let padding = "  ".repeat(depth);

        println!("{}/{} => ino:{}", &padding, String::from_utf8(entry.0).unwrap(), entry.1);

        match fs_db.read_descriptor(entry.1).unwrap() {
          InodeDescriptor::File(vf) => {
            println!("{}   file size: {}. chunk: {} / {}", &padding, vf.size, vf.chunk_size, vf.chunks());
            for blk in vf.blocks {
              println!("{}        {:?}", &padding, blk);
            }
            println!();
          }
          InodeDescriptor::Directory(_) => {
            println!();
            enter_dir(fs_db, entry.1, depth + 1);
          }
          InodeDescriptor::Symlink => {}
        }
      }
    }
    _ => { panic!("enter_dir was not given a directory inode")}
  }
}

fn main() {
  let root_inode = 1;
  let fs_db = FsDB::open(PathBuf::from("./metadata")).unwrap();

  println!();
  println!();
  enter_dir(&fs_db, 1, 1);

}
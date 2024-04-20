/// FileDB.
///
/// Goal is to provide a simple abstraction over on-disk data. Each Inode & InodeDescriptor is stored
/// as indivual files on disk. The idea is, with the help of helper utilities, you can reconstruct
/// files without needing to run the filesystem.
use std::io::Read;
use anyhow::{bail, Result};
use rkyv::Deserialize;
use std::path::{Path, PathBuf};
use log::{trace};
use walkdir::WalkDir;
use crate::fuse::{Inode, InodeDescriptor};

/// Database to store storage block information.
pub struct FsDB {
    base_dir: PathBuf,
    known_inodes: Vec<u64>
}
impl FsDB {
    fn read_file(&self, path: &Path) -> Result<Option<Vec<u8>>> {
        if !path.exists() {
            return Ok(None);
        }
        let mut file = std::fs::File::open(path)?;
        let mut buf = vec![];
        let _ = file.read_to_end(&mut buf)?;
        Ok(Some(buf))
    }

    fn get_path(&self, name: &str, ext: &str) -> PathBuf {
        let mut base = self.base_dir.clone();
        if name.len() > 6 {
            base.push(&name[0..2]); // first two characters
            base.push(&name[2..4]); // next two characters

            // if the name is "abcdef", then the path will be "ab/cd/abcdef.ext"
        }
        base = base.join(format!("{}.{}", name, ext));
        trace!("Resolved path for {}.{}: {:?}", name, ext, base);
        base
    }

    pub fn generate_id(&self) -> u64 {
        let mut id;
        // TODO try and generate sequential IDs
        loop {
            id = rand::random();
            if !self.known_inodes.contains(&id) {
                // check if the ID is already in use
                if self.read_file(&self.get_path(&id.to_string(), "inode")).unwrap().is_none() {
                    break;
                }
            }
        }
        id
    }

    pub fn open(path: &Path) -> Result<Self> {
        if !path.exists() {
            std::fs::create_dir_all(path)?;
        }
        if !path.is_dir() {
            bail!("Path exists and is not a directory: {:?}", path);
        }

        let mut s = Self { base_dir: path.to_path_buf(), known_inodes: vec![] };
        s.index()?;
        Ok(s)
    }

    pub fn index(&mut self) -> Result<()> {
        // the directory tree could be flat, or two levels deep
        let mut known_inodes = vec![];
        for entity in WalkDir::new(&self.base_dir) {
            let entry = entity?;
            if entry.file_type().is_file() {
                let file_name = entry.file_name().to_string_lossy();
                if file_name.ends_with(".inode") {
                    let inode = file_name.split('.').next().unwrap();
                    known_inodes.push(inode.parse::<u64>()?);
                }
            }
        }

        Ok(())
    }

    pub fn check_inode(&self, id: u64) -> bool {
        self.get_path(&id.to_string(), "inode").exists()
    }

    pub fn check_topology(&self, id: u64) -> bool {
        self.get_path(&id.to_string(), "shmr").exists()
    }

    /// Lookup & Read the given Block ID.
    /// Returns an Error when the block is not found or cannot be deserialized.
    pub fn read_inode(&self, id: u64) -> Result<Inode> {
        let path = self.get_path(&id.to_string(), "inode");
        let content = self.read_file(&path)?;

        if content.is_none() {
            bail!("Inode {} is empty", id);
        }

        let archive = content.unwrap();

        let archived = rkyv::check_archived_root::<Inode>(&archive).unwrap();
        let result: Inode = match archived.deserialize(&mut rkyv::Infallible) {
            Ok(inode) => inode,
            Err(e) => {
                // this should never happen, but you can never say never
                bail!("Failed to deserialize Inode: {:?}", e);
            }
        };

        Ok(result)
    }

    pub fn read_descriptor(&self, id: u64) -> Result<InodeDescriptor> {
        let path = self.get_path(&id.to_string(), "shmr");
        let content = self.read_file(&path)?;

        if content.is_none() {
            bail!("Topology for Inode {} is empty", id);
        }
        let archive = content.unwrap();
        let archived = rkyv::check_archived_root::<InodeDescriptor>(&archive).unwrap();
        let result: InodeDescriptor = match archived.deserialize(&mut rkyv::Infallible) {
            Ok(inode_descriptor) => inode_descriptor,
            Err(e) => {
                // this should never happen, but you can never say never
                bail!("Failed to deserialize InodeDescriptor: {:?}", e);
            }
        };

        Ok(result)
    }

    pub fn write_inode(&self, id: u64, descriptor: &Inode) -> Result<()> {
        let path = self.get_path(&id.to_string(), "inode");

        if !path.exists() {
            std::fs::create_dir_all(path.parent().unwrap())?;
        }

        let pos = rkyv::to_bytes::<_, 256>(descriptor)?;
        std::fs::write(&path, &pos[..])?;
        trace!("Wrote inode to {:?}", path);
        Ok(())
    }

    pub fn write_descriptor(&self, id: u64, descriptor: &InodeDescriptor) -> Result<()> {
        let path = self.get_path(&id.to_string(), "shmr");

        let pos = rkyv::to_bytes::<_, 256>(descriptor)?;
        std::fs::write(&path, &pos[..])?;
        trace!("Wrote topology to {:?}", path);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;
    use crate::fsdb::FsDB;

    #[test]
    fn test_inode_index() {
        let db = FsDB::open(Path::new("/tmp/shmr")).unwrap();
        assert_eq!(db.known_inodes.len(), 0);

    }
}
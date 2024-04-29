use crate::fuse::{Inode, InodeDescriptor};
use anyhow::{bail, Context, Result};
use rkyv::Deserialize;
use std::path::PathBuf;

pub struct FsDB {
    ino_db: sled::Db,
    // ino_cache: ()
    dct_db: sled::Db
    // dct_cache: ()
    // TODO Eventually we will want an object cache so we can skip some serialization action
}
impl FsDB {
    pub fn open(base_dir: PathBuf) -> Result<Self> {
        let ino_path = base_dir.join("superblock");
        let dct_path = base_dir.join("topology");
        if !ino_path.exists() {
            std::fs::create_dir_all(&ino_path)?;
        }
        if !dct_path.exists() {
            std::fs::create_dir_all(&dct_path)?;
        }

        let ino_db = sled::open(ino_path).context("unable to open 'superblock' directory")?;
        let dct_db = sled::open(dct_path).context("unable to open 'topology' directory")?;

        Ok(Self {
            ino_db, dct_db
        })
    }
    pub fn generate_id(&self) -> Result<u64> {
        Ok(self.ino_db.generate_id()?)
    }

    pub fn check_inode(&self, id: u64) -> Result<bool> {
        Ok(self.ino_db.contains_key(id.to_le_bytes())?)
    }

    pub fn check_descriptor(&self, id: u64) -> Result<bool> {
        Ok(self.dct_db.contains_key(id.to_le_bytes())?)
    }

    pub fn read_inode(&self, id: u64) -> Result<Inode> {
        let key = id.to_le_bytes();
        let archive = match self.ino_db.get(key)? {
            None => { bail!("inode does not exist") }
            Some(inode) => inode
        };

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
        let key = id.to_le_bytes();
        let archive = match self.dct_db.get(key)? {
            None => { bail!("inode does not exist") }
            Some(inode) => inode
        };

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
        let key = id.to_le_bytes();

        let pos = rkyv::to_bytes::<_, 256>(descriptor)?;
        self.ino_db.insert(key, &*pos)?;
        Ok(())
    }

    pub fn write_descriptor(&self, id: u64, descriptor: &InodeDescriptor) -> Result<()> {
        let key = id.to_le_bytes();

        let pos = rkyv::to_bytes::<_, 256>(descriptor)?;
        self.dct_db.insert(key, &*pos)?;
        Ok(())
    }
}

// #[cfg(test)]
// mod tests {
//     use crate::fsdb::FsDB;
//     use std::path::Path;
//
//     #[test]
//     fn test_inode_index() {
//         let db = FsDB::open(Path::new("/tmp/shmr")).unwrap();
//         assert_eq!(db.known_inodes.len(), 0);
//     }
// }

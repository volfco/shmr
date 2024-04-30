use crate::fuse::{Inode, InodeDescriptor};
use anyhow::{bail, Result};
use std::path::PathBuf;
use rand::Rng;
use redb::{Database, ReadableTableMetadata, TableDefinition};

const INO_TABLE: TableDefinition<u64, Inode> = TableDefinition::new("inode_data");
const DCT_TABLE: TableDefinition<u64, InodeDescriptor> = TableDefinition::new("descriptor_data");


pub struct FsDB {
    db: Database
}
impl FsDB {
    pub fn open(base_dir: PathBuf) -> Result<Self> {
        let db_path = base_dir.join("shmr.db");
        Ok(Self {
            db: if !db_path.exists() {
                Database::create(&db_path)?
            } else {
                Database::open(&db_path)?
            }
        })
    }

    pub fn check_init(&self) -> Result<()> {
        let txn = self.db.begin_write()?;
        {
            let ino_table = txn.open_table(INO_TABLE)?;
            let _ = ino_table.len();
        }
        txn.commit()?;
        Ok(())
    }

    pub fn generate_id(&self) -> Result<u64> {
        let mut rng = rand::thread_rng();
        loop {
            let ino: u64 = rng.gen();
            let read_txn = self.db.begin_read()?;
            let table = read_txn.open_table(INO_TABLE)?;
            if table.get(&ino)?.is_none() {
                return Ok(ino);
            }
        }
    }

    pub fn check_inode(&self, id: u64) -> Result<bool> {
        let read_txn = self.db.begin_read()?;
        let table = read_txn.open_table(INO_TABLE)?;
        Ok(table.get(&id)?.is_some())
    }

    pub fn check_descriptor(&self, id: u64) -> Result<bool> {
        let read_txn = self.db.begin_read()?;
        let table = read_txn.open_table(DCT_TABLE)?;
        Ok(table.get(&id)?.is_some())
    }

    pub fn read_inode(&self, id: u64) -> Result<Inode> {
        let read_txn = self.db.begin_read()?;
        let table = read_txn.open_table(INO_TABLE)?;

        match table.get(&id)? {
            None => { bail!("inode does not exist") }
            Some(inode) => { Ok(inode.value()) }
        }
    }
    pub fn read_descriptor(&self, id: u64) -> Result<InodeDescriptor> {
        let read_txn = self.db.begin_read()?;
        let table = read_txn.open_table(DCT_TABLE)?;

        match table.get(&id)? {
            None => { bail!("InodeDescriptor does not exist") }
            Some(descriptor ) => { Ok(descriptor.value()) }
        }
    }

    pub fn write_inode(&self, id: u64, descriptor: &Inode) -> Result<()> {
        let txn = self.db.begin_write()?;
        {
            let mut table = txn.open_table(INO_TABLE)?;
            let _ = table.insert(&id, descriptor)?;
        }
        txn.commit()?;
        Ok(())
    }

    pub fn write_descriptor(&self, id: u64, descriptor: &InodeDescriptor) -> Result<()> {
        let txn = self.db.begin_write()?;
        {
            let mut table = txn.open_table(DCT_TABLE)?;
            let _ = table.insert(&id, descriptor)?;
        }
        txn.commit()?;
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

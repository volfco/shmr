use crate::config::ShmrError;
use crate::db::now_unix;
use crate::vfs::block::VirtualBlock;
use crate::vfs::path::VirtualPath;
use crate::vfs::VirtualFile;
use log::trace;
use r2d2::Pool;
use r2d2_sqlite::SqliteConnectionManager;
use rusqlite::params;

#[derive(Clone, Debug)]
pub struct FileDB {
    conn: Pool<SqliteConnectionManager>,
}
impl FileDB {
    pub fn open(conn: Pool<SqliteConnectionManager>) -> FileDB {
        FileDB { conn }
    }

    pub fn load_virtual_file(&self, ino: u64) -> Result<VirtualFile, ShmrError> {
        let conn = self.conn.get()?;

        let mut vf = VirtualFile::new();
        vf.ino = ino;
        vf.blocks = self.load_blocks(ino)?;

        let mut stmt = conn.prepare("SELECT size FROM inode WHERE inode = ?1")?;
        stmt.query_row(params![ino], |row| {
            vf.size = row.get(0)?;
            Ok(())
        })?;

        trace!("successfully loaded inode {}", ino);

        Ok(vf)
    }

    /// Load and Return all VirtualBlocks associated with the given Inode
    fn load_blocks(&self, ino: u64) -> Result<Vec<VirtualBlock>, ShmrError> {
        let conn = self.conn.get()?;
        let mut stmt =
            conn.prepare("SELECT idx, size, topology FROM storage_block WHERE inode = ?1")?;
        let mut shard_stmt =
            conn.prepare("SELECT pool, bucket, filename FROM storage_block_shard WHERE inode = ?1 AND idx = ?2 ORDER BY shard_idx ASC")?;

        let mut rtn = vec![];

        let mut rows = stmt.query(params![&ino])?;
        while let Some(row) = rows.next()? {
            let topology: String = row.get(2)?;
            let mut block = VirtualBlock::new();
            block.idx = row.get(0)?;
            block.size = row.get(1)?;
            block.topology = topology.try_into().unwrap();

            let mut shard_rows = shard_stmt.query(params![&ino, &block.idx])?;
            while let Some(shard_row) = shard_rows.next()? {
                let pool: String = shard_row.get(0)?;
                let bucket: String = shard_row.get(1)?;
                let filename: String = shard_row.get(2)?;
                block.shards.push(VirtualPath {
                    pool,
                    bucket,
                    filename,
                });
            }

            rtn.push(block);
        }

        Ok(rtn)
    }

    /// Save the VirtualFile's metadata to the database. All Inode, Chunk, and Storageblock info is
    /// updated. This method is not intended to create a VirtualFile, as that should be done as part
    /// of the Inode creation.
    pub fn save_virtual_file(&self, vf: &VirtualFile) -> Result<(), ShmrError> {
        let conn = self.conn.get()?;

        // check to make sure the Inode entry already exists.
        let mut stmt = conn.prepare("SELECT EXISTS(SELECT inode FROM inode WHERE inode = ?1)")?;
        let row_exists: bool = stmt.query_row(params![vf.ino], |row| row.get(0))?;

        if !row_exists {
            return Err(ShmrError::InodeNotExist);
        }

        let mut conn = self.conn.get()?;
        let tx = conn.transaction()?;

        // mtime, or modification time, is the timestamp of the last content-related modification of a file.
        // ctime, or change time, records the last time the file's metadata (like permissions, link count, etc.) or content were changed
        let _ = tx.execute(
            "UPDATE inode SET size = ?2, ctime = ?3 WHERE inode = ?1",
            params![vf.ino, vf.size, now_unix()],
        )?;

        // update the blocks
        for (idx, block) in vf.blocks.iter().enumerate() {
            // update each shard first
            for (shard_idx, shard) in block.shards.iter().enumerate() {
                tx.execute(
                    "INSERT OR REPLACE INTO storage_block_shard (inode, idx, shard_idx, pool, bucket, filename) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                    params![vf.ino, idx, shard_idx, shard.pool, shard.bucket, shard.filename])?;
            }
            // then update the storage block
            let topology_str: String = block.topology.clone().into();
            tx.execute("INSERT OR REPLACE INTO storage_block (inode, idx, size, topology) VALUES (?1, ?2, ?3, ?4)", params![vf.ino, idx, vf.size, topology_str])?;
        }

        tx.commit()?;

        Ok(())
    }
}

fn parse_chunk_pointer(buf: Vec<u8>) -> (u64, u64) {
    (
        u64::from_be_bytes(buf[..8].try_into().expect("REASON")),
        u64::from_be_bytes(buf[8..].try_into().expect("REASON2")),
    )
}
fn create_chunk_pointer(idx: u64, pos: u64) -> Vec<u8> {
    let mut pointer = Vec::new();
    pointer.extend_from_slice(&idx.to_be_bytes());
    pointer.extend_from_slice(&pos.to_be_bytes());
    pointer
}

#[cfg(test)]
mod tests {
    use crate::db::file::{create_chunk_pointer, parse_chunk_pointer};

    #[test]
    fn test_chunk_pointer() {
        let block: u64 = 9957;
        let pos: u64 = 1234456;

        let buf = create_chunk_pointer(block, pos);
        assert_eq!(buf.len(), 16); // u64 = 8 bytes. x 2

        let (new_block, new_pos) = parse_chunk_pointer(buf);
        assert_eq!(block, new_block);
        assert_eq!(pos, new_pos);
    }
}

use log::trace;
use r2d2::Pool;
use r2d2_sqlite::SqliteConnectionManager;
use rusqlite::params;
use crate::config::ShmrError;
use crate::db::now_unix;
use crate::vfs::block::VirtualBlock;
use crate::vfs::path::VirtualPath;
use crate::vfs::VirtualFile;

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
        vf.chunk_map = self.load_chunk_map(ino)?;

        let mut stmt = conn.prepare("SELECT size, blksize FROM inode WHERE inode = ?1")?;
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
        let mut rtn = vec![];

        let blocks = stmt.query_map(params![&ino], |row| {
            let topology: String = row.get(2)?;
            let mut block = VirtualBlock::new();
            block.idx = row.get(0)?;
            block.size = row.get(1)?;
            block.topology = topology.try_into().unwrap();

            Ok(block)
        })?;

        for block in blocks {
            let mut block = block?;
            let mut stmt = conn.prepare("SELECT pool, bucket, filename FROM storage_block_shard WHERE inode = ?1 AND idx = ?2")?;
            stmt.query_row(params![&ino], |row| {
                block.shards.push(VirtualPath {
                    pool: row.get(0)?,
                    bucket: row.get(1)?,
                    filename: row.get(2)?,
                });
                Ok(())
            })?;

            rtn.push(block);
        }

        Ok(rtn)
    }

    fn load_chunk_map(&self, ino: u64) -> Result<Vec<(u64, u64)>, ShmrError> {
        // chunk map is (inode, chunk_idx) -> Vec<u8>. First half is u64 of StorageBlock, 2nd is block offset
        let conn = self.conn.get()?;
        let mut stmt = conn.prepare(
            "SELECT pointer FROM inode_chunk_map WHERE inode = ?1 ORDER BY chunk_idx ASC;",
        )?;

        let rows = stmt.query_map(params![ino], |row| {
            let blob: Vec<u8> = row.get(0)?;
            Ok(parse_chunk_pointer(blob))
        })?;

        let mut chunk_map = Vec::new();
        for row in rows {
            chunk_map.push(row?);
        }

        Ok(chunk_map)
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

        // update the chunk map
        for (idx, chunk) in vf.chunk_map.iter().enumerate() {
            tx.execute(
                "INSERT OR REPLACE INTO chunk (inode, chunk_idx, pointer) VALUES (?1, ?2, ?3)",
                params![vf.ino, idx, create_chunk_pointer(chunk.0, chunk.1)],
            )?;
        }

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

        let buf =create_chunk_pointer(block, pos);
        assert_eq!(buf.len(), 16); // u64 = 8 bytes. x 2

        let (new_block, new_pos) = parse_chunk_pointer(buf);
        assert_eq!(block, new_block);
        assert_eq!(pos, new_pos);
    }
}

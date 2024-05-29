use crate::config::ShmrError;
use crate::fuse::types::IFileType;
use fuser::{FileAttr, Request, TimeOrNow};
use r2d2::Pool;
use r2d2_sqlite::SqliteConnectionManager;
use rusqlite::{named_params, params};
use std::time::{SystemTime, UNIX_EPOCH};
use log::debug;

#[derive(Clone, Debug)]
pub struct InodeDB {
    conn: Pool<SqliteConnectionManager>,
}
impl InodeDB {
    pub fn open(conn: Pool<SqliteConnectionManager>) -> InodeDB {
        InodeDB { conn }
    }
    /// Get Inode Permission bits.
    /// (perm, uid, gid)
    fn get_inode_perms(&self, ino: u64) -> Result<(u16, u32, u32), ShmrError> {
        let conn = self.conn.get()?;
        let mut stmt = conn.prepare("SELECT perm, uid, gid FROM inode WHERE inode = ?1")?;

        Ok(stmt.query_row(params![&ino], |row| {
            Ok((row.get(0)?, row.get(1)?, row.get(2)?))
        })?)
    }

    /// Check the requestor's access to the given inode
    pub fn check_access(
        &self,
        ino: u64,
        request: &Request,
        mut access_mask: i32,
    ) -> Result<bool, ShmrError> {
        // F_OK tests for existence of file
        if access_mask == libc::F_OK {
            return Ok(true);
        }

        let inode_perms = self.get_inode_perms(ino)?;

        let file_mode = inode_perms.0 as i32;
        let uid = request.uid();
        let gid = request.gid();

        // root is allowed to read & write anything
        if uid == 0 {
            // root only allowed to exec if one of the X bits is set
            access_mask &= libc::X_OK;
            access_mask -= access_mask & (file_mode >> 6);
            access_mask -= access_mask & (file_mode >> 3);
            access_mask -= access_mask & file_mode;
            return Ok(access_mask == 0);
        }

        if uid == inode_perms.1 {
            access_mask -= access_mask & (file_mode >> 6);
        } else if gid == inode_perms.2 {
            // TODO we might need more indepth group checking
            access_mask -= access_mask & (file_mode >> 3);
        } else {
            access_mask -= access_mask & file_mode;
        }

        Ok(access_mask == 0)
    }

    pub fn to_fileattr(&self, ino: u64) -> Result<FileAttr, ShmrError> {
        let conn = self.conn.get()?;
        let mut stmt = conn.prepare("SELECT size, atime, mtime, ctime, crtime, kind, perm, nlink, uid, gid, rdev, blksize FROM inode WHERE inode = ?1")?;

        let result = stmt.query_row(params![&ino], |row| {
            let raw_kind: String = row.get(5)?;
            let filetype: IFileType = raw_kind.into();
            Ok(FileAttr {
                ino,
                size: row.get(0)?,
                blocks: 0,
                atime: convert_timestamp(row.get(1)?),
                mtime: convert_timestamp(row.get(2)?),
                ctime: convert_timestamp(row.get(3)?),
                crtime: convert_timestamp(row.get(4)?),
                kind: filetype.into(),
                perm: row.get(6)?,
                nlink: row.get(7)?,
                uid: row.get(8)?,
                gid: row.get(9)?,
                rdev: row.get(10)?,
                blksize: row.get(11)?,
                flags: 0,
            })
        })?;

        debug!("FileAttr for ino {}. {:?}", ino, result);

        Ok(result)
    }

    /// Create a new Inode.
    /// If ino is None, a new ID is generated and returned
    pub fn create_inode(
        &self,
        ino: Option<u64>,
        kind: IFileType,
        uid: u32,
        gid: u32,
        blksize: u64,
        flags: u32,
    ) -> Result<u64, ShmrError> {
        // inode is generated in the database
        let conn = self.conn.get()?;

        let now = SystemTime::now();
        let crtime = now
            .duration_since(UNIX_EPOCH)
            .expect("SystemTime before UNIX_EPOCH... which is bad")
            .as_secs();

        // TODO refactor this so the query is built in a more readable away
        let mut query = String::new();
        query.push_str("INSERT INTO inode (");
        if ino.is_some() {
            query.push_str("inode, ")
        }
        query.push_str("crtime, kind, perm, nlink, uid, gid, blksize, flags, rdev) VALUES (");
        if ino.is_some() {
            let v = format!("{}, ", ino.unwrap());
            query.push_str(v.as_str());
        }
        query.push_str(":crtime, :kind, :perm, :nlink, :uid, :gid, :blksize, :flags, :rdev)");

        let mut stmt = conn.prepare(&query)?;

        let mut nlink = 0;
        if IFileType::Directory == kind {
            // when we're creating a directory, we have two entries upon creation; "." and ".."
            // TODO confirm this is correct
            nlink = 2;
        }

        stmt.execute(named_params! {
            ":crtime": crtime,
            ":kind": String::from(kind.clone()),
            ":perm": 0o777,
            ":nlink": nlink,
            ":uid": uid,
            ":gid": gid,
            ":blksize": blksize,
            ":flags": flags,
            ":rdev": 0
        })?;

        let inode = conn.last_insert_rowid() as u64;

        // if the type of entry is a directory, populate the initial entries
        if IFileType::Directory == kind {
            self.add_directory_entry(inode, ".", inode)?;
        }

        Ok(inode)
    }

    pub fn check_inode(&self, ino: u64) -> Result<bool, ShmrError> {
        let conn = self.conn.get()?;

        let mut stmt = conn.prepare("SELECT EXISTS(SELECT inode FROM inode WHERE inode = ?1)")?;
        let row_exists: bool = stmt.query_row(params![ino], |row| row.get(0))?;

        Ok(row_exists)
    }

    pub fn update_inode(&self, ino: u64, mode: Option<u32>, uid: Option<u32>, gid: Option<u32>, _size: Option<u64>, _atime: Option<TimeOrNow>, _mtime: Option<TimeOrNow>, _ctime: Option<SystemTime>, _crtime: Option<SystemTime>, _chgtime: Option<SystemTime>, _bkuptime: Option<SystemTime>, flags: Option<u32>) -> Result<(), ShmrError> {
        let mut conn = self.conn.get()?;
        let tx = conn.transaction()?;

        if let Some(mode) = mode {
            tx.execute("UPDATE inode SET mode = ?2 WHERE inode = ?1", params![ino, mode])?;
        }
        if let Some(uid) = uid {
            tx.execute("UPDATE inode SET uid = ?2 WHERE inode = ?1", params![ino, uid])?;
        }
        if let Some(gid) = gid {
            tx.execute("UPDATE inode SET gid = ?2 WHERE inode = ?1", params![ino, gid])?;
        }
        if let Some(flags) = flags {
            tx.execute("UPDATE inode SET flags = ?2 WHERE inode = ?1", params![ino, flags])?;
        }
        // if let Some(size) = size {
        //     tx.execute("UPDATE inode SET size = ?2 WHERE inode = ?1", params![ino, size])?;
        // }

        // if let Some(atime) = atime {
        //     tx.execute("UPDATE inode SET atime = ?2 WHERE inode = ?1", params![ino, atime])?;
        // }
        // if let Some(mtime) = mtime {
        //     tx.execute("UPDATE inode SET mtime = ?2 WHERE inode = ?1", params![ino, mtime])?;
        // }
        // if let Some(ctime) = ctime {
        //     tx.execute("UPDATE inode SET ctime = ?2 WHERE inode = ?1", params![ino, ctime])?;
        // }
        // if let Some(crtime) = mtime {
        //     tx.execute("UPDATE inode SET crtime = ?2 WHERE inode = ?1", params![ino, crtime])?;
        // }

        tx.commit()?;
        Ok(())
    }

    pub fn is_file(&self, ino: u64) -> Result<bool, ShmrError> {
        let conn = self.conn.get()?;

        let mut stmt = conn.prepare(
            "SELECT EXISTS(SELECT inode FROM inode WHERE inode = ?1 AND kind = 'RegularFile')",
        )?;
        let row_exists: bool = stmt.query_row(params![ino], |row| row.get(0))?;

        Ok(row_exists)
    }

    /// Add a new Entry for the given Inode
    /// TODO Update the Parent's Inode modified time
    pub fn add_directory_entry(&self, ino: u64, name: &str, target: u64) -> Result<(), ShmrError> {
        let conn = self.conn.get()?;

        let mut stmt = conn.prepare("SELECT kind FROM inode WHERE inode = ?1")?;
        let filetype_row: (String,) = stmt.query_row(params![ino], |row| Ok((row.get(0)?,)))?;
        let filetype: IFileType = filetype_row.0.into();
        if filetype != IFileType::Directory {
            return Err(ShmrError::InvalidInodeType(format!(
                "inode {} is not a directory",
                ino
            )));
        }

        // No validation is needed, as the database *should* validate due to foreign key constraints
        let mut stmt = conn.prepare(
            "INSERT INTO directory (parent, entry, inode) VALUES (:parent, :entry, :target)",
        )?;
        stmt.execute(named_params! {
            ":parent": ino,
            ":entry": name.as_bytes(),
            ":target": target
        })?;

        Ok(())
    }

    /// TODO Update the Parent's Inode modified time
    pub fn remove_directory_entry(&self, ino: u64, name: &str) -> Result<(), ShmrError> {
        let conn = self.conn.get()?;
        let mut stmt = conn.prepare("DELETE FROM directory WHERE parent = ?1 AND entry = ?2")?;
        stmt.execute(params![ino, name.as_bytes()])?;
        Ok(())
    }

    pub fn get_directory_entry_inode(
        &self,
        parent: u64,
        name: &str,
    ) -> Result<Option<u64>, ShmrError> {
        let conn = self.conn.get()?;
        let mut stmt =
            conn.prepare("SELECT inode FROM directory WHERE parent = ?1 AND entry = ?2")?;
        let result = stmt.query_row(params![parent, name.as_bytes()], |row| row.get(0));
        match result {
            Ok(inode) => Ok(Some(inode)),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(err) => Err(ShmrError::DatabaseError(err)),
        }
    }

    pub fn get_directory_entries(&self, parent_ino: u64) -> Result<Vec<(Vec<u8>, u64)>, ShmrError> {
        let conn = self.conn.get()?;
        let mut stmt = conn
            .prepare("SELECT entry, inode FROM directory WHERE parent = ?1 ORDER BY entry ASC")?;

        let mut entries = Vec::new();
        let results = stmt.query_map(params![parent_ino], |row| {
            let entry: Vec<u8> = row.get(0)?;
            let inode: u64 = row.get(1)?;
            Ok((entry, inode))
        })?;

        for result in results {
            entries.push(result?);
        }
        Ok(entries)
    }
}

fn convert_timestamp(unix: u64) -> SystemTime {
    UNIX_EPOCH + std::time::Duration::from_secs(unix)
}

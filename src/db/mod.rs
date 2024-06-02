pub mod inode;
pub mod file;

use r2d2::Pool;
use r2d2_sqlite::SqliteConnectionManager;
use std::time::{SystemTime, UNIX_EPOCH};
use log::error;
use crate::config::ShmrFsConfig;

pub fn get_connection(config: &ShmrFsConfig) -> Pool<SqliteConnectionManager> {
    let db_path = config.metadata_dir.join("shmr.sqlite");
    let db = Pool::new(SqliteConnectionManager::file(db_path)).unwrap();

    let lconn = db.get().unwrap();
    for (key, val) in config.sqlite_options.iter() {
        if let Err(e) = lconn.pragma_update(None, key, val) {
            error!("unable to set db pragma. {} => {}. {:?}", key, val, e);
            // TODO should this be a panic?
        }
    }

    db
}

fn now_unix() -> u64 {
    let now = SystemTime::now();
    now.duration_since(UNIX_EPOCH)
        .expect("SystemTime before UNIX_EPOCH... which is bad")
        .as_secs()
}

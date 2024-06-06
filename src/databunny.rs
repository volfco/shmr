use parking_lot::{ArcRwLockReadGuard, ArcRwLockWriteGuard, RawRwLock, RwLock};
use std::collections::BTreeMap;
use log::{debug, error, info, trace};
use serde::{de::DeserializeOwned, Serialize};
use std::fmt::Debug;
use std::hash::Hash;
use std::ops::Deref;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::thread;
use std::thread::JoinHandle;
use std::time::{Duration, Instant};
use std::fs::OpenOptions;
use std::io::{Read, Write};
use base64::prelude::*;
use log::{debug, error, warn};

pub trait StorageBackend {
    /// Open the StorageBackend
    fn open(&self) -> Result<(), BunnyError>;

    /// Load the given key from disk
    fn load(&self, key: &[u8]) -> Result<Option<Vec<u8>>, BunnyError>;

    /// Return a Vector of all (Key, Value) pairs on disk
    fn load_all(&self) -> Result<Vec<(Vec<u8>, Vec<u8>)>, BunnyError>;

    fn exists(&self, key: &[u8]) -> bool;

    /// Write the given key,value pair
    fn save(&self, key: &[u8], val: &[u8]) -> Result<(), BunnyError>;
}

pub struct FilePerKey {
    pub base_dir: PathBuf
}
impl FilePerKey {
    fn get_path(&self, key: &[u8]) -> PathBuf {
        self.base_dir.join(format!("{}.toml", BASE64_STANDARD.encode(key)))
    }

    fn read_file(&self, path: PathBuf) -> Result<Vec<u8>, BunnyError> {
        let mut buf = vec![];
        let _amt = OpenOptions::new()
            .read(true)
            .open(path)?
            .read_to_end(&mut buf);

        Ok(buf)
    }

    fn write_file(&self, path: PathBuf, contents: &[u8]) -> Result<(), BunnyError> {
        OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(path)?
            .write_all(contents)?;

        Ok(())
    }
}
impl StorageBackend for FilePerKey {
    fn open(&self) -> Result<(), BunnyError> {
        todo!()
    }

    fn load(&self, key: &[u8]) -> Result<Option<Vec<u8>>, BunnyError> {
        let path = self.get_path(key);

        if !path.exists() {
            return Ok(None);
        }

        Ok(Some(self.read_file(path)?))
    }

    fn load_all(&self) -> Result<Vec<(Vec<u8>, Vec<u8>)>, BunnyError> {
        let mut entries = vec![];
        let paths = std::fs::read_dir(&self.base_dir).unwrap();
        for path in paths {
            let path = path.unwrap().path();
            if let Some(ext) = path.file_name() {
                let name = ext.to_str().unwrap().to_string();

                if !name.contains(".toml") {
                    debug!("skipping {:?}. invalid extension", &path);
                    continue;
                }

                // the rest of the filename is the key name, and should be base64 encoded
                let filename = BASE64_STANDARD.decode(&name.replace(".toml", ""));
                if filename.is_err() {
                    warn!("unable to decode {} as a base64 string. {:?}. skipping", name, filename.err().unwrap());
                    continue;
                }

                entries.push((filename.unwrap(), self.read_file(path)?))
            } else {
                error!("skipping {:?}. there's no filename??", &path);
            }
        }

        Ok(entries)
    }

    fn exists(&self, key: &[u8]) -> bool {
        self.get_path(key).exists()
    }

    fn save(&self, key: &[u8], val: &[u8]) -> Result<(), BunnyError> {
        self.write_file(self.get_path(key), val)
    }
}
const FLUSH_INTERVAL: u64 = 500; // in ms

pub enum BunnyError {
    IOError(std::io::Error),
}
impl From<std::io::Error> for BunnyError {
    fn from(value: std::io::Error) -> Self {
        BunnyError::IOError(value)
    }
}
impl From<toml::ser::Error> for BunnyError {
    fn from(value: toml::ser::Error) -> Self {
        todo!()
    }
}
impl From<toml::de::Error> for BunnyError {
    fn from(value: toml::de::Error) -> Self {
        todo!()
    }
}

pub type Entries<K, V> = Arc<RwLock<BTreeMap<K, Arc<RwLock<V>>>>>;

/// Basic In-memory database with persistence.
#[derive(Clone)]
pub struct DataBunny<
    K: Serialize + DeserializeOwned + Clone + Send + Sync + Ord + 'static,
    V: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
> {
    entries: Entries<K, V>,
    dirty_entries: Arc<RwLock<Vec<K>>>,
    storage_backend: Arc<dyn StorageBackend>
}
impl<
    K: Serialize + DeserializeOwned + Clone + Send + Sync + Ord + 'static,
    V: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
> DataBunny<K, V>
{
    pub fn open(path: PathBuf) -> Result<Self, BunnyError> {
        let s = Self {
            entries: Arc::new(RwLock::new(BTreeMap::new())),
            dirty_entries: Arc::new(RwLock::new(vec![])),
            storage_backend: Arc::new(FilePerKey {
                base_dir: path
            })
        };

        Ok(s)
    }

    pub fn gen_id(&self) -> Result<u64, BunnyError> {
        todo!()
    }

    fn mark_dirty(&self, key: &K) {
        let mut h = self.dirty_entries.write();
        h.push(key.clone());
    }

    fn decode_entry(&self, buf: Vec<u8>) -> Result<V, BunnyError> {
        let s = String::from_utf8(buf).unwrap();
        Ok(toml::from_str(&s)?)
    }

    fn encode_entry(&self, entry: &V) -> Result<Vec<u8>, BunnyError> {
        Ok(toml::to_string_pretty(entry)?.as_bytes().to_vec())
    }

    /// Return a read-only copy of the Record
    pub fn get(&self, key: &K) -> Result<Option<ArcRwLockReadGuard<RawRwLock, V>>, BunnyError> {

        let entry_handle = self.entries.read();
        if let Some(entry) = entry_handle.get(key) {
            return Ok(Some(entry.read_arc()))
        }
        drop(entry_handle);

        let key_name = toml::to_string(key)?;
        if let Some(record) = self.storage_backend.load(key_name.as_bytes())? {
            self.insert(key.clone(), self.decode_entry(record)?)?;

            self.get(key)
        } else {
            Ok(None)
        }

    }

    /// Return a writeable reference to the entry
    pub fn get_mut(&self, key: &K) -> Result<Option<ArcRwLockWriteGuard<RawRwLock, V>>, BunnyError> {
        let entry_handle = self.entries.read();

        if let Some(entry) = entry_handle.get(key) {
            self.mark_dirty(key);
            return Ok(Some(entry.write_arc()))
        }
        drop(entry_handle);

        let key_name = toml::to_string(key)?;
        if let Some(record) = self.storage_backend.load(key_name.as_bytes())? {
            self.insert(key.clone(), self.decode_entry(record)?)?;

            self.get_mut(key)
        } else {
            Ok(None)
        }
    }

    /// Is there an entry for the given key
    pub fn has(&self, ident: &K) -> bool {
        let handle = self.entries.read();
        handle.contains_key(ident)
    }

    /// Insert an entry
    pub fn insert(&self, key: K, value: V) -> Result<(), BunnyError> {
        let mut handle = self.entries.write();
        let _ =  handle.insert(key, Arc::new(RwLock::new(value)));
        Ok(())
    }

    // /// Serialize & Flush the given entry to the underlying database, and then flush the database
    // pub fn flush(&self, ident: &K) -> Result<(), BunnyError> {
    //     if let Some(inner) = self.entries.get(ident) {
    //         let raw_id = bincode::serialize(&ident).unwrap();
    //         let raw = bincode::serialize(&*inner).unwrap();
    //         let _ = self.db.insert(raw_id, raw).unwrap();
    //     } else {
    //         debug!(
    //             "entity id {:?} does not exist in main map. assuming deleted",
    //             ident
    //         );
    //         // todo!("remove entry from sled Db")
    //     }
    //
    //     // No way around flushing the entire Sled Db here, but this should be the quick part
    //     let _ = self.db.flush().unwrap();
    //
    //     Ok(())
    // }
    //
    // /// Serialize & Flush all entries to the underlying database, and then flush the database
    // pub fn flush_all(&self) -> Result<(), BunnyError> {
    //     // lock the dirty list, and hold it until we're done
    //     let mut handle = self.dirties.write().unwrap();
    //
    //     if handle.is_empty() {
    //         return Ok(());
    //     }
    //
    //     while let Some(entry_id) = handle.pop() {
    //         if let Some(inner) = self.entries.get(&entry_id) {
    //             let raw_id = bincode::serialize(&entry_id).unwrap();
    //             let raw = bincode::serialize(&*inner).unwrap();
    //             let _ = self.db.insert(raw_id, raw).unwrap();
    //         } else {
    //             debug!(
    //                 "entity id {:?} does not exist in main map. assuming deleted",
    //                 entry_id
    //             );
    //             // todo!("remove entry from sled Db")
    //         }
    //     }
    //
    //     let bytes = self.db.flush().unwrap();
    //     debug!("flushed {} bytes to disk", bytes);
    //
    //     Ok(())
    // }
}
// impl<
//     K: Serialize + DeserializeOwned + Clone + Send + Sync + Ord + 'static,
//     V: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
// > Drop for DataBunny<K, V>
// {
//     fn drop(&mut self) {
//         debug!("FsDB2 Dropped. shutting down");
//         let mut run = self.run_state.lock().unwrap();
//         if !(*run) {
//             trace!("already shutdown, nothing to do");
//             // check if we're running. If not, we get a deadlock?
//             // need to figure out the actual problem
//             return;
//         }
//         *run = false;
//
//         drop(run);
//
//         {
//             // info!("waiting for join_handle lock");
//             let mut join_handle_opt = self.flusher.lock().unwrap();
//             // info!("waiting for thread to end");
//             if let Some(join_handle) = join_handle_opt.take() {
//                 if let Err(e) = join_handle.join() {
//                     error!("error joining Background thread: {:?}", e);
//                 }
//             }
//             drop(join_handle_opt);
//         }
//     }
// }
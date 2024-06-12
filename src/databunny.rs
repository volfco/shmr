use base64::prelude::*;
use log::{debug, error, info, trace, warn};
use parking_lot::{ArcRwLockReadGuard, ArcRwLockWriteGuard, RawRwLock, RwLock};
use serde::{de::DeserializeOwned, Serialize};
use std::collections::BTreeMap;
use std::fmt::Debug;
use std::fs::OpenOptions;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::Arc;
use crate::tasks::{WorkerTask, WorkerThread};

pub trait StorageBackend: Debug + Send + Sync {
    /// Open the StorageBackend
    fn open(&self) -> Result<(), BunnyError>;

    /// Load the given key from disk
    fn load(&self, key: &[u8]) -> Result<Option<Vec<u8>>, BunnyError>;

    /// Return a Vector of all (Key, Value) pairs on disk
    fn load_all(&self) -> Result<Vec<(Vec<u8>, Vec<u8>)>, BunnyError>;

    fn exists(&self, key: &[u8]) -> bool;

    /// Write the given key,value pair to disk.
    /// This assumes that the data is fully persisted upon return
    fn save(&self, key: &[u8], val: &[u8]) -> Result<(), BunnyError>;
}

#[derive(Clone, Debug)]
pub struct FilePerKey {
    pub base_dir: PathBuf,
}
impl FilePerKey {
    fn get_path(&self, key: &[u8]) -> PathBuf {
        self.base_dir
            .join(format!("{}.yaml", String::from_utf8(key.to_vec()).unwrap()))
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
        info!("writing to {:?}", &path);
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

                if !name.contains(".yaml") {
                    debug!("skipping {:?}. invalid extension", &path);
                    continue;
                }

                // the rest of the filename is the key name, and should be base64 encoded
                let filename = BASE64_STANDARD.decode(name.replace(".yaml", ""));
                if filename.is_err() {
                    warn!(
                        "unable to decode {} as a base64 string. {:?}. skipping",
                        name,
                        filename.err().unwrap()
                    );
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

#[derive(Debug)]
pub enum BunnyError {
    EntryExists,
    IOError(std::io::Error),
    SerializationError(serde_yaml::Error),
}
impl From<std::io::Error> for BunnyError {
    fn from(value: std::io::Error) -> Self {
        BunnyError::IOError(value)
    }
}
impl From<serde_yaml::Error> for BunnyError {
    fn from(value: serde_yaml::Error) -> Self {
        BunnyError::SerializationError(value)
    }
}

pub type Entries<K, V> = Arc<RwLock<BTreeMap<K, Arc<RwLock<V>>>>>;

/// Basic In-memory database with persistence.
#[derive(Debug, Clone)]
pub struct DataBunny<
    K: ToString + FromStr + Clone + Send + Sync + Ord + 'static,
    V: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
> {
    entries: Entries<K, V>,
    dirty_entries: Arc<RwLock<Vec<K>>>,
    storage_backend: Arc<dyn StorageBackend>,
    worker_thread: Option<WorkerThread<BunnyWorker<K, V>>>
}
impl<
        K: ToString + FromStr + Clone + Send + Sync + Ord + 'static,
        V: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
    > DataBunny<K, V>
{
    pub fn open(path: &Path) -> Result<Self, BunnyError> {
        let mut s = Self {
            entries: Arc::new(RwLock::new(BTreeMap::new())),
            dirty_entries: Arc::new(RwLock::new(vec![])),
            storage_backend: Arc::new(FilePerKey {
                base_dir: path.to_path_buf(),
            }),
            worker_thread: None,
        };

        // TODO Load all entries from disk!

        let worker = WorkerThread::new(BunnyWorker::new(s.clone()));
        worker.spawn();

        s.worker_thread = Some(worker);

        Ok(s)
    }

    pub fn gen_id(&self) -> Result<u64, BunnyError> {
        todo!()
    }

    /// Return a copy of the last key in the BTree
    pub fn last_key(&self) -> K {
        let binding = self.entries.read();
        binding.last_key_value().unwrap().0.clone()
    }

    fn mark_dirty(&self, key: &K) {
        let mut h = self.dirty_entries.write();
        h.push(key.clone());
    }

    fn decode_entry(&self, buf: Vec<u8>) -> Result<V, BunnyError> {
        Ok(serde_yaml::from_slice(&*buf)?)
    }

    fn encode_entry(&self, entry: &V) -> Result<Vec<u8>, BunnyError> {
        Ok(serde_yaml::to_string(entry)?.as_bytes().to_vec())
    }

    /// Return a read-only copy of the Record
    pub fn get(&self, key: &K) -> Result<Option<ArcRwLockReadGuard<RawRwLock, V>>, BunnyError> {
        let entry_handle = self.entries.read();
        if let Some(entry) = entry_handle.get(key) {
            return Ok(Some(entry.read_arc()));
        }
        drop(entry_handle);

        let key_name = key.to_string();
        if let Some(record) = self.storage_backend.load(key_name.as_bytes())? {
            self.insert(key.clone(), self.decode_entry(record)?)?;

            self.get(key)
        } else {
            Ok(None)
        }
    }

    /// Return a writeable reference to the entry
    pub fn get_mut(
        &self,
        key: &K,
    ) -> Result<Option<ArcRwLockWriteGuard<RawRwLock, V>>, BunnyError> {
        let entry_handle = self.entries.read();

        if let Some(entry) = entry_handle.get(key) {
            self.mark_dirty(key);
            return Ok(Some(entry.write_arc()));
        }
        drop(entry_handle);

        if let Some(record) = self.storage_backend.load(key.to_string().as_bytes())? {
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
        if handle.contains_key(&key) {
            return Err(BunnyError::EntryExists);
        }
        self.mark_dirty(&key);
        let _ = handle.insert(key, Arc::new(RwLock::new(value)));
        Ok(())
    }

    /// Serialize & Flush the given entry to the underlying database, and then flush the database
    pub fn flush(&self, ident: &K) -> Result<(), BunnyError> {
        if let Some(inner) = self.get(ident)? {
            let key = ident.to_string();
            let val = serde_yaml::to_string(&*inner)?.as_bytes().to_vec();
            self.storage_backend.save(key.as_bytes(), &val)?;
        }
        Ok(())
    }

    pub fn flush_all(&self, dirty: bool) -> Result<(), BunnyError> {
        // TODO if dirty is true, only do dirty entries. If false, do all of them
        // lock the dirty list, and hold it until we're done
        let mut handle = self.dirty_entries.write();

        // TODO Refactor so the dirty marker isn't consumed until the file is written without error. Maybe use https://docs.rs/retry/latest/retry/ or https://docs.rs/retryiter/latest/retryiter/
        handle.sort();
        handle.dedup();

        while let Some(entry_id) = handle.pop() {
            self.flush(&entry_id)?;
        }
        Ok(())
    }
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

#[derive(Debug, Clone)]
struct BunnyWorker<
    K: ToString + FromStr + Clone + Send + Sync + Ord + 'static,
    V: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
> {
    inner: Arc<DataBunny<K, V>>
}
impl<
    K: ToString + FromStr + Clone + Send + Sync + Ord + 'static,
    V: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
> BunnyWorker<K, V> {
    fn new(inner: DataBunny<K, V>) -> Self {
        Self {
            inner: Arc::new(inner)
        }
    }
}
impl<
    K: ToString + FromStr + Clone + Send + Sync + Ord + 'static,
    V: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
> WorkerTask for BunnyWorker<K, V> {
    fn pre(&self) {
        info!("BunnyWorker started");
    }
    fn execute(&self) {
        // TODO every 5th iteration, set flush_all to false so we write everything
        if let Err(e) = self.inner.flush_all(true) {
            error!("an error occurred during the DataBunny flush. {:?}", e);
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::databunny::DataBunny;
    use std::collections::HashMap;

    #[test]
    fn test_data_bunny() {
        use std::path::PathBuf;
        use std::thread;
        use std::time::Duration;

        let test_db_path = PathBuf::from("test_db");
        if !test_db_path.exists() {
            std::fs::create_dir(&test_db_path).unwrap();
        }

        // Create new DataBunny instance
        let mut db =
            DataBunny::<String, HashMap<String, String>>::open(&test_db_path.clone()).unwrap();

        // Insert a new entry
        db.insert("test_key".to_string(), HashMap::new()).unwrap();

        // Save the entry
        db.flush(&"test_key".to_string()).unwrap();

        // Give the operating system a second to write the file to disk fully.
        thread::sleep(Duration::from_secs(1));

        // // Now it's time to check if the entry has actually been written to the disk.
        // // This is not a part of your question, but you might want to do this just to make sure
        // // your setup works as you expected.
        //
        // // Reload DataBunny from the disk
        // let mut db = DataBunny::<String, String>::open(test_db_path).unwrap();
        //
        // // Fetch the entry
        // let entry = db.get(&"test_key".to_string()).unwrap();
        //
        // // Unwrap from Arc and RwLock
        // let entry = entry.unwrap().read();
        // assert_eq!(*entry, "test_value".to_string());
    }
}

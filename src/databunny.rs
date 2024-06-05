use crate::ShmrError;
use chashmap::{CHashMap, ReadGuard, WriteGuard};
use log::{debug, error, info, trace};
use serde::{de::DeserializeOwned, Serialize};
use std::fmt::Debug;
use std::hash::Hash;
use std::path::Path;
use std::sync::{Arc, Mutex, RwLock};
use std::thread;
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

/*
 Need to have these traits:
 - When values are updated, modification is recorded
 - Modified values cannot be evicted from cache

 Then modified contents can be flushed to disk
*/

const FLUSH_INTERVAL: u64 = 500; // in ms

// Create a DashMap like struct that stores the data, and implements a write guard that marks the entry as dirty on drop?>
// ref; https://github.com/m-ou-se/rust-atomics-and-locks/blob/main/src/ch9_locks/rwlock_3.rs

/// Basic In-memory database with persistence.
///
/// BonsaiDB looks like a viable alternative to this.
#[derive(Clone)]
pub struct DataBunny<
    K: Serialize + DeserializeOwned + Eq + Hash + Clone + Send + Sync + Debug + 'static,
    V: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
> {
    /// HashMap of individual database entries, which each have their own lock
    entries: Arc<CHashMap<K, V>>,
    /// Vec of Dirty IDs
    dirties: Arc<RwLock<Vec<K>>>,

    db: sled::Db,

    run_state: Arc<Mutex<bool>>,
    flusher: Arc<Mutex<Option<JoinHandle<()>>>>,
}
impl<
    K: Serialize + DeserializeOwned + Eq + Hash + Clone + Send + Sync + Debug + 'static,
    V: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
> DataBunny<K, V>
{
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self, ShmrError> {
        let db: Result<sled::Db, sled::Error> = sled::open(path);
        let db = db.unwrap();
        let mut s = Self {
            entries: Arc::new(CHashMap::new()),
            dirties: Arc::new(RwLock::new(vec![])),
            db,

            run_state: Arc::new(Mutex::new(true)),
            flusher: Arc::new(Mutex::new(None)),
        };

        let interval = Duration::from_millis(FLUSH_INTERVAL);
        let thread_self = s.clone();

        let thread_handle = thread::spawn(move || {
            // TODO support a rapid shutdown,
            loop {
                {
                    let run = thread_self.run_state.lock().unwrap();
                    if !*run {
                        break;
                    }
                }

                thread::sleep(interval);

                {
                    if let Err(e) = thread_self.flush_all() {
                        error!("error occurred during flush_all()! {:?}", e);
                    }
                }
            }
            debug!("bg_thread has exited");
        });

        {
            let mut flush_lock = s.flusher.lock().unwrap();
            *flush_lock = Some(thread_handle);
        }

        s.load_entries()?;

        Ok(s)
    }

    fn load_entries(&mut self) -> Result<(), ShmrError> {
        // loop over every entry in the database
        if self.db.len() == 0 {
            debug!("empty database. nothing to load");
            return Ok(());
        }
        let start = Instant::now();
        trace!("starting entity load");

        for entry in self.db.iter() {
            let (key, val) = entry.unwrap();
            let kd = bincode::deserialize(&key[..]).unwrap();
            let vd = bincode::deserialize(&val[..]).unwrap();

            let _ = self.entries.insert(kd, vd);
        }

        info!(
            "loaded {} entries. took {:?}",
            self.entries.len(),
            start.elapsed()
        );
        Ok(())
    }
    pub fn gen_id(&self) -> Result<u64, ShmrError> {
        // when sled opens a database for the first time, the counter starts at zero. The next time
        // it opens, the counter starts at 2000000.
        // guess how I found this out.
        Ok(self.db.generate_id().unwrap() + 2)
    }

    /// Return a read-only copy of the Record
    pub fn get(&self, ident: &K) -> Option<ReadGuard<K, V>> {
        match self.entries.get(ident) {
            Some(entry) => Some(entry),
            None => {
                // try and load the entry from disk
                let key = bincode::serialize(ident).unwrap();
                match self.db.get(key).unwrap() {
                    None => None,
                    Some(raw) => {
                        self.entries
                            .insert(ident.clone(), bincode::deserialize(&raw[..]).unwrap());
                        self.entries.get(ident)
                    }
                }
            }
        }
    }

    /// Return a writeable reference to the entry
    pub fn get_mut(&self, ident: &K) -> Option<WriteGuard<K, V>> {
        // taint the entry, so we flush it to disk when we can
        let mut marker = self.dirties.write().unwrap();
        marker.push(ident.clone());
        drop(marker);

        self.entries.get_mut(ident)
    }

    /// Is there an entry for the given key
    pub fn has(&self, ident: &K) -> bool {
        self.entries.contains_key(ident)
    }

    /// Insert an entry
    pub fn insert(&self, ident: K, value: V) {
        let mut marker = self.dirties.write().unwrap();
        marker.push(ident.clone());

        let _ = self.entries.insert(ident, value);
        drop(marker);
    }

    /// Serialize & Flush the given entry to the underlying database, and then flush the database
    pub fn flush(&self, ident: &K) -> Result<(), ShmrError> {
        if let Some(inner) = self.entries.get(ident) {
            let raw_id = bincode::serialize(&ident).unwrap();
            let raw = bincode::serialize(&*inner).unwrap();
            let _ = self.db.insert(raw_id, raw).unwrap();
        } else {
            debug!(
                "entity id {:?} does not exist in main map. assuming deleted",
                ident
            );
            // todo!("remove entry from sled Db")
        }

        // No way around flushing the entire Sled Db here, but this should be the quick part
        let _ = self.db.flush().unwrap();

        Ok(())
    }

    /// Serialize & Flush all entries to the underlying database, and then flush the database
    pub fn flush_all(&self) -> Result<(), ShmrError> {
        // lock the dirty list, and hold it until we're done
        let mut handle = self.dirties.write().unwrap();

        if handle.is_empty() {
            return Ok(());
        }

        while let Some(entry_id) = handle.pop() {
            if let Some(inner) = self.entries.get(&entry_id) {
                let raw_id = bincode::serialize(&entry_id).unwrap();
                let raw = bincode::serialize(&*inner).unwrap();
                let _ = self.db.insert(raw_id, raw).unwrap();
            } else {
                debug!(
                    "entity id {:?} does not exist in main map. assuming deleted",
                    entry_id
                );
                // todo!("remove entry from sled Db")
            }
        }

        let bytes = self.db.flush().unwrap();
        debug!("flushed {} bytes to disk", bytes);

        Ok(())
    }
}
impl<
    K: Serialize + DeserializeOwned + Eq + Hash + Clone + Send + Sync + Debug + 'static,
    V: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
> Drop for DataBunny<K, V>
{
    fn drop(&mut self) {
        debug!("FsDB2 Dropped. shutting down");
        let mut run = self.run_state.lock().unwrap();
        if !(*run) {
            trace!("already shutdown, nothing to do");
            // check if we're running. If not, we get a deadlock?
            // need to figure out the actual problem
            return;
        }
        *run = false;

        drop(run);

        {
            // info!("waiting for join_handle lock");
            let mut join_handle_opt = self.flusher.lock().unwrap();
            // info!("waiting for thread to end");
            if let Some(join_handle) = join_handle_opt.take() {
                if let Err(e) = join_handle.join() {
                    error!("error joining Background thread: {:?}", e);
                }
            }
            drop(join_handle_opt);
        }
    }
}
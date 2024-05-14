use std::mem;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

const IO_TRACKER_ORDERING: Ordering = Ordering::Relaxed;
#[derive(Debug, Clone)]
pub struct IOTracker {
    /// When the IOTracker was zeroed
    when: Arc<Mutex<Instant>>,
    /// Number of Read Operations
    read: Arc<AtomicUsize>,
    /// Number of Write Operations
    write: Arc<AtomicUsize>,
}
impl Default for IOTracker {
    fn default() -> Self {
        IOTracker {
            when: Arc::new(Mutex::new(Instant::now())),
            read: Arc::new(AtomicUsize::new(0)),
            write: Arc::new(AtomicUsize::new(0)),
        }
    }
}
impl IOTracker {
    pub fn new() -> Self {
        IOTracker {
            when: Arc::new(Mutex::new(Instant::now())),
            read: Arc::new(Default::default()),
            write: Arc::new(Default::default()),
        }
    }
    pub fn inc_read(&self) {
        let _ = self.read.fetch_add(1, IO_TRACKER_ORDERING);
    }
    pub fn inc_write(&self) {
        let _ = self.write.fetch_add(1, IO_TRACKER_ORDERING);
    }

    /// Read the IO Operations since the given Instant. Resets counters after
    pub fn read(&self) -> (Instant, usize, usize) {
        let mut when = self.when.lock().unwrap();
        let read = self.read.swap(0, IO_TRACKER_ORDERING);
        let write = self.write.swap(0, IO_TRACKER_ORDERING);

        let then = mem::replace(&mut *when, Instant::now());

        (then, read, write)
    }
}
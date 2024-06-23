use log::debug;
use metrics::{describe_counter, describe_histogram, histogram, Histogram};
use std::mem;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

pub const METRIC_DISK_IO_OPERATION: &str = "disk_io_operation";
pub const METRIC_DISK_IO_OPERATION_DURATION: &str = "disk_io_operation_duration";
pub const METRIC_ERASURE_ENCODING_DURATION: &str = "erasure_encode_duration";

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

pub fn measure(histogram: Histogram, f: fn()) {
    let start = Instant::now();
    f();
    let duration = start.elapsed();

    histogram.record(duration.as_micros() as f64)
}


pub fn describe_metrics() {
    describe_counter!(METRIC_DISK_IO_OPERATION, "Disk I/O Operation Counter");

    // Histograms
    describe_histogram!(
        METRIC_ERASURE_ENCODING_DURATION,
        "Erasure Encoding Duration"
    );
}

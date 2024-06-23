pub mod cache;
// pub mod flush;

use rand::Rng;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::Duration;
use std::{mem, thread};

pub trait WorkerTask: Clone + Sync + Send {
    fn pre(&self) {}
    fn post(&self) {}
    // TODO give execute a mutable reference
    fn execute(&self);
}

#[derive(Clone, Debug)]
pub struct WorkerThread<T: WorkerTask + Sized> {
    run: Arc<AtomicBool>,
    join_handle: Arc<Mutex<Option<JoinHandle<()>>>>,
    interval: Duration,
    task: T,
}
impl<T: WorkerTask + Sized + 'static> WorkerThread<T> {
    pub fn new(task: T) -> Self {
        Self {
            run: Arc::new(AtomicBool::new(false)),
            join_handle: Arc::new(Mutex::new(None)),
            interval: Duration::from_secs(1),
            task,
        }
    }
    pub fn interval(self, interval: Duration) -> Self {
        Self { interval, ..self }
    }
    pub fn spawn(&self) {
        // set the status to run
        self.run.store(true, Ordering::Relaxed);

        let (a, b) = split_duration(self.interval);
        let local_run = self.run.clone();
        let local_task = self.task.clone();
        let handle = thread::spawn(move || {
            let task = local_task;

            task.pre();
            // TODO support a rapid shutdown, where the run state is checked every few ms
            while local_run.load(Ordering::Relaxed) {
                thread::sleep(a);
                task.execute();
                thread::sleep(b);
            }

            task.post();
        });

        // store the JoinHandle for the thread
        let mut h = self.join_handle.lock().unwrap();
        *h = Some(handle);
    }

    fn stop(&self) {
        self.run.store(false, Ordering::Relaxed);
    }

    fn stop_wait(&self) {
        let mut handle = self.join_handle.lock().unwrap();
        if handle.is_none() {
            return;
        }

        self.run.store(false, Ordering::Relaxed);

        let mut new_handle = None;
        mem::swap(&mut *handle, &mut new_handle);

        let binding = new_handle.unwrap();
        let _ = binding.join();
    }
}

pub(crate) fn split_duration(duration: Duration) -> (Duration, Duration) {
    let total_secs = duration.as_millis() as u64;
    let mut rng = rand::thread_rng();
    let part1_secs = rng.gen_range(0..=total_secs);
    let part2_secs = total_secs - part1_secs;
    (
        Duration::from_millis(part1_secs),
        Duration::from_millis(part2_secs),
    )
}

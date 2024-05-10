use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;

#[derive(Debug)]
pub struct Kernel {
    run: Arc<Mutex<bool>>,

    thread_handles: Arc<Mutex<Vec<JoinHandle<()>>>>,
}
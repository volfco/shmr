use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};
use log::{trace, warn};
use crossbeam_channel::{Sender, Receiver, RecvTimeoutError};

// Our BufferManager struct
#[derive(Clone)]
pub struct SlicePool {
    size: usize,
    clean_rx: Arc<Receiver<Vec<u8>>>,
    dirty_tx: Arc<Sender<(Vec<u8>, usize)>>
}

impl SlicePool {

    pub fn new(size: usize, count: usize) -> Self {

        let (clean_tx, clean_rx) = crossbeam_channel::bounded(count);
        let (dirty_tx, dirty_rx) = crossbeam_channel::bounded(count);

        let thread_clean_tx = clean_tx.clone();
        thread::spawn(move || zero_worker(dirty_rx, thread_clean_tx));

        // Create the buffers and push them into the channel
        let s = Instant::now();
        for _ in 0..count {
            dirty_tx.send((vec![], size)).unwrap();
        }

        trace!("BufferManager initialized with {} buffers of size {} bytes. took {:?}", count, size, s.elapsed());

        SlicePool {
            size,
            clean_rx: Arc::new(clean_rx),
            dirty_tx: Arc::new(dirty_tx)
        }

    }

    pub fn stats(&self) -> (usize, usize) {
        (self.clean_rx.len(), self.dirty_tx.len())
    }

    pub fn recv(&self, timeout: Duration) -> Result<Vec<u8>, RecvTimeoutError> {
        if self.clean_rx.is_empty() {
            warn!("No buffers available, creating a new one");
            self.dirty_tx.send((vec![], self.size)).unwrap();
            thread::sleep(Duration::from_micros(500));
        }
        trace!("Waiting {:?} for buffer", &timeout);
        self.clean_rx.recv_timeout(timeout)
    }

    pub fn rtn(&self, buf: Vec<u8>) -> Result<(), ()> {
        // TODO Fix error return type
        self.dirty_tx.send((buf, self.size)).map_err(|_| ())
    }

}

fn zero_worker(dirty_rx: Receiver<(Vec<u8>, usize)>, clean_tx: Sender<Vec<u8>>)
{
    loop {
        let mut buffer = dirty_rx.recv().unwrap();

        for i in 0..buffer.1 {
            if i >= buffer.0.len() {
                buffer.0.push(Default::default());
            } else {
                buffer.0[i] = Default::default();
            }
        }

        clean_tx.send(buffer.0).unwrap();
    }
}

//
// #[derive(Debug)]
// pub struct Buffer<T> {
//     buf_size: usize,
//     sender: Sender<Buffer<T>>,
//     inner: Vec<T>
// }
// impl<T> Buffer<T> {
//     pub fn get_mut(&mut self) -> &mut Vec<T> {
//         &mut self.inner
//     }
// }
//
// impl<T> Drop for Buffer<T> {
//     fn drop(&mut self) {
//         if self.sender.is_full() {
//             debug!("Sender is full, dropping buffer");
//             return;
//         }
//
//         debug!("Dropping buffer");
//         let mut inner = vec![];
//         std::mem::swap(&mut self.inner, &mut inner);
//
//         self.sender.send(Buffer {
//             sender: self.sender.clone(),
//             inner,
//             buf_size: self.buf_size
//         }).unwrap();
//     }
// }


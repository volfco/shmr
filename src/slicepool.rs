use std::sync::Arc;
use std::sync::mpsc::{Receiver, Sender};


// Our BufferManager struct
#[derive(Clone)]
pub struct BufferManager<T> {
    buffers: Arc<Receiver<Buffer<T>>>,
    rx: Arc<Sender<Buffer<T>>>
}

impl<T> BufferManager<T>
where
    T: Send + Default
{

    pub fn new(buf_size: usize, count: usize) -> Self {

        let (tx, rx) = std::sync::mpsc::channel();

        // Create the buffers and push them into the channel
        for _ in 0..count {
            tx.send(Buffer {
                sender: tx.clone(),
                inner: Vec::with_capacity(buf_size)
            }).unwrap();
        }

        BufferManager {
            buffers: Arc::new(rx),
            rx: Arc::new(tx)
        }

    }

    pub fn request(&self) -> Buffer<T> {
        self.buffers.recv().unwrap()
    }

}


pub struct Buffer<T> {
    sender: Sender<Buffer<T>>,
    inner: Vec<T>
}
impl<T> Buffer<T> {
    pub fn get_mut(&mut self) -> &mut Vec<T> {
        &mut self.inner
    }
}

impl<T> Drop for Buffer<T> {
    fn drop(&mut self) {
        let mut inner = vec![];
        std::mem::swap(&mut self.inner, &mut inner);

        self.sender.send(Buffer {
            sender: self.sender.clone(),
            inner
        }).unwrap();
    }
}
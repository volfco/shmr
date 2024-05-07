use std::{cmp, thread};
use std::collections::BTreeMap;
use std::fs::File;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use log::{debug, trace};
use crate::ShmrError;
use crate::storage::{IOEngine, StorageBlock};
use crate::vpf::VirtualPathBuf;

const FLUSH_INTERVAL: u64 = 500; // in ms

/// Provides a buffer for a StorageBlock, if enabled. Passes through I/O operations if not. 
/// 
/// This could be combined with StorageBlock down the road, but for now we can leave it as is. 
/// 
/// TODO Does it make sense to have this object store a reference to the I/O engine? We need it for flushing the buffer
#[derive(Clone, Debug)]
struct StorageBlockBuffer {
  /// Underlying StorageBlock
  // TOOD Rename to Inner
  topology: StorageBlock,

  /// Buffer Enabled?
  buffered: bool,

  /// Vec of Writes. Writes are appended to this list, so the order of writes is preserved.
  io_buf: Arc<Mutex<Vec<(usize, Vec<u8>)>>>,

  run_state: Arc<Mutex<bool>>,

  /// JoinHandle of the Background Thread
  worker_handle: Arc<Mutex<Option<JoinHandle<()>>>>,
}
impl StorageBlockBuffer {
  pub fn new(block: StorageBlock, buffer: bool) {
    let s = Self {
      topology: block,
      buffered: buffer,
      io_buf: Arc::new(Mutex::new(Vec::new())),
      run_state: Arc::new(Mutex::new(true)),
      worker_handle: Arc::new(Mutex::new(None))
    };

    let interval = Duration::from_millis(FLUSH_INTERVAL);
    let thread_self = s.clone();

    let thread_handle = thread::spawn(move || {
        loop {
            {
                let run = thread_self.run_state.lock().unwrap();
                if !*run {
                    break;
                }
            }

            thread::sleep(interval);

            {
                // this will lock the io_buf, which will prevent new writes from being accepted until this finishes
                thread_self.flush_buffer();
            }
        }
        debug!("bg_thread has exited");
    });

    {
        let mut flush_lock = s.flusher.lock().unwrap();
        *flush_lock = Some(thread_handle);
    }

    s
  }

  pub fn read(&self, engine: &IOEngine, offset: usize, buf: &mut [u8]) -> Result<usize, ShmrError> {
    // even if we're buffered, we still need to do the initial read from the StorageBlock
    let read_amt = self.topology.read(engine, offset, buf)?;

    if self.buffered {
      let mut io_buf = self.io_buf.lock().unwrap();
      write_into(buf, &mut io_buf, offset);

    }
    Ok(read_amt)
  }

  fn write(&self, engine: &IOEngine, offset: usize, buf: &[u8]) -> Result<usize, ShmrError> {
    if !self.buffered {
      debug!("unbuffered write, passing through");
      self.topology.write(engine, offset, buf)
    } else {
      let mut handle = self.io_buf.lock().unwrap();
      let write = buf.len();
      handle.push((offset, buf.to_vec()));
      Ok(write)
    }
  }

  fn flush_buffer(&self, engine: &IOEngine) -> Result<(), ShmrError> {
    let mut handle = self.io_buf.lock().unwrap();

    let mut buf: Vec<u8> = vec![MaybeUninit::uninit(); self.topology.size()];
    // read the on-disk contents into the buffer
    self.topology.read(engine, 0, buf.as_mut_slice())?;

    write_into(&mut buf, &mut handle, 0);

    // and write em back!
    self.topology.write(engine, 0, buf.as_slice())?;

    Ok(())
  }

}

fn write_into(buf: &mut [u8], writes: &mut Vec<(usize, Vec<u8>)>, offset: usize) {
  let write_buf_len = buf.len();
  writes.iter()
      .filter(|(start, contents)| offset <= start + contents.len())
      .for_each(|(start, contents)| {
          let content_start = offset.saturating_sub(*start);
          let content_end = cmp::min(contents.len(), write_buf_len);
          let buf_start = start.saturating_sub(offset);
          let buf_end = buf_start + content_end - content_start;
          buf[buf_start..buf_end].copy_from_slice(&contents[content_start..content_end]);
      });
}
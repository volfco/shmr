use std::cmp;
use std::collections::BTreeMap;
use std::fs::File;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use chashmap::CHashMap;
use log::{debug, trace};
use crate::ShmrError;
use crate::storage::{IOEngine, StorageBlock};
use crate::vpf::VirtualPathBuf;

/// Interval at which the IO buffer is flushed.
const IO_BUFFER_FLUSH_INTERVAL: Duration = Duration::from_secs(5);
/// Maximum size of the IO buffer before it is flushed.
const IO_BUFFER_FLUSH_SIZE: usize = 1024 * 1024; // 1MB

// we can just have two threads for flushing. One to check time, and one to check size.

struct StorageBlock2 {
  topology: StorageBlock,

  /// Should writes be buffered?
  /// For Erasure Coded blocks, this is assumed true.
  buffered: bool,
  /// Vec of Writes. Writes are appended to this list, so the order of writes is preserved.
  io_buf: Arc<Mutex<Vec<(usize, Vec<u8>)>>>,
}
impl StorageBlock2 {

  fn read(&self, engine: &IOEngine, offset: usize, buf: &mut [u8]) -> Result<usize, ShmrError> {
    // either way, we need to do the initial read from the StorageBlock
    self.topology.read(engine, offset, buf)?;

    if self.buffered {
      let mut io_buf = self.io_buf.lock().unwrap();
      // how do we tell write_into about the offset?


    }
    todo!()

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

  fn merge(&mut self, engine: &IOEngine) -> Result<(), ShmrError> {
    let mut handle = self.io_buf.lock().unwrap();

    let mut buf: Vec<u8> = vec![0; self.topology.size()];
    // read the on-disk contents into the buffer
    self.topology.read(engine, 0, buf.as_mut_slice())?;

    // write_into(&mut buf, &mut handle);

    // and write em back!
    self.topology.write(engine, 0, buf.as_slice())?;

    Ok(())
  }

}

// fn write_into(buf: &mut Vec<u8>, writes: &mut Vec<(usize, Vec<u8>)>, offset: usize) {
//   for (start, contents) in writes.drain( .. ) {
//     let end = start + contents.len();
//     if start <= offset && end >= offset {
//       let remainder = cmp::min(offset - start, 0);
//       buf[start..(end - remainder)].copy_from_slice(&contents[remainder..(end - start)]);
//     }
//   }
// }

fn write_into(buf: &mut Vec<u8>, writes: &mut Vec<(usize, Vec<u8>)>, offset: usize) {
  for (start, mut contents) in writes.drain(..) {
    if contents.len() > buf.len() - start {
      contents.truncate(buf.len() - start);
    }

    if let Some(sub_buf) = buf.get_mut(start..start + contents.len()) {
      sub_buf.copy_from_slice(&contents);
    }
  }
}

// fn write_into(buf: &mut Vec<u8>, writes: &mut Vec<(usize, Vec<u8>)>, offset: usize) {
//   for (start, contents) in writes.drain(..) {
//     let end = start + contents.len();
//     // Ensure the data fits within the existing buffer size from the offset
//     if start >= offset && end <= buf.len() {
//       let remainder = cmp::min(offset - start, 0);
//       let target_start = start - offset;
//       let target_end = end - offset;
//       buf[target_start..target_end].copy_from_slice(&contents[remainder..]);
//     }
//   }
// }
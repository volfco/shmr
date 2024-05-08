use std::cmp;

#[derive(Debug)]
struct StorageBlock {
  contents: Vec<u8>,

  /// start, size, contents
  io_buf: Vec<(usize, Vec<u8>)>,
}
impl StorageBlock {
  fn write(&mut self, offset: usize, buf: &[u8]) {
    let entry = (offset, buf.to_vec());
    self.io_buf.push(entry);
  }

  fn read(&mut self, offset: usize, buf: &mut [u8]) {

    let buf_size = buf.len();
    buf[0..buf_size].copy_from_slice(&self.contents[offset..offset+buf_size]);

    write_into(buf, &mut self.io_buf, offset);
  }

  fn merge(&mut self) {
    for (start, contents) in self.io_buf.drain(..) {
      self.contents[start..start+contents.len()].copy_from_slice(&contents);
    }
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

fn main() {

}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn test_storage_block() {
    let mut block = StorageBlock {
      contents: vec![0; 32],
      io_buf: Vec::new(),
    };

    block.write(0, &[1, 1, 1, 1]);
    // let mut buf = [2; 10];
    // block.read(0, &mut buf);
    // assert_eq!(buf, [1, 1, 1, 1, 0, 0, 0, 0, 0, 0]);
    //
    block.write(3, &[10, 10, 10, 10]);
    // let mut buf = [2; 10];
    // block.read(0, &mut buf);
    // assert_eq!(buf, [1, 1, 1, 10, 10, 10, 10, 0, 0, 0]);
    //
    block.write(1, &[2]);
    block.write(4, &[5]);
    // let mut buf = [2; 10];
    // block.read(0, &mut buf);
    // assert_eq!(buf, [1, 2, 1, 10, 5, 10, 10, 0, 0, 0]);

    let mut buf = [2; 6];
    block.read(4, &mut buf);
    assert_eq!(buf, [5, 10, 10, 0, 0, 0]);

    block.write(31, &[1]);
    block.merge();
    block.write(31, &[0]);
    assert_eq!(block.contents, [1, 2, 1, 10, 5, 10, 10, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1]);
  }
}
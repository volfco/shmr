pub struct InMemoryFile {
  data: Vec<Vec<u8>>,
  cursor: usize,
  chunk_size: usize,
  total_size: usize,
}

impl InMemoryFile {
  pub fn new(chunk_size: usize) -> InMemoryFile {
    InMemoryFile {
      data: Vec::new(),
      cursor: 0,
      chunk_size,
      total_size: 0,
    }
  }

  pub fn write(&mut self, buf: &[u8]) {
    let mut remaining = buf;
    while !remaining.is_empty() {
      if self.data.is_empty() || self.data.last().unwrap().len() == self.chunk_size {
        self.data.push(Vec::new());
      }

      let last_chunk = self.data.last_mut().unwrap();
      let available_space = self.chunk_size - last_chunk.len();
      let write_size = std::cmp::min(available_space, remaining.len());

      &last_chunk.extend(&remaining[..write_size]);
      remaining = &remaining[write_size..];
    }

    self.total_size += buf.len();
  }

  pub fn read(&mut self, buf: &mut [u8]) -> usize {
    let mut total_read = 0;
    let mut remaining = buf;

    while self.cursor < self.total_size && !remaining.is_empty() {
      let chunk_index = self.cursor / self.chunk_size;
      let cursor_in_chunk = self.cursor % self.chunk_size;

      let chunk = &self.data[chunk_index];
      let bytes_to_read = std::cmp::min(chunk.len() - cursor_in_chunk, remaining.len());

      &remaining[..bytes_to_read].copy_from_slice(&chunk[cursor_in_chunk..cursor_in_chunk+bytes_to_read]);
      remaining = &mut remaining[bytes_to_read..];
      total_read += bytes_to_read;
      self.cursor += bytes_to_read;
    }

    total_read
  }

  pub fn truncate(&mut self, size: usize) {
    if size >= self.total_size {
      return;
    }

    self.total_size = size;
    let chunks_needed = (size as f64 / self.chunk_size as f64).ceil() as usize;
    self.data.truncate(chunks_needed);

    // Truncate the last chunk further if needed.
    let last_chunk_extra = self.total_size % self.chunk_size;
    if last_chunk_extra != 0 {
      self.data.last_mut().unwrap().truncate(last_chunk_extra);
    }

    if self.cursor > self.total_size {
      self.cursor = self.total_size;
    }
  }

  pub fn size(&self) -> usize {
    self.total_size
  }

  pub fn reset_cursor(&mut self) {
    self.cursor = 0;
  }
}

fn main() {
  const CHUNK_SIZE: usize = 128;
  let mut file = InMemoryFile::new(CHUNK_SIZE);

  let data = "Hello, world!";  // It's only 13 bytes long.
  file.write(data.as_bytes());

  // Check the size after writing.
  println!("Size: {} bytes", file.size()); // Output: Size: 13 bytes

  let mut buf = [0u8; 5];
  file.read(&mut buf);
  file.reset_cursor();
  file.read(&mut buf);
  println!("Read: {}", std::str::from_utf8(&buf).unwrap()); // Output: Read: Hello

  // Let's generate some big data to write in order to fill multiple chunks.
  let big_data = std::iter::repeat(b'a').take(CHUNK_SIZE * 2 + 10).collect::<Vec<_>>();
  file.write(&big_data);
  println!("Size: {} bytes", file.size()); // Output: Size: 266 bytes

  file.truncate(CHUNK_SIZE * 2 + 5);
  println!("Size after truncation: {} bytes", file.size()); // Output: Size: 261 bytes
}
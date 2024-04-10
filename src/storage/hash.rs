use std::collections::HashMap;
use std::fs::File;
use std::hash::Hasher;
use std::io::{BufReader, Read};
use std::path::PathBuf;
use crate::vpf::VirtualPathBuf;
use seahash::SeaHasher;
use anyhow::Result;

pub fn compare(pool_map: &HashMap<String, PathBuf>, stuff: &[VirtualPathBuf]) -> bool {

  let mut hash = 0;
  for path in stuff {
    let file_path = path.resolve_path(pool_map).unwrap();

    // if hash is zero, we shouldn't compare it and just set it to the current file's hash
    if hash == 0 {
      hash = hash_file(&file_path).unwrap();
      continue;
    }

    // if the hash doesn't match, return false
    if hash != hash_file(&file_path).unwrap() {
      return false;
    }
  }

  true
}

fn hash_file(path: &PathBuf) -> Result<u64>{
  let mut hasher = SeaHasher::new();

  let input = File::open(path)?;
  let mut reader = BufReader::new(input);

  let mut buffer = [0; 1024];

  loop {
    let count = reader.read(&mut buffer)?;
    if count == 0 {
      break;
    }
    hasher.write(&buffer[..count]);
  }

  Ok(hasher.finish())
}
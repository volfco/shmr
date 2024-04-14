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

#[cfg(test)]
mod tests {
  use std::path::Path;
  use env_logger::init;
  use crate::random_string;
  use super::*;

  #[test]
  fn compare_identical_files() {
    init();

    let temp_dir = Path::new("/tmp");
    let filename1 = random_string();
    let filename2 = random_string();

    let mut pool_map: HashMap<String, PathBuf> = HashMap::new();
    pool_map.insert("test_pool".to_string(), temp_dir.to_path_buf());

    let paths = vec![
      VirtualPathBuf{ pool: "test_pool".to_string(), filename: filename1 },
      VirtualPathBuf{ pool: "test_pool".to_string(), filename: filename2 },
    ];

    let contents = random_string();
    let buf = contents.as_bytes();

    paths[0].create(&pool_map).unwrap();
    paths[0].write(&pool_map, 0, buf).unwrap();
    paths[1].create(&pool_map).unwrap();
    paths[1].write(&pool_map, 0, buf).unwrap();

    assert_eq!(compare(&pool_map, &paths), true);
  }

  #[test]
  fn compare_different_files() {
    let temp_dir = Path::new("/tmp");
    let filename1 = random_string();
    let filename2 = random_string();

    let mut pool_map: HashMap<String, PathBuf> = HashMap::new();
    pool_map.insert("test_pool".to_string(), temp_dir.to_path_buf());

    let paths = vec![
      VirtualPathBuf{ pool: "test_pool".to_string(), filename: filename1 },
      VirtualPathBuf{ pool: "test_pool".to_string(), filename: filename2 },
    ];

    let contents1 = random_string();
    let buf1 = contents1.as_bytes();
    let contents2 = random_string();
    let buf2 = contents2.as_bytes();

    paths[0].create(&pool_map).unwrap();
    paths[0].write(&pool_map, 0, buf1).unwrap();

    paths[1].create(&pool_map).unwrap();
    paths[1].write(&pool_map, 0, buf2).unwrap();

    assert_eq!(compare(&pool_map, &paths), false);
  }

}
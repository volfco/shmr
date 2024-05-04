use crate::storage::PoolMap;
use crate::vpf::VirtualPathBuf;
use anyhow::Result;
use seahash::SeaHasher;
use std::fs::File;
use std::hash::Hasher;
use std::io::{BufReader, Read};
use std::path::PathBuf;

pub fn compare(pool_map: &PoolMap, stuff: &[VirtualPathBuf]) -> bool {
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

fn hash_file(path: &PathBuf) -> Result<u64> {
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
    use super::*;
    use crate::random_string;
    use crate::storage::Engine;
    use crate::tests::get_pool;

    #[test]
    fn compare_identical_files() {
        let filename1 = random_string();
        let filename2 = random_string();

        let engine: Engine = Engine::new("test_pool".to_string(), get_pool());

        let paths = vec![
            VirtualPathBuf {
                pool: "test_pool".to_string(),
                bucket: "bucket1".to_string(),
                filename: filename1,
            },
            VirtualPathBuf {
                pool: "test_pool".to_string(),
                bucket: "bucket1".to_string(),
                filename: filename2,
            },
        ];

        let contents = random_string();
        let buf = contents.as_bytes();

        engine.create(&paths[0]).unwrap();
        engine.write(&paths[0], 0, buf).unwrap();

        engine.create(&paths[1]).unwrap();
        engine.write(&paths[1], 0, buf).unwrap();

        assert_eq!(compare(&engine.pools, &paths), true);
    }

    #[test]
    fn compare_different_files() {
        let filename1 = random_string();
        let filename2 = random_string();

        let engine: Engine = Engine::new("test_pool".to_string(), get_pool());

        let paths = vec![
            VirtualPathBuf {
                pool: "test_pool".to_string(),
                bucket: "bucket1".to_string(),
                filename: filename1,
            },
            VirtualPathBuf {
                pool: "test_pool".to_string(),
                bucket: "bucket1".to_string(),
                filename: filename2,
            },
        ];

        let contents1 = random_string();
        let buf1 = contents1.as_bytes();
        let contents2 = random_string();
        let buf2 = contents2.as_bytes();

        engine.create(&paths[0]).unwrap();
        engine.write(&paths[0], 0, buf1).unwrap();

        engine.create(&paths[1]).unwrap();
        engine.write(&paths[1], 0, buf2).unwrap();

        assert_eq!(compare(&engine.pools, &paths), false);
    }
}

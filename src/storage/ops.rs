use crate::storage::StorageBlock;
use anyhow::Result;
use log::debug;
use std::collections::HashMap;
use std::path::PathBuf;

/// Replaces the given StorageBlock with one that is Erasure Encoded.
pub fn replace_with_ec(
    old: &StorageBlock,
    pool_map: &HashMap<String, PathBuf>,
    new_topology: (u8, u8),
) -> Result<StorageBlock> {
    let mut contents = vec![];
    let m = old.read(pool_map, 0, &mut contents)?;

    assert!(m > 0, "read 0 bytes from storageblock");

    let new_block = StorageBlock::init_ec(pool_map, new_topology, m);
    new_block.create(pool_map)?;

    debug!("successfully created new storageblock");
    new_block.write(pool_map, 0, &contents)?;

    Ok(new_block)
}

#[cfg(test)]
mod tests {
    use crate::storage::ops::replace_with_ec;
    use crate::storage::StorageBlock;
    use crate::vpf::VirtualPathBuf;
    use crate::{random_data, random_string};
    use log::info;
    use std::collections::HashMap;
    use std::path::{Path, PathBuf};

    #[test]
    fn rewrite_single_storageblock_to_ec() {
        let temp_dir = Path::new("/tmp");
        let mut pool_map: HashMap<String, PathBuf> = HashMap::new();
        pool_map.insert("test_pool".to_string(), temp_dir.to_path_buf());
        let filename1 = random_string();

        let single_sb = StorageBlock::Single(VirtualPathBuf {
            pool: "test_pool".to_string(),
            filename: filename1.clone(),
        });
        let create = single_sb.create(&pool_map);
        assert!(create.is_ok());

        let single_buf = random_data(1024 * 1024 * 4);

        info!("generated {} bytes of data", single_buf.len());

        let write = single_sb.write(&pool_map, 0, &single_buf);
        assert!(write.is_ok());

        let new_sb = replace_with_ec(&single_sb, &pool_map, (3, 2)).unwrap();

        // read the new block and compare it to the original
        let mut new_buf = vec![];
        let read = new_sb.read(&pool_map, 0, &mut new_buf);
        assert!(read.is_ok());
        assert_eq!(read.unwrap(), single_buf.len());

        assert_eq!(single_buf.len(), new_buf.len());
    }
}
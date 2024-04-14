use std::collections::HashMap;
use std::path::PathBuf;
use crate::storage::StorageBlock;
use anyhow::Result;
use log::debug;

/// Replaces the given StorageBlock with one that is Erasure Encoded.
pub fn replace_with_ec(old: &StorageBlock, pool_map: &HashMap<String, PathBuf>, new_topology: (u8, u8), new_size: Option<usize>) -> Result<StorageBlock> {
  let mut contents = vec![];
  old.read(pool_map, 0, &mut contents)?;

  let shard_size = if let Some(size) = new_size {
    size
  } else {
    contents.len() / new_topology.0 as usize
  };

  debug!("selected shard size: {}", shard_size);

  let new_block = StorageBlock::init_ec(pool_map, new_topology, shard_size);
  new_block.write(pool_map, 0, &contents)?;

  Ok(new_block)
}

#[cfg(test)]
mod tests {
  use std::collections::HashMap;
  use std::path::{Path, PathBuf};
  use crate::{random_data, random_string};
  use crate::storage::ops::replace_with_ec;
  use crate::storage::StorageBlock;
  use crate::vpf::VirtualPathBuf;

  // #[test]
  // fn rewrite_single_storageblock_to_ec() {
  //   let temp_dir = Path::new("/tmp");
  //   let mut pool_map: HashMap<String, PathBuf> = HashMap::new();
  //   pool_map.insert("test_pool".to_string(), temp_dir.to_path_buf());
  //   let filename1 = random_string();
  //
  //   let single_sb = StorageBlock::Single(VirtualPathBuf{pool: "test_pool".to_string(), filename: filename1.clone()});
  //   let create = single_sb.create(&pool_map);
  //   assert!(create.is_ok());
  //   //
  //   // let single_buf = random_data(1024 * 1024 * 4);
  //   //
  //   // let write = single_sb.write(&pool_map, 0, &single_buf);
  //   // assert!(write.is_ok());
  //   //
  //   // let new_sb = replace_with_ec(&single_sb, &pool_map, (3, 2), None).unwrap();
  //   //
  //   //
  //
  // }

}
use std::collections::HashMap;
use std::path::PathBuf;
use anyhow::Result;
use log::{debug, error, trace, warn};
use reed_solomon_erasure::galois_8::ReedSolomon;
use crate::vpf::VirtualPathBuf;

pub fn read_ec_shards(r: &ReedSolomon, pool_map: &HashMap<String, PathBuf>, shards: &[VirtualPathBuf], shard_size: &usize) -> Result<Vec<Vec<u8>>> {
  // we can assume that the shards are in the correct order, as they were created in the correct order... in theory
  // We can just read the data from the shards, and then decode it
  let mut shards: Vec<Option<Vec<u8>>> = shards.iter().map(|path| {
    let mut buffer = vec![0; *shard_size];
    if let Err(e) = path.read(pool_map, 0, &mut buffer) {
      error!("Error opening file: {:?}", e);
      return None;
    }

    // if the buffer is still empty, then we are reading an empty file. We need to return a buffer
    // of the correct size so things become easier elsewhere. Well, just the initial write to an
    // empty file.
    if buffer.is_empty() {
      return None;
    }

    Some(buffer)
  }).collect();

  if shards.iter().take(r.data_shard_count()).all(|x| x.is_some()) {
    trace!("All data shards are intact. No need to reconstruct.");
  } else {
    warn!("missing data shards. Attempting to reconstruct.");

    let start_time = std::time::Instant::now();
    r.reconstruct(&mut shards).unwrap();

    debug!("Reconstruction complete. took {:?}", start_time.elapsed());

    // TODO Do something with the reconstructed data, so we don't need to reconstruct this block again
  }

  let mut buffer = vec![];

  for shard in shards {
    buffer.push(shard.unwrap_or(vec![0; *shard_size]));
  }

  Ok(buffer)
}

pub fn write_ec_shards(r: &ReedSolomon, pool_map: &HashMap<String, PathBuf>, shards: &[VirtualPathBuf], ec_shards: Vec<Vec<u8>>) -> Result<()> {
  // ensure the number of on disk shards is the same as the number of ec_shards
  assert_eq!(shards.len(), ec_shards.len());

  let mut ec_shards = ec_shards;

  // assume that the data has not already been encoded and that we should do it now
  r.encode(&mut ec_shards).unwrap();

  for (i, shard) in shards.iter().enumerate() {
    shard.write(pool_map, 0,&ec_shards[i])?;
  }

  Ok(())
}

pub fn update_ec_shards(shards: &mut [Vec<u8>], offset: usize, buf: &[u8]) -> Result<u8> {
  let mut shards_updated: u8 = 0;
  let mut remaining_offset: usize = offset;
  let mut data_index = 0;

  if buf.len() > shards[0].len() * shards.len() {
    return Err(anyhow::anyhow!("Data is too large for all the shards"));
  }

  for (i, shard) in shards.iter_mut().enumerate() {
    let sl = shard.len();
    // if the remaining offset is greater than the length of the shard, trim the offset and continue
    // to the next shard
    if remaining_offset > sl {
      remaining_offset -= sl;
      continue
    }

    if data_index >= buf.len() {
      break;
    }

    shards_updated += 1;

    let s_start = remaining_offset; // where we should start writing to in this shard
    let s_end = sl; // where we should end writing to in this shard. initially, this is the end of the shard

    let buf_start = data_index; // where we should start reading from in the buffer
    let buf_end = {
      let bl = buf.len();
      // where we should end reading from in the buffer
      if bl - data_index > s_end - s_start {
        data_index + (s_end - s_start)
      } else {
        bl
      }
    };

    let s_end = s_start + (buf_end - buf_start); // how much data are we writing into this shard

    trace!("Shard {}: Writing from {} to {} in shard, reading from {} to {} in buffer", i, s_start, s_end, buf_start, buf_end);

    let _ = &shard[s_start..s_end].copy_from_slice(&buf[buf_start..buf_end]);


    remaining_offset = 0;
    data_index = buf_end;
  }

  Ok(shards_updated)
}

#[cfg(test)]
mod tests {
  use crate::storage::erasure::update_ec_shards;

  #[test]
  fn test_update_ec_shards() {
    // Given:
    // A 5x5 Vec
    let mut shards: Vec<Vec<u8>> = vec![
      vec![0, 0, 0, 0, 0],
      vec![0, 0, 0, 0, 0],
      vec![0, 0, 0, 0, 0],
      vec![0, 0, 0, 0, 0],
      vec![0, 0, 0, 0, 0],
    ];

    // 8 new values to apply
    let update: Vec<u8> = vec![1, 2, 2];

    // When:
    // We update the ec shards
    let upd = update_ec_shards(&mut shards, 3, &update);

    // ensure that only two shards were updated
    assert_eq!(upd.unwrap(), 2);

    // Then:
    let expected_shards: Vec<Vec<u8>> = vec![
      vec![0, 0, 0, 1, 2],
      vec![2, 0, 0, 0, 0],
      vec![0, 0, 0, 0, 0],
      vec![0, 0, 0, 0, 0],
      vec![0, 0, 0, 0, 0],
    ];

    assert_eq!(shards, expected_shards);
  }

  #[test]
  fn test_invalid_update_ec_shards() {
    // Given:
    // A 5x5 Vec
    let mut shards: Vec<Vec<u8>> = vec![
      vec![0, 0, 0, 0, 0],
      vec![0, 0, 0, 0, 0],
      vec![0, 0, 0, 0, 0],
      vec![0, 0, 0, 0, 0],
      vec![0, 0, 0, 0, 0],
    ];

    // 8 new values to apply
    let update: Vec<u8> = vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0];

    // When:
    // We update the ec shards
    let upd = update_ec_shards(&mut shards, 3, &update);

    // ensure that only two shards were updated
    assert_eq!(upd.unwrap(), 5);
  }

  #[test]
  fn test_invalid_update_ec_shards2() {
    // Given:
    // A 5x5 Vec
    let mut shards: Vec<Vec<u8>> = vec![
      vec![0, 0, 0, 0, 0],
      vec![0, 0, 0, 0, 0],
      vec![0, 0, 0, 0, 0],
      vec![0, 0, 0, 0, 0],
      vec![0, 0, 0, 0, 0],
    ];

    // 8 new values to apply
    let update: Vec<u8> = vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0];

    // When:
    // We update the ec shards
    let upd = update_ec_shards(&mut shards, 3, &update);

    // ensure that only two shards were updated
    assert_eq!(upd.is_err(), true);
  }
}
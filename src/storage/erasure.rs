use std::collections::HashMap;
use std::path::PathBuf;
use std::time::Instant;
use anyhow::Result;
use log::{debug, error, trace, warn};
use reed_solomon_erasure::galois_8::ReedSolomon;
use crate::vpf::VirtualPathBuf;

/// Read the given sets of shards, reconstruct the data if applicable, and return the encoded data
pub fn read(r: &ReedSolomon, pool_map: &HashMap<String, PathBuf>, shards: &[VirtualPathBuf], shard_size: usize) -> Result<Vec<u8>> {

  let mut shards: Vec<Option<Vec<u8>>> = shards.iter().map(|path| {
    let mut buffer = vec![];
    if let Err(e) = path.read(pool_map, 0, &mut buffer) {
      error!("Error opening file: {:?}", e);
      return None;
    }

    // if the buffer is still empty, then we are reading an empty file. We need to return a buffer
    // of the correct size so things become easier elsewhere. Well, just the initial write to an
    // empty file.
    if buffer.is_empty() {
      buffer.resize(shard_size, 0);
    }

    Some(buffer)
  }).collect();

  if shards.iter().take(r.data_shard_count()).all(|x| x.is_some()) {
    trace!("All data shards are intact. No need to reconstruct.");
  } else {
    warn!("missing data shards. Attempting to reconstruct.");

    let start_time = Instant::now();
    r.reconstruct(&mut shards).unwrap();

    debug!("Reconstruction complete. took {:?}", start_time.elapsed());

    // TODO Do something with the reconstructed data, so we don't need to reconstruct this block again
  }

  let mut payload = vec![];

  for (shard_idx, shard) in shards.iter().enumerate() {
    if shard_idx == r.data_shard_count() {
      break;
    }

    let data = shard.clone().unwrap();
    payload.extend_from_slice(&data);
  }

  Ok(payload)
}

/// write the given data back to the given shards.
///
/// Assumes that the given data is only the data data, and no parity information. Parity information
/// is generated automatically
pub fn write(r: &ReedSolomon, pool_map: &HashMap<String, PathBuf>, shards: &[VirtualPathBuf], shard_size: usize, buf: Vec<u8>) -> Result<usize> {
  let mut data_shards = buf.chunks(shard_size).map(|x| {
    // pad with zeroes if it's not the right size
    // this is because the last shard might be a bit smaller than the rest
    let mut r = x.to_vec();
    if r.len() < shard_size {
      r.resize(shard_size, 0);
    }
    r
  }).collect::<Vec<_>>();

  for _ in 0..r.parity_shard_count() {
    data_shards.push(vec![0; shard_size]);
  }

  let start = Instant::now();

  r.encode(&mut data_shards).unwrap();

  let duration = start.elapsed();
  debug!("Encoding duration: {:?}", duration);

  let mut written = 0;

  for (shard_idx, shard) in shards.iter().enumerate() {
    written += shard.write(pool_map, 0, &data_shards[shard_idx])?;
  }

  Ok(written)

}

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
  if buf.len() > shards[0].len() * shards.len() {
    return Err(anyhow::anyhow!("Data is too large for all the shards"));
  }

  let shard_len = shards[0].len();
  let mut shard_pos = 0;
  let mut shard_idx = 0;

  if offset > 0 {
    shard_idx = offset / shard_len;
    shard_pos = offset % shard_len;
  }

  #[allow(clippy::needless_range_loop)]
  for i in 0..buf.len() {
    if shard_pos > shard_len - 1 {
      shard_pos = 0;
      shard_idx += 1;
    }

    shards[shard_idx][shard_pos] = buf[i];

    shard_pos += 1;
  }
  shard_idx += 1;

  Ok(shard_idx as u8)
}

#[cfg(test)]
mod tests {
  use log::info;
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
    // assert_eq!(upd.unwrap(), 2);

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

    info!("updated: {:?}", shards);

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
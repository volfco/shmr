use crate::vpf::VirtualPathBuf;
use anyhow::Result;
use log::{debug, error, trace, warn};
use reed_solomon_erasure::galois_8::ReedSolomon;
use std::time::Instant;
use crate::storage::PoolMap;

/// Read the given sets of shards, reconstruct the data if applicable, and return the encoded data
pub fn read(
    r: &ReedSolomon,
    pool_map: &PoolMap,
    shards: &[VirtualPathBuf],
    shard_size: usize,
) -> Result<Vec<u8>> {
    let mut shards = read_ec_shards(pool_map, shards, &shard_size);

    if shards
        .iter()
        .take(r.data_shard_count())
        .all(|x| x.is_some())
    {
        trace!("All data shards are intact. No need to reconstruct.");
    } else {
        warn!("missing data shards. Attempting to reconstruct.");

        let start_time = Instant::now();
        r.reconstruct(&mut shards).unwrap();

        debug!("Reconstruction complete. took {:?}", start_time.elapsed());

        // TODO Do something with the reconstructed data, so we don't need to reconstruct this block again
    }

    let shards: Vec<Vec<u8>> = shards.into_iter().map(|x| x.unwrap()).collect();

    let mut payload = vec![];

    for (shard_idx, shard) in shards.iter().enumerate() {
        if shard_idx == r.data_shard_count() {
            break;
        }

        payload.extend_from_slice(shard);
    }

    let ps = payload.len();
    let ds = r.data_shard_count() * shard_size;
    assert_eq!(
        ps, ds,
        "Payload size is not correct. Expected: {}, Got: {}",
        ds, ps
    );

    debug!("Read {} bytes from {} shards", payload.len(), shards.len());

    Ok(payload)
}

pub fn read_ec_shards(
    pool_map: &PoolMap,
    shards: &[VirtualPathBuf],
    shard_size: &usize,
) -> Vec<Option<Vec<u8>>> {
    shards
        .iter()
        .map(|path| {
            let mut buffer = vec![];
            if let Err(e) = path.read(pool_map, 0, &mut buffer) {
                error!("Error opening file: {:?}", e);
                return None;
            }

            // if the buffer is still empty, then we are reading an empty file. We need to return a buffer
            // of the correct size so things become easier elsewhere. Well, just the initial write to an
            // empty file.
            if buffer.is_empty() {
                buffer.resize(*shard_size, 0);
            }

            Some(buffer)
        })
        .collect()
}

/// write the given data back to the given shards.
///
/// Assumes that the given data is only the data data, and no parity information. Parity information
/// is generated automatically
pub fn write(
    r: &ReedSolomon,
    pool_map: &PoolMap,
    shards: &[VirtualPathBuf],
    shard_size: usize,
    buf: Vec<u8>,
) -> Result<usize> {
    let mut data_shards = buf
        .chunks(shard_size)
        .map(|x| {
            // pad with zeroes if it's not the right size
            // this is because the last shard might be a bit smaller than the rest
            let mut r = x.to_vec();
            if r.len() < shard_size {
                r.resize(shard_size, 0);
            }
            r
        })
        .collect::<Vec<_>>();

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
    use crate::storage::erasure::update_ec_shards;
    use log::info;

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
        let update: Vec<u8> = vec![
            1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9,
            0,
        ];

        // When:
        // We update the ec shards
        let upd = update_ec_shards(&mut shards, 3, &update);

        // ensure that only two shards were updated
        assert_eq!(upd.is_err(), true);
    }
}

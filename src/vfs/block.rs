use std::{cmp, thread};
use std::fs::{File, OpenOptions};
use std::io::Read;
use std::os::unix::prelude::FileExt;
use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};
use log::{debug, trace, warn};
use reed_solomon_erasure::galois_8::ReedSolomon;
use serde::{Deserialize, Serialize};
use uuid::Uuid;
use crate::ShmrError;
use crate::PoolMap;
use crate::vfs::path::{VirtualPath, VP_DEFAULT_FILE_EXT};

#[allow(clippy::identity_op)]
pub const VIRTUAL_BLOCK_DEFAULT_SIZE: usize = 1024 * 1024 * 1;
const VIRTUAL_BLOCK_FLUSH_INTERVAL: u64 = 500; // ms

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum BlockTopology {
    /// Single Shard
    Single,
    /// Mirrored Shards, with n mirrors
    Mirror(u8),
    /// Erasure Encoded. (Version, Data Shards, Parity Shards)
    Erasure(u8, u8, u8),
}

/// On read, the contents are read and stored in the buffer.
///
/// If the buffer is enabled, the buffer won't be flushed after ever write. Otherwise the buffer is
/// flushed after every write.
///
/// The VirtualBlock can be unloaded, which flushes and drops the buffer & file handles, to save
/// memory without dropping the entire object.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct VirtualBlock {
    pub(crate) uuid: Uuid,
    /// Size of the StorageBlock.
    /// This is a fixed size, and represents the maximum amount of data that can be stored in this
    /// block. On-disk size might be a bit larger (or smaller) than this value.
    size: usize,

    /// Layout of this StorageBlock
    topology: BlockTopology,

    /// Shards that make up this block.
    shards: Vec<VirtualPath>,

    /// File handles for each Shard. Stored in the same order as in shards
    #[serde(skip)]
    shard_handles: Arc<Mutex<Vec<File>>>,

    // stateful fields.
    #[serde(skip)]
    buffered: bool,

    #[serde(skip)]
    buffer_loaded: Arc<AtomicBool>,

    #[serde(skip)]
    buffer: Arc<Mutex<Vec<u8>>>,

    // none, because poolmap will default to a LazyLock that can be populated before the file is read from the database.
    // bit of a hack to give the poolmap a default value, but it works.
    #[serde(skip)]
    pool_map: Option<PoolMap>,

    #[serde(skip)]
    run: Arc<Mutex<bool>>,

    #[serde(skip)]
    run_handle: Arc<Mutex<Option<JoinHandle<()>>>>

    // TODO Add an IOTracker for the block as well?
}
impl Default for VirtualBlock {
    fn default() -> Self {
        VirtualBlock {
            uuid: Uuid::new_v4(),
            size: 0,
            topology: BlockTopology::Single,
            shards: Vec::new(),
            shard_handles: Arc::new(Mutex::new(Vec::new())),
            buffered: false,
            buffer_loaded: Arc::new(AtomicBool::new(false)),
            buffer: Arc::new(Mutex::new(Vec::new())),
            pool_map: None,
            run: Arc::new(Mutex::new(true)),
            run_handle: Arc::new(Mutex::new(None)),
        }
    }
}
impl VirtualBlock {
    pub fn populate(&mut self, map: PoolMap) {
        self.pool_map = Some(map);
    }

    pub fn buffered(self) -> Self {
        VirtualBlock {
            buffered: true,
            ..self
        }
    }

    pub fn start(&self) -> Result<(), ShmrError> {
        // set the handle to run
        let mut run = self.run.lock().unwrap();
        *run = true;

        let mut run_handle = self.run_handle.lock().unwrap();
        let local = self.clone();
        *run_handle = Some(thread::spawn(move || {
            flushomatic(local);
        }));

        Ok(())
    }

    pub fn stop(&self) -> Result<(), ShmrError> {
        let mut run = self.run.lock().unwrap();
        *run = false;

        let mut run_handle = self.run_handle.lock().unwrap();
        if let Some(handle) = run_handle.take() {
            handle.join().unwrap();
            *run_handle = None;
        }

        Ok(())
    }

    pub fn size(&self) -> usize {
        self.size
    }

    pub fn new(pools: &PoolMap, size: usize, topology: BlockTopology) -> Result<Self, ShmrError> {
        let uuid = Uuid::new_v4();
        debug!("[{}] creating new VirtualBlock", uuid);

        let (needed_shards, ident) = match &topology {
            BlockTopology::Single => (1, "single".to_string()),
            BlockTopology::Mirror(n) => (*n as usize, "mirror".to_string()),
            BlockTopology::Erasure(_, d, p) => ((d + p) as usize, format!("ec{}{}", d, p))
        };

        let mut buckets = pools.select_buckets(needed_shards)?;
        let mut shards = vec![];
        for i in 0..needed_shards {
            let vpf = VirtualPath {
                pool: pools.write_pool(),
                bucket: buckets.pop().unwrap(),
                filename: format!(
                    "{}_{}_{}.{}",
                    uuid, ident, i, VP_DEFAULT_FILE_EXT
                ),
            };
            // create the backing file while we're initializing everything
            vpf.create(pools)?;

            shards.push(vpf);
        }

        Ok(Self {
            uuid,
            size,
            topology,
            shards,
            shard_handles: Arc::new(Default::default()),
            buffered: false,
            buffer_loaded: Arc::new(AtomicBool::new(false)),
            buffer: Arc::new(Mutex::new(vec![])),
            pool_map: Some(pools.clone()),
            run: Arc::new(Mutex::new(false)),
            run_handle: Arc::new(Mutex::new(None)),
        })
    }

    /// Read from the VirtualBlock, at the given position, until the buffer is full
    pub fn read(&self, pos: usize, buf: &mut [u8]) -> Result<usize, ShmrError> {
        if buf.is_empty() {
            debug!("[{}] read was given an empty buffer", self.uuid);
            return Ok(0);
        }

        if !self.buffer_loaded.load(Ordering::Relaxed) {
            trace!("[{}] buffer is not populated. loading from disk.", self.uuid);
            // populate the buffer with the contents of the shard(s)
            self.load_block()?;
            // flag the buffer as loaded

        }

        let data = self.buffer.lock().unwrap();

        if data.len() < pos {
            return Err(ShmrError::OutOfSpace);
        }
        let read_bytes = cmp::min(data.len() - pos, buf.len());
        buf[..read_bytes].copy_from_slice(&data[pos..pos + read_bytes]);
        Ok(read_bytes)
    }

    pub fn write(&self, pos: usize, buf: &[u8]) -> Result<usize, ShmrError> {
        let written = buf.len();

        // make sure we're not writing past the end of the block
        if pos + buf.len() > self.size {
            return Err(ShmrError::OutOfSpace);
        }

        // update the buffer
        {
            let mut buffer = self.buffer.lock().unwrap();

            // there are instances when the buffer has not been initialized, and we need to resize it.
            // only resize it to the size of the incoming buffer, not the size of the block. One of
            // the reasons is that if we zero-fill the entire buffer, we will write it all to disk, which will take up extra space.
            // TODO this might have a big enough performance impact to warrant a better solution.
            let ending_pos = pos + buf.len();
            if buffer.len() < ending_pos {
                buffer.resize(ending_pos, 0);
            }

            buffer[pos..ending_pos].copy_from_slice(buf);
        }

        if !self.buffered {
            self.sync_data()?;
        }

        Ok(written)
    }

    /// Sync-Flush- the buffers to disk.
    pub fn sync_data(&self) -> Result<(), ShmrError> {
        {
            let shard_file_handles = self.shard_handles.lock().unwrap();
            if self.shards.len() != shard_file_handles.len() {

                drop(shard_file_handles); // dumb

                warn!("[{}] shard file handles are not opened at sync time", self.uuid);
                self.open_shards()?;
            }
        }

        let shard_file_handles = self.shard_handles.lock().unwrap();

        let buffer = self.buffer.lock().unwrap();

        if buffer.is_empty() {
            return Ok(());
        }

        match self.topology {
            BlockTopology::Single => {
                let shard = &shard_file_handles[0];

                shard.write_all_at(buffer.as_slice(), 0)?;

                shard.sync_all()?;
            }
            BlockTopology::Mirror(n) => {
                // TODO do these in parallel
                for i in 0..n {
                    let _ = &shard_file_handles[i as usize].write_all_at(buffer.as_slice(), 0)?;
                }
                for i in 0..n {
                    let _ = &shard_file_handles[i as usize].sync_all()?;
                }
            }
            BlockTopology::Erasure(_, data, parity) => {
                let r = ReedSolomon::new(data.into(), parity.into())?;
                let shard_size = calculate_shard_size(self.size, data as usize);

                let mut data_shards = buffer
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
                trace!("shards: {:#?}", data_shards.len());

                for _ in 0..(parity + (data - data_shards.len() as u8)) {
                    data_shards.push(vec![0; shard_size]);
                }

                let start = Instant::now();

                r.encode(&mut data_shards).unwrap();

                let duration = start.elapsed();
                debug!("Encoding duration: {:?}", duration);

                for (i, shard) in data_shards.iter().enumerate() {
                    let _ = &shard_file_handles[i].write_all_at(shard.as_slice(), 0)?;
                }
                for i in 0..data_shards.len() {
                    let _ = &shard_file_handles[i].sync_all()?;

                }

            }
        }

        debug!("[{}] successfully flushed buffer to disk", self.uuid);

        Ok(())
    }

    /// We can assume that this will always be all or nothing
    fn open_shards(&self) -> Result<(), ShmrError> {
        let mut shard_file_handles = self.shard_handles.lock().unwrap();
        if self.shards.len() == shard_file_handles.len() {
            debug!("[{}] shards are already opened. skipping.", self.uuid);
            return Ok(());
        }

        let pools = match self.pool_map {
            Some(ref p) => p,
            None => panic!("pool_map has not been populated. Unable to perform operation.")
        };

        for shard in &self.shards {
            let shard_path = shard.resolve(pools)?;
            let shard_file = OpenOptions::new().read(true).write(true).open(&shard_path.0)?;
            trace!("[{}] opening {:?}", self.uuid, shard_path.0);
            shard_file_handles.push(shard_file);
        }

        Ok(())
    }

    /// Read the entire contents of the VirtualBlock into the buffer
    fn load_block(&self) -> Result<(), ShmrError> {
        self.open_shards()?;
        let mut shard_file_handles = self.shard_handles.lock().unwrap();
        let mut buffer = self.buffer.lock().unwrap();

        // because Vec has a length of zero by default, and reading a file requires a buffer of a
        // certain size,
        if buffer.len() < self.size {
            buffer.resize(self.size, 0);
        }

        match self.topology {
            BlockTopology::Single => {
                let read_amt = &shard_file_handles[0].read(&mut buffer)?;
                debug!("[{}] read {} bytes from 1 shard", self.uuid, read_amt);
            }
            BlockTopology::Mirror(_) => {
                todo!("Implement Mirrored Read")
            }
            BlockTopology::Erasure(version, data, parity) => match version {
                1 => {
                    let r = ReedSolomon::new(data.into(), parity.into())?;
                    let shard_size = calculate_shard_size(self.size, data as usize);
                    let mut missing_shards = false;
                    let mut ec_shards: Vec<Option<Vec<u8>>> = shard_file_handles.iter().map(|mut h| {
                        let mut buffer = vec![];
                        let read_op = h.read_to_end(&mut buffer);
                        if read_op.is_err() {
                            missing_shards = true;
                            return None;
                        } else if read_op.unwrap() != shard_size {
                            missing_shards = true;
                            buffer.resize(shard_size, 0);
                        }
                        Some(buffer)
                    }).collect();

                    if missing_shards {
                        warn!("missing data shards. Attempting to reconstruct.");

                        let start_time = Instant::now();
                        r.reconstruct(&mut ec_shards).unwrap();

                        debug!("Reconstruction complete. took {:?}", start_time.elapsed());

                        // TODO Do something with the reconstructed data, so we don't need to reconstruct this block again
                    }

                    let ec_shards: Vec<Vec<u8>> = ec_shards.into_iter().map(|x| x.unwrap()).collect();
                    let mut ec_data = vec![];

                    for shard in ec_shards.iter() {
                        ec_data.extend_from_slice(shard);
                    }

                    // this should ignore any padding at the end of the ec_data slice
                    buffer.copy_from_slice(&ec_data[..self.size]);
                }
                _ => unimplemented!()
            }
        }

        self.buffer_loaded.store(true, Ordering::Relaxed);
        Ok(())
    }

    pub fn drop_buffer(&self) -> Result<(), ShmrError> {
        // sync before dropping the buffer
        self.sync_data()?;

        let mut buffer = self.buffer.lock().unwrap();
        *buffer = vec![];

        self.buffer_loaded.store(false, Ordering::Relaxed);

        Ok(())
    }

    pub fn drop_handles(&self) -> Result<(), ShmrError> {
        let mut handles = self.shard_handles.lock().unwrap();

        // sync before dropping handles
        self.sync_data()?;

        handles.clear();
        Ok(())
    }
}


fn flushomatic(block: VirtualBlock) {
    let (a, b) = crate::kernel::tasks::split_duration(Duration::from_millis(VIRTUAL_BLOCK_FLUSH_INTERVAL));
    debug!("[{}] starting worker. timings: {:?}/{:?}", block.uuid, &a, &b);
    loop {
        thread::sleep(a);
        {
            let run = block.run.lock().unwrap();
            if *run {
                if let Err(e) = block.sync_data() {
                    warn!("[{}] error syncing data: {:?}", block.uuid, e);
                }
            }
        }
        thread::sleep(b);
    }
}

// fn write_into(buf: &mut [u8], writes: &mut [(usize, Vec<u8>)], offset: usize) {
//     let write_buf_len = buf.len();
//     writes
//         .iter()
//         .filter(|(start, contents)| offset <= start + contents.len())
//         .for_each(|(start, contents)| {
//             let content_start = offset.saturating_sub(*start);
//             let content_end = cmp::min(contents.len(), write_buf_len);
//             let buf_start = start.saturating_sub(offset);
//             let buf_end = buf_start + content_end - content_start;
//             buf[buf_start..buf_end].copy_from_slice(&contents[content_start..content_end]);
//         });
// }

fn calculate_shard_size(length: usize, data_shards: usize) -> usize {
    (length as f32 / data_shards as f32).ceil() as usize
}

#[cfg(test)]
mod tests {
    use std::fs::File;
    use std::io::Read;
    use std::os::unix::prelude::MetadataExt;
    use std::sync::atomic::Ordering;
    use crate::random_data;
    use crate::tests::get_pool;
    use crate::vfs::block::VirtualBlock;

    #[test]
    fn test_virtual_block_new_block() {
        let pools = get_pool();

        let block = VirtualBlock::new(&pools, 1024, crate::vfs::block::BlockTopology::Single);
        assert!(block.is_ok());

        let block = block.unwrap();

        for shard in &block.shards {
            let path = shard.resolve(&pools);
            assert!(path.is_ok());
            assert!(path.unwrap().0.exists())
        }
    }

    #[test]
    fn test_virtual_block_unbuffered() {
        let pools = get_pool();

        let block = VirtualBlock::new(&pools, 1024, crate::vfs::block::BlockTopology::Single);
        assert!(block.is_ok());

        let block = block.unwrap();

        let shard_path = block.shards[0].resolve(&pools).unwrap();

        let data = random_data(500);
        let written = block.write(0, &data);
        assert!(written.is_ok(), "{:?}", written.err());

        assert_eq!(shard_path.0.metadata().unwrap().size() as usize, data.len());

        // drop the buffers and ensure they are empty
        assert!(block.drop_buffer().is_ok());
        assert!(!block.buffer_loaded.load(Ordering::Relaxed));
        {
            let buf_lock = block.buffer.lock().unwrap();
            assert_eq!(buf_lock.len(), 0);
        }

        // now read it back
        let mut read_buf = vec![0; 500];
        let read = block.read(0, &mut read_buf);
        assert!(read.is_ok(), "{:?}", read.err());
        assert_eq!(read.unwrap(), 500);
        assert_eq!(read_buf, data);
    }

    #[test]
    fn test_virtual_block_buffered() {
        let pools = get_pool();

        let block = VirtualBlock::new(&pools, 1024, crate::vfs::block::BlockTopology::Single);
        assert!(block.is_ok());

        let block = block.unwrap().buffered();
        let shard_path = block.shards[0].resolve(&pools).unwrap();

        assert!(block.buffered);

        let data = random_data(420);
        let written = block.write(0, &data);
        assert!(written.is_ok(), "{:?}", written.err());

        // check to see that the shard file has no content
        assert_eq!(shard_path.0.metadata().unwrap().size() as usize, 0);

        // check to see if the buffer has the data
        {
            let buf_lock = block.buffer.lock().unwrap();
            assert_eq!(buf_lock.len(), data.len());
            assert_eq!(&buf_lock[..data.len()], &data[..]);
        }

        // read it back normally
        let mut read_buf = vec![0; data.len()];
        let read = block.read(0, &mut read_buf);
        assert!(read.is_ok(), "{:?}", read.err());
        assert_eq!(read.unwrap(), data.len());

        // write an update to the data
        let data2 = random_data(420);
        let written = block.write(0, &data2);
        assert!(written.is_ok(), "{:?}", written.err());

        // read it again
        let mut read_buf = vec![0; data.len()];
        let read = block.read(0, &mut read_buf);
        assert!(read.is_ok(), "{:?}", read.err());
        assert_eq!(read_buf, data2);

        assert_eq!(shard_path.0.metadata().unwrap().size() as usize, 0);

        block.sync_data().unwrap();

        // check if the first 420 bytes are correct
        let mut read_buf = vec![0; 420];
        let mut h = File::open(shard_path.0).unwrap();
        h.read_exact(&mut read_buf).unwrap();

        assert_eq!(read_buf, data2);
    }

    #[test]
    fn test_virtual_block_erasure_buffered() {
        let pools = get_pool();

        let block = VirtualBlock::new(&pools, 1024, crate::vfs::block::BlockTopology::Erasure(1, 3, 2));
        assert!(block.is_ok());

        let block = block.unwrap();

        let data = random_data(500);
        let written = block.write(0, &data);
        assert!(written.is_ok(), "{:?}", written.err());

        // check to see if the data in the 1st shard is correct
        let mut read_buf = vec![0; 250];
        let read = block.read(0, &mut read_buf);
        assert!(read.is_ok(), "{:?}", read.err());
        assert_eq!(read_buf, &data[..250]);
    }


}
#![allow(clippy::needless_borrow)]
use crate::config::{ShmrError, ShmrFsConfig};
use crate::iostat::{
    METRIC_DISK_IO_OPERATION, METRIC_DISK_IO_OPERATION_DURATION, METRIC_ERASURE_ENCODING_DURATION,
};
use crate::vfs::calculate_shard_size;
use crate::vfs::path::{VirtualPath, VP_DEFAULT_FILE_EXT};
use log::{debug, trace, warn};
use metrics::{counter, histogram};
use reed_solomon_erasure::galois_8::ReedSolomon;
use serde::{Deserialize, Serialize};
use std::default::Default;
use std::fs::{File, OpenOptions};
use std::io::Read;
use std::os::unix::prelude::FileExt;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;
use std::{cmp, mem};
use std::{fmt, io};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum BlockTopology {
    /// Single Shard
    Single,
    /// Mirrored Shards, with n mirrors
    Mirror(u8),
    /// Erasure Encoded. (Version, Data Shards, Parity Shards)
    Erasure(u8, u8, u8),
}

impl fmt::Display for BlockTopology {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            BlockTopology::Single => write!(f, "Single"),
            BlockTopology::Mirror(n) => write!(f, "Mirror({})", n),
            BlockTopology::Erasure(v, ds, ps) => write!(f, "Erasure({}, {}, {})", v, ds, ps),
        }
    }
}
impl From<BlockTopology> for String {
    fn from(value: BlockTopology) -> Self {
        match value {
            BlockTopology::Single => "Single".to_string(),
            BlockTopology::Mirror(n) => format!("Mirror({})", n),
            BlockTopology::Erasure(v, d, p) => format!("Erasure({}, {}, {})", v, d, p),
        }
    }
}

impl TryFrom<String> for BlockTopology {
    type Error = String;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        let parts: Vec<&str> = value.splitn(2, '(').collect();

        if parts.len() != 2 {
            return Err(format!("'{}' does not have '('", value));
        }

        let (name, mut arg) = (parts[0], parts[1].to_string());
        arg.pop();
        match name {
            "Single" => Ok(BlockTopology::Single),
            "Mirror" => {
                let n: u8 = arg
                    .trim_end_matches(')')
                    .parse()
                    .map_err(|_| format!("Unable to parse {}. {} - {}", value, name, arg))?;
                Ok(BlockTopology::Mirror(n))
            }
            "Erasure" => {
                let params: Vec<&str> = arg.splitn(3, ',').collect();
                eprintln!("params: {:?}", &params);
                let v: u8 = params[0]
                    .trim()
                    .parse()
                    .map_err(|_| format!("Unable to parse version {:?}", params))?;
                let ds: u8 = params
                    .get(1)
                    .unwrap_or(&"")
                    .to_string()
                    .trim()
                    .parse()
                    .map_err(|_| format!("Unable to parse data shards {:?}", params))?;
                let ps: u8 = params
                    .get(2)
                    .unwrap_or(&"")
                    .trim()
                    .trim_end_matches(')')
                    .parse()
                    .map_err(|_| format!("Unable to parse parity shards {:?}", params))?;
                Ok(BlockTopology::Erasure(v, ds, ps))
            }
            _ => Err(format!("Unable to parse {}. {} - {}", value, name, arg)),
        }
    }
}

// use Intel QAT, AMD AOCL-Compression (https://github.com/amd/aocl-compression)
// ref: https://git.sr.ht/~quf/rust-compression-comparison
#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum BlockCompression {
    Lz4(u8),
    Lz4Hc(u8),
    Zlib,
    Zstd(u8),
    Brotli,
    Lzma,
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
    /// Parent Inode.
    pub ino: u64,

    /// Block Number
    pub idx: u64,
    /// Size of the StorageBlock.
    /// This is a fixed size, and represents the maximum amount of data that can be stored in this
    /// vfs. On-disk size might be a bit larger (or smaller) than this value.
    pub size: u64,

    /// Layout of this StorageBlock
    pub topology: BlockTopology,

    /// Shards that make up this block.
    pub shards: Vec<VirtualPath>,

    /// File handles for each Shard. Stored in the same order as in shards
    #[serde(skip)]
    shard_handles: Arc<Mutex<Vec<(VirtualPath, File)>>>,

    #[serde(skip)]
    shard_loaded: Arc<AtomicBool>,

    #[serde(skip)]
    should_flush: Arc<AtomicBool>,

    // stateful fields.
    #[serde(skip)]
    buffer_loaded: Arc<AtomicBool>,

    #[serde(skip)]
    buffer: Arc<Mutex<Vec<u8>>>,

    // none, because poolmap will default to a LazyLock that can be populated before the file is read from the database.
    // bit of a hack to give the poolmap a default value, but it works.
    #[serde(skip)]
    pool_map: Option<Arc<ShmrFsConfig>>,
}
impl Default for VirtualBlock {
    fn default() -> Self {
        VirtualBlock {
            ino: 0,
            idx: 0,
            size: 0,
            topology: BlockTopology::Single,
            shards: Vec::new(),
            shard_handles: Arc::new(Mutex::new(Vec::new())),
            shard_loaded: Arc::new(AtomicBool::new(false)),
            should_flush: Arc::new(AtomicBool::new(false)),
            buffer_loaded: Arc::new(AtomicBool::new(false)),
            buffer: Arc::new(Mutex::new(Vec::new())),
            pool_map: None,
        }
    }
}
#[allow(dead_code)]
impl VirtualBlock {
    pub fn new() -> Self {
        Default::default()
    }
    pub fn populate(&mut self, map: Arc<ShmrFsConfig>) {
        self.pool_map = Some(map);
    }

    pub fn size(&self) -> u64 {
        self.size
    }

    /// Create an empty VirtualBlock.
    pub fn create(
        ino: u64,
        idx: u64,
        config: Arc<ShmrFsConfig>,
        size: u64,
        topology: BlockTopology,
    ) -> Result<Self, ShmrError> {
        VirtualBlock::create_with_pool(
            ino,
            idx,
            config.write_pool.clone().as_str(),
            config,
            size,
            topology,
        )
    }

    pub fn create_with_pool(
        ino: u64,
        idx: u64,
        pool: &str,
        config: Arc<ShmrFsConfig>,
        size: u64,
        topology: BlockTopology,
    ) -> Result<Self, ShmrError> {
        debug!(
            "[{}:{:#016x}] creating new VirtualBlock of size {} in pool '{}'",
            ino, idx, size, pool
        );

        let (needed_shards, ident) = match &topology {
            BlockTopology::Single => (1, "single".to_string()),
            BlockTopology::Mirror(n) => (*n as usize, "mirror".to_string()),
            BlockTopology::Erasure(_, d, p) => ((d + p) as usize, format!("ec{}{}", d, p)),
        };

        let buckets = config.select_buckets(pool, needed_shards)?;
        let mut shards = vec![];
        for (i, bucket) in buckets.into_iter().enumerate() {
            // TODO need to add some randomness
            let mut filename = ino.to_string();
            filename += ":";
            filename.push_str(&idx.to_string());
            filename += "_";
            filename.push_str(&ident.to_string());
            filename += "_";
            filename.push_str(&i.to_string());
            filename += ".";
            filename.push_str(VP_DEFAULT_FILE_EXT);

            let vpf = VirtualPath {
                pool: pool.to_string(),
                bucket,
                filename,
            };
            // create the backing file while we're initializing everything
            vpf.create(&config)?;

            shards.push(vpf);
        }

        Ok(Self {
            ino,
            idx,
            size,
            topology,
            shards,
            shard_handles: Arc::new(Default::default()),
            buffer_loaded: Arc::new(AtomicBool::new(false)),
            shard_loaded: Arc::new(AtomicBool::new(false)),
            should_flush: Arc::new(AtomicBool::new(false)),
            buffer: Arc::new(Mutex::new(vec![])),
            pool_map: Some(config),
        })
    }

    /// Read from the VirtualBlock, at the given position, until the buffer is full
    pub fn read(&self, pos: usize, buf: &mut [u8]) -> Result<usize, ShmrError> {
        trace!(
            "[{}:{:#016x}] reading {} bytes at {}",
            self.ino,
            self.idx,
            buf.len(),
            pos
        );
        if buf.is_empty() {
            debug!(
                "[{}:{:#016x}] read was given an empty buffer, returning early",
                self.ino, self.idx
            );
            return Ok(0);
        }

        if !self.buffer_loaded.load(Ordering::Relaxed) {
            trace!(
                "[{}:{:#016x}] buffer is not populated. loading from disk.",
                self.ino,
                self.idx
            );
            // populate the buffer with the contents of the shard(s)
            self.load_block()?;
        }

        let data = self.buffer.lock().unwrap();
        if data.len() < pos {
            return Err(ShmrError::OutOfSpace);
        }

        let read_bytes = cmp::min(data.len() - pos, buf.len());
        let pos_end = pos + read_bytes;
        trace!(
            "[{}:{:#016x}] buf[..{}] = &data[{}.{}]",
            self.ino,
            self.idx,
            read_bytes,
            pos,
            pos_end
        );
        buf[..read_bytes].copy_from_slice(&data[pos..pos_end]);

        Ok(read_bytes)
    }

    pub fn write(&self, pos: u64, buf: &[u8]) -> Result<usize, ShmrError> {
        trace!(
            "[{}:{:#016x}] writing {} bytes at offset {}",
            self.ino,
            self.idx,
            buf.len(),
            pos
        );

        // make sure we're not writing past the end of the block
        if pos + buf.len() as u64 > self.size {
            return Err(ShmrError::OutOfSpace);
        }

        let written = buf.len();

        // update the buffer
        {
            let mut buffer = self.buffer.lock().unwrap();

            // there are instances when the buffer has not been initialized, and we need to resize it.
            // only resize the buffer to the size of the incoming buffer, not the size of the block.
            // the reasons is that if we zero-fill the entire buffer, we will write it all to disk-
            // which can take up extra space.
            // TODO this might have a big enough performance impact to warrant a better solution.
            let ending_pos = pos as usize + buf.len();
            if buffer.len() < ending_pos {
                buffer.resize(ending_pos, 0);
            }

            buffer[(pos as usize)..ending_pos].copy_from_slice(buf);
        }

        // if !self.buffered.load(Ordering::Relaxed) {
        //     trace!(
        //         "[{}:{:#016x}] block is not buffered. syncing data",
        //         self.ino,
        //         self.idx,
        //     );
        //     self.sync_data()?;
        // }
        // self.sync_data(true)?;
        self.should_flush.store(false, Ordering::Relaxed);

        Ok(written)
    }

    /// Sync-Flush- the buffers to disk.
    pub fn sync_data(&self, force: bool) -> Result<(), ShmrError> {
        // Don't sync if we're not being forced and no writes have occurred
        if !force && !self.should_flush.load(Ordering::Relaxed) {
            debug!(
                "[{}:{:#016x}] skipping buffer sync. not forced and not required to",
                self.ino, self.idx,
            );
            return Ok(());
        }

        trace!("[{}:{:#016x}] syncing buffer", self.ino, self.idx,);
        if !self.shard_loaded.load(Ordering::Relaxed) {
            self.open_handles()?;
        }

        let buffer = self.buffer.lock().unwrap();
        if buffer.is_empty() {
            return Ok(());
        }

        let shard_file_handles = self.shard_handles.lock().unwrap();
        match self.topology {
            BlockTopology::Single => {
                write_path(&shard_file_handles[0], buffer.as_slice())?;
            }
            BlockTopology::Mirror(n) => {
                // TODO do these in parallel
                for i in 0..n as usize {
                    write_path(&shard_file_handles[i], buffer.as_slice())?;
                }
            }
            BlockTopology::Erasure(_, data, parity) => {
                let r = ReedSolomon::new(data.into(), parity.into())?;
                let shard_size = calculate_shard_size(self.size, data);

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

                for _ in 0..(parity + (data - data_shards.len() as u8)) {
                    data_shards.push(vec![0; shard_size]);
                }

                let start = Instant::now();

                r.encode(&mut data_shards).unwrap();

                let duration = start.elapsed();
                histogram!(METRIC_ERASURE_ENCODING_DURATION).record(duration.as_micros() as f64);
                debug!(
                    "[{}:{:#016x}] took {:?} to perform erasure encoding",
                    self.ino, self.idx, duration
                );

                // TODO do these writes in parallel
                for (i, shard) in data_shards.iter().enumerate() {
                    write_path(&shard_file_handles[i], shard.as_slice())?;
                }
            }
        }

        trace!(
            "[{}:{:#016x}] successfully flushed buffer to disk",
            self.ino,
            self.idx
        );

        self.should_flush.store(false, Ordering::Relaxed);

        Ok(())
    }

    /// Open the Shard File Handles, closing any existing handles.
    fn open_handles(&self) -> Result<(), ShmrError> {
        let pools = match self.pool_map {
            Some(ref p) => p,
            None => panic!("pool_map has not been populated. Unable to perform operation."),
        };

        let mut shard_file_handles = self.shard_handles.lock().unwrap();
        if shard_file_handles.len() > 0 {
            debug!(
                "[{}:{:#016x}] dropping existing handles",
                self.ino, self.idx
            );
            // take the contents of the Vec and just throw them on the ground
            // https://www.youtube.com/watch?v=gAYL5H46QnQ
            let _ = mem::take(&mut *shard_file_handles);
        }
        let shard_copies = self.shards.clone();
        for shard in shard_copies {
            let shard_path = shard.resolve(pools)?;

            trace!(
                "[{}:{:#016x}] opening {:?}",
                self.ino,
                self.idx,
                &shard_path.0
            );
            shard_file_handles.push((
                shard,
                OpenOptions::new()
                    .read(true)
                    .write(true)
                    .open(&shard_path.0)?,
            ));
        }

        self.shard_loaded.store(true, Ordering::Relaxed);

        Ok(())
    }

    /// Read the entire contents of the VirtualBlock into the buffer
    fn load_block(&self) -> Result<(), ShmrError> {
        if !self.shard_loaded.load(Ordering::Relaxed) {
            self.open_handles()?;
        }
        let mut buffer = self.buffer.lock().unwrap();

        // because Vec has a length of zero by default, we need to size it before it's usable as a
        // buffer
        let size = self.size as usize;
        if buffer.len() < size {
            buffer.resize(size, 0);
        }

        let mut shard_file_handles = self.shard_handles.lock().unwrap();
        match self.topology {
            BlockTopology::Single => {
                counter!(METRIC_DISK_IO_OPERATION,
                    "pool" => (&shard_file_handles[0].0.pool).clone(),
                    "bucket" => (&shard_file_handles[0].0.bucket).clone(),
                    "op" => "read",
                )
                .increment(1);
                let read_amt = &shard_file_handles[0].1.read(&mut buffer)?;
                trace!(
                    "[{}:{:#016x}] read {} bytes from 1 shard",
                    self.ino,
                    self.idx,
                    read_amt
                );
            }
            BlockTopology::Mirror(_) => {
                todo!("Implement Mirrored Read")
            }
            BlockTopology::Erasure(version, data, parity) => match version {
                1 => {
                    let r = ReedSolomon::new(data.into(), parity.into())?;
                    let shard_size = calculate_shard_size(self.size, data);
                    let mut missing_shards = false;
                    let mut ec_shards: Vec<Option<Vec<u8>>> = shard_file_handles
                        .iter_mut()
                        .map(|h| {
                            counter!(METRIC_DISK_IO_OPERATION,
                                "pool" => h.0.pool.clone(),
                                "bucket" => h.0.bucket.clone(),
                                "op" => "read",
                            )
                            .increment(1);
                            let mut buffer = vec![];
                            let read_op = h.1.read_to_end(&mut buffer);
                            if read_op.is_err() {
                                missing_shards = true;
                                return None;
                            } else if read_op.unwrap() != shard_size {
                                missing_shards = true;
                                buffer.resize(shard_size, 0);
                            }
                            Some(buffer)
                        })
                        .collect();

                    if missing_shards {
                        warn!("missing data shards. Attempting to reconstruct.");

                        let start_time = Instant::now();
                        r.reconstruct(&mut ec_shards).unwrap();

                        debug!("Reconstruction complete. took {:?}", start_time.elapsed());

                        // TODO Do something with the reconstructed data, so we don't need to reconstruct this block again
                    }

                    let ec_shards: Vec<Vec<u8>> =
                        ec_shards.into_iter().map(|x| x.unwrap()).collect();
                    let mut ec_data = vec![];

                    for shard in ec_shards.iter() {
                        ec_data.extend_from_slice(shard);
                    }

                    // this should ignore any padding at the end of the ec_data slice
                    buffer.copy_from_slice(&ec_data[..self.size as usize]);
                }
                _ => unimplemented!(),
            },
        }

        self.buffer_loaded.store(true, Ordering::Relaxed);
        Ok(())
    }

    pub fn drop_buffer(&self) -> Result<(), ShmrError> {
        // sync before dropping the buffer
        self.sync_data(true)?;

        let mut buffer = self.buffer.lock().unwrap();
        *buffer = vec![];

        self.buffer_loaded.store(false, Ordering::Relaxed);

        Ok(())
    }

    pub fn drop_handles(&self) -> Result<(), ShmrError> {
        let mut handles = self.shard_handles.lock().unwrap();

        // sync before dropping handles
        self.sync_data(true)?;

        handles.clear();

        self.shard_loaded.store(false, Ordering::Relaxed);
        Ok(())
    }
}

fn write_path(shard: &(VirtualPath, File), buf: &[u8]) -> io::Result<()> {
    let start = Instant::now();

    shard.1.write_all_at(buf, 0)?;
    shard.1.sync_all()?;

    let duration = start.elapsed();

    histogram!(METRIC_DISK_IO_OPERATION_DURATION,
        "pool" => shard.0.pool.clone(),
        "bucket" => shard.0.bucket.clone(),
        "op" => "write"
    )
    .record(duration.as_micros() as f64);

    counter!(METRIC_DISK_IO_OPERATION,
        "pool" => shard.0.pool.clone(),
        "bucket" => shard.0.bucket.clone(),
        "op" => "write",
    )
    .increment(1);

    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::config::random_data;
    use crate::tests::get_shmr_config;
    use crate::vfs::block::{BlockTopology, VirtualBlock};
    use std::fs::File;
    use std::io::Read;
    use std::os::unix::prelude::MetadataExt;
    use std::sync::atomic::Ordering;
    use std::sync::Arc;

    #[test]
    fn test_block_topology_try_from() {
        let topology = BlockTopology::try_from("Erasure(1, 3, 2)".to_string());
        assert!(topology.is_ok(), "{:?}", topology);
        match topology.unwrap() {
            BlockTopology::Erasure(v, d, p) => {
                assert_eq!(v, 1);
                assert_eq!(d, 3);
                assert_eq!(p, 2);
            }
            _ => panic!("Expected an Erasure topology"),
        }
    }

    #[test]
    fn test_virtual_block_new_block() {
        let cfg = Arc::new(get_shmr_config());

        let block = VirtualBlock::create(1, 0, cfg.clone(), 1024, BlockTopology::Single);
        assert!(block.is_ok());

        let block = block.unwrap();

        for shard in &block.shards {
            let path = shard.resolve(&cfg);
            assert!(path.is_ok());
            assert!(path.unwrap().0.exists())
        }
    }

    #[test]
    fn test_virtual_block_unbuffered_backing() {
        let cfg = Arc::new(get_shmr_config());

        let block = VirtualBlock::create(3, 0, cfg.clone(), 1024, BlockTopology::Single);
        assert!(block.is_ok());

        let block = block.unwrap();

        let shard_path = block.shards[0].resolve(&cfg).unwrap();

        let data = random_data(500);
        let written = block.write(0, &data);
        assert!(written.is_ok(), "{:?}", written.err());

        block.sync_data(true).unwrap();
        assert_eq!(shard_path.0.metadata().unwrap().size() as usize, data.len());

        // drop the buffers and ensure they are empty
        assert!(block.drop_buffer().is_ok());
        assert!(!block.buffer_loaded.load(Ordering::Relaxed));
        {
            let buf_lock = block.buffer.lock().unwrap();
            assert_eq!(buf_lock.len(), 0);
        }

        let shard_path = block.shards[0].resolve(&cfg).unwrap();
        let mut buf = vec![0; 500];

        let mut file = File::open(&shard_path.0).unwrap();
        file.read_exact(&mut buf).unwrap();

        assert_eq!(buf, data)
    }

    #[test]
    fn test_virtual_block_unbuffered() {
        let cfg = Arc::new(get_shmr_config());

        let block = VirtualBlock::create(3, 0, cfg.clone(), 1024, BlockTopology::Single);
        assert!(block.is_ok());

        let block = block.unwrap();

        let shard_path = block.shards[0].resolve(&cfg).unwrap();

        let data = random_data(500);
        let written = block.write(0, &data);
        assert!(written.is_ok(), "{:?}", written.err());

        block.sync_data(true).unwrap();
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
        let cfg = Arc::new(get_shmr_config());

        let block = VirtualBlock::create(5, 0, cfg.clone(), 1024, BlockTopology::Single);
        assert!(block.is_ok());

        let block = block.unwrap();
        let shard_path = block.shards[0].resolve(&cfg).unwrap();

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

        block.sync_data(true).unwrap();

        // check if the first 420 bytes are correct
        let mut read_buf = vec![0; 420];
        let mut h = File::open(shard_path.0).unwrap();
        h.read_exact(&mut read_buf).unwrap();

        assert_eq!(read_buf, data2);
    }

    #[test]
    fn test_virtual_block_erasure_buffered() {
        let cfg = Arc::new(get_shmr_config());

        let block = VirtualBlock::create(7, 0, cfg.clone(), 1024, BlockTopology::Single);
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

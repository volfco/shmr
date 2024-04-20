#![feature(trivial_bounds)]
use rand::Rng;
pub mod file;
pub mod fsdb;
pub mod fuse;
pub mod storage;
pub mod vpf;

// just some helper functions for now
pub fn random_string() -> String {
    let mut rng = rand::thread_rng();
    let s: String = (0..14).map(|_| rng.gen_range(0..9).to_string()).collect();
    s
}

pub fn random_data(size: usize) -> Vec<u8> {
    let mut rng = rand::thread_rng();
    (0..size).map(|_| rng.gen_range(0..255)).collect()
}

#[cfg(test)]
pub mod tests {
    pub fn init() {
        let _ = env_logger::builder().is_test(true).try_init();
    }
}

// const FILE_HANDLE_READ_BIT: u64 = 1 << 63;
// const FILE_HANDLE_WRITE_BIT: u64 = 1 << 62;
//
// #[derive(Debug, Clone, Serialize, Deserialize)]
// pub struct ConfigStub {
//     pub mount: String,
//     pub workspace: PathBuf,
//   pub archives: HashMap<String, PathBuf>,
// }
//
// pub struct ShmrFilesystem {
//     config: ConfigStub,
//     superblock: Superblock,
//
//     atime: bool,
//     pub(crate) current_fh: AtomicU64,
//
// }
// impl ShmrFilesystem {
//     pub fn init(config: ConfigStub) -> Result<Self> {
//       debug!("initializing filesystem");
//         let superblock = Superblock::open(&config)?;
//         Ok(Self {
//             config,
//             superblock,
//             atime: false,
//             current_fh: Default::default(),
//         })
//     }
//
//   fn allocate_next_file_handle(&self, read: bool, write: bool) -> u64 {
//     let mut fh = self.current_fh.fetch_add(1, Ordering::SeqCst);
//     // Assert that we haven't run out of file handles
//     assert!(fh < FILE_HANDLE_READ_BIT.min(FILE_HANDLE_WRITE_BIT));
//     if read {
//       fh |= FILE_HANDLE_READ_BIT;
//     }
//     if write {
//       fh |= FILE_HANDLE_WRITE_BIT;
//     }
//
//     fh
//   }
// }
//
//
// pub fn time_now() -> (i64, u32) {
//     let now = SystemTime::now();
//     let since_the_epoch = now.duration_since(UNIX_EPOCH).expect("Time went backwards");
//     (since_the_epoch.as_secs() as i64, since_the_epoch.subsec_nanos())
//   }

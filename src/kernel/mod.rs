pub(crate) mod tasks;

// const KERNEL_BLOCK_CACHE_EVICTION_SWEEP_INTERVAL: u64 = 333; // ms
// const KERNEL_BLOCK_CACHE_EVICTION_HIGH_MARK: usize = 1024 * 1024 * 1024; // 1GB
//
// // Evict until heap size is below this threshold
// const KERNEL_BLOCK_CACHE_EVICTION_LOW_MARK: usize = 1024 * 1024 * 768; // 768MB
//
// #[derive(Clone, Debug)]
// pub struct Kernel {
//     pub pools: PoolMap,
//     run: Arc<Mutex<bool>>,
//     buffered: bool,
//     thread_handles: Arc<Mutex<Vec<JoinHandle<()>>>>,
//
//     block_cache: Arc<RwLock<BTreeMap<Uuid, VirtualBlock>>>
// }
// impl Kernel {
//     pub fn new(pools: PoolMap) -> Self {
//
//         Kernel {
//             run: Arc::new(Mutex::new(true)),
//             buffered: true,
//             thread_handles: Arc::new(Mutex::new(Vec::new())),
//             block_cache: Arc::new(RwLock::new(BTreeMap::new())),
//             pools
//         }
//     }
//
//     pub fn enable_buffer(&self) {
//
//     }
//
//     pub fn start(&mut self) -> Result<(), ShmrError> {
//
//         let mut thread_handle = self.thread_handles.lock().unwrap();
//         let thread_kernel = self.clone();
//         thread_handle.push(thread::spawn(move || {
//             block_cache_worker(thread_kernel)
//         }));
//         Ok(())
//     }
//
// }

// fn block_cache_worker(kernel: Kernel) {
//     debug!("entering block cache worker");
//     fn read_size(kernel: &Kernel) -> usize {
//         {
//             let block_cache = kernel.block_cache.read().unwrap();
//             mem::size_of_val(&*block_cache)
//         }
//     }
//     let (a, b) = crate::kernel::tasks::split_duration(Duration::from_millis(KERNEL_BLOCK_CACHE_EVICTION_SWEEP_INTERVAL));
//     let mut rng = rand::thread_rng();
//     loop {
//         thread::sleep(a);
//         debug!("block cache current size: {} bytes", read_size(&kernel));
//         // if we're past the high watermark...
//         if read_size(&kernel) > KERNEL_BLOCK_CACHE_EVICTION_HIGH_MARK {
//             debug!("evicting blocks from cache");
//             // evict until we're below the low watermark
//             while read_size(&kernel) > KERNEL_BLOCK_CACHE_EVICTION_LOW_MARK {
//                 // MVP eviction strategy: randomly evict a block
//                 let mut block_cache = kernel.block_cache.write().unwrap();
//                 // pick a random entry and drop it
//                 let idx = rng.gen_range(0..block_cache.len());
//                 let key = *block_cache.keys().nth(idx).unwrap();
//                 block_cache.remove(&key);
//             }
//         }
//         thread::sleep(b);
//     }
// }
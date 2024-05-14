use std::collections::HashMap;
use std::mem;
use std::sync::{Arc, Mutex, RwLock};
use std::sync::atomic::{AtomicUsize, Ordering};
use log::debug;
use crate::fuse::magics::FILE_CACHE_MANAGER_LOW_WATERMARK_RATIO;
use crate::vfs::VirtualFile;

#[derive(Debug, Clone)]
pub enum FileCacheStrategy {
    /// Prioritize keeping Read Heavy VirtualFiles in memory
    ReadCachePriority,
    /// Prioritize keeping Write Heavy VirtualFiles in memory
    WriteCachePriority,
    /// Prioritize keeping I/O Heavy VirtualFiles in memory, ignoring the type of I/O
    IOCountPriority
}

#[derive(Debug, Clone)]
pub struct FileCacheManager {
    // VirtualFile, as of 2024-05-14, is not threadsafe?
    // TODO Make VirtualFile threadsafe, so we don't need to have a Mutex here
    entries: Arc<RwLock<HashMap<u64, Arc<Mutex<VirtualFile>>>>>,

    strategy: Arc<Mutex<FileCacheStrategy>>,

    /// The memory usage target
    memory_limit: Arc<AtomicUsize>
}

/// Thread Logic:
/// - Get memory size of entries. If it is below the watermark, do nothing.
/// - Iterate over the BTreeMap and get a copy of all the VirtualFile entries
/// - For each entry, lock the handle and call .iostat() and store the entries in a Vec
/// - Sort the Vec according to FileCacheStrategy
/// - Unload each VirtualFile, in order of highest to lowest score
fn cache_worker(mgr: FileCacheManager) {
    fn read_size(mgr: &FileCacheManager) -> usize {
        {
            let block_cache = mgr.entries.read().unwrap();
            mem::size_of_val(&*block_cache)
        }
    }
    debug!("block cache current size: {} bytes", read_size(&mgr));
    let memory_limit = mgr.memory_limit.load(Ordering::Relaxed);
    if read_size(&mgr) > memory_limit {
        let mem_target = (memory_limit as f32 * FILE_CACHE_MANAGER_LOW_WATERMARK_RATIO) as usize;

        let mut entries: Vec<(u64, Arc<Mutex<VirtualFile>>)> = {
            let block_cache = mgr.entries.read().unwrap();
            block_cache.iter().map(|(k, v)| (*k, Arc::clone(v))).collect()
        };
        entries.sort_by(|(_, a), (_, b)| {
            let (a_instant, a_read_count, a_write_count) = a.lock().unwrap().iostat();
            let (b_instant, b_read_count, b_write_count) = b.lock().unwrap().iostat();
            match *mgr.strategy.lock().unwrap() {
                FileCacheStrategy::ReadCachePriority => {
                    a_instant.cmp(&b_instant)
                }
                FileCacheStrategy::WriteCachePriority => {
                    (a_write_count + a_read_count).cmp(&(b_write_count + b_read_count))
                }
                FileCacheStrategy::IOCountPriority => {
                    (a_write_count + a_read_count).cmp(&(b_write_count + b_read_count))
                }
            }
        });
        // for (key, entry) in entries {
        //     let mut vf = entry.lock().unwrap();
        //     vf.unload().unwrap();
        //     let size = vf.size();
        //     debug!("Unloaded VirtualFile with key: {}, size: {} bytes", key, size);
        // }
        while read_size(&mgr) > mem_target {
            let (key, entry) = entries.pop().unwrap();
            let vf = entry.lock().unwrap();
            let vf_mem_size = mem::size_of_val(&*vf);
            vf.unload().unwrap();
            let new_vf_mem_size = mem::size_of_val(&*vf);
            debug!("unloaded VirtualFile cache entry {}. old size: {}. new size: {} ", key, vf_mem_size, new_vf_mem_size);
        }

    }
    todo!()
}
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
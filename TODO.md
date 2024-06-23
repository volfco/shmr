
- dbus implementations
  - Methods
    - co.volf.shmr.ShmrFs1
      - [ ] Unmount
      - [ ] FlushAll
      - [ ] FlushInode
      - [ ] RewriteFile
      - [ ] "method to rewrite file with a new block size. i.e. 1MB to 32MB"
    - Properties
    - Signals
- Background Processes
  - Memory Size Monitor Thing
    - Track used memory for file cache, and evict according to Io policy

- Implement dbus server as a `WorkerTask`
- Implement a `WorkerTask` Manager to view status of running tasks
- Parallelize `flush` so all blocks in a VirtualFile flush in parallel
- Writes don't hit the cache if the cache is empty?
- File Compression
- [ ] Adopt https://github.com/mehcode/config-rs for config file
- [ ] Need to handle open file limits
  - [ ] Allow configuration of the value in config
  - [ ] Worker to monitor open files, and trigger eviction if too high
- [ ] Add a facility re-use `vec![0; 4096]` and other vecs for performance (if it can improve perf)
  ```rust
    let mut v = vec![1; 1024 * 1024];  // 1MB Vec
    // ...
    
    // Zero out the Vec
    for i in &mut v {
    *i = 0;
    }
  ```
  

## Major Features
- Write this as a kernel module? https://github.com/Rust-for-Linux/rust-out-of-tree-module/blob/main/rust_out_of_tree.rs
  - https://github.com/project-machine/puzzlefs
  - https://rust-for-linux.com/null-block-driver
- Store first few blocks on SSD, while the rest on SMR drives. So the drives can be powered off when not in use
- 
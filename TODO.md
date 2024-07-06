- ~~unlink (rename) function~~
- ~~Parallelize `flush` so all blocks in a VirtualFile flush in parallel~~
- add warning statements around get_mut and other write locks
- dbus implementations
  - Methods
    - co.volf.shmr.ShmrFs1
      - [ ] Unmount
      - [ ] FlushAll
      - [ ] FlushInode
      - [ ] RewriteFile
      - [ ] "method to rewrite file with a new block size. i.e. 1MB to 32MB"
      - [ ] PurgeTombstone(inode)
      - [ ] GetTombstones
    - Properties
    - Signals
- Metrics
  - metrics-rs to OpenTelemetry gRPC thingie
  - metrics-rs in memory query engine
    - specify collect interval for metrics-rs to thingie sweep
    - specify upstream flush interval. Like 1s
    - specify retention intervals. So if there are 30 1s intervals, there is a 30 element ring buffer holding the values
    - specify high resolution retention intervals. If set to 5, will keep full set of metric data for the given intervals. This value defaults to 1
    - Interface to query for data. This is looking for the average value of disk_io_operation_duration for the abc pool
      over the last 30 flush intervals. This will always include the current interval in its calculations
      
      ```rust
        let res = MetricDb.get("disk_io_operation_duration", filters=[
            MetricSelector::Match("pool", "abc")
        ]).avg(30);
      ``` 
    - 
- Background Processes
  - Memory Size Monitor Thing
    - Track used memory for file cache, and evict according to Io policy
- Implement a `WorkerTask` Manager to view status of running tasks
- Writes don't hit the cache if the cache is empty?
- Have shmrd spawn the fuse server in a seperate thread, and then isolate that thread to the same CPU core as the fuse 
  kernel thread is running on, then prevent other processes from using it
- shmr_crond daemon. runs housekeeping tasks and stuff via dbus calls
  - tombstone cleanup !!!
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

Library to sync metrics!() to a persistent on-disk format that can be read by others. Also can be queried by the application 

## Major Features
- Write this as a kernel module? https://github.com/Rust-for-Linux/rust-out-of-tree-module/blob/main/rust_out_of_tree.rs
  - https://github.com/project-machine/puzzlefs
  - https://rust-for-linux.com/null-block-driver
- Store first few blocks on SSD, while the rest on SMR drives. So the drives can be powered off when not in use
- 
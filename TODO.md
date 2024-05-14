
## 0.0.1
Goal. Full FUSE implementation that only uses the workspace/blocks directory to store data

- [X] StorageBlock Working
  - [X] Single
  - [X] Mirror
  - [X] Erasure Coding
    - [X] Create
    - [X] Write
    - [X] Read
- [X] Fully Initialize Filesystem on first run, if no superblock is found
- [X] Take the fuse `simple.rs` example and modify it to use the VirtualFile and FsDB structs
- [X] File I/O works
  - [ ] Directories
    - [X] Create
    - [X] Read
    - [ ] Delete
    - [ ] Rename
  - [X] Files
    - [X] Create
    - [X] Read
    - [X] Write
    - [ ] Delete
    - [ ] Rename
    - [ ] Truncate

- [ ] Rebuild decode utility
- [ ] Test moving file from a Single file to Erasure Format
- [ ] Garbage Collect Filehandles
- [ ] Garbage Collect Cached database entries
- [ ] Copy on Write for Erasure Blocks

## 0.0.3
## 0.0.4
### General
- [ ] https://docs.rs/metrics/
  - [ ] Prometheus Metrics Web Interface?
  - [ ] `/mount_dir/_stats` virtual file system
### Kernel - Block Cache
- [ ] Implement an intelligent eviction policy. Bonus points if it's pluggable. Being able to have the fuse interface provide hints would be nice down the road.
      ref: https://en.wikipedia.org/wiki/Page_replacement_algorithm & https://en.wikipedia.org/wiki/Cache_replacement_policies

## 0.0.5 
- figure out why 2MBs of data takes 3MB of blocks. I'm off by 1 somewhere

## 0.1.0
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
- [X] Test moving file from a Single file to Erasure Format
- [ ] Garbage Collect Filehandles
- [ ] Garbage Collect Cached database entries
- [ ] Copy on Write for Erasure Blocks

cow

for each storage block, there is a writes file. All writes are directed there, and then a background process merges 
them in to the underlying file. 


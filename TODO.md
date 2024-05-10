
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


Refactor.
- StorageBlock becomes a Struct, with buffer as non-serializable fields. Then there is a thread that periodically flushes the buffer.
- VirtualPathBuf can hold it's own file handle. So no more management in the IOEngine

need a way to make threads run at the same interval, but not exactly at the same time. . wrapper around the thread closure...
that can also support a rapid shutdown. . 


---

Kernel is the thing that manages threads and shit. 
```rust

let k = Kernel::new();

k.read(&vpb, buf, offset);

```

VirtualFile. Kernel passes I/O operation to the VirtualFile. VF returns the StorageBlock and offsets for the data. 
Kernel takes the SBs to the SBBufferManager? and passes the I/O operation. The SBBM checks to see if the block is buffered.

Kernel --> BlockBufferMgr
             - keeps open buffers, and thread to flush them
             - keeps open the file handles as well

VirtualFile --> StorageBlock --> VirtualPathBuf
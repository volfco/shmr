A Purpose Built filesystem for use with SMR Hard Drives. 

Unlike a traditional file system, Shmr acts as a Virtual Filesystem on top of a standard file system. Shmr stores files 
as a series of fixed size blocks, and uses an internal key/value store to map the blocks to the underlying storage. 

This enables 'RAID'* like functionality to be implemented at the filesystem level, which makes it a lot easier to deal 
with the complexities of using SMR drives in RAID arrays.

Shmr is designed to act as a tiered filesystem, which requires a high speed, non-volitile, storage pool to act as the
write. Initially, all I/O operations are directed at the write pool. Then in the background, the data is moved to the
slower drives. 

As of `v0.1.0`, Shmr is designed for the following workload:
- Read Often, Write Rarely
- Large Files
- Sequential Access
- A Single SSD Filesystem, that is backed by a ZFS zpool in a RAIDZ2 configuration, is used as the write pool
- An Archive pool made up of XFS formatted SMR drives mounted individually (so /mnt/smr0, /mnt/smr1, etc)

## Features
- Support for mixed size drives in a storage pool. You can combine 1TB drives with 10TB drives in the same pool without issues. 
- Policy Engine to enable programmatic control over the tiring of data; down to the individual file level. Files on different tiers can exist in the same directory. 
- Fine grain throttling of the background processes, so SMR drives don't get overwhelmed. 
- Up to 300MB/s write speed. Could be faster, but fuser isn't multi-threaded... yet. 

## on disk format
There is no modification of the data stored on the filesystem. The original file can be reconstructed by cat-ing the blocks
together in the correct order. 

For Erasure-Encoded files, the blocks are made up of shards. The shard's filename identifies the kind of shard, and the
order of the shard in the block. If the configuration is (3,2), the first 3 shards are the data shards. These can be
cat-ed together in order to form the original block. The parity shards can be used to recover up to two other data 
shards, but you will need a special utility for this. 

NOTE: There might be a few bytes of padding in the last data shard. You need to know the default block size. 
` (length as f32 / data_shards as f32).ceil() as usize`

```rust
// TODO Implement the shard identification by filename
```

## metadata disk format
`sled` is used to store the metadata. All you need to know is that it is an embedded key-value store. 

### inode db
### descriptor db
This database is used to store the contents of the inode; represented as the [`fuse::InodeDescriptor`] enum. 

#### [`fuse::InodeDescriptor::Directory`]
For Directories, the contents are stored as a `BTreeMap<FileName, Inode>`. 

#### [`fuse::InodeDescriptor::File`]
Files are represented as by [`file::VirtualFile`]. This Struct contains the File Size, Chunk Size, Chunk Map, and Block Map.




## unfinished thoughts


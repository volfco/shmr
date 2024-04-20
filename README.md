A Purpose Built filesystem for use with SMR Hard Drives. 





## references
- https://github.com/wfraser/fuse-mt/
- https://github.com/carlosgaldino/gotenksfs


## assumptions
- we are dealing with largely read heavy workload on medium to large sized files
- xfs is the filesystem used for data storage
- the workspace directory is assumed to be a high speed, non-volitile, xfs filesystem
  - xfs here because we might be making a lot of files to buffer data, and for consistency

## thingies
- If the block size does not divide evenly into the required number of data shards, the end of the last data shard is zero filled
- 

## sled keyspace
```plaintext
shmr_0..n      inode file attributes
shmr_0..n_ds   FileDescriptor or DirectoryDescriptor, based on inode file attributes
```

## subsystems
### StorageBlockDB
A key/value store that stores information about individual storage blocks, and the underlying location of the data on disk.

### Superblock
A key/value store that acts more like a filesystem superblock, storing information about the filesystem as a whole. 

A superblock entry is basically [u64: (Inode, InodeDescriptor)], where Inode contains the traditional filesystem 
information, where InodeTopology contains the list of StorageBlocks that map to the actual data that makes up the inode.


## thoughts
- when data is first written, it is written to a single buffer file. SOMETHING HAPPENS and the rules are evaluated to 
  figure out how the data is to be stored. This is where the shard size is determined (if applicable).
- The logic for deciding where writes should go is handled at the Fuse layer. VirtualFile doesn't need to think about
  how to select where to write the data, just select from the given pool. 
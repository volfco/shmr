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

async makes sense here, because we're dealing with file I/O? I'm not sure how multi-threading in fuse works to know if it would benifit. 
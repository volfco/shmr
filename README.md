A Purpose Built psuedo-raid-filesystem for use with SMR Hard Drives. 

Uses a (presumably) SSD as a write-cache, where data is stored until it is written to the underlying harddrives. 

Using FUSE, shmr presents a single virtual filesystem. 



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


## on disk 
```text
/mnt/md0/workspace              Shmr Workspace Directory
  
  /io/[UUID].[Block ID].bin     IO File where writes are written to

/mnt/archives/disk0
  /metadata/                fjall metadata database directory
  /uu/id/uuid               file directory
    /[Block ID]                 Block Directory
      /[Shard ID].bin               Shard File
      
```

##
```text
/proc/shmr              Shmr info in text format
/proc/shmr.json         Shmr info in json format
```
  

## engines
### 

## 0.0.1 
Goal. Full FUSE implementation that only uses the workspace/blocks directory to store data

- [ ] Fully Initialize Filesystem on first run, if no superblock is found
- [ ] Implement FUSE Interface where all File I/O is written to blocks in WORKSPACE/blocks/xx_xx.bin
  - [X] `echo abc123 > mnt/test_file.txt` works
  - [ ] `echo abc123 >> mnt/test_file.txt` works
  - [ ] Block Topology is persisted to the superblock
- [ ] Ensure consistent handling of the block size

## 0.0.2 
Goal. Ability to relocate blocks from the Workspace to the Archive drives, where blocks for a file can exist in both places.

- [ ] Read/Write I/O Operations
- [ ] Implement Offline Program to move a file from the Workspace to the Archive Disks
  -  `shmr_shift --config config.yaml INODE`. This will move the file from the workspace to the archive disks while erasure coding the file.
  - [ ] Create command skeleton
  - [ ] ensure the filesystem is unmounted/offline
  - [ ] Update Inode with new block topology

## 0.0.5
- [ ] Implement basic metrics that get written to some sort of file. like /proc/mdinfo
- [ ] Investigate how multi-threading/parallel processing would work

## 0.1.0
- [ ] `shmr_inspect`
  - [ ] `topology INODE` - Show the block topology of a file
  - [ ] `overview` - Shows information about the general state of the filesystem

## 0.2.0
- [ ] Redesign how storage layers work, so that there can be any number of 
      arbitrary storage layers. So you could have two single layers- i.e. fast and slow workspace
```yaml
pools:
  - name: ssd0
    priority: 0
    drives:
      - /mnt/ssd0-1
      - /mnt/ssd0-2
  - name: ssd1
  - name: dm-smr0
  - name: dm-smr1
``` 
- [ ] Implement some sort of timeseries database to enable dynamic moving of files based on access patterns
- [ ] dbus interface to do dbus stuff
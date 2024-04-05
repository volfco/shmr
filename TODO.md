
## 0.0.1 

- [ ] Implement FUSE Interface where all File I/O is written to blocks in WORKSPACE/blocks/xx_xx.bin
  - [X] `echo abc123 > mnt/test_file.txt` works
  - [ ] `echo abc123 >> mnt/test_file.txt` works
- [ ] Implement Offline Program to move a file from the Workspace to the Archive Disks
  -  `shmr_shift --config config.yaml INODE`. This will move the file from the workspace to the archive disks while erasure coding the file.
  - [ ] Create command skeleton
  - [ ] ensure the filesystem is unmounted/offline
  - [ ] Update Inode with new block topology
- [ ] Ensure consistent handling of the block size

## 0.1.0
- [ ] Rewrite to use https://github.com/Sherlock-Holo/fuse3 or https://github.com/cloud-hypervisor/fuse-backend-rs
- [ ] `shmr_inspect`
  - [ ] `topology INODE` - Show the block topology of a file
  - [ ] `overview` - Shows information about the general state of the filesystem
  - [ ] 

## 0.2.0
- [ ] Redesign how storage layers work, so that there can be any number of 
      arbitrary storage layers. 
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

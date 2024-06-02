
# Goals

# Non-goals
## Efficient Handling of Small Files
Because shmr is a file based redundancy system

## Subsystems

### Block Cache
The Block Cache lazy-loads StorageBlocks that make up VirtualFiles when the block is read.

Blocks remain in cache until one of the following happens:
- All FileHandles are closed
- Block is evicted

Block Evictions take place when the Cache is reaching it's configured memory limit. 


### File Handles
File Handles are used to manage the lifecycle of the block cache for a specific VirtualFile.


VirtualBlocks cannot be modified after creation. You need to create 
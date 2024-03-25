
## assumptions
- we are dealing with largely read heavy workload on medium to large sized files

## thingies
- If the block size does not divide evenly into the required number of data shards, the end of the last data shard is zero filled



writes:
- allocate a block file on the ssd tier
- write contents to the file and update inode to indacate that the block has changes
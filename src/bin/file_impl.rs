// // Goal: Run this to read in a file, and then create a VirtualFile from it.
// //       Then step through moving it from a single buffer file, to one with multiple StorageBlocks.
// //       Then do some I/O operations on it.
// #![feature(trivial_bounds)]
// use anyhow::{bail, Result};
// use log::{info, trace};
// use rkyv::{Archive, Deserialize, Serialize};
// use shmr::storage::StorageBlock;
// use std::collections::HashMap;
// use std::io::{Read, Write};
// use std::ops::RangeInclusive;
// use std::path::PathBuf;
//
// #[derive(Debug, Archive, Serialize, Deserialize, Clone, PartialEq)]
// #[archive(compare(PartialEq), check_bytes)]
// pub struct VirtualFile {
//     base_dir: String,
//
//     /// File size, in bytes
//     #[allow(dead_code)]
//     size: u64,
//
//     /// List of StorageBlocks, in order, that make up the file
//     blocks: Vec<StorageBlock>,
//
//     block_size: u64,
// }
// impl VirtualFile {
//     /// path is the file location, on disk
//     /// block_size is the size of each block, in bytes
//     pub fn open(path: impl Into<PathBuf>) -> Result<Self> {
//         let metadata_file = path.into().join("topology.shmr-v0");
//         if !metadata_file.exists() {
//             bail!("File not found: {:?}", metadata_file);
//         }
//
//         let mut file = std::fs::File::open(&metadata_file)?;
//         let mut buf = vec![];
//         let _ = file.read_to_end(&mut buf)?;
//
//         let archived = rkyv::check_archived_root::<Self>(&buf).unwrap();
//         Ok(archived.deserialize(&mut rkyv::Infallible)?)
//     }
//
//     pub fn save_to_disk(&self) -> Result<()> {
//         let metadata_file = PathBuf::from(&self.base_dir).join("topology.shmr-v0");
//
//         let buf = rkyv::to_bytes::<_, 256>(self)?;
//
//         let mut file = std::fs::File::create(&metadata_file)?;
//         file.write_all(&buf[..])?;
//
//         info!("wrote VirtualFile to disk at {:?}", metadata_file);
//         Ok(())
//     }
//
//     fn pool_map(&self) -> HashMap<String, PathBuf> {
//         // TODO This is dirty. Bad.
//         let mut map = HashMap::new();
//         map.insert("default".to_string(), PathBuf::from(&self.base_dir));
//         map
//     }
//
//     fn calculate_block_range(&self, offset: u64, buf_len: u64) -> RangeInclusive<u64> {
//         let starting_block = match offset > 0 {
//             true => offset / self.block_size,
//             false => 0,
//         };
//         let ending_block = starting_block + (buf_len / self.block_size);
//         starting_block..=ending_block
//     }
//
//     pub fn read(&self, offset: u64, buf: &mut Vec<u8>) -> Result<usize> {
//         // the upper bound of the block range is the total number of blocks we have
//         let block_range = (match offset > 0 {
//             true => offset / self.block_size,
//             false => 0,
//         })..self.blocks.len() as u64;
//
//         trace!("reading blocks: {:?} with offset {}", &block_range, offset);
//
//         let mut read = 0;
//         // keep reading until we can't put anymore into the buffer
//         for block_idx in block_range {
//             let block = self.blocks.get(block_idx as usize).unwrap();
//             let block_offset = (block_idx * self.block_size) - offset;
//             let block_end = block_offset + self.block_size;
//             trace!(
//                 "reading block {} from offset {} to {}",
//                 block_idx,
//                 block_offset,
//                 block_end
//             );
//             read += block.read(&self.pool_map(), block_offset as usize, buf)?;
//         }
//
//         Ok(read)
//     }
//
//     pub fn write(&mut self, offset: u64, buf: &[u8]) -> Result<usize> {
//         // I'm switching back and forth between needing zero indexed data and non-zero indexed data :/
//         let block_range = self.calculate_block_range(offset, buf.len() as u64);
//         info!(
//             "writing {} bytes, starting at offset {}. target block range: {:?}",
//             buf.len(),
//             offset,
//             &block_range
//         );
//
//         let mut written = 0;
//
//         for block_idx in block_range.clone() {
//             if self.blocks.get(block_idx as usize).is_none() {
//                 let pool = self.pool_map();
//                 let sb = StorageBlock::init_single(&pool)?;
//                 sb.create(&pool)?;
//                 self.blocks.push(sb);
//             }
//
//             let block = self.blocks.get(block_idx as usize).unwrap();
//
//             let block_offset = (block_idx * self.block_size) - offset;
//             let mut block_end = block_offset + self.block_size;
//             if block_end > buf.len() as u64 {
//                 block_end = buf.len() as u64;
//             }
//             written += block.write(
//                 &self.pool_map(),
//                 block_offset as usize,
//                 &buf[written..(block_end as usize)],
//             )?;
//         }
//         Ok(written)
//     }
// }
//
// fn main() {}
//
// #[cfg(test)]
// mod tests {
//     use super::*;
//     use shmr::random_data;
//
//     #[test]
//     fn test_virtual_file_write_one_block() {
//         let mut vf = VirtualFile {
//             base_dir: "/tmp".to_string(),
//             size: 0,
//             blocks: vec![],
//             block_size: 1024,
//         };
//
//         let buffer = vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9];
//         let result = vf.write(0, &buffer);
//         assert!(result.is_ok());
//         assert_eq!(result.unwrap(), 10);
//
//         // read it back
//         let mut read_buffer = vec![];
//         let result = vf.read(0, &mut read_buffer);
//         assert!(result.is_ok());
//         assert_eq!(result.unwrap(), 10);
//         assert_eq!(read_buffer, buffer);
//     }
//
//     #[test]
//     fn test_virtual_file_write_two_blocks() {
//         let mut vf = VirtualFile {
//             base_dir: "/tmp".to_string(),
//             size: 0,
//             blocks: vec![],
//             block_size: 128,
//         };
//
//         let buffer = random_data(200);
//         let result = vf.write(0, &buffer);
//         assert!(result.is_ok());
//         assert_eq!(result.unwrap(), 200);
//
//         // read it back
//         let mut read_buffer = vec![];
//         let result = vf.read(0, &mut read_buffer);
//         assert!(result.is_ok());
//         assert_eq!(result.unwrap(), 200);
//         assert_eq!(read_buffer, buffer);
//     }
//     #[test]
//     fn test_virtual_file_write_lots_of_blocks_1() {
//         let mut vf = VirtualFile {
//             base_dir: "/tmp".to_string(),
//             size: 0,
//             blocks: vec![],
//             block_size: 4096,
//         };
//
//         let buffer = random_data(199990);
//         let result = vf.write(0, &buffer);
//         assert!(result.is_ok());
//         assert_eq!(result.unwrap(), 199990);
//
//         // read it back
//         let mut read_buffer = vec![];
//         let result = vf.read(0, &mut read_buffer);
//         assert!(result.is_ok());
//         assert_eq!(result.unwrap(), 199990);
//         assert_eq!(read_buffer, buffer);
//     }
//
//     // test writing the virtualfile to disk, and then read it back as a new object
//     #[test]
//     fn test_virtual_file_save_to_disk() {
//         let base_dir = "/tmp".to_string();
//
//         let mut vf = VirtualFile {
//             base_dir: base_dir.clone(),
//             size: 0,
//             blocks: vec![],
//             block_size: 4096,
//         };
//
//         let buffer = random_data(199990);
//         let result = vf.write(0, &buffer);
//         assert!(result.is_ok());
//         assert_eq!(result.unwrap(), 199990);
//
//         let result = vf.save_to_disk();
//         assert!(result.is_ok());
//
//         // read it back
//         let metadata_file = PathBuf::from(&base_dir).join("topology.shmr-v0");
//         let file = std::fs::File::open(&metadata_file).unwrap();
//         let mut reader = std::io::BufReader::new(file);
//         let mut buf = vec![];
//         reader.read_to_end(&mut buf).unwrap();
//
//         let new_vf = rkyv::from_bytes::<VirtualFile>(&buf).unwrap();
//         assert_eq!(vf, new_vf);
//
//         // initialize a new VirtualFile from disk
//         let nvf = VirtualFile::open(&base_dir);
//         assert!(nvf.is_ok());
//         let nvf = nvf.unwrap();
//         let mut buf1 = vec![0];
//         let mut buf2 = vec![0];
//
//         let _ = nvf.read(0, &mut buf1);
//         let _ = vf.read(0, &mut buf2);
//
//         assert_eq!(buf1, buf2);
//     }
// }
fn main() {}

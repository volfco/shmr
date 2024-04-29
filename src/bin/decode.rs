// Write a program that reads in a file that contains a rkyv struct and prints the contents to the console. The program should accept an argument specifying the type of Struct that is being read in. The two types of structs that need to be supported are Inode and InodeDescriptor
use std::env;
use std::fs::File;
use std::io::Read;
use std::process;
use rkyv::Deserialize;
use shmr::fuse::{Inode, InodeDescriptor};

fn main() {
    let args: Vec<String> = env::args().collect();

    if args.len() != 5 {
        eprintln!("Usage: decode --type <Inode|InodeDescriptor> --path <file.shmr>");
        println!("Args: {:?}", args);
        process::exit(1);
    }

    let struct_type = &args[2];
    let path = &args[4];

    let mut file = File::open(path).expect("Failed to open file");
    let mut buffer = Vec::new();
    file.read_to_end(&mut buffer).expect("Failed to read file");

    match struct_type.as_str() {
        "Inode" => print_inode(&buffer),
        "InodeDescriptor" => print_inode_descriptor(&buffer),
        _ => {
            eprintln!("Unsupported struct type: {}", struct_type);
            process::exit(1);
        }
    }
}

fn print_inode(buffer: &[u8])  {
    unsafe {
        let archived = rkyv::archived_root::<Inode>(&buffer);
        let result: Inode = archived.deserialize(&mut rkyv::Infallible).unwrap();
        println!("{:#?}", result);
    }

}

fn print_inode_descriptor(buffer: &[u8])  {
    unsafe {
        let archived = rkyv::archived_root::<InodeDescriptor>(&buffer);
        let result: InodeDescriptor = archived.deserialize(&mut rkyv::Infallible).unwrap();

        println!("{:#?}", result);
    }
}
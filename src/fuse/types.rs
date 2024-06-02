use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub enum IFileType {
    /// Named pipe (S_IFIFO)
    NamedPipe,
    /// Character device (S_IFCHR)
    CharDevice,
    /// Block device (S_IFBLK)
    BlockDevice,
    /// Directory (S_IFDIR)
    Directory,
    /// Regular file (S_IFREG)
    RegularFile,
    /// Symbolic link (S_IFLNK)
    Symlink,
    /// Unix domain socket (S_IFSOCK)
    Socket,
}
impl IFileType {
    pub fn from_mode(mode: u16) -> IFileType {
        let mut mode = mode;
        mode &= libc::S_IFMT;

        if mode == libc::S_IFREG {
            IFileType::RegularFile
        } else if mode == libc::S_IFLNK {
            IFileType::Symlink
        } else if mode == libc::S_IFDIR {
            IFileType::Directory
        } else {
            unimplemented!("{}", mode);
        }
    }
}
impl From<IFileType> for fuser::FileType {
    fn from(value: IFileType) -> Self {
        match value {
            IFileType::NamedPipe => fuser::FileType::NamedPipe,
            IFileType::CharDevice => fuser::FileType::CharDevice,
            IFileType::BlockDevice => fuser::FileType::BlockDevice,
            IFileType::Directory => fuser::FileType::Directory,
            IFileType::RegularFile => fuser::FileType::RegularFile,
            IFileType::Symlink => fuser::FileType::Symlink,
            IFileType::Socket => fuser::FileType::Socket,
        }
    }
}
impl From<String> for IFileType {
    fn from(value: String) -> Self {
        match value.as_str() {
            "NamedPipe" => IFileType::NamedPipe,
            "CharDevice" => IFileType::CharDevice,
            "BlockDevice" => IFileType::BlockDevice,
            "Directory" => IFileType::Directory,
            "RegularFile" => IFileType::RegularFile,
            "Symlink" => IFileType::Symlink,
            "Socket" => IFileType::Socket,
            _ => panic!("Unexpected value"),
        }
    }
}
impl From<IFileType> for String {
    fn from(value: IFileType) -> Self {
        match value {
            IFileType::NamedPipe => String::from("NamedPipe"),
            IFileType::CharDevice => String::from("CharDevice"),
            IFileType::BlockDevice => String::from("BlockDevice"),
            IFileType::Directory => String::from("Directory"),
            IFileType::RegularFile => String::from("RegularFile"),
            IFileType::Symlink => String::from("Symlink"),
            IFileType::Socket => String::from("Socket"),
        }
    }
}

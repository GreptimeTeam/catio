use libc::O_DIRECT;
use std::fs::File;
use std::fs::OpenOptions;
use std::os::unix::prelude::OpenOptionsExt;
use std::path::Path;

pub fn open_file<P: AsRef<Path>>(path: P) -> File {
    OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .custom_flags(O_DIRECT)
        .open(path)
        .unwrap()
}

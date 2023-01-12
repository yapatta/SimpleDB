use super::blockid::BlockId;
use super::page::Page;
use anyhow::Result;
use std::collections::HashMap;
use std::fmt;
use std::fs;
use std::fs::File;
use std::fs::OpenOptions;
use std::io::prelude::*;
use std::io::SeekFrom;
use std::path::Path;
use std::sync::{Arc, Mutex};

#[derive(Debug)]
enum FileMgrError {
    ParseFailed,
    FileAccessFailed(String),
}

impl std::error::Error for FileMgrError {}
impl fmt::Display for FileMgrError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            FileMgrError::ParseFailed => write!(f, "parse failed"),
            FileMgrError::FileAccessFailed(filename) => {
                write!(f, "file access failed: {}", filename)
            }
        }
    }
}

pub struct FileMgr {
    db_directory: String,
    blocksize: u64,
    is_new: bool,
    open_files: HashMap<String, File>,
    l: Arc<Mutex<()>>,
}

impl FileMgr {
    pub fn new(db_directory: &str, blocksize: u64) -> Result<FileMgr> {
        let path = Path::new(db_directory);
        let is_new = !path.exists();

        if is_new {
            fs::create_dir_all(path)?;
        }

        // remove any leftover temporary tables
        for entry in fs::read_dir(path)? {
            let entry = entry?;
            let epath = entry.path();
            let filename = match entry.file_name().into_string() {
                Ok(s) => s,
                Err(_) => return Err(From::from(FileMgrError::ParseFailed)),
            };

            if filename.starts_with("temp") {
                fs::remove_file(epath)?;
            }
        }

        Ok(FileMgr {
            db_directory: String::from(db_directory),
            blocksize,
            is_new,
            open_files: HashMap::new(),
            l: Arc::new(Mutex::default()),
        })
    }

    // write the content of buf into p
    pub fn read(&mut self, blk: &BlockId, p: &mut Page) -> anyhow::Result<()> {
        if self.l.lock().is_ok() {
            let blocksize = self.blocksize;
            let f = self.configure_file_table(blk.filename())?;

            let offset = blk.number() * blocksize;
            f.seek(SeekFrom::Start(offset))?;

            // ERROR: bytes are not added because of ...
            let read_len = f.read(p.contents())?;

            if read_len < p.contents().len() {
                for i in read_len..p.contents().len() {
                    p.contents()[i] = 0;
                }
            }

            return Ok(());
        }

        Err(From::from(FileMgrError::FileAccessFailed(
            blk.filename().into(),
        )))
    }

    // write all contents of p into the file refered in blk
    pub fn write(&mut self, blk: &BlockId, p: &mut Page) -> anyhow::Result<()> {
        if self.l.lock().is_ok() {
            let blocksize = self.blocksize;
            let f = self.configure_file_table(blk.filename())?;
            let offset = blk.number() * blocksize;
            f.seek(SeekFrom::Start(offset))?;
            f.write_all(p.contents())?;

            return Ok(());
        }

        Err(From::from(FileMgrError::FileAccessFailed(
            blk.filename().into(),
        )))
    }

    // seek to the end of the file and write an empty array of bytes to it
    pub fn append(&mut self, filename: impl Into<String>) -> anyhow::Result<BlockId> {
        let filename = filename.into();
        if self.l.lock().is_ok() {
            let newblknum = self.length(&filename)?;
            let blk = BlockId::new(&filename, newblknum);

            let b: Vec<u8> = vec![0; self.blocksize as usize];

            let blocksize = self.blocksize;
            let f = self.configure_file_table(blk.filename())?;
            f.seek(SeekFrom::Start(blk.number() * blocksize))?;
            f.write_all(&b)?;

            return Ok(blk);
        }

        Err(From::from(FileMgrError::FileAccessFailed(filename)))
    }

    pub fn length(&mut self, filename: impl Into<String>) -> Result<u64> {
        let filename = filename.into();
        let path = Path::new(&self.db_directory).join(&filename);
        let md = fs::metadata(&path)?;

        // ceil
        Ok((md.len() + self.blocksize - 1) / self.blocksize)
    }

    pub fn configure_file_table(
        &mut self,
        filename: impl Into<String>,
    ) -> anyhow::Result<&mut std::fs::File> {
        let filename = filename.into();
        let path = Path::new(&self.db_directory).join(&filename);

        let created_file = self.open_files.entry(filename).or_insert(
            OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .open(&path)?,
        );

        Ok(created_file)
    }

    pub fn blocksize(&self) -> u64 {
        self.blocksize
    }

    pub fn is_new(&self) -> bool {
        self.is_new
    }
}

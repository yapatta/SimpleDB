use super::blockid::BlockId;
use super::page::Page;
use anyhow::Result;
use std::collections::HashMap;
use std::fmt;
use std::fs;
use std::io::prelude::*;
use std::io::BufReader;
use std::io::BufWriter;
use std::io::SeekFrom;
use std::path::Path;
use std::sync::RwLock;
#[derive(Debug)]
enum FileMgrError {
    ParseFailed,
}

impl std::error::Error for FileMgrError {}
impl fmt::Display for FileMgrError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            FileMgrError::ParseFailed => write!(f, "parse failed"),
        }
    }
}

pub struct FileMgr<'a> {
    db_directory: &'a Path,
    blocksize: usize,
    is_new: bool,
    open_files: HashMap<String, String>,
}

impl FileMgr<'_> {
    pub fn new<'a>(db_directory: &'a str, blocksize: usize) -> Result<FileMgr<'a>> {
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
                Err(e) => return Err(From::from(FileMgrError::ParseFailed)),
            };

            if filename.starts_with("temp") {
                fs::remove_file(epath)?;
            }
        }

        Ok(FileMgr {
            db_directory: path,
            blocksize: blocksize,
            is_new: is_new,
            open_files: HashMap::new(),
        })
    }

    pub fn read(&self, blk: &BlockId, p: &mut Page) -> anyhow::Result<()> {
        let f = fs::File::open(blk.filename())?;
        let mut buf = BufReader::new(f);
        // let fl = RwLock::new(buf);

        let offset = blk.number() as usize * self.blocksize;
        buf.seek(SeekFrom::Start(offset as u64))?;
        buf.read(p.contents())?;

        Ok(())
    }

    pub fn write(&mut self, blk: &BlockId, p: &mut Page) -> anyhow::Result<()> {
        let f = fs::File::open(blk.filename())?;
        let mut buf = BufWriter::new(f);

        let offset = blk.number() as usize * self.blocksize;
        buf.seek(SeekFrom::Start(offset as u64))?;
        buf.write(p.contents())?;

        Ok(())
    }
}

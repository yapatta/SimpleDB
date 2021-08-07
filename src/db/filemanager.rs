use super::blockid::BlockId;
use super::page::Page;
use anyhow::Result;
use fs2::FileExt;
use std::collections::HashMap;
use std::fmt;
use std::fs;
use std::fs::File;
use std::io::prelude::*;
use std::io::SeekFrom;
use std::path::Path;

#[derive(Debug)]
enum FileMgrError {
    ParseFailed,
    InternalError,
}

impl std::error::Error for FileMgrError {}
impl fmt::Display for FileMgrError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            FileMgrError::ParseFailed => write!(f, "parse failed"),
            FileMgrError::InternalError => write!(f, "internal errror"),
        }
    }
}

pub struct FileMgr<'a> {
    db_directory: &'a Path,
    blocksize: u64,
    is_new: bool,
    open_files: HashMap<String, File>,
}

impl FileMgr<'_> {
    pub fn new<'a>(db_directory: &'a str, blocksize: u64) -> Result<FileMgr<'a>> {
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
            db_directory: path,
            blocksize: blocksize,
            is_new: is_new,
            open_files: HashMap::new(),
        })
    }

    // bufの内容をpに書き込み
    // fileをLockしたらもれなく他のスレッドが進めないからpのロックはいらない？
    pub fn read(&self, blk: &BlockId, p: &mut Page) -> anyhow::Result<()> {
        let mut f = File::open(blk.filename())?;
        f.lock_exclusive()?;

        let offset = blk.number() * self.blocksize;
        f.seek(SeekFrom::Start(offset))?;
        f.read(p.contents())?;

        f.unlock()?;

        Ok(())
    }

    // pの内容をbufに書き込み
    // fileをLockしたらもれなく他のスレッドが進めないからpのロックはいらない？
    pub fn write(&mut self, blk: &BlockId, p: &mut Page) -> anyhow::Result<()> {
        let mut f = File::open(blk.filename())?;
        f.lock_exclusive()?;

        let offset = blk.number() * self.blocksize;
        f.seek(SeekFrom::Start(offset))?;
        f.write(p.contents())?;

        f.unlock()?;

        Ok(())
    }

    pub fn append(&mut self, filename: String) -> anyhow::Result<BlockId> {
        // FIX: needing O(|S|), find out more efficient solution
        let newblknum = filename.chars().count() as u64;
        let blk = BlockId::new(filename, newblknum);

        let b: Vec<u8> = vec![0; self.blocksize as usize];

        self.configure_file_table(blk.filename())?;

        if let Some(f) = self.open_files.get_mut(&blk.filename()) {
            f.seek(SeekFrom::Start(blk.number() * self.blocksize))?;
            f.write(&b)?;

            return Ok(blk);
        }

        Err(From::from(FileMgrError::InternalError))
    }

    pub fn length(&mut self, filename: String) -> Result<u64> {
        let path = Path::new(self.db_directory).join(&filename);
        self.configure_file_table(filename)?;
        let md = fs::metadata(&path)?;

        Ok(md.len() / self.blocksize)
    }

    pub fn configure_file_table(&mut self, filename: String) -> anyhow::Result<()> {
        let path = Path::new(self.db_directory).join(&filename);

        if !self.open_files.contains_key(&filename) {
            let f = File::create(&path)?;

            // never happen
            if let Some(_) = self.open_files.insert(filename, f) {
                return Err(From::from(FileMgrError::InternalError));
            }
        }

        Ok(())
    }
}

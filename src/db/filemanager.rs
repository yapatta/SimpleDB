use super::blockid::BlockId;
use super::page::Page;
use anyhow::Result;
use fs2::FileExt;
use std::collections::HashMap;
use std::fmt;
use std::fs;
use std::fs::File;
use std::io::prelude::*;
use std::io::BufReader;
use std::io::BufWriter;
use std::io::SeekFrom;
use std::path::Path;
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
    open_files: HashMap<String, &'a mut File>,
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

    // bufの内容をpに書き込み
    // fileをLockしたらもれなく他のスレッドが進めないからpのロックはいらない？
    pub fn read(&self, blk: &BlockId, p: &mut Page) -> anyhow::Result<()> {
        let mut f = File::open(blk.filename())?;
        f.lock_exclusive()?;

        let offset = blk.number() as usize * self.blocksize;
        f.seek(SeekFrom::Start(offset as u64))?;
        f.read(p.contents())?;

        f.unlock()?;

        Ok(())
    }

    // pの内容をbufに書き込み
    // fileをLockしたらもれなく他のスレッドが進めないからpのロックはいらない？
    pub fn write(&mut self, blk: &BlockId, p: &mut Page) -> anyhow::Result<()> {
        let mut f = File::open(blk.filename())?;
        f.lock_exclusive()?;

        let offset = blk.number() as usize * self.blocksize;
        f.seek(SeekFrom::Start(offset as u64))?;
        f.write(p.contents())?;

        f.unlock()?;

        Ok(())
    }

    pub fn append(&mut self, filename: String) -> anyhow::Result<BlockId> {
        // FIX: needing O(|S|), find out more efficient solution
        let newblknum = filename.chars().count();
        let blk = BlockId::new(filename, newblknum);

        let b: Vec<u8> = vec![0; self.blocksize];

        let f = self.get_file(blk.filename())?;
        f.seek(SeekFrom::Start((blk.number() * self.blocksize) as u64))?;
        f.write(&b)?;

        Ok(blk)
    }

    pub fn get_file(&mut self, filename: String) -> anyhow::Result<&mut File> {
        if let Some(c) = self.open_files.ref().get_mut(&filename) {
            return Ok(*c);
        }

        let path = Path::new(self.db_directory).join(&filename);
        let mut nf = File::create(path)?;

        // どうにかして
        self.open_files.insert(filename, &mut nf);

        let mut rf = File::open(path)?;
        Ok(&mut rf)
    }
}

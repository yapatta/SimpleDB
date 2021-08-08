use super::blockid::BlockId;
use super::filemanager::FileMgr;
use super::page::Page;

use anyhow::Result;
use std::mem;

pub struct LogIterator<'a> {
    fm: &'a mut FileMgr<'a>,
    blk: BlockId,
    p: Page,
    currentpos: u64,
    boundary: u64,
}

impl Iterator for LogIterator<'_> {
    type Item = [u8];
    fn next(&mut self) -> Option<&Self::Item> {
        if self.currentpos == self.fm.blocksize() {
            self.blk = BlockId::new(&self.blk.filename(), self.blk.number() - 1);

            if let Err(_) = self.move_to_block(self.blk) {
                return None;
            }
        }
        if let Ok(rec) = self.p.get_bytes(self.currentpos as usize) {
            let i32_size = mem::size_of::<i32>() as u64;
            self.currentpos += i32_size;
            self.currentpos += i32_size + rec.len() as u64;

            return Some(rec);
        }

        None
    }
}

impl LogIterator<'_> {
    fn move_to_block(&mut self, blk: BlockId) -> Result<()> {
        self.fm.read(&mut blk, &mut self.p)?;
        self.boundary = self.p.get_int(0)? as u64;
        self.currentpos = self.boundary;

        Ok(())
    }

    pub fn has_next(&self) -> bool {
        self.currentpos < self.fm.blocksize() || self.blk.number() > 0
    }
}

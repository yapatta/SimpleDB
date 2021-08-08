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
    type Item = Vec<u8>;
    fn next(&mut self) -> Option<Self::Item> {
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

            return Some(rec.into_vec());
        }

        None
    }
}

impl LogIterator<'_> {
    pub fn new<'a>(fm: &'a mut FileMgr<'a>, blk: BlockId) -> Result<LogIterator<'a>> {
        let mut page = Page::new_from_size(fm.blocksize() as usize);

        fm.read(&mut blk, &mut page)?;
        let boundary = page.get_int(0)? as u64;
        let currentpos = boundary;

        Ok(LogIterator {
            fm: fm,
            blk: blk,
            p: page,
            currentpos: currentpos,
            boundary: boundary,
        })
    }

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

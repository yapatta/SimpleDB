use super::blockid::BlockId;
use super::filemanager::FileMgr;
use super::page::Page;

use anyhow::Result;
use std::mem;

pub struct LogIterator<'a> {
    fm: &'a mut FileMgr<'a>,
    blk: &'a mut BlockId,
    p: Page,
    currentpos: u64,
    boundary: u64,
}

impl LogIterator<'_> {
    pub fn new<'a>(fm: &'a mut FileMgr<'a>, blk: &'a mut BlockId) -> Result<LogIterator<'a>> {
        let mut page = Page::new_from_size(fm.blocksize() as usize);

        fm.read(blk, &mut page)?;
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

    fn next(&mut self) -> Option<&[u8]> {
        if self.currentpos == self.fm.blocksize() {
            *self.blk = BlockId::new(&self.blk.filename(), self.blk.number() - 1);

            if let Err(_) = self.fm.read(&mut self.blk, &mut self.p) {
                return None;
            }

            if let Ok(n) = self.p.get_int(0) {
                self.boundary = n as u64;
                self.currentpos = self.boundary;
            } else {
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

    pub fn has_next(&self) -> bool {
        self.currentpos < self.fm.blocksize() || self.blk.number() > 0
    }
}

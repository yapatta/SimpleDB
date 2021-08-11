use super::blockid::BlockId;
use super::constants::BLOCKSIZE;
use super::filemanager::FileMgr;
use super::page::Page;

use anyhow::Result;
use std::mem;

pub struct LogIterator<'a, 'b> {
    fm: &'a mut FileMgr<'b>,
    blk: &'a mut BlockId,
    p: Page,
    currentpos: u64,
    boundary: u64,
}

impl Iterator for LogIterator<'_, '_> {
    type Item = [u8; BLOCKSIZE as usize];
    fn next(&mut self) -> Option<Self::Item> {
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
        if let Ok(rec) = self.p.get_bytes_array(self.currentpos as usize) {
            let i32_size = mem::size_of::<i32>() as u64;
            self.currentpos += i32_size;
            self.currentpos += i32_size + rec.len() as u64;

            return Some(rec);
        }

        None
    }
}

impl LogIterator<'_, '_> {
    pub fn new<'a, 'b>(
        fm: &'a mut FileMgr<'b>,
        blk: &'a mut BlockId,
    ) -> Result<LogIterator<'a, 'b>> {
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

    pub fn has_next(&self) -> bool {
        self.currentpos < self.fm.blocksize() || self.blk.number() > 0
    }
}

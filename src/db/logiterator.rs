use super::blockid::BlockId;
use super::filemanager::FileMgr;
use super::page::Page;

use anyhow::Result;
use std::cell::RefCell;
use std::mem;
use std::sync::Arc;

pub struct LogIterator {
    fm: Arc<RefCell<FileMgr>>,
    blk: BlockId,
    p: Page,
    currentpos: u64,
    boundary: u64,
}

impl LogIterator {
    pub fn new(fm: Arc<RefCell<FileMgr>>, blk: BlockId) -> Result<LogIterator> {
        let mut p = Page::new_from_size(fm.borrow().blocksize() as usize);

        fm.borrow_mut().read(&blk, &mut p)?;
        let boundary = p.get_int(0)? as u64;
        let currentpos = boundary;

        Ok(LogIterator {
            fm,
            blk,
            p,
            currentpos,
            boundary,
        })
    }

    pub fn has_next(&self) -> bool {
        self.currentpos < self.fm.borrow().blocksize() || self.blk.number() > 0
    }
}

impl Iterator for LogIterator {
    type Item = Vec<u8>;

    fn next(&mut self) -> Option<Self::Item> {
        if !self.has_next() {
            return None;
        }

        if self.currentpos == self.fm.borrow().blocksize() {
            self.blk = BlockId::new(self.blk.filename(), self.blk.number() - 1);

            if self.fm.borrow_mut().read(&self.blk, &mut self.p).is_err() {
                return None;
            }

            if let Ok(n) = self.p.get_int(0) {
                self.boundary = n as u64;
                self.currentpos = self.boundary;
            } else {
                return None;
            }
        }

        if let Ok(rec) = self.p.get_bytes_vec(self.currentpos as usize) {
            let i32_size = mem::size_of::<i32>() as u64;
            self.currentpos += i32_size + rec.len() as u64;

            return Some(rec);
        }

        None
    }
}

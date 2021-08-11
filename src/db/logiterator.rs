use super::blockid::BlockId;
use super::constants::BLOCKSIZE;
use super::filemanager::FileMgr;
use super::page::Page;

use anyhow::Result;
use std::cell::RefCell;
use std::mem;
use std::rc::Rc;

pub struct LogIterator {
    fm: Rc<RefCell<FileMgr>>,
    blk: BlockId,
    p: Page,
    currentpos: u64,
    boundary: u64,
}

impl LogIterator {
    pub fn new<'a>(fm: Rc<RefCell<FileMgr>>, mut blk: BlockId) -> Result<LogIterator> {
        let mut page = Page::new_from_size(fm.borrow().blocksize() as usize);

        fm.borrow_mut().read(&mut blk, &mut page)?;
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
        self.currentpos < self.fm.borrow().blocksize() || self.blk.number() > 0
    }
}

impl Iterator for LogIterator {
    type Item = [u8; BLOCKSIZE as usize];

    fn next(&mut self) -> Option<Self::Item> {
        if self.currentpos == self.fm.borrow().blocksize() {
            self.blk = BlockId::new(&self.blk.filename(), self.blk.number() - 1);

            if let Err(_) = self.fm.borrow_mut().read(&mut self.blk, &mut self.p) {
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

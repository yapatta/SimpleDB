use super::blockid::BlockId;
use super::filemanager::FileMgr;
use super::logmanager::LogMgr;
use super::page::Page;

use std::cell::RefCell;
use std::fmt;
use std::rc::Rc;

use anyhow::Result;

#[derive(Debug)]
enum BufferError {
    BlockNotFound,
}

impl std::error::Error for BufferError {}
impl fmt::Display for BufferError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            BufferError::BlockNotFound => {
                write!(f, "block not found")
            }
        }
    }
}

pub struct Buffer {
    fm: Rc<RefCell<FileMgr>>,
    lm: Rc<RefCell<LogMgr>>,
    contents: Page,
    blk: Option<BlockId>, // reference to the block assigned to its page
    pins: u64,            // the number of times the page is pinned
    txnum: i32, // an integer indicating if the page has been modified. The integer indentifies the transaction that make the change
    lsn: i32, // log information. if the page has been modified, the buffer holds the LSN of the most recent log record.
}

impl Buffer {
    pub fn new(fm: Rc<RefCell<FileMgr>>, lm: Rc<RefCell<LogMgr>>) -> Buffer {
        let blksize = fm.borrow().blocksize() as usize;

        Buffer {
            fm,
            lm,
            contents: Page::new_from_size(blksize),
            blk: None,
            pins: 0,
            txnum: -1,
            lsn: -1,
        }
    }

    pub fn contents(&mut self) -> &mut Page {
        &mut self.contents
    }

    pub fn block(&self) -> Option<&BlockId> {
        self.blk.as_ref()
    }

    pub fn set_modified(&mut self, txnum: i32, lsn: i32) {
        self.txnum = txnum;
        if lsn >= 0 {
            self.lsn = lsn;
        }
    }

    pub fn is_pinned(&self) -> bool {
        self.pins > 0
    }

    pub fn modifying_tx(&self) -> i32 {
        self.txnum
    }

    // associate the buffer with the specific block, reading its content from disk
    pub fn assign_to_block(&mut self, b: BlockId) -> Result<()> {
        println!("before flush");
        self.flush()?;
        println!("before read");
        self.fm.borrow_mut().read(&b, &mut self.contents)?;
        println!("after read");
        self.blk = Some(b);
        self.pins = 0;

        Ok(())
    }

    // ensure the buffer's assigned disk block has the same values as its page
    pub fn flush(&mut self) -> Result<()> {
        // page has been changed
        if self.txnum >= 0 {
            self.lm.borrow_mut().flush_from_lsn(self.lsn as u64)?;

            if let Some(br) = self.blk.as_ref() {
                self.fm.borrow_mut().write(br, &mut self.contents)?;
                // need not flush again
                self.txnum = -1;
            } else {
                return Err(From::from(BufferError::BlockNotFound));
            }
        }

        Ok(())
    }

    pub fn pin(&mut self) {
        self.pins += 1;
    }

    pub fn unpin(&mut self) {
        self.pins -= 1;
    }
}

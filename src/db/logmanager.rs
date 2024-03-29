use super::blockid::BlockId;
use super::filemanager::FileMgr;
use super::logiterator::LogIterator;
use super::page::Page;

use anyhow::Result;
use std::cell::RefCell;
use std::mem;
use std::sync::Arc;

pub struct LogMgr {
    fm: Arc<RefCell<FileMgr>>,
    logfile: String,
    logpage: Page,
    currentblk: BlockId,
    latest_lsn: u64,
    lastsaved_lsn: u64,
}

impl LogMgr {
    pub fn new(fm: Arc<RefCell<FileMgr>>, logfile: String) -> Result<LogMgr> {
        let mut logpage = Page::new_from_size(fm.borrow().blocksize() as usize);
        let logsize = fm.borrow_mut().length(logfile.clone())?;

        let logmgr;

        if logsize == 0 {
            let blk = fm.borrow_mut().append(&logfile)?;
            logpage.set_int(0, fm.borrow().blocksize() as i32)?;
            fm.borrow_mut().write(&blk, &mut logpage)?;

            logmgr = LogMgr {
                fm,
                logfile,
                logpage,
                currentblk: blk,
                latest_lsn: 0,
                lastsaved_lsn: 0,
            };
        } else {
            let newblk = BlockId::new(&logfile, logsize - 1);
            fm.borrow_mut().read(&newblk, &mut logpage)?;

            logmgr = LogMgr {
                fm,
                logfile,
                logpage,
                currentblk: newblk,
                latest_lsn: 0,
                lastsaved_lsn: 0,
            };
        };

        Ok(logmgr)
    }

    // ブロック内に空き容量があればログを追加, なければ新しいブロックを作成してログ追加
    // TODO: implement thread safe func
    pub fn append(&mut self, logrec: &mut Vec<u8>) -> Result<u64> {
        let mut boundary = self.logpage.get_int(0)?;
        let recsize = logrec.len() as i32;
        let int32_size = mem::size_of::<i32>() as i32;
        let bytesneeded = recsize + int32_size;

        if boundary - bytesneeded < int32_size {
            self.flush()?;

            self.currentblk = self.append_newblk()?;
            boundary = self.logpage.get_int(0)?;
        }

        let recpos = (boundary - bytesneeded) as usize;
        self.logpage.set_bytes(recpos, logrec)?;
        self.logpage.set_int(0, recpos as i32)?;
        self.latest_lsn += 1;

        Ok(self.lastsaved_lsn)
    }

    pub fn iterator(&mut self) -> Result<LogIterator> {
        self.flush()?;
        let iter = LogIterator::new(Arc::clone(&self.fm), self.currentblk.clone())?;

        Ok(iter)
    }

    pub fn flush_from_lsn(&mut self, lsn: u64) -> Result<()> {
        if lsn > self.lastsaved_lsn {
            self.flush()?;
        }

        Ok(())
    }

    fn flush(&mut self) -> Result<()> {
        self.fm
            .borrow_mut()
            .write(&self.currentblk, &mut self.logpage)?;
        self.lastsaved_lsn = self.latest_lsn;

        Ok(())
    }

    fn append_newblk(&mut self) -> Result<BlockId> {
        let blk = self.fm.borrow_mut().append(&self.logfile)?;
        self.logpage
            .set_int(0, self.fm.borrow().blocksize() as i32)?;
        self.fm.borrow_mut().write(&blk, &mut self.logpage)?;

        Ok(blk)
    }
}

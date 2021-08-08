use super::blockid::BlockId;
use super::filemanager::FileMgr;
use super::logiterator::LogIterator;
use super::page::Page;

use anyhow::Result;
use std::mem;

pub struct LogMgr<'a> {
    fm: &'a mut FileMgr<'a>,
    logfile: String,
    logpage: Page,
    currentblk: BlockId,
    latest_lsn: u64,
    lastsaved_lsn: u64,
}

impl LogMgr<'_> {
    pub fn new<'a>(fm: &'a mut FileMgr<'a>, logfile: String) -> Result<LogMgr<'a>> {
        let mut logpage = Page::new_from_size(fm.blocksize() as usize);
        let logsize = fm.length(logfile.clone())?;

        if logsize == 0 {
            let mut blk = fm.append(logfile.clone())?;
            logpage.set_int(0, fm.blocksize() as i32)?;
            fm.write(&mut blk, &mut logpage)?;

            return Ok(LogMgr {
                fm: fm,
                logfile: logfile,
                logpage: logpage,
                currentblk: blk,
                latest_lsn: 0,
                lastsaved_lsn: 0,
            });
        } else {
            let mut newblk = BlockId::new(&logfile, logsize - 1);
            fm.read(&mut newblk, &mut logpage)?;

            return Ok(LogMgr {
                fm: fm,
                logfile: logfile,
                logpage: logpage,
                currentblk: newblk,
                latest_lsn: 0,
                lastsaved_lsn: 0,
            });
        };
    }

    // ブロック内に空き容量があればログを追加, なければ新しいブロックを作成してログ追加
    // TODO: implement thread safe func
    pub fn append(&mut self, logrec: &mut Vec<u8>) -> Result<u64> {
        let mut boundary = self.logpage.get_int(0)?;
        let recsize = logrec.len();
        let int32_size = mem::size_of::<i32>();
        let bytesneeded = recsize + int32_size;

        if boundary as usize - bytesneeded < int32_size {
            self.flush()?;

            self.currentblk = self.append_newblk()?;
            boundary = self.logpage.get_int(0)?;
        }

        let recpos = boundary as usize - bytesneeded;
        self.logpage.set_bytes(recpos, logrec)?;
        self.logpage.set_int(0, recpos as i32)?;
        self.latest_lsn += 1;

        Ok(self.lastsaved_lsn)
    }

    pub fn iterator(&mut self) -> Result<LogIterator> {
        self.flush()?;

        let t = LogIterator::new(self.fm, self.currentblk)?;
        t.next();
    }

    pub fn flush_from_lsn(&mut self, lsn: u64) -> Result<()> {
        if lsn > self.lastsaved_lsn {
            self.flush()?;
        }

        Ok(())
    }

    fn flush(&mut self) -> Result<()> {
        self.fm.write(&mut self.currentblk, &mut self.logpage)?;
        self.lastsaved_lsn = self.latest_lsn;

        Ok(())
    }

    fn append_newblk(&mut self) -> Result<BlockId> {
        let mut blk = self.fm.append(self.logfile.clone())?;
        self.logpage.set_int(0, self.fm.blocksize() as i32)?;
        self.fm.write(&mut blk, &mut self.logpage)?;

        Ok(blk)
    }
}

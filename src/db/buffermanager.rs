use super::blockid::BlockId;
use super::buffer::Buffer;
use super::filemanager::FileMgr;
use super::logmanager::LogMgr;

use std::cell::RefCell;
use std::fmt;
use std::rc::Rc;
use std::sync::{Arc, Mutex, RwLock};

use anyhow::Result;

#[derive(Debug)]
enum BufferMgrError {
    LockFailed(String),
}

impl std::error::Error for BufferMgrError {}
impl fmt::Display for BufferMgrError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            BufferMgrError::LockFailed(s) => {
                write!(f, "lock failed: {}", s)
            }
        }
    }
}

pub struct BufferMgr {
    bufferpool: Arc<RwLock<Vec<Buffer>>>,
    num_available: Arc<Mutex<usize>>,
}

impl BufferMgr {
    #[inline]
    pub fn max_time() -> i32 {
        10000 // 10 sec
    }

    pub fn new(fm: Rc<RefCell<FileMgr>>, lm: Rc<RefCell<LogMgr>>, numbuffs: usize) -> BufferMgr {
        let bufferpool: Vec<Buffer> = (0..numbuffs)
            .map(|_| Buffer::new(Rc::clone(&fm), Rc::clone(&lm)))
            .collect();

        BufferMgr {
            bufferpool: Arc::new(RwLock::new(bufferpool)),
            num_available: Arc::new(Mutex::new(numbuffs)),
        }
    }

    pub fn flush_all(&mut self, txnum: i32) -> Result<()> {
        if let Ok(mut bufferpool) = self.bufferpool.write() {
            for i in 0..bufferpool.len() {
                if bufferpool[i].modifying_tx() == txnum {
                    bufferpool[i].flush()?;
                }
            }
        }

        Ok(())
    }

    pub fn unpin(&mut self, buff: &mut Buffer) -> Result<()> {
        buff.unpin();

        if !buff.is_pinned() {
            if let Ok(mut num_available) = self.num_available.lock() {
                *num_available += 1;
            } else {
                return Err(From::from(BufferMgrError::LockFailed(String::from(
                    "num_available",
                ))));
            }
        }

        Ok(())
    }

    // TODO: 燃え尽きた、あとで
    pub fn pin(&mut self, blk: &BlockId) {
        //if self.try_to_pin(blk).is_some() {
        //
        //      }
    }

    pub fn try_to_pin(&mut self, blk: &BlockId) -> Option<usize> {
        let mut buff_index: i32 = -1;
        if let Some(i) = self.find_existing_buffer(blk) {
            buff_index = i as i32;
        } else {
            if let Some(i) = self.choose_unpinned_buffer() {
                buff_index = i as i32;

                if let Ok(mut bufferpool) = self.bufferpool.write() {
                    if bufferpool[i].assign_to_block(blk.clone()).is_err() {
                        return None;
                    }
                }
            }
        }

        if buff_index >= 0 {
            if let Ok(mut bufferpool) = self.bufferpool.write() {
                bufferpool[buff_index as usize].pin();

                return Some(buff_index as usize);
            }
        }

        None
    }

    pub fn find_existing_buffer(&mut self, blk: &BlockId) -> Option<usize> {
        if let Ok(bufferpool) = self.bufferpool.read() {
            for i in 0..bufferpool.len() {
                if let Some(b) = bufferpool[i].block() {
                    if *b == *blk {
                        return Some(i);
                    }
                }
            }
        }

        None
    }

    pub fn choose_unpinned_buffer(&mut self) -> Option<usize> {
        if let Ok(bufferpool) = self.bufferpool.read() {
            for i in 0..bufferpool.len() {
                if !bufferpool[i].is_pinned() {
                    return Some(i);
                }
            }
        }

        None
    }
}

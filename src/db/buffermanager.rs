use super::blockid::BlockId;
use super::buffer::Buffer;
use super::filemanager::FileMgr;
use super::logmanager::LogMgr;

use std::cell::RefCell;
use std::fmt;
use std::rc::Rc;
use std::sync::{Arc, Mutex};
use std::thread::sleep;
use std::time::{Duration, SystemTime};

use anyhow::Result;

const MAX_TIME: i64 = 10000;

#[derive(Debug)]
enum BufferMgrError {
    LockFailed(String),
    BufferAbort,
}

impl std::error::Error for BufferMgrError {}
impl fmt::Display for BufferMgrError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            BufferMgrError::LockFailed(s) => {
                write!(f, "lock failed function: {}", s)
            }
            BufferMgrError::BufferAbort => {
                write!(f, "buffer abort")
            }
        }
    }
}

pub struct BufferMgr {
    bufferpool: Vec<Buffer>,
    num_available: usize,
    l: Arc<Mutex<()>>,
}

impl BufferMgr {
    pub fn new(fm: Rc<RefCell<FileMgr>>, lm: Rc<RefCell<LogMgr>>, numbuffs: usize) -> BufferMgr {
        let bufferpool: Vec<Buffer> = (0..numbuffs)
            .map(|_| Buffer::new(Rc::clone(&fm), Rc::clone(&lm)))
            .collect();

        BufferMgr {
            bufferpool,
            num_available: numbuffs,
            l: Arc::new(Mutex::default()),
        }
    }

    pub fn available(&self) -> usize {
        self.num_available
    }

    pub fn flush_all(&mut self, txnum: i32) -> Result<()> {
        if self.l.lock().is_ok() {
            for i in 0..self.bufferpool.len() {
                if self.bufferpool[i].modifying_tx() == txnum {
                    self.bufferpool[i].flush()?;
                }
            }
        }

        Err(From::from(BufferMgrError::LockFailed(
            "flush_all".to_string(),
        )))
    }

    pub fn unpin(&mut self, buff_index: usize) -> Result<()> {
        if self.l.lock().is_ok() {
            self.bufferpool[buff_index].unpin();

            if !self.bufferpool[buff_index].is_pinned() {
                self.num_available += 1;
            }

            return Ok(());
        }

        Err(From::from(BufferMgrError::LockFailed(String::from(
            "unpin",
        ))))
    }

    pub fn pin(&mut self, blk: &BlockId) -> Result<usize> {
        if self.l.lock().is_ok() {
            let timestamp = SystemTime::now();

            while !waiting_too_long(timestamp) {
                if let Some(b) = self.try_to_pin(blk) {
                    return Ok(b);
                }
                sleep(Duration::new(2, 0));
            }

            return Err(From::from(BufferMgrError::BufferAbort));
        }

        Err(From::from(BufferMgrError::BufferAbort))
    }

    pub fn try_to_pin(&mut self, blk: &BlockId) -> Option<usize> {
        let mut buff_index: i32 = -1;
        if let Some(i) = self.find_existing_buffer(blk) {
            buff_index = i as i32;
        } else if let Some(i) = self.choose_unpinned_buffer() {
            buff_index = i as i32;

            if self.bufferpool[i].assign_to_block(blk.clone()).is_err() {
                return None;
            }
        }

        if buff_index >= 0 {
            self.bufferpool[buff_index as usize].pin();

            return Some(buff_index as usize);
        }

        None
    }

    pub fn find_existing_buffer(&mut self, blk: &BlockId) -> Option<usize> {
        for i in 0..self.bufferpool.len() {
            if let Some(b) = self.bufferpool[i].block() {
                if *b == *blk {
                    return Some(i);
                }
            }
        }

        None
    }

    pub fn choose_unpinned_buffer(&mut self) -> Option<usize> {
        for i in 0..self.bufferpool.len() {
            if !self.bufferpool[i].is_pinned() {
                return Some(i);
            }
        }

        None
    }
}

fn waiting_too_long(starttime: SystemTime) -> bool {
    let now = SystemTime::now();
    let diff = now.duration_since(starttime).unwrap();

    diff.as_millis() as i64 > MAX_TIME // 10 secs
}

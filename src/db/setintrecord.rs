use super::blockid::BlockId;
use super::logmanager::LogMgr;
use super::logrecord::SETINT;
use super::page::Page;

use std::cell::RefCell;
use std::fmt;
use std::mem;
use std::sync::Arc;

use anyhow::Result;

pub struct SetIntRecord {
    txnum: i32,
    offset: i32,
    val: i32,
    blk: BlockId,
}

impl fmt::Display for SetIntRecord {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "<SETINT {} {} {} {}>",
            self.txnum, self.blk, self.offset, self.val
        )
    }
}

/**
 *          tpos   fpos            bpos     opos    vpos
 * | SetInt | txnum |   filename   | blknum | offset | val |
 *    int       int   int + stirng    int　　　int     int
 **/
impl SetIntRecord {
    pub fn new(p: &Page) -> Result<SetIntRecord> {
        let tpos = mem::size_of::<i32>();
        let txnum = p.get_int(tpos)?;
        let fpos = tpos + mem::size_of::<i32>();
        let filename = p.get_string(fpos)?;
        let bpos = fpos + Page::max_length(filename.len());
        let blknum = p.get_int(bpos)?;
        let blk = BlockId::new(&filename, blknum as u64);
        let opos = bpos + mem::size_of::<i32>();
        let offset = p.get_int(opos)?;
        let vpos = opos + mem::size_of::<i32>();
        let val = p.get_int(vpos)?;

        Ok(SetIntRecord {
            txnum,
            offset,
            val,
            blk,
        })
    }

    pub fn op(&self) -> i32 {
        SETINT
    }

    pub fn tx_number(&self) -> i32 {
        self.txnum
    }

    pub fn write_to_log(
        lm: Arc<RefCell<LogMgr>>,
        txnum: i32,
        blk: BlockId,
        offset: i32,
        val: i32,
    ) -> Result<u64> {
        let tpos = mem::size_of::<i32>();
        let fpos = tpos + mem::size_of::<i32>();
        let bpos = fpos + Page::max_length(blk.filename().len());
        let opos = bpos + mem::size_of::<i32>();
        let vpos = opos + mem::size_of::<i32>();
        let reclen = vpos + mem::size_of::<i32>();

        let mut p = Page::new_from_size(reclen as usize);
        p.set_int(0, SETINT)?;
        p.set_int(tpos, txnum)?;
        p.set_string(fpos, blk.filename())?;
        p.set_int(bpos, blk.number() as i32)?;
        p.set_int(opos, offset)?;
        p.set_int(vpos, val)?;

        lm.borrow_mut().append(p.contents())
    }
}

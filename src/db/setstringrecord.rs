use super::blockid::BlockId;
use super::logmanager::LogMgr;
use super::logrecord::SETSTRING;
use super::page::Page;

use std::cell::RefCell;
use std::fmt;
use std::mem;
use std::sync::Arc;

use anyhow::Result;

pub struct SetStringRecord {
    txnum: i32,
    offset: i32,
    val: String,
    blk: BlockId,
}

impl fmt::Display for SetStringRecord {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "<SETSTRING {} {} {} {}>",
            self.txnum, self.blk, self.offset, self.val
        )
    }
}

/**
 *            tpos   fpos            bpos     opos     vpos
 * | SetString | txnum |   filename   | blknum | offset |      val     |
 *      int       int    int + stirng    int      int     int + string
 **/
impl SetStringRecord {
    pub fn new(p: &Page) -> Result<SetStringRecord> {
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
        let val = p.get_string(vpos)?;

        Ok(SetStringRecord {
            txnum,
            offset,
            val,
            blk,
        })
    }

    pub fn op(&self) -> i32 {
        SETSTRING
    }

    pub fn tx_number(&self) -> i32 {
        self.txnum
    }

    pub fn write_to_log(
        lm: Arc<RefCell<LogMgr>>,
        txnum: i32,
        blk: BlockId,
        offset: i32,
        val: String,
    ) -> Result<u64> {
        let tpos = mem::size_of::<i32>();
        let fpos = tpos + mem::size_of::<i32>();
        let bpos = fpos + Page::max_length(blk.filename().len());
        let opos = bpos + mem::size_of::<i32>();
        let vpos = opos + mem::size_of::<i32>();
        let reclen = vpos + Page::max_length(val.len());

        let mut p = Page::new_from_size(reclen as usize);
        p.set_int(0, SETSTRING)?;
        p.set_int(tpos, txnum)?;
        p.set_string(fpos, blk.filename())?;
        p.set_int(bpos, blk.number() as i32)?;
        p.set_int(opos, offset)?;
        p.set_string(vpos, val)?;

        lm.borrow_mut().append(p.contents())
    }
}

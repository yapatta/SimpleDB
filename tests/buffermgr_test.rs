use simple_db::blockid::BlockId;
use simple_db::buffer::Buffer;
use simple_db::buffermanager::BufferMgr;
use simple_db::filemanager::FileMgr;
use simple_db::logmanager::LogMgr;

use std::cell::RefCell;
use std::mem;
use std::rc::Rc;

use anyhow::Result;

#[test]
fn buffermgr_test() {
    let fm = FileMgr::new("./buffertests", 400).unwrap();
    let fmrc = Rc::new(RefCell::new(fm));
    let fmrc2 = Rc::clone(&fmrc);
    let mut lm = LogMgr::new(fmrc, String::from("bufferfile")).unwrap();
    let mut bm = BufferMgr::new(fmrc2, Rc::new(RefCell::new(lm)), 3);

    println!("Available buffers: {}", bm.available());
    let b0 = BlockId::new("bufferfile", 0);
    let n0 = bm.pin(&b0).unwrap();
    bm.unpin(0).unwrap();
    println!("Available buffers: {}", bm.available());
    let b1 = BlockId::new("bufferfile", 1);
    let n1 = bm.pin(&b1).unwrap();
    let b2 = bm.pin(&BlockId::new("bufferfile", 2)).unwrap();
    bm.unpin(1).unwrap();
    let b3 = bm.pin(&BlockId::new("bufferfile", 0)).unwrap();
    let b4 = bm.pin(&BlockId::new("bufferfile", 1)).unwrap();
    println!("Available buffers: {}", bm.available());
    println!("Attempting to pin block 3...");
    assert_eq!(0, 1);
}

use simple_db::blockid::BlockId;
use simple_db::buffermanager::BufferMgr;
use simple_db::filemanager::FileMgr;
use simple_db::logmanager::LogMgr;

use std::cell::RefCell;
use std::sync::Arc;

#[test]
fn buffermgr_test() {
    let fm = FileMgr::new("./buffertests", 400).unwrap();
    let fmrc = Arc::new(RefCell::new(fm));
    let fmrc2 = Arc::clone(&fmrc);
    let lm = LogMgr::new(fmrc, String::from("bufferfile")).unwrap();
    let mut bm = BufferMgr::new(fmrc2, Arc::new(RefCell::new(lm)), 3);

    println!("Available buffers: {}", bm.available());

    // pos: 0, buffer: 0
    let n0 = bm.pin(&BlockId::new("bufferfile", 0)).unwrap();

    // pos: 1, buffer: 1
    let n1 = bm.pin(&BlockId::new("bufferfile", 1)).unwrap();

    // pos: 2, buffer: 2
    let n2 = bm.pin(&BlockId::new("bufferfile", 2)).unwrap();

    // pos: 1, buffer: none
    bm.unpin(Arc::clone(&n1)).unwrap();

    // pos: 0, buffer: 0
    let _n3 = bm.pin(&BlockId::new("bufferfile", 0)).unwrap();

    // pos: 1, buffer: 1
    let n4 = bm.pin(&BlockId::new("bufferfile", 1)).unwrap();

    // pos: buffer
    //   0:      0
    //   1:      1
    //   2:      2
    println!("Available buffers: {}", bm.available());
    println!("Attempting to pin block 3...");
    match bm.pin(&BlockId::new("bufferfile", 3)) {
        Ok(_) => {}
        Err(e) => {
            eprintln!("Exception: No available buffers. {}", e);
        }
    };

    // pos: 2, buffer: none
    bm.unpin(n2).unwrap();

    // pos: buffer
    //   0:      0
    //   1:      1
    //   2:      3
    let n5 = bm.pin(&BlockId::new("bufferfile", 3)).unwrap();

    println!("Final Buffer Allocation");

    assert_eq!(n0.borrow().block().unwrap().number(), 0);
    assert_eq!(n4.borrow().block().unwrap().number(), 1);
    assert_eq!(n5.borrow().block().unwrap().number(), 3);
}

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

    // 0 . .
    let b0 = BlockId::new("bufferfile", 0);
    let _n0 = bm.pin(&b0).unwrap();

    // 0 1 .
    let b1 = BlockId::new("bufferfile", 1);
    let n1 = bm.pin(&b1).unwrap();

    // 0 1 2
    let b2 = BlockId::new("bufferfile", 2);
    let n2 = bm.pin(&b2).unwrap();

    // 0 . 2
    bm.unpin(n1).unwrap();

    // 0 . 2
    let b3 = BlockId::new("bufferfile", 0);
    let _n3 = bm.pin(&b3).unwrap();

    // 0 1 2
    let b4 = BlockId::new("bufferfile", 1);
    let _n4 = bm.pin(&b4).unwrap();

    println!("Available buffers: {}", bm.available());
    println!("Attempting to pin block 3...");
    match bm.pin(&BlockId::new("bufferfile", 3)) {
        Ok(_) => {}
        Err(e) => {
            eprintln!("Exception: No available buffers. {}", e);
        }
    };

    // 0 1 .
    bm.unpin(n2).unwrap();

    // 0 1 3
    let _n5 = bm.pin(&BlockId::new("bufferfile", 3)).unwrap();

    println!("Final Buffer Allocation");

    assert_eq!(bm.pool()[0].block().unwrap().number(), 0);
    assert_eq!(bm.pool()[1].block().unwrap().number(), 1);
    assert_eq!(bm.pool()[2].block().unwrap().number(), 3);
}

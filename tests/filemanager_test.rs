use simple_db::blockid::BlockId;
use simple_db::constants::BLOCKSIZE;
use simple_db::filemanager::FileMgr;
use simple_db::page::Page;

#[test]
fn test_new_filemgr() {
    let mut fm = FileMgr::new("./testdb", BLOCKSIZE).unwrap();
    let mut blk = BlockId::new("testfile", 2);

    let mut p1 = Page::new_from_size(fm.blocksize() as usize);
    let pos1 = 88;
    p1.set_string(pos1, "abcdefghijklm".to_string()).unwrap();
    let size = Page::max_length("abcdefghijklm".len());

    let pos2 = pos1 + size;
    p1.set_int(pos2, 345).unwrap();

    // println!("{}", p1.contents_str());

    fm.write(&mut blk, &mut p1).unwrap();

    let mut p2 = Page::new_from_size(fm.blocksize() as usize);
    fm.read(&mut blk, &mut p2).unwrap();

    // string
    assert_eq!(88, pos1);
    assert_eq!(String::from("abcdefghijklm"), p2.get_string(pos1).unwrap());
    println!(
        "offset: {}, contains: {}",
        pos1,
        p2.get_string(pos1).unwrap()
    );

    // int
    assert_eq!(105, pos2);
    assert_eq!(345, p2.get_int(pos2).unwrap());
    println!("offset: {}, contains: {}", pos2, p2.get_int(pos2).unwrap());
}

use simple_db::blockid::BlockId;
use simple_db::filemanager::FileMgr;
use simple_db::page::Page;

use itertools::izip;
use std::sync::{Arc, Mutex};
use std::thread;

#[test]
fn read_write() {
    let mut fm = FileMgr::new("./testdb", 400).unwrap();
    let blk = BlockId::new("testfile", 2);

    let mut p1 = Page::new_from_size(fm.blocksize() as usize);
    let pos1 = 88;
    p1.set_string(pos1, "abcdefghijklm").unwrap();

    let pos2 = pos1 + Page::max_length("abcdefghijklm".len());
    p1.set_int(pos2, 345).unwrap();

    fm.write(&blk, &mut p1).unwrap();

    let mut p2 = Page::new_from_size(fm.blocksize() as usize);
    fm.read(&blk, &mut p2).unwrap();

    // string
    assert_eq!(String::from("abcdefghijklm"), p2.get_string(pos1).unwrap());
    println!(
        "offset: {}, contains: {}",
        pos1,
        p2.get_string(pos1).unwrap()
    );

    // int
    assert_eq!(345, p2.get_int(pos2).unwrap());
    println!("offset: {}, contains: {}", pos2, p2.get_int(pos2).unwrap());
}

#[test]
fn keep_map_after_renewing() {
    const POS: usize = 100;
    const TEXT: &str = "Make America great again.";
    const NUM: i32 = 500;

    {
        let mut fm = FileMgr::new("./keep_map_after_renewing", 400).unwrap();
        let blk = BlockId::new("file1", 0);
        let blk2 = BlockId::new("file2", 0);

        let mut page = page_test_case(fm.blocksize(), POS, TEXT, NUM);

        fm.write(&blk, &mut page).unwrap();
        fm.write(&blk2, &mut page).unwrap();

        for (k, v) in izip!(fm.open_files().keys(), fm.open_files().values()) {
            println!("key: {:?}, value: {:?}", k, v);
        }
    }
    {
        let mut fm = FileMgr::new("./keep_map_after_renewing", 400).unwrap();
        let blk = BlockId::new("file1", 0);
        let blk2 = BlockId::new("file2", 0);

        for (k, v) in izip!(fm.open_files().keys(), fm.open_files().values()) {
            println!("key: {:?}, value: {:?}", k, v);
        }
        let mut p1 = Page::new_from_size(fm.blocksize() as usize);
        let mut p2 = Page::new_from_size(fm.blocksize() as usize);

        fm.read(&blk, &mut p1).unwrap();
        fm.read(&blk2, &mut p2).unwrap();

        println!("offset: {}, contains: {}", POS, p1.get_string(POS).unwrap());
        assert_eq!(TEXT.to_string(), p1.get_string(POS).unwrap());
        println!("offset: {}, contains: {}", POS, p2.get_string(POS).unwrap());
        assert_eq!(TEXT.to_string(), p2.get_string(POS).unwrap());
    }
}

#[test]
fn multithread_read_write() {
    let fm = Arc::new(Mutex::new(
        FileMgr::new("./multithread_read_write", 400).unwrap(),
    ));
    let blk = Arc::new(BlockId::new("mutexfile", 0));
    let mut threads = Vec::with_capacity(2);

    let blocksize = fm.lock().unwrap().blocksize();

    {
        let fm_clone = Arc::clone(&fm);
        let blk_clone = Arc::clone(&blk);

        const POS: usize = 100;
        const TEXT: &str = "Make America great again.";
        const NUM: i32 = 500;

        let mut page = page_test_case(blocksize, POS, TEXT, NUM);

        threads.push(thread::spawn(move || {
            let mut fm = fm_clone.lock().unwrap();
            fm.write(&blk_clone, &mut page).unwrap();

            let mut p2 = Page::new_from_size(blocksize as usize);
            fm.read(&blk_clone, &mut p2).unwrap();

            assert_eq!(String::from(TEXT), p2.get_string(POS).unwrap());
            println!("offset: {}, contents: {}", POS, p2.get_string(POS).unwrap());

            let pos_int = POS + Page::max_length_text(TEXT);
            assert_eq!(NUM, p2.get_int(pos_int).unwrap());
            println!(
                "offset: {}, contents: {}",
                pos_int,
                p2.get_int(pos_int).unwrap()
            );

            println!("thread1 finished");
        }));
    }

    {
        let fm_clone = Arc::clone(&fm);
        let blk_clone = Arc::clone(&blk);

        const POS: usize = 100;
        const TEXT: &str = "I have a dream";
        const NUM: i32 = 1000;

        let mut page = page_test_case(blocksize, POS, TEXT, NUM);

        threads.push(thread::spawn(move || {
            let mut fm = fm_clone.lock().unwrap();
            fm.write(&blk_clone, &mut page).unwrap();

            let mut p2 = Page::new_from_size(blocksize as usize);
            fm.read(&blk, &mut p2).unwrap();

            assert_eq!(String::from(TEXT), p2.get_string(POS).unwrap());
            println!("offset: {}, contains: {}", POS, p2.get_string(POS).unwrap());

            let pos_int = POS + Page::max_length_text(TEXT);
            assert_eq!(NUM, p2.get_int(pos_int).unwrap());
            println!(
                "offset: {}, contains: {}",
                pos_int,
                p2.get_int(pos_int).unwrap()
            );

            println!("thread2 finished");
        }));
    }

    threads.into_iter().for_each(|thread| {
        thread
            .join()
            .expect("The thread creating or execution failed !")
    });
}

// return the page with text and number on specified offset
fn page_test_case(blocksize: u64, offset: usize, text: &str, num: impl Into<i32>) -> Page {
    let mut page = Page::new_from_size(blocksize as usize);

    page.set_string(offset, text).unwrap();

    let pos2 = offset + Page::max_length_text(text);
    page.set_int(pos2, num.into()).unwrap();

    page
}

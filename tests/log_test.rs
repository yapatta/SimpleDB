use simple_db::filemanager::FileMgr;
use simple_db::logmanager::LogMgr;
use simple_db::page::Page;

use std::cell::RefCell;
use std::mem;
use std::sync::Arc;

use anyhow::Result;

#[test]
fn log_test() {
    let fm = FileMgr::new("./logtests", 400).unwrap();
    let mut lm = LogMgr::new(Arc::new(RefCell::new(fm)), String::from("logfile")).unwrap();
    create_records(&mut lm, 1, 35).unwrap();
    print_log_record(&mut lm, String::from("The log file now has these records:")).unwrap();
}

fn print_log_record(lm: &mut LogMgr, msg: String) -> Result<()> {
    println!("{}", msg);

    for rec in lm.iterator()? {
        let p = Page::new_from_bytes(rec);
        let s = p.get_string(0)?;
        let npos = Page::max_length(s.len());
        let val = p.get_int(npos)?;
        println!("[{}, {}]", s, val);
    }
    println!();

    Ok(())
}

fn create_records(lm: &mut LogMgr, start: u64, end: u64) -> Result<()> {
    for i in start..end + 1 {
        let mut page = create_log_record(String::from("record") + &i.to_string(), i + 100)?;
        let lsn = lm.append(page.contents())?;
        print!("{} ", lsn);
    }
    println!();

    Ok(())
}

fn create_log_record(s: String, n: u64) -> Result<Page> {
    let npos = Page::max_length(s.len());
    let mut p = Page::new_from_size(npos + mem::size_of::<i32>());
    p.set_string(0, s)?;
    p.set_int(npos, n as i32)?;

    Ok(p)
}

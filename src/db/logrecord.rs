use super::page::Page;
use anyhow::Result;

pub const CheckPoint: isize = 0;
pub const Start: isize = 1;
pub const Commit: isize = 2;
pub const Rollback: isize = 3;
pub const SetInt: isize = 4;
pub const SetString: isize = 5;

/*
trait LogRecord {
    fn create_logrecord(bytes: Vec<u8>) -> Result<LogRecord> {
        let p = Page::new_from_bytes(bytes);

        match p.get_int(0)? {
            CheckPoint => {}
        }
    }
}
*/

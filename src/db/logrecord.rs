use super::page::Page;
use super::setintrecord::SetIntRecord;
use super::setstringrecord::SetStringRecord;

use anyhow::Result;

pub const CheckPoint: i32 = 0;
pub const Start: i32 = 1;
pub const Commit: i32 = 2;
pub const Rollback: i32 = 3;
pub const SetInt: i32 = 4;
pub const SetString: i32 = 5;

enum LogRecord {
    SetInt(SetIntRecord),
    SetString(SetStringRecord)
}

pub fn create_logrecord(bytes: Vec<u8>) -> Result<LogRecord> {
    let mut p = Page::new_from_bytes(bytes);

    // TODO: implement other log records
    let ret = match p.get_int(0)? {
        //CheckPoint => {}
        //Start => {}
        //Commit => {}
        //Rollback => {}
        SetInt => LogRecord::SetInt(SetIntRecord::new(&p)?),
        SetString => LogRecord::SetString(SetStringRecord::new(&p)?),
    }

    Ok(ret)
}

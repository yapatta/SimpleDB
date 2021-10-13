use super::page::Page;
use super::setintrecord::SetIntRecord;
use super::setstringrecord::SetStringRecord;

use std::fmt;

use anyhow::Result;

pub const CHECKPOINT: i32 = 0;
pub const START: i32 = 1;
pub const COMMIT: i32 = 2;
pub const ROLLBACK: i32 = 3;
pub const SETINT: i32 = 4;
pub const SETSTRING: i32 = 5;

#[derive(Debug)]
enum LogRecordError {
    UnknownRecord,
}

impl std::error::Error for LogRecordError {}
impl fmt::Display for LogRecordError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            LogRecordError::UnknownRecord => {
                write!(f, "unknown log record")
            }
        }
    }
}

pub enum LogRecord {
    SetInt(SetIntRecord),
    SetString(SetStringRecord),
}

pub fn create_logrecord(bytes: Vec<u8>) -> Result<LogRecord> {
    let p = Page::new_from_bytes(bytes);

    // TODO: implement other log records
    match p.get_int(0)? {
        //CHECKPOINT => {}
        //START => {}
        //COMMIT => {}
        //ROLLBACK => {}
        SETINT => Ok(LogRecord::SetInt(SetIntRecord::new(&p)?)),
        SETSTRING => Ok(LogRecord::SetString(SetStringRecord::new(&p)?)),
        _ => Err(From::from(LogRecordError::UnknownRecord)),
    }
}

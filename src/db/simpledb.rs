use super::filemanager::FileMgr;

pub struct SimpleDB {
    blocksize: u64,
    buffersize: u64,
    log_filename: String,
    file_mgr: FileMgr,
}

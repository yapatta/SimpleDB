use super::filemanager::FileMgr;

pub struct SimpleDB<'a> {
    blocksize: u64,
    buffersize: u64,
    log_filename: String,
    file_mgr: &'a FileMgr<'a>,
}

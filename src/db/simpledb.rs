use super::filemanager::FileMgr;

pub struct SimpleDB {
    _blocksize: u64,
    _buffersize: u64,
    _log_filename: String,
    _file_mgr: FileMgr,
}

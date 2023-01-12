use std::fmt;
use std::hash::Hash;

#[derive(Debug, PartialEq, Eq, Hash, Clone)]
pub struct BlockId {
    filename: String,
    blknum: u64,
}

impl BlockId {
    pub fn new(filename: impl Into<String>, blknum: u64) -> BlockId {
        BlockId {
            filename: filename.into(),
            blknum,
        }
    }

    pub fn filename(&self) -> &str {
        &self.filename
    }

    pub fn number(&self) -> u64 {
        self.blknum
    }
}

impl fmt::Display for BlockId {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "[file {}, block {}]", self.filename, self.blknum)
    }
}

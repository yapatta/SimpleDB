use std::fmt;
use std::hash::Hash;

#[derive(PartialEq, Eq, Hash)]
pub struct BlockId {
    filename: String,
    blknum: u64,
}

impl BlockId {
    pub fn new(filename: &str, blknum: u64) -> BlockId {
        BlockId {
            filename: filename.to_string(),
            blknum: blknum,
        }
    }

    pub fn filename(&self) -> String {
        self.filename.clone()
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

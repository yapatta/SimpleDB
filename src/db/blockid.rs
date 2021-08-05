use std::fmt;

#[derive(PartialEq, Eq)]
pub struct BlockId {
    filename: String,
    blknum: i32,
}

impl BlockId {
    pub fn new(filename: String, blknum: i32) -> BlockId {
        BlockId {
            filename: filename,
            blknum: blknum,
        }
    }

    pub fn filename(&self) -> String {
        self.filename.clone()
    }

    pub fn number(&self) -> i32 {
        self.blknum
    }
}

impl fmt::Display for BlockId {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "[file {}, block {}]", self.filename, self.blknum)
    }
}

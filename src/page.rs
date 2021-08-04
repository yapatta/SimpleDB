use anyhow;
use itertools::izip;
use std::convert::TryInto;
use std::fmt;
use std::mem;

#[derive(Debug)]
enum PageError {
    BufferSizeExceeded,
}

impl std::error::Error for PageError {}
impl fmt::Display for PageError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            PageError::BufferSizeExceeded => write!(f, "buffer size exceeded"),
        }
    }
}

struct Page {
    bb: Vec<u8>,
}

impl Page {
    pub fn newFromByte(b: Vec<u8>) -> Page {
        Page { bb: b }
    }

    pub fn newFromSize(blocksize: usize) -> Page {
        Page {
            bb: Vec::with_capacity(blocksize),
        }
    }

    pub fn getInt(&self, offset: usize) -> anyhow::Result<(i32)> {
        let i32_size = mem::size_of::<i32>();

        if offset + i32_size - 1 < self.bb.len() {
            let bytes = &self.bb[offset..offset + i32_size];
            Ok(i32::from_be_bytes((*bytes).try_into()?))
        } else {
            return Err(PageError::BufferSizeExceeded)?;
        }
    }

    pub fn setInt(&mut self, offset: usize, n: i32) -> anyhow::Result<()> {
        let bytes = n.to_be_bytes();

        if offset + bytes.len() - 1 < self.bb.len() {
            for (b, added) in izip!(&mut self.bb[offset..offset + bytes.len()], &bytes) {
                *b = *added;
            }

            Ok(())
        } else {
            return Err(PageError::BufferSizeExceeded)?;
        }
    }

    pub fn getByte(&self, offset: usize) {}
}

fn main() {}

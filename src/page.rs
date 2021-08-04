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
            Err(PageError::BufferSizeExceeded)?
        }
    }

    pub fn setInt(&mut self, offset: usize, n: i32) -> anyhow::Result<(usize)> {
        let bytes = n.to_be_bytes();

        if offset + bytes.len() - 1 < self.bb.len() {
            for (b, added) in izip!(&mut self.bb[offset..offset + bytes.len()], &bytes) {
                *b = *added;
            }

            Ok(offset + mem::size_of::<i32>())
        } else {
            Err(PageError::BufferSizeExceeded)?
        }
    }

    fn getBytes(&self, offset: usize) -> anyhow::Result<(&[u8])> {
        let len = self.getInt(offset)? as usize;
        let new_offset = offset + mem::size_of::<i32>();

        if new_offset + len - 1 < self.bb.len() {
            Ok(&self.bb[new_offset..new_offset + len])
        } else {
            Err(PageError::BufferSizeExceeded)?
        }
    }

    fn setBytes(&mut self, offset: usize, b: &[u8]) -> anyhow::Result<(usize)> {
        if offset + mem::size_of::<i32>() + b.len() - 1 < self.bb.len() {
            let new_offset = self.setInt(offset, b.len() as i32)?;
            for (p, added) in izip!(&mut self.bb[new_offset..new_offset + b.len()], b) {
                *p = *added
            }
            Ok(new_offset + b.len())
        } else {
            Err(PageError::BufferSizeExceeded)?
        }
    }

    pub fn getString(&self, offset: usize) -> anyhow::Result<String> {
        let bytes = self.getBytes(offset)?;
        let s = String::from_utf8(bytes.to_vec())?;

        Ok(s)
    }

    pub fn setString(&mut self, offset: usize, s: String) -> anyhow::Result<(usize)> {
        self.setBytes(offset, s.as_bytes())
    }
}

fn main() {}

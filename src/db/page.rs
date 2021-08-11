use anyhow;
use itertools::izip;
use std::convert::TryInto;
use std::fmt;
use std::mem;
use std::str;

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

pub struct Page {
    bb: Vec<u8>,
}

impl Page {
    pub fn new_from_bytes(b: Vec<u8>) -> Page {
        Page { bb: b }
    }

    pub fn new_from_size(blocksize: usize) -> Page {
        Page {
            bb: vec![0; blocksize],
        }
    }

    pub fn get_int(&self, offset: usize) -> anyhow::Result<i32> {
        let i32_size = mem::size_of::<i32>();

        if offset + i32_size - 1 < self.bb.len() {
            let bytes = &self.bb[offset..offset + i32_size];
            Ok(i32::from_be_bytes((*bytes).try_into()?))
        } else {
            Err(PageError::BufferSizeExceeded)?
        }
    }

    pub fn set_int(&mut self, offset: usize, n: i32) -> anyhow::Result<usize> {
        let bytes = n.to_be_bytes();

        if offset + bytes.len() - 1 < self.bb.len() {
            for (b, added) in izip!(&mut self.bb[offset..offset + bytes.len()], &bytes) {
                *b = *added;
            }

            Ok(offset + bytes.len())
        } else {
            Err(PageError::BufferSizeExceeded)?
        }
    }

    pub(crate) fn get_bytes(&self, offset: usize) -> anyhow::Result<&[u8]> {
        let len = self.get_int(offset)? as usize;
        let new_offset = offset + mem::size_of::<i32>();

        if new_offset + len - 1 < self.bb.len() {
            Ok(&self.bb[new_offset..new_offset + len])
        } else {
            Err(PageError::BufferSizeExceeded)?
        }
    }

    pub(crate) fn set_bytes(&mut self, offset: usize, b: &[u8]) -> anyhow::Result<usize> {
        if offset + mem::size_of::<i32>() + b.len() - 1 < self.bb.len() {
            let new_offset = self.set_int(offset, b.len() as i32)?;
            for (p, added) in izip!(&mut self.bb[new_offset..new_offset + b.len()], b) {
                *p = *added
            }

            Ok(new_offset + b.len())
        } else {
            Err(PageError::BufferSizeExceeded)?
        }
    }

    pub fn get_string(&self, offset: usize) -> anyhow::Result<String> {
        let bytes = self.get_bytes(offset)?;
        let s = String::from_utf8(bytes.to_vec())?;

        Ok(s)
    }

    pub fn set_string(&mut self, offset: usize, s: String) -> anyhow::Result<usize> {
        self.set_bytes(offset, s.as_bytes())
    }

    pub fn max_length(strlen: usize) -> usize {
        mem::size_of::<i32>() + (strlen * mem::size_of::<u8>())
    }

    // needed by FileMgr
    pub fn contents(&mut self) -> &mut Vec<u8> {
        &mut self.bb
    }

    // for tests
    pub(crate) fn contents_str(&mut self) -> &str {
        str::from_utf8(self.contents()).unwrap()
    }

    pub(crate) fn get_bytes_vec(&self, offset: usize) -> anyhow::Result<Vec<u8>> {
        let len = self.get_int(offset)? as usize;
        let new_offset = offset + mem::size_of::<i32>();

        if new_offset + len - 1 < self.bb.len() {
            Ok(self.bb[new_offset..new_offset + len].try_into()?)
        } else {
            Err(PageError::BufferSizeExceeded)?
        }
    }
}

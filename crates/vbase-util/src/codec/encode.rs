use std::slice;

use super::Encoder;

impl Encoder for Vec<u8> {
    fn put(&mut self, byte: u8) {
        self.push(byte);
    }

    fn append(&mut self, bytes: &[u8]) {
        self.extend_from_slice(bytes);
    }
}

fn take(buf: &mut &mut [u8], len: usize) -> *mut u8 {
    assert!(buf.len() >= len);
    let ptr = buf.as_mut_ptr();
    unsafe {
        *buf = slice::from_raw_parts_mut(ptr.add(len), buf.len() - len);
    }
    ptr
}

impl Encoder for &mut [u8] {
    fn put(&mut self, byte: u8) {
        let ptr = take(self, 1);
        unsafe {
            ptr.write(byte);
        }
    }

    fn append(&mut self, bytes: &[u8]) {
        let ptr = take(self, bytes.len());
        unsafe {
            ptr.copy_from_nonoverlapping(bytes.as_ptr(), bytes.len());
        }
    }
}

pub struct BytesEncoder<'a> {
    buf: &'a mut [u8],
    pos: usize,
}

impl BytesEncoder<'_> {
    pub fn new<T: AsMut<[u8]>>(buf: &mut T) -> BytesEncoder<'_> {
        BytesEncoder {
            buf: buf.as_mut(),
            pos: 0,
        }
    }

    pub fn encoded_size(&self) -> usize {
        self.pos
    }

    pub fn encoded_bytes(&self) -> &[u8] {
        &self.buf[..self.pos]
    }
}

impl Encoder for BytesEncoder<'_> {
    fn put(&mut self, byte: u8) {
        self.buf[self.pos] = byte;
        self.pos += 1;
    }

    fn append(&mut self, bytes: &[u8]) {
        self.buf[self.pos..self.pos + bytes.len()].copy_from_slice(bytes);
        self.pos += bytes.len();
    }
}

use std::slice;

use super::Decoder;

fn take(buf: &mut &[u8], len: usize) -> *const u8 {
    assert!(buf.len() >= len);
    let ptr = buf.as_ptr();
    unsafe {
        *buf = slice::from_raw_parts(ptr.add(len), buf.len() - len);
    }
    ptr
}

impl<'de> Decoder<'de> for &'de [u8] {
    fn pop(&mut self) -> u8 {
        let ptr = take(self, 1);
        unsafe { ptr.read() }
    }

    fn remove(&mut self, len: usize) -> &'de [u8] {
        let ptr = take(self, len);
        unsafe { slice::from_raw_parts(ptr, len) }
    }
}

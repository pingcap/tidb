// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.



//! Go `bytes.Buffer`'s writes and growth.
//!
//! The executor charges a buffer by its capacity (`GROUP_CONCAT`'s
//! `p.buffer.Cap()`), so the capacity follows Go's `grow`: a first small
//! write allocates `smallBufferSize`, a write that does not fit slides the
//! unread bytes down or doubles through `growSlice`, which rounds the
//! allocation up to a size class.

use crate::go_runtime::{go_64_next_slice_capacity_for_element, GoSliceElementLayout};

/// Go `smallBufferSize`.
const SMALL_BUFFER_SIZE: usize = 64;

/// Go `bytes.Buffer`: the contents are `buf[off..]`, `cap` is Go's
/// `cap(b.buf)`, and `allocated` tells a nil `b.buf` (a first small write
/// takes `smallBufferSize`) from an empty non-nil one (it doubles).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Buffer {
    buf: Vec<u8>,
    off: usize,
    cap: usize,
    allocated: bool,
}

impl Buffer {
    /// Go `&bytes.Buffer{}`.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Go `bytes.NewBuffer(buf)` of a `make([]byte, len(buf))` copy: the
    /// capacity is the length.
    #[must_use]
    pub fn new_buffer(buf: Vec<u8>) -> Self {
        let cap = buf.len();
        Self { buf, off: 0, cap, allocated: true }
    }

    /// Go `Bytes`: the unread portion.
    #[must_use]
    pub fn bytes(&self) -> &[u8] {
        &self.buf[self.off..]
    }

    /// Go `Len`.
    #[must_use]
    pub fn len(&self) -> usize {
        self.buf.len() - self.off
    }

    /// Go `b.Len() == 0`.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Go `Cap`.
    #[must_use]
    pub fn cap(&self) -> usize {
        self.cap
    }

    /// Go `Reset`.
    pub fn reset(&mut self) {
        self.buf.clear();
        self.off = 0;
    }

    /// Go `Truncate`: keeps the first `n` unread bytes.
    ///
    /// # Panics
    ///
    /// Go's "truncation out of range" past the unread length.
    pub fn truncate(&mut self, n: usize) {
        if n == 0 {
            self.reset();
            return;
        }
        assert!(n <= self.len(), "bytes.Buffer: truncation out of range");
        self.buf.truncate(self.off + n);
    }

    /// Go `tryGrowByReslice`.
    fn try_grow_by_reslice(&self, n: usize) -> bool {
        n <= self.cap - self.buf.len()
    }

    /// Go `grow`: room for `n` more bytes.
    fn grow(&mut self, n: usize) {
        let m = self.len();
        if m == 0 && self.off != 0 {
            self.reset();
        }
        if self.try_grow_by_reslice(n) {
            return;
        }
        if !self.allocated && n <= SMALL_BUFFER_SIZE {
            self.set_cap(SMALL_BUFFER_SIZE);
            return;
        }
        let c = self.cap;
        if (n as i64) <= (c / 2) as i64 - m as i64 {
            // Slide the unread bytes down instead of allocating.
            self.buf.drain(..self.off);
        } else {
            // Go `growSlice(b.buf[b.off:], b.off+n)`.
            self.buf.drain(..self.off);
            let mut wanted = m + self.off + n;
            let sliced_cap = c - self.off;
            if wanted < 2 * sliced_cap {
                wanted = 2 * sliced_cap;
            }
            // `append([]byte(nil), make([]byte, c)...)`.
            self.set_cap(go_64_next_slice_capacity_for_element(wanted, 0, 1, GoSliceElementLayout::NoPointers));
        }
        self.off = 0;
    }

    fn set_cap(&mut self, cap: usize) {
        self.cap = cap;
        self.allocated = true;
        if cap > self.buf.capacity() {
            self.buf.reserve_exact(cap - self.buf.len());
        }
    }

    /// Go `Grow`.
    pub fn grow_by(&mut self, n: usize) {
        self.grow(n);
    }

    /// Go `Write` / `WriteString`.
    pub fn write(&mut self, p: &[u8]) {
        if !self.try_grow_by_reslice(p.len()) {
            self.grow(p.len());
        }
        self.buf.extend_from_slice(p);
    }

    /// Go `WriteByte`.
    pub fn write_byte(&mut self, c: u8) {
        self.write(&[c]);
    }
}

#[cfg(test)]
mod tests {
    use super::Buffer;

    /// Go 1.26 `bytes.Buffer` capacities (captured with `go run`): a first
    /// small write takes 64 bytes, a growth doubles through `growSlice`'s
    /// size-class rounding.
    #[test]
    fn capacities_grow_like_gos() {
        let mut b = Buffer::new();
        assert_eq!(b.cap(), 0);
        b.write(b"0");
        assert_eq!(b.cap(), 64);
        b.write(&[b'x'; 63]);
        assert_eq!(b.cap(), 64);
        b.write(b"y");
        assert_eq!(b.cap(), 128);
        b.write(&[b'z'; 200]);
        assert_eq!(b.cap(), 288);
        let mut big = Buffer::new();
        big.write(&[b'a'; 65]);
        assert_eq!(big.cap(), 80);
        big.truncate(10);
        assert_eq!((big.len(), big.cap()), (10, 80));
        big.reset();
        big.write(&[b'b'; 80]);
        assert_eq!(big.cap(), 80);
        assert_eq!(big.bytes(), &[b'b'; 80][..]);
        // `bytes.NewBuffer(make([]byte, 0))` doubles from nothing.
        let mut empty = Buffer::new_buffer(Vec::new());
        empty.write(b"0");
        assert_eq!(empty.cap(), 8);
        let mut three = Buffer::new_buffer(b"abc".to_vec());
        three.write(b"d");
        assert_eq!(three.cap(), 8);
    }

    /// `GROUP_CONCAT`'s pattern -- the values 0..200 joined by "," -- leaves
    /// Go's capacities 64 (25 writes), 128 (21), 256 (43), 512 (66), 1024.
    #[test]
    fn a_joined_list_grows_like_gos() {
        let mut g = Buffer::new();
        let mut caps = Vec::new();
        for i in 0..200 {
            if !g.is_empty() {
                g.write(b",");
            }
            g.write(i.to_string().as_bytes());
            caps.push(g.cap());
        }
        let mut expected = Vec::new();
        for (cap, count) in [(64, 25), (128, 21), (256, 43), (512, 66), (1024, 45)] {
            expected.extend(std::iter::repeat_n(cap, count));
        }
        assert_eq!(caps, expected);
    }
}

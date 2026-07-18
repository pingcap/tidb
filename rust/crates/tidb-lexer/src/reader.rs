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

//! A byte cursor over the SQL source, mirroring `pkg/parser/lexer.go`'s
//! `reader`. Positions are byte offsets; `peek` past end returns 0, matching
//! the Go reader's EOF sentinel.

#[derive(Debug)]
pub(crate) struct Reader<'a> {
    src: &'a str,
    bytes: &'a [u8],
    off: usize,
}

impl<'a> Reader<'a> {
    pub(crate) fn new(src: &'a str) -> Self {
        Reader {
            src,
            bytes: src.as_bytes(),
            off: 0,
        }
    }

    pub(crate) fn src(&self) -> &'a str {
        self.src
    }

    pub(crate) fn offset(&self) -> usize {
        self.off
    }

    pub(crate) fn set_offset(&mut self, off: usize) {
        self.off = off;
    }

    pub(crate) fn eof(&self) -> bool {
        self.off >= self.bytes.len()
    }

    /// Byte at the current position, or 0 at/after EOF.
    pub(crate) fn peek(&self) -> u8 {
        self.byte_at(self.off)
    }

    /// Byte at an absolute offset, or 0 if out of range.
    pub(crate) fn byte_at(&self, off: usize) -> u8 {
        if off < self.bytes.len() {
            self.bytes[off]
        } else {
            0
        }
    }

    pub(crate) fn inc(&mut self) {
        if self.off < self.bytes.len() {
            self.off += 1;
        }
    }

    pub(crate) fn inc_n(&mut self, n: usize) {
        self.off = (self.off + n).min(self.bytes.len());
    }

    /// Reads the current byte and advances (0 at EOF).
    pub(crate) fn read_byte(&mut self) -> u8 {
        let b = self.peek();
        self.inc();
        b
    }

    /// Advances while `pred` holds on the current byte; returns the byte that
    /// stopped it (0 at EOF), mirroring `reader.incAsLongAs`.
    pub(crate) fn inc_as_long_as<F: Fn(u8) -> bool>(&mut self, pred: F) -> u8 {
        loop {
            let b = self.peek();
            if self.eof() || !pred(b) {
                return b;
            }
            self.inc();
        }
    }

    /// Case-insensitive prefix check at the current position.
    pub(crate) fn starts_with_ci(&self, prefix: &str) -> bool {
        let rest = &self.bytes[self.off.min(self.bytes.len())..];
        let p = prefix.as_bytes();
        if rest.len() < p.len() {
            return false;
        }
        rest[..p.len()].eq_ignore_ascii_case(p)
    }
}

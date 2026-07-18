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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Buffered string-writer adapter from
//! `pkg/planner/cascades/util/string_writer.go`.
//!
//! The Go helper intentionally hides write lengths and errors from cascades
//! stringers.  This Rust adapter keeps the same two-operation surface while
//! retaining the underlying writer for callers that need the rendered bytes.

use std::io::{self, Write};

/// Small writer interface used by cascades stringers.
pub trait StrBufferWriter {
    /// Appends a string and preserves source fail-fast error handling.
    fn write_string(&mut self, value: &str);
    /// Flushes pending bytes and preserves source fail-fast error handling.
    fn flush(&mut self);
}

/// Buffered writer that hides write lengths and errors from callers.
pub struct StrBuffer<W> {
    writer: W,
}

impl<W> StrBuffer<W> {
    /// Creates a writer around an arbitrary standard-library sink.
    #[must_use]
    pub const fn new(writer: W) -> Self {
        Self { writer }
    }

    /// Returns the wrapped sink after all desired writes have been flushed.
    #[must_use]
    pub fn into_inner(self) -> W {
        self.writer
    }
}

impl<W: Write> StrBufferWriter for StrBuffer<W> {
    fn write_string(&mut self, value: &str) {
        self.writer
            .write_all(value.as_bytes())
            .expect("buffer-io WriteString should be no error in test");
    }

    fn flush(&mut self) {
        Write::flush(&mut self.writer).expect("buffer-io Flush should be no error in test");
    }
}

/// Creates a source-shaped string writer.
#[must_use]
pub const fn new_str_buffer<W: Write>(writer: W) -> StrBuffer<W> {
    StrBuffer::new(writer)
}

/// A convenient in-memory writer for source-derived tests and diagnostics.
#[must_use]
pub fn new_memory_buffer() -> StrBuffer<Vec<u8>> {
    new_str_buffer(Vec::new())
}

/// Returns the source-compatible writer error type for adapter callers.
pub type BufferError = io::Error;

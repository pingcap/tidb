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

//! Source-shaped I/O contracts shared by layered utility packages.
//!
//! Go's `io.ReaderAt` returns a byte count together with an error. Rust's
//! standard `Read` traits cannot represent a successful prefix and `io.EOF`
//! simultaneously, so this small contract retains both values. `CloseWrite`
//! likewise preserves the explicit close cascade used by checksum/encryption
//! wrappers instead of relying on destructor timing.

use std::fmt;
use std::io::{self, Write};

/// Error returned alongside a possibly nonzero `ReadAt` byte count.
#[derive(Debug)]
pub enum ReadAtError {
    /// Go's exact `io.EOF` condition.
    Eof,
    /// Any non-EOF underlying I/O failure.
    Io(io::Error),
}

impl ReadAtError {
    /// Returns whether this is the source `io.EOF` sentinel.
    #[must_use]
    pub const fn is_eof(&self) -> bool {
        matches!(self, Self::Eof)
    }
}

impl fmt::Display for ReadAtError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Eof => formatter.write_str("EOF"),
            Self::Io(error) => error.fmt(formatter),
        }
    }
}

impl std::error::Error for ReadAtError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Eof => None,
            Self::Io(error) => Some(error),
        }
    }
}

/// Result of one positional read.
#[derive(Debug)]
pub struct ReadAtResult {
    /// Number of bytes copied into the caller's buffer.
    pub n: usize,
    /// Optional EOF or non-EOF failure returned with that byte count.
    pub error: Option<ReadAtError>,
}

impl ReadAtResult {
    /// Constructs a successful positional read.
    #[must_use]
    pub const fn ok(n: usize) -> Self {
        Self { n, error: None }
    }

    /// Constructs a positional read that reached EOF.
    #[must_use]
    pub const fn eof(n: usize) -> Self {
        Self {
            n,
            error: Some(ReadAtError::Eof),
        }
    }

    /// Constructs a positional read that returned an I/O failure.
    #[must_use]
    pub fn io(n: usize, error: io::Error) -> Self {
        Self {
            n,
            error: Some(ReadAtError::Io(error)),
        }
    }
}

/// Go-compatible positional-reader contract.
pub trait ReadAt {
    /// Reads at `offset` without changing shared sequential position.
    fn read_at(&self, destination: &mut [u8], offset: i64) -> ReadAtResult;
}

impl<T> ReadAt for &T
where
    T: ReadAt + ?Sized,
{
    fn read_at(&self, destination: &mut [u8], offset: i64) -> ReadAtResult {
        (**self).read_at(destination, offset)
    }
}

#[cfg(unix)]
impl ReadAt for std::fs::File {
    fn read_at(&self, destination: &mut [u8], offset: i64) -> ReadAtResult {
        use std::os::unix::fs::FileExt;

        if offset < 0 {
            return ReadAtResult::io(
                0,
                io::Error::new(io::ErrorKind::InvalidInput, "negative read offset"),
            );
        }
        match FileExt::read_at(self, destination, offset as u64) {
            Ok(n) if n < destination.len() => ReadAtResult::eof(n),
            Ok(n) => ReadAtResult::ok(n),
            Err(error) => ReadAtResult::io(0, error),
        }
    }
}

/// Writable object with Go's explicit `Close` operation.
///
/// Consuming `self` gives Rust the same terminal ownership boundary as Go's
/// close cascade and ensures an owned file is actually dropped before success
/// is returned.
pub trait CloseWrite: Write + Sized {
    /// Flushes and closes this layer and every layer it owns.
    fn close(self) -> io::Result<()>;
}

impl CloseWrite for std::fs::File {
    fn close(mut self) -> io::Result<()> {
        self.flush()?;
        drop(self);
        Ok(())
    }
}

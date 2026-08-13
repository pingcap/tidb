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

//! Reusable gzip encoders and decoders from Go `pkg/util/compress`.

use std::io::{self, Cursor, Read, Write};
use std::sync::{Mutex, OnceLock};

use flate2::{read::GzDecoder, write::GzEncoder, Compression};

#[derive(Default)]
struct WriterCore {
    plaintext: Vec<u8>,
}

fn writer_pool() -> &'static Mutex<Vec<WriterCore>> {
    static POOL: OnceLock<Mutex<Vec<WriterCore>>> = OnceLock::new();
    POOL.get_or_init(|| Mutex::new(Vec::new()))
}

/// A reusable gzip encoder lease.
///
/// This is the ownership-native form of borrowing an item from Go's
/// `GzipWriterPool`: write the plaintext, call [`finish`](Self::finish), and
/// dropping the lease returns its reusable staging storage to the pool.
pub struct GzipWriter {
    core: Option<WriterCore>,
}

impl GzipWriter {
    fn new() -> Self {
        let core = writer_pool()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .pop()
            .unwrap_or_default();
        Self { core: Some(core) }
    }

    fn core(&mut self) -> &mut WriterCore {
        self.core
            .as_mut()
            .expect("gzip writer is always returned only at drop")
    }

    /// Appends plaintext to the current gzip stream.
    pub fn write_all(&mut self, bytes: &[u8]) -> io::Result<()> {
        self.core().plaintext.write_all(bytes)
    }

    /// Finalizes the current stream and returns its gzip bytes.
    ///
    /// The lease is reset immediately, so another stream may be written before
    /// it returns to the pool.
    pub fn finish(&mut self) -> io::Result<Vec<u8>> {
        let mut plaintext = std::mem::take(&mut self.core().plaintext);
        let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
        let result = encoder
            .write_all(&plaintext)
            .and_then(|()| encoder.finish());
        plaintext.clear();
        self.core().plaintext = plaintext;
        result
    }
}

impl Drop for GzipWriter {
    fn drop(&mut self) {
        let Some(mut core) = self.core.take() else {
            return;
        };
        core.plaintext.clear();
        writer_pool()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(core);
    }
}

/// Borrows a reusable gzip writer, matching Go `GzipWriterPool.Get`/`Put`.
#[must_use]
pub fn gzip_writer() -> GzipWriter {
    GzipWriter::new()
}

/// A reusable gzip decoder lease.
///
/// Go's reader pool holds resettable readers. Rust's decoder source is owned,
/// so this lease reuses the public operation boundary: each call decodes one
/// complete gzip stream and does not retain its input after return.
#[derive(Default)]
struct ReaderCore {
    scratch: Vec<u8>,
}

fn reader_pool() -> &'static Mutex<Vec<ReaderCore>> {
    static POOL: OnceLock<Mutex<Vec<ReaderCore>>> = OnceLock::new();
    POOL.get_or_init(|| Mutex::new(Vec::new()))
}

/// A reusable gzip decoder lease.
pub struct GzipReader {
    core: Option<ReaderCore>,
}

impl GzipReader {
    fn core(&mut self) -> &mut ReaderCore {
        self.core
            .as_mut()
            .expect("gzip reader is always returned only at drop")
    }

    /// Decodes one gzip stream.
    pub fn read_to_end(&mut self, compressed: &[u8]) -> io::Result<&[u8]> {
        let core = self.core();
        core.scratch.clear();
        GzDecoder::new(Cursor::new(compressed)).read_to_end(&mut core.scratch)?;
        Ok(&core.scratch)
    }
}

impl Drop for GzipReader {
    fn drop(&mut self) {
        let Some(mut reader) = self.core.take() else {
            return;
        };
        reader.scratch.clear();
        reader_pool()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(reader);
    }
}

/// Borrows a reusable gzip reader, matching Go `GzipReaderPool.Get`/`Put`.
#[must_use]
pub fn gzip_reader() -> GzipReader {
    let core = reader_pool()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .pop()
        .unwrap_or_default();
    GzipReader { core: Some(core) }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn writer_and_reader_round_trip_and_reuse() {
        let mut writer = gzip_writer();
        writer.write_all(b"first").unwrap();
        let first = writer.finish().unwrap();
        writer.write_all(b"second").unwrap();
        let second = writer.finish().unwrap();
        assert_ne!(first, second);

        let mut reader = gzip_reader();
        assert_eq!(reader.read_to_end(&first).unwrap(), b"first");
        assert_eq!(reader.read_to_end(&second).unwrap(), b"second");
    }

    #[test]
    fn invalid_gzip_returns_the_decoder_error() {
        let mut reader = gzip_reader();
        assert!(reader.read_to_end(b"not gzip").is_err());
    }
}

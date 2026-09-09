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

//! Reusable gzip streams from Go's `pkg/util/compress` package.
//!
//! Go exposes two process-wide `sync.Pool` values. Rust keeps the same
//! ownership boundary with the native [`zeropool::Pool`], while the pooled
//! stream wrappers erase the caller's reader/writer type at the pool boundary.
//! A caller resets a stream before use and returns it after the normal close or
//! read lifecycle, just as the Go consumers do.

use std::io::{self, BufReader, Read, Write};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, LazyLock};

use flate2::bufread::GzDecoder;
use flate2::write::GzEncoder;
use flate2::Compression;

use crate::zeropool::Pool;

struct WriterTarget {
    writer: Box<dyn Write + Send>,
    active: Arc<AtomicBool>,
}

impl WriterTarget {
    fn new<W>(writer: W) -> (Self, Arc<AtomicBool>)
    where
        W: Write + Send + 'static,
    {
        let active = Arc::new(AtomicBool::new(true));
        (
            Self {
                writer: Box::new(writer),
                active: Arc::clone(&active),
            },
            active,
        )
    }
}

impl Write for WriterTarget {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if self.active.load(Ordering::Relaxed) {
            self.writer.write(bytes)
        } else {
            Ok(bytes.len())
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        if self.active.load(Ordering::Relaxed) {
            self.writer.flush()
        } else {
            Ok(())
        }
    }
}

type ReaderSource = BufReader<Box<dyn Read + Send>>;

/// A pooled gzip writer.
///
/// The caller owns the target writer and must call [`Self::reset`] before
/// writing. `Write` forwards to the gzip stream; [`Self::close`] emits the
/// gzip trailer without closing the caller's target.
pub struct GzipWriter {
    inner: GzEncoder<WriterTarget>,
    active: Arc<AtomicBool>,
}

impl GzipWriter {
    fn new() -> Self {
        let (target, active) = WriterTarget::new(io::sink());
        Self {
            inner: GzEncoder::new(target, Compression::default()),
            active,
        }
    }

    /// Rebinds this stream to `writer` and resets its compression state.
    ///
    /// Go's `gzip.Writer.Reset` does not report an error. The Rust encoder is
    /// replaced in the same operation; any unfinished bytes from the prior
    /// target are discarded with the pooled object.
    pub fn reset<W>(&mut self, writer: W)
    where
        W: Write + Send + 'static,
    {
        // `GzEncoder`'s Drop implementation attempts to finish the old
        // stream. Disable forwarding first so Reset discards that state just
        // like Go's gzip.Writer.Reset instead of appending a stale trailer.
        self.active.store(false, Ordering::Relaxed);
        let (target, active) = WriterTarget::new(writer);
        self.inner = GzEncoder::new(target, Compression::default());
        self.active = active;
    }

    /// Finishes the gzip member, matching Go's `gzip.Writer.Close`.
    pub fn close(&mut self) -> io::Result<()> {
        let result = self.inner.try_finish();
        if result.is_ok() {
            self.active.store(false, Ordering::Relaxed);
        }
        result
    }
}

impl Write for GzipWriter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.inner.write(bytes)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
}

/// A pooled gzip reader.
///
/// `reset` validates the gzip header when possible, while checksum and body
/// errors are returned by the normal `Read` implementation. `close` is a
/// no-op, matching Go's reader, which never closes the caller's reader.
pub struct GzipReader {
    inner: GzDecoder<ReaderSource>,
}

impl GzipReader {
    fn new() -> Self {
        Self {
            inner: GzDecoder::new(BufReader::new(Box::new(io::empty()))),
        }
    }

    /// Rebinds this reader to `reader` and resets its decompression state.
    pub fn reset<R>(&mut self, reader: R) -> io::Result<()>
    where
        R: Read + Send + 'static,
    {
        self.inner = GzDecoder::new(BufReader::new(Box::new(reader)));
        let mut empty = [];
        self.inner.read(&mut empty).map(|_| ())
    }

    /// Leaves the caller's reader open, as Go's `gzip.Reader.Close` does.
    pub fn close(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl Read for GzipReader {
    fn read(&mut self, bytes: &mut [u8]) -> io::Result<usize> {
        self.inner.read(bytes)
    }
}

/// Go `GzipWriterPool`, initialized with a discard-bound gzip writer.
#[allow(non_upper_case_globals)]
pub static GzipWriterPool: LazyLock<Pool<GzipWriter>> =
    LazyLock::new(|| Pool::new(GzipWriter::new));

/// Go `GzipReaderPool`, initialized with an empty gzip reader.
#[allow(non_upper_case_globals)]
pub static GzipReaderPool: LazyLock<Pool<GzipReader>> =
    LazyLock::new(|| Pool::new(GzipReader::new));

#[cfg(test)]
mod tests {
    use super::{GzipReaderPool, GzipWriterPool};
    use std::io::{Cursor, Read, Write};
    use std::sync::{Arc, Mutex};

    #[derive(Clone)]
    struct SharedBuffer(Arc<Mutex<Vec<u8>>>);

    impl Write for SharedBuffer {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.0
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .extend_from_slice(bytes);
            Ok(bytes.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    #[test]
    fn pooled_streams_reset_and_round_trip() {
        let compressed = Arc::new(Mutex::new(Vec::new()));
        let mut writer = GzipWriterPool.get();
        writer.reset(SharedBuffer(Arc::clone(&compressed)));
        writer.write_all(b"go-owned gzip payload").unwrap();
        writer.close().unwrap();
        GzipWriterPool.put(writer);

        let mut reader = GzipReaderPool.get();
        reader
            .reset(Cursor::new(
                compressed
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .clone(),
            ))
            .unwrap();
        let mut decoded = Vec::new();
        reader.read_to_end(&mut decoded).unwrap();
        reader.close().unwrap();
        GzipReaderPool.put(reader);
        assert_eq!(decoded, b"go-owned gzip payload");
    }

    #[test]
    fn reader_reset_rejects_an_invalid_header() {
        let mut reader = GzipReaderPool.get();
        let result = reader.reset(Cursor::new(b"not gzip".to_vec()));
        GzipReaderPool.put(reader);
        assert!(result.is_err());
    }

    #[test]
    fn writer_pool_reuses_a_closed_stream_for_a_new_target() {
        let first = Arc::new(Mutex::new(Vec::new()));
        let second = Arc::new(Mutex::new(Vec::new()));
        let mut writer = GzipWriterPool.get();
        writer.reset(SharedBuffer(Arc::clone(&first)));
        writer.write_all(b"first").unwrap();
        writer.close().unwrap();
        writer.reset(SharedBuffer(Arc::clone(&second)));
        writer.write_all(b"second").unwrap();
        writer.close().unwrap();
        GzipWriterPool.put(writer);

        let mut reader = GzipReaderPool.get();
        reader
            .reset(Cursor::new(
                second
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .clone(),
            ))
            .unwrap();
        let mut decoded = Vec::new();
        reader.read_to_end(&mut decoded).unwrap();
        GzipReaderPool.put(reader);
        assert_eq!(decoded, b"second");
    }

    #[test]
    fn writer_reset_discards_unfinished_stream_state() {
        let first = Arc::new(Mutex::new(Vec::new()));
        let second = Arc::new(Mutex::new(Vec::new()));
        let mut writer = GzipWriterPool.get();
        writer.reset(SharedBuffer(Arc::clone(&first)));
        writer.write_all(b"unfinished").unwrap();
        let bytes_before_reset = first
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .len();
        writer.reset(SharedBuffer(Arc::clone(&second)));
        let bytes_after_reset = first
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .len();
        writer.close().unwrap();
        GzipWriterPool.put(writer);
        assert_eq!(bytes_after_reset, bytes_before_reset);
    }
}

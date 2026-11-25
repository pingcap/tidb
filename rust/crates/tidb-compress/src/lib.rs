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

use std::io::{self, BufRead, BufReader, Read, Write};
use std::sync::{Arc, LazyLock};

use flate2::read::GzDecoder;
use flate2::{Compress, Compression, Crc, FlushCompress, Status};
use snap::raw::{max_compress_len, Encoder};

use tidb_zeropool::Pool;

type ReaderSource = Box<dyn Read + Send>;

const MAX_BLOCK_SIZE: usize = u16::MAX as usize;
const SHORT_BLOCK_SIZE: usize = 1024;
const GZIP_READ_BUFFER_SIZE: usize = 32 * 1024;
const MIN_GZIP_READ_AHEAD: usize = 4 * 1024;
const GZIP_HEADER: [u8; 10] = [0x1f, 0x8b, 8, 0, 0, 0, 0, 0, 0, 255];

/// A pooled gzip writer.
///
/// The caller owns the target writer and must call [`Self::reset`] before
/// writing. `Write` forwards to the gzip stream; [`Self::close`] emits the
/// gzip trailer without closing the caller's target.
pub struct GzipWriter {
    target: Box<dyn Write + Send>,
    deflate: Option<(Compress, Compression)>,
    probe: Option<Box<Encoder>>,
    probe_output: Vec<u8>,
    pending: Vec<u8>,
    crc: Crc,
    deflating: bool,
    wrote_header: bool,
    closed: bool,
    error: Option<Arc<io::Error>>,
}

impl GzipWriter {
    fn new() -> Self {
        Self {
            target: Box::new(io::sink()),
            deflate: None,
            probe: None,
            probe_output: Vec::new(),
            pending: Vec::new(),
            crc: Crc::new(),
            deflating: false,
            wrote_header: false,
            closed: false,
            error: None,
        }
    }

    /// Rebinds this stream to `writer` and resets its compression state.
    ///
    /// An unfinished member is discarded without writing to its previous target.
    pub fn reset<W>(&mut self, writer: W)
    where
        W: Write + Send + 'static,
    {
        self.target = Box::new(writer);
        self.crc.reset();
        self.pending.clear();
        self.deflating = false;
        self.wrote_header = false;
        self.closed = false;
        self.error = None;
    }

    /// Finishes the gzip member, matching Go's `gzip.Writer.Close`.
    pub fn close(&mut self) -> io::Result<()> {
        self.run(|stream| {
            if stream.closed {
                return Ok(());
            }
            if !stream.wrote_header {
                let mut empty = [0; 20];
                empty[..GZIP_HEADER.len()].copy_from_slice(&GZIP_HEADER);
                empty[GZIP_HEADER.len()] = 3;
                stream.target.write_all(&empty)?;
                stream.wrote_header = true;
                stream.closed = true;
                return Ok(());
            }
            stream.write_pending(FlushCompress::Finish)?;
            let mut trailer = [0; 8];
            trailer[..4].copy_from_slice(&stream.crc.sum().to_le_bytes());
            trailer[4..].copy_from_slice(&stream.crc.amount().to_le_bytes());
            stream.target.write_all(&trailer)?;
            stream.closed = true;
            Ok(())
        })
    }

    fn run<T>(&mut self, operation: impl FnOnce(&mut Self) -> io::Result<T>) -> io::Result<T> {
        if let Some(error) = &self.error {
            return Err(io::Error::new(error.kind(), Arc::clone(error)));
        }
        operation(self).map_err(|error| {
            let error = Arc::new(error);
            self.error = Some(Arc::clone(&error));
            io::Error::new(error.kind(), error)
        })
    }

    fn write_header(&mut self) -> io::Result<()> {
        if !self.wrote_header {
            self.target.write_all(&GZIP_HEADER)?;
            self.wrote_header = true;
        }
        Ok(())
    }

    fn write_block(&mut self, bytes: &[u8], flush: FlushCompress) -> io::Result<()> {
        // Pending writes are checksummed together when their block is emitted.
        self.crc.update(bytes);
        let finish = flush == FlushCompress::Finish;
        let deflate = if bytes.len() <= 32 {
            // A tiny tail can finish the active block without flushing the
            // compressor and starting a separate stored block.
            finish && self.deflating
        } else {
            !self.should_store(bytes)
        };
        if deflate {
            self.write_deflate(bytes, flush, finish && bytes.len() <= SHORT_BLOCK_SIZE)
        } else {
            self.store_block(bytes, finish)
        }
    }

    fn write_pending(&mut self, flush: FlushCompress) -> io::Result<()> {
        if self.pending.is_empty() {
            return if flush != FlushCompress::Finish {
                Ok(())
            } else if self.deflating {
                self.write_deflate(&[], flush, false)
            } else {
                // A final empty fixed-Huffman block.
                self.target.write_all(&[3, 0])
            };
        }
        let mut pending = std::mem::take(&mut self.pending);
        let result = self.write_block(&pending, flush);
        pending.clear();
        self.pending = pending;
        result
    }

    fn store_block(&mut self, bytes: &[u8], finish: bool) -> io::Result<()> {
        if self.deflating {
            // Align to a byte boundary and discard the old match history before
            // introducing bytes the compressor never sees.
            self.write_deflate(&[], FlushCompress::Full, false)?;
            self.deflating = false;
        }
        let [lo, hi] = (bytes.len() as u16).to_le_bytes();
        self.target
            .write_all(&[u8::from(finish), lo, hi, !lo, !hi])?;
        self.target.write_all(bytes)
    }

    fn should_store(&mut self, bytes: &[u8]) -> bool {
        // Like Go's flate storeFast, cheaply probe for matches before spending
        // time on entropy coding. Use the existing Snappy encoder for the
        // probe only. The stream always contains DEFLATE blocks.
        let prefix = &bytes[..bytes.len().min(1024)];
        self.probe_stores(prefix) && (prefix.len() == bytes.len() || self.probe_stores(bytes))
    }

    fn probe_stores(&mut self, bytes: &[u8]) -> bool {
        let needed = max_compress_len(bytes.len());
        if self.probe_output.len() < needed {
            self.probe_output.resize(needed, 0);
        }
        // The probe's inline hash table would otherwise be moved in and
        // out of the pool even on writers that never need to compress.
        let probe = self.probe.get_or_insert_with(|| Box::new(Encoder::new()));
        let encoded = probe.compress(bytes, &mut self.probe_output);
        encoded.is_ok_and(|size| size >= bytes.len())
    }

    fn write_deflate(
        &mut self,
        mut bytes: &[u8],
        flush: FlushCompress,
        short_final_block: bool,
    ) -> io::Result<()> {
        let level = if short_final_block {
            Compression::fast()
        } else {
            // Use the bounded-search profile for the streaming body.
            Compression::new(2)
        };
        let deflate = if let Some((deflate, previous_level)) = &mut self.deflate {
            if !self.deflating {
                deflate.reset();
                if level != *previous_level {
                    deflate.set_level(level)?;
                    *previous_level = level;
                }
            }
            deflate
        } else {
            &mut self.deflate.insert((Compress::new(level, false), level)).0
        };
        self.deflating = true;
        let mut output = [0; 8192];
        loop {
            let old_in = deflate.total_in();
            let old_out = deflate.total_out();
            let status = deflate.compress(bytes, &mut output, flush)?;
            let consumed = (deflate.total_in() - old_in) as usize;
            let produced = (deflate.total_out() - old_out) as usize;
            if produced != 0 {
                self.target.write_all(&output[..produced])?;
            }
            bytes = &bytes[consumed..];
            if status == Status::StreamEnd
                || (flush != FlushCompress::Finish && bytes.is_empty() && produced < output.len())
            {
                return Ok(());
            }
            if consumed == 0 && produced == 0 {
                return Err(io::Error::other("gzip compressor did not make progress"));
            }
        }
    }
}

impl Write for GzipWriter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if self.closed {
            return Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "gzip writer is closed",
            ));
        }
        self.run(|stream| {
            stream.write_header()?;
            let mut rest = bytes;
            while !rest.is_empty() {
                if stream.pending.is_empty() && rest.len() >= MAX_BLOCK_SIZE {
                    let (block, tail) = rest.split_at(MAX_BLOCK_SIZE);
                    stream.write_block(block, FlushCompress::None)?;
                    rest = tail;
                } else {
                    let len = rest.len().min(MAX_BLOCK_SIZE - stream.pending.len());
                    let (part, tail) = rest.split_at(len);
                    stream.pending.extend_from_slice(part);
                    if stream.pending.len() == MAX_BLOCK_SIZE {
                        stream.write_pending(FlushCompress::None)?;
                    }
                    rest = tail;
                }
            }
            Ok(bytes.len())
        })
    }

    fn flush(&mut self) -> io::Result<()> {
        self.run(|stream| {
            if stream.closed {
                return Ok(());
            }
            stream.write_header()?;
            stream.write_pending(FlushCompress::None)?;
            if stream.deflating {
                stream.write_deflate(&[], FlushCompress::Sync, false)?;
            } else {
                stream.store_block(&[], false)?;
            }
            stream.target.flush()
        })
    }
}

impl Drop for GzipWriter {
    fn drop(&mut self) {
        let _ = self.close();
    }
}

/// A pooled gzip reader.
///
/// `reset` validates the gzip header when possible, while checksum and body
/// errors are returned by the normal `Read` implementation. `close` is a
/// no-op, matching Go's reader, which never closes the caller's reader.
pub struct GzipReader {
    inner: BufReader<Box<GzDecoder<ReaderSource>>>,
    failed_header: bool,
    buffer_reads: bool,
}

impl GzipReader {
    fn new() -> Self {
        let source: ReaderSource = Box::new(io::empty());
        Self {
            inner: BufReader::with_capacity(
                GZIP_READ_BUFFER_SIZE,
                Box::new(GzDecoder::new(source)),
            ),
            failed_header: false,
            buffer_reads: false,
        }
    }

    /// Rebinds this reader to `reader` and resets its decompression state.
    pub fn reset<R>(&mut self, reader: R) -> io::Result<()>
    where
        R: Read + Send + 'static,
    {
        self.buffer_reads = false;
        self.inner.consume(self.inner.buffer().len());
        let decoder = self.inner.get_mut();
        decoder.reset(Box::new(reader));
        let result = decoder.read(&mut []).map(|_| ());
        // Header errors exhaust the member. A would-block reader can resume.
        self.failed_header = result
            .as_ref()
            .is_err_and(|error| error.kind() != io::ErrorKind::WouldBlock);
        result
    }

    /// Leaves the caller's reader open, as Go's `gzip.Reader.Close` does.
    pub fn close(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl Read for GzipReader {
    fn read(&mut self, bytes: &mut [u8]) -> io::Result<usize> {
        if self.failed_header {
            return Ok(0);
        }
        if self.buffer_reads && !bytes.is_empty() {
            return self.inner.read(bytes);
        }
        // Decode short members directly. Once a read fills a small destination
        // or produces a substantial prefix, buffer output in larger batches.
        let count = self.inner.get_mut().read(bytes)?;
        self.buffer_reads |= count != 0 && count >= bytes.len().min(MIN_GZIP_READ_AHEAD);
        Ok(count)
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
    use super::{
        GzipReader, GzipReaderPool, GzipWriter, GzipWriterPool, GZIP_HEADER, MAX_BLOCK_SIZE,
    };
    use std::io::{self, Cursor, Read, Write};
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
        assert!(result.is_err());
        assert_eq!(reader.read(&mut [0; 1]).unwrap(), 0);
        GzipReaderPool.put(reader);
    }

    #[test]
    fn empty_read_does_not_fetch_the_gzip_body() {
        struct BlockedBody;

        impl Read for BlockedBody {
            fn read(&mut self, _: &mut [u8]) -> io::Result<usize> {
                Err(io::ErrorKind::WouldBlock.into())
            }
        }

        let mut reader = GzipReader::new();
        reader
            .reset(Cursor::new(GZIP_HEADER).chain(BlockedBody))
            .unwrap();
        assert_eq!(reader.read(&mut []).unwrap(), 0);
        assert_eq!(
            reader.read(&mut [0]).unwrap_err().kind(),
            io::ErrorKind::WouldBlock
        );
    }

    #[test]
    fn reader_reset_discards_buffered_input_and_recovers_after_corruption() {
        fn member(bytes: &[u8]) -> Vec<u8> {
            let mut encoder =
                flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
            encoder.write_all(bytes).unwrap();
            encoder.finish().unwrap()
        }

        let mut reader = GzipReaderPool.get();
        let mut first = vec![b'x'; 32 * 1024];
        first[0] = b'a';
        reader.reset(Cursor::new(member(&first))).unwrap();
        let mut head = [0];
        reader.read_exact(&mut head).unwrap();
        assert_eq!(head, [b'a']);

        let second = member(b"second member");
        let mut corrupt = second.clone();
        let crc = corrupt.len() - 8;
        corrupt[crc] ^= 1;
        reader.reset(Cursor::new(corrupt)).unwrap();
        assert!(reader.read_to_end(&mut Vec::new()).is_err());

        reader.reset(Cursor::new(second)).unwrap();
        let mut output = Vec::new();
        reader.read_to_end(&mut output).unwrap();
        assert_eq!(output, b"second member");
        GzipReaderPool.put(reader);
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
    fn writer_reset_preserves_output_when_compression_levels_change() {
        fn encode(writer: &mut GzipWriter, bytes: &[u8]) -> Vec<u8> {
            let target = Arc::new(Mutex::new(Vec::new()));
            writer.reset(SharedBuffer(Arc::clone(&target)));
            writer.write_all(bytes).unwrap();
            writer.close().unwrap();
            let mut output = target.lock().unwrap();
            std::mem::take(&mut *output)
        }

        let short = b"short repeated data".repeat(40);
        let long = vec![b'x'; MAX_BLOCK_SIZE + 1];
        let mut reused = GzipWriter::new();
        for input in [&short, &long, &long, &short, &short] {
            let actual = encode(&mut reused, input);
            assert_eq!(actual, encode(&mut GzipWriter::new(), input));
            let mut decoded = Vec::new();
            flate2::read::GzDecoder::new(actual.as_slice())
                .read_to_end(&mut decoded)
                .unwrap();
            assert_eq!(&decoded, input);
        }
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

    #[test]
    fn mixed_blocks_survive_fragmented_writes_and_flush() {
        struct ShortWriter(SharedBuffer);

        impl Write for ShortWriter {
            fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
                self.0.write(&bytes[..bytes.len().min(37)])
            }

            fn flush(&mut self) -> io::Result<()> {
                self.0.flush()
            }
        }

        let mut state = 19_u32;
        let random: Vec<_> = (0..MAX_BLOCK_SIZE)
            .map(|_| {
                state ^= state << 13;
                state ^= state >> 17;
                state ^= state << 5;
                state as u8
            })
            .collect();
        let repeated = vec![b'a'; MAX_BLOCK_SIZE];
        let target = Arc::new(Mutex::new(Vec::new()));
        let mut writer = GzipWriter::new();
        writer.reset(ShortWriter(SharedBuffer(Arc::clone(&target))));

        writer.write_all(&repeated).unwrap();
        assert!(writer.deflating);
        for part in random.chunks(777) {
            writer.write_all(part).unwrap();
        }
        assert!(!writer.deflating);
        writer.write_all(&repeated[..999]).unwrap();
        writer.flush().unwrap();
        let prefix = target.lock().unwrap().clone();
        let mut decoded = Vec::new();
        let mut decoder = flate2::read::GzDecoder::new(prefix.as_slice());
        let _ = decoder.read_to_end(&mut decoded).unwrap_err();
        assert_eq!(decoded, [&repeated[..], &random, &repeated[..999]].concat());

        writer.write_all(&repeated[999..]).unwrap();
        writer.flush().unwrap();
        writer.flush().unwrap();
        writer.close().unwrap();
        writer.close().unwrap();

        decoded.clear();
        let bytes = target.lock().unwrap();
        flate2::read::GzDecoder::new(bytes.as_slice())
            .read_to_end(&mut decoded)
            .unwrap();
        assert_eq!(decoded, [&repeated[..], &random, &repeated].concat());
    }

    #[test]
    fn writer_can_be_reset_after_a_partial_target_error() {
        struct FailingWriter(bool);

        impl Write for FailingWriter {
            fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
                if std::mem::replace(&mut self.0, false) {
                    Ok(bytes.len().min(2))
                } else {
                    Err(io::Error::other("target failed"))
                }
            }

            fn flush(&mut self) -> io::Result<()> {
                Ok(())
            }
        }

        let mut writer = GzipWriter::new();
        writer.reset(FailingWriter(true));
        assert_eq!(
            writer.write_all(b"data").unwrap_err().to_string(),
            "target failed"
        );
        assert_eq!(writer.close().unwrap_err().to_string(), "target failed");
        writer.reset(io::sink());
        writer.write_all(b"fresh stream").unwrap();
        writer.close().unwrap();
    }
}

//! Go `pkg/objstore/compressedio`: the compression types storage files
//! use, with their readers, writers and the compressing part buffer.

use std::io::{self, Read, Write};
use std::sync::{Arc, Mutex, PoisonError};

use crate::error::{Error, Result};

/// Go `CompressType`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum CompressType {
    /// Go `NoCompression`.
    #[default]
    NoCompression,
    /// Go `Gzip`.
    Gzip,
    /// Go `Snappy`: the framed snappy stream format.
    Snappy,
    /// Go `Zstd`.
    Zstd,
}

impl CompressType {
    /// Go `FileSuffix`.
    #[must_use]
    pub fn file_suffix(self) -> &'static str {
        match self {
            Self::Gzip => ".gz",
            Self::Snappy => ".snappy",
            Self::Zstd => ".zst",
            Self::NoCompression => "",
        }
    }
}

/// Go `DecompressConfig`.
#[derive(Clone, Copy, Debug, Default)]
pub struct DecompressConfig {
    /// Go `ZStdDecodeConcurrency`; the Rust decoder is single threaded.
    pub zstd_decode_concurrency: usize,
}

/// Go `ParseCompressType`.
pub fn parse_compress_type(compress_type: &str) -> Result<CompressType> {
    match compress_type {
        "" | "no-compression" => Ok(CompressType::NoCompression),
        "gzip" | "gz" => Ok(CompressType::Gzip),
        "snappy" => Ok(CompressType::Snappy),
        "zstd" | "zst" => Ok(CompressType::Zstd),
        _ => Err(Error::other(format!("unknown compress type {compress_type}"))),
    }
}

/// Go `NewReader`: a decompressing reader over `r`; `None` for
/// `NoCompression`, as Go returns nil.
pub fn new_reader<'a, R: Read + Send + 'a>(
    compress_type: CompressType,
    _cfg: DecompressConfig,
    r: R,
) -> Result<Option<Box<dyn Read + Send + 'a>>> {
    Ok(match compress_type {
        // Go's gzip.Reader reads concatenated members.
        CompressType::Gzip => Some(Box::new(flate2::read::MultiGzDecoder::new(r))),
        CompressType::Snappy => Some(Box::new(snap::read::FrameDecoder::new(r))),
        CompressType::Zstd => Some(Box::new(zstd::stream::read::Decoder::new(r).map_err(Error::from)?)),
        CompressType::NoCompression => None,
    })
}

/// Go `Writer`: a compressing writer that can flush and close.
pub trait CompressWriter: Write + Send {
    /// Go `Close`: writes the stream's trailer.
    fn close(&mut self) -> io::Result<()>;
}

impl<W: Write + Send> CompressWriter for flate2::write::GzEncoder<W> {
    fn close(&mut self) -> io::Result<()> {
        self.try_finish()
    }
}

impl<W: Write + Send> CompressWriter for snap::write::FrameEncoder<W> {
    fn close(&mut self) -> io::Result<()> {
        self.flush()
    }
}

impl<W: Write + Send> CompressWriter for zstd::stream::write::Encoder<'static, W> {
    fn close(&mut self) -> io::Result<()> {
        self.do_finish()
    }
}

/// Go `NewWriter`; `None` for `NoCompression`.
pub fn new_writer<W: Write + Send + 'static>(compress_type: CompressType, w: W) -> Option<Box<dyn CompressWriter>> {
    match compress_type {
        CompressType::Gzip => Some(Box::new(flate2::write::GzEncoder::new(w, flate2::Compression::default()))),
        CompressType::Snappy => Some(Box::new(snap::write::FrameEncoder::new(w))),
        CompressType::Zstd => zstd::stream::write::Encoder::new(w, 0)
            .ok()
            .map(|e| Box::new(e) as Box<dyn CompressWriter>),
        CompressType::NoCompression => None,
    }
}

/// The byte buffer a [`Buffer`]'s compressor writes into.
#[derive(Clone, Default)]
struct SharedSink(Arc<Mutex<Vec<u8>>>);

impl Write for SharedSink {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.0.lock().unwrap_or_else(PoisonError::into_inner).extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

/// Go `Buffer`: compresses what is written into an in-memory part whose
/// length counts compressed bytes.
pub struct Buffer {
    sink: SharedSink,
    cap: usize,
    writer: Box<dyn CompressWriter>,
}

impl Buffer {
    /// Go `NewBuffer`.
    ///
    /// # Panics
    /// When `compress_type` is `NoCompression`, which Go never passes.
    #[must_use]
    pub fn new(chunk_size: usize, compress_type: CompressType) -> Self {
        let sink = SharedSink(Arc::new(Mutex::new(Vec::with_capacity(chunk_size))));
        let writer = new_writer(compress_type, sink.clone()).expect("a compressed buffer");
        Self { sink, cap: chunk_size, writer }
    }

    /// Go `Write`.
    pub fn write(&mut self, p: &[u8]) -> Result<usize> {
        self.writer.write_all(p).map_err(Error::from)?;
        Ok(p.len())
    }

    /// Go `Len`.
    #[must_use]
    pub fn len(&self) -> usize {
        self.sink.0.lock().unwrap_or_else(PoisonError::into_inner).len()
    }

    /// Go `Len() == 0`.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Go `Cap`.
    #[must_use]
    pub fn cap(&self) -> usize {
        self.cap
    }

    /// Go `Flush`.
    pub fn flush(&mut self) -> Result<()> {
        self.writer.flush().map_err(Error::from)
    }

    /// Go `Close`.
    pub fn close(&mut self) -> Result<()> {
        self.writer.close().map_err(Error::from)
    }

    /// Go `Bytes` then `Reset`.
    pub fn take(&mut self) -> Vec<u8> {
        std::mem::take(&mut *self.sink.0.lock().unwrap_or_else(PoisonError::into_inner))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trips_every_type() {
        for ty in [CompressType::Gzip, CompressType::Snappy, CompressType::Zstd] {
            let mut buf = Buffer::new(1024, ty);
            buf.write(b"hello ").unwrap();
            buf.flush().unwrap();
            buf.write(b"world").unwrap();
            buf.close().unwrap();
            let data = buf.take();
            let mut out = String::new();
            new_reader(ty, DecompressConfig::default(), &data[..])
                .unwrap()
                .unwrap()
                .read_to_string(&mut out)
                .unwrap();
            assert_eq!(out, "hello world", "{ty:?}");
        }
        assert_eq!(parse_compress_type("zst").unwrap(), CompressType::Zstd);
        assert_eq!(parse_compress_type("lz4").unwrap_err().to_string(), "unknown compress type lz4");
    }
}

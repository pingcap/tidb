//! Go `pkg/objstore/compress.go`: a storage whose files are compressed,
//! and the decompressing readers IMPORT INTO opens data files through.

use std::io::{self, Read, Seek, SeekFrom};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use tidb_gocontext::Context;

use crate::compressedio::{self, CompressType, DecompressConfig};
use crate::error::{berrors, Error, Result};
use crate::objectio::{BufferedWriter, Reader, Writer};
use crate::storeapi::{ReaderOption, Storage, WalkOption, WriterOption};

/// Go `withCompression`.
struct WithCompression {
    inner: Arc<dyn Storage>,
    compress_type: CompressType,
    decompress_cfg: DecompressConfig,
}

/// Go `WithCompression`.
pub fn with_compression(
    inner: Arc<dyn Storage>,
    compress_type: CompressType,
    cfg: DecompressConfig,
) -> Arc<dyn Storage> {
    if compress_type == CompressType::NoCompression {
        return inner;
    }
    Arc::new(WithCompression { inner, compress_type, decompress_cfg: cfg })
}

impl Storage for WithCompression {
    fn write_file(&self, ctx: &Context, name: &str, data: &[u8]) -> Result<()> {
        let mut buf = compressedio::Buffer::new(data.len(), self.compress_type);
        buf.write(data)?;
        buf.close()?;
        self.inner.write_file(ctx, name, &buf.take())
    }

    fn read_file(&self, ctx: &Context, name: &str) -> Result<Vec<u8>> {
        let data = self.inner.read_file(ctx, name)?;
        let mut out = Vec::new();
        if let Some(mut r) = compressedio::new_reader(self.compress_type, self.decompress_cfg, &data[..])? {
            r.read_to_end(&mut out).map_err(Error::from)?;
        }
        Ok(out)
    }

    fn file_exists(&self, ctx: &Context, name: &str) -> Result<bool> {
        self.inner.file_exists(ctx, name)
    }

    fn delete_file(&self, ctx: &Context, name: &str) -> Result<()> {
        self.inner.delete_file(ctx, name)
    }

    fn open(&self, ctx: &Context, path: &str, option: Option<&ReaderOption>) -> Result<Box<dyn Reader>> {
        let reader = self.inner.open(ctx, path, option)?;
        intercept_decompress_reader(reader, self.compress_type, self.decompress_cfg)
    }

    fn delete_files(&self, ctx: &Context, names: &[String]) -> Result<()> {
        self.inner.delete_files(ctx, names)
    }

    fn walk_dir(&self, ctx: &Context, opt: Option<&WalkOption>, f: &mut dyn FnMut(&str, i64) -> Result<()>) -> Result<()> {
        self.inner.walk_dir(ctx, opt, f)
    }

    fn uri(&self) -> String {
        self.inner.uri()
    }

    fn create(&self, ctx: &Context, path: &str, option: Option<&WriterOption>) -> Result<Box<dyn Writer>> {
        let writer = self.inner.create(ctx, path, option)?.into_unbuffered();
        Ok(Box::new(BufferedWriter::new(
            writer,
            tidb_objstore_s3like::HARDCODED_CHUNK_SIZE,
            self.compress_type,
            None,
        )))
    }

    fn rename(&self, ctx: &Context, old_file_name: &str, new_file_name: &str) -> Result<()> {
        self.inner.rename(ctx, old_file_name, new_file_name)
    }

    fn presign_file(&self, ctx: &Context, file_name: &str, expire: Duration) -> Result<String> {
        self.inner.presign_file(ctx, file_name, expire)
    }

    fn close(&self) {
        self.inner.close();
    }

    fn features(&self) -> crate::storeapi::Features {
        self.inner.features()
    }
}

/// The file reader a decompressor reads through, shared with the
/// [`CompressReader`] that seeks and closes it.
#[derive(Clone)]
struct SharedReader {
    inner: Arc<Mutex<Box<dyn Reader>>>,
    limit: Option<Arc<Mutex<u64>>>,
}

impl Read for SharedReader {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let mut inner = self.inner.lock().unwrap_or_else(PoisonError::into_inner);
        match &self.limit {
            None => inner.read(buf),
            Some(remaining) => {
                let mut remaining = remaining.lock().unwrap_or_else(PoisonError::into_inner);
                if *remaining == 0 {
                    return Ok(0);
                }
                let max = buf.len().min(usize::try_from(*remaining).unwrap_or(usize::MAX));
                let n = inner.read(&mut buf[..max])?;
                *remaining -= n as u64;
                Ok(n)
            }
        }
    }
}

/// Go `compressReader`: reads the decompressed stream; seeks only to report
/// the file position.
struct CompressReader {
    reader: Box<dyn Read + Send>,
    file: SharedReader,
}

impl Read for CompressReader {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.reader.read(buf)
    }
}

impl Seek for CompressReader {
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        if pos == SeekFrom::Current(0) {
            return self.file.inner.lock().unwrap_or_else(PoisonError::into_inner).seek(pos);
        }
        let (offset, whence) = match pos {
            SeekFrom::Start(o) => (o as i64, 0),
            SeekFrom::Current(o) => (o, 1),
            SeekFrom::End(o) => (o, 2),
        };
        Err(berrors::annotate(
            berrors::ERR_STORAGE_INVALID_CONFIG,
            format!("compressReader doesn't support Seek now, offset {offset}, whence {whence}"),
        )
        .into())
    }
}

impl Reader for CompressReader {
    fn get_file_size(&self) -> Result<i64> {
        Err(berrors::annotate(
            berrors::ERR_UNSUPPORTED_OPERATION,
            "compressReader doesn't support GetFileSize now",
        ))
    }

    fn close(&mut self) -> Result<()> {
        self.file.inner.lock().unwrap_or_else(PoisonError::into_inner).close()
    }
}

fn decompress(file: SharedReader, compress_type: CompressType, cfg: DecompressConfig) -> Result<Box<dyn Reader>> {
    let reader = compressedio::new_reader(compress_type, cfg, file.clone())?
        .ok_or_else(|| Error::other("no decompressor"))?;
    Ok(Box::new(CompressReader { reader, file }))
}

/// Go `InterceptDecompressReader`.
pub fn intercept_decompress_reader(
    file_reader: Box<dyn Reader>,
    compress_type: CompressType,
    cfg: DecompressConfig,
) -> Result<Box<dyn Reader>> {
    if compress_type == CompressType::NoCompression {
        return Ok(file_reader);
    }
    decompress(
        SharedReader { inner: Arc::new(Mutex::new(file_reader)), limit: None },
        compress_type,
        cfg,
    )
}

/// Go `NewLimitedInterceptReader`: decompresses at most `n` compressed
/// bytes of the file (all of it when `n` is 0).
pub fn new_limited_intercept_reader(
    file_reader: Box<dyn Reader>,
    compress_type: CompressType,
    cfg: DecompressConfig,
    n: i64,
) -> Result<Box<dyn Reader>> {
    if n < 0 {
        return Err(berrors::annotate(
            berrors::ERR_STORAGE_INVALID_CONFIG,
            format!("compressReader doesn't support negative limit, n: {n}"),
        ));
    }
    if compress_type == CompressType::NoCompression && n == 0 {
        return Ok(file_reader);
    }
    let limit = (n > 0).then(|| Arc::new(Mutex::new(n as u64)));
    let shared = SharedReader { inner: Arc::new(Mutex::new(file_reader)), limit };
    if compress_type == CompressType::NoCompression {
        // Go wraps the limited file in a compressReader, then returns it
        // undecompressed.
        return Ok(Box::new(CompressReader { reader: Box::new(shared.clone()), file: shared }));
    }
    decompress(shared, compress_type, cfg)
}

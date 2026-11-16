//! Go `pkg/objstore/objectio`: the reader and writer storages hand out,
//! and the buffered writer that uploads in parts.

use std::io;

use tidb_gocontext::Context;

use crate::compressedio::{self, CompressType};
use crate::error::Result;

/// Go `Reader`: `io.ReadSeekCloser` with the object's size.
pub trait Reader: io::Read + io::Seek + Send {
    /// Go `GetFileSize`.
    fn get_file_size(&self) -> Result<i64>;
    /// Go `Close`.
    fn close(&mut self) -> Result<()> {
        Ok(())
    }
}

/// Go `Writer`.
pub trait Writer: Send {
    /// Go `Write`.
    fn write(&mut self, ctx: &Context, p: &[u8]) -> Result<usize>;
    /// Go `Close`: writes the final part and completes the upload.
    fn close(&mut self, ctx: &Context) -> Result<()>;
    /// Go's `writer.(*objectio.BufferedWriter).GetWriter()` check in
    /// `withCompression.Create`: the writer under a buffered writer, or the
    /// writer itself.
    fn into_unbuffered(self: Box<Self>) -> Box<dyn Writer>;
}

/// Go `NewIOWriter`: an `io.Write` over a storage writer.
pub struct IoWriter<'a> {
    ctx: &'a Context,
    w: &'a mut dyn Writer,
}

impl<'a> IoWriter<'a> {
    /// Go `NewIOWriter`.
    pub fn new(ctx: &'a Context, w: &'a mut dyn Writer) -> Self {
        Self { ctx, w }
    }
}

impl io::Write for IoWriter<'_> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.w.write(self.ctx, buf).map_err(io::Error::from)
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

/// Go `interceptBuffer`: plain bytes, or bytes compressed as written.
enum InterceptBuffer {
    Plain { buf: Vec<u8>, cap: usize },
    Compressed(compressedio::Buffer),
}

impl InterceptBuffer {
    fn new(chunk_size: usize, compress_type: CompressType) -> Self {
        if compress_type == CompressType::NoCompression {
            Self::Plain { buf: Vec::with_capacity(chunk_size), cap: chunk_size }
        } else {
            Self::Compressed(compressedio::Buffer::new(chunk_size, compress_type))
        }
    }

    fn len(&self) -> usize {
        match self {
            Self::Plain { buf, .. } => buf.len(),
            Self::Compressed(b) => b.len(),
        }
    }

    fn cap(&self) -> usize {
        match self {
            Self::Plain { cap, .. } => *cap,
            Self::Compressed(b) => b.cap(),
        }
    }

    fn write(&mut self, p: &[u8]) -> Result<usize> {
        match self {
            Self::Plain { buf, .. } => {
                buf.extend_from_slice(p);
                Ok(p.len())
            }
            Self::Compressed(b) => b.write(p),
        }
    }

    fn compressed(&self) -> bool {
        matches!(self, Self::Compressed(_))
    }

    fn flush(&mut self) -> Result<()> {
        match self {
            Self::Plain { .. } => Ok(()),
            Self::Compressed(b) => b.flush(),
        }
    }

    fn close(&mut self) -> Result<()> {
        match self {
            Self::Plain { .. } => Ok(()),
            Self::Compressed(b) => b.close(),
        }
    }

    fn take(&mut self) -> Vec<u8> {
        match self {
            Self::Plain { buf, .. } => std::mem::take(buf),
            Self::Compressed(b) => b.take(),
        }
    }
}

/// Go `BufferedWriter`: collects writes into parts of the buffer's size and
/// writes each full part to the inner writer.
pub struct BufferedWriter {
    buf: InterceptBuffer,
    writer: Box<dyn Writer>,
    access_rec: Option<std::sync::Arc<tidb_objstore_recording::AccessStats>>,
}

impl BufferedWriter {
    /// Go `NewBufferedWriter`.
    pub fn new(
        writer: Box<dyn Writer>,
        chunk_size: usize,
        compress_type: CompressType,
        access_rec: Option<std::sync::Arc<tidb_objstore_recording::AccessStats>>,
    ) -> Self {
        Self { buf: InterceptBuffer::new(chunk_size, compress_type), writer, access_rec }
    }

    /// Go `NewUploaderWriter`.
    pub fn new_uploader_writer(writer: Box<dyn Writer>, chunk_size: usize, compress_type: CompressType) -> Self {
        Self::new(writer, chunk_size, compress_type, None)
    }

    fn write0(&mut self, ctx: &Context, mut p: &[u8]) -> Result<usize> {
        let mut bytes_written = 0;
        while self.buf.len() + p.len() > self.buf.cap() {
            let chunk_to_fill = self.buf.cap().saturating_sub(self.buf.len());
            if chunk_to_fill > 0 {
                let w = self.buf.write(&p[..chunk_to_fill])?;
                bytes_written += w;
                p = &p[w..];
                if self.buf.compressed() {
                    continue;
                }
            }
            self.buf.flush()?;
            self.upload_chunk(ctx)?;
        }
        bytes_written += self.buf.write(p)?;
        Ok(bytes_written)
    }

    fn upload_chunk(&mut self, ctx: &Context) -> Result<()> {
        if self.buf.len() == 0 {
            return Ok(());
        }
        let b = self.buf.take();
        self.writer.write(ctx, &b).map(|_| ())
    }
}

impl Writer for BufferedWriter {
    fn write(&mut self, ctx: &Context, p: &[u8]) -> Result<usize> {
        let n = self.write0(ctx, p)?;
        if let Some(rec) = &self.access_rec {
            rec.rec_write(n);
        }
        Ok(n)
    }

    fn close(&mut self, ctx: &Context) -> Result<()> {
        self.buf.close()?;
        self.upload_chunk(ctx)?;
        self.writer.close(ctx)
    }

    fn into_unbuffered(self: Box<Self>) -> Box<dyn Writer> {
        self.writer
    }
}

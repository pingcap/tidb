//! Go `pkg/lightning/mydump/parser.go`'s `OpenReader` / `NewReaderOpener`:
//! a data file opened through its compression.

use tidb_gocontext::Context;
use tidb_objstore::compressedio::DecompressConfig;
use tidb_objstore::objectio::Reader;
use tidb_objstore::storeapi::Storage;

use crate::{Compression, SourceFileMeta, SourceType};

/// Go `OpenReader`.
pub fn open_reader(
    ctx: &Context,
    file_meta: &SourceFileMeta,
    store: &std::sync::Arc<dyn Storage>,
    decompress_cfg: DecompressConfig,
) -> Result<Box<dyn Reader>, String> {
    if file_meta.compression != Compression::None {
        let compress_type = crate::to_storage_compress_type(file_meta.compression)?;
        tidb_objstore::compress::with_compression(store.clone(), compress_type, decompress_cfg)
            .open(ctx, &file_meta.path, None)
            .map_err(|e| e.to_string())
    } else {
        store.open(ctx, &file_meta.path, None).map_err(|e| e.to_string())
    }
}

/// Go `NewReaderOpener`'s eager open: a reader for every file but Parquet,
/// which its parser opens itself.
pub fn new_reader_opener(
    ctx: &Context,
    file_meta: &SourceFileMeta,
    store: &std::sync::Arc<dyn Storage>,
    decompress_cfg: DecompressConfig,
) -> Result<Option<Box<dyn Reader>>, String> {
    if file_meta.r#type == SourceType::Parquet {
        return Ok(None);
    }
    open_reader(ctx, file_meta, store, decompress_cfg).map(Some)
}

/// A storage reader as a parser's input.
pub struct ParserReader(pub Box<dyn Reader>);

impl std::io::Read for ParserReader {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        self.0.read(buf)
    }
}

impl std::io::Seek for ParserReader {
    fn seek(&mut self, pos: std::io::SeekFrom) -> std::io::Result<u64> {
        self.0.seek(pos)
    }
}

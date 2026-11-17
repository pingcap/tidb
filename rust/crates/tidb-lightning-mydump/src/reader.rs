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

/// Go's `*parquetfile.Parser` as a `mydump.Parser`.
pub struct ParquetParser {
    inner: tidb_dumpformat_parquetfile::Parser,
    last_row: crate::Row,
}

impl ParquetParser {
    /// Go `parquetfile.NewParser`.
    pub fn new(
        ctx: &Context,
        store: std::sync::Arc<dyn Storage>,
        path: &str,
        file_size: i64,
        meta: tidb_dumpformat_parquetfile::FileMeta,
    ) -> Result<Self, String> {
        let inner = tidb_dumpformat_parquetfile::Parser::new(ctx, store, path, file_size, meta)?;
        Ok(Self { inner, last_row: crate::Row::default() })
    }
}

impl crate::Parser for ParquetParser {
    fn read_row(&mut self) -> Result<(), crate::CsvError> {
        match self.inner.read_row() {
            Ok(true) => {
                let row = self.inner.last_row();
                self.last_row = crate::Row { row_id: row.row_id, row: row.row.clone(), length: row.length };
                Ok(())
            }
            Ok(false) => Err(crate::CsvError::Eof),
            Err(e) if e == tidb_dumpformat_parquetfile::ERR_ROW_TOO_LARGE => Err(crate::CsvError::RowTooLarge),
            Err(e) => Err(crate::CsvError::Other(e)),
        }
    }
    fn last_row(&self) -> &crate::Row {
        &self.last_row
    }
    fn pos(&self) -> (i64, i64) {
        self.inner.pos()
    }
    fn set_pos(&mut self, pos: i64, row_id: i64) -> Result<(), crate::CsvError> {
        self.inner.set_pos(pos, row_id).map_err(crate::CsvError::Other)
    }
    fn set_row_id(&mut self, row_id: i64) {
        self.inner.set_row_id(row_id);
    }
    fn columns(&self) -> &[String] {
        self.inner.columns()
    }
    fn set_columns(&mut self, _columns: Vec<String>) {}
    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }
    fn scanned_pos(&mut self) -> Result<i64, crate::CsvError> {
        Ok(self.inner.scanned_pos())
    }
}

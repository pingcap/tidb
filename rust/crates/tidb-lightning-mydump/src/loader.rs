//! Go `pkg/lightning/mydump/loader.go`: source-file metadata, the table's
//! data files, and the real (decompressed) size estimate. Lightning's
//! directory loader (`MDLoader`, routing rules, schema files) is not
//! ported: IMPORT INTO lists its files itself.

use std::io::{Read, Seek, SeekFrom};

use tidb_gocontext::Context;
use tidb_objstore::compressedio::{CompressType, DecompressConfig};
use tidb_objstore::storeapi::Storage;

use crate::{Compression, SourceType};

/// Go `sampleCompressedFileSize`.
const SAMPLE_COMPRESSED_FILE_SIZE: usize = 4 * 1024;

/// Go `RawFile`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct RawFile {
    pub path: String,
    pub size: i64,
}

/// Go `ExtendColumnData`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ExtendColumnData {
    pub columns: Vec<String>,
    pub values: Vec<String>,
}

/// Go `SourceFileMeta`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct SourceFileMeta {
    pub path: String,
    pub r#type: SourceType,
    pub compression: Compression,
    pub sort_key: String,
    /// Go `FileSize`: the size on storage.
    pub file_size: i64,
    pub extend_data: ExtendColumnData,
    /// Go `RealSize`: the estimated decompressed size.
    pub real_size: i64,
    pub rows: i64,
    /// Go `ParquetMeta`, the JSON a parquet reader is reopened with.
    pub parquet_meta: crate::ParquetFileMeta,
}

/// Go `filter.Table`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct FilterTable {
    pub schema: String,
    pub name: String,
}

/// Go `FileInfo`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct FileInfo {
    pub table_name: FilterTable,
    pub file_meta: SourceFileMeta,
}

/// Go `MDTableMeta`.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct MDTableMeta {
    pub db: String,
    pub name: String,
    pub schema_file: FileInfo,
    pub data_files: Vec<FileInfo>,
    pub total_size: i64,
    pub index_ratio: f64,
    pub is_row_ordered: bool,
}

/// Go `ParallelProcess`: `hdl` over every input with at most `concurrency`
/// running, the outputs in input order; the first error wins.
pub fn parallel_process<T: Send, R: Sync>(
    ctx: &Context,
    inputs: &[R],
    concurrency: usize,
    hdl: impl Fn(&Context, &R) -> Result<T, String> + Sync,
) -> Result<Vec<T>, String> {
    let concurrency = concurrency.max(1);
    let (eg_ctx, cancel) = Context::with_cancel(ctx);
    let next = std::sync::atomic::AtomicUsize::new(0);
    let outputs: Vec<std::sync::Mutex<Option<T>>> = inputs.iter().map(|_| std::sync::Mutex::new(None)).collect();
    let first_err: std::sync::Mutex<Option<String>> = std::sync::Mutex::new(None);
    std::thread::scope(|scope| {
        for _ in 0..concurrency.min(inputs.len().max(1)) {
            scope.spawn(|| loop {
                let i = next.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                if i >= inputs.len() {
                    return;
                }
                if let Some(e) = eg_ctx.err() {
                    let mut first = first_err.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
                    first.get_or_insert(e.to_string());
                    return;
                }
                match hdl(&eg_ctx, &inputs[i]) {
                    Ok(v) => *outputs[i].lock().unwrap_or_else(std::sync::PoisonError::into_inner) = Some(v),
                    Err(e) => {
                        first_err.lock().unwrap_or_else(std::sync::PoisonError::into_inner).get_or_insert(e);
                        cancel.cancel();
                        return;
                    }
                }
            });
        }
    });
    if let Some(e) = first_err.into_inner().unwrap_or_else(std::sync::PoisonError::into_inner) {
        return Err(e);
    }
    Ok(outputs
        .into_iter()
        .map(|o| o.into_inner().unwrap_or_else(std::sync::PoisonError::into_inner).expect("every input processed"))
        .collect())
}

/// Go `calculateFileBytes`: the decompressed bytes of a sample and the
/// compressed position it ended at.
fn calculate_file_bytes(
    ctx: &Context,
    data_file: &str,
    compress_type: CompressType,
    store: &dyn Storage,
    offset: i64,
) -> Result<(usize, i64), String> {
    let mut bytes = vec![0; SAMPLE_COMPRESSED_FILE_SIZE];
    let reader = store.open(ctx, data_file, None).map_err(|e| e.to_string())?;
    let cfg = DecompressConfig { zstd_decode_concurrency: 1 };
    let mut compress_reader =
        tidb_objstore::compress::new_limited_intercept_reader(reader, compress_type, cfg, offset).map_err(|e| e.to_string())?;
    let mut tot = 0;
    if offset == 0 {
        // Go reads once (a short read is fine) and reports where it stopped.
        match compress_reader.read(&mut bytes) {
            Ok(n) => tot += n,
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {}
            Err(e) => return Err(e.to_string()),
        }
        let pos = compress_reader.seek(SeekFrom::Current(0)).map_err(|e| e.to_string())? as i64;
        let _ = compress_reader.close();
        return Ok((tot, pos));
    }
    loop {
        match compress_reader.read(&mut bytes) {
            Ok(0) => break,
            Ok(n) => tot += n,
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
            Err(e) => return Err(e.to_string()),
        }
    }
    let _ = compress_reader.close();
    Ok((tot, offset))
}

/// Go `EstimateRealSizeForFile`.
pub fn estimate_real_size_for_file(ctx: &Context, file_meta: &SourceFileMeta, store: &dyn Storage) -> i64 {
    if file_meta.compression == Compression::None {
        return file_meta.file_size;
    }
    match sample_file_compress_ratio(ctx, file_meta, store) {
        Ok(ratio) => (ratio * file_meta.file_size as f64) as i64,
        Err(_) => file_meta.file_size,
    }
}

/// Go `SampleFileCompressRatio`.
pub fn sample_file_compress_ratio(ctx: &Context, file_meta: &SourceFileMeta, store: &dyn Storage) -> Result<f64, String> {
    if let Some(v) = tidb_failpoint::eval("github.com/pingcap/tidb/pkg/lightning/mydump/SampleFileCompressPercentage") {
        return match v.as_int() {
            Some(pct) => Ok(pct as f64 / 100.0),
            None => Err(v.string()),
        };
    }
    if file_meta.compression == Compression::None {
        return Ok(1.0);
    }
    let compress_type = crate::to_storage_compress_type(file_meta.compression)?;
    let (_, pos) = calculate_file_bytes(ctx, &file_meta.path, compress_type, store, 0)?;
    let (tot, pos) = calculate_file_bytes(ctx, &file_meta.path, compress_type, store, pos)?;
    Ok(tot as f64 / pos as f64)
}

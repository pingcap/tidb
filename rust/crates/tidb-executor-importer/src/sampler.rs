// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
//! Go `pkg/executor/importer/sampler.go`: the encoded KV sizes of a few
//! source rows, for the resource parameters of a next-gen import.
//!
//! Go's `SampleFileImportKVSize` / `KVSizeSampleConfig` entry, which only
//! `pkg/importsdk` calls, is not ported; the controller's path is.

use std::sync::atomic::{AtomicI64, Ordering};

use tidb_driver_error::DriverError;
use tidb_gocontext::Context;
use tidb_lightning_backend_kv::PanickingAllocators;
use tidb_lightning_mydump::SourceFileMeta;

use super::chunk_process::{Chunk, EncodedKvGroupBatch};
use super::LoadDataController;

/// Go `maxSampleFileCount`.
const MAX_SAMPLE_FILE_COUNT: usize = 3;
/// Go `totalSampleRowCount`: about 30 rows in all, 10 per file of 3.
const TOTAL_SAMPLE_ROW_COUNT: usize = MAX_SAMPLE_FILE_COUNT * 10;

/// Go `maxSampleFileSize`: the bytes of a file sampled. Rows of 1 MiB or
/// more give a small index KV ratio even with many indexes, so fewer rows
/// are fine. A variable for tests.
pub static MAX_SAMPLE_FILE_SIZE: AtomicI64 = AtomicI64::new(10 << 20);

/// Go `SampledKVSizeResult`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct SampledKvSizeResult {
    /// Go `SourceSize`.
    pub source_size: i64,
    /// Go `DataKVSize`.
    pub data_kv_size: u64,
    /// Go `IndexKVSize`.
    pub index_kv_size: u64,
}

impl SampledKvSizeResult {
    /// Go `TotalKVSize`.
    #[must_use]
    pub fn total_kv_size(&self) -> i64 {
        (self.data_kv_size + self.index_kv_size) as i64
    }
}

/// Go `rand.Perm(n)` over a source seeded from the clock, as Go's global
/// source is randomly seeded.
fn perm(n: usize) -> Vec<usize> {
    let seed = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map_or(1, |d| d.as_nanos() as i64);
    let mut r = tidb_go_rand::Rand::new(seed);
    let mut m = vec![0; n];
    for i in 0..n {
        let j = r.intn(i as i64 + 1) as usize;
        m[i] = m[j];
        m[j] = i;
    }
    m
}

impl LoadDataController {
    /// Go `sampleIndexSizeRatio`: index KV bytes over data KV bytes in the
    /// sample, 0 without data KVs.
    ///
    /// # Errors
    /// A sampled file could not be read or encoded.
    pub fn sample_index_size_ratio(&self, ctx: &Context, ks_codec: &[u8]) -> Result<f64, DriverError> {
        let result = self.sample_kv_size(ctx, ks_codec)?;
        if result.data_kv_size == 0 {
            return Ok(0.0);
        }
        Ok(result.index_kv_size as f64 / result.data_kv_size as f64)
    }

    /// Go `sampleKVSize` (`newKVSizeSampler` + `sample`): up to 3 random
    /// files, 30 rows in all. The first failure is returned with the sizes
    /// of every file.
    ///
    /// # Errors
    /// The format cannot be sampled, or a sampled file could not be read or
    /// encoded.
    pub fn sample_kv_size(&self, ctx: &Context, ks_codec: &[u8]) -> Result<SampledKvSizeResult, DriverError> {
        self.validate_kv_size_sample_config()?;
        if self.data_files.is_empty() {
            return Ok(SampledKvSizeResult::default());
        }
        let p = perm(self.data_files.len());
        let files: Vec<&SourceFileMeta> =
            p.iter().take(self.data_files.len().min(MAX_SAMPLE_FILE_COUNT)).map(|&i| &self.data_files[i]).collect();
        let rows_per_file = TOTAL_SAMPLE_ROW_COUNT / files.len();
        let mut result = SampledKvSizeResult::default();
        let mut first_err = None;
        for file in files {
            match self.sample_one_file(ctx, file, ks_codec, rows_per_file) {
                Ok((source, data, index)) => {
                    result.source_size += source;
                    result.data_kv_size += data;
                    result.index_kv_size += index;
                }
                Err(e) => {
                    first_err.get_or_insert(e);
                }
            }
        }
        match first_err {
            Some(e) => Err(e),
            None => Ok(result),
        }
    }

    /// Go `validateKVSizeSampleConfig`.
    fn validate_kv_size_sample_config(&self) -> Result<(), DriverError> {
        let p = &self.plan;
        if !matches!(p.format.as_str(), "csv" | "sql" | "parquet") {
            return Err(DriverError::from(
                tidb_exeerrors::ERR_LOAD_DATA_UNSUPPORTED_FORMAT.gen_with_stack_by_args(&[p.format.clone().into()]),
            ));
        }
        if !p.fields_enclosed_by.is_empty()
            && (p.fields_enclosed_by.starts_with(&p.fields_terminated_by)
                || p.fields_terminated_by.starts_with(&p.fields_enclosed_by))
        {
            return Err(DriverError::from(tidb_exeerrors::ERR_LOAD_DATA_WRONG_FORMAT_CONFIG.gen_with_stack_by_args(&[
                "FIELDS ENCLOSED BY and TERMINATED BY must not be prefix of each other".into(),
            ])));
        }
        Ok(())
    }

    /// Go `sampleOneFile`: up to `max_row_count` rows from the first 10 MiB
    /// of the file, encoded with fresh allocators.
    fn sample_one_file(
        &self,
        ctx: &Context,
        file: &SourceFileMeta,
        ks_codec: &[u8],
        max_row_count: usize,
    ) -> Result<(i64, u64, u64), DriverError> {
        let other = |e: String| DriverError::from(tidb_terror::MysqlError::unknown(e));
        let chunk = Chunk {
            path: file.path.clone(),
            file_size: file.file_size,
            end_offset: MAX_SAMPLE_FILE_SIZE.load(Ordering::Relaxed),
            r#type: file.r#type as i32,
            compression: file.compression as i32,
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map_or(0, |d| d.as_secs() as i64),
            parquet_meta: file.parquet_meta.clone(),
            ..Chunk::default()
        };
        let allocators = PanickingAllocators::new(self.table.meta().read().sep_auto_inc());
        let mut encoder = self.get_kv_encoder(&chunk, allocators).map_err(other)?;
        let mut parser = self.get_parser(ctx, &chunk).map_err(other)?;
        let parquet = self.plan.format == "parquet";
        let mut source_size = 0i64;
        let mut batch = EncodedKvGroupBatch::new(ks_codec, max_row_count);
        let key = chunk.get_key();
        let encode_err = |cause: std::sync::Arc<dyn std::error::Error + Send + Sync>, pos: i64| {
            DriverError::from(super::chunk_process::encode_kv_error(cause, &key, pos))
        };
        for _ in 0..max_row_count {
            let (start_pos, _) = parser.pos();
            if !parquet && start_pos >= chunk.end_offset {
                break;
            }
            match parser.read_row() {
                Ok(()) => {}
                Err(tidb_lightning_mydump::CsvError::Eof) => break,
                Err(e) => return Err(encode_err(std::sync::Arc::new(std::io::Error::other(e.to_string())), start_pos)),
            }
            let row = parser.last_row().clone();
            source_size += sampled_row_source_size(parquet, parser.as_ref(), start_pos, row.length);
            let kvs = encoder.encode(&row.row, row.row_id).map_err(|e| encode_err(std::sync::Arc::new(e), start_pos))?;
            batch.add(kvs).map_err(other)?;
        }
        let (data, index) = batch.group_checksum.data_and_index_sum_size();
        Ok((source_size, data, index))
    }
}

/// Go `sampledRowSourceSize`: the bytes the parser moved over the row. A
/// Parquet position counts rows, so its row size estimate is used.
fn sampled_row_source_size(
    parquet: bool,
    parser: &(dyn tidb_lightning_mydump::Parser + Send),
    start_pos: i64,
    length: usize,
) -> i64 {
    if parquet {
        return length as i64;
    }
    let (end_pos, _) = parser.pos();
    let delta = end_pos - start_pos;
    if delta > 0 { delta } else { length as i64 }
}

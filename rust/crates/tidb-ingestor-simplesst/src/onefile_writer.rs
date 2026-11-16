// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
//! Go `onefile_writer.go`: writes already sorted KV pairs into a single data
//! file and stat file, handling duplicates as they arrive.

use std::sync::Arc;

use tidb_gocontext::Context;
use tidb_ingestor_engineapi::{ConflictInfo, OnDuplicateKey};
use tidb_lightning_membuf::Buffer;
use tidb_objstore::objectio::Writer as ObjWriter;
use tidb_objstore::storeapi::{Storage, WriterOption};

use crate::file::{encode_to_buf, KeyValueStore, DUP_SUFFIX, LENGTH_BYTES, STAT_SUFFIX};
use crate::writer::{
    found_duplicate_keys, int_field, rand_partitioned_prefix, str_field, MultipleFilesStat,
    OnWriterCloseFunc, RangePropertiesCollector, WriterSummary, MAX_UPLOAD_WORKERS_PER_THREAD,
    MIN_UPLOAD_PART_SIZE,
};
use crate::{Error, Result};

/// Go `DefaultOneWriterMemSizeLimit`: 128 MiB.
pub const DEFAULT_ONE_WRITER_MEM_SIZE_LIMIT: u64 = 128 << 20;
/// Go `DefaultOneWriterBlockSize`.
pub const DEFAULT_ONE_WRITER_BLOCK_SIZE: usize = DEFAULT_ONE_WRITER_MEM_SIZE_LIMIT as usize;
/// Go `MaxUploadPartCount`: S3's and GCS's part limit.
pub const MAX_UPLOAD_PART_COUNT: i64 = 5000;
/// Go `logPartNumInterval`.
const LOG_PART_NUM_INTERVAL: u64 = 999;
/// Go `units.MiB * 16`: how often the merge-sort write bytes are counted.
const WRITE_BYTES_METRIC_INTERVAL: i64 = 16 << 20;

/// Go `OneFileWriter`.
pub struct OneFileWriter {
    store: Arc<dyn Storage>,
    kv_buffer: Buffer,
    total_size: u64,
    total_cnt: u64,
    rc: RangePropertiesCollector,
    writer_id: String,
    filename_prefix: String,
    rnd: tidb_go_rand::Rand,
    data_file: String,
    stat_file: String,
    data_writer: Option<Box<dyn ObjWriter>>,
    stat_writer: Option<Box<dyn ObjWriter>>,
    /// Go `kvStore.offset`.
    data_offset: u64,
    on_close: OnWriterCloseFunc,
    closed: bool,
    on_dup: OnDuplicateKey,
    /// Go `pivotKey` / `pivotValue`: the key being checked for duplicates
    /// and its first value.
    pivot_key: Vec<u8>,
    pivot_value: Vec<u8>,
    /// Go `currDupCnt`: pairs seen with the pivot key; 0 when none yet.
    curr_dup_cnt: usize,
    recorded_dup_cnt: usize,
    dup_file: String,
    dup_writer: Option<Box<dyn ObjWriter>>,
    dup_offset: u64,
    /// Go `minKey`: `None` until the first written row.
    min_key: Option<Vec<u8>>,
    max_key: Vec<u8>,
    part_size: i64,
    written_bytes: i64,
    last_log_write_size: u64,
}

impl OneFileWriter {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        store: Arc<dyn Storage>,
        kv_buffer: Buffer,
        rc: RangePropertiesCollector,
        rnd: tidb_go_rand::Rand,
        filename_prefix: String,
        writer_id: String,
        on_close: OnWriterCloseFunc,
        on_dup: OnDuplicateKey,
    ) -> Self {
        Self {
            store,
            kv_buffer,
            total_size: 0,
            total_cnt: 0,
            rc,
            writer_id,
            filename_prefix,
            rnd,
            data_file: String::new(),
            stat_file: String::new(),
            data_writer: None,
            stat_writer: None,
            data_offset: 0,
            on_close,
            closed: false,
            on_dup,
            pivot_key: Vec::new(),
            pivot_value: Vec::new(),
            curr_dup_cnt: 0,
            recorded_dup_cnt: 0,
            dup_file: String::new(),
            dup_writer: None,
            dup_offset: 0,
            min_key: None,
            max_key: Vec::new(),
            part_size: 0,
            written_bytes: 0,
            last_log_write_size: 0,
        }
    }

    /// Go `lazyInitWriter`.
    fn lazy_init_writer(&mut self, ctx: &Context) -> Result<()> {
        if self.data_writer.is_some() {
            return Ok(());
        }
        let data_file = tidb_objstore::gopath::join(&[&self.get_partitioned_prefix(), "one-file"]);
        let mut data_writer = self.store.create(
            ctx,
            &data_file,
            Some(&WriterOption {
                concurrency: MAX_UPLOAD_WORKERS_PER_THREAD,
                part_size: self.part_size,
            }),
        )?;
        let stat_file = tidb_objstore::gopath::join(&[
            &(self.get_partitioned_prefix() + STAT_SUFFIX),
            "one-file",
        ]);
        let stat_writer = match self.store.create(
            ctx,
            &stat_file,
            Some(&WriterOption {
                concurrency: MAX_UPLOAD_WORKERS_PER_THREAD,
                part_size: MIN_UPLOAD_PART_SIZE,
            }),
        ) {
            Ok(w) => w,
            Err(e) => {
                tidb_log::info(
                    "create stat writer failed",
                    &[str_field("error", &e.to_string())],
                );
                let _ = data_writer.close(ctx);
                return Err(e.into());
            }
        };
        tidb_log::info(
            "one file writer",
            &[
                str_field("data-file", &data_file),
                str_field("stat-file", &stat_file),
                str_field("on-dup", &self.on_dup.to_string()),
            ],
        );
        self.data_file = data_file;
        self.data_writer = Some(data_writer);
        self.stat_file = stat_file;
        self.stat_writer = Some(stat_writer);
        self.data_offset = 0;
        Ok(())
    }

    /// Go `lazyInitDupFile`.
    fn lazy_init_dup_file(&mut self, ctx: &Context) -> Result<()> {
        if self.dup_writer.is_some() {
            return Ok(());
        }
        let dup_file = tidb_objstore::gopath::join(&[
            &(self.get_partitioned_prefix() + DUP_SUFFIX),
            "one-file",
        ]);
        let dup_writer = self
            .store
            .create(
                ctx,
                &dup_file,
                Some(&WriterOption {
                    concurrency: 1,
                    part_size: self.part_size,
                }),
            )
            .inspect_err(|e| {
                tidb_log::info(
                    "create dup writer failed",
                    &[str_field("error", &e.to_string())],
                )
            })?;
        self.dup_file = dup_file;
        self.dup_writer = Some(dup_writer);
        self.dup_offset = 0;
        Ok(())
    }

    /// Go `InitPartSizeAndLogger`: must be called before writing.
    pub fn init_part_size_and_logger(&mut self, _ctx: &Context, part_size: i64) {
        self.part_size = part_size;
    }

    /// Go `WriteRow`: keys must arrive sorted.
    ///
    /// # Errors
    /// A file could not be written, or a duplicate key with
    /// [`OnDuplicateKey::Error`].
    pub fn write_row(&mut self, ctx: &Context, idx_key: &[u8], idx_val: &[u8]) -> Result<()> {
        let result = if self.on_dup == OnDuplicateKey::Ignore {
            self.do_write_row(ctx, idx_key, idx_val)
        } else {
            self.handle_dup_and_write(ctx, idx_key, idx_val)
        };
        // Go divides by `partSize` and so panics when it was never set.
        if let Some(parts) =
            (self.total_size - self.last_log_write_size).checked_div(self.part_size as u64)
        {
            if parts >= LOG_PART_NUM_INTERVAL {
                tidb_log::info(
                    "one file writer progress",
                    &[
                        str_field("writerID", &self.writer_id),
                        int_field("partSize", self.part_size),
                        tidb_log::Field::new("totalSize", tidb_log::Value::U64(self.total_size)),
                        tidb_log::Field::new(
                            "estimatePartNum",
                            tidb_log::Value::U64(self.total_size / self.part_size as u64),
                        ),
                    ],
                );
                self.last_log_write_size = self.total_size;
            }
        }
        result
    }

    /// Go `handleDupAndWrite`.
    fn handle_dup_and_write(
        &mut self,
        ctx: &Context,
        idx_key: &[u8],
        idx_val: &[u8],
    ) -> Result<()> {
        if self.curr_dup_cnt == 0 || self.pivot_key != idx_key {
            return self.on_next_pivot(ctx, Some((idx_key, idx_val)));
        }
        self.curr_dup_cnt += 1;
        match self.on_dup {
            OnDuplicateKey::Record => {
                // Keep the first 2 pairs in the data file, record the rest.
                if self.curr_dup_cnt == 2 {
                    let (pk, pv) = (
                        std::mem::take(&mut self.pivot_key),
                        std::mem::take(&mut self.pivot_value),
                    );
                    let r = self.do_write_row(ctx, &pk, &pv);
                    (self.pivot_key, self.pivot_value) = (pk, pv);
                    r?;
                    self.do_write_row(ctx, idx_key, idx_val)?;
                } else {
                    self.lazy_init_dup_file(ctx)?;
                    let writer = self.dup_writer.as_mut().expect("initialized");
                    let mut store =
                        KeyValueStore::resume(ctx, writer.as_mut(), None, self.dup_offset);
                    let r = store.add_raw_kv(idx_key, idx_val);
                    self.dup_offset = store.offset;
                    r?;
                    self.recorded_dup_cnt += 1;
                }
            }
            OnDuplicateKey::Error => return Err(found_duplicate_keys(idx_key, idx_val)),
            OnDuplicateKey::Remove | OnDuplicateKey::Ignore => {}
        }
        Ok(())
    }

    /// Go `onNextPivot`: a lone pivot pair is written once the key changes.
    fn on_next_pivot(&mut self, ctx: &Context, next: Option<(&[u8], &[u8])>) -> Result<()> {
        if self.curr_dup_cnt == 1 {
            let (pk, pv) = (
                std::mem::take(&mut self.pivot_key),
                std::mem::take(&mut self.pivot_value),
            );
            let r = self.do_write_row(ctx, &pk, &pv);
            (self.pivot_key, self.pivot_value) = (pk, pv);
            r?;
        }
        match next {
            Some((k, v)) => {
                self.pivot_key = k.to_vec();
                self.pivot_value = v.to_vec();
                self.curr_dup_cnt = 1;
            }
            None => {
                self.pivot_key = Vec::new();
                self.pivot_value = Vec::new();
                self.curr_dup_cnt = 0;
            }
        }
        Ok(())
    }

    /// Go `doWriteRow`.
    fn do_write_row(&mut self, ctx: &Context, idx_key: &[u8], idx_val: &[u8]) -> Result<()> {
        if self.min_key.is_none() {
            self.min_key = Some(idx_key.to_vec());
        }
        self.lazy_init_writer(ctx)?;
        let length = idx_key.len() + idx_val.len() + LENGTH_BYTES * 2;
        let (mut buf, _) = self
            .kv_buffer
            .alloc_bytes_with_slice_location(length as isize);
        if buf.is_none() {
            self.kv_buffer.reset();
            (buf, _) = self
                .kv_buffer
                .alloc_bytes_with_slice_location(length as isize);
            if buf.is_none() {
                return Err(Error::Other(format!(
                    "failed to allocate kv buffer: {length}"
                )));
            }
            // The buffer refilled: write the stat of what was written so far.
            self.rc.on_file_end();
            let encoded_stat = self.rc.encode();
            self.stat_writer
                .as_mut()
                .expect("initialized")
                .write(ctx, &encoded_stat)?;
            self.rc.reset();
            self.rc.curr_prop.offset = self.data_offset;
        }
        let buf = buf.expect("allocated");
        let mut data = buf.as_mut_slice();
        encode_to_buf(&mut data, idx_key, idx_val);
        self.max_key = idx_key.to_vec();
        let writer = self.data_writer.as_mut().expect("initialized");
        let mut store =
            KeyValueStore::resume(ctx, writer.as_mut(), Some(&mut self.rc), self.data_offset);
        let r = store.add_encoded_data(&data[..length]);
        self.data_offset = store.offset;
        r?;
        self.total_cnt += 1;
        self.total_size += (idx_key.len() + idx_val.len()) as u64;
        self.written_bytes += length as i64;
        if self.written_bytes >= WRITE_BYTES_METRIC_INTERVAL {
            tidb_metrics::globalsort::MERGE_SORT_WRITE_BYTES.inc_by(self.written_bytes as f64);
            self.written_bytes = 0;
        }
        Ok(())
    }

    /// Go `Close`.
    ///
    /// # Errors
    /// The writer was closed already, or a file failed to finish.
    pub fn close(&mut self, ctx: &Context) -> Result<()> {
        if self.closed {
            return Err(Error::Other(format!(
                "writer {} has been closed",
                self.writer_id
            )));
        }
        self.close_impl(ctx)?;
        tidb_log::info(
            "close one file writer",
            &[
                str_field("writerID", &self.writer_id),
                tidb_log::Field::new("totalCnt", tidb_log::Value::U64(self.total_cnt)),
                tidb_log::Field::new("totalSize", tidb_log::Value::U64(self.total_size)),
                int_field("recordedDupCnt", self.recorded_dup_cnt as i64),
            ],
        );
        let (mut min_key, mut max_key) = (Vec::new(), Vec::new());
        let mut m_stats = Vec::with_capacity(1);
        if self.total_cnt > 0 {
            min_key = self.min_key.clone().unwrap_or_default();
            max_key = self.max_key.clone();
            let mut stat = MultipleFilesStat::default();
            stat.filenames
                .push([self.data_file.clone(), self.stat_file.clone()]);
            stat.build(&mut [min_key.clone()], &[max_key.clone()]);
            m_stats.push(stat);
        }
        let mut conflict_info = ConflictInfo::default();
        if self.recorded_dup_cnt > 0 {
            conflict_info.count = self.recorded_dup_cnt as u64;
            conflict_info.files = vec![self.dup_file.clone()];
        }
        (self.on_close)(&WriterSummary {
            writer_id: self.writer_id.clone(),
            group_offset: 0,
            seq: 0,
            min: min_key,
            max: max_key,
            total_size: self.total_size,
            total_cnt: self.total_cnt,
            kv_file_count: 1,
            multiple_files_stats: m_stats,
            conflict_info,
        });
        self.total_cnt = 0;
        self.total_size = 0;
        self.closed = true;
        Ok(())
    }

    /// Go `closeImpl`.
    fn close_impl(&mut self, ctx: &Context) -> Result<()> {
        self.on_next_pivot(ctx, None)?;
        if let Some(data_writer) = self.data_writer.as_mut() {
            self.rc.on_file_end();
            let encoded_stat = self.rc.encode();
            let stat_writer = self.stat_writer.as_mut().expect("initialized");
            stat_writer.write(ctx, &encoded_stat)?;
            self.rc.reset();
            data_writer.close(ctx).inspect_err(|e| {
                tidb_log::error(
                    "Close data writer failed",
                    &[str_field("error", &e.to_string())],
                );
            })?;
            stat_writer.close(ctx).inspect_err(|e| {
                tidb_log::error(
                    "Close stat writer failed",
                    &[str_field("error", &e.to_string())],
                );
            })?;
        }
        if let Some(dup_writer) = self.dup_writer.as_mut() {
            dup_writer.close(ctx).inspect_err(|e| {
                tidb_log::error(
                    "Close dup writer failed",
                    &[str_field("error", &e.to_string())],
                );
            })?;
        }
        Ok(())
    }

    fn get_partitioned_prefix(&mut self) -> String {
        rand_partitioned_prefix(&self.filename_prefix, &mut self.rnd)
    }
}

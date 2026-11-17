// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
//! Go `engine.go`: the external engine. It loads a write-and-ingest
//! subtask's range jobs from the sorted files, a batch of jobs at a time,
//! sorts and deduplicates them in memory, and hands them to the ingest
//! backend as [`MemoryIngestData`].

use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;
use std::time::Instant;

use tidb_gocontext::Context;
use tidb_ingestor_engineapi::{
    ConflictInfo, DataAndRanges, Error as EngineError, ForwardIter, IngestData, OnDuplicateKey, Range,
};
use tidb_ingestor_simplesst::{KVPair, KeyValueStore, MIN_UPLOAD_PART_SIZE};
use tidb_objstore::objectio::Writer as ObjWriter;
use tidb_objstore::storeapi::{Storage, WriterOption};

use crate::reader::read_all_data;
use crate::split::WRITE_STEP_MEM_SHARE_COUNT;
use crate::{int_field, str_field, Result};

/// Go `getEngineMemoryLimit`.
fn get_engine_memory_limit(mem_capacity: i64) -> i64 {
    (mem_capacity as f64 / WRITE_STEP_MEM_SHARE_COUNT * 3.0) as i64
}

/// Go `memKVsAndBuffers`: the pairs read for a batch of range jobs.
#[derive(Debug, Default)]
pub(crate) struct MemKVsAndBuffers {
    pub(crate) kvs: Vec<KVPair>,
    pub(crate) size: usize,
    pub(crate) dropped_size: usize,
    pub(crate) kvs_per_file: Vec<Vec<KVPair>>,
    pub(crate) dropped_size_per_file: Vec<usize>,
}

impl MemKVsAndBuffers {
    /// Go `build`: concatenates the pairs of every file.
    pub(crate) fn build(&mut self) {
        let sum_kv_cnt: usize = self.kvs_per_file.iter().map(Vec::len).sum();
        self.dropped_size = self.dropped_size_per_file.drain(..).sum();
        tidb_log::info(
            "building memKVsAndBuffers",
            &[int_field("sumKVCnt", sum_kv_cnt as i64), int_field("droppedSize", self.dropped_size as i64)],
        );
        let mut kvs = Vec::with_capacity(sum_kv_cnt);
        for file in self.kvs_per_file.drain(..) {
            kvs.extend(file);
        }
        self.kvs = kvs;
    }
}

/// Go `EngineConfig`.
#[derive(Clone)]
pub struct EngineConfig {
    /// Go `ExtStore`.
    pub ext_store: Arc<dyn Storage>,
    /// Go `DataFiles`.
    pub data_files: Vec<String>,
    /// Go `StatFiles`.
    pub stat_files: Vec<String>,
    /// Go `StartKey`.
    pub start_key: Vec<u8>,
    /// Go `EndKey`.
    pub end_key: Vec<u8>,
    /// Go `JobKeys`: the range job boundaries, start and end included.
    pub job_keys: Vec<Vec<u8>>,
    /// Go `SplitKeys`: the region split keys, start and end included.
    pub split_keys: Vec<Vec<u8>>,
    /// Go `TotalFileSize`.
    pub total_file_size: i64,
    /// Go `TotalKVCount`.
    pub total_kv_count: i64,
    /// Go `CheckHotspot`.
    pub check_hotspot: bool,
    /// Go `MemCapacity`.
    pub mem_capacity: i64,
    /// Go `OnDup`.
    pub on_dup: OnDuplicateKey,
    /// Go `FilePrefix`: where the dup file goes.
    pub file_prefix: String,
}

/// Go `Engine` (the external engine).
pub struct Engine {
    storage: Arc<dyn Storage>,
    data_files: Vec<String>,
    stats_files: Vec<String>,
    start_key: Vec<u8>,
    end_key: Vec<u8>,
    job_keys: Vec<Vec<u8>>,
    split_keys: Vec<Vec<u8>>,
    mem_kvs_and_buffers: MemKVsAndBuffers,
    total_loaded_kvs_count: i64,
    worker_concurrency: usize,
    ts: u64,
    total_kv_size: i64,
    total_kv_count: i64,
    imported_kv_size: Arc<AtomicI64>,
    imported_kv_count: Arc<AtomicI64>,
    mem_limit: i64,
    on_dup: OnDuplicateKey,
    file_prefix: String,
    recorded_dup_cnt: usize,
    recorded_dup_size: i64,
    dup_file: String,
    dup_writer: Option<Box<dyn ObjWriter>>,
    dup_offset: u64,
}

impl Engine {
    /// Go `NewExternalEngine`.
    #[must_use]
    pub fn new(cfg: EngineConfig, worker_concurrency: usize, ts: u64) -> Self {
        let mem_limit = get_engine_memory_limit(cfg.mem_capacity);
        tidb_log::info(
            "create external engine",
            &[
                str_field("memLimitForLoadRange", &tidb_config_configtypes::bytes_size(mem_limit as f64)),
                int_field("dataFileCount", cfg.data_files.len() as i64),
                int_field("jobKeysCount", cfg.job_keys.len() as i64),
                int_field("splitKeysCount", cfg.split_keys.len() as i64),
            ],
        );
        Self {
            storage: cfg.ext_store,
            data_files: cfg.data_files,
            stats_files: cfg.stat_files,
            start_key: cfg.start_key,
            end_key: cfg.end_key,
            job_keys: cfg.job_keys,
            split_keys: cfg.split_keys,
            mem_kvs_and_buffers: MemKVsAndBuffers::default(),
            total_loaded_kvs_count: 0,
            worker_concurrency: worker_concurrency.max(1),
            ts,
            total_kv_size: cfg.total_file_size,
            total_kv_count: cfg.total_kv_count,
            imported_kv_size: Arc::new(AtomicI64::new(0)),
            imported_kv_count: Arc::new(AtomicI64::new(0)),
            mem_limit,
            on_dup: cfg.on_dup,
            file_prefix: cfg.file_prefix,
            recorded_dup_cnt: 0,
            recorded_dup_size: 0,
            dup_file: String::new(),
            dup_writer: None,
            dup_offset: 0,
        }
    }

    /// Go `GetTotalLoadedKVsCount`: the pairs handed to the backend after
    /// deduplication.
    #[must_use]
    pub fn get_total_loaded_kvs_count(&self) -> i64 {
        self.total_loaded_kvs_count
    }

    /// Go `GetOnDup`.
    #[must_use]
    pub fn get_on_dup(&self) -> OnDuplicateKey {
        self.on_dup
    }

    /// The memory limit of a loaded batch.
    #[must_use]
    pub fn mem_limit(&self) -> i64 {
        self.mem_limit
    }

    /// Go `loadRangeBatchData`: loads `job_keys[0]..job_keys[last]` and
    /// hands it, with one range per job, to `out`.
    fn load_range_batch_data(
        &mut self,
        ctx: &Context,
        job_keys: &[Vec<u8>],
        start_offsets: &[u64],
        estimated_end_offsets: &[u64],
        out: &mut dyn FnMut(DataAndRanges) -> std::result::Result<(), EngineError>,
    ) -> Result<()> {
        let start_key = &job_keys[0];
        let end_key = &job_keys[job_keys.len() - 1];
        let read_start = Instant::now();
        read_all_data(
            ctx,
            &self.storage,
            &self.data_files,
            &self.stats_files,
            start_key,
            end_key,
            start_offsets,
            estimated_end_offsets,
            &mut self.mem_kvs_and_buffers,
        )?;
        self.mem_kvs_and_buffers.build();
        let read_dur = read_start.elapsed();
        let hist = |name: &str| tidb_metrics::globalsort::READ_FROM_CLOUD_STORAGE_DURATION.with_label_values(&[name]);
        let rate = |name: &str| tidb_metrics::globalsort::READ_FROM_CLOUD_STORAGE_RATE.with_label_values(&[name]);
        hist("read").observe(read_dur.as_secs_f64());

        let sort_start = Instant::now();
        let kvs = &mut self.mem_kvs_and_buffers.kvs;
        kvs.sort_by(|a, b| a.key.cmp(&b.key));
        let dup = kvs.windows(2).find(|w| w[0].key == w[1].key).map(|w| (w[0].key.clone(), w[0].value.clone()));
        let sort_dur = sort_start.elapsed();
        hist("sort").observe(sort_dur.as_secs_f64());
        if let Some((k, v)) = &dup {
            match self.on_dup {
                OnDuplicateKey::Ignore => {
                    return Err(crate::other(format!("duplicate key found: {}", hex::encode(k))));
                }
                OnDuplicateKey::Error => {
                    return Err(Box::new(tidb_ingestor_simplesst::found_duplicate_keys(k, v)));
                }
                OnDuplicateKey::Record | OnDuplicateKey::Remove => {}
            }
        }
        let read_and_sort = read_start.elapsed().as_secs_f64();
        hist("read_and_sort").observe(read_and_sort);
        let size = self.mem_kvs_and_buffers.size as f64 / 1024.0 / 1024.0;
        rate("read_and_sort").observe(size / read_and_sort);
        rate("read").observe(size / read_dur.as_secs_f64());
        rate("sort").observe(size / sort_dur.as_secs_f64());

        let mut deduplicated = std::mem::take(&mut self.mem_kvs_and_buffers.kvs);
        let loaded_kvs = deduplicated.len();
        let (mut dup_count, mut recorded) = (0, 0);
        let dedup_start = Instant::now();
        if dup.is_some() {
            match self.on_dup {
                OnDuplicateKey::Record => {
                    self.lazy_init_dup_writer(ctx)?;
                    let (kept, dups, total) =
                        tidb_ingestor_simplesst::remove_duplicates(deduplicated, |p: &KVPair| p.key.clone(), true);
                    deduplicated = kept;
                    dup_count = total;
                    recorded = dups.len();
                    self.recorded_dup_cnt += dups.len();
                    let writer = self.dup_writer.as_mut().expect("initialized");
                    let mut store = KeyValueStore::resume(ctx, writer.as_mut(), None, self.dup_offset);
                    let mut result = Ok(());
                    for p in &dups {
                        self.recorded_dup_size += (p.key.len() + p.value.len()) as i64;
                        if let Err(e) = store.add_raw_kv(&p.key, &p.value) {
                            result = Err(crate::sst(e));
                            break;
                        }
                    }
                    self.dup_offset = store.offset();
                    result?;
                }
                OnDuplicateKey::Remove => {
                    let (kept, _, total) =
                        tidb_ingestor_simplesst::remove_duplicates(deduplicated, |p: &KVPair| p.key.clone(), false);
                    deduplicated = kept;
                    dup_count = total;
                }
                OnDuplicateKey::Ignore | OnDuplicateKey::Error => {}
            }
        }
        tidb_log::info(
            "load range batch done",
            &[
                int_field("readDurMs", read_dur.as_millis() as i64),
                int_field("sortDurMs", sort_dur.as_millis() as i64),
                int_field("droppedSize", self.mem_kvs_and_buffers.dropped_size as i64),
                int_field("loadedKVs", loaded_kvs as i64),
                int_field("loadedSize", self.mem_kvs_and_buffers.size as i64),
                str_field("onDup", &self.on_dup.to_string()),
                int_field("dupCount", dup_count as i64),
                int_field("recordedDupCount", recorded as i64),
                int_field("totalRecordedDupCount", self.recorded_dup_cnt as i64),
                int_field("totalRecordedDupSize", self.recorded_dup_size),
                int_field("deduplicateDurMs", dedup_start.elapsed().as_millis() as i64),
            ],
        );
        self.total_loaded_kvs_count += deduplicated.len() as i64;
        self.mem_kvs_and_buffers.size = 0;

        let mut ranges = Vec::with_capacity(job_keys.len() - 1);
        let mut prev = job_keys[0].clone();
        for key in &job_keys[1..job_keys.len() - 1] {
            ranges.push(Range { start: prev, end: key.clone() });
            prev = key.clone();
        }
        ranges.push(Range { start: prev, end: job_keys[job_keys.len() - 1].clone() });
        if let Some(e) = ctx.err() {
            return Err(crate::other(e.to_string()));
        }
        let data = Arc::new(MemoryIngestData {
            kvs: deduplicated,
            ts: self.ts,
            imported_kv_size: Arc::clone(&self.imported_kv_size),
            imported_kv_count: Arc::clone(&self.imported_kv_count),
        });
        out(DataAndRanges { data, sorted_ranges: ranges })
    }

    /// Go `lazyInitDupWriter`.
    fn lazy_init_dup_writer(&mut self, ctx: &Context) -> Result<()> {
        if self.dup_writer.is_some() {
            return Ok(());
        }
        let dup_file = tidb_objstore::gopath::join(&[&self.file_prefix, "dup"]);
        let writer = self
            .storage
            .create(ctx, &dup_file, Some(&WriterOption { concurrency: 1, part_size: 3 * MIN_UPLOAD_PART_SIZE }))
            .map_err(|e| {
                tidb_log::error("create dup writer failed", &[str_field("error", &e.to_string())]);
                crate::sst(e.into())
            })?;
        self.dup_file = dup_file;
        self.dup_writer = Some(writer);
        self.dup_offset = 0;
        Ok(())
    }

    /// Go `closeDupWriterAsNeeded`.
    fn close_dup_writer_as_needed(&mut self, ctx: &Context) -> Result<()> {
        let Some(mut writer) = self.dup_writer.take() else {
            return Ok(());
        };
        writer.close(ctx).map_err(|e| {
            tidb_log::error("close dup writer failed", &[str_field("error", &e.to_string())]);
            crate::sst(e.into())
        })
    }

    fn load(
        &mut self,
        ctx: &Context,
        out: &mut dyn FnMut(DataAndRanges) -> std::result::Result<(), EngineError>,
    ) -> Result<()> {
        let batch_size = self.worker_concurrency;
        tidb_log::info("load ingest data", &[int_field("current batchSize", batch_size as i64)]);
        let read_ranges_per_key = tidb_ingestor_simplesst::get_read_range_from_props(
            ctx,
            &self.job_keys,
            &self.stats_files,
            self.storage.as_ref(),
        )
        .map_err(crate::sst)?;
        let job_keys = self.job_keys.clone();
        let mut start = 0;
        while start + 1 < job_keys.len() {
            let end = (1 + start + batch_size).min(job_keys.len());
            self.load_range_batch_data(
                ctx,
                &job_keys[start..end],
                &read_ranges_per_key[start],
                &read_ranges_per_key[end - 1],
                out,
            )?;
            start += batch_size;
        }
        Ok(())
    }
}

impl tidb_ingestor_engineapi::Engine for Engine {
    fn id(&self) -> String {
        "external".to_owned()
    }

    /// Go `LoadIngestData`: batches of `worker_concurrency` range jobs. With
    /// `Record`, the dup file is closed at the end.
    fn load_ingest_data(
        &mut self,
        ctx: &Context,
        out: &mut dyn FnMut(DataAndRanges) -> std::result::Result<(), EngineError>,
    ) -> std::result::Result<(), EngineError> {
        let result = self.load(ctx, out);
        if self.on_dup == OnDuplicateKey::Record {
            let closed = self.close_dup_writer_as_needed(ctx);
            return result.and(closed);
        }
        result
    }

    fn kv_statistics(&self) -> (i64, i64) {
        (self.total_kv_size, self.total_kv_count)
    }

    fn imported_statistics(&self) -> (i64, i64) {
        (self.imported_kv_size.load(Ordering::SeqCst), self.imported_kv_count.load(Ordering::SeqCst))
    }

    fn conflict_info(&self) -> ConflictInfo {
        if self.recorded_dup_cnt == 0 {
            return ConflictInfo::default();
        }
        ConflictInfo { count: self.recorded_dup_cnt as u64, files: vec![self.dup_file.clone()] }
    }

    fn get_key_range(&self) -> std::result::Result<(Vec<u8>, Vec<u8>), EngineError> {
        Ok((self.start_key.clone(), self.end_key.clone()))
    }

    fn get_region_split_keys(&self) -> std::result::Result<Vec<Vec<u8>>, EngineError> {
        Ok(self.split_keys.clone())
    }

    fn close(&mut self) -> std::result::Result<(), EngineError> {
        self.mem_kvs_and_buffers = MemKVsAndBuffers::default();
        Ok(())
    }
}

/// Go `MemoryIngestData`: sorted pairs in memory.
pub struct MemoryIngestData {
    kvs: Vec<KVPair>,
    ts: u64,
    imported_kv_size: Arc<AtomicI64>,
    imported_kv_count: Arc<AtomicI64>,
}

impl MemoryIngestData {
    /// Go `firstAndLastKeyIndex`: the indexes of the first and last pairs in
    /// `[lower, upper)`, or `None`.
    fn first_and_last_key_index(&self, lower: &[u8], upper: &[u8]) -> Option<(usize, usize)> {
        let n = self.kvs.len();
        let mut first = 0;
        if !lower.is_empty() {
            first = self.kvs.partition_point(|p| p.key.as_slice() < lower);
            if first == n {
                return None;
            }
        }
        let mut last = n.checked_sub(1)?;
        if !upper.is_empty() {
            let below = self.kvs.partition_point(|p| p.key.as_slice() < upper);
            last = below.checked_sub(1)?;
        }
        Some((first, last))
    }

    /// The pairs.
    #[must_use]
    pub fn kvs(&self) -> &[KVPair] {
        &self.kvs
    }
}

struct MemoryDataIter<'a> {
    kvs: &'a [KVPair],
    cur: usize,
    last: Option<usize>,
}

impl ForwardIter for MemoryDataIter<'_> {
    fn next_pair(&mut self) -> std::result::Result<Option<(Vec<u8>, Vec<u8>)>, EngineError> {
        match self.last {
            Some(last) if self.cur <= last => {
                let p = &self.kvs[self.cur];
                self.cur += 1;
                Ok(Some((p.key.clone(), p.value.clone())))
            }
            _ => Ok(None),
        }
    }
}

impl IngestData for MemoryIngestData {
    fn get_first_and_last_key(
        &self,
        lower: &[u8],
        upper: &[u8],
    ) -> std::result::Result<Option<(Vec<u8>, Vec<u8>)>, EngineError> {
        Ok(self
            .first_and_last_key_index(lower, upper)
            .filter(|(f, l)| f <= l)
            .map(|(f, l)| (self.kvs[f].key.clone(), self.kvs[l].key.clone())))
    }

    fn new_iter<'a>(&'a self, lower: &[u8], upper: &[u8]) -> std::result::Result<Box<dyn ForwardIter + 'a>, EngineError> {
        let (cur, last) = match self.first_and_last_key_index(lower, upper) {
            Some((f, l)) => (f, Some(l)),
            None => (0, None),
        };
        Ok(Box::new(MemoryDataIter { kvs: &self.kvs, cur, last }))
    }

    fn get_ts(&self) -> u64 {
        self.ts
    }

    fn finish(&self, total_bytes: i64, total_count: i64) {
        self.imported_kv_size.fetch_add(total_bytes, Ordering::SeqCst);
        self.imported_kv_count.fetch_add(total_count, Ordering::SeqCst);
    }
}

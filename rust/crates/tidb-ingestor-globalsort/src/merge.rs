// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
//! Go `merge.go`: merge sort of overlapping sorted KV files into fewer,
//! non-overlapping ones.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use tidb_dxf_execute::{Collector, SubtaskSummary};
use tidb_gocontext::Context;
use tidb_ingestor_engineapi::OnDuplicateKey;
use tidb_ingestor_simplesst::{
    MergeKVIter, OnWriterCloseFunc, WriterBuilder, DEFAULT_ONE_WRITER_MEM_SIZE_LIMIT, DEFAULT_READ_BUFFER_SIZE,
    MAX_UPLOAD_PART_COUNT, MIN_UPLOAD_PART_SIZE,
};
use tidb_objstore::storeapi::Storage;

use crate::{int_field, str_field, Result};

/// Go `MaxMergingFilesPerThread`.
pub const MAX_MERGING_FILES_PER_THREAD: usize = 250;

/// Go `maxMergeReaderMemoryPerCore`: 256 MiB.
const MAX_MERGE_READER_MEMORY_PER_CORE: i64 = 256 << 20;

/// Go `mergeCollector`: merged bytes and pairs into the subtask summary.
pub struct MergeCollector {
    summary: Option<Arc<SubtaskSummary>>,
}

impl MergeCollector {
    /// Go `NewMergeCollector`. Go also adds the bytes to Lightning's
    /// `BytesCounter{state="merged"}` when the context carries the task's
    /// common metrics; this port has no such metrics.
    #[must_use]
    pub fn new(summary: Option<Arc<SubtaskSummary>>) -> Self {
        Self { summary }
    }
}

impl Collector for MergeCollector {
    fn accepted(&self, _: i64) {}

    fn processed(&self, bytes: i64, rows: i64) {
        if let Some(s) = &self.summary {
            s.processed.fetch_add(bytes, Ordering::SeqCst);
            s.row_cnt.fetch_add(rows, Ordering::SeqCst);
        }
    }
}

/// Go `MergeOperator`: merges groups of files on `concurrency` workers,
/// each writing one file with a [`tidb_ingestor_simplesst::OneFileWriter`].
pub struct MergeOperator {
    store: Arc<dyn Storage>,
    total_reader_memory_size: i64,
    new_file_prefix: String,
    block_size: usize,
    on_writer_close: OnWriterCloseFunc,
    collector: Option<Arc<dyn Collector>>,
    concurrency: usize,
    check_hotspot: bool,
    on_dup: OnDuplicateKey,
}

/// Go `getMergeReaderMemory`.
fn get_merge_reader_memory(memory_per_core: i64, concurrency: usize) -> i64 {
    MAX_MERGE_READER_MEMORY_PER_CORE.min(memory_per_core / 5) * concurrency as i64
}

impl MergeOperator {
    /// Go `NewMergeOperator`.
    #[allow(clippy::too_many_arguments)]
    #[must_use]
    pub fn new(
        store: Arc<dyn Storage>,
        memory_per_core: i64,
        new_file_prefix: &str,
        block_size: usize,
        on_writer_close: OnWriterCloseFunc,
        collector: Option<Arc<dyn Collector>>,
        concurrency: usize,
        check_hotspot: bool,
        on_dup: OnDuplicateKey,
    ) -> Self {
        let concurrency = concurrency.max(1);
        let total_reader_memory_size = get_merge_reader_memory(memory_per_core, concurrency);
        tidb_log::info(
            "create merge operator",
            &[int_field("memory-per-core", memory_per_core), int_field("total-reader-memory-size", total_reader_memory_size)],
        );
        Self {
            store,
            total_reader_memory_size,
            new_file_prefix: new_file_prefix.to_owned(),
            block_size,
            on_writer_close,
            collector,
            concurrency,
            check_hotspot,
            on_dup,
        }
    }
}

/// Go `mergeMinimalTask`.
struct MergeMinimalTask {
    files: Vec<String>,
    active_group_count: usize,
    writer_id: String,
}

/// Go `MergeOverlappingFiles`: splits `paths` into groups
/// ([`split_data_files`]) and merges each group into one new file under the
/// operator's prefix, with a random writer ID.
///
/// # Errors
/// A group failed to merge; the first failure cancels the others.
pub fn merge_overlapping_files(ctx: &Context, paths: &[String], op: &MergeOperator) -> Result<()> {
    let concurrency = op.concurrency;
    let groups = split_data_files(paths, concurrency);
    tidb_log::info(
        "start to merge overlapping files",
        &[
            int_field("file-count", paths.len() as i64),
            int_field("file-groups", groups.len() as i64),
            int_field("concurrency", concurrency as i64),
        ],
    );
    let active_group_count = groups.len().min(concurrency);
    let tasks: Vec<MergeMinimalTask> = groups
        .into_iter()
        .map(|files| MergeMinimalTask { files, active_group_count, writer_id: uuid::Uuid::new_v4().to_string() })
        .collect();
    let (worker_ctx, cancel) = Context::with_cancel(ctx);
    let next = AtomicUsize::new(0);
    let first_err: Mutex<Option<crate::Error>> = Mutex::new(None);
    std::thread::scope(|scope| {
        for _ in 0..concurrency.min(tasks.len()) {
            scope.spawn(|| loop {
                let i = next.fetch_add(1, Ordering::SeqCst);
                if i >= tasks.len() || worker_ctx.err().is_some() {
                    return;
                }
                let task = &tasks[i];
                // Go `mergeWorker.HandleTask`.
                let memory_size_per_group = op.total_reader_memory_size / task.active_group_count as i64;
                if let Err(e) = merge_overlapping_files_internal(
                    &worker_ctx,
                    &task.files,
                    &op.store,
                    &op.new_file_prefix,
                    &task.writer_id,
                    op.block_size,
                    &op.on_writer_close,
                    op.collector.as_deref(),
                    op.check_hotspot,
                    op.on_dup,
                    memory_size_per_group,
                ) {
                    first_err.lock().unwrap_or_else(std::sync::PoisonError::into_inner).get_or_insert(e);
                    cancel.cancel();
                    return;
                }
            });
        }
    });
    if let Some(e) = first_err.into_inner().unwrap_or_else(std::sync::PoisonError::into_inner) {
        return Err(e);
    }
    if let Some(e) = ctx.err() {
        return Err(crate::other(e.to_string()));
    }
    Ok(())
}

/// Go `getTargetFileCount`: how many files merging `file_count` files
/// writes.
pub(crate) fn get_target_file_count(file_count: usize, concurrency: usize) -> usize {
    if file_count == 0 {
        return 0;
    }
    let mut shares = file_count.div_ceil(MAX_MERGING_FILES_PER_THREAD).max(concurrency);
    if file_count < 2 * concurrency {
        shares = (file_count / 2).max(1);
    }
    shares
}

/// Go `getGroupedTargetFileCount`: the target file count when `total`
/// files are divided as evenly as possible into `group_count` groups.
pub(crate) fn get_grouped_target_file_count(total: usize, group_count: usize, concurrency: usize) -> usize {
    let quotient = total / group_count;
    let remainder = total % group_count;
    remainder * get_target_file_count(quotient + 1, concurrency)
        + (group_count - remainder) * get_target_file_count(quotient, concurrency)
}

/// Go `splitDataFiles`: into [`get_target_file_count`] groups, the first
/// ones one file larger.
pub(crate) fn split_data_files(paths: &[String], concurrency: usize) -> Vec<Vec<String>> {
    let shares = get_target_file_count(paths.len(), concurrency);
    if shares == 0 {
        return Vec::new();
    }
    let mut out = Vec::with_capacity(shares);
    let batch_count = paths.len() / shares;
    let mut remainder = paths.len() % shares;
    let mut start = 0;
    while start < paths.len() {
        let mut end = start + batch_count;
        if remainder > 0 {
            end += 1;
            remainder -= 1;
        }
        end = end.min(paths.len());
        out.push(paths[start..end].to_vec());
        start = end;
    }
    out
}

/// Go `mergeOverlappingFilesInternal`: merges `paths` into one file.
#[allow(clippy::too_many_arguments)]
fn merge_overlapping_files_internal(
    ctx: &Context,
    paths: &[String],
    store: &Arc<dyn Storage>,
    new_file_prefix: &str,
    writer_id: &str,
    block_size: usize,
    on_writer_close: &OnWriterCloseFunc,
    collector: Option<&dyn Collector>,
    check_hotspot: bool,
    on_dup: OnDuplicateKey,
    memory_size_per_group: i64,
) -> Result<()> {
    if let Some(v) = tidb_failpoint::eval("github.com/pingcap/tidb/pkg/ingestor/globalsort/mergeOverlappingFilesInternal") {
        if let Some(v) = v.as_int() {
            match v {
                1 => return Err(crate::other("mock error in mergeOverlappingFilesInternal")),
                2 => panic!("mock panic in mergeOverlappingFilesInternal"),
                3 => {
                    ctx.sleep(std::time::Duration::from_secs(5));
                    return ctx.err().map_or(Ok(()), |e| Err(crate::other(e.to_string())));
                }
                _ => return Ok(()),
            }
        }
    }
    let task = tidb_lightning_log::begin_logutil_task(tidb_logutil::bg_logger(), "merge overlapping files");
    let result = merge_into_one_file(
        ctx,
        paths,
        store,
        new_file_prefix,
        writer_id,
        block_size,
        on_writer_close,
        collector,
        check_hotspot,
        on_dup,
        memory_size_per_group,
    );
    task.end(
        tidb_log::Level::Error,
        result.as_ref().err().map(|e| e.as_ref() as &(dyn std::error::Error + 'static)),
        &[str_field("writer-id", writer_id), int_field("file-count", paths.len() as i64)],
    );
    result
}

#[allow(clippy::too_many_arguments)]
fn merge_into_one_file(
    ctx: &Context,
    paths: &[String],
    store: &Arc<dyn Storage>,
    new_file_prefix: &str,
    writer_id: &str,
    block_size: usize,
    on_writer_close: &OnWriterCloseFunc,
    collector: Option<&dyn Collector>,
    check_hotspot: bool,
    on_dup: OnDuplicateKey,
    memory_size_per_group: i64,
) -> Result<()> {
    let zero_offsets = vec![0; paths.len()];
    let mut iter = MergeKVIter::new(
        ctx,
        paths,
        &zero_offsets,
        Arc::clone(store),
        DEFAULT_READ_BUFFER_SIZE,
        check_hotspot,
        memory_size_per_group,
    )
    .map_err(crate::sst)?;
    let part_size = get_merge_part_size(iter.input_size(), paths.len(), block_size);
    tidb_log::info(
        "calculated merge writer part size",
        &[
            int_field("input-size", iter.input_size()),
            int_field("part-size", part_size),
            int_field("file-count", paths.len() as i64),
        ],
    );
    let mut writer = WriterBuilder::new()
        .set_memory_size_limit(DEFAULT_ONE_WRITER_MEM_SIZE_LIMIT)
        .set_block_size(block_size)
        .set_on_close_func(Arc::clone(on_writer_close))
        .set_on_dup(on_dup)
        .build_one_file(Arc::clone(store), new_file_prefix, writer_id);
    writer.init_part_size_and_logger(ctx, part_size);
    let mut result = Ok(());
    while iter.next() {
        let (key, value) = (iter.key(), iter.value());
        if let Err(e) = writer.write_row(ctx, key, value) {
            result = Err(crate::sst(e));
            break;
        }
        if let Some(c) = collector {
            c.processed((key.len() + value.len()) as i64, 1);
        }
    }
    if result.is_ok() {
        if let Some(e) = iter.error() {
            result = Err(crate::sst(e.clone()));
        }
    }
    // Go closes the writer, then the iterator, in its defers.
    if let Err(e) = writer.close(ctx) {
        if result.is_ok() {
            result = Err(crate::sst(e));
        } else {
            tidb_log::warn("close writer failed", &[str_field("error", &e.to_string())]);
        }
    }
    if let Err(e) = iter.close() {
        tidb_log::warn("close iterator failed", &[str_field("error", &e.to_string())]);
    }
    result
}

/// Go `getMergePartSize`: the upload part size that keeps the merged file
/// within `MaxUploadPartCount` parts.
pub(crate) fn get_merge_part_size(input_size: i64, file_count: usize, block_size: usize) -> i64 {
    let padding = file_count as i64 * block_size as i64;
    let max_output_size = input_size + padding;
    let mut part_size = max_output_size / MAX_UPLOAD_PART_COUNT;
    if max_output_size % MAX_UPLOAD_PART_COUNT != 0 {
        part_size += 1;
    }
    MIN_UPLOAD_PART_SIZE.max(part_size)
}

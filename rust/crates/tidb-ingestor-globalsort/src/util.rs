// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
//! Go `util.go`: the sorted KV metadata of a group, the external meta
//! paths, merge-sort subtask division, and file cleanup.

use serde::{Deserialize, Serialize};
use tidb_gocontext::Context;
use tidb_ingestor_engineapi::ConflictInfo;
use tidb_ingestor_simplesst::{go_bytes, MultipleFilesStat, WriterSummary};
use tidb_objstore::storeapi::Storage;

use crate::merge::{get_grouped_target_file_count, get_target_file_count};
use crate::Result;

/// Go `metaName`.
const META_NAME: &str = "meta.json";

/// Go `CleanUpFiles`: deletes every file under the task directories
/// `non_partitioned_dirs`, with or without a partition prefix.
///
/// # Errors
/// The files could not be listed or deleted.
pub fn clean_up_files(ctx: &Context, store: &dyn Storage, non_partitioned_dirs: &[&str]) -> Result<()> {
    if tidb_failpoint::eval("github.com/pingcap/tidb/pkg/ingestor/globalsort/skipCleanUpFiles").is_some() {
        return Ok(());
    }
    if non_partitioned_dirs.is_empty() {
        return Ok(());
    }
    let names = tidb_ingestor_simplesst::get_all_file_names(ctx, store, non_partitioned_dirs).map_err(crate::sst)?;
    store.delete_files(ctx, &names).map_err(|e| crate::sst(e.into()))
}

/// Go `SortedKVMeta`: the sorted KV files of a group and their key range.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct SortedKVMeta {
    /// Go `StartKey`.
    #[serde(rename = "start-key", with = "go_bytes")]
    pub start_key: Vec<u8>,
    /// Go `EndKey`: exclusive.
    #[serde(rename = "end-key", with = "go_bytes")]
    pub end_key: Vec<u8>,
    /// Go `TotalKVSize`.
    #[serde(rename = "total-kv-size")]
    pub total_kv_size: u64,
    /// Go `TotalKVCnt`.
    #[serde(rename = "total-kv-cnt")]
    pub total_kv_cnt: u64,
    /// Go `MultipleFilesStats`: `None` for Go's nil slice.
    #[serde(rename = "multiple-files-stats")]
    pub multiple_files_stats: Option<Vec<MultipleFilesStat>>,
    /// Go `ConflictInfo`.
    #[serde(rename = "conflict-info")]
    pub conflict_info: ConflictInfo,
}

impl SortedKVMeta {
    /// Go `NewSortedKVMeta`.
    #[must_use]
    pub fn new(summary: Option<&WriterSummary>) -> Self {
        let Some(summary) = summary.filter(|s| !s.min.is_empty() || !s.max.is_empty()) else {
            return Self::default();
        };
        let mut end_key = summary.max.clone();
        end_key.push(0);
        Self {
            start_key: summary.min.clone(),
            end_key,
            total_kv_size: summary.total_size,
            total_kv_cnt: summary.total_cnt,
            multiple_files_stats: Some(summary.multiple_files_stats.clone()),
            conflict_info: summary.conflict_info.clone(),
        }
    }

    /// Go `Merge`.
    pub fn merge(&mut self, other: &Self) {
        if other.start_key.is_empty() && other.end_key.is_empty() {
            return;
        }
        if self.start_key.is_empty() && self.end_key.is_empty() {
            *self = other.clone();
            return;
        }
        self.start_key = bytes_min(&self.start_key, &other.start_key).to_vec();
        self.end_key = bytes_max(&self.end_key, &other.end_key).to_vec();
        self.total_kv_size += other.total_kv_size;
        self.total_kv_cnt += other.total_kv_cnt;
        if let Some(stats) = &other.multiple_files_stats {
            if !stats.is_empty() || self.multiple_files_stats.is_some() {
                self.multiple_files_stats.get_or_insert_with(Vec::new).extend(stats.iter().cloned());
            }
        }
        self.conflict_info.merge(&other.conflict_info);
    }

    /// Go `MergeSummary`.
    pub fn merge_summary(&mut self, summary: &WriterSummary) {
        self.merge(&Self::new(Some(summary)));
    }

    /// The statistics, empty for Go's nil slice.
    #[must_use]
    pub fn stats(&self) -> &[MultipleFilesStat] {
        self.multiple_files_stats.as_deref().unwrap_or_default()
    }

    /// Go `GetDataFiles`.
    #[must_use]
    pub fn get_data_files(&self) -> Vec<String> {
        self.stats().iter().flat_map(|s| s.filenames.iter().map(|f| f[0].clone())).collect()
    }

    /// Go `GetStatFiles`.
    #[must_use]
    pub fn get_stat_files(&self) -> Vec<String> {
        self.stats().iter().flat_map(|s| s.filenames.iter().map(|f| f[1].clone())).collect()
    }
}

/// Go `BytesMin`.
#[must_use]
pub fn bytes_min<'a>(a: &'a [u8], b: &'a [u8]) -> &'a [u8] {
    if a < b {
        a
    } else {
        b
    }
}

/// Go `BytesMax`.
#[must_use]
pub fn bytes_max<'a>(a: &'a [u8], b: &'a [u8]) -> &'a [u8] {
    if a > b {
        a
    } else {
        b
    }
}

/// Go `PlanMetaPath`: `{task}/plan/{step}/{idx}/meta.json`, the external
/// meta of the `idx`-th subtask the planner made for `step`.
#[must_use]
pub fn plan_meta_path(task_id: i64, step: &str, idx: usize) -> String {
    tidb_objstore::gopath::join(&[&task_id.to_string(), "plan", step, &idx.to_string(), META_NAME])
}

/// Go `PreparedMetaPath`.
#[must_use]
pub fn prepared_meta_path(task_id: i64) -> String {
    tidb_objstore::gopath::join(&[&task_id.to_string(), "plan", "prepared", META_NAME])
}

/// Go `SubtaskMetaPath`: `{task}/{subtask}/meta.json`.
#[must_use]
pub fn subtask_meta_path(task_id: i64, subtask_id: i64) -> String {
    tidb_objstore::gopath::join(&[&task_id.to_string(), &subtask_id.to_string(), META_NAME])
}

/// Go `DivideMergeSortDataFiles`: divides the files into merge-sort
/// subtasks. Full groups of `GetAdjustedMergeSortFileCountStep` files come
/// first, a multiple of `node_cnt` of them; the rest is split evenly over
/// as many groups as keep the total target file count within
/// `GetAdjustedMergeSortOverlapThreshold`.
///
/// # Errors
/// `node_cnt` is 0, or the files cannot be merged into few enough target
/// files (`ErrTooManyDataFiles`).
pub fn divide_merge_sort_data_files(files: &[String], node_cnt: usize, concurrency: usize) -> Result<Vec<Vec<String>>> {
    if node_cnt == 0 {
        return Err(crate::other("unsupported zero node count"));
    }
    if files.is_empty() {
        return Ok(Vec::new());
    }
    let too_many = |file_cnt: usize, target_limit: usize| -> crate::Error {
        Box::new(tidb_ingestor_errdef::ERR_TOO_MANY_DATA_FILES.gen_with_stack_by_args(&[
            (file_cnt as i64).into(),
            (concurrency as i64).into(),
            (target_limit as i64).into(),
        ]))
    };
    let max_files = tidb_ingestor_simplesst::get_adjusted_merge_sort_file_count_step(concurrency as i64);
    let file_cnt = files.len();
    let mut rest = files;
    let mut groups = Vec::with_capacity(node_cnt);
    let full_group_cnt = file_cnt / max_files / node_cnt * node_cnt;
    for _ in 0..full_group_cnt {
        groups.push(rest[..max_files].to_vec());
        rest = &rest[max_files..];
    }
    let target_cnt = full_group_cnt * get_target_file_count(max_files, concurrency);
    let target_limit = tidb_ingestor_simplesst::get_adjusted_merge_sort_overlap_threshold(concurrency as i64) as usize;
    let remaining = file_cnt - full_group_cnt * max_files;
    if remaining == 0 {
        if target_cnt > target_limit {
            return Err(too_many(file_cnt, target_limit));
        }
        return Ok(groups);
    }
    // Each subtask should merge at least 32 files.
    let min_files = 32;
    let max_groups = (remaining / min_files).min(node_cnt).max(1);
    let min_groups = remaining.div_ceil(max_files);
    let mut group_cnt = 0;
    let mut candidate = max_groups;
    while candidate >= min_groups && candidate > 0 {
        if target_cnt + get_grouped_target_file_count(remaining, candidate, concurrency) <= target_limit {
            group_cnt = candidate;
            break;
        }
        candidate -= 1;
    }
    if group_cnt == 0 {
        return Err(too_many(file_cnt, target_limit));
    }
    for size in tidb_mathutil::divide_2_batches(remaining, group_cnt) {
        groups.push(rest[..size].to_vec());
        rest = &rest[size..];
    }
    Ok(groups)
}

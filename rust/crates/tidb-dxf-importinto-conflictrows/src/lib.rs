// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
//! Go `pkg/dxf/importinto/conflictrows`: the retention of the conflict row
//! files IMPORT INTO writes under `conflicted-rows/`.

use std::collections::BTreeMap;
use std::sync::Arc;

use chrono::{DateTime, Duration, FixedOffset};
use tidb_dxf::{IMPORT_INTO, TASK_STATE_FAILED, TASK_STATE_REVERTED, TASK_STATE_SUCCEED};
use tidb_dxf_importinto_conflictpath::STORAGE_PREFIX;
use tidb_dxf_storage::{TaskCleanupInfo, TaskCleanupInfoGetter};
use tidb_gocontext::Context;
use tidb_objstore::storeapi::{Storage, WalkOption};

/// A failure of the cleanup.
pub type Error = Box<dyn std::error::Error + Send + Sync>;

/// Go `retention`: the files of a succeeded task are kept a week. The files
/// of a failed or reverted task, of no IMPORT INTO task, or with an invalid
/// name are deleted at once. Hardcoded until users ask otherwise.
const RETENTION: Duration = Duration::days(7);
/// Go `maxTaskIDsPerFlush`.
const MAX_TASK_IDS_PER_FLUSH: usize = 128;
/// Go `maxObjectsPerFlush`.
const MAX_OBJECTS_PER_FLUSH: usize = 1000;
/// Go `maxLoggedSamples`.
const MAX_LOGGED_SAMPLES: usize = 16;

/// Go `countWithSamples`.
#[derive(Clone, Debug, Default, PartialEq, serde::Serialize)]
struct CountWithSamples {
    #[serde(skip_serializing_if = "is_zero")]
    count: i64,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    samples: Vec<String>,
}

fn is_zero(v: &i64) -> bool {
    *v == 0
}

fn is_empty_count(v: &CountWithSamples) -> bool {
    v.count == 0 && v.samples.is_empty()
}

impl CountWithSamples {
    /// Go `appendSamples`.
    fn append_samples(&mut self, samples: &[String]) {
        let remaining = MAX_LOGGED_SAMPLES.saturating_sub(self.samples.len());
        self.samples.extend(samples.iter().take(remaining).cloned());
    }

    /// Go `recordCountWithSamples`.
    fn record(&mut self, samples: &[String]) {
        if samples.is_empty() {
            return;
        }
        self.count += samples.len() as i64;
        self.append_samples(samples);
    }

    /// Go `mergeCountWithSamples`.
    fn merge(&mut self, completed: &CountWithSamples) {
        self.count += completed.count;
        self.append_samples(&completed.samples);
    }
}

/// Go `cleanupStats`.
#[derive(Clone, Debug, Default, PartialEq, serde::Serialize)]
struct CleanupStats {
    #[serde(rename = "deleted-files", skip_serializing_if = "is_zero")]
    deleted_files: i64,
    #[serde(rename = "missing-tasks", skip_serializing_if = "is_empty_count")]
    missing_tasks: CountWithSamples,
    #[serde(rename = "missing-task-files", skip_serializing_if = "is_empty_count")]
    missing_task_files: CountWithSamples,
    /// Rare: kept with path samples so an operator can find the files.
    #[serde(rename = "non-import-into-task-files", skip_serializing_if = "is_empty_count")]
    non_import_into_task_files: CountWithSamples,
    #[serde(rename = "unparsed-task-id-files", skip_serializing_if = "is_empty_count")]
    unparsed_task_id_files: CountWithSamples,
    #[serde(skip_serializing_if = "is_zero")]
    failures: i64,
}

impl CleanupStats {
    /// Go `recordTaskDiagnostics`.
    fn record_task_diagnostics(&mut self, task_id: i64, files: &[String], info: Option<&TaskCleanupInfo>) {
        match info {
            // Files should not outlive their task metadata.
            None => {
                self.missing_tasks.record(&[task_id.to_string()]);
                self.missing_task_files.record(files);
            }
            // A task ID of another task type cannot own these files.
            Some(info) if info.tp != IMPORT_INTO => self.non_import_into_task_files.record(files),
            Some(_) => {}
        }
    }

    /// Go `mergeCompletedFlush`.
    fn merge_completed_flush(&mut self, completed: &CleanupStats) {
        self.deleted_files += completed.deleted_files;
        self.missing_tasks.merge(&completed.missing_tasks);
        self.missing_task_files.merge(&completed.missing_task_files);
        self.non_import_into_task_files.merge(&completed.non_import_into_task_files);
        self.unparsed_task_id_files.merge(&completed.unparsed_task_id_files);
    }
}

/// Go `parseTaskID`: the positive task ID of `conflicted-rows/<id>/<file>`.
fn parse_task_id(name: &str) -> Option<i64> {
    let relative = name.strip_prefix(STORAGE_PREFIX)?;
    let (component, descendant) = relative.split_once('/')?;
    if descendant.trim_matches('/').is_empty() || component.is_empty() || !component.bytes().all(|c| c.is_ascii_digit()) {
        return None;
    }
    component.parse::<i64>().ok().filter(|id| *id > 0)
}

/// Go `shouldDelete`: a failed or reverted task's files at once (they may
/// be incomplete), a succeeded task's after the retention.
fn should_delete(info: &TaskCleanupInfo, now: DateTime<FixedOffset>) -> bool {
    if info.tp != IMPORT_INTO {
        return false;
    }
    if info.state == TASK_STATE_FAILED || info.state == TASK_STATE_REVERTED {
        return true;
    }
    if info.state == TASK_STATE_SUCCEED {
        return info.end_time.is_some_and(|end| now >= end + RETENTION);
    }
    false
}

/// The walked files not flushed yet.
#[derive(Default)]
struct Pending {
    task_files: BTreeMap<i64, Vec<String>>,
    unparsed_task_id_files: Vec<String>,
    file_count: usize,
}

/// Go `cleanFiles`' `flush`.
fn flush(
    ctx: &Context,
    store: &dyn Storage,
    info_getter: &dyn TaskCleanupInfoGetter,
    now: DateTime<FixedOffset>,
    stats: &mut CleanupStats,
    pending: &mut Pending,
) -> Result<(), Error> {
    if pending.file_count == 0 {
        return Ok(());
    }
    let task_ids: Vec<i64> = pending.task_files.keys().copied().collect();
    let infos = if task_ids.is_empty() { BTreeMap::new() } else { info_getter.get_task_cleanup_info_by_ids(ctx, &task_ids)? };
    let mut flush_stats = CleanupStats::default();
    flush_stats.unparsed_task_id_files.record(&pending.unparsed_task_id_files);
    let mut files_to_delete: Vec<String> = pending.unparsed_task_id_files.clone();
    for task_id in &task_ids {
        let files = &pending.task_files[task_id];
        let info = infos.get(task_id);
        let delete = info.is_none_or(|info| info.tp != IMPORT_INTO || should_delete(info, now));
        flush_stats.record_task_diagnostics(*task_id, files, info);
        if delete {
            files_to_delete.extend(files.iter().cloned());
        }
    }
    if !files_to_delete.is_empty() {
        store.delete_files(ctx, &files_to_delete)?;
        flush_stats.deleted_files = files_to_delete.len() as i64;
    }
    stats.merge_completed_flush(&flush_stats);
    *pending = Pending::default();
    Ok(())
}

/// Go `cleanFiles`.
fn clean_files(
    ctx: &Context,
    store: &dyn Storage,
    info_getter: &dyn TaskCleanupInfoGetter,
    now: DateTime<FixedOffset>,
) -> (CleanupStats, Result<(), Error>) {
    let mut stats = CleanupStats::default();
    let mut pending = Pending::default();
    let opt = WalkOption { sub_dir: STORAGE_PREFIX.to_owned(), ..WalkOption::default() };
    let mut flush_err: Option<Error> = None;
    let walked = store.walk_dir(ctx, Some(&opt), &mut |name, _| {
        match parse_task_id(name) {
            // IMPORT INTO always writes a positive task ID here, so a
            // malformed entry is rare: deleted, with samples kept.
            None => pending.unparsed_task_id_files.push(name.to_owned()),
            Some(id) => pending.task_files.entry(id).or_default().push(name.to_owned()),
        }
        pending.file_count += 1;
        // Checked after the insert, so a batch can exceed either limit by one.
        if pending.file_count > MAX_OBJECTS_PER_FLUSH || pending.task_files.len() > MAX_TASK_IDS_PER_FLUSH {
            if let Err(e) = flush(ctx, store, info_getter, now, &mut stats, &mut pending) {
                let msg = e.to_string();
                flush_err = Some(e);
                return Err(tidb_objstore::error::Error::other(msg));
            }
        }
        Ok(())
    });
    let result = match (walked, flush_err) {
        (_, Some(e)) => Err(e),
        (Err(e), None) => Err(Box::new(e) as Error),
        (Ok(()), None) => flush(ctx, store, info_getter, now, &mut stats, &mut pending),
    };
    if result.is_err() {
        stats.failures += 1;
    }
    tidb_logutil::bg_logger().info(
        "finished conflict-row file cleanup",
        &[tidb_log::Field::new("stats", tidb_log::Value::Reflect(serde_json::to_string(&stats).unwrap_or_default()))],
    );
    (stats, result)
}

/// Go `CleanConflictRowFiles`: applies the retention policy above to the
/// files under the sort store. Successful tasks' files go after the
/// retention; other states are kept. The policy lives in the kernel rather
/// than in bucket lifecycle rules, so one owner maintains it.
///
/// # Errors
/// The store could not be opened, walked or written, or the task metadata
/// not read.
pub fn clean_conflict_row_files(
    ctx: &Context,
    info_getter: &dyn TaskCleanupInfoGetter,
    cloud_storage_uri: &str,
) -> Result<(), Error> {
    if cloud_storage_uri.is_empty() {
        return Ok(());
    }
    let sort_store: Arc<dyn Storage> = tidb_executor::importer::get_sort_store(ctx, cloud_storage_uri)
        .map_err(|e| Box::new(e.to_mysql_error()) as Error)?;
    let result = clean_files(ctx, sort_store.as_ref(), info_getter, chrono::Utc::now().fixed_offset()).1;
    sort_store.close();
    result
}

#[cfg(test)]
mod tests;

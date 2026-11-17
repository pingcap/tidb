// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
//! Go `pkg/dxf/importinto/job.go`'s `RuntimeInfo` and
//! `GetRuntimeInfoForJob`: a running job's progress, as SHOW IMPORT JOB
//! shows it.

use std::sync::Arc;

use tidb_dxf::*;
use tidb_gocontext::Context;

use crate::proto::TaskMeta;
use crate::Error;

/// Go `RuntimeInfo`.
#[derive(Clone, Debug, Default)]
pub struct RuntimeInfo {
    /// Go `Status`.
    pub status: TaskState,
    /// Go `ImportRows`.
    pub import_rows: i64,
    /// Go `ErrorMsg`.
    pub error_msg: String,
    /// Go `Step`.
    pub step: Step,
    /// Go `UpdateTime`: the latest subtask update, in the session's zone.
    pub update_time: Option<chrono::DateTime<chrono::FixedOffset>>,
    /// Go `Speed`.
    pub speed: i64,
    /// Go `Processed`.
    pub processed: i64,
    /// Go `Total`.
    pub total: i64,
}

/// Go `notAvailable`.
const NOT_AVAILABLE: &str = "N/A";

impl RuntimeInfo {
    fn is_conflict_step(&self) -> bool {
        self.step == IMPORT_STEP_COLLECT_CONFLICTS || self.step == IMPORT_STEP_CONFLICT_RESOLUTION
    }

    /// Go `Percent`.
    #[must_use]
    pub fn percent(&self) -> String {
        if self.step == IMPORT_STEP_POST_PROCESS || self.step == STEP_INIT || self.step == IMPORT_STEP_QUERY {
            return NOT_AVAILABLE.to_owned();
        }
        let mut percentage = 0.0;
        if self.total > 0 {
            percentage = (self.processed as f64 / self.total as f64).min(1.0);
        }
        ((percentage * 100.0) as i64).to_string()
    }

    /// Go `ETA`.
    #[must_use]
    pub fn eta(&self) -> String {
        if self.speed > 0 && self.total > 0 {
            return format_second_as_time(((self.total - self.processed) / self.speed).max(0));
        }
        NOT_AVAILABLE.to_owned()
    }

    /// Go `TotalSize`.
    #[must_use]
    pub fn total_size(&self) -> String {
        if self.step == IMPORT_STEP_QUERY {
            return NOT_AVAILABLE.to_owned();
        }
        if self.is_conflict_step() {
            return format!("{} conflicts", self.total);
        }
        tidb_config_configtypes::bytes_size(self.total as f64)
    }

    /// Go `ProcessedSize`.
    #[must_use]
    pub fn processed_size(&self) -> String {
        if self.step == IMPORT_STEP_QUERY {
            return NOT_AVAILABLE.to_owned();
        }
        if self.is_conflict_step() {
            return format!("{} conflicts", self.processed);
        }
        tidb_config_configtypes::bytes_size(self.processed as f64)
    }

    /// Go `SpeedStr`.
    #[must_use]
    pub fn speed_str(&self) -> String {
        if self.step == IMPORT_STEP_QUERY {
            return NOT_AVAILABLE.to_owned();
        }
        if self.is_conflict_step() {
            return format!("{} conflicts/s", self.speed);
        }
        format!("{}/s", tidb_config_configtypes::bytes_size(self.speed as f64))
    }
}

/// Go `FormatSecondAsTime`: `[N d ]HH:MM:SS`.
#[must_use]
pub fn format_second_as_time(sec: i64) -> String {
    let day = if sec >= 24 * 3600 { format!("{} d ", sec / (24 * 3600)) } else { String::new() };
    format!("{day}{:02}:{:02}:{:02}", (sec / 3600) % 24, (sec / 60) % 60, sec % 60)
}

/// Go `GetRuntimeInfoForJob`.
///
/// # Errors
/// The task of the job cannot be read.
pub fn get_runtime_info_for_job(ctx: &Context, job_id: i64) -> Result<RuntimeInfo, Error> {
    let ctx = tidb_dxf_storage::with_internal_source_type(ctx, tidb_txnkv::INTERNAL_DIST_TASK);
    let mgr = tidb_dxf_storage::get_dxf_svc_task_mgr().map_err(|e| Arc::new(e) as Error)?;
    let task = mgr.get_task_by_key_with_history(&ctx, &crate::job::task_key(job_id)).map_err(|e| Arc::new(e) as Error)?;
    let meta = TaskMeta::from_json(&task.meta)?;
    let mut ri = RuntimeInfo { status: task.base.state.clone(), step: task.base.step, ..RuntimeInfo::default() };
    if let Some(e) = &task.error {
        ri.error_msg = e.to_string();
        return Ok(ri);
    }
    let summaries = mgr
        .get_all_subtask_summary_by_step(&ctx, task.base.id, task.base.step)
        .map_err(|e| Arc::new(e) as Error)?;
    let now = chrono::Local::now().fixed_offset();
    let mut latest: Option<chrono::DateTime<chrono::FixedOffset>> = None;
    for s in &summaries {
        use std::sync::atomic::Ordering;
        ri.processed += s.processed.load(Ordering::SeqCst);
        ri.import_rows += s.row_cnt.load(Ordering::SeqCst);
        ri.speed += s.get_speed_in_time_range(now, tidb_dxf_execute::SUBTASK_SPEED_UPDATE_INTERVAL);
        let t = s.update_time();
        if latest.is_none_or(|l| t > l) {
            latest = Some(t);
        }
    }
    if task.base.step == IMPORT_STEP_POST_PROCESS {
        ri.import_rows = meta.summary.imported_rows;
    } else if task.base.step != IMPORT_STEP_WRITE_AND_INGEST && task.base.step != IMPORT_STEP_IMPORT {
        ri.import_rows = 0;
    }
    ri.total = match task.base.step {
        IMPORT_STEP_IMPORT | IMPORT_STEP_WRITE_AND_INGEST => meta.summary.ingest_summary.bytes,
        IMPORT_STEP_ENCODE_AND_SORT => meta.summary.encode_summary.bytes,
        IMPORT_STEP_MERGE_SORT => meta.summary.merge_summary.bytes,
        IMPORT_STEP_COLLECT_CONFLICTS => meta.summary.collect_conflicts_summary.row_cnt,
        IMPORT_STEP_CONFLICT_RESOLUTION => meta.summary.resolve_conflicts_summary.row_cnt,
        _ => 0,
    };
    ri.update_time = latest.filter(|t| t.timestamp() > 0);
    Ok(ri)
}

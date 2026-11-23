// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! The `mysql.analyze_jobs` writers of Go `autoanalyze.go`: `InsertAnalyzeJob`,
//! `StartAnalyzeJob`, `UpdateAnalyzeJobProgress`, `FinishAnalyzeJob`,
//! `DeleteAnalyzeJobs` and the corrupted-job cleanups, plus the
//! `dataForAnalyzeStatusHelper` read.




/// One persisted `mysql.analyze_jobs` identifier.
pub type AnalyzeJobId = u64;

/// Pinned Go `BatchUpdateAnalyzeJobSQL`'s failure reason.
pub const CORRUPTED_ANALYZE_JOB_FAILURE: &str =
    "The TiDB Server has either shut down or the analyze query was terminated during the analyze job execution";

/// Storage-backed reader used by pinned Go `SHOW ANALYZE STATUS`.
pub trait AnalyzeStatusProvider: Send + Sync {
    /// Runs Go's fresh restricted read of the thirty newest persisted jobs.
    fn load_analyze_status(&self) -> Result<Vec<tidb_stats::AnalyzeStatusJob>, String>;

    /// Go `GlobalPDHelper.GetApproximateTableCountFromStorage`. Errors and a
    /// backend without PD both produce zero, as its caller ignores `hasPD`.
    fn approximate_table_count(
        &self,
        resource_group: &str,
        physical_id: i64,
        database: &str,
        table: &str,
        partition: &str,
    ) -> i64;
}

/// Go `statistics.AnalyzePending` / `AnalyzeRunning` / `AnalyzeFinished` /
/// `AnalyzeFailed`.
const ANALYZE_PENDING: &str = "pending";
const ANALYZE_RUNNING: &str = "running";
const ANALYZE_FINISHED: &str = "finished";
const ANALYZE_FAILED: &str = "failed";
/// Go's `textMaxLength` for `job_info` and `fail_reason`.
const TEXT_MAX_LENGTH: usize = 65535;

/// Go `time.Now().UTC().Format(types.TimeFormat)`.
fn utc_now_text() -> String {
    chrono::Utc::now().format("%Y-%m-%d %H:%M:%S").to_string()
}

/// Go `statsAnalyze`'s analyze-job lifecycle: each statement on the
/// statistics handle's session pool (`util.CallWithSCtx(sa.statsHandle.SPool(),
/// ...)`), in its own autocommit transaction, recording the running instance
/// and process.
pub struct StatsAnalyzeJobs<C: ?Sized + 'static> {
    pool: Option<std::sync::Arc<dyn tidb_syssession::Pool<C>>>,
    instance: String,
    process_id: u64,
}

impl<C: ?Sized + 'static> StatsAnalyzeJobs<C> {
    /// The lifecycle over `pool` (absent once the node's domain closed) for
    /// the statement of `process_id` on `instance`.
    #[must_use]
    pub fn new(
        pool: Option<std::sync::Arc<dyn tidb_syssession::Pool<C>>>,
        instance: String,
        process_id: u64,
    ) -> Self {
        Self { pool, instance, process_id }
    }
}

impl<C> StatsAnalyzeJobs<C>
where
    C: tidb_stats_handle_util::StatsSessionContext + ?Sized + 'static,
{
    /// Go `statsutil.CallWithSCtx(pool, f)` without a wrapping transaction.
    fn call_with_sctx(&self, f: impl FnOnce(&C) -> Result<(), tidb_sqlexec::SqlExecError>) -> Result<(), String> {
        let pool = self.pool.as_ref().ok_or_else(|| "the node's domain is closed".to_owned())?;
        tidb_stats_handle_util::call_with_sctx(pool.as_ref(), f, &[]).map_err(|error| error.to_string())
    }
}

fn zap_error(error: impl std::fmt::Display) -> tidb_log::Field {
    tidb_log::Field::new("error", tidb_log::Value::Str(error.to_string()))
}

fn log_update_analyze_job_failed(logger: &tidb_logutil::Logger, update: String, error: impl std::fmt::Display) {
    logger.warn(
        "failed to update analyze job",
        &[tidb_log::Field::new("update", tidb_log::Value::Str(update)), zap_error(error)],
    );
}

impl<C> tidb_stats_handle_types::analyze_jobs::AnalyzeJobLifecycle for StatsAnalyzeJobs<C>
where
    C: tidb_stats_handle_util::StatsSessionContext + ?Sized + 'static,
{
    /// Go `InsertAnalyzeJob` / `insertAnalyzeJob`.
    fn insert(&self, spec: &tidb_stats_handle_types::analyze_jobs::AnalyzeJobSpec) -> Option<u64> {
        let mut job_id = None;
        let result = self.call_with_sctx(|sctx| {
            use tidb_sqlescape::SqlArg;
            let job_info = spec.job_info.as_bytes();
            let job_info = &job_info[..job_info.len().min(TEXT_MAX_LENGTH)];
            tidb_stats_handle_util::exec_rows(
                sctx,
                "INSERT INTO mysql.analyze_jobs (table_schema, table_name, partition_name, job_info, state, instance, process_id) VALUES (%?, %?, %?, %?, %?, %?, %?)",
                &[
                    SqlArg::from(spec.schema.as_str()),
                    SqlArg::from(spec.table.as_str()),
                    SqlArg::from(spec.partition.as_str()),
                    SqlArg::String(job_info),
                    SqlArg::from(ANALYZE_PENDING),
                    SqlArg::from(self.instance.as_str()),
                    SqlArg::Unsigned(self.process_id),
                ],
            )?;
            let (rows, _) = tidb_stats_handle_util::exec_rows(sctx, "SELECT LAST_INSERT_ID()", &[])?;
            job_id = match rows.first().and_then(|row| row.first()) {
                Some(tidb_datatype::Datum::UInt(id)) => Some(*id),
                Some(tidb_datatype::Datum::Int(id)) => Some(*id as u64),
                _ => Some(0),
            };
            Ok(())
        });
        match result {
            Ok(()) => job_id,
            Err(error) => {
                // Go `AddNewAnalyzeJob`.
                tidb_stats_handle_logutil::stats_err_verbose_logger()
                    .error("failed to insert analyze job", &[zap_error(error)]);
                None
            }
        }
    }

    /// Go `StartAnalyzeJob` / `startAnalyzeJob`.
    fn start(&self, job_id: u64) {
        let result = self.call_with_sctx(|sctx| {
            use tidb_sqlescape::SqlArg;
            let start_time = utc_now_text();
            tidb_stats_handle_util::exec_rows(
                sctx,
                "UPDATE mysql.analyze_jobs SET start_time = CONVERT_TZ(%?, '+00:00', @@TIME_ZONE), state = %? WHERE id = %?",
                &[SqlArg::from(start_time.as_str()), SqlArg::from(ANALYZE_RUNNING), SqlArg::Unsigned(job_id)],
            )?;
            Ok(())
        });
        if let Err(error) = result {
            log_update_analyze_job_failed(
                &tidb_stats_handle_logutil::stats_logger(),
                format!("{ANALYZE_PENDING}->{ANALYZE_RUNNING}"),
                error,
            );
        }
    }

    /// Go `UpdateAnalyzeJobProgress` / `updateAnalyzeJobProgress`,
    /// `processed_rows` being the delta.
    fn update_progress(&self, job_id: u64, processed_rows: i64) {
        let result = self.call_with_sctx(|sctx| {
            use tidb_sqlescape::SqlArg;
            tidb_stats_handle_util::exec_rows(
                sctx,
                "UPDATE mysql.analyze_jobs SET processed_rows = processed_rows + %? WHERE id = %?",
                &[SqlArg::Signed(processed_rows), SqlArg::Unsigned(job_id)],
            )?;
            Ok(())
        });
        if let Err(error) = result {
            log_update_analyze_job_failed(
                &tidb_stats_handle_logutil::stats_logger(),
                format!("process {processed_rows} rows"),
                error,
            );
        }
    }

    /// Go `FinishAnalyzeJob` / `finishAnalyzeJob`.
    fn finish(
        &self,
        job_id: u64,
        processed_rows: i64,
        failure: Option<&str>,
        kind: tidb_stats_handle_types::analyze_jobs::AnalyzeJobKind,
    ) {
        let result = self.call_with_sctx(|sctx| {
            use tidb_sqlescape::SqlArg;
            let end_time = utc_now_text();
            let fail_reason = failure.map(|reason| {
                let reason = reason.as_bytes();
                &reason[..reason.len().min(TEXT_MAX_LENGTH)]
            });
            let table_job = kind == tidb_stats_handle_types::analyze_jobs::AnalyzeJobKind::Table;
            let (sql, args): (&str, Vec<SqlArg<'_>>) = match (fail_reason, table_job) {
                (Some(reason), true) => (
                    "UPDATE mysql.analyze_jobs SET processed_rows = processed_rows + %?, end_time = CONVERT_TZ(%?, '+00:00', @@TIME_ZONE), state = %?, fail_reason = %?, process_id = NULL WHERE id = %?",
                    vec![
                        SqlArg::Signed(processed_rows),
                        SqlArg::from(end_time.as_str()),
                        SqlArg::from(ANALYZE_FAILED),
                        SqlArg::String(reason),
                        SqlArg::Unsigned(job_id),
                    ],
                ),
                (Some(reason), false) => (
                    "UPDATE mysql.analyze_jobs SET end_time = CONVERT_TZ(%?, '+00:00', @@TIME_ZONE), state = %?, fail_reason = %?, process_id = NULL WHERE id = %?",
                    vec![
                        SqlArg::from(end_time.as_str()),
                        SqlArg::from(ANALYZE_FAILED),
                        SqlArg::String(reason),
                        SqlArg::Unsigned(job_id),
                    ],
                ),
                (None, true) => (
                    "UPDATE mysql.analyze_jobs SET processed_rows = processed_rows + %?, end_time = CONVERT_TZ(%?, '+00:00', @@TIME_ZONE), state = %?, process_id = NULL WHERE id = %?",
                    vec![
                        SqlArg::Signed(processed_rows),
                        SqlArg::from(end_time.as_str()),
                        SqlArg::from(ANALYZE_FINISHED),
                        SqlArg::Unsigned(job_id),
                    ],
                ),
                (None, false) => (
                    "UPDATE mysql.analyze_jobs SET end_time = CONVERT_TZ(%?, '+00:00', @@TIME_ZONE), state = %?, process_id = NULL WHERE id = %?",
                    vec![SqlArg::from(end_time.as_str()), SqlArg::from(ANALYZE_FINISHED), SqlArg::Unsigned(job_id)],
                ),
            };
            tidb_stats_handle_util::exec_rows(sctx, sql, &args)?;
            Ok(())
        });
        if let Err(error) = result {
            // Go `finishAnalyzeJob`, through the plain logger.
            let state = if failure.is_some() { ANALYZE_FAILED } else { ANALYZE_FINISHED };
            log_update_analyze_job_failed(&tidb_logutil::bg_logger(), format!("{ANALYZE_RUNNING}->{state}"), error);
        }
    }
}

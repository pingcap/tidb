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

//! The ANALYZE a reorganizing DDL runs when `tidb_stats_update_during_ddl`
//! is ON: Go `multi_schema_change.go`'s `checkNeedAnalyze`, `index.go`'s
//! `doAnalyzeWithoutReorg`, `startAnalyzeAndWait`, `analyzeTableInner`,
//! `queryAnalyzeStatusSince` and `analyzeStatusDecision`, and the analyze
//! fields of `ddl.go`'s job context.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{mpsc, LazyLock, Mutex, PoisonError};
use std::time::{Duration, Instant};

use tidb_ddl_build::session_pool::{DdlSessionPool, RestrictedSqlOptions};
use tidb_log::{Field, Value};
use tidb_model::job::Job;
use tidb_model::reorg::analyze_state;
use tidb_model::schema_state::SchemaState;
use tidb_model::table_info::TableInfo;

use crate::job_txn::JobTxn;

/// Go `DefaultCumulativeTimeout`, in milliseconds.
pub static DEFAULT_CUMULATIVE_TIMEOUT_MILLIS: AtomicU64 = AtomicU64::new(60_000);
/// Go `DefaultAnalyzeCheckInterval`, in milliseconds.
pub static DEFAULT_ANALYZE_CHECK_INTERVAL_MILLIS: AtomicU64 = AtomicU64::new(10_000);

fn default_cumulative_timeout() -> Duration {
    Duration::from_millis(DEFAULT_CUMULATIVE_TIMEOUT_MILLIS.load(Ordering::Acquire))
}

fn default_analyze_check_interval() -> Duration {
    Duration::from_millis(DEFAULT_ANALYZE_CHECK_INTERVAL_MILLIS.load(Ordering::Acquire))
}

/// The analyze fields of Go's per-job `ReorgContext` (`jobCtxMap`).
#[derive(Default)]
struct JobAnalyzeCtx {
    /// Go `analyzeDone`: the running ANALYZE's report, an error or none.
    analyze_done: Option<mpsc::Receiver<Option<String>>>,
    /// Go `analyzeStartTime`.
    analyze_start_time: Option<Instant>,
    /// Go `analyzeCumulativeTimeout`.
    analyze_cumulative_timeout: Option<Duration>,
}

/// Go `ddlCtx.jobCtx.jobCtxMap`'s analyze half.
static JOB_CTX_MAP: LazyLock<Mutex<HashMap<i64, JobAnalyzeCtx>>> = LazyLock::new(Mutex::default);

fn with_job_ctx<T>(job_id: i64, f: impl FnOnce(&mut JobAnalyzeCtx) -> T) -> T {
    let mut map = JOB_CTX_MAP.lock().unwrap_or_else(PoisonError::into_inner);
    f(map.entry(job_id).or_default())
}

fn get_analyze_start_time(job_id: i64) -> Option<Instant> {
    with_job_ctx(job_id, |ctx| ctx.analyze_start_time)
}

fn set_analyze_start_time(job_id: i64, t: Instant) {
    with_job_ctx(job_id, |ctx| ctx.analyze_start_time = Some(t));
}

fn clear_analyze_start_time(job_id: i64) {
    with_job_ctx(job_id, |ctx| ctx.analyze_start_time = None);
}

fn clear_analyze_cumulative_timeout(job_id: i64) {
    with_job_ctx(job_id, |ctx| ctx.analyze_cumulative_timeout = None);
}

/// Go `removeJobCtx`, for the analyze fields: the job is done with its context.
pub(crate) fn remove_job_ctx(job_id: i64) {
    JOB_CTX_MAP
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .remove(&job_id);
}

/// Go `checkNeedAnalyze` check if the job need analyze.
pub(crate) fn check_need_analyze(job: &Job, tbl_info: &TableInfo) -> bool {
    let mut analyze_ver = tidb_vardef::defaults::DEF_TIDB_ANALYZE_VERSION;
    if let Some(val) = job.get_system_var(tidb_vardef::tidb_vars::TIDB_ANALYZE_VERSION) {
        analyze_ver = tidb_variable::varsutil::tidb_opt_int(&val.to_string(), analyze_ver);
    }
    let mut enable_ddl_analyze = tidb_vardef::defaults::DEF_TIDB_ENABLE_DDL_ANALYZE;
    if let Some(val) = job.get_system_var(tidb_vardef::tidb_vars::TIDB_ENABLE_DDL_ANALYZE) {
        enable_ddl_analyze = tidb_vardef::option_values::tidb_opt_on(&val.to_string());
    }
    let has_partition = tbl_info.get_partition_info().is_some();
    if !enable_ddl_analyze || has_partition || analyze_ver != 2 {
        tidb_ddl_logutil::ddl_logger().info(
            "skip analyze",
            &[
                Field::new("tidb_stats_update_during_ddl", Value::Bool(enable_ddl_analyze)),
                Field::new("is partitioned table", Value::Bool(has_partition)),
                Field::new("tidb_analyze_version", Value::I64(analyze_ver)),
            ],
        );
        return false;
    }

    // If we reach here, it means all the reorg work has been done, either after
    // MODIFY COLUMN or ADD INDEX. So we can just check the index state to decide
    // whether there are new indexes added.
    tbl_info
        .indices
        .iter_deref()
        .any(|idx| idx.read().state == SchemaState::WRITE_REORGANIZATION)
}

fn set_analyze_state(job: &Job, state: i8) {
    if let Some(meta) = &job.reorg_meta {
        meta.write().analyze_state = state;
    }
}

fn get_analyze_state(job: &Job) -> i8 {
    job.reorg_meta
        .as_ref()
        .map_or(analyze_state::NONE, |meta| meta.read().analyze_state)
}

/// Go `doAnalyzeWithoutReorg` performs analyze for the table if needed
/// without the logic of reorg and returns whether the analyze is finished.
pub(crate) fn do_analyze_without_reorg(txn: &JobTxn<'_>, job: &Job, tbl_info: &TableInfo) -> bool {
    match get_analyze_state(job) {
        analyze_state::NONE => {
            if check_need_analyze(job, tbl_info) {
                // Start analyze for the next time.
                set_analyze_state(job, analyze_state::RUNNING);
            } else {
                set_analyze_state(job, analyze_state::SKIPPED);
            }
            false
        }
        analyze_state::RUNNING => {
            start_analyze_and_wait(txn, job, tbl_info);
            false
        }
        state => {
            tidb_ddl_logutil::ddl_logger().info(
                "analyze skipped or finished for multi-schema change",
                &[
                    Field::new("job", Value::I64(job.id)),
                    Field::new("state", Value::I64(i64::from(state))),
                ],
            );
            true
        }
    }
}

/// Go `startAnalyzeAndWait`.
pub(crate) fn start_analyze_and_wait(txn: &JobTxn<'_>, job: &Job, tbl_info: &TableInfo) {
    let (done, timed_out, failed) = analyze_table_inner(txn, job, tbl_info, &job.schema_name.to_string());
    if done || timed_out || failed {
        if done {
            set_analyze_state(job, analyze_state::DONE);
        }
        if timed_out {
            set_analyze_state(job, analyze_state::TIMEOUT);
        }
        if failed {
            set_analyze_state(job, analyze_state::FAILED);
        }
    }
}

/// Go `analyzeStatus`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum AnalyzeStatus {
    Unknown,
    Running,
    Finished,
    Failed,
}

/// Go `analyzeTableInner` analyzes the table after creating index/modify column.
fn analyze_table_inner(
    txn: &JobTxn<'_>,
    job: &Job,
    tbl_info: &TableInfo,
    db_name: &str,
) -> (bool, bool, bool) {
    let pool = txn.session().map(|session| session.pool);
    let tbl_name = tbl_info.name.lowercase().to_owned();
    let mut cumulative_timeout =
        match with_job_ctx(job.id, |ctx| ctx.analyze_cumulative_timeout) {
            Some(timeout) => timeout,
            None => {
                let mut timeout = default_cumulative_timeout();
                if job.real_start_ts != 0 {
                    let add_start_millis = (job.real_start_ts >> 18) as i64;
                    let elapsed = Duration::from_millis(
                        (chrono::Utc::now().timestamp_millis() - add_start_millis).max(0) as u64,
                    );
                    if elapsed * 2 > timeout {
                        timeout = elapsed * 2;
                    }
                }
                with_job_ctx(job.id, |ctx| ctx.analyze_cumulative_timeout = Some(timeout));
                timeout
            }
        };

    let has_done_ch = with_job_ctx(job.id, |ctx| ctx.analyze_done.is_some());
    if !has_done_ch {
        let status = match pool
            .as_deref()
            .ok_or_else(|| "no DDL session pool".to_owned())
            .and_then(|pool| query_analyze_status_since(pool, job.start_ts, db_name, &tbl_name))
        {
            Ok(status) => status,
            Err(error) => {
                tidb_ddl_logutil::ddl_logger().warn(
                    "query analyze status failed",
                    &[
                        Field::new("jobID", Value::I64(job.id)),
                        tidb_logutil::zap_error(&error),
                    ],
                );
                AnalyzeStatus::Unknown
            }
        };

        let (done, timed_out, failed, proceed) =
            analyze_status_decision(job, db_name, &tbl_name, status, cumulative_timeout);
        if done || timed_out || failed {
            return (done, timed_out, failed);
        }
        if !proceed {
            // We decided not to proceed to start ANALYZE locally (i.e. it's running and we waited),
            // so simply return and retry later.
            return (false, false, false);
        }

        if get_analyze_start_time(job.id).is_none() {
            set_analyze_start_time(job.id, Instant::now());
        }

        // A channel the analyze thread can always report an error into
        // even after the caller has timed out and moved on; a thread that
        // reports nothing closes it.
        let (done_tx, done_rx) = mpsc::channel();
        let db_table = format!("`{db_name}`.`{tbl_name}`");
        let job_id = job.id;
        let db = db_name.to_owned();
        let table = tbl_name.clone();
        std::thread::spawn(move || {
            let Some(pool) = pool else {
                return;
            };
            // Go `statsutil.UpdateSCtxVarsForStats`: the system session
            // loads the statistics variables from the global ones itself.
            let result = pool.exec_restricted_sql_opts(
                RestrictedSqlOptions {
                    enable_ddl_analyze: true,
                    utc_time_zone: false,
                },
                &format!("ANALYZE TABLE {db_table};"),
                &[],
            );
            if let Err(error) = result {
                tidb_ddl_logutil::ddl_logger().warn(
                    "analyze table failed",
                    &[
                        Field::new("jobID", Value::I64(job_id)),
                        Field::new("db", Value::Str(db)),
                        Field::new("table", Value::Str(table)),
                        tidb_logutil::zap_error(&error.message),
                    ],
                );
                // We can continue to finish the job even if analyze table failed.
                let _ = done_tx.send(Some(error.message));
            }
        });
        with_job_ctx(job.id, |ctx| ctx.analyze_done = Some(done_rx));
    }
    let done_ch = with_job_ctx(job.id, |ctx| ctx.analyze_done.take())
        .expect("the analyze done channel is set");
    match done_ch.recv_timeout(default_analyze_check_interval()) {
        Ok(error) => {
            tidb_ddl_logutil::ddl_logger()
                .info("analyze table after create index done", &[Field::new("jobID", Value::I64(job.id))]);
            clear_analyze_start_time(job.id);
            clear_analyze_cumulative_timeout(job.id);
            (true, false, error.is_some())
        }
        // Go reads nil from the closed channel.
        Err(mpsc::RecvTimeoutError::Disconnected) => {
            tidb_ddl_logutil::ddl_logger()
                .info("analyze table after create index done", &[Field::new("jobID", Value::I64(job.id))]);
            clear_analyze_start_time(job.id);
            clear_analyze_cumulative_timeout(job.id);
            (true, false, false)
        }
        Err(mpsc::RecvTimeoutError::Timeout) => {
            with_job_ctx(job.id, |ctx| ctx.analyze_done = Some(done_ch));
            if let Some(timeout) = mock_analyze_timeout() {
                cumulative_timeout = timeout;
            }
            if let Some(start) = get_analyze_start_time(job.id) {
                if start.elapsed() > cumulative_timeout {
                    tidb_ddl_logutil::ddl_logger().warn(
                        "analyze table after create index exceed cumulative timeout, proceeding to finish DDL",
                        &[
                            Field::new("jobID", Value::I64(job.id)),
                            Field::new("elapsed", Value::Duration(start.elapsed().as_nanos() as i64)),
                        ],
                    );
                    // Do not persist job here. Let the caller mark AnalyzeStateTimeout and persist.
                    clear_analyze_start_time(job.id);
                    clear_analyze_cumulative_timeout(job.id);
                    return (false, true, false);
                }
            } else {
                set_analyze_start_time(job.id, Instant::now());
            }
            (false, false, false)
        }
    }
}

/// Go failpoint `mockAnalyzeTimeout`: the cumulative timeout in
/// milliseconds.
fn mock_analyze_timeout() -> Option<Duration> {
    #[cfg(feature = "failpoints")]
    {
        fail::eval("mockAnalyzeTimeout", |val| val.and_then(|val| val.parse::<u64>().ok()))
            .flatten()
            .map(Duration::from_millis)
    }
    #[cfg(not(feature = "failpoints"))]
    {
        None
    }
}

/// Go `queryAnalyzeStatusSince` queries `analyze_jobs` for the specified table
/// and start_time >= startTS.
fn query_analyze_status_since(
    pool: &dyn DdlSessionPool,
    start_ts: u64,
    db_name: &str,
    tbl_name: &str,
) -> Result<AnalyzeStatus, String> {
    let start_time = if start_ts > 0 {
        chrono::DateTime::from_timestamp_millis((start_ts >> 18) as i64).unwrap_or_default()
    } else {
        chrono::Utc::now()
    };
    let start_time_str = start_time.format("%Y-%m-%d %H:%M:%S").to_string();
    // set session time zone to UTC to match the time format in `startTimeStr`
    let rows = pool
        .exec_restricted_sql_opts(
            RestrictedSqlOptions {
                enable_ddl_analyze: false,
                utc_time_zone: true,
            },
            "SELECT state FROM mysql.analyze_jobs WHERE table_schema = %? AND table_name = %? AND start_time >= %?",
            &[
                tidb_sqlescape::SqlArg::String(db_name.as_bytes()),
                tidb_sqlescape::SqlArg::String(tbl_name.as_bytes()),
                tidb_sqlescape::SqlArg::String(start_time_str.as_bytes()),
            ],
        )
        .map_err(|error| error.message)?;
    if rows.is_empty() {
        return Ok(AnalyzeStatus::Unknown);
    }

    for r in &rows {
        let state = String::from_utf8_lossy(r[0].go_bytes()).into_owned();
        if state.eq_ignore_ascii_case("running") {
            return Ok(AnalyzeStatus::Running);
        } else if state.eq_ignore_ascii_case("failed") {
            return Ok(AnalyzeStatus::Failed);
        } else if state.eq_ignore_ascii_case("finished") {
            return Ok(AnalyzeStatus::Finished);
        }
        // unknown state: continue checking other rows
    }
    // If no recognizable state found, treat as unknown so caller may attempt to start ANALYZE.
    Ok(AnalyzeStatus::Unknown)
}

/// Go `analyzeStatusDecision`: `(done, timedOut, failed, proceed)`.
fn analyze_status_decision(
    job: &Job,
    db_name: &str,
    tbl_name: &str,
    status: AnalyzeStatus,
    cumulative_timeout: Duration,
) -> (bool, bool, bool, bool) {
    let logger = tidb_ddl_logutil::ddl_logger();
    let table_fields = || {
        [
            Field::new("jobID", Value::I64(job.id)),
            Field::new("db", Value::Str(db_name.to_owned())),
            Field::new("table", Value::Str(tbl_name.to_owned())),
        ]
    };
    match status {
        AnalyzeStatus::Finished => {
            logger.info("analyze already finished by other owner", &table_fields());
            clear_analyze_start_time(job.id);
            clear_analyze_cumulative_timeout(job.id);
            (true, false, false, false)
        }
        AnalyzeStatus::Failed => {
            logger.warn(
                "analyze previously failed on another owner, continue finishing DDL",
                &table_fields(),
            );
            clear_analyze_start_time(job.id);
            clear_analyze_cumulative_timeout(job.id);
            (true, false, true, false)
        }
        AnalyzeStatus::Running => {
            // If analyze is running, respect cumulative timeout. If expired, return timeout to let caller proceed.
            if let Some(start) = get_analyze_start_time(job.id) {
                if start.elapsed() > cumulative_timeout {
                    logger.warn(
                        "analyze table is running but exceeded cumulative timeout, proceeding to finish DDL",
                        &[
                            Field::new("jobID", Value::I64(job.id)),
                            Field::new("elapsed", Value::Duration(start.elapsed().as_nanos() as i64)),
                        ],
                    );
                    clear_analyze_start_time(job.id);
                    clear_analyze_cumulative_timeout(job.id);
                    return (false, true, false, false);
                }
            } else {
                set_analyze_start_time(job.id, Instant::now());
            }
            std::thread::sleep(default_analyze_check_interval());
            (false, false, false, false)
        }
        AnalyzeStatus::Unknown => {
            // analyzeUnknown: caller should proceed to start ANALYZE locally.
            (false, false, false, true)
        }
    }
}

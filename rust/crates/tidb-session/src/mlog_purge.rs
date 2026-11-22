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

//! Go master `executor.PurgeMaterializedViewLogExec`
//! (`pkg/executor/mlog_purge.go`): PURGE MATERIALIZED VIEW LOG on three
//! system sessions of the domain -- the purge-info row locked in a
//! pessimistic transaction, the log's rows deleted in batches by the
//! `_tidb_commit_ts` window up to the safe TSO, and the purge's history row
//! in `mysql.tidb_mlog_purge_hist`.
//!
//! A statement this node runs is never Go's internal (scheduled) SQL, so the
//! purge method is `manual` and the next purge time is not derived here.

use std::time::{Duration, Instant};

use tidb_ast::PurgeMaterializedViewLogStmt;
use tidb_datatype::Datum;
use tidb_driver_error::DriverError;
use tidb_executor::sessionctx::StmtOutput;
use tidb_sqlescape::SqlArg;
use tidb_terror::MysqlError;

use crate::mview_refresh::{
    annotate, datum_i64, datum_u64, driver_message, format_duration_seconds_between, hist_time, is_table_not_exists,
    plain, session_current_tso, sys_sql, sys_sql_ctx,
};
use crate::mview_task::{read_hist_cancel_request, start_mv_task_monitor, MvTaskCancelController, MvTaskMonitor};
use crate::Session;

/// Go master `mlogPurgeAdaptiveBatchWindow`.
const MLOG_PURGE_ADAPTIVE_BATCH_WINDOW: Duration = Duration::from_millis(200);
/// Go master `mlogPurgeAdaptiveMinBatchSize`.
const MLOG_PURGE_ADAPTIVE_MIN_BATCH_SIZE: i64 = 8000;
/// Go master `mlogPurgeAdaptiveMaxRangeCount`.
const MLOG_PURGE_ADAPTIVE_MAX_RANGE_COUNT: i64 = 16;
/// Go master `mlogPurgeAdaptiveMaxBudget`: the source MV service's default
/// budget less `mlogPurgeAdaptiveDeadlineBuffer`.
const MLOG_PURGE_ADAPTIVE_MAX_BUDGET: Duration = Duration::from_secs(10 * 60 - 10);
/// Go `autoid.RowIDBitLength`.
const ROW_ID_BIT_LENGTH: u64 = 64;

/// Go master `mvTaskHistStatusRunning` / `Success` / `Failed`.
const MV_TASK_HIST_STATUS_RUNNING: &str = "running";
const MV_TASK_HIST_STATUS_SUCCESS: &str = "success";
const MV_TASK_HIST_STATUS_FAILED: &str = "failed";

/// Go `mysql.ErrLockAcquireFailAndNoWaitSet`.
const ERR_LOCK_ACQUIRE_FAIL_AND_NO_WAIT_SET: u16 = 3572;

const FP: &str = "github.com/pingcap/tidb/pkg/executor/";

/// Go `if v, ok := val.(bool); ok && v` on an executor failpoint.
fn failpoint_on(name: &str) -> bool {
    tidb_failpoint::eval(&format!("{FP}{name}")).and_then(|value| value.as_bool()) == Some(true)
}

/// Go master `mlogPurgeThrottleConfig`.
#[derive(Clone, Copy)]
struct MLogPurgeThrottleConfig {
    min_rate: f64,
    budget_ratio: f64,
}

/// Go master `mlogPurgeThrottlePlan`.
#[derive(Clone, Copy, Debug, PartialEq)]
struct MLogPurgeThrottlePlan {
    target_rate: f64,
    pending_rows: i64,
    effective_batch_size: i64,
    min_rate: f64,
    deadline: chrono::DateTime<chrono::Utc>,
    no_wait_streak: i64,
}

/// Go master `mlogPurgePendingRowStats`.
#[derive(Clone, Copy, Default)]
struct MLogPurgePendingRowStats {
    pending_rows: i64,
    min_row_id: i64,
    max_row_id: i64,
    has_row_id_bounds: bool,
}

/// Go master `mlogPurgeDeleteRowIDRange`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct MLogPurgeDeleteRowIdRange {
    start_row_id: i64,
    end_row_id: i64,
}

/// Go master `mlogPurgeDeletePlan`.
struct MLogPurgeDeletePlan {
    pending_rows: i64,
    throttle_plan: Option<MLogPurgeThrottlePlan>,
    row_id_ranges: Vec<MLogPurgeDeleteRowIdRange>,
}

/// What `resolvePurgeMaterializedViewLogMeta` answers.
struct PurgeTarget {
    schema: String,
    base_table: tidb_model::TableInfo,
    mlog_name: String,
    mlog_id: i64,
    mlog_shard_row_id_bits: u64,
    /// Go `mlogInfo`: the log's metadata as the schema holds it.
    mlog_info: Option<tidb_model::GoShared<tidb_model::table::MaterializedViewLogInfo>>,
    /// Go `isInternalSQL` (`vars.InRestrictedSQL`).
    is_internal_sql: bool,
}

/// What a failed purge leaves for its failed-history fallback (Go's
/// closure variables).
#[derive(Default)]
struct PurgeProgress {
    purge_job_id: u64,
    hist_running_inserted: bool,
    total_purge_rows: i64,
}

/// Go `logutil.BgLogger().Warn(msg, schemaName, tableName, zap.Error(err))`.
fn log_purge_warn(msg: &str, schema: &str, table: &str, error: Option<&DriverError>) {
    let mut fields = vec![
        tidb_log::Field::new("schemaName", tidb_log::Value::Str(schema.to_owned())),
        tidb_log::Field::new("tableName", tidb_log::Value::Str(table.to_owned())),
    ];
    if let Some(error) = error {
        fields.push(tidb_logutil::zap_error(driver_message(error)));
    }
    tidb_logutil::bg_logger().warn(msg, &fields);
}

/// Go master `purgeMethod`.
fn purge_method(internal: bool) -> &'static str {
    if internal { "auto" } else { "manual" }
}

/// Go master `validatePurgeMaterializedViewLogStmt`.
fn validate_purge_materialized_view_log_stmt(stmt: &PurgeMaterializedViewLogStmt, is_internal_sql: bool) -> Result<&'static str, DriverError> {
    if stmt.table.last().is_none_or(String::is_empty) {
        return Err(plain("purge materialized view log: missing table name"));
    }
    Ok(purge_method(is_internal_sql))
}

/// Go master `deriveMLogPurgeNextUnixSeconds`: the log's NEXT evaluated now
/// (UTC), and whether the stored value is replaced.
fn derive_mlog_purge_next_unix_seconds(target: &PurgeTarget) -> Result<(Option<i64>, bool), DriverError> {
    let info = target
        .mlog_info
        .as_ref()
        .ok_or_else(|| plain("purge materialized view log: schedule evaluation metadata is unavailable"))?
        .read()
        .clone();
    if info.purge_next.trim().is_empty() {
        return Ok((None, true));
    }
    let (next, should_update) = tidb_ddl_build::materialized_view::derive_materialized_schedule_next_unix_seconds(
        "",
        &info.purge_next,
        info.purge_schedule_sql_mode,
        chrono::Utc::now(),
    )
    .map_err(DriverError::Mysql)?;
    if next.is_none() && should_update {
        log_runtime_materialized_view_log_purge_next_unix_seconds_update_null(&target.schema, &target.mlog_name, &info.purge_next);
    }
    Ok((next, should_update))
}

/// Go master `logRuntimeMaterializedViewLogPurgeNextUnixSecondsUpdateNull`.
fn log_runtime_materialized_view_log_purge_next_unix_seconds_update_null(schema: &str, mlog_name: &str, next_expr: &str) {
    if next_expr.trim().is_empty() {
        return;
    }
    // Go `parser.Normalize(nextExpr, errors.RedactLogEnable)`: pingcap/errors'
    // constant `"ON"`, so the expression's literals never reach the log.
    let redact = tidb_parser::RedactMode::Enabled;
    tidb_logutil::bg_logger().error(
        "purge materialized view log: automatic purge schedule disabled because NEXT expression evaluated to NULL, updating NEXT_PURGE_UNIX_SECONDS to NULL",
        &[
            tidb_log::Field::new("schemaName", tidb_log::Value::Str(schema.to_owned())),
            tidb_log::Field::new("tableName", tidb_log::Value::Str(mlog_name.to_owned())),
            tidb_log::Field::new("purgeNext", tidb_log::Value::Str(tidb_parser::normalize(next_expr, redact))),
        ],
    );
}

fn purge_hist_missing_table() -> DriverError {
    plain("required system table mysql.tidb_mlog_purge_hist does not exist")
}

fn purge_info_missing_table() -> DriverError {
    plain("required system table mysql.tidb_mlog_purge_info does not exist")
}

/// Go master `mvMaintainMemQuotaAppliedOn<site>` (`(session MemQuotaQuery,
/// MViewMaintainMemQuota)`) and `mvMaintainIsolationReadEnginesAppliedOn<site>`
/// (`(session engines, MViewMaintainIsolationReadEngines)`).
fn inject_maintenance_vars_applied(session: &mut Session, mem_quota: &str, engines: &str, site: &str) {
    let applied = read_vars(session, &["tidb_mem_quota_query", "tidb_isolation_read_engines"]).unwrap_or_default();
    let applied_quota = applied.first().and_then(|value| value.parse::<i64>().ok()).unwrap_or_default();
    tidb_failpoint::inject_call(
        &format!("github.com/pingcap/tidb/pkg/executor/mvMaintainMemQuotaAppliedOn{site}"),
        &mut (applied_quota, mem_quota.parse::<i64>().unwrap_or_default()),
    );
    tidb_failpoint::inject_call(
        &format!("github.com/pingcap/tidb/pkg/executor/mvMaintainIsolationReadEnginesAppliedOn{site}"),
        &mut (applied.get(1).cloned().unwrap_or_default(), engines.to_owned()),
    );
}

/// Go master `applyMLogPurgeMaintenanceSessionVars`: materialized views
/// enabled and the user's maintenance memory quota and read engines applied
/// -- best effort for an internal purge, whose failures keep the session's
/// value; the answer is what restores them.
fn apply_mlog_purge_maintenance_session_vars(
    session: &mut Session,
    target_mem_quota: &str,
    target_isolation_read_engines: &str,
    best_effort: bool,
) -> Result<Vec<(&'static str, String)>, DriverError> {
    let original = read_vars(session, &["tidb_mview_enable", "tidb_mem_quota_query", "tidb_isolation_read_engines"])?;
    let (enable, mem_quota, engines) = (original[0].clone(), original[1].clone(), original[2].clone());
    let restore_enable = |session: &mut Session| {
        let _ = set_var(session, "tidb_mview_enable", &enable);
    };
    if enable != "1" && !enable.eq_ignore_ascii_case("ON") {
        set_var(session, "tidb_mview_enable", "ON")
            .map_err(|error| annotate(&error, "mv maintenance: failed to enable materialized view support"))?;
    }
    if mem_quota != target_mem_quota {
        let applied = if failpoint_on("mockMVMaintenanceMemQuotaApplyError") {
            Err(plain("mock mv maintenance mem quota apply error"))
        } else {
            set_var(session, "tidb_mem_quota_query", target_mem_quota)
        };
        if let Err(error) = applied {
            if !best_effort {
                restore_enable(session);
                return Err(annotate(&error, "mv maintenance: failed to apply maintenance memory quota"));
            }
            tidb_logutil::bg_logger().warn(
                "mv maintenance: failed to apply maintenance memory quota, using current value",
                &[tidb_log::Field::new("error", tidb_log::Value::Str(driver_message(&error)))],
            );
        }
    }
    if engines != target_isolation_read_engines {
        if let Err(error) = set_var(session, "tidb_isolation_read_engines", target_isolation_read_engines) {
            if !best_effort {
                let _ = set_var(session, "tidb_mem_quota_query", &mem_quota);
                restore_enable(session);
                return Err(annotate(&error, "mv maintenance: failed to apply isolation read engines"));
            }
            tidb_logutil::bg_logger().warn(
                "mv maintenance: failed to apply isolation read engines, using current value",
                &[tidb_log::Field::new("error", tidb_log::Value::Str(driver_message(&error)))],
            );
        }
    }
    Ok(vec![
        ("tidb_mview_enable", enable),
        ("tidb_isolation_read_engines", engines),
        ("tidb_mem_quota_query", mem_quota),
    ])
}

/// Go master `applyMLogPurgeDeleteTiFlashThreads`: the delete session's
/// TiFlash concurrency -- best effort for an internal purge.
fn apply_mlog_purge_delete_tiflash_threads(
    session: &mut Session,
    target_threads: i64,
    best_effort: bool,
) -> Result<Vec<(&'static str, String)>, DriverError> {
    let original = read_vars(session, &["tidb_max_tiflash_threads"])?.remove(0);
    if target_threads <= 0 || original == target_threads.to_string() {
        return Ok(Vec::new());
    }
    if let Err(error) = set_var(session, "tidb_max_tiflash_threads", &target_threads.to_string()) {
        if !best_effort {
            return Err(annotate(&error, "mv maintenance: failed to apply TiFlash thread limit"));
        }
        tidb_logutil::bg_logger().warn(
            "mv maintenance: failed to apply TiFlash thread limit, using current value",
            &[tidb_log::Field::new("error", tidb_log::Value::Str(driver_message(&error)))],
        );
        return Ok(Vec::new());
    }
    let applied = read_vars(session, &["tidb_max_tiflash_threads"])?.remove(0).parse::<i64>().unwrap_or_default();
    tidb_failpoint::inject_call(
        "github.com/pingcap/tidb/pkg/executor/mvMLogPurgeDeleteTiFlashThreadsAppliedOnPurgeDeleteSession",
        &mut (applied, target_threads),
    );
    Ok(vec![("tidb_max_tiflash_threads", original)])
}

fn set_var(session: &mut Session, name: &str, value: &str) -> Result<(), DriverError> {
    sys_sql(session, &format!("SET @@SESSION.{name} = %?"), &[SqlArg::String(value.as_bytes())]).map(|_| ())
}

fn read_vars(session: &mut Session, names: &[&str]) -> Result<Vec<String>, DriverError> {
    let select = format!("SELECT {}", names.iter().map(|name| format!("@@SESSION.{name}")).collect::<Vec<_>>().join(", "));
    let rows = sys_sql(session, &select, &[])?;
    Ok(rows.first().map(|row| row.iter().map(crate::mview_refresh::datum_text).collect()).unwrap_or_default())
}

fn restore_vars(session: &mut Session, vars: &[(&'static str, String)]) {
    for (name, value) in vars {
        let _ = set_var(session, name, value);
    }
}

/// Go master `acquireMaterializedViewLogPurgeLock`: the purge-info row
/// locked without waiting; its checkpoint and next purge time.
fn acquire_materialized_view_log_purge_lock(
    session: &mut Session,
    kctx: &tidb_gocontext::Context,
    schema: &str,
    base_table: &str,
    mlog_id: i64,
) -> Result<(Option<u64>, Option<chrono::DateTime<chrono::Utc>>), DriverError> {
    // Go `errors.Annotatef(errMLogPurgeLockConflict, ...)`.
    let conflict = || {
        annotate(
            &plain("mlog purge lock conflict"),
            &format!("another purge is running for materialized view log on {schema}.{base_table}, please retry later"),
        )
    };
    if failpoint_on("mockPurgeMaterializedViewLogLockConflict") {
        return Err(conflict());
    }
    let rows = sys_sql_ctx(
        session,
        kctx,
        "SELECT LAST_PURGED_TSO, NEXT_PURGE_UNIX_SECONDS FROM mysql.tidb_mlog_purge_info WHERE MLOG_ID = %? FOR UPDATE NOWAIT",
        &[SqlArg::Signed(mlog_id)],
    )
    .map_err(|error| match &error {
        DriverError::Mysql(mysql) if mysql.code == ERR_LOCK_ACQUIRE_FAIL_AND_NO_WAIT_SET => conflict(),
        _ if is_table_not_exists(&error) => purge_info_missing_table(),
        _ => error,
    })?;
    let Some(row) = rows.first() else {
        return Err(plain(format!("mlog purge lock row does not exist for mlog id {mlog_id}")));
    };
    let next = row
        .get(1)
        .and_then(datum_i64)
        .and_then(|seconds| chrono::DateTime::<chrono::Utc>::from_timestamp(seconds, 0));
    let Some(last) = row.first().and_then(datum_i64) else {
        return Ok((None, next));
    };
    let last = u64::try_from(last).map_err(|_| plain(format!("invalid LAST_PURGED_TSO {last} for mlog id {mlog_id}")))?;
    Ok((Some(last), next))
}

/// Go master `collectDependentMViewIDsForMLogPurge`: the base table's
/// public views, and the views a running CREATE MATERIALIZED VIEW is
/// building over the log.
pub(crate) fn collect_dependent_mview_ids_for_mlog_purge(
    session: &mut Session,
    kctx: &tidb_gocontext::Context,
    base_table: &tidb_model::TableInfo,
    mlog_id: i64,
) -> Result<(Vec<i64>, Vec<i64>), DriverError> {
    let mut public = Vec::new();
    if let Some(base) = &base_table.materialized_view_base {
        for &id in base.read().mview_ids.iter() {
            if id > 0 && !public.contains(&id) {
                public.push(id);
            }
        }
    }
    let rows = sys_sql_ctx(
        session,
        kctx,
        "SELECT job_meta FROM mysql.tidb_ddl_job WHERE type = %? AND FIND_IN_SET(%?, table_ids)",
        &[
            SqlArg::Signed(i64::from(tidb_model::ActionType::ACTION_CREATE_MATERIALIZED_VIEW.0)),
            SqlArg::Signed(mlog_id),
        ],
    )
    .map_err(|error| {
        if is_table_not_exists(&error) {
            plain("required system table mysql.tidb_ddl_job does not exist")
        } else {
            error
        }
    })?;
    let mut building = Vec::new();
    for row in rows {
        let bytes = match row.first() {
            Some(Datum::Bytes(bytes)) => bytes.clone(),
            Some(Datum::String(text)) => text.bytes().to_vec(),
            _ => continue,
        };
        if bytes.is_empty() {
            continue;
        }
        let mut job = tidb_model::Job::default();
        job.decode(&bytes).map_err(|error| plain(error.to_string()))?;
        if job.table_id > 0 && !public.contains(&job.table_id) && !building.contains(&job.table_id) {
            building.push(job.table_id);
        }
    }
    Ok((public, building))
}

fn build_in_list(ids: &[i64]) -> String {
    ids.iter().map(i64::to_string).collect::<Vec<_>>().join(",")
}

/// Go master `calcMaterializedViewLogSafePurgeTSO`: the purge's start TSO,
/// lowered to the least last-read TSO of the dependent views.
pub(crate) fn calc_materialized_view_log_safe_purge_tso(
    session: &mut Session,
    kctx: &tidb_gocontext::Context,
    base_schema: &str,
    base_table: &str,
    purge_start_ts: u64,
    public: &[i64],
    building: &[i64],
) -> Result<u64, DriverError> {
    let refresh_info_missing = |error: DriverError| {
        if is_table_not_exists(&error) {
            plain("required system table mysql.tidb_mview_refresh_info does not exist")
        } else {
            error
        }
    };
    let mut safe_purge_tso = purge_start_ts;
    if !public.is_empty() {
        let rows = sys_sql_ctx(
            session,
            kctx,
            &format!("SELECT COUNT(1) FROM mysql.tidb_mview_refresh_info WHERE MVIEW_ID IN ({})", build_in_list(public)),
            &[],
        )
        .map_err(refresh_info_missing)?;
        let count = rows.first().and_then(|row| row.first()).and_then(datum_i64).unwrap_or(0);
        if count != i64::try_from(public.len()).unwrap_or(i64::MAX) {
            return Err(plain(format!(
                "materialized view refresh info is missing for some dependent materialized views on base table {base_schema}.{base_table} (expected {}, got {count})",
                public.len()
            )));
        }
    }
    let all: Vec<i64> = public.iter().chain(building).copied().collect();
    if all.is_empty() {
        return Ok(safe_purge_tso);
    }
    let rows = sys_sql_ctx(
        session,
        kctx,
        &format!(
            "SELECT MIN(COALESCE(LAST_SUCCESS_READ_TSO, CAST(0 AS UNSIGNED))) FROM mysql.tidb_mview_refresh_info WHERE MVIEW_ID IN ({})",
            build_in_list(&all)
        ),
        &[],
    )
    .map_err(refresh_info_missing)?;
    if let Some(value) = rows.first().and_then(|row| row.first()).and_then(datum_u64) {
        safe_purge_tso = safe_purge_tso.min(value);
    }
    Ok(safe_purge_tso)
}

/// Go master `buildPurgeMaterializedViewLogDeleteSQL`.
fn build_purge_materialized_view_log_delete_sql(
    schema: &str,
    mlog_name: &str,
    last_purged_tso: Option<u64>,
    safe_purge_tso: u64,
    row_id_range: Option<MLogPurgeDeleteRowIdRange>,
    batch_size: i64,
) -> Result<String, DriverError> {
    let names = [
        SqlArg::String(schema.as_bytes()),
        SqlArg::String(mlog_name.as_bytes()),
        SqlArg::String(schema.as_bytes()),
        SqlArg::String(mlog_name.as_bytes()),
    ];
    let mut arguments: Vec<SqlArg<'_>> = names.to_vec();
    let mut filter = String::new();
    if let Some(range) = row_id_range {
        filter.push_str("_tidb_rowid >= %? AND _tidb_rowid <= %? AND ");
        arguments.push(SqlArg::Signed(range.start_row_id));
        arguments.push(SqlArg::Signed(range.end_row_id));
    }
    if let Some(last) = last_purged_tso {
        filter.push_str("_tidb_commit_ts > %? AND ");
        arguments.push(SqlArg::Unsigned(last));
    }
    filter.push_str("_tidb_commit_ts <= %? LIMIT %?");
    arguments.push(SqlArg::Unsigned(safe_purge_tso));
    arguments.push(SqlArg::Signed(batch_size));
    tidb_sqlescape::escape_sql_text(
        &format!("DELETE /*+ read_from_storage(tiflash[%n.%n]) */ FROM %n.%n WHERE {filter}"),
        &arguments,
    )
    .map_err(|error| plain(error.to_string()))
}

/// Go master `purgeMaterializedViewLogData`: one delete batch on the delete
/// session; the rows it deleted.
fn purge_materialized_view_log_data(
    session: &mut Session,
    kctx: &tidb_gocontext::Context,
    schema: &str,
    mlog_name: &str,
    last_purged_tso: Option<u64>,
    safe_purge_tso: u64,
    row_id_range: Option<MLogPurgeDeleteRowIdRange>,
    batch_size: i64,
) -> Result<i64, DriverError> {
    if failpoint_on("mockPurgeMaterializedViewLogDeleteErr") {
        return Err(plain("mock purge mlog delete error"));
    }
    if let Some(rows) = tidb_failpoint::eval(&format!("{FP}mockPurgeMaterializedViewLogDeleteRows")).and_then(|value| value.as_int())
    {
        return Ok(rows);
    }
    let delete_sql =
        build_purge_materialized_view_log_delete_sql(schema, mlog_name, last_purged_tso, safe_purge_tso, row_id_range, batch_size)?;
    tidb_failpoint::inject_call(&format!("{FP}purgeMaterializedViewLogDeleteSQL"), &mut delete_sql.clone());
    // Go master: the delete runs in materialized view maintenance.
    let original_maintenance = std::mem::replace(&mut session.vars.in_mview_maintenance, true);
    let deleted = sys_sql_ctx(session, kctx, &delete_sql, &[]);
    session.vars.in_mview_maintenance = original_maintenance;
    deleted?;
    Ok(i64::try_from(session.affected_rows()).unwrap_or(i64::MAX))
}

/// Go master `readMLogPurgePendingRowStatsOnTiFlash`: the window's row count
/// and row-id bounds, read on TiFlash only.
fn read_mlog_purge_pending_row_stats_on_tiflash(
    session: &mut Session,
    kctx: &tidb_gocontext::Context,
    schema: &str,
    mlog_name: &str,
    last_purged_tso: Option<u64>,
    safe_purge_tso: u64,
) -> Result<MLogPurgePendingRowStats, DriverError> {
    if failpoint_on("mockMLogPurgeAdaptiveCountErr") {
        return Err(plain("mock adaptive purge count error"));
    }
    let threads = read_vars(session, &["tidb_max_tiflash_threads"])?.remove(0).parse::<i64>().unwrap_or_default();
    tidb_failpoint::inject_call("github.com/pingcap/tidb/pkg/executor/mvMLogPurgeCountTiFlashThreadsUsed", &mut threads.clone());
    let original = read_vars(session, &["tidb_isolation_read_engines", "tidb_allow_fallback_to_tikv"])?;
    let restore: Vec<(&'static str, String)> = [("tidb_isolation_read_engines", "tiflash"), ("tidb_allow_fallback_to_tikv", "")]
        .into_iter()
        .zip(original)
        .filter(|((_, value), original)| original != value)
        .map(|((name, _), original)| (name, original))
        .collect();
    let counted = (|| {
        for (name, _) in &restore {
            set_var(session, name, if *name == "tidb_isolation_read_engines" { "tiflash" } else { "" })?;
        }
        let names = [
            SqlArg::String(schema.as_bytes()),
            SqlArg::String(mlog_name.as_bytes()),
            SqlArg::String(schema.as_bytes()),
            SqlArg::String(mlog_name.as_bytes()),
        ];
        let mut arguments: Vec<SqlArg<'_>> = names.to_vec();
        let filter = match last_purged_tso {
            Some(last) => {
                arguments.push(SqlArg::Unsigned(last));
                "_tidb_commit_ts > %? AND _tidb_commit_ts <= %?"
            }
            None => "_tidb_commit_ts <= %?",
        };
        arguments.push(SqlArg::Unsigned(safe_purge_tso));
        // Go master: the count runs in materialized view maintenance.
        let original_maintenance = std::mem::replace(&mut session.vars.in_mview_maintenance, true);
        let counted = sys_sql_ctx(
            session,
            kctx,
            &format!(
                "SELECT /*+ read_from_storage(tiflash[%n.%n]) */ COUNT(*), MIN(_tidb_rowid), MAX(_tidb_rowid) FROM %n.%n WHERE {filter}"
            ),
            &arguments,
        );
        session.vars.in_mview_maintenance = original_maintenance;
        counted
    })();
    restore_vars(session, &restore);
    let rows = counted?;
    let Some(row) = rows.first() else {
        return Ok(MLogPurgePendingRowStats::default());
    };
    let Some(pending_rows) = row.first().and_then(datum_i64) else {
        return Ok(MLogPurgePendingRowStats::default());
    };
    let mut stats = MLogPurgePendingRowStats { pending_rows, ..MLogPurgePendingRowStats::default() };
    let (Some(min), Some(max)) = (row.get(1).and_then(datum_i64), row.get(2).and_then(datum_i64)) else {
        return Ok(stats);
    };
    if stats.pending_rows <= 0 {
        return Ok(stats);
    }
    stats.min_row_id = min;
    stats.max_row_id = max;
    stats.has_row_id_bounds = true;
    Ok(stats)
}

/// Go master `buildMLogPurgeDeleteRowIDRanges`.
fn build_mlog_purge_delete_row_id_ranges(stats: MLogPurgePendingRowStats, shard_row_id_bits: u64) -> Vec<MLogPurgeDeleteRowIdRange> {
    if stats.pending_rows <= 0 || !stats.has_row_id_bounds || stats.min_row_id > stats.max_row_id {
        return Vec::new();
    }
    let mut range_count = (stats.pending_rows / MLOG_PURGE_ADAPTIVE_MIN_BATCH_SIZE).clamp(1, MLOG_PURGE_ADAPTIVE_MAX_RANGE_COUNT);
    if shard_row_id_bits > 0 {
        let shard_bucket_count = 1_i64.checked_shl(u32::try_from(shard_row_id_bits).unwrap_or(u32::MAX)).unwrap_or(0);
        if shard_bucket_count > 0 && range_count > shard_bucket_count {
            range_count = shard_bucket_count;
        }
        range_count = floor_power_of_two(range_count);
        return build_sharded_mlog_purge_delete_row_id_ranges(stats.min_row_id, stats.max_row_id, shard_row_id_bits, range_count);
    }
    build_linear_mlog_purge_delete_row_id_ranges(stats.min_row_id, stats.max_row_id, range_count)
}

/// Go master `buildLinearMLogPurgeDeleteRowIDRanges`.
fn build_linear_mlog_purge_delete_row_id_ranges(min_row_id: i64, max_row_id: i64, range_count: i64) -> Vec<MLogPurgeDeleteRowIdRange> {
    let whole = vec![MLogPurgeDeleteRowIdRange { start_row_id: min_row_id, end_row_id: max_row_id }];
    if range_count <= 1 || min_row_id >= max_row_id {
        return whole;
    }
    let span = max_row_id.wrapping_sub(min_row_id).wrapping_add(1);
    if span <= range_count {
        return whole;
    }
    let step = span / range_count;
    (0..range_count)
        .filter_map(|i| {
            let start = min_row_id + i * step;
            let end = if i < range_count - 1 { min_row_id + (i + 1) * step - 1 } else { max_row_id };
            (start <= end).then_some(MLogPurgeDeleteRowIdRange { start_row_id: start, end_row_id: end })
        })
        .collect()
}

/// Go master `buildShardedMLogPurgeDeleteRowIDRanges`: whole shard buckets
/// per range (Go `autoid.NewShardIDFormat` of a signed BIGINT row id).
fn build_sharded_mlog_purge_delete_row_id_ranges(
    min_row_id: i64,
    max_row_id: i64,
    shard_row_id_bits: u64,
    range_count: i64,
) -> Vec<MLogPurgeDeleteRowIdRange> {
    let whole = vec![MLogPurgeDeleteRowIdRange { start_row_id: min_row_id, end_row_id: max_row_id }];
    if range_count <= 1 || min_row_id >= max_row_id {
        return whole;
    }
    let shard_bucket_count = 1_i64.checked_shl(u32::try_from(shard_row_id_bits).unwrap_or(u32::MAX)).unwrap_or(0);
    if shard_bucket_count <= 0 {
        return build_linear_mlog_purge_delete_row_id_ranges(min_row_id, max_row_id, range_count);
    }
    let range_count = floor_power_of_two(range_count);
    if range_count <= 1 {
        return whole;
    }
    let incremental_bits = ROW_ID_BIT_LENGTH - shard_row_id_bits - 1;
    let buckets_per_range = shard_bucket_count / range_count;
    if buckets_per_range <= 0 {
        return whole;
    }
    (0..range_count)
        .filter_map(|i| {
            let mut start = ((i * buckets_per_range) as u64).wrapping_shl(incremental_bits as u32) as i64;
            let mut end = ((((i + 1) * buckets_per_range) as u64).wrapping_shl(incremental_bits as u32)).wrapping_sub(1) as i64;
            if i == range_count - 1 {
                end = max_row_id;
            }
            start = start.max(min_row_id);
            end = end.min(max_row_id);
            (start <= end).then_some(MLogPurgeDeleteRowIdRange { start_row_id: start, end_row_id: end })
        })
        .collect()
}

/// Go master `floorPowerOfTwo`.
fn floor_power_of_two(value: i64) -> i64 {
    if value <= 0 {
        return 0;
    }
    1 << (63 - value.leading_zeros())
}

/// Go master `shouldThrottleMLogPurgeDeleteBatch`.
fn should_throttle_mlog_purge_delete_batch(batch_completed: bool, has_more_ranges: bool) -> bool {
    batch_completed || has_more_ranges
}

/// Go master `calcMLogPurgeAdaptiveBatchSize`.
fn calc_mlog_purge_adaptive_batch_size(target_rate: f64) -> i64 {
    if target_rate <= 0.0 {
        return MLOG_PURGE_ADAPTIVE_MIN_BATCH_SIZE;
    }
    #[allow(clippy::cast_possible_truncation)]
    let batch_size = (target_rate * MLOG_PURGE_ADAPTIVE_BATCH_WINDOW.as_secs_f64()).ceil() as i64;
    batch_size.max(MLOG_PURGE_ADAPTIVE_MIN_BATCH_SIZE)
}

/// Go master `tryBuildMLogPurgeThrottlePlan`.
fn try_build_mlog_purge_throttle_plan(
    stats: MLogPurgePendingRowStats,
    deadline: chrono::DateTime<chrono::Utc>,
    cfg: MLogPurgeThrottleConfig,
) -> Option<MLogPurgeThrottlePlan> {
    if stats.pending_rows <= 0 {
        return None;
    }
    let now = chrono::Utc::now();
    if deadline <= now || cfg.budget_ratio <= 0.0 {
        return None;
    }
    let budget = (deadline - now).to_std().ok()?.as_secs_f64() * cfg.budget_ratio;
    if budget <= 0.0 {
        return None;
    }
    #[allow(clippy::cast_precision_loss)]
    let target_rate = (stats.pending_rows as f64 / budget).max(cfg.min_rate);
    Some(MLogPurgeThrottlePlan {
        target_rate,
        pending_rows: stats.pending_rows,
        effective_batch_size: calc_mlog_purge_adaptive_batch_size(target_rate),
        min_rate: cfg.min_rate,
        deadline,
        no_wait_streak: 0,
    })
}

impl MLogPurgeThrottlePlan {
    /// Go master `mlogPurgeThrottlePlan.maybeSleep`: sleep until the deleted
    /// rows are on the target rate.
    fn maybe_sleep(&mut self, start: Instant, total_deleted_rows: i64) -> Result<(), DriverError> {
        if failpoint_on("mockMLogPurgeAdaptiveSleepErr") {
            return Err(plain("mock adaptive purge sleep error"));
        }
        if self.target_rate <= 0.0 || total_deleted_rows <= 0 {
            return Ok(());
        }
        #[allow(clippy::cast_precision_loss)]
        let expected = Duration::from_secs_f64(total_deleted_rows as f64 / self.target_rate);
        match expected.checked_sub(start.elapsed()) {
            Some(sleep_for) if !sleep_for.is_zero() => {
                self.no_wait_streak = 0;
                std::thread::sleep(sleep_for);
            }
            _ => {
                self.no_wait_streak += 1;
                self.recalculate_batch_size_on_no_wait(total_deleted_rows);
            }
        }
        Ok(())
    }

    /// Go master `mlogPurgeThrottlePlan.recalculateBatchSizeOnNoWait`.
    fn recalculate_batch_size_on_no_wait(&mut self, total_deleted_rows: i64) {
        if self.no_wait_streak < 2 || self.pending_rows <= 0 {
            return;
        }
        let remaining_rows = self.pending_rows - total_deleted_rows;
        let Ok(remaining_budget) = (self.deadline - chrono::Utc::now()).to_std() else {
            return;
        };
        if remaining_rows <= 0 || remaining_budget.is_zero() {
            return;
        }
        #[allow(clippy::cast_precision_loss)]
        let new_rate = (remaining_rows as f64 / remaining_budget.as_secs_f64()).max(self.min_rate);
        self.target_rate = new_rate;
        self.effective_batch_size = calc_mlog_purge_adaptive_batch_size(new_rate);
        self.no_wait_streak = 0;
    }

    /// Go master `mlogPurgeThrottlePlan.effectiveDeleteBatchSize`.
    fn effective_delete_batch_size(&mut self, configured_batch_size: i64) -> i64 {
        if configured_batch_size <= 0 {
            return configured_batch_size;
        }
        if self.effective_batch_size <= 0 {
            self.effective_batch_size = calc_mlog_purge_adaptive_batch_size(self.target_rate);
        }
        self.effective_batch_size = self.effective_batch_size.min(configured_batch_size);
        self.effective_batch_size
    }
}

/// Go master `readPurgeHistCancelRequest`.
fn read_purge_hist_cancel_request(session: &mut Session, purge_job_id: u64, mlog_id: i64) -> Result<(bool, String), DriverError> {
    read_hist_cancel_request(
        session,
        "SELECT CANCEL_REQUEST_TIME, CANCEL_REQUESTED_BY\nFROM mysql.tidb_mlog_purge_hist\nWHERE PURGE_JOB_ID = %? AND MLOG_ID = %?",
        purge_job_id,
        mlog_id,
        "required system table mysql.tidb_mlog_purge_hist does not exist",
    )
}

/// Go master `updatePurgeHistHeartbeat`.
fn update_purge_hist_heartbeat(session: &mut Session, purge_job_id: u64, mlog_id: i64) -> Result<(), DriverError> {
    sys_sql(
        session,
        "UPDATE mysql.tidb_mlog_purge_hist\nSET LAST_HEARTBEAT_TIME = NOW(6)\nWHERE PURGE_JOB_ID = %? AND MLOG_ID = %? AND PURGE_STATUS = 'running'",
        &[SqlArg::Unsigned(purge_job_id), SqlArg::Signed(mlog_id)],
    )
    .map(|_| ())
    .map_err(|error| if is_table_not_exists(&error) { purge_hist_missing_table() } else { error })
}

/// Go master `readLatestMLogPurgeCutoffFenceTSO`.
fn read_latest_mlog_purge_cutoff_fence_tso(session: &mut Session, kctx: &tidb_gocontext::Context, mlog_id: i64) -> Result<Option<u64>, DriverError> {
    let rows = sys_sql_ctx(
        session,
        kctx,
        "SELECT PURGE_CUTOFF_TSO\nFROM mysql.tidb_mlog_purge_hist\nWHERE MLOG_ID = %? AND PURGE_CUTOFF_TSO IS NOT NULL\nORDER BY PURGE_JOB_ID DESC LIMIT 1",
        &[SqlArg::Signed(mlog_id)],
    )
    .map_err(|error| {
        if is_table_not_exists(&error) {
            plain("purge materialized view log: required system table mysql.tidb_mlog_purge_hist does not exist")
        } else {
            error
        }
    })?;
    Ok(rows.first().and_then(|row| row.first()).and_then(datum_u64))
}

/// Go master `insertMLogPurgeHistRunning`.
#[allow(clippy::too_many_arguments)]
fn insert_mlog_purge_hist_running(
    session: &mut Session,
    kctx: &tidb_gocontext::Context,
    purge_job_id: u64,
    mlog_id: i64,
    base_schema: &str,
    base_table: &str,
    method: &str,
    cutoff_tso: u64,
    start: &str,
) -> Result<(), DriverError> {
    sys_sql_ctx(
        session,
        kctx,
        "INSERT INTO mysql.tidb_mlog_purge_hist (\nPURGE_JOB_ID, MLOG_ID, BASE_TABLE_SCHEMA, BASE_TABLE_NAME, PURGE_METHOD,\nPURGE_START_TIME, PURGE_ROWS, PURGE_STATUS, PURGE_CUTOFF_TSO, LAST_HEARTBEAT_TIME)\nVALUES (%?, %?, %?, %?, %?, %?, %?, %?, %?, %?)",
        &[
            SqlArg::Unsigned(purge_job_id),
            SqlArg::Signed(mlog_id),
            SqlArg::String(base_schema.as_bytes()),
            SqlArg::String(base_table.as_bytes()),
            SqlArg::String(method.as_bytes()),
            SqlArg::String(start.as_bytes()),
            SqlArg::Signed(0),
            SqlArg::String(MV_TASK_HIST_STATUS_RUNNING.as_bytes()),
            SqlArg::Unsigned(cutoff_tso),
            SqlArg::String(start.as_bytes()),
        ],
    )
    .map(|_| ())
    .map_err(|error| if is_table_not_exists(&error) { purge_hist_missing_table() } else { error })
}

/// Go master `insertMLogPurgeHistFailed`.
#[allow(clippy::too_many_arguments)]
fn insert_mlog_purge_hist_failed(
    session: &mut Session,
    purge_job_id: u64,
    mlog_id: i64,
    base_schema: &str,
    base_table: &str,
    method: &str,
    start: chrono::DateTime<chrono::Local>,
    end: chrono::DateTime<chrono::Local>,
    rows: i64,
    reason: &str,
) -> Result<(), DriverError> {
    let (start_text, end_text) = (hist_time(start), hist_time(end));
    let duration = format_duration_seconds_between(start, end);
    sys_sql(
        session,
        "INSERT INTO mysql.tidb_mlog_purge_hist (\nPURGE_JOB_ID, MLOG_ID, BASE_TABLE_SCHEMA, BASE_TABLE_NAME, PURGE_METHOD,\nPURGE_START_TIME, PURGE_END_TIME, PURGE_ROWS, PURGE_DURATION_SEC, PURGE_STATUS, PURGE_FAILED_REASON)\nVALUES (%?, %?, %?, %?, %?, %?, %?, %?, %?, %?, %?)",
        &[
            SqlArg::Unsigned(purge_job_id),
            SqlArg::Signed(mlog_id),
            SqlArg::String(base_schema.as_bytes()),
            SqlArg::String(base_table.as_bytes()),
            SqlArg::String(method.as_bytes()),
            SqlArg::String(start_text.as_bytes()),
            SqlArg::String(end_text.as_bytes()),
            SqlArg::Signed(rows),
            SqlArg::String(duration.as_bytes()),
            SqlArg::String(MV_TASK_HIST_STATUS_FAILED.as_bytes()),
            SqlArg::String(reason.as_bytes()),
        ],
    )
    .map(|_| ())
    .map_err(|error| if is_table_not_exists(&error) { purge_hist_missing_table() } else { error })
}

/// Go master `finalizeMLogPurgeHist`.
#[allow(clippy::too_many_arguments)]
fn finalize_mlog_purge_hist(
    session: &mut Session,
    purge_job_id: u64,
    status: &str,
    start: chrono::DateTime<chrono::Local>,
    end: chrono::DateTime<chrono::Local>,
    rows: i64,
    reason: Option<&str>,
) -> Result<(), DriverError> {
    let end_text = hist_time(end);
    let duration = format_duration_seconds_between(start, end);
    let updated = sys_sql(
        session,
        "UPDATE mysql.tidb_mlog_purge_hist\nSET PURGE_END_TIME = %?, PURGE_ROWS = %?, PURGE_DURATION_SEC = %?, PURGE_STATUS = %?, PURGE_FAILED_REASON = %?\nWHERE PURGE_JOB_ID = %?",
        &[
            SqlArg::String(end_text.as_bytes()),
            SqlArg::Signed(rows),
            SqlArg::String(duration.as_bytes()),
            SqlArg::String(status.as_bytes()),
            reason.map_or(SqlArg::Null, |reason| SqlArg::String(reason.as_bytes())),
            SqlArg::Unsigned(purge_job_id),
        ],
    );
    let updated = if failpoint_on("mockUpdateMaterializedViewLogPurgeStateErr") {
        Err(plain("mock update mlog purge state error"))
    } else {
        updated
    };
    updated.map(|_| ()).map_err(|error| if is_table_not_exists(&error) { purge_hist_missing_table() } else { error })
}

/// Go master `finalizeMLogPurgeHistWithRetry`.
#[allow(clippy::too_many_arguments)]
fn finalize_mlog_purge_hist_with_retry(
    session: &mut Session,
    purge_job_id: u64,
    status: &str,
    start: chrono::DateTime<chrono::Local>,
    end: chrono::DateTime<chrono::Local>,
    rows: i64,
    reason: Option<&str>,
) -> Result<(), DriverError> {
    let first = match finalize_mlog_purge_hist(session, purge_job_id, status, start, end, rows, reason) {
        Ok(()) => return Ok(()),
        Err(error) => error,
    };
    finalize_mlog_purge_hist(session, purge_job_id, status, start, end, rows, reason)
        .map_err(|retry| annotate(&retry, &format!("first finalize attempt failed: {}", driver_message(&first))))
}

/// Go master `updateMaterializedViewLogPurgeInfoOnSuccess` for a manual
/// purge: the checkpoint advanced, the next purge time left alone.
fn update_materialized_view_log_purge_info_on_success(
    session: &mut Session,
    kctx: &tidb_gocontext::Context,
    mlog_id: i64,
    last_purged_tso: Option<u64>,
    next_unix_seconds: Option<i64>,
    should_update_next: bool,
) -> Result<(), DriverError> {
    let missing = |error: DriverError| if is_table_not_exists(&error) { purge_info_missing_table() } else { error };
    if let Some(last) = last_purged_tso {
        sys_sql_ctx(
            session,
            kctx,
            "UPDATE mysql.tidb_mlog_purge_info\nSET LAST_PURGED_TSO = %?\nWHERE MLOG_ID = %? AND (LAST_PURGED_TSO IS NULL OR LAST_PURGED_TSO < %?)",
            &[SqlArg::Unsigned(last), SqlArg::Signed(mlog_id), SqlArg::Unsigned(last)],
        )
        .map_err(missing)?;
    }
    if !should_update_next {
        return Ok(());
    }
    let next = next_unix_seconds.map_or(SqlArg::Null, SqlArg::Signed);
    sys_sql_ctx(
        session,
        kctx,
        "UPDATE mysql.tidb_mlog_purge_info\nSET NEXT_PURGE_UNIX_SECONDS = %? WHERE MLOG_ID = %?",
        &[next, SqlArg::Signed(mlog_id)],
    )
    .map(|_| ())
    .map_err(missing)
}

impl Session {
    /// Go master `PurgeMaterializedViewLogExec.Next`.
    pub(crate) fn purge_materialized_view_log(&mut self, stmt: &PurgeMaterializedViewLogStmt) -> Result<StmtOutput, DriverError> {
        let purge_start = chrono::Local::now();
        let is_internal_sql = self.vars.restricted_sql;
        let method = validate_purge_materialized_view_log_stmt(stmt, is_internal_sql)?;
        let mut target = self.resolve_purge_materialized_view_log_meta(stmt)?;
        target.is_internal_sql = is_internal_sql;
        let mut progress = PurgeProgress::default();
        // Go `newMVTaskCancelController(kctx)` over the statement's context.
        let controller = std::sync::Arc::new(MvTaskCancelController::new(&self.exec_ctx));
        // Go's deferred `recover()`: a panic fails the purge with its value.
        let result = crate::mview_refresh::recover_refresh_panic(|| {
            self.check_operate_view_on_mlog(&target.schema, &target.mlog_name)
                .and_then(|()| self.execute_purge_materialized_view_log(&target, method, purge_start, &controller, &mut progress))
        });
        let Err(error) = result else {
            return result;
        };
        if progress.hist_running_inserted {
            return Err(error);
        }
        // Go master `insertMLogPurgeHistFailedFallback`: a failure before the
        // running row still leaves a durable failed history row.
        let (failed_reason, error) = controller.normalize_task_failure(error);
        let reason = failed_reason.unwrap_or_else(|| driver_message(&error));
        let mut hist = self.open_refresh_system_session().map_err(|open| {
            annotate(&open, &format!("purge materialized view log: failed to open history session after error {reason}"))
        })?;
        if progress.purge_job_id == 0 {
            progress.purge_job_id = session_current_tso(&mut hist).map_err(|alloc| {
                annotate(
                    &alloc,
                    &format!("purge materialized view log: failed to allocate history job id after error {reason}"),
                )
            })?;
        }
        insert_mlog_purge_hist_failed(
            &mut hist,
            progress.purge_job_id,
            target.mlog_id,
            &target.schema,
            target.base_table.name.original(),
            method,
            purge_start,
            chrono::Local::now(),
            progress.total_purge_rows,
            &reason,
        )
        .map_err(|hist_error| {
            annotate(
                &hist_error,
                &format!("purge materialized view log: failed to insert failed purge history after error {reason}"),
            )
        })?;
        Err(error)
    }

    /// Go master `checkOperateViewOnMLog`.
    fn check_operate_view_on_mlog(&self, schema: &str, mlog_name: &str) -> Result<(), DriverError> {
        let Some((user, host)) = self.current_identity().map(|(user, host)| (user.to_owned(), host.to_owned())) else {
            return Ok(());
        };
        let mlog_lower = tidb_hack::go_to_lower(mlog_name);
        if self.has_table_privilege(&tidb_hack::go_to_lower(schema), &mlog_lower, tidb_mysql::OperateViewPriv) {
            return Ok(());
        }
        Err(DriverError::Mysql(MysqlError::from_terror(&tidb_plannererrors::ERR_TABLEACCESS_DENIED.gen_with_stack_by_args(&[
            "OPERATE VIEW".into(),
            user.into(),
            host.into(),
            mlog_lower.into(),
        ]))))
    }

    /// Go master `resolvePurgeMaterializedViewLogMeta`, on the session's
    /// latest schema.
    fn resolve_purge_materialized_view_log_meta(&self, stmt: &PurgeMaterializedViewLogStmt) -> Result<PurgeTarget, DriverError> {
        let catalog = self.refresh_latest_info_schema();
        let (schema, table) = match stmt.table.as_slice() {
            [schema, table] if !schema.is_empty() => (schema.clone(), table.clone()),
            [.., table] => {
                if self.current_database().is_empty() {
                    return Err(DriverError::Mysql(MysqlError::from_terror(
                        &tidb_plannererrors::ERR_NO_DB.gen_with_stack_by_args(&[]),
                    )));
                }
                (self.current_database().to_owned(), table.clone())
            }
            [] => return Err(plain("purge materialized view log: missing table name")),
        };
        if catalog.schema_by_name(&schema).is_none() {
            return Err(DriverError::Mysql(MysqlError::from_terror(
                &tidb_infoschema::infoschema_errors::ERR_DATABASE_NOT_EXISTS.gen_with_stack_by_args(&[schema.into()]),
            )));
        }
        let table_not_exists = |name: &str| {
            DriverError::Mysql(MysqlError::from_terror(
                &tidb_infoschema::infoschema_errors::ERR_TABLE_NOT_EXISTS
                    .gen_with_stack_by_args(&[schema.clone().into(), name.to_owned().into()]),
            ))
        };
        let Some((_, base)) = catalog.table_info_by_name(&schema, &table) else {
            return Err(table_not_exists(&table));
        };
        let base = base.read().clone_like_go();
        if base.is_view() || base.is_sequence() || base.temp_table_type != tidb_model::TempTableType::NONE {
            return Err(plain(format!("table {schema}.{table} is not a base table")));
        }
        let mlog_name = tidb_model::table::materialized_view_log_table_name(&base.name);
        let Some((_, mlog)) = catalog.table_info_by_name(&schema, mlog_name.original()) else {
            return Err(plain(format!("materialized view log does not exist for base table {schema}.{table}")));
        };
        let mlog = mlog.read();
        if mlog.materialized_view_log.as_ref().is_none_or(|info| info.read().base_table_id != base.id) {
            return Err(plain(format!(
                "table {schema}.{} is not a materialized view log for base table {schema}.{table}",
                mlog_name.original()
            )));
        }
        Ok(PurgeTarget {
            mlog_name: mlog_name.original().to_owned(),
            mlog_id: mlog.id,
            mlog_shard_row_id_bits: mlog.shard_row_id_bits,
            mlog_info: mlog.materialized_view_log.clone(),
            is_internal_sql: false,
            schema,
            base_table: base,
        })
    }

    /// Go master `executePurgeMaterializedViewLog` past its target checks:
    /// the purge, delete and history system sessions, then the purge.
    fn execute_purge_materialized_view_log(
        &mut self,
        target: &PurgeTarget,
        method: &str,
        purge_start: chrono::DateTime<chrono::Local>,
        controller: &std::sync::Arc<MvTaskCancelController>,
        progress: &mut PurgeProgress,
    ) -> Result<StmtOutput, DriverError> {
        let user_var = |name: &str| self.vars.get_system(name).unwrap_or_default();
        let mem_quota = user_var(tidb_vardef::tidb_vars::TIDB_MVIEW_MAINTAIN_MEM_QUOTA);
        let engines = user_var(tidb_vardef::tidb_vars::TIDB_MVIEW_MAINTAIN_ISOLATION_READ_ENGINES);
        let batch_size = user_var(tidb_vardef::tidb_vars::TIDB_MLOG_PURGE_BATCH_SIZE)
            .parse::<i64>()
            .ok()
            .filter(|&size| size > 0)
            .unwrap_or(tidb_vardef::defaults::DEF_TIDB_MLOG_PURGE_BATCH_SIZE);
        let delete_tiflash_threads =
            user_var(tidb_vardef::tidb_vars::TIDB_MLOG_PURGE_DELETE_TIFLASH_THREADS).parse::<i64>().unwrap_or(0);
        let throttle_config = || -> Result<MLogPurgeThrottleConfig, DriverError> {
            let parse = |name: &str| user_var(name).parse::<f64>().map_err(|error| plain(error.to_string()));
            Ok(MLogPurgeThrottleConfig {
                min_rate: parse(tidb_vardef::tidb_vars::TIDB_MLOG_PURGE_MIN_RATE)?,
                budget_ratio: parse(tidb_vardef::tidb_vars::TIDB_MLOG_PURGE_RATE_BUDGET_RATIO)?,
            })
        };
        let throttle_config = throttle_config();

        // The purge-info row lock and checkpoint are kept in this
        // transaction.
        let mut purge = self.open_refresh_system_session()?;
        let restore_purge = apply_mlog_purge_maintenance_session_vars(&mut purge, &mem_quota, &engines, target.is_internal_sql)?;
        inject_maintenance_vars_applied(&mut purge, &mem_quota, &engines, "PurgeSession");
        // Go master: the purge advisory lock a cutover retiring this log
        // also takes. "Do not retry acquisition: this statement may hold MDL
        // needed by cutover. The later transaction cleanup runs before this
        // advisory lock is released."
        let advisory_lock_name = tidb_mviewutil::purge_advisory_lock_name(target.mlog_id);
        let locked = match sys_sql(&mut purge, "SELECT GET_LOCK(%?, 1)", &[SqlArg::String(advisory_lock_name.as_bytes())]) {
            Ok(rows) => rows.first().and_then(|row| row.first()).and_then(crate::mview_refresh::datum_i64) == Some(1),
            Err(error) => {
                restore_vars(&mut purge, &restore_purge);
                return Err(error);
            }
        };
        if !locked {
            restore_vars(&mut purge, &restore_purge);
            return Err(DriverError::Mysql(MysqlError::from_terror(
                &tidb_exeerrors::ERR_MV_ADVISORY_LOCK_CONFLICT.fast_gen_by_args(&[format!(
                    "another purge or materialized view cutover is running for materialized view log on {}.{}, please retry later",
                    target.schema,
                    target.base_table.name.original()
                )
                .into()]),
            )));
        }
        let release_advisory_lock = |purge: &mut Session| {
            let _ = sys_sql(purge, "SELECT RELEASE_LOCK(%?)", &[SqlArg::String(advisory_lock_name.as_bytes())]);
        };
        let mut delete = match self.open_refresh_system_session() {
            Ok(delete) => delete,
            Err(error) => {
                release_advisory_lock(&mut purge);
                restore_vars(&mut purge, &restore_purge);
                return Err(error);
            }
        };
        let restore_delete = match apply_mlog_purge_maintenance_session_vars(&mut delete, &mem_quota, &engines, target.is_internal_sql) {
            Ok(restore) => restore,
            Err(error) => {
                release_advisory_lock(&mut purge);
                restore_vars(&mut purge, &restore_purge);
                return Err(error);
            }
        };
        inject_maintenance_vars_applied(&mut delete, &mem_quota, &engines, "PurgeDeleteSession");
        let mut restore_delete_threads = Vec::new();
        // Go: the purge and delete sessions run under the task's cancelable
        // context.
        let purge_handle = controller.watch(&purge);
        let delete_handle = controller.watch(&delete);
        let open_session = || self.open_refresh_system_session();
        let result = open_session().and_then(|mut hist| {
            let mut txn_started = false;
            let mut txn_finished = false;
            let outcome = purge_locked(
                &mut purge,
                &mut delete,
                &mut hist,
                target,
                method,
                purge_start,
                batch_size,
                delete_tiflash_threads,
                throttle_config,
                progress,
                &mut txn_started,
                &mut txn_finished,
                &mut restore_delete_threads,
                controller,
                &purge_handle,
                &open_session,
            );
            // Go `finalizeCtx`: the cleanup runs uncanceled.
            purge_handle.reset();
            if txn_started && !txn_finished {
                let _ = sys_sql(&mut purge, "ROLLBACK", &[]);
            }
            outcome
        });
        purge_handle.reset();
        delete_handle.reset();
        release_advisory_lock(&mut purge);
        restore_vars(&mut delete, &restore_delete_threads);
        restore_vars(&mut delete, &restore_delete);
        restore_vars(&mut purge, &restore_purge);
        let (total_purge_rows, history_error) = result?;
        if let Some(error) = history_error {
            crate::mview_refresh::append_cause_warning(
                self,
                &annotate(&error, "purge materialized view log: purge committed but failed to finalize purge history"),
            );
        }
        self.statement_message = format!("Rows inserted: 0  Updated: 0  Deleted: {total_purge_rows}");
        Ok(StmtOutput::Affected(u64::try_from(total_purge_rows).unwrap_or_default()))
    }
}

/// The body of Go master `executePurgeMaterializedViewLog` from `BEGIN
/// PESSIMISTIC` to `COMMIT` and the finalized history row; answers the rows
/// deleted and a history error to warn about.
#[allow(clippy::too_many_arguments)]
fn purge_locked(
    purge: &mut Session,
    delete: &mut Session,
    hist: &mut Session,
    target: &PurgeTarget,
    method: &str,
    purge_start: chrono::DateTime<chrono::Local>,
    batch_size: i64,
    delete_tiflash_threads: i64,
    throttle_config: Result<MLogPurgeThrottleConfig, DriverError>,
    progress: &mut PurgeProgress,
    txn_started: &mut bool,
    txn_finished: &mut bool,
    restore_delete_threads: &mut Vec<(&'static str, String)>,
    controller: &std::sync::Arc<MvTaskCancelController>,
    purge_handle: &tidb_sqlkiller::StatementCancellation,
    open_session: &dyn Fn() -> Result<Session, DriverError>,
) -> Result<(i64, Option<DriverError>), DriverError> {
    // Go `kctx = taskCancelController.context()`: the running statements'
    // context; the cleanup runs on `finalizeCtx` (uncanceled).
    let kctx = controller.context().clone();
    let kctx = &kctx;
    // Go `stopTaskMonitor`, deferred: dropping the monitor stops it.
    let mut _monitor: Option<MvTaskMonitor> = None;
    let schema = target.schema.as_str();
    let base_table = target.base_table.name.original();
    let mlog_id = target.mlog_id;
    // Go `finalizeFailure`.
    let finalize_failure = |purge: &mut Session, hist: &mut Session, progress: &PurgeProgress, txn_started: bool, txn_finished: &mut bool, error: DriverError| {
        let (failed_reason, error) = controller.normalize_task_failure(error);
        if txn_started && !*txn_finished {
            // Go `finalizeCtx`: the rollback runs uncanceled.
            purge_handle.reset();
            let _ = sys_sql(purge, "ROLLBACK", &[]);
            *txn_finished = true;
        }
        if !progress.hist_running_inserted {
            return error;
        }
        let reason = failed_reason.unwrap_or_else(|| driver_message(&error));
        match finalize_mlog_purge_hist_with_retry(
            hist,
            progress.purge_job_id,
            MV_TASK_HIST_STATUS_FAILED,
            purge_start,
            chrono::Local::now(),
            progress.total_purge_rows,
            Some(&reason),
        ) {
            Ok(()) => error,
            Err(hist_error) => annotate(
                &hist_error,
                &format!("purge materialized view log: failed to finalize purge history after error {reason}"),
            ),
        }
    };
    if let Some(message) =
        tidb_failpoint::eval(&format!("{FP}mockPurgeMaterializedViewLogErrorBeforeInsertHist")).and_then(|value| value.as_string().map(str::to_owned))
    {
        return Err(plain(message));
    }
    if failpoint_on("mockPurgeMaterializedViewLogBeginErr") {
        return Err(finalize_failure(purge, hist, progress, false, txn_finished, plain("mock purge begin error")));
    }
    if let Err(error) = sys_sql_ctx(purge, kctx, "BEGIN PESSIMISTIC", &[]) {
        return Err(finalize_failure(purge, hist, progress, false, txn_finished, error));
    }
    *txn_started = true;

    macro_rules! try_purge {
        ($expr:expr) => {
            match $expr {
                Ok(value) => value,
                Err(error) => return Err(finalize_failure(purge, hist, progress, true, txn_finished, error)),
            }
        };
    }

    let (locked_last_purged_tso, locked_next_time) =
        try_purge!(acquire_materialized_view_log_purge_lock(purge, kctx, schema, base_table, mlog_id));
    // Go `purgeSctx.Txn(true).StartTS()`: the lock above activated the
    // transaction.
    let mut purge_start_tso = purge.node_conn().explicit_start_ts().unwrap_or(0);
    if failpoint_on("mockPurgeMaterializedViewLogZeroStartTS") {
        purge_start_tso = 0;
    }
    if purge_start_tso == 0 {
        try_purge!(Err(plain("purge materialized view log: invalid transaction start tso")));
    }
    progress.purge_job_id = purge_start_tso;

    let (public, building) = try_purge!(collect_dependent_mview_ids_for_mlog_purge(purge, kctx, &target.base_table, mlog_id));
    let safe_purge_tso =
        try_purge!(calc_materialized_view_log_safe_purge_tso(purge, kctx, schema, base_table, purge_start_tso, &public, &building));

    let mut purge_cutoff_fence_tso = locked_last_purged_tso.unwrap_or(0);
    if let Some(latest) = try_purge!(read_latest_mlog_purge_cutoff_fence_tso(hist, kctx, mlog_id)) {
        purge_cutoff_fence_tso = purge_cutoff_fence_tso.max(latest);
    }
    let skip_purge_by_cutoff_fence = safe_purge_tso < purge_cutoff_fence_tso;
    if !skip_purge_by_cutoff_fence {
        try_purge!(insert_mlog_purge_hist_running(
            hist,
            kctx,
            progress.purge_job_id,
            mlog_id,
            schema,
            base_table,
            method,
            safe_purge_tso,
            &hist_time(purge_start),
        ));
        progress.hist_running_inserted = true;
        let purge_job_id = progress.purge_job_id;
        let started = open_session().and_then(|watch| {
            start_mv_task_monitor(
                watch,
                std::sync::Arc::clone(controller),
                format!("mlog-purge-{purge_job_id}"),
                Box::new(move |session: &mut Session| read_purge_hist_cancel_request(session, purge_job_id, mlog_id)),
                Some(Box::new(move |session: &mut Session| update_purge_hist_heartbeat(session, purge_job_id, mlog_id))),
            )
        });
        _monitor = Some(try_purge!(started));
        tidb_failpoint::inject(&format!("{FP}pausePurgeMaterializedViewLogAfterInsertPurgeHistRunning"), |_| {});
    }
    let skip_delete_by_checkpoint = locked_last_purged_tso.is_some_and(|last| last >= safe_purge_tso);
    if !skip_purge_by_cutoff_fence && !skip_delete_by_checkpoint && safe_purge_tso > 0 {
        let delete_plan = try_build_mlog_purge_delete_plan_best_effort(
            delete,
            kctx,
            target,
            locked_last_purged_tso,
            safe_purge_tso,
            locked_next_time,
            throttle_config,
        );
        // Keep COUNT at the session's original TiFlash concurrency.
        if delete_plan.as_ref().is_none_or(|plan| plan.pending_rows > 0) {
            *restore_delete_threads = try_purge!(apply_mlog_purge_delete_tiflash_threads(delete, delete_tiflash_threads, target.is_internal_sql));
        }
        match delete_plan {
            Some(plan) if !(plan.pending_rows > 0 && plan.row_id_ranges.is_empty()) => {
                if plan.pending_rows > 0 {
                    let mut throttle_plan = plan.throttle_plan;
                    let mut effective_batch_size =
                        throttle_plan.as_mut().map_or(batch_size, |throttle| throttle.effective_delete_batch_size(batch_size));
                    let delete_loop_start = Instant::now();
                    for (range_index, &row_id_range) in plan.row_id_ranges.iter().enumerate() {
                        loop {
                            let deleted = purge_materialized_view_log_data(
                                delete,
                                kctx,
                                schema,
                                &target.mlog_name,
                                locked_last_purged_tso,
                                safe_purge_tso,
                                Some(row_id_range),
                                effective_batch_size,
                            );
                            let rows = try_purge!(deleted);
                            progress.total_purge_rows += rows;
                            tidb_failpoint::inject(&format!("{FP}pausePurgeMaterializedViewLogAfterDeleteBatch"), |_| {});
                            let has_more_ranges = range_index + 1 < plan.row_id_ranges.len();
                            let batch_completed = rows >= effective_batch_size;
                            if let Some(throttle) = throttle_plan.as_mut() {
                                if should_throttle_mlog_purge_delete_batch(batch_completed, has_more_ranges) {
                                    match throttle.maybe_sleep(delete_loop_start, progress.total_purge_rows) {
                                        Ok(()) => effective_batch_size = throttle.effective_delete_batch_size(batch_size),
                                        Err(error) if controller.is_manual_cancel_requested() => {
                                            try_purge!(Err(error));
                                        }
                                        Err(error) => {
                                            log_purge_warn("purge materialized view log: adaptive throttle sleep failed, fallback to unthrottled purge", &target.schema, &target.mlog_name, Some(&error));
                                            throttle_plan = None;
                                            effective_batch_size = batch_size;
                                        }
                                    }
                                }
                            }
                            if !batch_completed {
                                break;
                            }
                        }
                    }
                }
            }
            _ => loop {
                let deleted = purge_materialized_view_log_data(
                    delete,
                    kctx,
                    schema,
                    &target.mlog_name,
                    locked_last_purged_tso,
                    safe_purge_tso,
                    None,
                    batch_size,
                );
                let rows = try_purge!(deleted);
                progress.total_purge_rows += rows;
                tidb_failpoint::inject(&format!("{FP}pausePurgeMaterializedViewLogAfterDeleteBatch"), |_| {});
                if rows < batch_size {
                    break;
                }
            },
        }
    }

    let (next_unix_seconds, should_update_next) =
        if target.is_internal_sql { try_purge!(derive_mlog_purge_next_unix_seconds(target)) } else { (None, false) };
    let checkpoint = (!skip_purge_by_cutoff_fence && !skip_delete_by_checkpoint).then_some(safe_purge_tso);
    try_purge!(update_materialized_view_log_purge_info_on_success(purge, kctx, mlog_id, checkpoint, next_unix_seconds, should_update_next));
    try_purge!(sys_sql_ctx(purge, kctx, "COMMIT", &[]));
    *txn_finished = true;

    let mut history_error = None;
    if progress.hist_running_inserted {
        let finalized = if failpoint_on("mockPurgeMaterializedViewLogFinalizeSuccessErr") {
            Err(plain("mock purge finalize success error"))
        } else {
            finalize_mlog_purge_hist_with_retry(
                hist,
                progress.purge_job_id,
                MV_TASK_HIST_STATUS_SUCCESS,
                purge_start,
                chrono::Local::now(),
                progress.total_purge_rows,
                None,
            )
        };
        history_error = finalized.err();
    }
    Ok((progress.total_purge_rows, history_error))
}

/// Go master `tryBuildMLogPurgeDeletePlanBestEffort` for a manual purge:
/// `None` (an unscoped, unthrottled purge) when the pending rows cannot be
/// counted.
fn try_build_mlog_purge_delete_plan_best_effort(
    delete: &mut Session,
    kctx: &tidb_gocontext::Context,
    target: &PurgeTarget,
    last_purged_tso: Option<u64>,
    safe_purge_tso: u64,
    fallback_next_time: Option<chrono::DateTime<chrono::Utc>>,
    throttle_config: Result<MLogPurgeThrottleConfig, DriverError>,
) -> Option<MLogPurgeDeletePlan> {
    if safe_purge_tso == 0 {
        return None;
    }
    let stats = match read_mlog_purge_pending_row_stats_on_tiflash(delete, kctx, &target.schema, &target.mlog_name, last_purged_tso, safe_purge_tso) {
        Ok(stats) => stats,
        Err(error) => {
            log_purge_warn("purge materialized view log: failed to read pending row stats, fallback to unscoped unthrottled purge", &target.schema, &target.mlog_name, Some(&error));
            return None;
        }
    };
    let mut plan = MLogPurgeDeletePlan { pending_rows: stats.pending_rows, throttle_plan: None, row_id_ranges: Vec::new() };
    if stats.pending_rows <= 0 {
        return Some(plan);
    }
    plan.row_id_ranges = build_mlog_purge_delete_row_id_ranges(stats, target.mlog_shard_row_id_bits);
    let config = match throttle_config {
        Ok(config) => config,
        Err(error) => {
            log_purge_warn("purge materialized view log: failed to load adaptive throttle config, fallback to unthrottled purge", &target.schema, &target.mlog_name, Some(&error));
            return Some(plan);
        }
    };
    let deadline = match derive_mlog_purge_throttle_deadline(target, fallback_next_time) {
        Ok(deadline) => deadline,
        Err(error) => {
            log_purge_warn("purge materialized view log: failed to derive adaptive throttle deadline, fallback to unthrottled purge", &target.schema, &target.mlog_name, Some(&error));
            return Some(plan);
        }
    };
    plan.throttle_plan = try_build_mlog_purge_throttle_plan(stats, deadline, config);
    Some(plan)
}

/// Go master `deriveMLogPurgeThrottleDeadline`: the adaptive budget's end,
/// no later than an internal purge's next scheduled run (a manual one's
/// stored next run).
fn derive_mlog_purge_throttle_deadline(
    target: &PurgeTarget,
    fallback_next_time: Option<chrono::DateTime<chrono::Utc>>,
) -> Result<chrono::DateTime<chrono::Utc>, DriverError> {
    if failpoint_on("mockMLogPurgeAdaptiveDeadlineErr") {
        return Err(plain("mock adaptive purge deadline error"));
    }
    let mut deadline = chrono::Utc::now() + MLOG_PURGE_ADAPTIVE_MAX_BUDGET;
    if target.is_internal_sql {
        let (next, should_update) = derive_mlog_purge_next_unix_seconds(target)?;
        if let Some(next) = next.filter(|_| should_update).and_then(|next| chrono::DateTime::from_timestamp(next, 0)) {
            if next < deadline {
                deadline = next;
            }
        }
    } else if let Some(next) = fallback_next_time.filter(|&next| next < deadline) {
        deadline = next;
    }
    Ok(deadline)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn delete_sql_scopes_the_commit_ts_window_as_go() {
        let range = MLogPurgeDeleteRowIdRange { start_row_id: 1, end_row_id: 9 };
        assert_eq!(
            build_purge_materialized_view_log_delete_sql("d", "$mlog$t", None, 7, None, 100).unwrap(),
            "DELETE /*+ read_from_storage(tiflash[`d`.`$mlog$t`]) */ FROM `d`.`$mlog$t` WHERE _tidb_commit_ts <= 7 LIMIT 100"
        );
        assert_eq!(
            build_purge_materialized_view_log_delete_sql("d", "$mlog$t", Some(3), 7, Some(range), 100).unwrap(),
            "DELETE /*+ read_from_storage(tiflash[`d`.`$mlog$t`]) */ FROM `d`.`$mlog$t` WHERE _tidb_rowid >= 1 AND _tidb_rowid <= 9 AND _tidb_commit_ts > 3 AND _tidb_commit_ts <= 7 LIMIT 100"
        );
    }

    #[test]
    fn row_id_ranges_split_as_go() {
        let stats = |pending_rows, min_row_id, max_row_id| MLogPurgePendingRowStats {
            pending_rows,
            min_row_id,
            max_row_id,
            has_row_id_bounds: true,
        };
        let r = |start_row_id, end_row_id| MLogPurgeDeleteRowIdRange { start_row_id, end_row_id };
        assert_eq!(build_mlog_purge_delete_row_id_ranges(stats(10, 1, 100), 0), vec![r(1, 100)]);
        assert_eq!(build_mlog_purge_delete_row_id_ranges(stats(24_000, 1, 100), 0), vec![r(1, 33), r(34, 66), r(67, 100)]);
        assert_eq!(build_mlog_purge_delete_row_id_ranges(stats(1_000_000, 1, 10), 0).len(), 1);
        // Two shard bits, a signed row id: 61 incremental bits per bucket;
        // three ranges floor to two of two buckets each.
        assert_eq!(
            build_mlog_purge_delete_row_id_ranges(stats(24_000, 5, i64::MAX), 2),
            vec![r(5, (1 << 62) - 1), r(1 << 62, i64::MAX)]
        );
        assert_eq!(floor_power_of_two(3), 2);
        assert_eq!(floor_power_of_two(16), 16);
        assert_eq!(floor_power_of_two(0), 0);
    }

    #[test]
    fn throttle_batches_follow_the_target_rate_as_go() {
        assert_eq!(calc_mlog_purge_adaptive_batch_size(0.0), 8000);
        assert_eq!(calc_mlog_purge_adaptive_batch_size(100_000.0), 20_000);
        let stats = MLogPurgePendingRowStats { pending_rows: 10, ..MLogPurgePendingRowStats::default() };
        let config = MLogPurgeThrottleConfig { min_rate: 2000.0, budget_ratio: 0.5 };
        let deadline = chrono::Utc::now() + Duration::from_secs(100);
        let mut plan = try_build_mlog_purge_throttle_plan(stats, deadline, config).unwrap();
        assert!((plan.target_rate - 2000.0).abs() < f64::EPSILON);
        assert_eq!(plan.effective_delete_batch_size(10_000), 8000);
        assert_eq!(plan.effective_delete_batch_size(500), 500);
        assert!(try_build_mlog_purge_throttle_plan(stats, chrono::Utc::now(), config).is_none());
        assert!(try_build_mlog_purge_throttle_plan(stats, deadline, MLogPurgeThrottleConfig { budget_ratio: 0.0, ..config }).is_none());
    }
}

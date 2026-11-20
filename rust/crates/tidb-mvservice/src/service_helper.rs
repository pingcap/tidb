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

//! Go master `pkg/mvservice/service_helper.go`: the [`Helper`] the service
//! runs its tasks with, each on a pooled system session.
//!
//! [`Helper`]: crate::Helper

use std::collections::HashMap;
use std::sync::Arc;

use tidb_datatype::Datum;
use tidb_gocontext::Context;
use tidb_log::{Field, Value};
use tidb_logutil::bg_logger;
use tidb_model::TableInfo;
use tidb_sqlescape::SqlArg;
use tidb_timer::go_time::{GoTime, MINUTE};

use crate::server_maintainer::{ServerDiscovery, ServerInfo};
use crate::service::{
    MVMetricsReporter, MVService, MViewTask, MViewTaskState, MlogPurgeTask, RefreshAlertTask,
    DEFAULT_MV_HISTORY_GC_MAX_RECORDS,
};
use crate::task_handler::{
    err_mv_task_canceled_manually, is_mv_task_canceled_manually, MVTaskHandler, MlogAccumulationTask, MlogAnalyzeTask,
    MvsError, SessionPool,
};
use crate::time_proxy::{mvs_now, mvs_since, mvs_unix, mvs_unix_milli};

const HISTORY_GC_DELETE_BATCH_SIZE: u64 = 10_000;
const REFRESH_ALERT_WRITE_BATCH_SIZE: usize = 128;
/// Go `mvHistoryHeartbeatTimeout` (24h), in nanoseconds.
const MV_HISTORY_HEARTBEAT_TIMEOUT: i64 = 24 * 60 * MINUTE;
const MV_HISTORY_ORPHANED_STATUS: &str = "orphaned";
const MV_REFRESH_HISTORY_ORPHANED_REASON: &str = "task heartbeat expired before refresh history finalize";
const MV_LOG_PURGE_HISTORY_ORPHANED_REASON: &str = "task heartbeat expired before purge history finalize";

/// MySQL `ER_LOCK_NOWAIT` (Go `storeerr.ErrLockAcquireFailAndNoWaitSet`).
const ER_LOCK_NOWAIT: u16 = 3572;
/// MySQL `ER_NO_SUCH_TABLE` (Go `infoschema.ErrTableNotExists`).
const ER_NO_SUCH_TABLE: u16 = 1146;

/// Go `sessionctx.Context` as `service_helper.go` uses a pooled system
/// session: restricted SQL on the session itself
/// (`ExecOptionUseCurSession`), the latest schema without the session's
/// temporary tables, its variables and store.
#[allow(clippy::missing_errors_doc)]
pub trait MVServiceSessionContext: Send + Sync {
    /// Go `GetRestrictedSQLExecutor().ExecRestrictedSQL(ctx,
    /// ExecOptionUseCurSession, sql, args...)` under
    /// `kv.InternalTxnMViewMaintenance`.
    fn exec_restricted_sql(&self, sql: &str, args: &[SqlArg<'_>]) -> Result<Vec<Vec<Datum>>, MvsError>;
    /// Go `GetSQLExecutor().ExecuteInternal(ctx, sql)`.
    fn execute_internal(&self, sql: &str) -> Result<(), MvsError>;
    /// Go `GetSessionVars().StmtCtx.AffectedRows()` of the last statement.
    fn affected_rows(&self) -> u64;
    /// Go `GetLatestISWithoutSessExt().TableByID(id).Meta()`.
    fn table_info_by_id(&self, id: i64) -> Option<TableInfo>;
    /// Go `GetLatestISWithoutSessExt().SchemaByID(id).Name.L`.
    fn schema_name_by_id(&self, id: i64) -> Option<String>;
    /// Go `GetSessionVars().GetGlobalSystemVar(ctx, name)`.
    fn global_system_var(&self, name: &str) -> Result<String, MvsError>;
    /// Go `GetSessionVars().GetSystemVar(name)`.
    fn system_var(&self, name: &str) -> Option<String>;
    /// Go `GetSessionVars().SetSystemVar(name, value)`.
    fn set_system_var(&self, name: &str, value: &str) -> Result<(), MvsError>;
    /// Go `GetStore().CurrentVersion(kv.GlobalTxnScope).Ver`.
    fn store_current_version(&self) -> Result<u64, MvsError>;
    /// Go `GetSessionVars().StmtCtx`'s statement time (the session's
    /// `NOW()` after a statement), in UTC.
    fn statement_now(&self) -> chrono::DateTime<chrono::Utc>;
}

/// Go `basic.SessionPool` of system sessions as the helper borrows them.
pub trait MVServiceSessionPool: Send + Sync {
    /// Go `pool.Get()` .. `pool.Put(se)` around `f`.
    fn with_session(&self, f: &mut dyn FnMut(&dyn MVServiceSessionContext) -> Result<(), MvsError>) -> Result<(), MvsError>;
}

/// Go `serviceHelper`'s environment: the local and cluster server views
/// and the metrics sink. The analyze half (stats handle, process tracker)
/// is the session layer's ([`MVServiceAnalyzer`]).
pub struct ServiceHelper {
    /// Go `infosync.GetServerInfo`.
    pub local_server: Box<dyn Fn() -> Result<ServerInfo, String> + Send + Sync>,
    /// Go `infosync.GetAllServerInfo`, keyed by ID with Go's exec ID.
    pub all_servers: Box<dyn Fn() -> Result<Vec<(ServerInfo, String)>, String> + Send + Sync>,
    /// Go `statsHandleGetter` / `sysProcTracker` / `isAutoAnalyzeStopped`.
    pub analyzer: Option<Arc<dyn MVServiceAnalyzer>>,
    /// Go `reportMetrics` and its observers.
    pub metrics: Arc<dyn MVMetricsReporter>,
}

/// The MV log analyze half of Go's helper, which reads the statistics
/// handle and runs `ANALYZE` through `statsautoanalyzeexec.RunAnalyzeStmt`.
#[allow(clippy::missing_errors_doc)]
pub trait MVServiceAnalyzer: Send + Sync {
    /// Go `LoadAllTiDBMVLogAnalyzeTasks`.
    fn load_all_tidb_mv_log_analyze_tasks(&self, ctx: &Context) -> Result<HashMap<i64, MlogAnalyzeTask>, MvsError>;
    /// Go `AnalyzeMVLog`.
    fn analyze_mv_log(&self, ctx: &Context, mlog_id: i64) -> Result<(), MvsError>;
}

/// The service's [`SessionPool`] over system sessions.
pub struct SystemSessionPool(pub Arc<dyn MVServiceSessionPool>);

impl SessionPool for SystemSessionPool {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

fn pool_of(pool: &dyn SessionPool) -> Result<&dyn MVServiceSessionPool, MvsError> {
    pool.as_any()
        .downcast_ref::<SystemSessionPool>()
        .map(|pool| pool.0.as_ref())
        .ok_or_else(|| MvsError::Other("mv service: the session pool is not a system session pool".to_owned()))
}

/// Go `pool.Get()` .. `pool.Put` around one call that answers a value.
fn with_session<T>(
    pool: &dyn SessionPool,
    mut f: impl FnMut(&dyn MVServiceSessionContext) -> Result<T, MvsError>,
) -> Result<T, MvsError> {
    let mut out = None;
    pool_of(pool)?.with_session(&mut |sctx| {
        out = Some(f(sctx)?);
        Ok(())
    })?;
    out.ok_or_else(|| MvsError::Other("mv service: no system session".to_owned()))
}

/// Go `execRCRestrictedSQLWithSession`: logs a failure.
fn exec_rc(sctx: &dyn MVServiceSessionContext, sql: &str, args: &[SqlArg<'_>]) -> Result<Vec<Vec<Datum>>, MvsError> {
    sctx.exec_restricted_sql(sql, args).inspect_err(|err| {
        bg_logger().warn(
            "execute restricted SQL failed",
            &[
                Field::new("sql", Value::Str(sql.to_owned())),
                Field::new("param_count", Value::I64(args.len() as i64)),
                Field::new("error", Value::Str(err.to_string())),
            ],
        );
    })
}

fn datum_i64(datum: &Datum) -> Option<i64> {
    match datum {
        Datum::Int(v) => Some(*v),
        Datum::UInt(v) => i64::try_from(*v).ok(),
        Datum::Null => None,
        _ => None,
    }
}

fn datum_u64(datum: &Datum) -> Option<u64> {
    match datum {
        Datum::UInt(v) => Some(*v),
        Datum::Int(v) => Some(*v as u64),
        Datum::Null => None,
        _ => None,
    }
}

/// Go `oracle.ExtractPhysical`.
const fn extract_physical(ts: u64) -> i64 {
    (ts >> 18) as i64
}

/// Go `oracle.ComposeTS(physical, 0)`.
const fn compose_ts(physical: i64) -> u64 {
    (physical as u64) << 18
}

/// Go `latestServerInfosByInstance`: one server per `IP:Port`, the latest
/// start winning (a fast restart leaves the old ID visible for a while).
#[must_use]
pub fn latest_server_infos_by_instance(all_servers: Vec<(ServerInfo, String)>) -> HashMap<String, ServerInfo> {
    let mut servers: HashMap<String, ServerInfo> = HashMap::with_capacity(all_servers.len());
    let mut instance_to_id: HashMap<String, String> = HashMap::with_capacity(all_servers.len());
    for (info, instance) in all_servers {
        if let Some(existing_id) = instance_to_id.get(&instance) {
            if info.start_timestamp <= servers[existing_id].start_timestamp {
                continue;
            }
            servers.remove(&existing_id.clone());
        }
        instance_to_id.insert(instance, info.id.clone());
        servers.insert(info.id.clone(), info);
    }
    servers
}

/// Go `appendMViewIDInCondition`.
fn append_mview_id_in_condition(sql: &mut String, mview_ids: &[i64]) {
    sql.push('(');
    for (i, id) in mview_ids.iter().enumerate() {
        if i > 0 {
            sql.push(',');
        }
        sql.push_str(&id.to_string());
    }
    sql.push(')');
}

/// Go `buildDeleteResolvedMVRefreshAlertSQL`.
#[must_use]
pub fn build_delete_resolved_mv_refresh_alert_sql(mview_ids: &[i64]) -> String {
    let mut sql = "DELETE FROM mysql.tidb_mview_refresh_alert WHERE MVIEW_ID IN ".to_owned();
    append_mview_id_in_condition(&mut sql, mview_ids);
    sql.push_str(" AND REFRESH_FAILED IS NULL");
    sql
}

fn time_arg(time: &GoTime) -> SqlArg<'static> {
    if time.is_zero() {
        return SqlArg::Time(None);
    }
    SqlArg::Time(Some(time.naive_local()))
}

/// Go `buildClearResolvedMVRefreshAlertLevelSQL`.
#[must_use]
pub fn build_clear_resolved_mv_refresh_alert_level_sql(updated_at: &GoTime, mview_ids: &[i64]) -> String {
    let mut sql = tidb_sqlescape::must_escape_sql_text(
        "UPDATE mysql.tidb_mview_refresh_alert SET ALERT_LEVEL = NULL, UPDATE_TIME = %? WHERE MVIEW_ID IN ",
        &[time_arg(updated_at)],
    );
    append_mview_id_in_condition(&mut sql, mview_ids);
    sql.push_str(" AND REFRESH_FAILED IS NOT NULL AND ALERT_LEVEL IS NOT NULL");
    sql
}

/// Go `buildUpsertMVRefreshAlertSQL`.
#[must_use]
pub fn build_upsert_mv_refresh_alert_sql(updated_at: &GoTime, states: &[RefreshAlertTask]) -> String {
    let mut sql = "INSERT INTO mysql.tidb_mview_refresh_alert (
MVIEW_ID,
MVIEW_SCHEMA,
MVIEW_NAME,
ALERT_LEVEL,
LAST_SUCCESS_SNAPSHOT_TIME,
UPDATE_TIME
) VALUES "
        .to_owned();
    for (i, state) in states.iter().enumerate() {
        if i > 0 {
            sql.push(',');
        }
        let last_success_time =
            if state.last_success_time.is_zero() { SqlArg::Null } else { time_arg(&state.last_success_time) };
        sql.push_str(&tidb_sqlescape::must_escape_sql_text(
            "(%?, %?, %?, %?, %?, %?)",
            &[
                SqlArg::Signed(state.mview_id),
                SqlArg::String(state.schema_name.as_bytes()),
                SqlArg::String(state.mview_name.as_bytes()),
                SqlArg::String(state.alert_level.as_bytes()),
                last_success_time,
                time_arg(updated_at),
            ],
        ));
    }
    sql.push_str(
        " ON DUPLICATE KEY UPDATE
MVIEW_SCHEMA = VALUES(MVIEW_SCHEMA),
MVIEW_NAME = VALUES(MVIEW_NAME),
ALERT_LEVEL = VALUES(ALERT_LEVEL),
LAST_SUCCESS_SNAPSHOT_TIME = VALUES(LAST_SUCCESS_SNAPSHOT_TIME),
UPDATE_TIME = VALUES(UPDATE_TIME)",
    );
    sql
}

/// Go `buildCleanupStaleMVRefreshAlertSQL`.
#[must_use]
pub fn build_cleanup_stale_mv_refresh_alert_sql() -> &'static str {
    "DELETE a
FROM mysql.tidb_mview_refresh_alert AS a
LEFT JOIN mysql.tidb_mview_refresh_info AS i ON a.MVIEW_ID = i.MVIEW_ID
WHERE i.MVIEW_ID IS NULL OR (a.ALERT_LEVEL IS NULL AND a.REFRESH_FAILED IS NULL)"
}

/// Go `buildClearDisabledMVRefreshAlertLevelSQL`.
#[must_use]
pub fn build_clear_disabled_mv_refresh_alert_level_sql() -> &'static str {
    "UPDATE mysql.tidb_mview_refresh_alert AS a
JOIN mysql.tidb_mview_refresh_info AS i ON a.MVIEW_ID = i.MVIEW_ID
SET a.ALERT_LEVEL = NULL, a.UPDATE_TIME = NOW(6)
WHERE i.NEXT_REFRESH_UNIX_SECONDS IS NULL AND a.ALERT_LEVEL IS NOT NULL"
}

/// Go `countRefreshHistorySQL`.
pub const COUNT_REFRESH_HISTORY_SQL: &str = "SELECT COUNT(*), MIN(REFRESH_JOB_ID) FROM mysql.tidb_mview_refresh_hist";
/// Go `countLogPurgeHistorySQL`.
pub const COUNT_LOG_PURGE_HISTORY_SQL: &str = "SELECT COUNT(*), MIN(PURGE_JOB_ID) FROM mysql.tidb_mlog_purge_hist";

/// Go `buildMarkRefreshHistoryRunningRowsOrphanedSQL`.
#[must_use]
pub fn build_mark_refresh_history_running_rows_orphaned_sql() -> String {
    format!(
        "UPDATE mysql.tidb_mview_refresh_hist FORCE INDEX (idx_refresh_status_start_time)
SET REFRESH_STATUS = '{MV_HISTORY_ORPHANED_STATUS}',
\tREFRESH_END_TIME = IFNULL(REFRESH_END_TIME, NOW(6)),
\tREFRESH_DURATION_SEC = IFNULL(REFRESH_DURATION_SEC, CASE WHEN REFRESH_START_TIME IS NULL THEN NULL ELSE TIMESTAMPDIFF(MICROSECOND, REFRESH_START_TIME, NOW(6)) / 1000000.0 END),
\tREFRESH_FAILED_REASON = IFNULL(REFRESH_FAILED_REASON, '{MV_REFRESH_HISTORY_ORPHANED_REASON}')
WHERE REFRESH_STATUS = 'running'
  AND COALESCE(LAST_HEARTBEAT_TIME, REFRESH_START_TIME) < %?
ORDER BY REFRESH_JOB_ID
LIMIT {HISTORY_GC_DELETE_BATCH_SIZE}"
    )
}

/// Go `buildMarkLogPurgeHistoryRunningRowsOrphanedSQL`.
#[must_use]
pub fn build_mark_log_purge_history_running_rows_orphaned_sql() -> String {
    format!(
        "UPDATE mysql.tidb_mlog_purge_hist FORCE INDEX (idx_purge_status_start_time)
SET PURGE_STATUS = '{MV_HISTORY_ORPHANED_STATUS}',
\tPURGE_END_TIME = IFNULL(PURGE_END_TIME, NOW(6)),
\tPURGE_DURATION_SEC = IFNULL(PURGE_DURATION_SEC, CASE WHEN PURGE_START_TIME IS NULL THEN NULL ELSE TIMESTAMPDIFF(MICROSECOND, PURGE_START_TIME, NOW(6)) / 1000000.0 END),
\tPURGE_FAILED_REASON = IFNULL(PURGE_FAILED_REASON, '{MV_LOG_PURGE_HISTORY_ORPHANED_REASON}')
WHERE PURGE_STATUS = 'running'
  AND COALESCE(LAST_HEARTBEAT_TIME, PURGE_START_TIME) < %?
ORDER BY PURGE_JOB_ID
LIMIT {HISTORY_GC_DELETE_BATCH_SIZE}"
    )
}

/// Go `buildDeleteRefreshHistoryByCutoffTSOSQL`.
#[must_use]
pub fn build_delete_refresh_history_by_cutoff_tso_sql() -> String {
    format!("DELETE FROM mysql.tidb_mview_refresh_hist WHERE (REFRESH_STATUS IS NULL OR REFRESH_STATUS <> 'running') AND REFRESH_JOB_ID < %? ORDER BY REFRESH_JOB_ID LIMIT {HISTORY_GC_DELETE_BATCH_SIZE}")
}

/// Go `buildDeleteLogPurgeHistoryByCutoffTSOSQL`.
#[must_use]
pub fn build_delete_log_purge_history_by_cutoff_tso_sql() -> String {
    format!("DELETE FROM mysql.tidb_mlog_purge_hist WHERE (PURGE_STATUS IS NULL OR PURGE_STATUS <> 'running') AND PURGE_JOB_ID < %? ORDER BY PURGE_JOB_ID LIMIT {HISTORY_GC_DELETE_BATCH_SIZE}")
}

/// Go `buildDeleteRefreshHistoryByCountLimitSQL`.
#[must_use]
pub fn build_delete_refresh_history_by_count_limit_sql(limit: u64) -> String {
    format!("DELETE FROM mysql.tidb_mview_refresh_hist WHERE (REFRESH_STATUS IS NULL OR REFRESH_STATUS <> 'running') AND REFRESH_JOB_ID >= %? ORDER BY REFRESH_JOB_ID LIMIT {limit}")
}

/// Go `buildDeleteLogPurgeHistoryByCountLimitSQL`.
#[must_use]
pub fn build_delete_log_purge_history_by_count_limit_sql(limit: u64) -> String {
    format!("DELETE FROM mysql.tidb_mlog_purge_hist WHERE (PURGE_STATUS IS NULL OR PURGE_STATUS <> 'running') AND PURGE_JOB_ID >= %? ORDER BY PURGE_JOB_ID LIMIT {limit}")
}

/// Go `resolveMVIdentityByID`: the view's schema and name (lowercase) and
/// its alert thresholds.
fn resolve_mv_identity_by_id(
    sctx: &dyn MVServiceSessionContext,
    mview_id: i64,
) -> Result<Option<(String, String, i64, i64)>, MvsError> {
    if mview_id <= 0 {
        return Err(MvsError::Other("mview id is invalid".to_owned()));
    }
    let Some(meta) = sctx.table_info_by_id(mview_id) else {
        return Ok(None);
    };
    let (warning, overdue) = meta
        .materialized_view
        .as_ref()
        .map_or((0, 0), |info| (info.read().alert_warning_sec.max(0), info.read().alert_overdue_sec.max(0)));
    let invalid = || MvsError::Other("mview metadata is invalid".to_owned());
    let schema_name = sctx.schema_name_by_id(meta.db_id).ok_or_else(invalid)?;
    let mview_name = meta.name.lowercase().to_owned();
    if schema_name.is_empty() || mview_name.is_empty() {
        return Err(invalid());
    }
    Ok(Some((schema_name, mview_name, warning, overdue)))
}

/// Go `resolveMVLogMetaByID`.
fn resolve_mv_log_meta_by_id(sctx: &dyn MVServiceSessionContext, mlog_id: i64) -> Result<Option<TableInfo>, MvsError> {
    if mlog_id <= 0 {
        return Err(MvsError::Other("materialized view log id is invalid".to_owned()));
    }
    Ok(sctx.table_info_by_id(mlog_id).filter(|meta| meta.materialized_view_log.is_some()))
}

/// Go `resolveMVLogIdentityFromMeta`.
fn resolve_mv_log_identity_from_meta(sctx: &dyn MVServiceSessionContext, meta: &TableInfo) -> Option<(String, String)> {
    let schema_name = sctx.schema_name_by_id(meta.db_id)?;
    let mlog_name = meta.name.lowercase().to_owned();
    if schema_name.is_empty() || mlog_name.is_empty() {
        return None;
    }
    Some((schema_name, mlog_name))
}

/// Go `resolveMVLogIdentityByID`.
pub fn resolve_mv_log_identity_by_id(
    sctx: &dyn MVServiceSessionContext,
    mlog_id: i64,
) -> Result<Option<(String, String)>, MvsError> {
    Ok(resolve_mv_log_meta_by_id(sctx, mlog_id)?.and_then(|meta| resolve_mv_log_identity_from_meta(sctx, &meta)))
}

/// Go `getGlobalSystemVarBestEffort`.
pub(crate) fn get_global_system_var_best_effort(sctx: &dyn MVServiceSessionContext, name: &str) -> Option<String> {
    match sctx.global_system_var(name) {
        Ok(value) => Some(value),
        Err(err) => {
            bg_logger().warn(
                "mv service: failed to read global session var, fallback to current session value",
                &[Field::new("var", Value::Str(name.to_owned())), Field::new("error", Value::Str(err.to_string()))],
            );
            None
        }
    }
}

/// Go `applyMVMaintenanceSessionVarBestEffort`: sets `name` to `target`
/// when it differs from `origin`; the restore puts `origin` back.
fn apply_session_var_best_effort(
    sctx: &dyn MVServiceSessionContext,
    name: &str,
    origin: String,
    target: String,
    restores: &mut Vec<(String, String)>,
) {
    if origin == target {
        return;
    }
    match sctx.set_system_var(name, &target) {
        Ok(()) => restores.push((name.to_owned(), origin)),
        Err(err) => bg_logger().warn(
            "mv service: failed to apply session var from global setting, fallback to current session value",
            &[
                Field::new("var", Value::Str(name.to_owned())),
                Field::new("value", Value::Str(target)),
                Field::new("error", Value::Str(err.to_string())),
            ],
        ),
    }
}

fn restore_session_vars(sctx: &dyn MVServiceSessionContext, restores: Vec<(String, String)>) {
    for (name, origin) in restores.into_iter().rev() {
        if let Err(err) = sctx.set_system_var(&name, &origin) {
            bg_logger().warn(
                "mv service: failed to restore session var after maintenance",
                &[
                    Field::new("var", Value::Str(name)),
                    Field::new("origin", Value::Str(origin)),
                    Field::new("error", Value::Str(err.to_string())),
                ],
            );
        }
    }
}

/// Go `applyMVRefreshSessionVarsFromGlobal` (via Go
/// `ApplyMViewExecutionSessionVarsWithConfig` best effort): the refresh's
/// execution variables from their GLOBAL values; the returned list
/// restores the session.
fn apply_mv_refresh_session_vars_from_global(sctx: &dyn MVServiceSessionContext) -> Vec<(String, String)> {
    use tidb_vardef::tidb_vars as v;
    let mut restores = Vec::new();
    for name in [
        v::TIDB_MVIEW_MAINTAIN_MEM_QUOTA,
        v::TIDB_MAX_TIFLASH_THREADS,
        v::TIDB_MAX_BYTES_BEFORE_TIFLASH_EXTERNAL_JOIN,
        v::TIDB_MAX_BYTES_BEFORE_TIFLASH_EXTERNAL_GROUP_BY,
        v::TIDB_MAX_BYTES_BEFORE_TIFLASH_EXTERNAL_SORT,
        v::TIFLASH_MEM_QUOTA_QUERY_PER_NODE,
        v::TIFLASH_QUERY_SPILL_RATIO,
        v::TIFLASH_FINE_GRAINED_SHUFFLE_STREAM_COUNT,
        v::TIFLASH_FINE_GRAINED_SHUFFLE_BATCH_SIZE,
        v::TIDB_MVIEW_MAINTAIN_IMPORT_THREADS,
        v::TIDB_MVIEW_MAINTAIN_IMPORT_DISK_QUOTA,
        v::TIDB_MVIEW_MAINTAIN_ISOLATION_READ_ENGINES,
    ] {
        let Some(target) = get_global_system_var_best_effort(sctx, name) else {
            continue;
        };
        let origin = sctx.system_var(name).unwrap_or_default();
        apply_session_var_best_effort(sctx, name, origin, target, &mut restores);
    }
    restores
}

/// Go `applyMVMaintenanceSessionVarsFromGlobal`: the purge's maintenance
/// variables from their GLOBAL values.
fn apply_mv_maintenance_session_vars_from_global(sctx: &dyn MVServiceSessionContext) -> Vec<(String, String)> {
    use tidb_vardef::tidb_vars as v;
    let mut restores = Vec::new();
    for name in [
        v::TIDB_MVIEW_MAINTAIN_MEM_QUOTA,
        v::TIDB_MVIEW_MAINTAIN_ISOLATION_READ_ENGINES,
        v::TIDB_MLOG_PURGE_BATCH_SIZE,
        v::TIDB_MLOG_PURGE_MIN_RATE,
        v::TIDB_MLOG_PURGE_RATE_BUDGET_RATIO,
        v::TIDB_MLOG_PURGE_DELETE_TIFLASH_THREADS,
    ] {
        let Some(target) = get_global_system_var_best_effort(sctx, name) else {
            continue;
        };
        let origin = sctx.system_var(name).unwrap_or_default();
        apply_session_var_best_effort(sctx, name, origin, target, &mut restores);
    }
    restores
}

impl ServiceHelper {
    /// Go `&serviceHelper{}`: no server view, metrics to a test sink.
    #[cfg(test)]
    pub(crate) fn new_for_test() -> Self {
        Self {
            local_server: Box::new(|| Ok(ServerInfo::default())),
            all_servers: Box::new(|| Ok(Vec::new())),
            analyzer: None,
            metrics: Arc::new(crate::mvservice_test::MockMVServiceHelper::default()),
        }
    }

    fn run_with_restores<T>(
        sctx: &dyn MVServiceSessionContext,
        restores: Vec<(String, String)>,
        f: impl FnOnce() -> Result<T, MvsError>,
    ) -> Result<T, MvsError> {
        let result = f();
        restore_session_vars(sctx, restores);
        result
    }
}

impl ServerDiscovery for ServiceHelper {
    fn server_filter(&self, _: &ServerInfo) -> bool {
        true
    }

    fn get_server_info(&self) -> Result<ServerInfo, String> {
        (self.local_server)()
    }

    fn get_all_server_info(&self, _: &Context) -> Result<HashMap<String, ServerInfo>, String> {
        Ok(latest_server_infos_by_instance((self.all_servers)()?))
    }
}

/// The rows' first column as a next schedule time: `None` for no row or NULL.
fn next_time_of(rows: &[Vec<Datum>]) -> GoTime {
    rows.first().and_then(|row| row.first()).and_then(datum_i64).map_or_else(GoTime::zero, |sec| mvs_unix(sec, 0))
}

/// Go `deriveMaterializedScheduleNextTimeForManualCancel`.
fn derive_schedule_next_for_manual_cancel(
    sctx: &dyn MVServiceSessionContext,
    next_expr: &str,
    schedule_sql_mode: i64,
) -> Result<(Option<GoTime>, bool), MvsError> {
    // Go `DeriveMaterializedScheduleNextTime` refreshes the statement time
    // with `SELECT NOW(6)` before evaluating NOW() in the NEXT expression.
    if next_expr.trim().is_empty() {
        return Ok((None, true));
    }
    sctx.exec_restricted_sql("SELECT NOW(6)", &[])?;
    let (next, should_update) = tidb_ddl_build::materialized_view::derive_materialized_schedule_next_unix_seconds(
        "",
        next_expr,
        schedule_sql_mode,
        sctx.statement_now(),
    )
    .map_err(|err| MvsError::Other(err.to_string()))?;
    if !should_update {
        return Ok((None, false));
    }
    Ok((next.map(|sec| mvs_unix(sec, 0)), true))
}

/// Go `tryBackoffMVTaskManualCancel`.
pub(crate) fn try_backoff_mv_task_manual_cancel(
    pool: &dyn SessionPool,
    lock_sql: &str,
    update_sql: &str,
    object_id: i64,
    next_time: &GoTime,
    resolve_expected_next: fn(&dyn MVServiceSessionContext, i64) -> Result<(Option<GoTime>, bool), MvsError>,
) -> (bool, GoTime, Option<MvsError>) {
    if object_id <= 0 {
        return (false, GoTime::zero(), Some(MvsError::Other("mv service manual cancel backoff target id is invalid".to_owned())));
    }
    if next_time.is_zero() {
        return (false, GoTime::zero(), Some(MvsError::Other("mv service manual cancel backoff target time is invalid".to_owned())));
    }
    let result = with_session(pool, |sctx| {
        sctx.execute_internal("BEGIN PESSIMISTIC")?;
        let outcome = (|| -> Result<(bool, GoTime, bool), MvsError> {
            let rows = match sctx.exec_restricted_sql(lock_sql, &[SqlArg::Signed(object_id)]) {
                Ok(rows) => rows,
                Err(MvsError::Sql { code: ER_LOCK_NOWAIT, .. }) => return Ok((false, GoTime::zero(), false)),
                Err(err) => return Err(err),
            };
            let Some(row) = rows.first() else {
                return Ok((false, GoTime::zero(), false));
            };
            if matches!(row.first(), None | Some(Datum::Null)) {
                sctx.execute_internal("COMMIT")?;
                return Ok((true, GoTime::zero(), true));
            }
            let mut applied_next = next_time.clone();
            let mut applied_next_arg = SqlArg::Signed(next_time.unix());
            let mut clear_applied_next = false;
            // If current metadata cannot provide a better schedule, keep the cooldown fallback
            // instead of dropping the persisted backoff on the floor.
            match resolve_expected_next(sctx, object_id) {
                Ok((resolved, true)) => match resolved {
                    None => {
                        applied_next_arg = SqlArg::Null;
                        clear_applied_next = true;
                    }
                    Some(resolved) => {
                        if !resolved.before(next_time) {
                            applied_next = resolved;
                        }
                        applied_next_arg = SqlArg::Signed(applied_next.unix());
                    }
                },
                Ok((_, false)) => {}
                Err(err) => bg_logger().warn(
                    "derive materialized view task next schedule after manual cancel failed, fallback to cooldown",
                    &[Field::new("object_id", Value::I64(object_id)), Field::new("error", Value::Str(err.to_string()))],
                ),
            }
            exec_rc(sctx, update_sql, &[applied_next_arg, SqlArg::Signed(object_id)])?;
            sctx.execute_internal("COMMIT")?;
            if clear_applied_next {
                return Ok((true, GoTime::zero(), true));
            }
            Ok((true, applied_next.in_location(next_time.location()), true))
        })();
        match outcome {
            Ok((applied, next, committed)) => {
                if !committed {
                    let _ = sctx.execute_internal("ROLLBACK");
                }
                Ok((applied, next))
            }
            Err(err) => {
                let _ = sctx.execute_internal("ROLLBACK");
                Err(err)
            }
        }
    });
    match result {
        Ok((applied, next)) => (applied, next, None),
        Err(err) => (false, GoTime::zero(), Some(err)),
    }
}

/// Go `deriveMVRefreshManualCancelNextTime`.
fn derive_mv_refresh_manual_cancel_next_time(
    sctx: &dyn MVServiceSessionContext,
    mview_id: i64,
) -> Result<(Option<GoTime>, bool), MvsError> {
    let Some(meta) = sctx.table_info_by_id(mview_id) else {
        return Ok((None, false));
    };
    let Some(info) = meta.materialized_view.as_ref().map(|info| info.read().clone()) else {
        return Err(MvsError::Other("materialized view metadata is invalid".to_owned()));
    };
    let (next, should_update) = derive_schedule_next_for_manual_cancel(sctx, &info.refresh_next, info.refresh_schedule_sql_mode)?;
    if should_update && next.is_none() && !info.refresh_next.trim().is_empty() {
        bg_logger().error(
            "refresh MV manual cancel backoff: automatic refresh schedule disabled because NEXT expression evaluated to NULL, updating NEXT_REFRESH_UNIX_SECONDS to NULL",
            &[
                Field::new("schemaName", Value::Str(sctx.schema_name_by_id(meta.db_id).unwrap_or_default())),
                Field::new("tableName", Value::Str(meta.name.original().to_owned())),
                Field::new("refreshNext", Value::Str(info.refresh_next.clone())),
            ],
        );
    }
    Ok((next, should_update))
}

/// Go `deriveMLogPurgeManualCancelNextTime`.
fn derive_mlog_purge_manual_cancel_next_time(
    sctx: &dyn MVServiceSessionContext,
    mlog_id: i64,
) -> Result<(Option<GoTime>, bool), MvsError> {
    let Some(meta) = sctx.table_info_by_id(mlog_id) else {
        return Ok((None, false));
    };
    let Some(info) = meta.materialized_view_log.as_ref().map(|info| info.read().clone()) else {
        return Err(MvsError::Other("materialized view log metadata is invalid".to_owned()));
    };
    let (next, should_update) = derive_schedule_next_for_manual_cancel(sctx, &info.purge_next, info.purge_schedule_sql_mode)?;
    if should_update && next.is_none() && !info.purge_next.trim().is_empty() {
        bg_logger().error(
            "purge MV log manual cancel backoff: automatic purge schedule disabled because NEXT expression evaluated to NULL, updating NEXT_PURGE_UNIX_SECONDS to NULL",
            &[
                Field::new("schemaName", Value::Str(sctx.schema_name_by_id(meta.db_id).unwrap_or_default())),
                Field::new("tableName", Value::Str(meta.name.original().to_owned())),
                Field::new("purgeNext", Value::Str(info.purge_next.clone())),
            ],
        );
    }
    Ok((next, should_update))
}

/// Go `reconcileMVHistoryRunningRowsInBatches` / `purgeMVHistoryByCutoffTSOInBatches`.
fn exec_in_batches(sctx: &dyn MVServiceSessionContext, sql: &str, arg: SqlArg<'_>) -> Result<(), MvsError> {
    loop {
        exec_rc(sctx, sql, std::slice::from_ref(&arg))?;
        if sctx.affected_rows() < HISTORY_GC_DELETE_BATCH_SIZE {
            return Ok(());
        }
    }
}

/// Go `purgeMVHistoryByCountLimitInBatches`.
fn purge_mv_history_by_count_limit_in_batches(
    sctx: &dyn MVServiceSessionContext,
    count_sql: &str,
    delete_sql: fn(u64) -> String,
    max_records: u64,
) -> Result<(), MvsError> {
    if max_records == 0 {
        return Ok(());
    }
    let rows = exec_rc(sctx, count_sql, &[])?;
    let Some(row) = rows.first() else {
        return Ok(());
    };
    let Some(total_count) = row.first().and_then(datum_i64) else {
        return Ok(());
    };
    if total_count <= 0 {
        return Ok(());
    }
    let Some(min_job_id) = row.get(1).and_then(datum_u64) else {
        return Ok(());
    };
    let total_count = total_count as u64;
    if total_count <= max_records {
        return Ok(());
    }
    let mut remaining_delete = total_count - max_records;
    while remaining_delete > 0 {
        let batch_size = remaining_delete.min(HISTORY_GC_DELETE_BATCH_SIZE);
        exec_rc(sctx, &delete_sql(batch_size), &[SqlArg::Unsigned(min_job_id)])?;
        let affected_rows = sctx.affected_rows();
        if affected_rows == 0 || affected_rows < batch_size {
            return Ok(());
        }
        remaining_delete -= affected_rows;
    }
    Ok(())
}

impl MVTaskHandler for ServiceHelper {
    /// Go `RefreshMV`: one `REFRESH MATERIALIZED VIEW ... FAST` and the
    /// view's next refresh time; a zero time stops scheduling it.
    fn refresh_mv(&self, _: &Context, pool: &dyn SessionPool, mview_id: i64) -> Result<GoTime, MvsError> {
        const REFRESH_MV_SQL: &str = "REFRESH MATERIALIZED VIEW %n.%n FAST";
        const FIND_NEXT_TIME_SQL: &str = "SELECT NEXT_REFRESH_UNIX_SECONDS FROM mysql.tidb_mview_refresh_info WHERE MVIEW_ID = %? AND NEXT_REFRESH_UNIX_SECONDS IS NOT NULL";
        let start_at = mvs_now();
        let mut names = (String::new(), String::new());
        let result = with_session(pool, |sctx| {
            let Some((schema_name, mview_name, _, _)) = resolve_mv_identity_by_id(sctx, mview_id)? else {
                return Ok(GoTime::zero());
            };
            names = (schema_name.clone(), mview_name.clone());
            let restores = apply_mv_refresh_session_vars_from_global(sctx);
            Self::run_with_restores(sctx, restores, || {
                if let Err(err) = exec_rc(
                    sctx,
                    REFRESH_MV_SQL,
                    &[SqlArg::String(schema_name.as_bytes()), SqlArg::String(mview_name.as_bytes())],
                ) {
                    if is_mv_task_canceled_manually(&err) {
                        return Err(err_mv_task_canceled_manually());
                    }
                    return Err(err);
                }
                Ok(next_time_of(&exec_rc(sctx, FIND_NEXT_TIME_SQL, &[SqlArg::Signed(mview_id)])?))
            })
        });
        if let Err(err) = &result {
            bg_logger().warn(
                "refresh materialized view failed",
                &[
                    Field::new("mview_id", Value::I64(mview_id)),
                    Field::new("schema", Value::Str(names.0)),
                    Field::new("mview", Value::Str(names.1)),
                    Field::new("elapsed", Value::Duration(mvs_since(&start_at))),
                    Field::new("error", Value::Str(err.to_string())),
                ],
            );
        }
        result
    }

    /// Go `PurgeMVLog`: one `PURGE MATERIALIZED VIEW LOG ON <base>` and the
    /// log's next purge time.
    fn purge_mv_log(&self, _: &Context, pool: &dyn SessionPool, mlog_id: i64) -> Result<GoTime, MvsError> {
        const PURGE_MV_LOG_SQL: &str = "PURGE MATERIALIZED VIEW LOG ON %n.%n";
        const FIND_NEXT_TIME_SQL: &str = "SELECT NEXT_PURGE_UNIX_SECONDS FROM mysql.tidb_mlog_purge_info WHERE MLOG_ID = %? AND NEXT_PURGE_UNIX_SECONDS IS NOT NULL";
        let start_at = mvs_now();
        let mut names = (String::new(), String::new());
        let result = with_session(pool, |sctx| {
            if mlog_id <= 0 {
                return Err(MvsError::Other("materialized view log id is invalid".to_owned()));
            }
            let Some(mlog_meta) = sctx.table_info_by_id(mlog_id) else {
                return Ok(GoTime::zero());
            };
            let invalid = || MvsError::Other("materialized view log metadata is invalid".to_owned());
            let base_table_id = mlog_meta.materialized_view_log.as_ref().ok_or_else(invalid)?.read().base_table_id;
            if base_table_id <= 0 {
                return Err(invalid());
            }
            let base_meta = sctx
                .table_info_by_id(base_table_id)
                .ok_or_else(|| MvsError::Other("materialized view base table not found".to_owned()))?;
            let base_table = base_meta.name.lowercase().to_owned();
            if base_table.is_empty() {
                return Err(MvsError::Other("materialized view base table name is empty".to_owned()));
            }
            names.1 = base_table.clone();
            let base_schema = sctx.schema_name_by_id(base_meta.db_id).unwrap_or_default();
            names.0 = base_schema.clone();
            if base_schema.is_empty() {
                return Err(MvsError::Other("materialized view base table schema name is empty".to_owned()));
            }
            let restores = apply_mv_maintenance_session_vars_from_global(sctx);
            Self::run_with_restores(sctx, restores, || {
                if let Err(err) = exec_rc(
                    sctx,
                    PURGE_MV_LOG_SQL,
                    &[SqlArg::String(base_schema.as_bytes()), SqlArg::String(base_table.as_bytes())],
                ) {
                    if is_mv_task_canceled_manually(&err) {
                        return Err(err_mv_task_canceled_manually());
                    }
                    return Err(err);
                }
                Ok(next_time_of(&exec_rc(sctx, FIND_NEXT_TIME_SQL, &[SqlArg::Signed(mlog_id)])?))
            })
        });
        if let Err(err) = &result {
            bg_logger().warn(
                "purge materialized view log failed",
                &[
                    Field::new("mvlog_id", Value::I64(mlog_id)),
                    Field::new("base_table_schema", Value::Str(names.0)),
                    Field::new("base_table_name", Value::Str(names.1)),
                    Field::new("elapsed", Value::Duration(mvs_since(&start_at))),
                    Field::new("error", Value::Str(err.to_string())),
                ],
            );
        }
        result
    }

    fn try_backoff_refresh_manual_cancel(
        &self,
        _: &Context,
        pool: &dyn SessionPool,
        mview_id: i64,
        next_refresh: &GoTime,
    ) -> (bool, GoTime, Option<MvsError>) {
        try_backoff_mv_task_manual_cancel(
            pool,
            "SELECT NEXT_REFRESH_UNIX_SECONDS FROM mysql.tidb_mview_refresh_info WHERE MVIEW_ID = %? FOR UPDATE NOWAIT",
            "UPDATE mysql.tidb_mview_refresh_info SET NEXT_REFRESH_UNIX_SECONDS = %? WHERE MVIEW_ID = %?",
            mview_id,
            next_refresh,
            derive_mv_refresh_manual_cancel_next_time,
        )
    }

    fn try_backoff_purge_manual_cancel(
        &self,
        _: &Context,
        pool: &dyn SessionPool,
        mlog_id: i64,
        next_purge: &GoTime,
    ) -> (bool, GoTime, Option<MvsError>) {
        try_backoff_mv_task_manual_cancel(
            pool,
            "SELECT NEXT_PURGE_UNIX_SECONDS FROM mysql.tidb_mlog_purge_info WHERE MLOG_ID = %? FOR UPDATE NOWAIT",
            "UPDATE mysql.tidb_mlog_purge_info SET NEXT_PURGE_UNIX_SECONDS = %? WHERE MLOG_ID = %?",
            mlog_id,
            next_purge,
            derive_mlog_purge_manual_cancel_next_time,
        )
    }

    fn sync_mv_refresh_alert_states(
        &self,
        _: &Context,
        pool: &dyn SessionPool,
        updated_at: &GoTime,
        states: &[RefreshAlertTask],
    ) -> Result<(), MvsError> {
        if states.is_empty() {
            return Ok(());
        }
        with_session(pool, |sctx| {
            let mut resolved_ids = Vec::with_capacity(states.len());
            let mut upsert_states = Vec::with_capacity(states.len());
            for state in states {
                if state.mview_id <= 0 || state.metadata_unresolved {
                    continue;
                }
                if state.alert_level.is_empty() {
                    resolved_ids.push(state.mview_id);
                    continue;
                }
                upsert_states.push(state.clone());
            }
            for ids in resolved_ids.chunks(REFRESH_ALERT_WRITE_BATCH_SIZE) {
                exec_rc(sctx, &build_delete_resolved_mv_refresh_alert_sql(ids), &[])?;
                exec_rc(sctx, &build_clear_resolved_mv_refresh_alert_level_sql(updated_at, ids), &[])?;
            }
            for chunk in upsert_states.chunks(REFRESH_ALERT_WRITE_BATCH_SIZE) {
                exec_rc(sctx, &build_upsert_mv_refresh_alert_sql(updated_at, chunk), &[])?;
            }
            Ok(())
        })
    }

    fn cleanup_stale_mv_refresh_alerts(&self, _: &Context, pool: &dyn SessionPool) -> Result<(), MvsError> {
        with_session(pool, |sctx| {
            exec_rc(sctx, build_clear_disabled_mv_refresh_alert_level_sql(), &[])?;
            exec_rc(sctx, build_cleanup_stale_mv_refresh_alert_sql(), &[])?;
            Ok(())
        })
    }

    fn load_all_tidb_mv_log_purge(&self, _: &Context, pool: &dyn SessionPool) -> Result<HashMap<i64, Arc<MlogPurgeTask>>, MvsError> {
        const SQL: &str = "SELECT NEXT_PURGE_UNIX_SECONDS, MLOG_ID FROM mysql.tidb_mlog_purge_info WHERE NEXT_PURGE_UNIX_SECONDS IS NOT NULL";
        let rows = with_session(pool, |sctx| exec_rc(sctx, SQL, &[]))?;
        let mut new_pending = HashMap::with_capacity(rows.len());
        for row in rows {
            let (Some(next), Some(mlog_id)) = (row.first().and_then(datum_i64), row.get(1).and_then(datum_i64)) else {
                continue;
            };
            if mlog_id <= 0 {
                continue;
            }
            let next_purge = mvs_unix(next, 0);
            let order_ts = crate::time_proxy::unix_milli(&next_purge);
            new_pending.insert(mlog_id, MlogPurgeTask::new(mlog_id, next_purge, order_ts));
        }
        Ok(new_pending)
    }

    fn load_all_tidb_mv_log_accumulation_tasks(
        &self,
        _: &Context,
        pool: &dyn SessionPool,
    ) -> Result<HashMap<i64, MlogAccumulationTask>, MvsError> {
        const FETCH_SQL: &str = "SELECT MLOG_ID FROM mysql.tidb_mlog_purge_info";
        with_session(pool, |sctx| {
            let rows = exec_rc(sctx, FETCH_SQL, &[])?;
            let mut tasks = HashMap::new();
            for row in rows {
                let Some(mlog_id) = row.first().and_then(datum_i64) else {
                    continue;
                };
                if mlog_id <= 0 {
                    continue;
                }
                // Go `resolveMVLogAccumulationAlertByID`.
                let Some(meta) = resolve_mv_log_meta_by_id(sctx, mlog_id)? else {
                    continue;
                };
                let info = meta.materialized_view_log.as_ref().expect("a log").read().clone();
                let (alert_rows, enabled) = info.effective_log_accumulation_alert_rows();
                if !enabled {
                    continue;
                }
                let Some((schema_name, mlog_name)) = resolve_mv_log_identity_from_meta(sctx, &meta) else {
                    continue;
                };
                tasks.insert(mlog_id, MlogAccumulationTask { schema_name, mlog_name, alert_rows });
            }
            Ok(tasks)
        })
    }

    fn load_tidb_mv_log_accumulation_row_counts(
        &self,
        _: &Context,
        pool: &dyn SessionPool,
        tasks: &HashMap<i64, MlogAccumulationTask>,
    ) -> Result<HashMap<i64, u64>, MvsError> {
        const COUNT_SQL: &str = "SELECT /*+ read_from_storage(tiflash[%n.%n]) */ COUNT(*) FROM %n.%n";
        if tasks.is_empty() {
            return Ok(HashMap::new());
        }
        with_session(pool, |sctx| {
            let concurrency = tidb_vardef::tidb_vars::TIDB_DIST_SQL_SCAN_CONCURRENCY;
            let origin = sctx.system_var(concurrency).unwrap_or_default();
            let mut restores = Vec::new();
            apply_session_var_best_effort(sctx, concurrency, origin, "1".to_owned(), &mut restores);
            Self::run_with_restores(sctx, restores, || {
                let mut row_counts = HashMap::with_capacity(tasks.len());
                for (&mlog_id, task) in tasks {
                    let (schema, name) = (task.schema_name.as_bytes(), task.mlog_name.as_bytes());
                    let count_rows = match exec_rc(
                        sctx,
                        COUNT_SQL,
                        &[SqlArg::String(schema), SqlArg::String(name), SqlArg::String(schema), SqlArg::String(name)],
                    ) {
                        Ok(rows) => rows,
                        Err(MvsError::Sql { code: ER_NO_SUCH_TABLE, .. }) => continue,
                        Err(err) => return Err(err),
                    };
                    if count_rows.len() != 1 {
                        return Err(MvsError::Other(format!(
                            "unexpected COUNT(*) result length for mvlog accumulation: {}",
                            count_rows.len()
                        )));
                    }
                    let Some(row_count) = count_rows[0].first().and_then(datum_i64) else {
                        return Err(MvsError::Other("unexpected NULL COUNT(*) result for mvlog accumulation".to_owned()));
                    };
                    if row_count < 0 {
                        return Err(MvsError::Other(format!(
                            "unexpected negative COUNT(*) result for mvlog accumulation: {row_count}"
                        )));
                    }
                    row_counts.insert(mlog_id, row_count as u64);
                }
                Ok(row_counts)
            })
        })
    }

    fn load_all_tidb_mv_log_analyze_tasks(&self, ctx: &Context, _: &dyn SessionPool) -> Result<HashMap<i64, MlogAnalyzeTask>, MvsError> {
        match &self.analyzer {
            Some(analyzer) => analyzer.load_all_tidb_mv_log_analyze_tasks(ctx),
            None => Err(MvsError::Other("mv service stats handle getter is nil".to_owned())),
        }
    }

    fn analyze_mv_log(&self, ctx: &Context, _: &dyn SessionPool, mlog_id: i64) -> Result<(), MvsError> {
        match &self.analyzer {
            Some(analyzer) => analyzer.analyze_mv_log(ctx, mlog_id),
            None => Err(MvsError::Other("mv service stats handle getter is nil".to_owned())),
        }
    }

    fn load_all_tidb_mv_refresh(&self, _: &Context, pool: &dyn SessionPool) -> Result<HashMap<i64, Arc<MViewTask>>, MvsError> {
        const SQL: &str = "SELECT NEXT_REFRESH_UNIX_SECONDS, MVIEW_ID, LAST_SUCCESS_READ_TSO FROM mysql.tidb_mview_refresh_info WHERE NEXT_REFRESH_UNIX_SECONDS IS NOT NULL";
        with_session(pool, |sctx| {
            let rows = exec_rc(sctx, SQL, &[])?;
            let mut new_pending = HashMap::with_capacity(rows.len());
            for row in rows {
                let (Some(next), Some(mview_id)) = (row.first().and_then(datum_i64), row.get(1).and_then(datum_i64)) else {
                    continue;
                };
                if mview_id <= 0 {
                    continue;
                }
                let mut state = MViewTaskState { next_refresh: mvs_unix(next, 0), ..MViewTaskState::default() };
                if let Some(tso) = row.get(2).and_then(datum_u64).filter(|tso| *tso > 0) {
                    state.last_success_read_tso = tso;
                    state.last_success_time = mvs_unix_milli(extract_physical(tso));
                }
                match resolve_mv_identity_by_id(sctx, mview_id) {
                    Ok(Some((schema_name, mview_name, warning, overdue))) => {
                        state.schema_name = schema_name;
                        state.mview_name = mview_name;
                        state.alert_warning_sec = warning;
                        state.alert_overdue_sec = overdue;
                    }
                    _ => state.metadata_unresolved = true,
                }
                let order_ts = crate::time_proxy::unix_milli(&state.next_refresh);
                new_pending.insert(mview_id, MViewTask::new(mview_id, state, order_ts));
            }
            Ok(new_pending)
        })
    }

    fn get_current_tso(&self, _: &Context, pool: &dyn SessionPool) -> Result<u64, MvsError> {
        with_session(pool, |sctx| {
            let version = sctx.store_current_version()?;
            if version == 0 {
                return Err(MvsError::Other("get current tso failed: invalid version".to_owned()));
            }
            Ok(version)
        })
    }

    fn purge_mv_history_before_tso(
        &self,
        _: &Context,
        pool: &dyn SessionPool,
        current_tso: u64,
        mview_refresh_retention: i64,
        mlog_purge_retention: i64,
    ) -> Result<(), MvsError> {
        let calc_cutoff_tso = |retention: i64| {
            if retention <= 0 {
                return current_tso;
            }
            compose_ts((extract_physical(current_tso) - retention / 1_000_000).max(0))
        };
        let heartbeat_cutoff = {
            let physical = (extract_physical(current_tso) - MV_HISTORY_HEARTBEAT_TIMEOUT / 1_000_000).max(0);
            // Go `oracle.GetTimeFromTS`: the local wall clock of the physical part.
            mvs_unix_milli(physical)
        };
        with_session(pool, |sctx| {
            let mut purge_errs = Vec::new();
            let heartbeat = time_arg(&heartbeat_cutoff);
            if let Err(err) = exec_in_batches(sctx, &build_mark_refresh_history_running_rows_orphaned_sql(), heartbeat.clone()) {
                purge_errs.push(format!("reconcile stale mview refresh history running rows failed: {err}"));
            }
            if let Err(err) = exec_in_batches(
                sctx,
                &build_delete_refresh_history_by_cutoff_tso_sql(),
                SqlArg::Unsigned(calc_cutoff_tso(mview_refresh_retention)),
            ) {
                purge_errs.push(format!("purge mview refresh history by retention failed: {err}"));
            }
            if let Err(err) = purge_mv_history_by_count_limit_in_batches(
                sctx,
                COUNT_REFRESH_HISTORY_SQL,
                build_delete_refresh_history_by_count_limit_sql,
                DEFAULT_MV_HISTORY_GC_MAX_RECORDS,
            ) {
                purge_errs.push(format!("purge mview refresh history by count limit failed: {err}"));
            }
            if let Err(err) = exec_in_batches(sctx, &build_mark_log_purge_history_running_rows_orphaned_sql(), heartbeat.clone()) {
                purge_errs.push(format!("reconcile stale mlog purge history running rows failed: {err}"));
            }
            if let Err(err) = exec_in_batches(
                sctx,
                &build_delete_log_purge_history_by_cutoff_tso_sql(),
                SqlArg::Unsigned(calc_cutoff_tso(mlog_purge_retention)),
            ) {
                purge_errs.push(format!("purge mlog purge history by retention failed: {err}"));
            }
            if let Err(err) = purge_mv_history_by_count_limit_in_batches(
                sctx,
                COUNT_LOG_PURGE_HISTORY_SQL,
                build_delete_log_purge_history_by_count_limit_sql,
                DEFAULT_MV_HISTORY_GC_MAX_RECORDS,
            ) {
                purge_errs.push(format!("purge mlog purge history by count limit failed: {err}"));
            }
            if purge_errs.is_empty() {
                return Ok(());
            }
            // Go `errors.Join`: the messages, one per line.
            Err(MvsError::Other(purge_errs.join("\n")))
        })
    }
}

impl MVMetricsReporter for ServiceHelper {
    fn report_metrics(&self, service: &MVService) {
        self.metrics.report_metrics(service);
    }

    fn observe_task_duration(&self, task_type: &str, result: &str, duration: i64) {
        self.metrics.observe_task_duration(task_type, result, duration);
    }

    fn observe_run_event(&self, event_type: &str) {
        self.metrics.observe_run_event(event_type);
    }
}

/// Go `shouldHandleMVCreateEvent`: the DDL events the owner fans out to the
/// service (other changes wait for the periodic metadata refresh).
#[must_use]
pub fn should_handle_mv_create_event(event: &tidb_ddl_notifier::SchemaChangeEvent) -> bool {
    use tidb_model::ActionType;
    let has_mv_related_table_info = |table: &TableInfo| table.materialized_view.is_some() || table.materialized_view_log.is_some();
    match event.action_type() {
        ActionType::ACTION_CREATE_TABLE => has_mv_related_table_info(event.get_create_table_info()),
        ActionType::ACTION_DROP_TABLE => has_mv_related_table_info(event.get_drop_table_info()),
        ActionType::ACTION_ALTER_MATERIALIZED_VIEW_REFRESH
        | ActionType::ACTION_ALTER_MATERIALIZED_VIEW_ATTRIBUTES
        | ActionType::ACTION_ALTER_MATERIALIZED_VIEW_LOG_PURGE
        | ActionType::ACTION_MVIEW_REFRESH_OUT_OF_PLACE_CUTOVER => true,
        _ => false,
    }
}

/// The fields of Go's `statistics.Table` `needAnalyzeMLog` reads.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct MLogStats {
    /// Go `Pseudo`.
    pub pseudo: bool,
    /// Go `RealtimeCount`.
    pub realtime_count: i64,
    /// Go `ModifyCount`.
    pub modify_count: i64,
    /// Go `IsAnalyzed()`.
    pub analyzed: bool,
    /// Go `GetAnalyzeRowCount()`.
    pub analyze_row_count: f64,
}

/// Go `needAnalyzeMLog`: whether an MV log's statistics call for an
/// `ANALYZE`, and why (`auto_analyze_min_count` is Go's
/// `statistics.AutoAnalyzeMinCnt`).
#[must_use]
pub fn need_analyze_mlog(stats: Option<&MLogStats>, ratio: f64, auto_analyze_min_count: i64) -> (bool, String) {
    let Some(stats) = stats.filter(|stats| !stats.pseudo && stats.realtime_count >= auto_analyze_min_count) else {
        return (false, String::new());
    };
    if !stats.analyzed {
        return (true, "mlog unanalyzed".to_owned());
    }
    if ratio == 0.0 {
        return (false, String::new());
    }
    let mut tbl_cnt = stats.realtime_count as f64;
    if stats.analyze_row_count > 0.0 {
        tbl_cnt = stats.analyze_row_count;
    }
    if tbl_cnt <= 0.0 {
        return (false, String::new());
    }
    if stats.modify_count as f64 / tbl_cnt <= ratio {
        return (false, String::new());
    }
    (true, format!("too many mlog modifications({}/{}>{})", stats.modify_count, tbl_cnt, ratio))
}

/// Go `readMLogAutoAnalyzeRatio`: the GLOBAL `tidb_mlog_auto_analyze_ratio`,
/// at least zero.
#[must_use]
pub fn read_mlog_auto_analyze_ratio(sctx: &dyn MVServiceSessionContext) -> f64 {
    let default = tidb_vardef::defaults::DEF_MLOG_AUTO_ANALYZE_RATIO;
    let text = get_global_system_var_best_effort(sctx, tidb_vardef::tidb_vars::TIDB_MLOG_AUTO_ANALYZE_RATIO)
        .unwrap_or_else(|| default.to_string());
    match text.parse::<f64>() {
        Ok(ratio) => ratio.max(0.0),
        Err(err) => {
            bg_logger().warn(
                "mv service: failed to parse mlog auto analyze ratio, fallback to default",
                &[
                    Field::new("var", Value::Str(tidb_vardef::tidb_vars::TIDB_MLOG_AUTO_ANALYZE_RATIO.to_owned())),
                    Field::new("value", Value::Str(text)),
                    Field::new("error", Value::Str(err.to_string())),
                ],
            );
            default
        }
    }
}

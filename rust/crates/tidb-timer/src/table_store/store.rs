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

//! Transcreation of Go `pkg/timer/tablestore/store.go`: the `TimerStoreCore`
//! implementation that keeps timers in a TiDB table, driven through an
//! internal SQL session.
//!
//! Everything this file borrows from outside `pkg/timer` is narrowed to a
//! local trait or value type, each carrying its own `boundary:` note:
//! [`SqlExecutor`] for `pkg/util/sqlexec.RestrictedSQLExecutor`'s
//! `ExecuteInternal`, [`Row`]/[`Datum`] for `pkg/util/chunk.Row`,
//! [`SysSession`]/[`SessionPool`] for `pkg/session/syssession`,
//! [`SessionContext`] for `pkg/sessionctx.Context`'s session variables, and
//! [`SqlContext`] for the `client-go` internal-source tag that the upstream
//! test's context matcher inspects.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use chrono::{Datelike, NaiveDateTime, Timelike};
use tidb_log::{Field, Value};
use tidb_util::logutil::bg_logger;
use tidb_util::timeutil::{parse_time_zone, system_location, TimeZone};

use crate::error::{Result, TimerError};
use crate::go_time::GoTime;
use crate::mem_store::new_mem_timer_watch_event_notifier;
use crate::store::{
    Cond, Context, TimerCond, TimerStore, TimerStoreCore, TimerUpdate, TimerWatchEventNotifier,
    WatchTimerChan, WatchTimerEventType,
};
use crate::timer::{
    create_sched_event_policy, validate_time_zone, SchedEventStatus, SchedPolicyType, TimerRecord,
    TimerSpec,
};

use super::sql::{
    build_delete_timer_sql, build_insert_timer_sql, build_select_timer_sql, build_update_timer_sql,
    indent_string, EventExtObj, ManualRequestObj, SqlArg, TimerExt,
};

/// Go `kv.InternalTimer`.
pub const INTERNAL_TIMER: &str = "Timer";

/// Go `vardef.TimeZone`.
pub const VARDEF_TIME_ZONE: &str = "time_zone";

/// `boundary:` the `context.Context` that `store.go` threads into every
/// `ExecuteInternal`, after `clitutil.WithInternalSourceType(ctx,
/// kv.InternalTimer)` has tagged it. The upstream test's `matchCtx` asserts
/// exactly that tag, so it is the only capability carried across.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SqlContext {
    /// The internal request source, as set by `WithInternalSourceType`.
    pub internal_source: Option<String>,
}

impl SqlContext {
    /// The tagged context `executeSQL` builds.
    pub fn internal_timer() -> Self {
        Self {
            internal_source: Some(INTERNAL_TIMER.to_string()),
        }
    }
}

/// `boundary:` one column of `pkg/util/chunk.Row`, restricted to the types
/// `listWithSctx` reads.
///
/// Go's `types.Time` is a zone-less wall clock that `GoTime(loc)` resolves in
/// the session's location, so [`Datum::Time`] carries a `NaiveDateTime` and
/// [`Row::get_time`] returns it unresolved.
#[derive(Debug, Clone, PartialEq)]
pub enum Datum {
    /// A SQL `NULL`.
    Null,
    /// A string column.
    Str(String),
    /// A binary column.
    Bytes(Vec<u8>),
    /// A signed integer column.
    Int64(i64),
    /// An unsigned integer column.
    Uint64(u64),
    /// A `DATETIME`/`TIMESTAMP` column's wall clock.
    Time(NaiveDateTime),
    /// A `JSON` column, as its text form (Go's `row.GetJSON(i).String()`).
    Json(String),
}

/// `boundary:` Go `pkg/util/chunk.Row`.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Row {
    values: Vec<Datum>,
}

impl Row {
    /// Builds a row from its columns.
    pub fn new(values: Vec<Datum>) -> Self {
        Self { values }
    }

    /// Go `row.Len()`.
    pub fn len(&self) -> usize {
        self.values.len()
    }

    /// Whether the row has no columns.
    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }

    /// Go `row.IsNull(i)`; a missing column reads as null.
    pub fn is_null(&self, index: usize) -> bool {
        !matches!(self.values.get(index), Some(value) if value != &Datum::Null)
    }

    /// Go `row.GetString(i)`.
    pub fn get_string(&self, index: usize) -> String {
        match self.values.get(index) {
            Some(Datum::Str(text)) => text.clone(),
            Some(Datum::Bytes(bytes)) => String::from_utf8_lossy(bytes).into_owned(),
            _ => String::new(),
        }
    }

    /// Go `row.GetBytes(i)`.
    pub fn get_bytes(&self, index: usize) -> Vec<u8> {
        match self.values.get(index) {
            Some(Datum::Bytes(bytes)) => bytes.clone(),
            Some(Datum::Str(text)) => text.clone().into_bytes(),
            _ => Vec::new(),
        }
    }

    /// Go `row.GetInt64(i)`.
    pub fn get_int64(&self, index: usize) -> i64 {
        match self.values.get(index) {
            Some(Datum::Int64(value)) => *value,
            Some(Datum::Uint64(value)) => *value as i64,
            _ => 0,
        }
    }

    /// Go `row.GetUint64(i)`.
    pub fn get_uint64(&self, index: usize) -> u64 {
        match self.values.get(index) {
            Some(Datum::Uint64(value)) => *value,
            Some(Datum::Int64(value)) => *value as u64,
            _ => 0,
        }
    }

    /// Go `row.GetTime(i)`, before its `GoTime(loc)` resolution.
    pub fn get_time(&self, index: usize) -> Option<NaiveDateTime> {
        match self.values.get(index) {
            Some(Datum::Time(value)) => Some(*value),
            _ => None,
        }
    }

    /// Go `row.GetJSON(i).String()`.
    pub fn get_json(&self, index: usize) -> String {
        match self.values.get(index) {
            Some(Datum::Json(text)) => text.clone(),
            Some(Datum::Str(text)) => text.clone(),
            _ => String::new(),
        }
    }
}

/// Go `types.Time.GoTime(loc)`: resolves a stored wall clock in `location`.
pub fn wall_clock_go_time(wall: NaiveDateTime, location: &TimeZone) -> GoTime {
    GoTime::date(
        wall.year(),
        wall.month() as i32,
        wall.day() as i32,
        wall.hour() as i32,
        wall.minute() as i32,
        wall.second() as i32,
        i64::from(wall.nanosecond()),
        location,
    )
}

/// `boundary:` Go `pkg/util/sqlexec.SQLExecutor` (the `RestrictedSQLExecutor`
/// family). `store.go` only ever calls `ExecuteInternal` and then drains the
/// result set, so the trait returns the drained rows directly; `None` is Go's
/// nil `RecordSet` for statements that produce no result.
pub trait SqlExecutor: Send + Sync {
    /// Go `ExecuteInternal`.
    fn execute_internal(
        &self,
        ctx: &SqlContext,
        sql: &str,
        args: &[SqlArg],
    ) -> Result<Option<Vec<Row>>>;
}

/// Go `executeSQL`, including its `WithInternalSourceType` tagging and its
/// `sqlexec.DrainRecordSet` of the returned set.
pub fn execute_sql(exec: &dyn SqlExecutor, sql: &str, args: &[SqlArg]) -> Result<Vec<Row>> {
    let ctx = SqlContext::internal_timer();
    Ok(exec.execute_internal(&ctx, sql, args)?.unwrap_or_default())
}

/// `boundary:` Go `pkg/sessionctx.Context`, restricted to the session
/// variables `listWithSctx` touches.
pub trait SessionContext: Send + Sync {
    /// Go `sessVars.GetEnableIndexMerge()`.
    fn get_enable_index_merge(&self) -> bool;
    /// Go `sessVars.SetEnableIndexMerge(v)`.
    fn set_enable_index_merge(&self, enable: bool);
    /// Go `sessVars.Location()`.
    fn location(&self) -> TimeZone;
    /// Go `sessVars.GetGlobalSystemVar(ctx, name)`.
    fn get_global_system_var(&self, name: &str) -> Result<String>;
    /// Go `sctx.GetSQLExecutor()`.
    fn sql_executor(&self) -> Arc<dyn SqlExecutor>;
}

/// `boundary:` Go `pkg/session/syssession.Session`, narrowed to the three
/// capabilities `store.go` uses: it is a `SQLExecutor`, it can hand out its
/// `sessionctx.Context`, and it can be marked unreusable.
///
/// `pkg/session/syssession` itself is ported in `tidb-exec`, which
/// `tidb-timer` does not depend on; adding that edge would invert the
/// dependency direction, so the surface is restated here instead.
pub struct SysSession {
    sctx: Arc<dyn SessionContext>,
    avoid_reuse: AtomicBool,
}

impl SysSession {
    /// Go `syssession.NewSessionForTest(sctx)` — the only constructor the
    /// package (and its tests) need.
    pub fn new(sctx: Arc<dyn SessionContext>) -> Self {
        Self {
            sctx,
            avoid_reuse: AtomicBool::new(false),
        }
    }

    /// Go `se.AvoidReuse()`.
    pub fn avoid_reuse(&self) {
        self.avoid_reuse.store(true, Ordering::SeqCst);
    }

    /// Go `se.IsAvoidReuse()`.
    pub fn is_avoid_reuse(&self) -> bool {
        self.avoid_reuse.load(Ordering::SeqCst)
    }

    /// Go `se.WithSessionContext(fn)`.
    pub fn with_session_context(
        &self,
        callback: &mut dyn FnMut(&dyn SessionContext) -> Result<()>,
    ) -> Result<()> {
        callback(self.sctx.as_ref())
    }
}

impl SqlExecutor for SysSession {
    fn execute_internal(
        &self,
        ctx: &SqlContext,
        sql: &str,
        args: &[SqlArg],
    ) -> Result<Option<Vec<Row>>> {
        self.sctx.sql_executor().execute_internal(ctx, sql, args)
    }
}

/// `boundary:` Go `pkg/session/syssession.Pool`, restricted to `WithSession`.
pub trait SessionPool: Send + Sync {
    /// Go `pool.WithSession(fn)`.
    fn with_session(&self, callback: &mut dyn FnMut(&SysSession) -> Result<()>) -> Result<()>;
}

/// Go `tableTimerStoreCore`.
pub struct TableTimerStoreCore {
    pool: Arc<dyn SessionPool>,
    db_name: String,
    tbl_name: String,
    notifier: Arc<dyn TimerWatchEventNotifier>,
}

impl TableTimerStoreCore {
    /// The core behind [`new_table_timer_store`].
    pub fn new(pool: Arc<dyn SessionPool>, db_name: &str, tbl_name: &str) -> Self {
        Self::with_notifier(
            pool,
            db_name,
            tbl_name,
            new_mem_timer_watch_event_notifier(),
        )
    }

    /// The same core with a caller-supplied notifier.
    pub fn with_notifier(
        pool: Arc<dyn SessionPool>,
        db_name: &str,
        tbl_name: &str,
        notifier: Arc<dyn TimerWatchEventNotifier>,
    ) -> Self {
        Self {
            pool,
            db_name: db_name.to_string(),
            tbl_name: tbl_name.to_string(),
            notifier,
        }
    }

    /// Go `(*tableTimerStoreCore).withSession`.
    ///
    /// Go's `defer` for the time-zone restore becomes [`RestoreGuard`], whose
    /// `Drop` runs on the normal path, on the error path, and while a panic
    /// unwinds — the three cases the upstream test exercises.
    pub fn with_session(&self, callback: &mut dyn FnMut(&SysSession) -> Result<()>) -> Result<()> {
        self.pool.with_session(&mut |se| {
            // rollback first to terminate unexpected transactions
            execute_sql(se, "ROLLBACK", &[])?;
            // we should force to set time zone to UTC to make sure time
            // operations are consistent.
            let rows = execute_sql(se, "SELECT @@time_zone", &[])?;
            if rows.is_empty() || rows[0].is_empty() {
                return Err(TimerError::message(
                    "failed to get original time zone of session",
                ));
            }
            let original_time_zone = rows[0].get_string(0);

            execute_sql(se, "SET @@time_zone='UTC'", &[])?;

            let _restore = RestoreGuard {
                session: se,
                original_time_zone,
            };
            callback(se)
        })
    }

    /// Go `(*tableTimerStoreCore).withSctx`.
    pub fn with_sctx(
        &self,
        callback: &mut dyn FnMut(&dyn SessionContext) -> Result<()>,
    ) -> Result<()> {
        self.with_session(&mut |se| se.with_session_context(callback))
    }

    /// Go `(*tableTimerStoreCore).createWithSession`.
    fn create_with_session(&self, se: &SysSession, record: &TimerRecord) -> Result<String> {
        let (sql, args) = build_insert_timer_sql(&self.db_name, &self.tbl_name, record)?;
        execute_sql(se, &sql, &args)?;

        let rows = execute_sql(se, "select @@last_insert_id", &[])?;
        let timer_id = rows
            .first()
            .map(|row| row.get_uint64(0))
            .unwrap_or_default()
            .to_string();
        self.notifier.notify(WatchTimerEventType::Create, &timer_id);
        Ok(timer_id)
    }

    /// Go `(*tableTimerStoreCore).listWithSctx`.
    fn list_with_sctx(
        &self,
        sctx: &dyn SessionContext,
        cond: Option<&dyn Cond>,
    ) -> Result<Vec<TimerRecord>> {
        // Enable index merge is used to make sure filtering timers with tags
        // quickly. Currently, we are using multi-value index to index tags for
        // timers which requires index merge enabled.
        let restore_index_merge = !sctx.get_enable_index_merge();
        if restore_index_merge {
            sctx.set_enable_index_merge(true);
        }
        let result = self.list_with_sctx_inner(sctx, cond);
        if restore_index_merge {
            sctx.set_enable_index_merge(false);
        }
        result
    }

    fn list_with_sctx_inner(
        &self,
        sctx: &dyn SessionContext,
        cond: Option<&dyn Cond>,
    ) -> Result<Vec<TimerRecord>> {
        let se_tz = sctx.location();
        let (sql, args) = build_select_timer_sql(&self.db_name, &self.tbl_name, cond)?;
        let exec = sctx.sql_executor();
        let rows = execute_sql(exec.as_ref(), &sql, &args)?;
        let tidb_time_zone = sctx.get_global_system_var(VARDEF_TIME_ZONE)?;

        let mut timers = Vec::with_capacity(rows.len());
        for row in &rows {
            let timer_data = if row.is_null(3) {
                Vec::new()
            } else {
                row.get_bytes(3)
            };

            let tz = row.get_string(4);
            // handling value "TIDB" is for compatibility of version 7.3.0
            let tz_parse = if tz.is_empty() || tz.eq_ignore_ascii_case("TIDB") {
                tidb_time_zone.clone()
            } else {
                tz.clone()
            };

            let loc = parse_time_zone(&tz_parse).unwrap_or_else(|_| system_location());

            let watermark = match row.get_time(8) {
                Some(wall) if !row.is_null(8) => wall_clock_go_time(wall, &se_tz).in_location(&loc),
                _ => GoTime::zero(),
            };

            let ext = if row.is_null(10) {
                TimerExt::default()
            } else {
                TimerExt::unmarshal(&row.get_json(10))?
            };

            let event_data = if row.is_null(13) {
                Vec::new()
            } else {
                row.get_bytes(13)
            };

            let event_start = match row.get_time(14) {
                Some(wall) if !row.is_null(14) => {
                    wall_clock_go_time(wall, &se_tz).in_location(&loc)
                }
                _ => GoTime::zero(),
            };

            let summary_data = if row.is_null(15) {
                Vec::new()
            } else {
                row.get_bytes(15)
            };

            let create_time = match row.get_time(16) {
                Some(wall) if !row.is_null(16) => {
                    wall_clock_go_time(wall, &se_tz).in_location(&loc)
                }
                _ => GoTime::zero(),
            };

            timers.push(TimerRecord {
                id: row.get_uint64(0).to_string(),
                spec: TimerSpec {
                    namespace: row.get_string(1),
                    key: row.get_string(2),
                    tags: ext.tags.clone(),
                    data: timer_data,
                    time_zone: tz,
                    sched_policy_type: SchedPolicyType(row.get_string(5)),
                    sched_policy_expr: row.get_string(6),
                    hook_class: row.get_string(7),
                    watermark,
                    enable: row.get_int64(9) != 0,
                },
                manual_request: ManualRequestObj::to_manual_request(ext.manual.as_ref()),
                event_status: SchedEventStatus(row.get_string(11)),
                event_id: row.get_string(12),
                event_data,
                event_start,
                event_extra: EventExtObj::to_event_extra(ext.event.as_ref()),
                summary_data,
                location: Some(loc),
                create_time,
                version: row.get_uint64(18),
            });
        }
        Ok(timers)
    }

    /// Go `(*tableTimerStoreCore).updateWithSession`.
    fn update_with_session(
        &self,
        se: &SysSession,
        timer_id: &str,
        update: &TimerUpdate,
    ) -> Result<()> {
        run_in_txn(se, &mut || {
            let get_check_cols_sql = format!(
                "SELECT EVENT_ID, VERSION, SCHED_POLICY_TYPE, SCHED_POLICY_EXPR FROM {} WHERE ID=%?",
                indent_string(&self.db_name, &self.tbl_name),
            );

            let rows = execute_sql(se, &get_check_cols_sql, &[SqlArg::str(timer_id)])?;
            if rows.is_empty() {
                return Err(TimerError::TimerNotExist);
            }

            check_update_constraints(
                update,
                &rows[0].get_string(0),
                rows[0].get_uint64(1),
                &SchedPolicyType(rows[0].get_string(2)),
                &rows[0].get_string(3),
            )?;

            let (update_sql, args) =
                build_update_timer_sql(&self.db_name, &self.tbl_name, timer_id, update)?;
            execute_sql(se, &update_sql, &args)?;
            Ok(())
        })?;

        self.notifier.notify(WatchTimerEventType::Update, timer_id);
        Ok(())
    }

    /// Go `(*tableTimerStoreCore).deleteWithSession`.
    fn delete_with_session(&self, se: &SysSession, timer_id: &str) -> Result<bool> {
        let (delete_sql, args) = build_delete_timer_sql(&self.db_name, &self.tbl_name, timer_id);
        execute_sql(se, &delete_sql, &args)?;

        let rows = execute_sql(se, "SELECT ROW_COUNT()", &[])?;
        let exist = rows.first().map(|row| row.get_int64(0)).unwrap_or_default() > 0;
        if exist {
            self.notifier.notify(WatchTimerEventType::Delete, timer_id);
        }
        Ok(exist)
    }
}

/// The `defer` in `withSession` that restores the session's original time zone
/// and marks the session unreusable when the restore itself fails.
struct RestoreGuard<'session> {
    session: &'session SysSession,
    original_time_zone: String,
}

impl Drop for RestoreGuard<'_> {
    fn drop(&mut self) {
        // Though `pool.WithSession` will discard a not committed transaction,
        // we still rollback here so the assertion in `Pool.Put` passes.
        if let Err(err) = execute_sql(self.session, "ROLLBACK", &[]) {
            terror_log(&err);
            self.session.avoid_reuse();
            return;
        }

        if let Err(err) = execute_sql(
            self.session,
            "SET @@time_zone=%?",
            &[SqlArg::str(&self.original_time_zone)],
        ) {
            terror_log(&err);
            self.session.avoid_reuse();
        }
    }
}

/// `boundary:` Go `pkg/parser/terror.Log`, which logs an error and swallows it.
/// `tidb-error`'s port is not a `tidb-timer` dependency, so the one behavior
/// this file needs is spelled directly against the workspace logger.
fn terror_log(err: &TimerError) {
    bg_logger().warn(
        "encountered error",
        &[Field::new("error", Value::Str(err.to_string()))],
    );
}

/// Go `runInTxn`.
pub fn run_in_txn(exec: &dyn SqlExecutor, body: &mut dyn FnMut() -> Result<()>) -> Result<()> {
    execute_sql(exec, "BEGIN PESSIMISTIC", &[])?;

    let result = body().and_then(|()| execute_sql(exec, "COMMIT", &[]).map(|_| ()));
    if result.is_err() {
        if let Err(err) = execute_sql(exec, "ROLLBACK", &[]) {
            terror_log(&err);
        }
    }
    result
}

/// Go `checkUpdateConstraints`.
pub fn check_update_constraints(
    update: &TimerUpdate,
    event_id: &str,
    version: u64,
    policy: &SchedPolicyType,
    expr: &str,
) -> Result<()> {
    if let Some(value) = update.check_event_id.get() {
        if event_id != value {
            return Err(TimerError::EventIDNotMatch);
        }
    }

    if let Some(value) = update.check_version.get() {
        if version != *value {
            return Err(TimerError::VersionNotMatch);
        }
    }

    if let Some(value) = update.time_zone.get() {
        validate_time_zone(value)?;
    }

    let mut check_policy = false;
    let mut policy = policy.clone();
    let mut expr = expr.to_string();
    if let Some(value) = update.sched_policy_type.get() {
        check_policy = true;
        policy = value.clone();
    }

    if let Some(value) = update.sched_policy_expr.get() {
        check_policy = true;
        expr = value.clone();
    }

    if check_policy {
        create_sched_event_policy(&policy, &expr)
            .map_err(|err| err.wrap("schedule event configuration is not valid"))?;
    }

    Ok(())
}

impl TimerStoreCore for TableTimerStoreCore {
    fn create(&self, _ctx: &Context, record: &TimerRecord) -> Result<String> {
        if !record.id.is_empty() {
            return Err(TimerError::message(
                "ID should not be specified when create record",
            ));
        }

        if record.version != 0 {
            return Err(TimerError::message(
                "Version should not be specified when create record",
            ));
        }

        if !record.create_time.is_zero() {
            return Err(TimerError::message(
                "CreateTime should not be specified when create record",
            ));
        }

        record.validate()?;

        let mut timer_id = String::new();
        self.with_session(&mut |se| {
            timer_id = self.create_with_session(se, record)?;
            Ok(())
        })?;
        Ok(timer_id)
    }

    fn list(&self, _ctx: &Context, cond: Option<&dyn Cond>) -> Result<Vec<TimerRecord>> {
        let mut result = Vec::new();
        self.with_sctx(&mut |sctx| {
            result = self.list_with_sctx(sctx, cond)?;
            Ok(())
        })?;
        Ok(result)
    }

    fn update(&self, _ctx: &Context, timer_id: &str, update: &TimerUpdate) -> Result<()> {
        self.with_session(&mut |se| self.update_with_session(se, timer_id, update))
    }

    fn delete(&self, _ctx: &Context, timer_id: &str) -> Result<bool> {
        let mut ok = false;
        self.with_session(&mut |se| {
            ok = self.delete_with_session(se, timer_id)?;
            Ok(())
        })?;
        Ok(ok)
    }

    fn watch_supported(&self) -> bool {
        true
    }

    fn watch(&self, ctx: &Context) -> WatchTimerChan {
        self.notifier.watch(ctx)
    }

    fn close(&self) {
        self.notifier.close();
    }
}

/// Go `NewTableTimerStore`.
///
/// Narrowing: Go picks `NewEtcdNotifier(clusterID, etcd)` when an etcd client
/// is supplied. `notifier.go` is not ported (see the module header), so this
/// constructor always uses `api.NewMemTimerWatchEventNotifier`, which is
/// exactly Go's `etcd == nil` branch; the `clusterID` and `etcd` parameters
/// drop with it. [`TableTimerStoreCore::with_notifier`] keeps the injection
/// point open for the etcd notifier once it lands.
pub fn new_table_timer_store(
    pool: Arc<dyn SessionPool>,
    db_name: &str,
    tbl_name: &str,
) -> TimerStore {
    TimerStore::new(Arc::new(TableTimerStoreCore::new(pool, db_name, tbl_name)))
}

/// Convenience for callers building an id condition, mirroring the shape
/// `store.go` uses internally.
pub fn timer_id_cond(timer_id: &str) -> TimerCond {
    TimerCond {
        id: crate::store::OptionalVal::new(timer_id.to_string()),
        ..Default::default()
    }
}

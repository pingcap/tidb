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

//! `pkg/ddl/mview_schedule_expr.go`: deriving the persisted NEXT unix seconds
//! for materialized view and materialized view log refresh/purge schedules
//! (Go master `94a9cbedab`).
//!
//! The schedule-eval session swap (`setCreateMaterializedViewScheduleEvalSession`)
//! is carried by the [`tidb_ddl_session::SessionContext`] capability pair
//! `install_schedule_eval_session` / `restore_schedule_eval_session`; the
//! caller of [`derive_create_materialized_schedule_next_unix_seconds`]
//! installs it around the derive exactly as Go's create path does.

use std::cmp::Ordering;

use tidb_ddl_session::{Error, ScheduleTime, Session, SessionContext};
use tidb_model::job::ResolvedTimeZone;
use tidb_model::MaterializedViewBaseInfo;
use tidb_model::{MaterializedViewInfo, MaterializedViewLogInfo};
use tidb_sqlexec::ExecutionContext;

/// Go `BuildAndValidateMViewScheduleExpr`: restores an AST expression into
/// canonical SQL and validates that its expression type is
/// DATETIME/TIMESTAMP.
///
const NOW_QUERY: &str = "SELECT NOW(6)";
const NOW_LABEL: &str = "mview-refresh-info-next-time-now";
const EVAL_FAILURE_MESSAGE: &str =
    "create materialized view: failed to evaluate refresh schedule expression";

/// Go `deriveCreateMaterializedScheduleNextUnixSeconds`: decides the
/// persisted NEXT unix seconds and whether the persisted value should be
/// overwritten. `START WITH` takes precedence unless it is near now and
/// `NEXT` is present.
///
/// Errors flow through [`tidb_ddl_session::Error`], matching Go's
/// `errors.Trace` propagation of the session-eval failures.
#[allow(clippy::too_many_arguments)]
pub fn derive_create_materialized_schedule_next_unix_seconds<C: SessionContext>(
    session: &Session<C>,
    context: &dyn ExecutionContext,
    schema_name: &str,
    table_name: &str,
    start_expr: &str,
    next_expr: &str,
    schedule_time_zone: &ResolvedTimeZone,
    log_null_update: &dyn Fn(&str, &str, &str, &str, &str),
) -> Result<(Option<i64>, bool), Error> {
    // shouldUpdate reports whether the persisted NEXT_* value should be
    // overwritten.
    let start_expr = start_expr.trim();
    let next_expr = next_expr.trim();
    if start_expr.is_empty() && next_expr.is_empty() {
        return Ok((None, true));
    }

    let now_time = load_create_materialized_view_schedule_now(session, context)?;

    // START WITH takes precedence unless it is near now and NEXT is present.
    if !start_expr.is_empty() {
        let Some(start_at) =
            eval_create_materialized_view_schedule_expr_to_datetime(session, context, start_expr)?
        else {
            log_null_update(schema_name, table_name, "START WITH", start_expr, next_expr);
            return Ok((None, true));
        };
        if next_expr.is_empty() {
            let next_unix_seconds = schedule_time_to_unix_seconds(start_at, schedule_time_zone)?;
            return Ok((Some(next_unix_seconds), true));
        }

        let go_now = go_time_of(now_time, schedule_time_zone)?;
        let near_now_threshold = near_now_threshold(go_now, &now_time);
        if start_at.compare(near_now_threshold) == Ordering::Less {
            let Some(next_at) = eval_create_materialized_view_schedule_expr_to_datetime(
                session, context, next_expr,
            )?
            else {
                log_null_update(schema_name, table_name, "NEXT", start_expr, next_expr);
                return Ok((None, true));
            };
            let next_unix_seconds = schedule_time_to_unix_seconds(next_at, schedule_time_zone)?;
            return Ok((Some(next_unix_seconds), true));
        }
        let next_unix_seconds = schedule_time_to_unix_seconds(start_at, schedule_time_zone)?;
        return Ok((Some(next_unix_seconds), true));
    }

    if !next_expr.is_empty() {
        let Some(next_at) =
            eval_create_materialized_view_schedule_expr_to_datetime(session, context, next_expr)?
        else {
            log_null_update(schema_name, table_name, "NEXT", start_expr, next_expr);
            return Ok((None, true));
        };
        let next_unix_seconds = schedule_time_to_unix_seconds(next_at, schedule_time_zone)?;
        return Ok((Some(next_unix_seconds), true));
    }
    Ok((None, false))
}

/// Go `logCreateMaterializedViewNextUnixSecondsUpdateNull`.
pub fn log_create_materialized_view_next_unix_seconds_update_null(
    mview_schema_name: &str,
    mv_table_name: &str,
    null_expr_clause: &str,
    start_expr: &str,
    next_expr: &str,
) {
    if !next_expr.trim().is_empty() {
        tracing::error!(
            schemaName = mview_schema_name,
            tableName = mv_table_name,
            nullExprClause = null_expr_clause,
            refreshStartWith = start_expr,
            refreshNext = next_expr,
            "create materialized view: automatic refresh schedule disabled because schedule expression evaluated to NULL, updating NEXT_REFRESH_UNIX_SECONDS to NULL",
        );
        return;
    }
    tracing::warn!(
        schemaName = mview_schema_name,
        tableName = mv_table_name,
        nullExprClause = null_expr_clause,
        refreshStartWith = start_expr,
        refreshNext = next_expr,
        "create materialized view: schedule expression evaluated to NULL, updating NEXT_REFRESH_UNIX_SECONDS to NULL",
    );
}

/// Go `logCreateMaterializedViewLogNextUnixSecondsUpdateNull`.
pub fn log_create_materialized_view_log_next_unix_seconds_update_null(
    mlog_schema_name: &str,
    mlog_table_name: &str,
    null_expr_clause: &str,
    start_expr: &str,
    next_expr: &str,
) {
    if !next_expr.trim().is_empty() {
        tracing::error!(
            schemaName = mlog_schema_name,
            tableName = mlog_table_name,
            nullExprClause = null_expr_clause,
            purgeStartWith = start_expr,
            purgeNext = next_expr,
            "create materialized view log: automatic purge schedule disabled because schedule expression evaluated to NULL, updating NEXT_PURGE_UNIX_SECONDS to NULL",
        );
        return;
    }
    tracing::warn!(
        schemaName = mlog_schema_name,
        tableName = mlog_table_name,
        nullExprClause = null_expr_clause,
        purgeStartWith = start_expr,
        purgeNext = next_expr,
        "create materialized view log: purge schedule expression evaluated to NULL, updating NEXT_PURGE_UNIX_SECONDS to NULL",
    );
}

/// Go `loadCreateMaterializedViewScheduleNow`: reads `SELECT NOW(6)` through
/// the DDL session.
pub fn load_create_materialized_view_schedule_now<C: SessionContext>(
    session: &Session<C>,
    context: &dyn ExecutionContext,
) -> Result<ScheduleTime, Error> {
    let rows = session
        .execute(context, NOW_QUERY, NOW_LABEL, &[])
        .map_err(|error| Error::new(error.to_string()))?;
    let Some(rows) = rows else {
        return Err(Error::new(EVAL_FAILURE_MESSAGE));
    };
    let Some(row) = rows.first() else {
        return Err(Error::new(EVAL_FAILURE_MESSAGE));
    };
    match row.first() {
        Some(tidb_datatype::Datum::Time(time)) => Ok(*time),
        _ => Err(Error::new(EVAL_FAILURE_MESSAGE)),
    }
}

/// Go `evalCreateMaterializedViewScheduleExprToDatetime`.
pub fn eval_create_materialized_view_schedule_expr_to_datetime<C: SessionContext>(
    session: &Session<C>,
    context: &dyn ExecutionContext,
    expr_sql: &str,
) -> Result<Option<ScheduleTime>, Error> {
    session
        .session()
        .eval_schedule_expression(expr_sql)
        .map_err(|error| Error::new(error.to_string()))
}

/// Go `deriveCreateMaterializedViewNextUnixSeconds`.
pub fn derive_create_materialized_view_next_unix_seconds<C: SessionContext>(
    session: &Session<C>,
    context: &dyn ExecutionContext,
    mview_schema_name: &str,
    mv_table_name: &str,
    mview_info: Option<&MaterializedViewInfo>,
) -> Result<(Option<i64>, bool), Error> {
    let Some(mview_info) = mview_info else {
        return Ok((None, false));
    };
    let tz = mview_info
        .refresh_schedule_time_zone
        .get_location()
        .map_err(|error| Error::new(error))?;
    let tz = tz.read();
    derive_create_materialized_schedule_next_unix_seconds(
        session,
        context,
        mview_schema_name,
        mv_table_name,
        mview_info.refresh_start_with.as_str(),
        mview_info.refresh_next.as_str(),
        &tz,
        &log_create_materialized_view_next_unix_seconds_update_null,
    )
}

/// Go `deriveCreateMaterializedViewLogNextUnixSeconds`.
pub fn derive_create_materialized_view_log_next_unix_seconds<C: SessionContext>(
    session: &Session<C>,
    context: &dyn ExecutionContext,
    mlog_schema_name: &str,
    mlog_table_name: &str,
    mlog_info: Option<&MaterializedViewLogInfo>,
) -> Result<(Option<i64>, bool), Error> {
    let Some(mlog_info) = mlog_info else {
        return Ok((None, false));
    };
    let tz = mlog_info
        .purge_schedule_time_zone
        .get_location()
        .map_err(|error| Error::new(error))?;
    let tz = tz.read();
    derive_create_materialized_schedule_next_unix_seconds(
        session,
        context,
        mlog_schema_name,
        mlog_table_name,
        mlog_info.purge_start_with.as_str(),
        mlog_info.purge_next.as_str(),
        &tz,
        &log_create_materialized_view_log_next_unix_seconds_update_null,
    )
}

/// Go `t.GoTime(zone).Unix()` for a schedule time under the resolved zone.
fn go_time_of(time: ScheduleTime, zone: &ResolvedTimeZone) -> Result<i64, Error> {
    schedule_time_to_unix_seconds(time, zone)
}

/// Go `types.NewTime(types.FromGoTime(goNow.Add(10*time.Second)),
/// nowTime.Type(), nowTime.Fsp())`: the near-now comparison threshold.
fn near_now_threshold(now_go_unix: i64, now_time: &ScheduleTime) -> ScheduleTime {
    let threshold_core = now_time.core_time().add_duration(10_000_000_000);
    let _ = now_go_unix;
    ScheduleTime::new(threshold_core, now_time.kind(), i64::from(now_time.fsp()))
        .expect("the now-derived threshold keeps a valid calendar")
}

/// Go `expression.MaterializedScheduleTimeToUnixSeconds(startAt,
/// scheduleTimeZone)`.
fn schedule_time_to_unix_seconds(
    time: ScheduleTime,
    zone: &ResolvedTimeZone,
) -> Result<i64, Error> {
    let core = time.core_time();
    let unix = match zone {
        ResolvedTimeZone::Local => core
            .to_datetime(&chrono::Local)
            .map(|datetime| datetime.timestamp()),
        ResolvedTimeZone::Named(tz) => core.to_datetime(tz).map(|datetime| datetime.timestamp()),
        ResolvedTimeZone::Fixed { offset_seconds, .. } => {
            let offset = chrono::FixedOffset::east_opt(i32::try_from(*offset_seconds).unwrap_or(0))
                .ok_or_else(|| Error::new("invalid schedule time zone offset"))?;
            core.to_datetime(&offset)
                .map(|datetime| datetime.timestamp())
        }
    };
    unix.map_err(|error| Error::new(error.to_string()))
}

/// Go `restoreNodeToCanonicalSQL` (`format.DefaultRestoreFlags |
/// format.RestoreStringWithoutCharset`).
fn restore_node_to_canonical_sql(expr: &tidb_ast::Expr) -> String {
    use tidb_ast::RestoreFlags;
    expr.restore_with_flags(RestoreFlags::DEFAULT | RestoreFlags::STRING_WITHOUT_CHARSET)
}

/// Go's empty expression-column scope: a schedule expression references no
/// table columns, and any column reference fails resolution exactly as it
/// does in Go's session expression context at DDL time.
struct NoColumns;

impl tidb_expr::rewriter::ColumnResolver for NoColumns {
    fn resolve(&self, _path: &[String]) -> Option<(usize, tidb_datatype::FieldType, i64)> {
        None
    }
    fn time_zone(&self) -> tidb_datatype::SessionTimeZone {
        tidb_datatype::SessionTimeZone::utc()
    }
}

/// Go `BuildAndValidateMViewScheduleExpr`: restores an AST expression into
/// canonical SQL and validates that its expression type is
/// DATETIME/TIMESTAMP. The eval-session swap and the evaluation itself are
/// the session capabilities this module already carries; this function is
/// the pure build-and-check half the CREATE path calls before persisting.
pub fn build_and_validate_m_view_schedule_expr(
    expr: &tidb_ast::Expr,
    clause: &str,
) -> Result<String, Error> {
    let expr_sql = restore_node_to_canonical_sql(expr);
    let built = tidb_expr::simple_expr::build_simple_expr(
        &NoColumns,
        expr,
        &tidb_expr::simple_expr::BuildOptions::default(),
    )
    .map_err(|error| Error::new(error.to_string()))?;
    let Some(field_type) = built.static_type() else {
        return Err(Error::new(format!(
            "failed to infer expression type for {clause}"
        )));
    };
    if !matches!(
        field_type.code(),
        tidb_datatype::FieldTypeCode::Datetime | tidb_datatype::FieldTypeCode::Timestamp
    ) {
        return Err(Error::new(format!(
            "Unsupported {clause} expression must return DATETIME/TIMESTAMP, but got {}",
            tidb_datatype::type_str(field_type.code())
        )));
    }
    Ok(expr_sql)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::VecDeque;
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::sync::Mutex;
    use tidb_ast::{QueryStmt, SelectField};
    use tidb_datatype::{Datum, FieldType, FieldTypeCode, TimeType};
    use tidb_ddl_session::ScheduleEvalOriginals;
    use tidb_model::{ColumnInfo, GoShared};
    use tidb_resolve::ResultField;
    use tidb_sqlexec::{ExecutionContext, RecordSet, SimpleRecordSet};
    use tidb_util::sqlescape::SqlArg;

    /// A `sessionctx.Context` mock whose only live capabilities are the
    /// `SELECT NOW(6)` row and the queued schedule-expression evaluations.
    struct MockScheduleContext {
        now_time: Mutex<Option<ScheduleTime>>,
        now_empty: AtomicU32,
        eval_results: Mutex<VecDeque<std::result::Result<Option<ScheduleTime>, String>>>,
        eval_calls: Mutex<Vec<String>>,
        install_calls: AtomicU32,
        restore_calls: AtomicU32,
    }

    impl MockScheduleContext {
        fn new(now_time: ScheduleTime) -> Self {
            Self {
                now_time: Mutex::new(Some(now_time)),
                now_empty: AtomicU32::new(0),
                eval_results: Mutex::new(VecDeque::new()),
                eval_calls: Mutex::new(Vec::new()),
                install_calls: AtomicU32::new(0),
                restore_calls: AtomicU32::new(0),
            }
        }

        fn queue_eval(&self, result: std::result::Result<Option<ScheduleTime>, String>) {
            self.eval_results.lock().unwrap().push_back(result);
        }

        fn refuse_now(&self) {
            self.now_empty.store(1, Ordering::SeqCst);
        }
    }

    fn result_fields() -> Vec<GoShared<ResultField>> {
        vec![GoShared::new(ResultField {
            column: Some(GoShared::new(ColumnInfo {
                field_type: FieldType::new(FieldTypeCode::Datetime),
                ..ColumnInfo::default()
            })),
            ..Default::default()
        })]
    }

    impl SessionContext for MockScheduleContext {
        fn new_txn(&self, _: &dyn ExecutionContext) -> tidb_ddl_session::Result<()> {
            unimplemented!()
        }
        fn enter_new_pessimistic_txn(
            &self,
            _: &dyn ExecutionContext,
        ) -> tidb_ddl_session::Result<()> {
            unimplemented!()
        }
        fn set_in_txn(&self, _: bool) {}
        fn stmt_commit(&self, _: &dyn ExecutionContext) {}
        fn commit_txn(&self, _: &dyn ExecutionContext) -> tidb_ddl_session::Result<()> {
            unimplemented!()
        }
        fn txn(
            &self,
            _: bool,
        ) -> tidb_ddl_session::Result<Option<std::sync::Arc<dyn tidb_ddl_session::Transaction>>>
        {
            Ok(None)
        }
        fn stmt_rollback(&self, _: &dyn ExecutionContext, _: bool) {}
        fn rollback_txn(&self, _: &dyn ExecutionContext) {}
        fn request_source(&self, _: &dyn ExecutionContext) -> Option<String> {
            None
        }
        fn execute_internal(
            &self,
            _: &dyn ExecutionContext,
            _source: &str,
            query: &str,
            _arguments: &[SqlArg<'_>],
        ) -> std::result::Result<Option<Box<dyn RecordSet>>, tidb_sqlexec::SqlExecError> {
            assert_eq!(query, "SELECT NOW(6)");
            if self.now_empty.load(Ordering::SeqCst) == 1 {
                return Ok(Some(Box::new(SimpleRecordSet::new(
                    result_fields(),
                    Vec::new(),
                    32,
                ))));
            }
            let time = self.now_time.lock().unwrap().expect("now time configured");
            Ok(Some(Box::new(SimpleRecordSet::new(
                result_fields(),
                vec![vec![Datum::Time(time)]],
                32,
            ))))
        }
        fn set_autocommit(&self, _: bool) {}
        fn set_restricted_sql(&self, _: bool) {}
        fn set_statement_timezone_to_session_location(&self) {}
        fn allow_on_almost_full(&self) {}
        fn clear_disk_full_option(&self) {}
        fn register_internal_session(&self) {}
        fn unregister_internal_session(&self) {}
        fn close(&self) {}
        fn install_schedule_eval_session(
            &self,
            _sql_mode: tidb_mysql::SqlMode,
            _zone: &ResolvedTimeZone,
        ) -> ScheduleEvalOriginals {
            self.install_calls.fetch_add(1, Ordering::SeqCst);
            ScheduleEvalOriginals {
                sql_mode: tidb_mysql::SqlMode::default(),
                stmt_type_flags: tidb_datatype::ConversionFlags::default(),
                stmt_err_levels: tidb_error::errctx::LevelMap::strict(),
                session_time_zone: None,
                stmt_time_zone: None,
            }
        }
        fn restore_schedule_eval_session(&self, _originals: &ScheduleEvalOriginals) {
            self.restore_calls.fetch_add(1, Ordering::SeqCst);
        }
        fn eval_schedule_expression(
            &self,
            expr_sql: &str,
        ) -> tidb_ddl_session::Result<Option<ScheduleTime>> {
            self.eval_calls.lock().unwrap().push(expr_sql.to_owned());
            let slot = self
                .eval_results
                .lock()
                .unwrap()
                .pop_front()
                .expect("no queued schedule evaluation");
            match slot {
                Ok(value) => Ok(value),
                Err(message) => Err(Error::new(message)),
            }
        }
    }

    fn datetime(year: u16, month: u8, day: u8, hour: u8) -> ScheduleTime {
        ScheduleTime::new(
            tidb_datatype::CoreTime::from_date(year, month, day, hour, 0, 0, 0),
            TimeType::DateTime,
            6,
        )
        .expect("valid schedule datetime")
    }

    fn utc_zone() -> ResolvedTimeZone {
        ResolvedTimeZone::Named(chrono_tz::Tz::UTC)
    }

    fn unix(year: u16, month: u8, day: u8, hour: u8) -> i64 {
        use chrono::TimeZone as _;
        chrono::Utc
            .with_ymd_and_hms(
                i32::from(year),
                u32::from(month),
                u32::from(day),
                u32::from(hour),
                0,
                0,
            )
            .unwrap()
            .timestamp()
    }

    fn null_log() -> (std::rc::Rc<AtomicU32>, std::rc::Rc<Mutex<Vec<String>>>) {
        let count = std::rc::Rc::new(AtomicU32::new(0));
        let clauses = std::rc::Rc::new(Mutex::new(Vec::new()));
        (count, clauses)
    }

    #[test]
    fn empty_expressions_skip_the_update() {
        let context = MockScheduleContext::new(datetime(2026, 1, 2, 8));
        let session = Session::new(std::sync::Arc::new(context));
        let (count, clauses) = null_log();
        let count = &*count;
        let clauses = &*clauses;
        let derived = derive_create_materialized_schedule_next_unix_seconds(
            &session,
            &tidb_sqlexec::BackgroundContext,
            "db",
            "mv",
            "   ",
            "",
            &utc_zone(),
            &|_, _, clause, _, _| {
                count.fetch_add(1, Ordering::SeqCst);
                clauses.lock().unwrap().push(clause.to_owned());
            },
        )
        .expect("derive runs");
        assert_eq!(derived, (None, true));
        assert_eq!(count.load(Ordering::SeqCst), 0);
        assert!(clauses.lock().unwrap().is_empty());
    }

    #[test]
    fn start_with_alone_sets_next_to_the_start_instant() {
        let context = MockScheduleContext::new(datetime(2026, 1, 2, 8));
        context.queue_eval(Ok(Some(datetime(2026, 6, 1, 9))));
        let session = Session::new(std::sync::Arc::new(context));

        let derived = derive_create_materialized_view_next_unix_seconds(
            &session,
            &tidb_sqlexec::BackgroundContext,
            "db",
            "mv",
            Some(&MaterializedViewInfo {
                refresh_start_with: "START '2026-06-01 09:00:00'".to_owned(),
                refresh_next: String::new(),
                ..Default::default()
            }),
        )
        .expect("derive runs");
        assert_eq!(derived, (Some(unix(2026, 6, 1, 9)), true));
    }

    #[test]
    fn start_far_future_ignores_next() {
        // now = Jan 2; START = Jun 1 (far beyond the 10s near-now window), so
        // the NEXT evaluation is never queued and START wins.
        let context = MockScheduleContext::new(datetime(2026, 1, 2, 8));
        context.queue_eval(Ok(Some(datetime(2026, 6, 1, 9))));
        let session = Session::new(std::sync::Arc::new(context));

        let derived = derive_create_materialized_view_next_unix_seconds(
            &session,
            &tidb_sqlexec::BackgroundContext,
            "db",
            "mv",
            Some(&MaterializedViewInfo {
                refresh_start_with: "START '2026-06-01 09:00:00'".to_owned(),
                refresh_next: "NEXT '2026-06-02 09:00:00'".to_owned(),
                ..Default::default()
            }),
        )
        .expect("derive runs");
        assert_eq!(derived, (Some(unix(2026, 6, 1, 9)), true));
        assert_eq!(session.session().eval_calls.lock().unwrap().len(), 1);
    }

    #[test]
    fn start_near_now_uses_next() {
        // now = Jan 2 08:00; START = Jan 2 08:05 (within the 10s window is
        // impossible for a 5-minute lead, so use an explicit 5-second lead).
        let context = MockScheduleContext::new(datetime(2026, 1, 2, 8));
        // START at 08:00:05 is 5s after now and thus NOT < now+10s... Go's
        // rule: START < now+10s means START is (effectively) already due, so
        // the NEXT expression decides. Queue START at now minus one minute.
        context.queue_eval(Ok(Some(datetime(2026, 1, 2, 7))));
        context.queue_eval(Ok(Some(datetime(2026, 1, 9, 8))));
        let session = Session::new(std::sync::Arc::new(context));

        let derived = derive_create_materialized_view_next_unix_seconds(
            &session,
            &tidb_sqlexec::BackgroundContext,
            "db",
            "mv",
            Some(&MaterializedViewInfo {
                refresh_start_with: "START '2026-01-02 07:00:00'".to_owned(),
                refresh_next: "NEXT '2026-01-09 08:00:00'".to_owned(),
                ..Default::default()
            }),
        )
        .expect("derive runs");
        assert_eq!(derived, (Some(unix(2026, 1, 9, 8)), true));
    }

    #[test]
    fn start_evaluating_null_logs_and_skips_the_update() {
        let context = MockScheduleContext::new(datetime(2026, 1, 2, 8));
        context.queue_eval(Ok(None));
        let session = Session::new(std::sync::Arc::new(context));
        let (count, clauses) = null_log();
        let count = &*count;
        let clauses = &*clauses;

        // Go's derive takes the logger as the `logNullUpdate` parameter, so
        // the NULL path is observed through the injected closure.
        let derived = derive_create_materialized_schedule_next_unix_seconds(
            &session,
            &tidb_sqlexec::BackgroundContext,
            "db",
            "mv",
            "START NULL_EXPR",
            "",
            &utc_zone(),
            &|_, _, clause, _, _| {
                count.fetch_add(1, Ordering::SeqCst);
                clauses.lock().unwrap().push(clause.to_owned());
            },
        )
        .expect("derive runs");
        assert_eq!(derived, (None, true), "NULL START disables the schedule");
        assert_eq!(count.load(Ordering::SeqCst), 1);
        assert_eq!(clauses.lock().unwrap().as_slice(), ["START WITH"]);
    }

    /// Extracts the first projected expression of a `SELECT <expr>` fixture.
    fn first_field_expr(sql: &str) -> tidb_ast::Expr {
        match tidb_parser::parse(sql).expect("parse fixture") {
            tidb_ast::Stmt::Query(query) => match &*query {
                QueryStmt::Select(sel) => match sel.fields.fields().first() {
                    Some(SelectField::Expr { expr, .. }) => expr.clone(),
                    other => panic!("expected an expression field, got {other:?}"),
                },
                other => panic!("expected a select, got {other:?}"),
            },
            other => panic!("expected a query, got {other:?}"),
        }
    }

    #[test]
    fn build_and_validate_accepts_datetime_and_refuses_other_types() {
        let expr = first_field_expr("SELECT NOW() + INTERVAL 1 DAY");
        let restored =
            build_and_validate_m_view_schedule_expr(&expr, "START WITH").expect("datetime passes");
        assert!(!restored.is_empty());

        let int_expr = first_field_expr("SELECT 1 + 1");
        let error = build_and_validate_m_view_schedule_expr(&int_expr, "START WITH")
            .expect_err("an integer expression is refused");
        assert_eq!(
            error.to_string(),
            // Go  is .
            "Unsupported START WITH expression must return DATETIME/TIMESTAMP, but got bigint"
        );

        // A column reference has no scope at DDL time and fails the build,
        // exactly as Go's session expression context reports it.
        let column_expr = first_field_expr("SELECT some_col FROM t");
        assert!(build_and_validate_m_view_schedule_expr(&column_expr, "START WITH").is_err());
    }

    #[test]
    fn next_only_sets_next_instant() {
        let context = MockScheduleContext::new(datetime(2026, 1, 2, 8));
        context.queue_eval(Ok(Some(datetime(2026, 2, 3, 10))));
        let session = Session::new(std::sync::Arc::new(context));

        let derived = derive_create_materialized_view_log_next_unix_seconds(
            &session,
            &tidb_sqlexec::BackgroundContext,
            "db",
            "mlog",
            Some(&MaterializedViewLogInfo {
                purge_start_with: String::new(),
                purge_next: "NEXT '2026-02-03 10:00:00'".to_owned(),
                ..Default::default()
            }),
        )
        .expect("derive runs");
        assert_eq!(derived, (Some(unix(2026, 2, 3, 10)), true));
    }

    #[test]
    fn failed_now_evaluation_errors_with_the_job_message() {
        let context = MockScheduleContext::new(datetime(2026, 1, 2, 8));
        context.refuse_now();
        let session = Session::new(std::sync::Arc::new(context));

        let error = derive_create_materialized_view_next_unix_seconds(
            &session,
            &tidb_sqlexec::BackgroundContext,
            "db",
            "mv",
            Some(&MaterializedViewInfo {
                refresh_start_with: "START '2026-06-01 09:00:00'".to_owned(),
                ..Default::default()
            }),
        )
        .expect_err("the NOW failure propagates");
        assert_eq!(
            error.to_string(),
            "create materialized view: failed to evaluate refresh schedule expression"
        );
    }

    #[test]
    fn derive_does_not_install_the_eval_session_itself() {
        // Go installs the eval session in the CREATE path around the whole
        // derive+persist, not inside the derive.
        let context = MockScheduleContext::new(datetime(2026, 1, 2, 8));
        let session = Session::new(std::sync::Arc::new(context));
        derive_create_materialized_schedule_next_unix_seconds(
            &session,
            &tidb_sqlexec::BackgroundContext,
            "db",
            "mv",
            "",
            "",
            &utc_zone(),
            &|_, _, _, _, _| {},
        )
        .expect("derive runs");
        assert_eq!(session.session().install_calls.load(Ordering::SeqCst), 0);
        assert_eq!(session.session().restore_calls.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn nil_metadata_reports_no_update() {
        let context = MockScheduleContext::new(datetime(2026, 1, 2, 8));
        let session = Session::new(std::sync::Arc::new(context));
        assert_eq!(
            derive_create_materialized_view_next_unix_seconds(
                &session,
                &tidb_sqlexec::BackgroundContext,
                "db",
                "mv",
                None,
            )
            .expect("nil info"),
            (None, false)
        );
        assert_eq!(
            derive_create_materialized_view_log_next_unix_seconds(
                &session,
                &tidb_sqlexec::BackgroundContext,
                "db",
                "mlog",
                None,
            )
            .expect("nil info"),
            (None, false)
        );
    }
}

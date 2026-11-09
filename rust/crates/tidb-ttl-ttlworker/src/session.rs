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

//! Go `pkg/ttl/ttlworker/session.go`.
//!
//! `// boundary:` Go's `withSession` wraps the session's executor with
//! `statshandle.AttachStatsCollector` so TTL deletes feed the stats delta;
//! the Rust internal session records its own deltas, so there is no
//! collector to attach.

use std::sync::Arc;

use tidb_datatype::Datum;
use tidb_log::{Field, Value};
use tidb_logutil::bg_logger;
use tidb_timer::go_time::GoTime;
use tidb_ttl::cache::table::{new_physical_table, PhysicalTable};
use tidb_ttl::session::{ResultRow, SessionError, TtlSession, TxnMode};
use tidb_ttl_metrics::{PhaseTracer, PhaseTracerExt, PHASE_CHECK_TTL, PHASE_OTHER, PHASE_QUERY};

use crate::{session_now, str_arg, Result, Row, SctxRef, Session, SysSessionContext};

/// Go `syssession.Pool` as this package uses it, its session type erased.
pub trait SessionPool: Send + Sync {
    /// Go `withSession(pool, fn)`: a pooled session, prepared for TTL work
    /// and restored afterwards.
    fn with_session(&self, f: &mut dyn FnMut(&Session) -> Result<()>) -> Result<()>;
}

/// A `syssession.Pool` of [`SysSessionContext`]s.
pub struct SysSessionPool<C: SysSessionContext + ?Sized + 'static>(pub Arc<dyn tidb_syssession::Pool<C>>);

impl<C: SysSessionContext + ?Sized + 'static> SessionPool for SysSessionPool<C> {
    fn with_session(&self, f: &mut dyn FnMut(&Session) -> Result<()>) -> Result<()> {
        let mut outcome: Result<()> = Ok(());
        let pooled = self.0.with_session(&mut |s: &tidb_syssession::Session<C>| {
            let proxy = s.clone();
            let result = s.with_session_context(|sctx: &C| -> std::result::Result<Result<()>, tidb_syssession::SysSessionError> {
                let se = TtlSession::new(SctxRef(sctx.ttl_session_context()), Box::new(move || proxy.avoid_reuse()));
                let se: &Session = &se;
                let restore = match prepare_session(se) {
                    Ok(restore) => restore,
                    Err(err) => return Ok(Err(err)),
                };
                let result = f(se);
                restore(se);
                Ok(result)
            });
            match result {
                Ok(Ok(())) => Ok(()),
                Ok(Err(err)) => {
                    outcome = Err(err.clone());
                    Err(Box::new(SessionError(err)) as tidb_sqlexec::SqlExecError)
                }
                Err(err) => Err(Box::new(err) as tidb_sqlexec::SqlExecError),
            }
        });
        match (outcome, pooled) {
            (Err(err), _) => Err(err),
            (Ok(()), Err(err)) => Err(err.to_string()),
            (Ok(()), Ok(())) => Ok(()),
        }
    }
}

/// Go `withSession`.
pub fn with_session(pool: &dyn SessionPool, f: &mut dyn FnMut(&Session) -> Result<()>) -> Result<()> {
    pool.with_session(f)
}

fn exec(ctx: &tidb_gocontext::Context, se: &Session, sql: &str, args: &[Datum]) -> Result<Vec<Row>> {
    se.execute_sql(ctx, sql, args).map_err(|err| err.to_string())
}

/// Go `terror.Log(err)`.
fn terror_log(err: &str) {
    bg_logger().warn("encountered error", &[Field::new("error", Value::Str(err.to_owned()))]);
}

type Restore = Box<dyn FnOnce(&Session)>;

/// Go `prepareSession`: retry limit 0, 1PC and async commit on, no explicit
/// transaction, UTC, every read engine; the returned restore undoes it.
pub fn prepare_session(se: &Session) -> Result<Restore> {
    let vars = se.get_session_vars();
    let original_retry_limit = vars.retry_limit();
    let original_enable_1pc = vars.enable_1pc();
    let original_enable_async_commit = vars.enable_async_commit();

    // store and set the retry limit to 0
    exec(&tidb_gocontext::Context::background(), se, "set tidb_retry_limit=0", &[])?;
    // set enable 1pc to ON
    exec(&tidb_gocontext::Context::background(), se, "set tidb_enable_1pc=ON", &[])?;
    // set enable async commit to ON
    exec(&tidb_gocontext::Context::background(), se, "set tidb_enable_async_commit=ON", &[])?;
    // Force rollback the session to guarantee the session is not in any explicit transaction
    exec(&tidb_gocontext::Context::background(), se, "ROLLBACK", &[])?;

    // set the time zone to UTC
    let rows = exec(&tidb_gocontext::Context::background(), se, "select @@time_zone", &[])?;
    if rows.is_empty() || rows[0].is_empty() {
        return Err("failed to get time_zone variable".to_owned());
    }
    let original_time_zone = rows[0].get_string(0);
    exec(&tidb_gocontext::Context::background(), se, "set @@time_zone='UTC'", &[])?;
    // Go's `restoreTimeZone = true`: every restore that can run restores it.
    let restore_time_zone = true;

    // allow the session in TTL to use all read engines.
    let mut original_isolation_read_engines = String::new();
    let mut restore_isolation_read_engines = false;
    if !se.get_session_vars().isolation_read_engines_are_all() {
        let rows = exec(&tidb_gocontext::Context::background(), se, "select @@tidb_isolation_read_engines", &[])?;
        if rows.is_empty() || rows[0].is_empty() {
            return Err("failed to get tidb_isolation_read_engines variable".to_owned());
        }
        original_isolation_read_engines = rows[0].get_string(0);
        exec(&tidb_gocontext::Context::background(), se, "set tidb_isolation_read_engines='tikv,tiflash,tidb'", &[])?;
        restore_isolation_read_engines = true;
    }

    Ok(Box::new(move |se: &Session| {
        if let Err(err) = exec(&tidb_gocontext::Context::background(), se, &format!("set tidb_retry_limit={original_retry_limit}"), &[]) {
            bg_logger().warn(
                "fail to reset tidb_retry_limit",
                &[
                    Field::new("originalRetryLimit", Value::I64(original_retry_limit)),
                    Field::new("error", Value::Str(err)),
                ],
            );
            se.avoid_reuse();
            return;
        }
        let steps: [(bool, &str, Vec<Datum>); 4] = [
            (!original_enable_1pc, "set tidb_enable_1pc=OFF", Vec::new()),
            (!original_enable_async_commit, "set tidb_enable_async_commit=OFF", Vec::new()),
            (restore_time_zone, "set @@time_zone=%?", vec![str_arg(&original_time_zone)]),
            (
                restore_isolation_read_engines,
                "set tidb_isolation_read_engines=%?",
                vec![str_arg(&original_isolation_read_engines)],
            ),
        ];
        for (needed, sql, args) in steps {
            if !needed {
                continue;
            }
            if let Err(err) = exec(&tidb_gocontext::Context::background(), se, sql, &args) {
                terror_log(&err);
                se.avoid_reuse();
                return;
            }
        }
    }))
}

/// Go `ttlTableSession`: a session bound to one table and expire time.
pub struct TableSession<'a> {
    /// Go's embedded `session.Session`.
    pub session: &'a Session,
    tbl: &'a PhysicalTable,
    expire: GoTime,
}

/// Go `newTableSession`.
pub fn new_table_session<'a>(se: &'a Session, tbl: &'a PhysicalTable, expire: &GoTime) -> TableSession<'a> {
    TableSession {
        session: se,
        tbl,
        expire: expire.clone(),
    }
}

/// The restore Go's `NewScanSession` returns.
pub type ScanRestore = Box<dyn FnOnce(&Session) -> Result<()>>;

/// Go `NewScanSession`: one scan concurrency, no paging.
pub fn new_scan_session<'a>(
    ctx: &tidb_gocontext::Context,
    se: &'a Session,
    tbl: &'a PhysicalTable,
    expire: &GoTime,
) -> Result<(TableSession<'a>, ScanRestore)> {
    let orig_concurrency = se.get_session_vars().distsql_scan_concurrency();
    let orig_paging = se.get_session_vars().enable_paging();
    se.get_session_vars().set_internal_sql_scan_user_table(true);
    let restore = move |se: &Session| -> Result<()> {
        se.get_session_vars().set_internal_sql_scan_user_table(false);
        let mut err = exec(
            &tidb_gocontext::Context::background(),
            se,
            "set @@tidb_distsql_scan_concurrency=%?",
            &[Datum::new_int(orig_concurrency)],
        )
        .err();
        if let Some(err) = &err {
            terror_log(err);
            se.avoid_reuse();
        }
        if let Err(tmp_err) = exec(&tidb_gocontext::Context::background(), se, "set @@tidb_enable_paging=%?", &[Datum::new_int(i64::from(orig_paging))]) {
            err = Some(match err {
                Some(err) => format!("{err}; {tmp_err}"),
                None => tmp_err,
            });
            se.avoid_reuse();
        }
        err.map_or(Ok(()), Err)
    };

    // Set the distsql scan concurrency to 1 to reduce the number of cop tasks in TTL scan.
    if let Err(err) = exec(ctx, se, "set @@tidb_distsql_scan_concurrency=1", &[]) {
        if let Err(restore_err) = restore(se) {
            terror_log(&restore_err);
        }
        return Err(err);
    }

    // Disable tidb_enable_paging because we have already had a `LIMIT` in the SQL to limit the result set.
    // If `tidb_enable_paging` is enabled, it may have multiple cop tasks even in one region that makes some extra
    // processed keys in TiKV side, see issue: https://github.com/pingcap/tidb/issues/58342.
    // Disable it to make the scan more efficient.
    if let Err(err) = exec(ctx, se, "set @@tidb_enable_paging=OFF", &[]) {
        if let Err(restore_err) = restore(se) {
            terror_log(&restore_err);
        }
        return Err(err);
    }

    Ok((new_table_session(se, tbl, expire), Box::new(restore)))
}

impl TableSession<'_> {
    /// Go `ExecuteSQLWithCheck`: the rows, whether a failure may be retried,
    /// and the failure.
    pub fn execute_sql_with_check(
        &self,
        ctx: &tidb_gocontext::Context,
        tracer: Option<&PhaseTracer>,
        sql: &str,
    ) -> (Vec<Row>, bool, Option<String>) {
        let restore_phase = tracer.phase();
        let result = self.execute_sql_with_check_inner(ctx, tracer, sql);
        tracer.enter_phase(&restore_phase);
        result
    }

    fn execute_sql_with_check_inner(
        &self,
        ctx: &tidb_gocontext::Context,
        tracer: Option<&PhaseTracer>,
        sql: &str,
    ) -> (Vec<Row>, bool, Option<String>) {
        tracer.enter_phase(PHASE_OTHER);
        if !tidb_vardef::ENABLE_TTL_JOB.load(std::sync::atomic::Ordering::SeqCst) {
            return (Vec::new(), false, Some("global TTL job is disabled".to_owned()));
        }

        if let Err(err) = self.session.reset_with_global_time_zone(ctx) {
            return (Vec::new(), false, Some(err.to_string()));
        }

        let mut result = Vec::new();
        let mut should_retry = true;
        let err = self.session.run_in_txn(
            ctx,
            &mut || {
                tracer.enter_phase(PHASE_QUERY);
                let restore_phase = tracer.phase();
                let rows = self.session.execute_sql(ctx, sql, &[]);
                tracer.enter_phase(PHASE_CHECK_TTL);
                // We must check the configuration after ExecuteSQL because of MDL and the meta the current transaction used
                // can only be determined after executed one query.
                if let Err(validate_err) = validate_ttl_work(ctx, self.session, self.tbl, &self.expire) {
                    should_retry = false;
                    tracer.enter_phase(&restore_phase);
                    return Err(SessionError(format!(
                        "table '{}.{}' meta changed, should abort current job: {validate_err}",
                        self.tbl.schema.original(),
                        self.tbl.name_original()
                    )));
                }
                tracer.enter_phase(&restore_phase);
                result = rows?;
                Ok(())
            },
            TxnMode::OPTIMISTIC,
            tracer,
        );
        if let Err(err) = err {
            return (Vec::new(), should_retry, Some(err.to_string()));
        }
        (result, false, None)
    }
}

/// Go `tbl.EvalExpireTime(ctx, se, now)` over a Go `time.Time`.
pub fn eval_expire_time(ctx: &tidb_gocontext::Context, tbl: &PhysicalTable, se: &Session, now: &GoTime) -> Result<GoTime> {
    let expire = tbl
        .eval_expire_time(ctx, se, &now.instant())
        .map_err(|err| err.to_string())?;
    Ok(GoTime::from_instant(expire, now.location()))
}

/// Go `validateTTLWork`: whether the table's TTL meta still allows the work
/// that `tbl` and `expire` describe.
pub fn validate_ttl_work(ctx: &tidb_gocontext::Context, s: &Session, tbl: &PhysicalTable, expire: &GoTime) -> Result<()> {
    let new_tbl_info = s
        .session_info_schema()
        .0
        .table_info_by_name(&tbl.schema, &tbl.name())?;

    if tbl.table_info_ptr_eq(&new_tbl_info) {
        return Ok(());
    }

    if tbl.table_info.read().id != new_tbl_info.read().id {
        return Err("table id changed".to_owned());
    }

    let new_ttl_tbl = new_physical_table(tbl.schema.clone(), &new_tbl_info, tbl.partition.clone())
        .map_err(|err| err.to_string())?;

    if new_ttl_tbl.id != tbl.id {
        return Err("physical id changed".to_owned());
    }

    if !tbl.partition.lowercase().is_empty() {
        let new_name = new_ttl_tbl.partition_def.as_ref().map(|def| def.name.lowercase().to_owned());
        let old_name = tbl.partition_def.as_ref().map(|def| def.name.lowercase().to_owned());
        if new_name != old_name {
            return Err("partition name changed".to_owned());
        }
    }

    let ttl_info = |tbl_info: &tidb_model::GoShared<tidb_model::TableInfo>| {
        tbl_info.read().ttl_info.as_ref().map(|ttl| {
            let ttl = ttl.read();
            (ttl.enable, ttl.interval_expr_str.clone(), ttl.interval_time_unit)
        })
    };
    let (new_enable, new_expr, new_unit) = ttl_info(&new_ttl_tbl.table_info).unwrap_or_default();
    if !new_enable {
        return Err("table TTL disabled".to_owned());
    }

    let column_name = |tbl: &PhysicalTable| {
        tbl.time_column
            .as_ref()
            .map(|column| column.name.lowercase().to_owned())
            .unwrap_or_default()
    };
    if column_name(&new_ttl_tbl) != column_name(tbl) {
        return Err("time column name changed".to_owned());
    }

    let (_, old_expr, old_unit) = ttl_info(&tbl.table_info).unwrap_or_default();
    if new_expr != old_expr || new_unit != old_unit {
        let new_expire_time = eval_expire_time(ctx, &new_ttl_tbl, s, &session_now(s))?;
        if new_expire_time.before(expire) {
            return Err("expire interval changed".to_owned());
        }
    }

    Ok(())
}

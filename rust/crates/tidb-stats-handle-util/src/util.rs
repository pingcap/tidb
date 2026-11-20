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

use std::any::Any;
use std::collections::BTreeSet;
use std::fmt;
use std::panic::{catch_unwind, resume_unwind, AssertUnwindSafe};
use std::sync::{Arc, LazyLock};
use tidb_hack::GoToLower;
use tidb_log::{Field, Value};
use tidb_logutil::bg_logger;
use tidb_vardef::option_values::tidb_opt_on;

use tidb_datatype::{Datum, UNSPECIFIED_LENGTH};
use tidb_model::{IndexInfo, TableInfo};
use tidb_resolve::ResultFieldRef;
use tidb_sqlexec::{
    exec_option_use_cur_session, ExecutionContext, OptionFuncAlias, RecordSet,
    RestrictedSqlExecutor, SqlExecError,
};
use tidb_sqlexec_mock::MockRestrictedSqlExecutor;
use tidb_syssession::{Pool as SessionPool, SessionContext};
use tidb_sqlescape::SqlArg;
use tidb_vardef::tidb_vars;
use tikv_client::trace::TraceContext;
use tikv_client::util::with_internal_source_type;

/// Go `StatsMetaHistorySourceAnalyze`.
pub const STATS_META_HISTORY_SOURCE_ANALYZE: &str = "analyze";
/// Go `StatsMetaHistorySourceLoadStats`.
pub const STATS_META_HISTORY_SOURCE_LOAD_STATS: &str = "load stats";
/// Go `StatsMetaHistorySourceFlushStats`.
pub const STATS_META_HISTORY_SOURCE_FLUSH_STATS: &str = "flush stats";
/// Go `StatsMetaHistorySourceSchemaChange`.
pub const STATS_META_HISTORY_SOURCE_SCHEMA_CHANGE: &str = "schema change";
/// Go `StatsMetaHistorySourceExtendedStats`.
pub const STATS_META_HISTORY_SOURCE_EXTENDED_STATS: &str = "extended stats";

/// Go `FlagWrapTxn`.
pub const FLAG_WRAP_TXN: i32 = 0;

const INNODB_LOCK_WAIT_TIMEOUT: &str = "innodb_lock_wait_timeout";
const TIME_ZONE: &str = "time_zone";
const INTERNAL_STATS_FOREGROUND_PRIORITY: &str = "StatsForegroundPriority";

/// Go `UseCurrentSessionOpt`.
pub static USE_CURRENT_SESSION_OPT: LazyLock<Vec<OptionFuncAlias>> =
    LazyLock::new(|| vec![Arc::new(exec_option_use_cur_session)]);

/// Shared Go `StatsCtx` value.
pub static STATS_CONTEXT: LazyLock<TraceContext> = LazyLock::new(|| {
    with_internal_source_type(&TraceContext::new(), INTERNAL_STATS_FOREGROUND_PRIORITY)
});

/// The complete `sessionctx.Context` and `SessionVars` surface consumed by
/// this package.
pub trait StatsSessionContext: SessionContext {
    /// Go `GlobalVarsAccessor.GetGlobalSysVar`.
    fn global_system_var(&self, name: &str) -> Result<String, SqlExecError>;
    /// Go `EnableAsyncMergeGlobalStats`.
    fn set_enable_async_merge_global_stats(&self, enabled: bool);
    /// Go `AnalyzePartitionConcurrency`.
    fn set_analyze_partition_concurrency(&self, concurrency: i64);
    /// Go `AnalyzeVersion`.
    fn set_analyze_version(&self, version: i64);
    /// Go `SessionVars.SetSystemVar(tidb_analyze_store_batch_size, value)`.
    fn set_analyze_store_batch_size(&self, value: &str) -> Result<(), SqlExecError>;
    /// Go `EnableHistoricalStats`.
    fn set_enable_historical_stats(&self, enabled: bool);
    /// Go `PartitionPruneMode.Store`.
    fn set_partition_prune_mode(&self, mode: &str);
    /// Go `PartitionPruneMode.Load`.
    fn partition_prune_mode(&self) -> String;
    /// Go `EnableAnalyzeSnapshot`.
    fn set_enable_analyze_snapshot(&self, enabled: bool);
    /// Go `ParseAnalyzeSkipColumnTypes` plus assignment.
    fn set_analyze_skip_column_types(&self, value: BTreeSet<String>);
    /// Go `SkipMissingPartitionStats`.
    fn set_skip_missing_partition_stats(&self, enabled: bool);
    /// Go `LockWaitTimeout`, in milliseconds.
    fn set_lock_wait_timeout(&self, milliseconds: i64);
    /// Go `SessionVars.SetSystemVar(time_zone, value)`.
    fn set_time_zone(&self, value: &str) -> Result<(), SqlExecError>;
    /// Go `SessionVars.Location` after setting `time_zone`.
    fn location(&self) -> String;
    /// Go `StmtCtx.SetTimeZone`.
    fn set_statement_time_zone(&self, value: &str);
    /// Go `StmtCtx.TimeZone()`.
    fn statement_time_zone(&self) -> String;
    /// Go `SessionVars.GetSystemVar`: the session's value of `name`, `None`
    /// when the session has not loaded it.
    fn system_var(&self, name: &str) -> Option<String>;
    /// Go `SessionVars.AnalyzeSkipColumnTypes`.
    fn analyze_skip_column_types(&self) -> BTreeSet<String>;
    /// Go `SessionVars.EnableAnalyzeSnapshot`.
    fn enable_analyze_snapshot(&self) -> bool;
    /// Go `SessionVars.SkipMissingPartitionStats`.
    fn skip_missing_partition_stats(&self) -> bool;
    /// Go `SessionVars.AnalyzePartitionConcurrency`.
    fn analyze_partition_concurrency(&self) -> i64;
    /// Go `SessionVars.AnalyzeVersion`.
    fn analyze_version(&self) -> i64;
    /// Go `SessionVars.AnalyzeStoreBatchSize`.
    fn analyze_store_batch_size(&self) -> i64;
    /// Go `SessionVars.LockWaitTimeout`, in milliseconds.
    fn lock_wait_timeout(&self) -> i64;
    /// Go `Txn(true).StartTS`.
    fn transaction_start_ts(&self, active: bool) -> Result<u64, SqlExecError>;
    /// Go `GetStore().GetOracle().GetTimestamp`: a fresh timestamp.
    fn current_ts(&self) -> Result<u64, SqlExecError>;
    /// Go `StmtCtx.AffectedRows()`: the rows the last statement changed.
    fn affected_rows(&self) -> u64;
    /// Go `sctx.GetLatestInfoSchema().TableByID(tableID)`'s `Meta()`; `None`
    /// when the latest schema has no such table.
    fn latest_table_meta_by_id(&self, table_id: i64) -> Option<tidb_model::GoShared<TableInfo>>;
    /// The value stored under Go `mock.RestrictedSQLExecutorKey`.
    fn mock_restricted_sql_executor(&self) -> Option<Arc<MockRestrictedSqlExecutor>> {
        None
    }
}

#[derive(Debug)]
struct StatsUtilError(String);

impl fmt::Display for StatsUtilError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl std::error::Error for StatsUtilError {}

fn error(message: impl Into<String>) -> SqlExecError {
    Box::new(StatsUtilError(message.into()))
}

/// Go `variable.ParseAnalyzeSkipColumnTypes`.
pub fn parse_analyze_skip_column_types(value: &str) -> BTreeSet<String> {
    const ALLOWED: [&str; 7] = [
        "json",
        "text",
        "mediumtext",
        "longtext",
        "blob",
        "mediumblob",
        "longblob",
    ];
    value
        .go_to_lower()
        .split(',')
        .filter(|column_type| ALLOWED.contains(column_type))
        .map(ToOwned::to_owned)
        .collect()
}

fn parse_i64(name: &str, value: &str) -> Result<i64, SqlExecError> {
    value
        .parse::<i64>()
        .map_err(|parse_error| error(format!("parse {name}: {parse_error}")))
}

/// Go `statsSessionVarsSnapshot`: the session's statistics settings before
/// [`update_sctx_vars_for_stats`] replaced them.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct StatsSessionVarsSnapshot {
    analyze_skip_column_types: BTreeSet<String>,
    stmt_time_zone: String,
    partition_prune_mode: String,
    time_zone_value: String,
    analyze_store_batch_size_value: String,
    lock_wait_timeout: i64,
    analyze_version: i64,
    analyze_partition_concurrency: i64,
    skip_missing_partition_stats: bool,
    enable_analyze_snapshot: bool,
}

impl StatsSessionVarsSnapshot {
    fn restore<C: StatsSessionContext + ?Sized>(&self, context: &C) {
        if let Err(err) = context.set_analyze_store_batch_size(&self.analyze_store_batch_size_value) {
            bg_logger().warn(
                "restore analyze store batch size failed",
                &[Field::new("error", Value::Str(err.to_string()))],
            );
        }
        if let Err(err) = context.set_time_zone(&self.time_zone_value) {
            bg_logger().warn(
                "restore statistics session time zone failed",
                &[Field::new("error", Value::Str(err.to_string()))],
            );
        }
        context.set_analyze_skip_column_types(self.analyze_skip_column_types.clone());
        context.set_partition_prune_mode(&self.partition_prune_mode);
        context.set_enable_analyze_snapshot(self.enable_analyze_snapshot);
        context.set_skip_missing_partition_stats(self.skip_missing_partition_stats);
        context.set_analyze_partition_concurrency(self.analyze_partition_concurrency);
        context.set_analyze_version(self.analyze_version);
        context.set_lock_wait_timeout(self.lock_wait_timeout);
        context.set_statement_time_zone(&self.stmt_time_zone);
    }
}

/// Go `UpdateSCtxVarsForStats`'s returned restore function: [`Self::restore`]
/// or dropping it (Go `defer restoreStatsVars()`) puts the snapshot back.
#[must_use = "dropping the guard restores the session's statistics settings at once"]
pub struct RestoreStatsVars<'a, C: StatsSessionContext + ?Sized> {
    context: &'a C,
    snapshot: Option<StatsSessionVarsSnapshot>,
}

impl<C: StatsSessionContext + ?Sized> RestoreStatsVars<'_, C> {
    /// Go `restore()`.
    pub fn restore(mut self) {
        self.restore_now();
    }

    fn restore_now(&mut self) {
        if let Some(snapshot) = self.snapshot.take() {
            snapshot.restore(self.context);
        }
    }
}

impl<C: StatsSessionContext + ?Sized> Drop for RestoreStatsVars<'_, C> {
    fn drop(&mut self) {
        self.restore_now();
    }
}

/// Go `UpdateSCtxVarsForStats`: updates every variable that may affect the
/// behavior of statistics. It restores the original settings on failure and
/// returns the guard that restores them after use on success.
pub fn update_sctx_vars_for_stats<C: StatsSessionContext + ?Sized>(
    context: &C,
) -> Result<RestoreStatsVars<'_, C>, SqlExecError> {
    // These setters update both cached system variables and typed fields. Unloaded
    // session variables must retain the session's original values, not current globals.
    let snapshot = StatsSessionVarsSnapshot {
        analyze_skip_column_types: context.analyze_skip_column_types(),
        stmt_time_zone: context.statement_time_zone(),
        partition_prune_mode: context.partition_prune_mode(),
        time_zone_value: context.system_var(TIME_ZONE).unwrap_or_else(|| context.location()),
        analyze_store_batch_size_value: context
            .system_var(tidb_vars::TIDB_ANALYZE_STORE_BATCH_SIZE)
            .unwrap_or_else(|| context.analyze_store_batch_size().to_string()),
        lock_wait_timeout: context.lock_wait_timeout(),
        analyze_version: context.analyze_version(),
        analyze_partition_concurrency: context.analyze_partition_concurrency(),
        skip_missing_partition_stats: context.skip_missing_partition_stats(),
        enable_analyze_snapshot: context.enable_analyze_snapshot(),
    };
    // On an error the guard drops here: Go `restoreOnErr`.
    let restore = RestoreStatsVars { context, snapshot: Some(snapshot) };

    let value = context.global_system_var(tidb_vars::TIDB_ENABLE_ASYNC_MERGE_GLOBAL_STATS)?;
    context.set_enable_async_merge_global_stats(tidb_opt_on(&value));

    // concurrency of save stats to storage
    let value = context.global_system_var(tidb_vars::TIDB_ANALYZE_PARTITION_CONCURRENCY)?;
    context.set_analyze_partition_concurrency(parse_i64(
        tidb_vars::TIDB_ANALYZE_PARTITION_CONCURRENCY,
        &value,
    )?);

    // analyzer version
    let value = context.global_system_var(tidb_vars::TIDB_ANALYZE_VERSION)?;
    context.set_analyze_version(parse_i64(tidb_vars::TIDB_ANALYZE_VERSION, &value)?);

    // Auto Analyze uses the latest global value instead of the potentially
    // stale value held by its pooled internal session.
    let value = context.global_system_var(tidb_vars::TIDB_ANALYZE_STORE_BATCH_SIZE)?;
    context.set_analyze_store_batch_size(&value)?;

    let value = context.global_system_var(tidb_vars::TIDB_ENABLE_HISTORICAL_STATS)?;
    context.set_enable_historical_stats(tidb_opt_on(&value));

    // partition mode
    let value = context.global_system_var(tidb_vars::TIDB_PARTITION_PRUNE_MODE)?;
    context.set_partition_prune_mode(&value);

    // enable analyze snapshot
    let value = context.global_system_var(tidb_vars::TIDB_ENABLE_ANALYZE_SNAPSHOT)?;
    context.set_enable_analyze_snapshot(tidb_opt_on(&value));

    // enable skip column types
    let value = context.global_system_var(tidb_vars::TIDB_ANALYZE_SKIP_COLUMN_TYPES)?;
    context.set_analyze_skip_column_types(parse_analyze_skip_column_types(&value));

    // skip missing partition stats
    let value = context.global_system_var(tidb_vars::TIDB_SKIP_MISSING_PARTITION_STATS)?;
    context.set_skip_missing_partition_stats(tidb_opt_on(&value));

    // sync innodb_lock_wait_timeout
    let value = context.global_system_var(INNODB_LOCK_WAIT_TIMEOUT)?;
    context.set_lock_wait_timeout(parse_i64(INNODB_LOCK_WAIT_TIMEOUT, &value)?.wrapping_mul(1_000));

    // timezone used to datetime/timestamp conversion when collecting stats.
    let value = context.global_system_var(TIME_ZONE)?;
    context.set_time_zone(&value)?;
    let location = context.location();
    context.set_statement_time_zone(&location);
    Ok(restore)
}

fn panic_text(payload: &(dyn Any + Send)) -> String {
    if let Some(message) = payload.downcast_ref::<&str>() {
        (*message).to_owned()
    } else if let Some(message) = payload.downcast_ref::<String>() {
        message.clone()
    } else {
        String::new()
    }
}

/// Go `CallWithSCtx`.
pub fn call_with_sctx<C, F>(
    pool: &dyn SessionPool<C>,
    callback: F,
    flags: &[i32],
) -> Result<(), SqlExecError>
where
    C: StatsSessionContext + ?Sized + 'static,
    F: FnOnce(&C) -> Result<(), SqlExecError>,
{
    let wrap_transaction = flags.contains(&FLAG_WRAP_TXN);
    let mut callback = Some(callback);
    let result = catch_unwind(AssertUnwindSafe(|| {
        pool.with_session(&mut |session| {
            session.with_session_context(|context| {
                // update stats variables automatically
                let _restore_stats_vars = update_sctx_vars_for_stats(context)?;
                let callback = callback
                    .take()
                    .expect("CallWithSCtx callback called more than once");
                if wrap_transaction {
                    wrap_txn(context, callback)
                } else {
                    callback(context)
                }
            })
        })
    }));
    match result {
        Ok(result) => result,
        Err(payload)
            if tidb_intest::IN_TEST
                && panic_text(payload.as_ref()).contains("assert failed") =>
        {
            resume_unwind(payload)
        }
        Err(_) => Ok(()),
    }
}

/// Go `GetCurrentPruneMode`.
pub fn get_current_prune_mode<C: StatsSessionContext + ?Sized + 'static>(
    pool: &dyn SessionPool<C>,
) -> Result<String, SqlExecError> {
    let mut mode = String::new();
    call_with_sctx(
        pool,
        |context| {
            mode = context.partition_prune_mode();
            Ok(())
        },
        &[],
    )?;
    Ok(mode)
}

/// Go `WrapTxn`.
pub fn wrap_txn<C, F>(context: &C, callback: F) -> Result<(), SqlExecError>
where
    C: StatsSessionContext + ?Sized,
    F: FnOnce(&C) -> Result<(), SqlExecError>,
{
    exec_rows(context, "BEGIN PESSIMISTIC", &[])?;
    match catch_unwind(AssertUnwindSafe(|| callback(context))) {
        Ok(Ok(())) => {
            exec_rows(context, "COMMIT", &[])?;
            Ok(())
        }
        Ok(Err(original)) => {
            let _ = exec_rows(context, "rollback", &[]);
            Err(original)
        }
        Err(payload) => {
            // Go assigns the callback result to the named return only after
            // the call finishes. If it panics, the deferred transaction
            // finisher therefore observes nil and attempts COMMIT before the
            // outer CallWithSCtx recovery runs.
            let _ = exec_rows(context, "COMMIT", &[]);
            resume_unwind(payload)
        }
    }
}

/// Go `GetStartTS`.
pub fn get_start_ts<C: StatsSessionContext + ?Sized>(context: &C) -> Result<u64, SqlExecError> {
    context.transaction_start_ts(true)
}

/// Go `Exec`.
pub fn exec<C: StatsSessionContext + ?Sized>(
    context: &C,
    sql: &str,
    arguments: &[SqlArg<'_>],
) -> Result<Option<Box<dyn RecordSet>>, SqlExecError> {
    exec_with_ctx(&*STATS_CONTEXT, context, sql, arguments)
}

/// Go `ExecWithCtx`.
pub fn exec_with_ctx<C: StatsSessionContext + ?Sized>(
    execution_context: &dyn ExecutionContext,
    context: &C,
    sql: &str,
    arguments: &[SqlArg<'_>],
) -> Result<Option<Box<dyn RecordSet>>, SqlExecError> {
    context
        .sql_executor()
        .execute_internal(execution_context, sql, arguments)
}

/// Go `ExecRows`.
pub fn exec_rows<C: StatsSessionContext + ?Sized>(
    context: &C,
    sql: &str,
    arguments: &[SqlArg<'_>],
) -> Result<(Vec<Vec<Datum>>, Vec<ResultFieldRef>), SqlExecError> {
    exec_rows_with_ctx(&*STATS_CONTEXT, context, sql, arguments)
}

/// Go `ExecRowsWithCtx`.
pub fn exec_rows_with_ctx<C: StatsSessionContext + ?Sized>(
    execution_context: &dyn ExecutionContext,
    context: &C,
    sql: &str,
    arguments: &[SqlArg<'_>],
) -> Result<(Vec<Vec<Datum>>, Vec<ResultFieldRef>), SqlExecError> {
    #[cfg(feature = "failpoints")]
    if tidb_failpoint::eval("github.com/pingcap/tidb/pkg/statistics/handle/util/ExecRowsTimeout").is_some() {
        return Err(error("inject timeout error"));
    }
    if tidb_intest::IN_TEST {
        if let Some(mock) = context.mock_restricted_sql_executor() {
            return mock.exec_restricted_sql(
                &*STATS_CONTEXT,
                USE_CURRENT_SESSION_OPT.as_slice(),
                sql,
                arguments,
            );
        }
    }
    context.restricted_sql_executor().exec_restricted_sql(
        execution_context,
        USE_CURRENT_SESSION_OPT.as_slice(),
        sql,
        arguments,
    )
}

/// Go `ExecWithOpts`.
pub fn exec_with_opts<C: StatsSessionContext + ?Sized>(
    context: &C,
    options: &[OptionFuncAlias],
    sql: &str,
    arguments: &[SqlArg<'_>],
) -> Result<(Vec<Vec<Datum>>, Vec<ResultFieldRef>), SqlExecError> {
    exec_with_opts_with_ctx(&*STATS_CONTEXT, context, options, sql, arguments)
}

/// Go `ExecWithOptsWithCtx`.
pub fn exec_with_opts_with_ctx<C: StatsSessionContext + ?Sized>(
    execution_context: &dyn ExecutionContext,
    context: &C,
    options: &[OptionFuncAlias],
    sql: &str,
    arguments: &[SqlArg<'_>],
) -> Result<(Vec<Vec<Datum>>, Vec<ResultFieldRef>), SqlExecError> {
    context.restricted_sql_executor().exec_restricted_sql(
        execution_context,
        options,
        sql,
        arguments,
    )
}

/// Go `DurationToTS`. `duration_nanoseconds` is the signed representation of
/// Go `time.Duration`.
pub fn duration_to_ts(duration_nanoseconds: i64) -> u64 {
    tikv_client::oracle::compose_ts(duration_nanoseconds / 1_000_000, 0)
}

/// Go `IsSpecialGlobalIndex` over the package-owned model types.
pub fn is_special_global_index(index: &IndexInfo, table: &TableInfo) -> bool {
    if !index.global {
        return false;
    }
    index.columns.iter_deref().any(|index_column| {
        let index_column = index_column.read();
        let offset = usize::try_from(index_column.offset).expect("negative index column offset");
        let table_column = table
            .columns
            .get(offset)
            .expect("index column offset outside table columns");
        table_column.read().is_virtual_generated() || index_column.length != UNSPECIFIED_LENGTH
    })
}

#[cfg(test)]
mod tests {
    use std::any::TypeId;
    use std::collections::HashMap;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};

    use tidb_ast::Stmt;
    use tidb_datatype::{Datum, UNSPECIFIED_LENGTH};
    use tidb_model::{ColumnInfo, GoSharedPointerSlice, IndexColumn, IndexInfo, TableInfo};
    use tidb_resolve::ResultFieldRef;
    #[cfg(feature = "intest")]
    use tidb_sqlexec::{exec_option, BackgroundContext};
    use tidb_sqlexec::{
        ExecutionContext, OptionFuncAlias, RecordSet, RestrictedSqlExecutor, SqlExecutor,
    };
    use tidb_syssession::{AdvancedSessionPool, SessionContext};

    use super::*;

    #[derive(Default)]
    struct RecordingExecutor {
        calls: Mutex<Vec<String>>,
        context_types: Mutex<Vec<TypeId>>,
        errors: Mutex<HashMap<String, String>>,
    }

    impl RecordingExecutor {
        fn answer(
            &self,
            sql: &str,
        ) -> Result<(Vec<Vec<Datum>>, Vec<ResultFieldRef>), SqlExecError> {
            self.calls.lock().unwrap().push(sql.to_owned());
            if let Some(message) = self.errors.lock().unwrap().get(sql) {
                return Err(error(message.clone()));
            }
            Ok((Vec::new(), Vec::new()))
        }
    }

    impl RestrictedSqlExecutor for RecordingExecutor {
        fn parse_with_params(
            &self,
            _context: &dyn ExecutionContext,
            _sql: &str,
            _arguments: &[SqlArg<'_>],
        ) -> Result<Stmt, SqlExecError> {
            Err(error("parse not used"))
        }

        fn exec_restricted_stmt(
            &self,
            _context: &dyn ExecutionContext,
            _statement: &Stmt,
            _options: &[OptionFuncAlias],
        ) -> Result<(Vec<Vec<Datum>>, Vec<ResultFieldRef>), SqlExecError> {
            Err(error("statement not used"))
        }

        fn exec_restricted_sql(
            &self,
            context: &dyn ExecutionContext,
            _options: &[OptionFuncAlias],
            sql: &str,
            _arguments: &[SqlArg<'_>],
        ) -> Result<(Vec<Vec<Datum>>, Vec<ResultFieldRef>), SqlExecError> {
            self.context_types
                .lock()
                .unwrap()
                .push(context.as_any().type_id());
            self.answer(sql)
        }
    }

    impl SqlExecutor for RecordingExecutor {
        fn execute(
            &self,
            _context: &dyn ExecutionContext,
            _sql: &str,
        ) -> Result<Vec<Box<dyn RecordSet>>, SqlExecError> {
            Ok(Vec::new())
        }

        fn execute_internal(
            &self,
            _context: &dyn ExecutionContext,
            sql: &str,
            _arguments: &[SqlArg<'_>],
        ) -> Result<Option<Box<dyn RecordSet>>, SqlExecError> {
            self.calls.lock().unwrap().push(sql.to_owned());
            Ok(None)
        }

        fn execute_stmt(
            &self,
            _context: &dyn ExecutionContext,
            _statement: &Stmt,
        ) -> Result<Option<Box<dyn RecordSet>>, SqlExecError> {
            Ok(None)
        }
    }

    #[derive(Default)]
    struct StatsState {
        async_merge: bool,
        partition_concurrency: i64,
        analyze_version: i64,
        /// The loaded `tidb_analyze_store_batch_size` (empty: not loaded).
        analyze_store_batch_size: String,
        /// Go `AnalyzeStoreBatchSize`.
        batch_size_typed: i64,
        historical: bool,
        prune_mode: String,
        snapshot: bool,
        skip_types: BTreeSet<String>,
        skip_missing: bool,
        lock_wait_ms: i64,
        /// The loaded `time_zone` variable.
        time_zone_var: Option<String>,
        /// Go `TimeZone`'s name, `SYSTEM` for nil.
        location: String,
        statement_location: String,
    }

    struct MockContext {
        globals: Mutex<HashMap<String, String>>,
        reads: Mutex<Vec<String>>,
        state: Mutex<StatsState>,
        executor: Arc<RecordingExecutor>,
        mock: Mutex<Option<Arc<MockRestrictedSqlExecutor>>>,
        closed: AtomicUsize,
        registered: AtomicBool,
        rollbacks: AtomicUsize,
        start_ts: u64,
    }

    impl MockContext {
        fn new() -> Self {
            Self {
                globals: Mutex::new(HashMap::from([
                    (
                        tidb_vars::TIDB_ENABLE_ASYNC_MERGE_GLOBAL_STATS.to_owned(),
                        "ON".to_owned(),
                    ),
                    (
                        tidb_vars::TIDB_ANALYZE_PARTITION_CONCURRENCY.to_owned(),
                        "2".to_owned(),
                    ),
                    (tidb_vars::TIDB_ANALYZE_VERSION.to_owned(), "2".to_owned()),
                    (
                        tidb_vars::TIDB_ANALYZE_STORE_BATCH_SIZE.to_owned(),
                        "4".to_owned(),
                    ),
                    (
                        tidb_vars::TIDB_ENABLE_HISTORICAL_STATS.to_owned(),
                        "1".to_owned(),
                    ),
                    (
                        tidb_vars::TIDB_PARTITION_PRUNE_MODE.to_owned(),
                        "dynamic".to_owned(),
                    ),
                    (
                        tidb_vars::TIDB_ENABLE_ANALYZE_SNAPSHOT.to_owned(),
                        "OFF".to_owned(),
                    ),
                    (
                        tidb_vars::TIDB_ANALYZE_SKIP_COLUMN_TYPES.to_owned(),
                        "json,blob".to_owned(),
                    ),
                    (
                        tidb_vars::TIDB_SKIP_MISSING_PARTITION_STATS.to_owned(),
                        "ON".to_owned(),
                    ),
                    (INNODB_LOCK_WAIT_TIMEOUT.to_owned(), "50".to_owned()),
                    (TIME_ZONE.to_owned(), "UTC".to_owned()),
                ])),
                reads: Mutex::new(Vec::new()),
                // Go `mock.NewContext`: a SYSTEM session zone, UTC statements.
                state: Mutex::new(StatsState {
                    batch_size_typed: tidb_vardef::defaults::DEF_TIDB_ANALYZE_STORE_BATCH_SIZE,
                    location: "SYSTEM".to_owned(),
                    statement_location: "UTC".to_owned(),
                    ..StatsState::default()
                }),
                executor: Arc::new(RecordingExecutor::default()),
                mock: Mutex::new(None),
                closed: AtomicUsize::new(0),
                registered: AtomicBool::new(false),
                rollbacks: AtomicUsize::new(0),
                start_ts: 123,
            }
        }
    }

    impl SessionContext for MockContext {
        fn close(&self) {
            self.closed.fetch_add(1, Ordering::SeqCst);
        }

        fn rollback_txn(&self, _context: &dyn ExecutionContext) {
            self.rollbacks.fetch_add(1, Ordering::SeqCst);
        }

        fn has_prepared_txn_future(&self) -> bool {
            false
        }

        fn txn_valid(&self) -> Result<bool, SqlExecError> {
            Ok(false)
        }

        fn sql_executor(&self) -> Arc<dyn SqlExecutor> {
            self.executor.clone()
        }

        fn restricted_sql_executor(&self) -> Arc<dyn RestrictedSqlExecutor> {
            self.executor.clone()
        }

        fn register_internal_session(&self) {
            self.registered.store(true, Ordering::SeqCst);
        }

        fn unregister_internal_session(&self) {
            self.registered.store(false, Ordering::SeqCst);
        }

        fn contains_internal_session(&self) -> bool {
            self.registered.load(Ordering::SeqCst)
        }

        fn store_internal_session(&self) -> bool {
            self.registered.store(true, Ordering::SeqCst);
            true
        }
    }

    impl StatsSessionContext for MockContext {
        fn affected_rows(&self) -> u64 {
            0
        }

        fn latest_table_meta_by_id(
            &self,
            _table_id: i64,
        ) -> Option<tidb_model::GoShared<TableInfo>> {
            None
        }

        fn global_system_var(&self, name: &str) -> Result<String, SqlExecError> {
            self.reads.lock().unwrap().push(name.to_owned());
            self.globals
                .lock()
                .unwrap()
                .get(name)
                .cloned()
                .ok_or_else(|| error(format!("missing {name}")))
        }

        fn set_enable_async_merge_global_stats(&self, enabled: bool) {
            self.state.lock().unwrap().async_merge = enabled;
        }

        fn set_analyze_partition_concurrency(&self, concurrency: i64) {
            self.state.lock().unwrap().partition_concurrency = concurrency;
        }

        fn set_analyze_version(&self, version: i64) {
            self.state.lock().unwrap().analyze_version = version;
        }

        fn set_analyze_store_batch_size(&self, value: &str) -> Result<(), SqlExecError> {
            // Go `SetSystemVar`'s integer validation.
            let typed = parse_i64(tidb_vars::TIDB_ANALYZE_STORE_BATCH_SIZE, value)?;
            let mut state = self.state.lock().unwrap();
            state.analyze_store_batch_size = value.to_owned();
            state.batch_size_typed = typed;
            Ok(())
        }

        fn set_enable_historical_stats(&self, enabled: bool) {
            self.state.lock().unwrap().historical = enabled;
        }

        fn set_partition_prune_mode(&self, mode: &str) {
            self.state.lock().unwrap().prune_mode = mode.to_owned();
        }

        fn partition_prune_mode(&self) -> String {
            self.state.lock().unwrap().prune_mode.clone()
        }

        fn set_enable_analyze_snapshot(&self, enabled: bool) {
            self.state.lock().unwrap().snapshot = enabled;
        }

        fn set_analyze_skip_column_types(&self, value: BTreeSet<String>) {
            self.state.lock().unwrap().skip_types = value;
        }

        fn set_skip_missing_partition_stats(&self, enabled: bool) {
            self.state.lock().unwrap().skip_missing = enabled;
        }

        fn set_lock_wait_timeout(&self, milliseconds: i64) {
            self.state.lock().unwrap().lock_wait_ms = milliseconds;
        }

        fn set_time_zone(&self, value: &str) -> Result<(), SqlExecError> {
            // Go `SetSystemVar(time_zone)`'s validation, for the zones these
            // tests use.
            let offset = value.len() == 6
                && (value.starts_with('+') || value.starts_with('-'))
                && value.as_bytes()[3] == b':';
            if !(offset || value == "SYSTEM" || value == "UTC" || value.contains('/')) {
                return Err(error(format!("unknown or incorrect time zone: '{value}'")));
            }
            let mut state = self.state.lock().unwrap();
            state.time_zone_var = Some(value.to_owned());
            state.location = value.to_owned();
            Ok(())
        }

        fn location(&self) -> String {
            self.state.lock().unwrap().location.clone()
        }

        fn set_statement_time_zone(&self, value: &str) {
            self.state.lock().unwrap().statement_location = value.to_owned();
        }

        fn statement_time_zone(&self) -> String {
            self.state.lock().unwrap().statement_location.clone()
        }

        fn system_var(&self, name: &str) -> Option<String> {
            let state = self.state.lock().unwrap();
            match name {
                tidb_vars::TIDB_ANALYZE_STORE_BATCH_SIZE => {
                    (!state.analyze_store_batch_size.is_empty()).then(|| state.analyze_store_batch_size.clone())
                }
                TIME_ZONE => state.time_zone_var.clone(),
                _ => None,
            }
        }

        fn analyze_skip_column_types(&self) -> BTreeSet<String> {
            self.state.lock().unwrap().skip_types.clone()
        }

        fn enable_analyze_snapshot(&self) -> bool {
            self.state.lock().unwrap().snapshot
        }

        fn skip_missing_partition_stats(&self) -> bool {
            self.state.lock().unwrap().skip_missing
        }

        fn analyze_partition_concurrency(&self) -> i64 {
            self.state.lock().unwrap().partition_concurrency
        }

        fn analyze_version(&self) -> i64 {
            self.state.lock().unwrap().analyze_version
        }

        fn analyze_store_batch_size(&self) -> i64 {
            self.state.lock().unwrap().batch_size_typed
        }

        fn lock_wait_timeout(&self) -> i64 {
            self.state.lock().unwrap().lock_wait_ms
        }

        fn transaction_start_ts(&self, active: bool) -> Result<u64, SqlExecError> {
            assert!(active);
            Ok(self.start_ts)
        }

        fn current_ts(&self) -> Result<u64, SqlExecError> {
            Ok(self.start_ts)
        }

        fn mock_restricted_sql_executor(&self) -> Option<Arc<MockRestrictedSqlExecutor>> {
            self.mock.lock().unwrap().clone()
        }
    }

    #[test]
    fn session_variables_are_synchronized_in_source_order() {
        let context = MockContext::new();
        let _restore = update_sctx_vars_for_stats(&context).unwrap();
        assert_eq!(
            *context.reads.lock().unwrap(),
            [
                tidb_vars::TIDB_ENABLE_ASYNC_MERGE_GLOBAL_STATS,
                tidb_vars::TIDB_ANALYZE_PARTITION_CONCURRENCY,
                tidb_vars::TIDB_ANALYZE_VERSION,
                tidb_vars::TIDB_ANALYZE_STORE_BATCH_SIZE,
                tidb_vars::TIDB_ENABLE_HISTORICAL_STATS,
                tidb_vars::TIDB_PARTITION_PRUNE_MODE,
                tidb_vars::TIDB_ENABLE_ANALYZE_SNAPSHOT,
                tidb_vars::TIDB_ANALYZE_SKIP_COLUMN_TYPES,
                tidb_vars::TIDB_SKIP_MISSING_PARTITION_STATS,
                INNODB_LOCK_WAIT_TIMEOUT,
                TIME_ZONE,
            ]
        );
        let state = context.state.lock().unwrap();
        assert!(state.async_merge);
        assert_eq!(state.partition_concurrency, 2);
        assert_eq!(state.analyze_version, 2);
        assert_eq!(state.analyze_store_batch_size, "4");
        assert!(state.historical);
        assert_eq!(state.prune_mode, "dynamic");
        assert!(!state.snapshot);
        assert_eq!(
            state.skip_types,
            BTreeSet::from(["blob".to_owned(), "json".to_owned()])
        );
        assert!(state.skip_missing);
        assert_eq!(state.lock_wait_ms, 50_000);
        assert_eq!(state.location, "UTC");
        assert_eq!(state.statement_location, "UTC");
    }

    #[test]
    fn analyze_store_batch_size_is_loaded_before_historical_stats() {
        let context = MockContext::new();
        let _restore = update_sctx_vars_for_stats(&context).unwrap();
        let reads = context.reads.lock().unwrap();
        let version = reads
            .iter()
            .position(|name| name == tidb_vars::TIDB_ANALYZE_VERSION)
            .unwrap();
        let batch_size = reads
            .iter()
            .position(|name| name == tidb_vars::TIDB_ANALYZE_STORE_BATCH_SIZE)
            .unwrap();
        let historical = reads
            .iter()
            .position(|name| name == tidb_vars::TIDB_ENABLE_HISTORICAL_STATS)
            .unwrap();
        assert!(version < batch_size && batch_size < historical);
    }

    #[test]
    fn exec_with_opts_with_ctx_forwards_the_caller_context() {
        let context = MockContext::new();
        let caller_context = tidb_sqlexec::BackgroundContext;
        exec_with_opts_with_ctx(&caller_context, &context, &[], "select 1", &[]).unwrap();
        assert_eq!(
            *context.executor.context_types.lock().unwrap(),
            [TypeId::of::<tidb_sqlexec::BackgroundContext>()]
        );
    }

    #[test]
    fn session_variable_failure_retains_earlier_mutations_only() {
        let context = MockContext::new();
        context.globals.lock().unwrap().insert(
            tidb_vars::TIDB_ANALYZE_PARTITION_CONCURRENCY.to_owned(),
            "not-an-int".to_owned(),
        );
        assert!(update_sctx_vars_for_stats(&context).is_err());
        let state = context.state.lock().unwrap();
        assert!(state.async_merge);
        assert_eq!(state.partition_concurrency, 0);
        assert_eq!(state.analyze_version, 0);
        assert_eq!(
            *context.reads.lock().unwrap(),
            [
                tidb_vars::TIDB_ENABLE_ASYNC_MERGE_GLOBAL_STATS,
                tidb_vars::TIDB_ANALYZE_PARTITION_CONCURRENCY,
            ]
        );
    }

    #[test]
    fn wrap_transaction_commits_success_and_preserves_original_error() {
        let context = MockContext::new();
        wrap_txn(&context, |_| Ok(())).unwrap();
        assert_eq!(
            *context.executor.calls.lock().unwrap(),
            ["BEGIN PESSIMISTIC", "COMMIT"]
        );

        context.executor.calls.lock().unwrap().clear();
        context
            .executor
            .errors
            .lock()
            .unwrap()
            .insert("rollback".to_owned(), "rollback error".to_owned());
        let original = wrap_txn(&context, |_| Err(error("original"))).unwrap_err();
        assert_eq!(original.to_string(), "original");
        assert_eq!(
            *context.executor.calls.lock().unwrap(),
            ["BEGIN PESSIMISTIC", "rollback"]
        );

        context.executor.calls.lock().unwrap().clear();
        context.executor.errors.lock().unwrap().clear();
        context
            .executor
            .errors
            .lock()
            .unwrap()
            .insert("COMMIT".to_owned(), "commit error".to_owned());
        assert_eq!(
            wrap_txn(&context, |_| Ok(())).unwrap_err().to_string(),
            "commit error"
        );
        assert_eq!(
            *context.executor.calls.lock().unwrap(),
            ["BEGIN PESSIMISTIC", "COMMIT"]
        );

        context.executor.calls.lock().unwrap().clear();
        assert!(catch_unwind(AssertUnwindSafe(|| {
            let _ = wrap_txn(&context, |_| -> Result<(), SqlExecError> {
                panic!("callback panic")
            });
        }))
        .is_err());
        assert_eq!(
            *context.executor.calls.lock().unwrap(),
            ["BEGIN PESSIMISTIC", "COMMIT"]
        );
    }

    #[test]
    #[cfg(feature = "intest")]
    fn restricted_mock_path_uses_stats_context_and_current_session_option() {
        let context = MockContext::new();
        let mock = Arc::new(MockRestrictedSqlExecutor::new());
        mock.expect()
            .exec_restricted_sql(|execution_context, options, sql, arguments| {
                let stats = execution_context
                    .as_any()
                    .downcast_ref::<TraceContext>()
                    .unwrap();
                assert_eq!(
                    tikv_client::util::request_source_from_context(stats),
                    "internal_StatsForegroundPriority"
                );
                assert!(exec_option(options).use_cur_session);
                assert_eq!(sql, "select 1");
                assert!(arguments.is_empty());
                Ok((Vec::new(), Vec::new()))
            });
        *context.mock.lock().unwrap() = Some(Arc::clone(&mock));
        exec_rows_with_ctx(&BackgroundContext, &context, "select 1", &[]).unwrap();
        context.mock.lock().unwrap().take();
        mock.verify();
    }

    /// Go `newStatsSessionContextForRestoreTest`: each setting's GLOBAL and
    /// session values.
    fn new_stats_session_context_for_restore_test() -> MockContext {
        let context = MockContext::new();
        let mut globals = context.globals.lock().unwrap();
        for (name, global) in [
            (tidb_vars::TIDB_ANALYZE_PARTITION_CONCURRENCY, "3"),
            (tidb_vars::TIDB_ANALYZE_VERSION, "2"),
            (tidb_vars::TIDB_ANALYZE_STORE_BATCH_SIZE, "6"),
            (tidb_vars::TIDB_PARTITION_PRUNE_MODE, "dynamic"),
            (tidb_vars::TIDB_ENABLE_ANALYZE_SNAPSHOT, "ON"),
            (tidb_vars::TIDB_ANALYZE_SKIP_COLUMN_TYPES, "blob"),
            (tidb_vars::TIDB_SKIP_MISSING_PARTITION_STATS, "ON"),
            (INNODB_LOCK_WAIT_TIMEOUT, "12"),
            (TIME_ZONE, "+03:00"),
        ] {
            globals.insert(name.to_owned(), global.to_owned());
        }
        drop(globals);
        *context.state.lock().unwrap() = StatsState {
            partition_concurrency: 1,
            // Go `vars.AnalyzeVersion = 1` after its session settings.
            analyze_version: 1,
            analyze_store_batch_size: "7".to_owned(),
            batch_size_typed: 7,
            prune_mode: "static".to_owned(),
            snapshot: false,
            skip_types: BTreeSet::from(["json".to_owned()]),
            skip_missing: false,
            lock_wait_ms: 45_000,
            time_zone_var: Some("+08:00".to_owned()),
            location: "+08:00".to_owned(),
            statement_location: "UTC".to_owned(),
            ..StatsState::default()
        };
        context
    }

    /// Go `checkRestoredStatsSessionVars`.
    fn check_restored_stats_session_vars(context: &MockContext) {
        assert_eq!(1, context.analyze_partition_concurrency());
        assert_eq!(1, context.analyze_version());
        assert_eq!(7, context.analyze_store_batch_size());
        assert_eq!("static", context.partition_prune_mode());
        assert!(!context.enable_analyze_snapshot());
        assert_eq!(BTreeSet::from(["json".to_owned()]), context.analyze_skip_column_types());
        assert!(!context.skip_missing_partition_stats());
        assert_eq!(45_000, context.lock_wait_timeout());
        assert_eq!("+08:00", context.location());
        assert_eq!("UTC", context.statement_time_zone());
        assert_eq!(Some("7".to_owned()), context.system_var(tidb_vars::TIDB_ANALYZE_STORE_BATCH_SIZE));
        assert_eq!(Some("+08:00".to_owned()), context.system_var(TIME_ZONE));
    }

    /// Go master `TestUpdateSCtxVarsForStatsRestore`.
    #[test]
    fn test_update_sctx_vars_for_stats_restore() {
        for fail_on in [
            "",
            tidb_vars::TIDB_ANALYZE_PARTITION_CONCURRENCY,
            tidb_vars::TIDB_ANALYZE_VERSION,
            tidb_vars::TIDB_ANALYZE_STORE_BATCH_SIZE,
            tidb_vars::TIDB_PARTITION_PRUNE_MODE,
            tidb_vars::TIDB_ENABLE_ANALYZE_SNAPSHOT,
            tidb_vars::TIDB_ANALYZE_SKIP_COLUMN_TYPES,
            tidb_vars::TIDB_SKIP_MISSING_PARTITION_STATS,
            INNODB_LOCK_WAIT_TIMEOUT,
            TIME_ZONE,
        ] {
            let context = new_stats_session_context_for_restore_test();
            if !fail_on.is_empty() {
                // Go `failingStatsGlobalAccessor`: the read of `fail_on` fails.
                context.globals.lock().unwrap().remove(fail_on);
            }
            match update_sctx_vars_for_stats(&context) {
                Err(err) => {
                    assert!(!fail_on.is_empty(), "{err}");
                    assert!(err.to_string().contains(fail_on), "{fail_on}: {err}");
                }
                Ok(restore) => {
                    assert!(fail_on.is_empty());
                    assert_eq!(3, context.analyze_partition_concurrency());
                    assert_eq!(2, context.analyze_version());
                    assert_eq!(6, context.analyze_store_batch_size());
                    assert_eq!("dynamic", context.partition_prune_mode());
                    assert!(context.enable_analyze_snapshot());
                    assert_eq!(BTreeSet::from(["blob".to_owned()]), context.analyze_skip_column_types());
                    assert!(context.skip_missing_partition_stats());
                    assert_eq!(12_000, context.lock_wait_timeout());
                    assert_eq!("+03:00", context.location());
                    assert_eq!(context.location(), context.statement_time_zone());
                    restore.restore();
                }
            }
            check_restored_stats_session_vars(&context);
        }
        for name in [
            tidb_vars::TIDB_ANALYZE_PARTITION_CONCURRENCY,
            tidb_vars::TIDB_ANALYZE_VERSION,
            tidb_vars::TIDB_ANALYZE_STORE_BATCH_SIZE,
            INNODB_LOCK_WAIT_TIMEOUT,
            TIME_ZONE,
        ] {
            // "invalid_<name>"
            let context = new_stats_session_context_for_restore_test();
            context.globals.lock().unwrap().insert(name.to_owned(), "invalid".to_owned());
            assert!(update_sctx_vars_for_stats(&context).is_err(), "{name}");
            check_restored_stats_session_vars(&context);
        }
        for (name, zone, value) in [("system", "SYSTEM", "SYSTEM"), ("utc", "UTC", "UTC"), ("fixed offset", "+08:00", "+08:00")] {
            // "unloaded/<name>": neither variable is loaded in the session.
            let context = MockContext::new();
            {
                let mut globals = context.globals.lock().unwrap();
                globals.insert(tidb_vars::TIDB_ANALYZE_STORE_BATCH_SIZE.to_owned(), "6".to_owned());
                globals.insert(TIME_ZONE.to_owned(), "+03:00".to_owned());
            }
            // Go `vars.TimeZone = tc.zone`.
            context.state.lock().unwrap().location = zone.to_owned();
            assert_eq!(None, context.system_var(tidb_vars::TIDB_ANALYZE_STORE_BATCH_SIZE), "{name}");
            assert_eq!(None, context.system_var(TIME_ZONE), "{name}");
            let restore = update_sctx_vars_for_stats(&context).unwrap();
            assert_eq!(6, context.analyze_store_batch_size(), "{name}");
            restore.restore();
            let default = tidb_vardef::defaults::DEF_TIDB_ANALYZE_STORE_BATCH_SIZE;
            assert_eq!(Some(default.to_string()), context.system_var(tidb_vars::TIDB_ANALYZE_STORE_BATCH_SIZE), "{name}");
            assert_eq!(default, context.analyze_store_batch_size(), "{name}");
            assert_eq!(Some(value.to_owned()), context.system_var(TIME_ZONE), "{name}");
            assert_eq!(zone, context.location(), "{name}");
            assert_eq!("UTC", context.statement_time_zone(), "{name}");
        }
    }

    /// Go master `TestCallWithSCtxRestoresStatsSessionVars`.
    #[test]
    fn test_call_with_sctx_restores_stats_session_vars() {
        for fail_callback in [false, true] {
            // "callback_error=<fail_callback>"
            let context = Arc::new(new_stats_session_context_for_restore_test());
            let created = Arc::new(AtomicUsize::new(0));
            let (factory_context, factory_created) = (Arc::clone(&context), Arc::clone(&created));
            let pool = AdvancedSessionPool::new(1, move || {
                factory_created.fetch_add(1, Ordering::SeqCst);
                Ok(Arc::clone(&factory_context))
            });
            let result = call_with_sctx(
                &pool,
                |ctx| {
                    assert!(std::ptr::eq(ctx, Arc::as_ptr(&context)));
                    assert_eq!(3, ctx.analyze_partition_concurrency());
                    assert_eq!(6, ctx.analyze_store_batch_size());
                    assert_eq!(12_000, ctx.lock_wait_timeout());
                    assert_eq!("+03:00", ctx.location());
                    assert_eq!(ctx.location(), ctx.statement_time_zone());
                    if fail_callback {
                        return Err(error("statistics callback failed"));
                    }
                    Ok(())
                },
                &[],
            );
            if fail_callback {
                assert!(result.unwrap_err().to_string().contains("statistics callback failed"));
            } else {
                result.unwrap();
            }
            check_restored_stats_session_vars(&context);
            if !fail_callback {
                AdvancedSessionPool::with_session(&pool, |session| {
                    session.with_session_context(|ctx: &MockContext| {
                        assert!(std::ptr::eq(ctx, Arc::as_ptr(&context)));
                        check_restored_stats_session_vars(ctx);
                        Ok(())
                    })
                })
                .unwrap();
                assert_eq!(1, created.load(Ordering::SeqCst));
            }
        }
        // "setup_error"
        let context = Arc::new(new_stats_session_context_for_restore_test());
        context.globals.lock().unwrap().remove(TIME_ZONE);
        let factory_context = Arc::clone(&context);
        let pool = AdvancedSessionPool::new(1, move || Ok(Arc::clone(&factory_context)));
        let mut called = false;
        let err = call_with_sctx(
            &pool,
            |_| {
                called = true;
                Ok(())
            },
            &[],
        )
        .unwrap_err();
        assert!(err.to_string().contains(TIME_ZONE), "{err}");
        assert!(!called);
        check_restored_stats_session_vars(&context);
    }

    #[test]
    fn call_with_sctx_releases_failed_session_and_synchronizes_timezone() {
        let context = Arc::new(MockContext::new());
        context
            .globals
            .lock()
            .unwrap()
            .insert(TIME_ZONE.to_owned(), "Asia/Shanghai".to_owned());
        let factory_context = Arc::clone(&context);
        let pool = AdvancedSessionPool::new(1, move || Ok(Arc::clone(&factory_context)));
        let synced = Mutex::new((String::new(), String::new()));
        let returned = call_with_sctx(
            &pool,
            |context| {
                *synced.lock().unwrap() = (context.location(), context.statement_time_zone());
                Err(error("simulated error"))
            },
            &[],
        )
        .unwrap_err();
        assert_eq!(returned.to_string(), "simulated error");
        assert_eq!(pool.size(), 0);
        assert_eq!(context.closed.load(Ordering::SeqCst), 1);
        assert_eq!(
            ("Asia/Shanghai".to_owned(), "Asia/Shanghai".to_owned()),
            *synced.lock().unwrap()
        );
        // Go `defer restoreStatsVars()`: the session's own zones come back.
        let state = context.state.lock().unwrap();
        assert_eq!(state.location, "SYSTEM");
        assert_eq!(state.statement_location, "UTC");
    }

    #[test]
    fn call_with_sctx_recovers_ordinary_panics_and_quarantines_the_session() {
        let context = Arc::new(MockContext::new());
        let factory_context = Arc::clone(&context);
        let pool = AdvancedSessionPool::new(1, move || Ok(Arc::clone(&factory_context)));
        assert!(call_with_sctx(
            &pool,
            |_| -> Result<(), SqlExecError> { panic!("ordinary panic") },
            &[],
        )
        .is_ok());
        assert_eq!(pool.size(), 0);
        assert_eq!(context.closed.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn timestamp_and_special_global_index_use_model_values() {
        assert_eq!(duration_to_ts(12_345_678), 12 << 18);
        let mut virtual_column = ColumnInfo::default();
        virtual_column.generated_expr_string = "a+1".to_owned();
        let table = TableInfo {
            columns: GoSharedPointerSlice::from_nullable(vec![
                Some(ColumnInfo::default()),
                Some(virtual_column),
            ]),
            ..TableInfo::default()
        };
        let regular = IndexInfo {
            global: true,
            columns: vec![IndexColumn {
                offset: 0,
                length: UNSPECIFIED_LENGTH,
                ..IndexColumn::default()
            }]
            .into(),
            ..IndexInfo::default()
        };
        assert!(!is_special_global_index(&regular, &table));
        let expression = IndexInfo {
            global: true,
            columns: vec![IndexColumn {
                offset: 1,
                length: UNSPECIFIED_LENGTH,
                ..IndexColumn::default()
            }]
            .into(),
            ..IndexInfo::default()
        };
        assert!(is_special_global_index(&expression, &table));
        let prefix = IndexInfo {
            global: true,
            columns: vec![IndexColumn {
                offset: 0,
                length: 3,
                ..IndexColumn::default()
            }]
            .into(),
            ..IndexInfo::default()
        };
        assert!(is_special_global_index(&prefix, &table));
    }

    #[test]
    fn get_start_ts_requests_an_active_transaction() {
        assert_eq!(get_start_ts(&MockContext::new()).unwrap(), 123);
    }

    #[test]
    #[cfg(feature = "failpoints")]
    fn exec_rows_timeout_failpoint_returns_source_error() {
        tidb_failpoint::enable("github.com/pingcap/tidb/pkg/statistics/handle/util/ExecRowsTimeout", "return").unwrap();
        let returned = exec_rows(&MockContext::new(), "select 1", &[]).unwrap_err();
        assert_eq!(returned.to_string(), "inject timeout error");
        let returned =
            exec_rows_with_ctx(&*STATS_CONTEXT, &MockContext::new(), "select 1", &[]).unwrap_err();
        assert_eq!(returned.to_string(), "inject timeout error");
        tidb_failpoint::disable("github.com/pingcap/tidb/pkg/statistics/handle/util/ExecRowsTimeout");
    }
}

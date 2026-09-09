// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Complete transcreation of pinned Go `pkg/ddl/session`.

use std::error::Error as StdError;
use std::fmt;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Arc, Condvar, LazyLock, Mutex};
use std::time::Instant;

use prometheus::{exponential_buckets, HistogramOpts, HistogramVec};
use tidb_datatype::{ConversionFlags, CoreTime, Datum};
use tidb_error::errctx::LevelMap;
use tidb_model::job::ResolvedTimeZone;
use tidb_mysql::SqlMode;
use tidb_sqlexec::{ExecutionContext, RecordSet};
use tidb_util::sqlescape::SqlArg;

/// Go `kv.InternalTxnDDL`.
pub const INTERNAL_TXN_DDL: &str = "ddl";

static DDL_JOB_TABLE_DURATION: LazyLock<HistogramVec> = LazyLock::new(|| {
    let histogram = HistogramVec::new(
        HistogramOpts::new(
            "tidb_ddl_job_table_duration_seconds",
            "Bucketed histogram of processing time (s) of the 3 DDL job tables",
        )
        .buckets(exponential_buckets(0.001, 2.0, 20).expect("valid DDL job-table buckets")),
        &["type"],
    )
    .expect("valid DDL job-table histogram");
    prometheus::default_registry()
        .register(Box::new(histogram.clone()))
        .expect("register DDL job-table histogram");
    histogram
});

struct DdlJobTableTimer<'a> {
    started: Instant,
    label: &'a str,
    result: &'static str,
}

impl Drop for DdlJobTableTimer<'_> {
    fn drop(&mut self) {
        DDL_JOB_TABLE_DURATION
            .with_label_values(&[&format!("{}-{}", self.label, self.result)])
            .observe(self.started.elapsed().as_secs_f64());
    }
}

struct RecordSetCloser {
    record_set: Box<dyn RecordSet>,
}

impl Drop for RecordSetCloser {
    fn drop(&mut self) {
        if let Err(error) = self.record_set.close() {
            tracing::error!(%error, "failed to close recordSet in DDL session Execute");
        }
    }
}

/// Package error preserving the source message.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Error(String);

impl Error {
    /// Creates an error with the source message.
    pub fn new(message: impl Into<String>) -> Self {
        Self(message.into())
    }
}

impl fmt::Display for Error {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl StdError for Error {}

/// Result returned by this package.
pub type Result<T> = std::result::Result<T, Error>;

/// Go `kv.Transaction` behavior observed by this package and its tests.
pub trait Transaction: Send + Sync {
    /// Whether the transaction is valid.
    fn valid(&self) -> bool;
    /// Transaction start timestamp.
    fn start_ts(&self) -> u64;
}

/// Exact `sessionctx.Context` capabilities used by this Go package.
pub trait SessionContext: Send + Sync + 'static {
    /// Go `sessiontxn.NewTxn`.
    fn new_txn(&self, context: &dyn ExecutionContext) -> Result<()>;
    /// Go `EnterNewTxn` with `ast.Pessimistic`.
    fn enter_new_pessimistic_txn(&self, context: &dyn ExecutionContext) -> Result<()>;
    /// Go `SessionVars.SetInTxn`.
    fn set_in_txn(&self, in_txn: bool);
    /// Go `StmtCommit`.
    fn stmt_commit(&self, context: &dyn ExecutionContext);
    /// Go `CommitTxn`.
    fn commit_txn(&self, context: &dyn ExecutionContext) -> Result<()>;
    /// Go `Txn(active)`.
    fn txn(&self, active: bool) -> Result<Option<Arc<dyn Transaction>>>;
    /// Go `StmtRollback`.
    fn stmt_rollback(&self, context: &dyn ExecutionContext, is_pessimistic_retry: bool);
    /// Go `RollbackTxn`.
    fn rollback_txn(&self, context: &dyn ExecutionContext);
    /// Request source carried by the caller's context, if present.
    fn request_source(&self, context: &dyn ExecutionContext) -> Option<String>;
    /// Go `GetSQLExecutor().ExecuteInternal`, with the resolved request source.
    fn execute_internal(
        &self,
        context: &dyn ExecutionContext,
        request_source: &str,
        query: &str,
        arguments: &[SqlArg<'_>],
    ) -> std::result::Result<Option<Box<dyn RecordSet>>, tidb_sqlexec::SqlExecError>;
    /// Sets `ServerStatusAutocommit`.
    fn set_autocommit(&self, enabled: bool);
    /// Sets `InRestrictedSQL`.
    fn set_restricted_sql(&self, enabled: bool);
    /// Copies the session location to the statement context.
    fn set_statement_timezone_to_session_location(&self);
    /// Sets `DiskFullOpt_AllowedOnAlmostFull`.
    fn allow_on_almost_full(&self);
    /// Clears the disk-full option.
    fn clear_disk_full_option(&self);
    /// Go `infosync.StoreInternalSession`.
    fn register_internal_session(&self);
    /// Go `infosync.DeleteInternalSession`.
    fn unregister_internal_session(&self);
    /// Go `pools.Resource.Close`.
    fn close(&self);
    /// Go's `setCreateMaterializedViewScheduleEvalSession`: installs the
    /// schedule sql mode, the statement type flags and error levels
    /// (`expression.MaterializedScheduleTypeFlagsWithSQLMode` /
    /// `MaterializedScheduleErrLevelsWithSQLMode`), and the schedule time
    /// zone on both the session variables and the statement context,
    /// returning the captured originals. Forward-port scaffolding: the
    /// `pkg/ddl/session` baseline at a85e0fd5df has no such surface; the
    /// caller lives in a later Go tree this port tracks ahead of.
    fn install_schedule_eval_session(
        &self,
        sql_mode: SqlMode,
        zone: &ResolvedTimeZone,
    ) -> ScheduleEvalOriginals;
    /// Restores the originals captured by
    /// [`SessionContext::install_schedule_eval_session`] (Go's returned
    /// closure).
    fn restore_schedule_eval_session(&self, originals: &ScheduleEvalOriginals);
    /// Go's `evalCreateMaterializedViewScheduleExprToDatetime`: parses
    /// `expr_sql` as a generated expression, builds and evaluates it in the
    /// session's expression context, and converts a non-NULL result to
    /// `TypeDatetime` at `MaxFsp`. SQL NULL yields `None`.
    fn eval_schedule_expression(&self, expr_sql: &str) -> Result<Option<ScheduleTime>>;
}

/// Go's originals captured by
/// `setCreateMaterializedViewScheduleEvalSession`.
#[derive(Clone, Debug)]
pub struct ScheduleEvalOriginals {
    /// Go `originalSQLMode`.
    pub sql_mode: SqlMode,
    /// Go `originalTypeFlags` (`StmtCtx.TypeFlags()`).
    pub stmt_type_flags: ConversionFlags,
    /// Go `originalErrLevels` (`StmtCtx.ErrLevels()`).
    pub stmt_err_levels: LevelMap,
    /// Go `originalTZ` (`sessVars.TimeZone`); `None` is Go's nil.
    pub session_time_zone: Option<ResolvedTimeZone>,
    /// Go `originalStmtTZ` (`StmtCtx.TimeZone()`); `None` is Go's nil.
    pub stmt_time_zone: Option<ResolvedTimeZone>,
}

/// The materialized-view schedule time type (`types.Time`).
pub type ScheduleTime = tidb_datatype::Time;

/// Go's `CoreTime` re-export for schedule conversions.
pub type ScheduleCoreTime = CoreTime;

/// Go `Session`.
pub struct Session<C: SessionContext> {
    context: Arc<C>,
}

impl<C: SessionContext> Clone for Session<C> {
    fn clone(&self) -> Self {
        Self {
            context: Arc::clone(&self.context),
        }
    }
}

impl<C: SessionContext> Session<C> {
    /// Go `NewSession`.
    pub fn new(context: Arc<C>) -> Self {
        Self { context }
    }

    /// Go `Begin`.
    pub fn begin(&self, context: &dyn ExecutionContext) -> Result<()> {
        self.context.new_txn(context)?;
        self.context.set_in_txn(true);
        Ok(())
    }

    /// Go `BeginPessimistic`.
    pub fn begin_pessimistic(&self, context: &dyn ExecutionContext) -> Result<()> {
        self.context.enter_new_pessimistic_txn(context)?;
        self.context.set_in_txn(true);
        Ok(())
    }

    /// Go `Commit`.
    pub fn commit(&self, context: &dyn ExecutionContext) -> Result<()> {
        self.context.stmt_commit(context);
        self.context.commit_txn(context)
    }

    /// Go `Txn`.
    pub fn txn(&self) -> Result<Option<Arc<dyn Transaction>>> {
        self.context.txn(true)
    }

    /// Go `Rollback`.
    pub fn rollback(&self) {
        let background = tidb_sqlexec::BackgroundContext;
        self.context.stmt_rollback(&background, false);
        self.context.rollback_txn(&background);
    }

    /// Go `Reset`.
    pub fn reset(&self) {
        self.context
            .stmt_rollback(&tidb_sqlexec::BackgroundContext, false);
    }

    /// Go `Execute`.
    pub fn execute(
        &self,
        context: &dyn ExecutionContext,
        query: &str,
        label: &str,
        arguments: &[SqlArg<'_>],
    ) -> std::result::Result<Option<Vec<Vec<Datum>>>, tidb_sqlexec::SqlExecError> {
        let mut timer = DdlJobTableTimer {
            started: Instant::now(),
            label,
            result: "ok",
        };
        let result = self.execute_inner(context, query, arguments);
        if result.is_err() {
            timer.result = "err";
        }
        result
    }

    fn execute_inner(
        &self,
        context: &dyn ExecutionContext,
        query: &str,
        arguments: &[SqlArg<'_>],
    ) -> std::result::Result<Option<Vec<Vec<Datum>>>, tidb_sqlexec::SqlExecError> {
        let source = self
            .context
            .request_source(context)
            .unwrap_or_else(|| INTERNAL_TXN_DDL.to_owned());
        let Some(record_set) = self
            .context
            .execute_internal(context, &source, query, arguments)?
        else {
            return Ok(None);
        };
        let mut record_set = RecordSetCloser { record_set };
        tidb_sqlexec::drain_record_set(context, record_set.record_set.as_mut(), 8).map(Some)
    }

    /// Go `Session`.
    pub fn session(&self) -> Arc<C> {
        Arc::clone(&self.context)
    }

    /// Go `RunInTxn`.
    pub fn run_in_txn<T>(&self, callback: impl FnOnce(&Self) -> Result<T>) -> Result<T> {
        self.begin(&tidb_sqlexec::BackgroundContext)?;
        notify_begin_txn_failpoint();
        match callback(self) {
            Ok(value) => {
                self.commit(&tidb_sqlexec::BackgroundContext)?;
                Ok(value)
            }
            Err(error) => {
                self.rollback();
                Err(error)
            }
        }
    }
}

/// Go `MockDDLOnce`.
pub static MOCK_DDL_ONCE: AtomicI64 = AtomicI64::new(0);

#[derive(Default)]
struct NotifyState {
    pending: bool,
}

/// Go's unbuffered `TestNotifyBeginTxnCh` test channel.
#[derive(Default)]
pub struct NotifyBeginTxnChannel {
    state: Mutex<NotifyState>,
    changed: Condvar,
}

impl NotifyBeginTxnChannel {
    /// Sends one notification and waits for its receiver, like an unbuffered
    /// Go channel.
    pub fn send(&self) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        while state.pending {
            state = self
                .changed
                .wait(state)
                .unwrap_or_else(std::sync::PoisonError::into_inner);
        }
        state.pending = true;
        self.changed.notify_all();
        while state.pending {
            state = self
                .changed
                .wait(state)
                .unwrap_or_else(std::sync::PoisonError::into_inner);
        }
    }

    /// Receives one notification.
    pub fn receive(&self) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        while !state.pending {
            state = self
                .changed
                .wait(state)
                .unwrap_or_else(std::sync::PoisonError::into_inner);
        }
        state.pending = false;
        self.changed.notify_all();
    }
}

/// Go `TestNotifyBeginTxnCh`.
pub static TEST_NOTIFY_BEGIN_TXN_CHANNEL: LazyLock<NotifyBeginTxnChannel> =
    LazyLock::new(NotifyBeginTxnChannel::default);

#[cfg(feature = "failpoints")]
fn notify_begin_txn_failpoint() {
    fail::fail_point!("NotifyBeginTxnCh", |value| {
        let value = value
            .and_then(|value| value.parse::<i64>().ok())
            .unwrap_or(0);
        if value == 1 {
            MOCK_DDL_ONCE.store(1, Ordering::SeqCst);
            TEST_NOTIFY_BEGIN_TXN_CHANNEL.send();
        } else if value == 2 && MOCK_DDL_ONCE.load(Ordering::SeqCst) == 1 {
            TEST_NOTIFY_BEGIN_TXN_CHANNEL.receive();
            MOCK_DDL_ONCE.store(0, Ordering::SeqCst);
        }
    });
}

#[cfg(not(feature = "failpoints"))]
fn notify_begin_txn_failpoint() {
    let _ = MOCK_DDL_ONCE.load(Ordering::Relaxed);
}

/// The branches distinguished by Go's destroy type switch.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DestroyMode {
    /// Go `util.DestroyableSessionPool`.
    Destroyable,
    /// Go `*pools.ResourcePool`.
    ResourcePool,
    /// Any unsupported pool type.
    Unsupported,
}

/// Exact `util.SessionPool` operations used by this package.
pub trait ResourcePool<C: SessionContext>: Send + Sync + 'static {
    /// Go `Get`.
    fn get(&self) -> Result<Arc<C>>;
    /// Go `Put`; `None` is the concrete resource pool's `Put(nil)`.
    fn put(&self, resource: Option<Arc<C>>);
    /// Go `Close`.
    fn close(&self);
    /// Which Go type-switch branch this native adapter represents.
    fn destroy_mode(&self) -> DestroyMode;
    /// Go `%T` text used by the unsupported-pool warning/assertion.
    fn pool_type(&self) -> &'static str {
        std::any::type_name::<Self>()
    }
    /// Go `DestroyableSessionPool.Destroy`.
    fn destroy(&self, resource: Arc<C>) {
        let _ = resource;
        unreachable!("destroy is only called for a destroyable pool")
    }
}

struct PoolState {
    closed: bool,
}

/// Go `Pool`.
pub struct Pool<C: SessionContext, P: ResourcePool<C>> {
    state: Mutex<PoolState>,
    resource_pool: P,
    _context: std::marker::PhantomData<fn() -> C>,
}

impl<C: SessionContext, P: ResourcePool<C>> Pool<C, P> {
    /// Go `NewSessionPool`.
    pub fn new(resource_pool: P) -> Self {
        Self {
            state: Mutex::new(PoolState { closed: false }),
            resource_pool,
            _context: std::marker::PhantomData,
        }
    }

    /// Go `Get`.
    pub fn get(&self) -> Result<Arc<C>> {
        if self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .closed
        {
            return Err(Error::new("session pool is closed"));
        }
        // Go type-asserts here ("need sessionctx.Context, but got %T");
        // `ResourcePool::get` returns a typed `Arc<C>`, so that failure path
        // is structurally impossible.
        let context = self.resource_pool.get()?;
        context.set_autocommit(true);
        context.set_restricted_sql(true);
        context.set_statement_timezone_to_session_location();
        context.allow_on_almost_full();
        context.register_internal_session();
        Ok(context)
    }

    /// Go `Put`.
    pub fn put(&self, context: Arc<C>) {
        debug_assert!(!context
            .txn(false)
            .is_ok_and(|txn| txn.is_some_and(|txn| txn.valid())));
        context.rollback_txn(&tidb_sqlexec::BackgroundContext);
        context.clear_disk_full_option();
        self.resource_pool.put(Some(Arc::clone(&context)));
        context.unregister_internal_session();
    }

    /// Go `Destroy`.
    pub fn destroy(&self, context: Arc<C>) {
        debug_assert!(!context
            .txn(false)
            .is_ok_and(|txn| txn.is_some_and(|txn| txn.valid())));
        context.rollback_txn(&tidb_sqlexec::BackgroundContext);
        context.clear_disk_full_option();
        context.unregister_internal_session();
        match self.resource_pool.destroy_mode() {
            DestroyMode::Destroyable => self.resource_pool.destroy(context),
            DestroyMode::ResourcePool => {
                context.close();
                self.resource_pool.put(None);
            }
            DestroyMode::Unsupported => {
                let pool_type = self.resource_pool.pool_type();
                tracing::warn!(
                    pool_type,
                    "session pool doesn't support Destroy, fall back to Put"
                );
                self.resource_pool.put(Some(context));
                debug_assert!(
                    false,
                    "unsupported session pool type for Destroy: {pool_type}"
                );
            }
        }
    }

    /// Go `Close`.
    pub fn close(&self) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.closed {
            return;
        }
        tracing::info!("closing session pool");
        self.resource_pool.close();
        state.closed = true;
    }
}

#[cfg(test)]
mod tests;

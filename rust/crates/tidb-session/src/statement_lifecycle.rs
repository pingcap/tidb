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

//! The statement lifecycle over the store seam: Go `ExecStmt`'s
//! `handlePessimisticDML` / `handlePessimisticLockError` loop, `runStmt`'s
//! `finishStmt` and the autocommit retry of `doCommitWithRetry`.
//!
//! A [`StatementLifecycle`] borrows the pieces one statement touches -- the
//! session, its [`TxnManager`], the slot the session's tables read through and
//! the catalog follower -- so the server that owns them keeps no statement
//! logic of its own. A statement binds a snapshot into the slot, runs, and
//! then locks what it staged, commits or rolls back, and tears its context
//! down; a lock conflict rolls the STATEMENT back and re-executes it at the
//! advanced `for_update_ts`.

use tidb_executor::stmt_ctx_build::ResetContextOfStmt;
use std::cell::Cell;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use tidb_store_driver_txn::cluster_table_storage::{
    LockKeysOutcome, StatementLockFailure, StatementLockFailureSlot,
};
use tidb_store_driver_error::sql_query_error::SqlQueryError;
use tidb_executor::access_path::StatementReadShape;
use tidb_kv::cluster_storage::ClusterSnapshot;
use tidb_store_driver_txn::cluster_storage::{BufferCheckpoint, SwappableSnapshot};

use tidb_domain_core::cluster_transactions as transactions;
use crate::txn_manager::SessionTxnContext;
use tidb_store_driver_error::pessimistic_lock_error::LockSqlError;
use crate::txn_manager::{CommitHooks, TxnManager};
use crate::Session;
use tidb_executor::adapter::PessimisticDmlSession as _;

/// Go `kv.ErrWriteConflict`'s MySQL code.
const ERR_WRITE_CONFLICT: u16 = tidb_store_driver_error::pessimistic_lock_error::ERR_WRITE_CONFLICT;

/// The catalog a connection follows, as a statement lifecycle needs it: the
/// version its tables were built from, and the rebuild a retried statement
/// does before its next attempt (Go's replay calls `RebuildPlan` per attempt).
pub trait CatalogFollowing {
    /// The schema version the session's tables were built from.
    fn schema_version(&self) -> i64;
    /// Rebinds the session's tables to the latest catalog, unless a
    /// transaction has pinned the one it started with.
    fn rebuild_if_stale(&mut self, session: &mut Session, txn: &TxnManager);
    /// Rebinds the session's tables to the latest catalog unconditionally:
    /// a transaction starts with the LATEST schema (Go
    /// `domain.GetSnapshotInfoSchema(startTS)`), so a table committed before
    /// `BEGIN` is visible to every statement of the new transaction.
    fn rebuild(&mut self, session: &mut Session);
}

/// Go `kv.retryBackOffBase`, in milliseconds (`pkg/kv/txn.go:182-183`; the
/// comment there says microsecond and the code multiplies by
/// `time.Millisecond` -- the code is the contract).
pub const RETRY_BACK_OFF_BASE_MS: u32 = 1;

/// Go `kv.retryBackOffCap`, in milliseconds (`pkg/kv/txn.go:184-185`).
pub const RETRY_BACK_OFF_CAP_MS: u32 = 100;

/// Go `kv.BackOff` (`pkg/kv/txn.go:191-197`): exponential backoff with full
/// jitter, sleeping a uniform draw from `[0, min(cap, base * 2^attempts))`
/// milliseconds. `attempts` counts from 1, as Go's does -- it increments
/// `retryCnt` before the sleep -- so the first wait is drawn from `[0, 2)ms`.
///
/// This is also what bounds a spinning retry in time as well as in count: even
/// a statement that loses all ten races sleeps a capped amount rather than
/// hammering the conflicting key.
pub fn back_off(attempts: u32) {
    std::thread::sleep(Duration::from_millis(u64::from(jitter_below(
        back_off_upper_ms(attempts),
    ))));
}

/// The exclusive upper bound, in milliseconds, of attempt `attempts`'s sleep:
/// Go's `min(retryBackOffCap, retryBackOffBase * 2^attempts)`.
pub fn back_off_upper_ms(attempts: u32) -> u32 {
    RETRY_BACK_OFF_BASE_MS
        .checked_shl(attempts)
        .unwrap_or(u32::MAX)
        .min(RETRY_BACK_OFF_CAP_MS)
}

/// A uniform draw from `[0, upper)`, the jitter half of [`back_off`].
///
/// This is a real generator rather than a clock reading, and the difference is
/// measured rather than stylistic: this machine's `SystemTime` advances in
/// 1000ns steps, so `subsec_nanos() % upper` is IDENTICALLY ZERO for every
/// `upper` that divides 1000 -- which is the first three backoffs, `2`, `4`
/// and `8` ms, exactly the ones that matter when two sessions are colliding on
/// one key. Two conflicting connections would then both spin with no sleep and
/// stay in lockstep. Seeding once per connection thread is what makes two
/// racing sessions draw different sleeps at all.
///
/// The generator is `xorshift64*`; a backoff length needs decorrelation, not
/// cryptographic quality, and this keeps the node free of a random-number
/// dependency.
pub fn jitter_below(upper: u32) -> u32 {
    thread_local! {
        static STATE: Cell<u64> = const { Cell::new(0) };
    }
    STATE.with(|state| {
        let mut x = state.get();
        if x == 0 {
            let nanos = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map_or(0, |since| since.as_nanos() as u64);
            // The thread-local's own address separates two connections that
            // seeded within the same clock tick.
            x = nanos ^ (state as *const Cell<u64> as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15);
            if x == 0 {
                x = 0x9E37_79B9_7F4A_7C15;
            }
        }
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        state.set(x);
        u32::try_from((x.wrapping_mul(0x2545_F491_4F6C_DD1D) >> 33) % u64::from(upper)).unwrap_or(0)
    })
}

/// One statement's transaction context: see
/// [`StatementLifecycle::begin_statement_scope`].
pub struct StatementScope {
    pub read_ts: transactions::StatementReadTs,
    pub autocommit: bool,
}

pub use tidb_executor::adapter::PessimisticStep;

pub struct ReadStatement {
    _pin: Box<dyn Send>,
    read_ts: transactions::StatementReadTs,
    savepoint: BufferCheckpoint,
    autocommit: bool,
    resource_group: String,
}

impl ReadStatement {
    /// Go `TxnCtx.StartTS` of the statement's transaction: the timestamp its
    /// reads activated, `0` before any read did.
    #[must_use]
    pub fn start_ts(&self) -> u64 {
        self.read_ts.get().unwrap_or_default()
    }
}

/// One statement's borrowed context; see the module documentation.
pub struct StatementLifecycle<'a> {
    /// The session the statement runs in.
    pub session: &'a mut Session,
    /// The session's open transaction.
    pub txn: &'a mut TxnManager,
    /// The slot every table of `session` reads through; rebound per statement.
    pub slot: &'a Arc<Mutex<SwappableSnapshot>>,
    /// The catalog the connection follows.
    pub follower: &'a mut dyn CatalogFollowing,
    /// What a commit reads off the server.
    pub hooks: Box<dyn CommitHooks>,
}

impl StatementLifecycle<'_> {
    /// The per-statement transaction context both statement lifecycles open
    /// (Go `PrepareTxnCtx` builds a fresh `TxnCtx` before every statement no
    /// transaction continues into): an autocommit statement starts with no
    /// published timestamp, and its own timestamp is filled in by its first
    /// read and read back by its publication after the read handle is gone --
    /// autocommit publishes THERE, not at a fresh one, see `StatementReadTs`.
    pub fn begin_statement_scope(&mut self, notify_executor_breakpoint: bool) -> StatementScope {
        let autocommit = self.txn.explicit().is_none() && self.session.historical_read_ts().is_none();
        if autocommit {
            self.session.current_tso().clear();
        }
        // Go `ActivateTxn` for a transaction outside `BEGIN`: the last
        // transaction's footprint tracker goes.
        if self.txn.explicit().is_none() {
            self.session.detach_mem_db_footprint();
        }
        // Go `session.Txn(true)` -> `SetMemoryFootprintChangeHook`, once per
        // transaction buffer (`MemHookSet`).
        if !self.txn.buffer().memory_hook_is_set() {
            if let Some(hook) = self
                .session
                .mem_db_footprint_hook(self.txn.buffer().in_stmt_buf_cleanup())
            {
                self.txn.buffer().set_memory_footprint_change_hook(hook);
            }
        }
        let read_ts = transactions::StatementReadTs::new(self.session.current_tso())
            .with_observer(self.session.transaction_observer())
            .with_phase_durations(self.session.phase_durations());
        self.session
            .begin_external_executor_breakpoint_scope(notify_executor_breakpoint);
        StatementScope {
            read_ts,
            autocommit,
        }
    }

    /// The end of [`Self::begin_statement_scope`]: the autocommit
    /// transaction's observation ends, Go's `FinishExecuteStmt` still reads
    /// this statement's `TxnCtx.StartTS` (its slow log and
    /// `tidb_last_query_info`), and the published timestamp goes with the
    /// transaction.
    pub fn end_statement_scope(&mut self, scope: &StatementScope) {
        self.session.end_external_executor_breakpoint_scope();
        if scope.autocommit {
            if let Some(observer) = self.session.transaction_observer() {
                observer.finished();
            }
            if let Some(ts) = scope.read_ts.get() {
                self.session.record_statement_txn_start_ts(ts);
            }
            self.session.current_tso().clear();
        }
    }

    /// The statement lifecycle proper: savepoint, attempt, replay budget.
    pub fn with_bound_statement<T>(
        &mut self,
        shape: StatementReadShape,
        resource_group: &str,
        notify_executor_breakpoint: bool,
        is_dml: bool,
        mut run: impl FnMut(&mut Session) -> Result<T, SqlQueryError>,
    ) -> Result<T, SqlQueryError> {
        // Go's MDL considers a RUNNING statement a user of its schema
        // version; this hold is what keeps a Go DDL owner from publishing a
        // drop under a statement mid-flight. Dropped with the scope.
        let _statement_pin = self.txn.hold_schema_pin(self.follower.schema_version());
        let mut savepoint = self.txn.buffer().checkpoint();
        // An optimistic transaction's locking reads lock nothing during
        // execution; their keys reach the owner's lock step, which flags
        // them as Go's optimistic `LockKeys` does.
        self.session
            .set_selected_lock_keys(self.txn.explicit().map(|transaction| {
                tidb_sessionctx::select_lock::SelectedLockKeys::new(
                    tidb_txnkv::transaction::LockWaitTime::Timeout(
                        self.session.lock_wait_timeout(),
                    ),
                    self.session.vars().shared_lock_promotion(),
                )
                .with_read_committed(self.session.is_read_committed())
                .with_lock_state_observer(self.session.transaction_lock_state_observer())
                .with_lock_in_execution(transaction.is_pessimistic())
            }));
        let delta_savepoint = self.session.table_delta_savepoint();
        let mut retried: u32 = 0;
        // Go `doCommitWithRetry` reads the failed transaction's
        // `TxnCtx.CouldRetry` once; `s.retry` then replays within its budget
        // although each replay's transaction is pessimistic.
        let mut could_retry = None;
        self.session.begin_external_mpp_query_scope();
        let outcome = loop {
            match self.attempt_statement(
                shape,
                savepoint.clone(),
                resource_group,
                notify_executor_breakpoint,
                retried > 0,
                is_dml,
                &mut run,
            ) {
                Ok(value) => break Ok(value),
                Err(error) => {
                    self.session
                        .restore_table_delta_savepoint(delta_savepoint.clone());
                    let could_retry = *could_retry.get_or_insert_with(|| self.txn.could_retry());
                    if !self.may_retry_autocommit_statement(&error, retried, could_retry) {
                        break Err(error);
                    }
                    retried += 1;
                    back_off(retried);
                    // Go `session.retry`: `StmtCtx.ResetForRetry`.
                    self.session.set_retrying_statement(true);
                    // The ids the losing attempt assigned carry into the
                    // replay -- Go's `RetryInfo.ResetOffset`
                    // (`pkg/session/session.go:1197`). The IDS cross between
                    // attempts; the timestamp must not, which is why this
                    // rewinds a list and touches nothing about the snapshot.
                    self.session
                        .retry_auto_ids()
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .begin_attempt();
                    // Go's replay calls `RebuildPlan` per attempt
                    // (`pkg/session/session.go:1207`), so a schema that moved
                    // under the conflict is picked up before the next try.
                    self.follower.rebuild_if_stale(self.session, self.txn);
                    // An autocommit attempt's failure ended its transaction
                    // and reset the whole buffer; the replay is a new
                    // transaction with a statement checkpoint of its own.
                    if self.txn.explicit().is_none() {
                        savepoint = self.txn.buffer().checkpoint();
                    }
                }
            }
        };
        self.session.set_retrying_statement(false);
        self.session.end_external_mpp_query_scope(outcome.is_err());
        if outcome.is_ok()
            && self.txn.explicit().is_some()
            && self.txn.lazy_uniqueness_check_enabled()
        {
            self.txn.buffer().preserve_statement_presumptions(savepoint);
        }
        self.txn.buffer().release(savepoint);
        self.session.set_selected_lock_keys(None);
        // Go's `cleanRetryInfo` (`pkg/session/session.go:329-336`, deferred
        // from `doCommitWithRetry`): the ids belong to the statement that is
        // now over, however it ended. The next statement's rows are not these
        // rows, and reusing an id across statements would write the same id
        // twice.
        self.session
            .retry_auto_ids()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clean();
        self.session.publish_transaction_buffer_metrics(
            self.txn.buffer().len(),
            self.txn.buffer().memory_footprint(),
        );
        // Go charges the statement's lock activity to the dashboard families
        // once per statement (`adapter.go:580-593`): keys locked and the
        // pessimistic retry count, only when a lock actually happened.
        if let Some(observation) =
            tidb_store_driver_txn::cluster_table_storage::take_last_pessimistic_lock_observation()
        {
            self.session
                .finish_statement_fair_locking(&observation, outcome.is_ok());
            if observation.lock_keys > 0 {
                tidb_metrics::session::STATEMENT_LOCK_KEYS_COUNT
                    .with_label_values(&[] as &[&str])
                    .observe(observation.lock_keys as f64);
            }
            if observation.retries > 0 {
                tidb_metrics::session::STATEMENT_PESSIMISTIC_RETRY_COUNT
                    .with_label_values(&[] as &[&str])
                    .observe(observation.retries as f64);
            }
        }
        if outcome.is_err()
            && (is_dml || shape == StatementReadShape::LockingRead)
            && self
                .txn
                .explicit()
                .is_some_and(|txn| txn.is_pessimistic())
            && self.session.in_transaction()
            && !self.session.pessimistic_constraint_check_in_place()
        {
            // Go handlePessimisticDML: with deferred uniqueness we cannot
            // identify which earlier statement caused the terminal failure.
            // End the SQL and native owners after releasing the statement stage.
            // Go `RollbackTxn`: the session half, then the store's.
            self.session.leave_txn_ctx_for_rollback();
            let _ = self.discard_explicit();
            // `exeerrors.ErrLazyUniquenessCheckFailure.GenWithStackByArgs(err.Error())`:
            // the inner error's text keeps its `[class:code]` prefix.
            return outcome.map_err(|error| {
                SqlQueryError::from(tidb_terror::MysqlError::from_terror(
                    &tidb_exeerrors::ERR_LAZY_UNIQUENESS_CHECK_FAILURE
                        .gen_with_stack_by_args(&[error.error_string().into()]),
                ))
            });
        }
        outcome
    }

    /// Runs one attempt of [`Self::with_bound_statement`]'s lifecycle.
    pub fn attempt_statement<T>(
        &mut self,
        shape: StatementReadShape,
        savepoint: BufferCheckpoint,
        resource_group: &str,
        notify_executor_breakpoint: bool,
        retrying: bool,
        is_dml: bool,
        run: &mut impl FnMut(&mut Session) -> Result<T, SqlQueryError>,
    ) -> Result<T, SqlQueryError> {
        let scope = self.begin_statement_scope(notify_executor_breakpoint);
        let result = self.attempt_statement_inner(
            shape,
            &savepoint,
            resource_group,
            retrying,
            is_dml,
            run,
            &scope.read_ts,
        );
        // The teardown is not a success step: every exit of the attempt --
        // including the `?`s inside the loop -- must leave no failed
        // statement's timestamp published, or `SET @x = @@tidb_current_ts`
        // after a failed autocommit statement reads a timestamp of a
        // transaction that is not open.
        self.end_statement_scope(&scope);
        result
    }

    /// The pessimistic statement loop of [`Self::attempt_statement`]; split
    /// out so the caller can run teardown on EVERY exit, `?`s included.
    pub fn attempt_statement_inner<T>(
        &mut self,
        shape: StatementReadShape,
        outer_savepoint: &BufferCheckpoint,
        resource_group: &str,
        retrying: bool,
        is_dml: bool,
        run: &mut impl FnMut(&mut Session) -> Result<T, SqlQueryError>,
        read_ts: &transactions::StatementReadTs,
    ) -> Result<T, SqlQueryError> {
        // The statement checkpoint (Go `LazyTxn.stagingHandle`): the
        // activation that binds the SQL buffer moves it into the store
        // transaction's MemDB.
        tidb_executor::adapter::HandlePessimisticDml::handle_pessimistic_dml(
            self,
            shape,
            *outer_savepoint,
            resource_group,
            retrying,
            is_dml,
            run,
            read_ts,
        )
    }

    /// Go `decideTxnMode` for an autocommit DML's implicit transaction.
    pub fn autocommit_write_is_pessimistic(&self, retrying: bool) -> bool {
        autocommit_dml_is_pessimistic(self.session, retrying)
    }

    /// Installs (or, with `false`, clears) an autocommit statement's
    /// pessimistic mode: Go `TxnCtx.IsPessimistic`, and the session lock wait
    /// its locking point reads take (`getLockWaitTime`).
    pub fn begin_autocommit_statement_locks(&mut self, pessimistic: bool) {
        self.session
            .set_autocommit_pessimistic_statement(pessimistic);
        let selected = pessimistic.then(|| {
            tidb_sessionctx::select_lock::SelectedLockKeys::new(
                tidb_txnkv::transaction::LockWaitTime::Timeout(self.session.lock_wait_timeout()),
                self.session.vars().shared_lock_promotion(),
            )
            .with_read_committed(self.session.is_read_committed())
            .with_lock_state_observer(self.session.transaction_lock_state_observer())
            .with_lock_in_execution(true)
        });
        self.session.set_selected_lock_keys(selected);
    }

    /// Go `doCommitWithRetry` (`session.go:806`) after an autocommit
    /// statement's commit failed: replay it when the error is retryable (Go
    /// `kv.IsTxnRetryableError`, narrowed to the write conflict that reaches
    /// this seam), `@@tidb_batch_insert` is off (its earlier batches are
    /// committed), and the failed transaction `could_retry` (Go
    /// `TxnCtx.CouldRetry`, decided once by its optimistic provider; a
    /// pessimistic one never sets it), within `s.retry`'s budget. The replay
    /// is pessimistic (`decideTxnMode`'s `RetryInfo.Retrying` arm).
    ///
    /// Go's `s.retry` replays the transaction's `StmtHistory`; this port's
    /// history is the one autocommit statement, so an explicit transaction
    /// (whose `CouldRetry` only restricted SQL can set, since user SQL
    /// always has `tidb_disable_txn_auto_retry = ON`) is not replayed.
    pub fn may_retry_autocommit_statement(&self, error: &SqlQueryError, retried: u32, could_retry: bool) -> bool {
        error.code == ERR_WRITE_CONFLICT
            && !self.session.vars().batch_insert()
            && could_retry
            && !self.txn.implicit_txn_pipelined()
            && retried < self.autocommit_retry_budget()
            && self.txn.explicit().is_none()
    }

    /// Go's autocommit retry budget: `@@tidb_retry_limit` (default 10),
    /// scaled DOWN by transaction size the way `doCommitWithRetry` does
    /// (`pkg/session/session.go:881-882`). A limit of 0 or less is Go's
    /// `commitRetryLimit > 0` gate.
    pub fn autocommit_retry_budget(&self) -> u32 {
        let limit = self
            .session
            .vars()
            .get_system(tidb_vardef::tidb_vars::TIDB_RETRY_LIMIT)
            .ok()
            .and_then(|value| value.parse::<i64>().ok())
            .unwrap_or(tidb_vardef::defaults::DEF_TIDB_RETRY_LIMIT);
        if limit <= 0 {
            return 0;
        }
        let limit = u32::try_from(limit).unwrap_or(u32::MAX);
        // Go: `maxRetryCount = limit - (limit-1) * txnSize/TxnTotalSizeLimit`.
        // This seam's autocommit writes are single statements far below the
        // 100 MiB `TxnTotalSizeLimit`, so the ratio is ~0 and the budget is
        // the full limit; the scaling is kept so a large statement retries
        // less, as Go's does.
        let txn_bytes = self.txn.autocommit_txn_bytes() as u64;
        let size_limit = tidb_config::config_tree::config::get_global_config()
            .performance
            .txn_total_size_limit
            .max(1);
        let reduction = u64::from(limit.saturating_sub(1)).saturating_mul(txn_bytes) / size_limit;
        limit
            .saturating_sub(u32::try_from(reduction).unwrap_or(u32::MAX))
            .max(1)
    }

    /// Go `ExecStmt.handlePessimisticLockError`'s
    /// `OnStmtErrorForNextAction(StmtErrAfterPessimisticLock, lockErr)`: `Ok`
    /// when the provider is ready to retry the statement, else the error the
    /// statement fails with (`lockErr` itself unless the provider answers
    /// another).
    fn provider_retries_lock_error(&mut self, lock_error: LockSqlError) -> Result<(), SqlQueryError> {
        let error = transactions::sql_error(lock_error);
        let in_txn = self.session.in_transaction();
        let Some(provider) = self.txn.provider_mut() else {
            return Err(error);
        };
        let mut cx = SessionTxnContext { session: self.session, in_txn, locking_read: false };
        match provider.on_stmt_error_for_next_action(
            &mut cx,
            tidb_sessiontxn::StmtErrorHandlePoint::AfterPessimisticLock,
            error.clone(),
        )? {
            tidb_sessiontxn::StmtErrorAction::RetryReady => Ok(()),
            _ => Err(error),
        }
    }

    /// Go `OnStmtErrorForNextAction(StmtErrAfterQuery)`: whether the
    /// transaction's provider asks the connection to retry a statement that
    /// failed with `code` before any row reached the client.
    pub fn retry_after_query_error(&mut self, code: u16) -> bool {
        let in_txn = self.session.in_transaction();
        let Some(provider) = self.txn.provider_mut() else {
            return false;
        };
        let mut cx = SessionTxnContext { session: self.session, in_txn, locking_read: false };
        matches!(
            provider.on_stmt_error_for_next_action(
                &mut cx,
                tidb_sessiontxn::StmtErrorHandlePoint::AfterQuery,
                SqlQueryError::new(code, *b"HY000", ""),
            ),
            Ok(tidb_sessiontxn::StmtErrorAction::RetryReady)
        )
    }

    /// Go `OnPessimisticStmtStart` on the session transaction's provider.
    fn on_provider_pessimistic_stmt_start(&mut self) -> Result<(), SqlQueryError> {
        let in_txn = self.session.in_transaction();
        let Some(provider) = self.txn.provider_mut() else {
            return Ok(());
        };
        let mut cx = SessionTxnContext { session: self.session, in_txn, locking_read: false };
        provider.on_pessimistic_stmt_start(&mut cx)
    }

    /// Go `OnPessimisticStmtEnd` on the session transaction's provider.
    fn on_provider_pessimistic_stmt_end(&mut self, is_successful: bool) -> Result<(), SqlQueryError> {
        let in_txn = self.session.in_transaction();
        let Some(provider) = self.txn.provider_mut() else {
            return Ok(());
        };
        let mut cx = SessionTxnContext { session: self.session, in_txn, locking_read: false };
        provider.on_pessimistic_stmt_end(&mut cx, is_successful)
    }

    /// Go `OnStmtStart(node)`, or `OnStmtRetry` when `retry`, on the session
    /// transaction's provider, if one is entered.
    fn on_provider_stmt_event(&mut self, retry: bool) -> Result<(), SqlQueryError> {
        self.on_provider_stmt(retry, None)
    }

    /// Go `TxnManager.OnStmtStart(ctx, node)` for a statement about to
    /// compile.
    pub fn on_provider_stmt_start(&mut self, node: &tidb_ast::Stmt) -> Result<(), SqlQueryError> {
        self.on_provider_stmt(false, Some(node))
    }

    fn on_provider_stmt(&mut self, retry: bool, node: Option<&tidb_ast::Stmt>) -> Result<(), SqlQueryError> {
        let in_txn = self.session.in_transaction();
        let Some(provider) = self.txn.provider_mut() else {
            return Ok(());
        };
        let mut cx = SessionTxnContext { session: self.session, in_txn, locking_read: false };
        if retry {
            return provider.on_stmt_retry(&mut cx);
        }
        provider.on_stmt_start(&mut cx, node)?;
        self.session.set_txn_plan_advisor(provider);
        Ok(())
    }

    pub fn open_read_snapshot(
        &mut self,
        shape: StatementReadShape,
        is_dml: bool,
    ) -> Result<Box<dyn ClusterSnapshot>, SqlQueryError> {
        // The explicit transaction, or an autocommit statement's implicit
        // one: its provider decides the timestamp (Go
        // `GetSnapshotWithStmtReadTS` / `GetSnapshotWithStmtForUpdateTS`; a
        // DML or locking read takes the for-update one).
        let in_txn = self.session.in_transaction();
        if let Some(provider) = self.txn.provider_mut() {
            let locking_read = shape == StatementReadShape::LockingRead;
            let mut cx = SessionTxnContext { session: self.session, in_txn, locking_read };
            let snapshot = if is_dml || locking_read {
                provider.get_snapshot_with_stmt_for_update_ts(&mut cx)
            } else {
                provider.get_snapshot_with_stmt_read_ts(&mut cx)
            };
            // Go `executorBuilder.getSnapshotTS`: in a locking SELECT, the
            // readers outside its `SelectLock` subtrees read at the read
            // timestamp (`GetSnapshotWithStmtReadTS`); an insert / update /
            // delete reads everything at the for-update one.
            if locking_read && !is_dml && provider.stmt_read_ts_precedes_for_update_ts() {
                let mut read_cx = SessionTxnContext { session: cx.session, in_txn, locking_read: false };
                let read_snapshot = provider.get_snapshot_with_stmt_read_ts(&mut read_cx)?;
                self.slot
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .bind_read_view(read_snapshot);
            }
            // Go `TxnCtx.SetForUpdateTS`: the statement's for-update
            // timestamp, which `tidb_last_query_info` reports.
            self.session
                .current_for_update_ts()
                .publish(provider.txn().for_update_ts());
            return snapshot;
        }
        // Every statement outside a transaction entered its implicit one
        // (`enter_implicit_transaction`) before opening its snapshot.
        Err(SqlQueryError::unknown(
            "an autocommit statement opened its snapshot without its implicit transaction",
        ))
    }

    /// Go `PrepareTxnCtx` -> `EnterNewTxn(EnterNewTxnBeforeStmt)` for a
    /// statement outside any transaction: an autocommit write enters its
    /// implicit transaction (pessimistic when `decideTxnMode` says so), a
    /// stale read the staleness provider, any other statement an implicit
    /// optimistic one, unprepared until its plan advice and warm-up
    /// (`AdviseOptimizeWithPlanAndThenWarmUp`). The entered provider then
    /// starts the statement (Go `OnStmtStart`), publishing its plan advisor.
    fn enter_implicit_transaction(
        &mut self,
        autocommit_write: bool,
        autocommit_pessimistic: bool,
        resource_group: &str,
        read_ts: &transactions::StatementReadTs,
    ) -> Result<(), SqlQueryError> {
        // The explicit transaction (a stale one included) serves the statement.
        if self.txn.current().is_some() {
            return Ok(());
        }
        self.enter_implicit_provider(autocommit_write, autocommit_pessimistic, resource_group, read_ts);
        self.on_provider_stmt_event(false)
    }

    fn enter_implicit_provider(
        &mut self,
        autocommit_write: bool,
        autocommit_pessimistic: bool,
        resource_group: &str,
        read_ts: &transactions::StatementReadTs,
    ) {
        // Go `EnterNewTxnWithReplaceProvider`: a statement's `AS OF` read
        // enters the staleness provider (never retryable).
        let request = match self.session.historical_read_ts() {
            Some(ts) => tidb_sessiontxn::EnterNewTxnRequest {
                tp: tidb_sessiontxn::EnterNewTxnType::WithReplaceProvider,
                stale_read_ts: ts,
                ..tidb_sessiontxn::EnterNewTxnRequest::default()
            },
            // Go `PrepareTxnCtx`'s `TxnMode: s.decideTxnMode(stmt)`.
            None => tidb_sessiontxn::EnterNewTxnRequest {
                tp: tidb_sessiontxn::EnterNewTxnType::BeforeStmt,
                txn_mode: if autocommit_write && autocommit_pessimistic {
                    tidb_sessiontxn::TxnMode::Pessimistic
                } else {
                    tidb_sessiontxn::TxnMode::Optimistic
                },
                ..tidb_sessiontxn::EnterNewTxnRequest::default()
            },
        };
        let inputs = crate::txn_manager::EnterNewTxnInputs {
            resource_group,
            schema_version: self.follower.schema_version(),
            read_ts: Some(read_ts),
            autocommit_write,
        };
        // An autocommit statement's transaction enters without failing; its
        // activation reports the error.
        let _ = self.txn.enter_new_txn(self.session, &request, inputs);
    }

    pub fn begin_read_statement(
        &mut self,
        shape: StatementReadShape,
        resource_group: &str,
    ) -> Result<ReadStatement, SqlQueryError> {
        self.on_provider_stmt_event(false)?;
        if let Some(transaction) = self.txn.explicit() {
            transaction
                .set_resource_group_name(resource_group)
                .map_err(SqlQueryError::unknown)?;
        }
        // An optimistic transaction's locking read runs as a read; its lock
        // operator publishes the keys, which `end_read_statement` flags.
        if shape == StatementReadShape::LockingRead
            && self
                .txn
                .explicit()
                .is_some_and(|transaction| !transaction.is_pessimistic())
        {
            self.session.set_selected_lock_keys(Some(
                tidb_sessionctx::select_lock::SelectedLockKeys::new(
                    tidb_txnkv::transaction::LockWaitTime::Timeout(
                        self.session.lock_wait_timeout(),
                    ),
                    self.session.vars().shared_lock_promotion(),
                )
                .with_lock_state_observer(self.session.transaction_lock_state_observer()),
            ));
        }
        let scope = self.begin_statement_scope(true);
        let statement = ReadStatement {
            _pin: self.txn.hold_schema_pin(self.follower.schema_version()),
            read_ts: scope.read_ts,
            savepoint: self.txn.buffer().checkpoint(),
            autocommit: scope.autocommit,
            resource_group: resource_group.to_owned(),
        };
        let setup = self
            .enter_implicit_transaction(false, false, resource_group, &statement.read_ts)
            .and_then(|()| self.open_read_snapshot(shape, false))
            .and_then(|snapshot| {
                drop(self.bind(snapshot));
                self.prepare_snapshot()
            });
        if let Err(error) = setup {
            let _ = self.end_read_statement(statement, false);
            return Err(error);
        }
        Ok(statement)
    }

    pub fn end_read_statement(
        &mut self,
        statement: ReadStatement,
        success: bool,
    ) -> Result<(), SqlQueryError> {
        let finished = self.finish_snapshot();
        // Go `CommitTxn` / `RollbackTxn` -> `setLastTxnInfoBeforeTxnEnd`: the
        // autocommit read's transaction, when the statement activated one.
        if statement.autocommit && self.session.historical_read_ts().is_none() {
            if let Some(start_ts) = statement.read_ts.get() {
                self.session.record_started_txn_info(start_ts);
            }
        }
        if success && self.txn.explicit().is_some() {
            self.lock_optimistic_statement_keys();
        }
        let result = if success {
            finished
                .and_then(|()| self.commit_if_session_left_transaction())
                .and_then(|()| {
                    self.flush_if_autocommit(statement.read_ts.get())
                })
        } else {
            self.txn.buffer().restore(statement.savepoint);
            finished
        };
        // Whatever the read did not commit ends with it (Go `RollbackTxn`).
        self.txn.rollback_implicit();
        self.txn.buffer().release(statement.savepoint);
        self.end_statement_scope(&StatementScope {
            read_ts: statement.read_ts.clone(),
            autocommit: statement.autocommit,
        });
        self.session.set_selected_lock_keys(None);
        self.session
            .retry_auto_ids()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clean();
        self.session.publish_transaction_buffer_metrics(
            self.txn.buffer().len(),
            self.txn.buffer().memory_footprint(),
        );
        result
    }

    /// Publishes the transaction a statement ended from the INSIDE.
    ///
    /// One statement does that: `SET autocommit = 1`, whose Go `SetSession`
    /// closure ends the ongoing transaction on the OFF->ON transition, so the
    /// statement's own finish commits it (captured: the row is durable
    /// immediately, and a `ROLLBACK` after it has nothing left to discard).
    /// The driver session applies that rule and clears its own transaction; the
    /// node hears about it only by asking, because no keyword went past
    /// [`Self::control_transaction`].
    ///
    /// The check is on the STATE rather than on the transition, so any future
    /// statement the session ends a transaction from is covered by the same
    /// line: an `explicit` the session no longer believes in is always one to
    /// publish.
    pub fn commit_if_session_left_transaction(&mut self) -> Result<(), SqlQueryError> {
        if self.txn.explicit().is_none() || self.session.in_transaction() {
            return Ok(());
        }
        self.commit_explicit()
    }

    /// Commits the implicit transaction when the session is not inside
    /// `BEGIN`; see [`TxnManager::flush_if_autocommit`].
    pub fn flush_if_autocommit(&mut self, read_ts: Option<u64>) -> Result<(), SqlQueryError> {
        self.txn.flush_if_autocommit(self.session, self.hooks.as_ref(), read_ts)
    }

    /// Ends the explicit transaction by publishing its buffer at its own start
    /// timestamp; see [`TxnManager::commit`].
    pub fn commit_explicit(&mut self) -> Result<(), SqlQueryError> {
        self.finish_snapshot()?;
        self.txn.commit(self.session, self.hooks.as_ref())
    }

    /// Drops the explicit transaction without publishing anything, along with
    /// every write it staged; see [`TxnManager::rollback`].
    pub fn discard_explicit(&mut self) -> Result<(), SqlQueryError> {
        self.finish_snapshot()?;
        self.txn.rollback(self.session)
    }

    /// See [`prepare_snapshot`].
    pub fn prepare_snapshot(&self) -> Result<(), SqlQueryError> {
        prepare_snapshot(self.slot)
    }

    /// See [`bind_snapshot`].
    pub fn bind(&self, snapshot: Box<dyn ClusterSnapshot>) -> Option<Box<dyn ClusterSnapshot>> {
        bind_snapshot(self.slot, self.session, snapshot)
    }

    /// See [`finish_snapshot`].
    pub fn finish_snapshot(&self) -> Result<(), SqlQueryError> {
        finish_snapshot(self.slot)
    }
}

/// Go `decideTxnMode` for an autocommit DML's implicit transaction:
/// pessimistic under `pessimistic-auto-commit` (bulk DML excepted), or for a
/// retry.
pub fn autocommit_dml_is_pessimistic(session: &Session, retrying: bool) -> bool {
    let vars = session.vars();
    crate::txn_mode::decide_txn_mode(
        crate::txn_mode::StatementTxnModeInputs {
            txn_mode_var: &vars.get_system("tidb_txn_mode").unwrap_or_default(),
            retrying,
            autocommit: true,
            pessimistic_auto_commit: tidb_config::config_tree::config::get_global_config()
                .pessimistic_txn
                .pessimistic_auto_commit
                .load(),
            is_dml: !vars.bulk_dml_enabled(),
        },
    )
    .is_pessimistic()
}

/// The slot every table of a session reads through.
pub type SnapshotSlot = Arc<Mutex<SwappableSnapshot>>;

/// Starts any asynchronous work the bound snapshot needs; the first read still
/// owns error delivery and timestamp publication.
pub fn prepare_snapshot(slot: &SnapshotSlot) -> Result<(), SqlQueryError> {
    slot.lock()
        .unwrap_or_else(|poison| poison.into_inner())
        .prepare()
        .map_err(|error| SqlQueryError::unknown(error.to_string()))
}

/// Binds the statement's snapshot into the slot, returning a handle a previous
/// statement left unbound.
pub fn bind_snapshot(
    slot: &SnapshotSlot,
    session: &Session,
    mut snapshot: Box<dyn ClusterSnapshot>,
) -> Option<Box<dyn ClusterSnapshot>> {
    // Go passes the statement ctx (its `RUDetails`) to every snapshot
    // call and sets the statement's resource-group tagger on it.
    snapshot.set_statement_kv_context(&session.statement_kv_context_cell());
    // Go `sessiontxn/isolation/base.go`: the transaction's snapshot reads a
    // temporary table's keys from the session, never the store.
    let snapshot = session.session_snapshot_interceptor(snapshot);
    slot.lock()
        .unwrap_or_else(|poison| poison.into_inner())
        .bind(snapshot)
}

/// Unbinds the statement's snapshot, ending the statement.
///
/// Dropping an autocommit statement's handle finishes its read transaction
/// on its own thread; dropping an explicit transaction's handle ends only
/// the statement, because the transaction outlives it. Either way the drop
/// is what makes the ordering unconditional.
pub fn finish_snapshot(slot: &SnapshotSlot) -> Result<(), SqlQueryError> {
    let bound = slot
        .lock()
        .unwrap_or_else(|poison| poison.into_inner())
        .unbind();
    drop(bound);
    Ok(())
}


impl tidb_executor::adapter::PessimisticDmlSession for StatementLifecycle<'_> {
    fn statement_txn_pessimistic(&self) -> Option<bool> {
        self.txn.current().map(|transaction| transaction.is_pessimistic())
    }

    fn explicit_txn_optimistic(&self) -> bool {
        self.txn.explicit().is_some_and(|transaction| !transaction.is_pessimistic())
    }

    fn statement_buffer(&self) -> &tidb_store_driver_txn::cluster_storage::MutationBuffer {
        self.txn.buffer()
    }

    fn take_statement_selected_locks(&mut self) -> tidb_sessionctx::select_lock::TakenLocks {
        self.session.take_selected_locks()
    }

    fn foreign_key_check_in_shared_lock(&self) -> bool {
        self.session.foreign_key_check_in_shared_lock()
    }

    fn statement_lock_wait(&self) -> tidb_txnkv::transaction::LockWaitTime {
        tidb_txnkv::transaction::LockWaitTime::Timeout(self.session.lock_wait_timeout())
    }

    fn statement_temporary_tables(&self) -> &tidb_tblctx::temporary_table::TemporaryTables {
        &self.session.txn_temporary_tables
    }

    fn publish_lock_waiting(&self, waiting: bool) {
        self.session.publish_transaction_lock_waiting(waiting);
    }

    fn record_lock_error(&mut self, code: u16) {
        self.session.record_pessimistic_lock_error(code);
    }

    fn lock_txn_keys(
        &self,
        keys: Vec<Vec<u8>>,
        presume_not_exists: std::collections::BTreeSet<Vec<u8>>,
        duplicate_hints: std::collections::BTreeMap<Vec<u8>, tidb_kv::cluster_storage::DuplicateKeyHint>,
        wait: tidb_txnkv::transaction::LockWaitTime,
        shared: bool,
    ) -> Option<Result<LockKeysOutcome, String>> {
        let transaction = self.txn.current()?;
        Some(if shared {
            transaction.lock_staged_keys_in_share_mode(keys, wait)
        } else {
            transaction.lock_staged_keys_with_assertions(keys, presume_not_exists, duplicate_hints, wait)
        })
    }
}

impl<'a> tidb_executor::adapter::HandlePessimisticDml for StatementLifecycle<'a> {
    type Sctx = Session;
    type ReadTs = transactions::StatementReadTs;

    fn read_ts_value(read_ts: &Self::ReadTs) -> Option<u64> {
        read_ts.get()
    }

    fn set_explicit_txn_statement_options(&mut self, resource_group: &str) -> Result<(), SqlQueryError> {
        if let Some(transaction) = self.txn.explicit() {
            transaction
                .set_resource_group_name(resource_group)
                .map_err(SqlQueryError::unknown)?;
            transaction
                .set_statement_assertion_level(
                    tidb_domain_core::session_transaction::set_options_on_txn_active(self.session.vars()).assertion_level,
                )
                .map_err(SqlQueryError::unknown)?;
        }
        Ok(())
    }

    fn provider_stmt_event(&mut self, retry: bool) -> Result<(), SqlQueryError> {
        StatementLifecycle::on_provider_stmt_event(self, retry)
    }

    fn begin_execution_attempt(&mut self) {
        // Selected rows belong to this execution attempt, not an earlier
        // plan or a partially executed failed read.
        self.session.take_selected_lock_keys();
        if let Some(transaction) = self.txn.explicit() {
            let _ = transaction.take_statement_lock_failure();
        }
    }

    fn explicit_txn_open(&self) -> bool {
        self.txn.explicit().is_some()
    }

    fn decide_autocommit_pessimistic(&self, retrying: bool) -> bool {
        StatementLifecycle::autocommit_write_is_pessimistic(self, retrying)
    }

    fn begin_autocommit_locks(&mut self, pessimistic: bool) {
        StatementLifecycle::begin_autocommit_statement_locks(self, pessimistic);
    }

    fn enter_statement_implicit_txn(
        &mut self,
        autocommit_write: bool,
        pessimistic: bool,
        resource_group: &str,
        read_ts: &Self::ReadTs,
    ) -> Result<(), SqlQueryError> {
        StatementLifecycle::enter_implicit_transaction(self, autocommit_write, pessimistic, resource_group, read_ts)
    }

    fn bind_statement_snapshot(&mut self, shape: StatementReadShape, is_dml: bool) -> Result<(), SqlQueryError> {
        let snapshot = self.open_read_snapshot(shape, is_dml)?;
        if let Some(stale) = self.bind(snapshot) {
            // A previous statement that did not unbind would otherwise leave
            // its read transaction open for the rest of the connection.
            drop(stale);
        }
        if let Err(error) = self.prepare_snapshot() {
            let _ = self.finish_snapshot();
            return Err(error);
        }
        Ok(())
    }

    fn finish_statement_snapshot(&self) -> Result<(), SqlQueryError> {
        StatementLifecycle::finish_snapshot(self)
    }

    fn provider_pessimistic_stmt_start(&mut self) -> Result<(), SqlQueryError> {
        StatementLifecycle::on_provider_pessimistic_stmt_start(self)
    }

    fn provider_pessimistic_stmt_end(&mut self, is_successful: bool) -> Result<(), SqlQueryError> {
        StatementLifecycle::on_provider_pessimistic_stmt_end(self, is_successful)
    }

    fn run_execution<T>(
        &mut self,
        run: &mut impl FnMut(&mut Session) -> Result<T, SqlQueryError>,
    ) -> Result<T, SqlQueryError> {
        run(self.session)
    }

    fn end_execution_attempt(&mut self, read_ts: &Self::ReadTs, autocommit_write: bool) {
        // Go `CommitTxn` / `RollbackTxn` -> `setLastTxnInfoBeforeTxnEnd` for
        // the transaction an autocommit statement ends: recorded only when
        // the statement activated one (`TxnCtx.StartTS != 0`), before the
        // commit whose hook overwrites it.
        if self.txn.explicit().is_none() && self.session.historical_read_ts().is_none() {
            if let Some(start_ts) = read_ts.get() {
                self.session.record_started_txn_info(start_ts);
            }
        }
        // Go `TxnCtx.GetForUpdateTS` as the statement left it: a deferred
        // handle advances the for-update timestamp at its first read, after
        // the open published the transaction's earlier one.
        if let Some(transaction) = self.txn.explicit() {
            self.session.current_for_update_ts().publish(transaction.for_update_ts());
        }
        if autocommit_write {
            // The attempt's locking reads end here; the statement's
            // `TxnCtx.IsPessimistic` stands until the next statement.
            self.session.set_selected_lock_keys(None);
        }
    }

    fn provider_retry_lock_error(&mut self, lock_error: LockSqlError) -> Result<(), SqlQueryError> {
        StatementLifecycle::provider_retries_lock_error(self, lock_error)
    }

    fn is_load_data_local(&self) -> bool {
        // The connection accepted a LOCAL load for this statement
        // (`preprocess_load_data_local`) and clears it once it finished.
        self.session.local_infile_accepted
    }

    fn record_statement_retry(&mut self) {
        self.session.record_statement_retry();
    }

    fn lock_error_retry_breakpoint(&self) {
        tidb_breakpoint::inject(&*self.session, "lockErrorAndThenOnStmtRetryCalled");
    }

    fn take_execution_lock_failure(&self) -> Option<StatementLockFailure> {
        self.txn.current().and_then(|transaction| transaction.take_statement_lock_failure())
    }

    fn retract_statement_failure(&mut self, code: u16, message: &str) {
        self.session.retract_statement_failure(code, message);
    }

    fn rerecord_last_query_error(&mut self, error: &str) {
        self.session.rerecord_last_query_error(error);
    }

    fn pending_max_exec_time_error(&self) -> Option<SqlQueryError> {
        // Go `handlePendingSQLKillerSignal`, for the timeout signal only.
        let killer = self.session.sql_killer();
        if killer.get_kill_signal() != tidb_sqlkiller::KillSignal::MaxExecTimeExceeded {
            return None;
        }
        killer
            .handle_signal()
            .map(|error| SqlQueryError::from(tidb_terror::MysqlError::from_terror(&error)))
    }

    fn finish_stmt(&mut self, read_ts: &Self::ReadTs) -> Result<(), SqlQueryError> {
        self.commit_if_session_left_transaction().and_then(|()| {
            // Go `finishStmt` -> `autoCommitAfterStmt`: outside a
            // transaction, `session.CommitTxn` -> `session.doCommitWithRetry`.
            let ends_transaction = self.txn.explicit().is_none() && !self.session.in_transaction();
            let _commit = ends_transaction.then(|| {
                (
                    tidb_tracing::begin_thread_region_scope("session.CommitTxn"),
                    tidb_tracing::begin_thread_region_scope("session.doCommitWithRetry"),
                )
            });
            self.flush_if_autocommit(read_ts.get())
        })
    }

    fn rollback_failed_statement(&mut self, committing: bool, savepoint: BufferCheckpoint) {
        // Go `autoCommitAfterStmt`: a statement that failed outside a
        // transaction is rolled back under `session.RollbackTxn`.
        let _rollback_txn = (!committing && self.txn.explicit().is_none() && !self.session.in_transaction())
            .then(|| tidb_tracing::start_thread_region_ex("session.RollbackTxn"));
        self.txn.rollback_implicit();
        // Cancel the client's active fair scope. Ordinary locks remain
        // transaction-owned, following Go StmtRollback.
        if self.txn.explicit().is_some() {
            let _ = StatementLifecycle::on_provider_pessimistic_stmt_end(self, false);
            // The statement's writes go; the transaction keeps its earlier
            // writes and native lock ownership.
            self.txn.buffer().restore(savepoint);
        } else {
            // Autocommit rollback ends the whole native transaction. Its
            // statement checkpoint cannot outlive that owner.
            self.txn.buffer().reset();
        }
    }
}

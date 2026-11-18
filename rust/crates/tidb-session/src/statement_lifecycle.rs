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

/// The session layer's next move after a pessimistic statement's lock step.
pub enum PessimisticStep {
    /// The statement stands; its keys are locked.
    Done,
    /// Re-execute the statement reading at this advanced `for_update_ts`.
    Retry {
        /// Go's `lockErr`, for the provider's decision.
        lock_error: LockSqlError,
    },
}

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
    /// Whether the running write is one whose transaction Go's executor
    /// builder activates before it runs.
    pub activate_statement_txn: bool,
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
            if let Some(hook) = self.session.mem_db_footprint_hook() {
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
        savepoint: &BufferCheckpoint,
        resource_group: &str,
        retrying: bool,
        is_dml: bool,
        run: &mut impl FnMut(&mut Session) -> Result<T, SqlQueryError>,
        read_ts: &transactions::StatementReadTs,
    ) -> Result<T, SqlQueryError> {
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
        // Go `handlePessimisticDML`'s statement loop: run, lock the staged
        // keys, and on a lock conflict roll the STATEMENT back and re-execute
        // it reading at the advanced `for_update_ts` (the provider's
        // `OnStmtRetry`). An implicit transaction's provider is entered fresh
        // inside the loop.
        self.on_provider_stmt_event(false)?;
        let mut retries = tidb_store_driver_error::pessimistic_lock_error::PessimisticStatementRetry::default();
        // Whether the statement reached `finishStmt`'s commit: a failure
        // there is the commit's own, not one `autoCommitAfterStmt` rolls
        // back.
        let mut committing = false;
        let result = loop {
            // Selected rows belong to this execution attempt, not an earlier
            // plan or a partially executed failed read.
            self.session.take_selected_lock_keys();
            if let Some(transaction) = self.txn.explicit() {
                let _ = transaction.take_statement_lock_failure();
            }
            // Go `decideTxnMode`: under the default `pessimistic-auto-commit =
            // false` an autocommit DML runs optimistically -- a read at its
            // start timestamp and a one-phase prewrite -- and only the retry
            // of a write conflict locks first. `shouldUsePessimisticAutoCommit`
            // (`session.go:4947`) reads the config and declines under bulk DML
            // (`tidb_dml_type = 'bulk'`); this arm only serves DML. The answer
            // is the implicit transaction's `TxnCtx.IsPessimistic`, so the
            // statement plans its point source with `Lock`
            // (`tryUpdatePointPlan`) and locks it inside execution with the
            // session's `LockWaitTimeout` (`getLockWaitTime`).
            let autocommit_write =
                self.txn.explicit().is_none() && shape == StatementReadShape::AutocommitWrite;
            let autocommit_pessimistic =
                autocommit_write && self.autocommit_write_is_pessimistic(retrying);
            if autocommit_write {
                self.begin_autocommit_statement_locks(autocommit_pessimistic);
            }
            self.enter_implicit_transaction(shape, autocommit_write, autocommit_pessimistic, resource_group, read_ts);
            let snapshot = {
                match self.open_read_snapshot(shape, read_ts, resource_group, is_dml)
                {
                    Ok(snapshot) => snapshot,
                    Err(error) => break Err(error),
                }
            };
            if let Some(stale) = self.bind(snapshot) {
                // A previous statement that did not unbind would otherwise
                // leave its read transaction open for the rest of the
                // connection.
                drop(stale);
            }
            self.declare_read_shape(shape);
            if let Err(error) = self.prepare_snapshot() {
                let _ = self.finish_snapshot();
                break Err(error);
            }
            // Go's executor builder activates an autocommit INSERT, UPDATE or
            // DELETE before it runs (`updateForUpdateTS` ->
            // `GetStmtForUpdateTS`): the statement has its start timestamp --
            // the one it commits at, and the one its slow log and
            // `tidb_last_query_info` report -- even when it never reads or
            // fails before reading. Asking the deferred snapshot for it is
            // that activation; a failed open is reported by the read or
            // commit that follows.
            if self.activate_statement_txn
                && self.txn.explicit().is_none()
                && shape != StatementReadShape::AutocommitWrite
            {
                let _ = self
                    .slot
                    .lock()
                    .unwrap_or_else(|poison| poison.into_inner())
                    .start_ts();
            }
            // Go's executor builder activates a DML's transaction before the
            // statement runs (`updateForUpdateTS` -> `GetStmtForUpdateTS`);
            // inside a pending `autocommit = 0` transaction that is its
            // activation.
            if self.activate_statement_txn {
                if let Some(transaction) = self.txn.explicit() {
                    if let Err(error) = transaction.activate() {
                        let _ = self.finish_snapshot();
                        break Err(SqlQueryError::unknown(error));
                    }
                }
            }
            let outcome = run(&mut self.session);
            // Go's executor builder activates an autocommit DML's transaction
            // before it runs (`updateForUpdateTS`), so a statement that never
            // read still has its `TxnCtx.StartTS`, from the same future.
            if autocommit_write {
                if let Some(transaction) = self.txn.current() {
                    let _ = transaction.activate();
                }
            }
            // Go `CommitTxn` / `RollbackTxn` -> `setLastTxnInfoBeforeTxnEnd`
            // for the transaction an autocommit statement ends: recorded
            // only when the statement activated one (`TxnCtx.StartTS != 0`),
            // before the commit whose hook overwrites it.
            if self.txn.explicit().is_none() && self.session.historical_read_ts().is_none() {
                if let Some(start_ts) = read_ts.get() {
                    self.session.record_started_txn_info(start_ts);
                }
            }
            // Go `TxnCtx.GetForUpdateTS` as the statement left it: a deferred
            // handle advances the for-update timestamp at its first read,
            // after the open published the transaction's earlier one.
            if let Some(transaction) = self.txn.explicit() {
                self.session
                    .current_for_update_ts()
                    .publish(transaction.for_update_ts());
            }
            if autocommit_write {
                // The attempt's locking reads end here; the statement's
                // `TxnCtx.IsPessimistic` stands until the next statement.
                self.session.set_selected_lock_keys(None);
            }
            let finished = self.finish_snapshot();
            match outcome {
                Ok(value) => {
                    if let Err(error) = finished {
                        break Err(error);
                    }
                    match self.lock_pessimistic_statement_keys(savepoint) {
                        Ok(PessimisticStep::Done) => {
                            if let Some(transaction) = self.txn.current() {
                                if let Err(error) = transaction.finish_pessimistic_statement(true) {
                                    break Err(SqlQueryError::unknown(error));
                                }
                            }
                        }
                        Ok(PessimisticStep::Retry { lock_error, .. }) if self.txn.explicit().is_none() => {
                            // The autocommit statement's lock found a newer
                            // committed version of a key it wrote: as for its
                            // locking reads, the replay is a new implicit
                            // transaction reading the version that beat it.
                            if let Err(error) = self.provider_retries_lock_error(lock_error) {
                                break Err(error);
                            }
                            if let Err(error) = retries.retry() {
                                break Err(transactions::sql_error(error));
                            }
                            self.session.record_statement_retry();
                            self.txn.rollback_implicit();
                            self.txn.buffer().restore(savepoint.clone());
                            tidb_breakpoint::inject(&*self.session, "lockErrorAndThenOnStmtRetryCalled");
                            continue;
                        }
                        Ok(PessimisticStep::Retry { lock_error }) => {
                            if let Err(error) = self.provider_retries_lock_error(lock_error) {
                                break Err(error);
                            }
                            if let Err(error) = retries.retry() {
                                break Err(transactions::sql_error(error));
                            }
                            self.session.record_statement_retry();
                            // The statement's writes go back; its locks STAY
                            // (fair locking's whole point), and the replay
                            // reads at the timestamp that sees the version
                            // that beat it.
                            self.txn.buffer().restore(savepoint.clone());
                            if let Err(error) = self.on_provider_stmt_event(true) {
                                break Err(error);
                            }
                            tidb_breakpoint::inject(
                                &*self.session,
                                "lockErrorAndThenOnStmtRetryCalled",
                            );
                            pessimistic_retry_failpoint(is_dml);
                            continue;
                        }
                        Err(error) => {
                            break Err(error);
                        }
                    }
                    committing = true;
                    match self.commit_if_session_left_transaction().and_then(|()| {
                        // Go `finishStmt` -> `autoCommitAfterStmt`: outside a
                        // transaction, `session.CommitTxn` ->
                        // `session.doCommitWithRetry`.
                        let ends_transaction =
                            self.txn.explicit().is_none() && !self.session.in_transaction();
                        let _commit = ends_transaction.then(|| {
                            (
                                tidb_tracing::begin_thread_region_scope("session.CommitTxn"),
                                tidb_tracing::begin_thread_region_scope(
                                    "session.doCommitWithRetry",
                                ),
                            )
                        });
                        self.flush_if_autocommit(read_ts.get(), resource_group)
                    }) {
                        Ok(()) => break Ok(value),
                        Err(error) => break Err(error),
                    }
                }
                Err(error) => {
                    // A locking point read locked during execution (Go
                    // `PointGetExecutor.getAndLock`) and the statement failed
                    // on that lock: act on the lock's outcome as
                    // `handlePessimisticLockError` does.
                    // Inside an explicit transaction the transaction keeps
                    // the outcome; an autocommit statement's lock, taken
                    // after its read, left it in `lock_failure`.
                    let failure =
                        self.txn.current().and_then(|transaction| transaction.take_statement_lock_failure());
                    if failure.is_some() {
                        // The storage error the lock raised is the statement
                        // owner's to consume, as `handlePessimisticLockError`
                        // does: what the client sees is the lock outcome.
                        self.session
                            .retract_statement_failure(error.code, &error.message);
                    }
                    if let (None, Some(StatementLockFailure::Retry { lock_error, .. })) =
                        (self.txn.explicit(), failure.as_ref())
                    {
                        // The autocommit statement's lock found a newer
                        // committed version of a row it read.
                        if let Err(error) = self.provider_retries_lock_error(lock_error.clone()) {
                            break Err(error);
                        }
                        if let Err(error) = retries.retry() {
                            break Err(transactions::sql_error(error));
                        }
                        self.session.record_statement_retry();
                        // The implicit transaction has nothing earlier to
                        // keep: the replay is a new one, reading the version
                        // that beat the statement.
                        self.txn.rollback_implicit();
                        self.txn.buffer().restore(savepoint.clone());
                        tidb_breakpoint::inject(
                            &*self.session,
                            "lockErrorAndThenOnStmtRetryCalled",
                        );
                        continue;
                    }
                    match failure {
                        None => break Err(error),
                        Some(StatementLockFailure::Retry { lock_error }) => {
                            self.session.record_pessimistic_lock_error(tidb_errno::errcode::ErrWriteConflict);
                            if let Err(error) = self.provider_retries_lock_error(lock_error) {
                                break Err(error);
                            }
                            if let Err(error) = retries.retry() {
                                break Err(transactions::sql_error(error));
                            }
                            self.session.record_statement_retry();
                            // The statement's writes go back; its locks stay,
                            // and the replay reads at the timestamp that sees
                            // the version that beat it.
                            self.txn.buffer().restore(savepoint.clone());
                            if let Err(error) = self.on_provider_stmt_event(true) {
                                break Err(error);
                            }
                            tidb_breakpoint::inject(
                                &*self.session,
                                "lockErrorAndThenOnStmtRetryCalled",
                            );
                            pessimistic_retry_failpoint(is_dml);
                            continue;
                        }
                        Some(
                            StatementLockFailure::Statement(error)
                            | StatementLockFailure::Transaction(error),
                        ) => {
                            self.session.record_pessimistic_lock_error(error.code);
                            let error = transactions::sql_error(error);
                            self.session
                                .rerecord_last_query_error(&error.error_string());
                            break Err(error);
                        }
                    }
                }
            }
        };
        if result.is_err() {
            // Go `autoCommitAfterStmt`: a statement that failed outside a
            // transaction is rolled back under `session.RollbackTxn`.
            let _rollback_txn =
                (!committing && self.txn.explicit().is_none() && !self.session.in_transaction())
                    .then(|| tidb_tracing::start_thread_region_ex("session.RollbackTxn"));
            self.txn.rollback_implicit();
            // Cancel the client's active fair scope. Ordinary locks remain
            // transaction-owned, following Go StmtRollback.
            if let Some(transaction) = self.txn.explicit() {
                let _ = transaction.finish_pessimistic_statement(false);
                // The statement's writes go; the transaction keeps its
                // earlier writes and native lock ownership.
                self.txn.buffer().restore(*savepoint);
            } else {
                // Autocommit rollback ends the whole native transaction.
                // Its statement checkpoint cannot outlive that owner.
                self.txn.buffer().reset();
            }
        }
        result
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

    /// Go `handlePessimisticDML`'s lock step for the statement that just ran:
    /// what the transaction says about the keys it staged.
    ///
    /// It locks in the statement's transaction: the explicit one, or a
    /// pessimistic autocommit statement's implicit one (Go's one `LazyTxn`,
    /// which `handlePessimisticDML` locks the same way).
    pub fn lock_pessimistic_statement_keys(
        &mut self,
        savepoint: &BufferCheckpoint,
    ) -> Result<PessimisticStep, SqlQueryError> {
        let Some(transaction) = self.txn.current() else {
            return Ok(PessimisticStep::Done);
        };
        if !transaction.is_pessimistic() {
            self.lock_optimistic_statement_keys();
            return Ok(PessimisticStep::Done);
        }
        // Go `handlePessimisticDML`: `KeysNeedToLock` plus
        // `CollectUnchangedKeysForXLock` lock in ONE `LockKeys`. The rows a
        // plan's `SelectLockExec` read were locked inside execution, and a
        // key the transaction already holds takes no second request, so what
        // is left here is what the plan did not lock: a statement whose lock
        // operator never saw its child drained, and the keys it wrote.
        let taken = self.session.take_selected_locks();
        let mut requests = taken.reads;
        let mut keys = self.txn.buffer().pessimistic_keys_since(*savepoint);
        keys.extend(taken.unchanged);
        // With `tidb_foreign_key_check_in_shared_lock` off, Go locks the
        // foreign-key checks' keys exclusively with the others; on, it takes
        // them in share mode after the exclusive locks, except a key this
        // transaction wrote (`moveWrittenSharedLockKeysToExclusive`).
        let mut shared_keys = Vec::new();
        if self.session.foreign_key_check_in_shared_lock() {
            let exclusive: std::collections::BTreeSet<Vec<u8>> = keys.iter().cloned().collect();
            for key in taken.unchanged_shared {
                if exclusive.contains(&key) {
                    continue;
                }
                // A key written by this transaction must not be protected
                // only by a shared lock: prewrite would put over it.
                if self.txn.buffer().get(&tidb_txnkv::Key::from(key.clone())).is_some() {
                    keys.push(key);
                } else {
                    shared_keys.push(key);
                }
            }
        } else {
            keys.extend(taken.unchanged_shared);
        }
        keys.sort();
        keys.dedup();
        let wait = tidb_txnkv::transaction::LockWaitTime::Timeout(self.session.lock_wait_timeout());
        if !keys.is_empty() {
            requests.push((wait, keys));
        }
        for (wait, keys) in requests {
            match self.lock_pessimistic_keys(savepoint, keys, wait, false)? {
                PessimisticStep::Done => {}
                retry => return Ok(retry),
            }
        }
        if !shared_keys.is_empty() {
            return self.lock_pessimistic_keys(savepoint, shared_keys, wait, true);
        }
        Ok(PessimisticStep::Done)
    }

    /// Go `SelectLockExec.Next` -> `doLockKeys` in an optimistic
    /// transaction: `LockKeys` takes no lock and only flags the keys the
    /// statement's lock operators published (see
    /// `MutationBuffer::lock_keys_optimistically`).
    fn lock_optimistic_statement_keys(&mut self) {
        if self
            .txn
            .explicit()
            .is_none_or(|transaction| transaction.is_pessimistic())
        {
            return;
        }
        let taken = self.session.take_selected_locks();
        let keys: Vec<Vec<u8>> = taken.reads.into_iter().flat_map(|(_, keys)| keys).collect();
        self.txn.buffer().lock_keys_optimistically(&keys);
    }

    pub fn lock_pessimistic_keys(
        &mut self,
        savepoint: &BufferCheckpoint,
        keys: Vec<Vec<u8>>,
        wait: tidb_txnkv::transaction::LockWaitTime,
        shared: bool,
    ) -> Result<PessimisticStep, SqlQueryError> {
        // Go `tryLockKeys`' `filterTemporaryTableKeys`: a temporary table's
        // rows are never locked in the store.
        let keys = tidb_tblctx::temporary_table::filter_temporary_table_keys(&self.session.txn_temporary_tables, keys);
        let Some(transaction) = self.txn.current() else {
            return Ok(PessimisticStep::Done);
        };
        // Table-selected native flags decide which keys need locking. A
        // live session switch cannot override an earlier key's selected policy.
        let key_set: std::collections::BTreeSet<Vec<u8>> = keys.iter().cloned().collect();
        let presume_not_exists = self
            .txn
            .buffer()
            .presume_not_exists_since(*savepoint)
            .into_iter()
            .filter(|key| key_set.contains(key))
            .collect::<std::collections::BTreeSet<_>>();
        // Ownership removes a repeated acquisition, not a NEW absence check.
        // Go KVTxn.LockKeys verifies NeedCheckExists even on a held key.
        if keys.is_empty() {
            return Ok(PessimisticStep::Done);
        }
        // Go `ExtractKeyExistsErrFromHandle` also decodes keys staged by earlier statements.
        let duplicate_hints = keys
            .iter()
            .filter_map(|key| {
                self.txn
                    .buffer()
                    .duplicate_key_hint_for(key)
                    .map(|hint| (key.clone(), hint))
            })
            .collect();
        // Every error exit rolls the STATEMENT back -- Go's `StmtRollback`
        // runs on any statement error, transport failures included.
        self.session.publish_transaction_lock_waiting(true);
        let lock_result = if shared {
            transaction.lock_staged_keys_in_share_mode(keys, wait)
        } else {
            transaction.lock_staged_keys_with_assertions(keys, presume_not_exists, duplicate_hints, wait)
        };
        self.session.publish_transaction_lock_waiting(false);
        let outcome = match lock_result {
            Ok(outcome) => outcome,
            Err(error) => {
                self.txn.buffer().restore(savepoint.clone());
                return Err(SqlQueryError::unknown(error));
            }
        };
        match outcome {
            LockKeysOutcome::Locked { .. } => Ok(PessimisticStep::Done),
            LockKeysOutcome::RetryStatement { lock_error, .. } => {
                self.session.record_pessimistic_lock_error(tidb_errno::errcode::ErrWriteConflict);
                Ok(PessimisticStep::Retry { lock_error })
            }
            LockKeysOutcome::StatementError(error) => {
                self.session.record_pessimistic_lock_error(error.code);
                // Statement-scoped, Go's 1205/1213 family: this statement's
                // writes go, the transaction stays open.
                self.txn.buffer().restore(savepoint.clone());
                Err(transactions::sql_error(error))
            }
            LockKeysOutcome::TransactionError(error) => {
                self.session.record_pessimistic_lock_error(error.code);
                // The transaction is no longer usable; later statements and
                // `ROLLBACK` report that state on their own.
                self.txn.buffer().restore(savepoint.clone());
                Err(transactions::sql_error(error))
            }
        }
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
        read_ts: &transactions::StatementReadTs,
        resource_group: &str,
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
            // Go `TxnCtx.SetForUpdateTS`: the statement's for-update
            // timestamp, which `tidb_last_query_info` reports.
            self.session
                .current_for_update_ts()
                .publish(provider.txn().for_update_ts());
            return snapshot;
        }
        if shape == StatementReadShape::AutocommitPointGet {
            let snapshot = self
                .txn
                .transactions()
                .open_max_ts_snapshot(resource_group)
                .map_err(SqlQueryError::unknown)?;
            // Go `AdviseOptimizeWithPlan` -> `forcePrepareConstStartTS(
            // math.MaxUint64)`: the point get's `getSnapshot` activates the
            // transaction at that constant, which its commit counts and
            // `tidb_last_txn_info` reports.
            read_ts.activate_const_start_ts(u64::MAX);
            return Ok(snapshot);
        }
        // Every other statement outside a transaction entered its implicit
        // one (`enter_implicit_transaction`) before opening its snapshot.
        Err(SqlQueryError::unknown(
            "an autocommit statement opened its snapshot without its implicit transaction",
        ))
    }

    /// Go `PrepareTxnCtx` -> `EnterNewTxn(EnterNewTxnBeforeStmt)` for a
    /// statement outside any transaction: an autocommit write enters its
    /// implicit transaction (pessimistic when `decideTxnMode` says so), any
    /// other statement an implicit optimistic one -- except a stale read,
    /// which has its own timestamp, and a point get the plan advised to read
    /// the latest version (`AdviseOptimizeWithPlan`), which needs none.
    fn enter_implicit_transaction(
        &mut self,
        shape: StatementReadShape,
        autocommit_write: bool,
        autocommit_pessimistic: bool,
        resource_group: &str,
        read_ts: &transactions::StatementReadTs,
    ) {
        // The explicit transaction (a stale one included) serves the statement.
        if self.txn.current().is_some() {
            return;
        }
        let vars = self.session.vars();
        let options = tidb_domain_core::session_transaction::set_options_on_txn_active(vars)
            .with_statement_kv_context(self.session.statement_kv_context_cell())
            .with_temporary_tables(self.session.temporary_table_ids())
            .with_guarantee_linearizability(self.session.guarantee_linearizability())
            .with_kill_signal(self.session.kv_kill_signal());
        // Go `EnterNewTxnWithReplaceProvider`: a statement's `AS OF` read
        // enters the staleness provider (never retryable).
        if let Some(ts) = self.session.historical_read_ts() {
            self.txn.enter_stale(true, ts, resource_group, options);
            return;
        }
        self.txn.decide_could_retry(
            self.session,
            // `decide_could_retry`'s `pessimistic` argument: no optimistic
            // provider decides for a pessimistic transaction.
            autocommit_write && autocommit_pessimistic,
            tidb_sessiontxn::EnterNewTxnType::BeforeStmt,
            false,
        );
        if autocommit_write {
            // Go `OnPessimisticStmtStart` (`isolation/base.go:711`): fair
            // locking follows the session variable; a client connection is
            // neither internal nor restricted SQL.
            let fair_locking = vars.pessimistic_transaction_fair_locking();
            let isolation = self.session.isolation_for_provider();
            self.txn.enter_implicit_write(
                autocommit_pessimistic,
                &isolation,
                fair_locking,
                resource_group,
                options,
                read_ts.clone(),
            );
        } else if shape != StatementReadShape::AutocommitPointGet {
            let low_resolution = self.session.use_low_resolution_tso();
            self.txn.enter_implicit_read(low_resolution, resource_group, options, read_ts.clone());
        }
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
        self.enter_implicit_transaction(shape, false, false, resource_group, &statement.read_ts);
        let setup = self
            .open_read_snapshot(shape, &statement.read_ts, resource_group, false)
            .and_then(|snapshot| {
                drop(self.bind(snapshot));
                self.declare_read_shape(shape);
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
                    self.flush_if_autocommit(statement.read_ts.get(), &statement.resource_group)
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

    /// Publishes the buffer when the session is not inside `BEGIN`; see
    /// [`TxnManager::flush_if_autocommit`].
    pub fn flush_if_autocommit(
        &mut self,
        read_ts: Option<u64>,
        resource_group: &str,
    ) -> Result<(), SqlQueryError> {
        self.txn.flush_if_autocommit(self.session, self.hooks.as_ref(), read_ts, resource_group)
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

    /// See [`declare_read_shape`].
    pub fn declare_read_shape(&self, shape: StatementReadShape) {
        declare_read_shape(self.slot, shape);
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

/// Tells the just-bound snapshot what shape the statement's whole read is,
/// before the statement runs and therefore before its first read.
///
/// This is Go's `AdviseOptimizeWithPlan`, and the position is the point of
/// it: after the plan-shape question has been answered and before any
/// timestamp has been spent. The declaration reaches only a snapshot that
/// can take it -- an explicit transaction's read handle refuses by
/// inheriting the trait default, which is `IsAutoCommitTxn`'s `!InTxn`
/// half made structural rather than re-asked here.
pub fn declare_read_shape(slot: &SnapshotSlot, shape: StatementReadShape) {
    if shape != StatementReadShape::AutocommitPointGet {
        return;
    }
    slot.lock()
        .unwrap_or_else(|poison| poison.into_inner())
        .declare_autocommit_point_get();
}

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

/// Go `handlePessimisticDML`'s `pessimisticDMLRetry` before each retry, and
/// `runPessimisticSelectForUpdate`'s `pessimisticSelectForUpdateRetry` at
/// its loop end.
fn pessimistic_retry_failpoint(is_dml: bool) {
    tidb_failpoint::eval(if is_dml {
        "github.com/pingcap/tidb/pkg/executor/pessimisticDMLRetry"
    } else {
        "github.com/pingcap/tidb/pkg/executor/pessimisticSelectForUpdateRetry"
    });
}

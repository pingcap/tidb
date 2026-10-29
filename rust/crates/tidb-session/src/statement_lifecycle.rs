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

use tidb_store_driver::cluster_table_storage::{LockKeysOutcome, StatementLockFailure};
use tidb_store_driver_error::sql_query_error::SqlQueryError;
use tidb_executor::access_path::StatementReadShape;
use tidb_kv::cluster_storage::{BufferCheckpoint, ClusterSnapshot, SwappableSnapshot};

use tidb_domain_core::cluster_transactions as transactions;
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
        /// The statement timestamp the replay reads at.
        for_update_ts: u64,
    },
}

pub struct ReadStatement {
    _pin: Box<dyn Send>,
    read_ts: transactions::StatementReadTs,
    savepoint: BufferCheckpoint,
    autocommit: bool,
    resource_group: String,
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
        let autocommit = self.txn.explicit.is_none() && self.session.historical_read_ts().is_none();
        if autocommit {
            self.session.current_tso().clear();
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
        let savepoint = self.txn.buffer().checkpoint();
        // An optimistic transaction's locking reads lock nothing during
        // execution; their keys reach the owner's lock step, which flags
        // them as Go's optimistic `LockKeys` does.
        self.session
            .set_selected_lock_keys(self.txn.explicit.as_ref().map(|transaction| {
                tidb_sessionctx::select_lock::SelectedLockKeys::new(
                    tidb_txnkv::transaction::LockWaitTime::Timeout(
                        self.session.lock_wait_timeout(),
                    ),
                    self.session.vars().shared_lock_promotion_enabled(),
                )
                .with_read_committed(self.session.is_read_committed())
                .with_lock_state_observer(self.session.transaction_lock_state_observer())
                .with_lock_in_execution(transaction.is_pessimistic())
            }));
        let delta_savepoint = self.session.table_delta_savepoint();
        let mut retried: u32 = 0;
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
                    if !self.may_retry_autocommit_statement(&error, retried) {
                        break Err(error);
                    }
                    retried += 1;
                    back_off(retried);
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
                }
            }
        };
        self.session.end_external_mpp_query_scope(outcome.is_err());
        if outcome.is_ok()
            && self.txn.explicit.is_some()
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
            tidb_store_driver::cluster_table_storage::take_last_pessimistic_lock_observation()
        {
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
                .explicit
                .as_ref()
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
                SqlQueryError::from(tidb_error::mysql::MysqlError::from_terror(
                    &tidb_dbterror::exeerrors::ERR_LAZY_UNIQUENESS_CHECK_FAILURE
                        .generate_with_stack_by_args(&[error.error_string().into()]),
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
        if let Some(transaction) = self.txn.explicit.as_ref() {
            transaction
                .set_resource_group_name(resource_group)
                .map_err(SqlQueryError::unknown)?;
            transaction
                .set_statement_assertion_level(
                    tidb_domain_core::session_transaction::transaction_options(self.session.vars()).assertion_level,
                )
                .map_err(SqlQueryError::unknown)?;
        }
        let write_transaction = (self.txn.explicit.is_none()
            && shape == StatementReadShape::AutocommitWrite)
            .then(|| Arc::new(Mutex::new(None)));
        // Go `handlePessimisticDML`'s statement loop: run, lock the staged
        // keys, and on a lock conflict roll the STATEMENT back and re-execute
        // it reading at the advanced `for_update_ts`. `None` is the first
        // attempt, reading at the transaction's own snapshot.
        let mut retry_read_ts: Option<u64> = None;
        let mut retries = tidb_store_driver_error::pessimistic_lock_error::PessimisticStatementRetry::default();
        // An autocommit pessimistic statement's lock conflict (see
        // [`transactions::LockRetrySlot`]).
        let lock_retry: transactions::LockRetrySlot = Arc::default();
        // Whether the statement reached `finishStmt`'s commit: a failure
        // there is the commit's own, not one `autoCommitAfterStmt` rolls
        // back.
        let mut committing = false;
        let result = loop {
            // Selected rows belong to this execution attempt, not an earlier
            // plan or a partially executed failed read.
            self.session.take_selected_lock_keys();
            if let Some(transaction) = self.txn.explicit.as_ref() {
                let _ = transaction.take_statement_lock_failure();
            }
            *lock_retry
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner) = None;
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
                self.txn.explicit.is_none() && shape == StatementReadShape::AutocommitWrite;
            let autocommit_pessimistic =
                autocommit_write && self.autocommit_write_is_pessimistic(retrying);
            if autocommit_write {
                self.begin_autocommit_statement_locks(autocommit_pessimistic);
            }
            let snapshot = if autocommit_write {
                let vars = self.session.vars();
                // Go `OnPessimisticStmtStart` (`isolation/base.go:711`):
                // fair locking follows the session variable; a client
                // connection is neither internal nor restricted SQL.
                let fair_locking = vars.pessimistic_transaction_fair_locking();
                transactions::prefetched_write_snapshot(
                    Arc::clone(self.txn.transactions()),
                    read_ts.clone(),
                    Arc::clone(
                        write_transaction
                            .as_ref()
                            .expect("autocommit write created its transaction handoff"),
                    ),
                    Arc::clone(&lock_retry),
                    self.txn.buffer().clone(),
                    Arc::<str>::from(resource_group),
                    autocommit_pessimistic,
                    fair_locking,
                    tidb_domain_core::session_transaction::transaction_options(vars)
                        .with_statement_kv_context(self.session.statement_kv_context_cell()),
                )
            } else {
                match self.open_read_snapshot(shape, retry_read_ts, read_ts, resource_group, is_dml)
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
                && self.txn.explicit.is_none()
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
                if let Some(transaction) = self.txn.explicit.as_ref() {
                    if let Err(error) = transaction.activate() {
                        let _ = self.finish_snapshot();
                        break Err(SqlQueryError::unknown(error));
                    }
                }
            }
            let outcome = run(&mut self.session);
            // Go `CommitTxn` / `RollbackTxn` -> `setLastTxnInfoBeforeTxnEnd`
            // for the transaction an autocommit statement ends: recorded
            // only when the statement activated one (`TxnCtx.StartTS != 0`),
            // before the commit whose hook overwrites it.
            if self.txn.explicit.is_none() && self.session.historical_read_ts().is_none() {
                if let Some(start_ts) = read_ts.get() {
                    self.session.record_started_txn_info(start_ts);
                }
            }
            // Go `TxnCtx.GetForUpdateTS` as the statement left it: a deferred
            // handle advances the for-update timestamp at its first read,
            // after the open published the transaction's earlier one.
            if let Some(transaction) = self.txn.explicit.as_ref() {
                self.session
                    .current_for_update_ts()
                    .publish(transaction.for_update_ts());
            }
            if autocommit_write {
                self.begin_autocommit_statement_locks(false);
            }
            let finished = self.finish_snapshot();
            match outcome {
                Ok(value) => {
                    if let Err(error) = finished {
                        break Err(error);
                    }
                    match self.lock_pessimistic_statement_keys(savepoint) {
                        Ok(PessimisticStep::Done) => {
                            if let Some(transaction) = self.txn.explicit.as_ref() {
                                if let Err(error) = transaction.finish_pessimistic_statement(true) {
                                    break Err(SqlQueryError::unknown(error));
                                }
                            }
                        }
                        Ok(PessimisticStep::Retry { for_update_ts }) => {
                            if let Err(error) = retries.retry() {
                                break Err(transactions::sql_error(error));
                            }
                            self.session.record_statement_retry();
                            // The statement's writes go back; its locks STAY
                            // (fair locking's whole point), and the replay
                            // reads at the timestamp that sees the version
                            // that beat it.
                            self.txn.buffer().restore(savepoint.clone());
                            retry_read_ts = Some(for_update_ts);
                            tidb_breakpoint::inject(
                                &*self.session,
                                "lockErrorAndThenOnStmtRetryCalled",
                            );
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
                            self.txn.explicit.is_none() && !self.session.in_transaction();
                        let _commit = ends_transaction.then(|| {
                            (
                                tidb_tracing::begin_thread_region_scope("session.CommitTxn"),
                                tidb_tracing::begin_thread_region_scope(
                                    "session.doCommitWithRetry",
                                ),
                            )
                        });
                        self.flush_if_autocommit(
                            read_ts.get(),
                            write_transaction.clone(),
                            resource_group,
                        )
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
                    let failure = self
                        .txn
                        .explicit
                        .as_ref()
                        .and_then(|transaction| transaction.take_statement_lock_failure());
                    // An autocommit statement's lock found a newer committed
                    // version of a row it read.
                    let autocommit_retry = if self.txn.explicit.is_none() {
                        lock_retry
                            .lock()
                            .unwrap_or_else(std::sync::PoisonError::into_inner)
                            .take()
                    } else {
                        None
                    };
                    if failure.is_some() || autocommit_retry.is_some() {
                        // The storage error the lock raised is the statement
                        // owner's to consume, as `handlePessimisticLockError`
                        // does: what the client sees is the lock outcome.
                        self.session
                            .retract_statement_failure(error.code, &error.message);
                    }
                    if autocommit_retry.is_some() {
                        if let Err(error) = retries.retry() {
                            break Err(transactions::sql_error(error));
                        }
                        self.session.record_statement_retry();
                        // The implicit transaction has nothing earlier to
                        // keep: the replay is a new one, reading the version
                        // that beat the statement.
                        Self::rollback_prefetched_write(write_transaction.clone());
                        self.txn.buffer().restore(savepoint.clone());
                        tidb_breakpoint::inject(
                            &*self.session,
                            "lockErrorAndThenOnStmtRetryCalled",
                        );
                        continue;
                    }
                    match failure {
                        None => break Err(error),
                        Some(StatementLockFailure::Retry { for_update_ts }) => {
                            if let Err(error) = retries.retry() {
                                break Err(transactions::sql_error(error));
                            }
                            self.session.record_statement_retry();
                            // The statement's writes go back; its locks stay,
                            // and the replay reads at the timestamp that sees
                            // the version that beat it.
                            self.txn.buffer().restore(savepoint.clone());
                            retry_read_ts = Some(for_update_ts);
                            tidb_breakpoint::inject(
                                &*self.session,
                                "lockErrorAndThenOnStmtRetryCalled",
                            );
                            continue;
                        }
                        Some(
                            StatementLockFailure::Statement(error)
                            | StatementLockFailure::Transaction(error),
                        ) => {
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
                (!committing && self.txn.explicit.is_none() && !self.session.in_transaction())
                    .then(|| tidb_tracing::start_thread_region_ex("session.RollbackTxn"));
            Self::rollback_prefetched_write(write_transaction);
            // Cancel the client's active fair scope. Ordinary locks remain
            // transaction-owned, following Go StmtRollback.
            if let Some(transaction) = self.txn.explicit.as_ref() {
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
        let vars = self.session.vars();
        tidb_planner::txn_mode::txn_mode_for_statement(
            tidb_planner::txn_mode::StatementTxnModeInputs {
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

    /// Installs (or, with `false`, clears) an autocommit statement's
    /// pessimistic mode: Go `TxnCtx.IsPessimistic`, and the session lock wait
    /// its locking point reads take (`getLockWaitTime`).
    pub fn begin_autocommit_statement_locks(&mut self, pessimistic: bool) {
        self.session
            .set_autocommit_pessimistic_statement(pessimistic);
        let selected = pessimistic.then(|| {
            tidb_sessionctx::select_lock::SelectedLockKeys::new(
                tidb_txnkv::transaction::LockWaitTime::Timeout(self.session.lock_wait_timeout()),
                self.session.vars().shared_lock_promotion_enabled(),
            )
            .with_read_committed(self.session.is_read_committed())
            .with_lock_state_observer(self.session.transaction_lock_state_observer())
            .with_lock_in_execution(true)
        });
        self.session.set_selected_lock_keys(selected);
    }

    /// Go `handlePessimisticDML`'s lock step for the statement that just ran:
    /// what the transaction says about the keys it staged.
    pub fn lock_pessimistic_statement_keys(
        &mut self,
        savepoint: &BufferCheckpoint,
    ) -> Result<PessimisticStep, SqlQueryError> {
        let Some(transaction) = self.txn.explicit.as_ref() else {
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
        if !taken.unchanged.is_empty() {
            keys.extend(taken.unchanged);
            keys.sort();
            keys.dedup();
        }
        if !keys.is_empty() {
            requests.push((
                tidb_txnkv::transaction::LockWaitTime::Timeout(self.session.lock_wait_timeout()),
                keys,
            ));
        }
        for (wait, keys) in requests {
            match self.lock_pessimistic_keys(savepoint, keys, wait)? {
                PessimisticStep::Done => {}
                retry => return Ok(retry),
            }
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
            .explicit
            .as_ref()
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
    ) -> Result<PessimisticStep, SqlQueryError> {
        let transaction = self.txn.explicit.as_ref().expect("pessimistic transaction");
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
        let duplicate_hints = presume_not_exists
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
        let lock_result = transaction.lock_staged_keys_with_assertions(
            keys,
            presume_not_exists,
            duplicate_hints,
            wait,
        );
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
            LockKeysOutcome::RetryStatement { for_update_ts, .. } => {
                Ok(PessimisticStep::Retry { for_update_ts })
            }
            LockKeysOutcome::StatementError(error) => {
                // Statement-scoped, Go's 1205/1213 family: this statement's
                // writes go, the transaction stays open.
                self.txn.buffer().restore(savepoint.clone());
                Err(transactions::sql_error(error))
            }
            LockKeysOutcome::TransactionError(error) => {
                // The transaction is no longer usable; later statements and
                // `ROLLBACK` report that state on their own.
                self.txn.buffer().restore(savepoint.clone());
                Err(transactions::sql_error(error))
            }
        }
    }

    /// Whether the statement that just failed is one Go would have retried
    /// internally instead of reporting.
    ///
    /// This is `isOptimisticTxnRetryable`
    /// (`pkg/sessiontxn/isolation/optimistic.go:66-104`) reduced to the only
    /// arm this node can reach. Go's `!sessVars.InTxn()` early return is the
    /// whole reason `tidb_disable_txn_auto_retry` -- which defaults to `true`
    /// (`pkg/sessionctx/vardef/tidb_vars.go:1528`) -- does not stop this:
    /// that variable is consulted eleven lines LATER, and only a
    /// multi-statement transaction ever gets there. A single autocommit
    /// statement has no earlier statements whose reads a replay could
    /// falsify, so it is retried unconditionally. Inside `BEGIN` we refuse,
    /// which is Go with the default variable.
    ///
    /// The error test is Go's `kv.IsTxnRetryableError` (`pkg/kv/error.go:85`)
    /// narrowed to the code that reaches this seam: only a write conflict.
    ///
    /// # The one thing this replay is NOT, and what it costs
    ///
    /// Go's replay runs in PESSIMISTIC mode whatever `@@tidb_txn_mode` says:
    /// `retry` calls `PrepareTxnCtx(ctx, nil)` (`pkg/session/session.go:1194`)
    /// while `RetryInfo.Retrying` is set, and `decideTxnMode`
    /// (`session.go:4921-4923`) returns `ast.Pessimistic` unconditionally for
    /// that. This node's replay stays optimistic, because it has no
    /// pessimistic statement path at all -- the seam is
    /// `RealOptimisticTransactionOpener` from end to end.
    ///
    /// What that does NOT cost is a lost update. The replay re-runs the whole
    /// statement against its own fresh timestamp and publishes at that same
    /// one, so a value derived from a stale read cannot reach the store: see
    /// `an_autocommit_update_that_loses_the_race_is_retried_at_a_new_read`,
    /// and the exhausted-budget case still reports 9007 rather than
    /// succeeding.
    ///
    /// What it does cost is the failure mode under a lock that is HELD rather
    /// than a commit that already landed. Measured on TiDB over `mockstore`
    /// (2026-08-03), with a second session holding a pessimistic lock on the
    /// row and `innodb_lock_wait_timeout = 1`:
    ///
    /// ```text
    /// s1 @@tidb_txn_mode -> pessimistic
    /// s1 @@tidb_disable_txn_auto_retry -> 1
    /// s2 holds a pessimistic lock on id=1
    /// s1 autocommit update -> [tikv:1205]Lock wait timeout exceeded; try
    ///                         restarting transaction
    /// ```
    ///
    /// Go WAITS on the lock -- 1205 is a pessimistic-lock error, so the replay
    /// really did take locks -- and against a lock that clears in time it
    /// succeeds. This node cannot wait: it spends its budget of re-reads and
    /// reports 9007. Closing that needs the pessimistic path (a
    /// `tidb_txnkv::transaction::RealTransaction` in pessimistic mode exists;
    /// nothing at this tier drives it), NOT a mode flag: locks have to
    /// be taken while the statement reads, and taking them after it has
    /// computed from a stale read would introduce exactly the lost update this
    /// seam exists to prevent.
    pub fn may_retry_autocommit_statement(&self, error: &SqlQueryError, retried: u32) -> bool {
        // Go `doCommitWithRetry` (`session.go:879`): never under
        // `@@tidb_batch_insert`, whose earlier batches are already committed.
        error.code == ERR_WRITE_CONFLICT
            && !self.session.vars().batch_insert()
            && retried < self.autocommit_retry_budget()
            && self.txn.explicit.is_none()
            && !self.session.in_transaction()
    }

    /// Go's autocommit retry budget: `@@tidb_retry_limit` (default 10),
    /// scaled DOWN by transaction size the way `doCommitWithRetry` does
    /// (`pkg/session/session.go:881-882`). A limit of 0 disables the retry
    /// entirely (`isOptimisticTxnRetryable`, `optimistic.go:82-84`); this
    /// autocommit seam has no explicit transaction, so `CouldRetry` and the
    /// `for_update`/pipelined gates that only apply inside one are always in
    /// the retryable state here.
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

    pub fn open_read_snapshot(
        &self,
        shape: StatementReadShape,
        retry_read_ts: Option<u64>,
        read_ts: &transactions::StatementReadTs,
        resource_group: &str,
        is_dml: bool,
    ) -> Result<Box<dyn ClusterSnapshot>, SqlQueryError> {
        if let Some(ts) = self.session.historical_read_ts() {
            return self
                .txn
                .transactions()
                .open_snapshot_at(ts, resource_group)
                .map_err(SqlQueryError::unknown);
        }
        if let Some(transaction) = &self.txn.explicit {
            let locking = shape == StatementReadShape::LockingRead;
            let snapshot = match retry_read_ts {
                Some(ts) => transaction.snapshot_at_for(ts, locking),
                // Go's pessimistic READ-COMMITTED provider reads every
                // statement at its own fresh timestamp (`getStmtTS`), so a
                // statement sees what peers committed after BEGIN; repeatable
                // read keeps the start timestamp.
                None if transaction.is_pessimistic() && self.session.is_read_committed() => {
                    transaction.fresh_statement_snapshot(locking)
                }
                // Go's pessimistic DML and locking reads read at
                // `GetStmtForUpdateTS`, a fresh timestamp, so they see rows
                // peers committed after BEGIN -- unless the plan starts with
                // a locking point read, whose lock answers the latest row
                // (`AdviseOptimizeWithPlan` -> `optimizeForNotFetchingLatestTS`).
                // The deferred handle lets the executor's first act decide.
                None if (is_dml || shape == StatementReadShape::LockingRead)
                    && transaction.is_pessimistic() =>
                {
                    transaction.deferred_statement_snapshot(locking)
                }
                None => transaction.snapshot_for(locking),
            }
            .map_err(SqlQueryError::unknown);
            // Go `TxnCtx.SetForUpdateTS`: the statement's for-update
            // timestamp, which `tidb_last_query_info` reports.
            self.session
                .current_for_update_ts()
                .publish(transaction.for_update_ts());
            return snapshot;
        }
        if shape == StatementReadShape::AutocommitPointGet {
            return self
                .txn
                .transactions()
                .open_max_ts_snapshot(resource_group)
                .map_err(SqlQueryError::unknown);
        }
        Ok(transactions::deferred_snapshot(
            Arc::clone(self.txn.transactions()),
            read_ts.clone(),
            Arc::<str>::from(resource_group),
        ))
    }

    pub fn begin_read_statement(
        &mut self,
        shape: StatementReadShape,
        resource_group: &str,
    ) -> Result<ReadStatement, SqlQueryError> {
        if let Some(transaction) = &self.txn.explicit {
            transaction
                .set_resource_group_name(resource_group)
                .map_err(SqlQueryError::unknown)?;
        }
        // An optimistic transaction's locking read runs as a read; its lock
        // operator publishes the keys, which `end_read_statement` flags.
        if shape == StatementReadShape::LockingRead
            && self
                .txn
                .explicit
                .as_ref()
                .is_some_and(|transaction| !transaction.is_pessimistic())
        {
            self.session.set_selected_lock_keys(Some(
                tidb_sessionctx::select_lock::SelectedLockKeys::new(
                    tidb_txnkv::transaction::LockWaitTime::Timeout(
                        self.session.lock_wait_timeout(),
                    ),
                    self.session.vars().shared_lock_promotion_enabled(),
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
            .open_read_snapshot(shape, None, &statement.read_ts, resource_group, false)
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
        if success && self.txn.explicit.is_some() {
            self.lock_optimistic_statement_keys();
        }
        let result = if success {
            finished
                .and_then(|()| self.commit_if_session_left_transaction())
                .and_then(|()| {
                    self.flush_if_autocommit(
                        statement.read_ts.get(),
                        None,
                        &statement.resource_group,
                    )
                })
        } else {
            self.txn.buffer().restore(statement.savepoint);
            finished
        };
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
        if self.txn.explicit.is_none() || self.session.in_transaction() {
            return Ok(());
        }
        self.commit_explicit()
    }

    /// Publishes the buffer when the session is not inside `BEGIN`; see
    /// [`TxnManager::flush_if_autocommit`].
    pub fn flush_if_autocommit(
        &mut self,
        read_ts: Option<u64>,
        write_transaction: Option<transactions::WriteTransactionSlot>,
        resource_group: &str,
    ) -> Result<(), SqlQueryError> {
        self.txn.flush_if_autocommit(
            self.session,
            self.hooks.as_ref(),
            read_ts,
            write_transaction,
            resource_group,
        )
    }

    pub fn rollback_prefetched_write(
        write_transaction: Option<transactions::WriteTransactionSlot>,
    ) {
        let Some(write_transaction) = write_transaction else {
            return;
        };
        let transaction = write_transaction
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .take();
        if let Some(transaction) = transaction {
            let _ = transaction.rollback();
        }
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

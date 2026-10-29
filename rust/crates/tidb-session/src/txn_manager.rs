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

//! The session's transaction manager over the store seam: Go
//! `sessiontxn.TxnManager` with the `LazyTxn` it drives.
//!
//! It owns the open transaction of the session -- the store transaction
//! (`BEGIN`'s, or the pending one an `autocommit = 0` statement enters), the
//! schema pin that stands for Go's metadata lock, and the staged writes --
//! and the moves that start and end it: `EnterNewTxn`
//! (`pkg/session/txnmanager.go:156`), `CommitTxn` / `RollbackTxn`
//! (`pkg/session/session.go:982`, `:1022`) and the membuffer checkpoints the
//! savepoint statements take (`pkg/executor/simple.go:662`). The savepoint
//! stack itself is the session's `TxnCtx.Savepoints`. The server that drives a statement hands in
//! the pieces only it owns -- the schema lease checker, the binding cache and
//! the catalog a write-conflict message is rewritten against -- as
//! [`CommitHooks`], the way Go's commit reads them off the session.

pub use tidb_domain_core::session_transaction::{options_from_values, transaction_options};
use std::sync::Arc;

use tidb_store_driver::session_commit_protocol::SessionTransactionOptions;
use tidb_cluster::sql_query_error::SqlQueryError;
use tidb_executor::cluster_storage::{BufferCheckpoint, MutationBuffer};
use tidb_txnkv::transaction::SchemaLeaseChecker;

use crate::cluster_transactions::{
    ClusterTransactions, CommittedTxnInfo, OpenClusterTransaction, WriteTransactionSlot,
};
use crate::lazy_transaction::{LazyClusterTransaction, TransactionOpener};
use crate::Session;



/// Go's metadata lock at transaction scope: holding the guard keeps the
/// schema-sync acknowledger from reporting anything newer than `version` to a
/// DDL owner (`CheckOldRunningTxn`).
pub trait SchemaPins: Send + Sync {
    /// Registers `connection_id` as running at `version` until the guard drops.
    fn hold(&self, connection_id: u64, version: i64) -> Box<dyn Send>;
}

/// What a commit reads off the server that owns the session: Go's
/// `SetOptionsBeforeCommit` checker (`isolation/base.go:560`), the binding
/// cache a committed binding row refreshes, and the infoschema a
/// write-conflict message is rewritten against (`tryReplaceWriteConflictError`,
/// `session.go:862`).
pub trait CommitHooks {
    /// The schema lease checker this transaction commits under, if the server
    /// has a validator.
    fn schema_lease_checker(&self) -> Option<Arc<dyn SchemaLeaseChecker>>;
    /// Whether the staged writes change binding records.
    fn binding_records_changed(&self, buffer: &MutationBuffer) -> bool;
    /// Reloads the binding cache after a commit that changed it.
    fn refresh_committed_bindings(&self, changed: bool);
    /// Names the tables of a write-conflict error from the transaction's
    /// infoschema.
    fn rewrite_write_conflict(&self, error: SqlQueryError) -> SqlQueryError;
}

/// Go `session.CommitTxn` and, over a valid transaction, the
/// `session.doCommitWithRetry` it calls; the commit runs under both until
/// the value drops.
pub(crate) struct CommitTxnRegions {
    // Dropped in declaration order: the inner region ends first.
    _do_commit: Option<tidb_util::tracing::ThreadRegion>,
    _commit_txn: tidb_util::tracing::ThreadRegion,
}

impl CommitTxnRegions {
    pub(crate) fn begin(valid: bool) -> Self {
        let commit_txn = tidb_util::tracing::start_thread_region_ex("session.CommitTxn");
        let do_commit =
            valid.then(|| tidb_util::tracing::start_thread_region_ex("session.doCommitWithRetry"));
        Self {
            _do_commit: do_commit,
            _commit_txn: commit_txn,
        }
    }
}

/// See the module documentation.
pub struct TxnManager {
    transactions: Arc<dyn ClusterTransactions>,
    schema_pins: Arc<dyn SchemaPins>,
    connection_id: u64,
    /// The session's staged writes: a handle shared with the tables it reads
    /// and writes through.
    buffer: MutationBuffer,
    /// The transaction a `BEGIN` holds open, or the pending one an
    /// `autocommit = 0` statement entered. `None` is autocommit, where a
    /// statement prepares a timestamp of its own after planning and waits for
    /// it at its first read.
    pub explicit: Option<Box<dyn OpenClusterTransaction>>,
    /// Go `LazyTxn` captures this policy when the transaction becomes valid.
    lazy_uniqueness_check_enabled: bool,
    /// Held from `BEGIN` to `COMMIT`/`ROLLBACK`.
    pin: Option<Box<dyn Send>>,
    /// Go `doCommitWithRetry`'s `txnSize`, read from the buffer just before
    /// the autocommit commit that may fail: the commit resets the buffer, and
    /// the retry budget is decided after it.
    autocommit_txn_bytes: usize,
}

impl TxnManager {
    /// A manager for one connection's session over the store seam.
    #[must_use]
    pub fn new(
        transactions: Arc<dyn ClusterTransactions>,
        schema_pins: Arc<dyn SchemaPins>,
        connection_id: u64,
        buffer: MutationBuffer,
    ) -> Self {
        Self {
            transactions,
            schema_pins,
            connection_id,
            buffer,
            explicit: None,
            lazy_uniqueness_check_enabled: false,
            pin: None,
            autocommit_txn_bytes: 0,
        }
    }

    /// Go `LazyTxn`'s captured uniqueness-check policy of the open transaction.
    #[must_use]
    pub fn lazy_uniqueness_check_enabled(&self) -> bool {
        self.lazy_uniqueness_check_enabled
    }

    /// The session's staged writes.
    #[must_use]
    pub fn buffer(&self) -> &MutationBuffer {
        &self.buffer
    }

    /// The store the session's transactions open on.
    #[must_use]
    pub fn transactions(&self) -> &Arc<dyn ClusterTransactions> {
        &self.transactions
    }

    /// Go's MDL considers a RUNNING statement a user of its schema version;
    /// this hold is what keeps a Go DDL owner from publishing a drop under a
    /// statement mid-flight. Dropped with the scope.
    #[must_use]
    pub fn hold_schema_pin(&self, schema_version: i64) -> Box<dyn Send> {
        self.schema_pins.hold(self.connection_id, schema_version)
    }

    /// Go `doCommitWithRetry`'s `txnSize` of the last autocommit commit.
    #[must_use]
    pub fn autocommit_txn_bytes(&self) -> usize {
        self.autocommit_txn_bytes
    }

    /// Records the autocommit commit's `txnSize` before the commit resets the
    /// buffer.
    pub fn note_autocommit_txn_bytes(&mut self, bytes: usize) {
        self.autocommit_txn_bytes = bytes;
    }

    /// Go `resolveTxnMode` of `newProviderWithRequest`: `BEGIN <mode>` wins, a
    /// bare `BEGIN` falls back to `@@tidb_txn_mode` -- whose default is
    /// PESSIMISTIC (`vardef.DefTiDBTxnMode`). The session resolved the keyword
    /// half at `BEGIN`; the variable half covers `SET autocommit = 0`, which
    /// carries no keyword.
    fn is_pessimistic(session: &Session) -> bool {
        match session.txn_mode() {
            Some(mode) => mode.is_pessimistic(),
            None => tidb_planner::txn_mode::txn_mode_variable(
                &session
                    .vars()
                    .get_system("tidb_txn_mode")
                    .unwrap_or_else(|_| "pessimistic".to_owned()),
            )
            .is_pessimistic(),
        }
    }

    /// Go `SetOptionsOnTxnActive`'s inputs: the validated session values and
    /// the statement ctx the transaction's requests report through.
    #[must_use]
    pub fn statement_transaction_options(session: &Session) -> SessionTransactionOptions {
        transaction_options(session.vars())
            .with_statement_kv_context(session.statement_kv_context_cell())
    }

    /// Go `PrepareTxnCtx` for `autocommit = 0`: enters the transaction when the
    /// statement about to run needs one and none is entered.
    pub fn enter_if_autocommit_off(
        &mut self,
        session: &Session,
        resource_group: &str,
        schema_version: i64,
    ) {
        if self.explicit.is_some() || session.is_autocommit() {
            return;
        }
        self.enter_pending(session, resource_group, schema_version);
    }

    /// Go `PrepareTxnCtx` -> `EnterNewTxn(EnterNewTxnBeforeStmt)`
    /// (`pkg/session/session.go:4908`): the `autocommit = 0` transaction is
    /// entered but left pending. It takes its timestamp, publishes
    /// `TxnCtx.StartTS` and registers in `information_schema.tidb_trx` at the
    /// first statement that activates it (`lazy_transaction`).
    pub fn enter_pending(&mut self, session: &Session, resource_group: &str, schema_version: i64) {
        if session.historical_read_ts().is_some() {
            // A stale read publishes its own timestamp and opens no store
            // transaction; `open` handles that arm.
            let _ = self.open(session, resource_group, schema_version);
            return;
        }
        let pessimistic = Self::is_pessimistic(session);
        // Go `OnPessimisticStmtStart` (`isolation/base.go:711`): fair locking
        // follows the session's `@@tidb_pessimistic_txn_fair_locking`.
        let fair_locking = session.vars().pessimistic_transaction_fair_locking();
        let transactions = Arc::clone(&self.transactions);
        let current_tso = session.current_tso();
        let observer = session.transaction_observer();
        let resource_group = resource_group.to_owned();
        let opener: TransactionOpener = Box::new(move |options| {
            let transaction =
                transactions.begin(pessimistic, fair_locking, &resource_group, options)?;
            current_tso.publish(transaction.start_ts());
            if let Some(observer) = observer {
                observer.activated(transaction.start_ts());
            }
            Ok(transaction)
        });
        self.explicit = Some(Box::new(LazyClusterTransaction::new(
            pessimistic,
            Self::statement_transaction_options(session),
            self.buffer.clone(),
            opener,
        )));
        self.pin_schema(session, schema_version);
    }

    /// The transaction reads this catalog version until it ends; the pin is
    /// what a Go owner's `WaitVersionSynced` waits out (Go
    /// `CheckOldRunningTxn`, at this port's whole-transaction scope).
    fn pin_schema(&mut self, session: &Session, schema_version: i64) {
        self.lazy_uniqueness_check_enabled = !session.pessimistic_constraint_check_in_place();
        self.pin = Some(self.schema_pins.hold(self.connection_id, schema_version));
    }

    /// Go `EnterNewTxnWithBeginStmt`: `BEGIN` activates the transaction at
    /// once (`OnInitialize`'s `activeNow`, `isolation/base.go:88`), so its
    /// timestamp is taken here.
    pub fn open(
        &mut self,
        session: &Session,
        resource_group: &str,
        schema_version: i64,
    ) -> Result<(), SqlQueryError> {
        if let Some(ts) = session.historical_read_ts() {
            session.current_tso().publish(ts);
            if let Some(observer) = session.transaction_observer() {
                observer.activated(ts);
            }
            return Ok(());
        }
        let pessimistic = Self::is_pessimistic(session);
        // Go `OnPessimisticStmtStart` (`isolation/base.go:711`): fair locking
        // follows the session's `@@tidb_pessimistic_txn_fair_locking`.
        let fair_locking = session.vars().pessimistic_transaction_fair_locking();
        let transaction = self
            .transactions
            .begin(
                pessimistic,
                fair_locking,
                resource_group,
                Self::statement_transaction_options(session),
            )
            .map_err(SqlQueryError::unknown)?;
        transaction.bind_mutation_buffer(&self.buffer);
        session.current_tso().publish(transaction.start_ts());
        if let Some(observer) = session.transaction_observer() {
            observer.activated(transaction.start_ts());
        }
        self.explicit = Some(transaction);
        self.pin_schema(session, schema_version);
        Ok(())
    }

    /// Go `executeSavepoint`'s `Txn(true)` and `GetMemBuffer().Checkpoint()`:
    /// with autocommit OFF the SAVEPOINT is what ACTIVATES the pending
    /// transaction. In autocommit no transaction is open and there is nothing
    /// to mark (`None`), which is what leaves a later `ROLLBACK TO` to report
    /// 1305 (captured).
    pub fn savepoint_checkpoint(
        &mut self,
        session: &Session,
        resource_group: &str,
        schema_version: i64,
    ) -> Result<Option<BufferCheckpoint>, SqlQueryError> {
        self.enter_if_autocommit_off(session, resource_group, schema_version);
        let Some(transaction) = self.explicit.as_ref() else {
            return Ok(None);
        };
        transaction.activate().map_err(SqlQueryError::unknown)?;
        Ok(Some(self.buffer.checkpoint()))
    }

    /// Go `RollbackMemDBToCheckpoint`: the staged writes go back to the
    /// checkpoint a savepoint took.
    pub fn rollback_to_checkpoint(&mut self, checkpoint: &BufferCheckpoint) {
        self.buffer.restore(checkpoint.clone());
    }

    /// Go `doCommitWithRetry`'s `tryReplaceWriteConflictError`, then Go's
    /// `kv.CommitHook`: a transaction that committed records its own timestamps
    /// as `@@tidb_last_txn_info`. One that published nothing leaves what the
    /// statement recorded.
    fn record_committed_txn_info(session: &mut Session, committed: Option<CommittedTxnInfo>) {
        if let Some(committed) = committed {
            session.record_committed_txn_info(
                committed.start_ts,
                committed.commit_ts,
                committed.commit_mode,
            );
        }
    }

    /// The staged writes' size and key count, read before the commit resets
    /// the buffer.
    #[must_use]
    pub fn buffer_write_details(&self) -> (isize, isize) {
        let (write_size, write_keys) = self.buffer.write_details();
        (write_size as isize, write_keys as isize)
    }

    /// Observes the committed transaction's write size.
    pub fn record_write_details(session: &mut Session, (write_size, write_keys): (isize, isize)) {
        if write_size > 0 {
            tidb_txnkv::client_go_metrics::observe_txn_write_size_bytes(write_size, false);
            session
                .txn_write_throughput_sli()
                .add_txn_write_size(write_size, write_keys);
        }
    }

    /// Publishes one autocommit statement's staged writes as its own
    /// transaction, at the timestamp the statement read at. A failed
    /// publication discards them, which is what a failed COMMIT does.
    ///
    /// A publication that lost the race against a commit made after the read is
    /// a 9007 the client is told about, where publishing at a fresh timestamp
    /// made it a silent overwrite.
    pub fn commit_autocommit_buffer(
        &mut self,
        session: &mut Session,
        hooks: &dyn CommitHooks,
        read_ts: Option<u64>,
        resource_group: &str,
    ) -> Result<(), SqlQueryError> {
        let bindings_changed = hooks.binding_records_changed(&self.buffer);
        match self.transactions.commit_with_schema_lease(
            &self.buffer,
            read_ts,
            resource_group,
            hooks.schema_lease_checker(),
            Self::statement_transaction_options(session),
            session.transaction_observer(),
        ) {
            Ok(committed) => {
                Self::record_committed_txn_info(session, committed);
                hooks.refresh_committed_bindings(bindings_changed);
                Ok(())
            }
            Err(error) => {
                self.buffer.reset();
                Err(hooks.rewrite_write_conflict(error))
            }
        }
    }

    /// Publishes the buffer when the session is not inside `BEGIN`: the
    /// autocommit statement's `CommitTxn` (Go `autoCommitAfterStmt`).
    ///
    /// An empty buffer -- every read statement -- publishes nothing and spends
    /// no timestamp, as a Go COMMIT of a transaction that wrote nothing does.
    pub fn flush_if_autocommit(
        &mut self,
        session: &mut Session,
        hooks: &dyn CommitHooks,
        read_ts: Option<u64>,
        write_transaction: Option<WriteTransactionSlot>,
        resource_group: &str,
    ) -> Result<(), SqlQueryError> {
        if self.explicit.is_some() || session.in_transaction() {
            return Ok(());
        }
        // Go's autocommit `CommitTxn`, which a failed commit has run too.
        session.set_txn_isolation_level_one_shot_state_for_next_txn();
        let write_details = self.buffer_write_details();
        self.autocommit_txn_bytes = self.buffer.staged_bytes();
        if let Some(write_transaction) = write_transaction {
            let transaction = write_transaction
                .lock()
                .unwrap_or_else(|poison| poison.into_inner())
                .take();
            if let Some(mut transaction) = transaction {
                if let Some(checker) = hooks.schema_lease_checker() {
                    transaction.set_schema_lease_checker(checker);
                }
                let bindings_changed = hooks.binding_records_changed(&self.buffer);
                return match transaction.commit(&self.buffer) {
                    Ok(committed) => {
                        Self::record_committed_txn_info(session, committed);
                        hooks.refresh_committed_bindings(bindings_changed);
                        Self::record_write_details(session, write_details);
                        session.publish_table_delta();
                        Ok(())
                    }
                    Err(error) => {
                        self.buffer.reset();
                        session.clear_table_delta();
                        Err(hooks.rewrite_write_conflict(error))
                    }
                };
            }
        }
        let outcome = self.commit_autocommit_buffer(session, hooks, read_ts, resource_group);
        if outcome.is_ok() {
            Self::record_write_details(session, write_details);
            session.publish_table_delta();
        } else {
            session.clear_table_delta();
        }
        outcome
    }

    /// Ends the explicit transaction by publishing its buffer at its own start
    /// timestamp (Go `CommitTxn`).
    ///
    /// A `COMMIT` with no transaction open is the autocommit path: it can only
    /// find writes the previous statement already published, so the buffer is
    /// empty and nothing is spent.
    pub fn commit(
        &mut self,
        session: &mut Session,
        hooks: &dyn CommitHooks,
    ) -> Result<(), SqlQueryError> {
        session.current_tso().clear();
        self.pin = None;
        let Some(mut transaction) = self.explicit.take() else {
            if let Some(observer) = session.transaction_observer() {
                observer.finished();
            }
            // No transaction and no statement read: the buffer can only hold
            // what a previous statement already published, so there is nothing
            // to publish and no timestamp to publish it at.
            let resource_group = session.current_resource_group().to_owned();
            return self.commit_autocommit_buffer(session, hooks, None, &resource_group);
        };
        if let Some(checker) = hooks.schema_lease_checker() {
            transaction.set_schema_lease_checker(checker);
        }
        let bindings_changed = hooks.binding_records_changed(&self.buffer);
        let write_details = self.buffer_write_details();
        // Go `session.Commit`'s `recordOnTransactionExecution`
        // (`session.go:897`): the transaction's wall-clock duration since
        // `BEGIN` and its statement count, labeled by pessimistic/optimistic
        // mode and the commit's outcome.
        let txn_mode = if transaction.is_pessimistic() {
            "pessimistic"
        } else {
            "optimistic"
        };
        let txn_opened_at = transaction.opened_at();
        let txn_statement_count = transaction.statement_count() as f64;
        // Go `CommitTxn` -> `setLastTxnInfoBeforeTxnEnd`: the start-only
        // record is written before the commit, and the commit hook
        // overwrites it only when a commit really ran.
        let start_ts = transaction.start_ts_if_active();
        if let Some(start_ts) = start_ts {
            session.record_started_txn_info(start_ts);
        }
        let commit_result = transaction.commit(&self.buffer);
        if let Some(observer) = session.transaction_observer() {
            observer.finished();
        }
        let (duration_label, num_label) = if commit_result.is_ok() {
            ("commit", "ok")
        } else {
            ("abort", "error")
        };
        // A pending transaction no statement activated has no commit to
        // observe (`doCommitWithRetry` returns for an invalid transaction).
        if start_ts.is_some() {
            tidb_metrics::session::TRANSACTION_DURATION
                .with_label_values(&[txn_mode, duration_label, "general"])
                .observe(txn_opened_at.elapsed().as_secs_f64());
            tidb_metrics::session::STATEMENT_PER_TRANSACTION
                .with_label_values(&[txn_mode, num_label, "general"])
                .observe(txn_statement_count);
        }
        match commit_result {
            Ok(committed) => {
                Self::record_committed_txn_info(session, committed);
                hooks.refresh_committed_bindings(bindings_changed);
                Self::record_write_details(session, write_details);
                session.publish_table_delta();
                Ok(())
            }
            Err(error) => {
                self.buffer.reset();
                session.clear_table_delta();
                Err(hooks.rewrite_write_conflict(error))
            }
        }
    }

    /// Drops the explicit transaction without publishing anything, along with
    /// every write it staged (Go `RollbackTxn`).
    pub fn rollback(&mut self, session: &mut Session) -> Result<(), SqlQueryError> {
        session.clear_table_delta();
        session.current_tso().clear();
        self.pin = None;
        let result = match self.explicit.take() {
            Some(transaction) => {
                // Go `pkg/session/metrics`: a rolled-back transaction counts
                // as an abort, and its statement count is observed under the
                // error result label.
                let txn_mode = if transaction.is_pessimistic() {
                    "pessimistic"
                } else {
                    "optimistic"
                };
                let txn_opened_at = transaction.opened_at();
                let txn_statement_count = transaction.statement_count() as f64;
                // Go `RollbackTxn` -> `setLastTxnInfoBeforeTxnEnd`.
                let start_ts = transaction.start_ts_if_active();
                if let Some(start_ts) = start_ts {
                    session.record_started_txn_info(start_ts);
                }
                let result = transaction.rollback();
                if let Some(observer) = session.transaction_observer() {
                    observer.finished();
                }
                // A pending transaction no statement activated was never a
                // transaction to abort.
                if start_ts.is_some() {
                    tidb_metrics::session::TRANSACTION_DURATION
                        .with_label_values(&[txn_mode, "abort", "general"])
                        .observe(txn_opened_at.elapsed().as_secs_f64());
                    tidb_metrics::session::STATEMENT_PER_TRANSACTION
                        .with_label_values(&[txn_mode, "error", "general"])
                        .observe(txn_statement_count);
                }
                result.map_err(SqlQueryError::unknown)
            }
            None => {
                if let Some(observer) = session.transaction_observer() {
                    observer.finished();
                }
                Ok(())
            }
        };
        // Rollback needs the native buffer's lock flags to release every key.
        self.buffer.reset();
        result
    }
}

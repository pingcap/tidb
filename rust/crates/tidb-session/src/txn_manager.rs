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

use tidb_sessiontxn::isolation::{optimistic::is_optimistic_txn_retryable, ProviderSessionVars};
use tidb_sessiontxn::isolation::optimistic::OptimisticTxnContextProvider;
use tidb_sessiontxn::isolation::readcommitted::PessimisticRCTxnContextProvider;
use tidb_sessiontxn::isolation::repeatable_read::PessimisticRRTxnContextProvider;
use tidb_sessiontxn::isolation::serializable::PessimisticSerializableTxnContextProvider;
use tidb_sessiontxn::staleread::provider::StalenessTxnContextProvider;
use tidb_sessiontxn::{EnterNewTxnRequest, EnterNewTxnType, TxnContext, TxnContextProvider, TxnMode};
use tidb_domain_core::session_transaction::set_options_on_txn_active;
use std::sync::Arc;

use tidb_store_driver_txn::session_commit_protocol::SessionTransactionOptions;
use tidb_store_driver_error::sql_query_error::SqlQueryError;
use tidb_store_driver_txn::cluster_storage::{BufferCheckpoint, MutationBuffer};
use tidb_txnkv::transaction::SchemaLeaseChecker;

use tidb_domain_core::cluster_transactions::{
    ClusterTransactions, CommittedTxnInfo, MaxTsReadTransaction, OpenClusterTransaction,
};
use crate::lazy_transaction::{ConstantFuture, LazyClusterTransaction, OracleFuture, TransactionOpener};
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
    _do_commit: Option<tidb_tracing::ThreadRegion>,
    _commit_txn: tidb_tracing::ThreadRegion,
}

impl CommitTxnRegions {
    pub(crate) fn begin(valid: bool) -> Self {
        let commit_txn = tidb_tracing::start_thread_region_ex("session.CommitTxn");
        let do_commit =
            valid.then(|| tidb_tracing::start_thread_region_ex("session.doCommitWithRetry"));
        Self {
            _do_commit: do_commit,
            _commit_txn: commit_txn,
        }
    }
}

/// The session's `SessionVars` as an optimistic provider reads them when its
/// transaction activates; `in_txn` is Go's `InTxn()` at that moment.
struct ActivationVars<'a> {
    session: &'a Session,
    in_txn: bool,
}

impl ProviderSessionVars for SessionTxnContext<'_> {
    fn retry_limit(&self) -> i64 {
        ActivationVars { session: self.session, in_txn: self.in_txn }.retry_limit()
    }
    fn snapshot_ts(&self) -> u64 {
        self.session.vars().snapshot_ts()
    }
    fn in_txn(&self) -> bool {
        self.in_txn
    }
    fn in_restricted_sql(&self) -> bool {
        self.session.vars().restricted_sql
    }
    fn disable_txn_auto_retry(&self) -> bool {
        ActivationVars { session: self.session, in_txn: self.in_txn }.disable_txn_auto_retry()
    }
}

impl ProviderSessionVars for ActivationVars<'_> {
    fn retry_limit(&self) -> i64 {
        self.session
            .vars()
            .get_system(tidb_vardef::tidb_vars::TIDB_RETRY_LIMIT)
            .ok()
            .and_then(|value| value.parse::<i64>().ok())
            .unwrap_or(tidb_vardef::defaults::DEF_TIDB_RETRY_LIMIT)
    }
    fn snapshot_ts(&self) -> u64 {
        self.session.vars().snapshot_ts()
    }
    fn in_txn(&self) -> bool {
        self.in_txn
    }
    fn in_restricted_sql(&self) -> bool {
        self.session.vars().restricted_sql
    }
    fn disable_txn_auto_retry(&self) -> bool {
        self.session.vars().system_bool(
            tidb_vardef::tidb_vars::TIDB_DISABLE_TXN_AUTO_RETRY,
            tidb_vardef::defaults::DEF_TIDB_DISABLE_TXN_AUTO_RETRY,
        )
    }
}

/// What `EnterNewTxn`'s initialization reads beyond Go's request (Go's
/// `OnInitialize` and `PrepareTxnCtx` read them from `sctx`).
#[derive(Clone, Copy)]
pub struct EnterNewTxnInputs<'a> {
    /// The statement's resource group.
    pub resource_group: &'a str,
    /// The catalog version an explicit transaction pins.
    pub schema_version: i64,
    /// An autocommit statement's read-ts record (`BeforeStmt` in
    /// autocommit).
    pub read_ts: Option<&'a tidb_domain_core::cluster_transactions::StatementReadTs>,
    /// The statement is an autocommit INSERT / UPDATE / DELETE (Go
    /// `isDMLStatement`).
    pub autocommit_write: bool,
}

/// What the session's one transaction is.
#[derive(Clone, Copy, PartialEq, Eq)]
enum TxnKind {
    /// Entered by `BEGIN` or an `autocommit = 0` statement (Go `InTxn()`).
    Explicit,
    /// An autocommit statement's implicit transaction (Go's `LazyTxn`
    /// entered by `EnterNewTxnBeforeStmt` or, for a statement's `AS OF`
    /// read, `EnterNewTxnWithReplaceProvider`), committed by the statement's
    /// `finishStmt`: a transaction that staged nothing reaches no store.
    Implicit,
    /// `START TRANSACTION READ ONLY AS OF`: Go's stale transaction. Its
    /// statements read through the staleness provider; it publishes nothing
    /// and ends without a commit.
    Stale,
}

/// The entered transaction and what it is.
struct EnteredTxn {
    kind: TxnKind,
    provider: Box<dyn TxnContextProvider>,
}

/// Go `newProviderWithRequest`: the staleness provider for a stale read,
/// else optimistic, else the pessimistic provider of the level the
/// transaction enters with (`IsolationLevelForNewTxn`), REPEATABLE-READ for
/// every level but READ-COMMITTED and SERIALIZABLE.
fn new_provider_with_request(
    stale: bool,
    pessimistic: bool,
    isolation: &str,
    txn: Box<dyn OpenClusterTransaction>,
) -> Box<dyn TxnContextProvider> {
    if stale {
        return Box::new(StalenessTxnContextProvider::new(txn));
    }
    if !pessimistic {
        return Box::new(OptimisticTxnContextProvider::new(txn));
    }
    match isolation.to_ascii_uppercase().as_str() {
        "READ-COMMITTED" => Box::new(PessimisticRCTxnContextProvider::new(txn)),
        "SERIALIZABLE" => Box::new(PessimisticSerializableTxnContextProvider::new(txn)),
        _ => Box::new(PessimisticRRTxnContextProvider::new(txn)),
    }
}

/// The session as Go's providers read `sctx` (see [`TxnContext`]).
pub(crate) struct SessionTxnContext<'a> {
    pub(crate) session: &'a mut Session,
    /// Go `InTxn()` at the moment of the call.
    pub(crate) in_txn: bool,
    /// The statement is a `SELECT ... FOR UPDATE`.
    pub(crate) locking_read: bool,
}

impl TxnContext for SessionTxnContext<'_> {
    fn rc_check_ts(&self) -> bool {
        self.session.stmt_rc_check_ts()
    }
    fn set_rc_check_ts(&mut self) {
        self.session.set_stmt_rc_check_ts();
    }
    fn pessimistic_fair_locking(&self) -> bool {
        let vars = self.session.vars();
        vars.pessimistic_transaction_fair_locking()
            && vars.connection_id.is_some_and(|id| id != 0)
            && !vars.restricted_sql
    }
    fn need_set_rc_check_ts_flag(&self, node: &tidb_ast::Stmt) -> bool {
        self.session.need_set_rc_check_ts_flag(node)
    }
    fn locking_read(&self) -> bool {
        self.locking_read
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
    /// Go's one `LazyTxn`: the transaction a `BEGIN` holds open (or the
    /// pending one an `autocommit = 0` statement entered), or an autocommit
    /// statement's implicit one, pending on its timestamp future (Go
    /// `txnFuture`) until a read, a lock or the commit activates it.
    txn: Option<EnteredTxn>,
    /// Go `LazyTxn` captures this policy when the transaction becomes valid.
    lazy_uniqueness_check_enabled: bool,
    /// Held from `BEGIN` to `COMMIT`/`ROLLBACK`.
    pin: Option<Box<dyn Send>>,
    /// Go `TxnCtx.CouldRetry`: decided by the optimistic provider when the
    /// transaction is entered (`isOptimisticTxnRetryable`); a pessimistic or
    /// stale-read transaction never sets it.
    could_retry: bool,
    /// Go `doCommitWithRetry`'s `txnSize`, read from the buffer just before
    /// the autocommit commit that may fail: the commit resets the buffer, and
    /// the retry budget is decided after it.
    autocommit_txn_bytes: usize,
    /// Go `doCommitWithRetry`'s `isPipelined` of the implicit transaction
    /// last ended, which no retry replays.
    implicit_txn_pipelined: bool,
}

impl TxnManager {
    /// Go `LazyTxn.GoString`.
    pub fn go_string(&self) -> String {
        match self.explicit() {
            None => "Txn{state=invalid}".into(),
            Some(txn) => match txn.start_ts_if_active() {
                Some(ts) => format!("Txn{{state=valid, txnStartTS={ts}}}"),
                None => "Txn{state=pending}".into(),
            },
        }
    }

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
            txn: None,
            lazy_uniqueness_check_enabled: false,
            pin: None,
            could_retry: false,
            autocommit_txn_bytes: 0,
            implicit_txn_pipelined: false,
        }
    }

    /// Go `txn.IsPipelined()` of the implicit transaction last ended.
    #[must_use]
    pub fn implicit_txn_pipelined(&self) -> bool {
        self.implicit_txn_pipelined
    }

    /// Go `TxnCtx.CouldRetry` of the transaction last entered.
    #[must_use]
    pub fn could_retry(&self) -> bool {
        self.could_retry
    }

    /// Go `OptimisticTxnContextProvider.onTxnActive`: an optimistic
    /// transaction's `CouldRetry`; the pessimistic providers leave the new
    /// `TxnCtx`'s `false`.
    pub(crate) fn decide_could_retry(&mut self, session: &Session, pessimistic: bool, tp: EnterNewTxnType, in_txn: bool) {
        self.could_retry = !pessimistic
            && is_optimistic_txn_retryable(
                &ActivationVars { session, in_txn },
                tp,
                false,
            );
    }

    /// The statement's transaction: the explicit one, else an autocommit
    /// write's implicit one (Go's one `LazyTxn`).
    #[must_use]
    pub fn current(&self) -> Option<&dyn OpenClusterTransaction> {
        self.txn.as_ref().map(|entered| entered.provider.txn())
    }

    /// The explicit transaction: Go's `LazyTxn` while `InTxn()`.
    #[must_use]
    pub fn explicit(&self) -> Option<&dyn OpenClusterTransaction> {
        self.txn
            .as_ref()
            .filter(|entered| entered.kind == TxnKind::Explicit)
            .map(|entered| entered.provider.txn())
    }

    /// The provider of the session's transaction, read-only.
    #[must_use]
    pub fn provider(&self) -> Option<&dyn TxnContextProvider> {
        self.txn.as_ref().map(|entered| &*entered.provider)
    }

    /// The provider of the session's transaction (Go
    /// `sessiontxn.GetTxnManager(sctx).GetContextProvider()`).
    pub fn provider_mut(&mut self) -> Option<&mut (dyn TxnContextProvider + 'static)> {
        self.txn.as_mut().map(|entered| &mut *entered.provider)
    }

    fn set_explicit(&mut self, txn: Box<dyn OpenClusterTransaction>, pessimistic: bool, isolation: &str) {
        // An autocommit statement ends its implicit transaction before the
        // next statement enters one.
        debug_assert!(self.txn.is_none(), "a transaction entered over another");
        self.txn = Some(EnteredTxn {
            kind: TxnKind::Explicit,
            provider: new_provider_with_request(false, pessimistic, isolation, txn),
        });
    }

    /// Go `TxnManager.EnterNewTxn`: enters the transaction `request` asks
    /// for, under the provider `newProviderWithRequest` chooses, initialized
    /// as that provider's `OnInitialize` does for the request type:
    ///
    /// * `WithBeginStmt` / `Default`: `BEGIN` activates the transaction at
    ///   once (`activeNow`), a stale `BEGIN` at its as-of timestamp;
    /// * `BeforeStmt`: before a statement that finds no transaction, a lazy
    ///   one -- the `autocommit = 0` transaction (Go `InTxn` from its
    ///   activation), or an autocommit statement's implicit one;
    /// * `WithReplaceProvider`: a statement's `AS OF` read.
    pub fn enter_new_txn(
        &mut self,
        session: &Session,
        request: &EnterNewTxnRequest,
        inputs: EnterNewTxnInputs<'_>,
    ) -> Result<(), SqlQueryError> {
        let pessimistic = match request.txn_mode {
            TxnMode::Pessimistic => true,
            TxnMode::Optimistic => false,
            TxnMode::FromVars => Self::is_pessimistic(session),
        };
        match request.tp {
            EnterNewTxnType::Default | EnterNewTxnType::WithBeginStmt => {
                self.begin(session, pessimistic, request.stale_read_ts, inputs.resource_group, inputs.schema_version)
            }
            // The `autocommit = 0` transaction: no statement read-ts record
            // (`enter_if_autocommit_off`); an autocommit statement passes its
            // own.
            EnterNewTxnType::BeforeStmt if inputs.read_ts.is_none() => {
                self.enter_pending(session, pessimistic, inputs.resource_group, inputs.schema_version);
                Ok(())
            }
            EnterNewTxnType::BeforeStmt => {
                let read_ts = inputs.read_ts.expect("checked by the arm above");
                let options = Self::implicit_statement_options(session);
                self.decide_could_retry(session, pessimistic, EnterNewTxnType::BeforeStmt, false);
                let isolation = session.isolation_for_provider();
                let low_resolution = session.vars().use_low_resolution_tso();
                self.enter_implicit(pessimistic, &isolation, low_resolution, inputs.resource_group, options, read_ts.clone());
                Ok(())
            }
            EnterNewTxnType::WithReplaceProvider => {
                let options = Self::implicit_statement_options(session);
                self.enter_stale(true, request.stale_read_ts, inputs.resource_group, options);
                Ok(())
            }
        }
    }

    /// `SetOptionsOnTxnActive`'s inputs for an autocommit statement's
    /// implicit transaction.
    fn implicit_statement_options(session: &Session) -> SessionTransactionOptions {
        set_options_on_txn_active(session.vars())
            .with_statement_kv_context(session.statement_kv_context_cell())
            .with_temporary_tables(session.temporary_table_ids())
            .with_guarantee_linearizability(session.guarantee_linearizability())
            .with_kill_signal(session.kv_kill_signal())
    }

    /// Ends the explicit transaction's hold on the slot (its commit or
    /// rollback follows).
    pub fn take_explicit(&mut self) -> Option<Box<dyn OpenClusterTransaction>> {
        self.explicit()?;
        self.txn.take().map(|entered| entered.provider.into_txn())
    }

    fn take_implicit(&mut self) -> Option<Box<dyn OpenClusterTransaction>> {
        if self.explicit().is_some() {
            return None;
        }
        let pipelined = self.buffer.is_pipelined();
        let entered = self.txn.take()?;
        self.implicit_txn_pipelined = pipelined;
        Some(entered.provider.into_txn())
    }

    /// Go `PrepareTxnCtx` -> `EnterNewTxn(EnterNewTxnBeforeStmt)` for a
    /// statement outside any transaction: the implicit transaction (Go's one
    /// `LazyTxn`, writable whatever the statement), entered unprepared and
    /// pessimistic when `decideTxnMode` says so. `AdviseWarmup` requests its
    /// timestamp (Go `newOracleFuture`, a low-resolution one under
    /// `tidb_low_resolution_tso`); an optimistic provider's
    /// `AdviseOptimizeWithPlan` may make it a `MaxUint64` read
    /// (`forcePrepareConstStartTS`); otherwise its first read, lock or write
    /// activates it (`ActivateTxn`) and records its start in `read_ts`.
    /// Fair locking starts per statement, from the pessimistic provider's
    /// `OnPessimisticStmtStart`.
    fn enter_implicit(
        &mut self,
        pessimistic: bool,
        isolation: &str,
        low_resolution: bool,
        resource_group: &str,
        options: SessionTransactionOptions,
        read_ts: tidb_domain_core::cluster_transactions::StatementReadTs,
    ) {
        let oracle: OracleFuture = {
            let transactions = Arc::clone(&self.transactions);
            let resource_group = resource_group.to_owned();
            let options = options.clone();
            let read_ts = read_ts.clone();
            Box::new(move |pipelined| {
                let pending = transactions.prepare_autocommit_txn(
                    &resource_group,
                    pessimistic,
                    false,
                    low_resolution,
                    pipelined,
                    options,
                );
                oracle_opener(pending, read_ts)
            })
        };
        let constant: Option<ConstantFuture> = (!pessimistic).then(|| {
            let transactions = Arc::clone(&self.transactions);
            let resource_group = resource_group.to_owned();
            Box::new(move |ts| {
                Box::new(move |_options| {
                    debug_assert_eq!(ts, u64::MAX, "the only constant start ts is the point get's MaxUint64");
                    read_ts.activate_const_start_ts(ts);
                    Ok(Box::new(MaxTsReadTransaction::new(transactions, &resource_group))
                        as Box<dyn OpenClusterTransaction>)
                }) as TransactionOpener
            }) as ConstantFuture
        });
        let txn: Box<dyn OpenClusterTransaction> = Box::new(LazyClusterTransaction::new_unprepared_implicit(
            pessimistic,
            options,
            self.buffer.clone(),
            oracle,
            constant,
        ));
        debug_assert!(self.txn.is_none(), "a transaction entered over another");
        self.txn = Some(EnteredTxn {
            kind: TxnKind::Implicit,
            provider: new_provider_with_request(false, pessimistic, isolation, txn),
        });
    }

    /// Go `StalenessTxnContextProvider.OnInitialize`: a read-only
    /// transaction at the as-of timestamp, its statements read through the
    /// staleness provider (`activateStaleTxn` for a stale `BEGIN`,
    /// `enterNewStaleTxnWithReplaceProvider` for one statement).
    fn enter_stale(
        &mut self,
        statement_only: bool,
        ts: u64,
        resource_group: &str,
        options: SessionTransactionOptions,
    ) {
        let pending = self.transactions.prepare_stale_read(ts, resource_group, options.clone());
        let opener: TransactionOpener = Box::new(move |_options| pending.and_then(|pending| pending.wait()));
        let txn: Box<dyn OpenClusterTransaction> =
            Box::new(LazyClusterTransaction::new_implicit_read(options, self.buffer.clone(), opener));
        debug_assert!(self.txn.is_none(), "a transaction entered over another");
        self.could_retry = false;
        self.txn = Some(EnteredTxn {
            kind: if statement_only { TxnKind::Implicit } else { TxnKind::Stale },
            provider: new_provider_with_request(true, false, "", txn),
        });
    }

    /// Ends a stale transaction (it publishes nothing).
    fn end_stale(&mut self) {
        if self.txn.as_ref().is_some_and(|entered| entered.kind == TxnKind::Stale) {
            if let Some(entered) = self.txn.take() {
                let _ = entered.provider.into_txn().rollback();
            }
        }
    }

    /// Go `RollbackTxn` for an autocommit statement that failed or is
    /// replayed: the implicit transaction ends without publishing.
    pub fn rollback_implicit(&mut self) {
        if let Some(transaction) = self.take_implicit() {
            let _ = transaction.rollback();
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

    /// Gives up the staged buffer to the owner it was lent by, keeping a
    /// fresh one: a DDL step session's buffer is the step's (Go `w.sess`'s
    /// transaction), which the step, not the session, commits or drops.
    pub(crate) fn release_buffer(&mut self) {
        self.buffer = MutationBuffer::new();
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
            None => tidb_vardef::txn_mode::txn_mode_variable(
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
        set_options_on_txn_active(session.vars())
            .with_statement_kv_context(session.statement_kv_context_cell())
            .with_temporary_tables(session.temporary_table_ids())
            .with_guarantee_linearizability(session.guarantee_linearizability())
            .with_low_resolution_tso(session.vars().use_low_resolution_tso())
            .with_kill_signal(session.kv_kill_signal())
    }

    /// Go `LazyTxn`'s activation sets `kv.SizeLimits` from the session's
    /// `tidb_txn_entry_size_limit` (above 0) or the configured limit: a
    /// statement whose transaction is not open yet writes under them.
    pub fn set_pending_size_limits(&self, session: &Session) {
        let entry_limit = session
            .vars()
            .system_value(tidb_vardef::tidb_vars::TIDB_TXN_ENTRY_SIZE_LIMIT)
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
            .filter(|limit| *limit > 0)
            .unwrap_or_else(tidb_txnkv::txn_entry_size_limit);
        self.buffer
            .set_pending_size_limits(entry_limit, tidb_txnkv::txn_total_size_limit());
    }

    /// Go `PrepareTxnCtx` for `autocommit = 0`: enters the transaction when the
    /// statement about to run needs one and none is entered.
    pub fn enter_if_autocommit_off(
        &mut self,
        session: &Session,
        resource_group: &str,
        schema_version: i64,
    ) {
        if self.explicit().is_some() || session.is_autocommit() {
            return;
        }
        let request = EnterNewTxnRequest {
            tp: EnterNewTxnType::BeforeStmt,
            stale_read_ts: session.historical_read_ts().unwrap_or(0),
            ..EnterNewTxnRequest::default()
        };
        let inputs = EnterNewTxnInputs { resource_group, schema_version, read_ts: None, autocommit_write: false };
        // A pending transaction enters without failing; its activation
        // reports the error.
        let _ = self.enter_new_txn(session, &request, inputs);
    }

    /// Go `PrepareTxnCtx` -> `EnterNewTxn(EnterNewTxnBeforeStmt)`
    /// (`pkg/session/session.go:4908`): the `autocommit = 0` transaction is
    /// entered but left pending. It takes its timestamp, publishes
    /// `TxnCtx.StartTS` and registers in `information_schema.tidb_trx` at the
    /// first statement that activates it (`lazy_transaction`).
    fn enter_pending(&mut self, session: &Session, pessimistic: bool, resource_group: &str, schema_version: i64) {
        if let Some(ts) = session.historical_read_ts() {
            // A stale read publishes its own timestamp and opens no store
            // transaction.
            let _ = self.begin(session, pessimistic, ts, resource_group, schema_version);
            return;
        }
        self.decide_could_retry(session, pessimistic, EnterNewTxnType::BeforeStmt, true);
        // Fair locking starts per statement, from the provider's
        // `OnPessimisticStmtStart`.
        let fair_locking = false;
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
        let isolation = session.isolation_for_provider();
        self.set_explicit(Box::new(LazyClusterTransaction::new(
            pessimistic,
            Self::statement_transaction_options(session),
            self.buffer.clone(),
            opener,
        )), pessimistic, &isolation);
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
    /// timestamp is taken here; a stale `BEGIN` (`stale_read_ts`) at its
    /// as-of timestamp.
    fn begin(
        &mut self,
        session: &Session,
        pessimistic: bool,
        stale_read_ts: u64,
        resource_group: &str,
        schema_version: i64,
    ) -> Result<(), SqlQueryError> {
        if stale_read_ts != 0 {
            let ts = stale_read_ts;
            session.current_tso().publish(ts);
            if let Some(observer) = session.transaction_observer() {
                observer.activated(ts);
            }
            self.enter_stale(false, ts, resource_group, Self::statement_transaction_options(session));
            return Ok(());
        }
        self.decide_could_retry(session, pessimistic, EnterNewTxnType::WithBeginStmt, true);
        // Fair locking starts per statement, from the provider's
        // `OnPessimisticStmtStart`.
        let fair_locking = false;
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
        self.set_explicit(transaction, pessimistic, &session.isolation_for_provider());
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
        let Some(transaction) = self.explicit() else {
            return Ok(None);
        };
        transaction.activate().map_err(SqlQueryError::unknown)?;
        Ok(Some(self.buffer.checkpoint()))
    }

    /// Go `RollbackMemDBToCheckpoint`: the staged writes go back to the
    /// checkpoint a savepoint took.
    pub fn rollback_mem_db_to_checkpoint(&mut self, checkpoint: &BufferCheckpoint) {
        self.buffer.restore(checkpoint.clone());
    }

    /// Go `doCommitWithRetry`'s `tryReplaceWriteConflictError`, then Go's
    /// `kv.CommitHook`: a transaction that committed records its own timestamps
    /// as `@@tidb_last_txn_info`. One that published nothing leaves what the
    /// statement recorded.
    fn record_committed_txn_info(session: &mut Session, committed: Option<CommittedTxnInfo>) {
        if let Some(committed) = committed {
            session.record_committed_txn_info(&committed);
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
            tidb_metrics::client_go_metrics::observe_txn_write_size_bytes(write_size, false);
            session
                .txn_write_throughput_sli()
                .add_txn_write_size(write_size, write_keys);
        }
    }

    /// Commits the autocommit statement's implicit transaction when the
    /// session is not inside `BEGIN`: the statement's `CommitTxn` (Go
    /// `autoCommitAfterStmt`).
    ///
    /// A transaction that staged nothing -- every read statement -- reaches
    /// no store and spends no timestamp, as a Go COMMIT of a transaction that
    /// wrote nothing does.
    pub fn flush_if_autocommit(
        &mut self,
        session: &mut Session,
        hooks: &dyn CommitHooks,
        read_ts: Option<u64>,
    ) -> Result<(), SqlQueryError> {
        if self.explicit().is_some() || session.in_transaction() {
            return Ok(());
        }
        let transaction = self.take_implicit();
        // Go's autocommit `CommitTxn`, which a failed commit has run too.
        session.set_txn_isolation_level_one_shot_state_for_next_txn();
        // Go `doCommitWithRetry` -> `recordOnTransactionExecution`: a
        // transaction the statement activated (Go `txn.Valid()`) counts once,
        // committed or not.
        let activated = transaction
            .as_ref()
            .is_some_and(|transaction| transaction.start_ts_if_active().is_some());
        if read_ts.is_some() || activated {
            if let Some(metrics) = session.session_ruv2_metrics() {
                metrics.add_txn_cnt(1);
            }
        }
        let write_details = self.buffer_write_details();
        self.autocommit_txn_bytes = self.buffer.staged_bytes();
        // Go `autoCommitAfterStmt` -> `LazyTxn.Commit`: the transaction is
        // committing from here on.
        session.publish_transaction_committing();
        tidb_failpoint::eval("github.com/pingcap/tidb/pkg/session/mockSlowCommit");
        let Some(mut transaction) = transaction else {
            // Go `doCommitWithRetry` returns for a transaction that is not
            // valid: a statement that entered none staged nothing (a write
            // activates the transaction it stages in).
            debug_assert!(self.buffer.is_empty(), "writes staged outside a transaction");
            session.finish_txn_fair_locking(true);
            return Ok(());
        };
        let pipelined = self.buffer.is_pipelined();
        if !pipelined {
            if let Some(checker) = hooks.schema_lease_checker() {
                transaction.set_schema_lease_checker(checker);
            }
        }
        let bindings_changed = hooks.binding_records_changed(&self.buffer);
        let renew = session.start_cached_table_renew_lease()?;
        if pipelined {
            // Go `SetOptionsBeforeCommit`: a pipelined transaction already
            // flushed its mutations into the stores, so it takes no option;
            // the invariants its designed use keeps are checked instead.
            Self::check_pipelined_before_commit(session, renew.is_some())?;
        } else if let Some(renew) = &renew {
            transaction.set_commit_ts_upper_bound_check(renew.commit_ts_check());
        }
        let temporary = session
            .stage_temporary_data_commit(&self.buffer)
            .map_err(|error| SqlQueryError::unknown(format!("{error:?}")))?;
        let committed = transaction.commit(&self.buffer);
        drop(renew);
        Session::finish_temporary_data_commit(temporary, committed.is_ok());
        let outcome = match committed {
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
        // Go `finishStmt`'s `FinishExecuteStmt` after the commit ran.
        session.finish_txn_fair_locking(true);
        outcome
    }

    /// Go `SetOptionsBeforeCommit`'s checks of a pipelined transaction.
    fn check_pipelined_before_commit(session: &Session, commit_ts_checker: bool) -> Result<(), SqlQueryError> {
        if !tidb_vardef::is_mdl_enabled(tidb_config_kerneltype::is_next_gen()) {
            return Err(SqlQueryError::unknown(
                "cannot commit pipelined transaction without Metadata Lock: MDL is OFF",
            ));
        }
        if !session.txn_temporary_tables.lock().unwrap_or_else(std::sync::PoisonError::into_inner).is_empty() {
            return Err(SqlQueryError::unknown("pipelined dml with temporary tables is not allowed"));
        }
        let cdc_write_source = session
            .vars()
            .get_system(tidb_vardef::tidb_vars::TIDB_CDC_WRITE_SOURCE)
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
            .unwrap_or(0);
        if cdc_write_source != 0 {
            return Err(SqlQueryError::unknown("pipelined dml with CDC source is not allowed"));
        }
        if commit_ts_checker {
            return Err(SqlQueryError::unknown("pipelined dml with commitTS checker is not allowed"));
        }
        Ok(())
    }

    /// Ends the explicit transaction by publishing its buffer at its own start
    /// timestamp (Go `CommitTxn`).
    ///
    /// A `COMMIT` with no transaction open commits nothing: the previous
    /// statement's implicit transaction already ended.
    pub fn commit(
        &mut self,
        session: &mut Session,
        hooks: &dyn CommitHooks,
    ) -> Result<(), SqlQueryError> {
        let result = self.commit_once(session, hooks);
        // Go COMMIT's `FinishExecuteStmt` after `CommitTxn` ran.
        session.finish_txn_fair_locking(false);
        result
    }

    fn commit_once(
        &mut self,
        session: &mut Session,
        hooks: &dyn CommitHooks,
    ) -> Result<(), SqlQueryError> {
        session.current_tso().clear();
        self.pin = None;
        let Some(mut transaction) = self.take_explicit() else {
            self.end_stale();
            if let Some(observer) = session.transaction_observer() {
                observer.finished();
            }
            // Go `CommitTxn` -> `doCommitWithRetry` of a transaction that is
            // not valid: nothing to publish.
            debug_assert!(self.buffer.is_empty(), "writes staged outside a transaction");
            return Ok(());
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
        let renew = session.start_cached_table_renew_lease()?;
        if let Some(renew) = &renew {
            transaction.set_commit_ts_upper_bound_check(renew.commit_ts_check());
        }
        let temporary = session
            .stage_temporary_data_commit(&self.buffer)
            .map_err(|error| SqlQueryError::unknown(format!("{error:?}")))?;
        // Go `LazyTxn.Commit`: the commit and its deferred cleanup may
        // release MemDB memory after the session tracker has been killed;
        // Rollback's dummy hook protects that terminal phase too.
        self.buffer
            .set_memory_footprint_change_hook(std::sync::Arc::new(|_: u64| {}));
        // Go `LazyTxn.Commit`, after publishing TxnCommitting.
        tidb_failpoint::eval("github.com/pingcap/tidb/pkg/session/mockSlowCommit");
        let commit_result = transaction.commit(&self.buffer);
        drop(renew);
        Session::finish_temporary_data_commit(temporary, commit_result.is_ok());
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
            // Go `recordOnTransactionExecution` / `recordAbortTxnDuration`.
            if let Some(metrics) = session.session_ruv2_metrics() {
                metrics.add_txn_cnt(1);
            }
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
        session.reset_txn_fair_locking();
        session.clear_table_delta();
        session.current_tso().clear();
        self.pin = None;
        let result = match self.take_explicit() {
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
                // Go `LazyTxn.Rollback`, after publishing TxnRollingBack.
                tidb_failpoint::eval("github.com/pingcap/tidb/pkg/session/mockSlowRollback");
                // When rolling back a txn, swap with a dummy hook to avoid
                // operations on an invalid memory tracker.
                self.buffer
                    .set_memory_footprint_change_hook(std::sync::Arc::new(|_: u64| {}));
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
                    // Go `recordOnTransactionExecution` / `recordAbortTxnDuration`.
                    if let Some(metrics) = session.session_ruv2_metrics() {
                        metrics.add_txn_cnt(1);
                    }
                }
                result.map_err(SqlQueryError::unknown)
            }
            None => {
                self.end_stale();
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

/// Go `cachedTableRenewLease` (`session.go`): before a transaction that wrote
/// cached tables commits, each table's write lock is taken and kept alive
/// until the commit ends; the commit refuses a commit ts past any of the
/// write leases.
pub(crate) struct CachedTableRenewLease {
    leases: Vec<std::sync::Arc<std::sync::atomic::AtomicU64>>,
    exits: Vec<std::sync::mpsc::Sender<()>>,
}

impl CachedTableRenewLease {
    /// Go `start`: waits until every table's `LockForWrite` returned.
    pub(crate) fn start(
        tables: &tidb_tblctx::temporary_table::CachedTables,
    ) -> Result<Option<Self>, SqlQueryError> {
        let tables: Vec<_> = tables
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .values()
            .cloned()
            .collect();
        if tables.is_empty() {
            return Ok(None);
        }
        let (wg, done) = std::sync::mpsc::channel();
        let mut renew = Self {
            leases: Vec::with_capacity(tables.len()),
            exits: Vec::with_capacity(tables.len()),
        };
        for raw in tables {
            let table = raw
                .downcast::<tidb_table::tables::cache::CachedTable>()
                .map_err(|_| SqlQueryError::unknown("a cached table handle is not a cached table"))?;
            let lease = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
            let (exit, exit_rx) = std::sync::mpsc::channel();
            let (thread_lease, thread_wg) = (std::sync::Arc::clone(&lease), wg.clone());
            std::thread::spawn(move || {
                table.write_lock_and_keep_alive(&exit_rx, &thread_lease, &thread_wg);
            });
            renew.leases.push(lease);
            renew.exits.push(exit);
        }
        drop(wg);
        let mut failure = None;
        for _ in 0..renew.leases.len() {
            if let Ok(Err(error)) = done.recv() {
                failure = Some(error);
            }
        }
        match failure {
            Some(error) => Err(SqlQueryError::unknown(error.to_string())),
            None => Ok(Some(renew)),
        }
    }

    /// Go `commitTSCheck`: a commit ts at or past a write lease fails the
    /// commit.
    pub(crate) fn commit_ts_check(&self) -> tidb_store_driver_txn::session_commit_protocol::CommitTsUpperBoundCheck {
        let leases = self.leases.clone();
        tidb_store_driver_txn::session_commit_protocol::CommitTsUpperBoundCheck(std::sync::Arc::new(
            move |commit_ts| {
                leases
                    .iter()
                    .all(|lease| commit_ts < lease.load(std::sync::atomic::Ordering::SeqCst))
            },
        ))
    }
}

/// Go `stop`: the keep-alives end with the commit.
impl Drop for CachedTableRenewLease {
    fn drop(&mut self) {
        self.exits.clear();
    }
}

/// The opener of a transaction pending on Go's oracle future: waits for the
/// timestamp and records the statement's start in `read_ts`.
fn oracle_opener(
    pending: Result<Box<dyn tidb_domain_core::cluster_transactions::PendingClusterTransaction>, String>,
    read_ts: tidb_domain_core::cluster_transactions::StatementReadTs,
) -> TransactionOpener {
    Box::new(move |_options| {
        let begin = std::time::Instant::now();
        let transaction = pending.and_then(|pending| pending.wait());
        read_ts.record_wait_ts(begin.elapsed());
        let transaction = transaction?;
        read_ts.record(transaction.start_ts());
        Ok(transaction)
    })
}

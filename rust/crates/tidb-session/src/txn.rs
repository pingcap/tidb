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

//! Transaction control on the session side: Go's `SessionVars.TxnCtx` (with
//! `InTxn`), the `SimpleExec` transaction statements (`executeBegin`,
//! `executeCommit`, `executeRollback`, `executeSavepoint`,
//! `executeReleaseSavepoint`, `pkg/executor/simple.go`) and the doors every
//! statement reaches the catalog through.
//!
//! The session owns no transaction. The store transaction is the store
//! session's ([`crate::txn_manager::TxnManager`], Go `LazyTxn` behind
//! `sessiontxn.TxnManager`), which the transaction statements reach through
//! [`TxnManagerOps`] as Go's executors reach `sessiontxn.GetTxnManager(sctx)`.

use std::sync::MutexGuard;

use tidb_ast::{SessionStmt, Stmt};
use tidb_datatype::Datum;
use tidb_store_driver_error::sql_query_error::SqlQueryError;
use tidb_store_driver_txn::cluster_storage::BufferCheckpoint;
use tidb_driver_error::DriverError;
use tidb_infoschema::SessionExtendedInfoSchema;

use crate::{txn_mode_for_begin, PESSIMISTIC_TXN_MODE};
use crate::{Session, SessionTxnMode};

/// A persisted schema image and the storage authority's retained timestamp guard.
pub struct HistoricalRead {
    pub catalog: SessionExtendedInfoSchema,
    /// Go's stale-read `sctx.Txn()`: the store the historical catalog's
    /// tables read through.
    pub store: Box<dyn tidb_kv::storage::TableStorage>,
    pub timestamp_hold: std::sync::Arc<dyn Send + Sync>,
}

/// Domain/storage callback shared by explicit and one-statement historical reads.
pub type HistoricalReadProvider =
    dyn Fn(u64, &str, Option<&SessionExtendedInfoSchema>) -> Result<HistoricalRead, DriverError> + Send + Sync;

/// A server's Go `sessionctx.Context.Txn(true)` for the running statement:
/// opens the statement's (lazily activated) transaction, which publishes its
/// start timestamp to the session's `CurrentTso`.
pub type StatementTxnActivator = dyn Fn() + Send + Sync;

/// SET-time validation and schema loading, without replacing the active row snapshot.
pub type SnapshotSchemaProvider =
    dyn Fn(u64, &str, bool) -> Result<SessionExtendedInfoSchema, DriverError> + Send + Sync;

/// Go `SessionVars.TxnCtx` of the open transaction; `Some` is Go's `InTxn`.
///
/// It holds the transaction's context and none of its data: the rows a
/// transaction writes are staged in the store session's `MutationBuffer`, and
/// its reads go through the statement's snapshot.
pub(crate) struct TxnCtx {
    /// The mode the transaction opened in, resolved from the `BEGIN` keyword
    /// and `@@tidb_txn_mode` exactly as Go resolves it (`TxnCtx.IsPessimistic`).
    mode: SessionTxnMode,
    /// Go `TxnCtx.Isolation == ReadCommitted`: a later `SET SESSION` affects
    /// the next transaction, not the missing-key lock policy of this one.
    read_committed: bool,
    /// Go `StalenessTxnContextProvider`: the as-of timestamp, which is also
    /// the transaction's `StartTS`, and the historical schema and timestamp
    /// hold every statement of the transaction reads (`TxnCtx.InfoSchema`).
    pub(crate) stale: Option<(u64, HistoricalRead)>,
    /// The transaction's savepoint stack, oldest first -- Go
    /// `TxnCtx.Savepoints` (`pkg/sessionctx/variable/session.go`).
    savepoints: tidb_variable::savepoints::SavepointStack<Savepoint>,
    /// Go `EnterNewTxnWithBeginStmt`: a `BEGIN` opened the transaction, and
    /// whether it said `WITH CAUSAL CONSISTENCY ONLY` (the provider's
    /// `causalConsistencyOnly`); `None` for one a statement opened.
    begin_stmt: Option<bool>,
    /// Go `TxnCtx.IsStaleness`: a staleness provider's transaction (`AS OF`,
    /// `READ ONLY AS OF`, `tidb_read_staleness`, `tidb_txn_read_ts`), not a
    /// `tidb_snapshot` read.
    is_staleness: bool,
}

impl TxnCtx {
    pub(crate) const fn is_stale_read(&self) -> bool {
        self.stale.is_some()
    }
}

/// The store half of the transaction statements: Go
/// `sessiontxn.GetTxnManager(sctx)` as the `SimpleExec` arms reach it. The
/// store session implements it over its `TxnManager`.
pub trait TxnManagerOps {
    /// Go `CommitTxn` of the store transaction (`session.go:982`), under its
    /// `session.CommitTxn` / `doCommitWithRetry` regions.
    fn commit_txn(&mut self, session: &mut Session) -> Result<(), DriverError>;
    /// Go `RollbackTxn` (`session.go:1022`): the store transaction, entered
    /// or pending, and every write it staged are dropped.
    fn rollback_txn(&mut self, session: &mut Session) -> Result<(), DriverError>;
    /// Go `EnterNewTxn(EnterNewTxnWithBeginStmt)`: the transaction activates
    /// at once (`OnInitialize`'s `activeNow`, `isolation/base.go:88`), with
    /// the latest schema (`GetSnapshotInfoSchema(startTS)`).
    fn enter_new_txn_with_begin(&mut self, session: &mut Session) -> Result<(), DriverError>;
    /// Go `executeSavepoint`'s `Txn(true)` and `GetMemBuffer().Checkpoint()`:
    /// activates a pending `autocommit = 0` transaction and marks its
    /// membuffer. `None` when no transaction is open (autocommit).
    fn savepoint_checkpoint(
        &mut self,
        session: &mut Session,
    ) -> Result<Option<BufferCheckpoint>, DriverError>;
    /// Go `RollbackMemDBToCheckpoint`.
    fn rollback_to_checkpoint(&mut self, checkpoint: &BufferCheckpoint);
}

/// The driver error `error` was rendered from, when its client-visible code
/// and message are still that error's own (a caller that reworded the
/// message has made the cause stale).
pub(crate) fn driver_cause(error: &SqlQueryError) -> Option<&DriverError> {
    let cause = error.cause_of::<DriverError>()?;
    let rendered = cause.clone().to_mysql_error();
    (rendered.code == error.code && rendered.message == error.message).then_some(cause)
}

/// A store-session error as a driver error, its typed cause (Go
/// `errors.Cause`) kept when it has one.
pub(crate) fn driver_error_of(error: SqlQueryError) -> DriverError {
    if let Some(cause) = driver_cause(&error) {
        return cause.clone();
    }
    let mut mapped = tidb_terror::MysqlError::from_parts(error.code, error.state, error.message);
    if let Some(identity) = error.identity {
        mapped = mapped.with_identity(identity);
    }
    DriverError::Mysql(mapped)
}

/// One entry of a transaction's savepoint stack: Go `SavepointRecord`.
///
/// Kept even after a `ROLLBACK TO` restores from it, because Go's
/// `RollbackToSavepoint` truncates the stack to `[:idx+1]` -- the named
/// savepoint SURVIVES its own rollback and can be rolled back to again.
pub(crate) struct Savepoint {
    /// Go `MemDBCheckpoint`: the store transaction's membuffer position, which
    /// `RollbackMemDBToCheckpoint` truncates back to. `None` for a transaction
    /// no store transaction backs (a stale read).
    buffer: Option<BufferCheckpoint>,
    /// The Go `TxnCtx.TableDeltaMap` alongside the row checkpoint.
    table_delta: std::collections::HashMap<i64, tidb_variable::table_delta::TableDelta>,
}

/// The session's LOCAL temporary tables while they are OFF the catalog, and
/// the two moves that put them on and take them back.
///
/// Go does this in two pieces, both per statement:
///
/// * `temptable.AttachLocalTemporaryTableInfoSchema` wraps the statement's
///   infoschema so `TableByName` finds a LOCAL temporary table before the
///   shared one of the same name;
/// * `temptable.SessionSnapshotInterceptor` routes reads of a temporary
///   table's key range away from TiKV and into the session's own membuffer,
///   for which the session publishes which ids are temporary.
///
/// A GLOBAL temporary table's `TableInfo` is shared and stays where it is;
/// its transaction state (allocator, size) is `TxnCtx.TemporaryTables`,
/// which the table reaches through its mutate context.
struct TemporaryTableOverlay {
    local: Vec<(String, String, tidb_table::tables::TableCommon)>,
}

impl TemporaryTableOverlay {
    /// Attaches the overlay to `catalog`, runs `body`, and takes the overlay
    /// back.
    ///
    /// A PANIC inside `body` leaves the overlay attached, which is safe only
    /// because of what a panic already costs here: on the shared-catalog path
    /// the mutex is poisoned and every later statement is refused with
    /// `CatalogPoisoned`, and on the transaction path the catalog being
    /// mutated is the transaction's private copy, which is dropped. Neither
    /// leaves a temporary table visible to another session.
    fn run<T>(
        &mut self,
        catalog: &mut SessionExtendedInfoSchema,
        temporary: &crate::temptable::SessionTemporaryTables,
        body: impl FnOnce(&mut SessionExtendedInfoSchema) -> Result<T, DriverError>,
    ) -> Result<T, DriverError> {
        let mut kinds: std::collections::HashMap<i64, tidb_model::TempTableType> = self
            .local
            .iter()
            .map(|(_, _, table)| (table.table_id(), tidb_model::TempTableType::LOCAL))
            .collect();
        catalog.attach_local_temporary_table_info_schema(std::mem::take(&mut self.local));
        global_temporary_kinds(catalog, &mut kinds);
        temporary.publish(kinds);
        let value = body(catalog);
        self.local = catalog.detach_local_temporary_table_info_schema();
        value
    }
}

/// Adds every GLOBAL temporary table's id to `kinds`.
///
/// Memoized walk (Go answers this from `temptable`'s session map, not by
/// iterating infoschema): a session with no global temporary tables -- every
/// OLTP workload -- pays one epoch compare per call.
fn global_temporary_kinds(
    catalog: &mut SessionExtendedInfoSchema,
    kinds: &mut std::collections::HashMap<i64, tidb_model::TempTableType>,
) {
    let ids = catalog.global_temporary_table_ids_memo();
    if ids.is_empty() {
        return;
    }
    for (database, name) in ids.to_vec() {
        if let Some((_, table)) = catalog.table_info_by_name(&database, &name) {
            kinds.insert(table.read().id, tidb_model::TempTableType::GLOBAL);
        }
    }
}

/// Runs a statement for a session that owns no LOCAL temporary tables,
/// returning the overlay when the statement creates its first one.
///
/// The metadata-version check keeps the ordinary OLTP path at one memoized
/// GLOBAL-table check: row statements cannot create temporary metadata, so
/// they do not need the post-statement catalog sweep.
fn run_discovering_temporary_overlay<T>(
    catalog: &mut SessionExtendedInfoSchema,
    temporary: &crate::temptable::SessionTemporaryTables,
    body: impl FnOnce(&mut SessionExtendedInfoSchema) -> Result<T, DriverError>,
) -> (Result<T, DriverError>, Option<TemporaryTableOverlay>) {
    if !catalog.global_temporary_table_ids_memo().is_empty() {
        let mut overlay = TemporaryTableOverlay { local: Vec::new() };
        let value = overlay.run(catalog, temporary, body);
        let discovered = (!overlay.local.is_empty()).then_some(overlay);
        return (value, discovered);
    }

    let metadata_version = catalog.metadata_version();
    let value = body(catalog);
    if catalog.metadata_version() == metadata_version {
        return (value, None);
    }

    // A metadata-changing statement may have created the session's first
    // LOCAL or GLOBAL temporary table. Detach it exactly as the ordinary
    // overlay path would have done around the statement.
    let mut kinds = std::collections::HashMap::new();
    global_temporary_kinds(catalog, &mut kinds);
    let overlay = TemporaryTableOverlay {
        local: catalog.detach_local_temporary_table_info_schema(),
    };
    kinds.extend(
        overlay
            .local
            .iter()
            .map(|(_, _, table)| (table.table_id(), tidb_model::TempTableType::LOCAL)),
    );
    temporary.publish(kinds);
    let discovered = (!overlay.local.is_empty()).then_some(overlay);
    (value, discovered)
}

impl Session {
    /// Go `SessionVars.IsAutocommit()`: whether each statement stands on its
    /// own, or joins a transaction the session keeps open for it.
    ///
    /// Public because a front end over cluster storage has to open the same
    /// transaction this session will: `SET autocommit = 0` opens one with no
    /// `BEGIN` keyword for anyone to route on, so the variable itself is the
    /// only thing there is to ask, and asking the session keeps it the ONE
    /// answer rather than a second copy that can disagree.
    #[must_use]
    pub fn is_autocommit(&self) -> bool {
        self.vars.is_autocommit()
    }

    /// The session half of Go's lazy `autocommit = 0` transaction: a
    /// statement that reads or writes data activates the pending transaction
    /// the store session entered for it (`PrepareTxnCtx`), and Go's
    /// `ActivateTxn` then sets `InTxn` (`isolation/base.go:322`), so a later
    /// `ROLLBACK` can discard it. `BEGIN` opens one explicitly; this only
    /// covers the statements that would otherwise have none.
    pub(crate) fn begin_implicit_transaction(&mut self) -> Result<(), DriverError> {
        // Go's condition: `!IsAutocommit() && SnapshotTS == 0`; a read under
        // `tidb_snapshot` activates no transaction of the session's.
        if self.txn_ctx.is_some() || self.is_autocommit() || self.vars.snapshot_ts() != 0 {
            return Ok(());
        }
        let mode = self.resolve_begin_txn_mode(tidb_ast::TransactionMode::Default);
        self.open_txn_ctx(mode, None);
        Ok(())
    }

    /// Whether a transaction is open (Go `SessionVars.InTxn`, the wire's
    /// `SERVER_STATUS_IN_TRANS`).
    #[must_use]
    pub fn in_transaction(&self) -> bool {
        self.txn_ctx.is_some()
    }

    /// Go `doCommitWithRetry`'s deferred
    /// `SetTxnIsolationLevelOneShotStateForNextTxn`: every `CommitTxn` -- a
    /// `COMMIT`, an implicit commit, an autocommit statement's, with or
    /// without a valid transaction -- moves `SET TRANSACTION`'s level on.
    pub fn set_txn_isolation_level_one_shot_state_for_next_txn(&mut self) {
        self.statement_committed_txn = true;
        self.vars
            .set_txn_isolation_level_one_shot_state_for_next_txn();
    }

    /// Go keeps an ended transaction's `TxnCtx` until the next statement
    /// builds a new one, so the statement that ended it still finishes with
    /// that context: its slow log carries the transaction's start timestamp
    /// (`FinishExecuteStmt(origTxnCtx.StartTS, ...)`) and `IsExplicitTxn`.
    fn record_statement_ended_txn(&mut self) {
        if self.statement_ended_txn_start_ts.is_none() {
            self.statement_ended_txn_start_ts =
                Some(u64::try_from(self.current_tso.value()).unwrap_or(0));
        }
    }

    /// Go SessionVars.ConstraintCheckInPlacePessimistic, read live per statement.
    pub fn pessimistic_constraint_check_in_place(&self) -> bool {
        self.vars
            .get_system("tidb_constraint_check_in_place_pessimistic")
            .is_ok_and(|value| value.eq_ignore_ascii_case("on") || value == "1")
    }

    /// The mode the open transaction runs in, if one is open (Go
    /// `TxnCtx.IsPessimistic`).
    #[must_use]
    pub fn txn_mode(&self) -> Option<SessionTxnMode> {
        self.txn_ctx.as_ref().map(|txn| txn.mode)
    }

    /// Whether locking reads must acquire only existing keys after reading.
    /// This is Go's IsIsolation(ReadCommitted) decision, retained by the
    /// open transaction. Before a lazy transaction starts, use its setting.
    /// Go `SessionVars.IsIsolation(ast.ReadCommitted)` for the transaction
    /// in force (or, before one starts, the session's setting).
    #[must_use]
    pub fn is_read_committed(&self) -> bool {
        self.read_committed_locking()
    }

    pub(crate) fn read_committed_locking(&self) -> bool {
        self.txn_ctx.as_ref().map_or_else(
            || match self.vars.txn_isolation_level_one_shot_in_use() {
                // Go `IsIsolation` adopts a waiting `SET TRANSACTION` level.
                Some(level) => level.eq_ignore_ascii_case("READ-COMMITTED"),
                None => self
                    .vars
                    .get_system("transaction_isolation")
                    .is_ok_and(|value| value.eq_ignore_ascii_case("READ-COMMITTED")),
            },
            |txn| txn.read_committed,
        )
    }

    /// The transaction mode a statement will use, including autocommit.
    ///
    /// Go's `optimizeDupKeyCheckForNormalInsert` asks the transaction returned
    /// by `Txn(true)`, so an autocommit statement still observes the session's
    /// `@@tidb_txn_mode` default before that implicit transaction is materialized.
    /// Explicit `BEGIN <mode>` continues to win over the variable.
    #[must_use]
    /// Go `TxnCtx.IsPessimistic` for the running statement: an explicit or
    /// `autocommit = 0` pessimistic transaction, or the autocommit
    /// statement's implicit transaction when `decideTxnMode` chose
    /// pessimistic (see [`Self::set_autocommit_pessimistic_statement`]).
    pub(crate) fn statement_is_pessimistic(&self) -> bool {
        ((!self.is_autocommit() || self.in_transaction())
            && self.statement_txn_mode().is_pessimistic())
            || self.autocommit_pessimistic_statement
    }

    /// Records that the storage owner runs this autocommit statement in a
    /// pessimistic implicit transaction (Go `decideTxnMode`), so its point
    /// plans carry `Lock` as Go's `tryUpdatePointPlan` sets it.
    pub fn set_autocommit_pessimistic_statement(&mut self, pessimistic: bool) {
        self.autocommit_pessimistic_statement = pessimistic;
    }

    pub fn statement_txn_mode(&self) -> SessionTxnMode {
        self.txn_mode()
            .unwrap_or_else(|| self.resolve_begin_txn_mode(tidb_ast::TransactionMode::Default))
    }

    /// Go `NewTxn`'s fresh `TxnCtx`: the context of a transaction the store
    /// session opens alongside it. The store transaction publishes its own
    /// `StartTS` (`@@tidb_current_ts`) and `TIDB_TRX` row when it activates.
    fn open_txn_ctx(&mut self, mode: SessionTxnMode, begin_stmt: Option<bool>) {
        // Go `txnManager.newProviderWithRequest` picks the provider from
        // `IsolationLevelForNewTxn`, which honors `SET TRANSACTION`.
        let read_committed = self
            .vars
            .isolation_level_for_new_txn(self.in_transaction())
            .eq_ignore_ascii_case("READ-COMMITTED");
        // A fresh transaction gets a fresh, empty staged-write tracker --
        // Go's own membuffer is empty the moment a transaction activates.
        self.staged_writes = std::sync::Arc::default();
        self.txn_ctx = Some(TxnCtx {
            mode,
            read_committed,
            stale: None,
            savepoints: tidb_variable::savepoints::SavepointStack::default(),
            begin_stmt,
            is_staleness: false,
        });
    }

    /// Go `SetOptionsOnTxnActive`'s `kv.GuaranteeLinearizability`: off under
    /// `tidb_guarantee_linearizability = OFF` or `WITH CAUSAL CONSISTENCY
    /// ONLY`; otherwise on unless the transaction is an autocommit statement's
    /// (its start timestamp already follows every earlier commit).
    pub(crate) fn guarantee_linearizability(&self) -> bool {
        let begin_stmt = self.txn_ctx.as_ref().and_then(|ctx| ctx.begin_stmt);
        if begin_stmt == Some(true)
            || !self
                .vars
                .get_system(tidb_vardef::tidb_vars::TIDB_GUARANTEE_LINEARIZABILITY)
                .is_ok_and(|value| tidb_vardef::option_values::tidb_opt_on(&value))
        {
            return false;
        }
        !self.is_autocommit() || self.vars.snapshot_ts() > 0 || begin_stmt.is_some()
    }

    /// Go `CalculateAsOfTsExpr` (`pkg/sessiontxn/staleread/util.go:41-86`),
    /// over this session's own expression engine.
    ///
    /// Order is Go's exactly: NULL refuses; a DATETIME interpretation is
    /// tried first (through `UNIX_TIMESTAMP`, whose session-zone semantics
    /// are the ported `time.Date`), and only then a raw TSO -- a positive
    /// integer or a string of digits. A raw TSO whose physical half is
    /// before 2013-01-01 refuses with Go's own message.
    pub(crate) fn calculate_as_of_ts_expr(&mut self, expr: &tidb_ast::Expr) -> Result<u64, DriverError> {
        const TSO_LOGICAL_BITS: u32 = 18;
        // 2013-01-01 00:00:00 UTC in milliseconds, Go's `minTSO` bound.
        const MIN_PHYSICAL_MS: u64 = 1_356_998_400_000;
        let value = self.eval_value(expr)?;
        let as_of_error = |cause: &str| {
            DriverError::from(
                tidb_plannererrors::ERR_AS_OF
                    .generate_with_stack_by_args(&[(cause.to_owned()).into()]),
            )
        };
        if matches!(value, Datum::Null) {
            return Err(as_of_error("as of timestamp cannot be NULL"));
        }
        let text = value.sql_string().unwrap_or_default();
        let unix_call = |text: &str| tidb_ast::Expr::Func {
            name: "UNIX_TIMESTAMP".to_owned(),
            args: vec![tidb_ast::Expr::String(text.to_owned())],
            origin_position: 0,
        };
        // Go tries the datetime reading FIRST (util.go:60-67), deliberately
        // differing from `tidb_snapshot` on compact forms. UNIX_TIMESTAMP
        // answers NULL (or 0) for a string that is no datetime, which is the
        // fall-through to the raw-TSO reading.
        let seconds = match self.eval_value(&unix_call(&text))? {
            Datum::Int(v) if v > 0 => Some(v as f64),
            Datum::UInt(v) if v > 0 => Some(v as f64),
            Datum::Real(v) if v > 0.0 => Some(v),
            Datum::Decimal(decimal) => {
                let parsed = decimal.to_string().parse::<f64>().unwrap_or(0.0);
                (parsed > 0.0).then_some(parsed)
            }
            _ => None,
        };
        if let Some(seconds) = seconds {
            return Ok(((seconds * 1000.0) as u64) << TSO_LOGICAL_BITS);
        }
        let Ok(tso) = text.parse::<u64>() else {
            return Err(as_of_error(
                "cannot parse AS OF TIMESTAMP expression as datetime or TSO",
            ));
        };
        // Only a raw TSO is bounded (util.go:77-80); a datetime before 2013
        // is a valid read timestamp that the GC safe point then refuses.
        if (tso >> TSO_LOGICAL_BITS) <= MIN_PHYSICAL_MS {
            return Err(as_of_error(
                "invalid TSO timestamp: TSO is before 2013-01-01",
            ));
        }
        Ok(tso)
    }

    /// Installs the existing store/Domain authority for historical schema and rows.
    pub fn set_historical_read_provider(
        &mut self,
        provider: std::sync::Arc<HistoricalReadProvider>,
    ) {
        self.historical_read_provider = Some(provider);
    }

    /// Installs the server's activation of a statement's transaction, which
    /// owns the statement snapshot this session only reads through.
    pub fn set_statement_txn_activator(
        &mut self,
        activator: std::sync::Arc<StatementTxnActivator>,
    ) {
        self.statement_txn_activator = Some(activator);
    }

    /// Go `Ctx().Txn(true)` from an executor builder that needs the
    /// statement's start timestamp without reading a row
    /// (`executorBuilder.buildShow` for `SHOW MASTER STATUS`): activating the
    /// transaction is what publishes `TxnCtx.StartTS`. An open transaction
    /// already published its own; a session no server drives has no lazy
    /// statement transaction to activate.
    pub(crate) fn activate_statement_txn(&self) {
        if let Some(activate) = &self.statement_txn_activator {
            activate();
        }
    }

    /// Installs the store's SET-time schema loader. The boolean requests timestamp
    /// and GC validation when a new nonzero timestamp is selected, as in Go SetExecutor.
    pub fn set_snapshot_schema_provider(
        &mut self,
        provider: std::sync::Arc<SnapshotSchemaProvider>,
    ) {
        self.snapshot_schema_provider = Some(provider);
    }

    /// Domain's internal sessions must ignore the external timestamp read switch.
    pub fn set_restricted_sql(&mut self, restricted: bool) {
        self.vars.restricted_sql = restricted;
    }

    /// Effective historical timestamp of the active statement or stale transaction.
    /// Go `TxnCtx.IsStaleness`.
    pub(crate) fn is_staleness_txn(&self) -> bool {
        self.txn_ctx.as_ref().is_some_and(|txn| txn.is_staleness)
    }

    pub fn historical_read_ts(&self) -> Option<u64> {
        self.statement_snapshot
            .as_ref()
            .map(|(ts, _)| *ts)
            .or_else(|| {
                self.txn_ctx
                    .as_ref()
                    .and_then(|txn| txn.stale.as_ref().map(|(ts, _)| *ts))
            })
    }

    /// `START TRANSACTION READ ONLY AS OF TIMESTAMP <resolved ts>`: the Go
    /// stale transaction (`StalenessTxnContextProvider`), whose `StartTS` IS
    /// the as-of timestamp and whose reads all see the store as of it.
    pub(crate) fn open_stale_transaction(
        &mut self,
        ts: u64,
        is_staleness: bool,
    ) -> Result<(), DriverError> {
        let provider = self.historical_read_provider.as_ref().ok_or_else(|| {
            DriverError::unsupported(
                "historical reads require the store's historical read provider",
            )
        })?;
        let schema = self
            .snapshot_schema
            .as_ref()
            .filter(|(selected, _)| *selected == ts)
            .map(|(_, catalog)| catalog);
        let read = provider(ts, self.current_resource_group(), schema)?;
        // A historical read also has an empty transaction membuffer.
        self.staged_writes = std::sync::Arc::default();
        self.txn_ctx = Some(TxnCtx {
            mode: SessionTxnMode::Optimistic,
            read_committed: false,
            stale: Some((ts, read)),
            savepoints: tidb_variable::savepoints::SavepointStack::default(),
            begin_stmt: Some(false),
            is_staleness,
        });
        self.current_tso().publish(ts);
        if let Some(observer) = self.transaction_observer() {
            observer.activated(ts);
        }
        if let Some(process) = &self.process {
            process.registry().transaction_started(process.id(), ts);
        }
        Ok(())
    }

    pub(crate) fn cleanup_txn_read_ts(&mut self) {
        if self.vars.cleanup_txn_read_ts() {
            self.snapshot_schema = None;
        }
    }

    /// Ends the one-statement stale transaction the as-of interception
    /// opened: nothing to publish (read-only by construction), the
    /// start-only `LastTxnInfo` record a read-only end leaves, and the
    /// published timestamp goes with it.
    pub(crate) fn discard_stale_statement_transaction(&mut self) {
        self.cleanup_txn_read_ts();
        if let Some((ts, _)) = self.txn_ctx.take().and_then(|txn| txn.stale) {
            self.set_last_txn_info_started(ts);
        }
        if let Some(process) = &self.process {
            process.registry().transaction_finished(process.id());
        }
        self.current_tso().clear();
    }

    /// Go `newProviderWithRequest`: `BEGIN <mode>` wins over `@@tidb_txn_mode`.
    fn resolve_begin_txn_mode(&self, mode: tidb_ast::TransactionMode) -> SessionTxnMode {
        let variable = self
            .vars
            .get_system("tidb_txn_mode")
            .unwrap_or_else(|_| PESSIMISTIC_TXN_MODE.to_owned());
        txn_mode_for_begin(mode, &variable)
    }

    /// Applies `BEGIN`/`START TRANSACTION`, `COMMIT`, `ROLLBACK` or a
    /// savepoint statement on a node-backed session, through its connection
    /// (a Go `testkit` session's transaction is its store transaction).
    ///
    /// Returns `Some(in_transaction)` for those statements and `None` for
    /// anything else, so a caller can answer with an OK packet carrying the
    /// right status flag without re-parsing.
    pub fn control_transaction(&mut self, sql: &str) -> Result<Option<bool>, DriverError> {
        self.node_conn().run_transaction_control(sql)
    }

    /// The `SimpleExec` transaction statements (`pkg/executor/simple.go`):
    /// `executeBegin`, `executeCommit`, `executeRollback`, `executeSavepoint`
    /// and `executeReleaseSavepoint`, with the store half through `txn`.
    ///
    /// Returns `Some(in_transaction)` for those statements and `None` for
    /// anything else.
    ///
    /// `COMMIT` only leaves the transaction here, as Go's `executeCommit`
    /// only clears `InTxn`; the store session commits it when the statement
    /// finishes (Go `finishStmt` -> `CommitTxn`).
    pub fn control_transaction_stmt(
        &mut self,
        stmt: &Stmt,
        txn: &mut dyn TxnManagerOps,
    ) -> Result<Option<bool>, DriverError> {
        let Stmt::Session(session_stmt) = stmt else {
            return Ok(None);
        };
        match &**session_stmt {
            SessionStmt::Begin(begin) => {
                self.execute_begin(begin, txn)?;
                Ok(Some(true))
            }
            SessionStmt::Commit(_) => {
                self.leave_txn_ctx_for_commit();
                Ok(Some(false))
            }
            SessionStmt::Rollback { savepoint, .. } => {
                if let Some(name) = savepoint {
                    // ROLLBACK TO does NOT end the transaction: it restores
                    // the data and leaves everything else running.
                    self.rollback_to_savepoint(name, txn)?;
                    return Ok(Some(true));
                }
                self.execute_rollback(txn)?;
                Ok(Some(false))
            }
            SessionStmt::Savepoint(name) => {
                self.set_savepoint(name, txn)?;
                Ok(Some(self.txn_ctx.is_some()))
            }
            SessionStmt::ReleaseSavepoint(name) => {
                self.release_savepoint(name)?;
                Ok(Some(true))
            }
            _ => Ok(None),
        }
    }

    /// Go `SimpleExec.executeBegin` -> `EnterNewTxn(EnterNewTxnWithBeginStmt)`.
    fn execute_begin(
        &mut self,
        begin: &tidb_ast::BeginStmt,
        txn: &mut dyn TxnManagerOps,
    ) -> Result<(), DriverError> {
        // `START TRANSACTION READ ONLY` is a no-op clause -- TiDB does not
        // actually stop writes -- so it goes through
        // `tidb_enable_noop_functions`, refusing with 1235 at the OFF
        // default, BEFORE `NewTxn` commits the open transaction. `AS OF
        // TIMESTAMP` exempts it, because that spelling names a real
        // historical read rather than the bare read-only claim.
        if begin.read_only && begin.as_of.is_none() {
            self.gate_noop_clause("READ ONLY", false)?;
        }
        // `START TRANSACTION READ ONLY AS OF TIMESTAMP <expr>` opens the
        // transaction AT that timestamp (Go `StalenessTxnContextProvider`):
        // the expression resolves through `CalculateAsOfTsExpr`'s rules when
        // the statement is built, before any commit, and the transaction's
        // `StartTS` -- which `@@tidb_current_ts` reports -- is that timestamp.
        let as_of = match &begin.as_of {
            Some(expr) => {
                let ts = self.calculate_as_of_ts_expr(&expr.clone())?;
                if self.vars.txn_read_ts() != 0 {
                    if let Some(provider) = &self.snapshot_schema_provider {
                        provider(ts, self.current_resource_group(), false)?;
                    }
                    return Err(DriverError::unsupported("start transaction read only as of is forbidden after set transaction read only as of"));
                }
                Some(ts)
            }
            None => None,
        };
        // Go's `NewTxn` gives the transaction a fresh `TxnCtx`, so the
        // statement's own (`origTxnCtx`) is not the one it opens. Its GLOBAL
        // temporary rows never reach a commit (`temporaryTableKVFilter`), and
        // a BEGIN refused above leaves the open transaction's untouched.
        self.statement_began_txn = true;
        self.reset_transaction_scope();
        self.commit_before_enter_new_txn(txn)?;
        // The fresh `TxnCtx`'s `TemporaryTables`, after the old transaction
        // committed under its own.
        self.txn_temporary_tables = std::sync::Arc::default();
        self.txn_cached_tables = std::sync::Arc::default();
        let stale_ts = match as_of {
            Some(ts) => Some(ts),
            None => match self.vars.use_txn_read_ts() {
                0 => self.external_read_ts()?,
                read_ts => Some(read_ts),
            },
        };
        match stale_ts {
            Some(ts) => self.open_stale_transaction(ts, true)?,
            None => {
                let mode = self.resolve_begin_txn_mode(begin.mode);
                self.open_txn_ctx(mode, Some(begin.causal_consistency_only));
            }
        }
        // Go sets `InTxn` only once `ActivateTxn` succeeded
        // (`isolation/base.go:108-114`): a failed activation leaves no
        // transaction open.
        if let Err(error) = txn.enter_new_txn_with_begin(self) {
            self.txn_ctx = None;
            self.current_tso().clear();
            return Err(error);
        }
        Ok(())
    }

    /// Go `sessiontxn.NewTxnInStmt`'s commit (`executor/ddl.go:151`,
    /// `grant.go:144`, `revoke.go:71`, `simple.go:153`): the executor commits
    /// the open transaction before its own work, and the statement goes on
    /// in a fresh transaction context. The caller asks only when the store
    /// holds a transaction; a commit that fails -- an undetermined one
    /// included -- is the statement's error.
    pub(crate) fn new_txn_in_stmt(&mut self, txn: &mut dyn TxnManagerOps) -> Result<(), DriverError> {
        self.reset_transaction_scope();
        self.leave_txn_ctx_for_commit();
        let committed = txn.commit_txn(self);
        self.txn_temporary_tables = std::sync::Arc::default();
        self.txn_cached_tables = std::sync::Arc::default();
        committed
    }

    /// Go `sessiontxn.CommitBeforeEnterNewTxn`: an open transaction is
    /// committed before a new one is entered; a pending one no statement
    /// activated, or writes a finished autocommit statement left behind, are
    /// dropped, not committed (`txn.Valid()` is false for them).
    fn commit_before_enter_new_txn(
        &mut self,
        txn: &mut dyn TxnManagerOps,
    ) -> Result<(), DriverError> {
        if self.in_transaction() {
            self.leave_txn_ctx_for_commit();
            if let Err(error) = txn.commit_txn(self) {
                // A commit that fails has ended the transaction (Go rolls it
                // back); its staged writes go with it.
                let _ = txn.rollback_txn(self);
                return Err(error);
            }
        }
        txn.rollback_txn(self)
    }

    /// The session half of Go `CommitTxn`: `doCommitWithRetry`'s deferred
    /// one-shot isolation step and the slow log's ended transaction, then the
    /// context goes. Go `executeCommit` is only this (`SetInTxn(false)`); the
    /// store commit follows when the statement finishes.
    pub(crate) fn leave_txn_ctx_for_commit(&mut self) {
        self.set_txn_isolation_level_one_shot_state_for_next_txn();
        if self.txn_ctx.is_some() {
            self.record_statement_ended_txn();
        }
        self.cleanup_txn_read_ts();
        let Some(txn) = self.txn_ctx.take() else {
            return;
        };
        if let Some(process) = &self.process {
            process
                .registry()
                .transaction_state(process.id(), "Committing");
        }
        if let Some((ts, _)) = txn.stale {
            // Go's stale transaction is read-only by construction; its
            // COMMIT publishes nothing, and `setLastTxnInfoBeforeTxnEnd`
            // leaves the start-only record (`pkg/session/session.go:1056`).
            self.set_last_txn_info_started(ts);
            self.current_tso().clear();
            if let Some(process) = &self.process {
                process.registry().transaction_finished(process.id());
            }
        }
    }

    /// Go `SimpleExec.executeRollback` without a savepoint: `RollbackTxn`.
    fn execute_rollback(&mut self, txn: &mut dyn TxnManagerOps) -> Result<(), DriverError> {
        self.leave_txn_ctx_for_rollback();
        txn.rollback_txn(self)
    }

    /// The session half of Go `RollbackTxn`: the context goes, and the rows
    /// of the local temporary tables go back to the transaction's start. The
    /// caller rolls the store transaction back.
    pub(crate) fn leave_txn_ctx_for_rollback(&mut self) {
        self.cleanup_txn_read_ts();
        if self.txn_ctx.is_some() {
            self.record_statement_ended_txn();
        }
        if let Some(ctx) = self.txn_ctx.take() {
            self.clear_table_delta();
            if let Some(process) = &self.process {
                process
                    .registry()
                    .transaction_state(process.id(), "RollingBack");
            }
            // Go `setLastTxnInfoBeforeTxnEnd`: an activated transaction that
            // ends without a commit leaves the start-only record. The store
            // transaction records its own; a stale one is recorded here.
            if let Some((ts, _)) = &ctx.stale {
                self.set_last_txn_info_started(*ts);
            }
            self.current_tso().clear();
            // The staged writes go with the store transaction, temporary
            // tables' rows among them (Go keeps those in the membuffer
            // until `commitTxnWithTemporaryData`).
            if let Some(process) = &self.process {
                process.registry().transaction_finished(process.id());
            }
        }
    }

    /// `SAVEPOINT name` -- Go `SimpleExec.executeSavepoint`.
    ///
    /// The no-op arm is narrower than "no transaction open": Go returns `nil`
    /// only when `!sessVars.InTxn() && sessVars.IsAutocommit()`. With
    /// autocommit OFF and no transaction yet, `e.Ctx().Txn(true)` ACTIVATES
    /// the pending transaction, so `SAVEPOINT` is what opens it and a later
    /// `ROLLBACK TO` that name finds it. Only in autocommit does the
    /// statement succeed while recording nothing, leaving `ROLLBACK TO` to
    /// report 1305.
    ///
    /// Redefining an existing name is `AddSavepoint`: DELETE the old entry,
    /// then APPEND the new one. The distinction matters -- the redefinition
    /// moves the name to the END of the stack, so savepoints that were taken
    /// after the original are no longer "after" it, and a later `ROLLBACK TO`
    /// the redefined name no longer drops them.
    fn set_savepoint(
        &mut self,
        name: &str,
        txn: &mut dyn TxnManagerOps,
    ) -> Result<(), DriverError> {
        // Go's `Txn(true)`: with autocommit OFF this is the statement that
        // opens the pending transaction.
        self.begin_implicit_transaction()?;
        if self.in_transaction()
            && self.statement_txn_mode().is_pessimistic()
            && !self.pessimistic_constraint_check_in_place()
        {
            return Err(DriverError::Mysql(tidb_terror::MysqlError::unknown(
                "savepoint is not supported in pessimistic transactions when in-place constraint check is disabled",
            )));
        }
        let buffer = txn.savepoint_checkpoint(self)?;
        let table_delta = self.table_delta_savepoint();
        let Some(ctx) = &mut self.txn_ctx else {
            return Ok(());
        };
        ctx.savepoints.add(
            name,
            Savepoint {
                buffer,
                table_delta,
            },
        );
        Ok(())
    }

    /// `ROLLBACK TO [SAVEPOINT] name` -- Go's `executeRollback` savepoint arm
    /// plus `TxnCtx.RollbackToSavepoint`.
    ///
    /// Restores the transaction's data to the savepoint (Go:
    /// `RollbackMemDBToCheckpoint`) and truncates the stack to `[:idx+1]`, so
    /// the savepoint itself survives and every savepoint taken after it is
    /// gone. The transaction stays OPEN -- Go returns before the
    /// `SetInTxn(false)` that a plain `ROLLBACK` reaches.
    ///
    /// With no transaction open Go's `txn.Valid()` is false and the error is
    /// the same 1305 an unknown name gets.
    fn rollback_to_savepoint(
        &mut self,
        name: &str,
        txn: &mut dyn TxnManagerOps,
    ) -> Result<(), DriverError> {
        let found = self.txn_ctx.as_mut().and_then(|ctx| {
            ctx.savepoints.rollback_to_savepoint(name).map(|savepoint| {
                (savepoint.buffer.clone(), savepoint.table_delta.clone())
            })
        });
        let Some((buffer, table_delta)) = found else {
            // go appends the statement error into the warning buffer as well
            // (driver_tidb.go:376's error door), so SHOW WARNINGS after the
            // refused ROLLBACK TO carries the same 1305 row.
            let error = DriverError::from(
                tidb_exeerrors::ERR_SAVEPOINT_NOT_EXISTS
                    .generate_with_stack_by_args(&["SAVEPOINT".into(), (name.to_owned()).into()]),
            );
            self.append_driver_error(crate::WarningLevel::Error, &error);
            return Err(error);
        };
        if let Some(buffer) = &buffer {
            txn.rollback_to_checkpoint(buffer);
        }
        self.restore_table_delta_savepoint(table_delta);
        Ok(())
    }

    /// `RELEASE SAVEPOINT name` -- Go `SimpleExec.executeReleaseSavepoint`
    /// plus `TxnCtx.ReleaseSavepoint`: drops the named savepoint AND every
    /// savepoint taken after it (`Savepoints[:i]`), touching no data.
    fn release_savepoint(&mut self, name: &str) -> Result<(), DriverError> {
        let released = self
            .txn_ctx
            .as_mut()
            .is_some_and(|ctx| ctx.savepoints.release(name));
        if !released {
            let error = DriverError::from(
                tidb_exeerrors::ERR_SAVEPOINT_NOT_EXISTS
                    .generate_with_stack_by_args(&["SAVEPOINT".into(), (name.to_owned()).into()]),
            );
            self.append_driver_error(crate::WarningLevel::Error, &error);
            return Err(error);
        }
        Ok(())
    }

    /// Runs `body` over the catalog the open transaction reads: a stale
    /// transaction's historical schema (Go `TxnCtx.InfoSchema`), otherwise the
    /// session's catalog.
    pub(crate) fn with_txn_catalog<T>(
        &self,
        body: impl FnOnce(&SessionExtendedInfoSchema) -> T,
    ) -> Result<T, DriverError> {
        if let Some((_, read)) = self.txn_ctx.as_ref().and_then(|ctx| ctx.stale.as_ref()) {
            return Ok(body(&read.catalog));
        }
        let catalog = self.lock_catalog()?;
        Ok(body(&catalog))
    }

    /// A stale transaction's historical catalog (Go `TxnCtx.InfoSchema`).
    pub(crate) fn txn_ctx_stale_catalog(&self) -> Option<&SessionExtendedInfoSchema> {
        self.txn_ctx
            .as_ref()
            .and_then(|ctx| ctx.stale.as_ref())
            .map(|(_, read)| &read.catalog)
    }

    /// Whether the open transaction is a stale read (Go `TxnCtx.IsStaleness`).
    pub(crate) fn txn_is_stale_read(&self) -> bool {
        self.txn_ctx.as_ref().is_some_and(TxnCtx::is_stale_read)
    }

    /// Borrows the shared catalog for one statement. The lock is held for the
    /// statement's duration only, which is the granularity Go's schema state
    /// is consumed at.
    pub(crate) fn lock_catalog(&self) -> Result<MutexGuard<'_, SessionExtendedInfoSchema>, DriverError> {
        self.catalog
            .lock()
            .map_err(|_| DriverError::CatalogPoisoned)
    }

    /// Runs `body` over the catalog this statement sees: the statement's
    /// snapshot catalog when one is bound, a stale transaction's historical
    /// catalog, otherwise the session's catalog. A transaction's own writes
    /// are read through the store session's buffer under its tables.
    pub(crate) fn with_catalog_mut<T>(
        &mut self,
        body: impl FnOnce(&mut SessionExtendedInfoSchema) -> Result<T, DriverError>,
    ) -> Result<T, DriverError> {
        self.with_catalog_scope(true, body)
    }

    /// [`Self::with_catalog_mut`] outside any statement, over a committed
    /// snapshot: what a test that looks at stored rows directly reads.
    #[cfg(test)]
    pub(crate) fn with_committed_catalog_mut<T>(
        &mut self,
        body: impl FnOnce(&mut SessionExtendedInfoSchema) -> Result<T, DriverError>,
    ) -> Result<T, DriverError> {
        self.with_committed_snapshot(|session| session.with_catalog_mut(body))?
    }

    /// Go's durable DDL executor uses the shared infoschema, without local
    /// temporary names. Global temporary row storage remains session-owned.
    pub(crate) fn with_persistent_catalog_mut<T>(
        &mut self,
        body: impl FnOnce(&mut SessionExtendedInfoSchema) -> Result<T, DriverError>,
    ) -> Result<T, DriverError> {
        self.with_catalog_scope(false, body)
    }

    /// Select the name scope while retaining the shared storage overlay lifecycle.
    pub(crate) fn with_catalog_scope<T>(
        &mut self,
        include_local: bool,
        body: impl FnOnce(&mut SessionExtendedInfoSchema) -> Result<T, DriverError>,
    ) -> Result<T, DriverError> {
        // A session that already owns temporary state always takes the full
        // attach/detach path. This is the uncommon branch.
        //
        // An empty session still has to ask the catalog whether GLOBAL
        // temporary metadata exists: another session may have created it,
        // and its rows must never use the shared table storage. The memoized
        // lookup is an epoch comparison in the common no-temporary-table
        // case. `run_discovering_temporary_overlay` also captures a LOCAL or
        // GLOBAL table created by this very statement, closing the gap where
        // the old early return left the first temporary table shared.
        let temporary = self.temporary.clone();
        if !include_local || self.local_temporary_tables.is_empty() {
            let (value, discovered) = if let Some((_, read)) = &mut self.statement_snapshot {
                run_discovering_temporary_overlay(&mut read.catalog, &temporary, body)
            } else if let Some((_, read)) = self.txn_ctx.as_mut().and_then(|ctx| ctx.stale.as_mut())
            {
                run_discovering_temporary_overlay(&mut read.catalog, &temporary, body)
            } else {
                let mut catalog = self
                    .catalog
                    .lock()
                    .map_err(|_| DriverError::CatalogPoisoned)?;
                run_discovering_temporary_overlay(&mut catalog, &temporary, body)
            };
            if let Some(discovered) = discovered {
                if include_local {
                    self.local_temporary_tables = discovered.local;
                } else {
                    debug_assert!(discovered.local.is_empty());
                }
            }
            return value;
        }
        // Go wraps every statement's infoschema in a
        // `SessionExtendedInfoSchema` (`temptable.AttachLocalTemporaryTable
        // InfoSchema`) and installs a snapshot interceptor for the temporary
        // rows (`temptable.SessionSnapshotInterceptor`). Both are per
        // STATEMENT, and both are undone before the statement's catalog is
        // shared with anyone -- which is exactly why they belong here, at the
        // one door every statement reaches the catalog through, rather than
        // in each statement arm.
        let mut overlay = TemporaryTableOverlay {
            local: if include_local {
                std::mem::take(&mut self.local_temporary_tables)
            } else {
                Vec::new()
            },
        };
        let value = if let Some((_, read)) = &mut self.statement_snapshot {
            overlay.run(&mut read.catalog, &temporary, body)
        } else if let Some((_, read)) = self.txn_ctx.as_mut().and_then(|ctx| ctx.stale.as_mut()) {
            overlay.run(&mut read.catalog, &temporary, body)
        } else {
            let mut catalog = self
                .catalog
                .lock()
                .map_err(|_| DriverError::CatalogPoisoned)?;
            overlay.run(&mut catalog, &temporary, body)
        };
        if include_local {
            self.local_temporary_tables = std::mem::take(&mut overlay.local);
        } else {
            debug_assert!(overlay.local.is_empty());
        }
        value
    }

    /// Starts the transaction-scoped state a statement sees when it does not
    /// continue an open transaction.
    ///
    /// Go hands every statement that is not continuing an open transaction
    /// a FRESH membuffer, so `session.HasDirtyContent` answers false for
    /// every table at this point -- and `BEGIN` therefore starts from an
    /// empty one. This tier's `staged_writes` handle is that membuffer:
    /// replacing it with a fresh, empty one is the O(1) equivalent of Go
    /// discarding its old one, no catalog walk involved. Every statement door
    /// -- this session's dispatch and the cluster node's routes -- calls this
    /// first. See [`Session::staged_writes`]'s own doc.
    ///
    /// The same boundary is where a GLOBAL temporary table empties. Its rows
    /// live in Go's `TxnCtx.TemporaryTables`, which is built fresh for each
    /// transaction and whose keys `temporaryTableKVFilter` strips before
    /// commit, so nothing a previous transaction wrote is ever readable again
    /// -- `ON COMMIT DELETE ROWS` is that and nothing more, which is also why
    /// TiDB refuses `ON COMMIT PRESERVE ROWS` outright. In autocommit every
    /// statement is its own transaction, so clearing here covers both shapes:
    /// the statement before this one was the transaction that ended, and a
    /// `BEGIN` arriving here starts from empty.
    pub fn start_statement_transaction_scope(&mut self) {
        if !self.in_transaction() {
            // Go's fresh `TxnCtx` for the transaction this statement starts:
            // its `TemporaryTables` begin empty. The previous transaction's
            // stay until here, because its COMMIT reads them
            // (`commitTxnWithTemporaryData`) after the scope reset above.
            self.txn_temporary_tables = std::sync::Arc::default();
            self.txn_cached_tables = std::sync::Arc::default();
            self.reset_transaction_scope();
        }
    }

    /// [`Self::start_statement_transaction_scope`] for a door that owns the
    /// transaction itself -- the cluster node, whose explicit transaction is
    /// its own and which knows no transaction continues into this statement.
    pub fn reset_transaction_scope(&mut self) {
        self.staged_writes = std::sync::Arc::default();
    }

    /// Runs one DML statement over the catalog this statement sees.
    ///
    /// Go's statement-level rollback stages the transaction's membuffer
    /// (`MemBuffer.Staging()`, then `StmtCommit`'s `Release()` or
    /// `StmtRollback`'s `Cleanup()`); every table writes through the
    /// session's transaction store, whose `MutationBuffer` the statement
    /// lifecycle checkpoints around each statement (`statement_lifecycle`),
    /// so a failed statement inside `BEGIN` discards only its own writes.
    /// AUTO_INCREMENT and `_tidb_rowid` ids a failed statement consumed stay
    /// consumed, as Go allocates them outside transaction semantics.
    pub(crate) fn with_staged_catalog<T>(
        &mut self,
        body: impl FnOnce(&mut SessionExtendedInfoSchema) -> Result<T, DriverError>,
    ) -> Result<T, DriverError> {
        self.with_catalog_mut(body)
    }

    /// Go `serverStatus2Str` over this session's status bits: the `State`
    /// column of `SHOW PROCESSLIST`.
    ///
    /// The typed autocommit state and transaction presence are the two status
    /// bits this tier owns, in the same order as Go's `ascServerStatus`.
    #[must_use]
    pub fn server_status_2_str(&self) -> String {
        match (self.txn_ctx.is_some(), self.is_autocommit()) {
            (true, true) => "in transaction; autocommit".to_owned(),
            (true, false) => "in transaction".to_owned(),
            (false, true) => "autocommit".to_owned(),
            (false, false) => String::new(),
        }
    }
}

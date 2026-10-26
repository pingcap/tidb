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
use tidb_exec::sql_query_error::SqlQueryError;
use tidb_executor::cluster_storage::BufferCheckpoint;
use tidb_executor::{Catalog, DriverError};

use crate::{txn_mode_for_begin, PESSIMISTIC_TXN_MODE};
use crate::{Session, SessionTxnMode, TxnErrorKind};

/// A persisted schema image and the storage authority's retained timestamp guard.
pub struct HistoricalRead {
    pub catalog: Catalog,
    pub timestamp_hold: std::sync::Arc<dyn Send + Sync>,
}

/// Domain/storage callback shared by explicit and one-statement historical reads.
pub type HistoricalReadProvider =
    dyn Fn(u64, &str, Option<&Catalog>) -> Result<HistoricalRead, DriverError> + Send + Sync;

/// A server's Go `sessionctx.Context.Txn(true)` for the running statement:
/// opens the statement's (lazily activated) transaction, which publishes its
/// start timestamp to the session's `CurrentTso`.
pub type StatementTxnActivator = dyn Fn() + Send + Sync;

/// SET-time validation and schema loading, without replacing the active row snapshot.
pub type SnapshotSchemaProvider =
    dyn Fn(u64, &str, bool) -> Result<Catalog, DriverError> + Send + Sync;

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
    stale: Option<(u64, HistoricalRead)>,
    /// The transaction's savepoint stack, oldest first -- Go
    /// `TxnCtx.Savepoints` (`pkg/sessionctx/variable/session.go`).
    savepoints: crate::SavepointStack<Savepoint>,
    /// The session's LOCAL temporary tables as they stood at `BEGIN`.
    ///
    /// Their ROWS are transactional in Go and their SCHEMA is not, and the
    /// two halves come from different places:
    /// `session.commitTxnWithTemporaryData` copies a local temporary table's
    /// keys out of the transaction membuffer into the session's own buffer
    /// only at COMMIT, so a rolled-back transaction's writes are simply
    /// dropped with the membuffer; but `createSessionTemporaryTable` and
    /// `DropLocalTemporaryTable` edit `SessionVars.LocalTemporaryTables`
    /// directly, outside any transaction, so a `CREATE TEMPORARY TABLE` or
    /// `DROP` inside a transaction SURVIVES its rollback. See
    /// [`Session::restore_local_temporary_rows`], which restores exactly the
    /// first half.
    local_temporary_at_open: Vec<(String, String, tidb_executor::KvTable)>,
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

/// A store-session error as a driver error, its typed cause (Go
/// `errors.Cause`) kept when it has one.
pub(crate) fn driver_error_of(error: SqlQueryError) -> DriverError {
    if let Some(cause) = error.driver_cause() {
        return cause.clone();
    }
    let mut mapped = tidb_executor::MysqlError::from_parts(error.code, error.state, error.message);
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
    /// The session's LOCAL temporary tables at the savepoint, for the same
    /// reason [`TxnCtx::local_temporary_at_open`] exists: their rows are
    /// in the transaction membuffer Go truncates back to the checkpoint, so
    /// they roll back with everything else, while the table map does not.
    local_temporary: Vec<(String, String, tidb_executor::KvTable)>,
    /// The Go `TxnCtx.TableDeltaMap` alongside the row checkpoint.
    table_delta: std::collections::HashMap<i64, tidb_stats_handle_usage::TableDelta>,
    /// The session's GLOBAL temporary rows at the savepoint.
    ///
    /// These are transactional in Go with no exception at all: a global
    /// temporary table has NO storage outside the transaction, so every one
    /// of its keys is in the membuffer `RollbackMemDBToCheckpoint` truncates.
    /// The corpus asserts exactly this -- `executor/executor_txn`'s
    /// `TestSavepointWithTemporaryTable` inserts three rows under three
    /// savepoints and rolls back to each in turn.
    global_temporary: std::collections::HashMap<i64, tidb_executor::TemporaryTableTxnData>,
}

/// The session's temporary-table state while it is OFF the catalog, and the
/// two moves that put it on and take it back.
///
/// Go does the same in two pieces that this tier has to do in one, because
/// its infoschema and its store are separate objects and here they are the
/// same object:
///
/// * `temptable.AttachLocalTemporaryTableInfoSchema` wraps the statement's
///   infoschema so `TableByName` finds a LOCAL temporary table before the
///   shared one of the same name;
/// * `temptable.SessionSnapshotInterceptor` routes reads of a temporary
///   table's key range away from TiKV and into the session's own membuffer.
///
/// The local half is a whole table object moved into the catalog and back;
/// the global half is only the ROW STORAGE, because a global temporary
/// table's `TableInfo` is genuinely shared and must stay where it is.
struct TemporaryTableOverlay {
    local: Vec<(String, String, tidb_executor::KvTable)>,
    global: std::collections::HashMap<i64, tidb_executor::TemporaryTableTxnData>,
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
        catalog: &mut Catalog,
        body: impl FnOnce(&mut Catalog) -> Result<T, DriverError>,
    ) -> Result<T, DriverError> {
        catalog.attach_local_temporary_tables(std::mem::take(&mut self.local));
        self.swap_global_storage(catalog);
        let value = body(catalog);
        self.swap_global_storage(catalog);
        self.local = catalog.take_local_temporary_tables();
        value
    }

    /// Exchanges every GLOBAL temporary table's row storage between the
    /// catalog's (always-empty) one and this session's.
    ///
    /// The same call attaches and detaches, because the catalog's own copy
    /// carries no rows in either direction: nothing writes to a global
    /// temporary table except a session that has its own storage swapped in,
    /// so what comes out at attach is empty and what goes back at detach can
    /// be the empty store that came out.
    fn swap_global_storage(&mut self, catalog: &mut Catalog) {
        // Memoized walk (Go answers this from `temptable`'s session map, not
        // by iterating infoschema): a session with no global temporary tables
        // -- every OLTP workload -- pays one epoch compare per call.
        let ids = catalog.global_temporary_table_ids_memo();
        if ids.is_empty() {
            return;
        }
        for (database, name) in ids.to_vec() {
            let Some(table) = catalog.temporary_overlay_table_mut(&database, &name) else {
                continue;
            };
            let id = table.table_id;
            // Go `TxnCtx.TemporaryTables`: the transaction's rows and its
            // own allocator, created at its first touch of the table.
            let incoming = self
                .global
                .remove(&id)
                .unwrap_or_else(|| table.new_temporary_txn_data());
            let outgoing = table.swap_temporary_txn_data(incoming);
            self.global.insert(id, outgoing);
        }
    }
}

/// Runs a statement for a session that does not yet own a temporary-table
/// overlay, returning one when the shared catalog already contains GLOBAL
/// temporary metadata or the statement creates its first temporary table.
///
/// The metadata-version check keeps the ordinary OLTP path at one memoized
/// GLOBAL-table check: row statements cannot create temporary metadata, so
/// they do not need the post-statement catalog sweep.
fn run_discovering_temporary_overlay<T>(
    catalog: &mut Catalog,
    body: impl FnOnce(&mut Catalog) -> Result<T, DriverError>,
) -> (Result<T, DriverError>, Option<TemporaryTableOverlay>) {
    if !catalog.global_temporary_table_ids_memo().is_empty() {
        let mut overlay = TemporaryTableOverlay {
            local: Vec::new(),
            global: std::collections::HashMap::new(),
        };
        let value = overlay.run(catalog, body);
        return (value, Some(overlay));
    }

    let metadata_version = catalog.metadata_version();
    let value = body(catalog);
    if catalog.metadata_version() == metadata_version {
        return (value, None);
    }

    // A metadata-changing statement may have created the session's first
    // LOCAL or GLOBAL temporary table. Detach it exactly as the ordinary
    // overlay path would have done around the statement.
    let mut overlay = TemporaryTableOverlay {
        local: Vec::new(),
        global: std::collections::HashMap::new(),
    };
    overlay.swap_global_storage(catalog);
    overlay.local = catalog.take_local_temporary_tables();
    let discovered = (!overlay.local.is_empty() || !overlay.global.is_empty()).then_some(overlay);
    (value, discovered)
}

/// The image of the catalog a statement started from, restored on ANY exit
/// that is not an explicit disarm -- an `Err` returned by the statement, and
/// a panic unwinding out of it (see [`Session::with_staged_catalog`]).
struct CatalogStage<'a> {
    /// The catalog the statement mutates in place.
    catalog: &'a mut Catalog,
    /// The image to put back, taken away once the statement has succeeded.
    stage: Option<Catalog>,
}

impl Drop for CatalogStage<'_> {
    fn drop(&mut self) {
        if let Some(stage) = self.stage.take() {
            *self.catalog = stage;
        }
    }
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
        self.open_txn_ctx(mode);
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
    fn open_txn_ctx(&mut self, mode: SessionTxnMode) {
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
            savepoints: crate::SavepointStack::default(),
            local_temporary_at_open: self.local_temporary_tables.clone(),
        });
    }

    /// Go `CalculateAsOfTsExpr` (`pkg/sessiontxn/staleread/util.go:41-86`),
    /// over this session's own expression engine.
    ///
    /// Order is Go's exactly: NULL refuses; a DATETIME interpretation is
    /// tried first (through `UNIX_TIMESTAMP`, whose session-zone semantics
    /// are the ported `time.Date`), and only then a raw TSO -- a positive
    /// integer or a string of digits. A raw TSO whose physical half is
    /// before 2013-01-01 refuses with Go's own message.
    pub(crate) fn resolve_as_of_ts(&mut self, expr: &tidb_ast::Expr) -> Result<u64, DriverError> {
        const TSO_LOGICAL_BITS: u32 = 18;
        // 2013-01-01 00:00:00 UTC in milliseconds, Go's `minTSO` bound.
        const MIN_PHYSICAL_MS: u64 = 1_356_998_400_000;
        let value = self.eval_value(expr)?;
        let as_of_error = |cause: &str| DriverError::Txn(TxnErrorKind::AsOf(cause.to_owned()));
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
    pub(crate) fn open_stale_transaction(&mut self, ts: u64) -> Result<(), DriverError> {
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
            savepoints: crate::SavepointStack::default(),
            local_temporary_at_open: self.local_temporary_tables.clone(),
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

    /// Puts back the ROWS of the local temporary tables that already existed
    /// at the point `snapshot` was taken, leaving every other change alone.
    ///
    /// A table created after the snapshot stays (it is not named there), a
    /// table dropped after it stays dropped (nothing matches it), and a table
    /// that was truncated and rebuilt under a new id is left alone for the
    /// same reason -- which is Go's split between the transactional row data
    /// and the non-transactional session table map. See
    /// [`TxnCtx::local_temporary_at_open`].
    fn restore_local_temporary_rows(
        &mut self,
        snapshot: Vec<(String, String, tidb_executor::KvTable)>,
    ) {
        for (database, name, table) in snapshot {
            if let Some(current) = self.local_temporary_tables.iter_mut().find(
                |(current_database, current_name, current_table)| {
                    current_database == &database
                        && current_name == &name
                        && current_table.table_id == table.table_id
                },
            ) {
                current.2 = table;
            }
        }
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
        let Some(mut node) = self.node.take() else {
            return Err(DriverError::unsupported(
                "transaction statements run on the store session",
            ));
        };
        let result = node.control_transaction(self, sql);
        self.node = Some(node);
        result
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
                let ts = self.resolve_as_of_ts(&expr.clone())?;
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
        let stale_ts = match as_of {
            Some(ts) => Some(ts),
            None => match self.vars.use_txn_read_ts() {
                0 => self.external_read_ts()?,
                read_ts => Some(read_ts),
            },
        };
        match stale_ts {
            Some(ts) => self.open_stale_transaction(ts)?,
            None => {
                let mode = self.resolve_begin_txn_mode(begin.mode);
                self.open_txn_ctx(mode);
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
            // The staged writes go with the store transaction. A local
            // temporary table's rows are not in it -- they are in the
            // session -- so they are put back by hand.
            self.restore_local_temporary_rows(ctx.local_temporary_at_open);
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
            return Err(DriverError::Mysql(tidb_executor::MysqlError::unknown(
                "savepoint is not supported in pessimistic transactions when in-place constraint check is disabled",
            )));
        }
        let buffer = txn.savepoint_checkpoint(self)?;
        let local_temporary = self.local_temporary_tables.clone();
        let global_temporary = self.global_temporary_data.clone();
        let table_delta = self.table_delta_savepoint();
        let Some(ctx) = &mut self.txn_ctx else {
            return Ok(());
        };
        ctx.savepoints.add(
            name,
            Savepoint {
                buffer,
                local_temporary,
                global_temporary,
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
            ctx.savepoints.rollback_to(name).map(|savepoint| {
                (
                    savepoint.buffer.clone(),
                    savepoint.local_temporary.clone(),
                    savepoint.global_temporary.clone(),
                    savepoint.table_delta.clone(),
                )
            })
        });
        let Some((buffer, local_temporary, global_temporary, table_delta)) = found else {
            // go appends the statement error into the warning buffer as well
            // (driver_tidb.go:376's error door), so SHOW WARNINGS after the
            // refused ROLLBACK TO carries the same 1305 row.
            self.append_warning(
                crate::WarningLevel::Error,
                1305,
                format!("SAVEPOINT {name} does not exist"),
            );
            return Err(DriverError::SavepointNotExists(name.to_owned()));
        };
        if let Some(buffer) = &buffer {
            txn.rollback_to_checkpoint(buffer);
        }
        self.restore_local_temporary_rows(local_temporary);
        self.global_temporary_data = global_temporary;
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
            self.append_warning(
                crate::WarningLevel::Error,
                1305,
                format!("SAVEPOINT {name} does not exist"),
            );
            return Err(DriverError::SavepointNotExists(name.to_owned()));
        }
        Ok(())
    }

    /// Runs `body` over the catalog the open transaction reads: a stale
    /// transaction's historical schema (Go `TxnCtx.InfoSchema`), otherwise the
    /// session's catalog.
    pub(crate) fn with_txn_catalog<T>(
        &self,
        body: impl FnOnce(&Catalog) -> T,
    ) -> Result<T, DriverError> {
        if let Some((_, read)) = self.txn_ctx.as_ref().and_then(|ctx| ctx.stale.as_ref()) {
            return Ok(body(&read.catalog));
        }
        let catalog = self.lock_catalog()?;
        Ok(body(&catalog))
    }

    /// A stale transaction's historical catalog (Go `TxnCtx.InfoSchema`).
    pub(crate) fn txn_ctx_stale_catalog(&self) -> Option<&Catalog> {
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
    pub(crate) fn lock_catalog(&self) -> Result<MutexGuard<'_, Catalog>, DriverError> {
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
        body: impl FnOnce(&mut Catalog) -> Result<T, DriverError>,
    ) -> Result<T, DriverError> {
        // Between statements a node session's tables are bound to no
        // snapshot; a look at them reads the committed rows.
        if self.node.is_some() {
            let mut node = self.node.take().expect("a node-backed session");
            let result = node.with_read_snapshot(self, |session| {
                session.with_catalog_scope(true, body)
            });
            self.node = Some(node);
            return result?;
        }
        self.with_catalog_scope(true, body)
    }

    /// Go's durable DDL executor uses the shared infoschema, without local
    /// temporary names. Global temporary row storage remains session-owned.
    pub(crate) fn with_persistent_catalog_mut<T>(
        &mut self,
        body: impl FnOnce(&mut Catalog) -> Result<T, DriverError>,
    ) -> Result<T, DriverError> {
        self.with_catalog_scope(false, body)
    }

    /// Select the name scope while retaining the shared storage overlay lifecycle.
    pub(crate) fn with_catalog_scope<T>(
        &mut self,
        include_local: bool,
        body: impl FnOnce(&mut Catalog) -> Result<T, DriverError>,
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
        if (!include_local || self.local_temporary_tables.is_empty())
            && self.global_temporary_data.is_empty()
        {
            let (value, discovered) = if let Some((_, read)) = &mut self.statement_snapshot {
                run_discovering_temporary_overlay(&mut read.catalog, body)
            } else if let Some((_, read)) = self.txn_ctx.as_mut().and_then(|ctx| ctx.stale.as_mut())
            {
                run_discovering_temporary_overlay(&mut read.catalog, body)
            } else {
                let mut catalog = self
                    .catalog
                    .lock()
                    .map_err(|_| DriverError::CatalogPoisoned)?;
                run_discovering_temporary_overlay(&mut catalog, body)
            };
            if let Some(discovered) = discovered {
                if include_local {
                    self.local_temporary_tables = discovered.local;
                } else {
                    debug_assert!(discovered.local.is_empty());
                }
                self.global_temporary_data = discovered.global;
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
            global: std::mem::take(&mut self.global_temporary_data),
        };
        let value = if let Some((_, read)) = &mut self.statement_snapshot {
            overlay.run(&mut read.catalog, body)
        } else if let Some((_, read)) = self.txn_ctx.as_mut().and_then(|ctx| ctx.stale.as_mut()) {
            overlay.run(&mut read.catalog, body)
        } else {
            let mut catalog = self
                .catalog
                .lock()
                .map_err(|_| DriverError::CatalogPoisoned)?;
            overlay.run(&mut catalog, body)
        };
        if include_local {
            self.local_temporary_tables = std::mem::take(&mut overlay.local);
        } else {
            debug_assert!(overlay.local.is_empty());
        }
        self.global_temporary_data = std::mem::take(&mut overlay.global);
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
            self.reset_transaction_scope();
        }
    }

    /// [`Self::start_statement_transaction_scope`] for a door that owns the
    /// transaction itself -- the cluster node, whose explicit transaction is
    /// its own and which knows no transaction continues into this statement.
    pub fn reset_transaction_scope(&mut self) {
        self.staged_writes = std::sync::Arc::default();
        self.discard_global_temporary_rows();
    }

    /// Drops every row this session has written to a GLOBAL temporary table.
    ///
    /// See the call site in `dispatch` for why the transaction boundary is
    /// the only place this happens.
    pub(crate) fn discard_global_temporary_rows(&mut self) {
        self.global_temporary_data.clear();
    }

    /// Runs one DML statement over a STAGE of the catalog this statement sees,
    /// so a statement that fails partway leaves the tables as it found them.
    ///
    /// This is Go's statement-level rollback. A statement opens a staging
    /// handle on the transaction's membuffer (`pkg/kv/union_store.go`:
    /// `MemBuffer.Staging()`), writes into it, and
    /// `pkg/executor/adapter.go` chooses between
    /// `pkg/session/session.go`'s `StmtCommit` -- `Release()`, folding the
    /// stage into the transaction -- and `StmtRollback` -- `Cleanup()`,
    /// dropping it. The transaction itself is untouched either way, which is
    /// why a failed statement inside `BEGIN` discards only its own writes and
    /// the statements around it survive to `COMMIT`.
    ///
    /// The stage here is an image of the catalog rather than an undo log,
    /// because this tier's tables ARE the buffer: `Catalog::clone` deep-copies
    /// the in-process bytes (`MemTableStorage::clone_box`). Restoring the
    /// image is the same observable effect as `Cleanup()`.
    ///
    /// # What the image does NOT undo
    ///
    /// The restore takes back exactly what `TableStorage::clone_box` copied by
    /// VALUE. `MemTableStorage` copies its bytes, so the rows come back. A
    /// storage whose `clone_box` clones a shared HANDLE does not: the image
    /// and the original write into the same place, so a failed statement's
    /// rows survive this restore. `tidb_executor::cluster_storage` is exactly
    /// that -- its `MutationBuffer` and snapshot are `Arc`s shared by every
    /// table of the session -- so on the cluster path the guard is NOT this
    /// function. It is the statement savepoint the convergence node takes over
    /// the buffer itself (`tidb_server`'s `ClusterServerSession::with_statement`:
    /// `MutationBuffer::staged()` before the statement, `restore()` on its
    /// error arm), which is the same `Staging()`/`Cleanup()` pair one tier
    /// down. Any future front end that drives a `Session` over cluster storage
    /// must bring such a savepoint with it; this restore alone will not roll
    /// its writes back.
    ///
    /// The `_tidb_rowid` a row without a clustered handle gets is NOT undone
    /// either, and for the same reason: Go allocates it from the very counter
    /// the AUTO_INCREMENT column uses (`autoid.NewAllocatorsFromTblInfo`
    /// builds one `RowIDAllocType` allocator for both), so a handle a failed
    /// statement consumed stays consumed.
    ///
    /// AUTO_INCREMENT deliberately survives the restore: Go allocates ids
    /// outside transaction semantics and never returns a consumed one, and
    /// `KvTable`'s `AutoIdAllocator` is a SHARED cell that a catalog copy
    /// keeps pointing at -- so the burn is retained with no exclusion rule
    /// here (captured: a failed one-row insert into an `AUTO_INCREMENT` table
    /// stores nothing and the next successful insert skips the burned id).
    ///
    /// Making this the one door every mutating statement goes through is the
    /// point: the restore lives in a guard's `Drop`, so no exit of `body` --
    /// and no DML arm added later -- can forget it.
    ///
    /// The guard rather than an error arm is what makes a PANIC take the same
    /// path as an `Err`. An `inspect_err` restore is skipped entirely when
    /// `body` unwinds, so a caught panic would leave a HALF-APPLIED statement
    /// in the in-process rows.
    pub(crate) fn with_staged_catalog<T>(
        &mut self,
        body: impl FnOnce(&mut Catalog) -> Result<T, DriverError>,
    ) -> Result<T, DriverError> {
        // Store sessions keep statement writes in the store session's
        // `MutationBuffer`/`BufferCheckpoint`. Their catalog contains only
        // KV-backed tables, so taking a deep image here duplicates schema
        // maps on every prepared DML without adding a rollback capability.
        // In-process rows (temporary tables) still need the image.
        self.with_catalog_mut(|catalog| {
            if catalog.all_tables_have_external_statement_rollback() {
                return body(catalog);
            }
            let mut guard = CatalogStage {
                stage: Some(catalog.clone()),
                catalog,
            };
            // `?` and an unwind both drop the guard while it is still armed;
            // only reaching the disarm below keeps the statement's writes.
            let value = body(guard.catalog)?;
            guard.stage = None;
            Ok(value)
        })
    }

    /// Runs a DML statement over its target table without taking a catalog
    /// image when the target is cluster-backed. Cluster sessions keep row
    /// mutations in the outer statement savepoint, while standalone and
    /// pipeline sessions (whose `KvTable`s use `MemTableStorage`) still need
    /// the image even though their table entry is not a `MemTable`.
    pub(crate) fn with_staged_catalog_for_table<T>(
        &mut self,
        database: &str,
        table_name: &str,
        body: impl FnOnce(&mut Catalog) -> Result<T, DriverError>,
    ) -> Result<T, DriverError> {
        self.with_catalog_mut(|catalog| {
            if catalog.table_has_external_statement_rollback(database, table_name) {
                return body(catalog);
            }
            let mut guard = CatalogStage {
                stage: Some(catalog.clone()),
                catalog,
            };
            let value = body(guard.catalog)?;
            guard.stage = None;
            Ok(value)
        })
    }

    /// Path-resolving wrapper for [`Self::with_staged_catalog_for_table`].
    /// Invalid/empty paths keep the original staged behavior so the executor
    /// remains responsible for returning the canonical schema error.
    pub(crate) fn with_staged_catalog_for_path<T>(
        &mut self,
        path: &[String],
        current_database: &str,
        body: impl FnOnce(&mut Catalog) -> Result<T, DriverError>,
    ) -> Result<T, DriverError> {
        let target = match path {
            [table] if !current_database.is_empty() => Some((current_database, table.as_str())),
            [database, table] => Some((database.as_str(), table.as_str())),
            _ => None,
        };
        match target {
            Some((database, table_name)) => {
                self.with_staged_catalog_for_table(database, table_name, body)
            }
            None => self.with_staged_catalog(body),
        }
    }

    /// Go `serverStatus2Str` over this session's status bits: the `State`
    /// column of `SHOW PROCESSLIST`.
    ///
    /// The typed autocommit state and transaction presence are the two status
    /// bits this tier owns, in the same order as Go's `ascServerStatus`.
    #[must_use]
    pub fn status_text(&self) -> String {
        match (self.txn_ctx.is_some(), self.is_autocommit()) {
            (true, true) => "in transaction; autocommit".to_owned(),
            (true, false) => "in transaction".to_owned(),
            (false, true) => "autocommit".to_owned(),
            (false, false) => String::new(),
        }
    }
}

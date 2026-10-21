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

//! Transaction control: the staged-catalog transaction a `BEGIN` opens, the
//! savepoint stack inside it, and the two doors every statement reaches the
//! catalog through.
//!
//! This is the seam of Go's `sessiontxn` provider plus the `kv.MemBuffer`
//! staging underneath it: [`Session::with_catalog_mut`] is the read-your-own-
//! writes door and [`Session::with_staged_catalog`] the statement-level
//! rollback one.

use std::sync::MutexGuard;

use tidb_ast::{SessionStmt, Stmt};
use tidb_datatype::Datum;
use tidb_executor::{Catalog, DriverError};

use crate::{statement_kind_of, Session, SessionTxnMode, TxnErrorKind};
use crate::{txn_mode_for_begin, PESSIMISTIC_TXN_MODE};
use tidb_util::stringutil::go_to_lower;

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

/// An open transaction's state.
///
/// Go stages a transaction's writes in a `kv.MemBuffer` over a read snapshot
/// and flushes them at commit; this stages them in a private copy of the
/// catalog taken at `BEGIN`, so the session reads its own writes while its
/// peers see nothing until commit.
///
/// `base_version` is the shared catalog's mutation counter at `BEGIN`. If it
/// moved by commit time, someone else wrote, and the commit replays this
/// transaction onto their work ([`Catalog::rebase_transaction`]), refusing it
/// only where Go's TiKV conflict check or schema validator would.
pub(crate) struct Transaction {
    pub(crate) working: Catalog,
    /// The catalog as `BEGIN` found it: Go's `GetSnapshot(TxnCtx.StartTS)`,
    /// the committed state the transaction reads beneath its own writes.
    /// The in-process store writes through into `working`, so a reader that
    /// must skip the membuffer (`TABLESAMPLE`) reads this image instead.
    pub(crate) read_snapshot: std::sync::Arc<Catalog>,
    /// Whether a locking read (`SELECT ... FOR UPDATE/SHARE`) ran in a
    /// transaction whose rows live outside its image (a cluster session's,
    /// whose owner takes the real locks): COMMIT keeps the whole-catalog
    /// check for it.
    pub(crate) locking_read: bool,
    /// Whether this transaction's rows live in its catalog image -- the
    /// in-process store -- so its statements read, lock and stage them here
    /// rather than through an outer owner's buffer and TiKV.
    writes_in_image: bool,
    /// Go's `Op_Lock` mutations of an optimistic transaction: the keys its
    /// locking reads selected. COMMIT prewrites them even when nothing else
    /// was written, and conflicts on one a peer committed after `BEGIN`.
    lock_keys: Vec<Vec<u8>>,
    /// This transaction's claim on the store's pessimistic locks -- a
    /// pessimistic transaction's `Op_Lock` mutations; dropping the
    /// transaction releases them. A stale read takes none.
    lock_guard: Option<tidb_executor::PessimisticLockGuard>,
    base_version: u64,
    /// The transaction's start timestamp -- Go `TxnCtx.StartTS`, which
    /// `@@tidb_current_ts` reports. For a stale transaction it IS the as-of
    /// timestamp, which is the corpus's own assertion.
    start_ts: u64,
    /// `Some(ts)` for `START TRANSACTION READ ONLY AS OF TIMESTAMP`: the
    /// working copy is a historical snapshot and COMMIT publishes nothing.
    stale_read_ts: Option<u64>,
    _historical_hold: Option<std::sync::Arc<dyn Send + Sync>>,
    /// The mode this transaction opened in, resolved from the `BEGIN` keyword
    /// and `@@tidb_txn_mode` exactly as Go resolves it.
    ///
    /// This tier's store is a catalog behind a mutex, not TiKV, so there is no
    /// lock to take and the mode changes nothing about how a statement runs
    /// here. It is still resolved and kept, because it is what the client
    /// asked for and what the real-TiKV tier consumes.
    mode: SessionTxnMode,
    /// Go's TxnCtx isolation: a later SET SESSION affects the next
    /// transaction, not the missing-key lock policy of this transaction.
    read_committed: bool,
    /// The transaction's savepoint stack, oldest first -- Go's
    /// `TxnCtx.Savepoints` (`pkg/sessionctx/variable/session.go`).
    savepoints: Vec<Savepoint>,
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

impl Transaction {
    pub(crate) const fn is_stale_read(&self) -> bool {
        self.stale_read_ts.is_some()
    }

    /// Opens a transaction over `catalog`: the ONE place a [`Transaction`] is
    /// built.
    ///
    /// Both openings -- the explicit `BEGIN` and the lazy one `autocommit = 0`
    /// performs for the first statement that touches data -- come through here,
    /// so a field added to the struct has exactly one place it can be forgotten
    /// and that place will not compile without it.
    ///
    /// A transaction opened lazily therefore carries the same empty savepoint
    /// stack an explicit BEGIN does, which is what Go does: it makes no
    /// distinction between the two once `InTxn()` holds, so SAVEPOINT works in
    /// either.
    fn open(
        catalog: &Catalog,
        mode: SessionTxnMode,
        read_committed: bool,
        local_temporary_at_open: Vec<(String, String, tidb_executor::KvTable)>,
    ) -> Self {
        let start_ts = catalog.allocate_tso();
        let working = catalog.clone();
        Transaction {
            working,
            read_snapshot: std::sync::Arc::new(catalog.clone()),
            locking_read: false,
            writes_in_image: !catalog.any_table_has_external_statement_rollback(),
            lock_keys: Vec::new(),
            lock_guard: Some(tidb_executor::PessimisticLockGuard::new(
                catalog.pessimistic_locks(),
                start_ts,
            )),
            base_version: catalog.version(),
            start_ts,
            stale_read_ts: None,
            _historical_hold: None,
            mode,
            read_committed,
            savepoints: Vec::new(),
            local_temporary_at_open,
        }
    }
}

/// One statement's transactional read and lock step inside an in-process
/// transaction -- Go `ExecStmt`'s pessimistic statement around its executor
/// (`handlePessimisticDML`, `runPessimisticSelectForUpdate`).
pub(crate) struct TransactionStatement {
    /// The committed image the statement read at its `for_update_ts`, for a
    /// statement that reads the latest committed rows; `None` for one that
    /// reads the transaction's snapshot.
    view_base: Option<Catalog>,
    /// The transaction's image before the statement, set aside while the
    /// statement runs over its view.
    saved: Option<Catalog>,
    /// Whether the view IS the transaction's image -- nothing committed since
    /// `BEGIN` -- so the statement's writes need no folding back.
    view_is_image: bool,
    /// Whether the transaction is pessimistic, so the statement locks what
    /// it read for writing.
    pessimistic: bool,
    /// Whether the statement is a locking read, whose rows are held back
    /// until their keys are locked.
    locking_read: bool,
}

/// How a statement's lock step ended.
pub(crate) enum StatementFinish {
    /// The statement's outcome, locks taken.
    Done(Result<crate::record_set::PendingExecution, DriverError>),
    /// A key the statement read was committed by another transaction before
    /// the lock could be taken: Go's pessimistic write conflict, which reruns
    /// the statement at a newer `for_update_ts`.
    Retry,
}

/// One entry of a transaction's savepoint stack.
///
/// Go records a `tikv.MemDBCheckpoint` -- a position in the transaction's
/// membuffer that `RollbackMemDBToCheckpoint` truncates back to. This tier's
/// transaction stages its writes in a private catalog copy rather than a
/// membuffer, so the mark is an IMAGE of that copy, restored by assignment.
/// It is the same primitive [`Session::with_staged_catalog`] already uses for
/// statement-level rollback, just held under a name for longer than one
/// statement.
pub(crate) struct Savepoint {
    /// The name, lowercased: Go's `AddSavepoint`/`RollbackToSavepoint` match
    /// `strings.ToLower(name)`, so `SAVEPOINT SP1` and `ROLLBACK TO sp1` are
    /// the same savepoint.
    name: String,
    /// The transaction's working catalog as of this savepoint.
    ///
    /// Kept even after a `ROLLBACK TO` restores from it, because Go's
    /// `RollbackToSavepoint` truncates the stack to `[:idx+1]` -- the named
    /// savepoint SURVIVES its own rollback and can be rolled back to again.
    image: Catalog,
    /// The session's LOCAL temporary tables at the savepoint, for the same
    /// reason [`Transaction::local_temporary_at_open`] exists: their rows are
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

    /// Go's lazy transaction start: with autocommit OFF, a statement that
    /// touches data runs INSIDE a transaction the session opens for it, so a
    /// later `ROLLBACK` can discard it. `BEGIN` still opens one explicitly;
    /// this only covers the statements that would otherwise have none.
    pub(crate) fn begin_implicit_transaction(&mut self) -> Result<(), DriverError> {
        if self.txn.is_some() || self.is_autocommit() {
            return Ok(());
        }
        let mode = self.resolve_begin_txn_mode(tidb_ast::TransactionMode::Default);
        self.open_transaction(mode)
    }

    /// Whether a transaction is open (the wire's `SERVER_STATUS_IN_TRANS`).
    #[must_use]
    pub fn in_transaction(&self) -> bool {
        self.txn.is_some()
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

    /// The mode the open transaction runs in, if one is open.
    ///
    /// `BEGIN PESSIMISTIC` and `BEGIN OPTIMISTIC` are accepted here and their
    /// mode is reported faithfully, but this tier takes no row locks in either
    /// mode: its store is one shared catalog behind a mutex, so concurrent
    /// sessions already serialize and a committing session that lost the race
    /// is refused with a write conflict. `SELECT ... FOR UPDATE` returns the
    /// same rows it would under a real pessimistic lock; what is missing is
    /// the lock, not the result (see [`Self::check_query_clauses`]).
    #[must_use]
    pub fn txn_mode(&self) -> Option<SessionTxnMode> {
        self.txn.as_ref().map(|txn| txn.mode)
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
        self.txn.as_ref().map_or_else(
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
    pub fn statement_txn_mode(&self) -> SessionTxnMode {
        self.txn_mode()
            .unwrap_or_else(|| self.resolve_begin_txn_mode(tidb_ast::TransactionMode::Default))
    }

    /// Installs a transaction over the shared catalog as it stands now.
    ///
    /// The lock is taken and released here rather than held across the
    /// installation, because [`Transaction::open`] copies the catalog it is
    /// given and needs nothing from it afterwards.
    fn open_transaction(&mut self, mode: SessionTxnMode) -> Result<(), DriverError> {
        let local_temporary_at_open = self.local_temporary_tables.clone();
        // Go `txnManager.newProviderWithRequest` picks the provider from
        // `IsolationLevelForNewTxn`, which honors `SET TRANSACTION`.
        let read_committed = self
            .vars
            .isolation_level_for_new_txn(self.in_transaction())
            .eq_ignore_ascii_case("READ-COMMITTED");
        let txn = Transaction::open(
            &*self.lock_catalog()?,
            mode,
            read_committed,
            local_temporary_at_open,
        );
        // A fresh transaction gets a fresh, empty staged-write tracker --
        // Go's own membuffer is empty the moment a transaction activates.
        // Direct protocol BEGIN and implicit activation both reach this
        // constructor without necessarily running SQL dispatch, so this is
        // the one door that must reset it, matching `Transaction::open`.
        self.staged_writes = std::sync::Arc::default();
        // Go publishes `TxnCtx.StartTS` the moment the transaction
        // activates; `@@tidb_current_ts` reads exactly that.
        self.current_tso().publish(txn.start_ts);
        if let Some(process) = &self.process {
            process
                .registry()
                .transaction_started(process.id(), txn.start_ts);
        }
        self.txn = Some(txn);
        Ok(())
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
        self.restricted_sql = restricted;
    }

    /// Effective historical timestamp of the active statement or stale transaction.
    pub fn historical_read_ts(&self) -> Option<u64> {
        self.statement_snapshot
            .as_ref()
            .map(|(ts, _)| *ts)
            .or_else(|| self.txn.as_ref().and_then(|txn| txn.stale_read_ts))
    }

    /// `START TRANSACTION READ ONLY AS OF TIMESTAMP <resolved ts>`: the Go
    /// stale transaction (`StalenessTxnContextProvider`), whose `StartTS` IS
    /// the as-of timestamp and whose reads all see the store as of it.
    pub(crate) fn open_stale_transaction(&mut self, ts: u64) -> Result<(), DriverError> {
        let mut historical_hold = None;
        let snapshot = if let Some(provider) = &self.historical_read_provider {
            let schema = self
                .snapshot_schema
                .as_ref()
                .filter(|(selected, _)| *selected == ts)
                .map(|(_, catalog)| catalog);
            let read = provider(ts, self.current_resource_group(), schema)?;
            historical_hold = Some(read.timestamp_hold);
            read.catalog
        } else {
            let shared = self.lock_catalog()?;
            shared.state_as_of(ts).ok_or_else(|| {
                // No retained commit is that old. Go's analogue is the GC
                // barrier; this tier's ring is its retention, and answering
                // from the PRESENT under a historical name would be the one
                // undetectable wrong answer.
                DriverError::Txn(TxnErrorKind::AsOf(
                    "the requested timestamp precedes this store's retained history".to_owned(),
                ))
            })?
        };
        // A historical read also has an empty transaction membuffer.
        self.staged_writes = std::sync::Arc::default();
        let local_temporary_at_open = self.local_temporary_tables.clone();
        self.txn = Some(Transaction {
            base_version: snapshot.version(),
            read_snapshot: std::sync::Arc::new(snapshot.clone()),
            locking_read: false,
            writes_in_image: false,
            lock_keys: Vec::new(),
            lock_guard: None,
            working: snapshot,
            start_ts: ts,
            stale_read_ts: Some(ts),
            _historical_hold: historical_hold,
            mode: SessionTxnMode::Optimistic,
            read_committed: false,
            savepoints: Vec::new(),
            local_temporary_at_open,
        });
        self.current_tso().publish(ts);
        if self.historical_read_provider.is_some() {
            if let Some(observer) = self.transaction_observer() {
                observer.activated(ts);
            }
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
        if let Some(txn) = self.txn.take() {
            self.set_last_txn_info_started(txn.start_ts);
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
    /// [`Transaction::local_temporary_at_open`].
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

    /// Applies `BEGIN`/`START TRANSACTION`, `COMMIT`, or `ROLLBACK`.
    ///
    /// Returns `Some(in_transaction)` for those statements and `None` for
    /// anything else, so a caller can answer with an OK packet carrying the
    /// right status flag without re-parsing.
    ///
    /// Go's `BEGIN` inside an open transaction implicitly commits the current
    /// one before starting the new one, which this reproduces. `COMMIT` and
    /// `ROLLBACK` with no open transaction are no-ops, as in MySQL.
    pub fn control_transaction(&mut self, sql: &str) -> Result<Option<bool>, DriverError> {
        let stmt = self.parse(sql)?;
        self.control_transaction_stmt(&stmt)
    }

    /// Go `SimpleExec.executeBegin`'s first check, for a caller that runs
    /// the implicit commit itself: a `READ ONLY` clause under
    /// `tidb_enable_noop_functions = OFF` is refused BEFORE `NewTxn` commits
    /// the open transaction. The WARN mode's warning is left to
    /// [`Self::control_transaction_stmt`], so it is appended once.
    pub fn refuse_begin_noop_clause(&mut self, stmt: &Stmt) -> Result<(), DriverError> {
        let Stmt::Session(session_stmt) = stmt else {
            return Ok(());
        };
        let SessionStmt::Begin(begin) = &**session_stmt else {
            return Ok(());
        };
        if begin.read_only
            && begin.as_of.is_none()
            && self.noop_funcs_mode(false) == crate::noop::NoopFuncsMode::Off
        {
            return self.gate_noop_clause("READ ONLY", false);
        }
        Ok(())
    }

    /// [`Self::control_transaction`] over a statement this session already
    /// parsed. The text form parses and delegates here, so both callers ask
    /// the same question of the same code.
    pub fn control_transaction_stmt(&mut self, stmt: &Stmt) -> Result<Option<bool>, DriverError> {
        let Stmt::Session(session_stmt) = stmt else {
            return Ok(None);
        };
        match &**session_stmt {
            SessionStmt::Begin(begin) => {
                // Go `SimpleExec.executeBegin`: `START TRANSACTION READ ONLY`
                // is a no-op clause -- TiDB does not actually stop writes --
                // so it goes through `tidb_enable_noop_functions`, refusing
                // with 1235 at the OFF default. `AS OF TIMESTAMP` exempts it,
                // because that spelling names a real historical read rather
                // than the bare read-only claim.
                if begin.read_only && begin.as_of.is_none() {
                    self.gate_noop_clause("READ ONLY", false)?;
                }
                // `START TRANSACTION READ ONLY AS OF TIMESTAMP <expr>` opens
                // the transaction AT that timestamp, so every statement in it
                // reads history through the installed store/schema owner.
                if let Some(expr) = &begin.as_of {
                    // Go `StalenessTxnContextProvider`: the expression
                    // resolves through `CalculateAsOfTsExpr`'s rules and the
                    // transaction opens AT that timestamp -- its `StartTS`,
                    // which `@@tidb_current_ts` reports.
                    let ts = self.resolve_as_of_ts(&expr.clone())?;
                    if self.vars.txn_read_ts() != 0 {
                        if let Some(provider) = &self.snapshot_schema_provider {
                            provider(ts, self.current_resource_group(), false)?;
                        }
                        return Err(DriverError::unsupported("start transaction read only as of is forbidden after set transaction read only as of"));
                    }
                    self.statement_began_txn = true;
                    if self.txn.is_some() {
                        self.commit()?;
                    }
                    self.open_stale_transaction(ts)?;
                    return Ok(Some(true));
                }
                // Go's `NewTxn` gives the transaction a fresh `TxnCtx`, so the
                // statement's own (`origTxnCtx`) is not the one it opens.
                self.statement_began_txn = true;
                // An open transaction is committed first (Go's implicit commit).
                if self.txn.is_some() {
                    self.commit()?;
                }
                let read_ts = self.vars.use_txn_read_ts();
                if read_ts != 0 {
                    self.open_stale_transaction(read_ts)?;
                    return Ok(Some(true));
                }
                if let Some(ts) = self.external_read_ts()? {
                    self.open_stale_transaction(ts)?;
                    return Ok(Some(true));
                }
                let mode = self.resolve_begin_txn_mode(begin.mode);
                self.open_transaction(mode)?;
                Ok(Some(true))
            }
            SessionStmt::Commit(_) => {
                self.commit()?;
                Ok(Some(false))
            }
            SessionStmt::Rollback { savepoint, .. } => {
                if let Some(name) = savepoint {
                    // ROLLBACK TO does NOT end the transaction: it restores
                    // the data and leaves everything else running.
                    self.rollback_to_savepoint(name)?;
                    return Ok(Some(true));
                }
                self.cleanup_txn_read_ts();
                if self.txn.is_some() {
                    self.record_statement_ended_txn();
                }
                // Dropping the staged copy discards every staged write.
                // A local temporary table's rows are not in that copy -- they
                // are in the session -- so they are put back by hand.
                if let Some(txn) = self.txn.take() {
                    self.clear_table_delta();
                    if let Some(process) = &self.process {
                        process
                            .registry()
                            .transaction_state(process.id(), "RollingBack");
                    }
                    // Go `setLastTxnInfoBeforeTxnEnd`: an activated
                    // transaction that ends without a commit leaves the
                    // start-only record.
                    self.set_last_txn_info_started(txn.start_ts);
                    self.current_tso().clear();
                    self.restore_local_temporary_rows(txn.local_temporary_at_open);
                    if let Some(process) = &self.process {
                        process.registry().transaction_finished(process.id());
                    }
                }
                Ok(Some(false))
            }
            SessionStmt::Savepoint(name) => {
                self.set_savepoint(name)?;
                Ok(Some(self.txn.is_some()))
            }
            SessionStmt::ReleaseSavepoint(name) => {
                self.release_savepoint(name)?;
                Ok(Some(true))
            }
            _ => Ok(None),
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
    fn set_savepoint(&mut self, name: &str) -> Result<(), DriverError> {
        // Go's `Txn(true)`: with autocommit OFF this is the statement that
        // opens the pending transaction.
        self.begin_implicit_transaction()?;
        if self.in_transaction()
            && self.statement_txn_mode().is_pessimistic()
            && !self.pessimistic_constraint_check_in_place()
        {
            return Err(DriverError::Mysql(tidb_executor::MysqlError::new(1105, "savepoint is not supported in pessimistic transactions when in-place constraint check is disabled")));
        }
        let local_temporary = self.local_temporary_tables.clone();
        let global_temporary = self.global_temporary_data.clone();
        let table_delta = self.table_delta_savepoint();
        let Some(txn) = &mut self.txn else {
            return Ok(());
        };
        let name = go_to_lower(name);
        let image = txn.working.clone();
        txn.savepoints.retain(|savepoint| savepoint.name != name);
        txn.savepoints.push(Savepoint {
            name,
            image,
            local_temporary,
            global_temporary,
            table_delta,
        });
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
    fn rollback_to_savepoint(&mut self, name: &str) -> Result<(), DriverError> {
        let lowered = go_to_lower(name);
        // go appends the statement error into the warning buffer as well
        // (driver_tidb.go:376's error door), so SHOW WARNINGS after the
        // refused ROLLBACK TO carries the same 1305 row.
        let txn = match self.txn.as_mut() {
            Some(txn) => txn,
            None => {
                self.append_warning(
                    crate::WarningLevel::Error,
                    1305,
                    format!("SAVEPOINT {name} does not exist"),
                );
                return Err(DriverError::SavepointNotExists(name.to_owned()));
            }
        };
        let index = match txn
            .savepoints
            .iter()
            .position(|savepoint| savepoint.name == lowered)
        {
            Some(index) => index,
            None => {
                drop(txn);
                self.append_warning(
                    crate::WarningLevel::Error,
                    1305,
                    format!("SAVEPOINT {name} does not exist"),
                );
                return Err(DriverError::SavepointNotExists(name.to_owned()));
            }
        };
        txn.working = txn.savepoints[index].image.clone();
        let local_temporary = txn.savepoints[index].local_temporary.clone();
        let global_temporary = txn.savepoints[index].global_temporary.clone();
        let table_delta = txn.savepoints[index].table_delta.clone();
        txn.savepoints.truncate(index + 1);
        self.restore_local_temporary_rows(local_temporary);
        self.global_temporary_data = global_temporary;
        self.restore_table_delta_savepoint(table_delta);
        Ok(())
    }

    /// `RELEASE SAVEPOINT name` -- Go `SimpleExec.executeReleaseSavepoint`
    /// plus `TxnCtx.ReleaseSavepoint`: drops the named savepoint AND every
    /// savepoint taken after it (`Savepoints[:i]`), touching no data.
    fn release_savepoint(&mut self, name: &str) -> Result<(), DriverError> {
        let lowered = go_to_lower(name);
        let index = match self.txn.as_ref().and_then(|txn| {
            txn.savepoints
                .iter()
                .position(|savepoint| savepoint.name == lowered)
        }) {
            Some(index) => index,
            None => {
                self.append_warning(
                    crate::WarningLevel::Error,
                    1305,
                    format!("SAVEPOINT {name} does not exist"),
                );
                return Err(DriverError::SavepointNotExists(name.to_owned()));
            }
        };
        if let Some(txn) = &mut self.txn {
            txn.savepoints.truncate(index);
        }
        Ok(())
    }

    /// Publishes the open transaction's staged writes, or refuses when the
    /// shared catalog moved under it. A refused commit ends the transaction,
    /// as an aborted Go transaction does -- the staged writes are gone either
    /// way, so the caller must retry the statements, not just the COMMIT.
    pub(crate) fn commit(&mut self) -> Result<(), DriverError> {
        self.set_txn_isolation_level_one_shot_state_for_next_txn();
        if self.txn.is_some() {
            self.record_statement_ended_txn();
        }
        self.cleanup_txn_read_ts();
        let Some(txn) = self.txn.take() else {
            // COMMIT with no open transaction is a no-op, as in MySQL.
            return Ok(());
        };
        if let Some(process) = &self.process {
            process
                .registry()
                .transaction_state(process.id(), "Committing");
        }
        if txn.stale_read_ts.is_some() {
            // Go's stale transaction is read-only by construction; its
            // COMMIT publishes nothing, and `setLastTxnInfoBeforeTxnEnd`
            // leaves the start-only record (`pkg/session/session.go:1056`).
            self.set_last_txn_info_started(txn.start_ts);
            self.current_tso().clear();
            if let Some(process) = &self.process {
                process.registry().transaction_finished(process.id());
            }
            return Ok(());
        }
        if txn.working.version() == txn.base_version
            && txn.lock_keys.is_empty()
            && !txn.locking_read
            && !txn
                .lock_guard
                .as_ref()
                .is_some_and(|guard| guard.locks().holds_any(guard.owner()))
        {
            // client-go `KVTxn.Commit` returns before prewrite when the
            // membuffer holds no mutation (`committer.mutations.Len() == 0`),
            // so a transaction that wrote nothing commits whatever its peers
            // committed meanwhile, and `LastTxnInfo` keeps the start-only
            // record `setLastTxnInfoBeforeTxnEnd` wrote. A working copy whose
            // mutation counter never moved is that empty membuffer.
            self.set_last_txn_info_started(txn.start_ts);
            self.current_tso().clear();
            if let Some(process) = &self.process {
                process.registry().transaction_finished(process.id());
            }
            return Ok(());
        }
        let mut shared = match self.lock_catalog() {
            Ok(shared) => shared,
            Err(error) => {
                self.clear_table_delta();
                if let Some(process) = &self.process {
                    process.registry().transaction_finished(process.id());
                }
                return Err(error);
            }
        };
        // Go's prewrite rejects an `Op_Insert` mutation whose key the
        // committed rows hold: a key an INSERT presumed absent (an optimistic
        // transaction under `tidb_constraint_check_in_place = OFF`) reports
        // its duplicate here, at COMMIT, and the transaction is gone. A key
        // whose working bytes equal the committed ones was rolled back with
        // its statement.
        for presumed in self.staged_writes.take_presumed() {
            let Some(committed) = shared.stored_raw_value(presumed.table_id, &presumed.key) else {
                continue;
            };
            if txn.working.stored_raw_value(presumed.table_id, &presumed.key) == Some(committed) {
                continue;
            }
            drop(shared);
            self.clear_table_delta();
            if let Some(process) = &self.process {
                process.registry().transaction_finished(process.id());
            }
            return Err(DriverError::DuplicateEntry {
                value: presumed.value,
                key: presumed.index,
            });
        }
        let moved = shared.version() != txn.base_version;
        let rebased = if moved
            && !txn.mode.is_pessimistic()
            && !shared.raw_values_match(&txn.read_snapshot, &txn.lock_keys)
        {
            // Go prewrites an optimistic transaction's `Op_Lock` keys like
            // its writes: one a peer committed after `BEGIN` is a conflict.
            Err(tidb_executor::RebaseRefusal::WriteConflict)
        } else if !moved {
            *shared = txn.working;
            Ok(())
        } else if txn.locking_read {
            // A locking read whose keys an outer owner holds needs the
            // unmoved image.
            Err(tidb_executor::RebaseRefusal::NotRebasable)
        } else {
            // Go's commit fails only on a key another transaction committed
            // meanwhile, or on a touched table whose definition changed; a
            // commit to an unrelated table is no conflict, and a key this
            // transaction's pessimistic lock covers was read after any
            // commit that preceded the lock.
            let held = |key: &[u8]| {
                txn.lock_guard
                    .as_ref()
                    .is_some_and(|guard| guard.locks().holds(guard.owner(), key))
            };
            shared.rebase_transaction(&txn.read_snapshot, &txn.working, &self.staged_writes, &held)
        };
        if let Err(refusal) = rebased {
            drop(shared);
            self.clear_table_delta();
            if let Some(process) = &self.process {
                process.registry().transaction_finished(process.id());
            }
            return Err(match refusal {
                tidb_executor::RebaseRefusal::SchemaChanged => {
                    let error = tidb_domain::schema_checker::SchemaCheckError::InfoSchemaChanged;
                    DriverError::DdlCoded {
                        errno: error.code(),
                        message: error.message(),
                    }
                }
                _ => DriverError::Txn(TxnErrorKind::WriteConflict),
            });
        }
        // The commit is durable in this store the moment the shared catalog
        // holds it; the history snapshot and Go's `LastTxnInfo` record
        // both.
        let commit_ts = shared.allocate_tso();
        shared.record_commit(commit_ts);
        drop(shared);
        if self.stats_collector.is_none() {
            self.publish_table_delta();
        }
        self.set_last_txn_info_committed(txn.start_ts, commit_ts);
        self.current_tso().clear();
        if let Some(process) = &self.process {
            process.registry().transaction_finished(process.id());
        }
        Ok(())
    }

    /// Borrows the shared catalog for one statement. The lock is held for the
    /// statement's duration only, which is the granularity Go's schema state
    /// is consumed at.
    pub(crate) fn lock_catalog(&self) -> Result<MutexGuard<'_, Catalog>, DriverError> {
        self.catalog
            .lock()
            .map_err(|_| DriverError::CatalogPoisoned)
    }

    /// Runs `body` over the catalog this statement sees: the transaction's
    /// staged copy when one is open (so it reads its own writes), otherwise
    /// the shared catalog directly (autocommit).
    pub(crate) fn with_catalog_mut<T>(
        &mut self,
        body: impl FnOnce(&mut Catalog) -> Result<T, DriverError>,
    ) -> Result<T, DriverError> {
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
            } else {
                match &mut self.txn {
                    Some(txn) => run_discovering_temporary_overlay(&mut txn.working, body),
                    None => {
                        let mut catalog = self
                            .catalog
                            .lock()
                            .map_err(|_| DriverError::CatalogPoisoned)?;
                        run_discovering_temporary_overlay(&mut catalog, body)
                    }
                }
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
        } else {
            match &mut self.txn {
                Some(txn) => overlay.run(&mut txn.working, body),
                None => {
                    let mut catalog = self
                        .catalog
                        .lock()
                        .map_err(|_| DriverError::CatalogPoisoned)?;
                    overlay.run(&mut catalog, body)
                }
            }
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
    /// `body` unwinds, and inside `BEGIN` the catalog being mutated is the
    /// transaction's own working copy, held behind no lock -- so a caught
    /// panic would leave a HALF-APPLIED statement for `COMMIT` to publish.
    pub(crate) fn with_staged_catalog<T>(
        &mut self,
        body: impl FnOnce(&mut Catalog) -> Result<T, DriverError>,
    ) -> Result<T, DriverError> {
        // Cluster sessions keep statement writes in the outer
        // `MutationBuffer`/`BufferCheckpoint` owned by `ClusterServerSession`.
        // Their catalog contains only KV-backed tables, so taking a deep image
        // here duplicates schema maps on every prepared DML without adding a
        // rollback capability.  The in-process backend still has to retain
        // the image (and an internal Session transaction must always retain
        // it), hence the deliberately narrow fast path.
        let cluster_catalog = self.txn.is_none();
        self.with_catalog_mut(|catalog| {
            if cluster_catalog && catalog.all_tables_have_external_statement_rollback() {
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
        let cluster_catalog = self.txn.is_none();
        self.with_catalog_mut(|catalog| {
            let has_external_rollback =
                catalog.table_has_external_statement_rollback(database, table_name);
            if cluster_catalog && has_external_rollback {
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

    /// Opens the running statement's transactional read and lock step, Go's
    /// `OnStmtStart` for an in-process transaction.
    ///
    /// Go reads an INSERT, UPDATE, DELETE or locking SELECT of a pessimistic
    /// transaction at its `for_update_ts` (`executorBuilder.getSnapshotTS`),
    /// the latest committed rows beneath the transaction's own writes, and
    /// every statement of a READ-COMMITTED one at a fresh timestamp; any
    /// other statement reads the transaction's snapshot. Such a statement
    /// runs over a view of the shared catalog with the transaction's writes
    /// laid over it, and its lock operators record the keys to lock. A
    /// session whose transaction an outer owner drives (a cluster node, which
    /// installs its own key collector) is left to that owner.
    pub(crate) fn begin_transaction_statement(&mut self, stmt: &Stmt) -> Result<(), DriverError> {
        let locking_read = matches!(stmt, Stmt::Query(_)) && crate::dispatch::query_has_lock(stmt);
        let Some(txn) = self.txn.as_mut() else {
            return Ok(());
        };
        if txn.is_stale_read()
            || self.statement_snapshot.is_some()
            || self.selected_lock_keys.is_some()
        {
            return Ok(());
        }
        if !txn.writes_in_image {
            txn.locking_read |= locking_read;
            return Ok(());
        }
        let pessimistic = txn.mode.is_pessimistic();
        let for_update_read = pessimistic
            && (locking_read
                || txn.read_committed
                || self.statement_kind == crate::StatementKind::Dml);
        if !for_update_read && !locking_read {
            return Ok(());
        }
        let mut statement = TransactionStatement {
            view_base: None,
            saved: None,
            view_is_image: true,
            pessimistic,
            locking_read,
        };
        if for_update_read {
            self.staged_writes.begin_statement_log();
            let view_base = self.lock_catalog()?.clone();
            let Some(txn) = self.txn.as_mut() else {
                return Ok(());
            };
            statement.view_is_image = view_base.version() == txn.base_version;
            let view = if statement.view_is_image {
                txn.working.clone()
            } else {
                view_base.clone().with_transaction_writes(
                    &txn.read_snapshot,
                    &txn.working,
                    &self.staged_writes,
                )
            };
            statement.saved = Some(std::mem::replace(&mut txn.working, view));
            statement.view_base = Some(view_base);
        }
        self.selected_lock_keys = Some(
            tidb_executor::select_lock::SelectedLockKeys::new(
                tidb_txnkv::transaction::LockWaitTime::Timeout(self.lock_wait_timeout()),
                self.vars().shared_lock_promotion_enabled(),
            )
            .with_read_committed(self.read_committed_locking()),
        );
        self.transaction_statement = Some(statement);
        Ok(())
    }

    /// Whether a statement that fails its lock step may be run again: one
    /// that [`Self::begin_transaction_statement`] reads at `for_update_ts`.
    pub(crate) fn transaction_statement_may_retry(&self, stmt: &Stmt) -> bool {
        self.txn.as_ref().is_some_and(|txn| {
            txn.writes_in_image
                && !txn.is_stale_read()
                && txn.mode.is_pessimistic()
                && (statement_kind_of(stmt) == crate::StatementKind::Dml
                    || (matches!(stmt, Stmt::Query(_)) && crate::dispatch::query_has_lock(stmt)))
        })
    }

    /// Ends the running statement's lock step: Go `handlePessimisticDML`
    /// and `SelectLockExec`'s `doLockKeys`.
    ///
    /// A locking read's rows are drained first, so its lock operator has
    /// published every key before any row reaches the client. A pessimistic
    /// statement then locks those keys and every key it wrote; on success its
    /// writes join the transaction's image, and on any failure the statement
    /// leaves the image as it found it -- Go's statement rollback, the
    /// transaction going on. An optimistic transaction only records a locking
    /// read's keys for COMMIT to check.
    pub(crate) fn finish_transaction_statement(
        &mut self,
        execution: Result<crate::record_set::PendingExecution, DriverError>,
    ) -> StatementFinish {
        let Some(statement) = self.transaction_statement.take() else {
            return StatementFinish::Done(execution);
        };
        let collector = self.selected_lock_keys.take();
        let mut execution = match execution {
            Ok(execution) => execution,
            Err(error) => {
                self.restore_statement_image(statement);
                return StatementFinish::Done(Err(error));
            }
        };
        if statement.locking_read
            && matches!(execution, crate::record_set::PendingExecution::Query(_))
        {
            match execution.collect(self) {
                Ok(output) => execution = crate::record_set::PendingExecution::Complete(output),
                Err(error) => {
                    self.restore_statement_image(statement);
                    return StatementFinish::Done(Err(error));
                }
            }
        }
        let written = self.staged_writes.statement_log_keys();
        let mut requests = collector
            .map(|collector| collector.take_requests())
            .unwrap_or_default();
        if !statement.pessimistic {
            self.staged_writes.end_statement_log(true);
            if let Some(txn) = self.txn.as_mut() {
                txn.lock_keys
                    .extend(requests.into_iter().flat_map(|(_, keys)| keys));
            }
            return StatementFinish::Done(Ok(execution));
        }
        if !written.is_empty() {
            requests.push((
                tidb_txnkv::transaction::LockWaitTime::Timeout(self.lock_wait_timeout()),
                written.clone(),
            ));
        }
        for (wait, keys) in requests {
            if let Err(finish) = self.acquire_pessimistic_locks(&statement, wait, &keys) {
                self.restore_statement_image(statement);
                return finish;
            }
        }
        self.staged_writes.end_statement_log(true);
        if let (Some(saved), Some(txn)) = (statement.saved, self.txn.as_mut()) {
            if !statement.view_is_image {
                let view = std::mem::replace(&mut txn.working, saved);
                txn.working.absorb_writes(&view, &written);
            }
        }
        StatementFinish::Done(Ok(execution))
    }

    /// Takes `keys` for this transaction, Go `KVTxn.LockKeys` with the
    /// statement's wait policy.
    ///
    /// The committed rows are checked and the lock taken under the catalog
    /// lock, so no commit lands between them: a key committed since the
    /// statement read it asks for the statement to be rerun at a newer
    /// `for_update_ts`. A key another live transaction holds fails a NOWAIT
    /// request with 3572 and otherwise waits, with the catalog released, for
    /// that transaction to end, failing with 1205 when the wait runs out.
    fn acquire_pessimistic_locks(
        &self,
        statement: &TransactionStatement,
        wait: tidb_txnkv::transaction::LockWaitTime,
        keys: &[Vec<u8>],
    ) -> Result<(), StatementFinish> {
        use tidb_txnkv::transaction::LockWaitTime;
        let Some(guard) = self.txn.as_ref().and_then(|txn| txn.lock_guard.as_ref()) else {
            return Ok(());
        };
        let deadline = match wait {
            LockWaitTime::Timeout(timeout) => Some(std::time::Instant::now() + timeout),
            LockWaitTime::NoWait | LockWaitTime::AlwaysWait => None,
        };
        loop {
            let shared = self
                .lock_catalog()
                .map_err(|error| StatementFinish::Done(Err(error)))?;
            if let Some(base) = &statement.view_base {
                if shared.version() != base.version() && !shared.raw_values_match(base, keys) {
                    return Err(StatementFinish::Retry);
                }
            }
            let outcome = guard.locks().try_acquire(guard.owner(), keys);
            drop(shared);
            let tidb_executor::LockAcquire::Blocked { holder } = outcome else {
                return Ok(());
            };
            let error = match wait {
                LockWaitTime::NoWait => (
                    tidb_error::tidb::errcode::ErrLockAcquireFailAndNoWaitSet,
                    tidb_error::tidb::errname::ErrLockAcquireFailAndNoWaitSet.raw,
                ),
                _ if guard.locks().wait_released(holder, deadline) => continue,
                _ => (
                    tidb_error::tidb::errcode::ErrLockWaitTimeout,
                    tidb_error::tidb::errname::ErrLockWaitTimeout.raw,
                ),
            };
            return Err(StatementFinish::Done(Err(DriverError::Mysql(
                tidb_executor::MysqlError::new(error.0, error.1),
            ))));
        }
    }

    /// Puts back the transaction's image a statement ran beside, and takes
    /// the keys only the failed statement wrote back out of the
    /// transaction's record of its writes.
    fn restore_statement_image(&mut self, statement: TransactionStatement) {
        self.staged_writes.end_statement_log(false);
        if let (Some(saved), Some(txn)) = (statement.saved, self.txn.as_mut()) {
            txn.working = saved;
        }
    }

    /// Go `serverStatus2Str` over this session's status bits: the `State`
    /// column of `SHOW PROCESSLIST`.
    ///
    /// The typed autocommit state and transaction presence are the two status
    /// bits this tier owns, in the same order as Go's `ascServerStatus`.
    #[must_use]
    pub fn status_text(&self) -> String {
        match (self.txn.is_some(), self.is_autocommit()) {
            (true, true) => "in transaction; autocommit".to_owned(),
            (true, false) => "in transaction".to_owned(),
            (false, true) => "autocommit".to_owned(),
            (false, false) => String::new(),
        }
    }
}

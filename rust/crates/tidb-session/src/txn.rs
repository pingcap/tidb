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

use crate::{txn_mode_for_begin, PESSIMISTIC_TXN_MODE};
use crate::{Session, SessionTxnMode, TxnErrorKind};

/// An open transaction's state.
///
/// Go stages a transaction's writes in a `kv.MemBuffer` over a read snapshot
/// and flushes them at commit; this stages them in a private copy of the
/// catalog taken at `BEGIN`, so the session reads its own writes while its
/// peers see nothing until commit.
///
/// `base_version` is the shared catalog's mutation counter at `BEGIN`. If it
/// moved by commit time, someone else wrote, and the commit is refused rather
/// than overwriting their work -- the outcome Go gets from TiKV's optimistic
/// conflict check, though Go compares the WRITTEN KEYS while this compares the
/// whole catalog, so this refuses some commits Go would allow (documented).
pub(crate) struct Transaction {
    pub(crate) working: Catalog,
    base_version: u64,
    /// The transaction's start timestamp -- Go `TxnCtx.StartTS`, which
    /// `@@tidb_current_ts` reports. For a stale transaction it IS the as-of
    /// timestamp, which is the corpus's own assertion.
    start_ts: u64,
    /// `Some(ts)` for `START TRANSACTION READ ONLY AS OF TIMESTAMP`: the
    /// working copy is a historical snapshot and COMMIT publishes nothing.
    stale_read_ts: Option<u64>,
    /// The mode this transaction opened in, resolved from the `BEGIN` keyword
    /// and `@@tidb_txn_mode` exactly as Go resolves it.
    ///
    /// This tier's store is a catalog behind a mutex, not TiKV, so there is no
    /// lock to take and the mode changes nothing about how a statement runs
    /// here. It is still resolved and kept, because it is what the client
    /// asked for and what the real-TiKV tier consumes.
    mode: SessionTxnMode,
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
        local_temporary_at_open: Vec<(String, String, tidb_executor::KvTable)>,
    ) -> Self {
        let start_ts = catalog.allocate_tso();
        Transaction {
            working: catalog.clone(),
            base_version: catalog.version(),
            start_ts,
            stale_read_ts: None,
            mode,
            savepoints: Vec::new(),
            local_temporary_at_open,
        }
    }
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
    /// The session's GLOBAL temporary rows at the savepoint.
    ///
    /// These are transactional in Go with no exception at all: a global
    /// temporary table has NO storage outside the transaction, so every one
    /// of its keys is in the membuffer `RollbackMemDBToCheckpoint` truncates.
    /// The corpus asserts exactly this -- `executor/executor_txn`'s
    /// `TestSavepointWithTemporaryTable` inserts three rows under three
    /// savepoints and rolls back to each in turn.
    global_temporary: std::collections::HashMap<i64, Box<dyn tidb_executor::storage::TableStorage>>,
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
    global: std::collections::HashMap<i64, Box<dyn tidb_executor::storage::TableStorage>>,
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
        for (database, name) in catalog.global_temporary_table_ids() {
            let Some(table) = catalog.temporary_overlay_table_mut(&database, &name) else {
                continue;
            };
            let id = table.table_id;
            let incoming = self.global.remove(&id).unwrap_or_else(|| {
                Box::new(tidb_executor::storage::MemTableStorage::new())
                    as Box<dyn tidb_executor::storage::TableStorage>
            });
            let outgoing = table.swap_storage(incoming);
            self.global.insert(id, outgoing);
        }
    }
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
        self.vars.get_system("autocommit").as_deref() != Ok("OFF")
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

    /// Installs a transaction over the shared catalog as it stands now.
    ///
    /// The lock is taken and released here rather than held across the
    /// installation, because [`Transaction::open`] copies the catalog it is
    /// given and needs nothing from it afterwards.
    fn open_transaction(&mut self, mode: SessionTxnMode) -> Result<(), DriverError> {
        let local_temporary_at_open = self.local_temporary_tables.clone();
        let txn = Transaction::open(&*self.lock_catalog()?, mode, local_temporary_at_open);
        // Go publishes `TxnCtx.StartTS` the moment the transaction
        // activates; `@@tidb_current_ts` reads exactly that.
        self.current_tso().publish(txn.start_ts);
        self.txn = Some(txn);
        Ok(())
    }

    /// Go `CalculateAsOfTsExpr` (`pkg/sessiontxn/staleread/util.go:41-86`),
    /// over this session's own expression engine.
    ///
    /// Order is Go's exactly: NULL refuses; a DATETIME interpretation is
    /// tried first (through `UNIX_TIMESTAMP`, whose session-zone semantics
    /// are the ported `time.Date`), and only then a raw TSO -- a positive
    /// integer or a string of digits. A TSO whose physical half is before
    /// 2013-01-01 refuses with Go's own message.
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
        let tso = if let Some(seconds) = seconds {
            ((seconds * 1000.0) as u64) << TSO_LOGICAL_BITS
        } else if let Ok(raw) = text.parse::<u64>() {
            raw
        } else {
            return Err(as_of_error(
                "cannot parse AS OF TIMESTAMP expression as datetime or TSO",
            ));
        };
        if (tso >> TSO_LOGICAL_BITS) <= MIN_PHYSICAL_MS {
            return Err(as_of_error(
                "invalid TSO timestamp: TSO is before 2013-01-01",
            ));
        }
        Ok(tso)
    }

    /// `START TRANSACTION READ ONLY AS OF TIMESTAMP <resolved ts>`: the Go
    /// stale transaction (`StalenessTxnContextProvider`), whose `StartTS` IS
    /// the as-of timestamp and whose reads all see the store as of it.
    pub(crate) fn open_stale_transaction(&mut self, ts: u64) -> Result<(), DriverError> {
        let snapshot = {
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
        let local_temporary_at_open = self.local_temporary_tables.clone();
        self.txn = Some(Transaction {
            base_version: snapshot.version(),
            working: snapshot,
            start_ts: ts,
            stale_read_ts: Some(ts),
            mode: SessionTxnMode::Optimistic,
            savepoints: Vec::new(),
            local_temporary_at_open,
        });
        self.current_tso().publish(ts);
        Ok(())
    }

    /// Ends the one-statement stale transaction the as-of interception
    /// opened: nothing to publish (read-only by construction), the
    /// start-only `LastTxnInfo` record a read-only end leaves, and the
    /// published timestamp goes with it.
    pub(crate) fn discard_stale_statement_transaction(&mut self) {
        if let Some(txn) = self.txn.take() {
            self.set_last_txn_info_started(txn.start_ts);
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
                // reads history. This tier's store keeps none, and answering
                // from the present under a historical name is undetectable --
                // the same reason a table reference's `AS OF TIMESTAMP` and a
                // pinned `tidb_snapshot` are refused.
                if let Some(expr) = &begin.as_of {
                    // Go `StalenessTxnContextProvider`: the expression
                    // resolves through `CalculateAsOfTsExpr`'s rules and the
                    // transaction opens AT that timestamp -- its `StartTS`,
                    // which `@@tidb_current_ts` reports.
                    let ts = self.resolve_as_of_ts(&expr.clone())?;
                    if self.txn.is_some() {
                        self.commit()?;
                    }
                    self.open_stale_transaction(ts)?;
                    return Ok(Some(true));
                }
                // An open transaction is committed first (Go's implicit commit).
                if self.txn.is_some() {
                    self.commit()?;
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
                // Dropping the staged copy discards every staged write.
                // A local temporary table's rows are not in that copy -- they
                // are in the session -- so they are put back by hand.
                if let Some(txn) = self.txn.take() {
                    // Go `setLastTxnInfoBeforeTxnEnd`: an activated
                    // transaction that ends without a commit leaves the
                    // start-only record.
                    self.set_last_txn_info_started(txn.start_ts);
                    self.current_tso().clear();
                    self.restore_local_temporary_rows(txn.local_temporary_at_open);
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
        let local_temporary = self.local_temporary_tables.clone();
        let global_temporary = self.global_temporary_data.clone();
        let Some(txn) = &mut self.txn else {
            return Ok(());
        };
        let name = name.to_lowercase();
        let image = txn.working.clone();
        txn.savepoints.retain(|savepoint| savepoint.name != name);
        txn.savepoints.push(Savepoint {
            name,
            image,
            local_temporary,
            global_temporary,
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
        let lowered = name.to_lowercase();
        let txn = self
            .txn
            .as_mut()
            .ok_or_else(|| DriverError::SavepointNotExists(name.to_owned()))?;
        let index = txn
            .savepoints
            .iter()
            .position(|savepoint| savepoint.name == lowered)
            .ok_or_else(|| DriverError::SavepointNotExists(name.to_owned()))?;
        txn.working = txn.savepoints[index].image.clone();
        let local_temporary = txn.savepoints[index].local_temporary.clone();
        let global_temporary = txn.savepoints[index].global_temporary.clone();
        txn.savepoints.truncate(index + 1);
        self.restore_local_temporary_rows(local_temporary);
        self.global_temporary_data = global_temporary;
        Ok(())
    }

    /// `RELEASE SAVEPOINT name` -- Go `SimpleExec.executeReleaseSavepoint`
    /// plus `TxnCtx.ReleaseSavepoint`: drops the named savepoint AND every
    /// savepoint taken after it (`Savepoints[:i]`), touching no data.
    fn release_savepoint(&mut self, name: &str) -> Result<(), DriverError> {
        let lowered = name.to_lowercase();
        let index = self
            .txn
            .as_ref()
            .and_then(|txn| {
                txn.savepoints
                    .iter()
                    .position(|savepoint| savepoint.name == lowered)
            })
            .ok_or_else(|| DriverError::SavepointNotExists(name.to_owned()))?;
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
        let Some(txn) = self.txn.take() else {
            // COMMIT with no open transaction is a no-op, as in MySQL.
            return Ok(());
        };
        if txn.stale_read_ts.is_some() {
            // Go's stale transaction is read-only by construction; its
            // COMMIT publishes nothing, and `setLastTxnInfoBeforeTxnEnd`
            // leaves the start-only record (`pkg/session/session.go:1056`).
            self.set_last_txn_info_started(txn.start_ts);
            self.current_tso().clear();
            return Ok(());
        }
        let mut shared = self.lock_catalog()?;
        if shared.version() != txn.base_version {
            return Err(DriverError::Txn(TxnErrorKind::WriteConflict));
        }
        *shared = txn.working;
        // The commit is durable in this store the moment the shared catalog
        // holds it; the history snapshot and Go's `LastTxnInfo` record
        // both.
        let commit_ts = shared.allocate_tso();
        shared.record_commit(commit_ts);
        drop(shared);
        self.set_last_txn_info_committed(txn.start_ts, commit_ts);
        self.current_tso().clear();
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
        // Go wraps every statement's infoschema in a
        // `SessionExtendedInfoSchema` (`temptable.AttachLocalTemporaryTable
        // InfoSchema`) and installs a snapshot interceptor for the temporary
        // rows (`temptable.SessionSnapshotInterceptor`). Both are per
        // STATEMENT, and both are undone before the statement's catalog is
        // shared with anyone -- which is exactly why they belong here, at the
        // one door every statement reaches the catalog through, rather than
        // in each statement arm.
        let mut overlay = TemporaryTableOverlay {
            local: std::mem::take(&mut self.local_temporary_tables),
            global: std::mem::take(&mut self.global_temporary_data),
        };
        let value = match &mut self.txn {
            Some(txn) => overlay.run(&mut txn.working, body),
            None => {
                let mut catalog = self
                    .catalog
                    .lock()
                    .map_err(|_| DriverError::CatalogPoisoned)?;
                overlay.run(&mut catalog, body)
            }
        };
        self.local_temporary_tables = std::mem::take(&mut overlay.local);
        self.global_temporary_data = std::mem::take(&mut overlay.global);
        value
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
        self.with_catalog_mut(|catalog| {
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

    /// Go `serverStatus2Str` over this session's status bits: the `State`
    /// column of `SHOW PROCESSLIST`.
    ///
    /// This tier's connections are always autocommit and set no other status
    /// bit, so the text is `in transaction; autocommit` inside an explicit
    /// transaction and `autocommit` outside one -- exactly the order Go's
    /// `ascServerStatus` produces for those bits.
    #[must_use]
    pub fn status_text(&self) -> String {
        if self.txn.is_some() {
            "in transaction; autocommit".to_owned()
        } else {
            "autocommit".to_owned()
        }
    }
}

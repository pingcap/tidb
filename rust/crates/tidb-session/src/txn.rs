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
    fn open(catalog: &Catalog, mode: SessionTxnMode) -> Self {
        Transaction {
            working: catalog.clone(),
            base_version: catalog.version(),
            mode,
            savepoints: Vec::new(),
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
        let txn = Transaction::open(&*self.lock_catalog()?, mode);
        self.txn = Some(txn);
        Ok(())
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
                self.txn = None;
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
        let Some(txn) = &mut self.txn else {
            return Ok(());
        };
        let name = name.to_lowercase();
        let image = txn.working.clone();
        txn.savepoints.retain(|savepoint| savepoint.name != name);
        txn.savepoints.push(Savepoint { name, image });
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
        txn.savepoints.truncate(index + 1);
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
        let mut shared = self.lock_catalog()?;
        if shared.version() != txn.base_version {
            return Err(DriverError::Txn(TxnErrorKind::WriteConflict));
        }
        *shared = txn.working;
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
        match &mut self.txn {
            Some(txn) => body(&mut txn.working),
            None => {
                let mut catalog = self
                    .catalog
                    .lock()
                    .map_err(|_| DriverError::CatalogPoisoned)?;
                body(&mut catalog)
            }
        }
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
    /// What the image DOES undo on either storage is the table state that is
    /// not bytes: `KvTable::next_handle`, the `_tidb_rowid` counter, is a
    /// plain field, so a failed statement gives back the handles it consumed.
    /// `AutoIdAllocator` is deliberately not, per the paragraph below.
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

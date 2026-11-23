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

//! Go `pkg/executor/simple.go`'s transaction statements: `executeBegin`,
//! `executeCommit`, `executeRollback`, `executeSavepoint` and
//! `executeReleaseSavepoint`. Entering and leaving the transaction and the
//! savepoint stack are the session's (`sessiontxn`, `TxnCtx`), which
//! [`TxnControlSession`] reaches.

use tidb_ast::{SessionStmt, Stmt};
use tidb_context::WarningLevel;

use crate::DriverError;

/// The session state Go's transaction statements reach through `sctx`.
///
/// `TxnOps` is the store half of the transaction manager.
pub trait TxnControlSession<TxnOps: ?Sized> {
    /// Go's `tidb_enable_noop_functions` gate for a no-op clause.
    ///
    /// # Errors
    ///
    /// 1235 when the variable is OFF.
    fn gate_noop_clause(&mut self, clause: &'static str, global: bool) -> Result<(), DriverError>;
    /// Go `CalculateAsOfTsExpr`.
    ///
    /// # Errors
    ///
    /// The expression's failure.
    fn calculate_as_of_ts_expr(&mut self, expr: &tidb_ast::Expr) -> Result<u64, DriverError>;
    /// Go `SessionVars.TxnReadTS` (`SET TRANSACTION READ ONLY AS OF`) is set:
    /// its snapshot schema is loaded for `ts` and the BEGIN refused.
    ///
    /// # Errors
    ///
    /// The refusal, or the schema load's failure.
    fn refuse_begin_as_of_after_txn_read_ts(&mut self, ts: u64) -> Result<(), DriverError>;
    /// Whether `SET TRANSACTION READ ONLY AS OF` is in force.
    fn txn_read_ts_is_set(&self) -> bool;
    /// Go `sessiontxn.NewTxn` with `EnterNewTxnWithBeginStmt`: the open
    /// transaction committed, a new one entered (at `as_of` when given).
    ///
    /// # Errors
    ///
    /// The commit's or the activation's failure.
    fn enter_new_txn_with_begin(
        &mut self,
        begin: &tidb_ast::BeginStmt,
        as_of: Option<u64>,
        txn: &mut TxnOps,
    ) -> Result<(), DriverError>;
    /// Go `executeCommit`: `SetInTxn(false)`; the store commits when the
    /// statement finishes.
    fn leave_txn_ctx_for_commit(&mut self);
    /// Go `RollbackTxn`.
    ///
    /// # Errors
    ///
    /// The store rollback's failure.
    fn rollback_txn(&mut self, txn: &mut TxnOps) -> Result<(), DriverError>;
    /// Go `Txn(true)`: with autocommit OFF the pending transaction opens.
    ///
    /// # Errors
    ///
    /// The activation's failure.
    fn activate_txn(&mut self) -> Result<(), DriverError>;
    /// Go `sessVars.InTxn()`.
    fn in_transaction(&self) -> bool;
    /// Whether the open transaction is pessimistic and checks constraints
    /// lazily (Go's `TxnCtx.IsPessimistic && !ConstraintCheckInPlacePessimistic`).
    fn pessimistic_without_constraint_check_in_place(&self) -> bool;
    /// Go `TxnCtx.AddSavepoint` over the store's checkpoint.
    ///
    /// # Errors
    ///
    /// The checkpoint's failure.
    fn add_savepoint(&mut self, name: &str, txn: &mut TxnOps) -> Result<(), DriverError>;
    /// Go `TxnCtx.RollbackToSavepoint` and `RollbackMemDBToCheckpoint`:
    /// whether the savepoint exists.
    fn rollback_to_savepoint(&mut self, name: &str, txn: &mut TxnOps) -> bool;
    /// Go `TxnCtx.ReleaseSavepoint`: whether the savepoint existed.
    fn release_savepoint(&mut self, name: &str) -> bool;
    /// The statement's error, also in the warning buffer.
    fn append_driver_error(&mut self, level: WarningLevel, error: &DriverError);
}

/// Go `SimpleExec`'s transaction statements over a session.
pub trait TxnControlExec<TxnOps: ?Sized>: TxnControlSession<TxnOps> {
    /// The `SimpleExec` transaction statements: `Some(in_transaction)` for
    /// BEGIN, COMMIT, ROLLBACK [TO], SAVEPOINT and RELEASE SAVEPOINT, `None`
    /// for anything else.
    ///
    /// `COMMIT` only leaves the transaction here, as Go's `executeCommit`
    /// only clears `InTxn`; the store session commits it when the statement
    /// finishes (Go `finishStmt` -> `CommitTxn`).
    ///
    /// # Errors
    ///
    /// The statement's failure.
    fn control_transaction_stmt(&mut self, stmt: &Stmt, txn: &mut TxnOps) -> Result<Option<bool>, DriverError> {
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
                    self.execute_rollback_to_savepoint(name, txn)?;
                    return Ok(Some(true));
                }
                self.rollback_txn(txn)?;
                Ok(Some(false))
            }
            SessionStmt::Savepoint(name) => {
                self.execute_savepoint(name, txn)?;
                Ok(Some(self.in_transaction()))
            }
            SessionStmt::ReleaseSavepoint(name) => {
                self.execute_release_savepoint(name)?;
                Ok(Some(true))
            }
            _ => Ok(None),
        }
    }

    /// Go `SimpleExec.executeBegin`.
    ///
    /// # Errors
    ///
    /// The statement's failure.
    fn execute_begin(&mut self, begin: &tidb_ast::BeginStmt, txn: &mut TxnOps) -> Result<(), DriverError> {
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
        // the statement is built, before any commit.
        let as_of = match &begin.as_of {
            Some(expr) => {
                let ts = self.calculate_as_of_ts_expr(&expr.clone())?;
                if self.txn_read_ts_is_set() {
                    self.refuse_begin_as_of_after_txn_read_ts(ts)?;
                }
                Some(ts)
            }
            None => None,
        };
        self.enter_new_txn_with_begin(begin, as_of, txn)
    }

    /// Go `SimpleExec.executeSavepoint`. The no-op arm is narrower than "no
    /// transaction open": Go returns `nil` only when `!sessVars.InTxn() &&
    /// sessVars.IsAutocommit()`; with autocommit OFF, `Txn(true)` activates
    /// the pending transaction, so `SAVEPOINT` is what opens it.
    ///
    /// # Errors
    ///
    /// The statement's failure.
    fn execute_savepoint(&mut self, name: &str, txn: &mut TxnOps) -> Result<(), DriverError> {
        self.activate_txn()?;
        if self.in_transaction() && self.pessimistic_without_constraint_check_in_place() {
            return Err(DriverError::Mysql(tidb_terror::MysqlError::unknown(
                "savepoint is not supported in pessimistic transactions when in-place constraint check is disabled",
            )));
        }
        self.add_savepoint(name, txn)
    }

    /// `ROLLBACK TO [SAVEPOINT] name` -- Go's `executeRollback` savepoint
    /// arm. With no transaction open Go's `txn.Valid()` is false and the
    /// error is the same 1305 an unknown name gets.
    ///
    /// # Errors
    ///
    /// 1305 for an unknown savepoint.
    fn execute_rollback_to_savepoint(&mut self, name: &str, txn: &mut TxnOps) -> Result<(), DriverError> {
        if self.rollback_to_savepoint(name, txn) {
            return Ok(());
        }
        // Go appends the statement error into the warning buffer as well
        // (driver_tidb.go:376's error door), so SHOW WARNINGS after the
        // refused ROLLBACK TO carries the same 1305 row.
        let error = savepoint_not_exists(name);
        self.append_driver_error(WarningLevel::Error, &error);
        Err(error)
    }

    /// Go `SimpleExec.executeReleaseSavepoint`: drops the named savepoint
    /// and every savepoint taken after it, touching no data.
    ///
    /// # Errors
    ///
    /// 1305 for an unknown savepoint.
    fn execute_release_savepoint(&mut self, name: &str) -> Result<(), DriverError> {
        if self.release_savepoint(name) {
            return Ok(());
        }
        let error = savepoint_not_exists(name);
        self.append_driver_error(WarningLevel::Error, &error);
        Err(error)
    }
}

impl<TxnOps: ?Sized, T: TxnControlSession<TxnOps> + ?Sized> TxnControlExec<TxnOps> for T {}

/// Go `exeerrors.ErrSavepointNotExists`.
fn savepoint_not_exists(name: &str) -> DriverError {
    DriverError::from(
        tidb_exeerrors::ERR_SAVEPOINT_NOT_EXISTS.gen_with_stack_by_args(&["SAVEPOINT".into(), name.to_owned().into()]),
    )
}

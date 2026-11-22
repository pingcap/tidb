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

//! Go `pkg/sessiontxn` (`interface.go`): how a session enters a
//! transaction, and the provider every statement of that transaction asks
//! for its read / for-update timestamps, its snapshots and what to do after
//! an error.
//!
//! Go's `kv.Transaction` is [`OpenClusterTransaction`] (the session's one
//! `LazyTxn`, X12), `kv.Snapshot` is [`ClusterSnapshot`], and an error is
//! [`SqlQueryError`].

pub mod isolation;
pub mod staleread;

use tidb_domain_core::cluster_transactions::OpenClusterTransaction;
use tidb_kv::cluster_storage::ClusterSnapshot;
use tidb_store_driver_error::sql_query_error::SqlQueryError;

/// Go `EnterNewTxnType`: how a new transaction is entered.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum EnterNewTxnType {
    /// Go `EnterNewTxnDefault`: start a new transaction now, without any
    /// scenario assumptions.
    #[default]
    Default,
    /// Go `EnterNewTxnWithBeginStmt`: `BEGIN` / `START TRANSACTION`.
    WithBeginStmt,
    /// Go `EnterNewTxnBeforeStmt`: before a statement when no transaction is
    /// present; the transaction is lazy, activated only when needed.
    BeforeStmt,
    /// Go `EnterNewTxnWithReplaceProvider`: replace the current provider
    /// (stale read).
    WithReplaceProvider,
}

/// Go `TxnMode` values of `EnterNewTxnRequest` (`ast.Pessimistic`,
/// `ast.Optimistic`, or empty: decided from the session variables).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum TxnMode {
    /// Empty: `tidb_txn_mode` decides.
    #[default]
    FromVars,
    /// `ast.Optimistic`.
    Optimistic,
    /// `ast.Pessimistic`.
    Pessimistic,
}

/// Go `EnterNewTxnRequest`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct EnterNewTxnRequest {
    /// Go `Type`.
    pub tp: EnterNewTxnType,
    /// Go `TxnMode`.
    pub txn_mode: TxnMode,
    /// Go `CausalConsistencyOnly`.
    pub causal_consistency_only: bool,
    /// Go `StaleReadTS`: zero for a transaction that is not a stale read.
    pub stale_read_ts: u64,
}

/// Go `StmtErrorHandlePoint`: where a statement error is being handled.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StmtErrorHandlePoint {
    /// Go `StmtErrAfterQuery`.
    AfterQuery,
    /// Go `StmtErrAfterPessimisticLock`.
    AfterPessimisticLock,
}

/// Go `StmtErrorAction`: the next action after a statement error.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StmtErrorAction {
    /// Go `StmtActionError`: return the error, no retry.
    Error,
    /// Go `StmtActionRetryReady`: this component caused the error and is
    /// ready for the retry.
    RetryReady,
    /// Go `StmtActionNoIdea`: not this component's error; another one
    /// decides, else the original error is returned.
    NoIdea,
}

/// Go `ErrorAction`: [`StmtErrorAction::Error`] with `error`.
///
/// # Errors
///
/// Always `error`, as Go's pair `(StmtActionError, err)`.
pub fn error_action(error: SqlQueryError) -> Result<StmtErrorAction, SqlQueryError> {
    Err(error)
}

/// Go `RetryReady`.
#[must_use]
pub fn retry_ready() -> Result<StmtErrorAction, SqlQueryError> {
    Ok(StmtErrorAction::RetryReady)
}

/// Go `NoIdea`.
#[must_use]
pub fn no_idea() -> Result<StmtErrorAction, SqlQueryError> {
    Ok(StmtErrorAction::NoIdea)
}

/// Go `TxnAdvisable`: optimizations within a transaction.
pub trait TxnAdvisable {
    /// Go `AdviseWarmup`: warm up inner state (fetch the timestamp early).
    ///
    /// # Errors
    ///
    /// The warm-up failed.
    fn advise_warmup(&mut self, cx: &mut dyn TxnContext) -> Result<(), SqlQueryError>;
    /// Go `AdviseOptimizeWithPlan`: optimize the transaction for the plan
    /// (the point get that reads the latest version without a timestamp).
    ///
    /// # Errors
    ///
    /// The advice could not be applied.
    fn advise_optimize_with_plan(
        &mut self,
        cx: &mut dyn TxnContext,
        plan: &dyn std::any::Any,
    ) -> Result<(), SqlQueryError>;
}

/// Go's `sctx` as a provider reads it. Go stores the session in the
/// provider; here the session lends it to each call, so the provider and the
/// session never alias.
pub trait TxnContext: isolation::ProviderSessionVars {
    /// Go `StmtCtx.RCCheckTS`.
    fn rc_check_ts(&self) -> bool;
    /// Sets Go `StmtCtx.RCCheckTS` for the running statement.
    fn set_rc_check_ts(&mut self);
    /// Go `OnPessimisticStmtStart`'s gate: `PessimisticTransactionFairLocking`
    /// on a client connection (`ConnectionID != 0`) outside restricted SQL.
    fn pessimistic_fair_locking(&self) -> bool;
    /// Go `isolation.NeedSetRCCheckTSFlag(sctx, node)`.
    fn need_set_rc_check_ts_flag(&self, node: &tidb_ast::Stmt) -> bool;
    /// Whether the statement is a locking read (`SELECT ... FOR UPDATE`):
    /// the snapshot handle takes it to route reads that lock in execution.
    fn locking_read(&self) -> bool;
}

/// The provider's pending transaction future, held weakly by its advisor:
/// the statement context that carries the advisor outlives the transaction
/// (a retained plan keeps it), and the provider never outlives its
/// transaction.
pub type WeakTxnFuture = std::sync::Weak<dyn tidb_domain_core::cluster_transactions::TxnFuturePreparer>;

/// The transaction's future, weakly.
#[must_use]
pub fn txn_future(txn: &dyn OpenClusterTransaction) -> Option<WeakTxnFuture> {
    txn.txn_future_preparer().map(|future| std::sync::Arc::downgrade(&future))
}

/// Go `baseTxnContextProvider.AdviseWarmup` -> `prepareTxn`: requests the
/// transaction's timestamp unless it is prepared.
pub fn advise_warmup(future: Option<&WeakTxnFuture>) {
    if let Some(future) = future.and_then(std::sync::Weak::upgrade) {
        let _ = future.prepare_txn();
    }
}

/// Go `baseTxnContextProvider`'s advice: no plan optimization, and the
/// warm-up.
#[derive(Debug)]
pub struct BaseTxnAdvisor {
    future: WeakTxnFuture,
}

impl tidb_sessionctx::TxnPlanAdvisor for BaseTxnAdvisor {
    fn advise_optimize_with_plan(
        &self,
        _plan: &dyn std::any::Any,
        _insert_replace_or_on_duplicate: bool,
    ) -> Option<bool> {
        None
    }

    fn advise_warmup(&self) {
        advise_warmup(Some(&self.future));
    }
}

/// Go `TxnContextProvider`: one transaction's context. It owns the
/// transaction (Go's `p.txn` and the session `LazyTxn` it activates). The
/// default bodies are Go's `baseTxnContextProvider` methods that a provider
/// does not override.
pub trait TxnContextProvider: Send {
    /// The transaction (Go `sctx.Txn(false)`).
    fn txn(&self) -> &dyn OpenClusterTransaction;
    /// The transaction, for its commit or rollback.
    fn into_txn(self: Box<Self>) -> Box<dyn OpenClusterTransaction>;
    /// Go `GetStmtReadTS`: the read timestamp of a SELECT (not FOR UPDATE).
    ///
    /// # Errors
    ///
    /// The transaction could not be activated.
    fn get_stmt_read_ts(&mut self, cx: &mut dyn TxnContext) -> Result<u64, SqlQueryError>;
    /// Go `GetStmtForUpdateTS`: the timestamp of an INSERT / UPDATE /
    /// DELETE or a SELECT ... FOR UPDATE.
    ///
    /// # Errors
    ///
    /// The transaction could not be activated.
    fn get_stmt_for_update_ts(&mut self, cx: &mut dyn TxnContext) -> Result<u64, SqlQueryError>;
    /// Go `GetSnapshotWithStmtReadTS`.
    ///
    /// # Errors
    ///
    /// The snapshot could not be opened.
    fn get_snapshot_with_stmt_read_ts(
        &mut self,
        cx: &mut dyn TxnContext,
    ) -> Result<Box<dyn ClusterSnapshot>, SqlQueryError>;
    /// Go `GetSnapshotWithStmtForUpdateTS`.
    ///
    /// # Errors
    ///
    /// The snapshot could not be opened.
    fn get_snapshot_with_stmt_for_update_ts(
        &mut self,
        cx: &mut dyn TxnContext,
    ) -> Result<Box<dyn ClusterSnapshot>, SqlQueryError>;
    /// Whether one statement's `GetStmtReadTS` can lie below its
    /// `GetStmtForUpdateTS`, so Go `executorBuilder.getSnapshotTS` gives a
    /// locking statement's readers outside `SelectLock` a snapshot of their
    /// own. Only `PessimisticRRTxnContextProvider` (start ts against a fresh
    /// for-update ts); RC's two are its one `stmtTS`, and the optimistic and
    /// serializable providers read both at the start ts.
    fn stmt_read_ts_precedes_for_update_ts(&self) -> bool {
        false
    }
    /// Go `OnStmtStart`.
    ///
    /// # Errors
    ///
    /// The statement could not start in this transaction.
    fn on_stmt_start(
        &mut self,
        _cx: &mut dyn TxnContext,
        _node: Option<&tidb_ast::Stmt>,
    ) -> Result<(), SqlQueryError> {
        Ok(())
    }
    /// Go `OnPessimisticStmtStart` (base: nothing). A pessimistic provider
    /// starts the statement's fair locking when the session asks for it.
    ///
    /// # Errors
    ///
    /// Fair locking could not start.
    fn on_pessimistic_stmt_start(&mut self, _cx: &mut dyn TxnContext) -> Result<(), SqlQueryError> {
        Ok(())
    }
    /// Go `OnPessimisticStmtEnd` (base: nothing). A pessimistic provider
    /// ends the statement's fair-locking scope: done when it succeeded,
    /// cancelled (its retry-round locks released) when it failed.
    ///
    /// # Errors
    ///
    /// Fair locking could not end.
    fn on_pessimistic_stmt_end(
        &mut self,
        _cx: &mut dyn TxnContext,
        _is_successful: bool,
    ) -> Result<(), SqlQueryError> {
        Ok(())
    }
    /// Go `OnStmtRetry`: the statement re-executes. A pessimistic provider
    /// fetches a new for-update timestamp (`updateForUpdateTS`), which the
    /// retry reads and locks at.
    ///
    /// # Errors
    ///
    /// The retry could not be prepared.
    fn on_stmt_retry(&mut self, _cx: &mut dyn TxnContext) -> Result<(), SqlQueryError> {
        Ok(())
    }
    /// Go `OnStmtErrorForNextAction` (base: an error after the pessimistic
    /// lock is returned, anything else is no idea).
    ///
    /// # Errors
    ///
    /// The error is to be returned ([`StmtErrorAction::Error`]).
    fn on_stmt_error_for_next_action(
        &mut self,
        _cx: &mut dyn TxnContext,
        point: StmtErrorHandlePoint,
        error: SqlQueryError,
    ) -> Result<StmtErrorAction, SqlQueryError> {
        match point {
            StmtErrorHandlePoint::AfterPessimisticLock => error_action(error),
            _ => no_idea(),
        }
    }
    /// Go `AdviseOptimizeWithPlan`, handed to the executor builder: the
    /// advisor of this provider's plan advice, `None` for a provider whose
    /// advice is the base no-op (serializable, staleness).
    /// `auto_commit_txn` is Go `IsAutoCommitTxn(vars)`.
    fn plan_advisor(
        &self,
        _retrying: bool,
        _rc_write_check_ts: bool,
        _auto_commit_txn: bool,
    ) -> Option<std::sync::Arc<dyn tidb_sessionctx::TxnPlanAdvisor>> {
        let future = txn_future(self.txn())?;
        Some(std::sync::Arc::new(BaseTxnAdvisor { future }))
    }
    /// Go `ActivateTxn`: the transaction, activated.
    ///
    /// # Errors
    ///
    /// The transaction could not be activated.
    fn activate_txn(&mut self) -> Result<&dyn OpenClusterTransaction, SqlQueryError> {
        self.txn().activate().map_err(SqlQueryError::unknown)?;
        Ok(self.txn())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Go `ErrorAction` / `RetryReady` / `NoIdea`.
    #[test]
    fn error_action_helpers_answer_as_go() {
        let error = SqlQueryError::unknown("boom".to_owned());
        assert_eq!(error_action(error.clone()).unwrap_err().message, error.message);
        assert_eq!(retry_ready().unwrap(), StmtErrorAction::RetryReady);
        assert_eq!(no_idea().unwrap(), StmtErrorAction::NoIdea);
        assert_eq!(EnterNewTxnRequest::default().tp, EnterNewTxnType::Default);
    }
}

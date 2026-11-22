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

//! Go `pkg/sessiontxn/isolation/optimistic.go`.

use super::{txn_start_ts, ProviderSessionVars};
use crate::{EnterNewTxnType, TxnContext, TxnContextProvider};
use std::sync::atomic::{AtomicBool, Ordering};
use tidb_domain_core::cluster_transactions::{OpenClusterTransaction, TxnFuturePreparer};
use tidb_planner::common_plans::is_point_get_with_pk_or_unique_key_by_auto_commit;
use tidb_planner_core_operator_physicalop::PhysicalPlan;
use tidb_kv::cluster_storage::ClusterSnapshot;
use tidb_store_driver_error::sql_query_error::SqlQueryError;

/// Go `OptimisticTxnContextProvider`: every statement reads and writes at the
/// transaction's start timestamp.
pub struct OptimisticTxnContextProvider {
    txn: Box<dyn OpenClusterTransaction>,
}

impl OptimisticTxnContextProvider {
    /// The provider of an optimistic transaction.
    #[must_use]
    pub fn new(txn: Box<dyn OpenClusterTransaction>) -> Self {
        Self { txn }
    }
}

impl TxnContextProvider for OptimisticTxnContextProvider {
    fn txn(&self) -> &dyn OpenClusterTransaction {
        &*self.txn
    }

    fn into_txn(self: Box<Self>) -> Box<dyn OpenClusterTransaction> {
        self.txn
    }

    // Go `getStmtReadTSFunc = p.getTxnStartTS`.
    fn get_stmt_read_ts(&mut self, _cx: &mut dyn TxnContext) -> Result<u64, SqlQueryError> {
        txn_start_ts(&*self.txn)
    }

    // Go `getStmtForUpdateTSFunc = p.getTxnStartTS`.
    fn get_stmt_for_update_ts(&mut self, _cx: &mut dyn TxnContext) -> Result<u64, SqlQueryError> {
        txn_start_ts(&*self.txn)
    }

    fn get_snapshot_with_stmt_read_ts(
        &mut self,
        cx: &mut dyn TxnContext,
    ) -> Result<Box<dyn ClusterSnapshot>, SqlQueryError> {
        self.txn.snapshot_for(cx.locking_read()).map_err(SqlQueryError::unknown)
    }

    fn get_snapshot_with_stmt_for_update_ts(
        &mut self,
        cx: &mut dyn TxnContext,
    ) -> Result<Box<dyn ClusterSnapshot>, SqlQueryError> {
        self.txn.snapshot_for(cx.locking_read()).map_err(SqlQueryError::unknown)
    }

    fn plan_advisor(
        &self,
        _retrying: bool,
        _rc_write_check_ts: bool,
        auto_commit_txn: bool,
    ) -> Option<std::sync::Arc<dyn tidb_sessionctx::TxnPlanAdvisor>> {
        let future = std::sync::Arc::downgrade(&self.txn.txn_future_preparer()?);
        Some(std::sync::Arc::new(OptimisticPlanAdvisor {
            future,
            auto_commit_txn,
            optimize_with_max_ts: AtomicBool::new(false),
        }))
    }
}

/// Go `OptimisticTxnContextProvider.AdviseOptimizeWithPlan` and
/// `AdviseWarmup` over the provider's pending transaction: an autocommit
/// point get by primary or unique key reads at `math.MaxUint64`
/// (`forcePrepareConstStartTS`) instead of spending a timestamp.
pub struct OptimisticPlanAdvisor {
    /// The provider's transaction future, held weakly: the statement context
    /// that carries this advisor outlives the transaction (a retained plan
    /// keeps it), and the provider never outlives its transaction.
    future: std::sync::Weak<dyn TxnFuturePreparer>,
    /// Go `IsAutoCommitTxn(vars)`.
    auto_commit_txn: bool,
    /// Go `optimizeWithMaxTS`.
    optimize_with_max_ts: AtomicBool,
}

impl std::fmt::Debug for OptimisticPlanAdvisor {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("OptimisticPlanAdvisor")
            .field("auto_commit_txn", &self.auto_commit_txn)
            .field("optimize_with_max_ts", &self.optimize_with_max_ts)
            .finish_non_exhaustive()
    }
}

impl tidb_sessionctx::TxnPlanAdvisor for OptimisticPlanAdvisor {
    fn advise_optimize_with_plan(
        &self,
        plan: &dyn std::any::Any,
        _insert_replace_or_on_duplicate: bool,
    ) -> Option<bool> {
        // `p.isTidbSnapshotEnabled()` / `p.isBeginStmtWithStaleRead()`: no
        // advisor is published for those statements.
        if self.optimize_with_max_ts.load(Ordering::Relaxed) {
            return None;
        }
        let plan = plan.downcast_ref::<PhysicalPlan>()?;
        if is_point_get_with_pk_or_unique_key_by_auto_commit(self.auto_commit_txn, plan)
            // A refusal is Go's `p.txn != nil`: the transaction has already
            // been activated, so its start ts has already been used.
            && self
                .future
                .upgrade()
                .is_some_and(|future| future.force_prepare_const_start_ts(u64::MAX).is_ok())
        {
            self.optimize_with_max_ts.store(true, Ordering::Relaxed);
        }
        None
    }

    fn optimize_with_max_ts(&self) -> bool {
        self.optimize_with_max_ts.load(Ordering::Relaxed)
    }

    // Go `baseTxnContextProvider.AdviseWarmup` -> `prepareTxn`.
    fn advise_warmup(&self) {
        if let Some(future) = self.future.upgrade() {
            let _ = future.prepare_txn();
        }
    }
}

/// Go `isOptimisticTxnRetryable`: the `TxnCtx.CouldRetry` an optimistic
/// transaction gets when it activates (`OptimisticTxnContextProvider.
/// onTxnActive`).
#[must_use]
pub fn is_optimistic_txn_retryable(
    vars: &dyn ProviderSessionVars,
    tp: EnterNewTxnType,
    is_pipelined: bool,
) -> bool {
    if tp == EnterNewTxnType::Default {
        return false;
    }
    if is_pipelined {
        return false;
    }
    // If retry limit is 0, the transaction could not retry.
    if vars.retry_limit() == 0 {
        return false;
    }
    // When `@@tidb_snapshot` is set, it is a ready-only statement and will
    // not cause the errors that should retry a transaction in optimistic mode.
    if vars.snapshot_ts() != 0 {
        return false;
    }
    // If the session is not InTxn, it is an auto-committed transaction. The
    // auto-committed transaction could always retry.
    if !vars.in_txn() {
        return true;
    }
    // The internal transaction could always retry.
    if vars.in_restricted_sql() {
        return true;
    }
    // If the retry is enabled, the transaction could retry.
    !vars.disable_txn_auto_retry()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Clone, Copy)]
    struct Vars {
        retry_limit: i64,
        snapshot_ts: u64,
        in_txn: bool,
        in_restricted_sql: bool,
        disable_txn_auto_retry: bool,
    }

    impl ProviderSessionVars for Vars {
        fn retry_limit(&self) -> i64 {
            self.retry_limit
        }
        fn snapshot_ts(&self) -> u64 {
            self.snapshot_ts
        }
        fn in_txn(&self) -> bool {
            self.in_txn
        }
        fn in_restricted_sql(&self) -> bool {
            self.in_restricted_sql
        }
        fn disable_txn_auto_retry(&self) -> bool {
            self.disable_txn_auto_retry
        }
    }

    /// Every branch of Go's `isOptimisticTxnRetryable`, in its order.
    #[test]
    fn is_optimistic_txn_retryable_follows_gos_branches() {
        let autocommit = Vars {
            retry_limit: 10,
            snapshot_ts: 0,
            in_txn: false,
            in_restricted_sql: false,
            disable_txn_auto_retry: true,
        };
        let before = EnterNewTxnType::BeforeStmt;
        assert!(is_optimistic_txn_retryable(&autocommit, before, false));
        assert!(!is_optimistic_txn_retryable(&autocommit, EnterNewTxnType::Default, false));
        assert!(!is_optimistic_txn_retryable(&autocommit, before, true));
        assert!(!is_optimistic_txn_retryable(&Vars { retry_limit: 0, ..autocommit }, before, false));
        assert!(!is_optimistic_txn_retryable(&Vars { snapshot_ts: 7, ..autocommit }, before, false));

        let explicit = Vars { in_txn: true, ..autocommit };
        let begin = EnterNewTxnType::WithBeginStmt;
        assert!(!is_optimistic_txn_retryable(&explicit, begin, false));
        assert!(is_optimistic_txn_retryable(&Vars { in_restricted_sql: true, ..explicit }, begin, false));
        assert!(is_optimistic_txn_retryable(&Vars { disable_txn_auto_retry: false, ..explicit }, begin, false));
    }
}

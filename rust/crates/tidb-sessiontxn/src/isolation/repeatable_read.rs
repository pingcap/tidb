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

//! Go `pkg/sessiontxn/isolation/repeatable_read.go`.

use super::txn_start_ts;
use crate::{no_idea, retry_ready, StmtErrorAction, StmtErrorHandlePoint, TxnContext, TxnContextProvider};
use tidb_domain_core::cluster_transactions::OpenClusterTransaction;
use tidb_planner_core_operator_physicalop::PhysicalPlan;
use tidb_kv::cluster_storage::ClusterSnapshot;
use tidb_store_driver_error::sql_query_error::SqlQueryError;

/// Go `PessimisticRRTxnContextProvider`: reads at the start timestamp; a DML
/// or locking read at a fresh for-update timestamp, resolved at its first
/// read or lock (Go `getForUpdateTs`), unless the statement is retried, which
/// reads at the timestamp the lock step advanced to.
pub struct PessimisticRRTxnContextProvider {
    txn: Box<dyn OpenClusterTransaction>,
    /// The for-update timestamp `OnStmtRetry` fetched for the current
    /// statement's retry.
    retry_ts: Option<u64>,
}

impl PessimisticRRTxnContextProvider {
    /// The provider of a pessimistic REPEATABLE-READ transaction.
    #[must_use]
    pub fn new(txn: Box<dyn OpenClusterTransaction>) -> Self {
        Self { txn, retry_ts: None }
    }

    fn retry_snapshot(&self, locking: bool) -> Option<Result<Box<dyn ClusterSnapshot>, SqlQueryError>> {
        self.retry_ts.map(|ts| self.txn.snapshot_at_for(ts, locking).map_err(SqlQueryError::unknown))
    }
}

impl TxnContextProvider for PessimisticRRTxnContextProvider {
    fn txn(&self) -> &dyn OpenClusterTransaction {
        &*self.txn
    }

    fn into_txn(self: Box<Self>) -> Box<dyn OpenClusterTransaction> {
        self.txn
    }

    // Go `basePessimisticTxnContextProvider.OnPessimisticStmtStart`.
    fn on_pessimistic_stmt_start(&mut self, cx: &mut dyn TxnContext) -> Result<(), SqlQueryError> {
        self.txn.set_fair_locking(cx.pessimistic_fair_locking());
        Ok(())
    }

    // Go `basePessimisticTxnContextProvider.OnPessimisticStmtEnd`.
    fn on_pessimistic_stmt_end(
        &mut self,
        _cx: &mut dyn TxnContext,
        is_successful: bool,
    ) -> Result<(), SqlQueryError> {
        self.txn
            .finish_pessimistic_statement(is_successful)
            .map_err(SqlQueryError::unknown)
    }

    fn get_stmt_read_ts(&mut self, _cx: &mut dyn TxnContext) -> Result<u64, SqlQueryError> {
        txn_start_ts(&*self.txn)
    }

    fn get_stmt_for_update_ts(&mut self, _cx: &mut dyn TxnContext) -> Result<u64, SqlQueryError> {
        txn_start_ts(&*self.txn)?;
        Ok(self.txn.for_update_ts())
    }

    fn get_snapshot_with_stmt_read_ts(
        &mut self,
        cx: &mut dyn TxnContext,
    ) -> Result<Box<dyn ClusterSnapshot>, SqlQueryError> {
        let locking = cx.locking_read();
        self.retry_snapshot(locking).unwrap_or_else(|| {
            self.txn.snapshot_for(locking).map_err(SqlQueryError::unknown)
        })
    }

    fn get_snapshot_with_stmt_for_update_ts(
        &mut self,
        cx: &mut dyn TxnContext,
    ) -> Result<Box<dyn ClusterSnapshot>, SqlQueryError> {
        let locking = cx.locking_read();
        self.retry_snapshot(locking).unwrap_or_else(|| {
            self.txn
                .deferred_statement_snapshot(locking)
                .map_err(SqlQueryError::unknown)
        })
    }

    fn plan_advisor(
        &self,
        _retrying: bool,
        _rc_write_check_ts: bool,
    ) -> Option<std::sync::Arc<dyn tidb_sessionctx::TxnPlanAdvisor>> {
        Some(std::sync::Arc::new(RRPlanAdvisor))
    }

    fn on_stmt_start(
        &mut self,
        _cx: &mut dyn TxnContext,
        _node: Option<&tidb_ast::Stmt>,
    ) -> Result<(), SqlQueryError> {
        self.retry_ts = None;
        Ok(())
    }

    // Go `updateForUpdateTS` (RC: `prepareStmt`'s fresh statement future).
    fn on_stmt_retry(&mut self, _cx: &mut dyn TxnContext) -> Result<(), SqlQueryError> {
        self.retry_ts = Some(self.txn.advance_for_update_ts().map_err(SqlQueryError::unknown)?);
        Ok(())
    }

    /// Go `handleAfterPessimisticLockError`: a write conflict or a
    /// retryable single-statement deadlock retries the statement; the lock
    /// step reports every other lock error to the statement directly, as the
    /// `ErrorAction` Go answers for it.
    fn on_stmt_error_for_next_action(
        &mut self,
        _cx: &mut dyn TxnContext,
        point: StmtErrorHandlePoint,
        _error: SqlQueryError,
    ) -> Result<StmtErrorAction, SqlQueryError> {
        match point {
            StmtErrorHandlePoint::AfterPessimisticLock => retry_ready(),
            StmtErrorHandlePoint::AfterQuery => no_idea(),
        }
    }

}

/// Go `PessimisticRRTxnContextProvider.AdviseOptimizeWithPlan`: a point get
/// (or a write whose reads are all point gets) keeps the current for-update
/// timestamp (`optimizeForNotFetchingLatestTS`).
#[derive(Debug)]
pub struct RRPlanAdvisor;

impl tidb_sessionctx::TxnPlanAdvisor for RRPlanAdvisor {
    fn advise_optimize_with_plan(
        &self,
        plan: &dyn std::any::Any,
        _insert_replace_or_on_duplicate: bool,
    ) -> Option<bool> {
        let plan = plan.downcast_ref::<PhysicalPlan>()?;
        not_need_get_latest_ts_from_pd(plan, false).then_some(false)
    }
}

/// Go `notNeedGetLatestTSFromPD`.
pub fn not_need_get_latest_ts_from_pd(plan: &PhysicalPlan, in_lock_or_write_stmt: bool) -> bool {
    match plan {
        PhysicalPlan::PointGet(point) => !in_lock_or_write_stmt || point.lock,
        PhysicalPlan::BatchPointGet(batch) => !in_lock_or_write_stmt || batch.lock,
        PhysicalPlan::Dml(root) => match root.go_operator.as_str() {
            "Insert" => root.select_plan.is_none(),
            _ => root
                .select_plan
                .as_deref()
                .is_some_and(|select| not_need_get_latest_ts_from_pd(select, true)),
        },
        _ => {
            let children = plan.children();
            let is_physical_lock = matches!(plan, PhysicalPlan::Lock(_));
            !children.is_empty()
                && children.iter().all(|child| {
                    not_need_get_latest_ts_from_pd(child, is_physical_lock || in_lock_or_write_stmt)
                })
        }
    }
}

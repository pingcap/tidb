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

//! Go `pkg/sessiontxn/isolation/readcommitted.go`.

use super::txn_start_ts;
use crate::{no_idea, retry_ready, StmtErrorAction, StmtErrorHandlePoint, TxnContext, TxnContextProvider};
use tidb_domain_core::cluster_transactions::OpenClusterTransaction;
use tidb_planner_core_operator_physicalop::PhysicalPlan;
use tidb_kv::cluster_storage::ClusterSnapshot;
use tidb_store_driver_error::pessimistic_lock_error::ERR_WRITE_CONFLICT;
use tidb_store_driver_error::sql_query_error::SqlQueryError;

/// Go `PessimisticRCTxnContextProvider`: every statement reads at its own
/// timestamp (`getStmtTS`), a future its first read or lock resolves --
/// under `RCCheckTS` the latest one the transaction fetched
/// (`prepareStmtTS`'s `latestOracleTS`), and after a retry the one the lock
/// step advanced to.
pub struct PessimisticRCTxnContextProvider {
    txn: Box<dyn OpenClusterTransaction>,
    /// The for-update timestamp `OnStmtRetry` fetched for the current
    /// statement's retry.
    retry_ts: Option<u64>,
}

impl PessimisticRCTxnContextProvider {
    /// The provider of a pessimistic READ-COMMITTED transaction.
    #[must_use]
    pub fn new(txn: Box<dyn OpenClusterTransaction>) -> Self {
        Self { txn, retry_ts: None }
    }

    /// Go `GetSnapshotWithStmtReadTS` and `GetSnapshotWithStmtForUpdateTS`
    /// both read at `getStmtTS`.
    fn stmt_snapshot(&self, cx: &dyn TxnContext) -> Result<Box<dyn ClusterSnapshot>, SqlQueryError> {
        let locking = cx.locking_read();
        if let Some(ts) = self.retry_ts {
            self.txn.snapshot_at_for(ts, locking)
        } else if cx.rc_check_ts() {
            self.txn.snapshot_at_for(self.txn.for_update_ts(), locking)
        } else {
            self.txn.deferred_statement_snapshot(locking)
        }
        .map_err(SqlQueryError::unknown)
    }
}

impl TxnContextProvider for PessimisticRCTxnContextProvider {
    fn txn(&self) -> &dyn OpenClusterTransaction {
        &*self.txn
    }

    fn into_txn(self: Box<Self>) -> Box<dyn OpenClusterTransaction> {
        self.txn
    }

    fn get_stmt_read_ts(&mut self, _cx: &mut dyn TxnContext) -> Result<u64, SqlQueryError> {
        txn_start_ts(&*self.txn)?;
        Ok(self.txn.for_update_ts())
    }

    fn get_stmt_for_update_ts(&mut self, _cx: &mut dyn TxnContext) -> Result<u64, SqlQueryError> {
        txn_start_ts(&*self.txn)?;
        Ok(self.txn.for_update_ts())
    }

    fn get_snapshot_with_stmt_read_ts(
        &mut self,
        cx: &mut dyn TxnContext,
    ) -> Result<Box<dyn ClusterSnapshot>, SqlQueryError> {
        self.stmt_snapshot(cx)
    }

    fn get_snapshot_with_stmt_for_update_ts(
        &mut self,
        cx: &mut dyn TxnContext,
    ) -> Result<Box<dyn ClusterSnapshot>, SqlQueryError> {
        self.stmt_snapshot(cx)
    }

    // Go: "Try to mark the `RCCheckTS` flag for the first time execution of
    // in-transaction read requests using read-consistency isolation level."
    fn plan_advisor(
        &self,
        retrying: bool,
        rc_write_check_ts: bool,
    ) -> Option<std::sync::Arc<dyn tidb_sessionctx::TxnPlanAdvisor>> {
        (!retrying).then(|| std::sync::Arc::new(RCPlanAdvisor { rc_write_check_ts }) as _)
    }

    fn on_stmt_start(
        &mut self,
        cx: &mut dyn TxnContext,
        node: Option<&tidb_ast::Stmt>,
    ) -> Result<(), SqlQueryError> {
        if node.is_some_and(|node| cx.need_set_rc_check_ts_flag(node)) {
            cx.set_rc_check_ts();
        }
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
        cx: &mut dyn TxnContext,
        point: StmtErrorHandlePoint,
        error: SqlQueryError,
    ) -> Result<StmtErrorAction, SqlQueryError> {
        match point {
            StmtErrorHandlePoint::AfterPessimisticLock => retry_ready(),
            // Go `handleAfterQueryError`: a write conflict met by a
            // statement that read with `RCCheckTS` is retried from the
            // beginning.
            StmtErrorHandlePoint::AfterQuery
                if error.code == ERR_WRITE_CONFLICT && cx.rc_check_ts() =>
            {
                retry_ready()
            }
            StmtErrorHandlePoint::AfterQuery => no_idea(),
        }
    }

}

/// Go `PessimisticRCTxnContextProvider.AdviseOptimizeWithPlan` under
/// `tidb_rc_write_check_ts`: the kept timestamp is read with `RCCheckTS`
/// (`checkTSInWriteStmt`).
#[derive(Debug)]
pub struct RCPlanAdvisor {
    /// Go `SessionVars.RcWriteCheckTS`.
    pub rc_write_check_ts: bool,
}

impl tidb_sessionctx::TxnPlanAdvisor for RCPlanAdvisor {
    fn advise_optimize_with_plan(
        &self,
        plan: &dyn std::any::Any,
        insert_replace_or_on_duplicate: bool,
    ) -> Option<bool> {
        let plan = plan.downcast_ref::<PhysicalPlan>()?;
        plan_skip_get_tso_from_pd(plan, self.rc_write_check_ts, insert_replace_or_on_duplicate, false)
            .then_some(true)
    }
}

/// Go `planSkipGetTsoFromPD`.
pub fn plan_skip_get_tso_from_pd(
    plan: &PhysicalPlan,
    rc_write_check_ts: bool,
    insert_replace_or_on_duplicate: bool,
    in_lock_or_write_stmt: bool,
) -> bool {
    match plan {
        PhysicalPlan::PointGet(point) => rc_write_check_ts && (point.lock || in_lock_or_write_stmt),
        PhysicalPlan::Dml(root) => match root.go_operator.as_str() {
            "Insert" => root.select_plan.is_none() && !insert_replace_or_on_duplicate,
            _ => root.select_plan.as_deref().is_some_and(|select| {
                plan_skip_get_tso_from_pd(select, rc_write_check_ts, false, true)
            }),
        },
        _ => {
            let children = plan.children();
            let is_physical_lock = matches!(plan, PhysicalPlan::Lock(_));
            !children.is_empty()
                && children.iter().all(|child| {
                    plan_skip_get_tso_from_pd(
                        child,
                        rc_write_check_ts,
                        false,
                        is_physical_lock || in_lock_or_write_stmt,
                    )
                })
        }
    }
}

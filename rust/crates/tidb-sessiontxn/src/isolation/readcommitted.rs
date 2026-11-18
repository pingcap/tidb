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
use tidb_kv::cluster_storage::ClusterSnapshot;
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

    fn on_stmt_start(&mut self, _cx: &mut dyn TxnContext) -> Result<(), SqlQueryError> {
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

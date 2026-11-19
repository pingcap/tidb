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

//! Go `pkg/sessiontxn/isolation/serializable.go`.

use super::txn_start_ts;
use crate::{error_action, no_idea, StmtErrorAction, StmtErrorHandlePoint, TxnContext, TxnContextProvider};
use tidb_domain_core::cluster_transactions::OpenClusterTransaction;
use tidb_kv::cluster_storage::ClusterSnapshot;
use tidb_store_driver_error::sql_query_error::SqlQueryError;

/// Go `PessimisticSerializableTxnContextProvider`: Oracle-like
/// serializable, snapshot isolation in pessimistic mode. Every statement
/// reads and locks at the transaction's start timestamp, and a pessimistic
/// lock error is returned rather than retried.
pub struct PessimisticSerializableTxnContextProvider {
    txn: Box<dyn OpenClusterTransaction>,
}

impl PessimisticSerializableTxnContextProvider {
    /// The provider of a pessimistic SERIALIZABLE transaction.
    #[must_use]
    pub fn new(txn: Box<dyn OpenClusterTransaction>) -> Self {
        Self { txn }
    }
}

impl TxnContextProvider for PessimisticSerializableTxnContextProvider {
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

    // Go `getStmtReadTSFunc = getStmtForUpdateTSFunc = getTxnStartTS`.
    fn get_stmt_read_ts(&mut self, _cx: &mut dyn TxnContext) -> Result<u64, SqlQueryError> {
        txn_start_ts(&*self.txn)
    }

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

    /// "In oracle-like serializable isolation, we do not retry encountering
    /// pessimistic lock error."
    fn on_stmt_error_for_next_action(
        &mut self,
        _cx: &mut dyn TxnContext,
        point: StmtErrorHandlePoint,
        error: SqlQueryError,
    ) -> Result<StmtErrorAction, SqlQueryError> {
        match point {
            StmtErrorHandlePoint::AfterPessimisticLock => error_action(error),
            StmtErrorHandlePoint::AfterQuery => no_idea(),
        }
    }
}

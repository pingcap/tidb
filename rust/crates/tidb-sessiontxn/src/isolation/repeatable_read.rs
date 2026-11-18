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
use crate::{TxnContext, TxnContextProvider};
use tidb_domain_core::cluster_transactions::OpenClusterTransaction;
use tidb_kv::cluster_storage::ClusterSnapshot;
use tidb_store_driver_error::sql_query_error::SqlQueryError;

/// Go `PessimisticRRTxnContextProvider`: reads at the start timestamp; a DML
/// or locking read at a fresh for-update timestamp, resolved at its first
/// read or lock (Go `getForUpdateTs`), unless the statement is retried, which
/// reads at the timestamp the lock step advanced to.
pub struct PessimisticRRTxnContextProvider {
    txn: Box<dyn OpenClusterTransaction>,
    /// The for-update timestamp of the current statement's retry (Go
    /// `OnStmtRetry`).
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

    fn on_stmt_start(&mut self, _cx: &mut dyn TxnContext) -> Result<(), SqlQueryError> {
        self.retry_ts = None;
        Ok(())
    }

    fn on_stmt_retry(&mut self, _cx: &mut dyn TxnContext, for_update_ts: u64) -> Result<(), SqlQueryError> {
        self.retry_ts = Some(for_update_ts);
        Ok(())
    }
}

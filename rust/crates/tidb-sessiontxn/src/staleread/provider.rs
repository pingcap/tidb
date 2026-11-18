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

//! Go `pkg/sessiontxn/staleread/provider.go`.

use crate::isolation::txn_start_ts;
use crate::{TxnContext, TxnContextProvider};
use tidb_domain_core::cluster_transactions::OpenClusterTransaction;
use tidb_kv::cluster_storage::ClusterSnapshot;
use tidb_store_driver_error::sql_query_error::SqlQueryError;

/// Go `StalenessTxnContextProvider`: a read-only transaction whose start
/// timestamp is the as-of timestamp; every statement reads there.
pub struct StalenessTxnContextProvider {
    txn: Box<dyn OpenClusterTransaction>,
}

impl StalenessTxnContextProvider {
    /// The provider of a stale read at `txn`'s (constant) start timestamp.
    #[must_use]
    pub fn new(txn: Box<dyn OpenClusterTransaction>) -> Self {
        Self { txn }
    }
}

impl TxnContextProvider for StalenessTxnContextProvider {
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
        Err(SqlQueryError::unknown(
            "GetForUpdateTS not supported for stalenessTxnProvider",
        ))
    }

    fn get_snapshot_with_stmt_read_ts(
        &mut self,
        _cx: &mut dyn TxnContext,
    ) -> Result<Box<dyn ClusterSnapshot>, SqlQueryError> {
        self.txn.snapshot().map_err(SqlQueryError::unknown)
    }

    fn get_snapshot_with_stmt_for_update_ts(
        &mut self,
        _cx: &mut dyn TxnContext,
    ) -> Result<Box<dyn ClusterSnapshot>, SqlQueryError> {
        Err(SqlQueryError::unknown(
            "GetSnapshotWithStmtForUpdateTS not supported for stalenessTxnProvider",
        ))
    }
}

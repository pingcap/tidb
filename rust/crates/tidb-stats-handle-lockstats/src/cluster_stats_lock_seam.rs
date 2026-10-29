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

//! The convergence-node route for Go's internal lockstats transaction.

use std::sync::Arc;
use std::time::Duration;

use crate::lock_target::ClusterStatsLockStatement;
use crate::real_tikv_stats_lock::{commit_cluster_stats_lock, ClusterStatsLockReport};
use tidb_txnkv::transaction::{RealOptimisticTransactionOpener, StoreClient};

use crate::real_tikv_stats_lock::cluster_stats_lock_error;
use tidb_store_driver::sql_query_error::SqlQueryError;

/// The one persisted-statistics-lock operation exposed to a cluster session.
pub trait ClusterStatsLock: Send + Sync {
    /// Executes one `LOCK STATS` or `UNLOCK STATS` internal transaction.
    fn execute(
        &self,
        statement: &ClusterStatsLockStatement,
    ) -> Result<ClusterStatsLockReport, SqlQueryError>;
}

/// The production TiKV-backed lockstats transaction.
pub struct RealClusterStatsLock<S = tikv_client::PdRpcClient>
where
    S: StoreClient,
{
    opener: Arc<RealOptimisticTransactionOpener<S>>,
    timeout: Duration,
}

impl<S> RealClusterStatsLock<S>
where
    S: StoreClient,
{
    /// Binds the route to an already-connected transaction authority.
    #[must_use]
    pub fn new(opener: Arc<RealOptimisticTransactionOpener<S>>, timeout: Duration) -> Self {
        Self { opener, timeout }
    }
}

impl<S> ClusterStatsLock for RealClusterStatsLock<S>
where
    S: StoreClient,
{
    fn execute(
        &self,
        statement: &ClusterStatsLockStatement,
    ) -> Result<ClusterStatsLockReport, SqlQueryError> {
        commit_cluster_stats_lock(&self.opener, statement, self.timeout)
            .map_err(cluster_stats_lock_error)
    }
}

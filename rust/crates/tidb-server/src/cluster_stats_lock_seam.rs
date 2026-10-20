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

use tidb_exec::cluster_stats_lock::ClusterStatsLockStatement;
use tidb_exec::real_tikv_stats_lock::{commit_cluster_stats_lock, ClusterStatsLockReport};
use tidb_pd_client::PdClient;
use tidb_txnkv::transaction::{RealOptimisticTransactionOpener, StoreClient, StorePdCapability};

use crate::sql_node::{cluster_stats_lock_error, SqlQueryError};

/// The one persisted-statistics-lock operation exposed to a cluster session.
pub trait ClusterStatsLock: Send + Sync {
    /// Executes one `LOCK STATS` or `UNLOCK STATS` internal transaction.
    fn execute(
        &self,
        statement: &ClusterStatsLockStatement,
    ) -> Result<ClusterStatsLockReport, SqlQueryError>;
}

/// The production TiKV-backed lockstats transaction.
pub struct RealClusterStatsLock<S = tikv_client::PdRpcClient, P = PdClient>
where
    S: StoreClient,
    P: StorePdCapability,
{
    opener: Arc<RealOptimisticTransactionOpener<S, P>>,
    timeout: Duration,
}

impl<S, P> RealClusterStatsLock<S, P>
where
    S: StoreClient,
    P: StorePdCapability,
{
    /// Binds the route to an already-connected transaction authority.
    #[must_use]
    pub fn new(opener: Arc<RealOptimisticTransactionOpener<S, P>>, timeout: Duration) -> Self {
        Self { opener, timeout }
    }
}

impl<S, P> ClusterStatsLock for RealClusterStatsLock<S, P>
where
    S: StoreClient,
    P: StorePdCapability,
{
    fn execute(
        &self,
        statement: &ClusterStatsLockStatement,
    ) -> Result<ClusterStatsLockReport, SqlQueryError> {
        commit_cluster_stats_lock(&self.opener, statement, self.timeout)
            .map_err(cluster_stats_lock_error)
    }
}

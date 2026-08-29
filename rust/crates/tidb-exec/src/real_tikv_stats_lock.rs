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

//! One cluster-backed `LOCK STATS` or `UNLOCK STATS` internal transaction.

use std::fmt;
use std::time::Duration;

use tidb_txnkv::rpc::UnaryCallContext;
use tidb_txnkv::transaction::{
    OptimisticCommitOutcome, RealOptimisticTransactionOpener, StorePdCapability, StoreWriteClient,
    StoreWriteLoader, MAX_OPTIMISTIC_TRANSACTION_BYTES,
};

use crate::cluster_catalog::load_cluster_catalog;
use crate::cluster_stats_lock::{
    plan_cluster_stats_lock, ClusterStatsLockError, ClusterStatsLockStatement,
};
use crate::mysql_bootstrap::utc_now_timestamp;
use crate::pessimistic_lock_error::{commit_outcome_to_sql_error, LockSqlError};
use crate::real_tikv_catalog::TransactionMetaSnapshot;

/// What one committed statistics-lock statement reports to the session.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ClusterStatsLockReport {
    /// Go's skipped-target warning, empty when every requested target changed.
    pub warning: String,
}

/// Why the internal lockstats transaction did not complete.
#[derive(Debug)]
pub enum ClusterStatsLockCommitError {
    /// TiKV returned no determinate commit verdict.
    Undetermined(String),
    /// A determinate transaction failure with its SQL error identity.
    Commit(LockSqlError),
    /// A statement target could not be resolved or admitted.
    Plan(ClusterStatsLockError),
    /// Planning, catalog, or transport failure before a commit verdict.
    Other(String),
}

impl fmt::Display for ClusterStatsLockCommitError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Undetermined(detail) => {
                write!(formatter, "execution result undetermined: {detail}")
            }
            Self::Commit(error) => formatter.write_str(&error.message),
            Self::Plan(error) => error.fmt(formatter),
            Self::Other(detail) => formatter.write_str(detail),
        }
    }
}

impl std::error::Error for ClusterStatsLockCommitError {}

/// Runs Go `pkg/statistics/handle/lockstats` against the cluster tables in one
/// transaction independent of the caller's user transaction.
pub fn commit_cluster_stats_lock<C: StoreWriteClient, L: StoreWriteLoader, P: StorePdCapability>(
    opener: &RealOptimisticTransactionOpener<C, L, P>,
    statement: &ClusterStatsLockStatement,
    timeout: Duration,
) -> Result<ClusterStatsLockReport, ClusterStatsLockCommitError> {
    // Go's restricted SQL transaction has no package-specific mutation-count
    // limit. A table target includes every partition in this one transaction.
    let mut transaction = opener
        .begin(usize::MAX, MAX_OPTIMISTIC_TRANSACTION_BYTES)
        .map_err(|error| ClusterStatsLockCommitError::Other(error.to_string()))?;
    let start_ts = transaction.start_ts();
    let plan = {
        let mut snapshot = TransactionMetaSnapshot::new(&mut transaction, timeout);
        let catalog = load_cluster_catalog(&mut snapshot)
            .map_err(|error| ClusterStatsLockCommitError::Other(error.to_string()))?;
        plan_cluster_stats_lock(
            &mut snapshot,
            &catalog,
            statement,
            start_ts,
            utc_now_timestamp(),
        )
        .map_err(ClusterStatsLockCommitError::Plan)?
    };
    let report = ClusterStatsLockReport {
        warning: plan.warning,
    };
    if plan.mutations.is_empty() {
        transaction
            .finish_without_writes()
            .map_err(|error| ClusterStatsLockCommitError::Other(error.to_string()))?;
        return Ok(report);
    }
    let outcome = transaction
        .commit(plan.mutations, &UnaryCallContext::with_timeout(timeout))
        .map_err(|error| ClusterStatsLockCommitError::Other(error.to_string()))?;
    classify_commit_outcome(&outcome)?;
    Ok(report)
}

fn classify_commit_outcome(
    outcome: &OptimisticCommitOutcome,
) -> Result<(), ClusterStatsLockCommitError> {
    match commit_outcome_to_sql_error(outcome) {
        Ok(()) => Ok(()),
        Err(error) if error.is_result_undetermined() => {
            Err(ClusterStatsLockCommitError::Undetermined(
                "the statistics lock transaction returned no commit verdict".to_owned(),
            ))
        }
        Err(error) => Err(ClusterStatsLockCommitError::Commit(error)),
    }
}

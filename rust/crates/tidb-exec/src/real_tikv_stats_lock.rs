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

use std::collections::BTreeSet;
use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use tidb_datatype::Time;
use tidb_executor::cluster_storage::MutationBuffer;
use tidb_stats::StatsLockTransaction;
use tidb_txnkv::transaction::{
    OptimisticMutation, RealOptimisticTransactionOpener, StorePdCapability, StoreWriteClient,
    StoreWriteLoader, MAX_OPTIMISTIC_TRANSACTION_BYTES,
};

use crate::cluster_catalog::load_cluster_catalog;
use crate::cluster_stats_lock::{
    apply_cluster_stats_lock, plan_cluster_delete_lock, plan_cluster_insert_lock,
    plan_cluster_update_meta_delta, plan_cluster_update_meta_version, query_cluster_lock_delta,
    query_cluster_locked_tables, ClusterStatsLockApplyError, ClusterStatsLockError,
    ClusterStatsLockStatement,
};
use crate::cluster_table_storage::{
    lock_pessimistic_statement, overlay_staged_mutations, PessimisticStatementTransactionError,
    SessionTransaction,
};
use crate::mysql_bootstrap::utc_now_timestamp;
use crate::pessimistic_lock_error::LockSqlError;
use crate::real_tikv_catalog::SnapshotMetaSnapshot;

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
    let transaction = SessionTransaction::begin_pessimistic_with_budget(
        Arc::new(opener.clone()),
        timeout,
        opener.commit_protocol(),
        usize::MAX,
        MAX_OPTIMISTIC_TRANSACTION_BYTES,
    )
    .map_err(|error| ClusterStatsLockCommitError::Other(error.to_string()))?;
    let staged = MutationBuffer::new();
    let result = (|| {
        let catalog = {
            let snapshot = transaction
                .snapshot()
                .map_err(|error| ClusterStatsLockCommitError::Other(error.to_string()))?;
            let mut snapshot = SnapshotMetaSnapshot::new(snapshot);
            load_cluster_catalog(&mut snapshot)
                .map_err(|error| ClusterStatsLockCommitError::Other(error.to_string()))?
        };
        let mut policy = PessimisticStatsLockTransaction {
            transaction: &transaction,
            staged: &staged,
            catalog: &catalog,
            now: utc_now_timestamp(),
        };
        let warning = apply_cluster_stats_lock(&mut policy, &catalog, statement).map_err(
            |error| match error {
                ClusterStatsLockApplyError::Plan(error) => ClusterStatsLockCommitError::Plan(error),
                ClusterStatsLockApplyError::Transaction(error) => error,
            },
        )?;
        Ok(ClusterStatsLockReport { warning })
    })();
    match result {
        Ok(report) => {
            transaction.commit(&staged).map_err(commit_error)?;
            Ok(report)
        }
        Err(error) => {
            let _ = transaction.rollback();
            Err(error)
        }
    }
}

struct PessimisticStatsLockTransaction<'a, C, L, P>
where
    C: StoreWriteClient,
    L: StoreWriteLoader,
    P: StorePdCapability,
{
    transaction: &'a SessionTransaction<C, L, P>,
    staged: &'a MutationBuffer,
    catalog: &'a crate::cluster_catalog::ClusterCatalog,
    now: Time,
}

impl<C: StoreWriteClient, L: StoreWriteLoader, P: StorePdCapability>
    PessimisticStatsLockTransaction<'_, C, L, P>
{
    fn snapshot(&self) -> Result<SnapshotMetaSnapshot, ClusterStatsLockCommitError> {
        let snapshot = self
            .transaction
            .snapshot()
            .map_err(|error| ClusterStatsLockCommitError::Other(error.to_string()))?;
        Ok(SnapshotMetaSnapshot::new(overlay_staged_mutations(
            snapshot,
            self.staged,
        )))
    }

    fn lock_statement<T>(
        &self,
        mut build: impl FnMut(
            &mut SnapshotMetaSnapshot,
            u64,
        ) -> Result<(T, Vec<OptimisticMutation>), ClusterStatsLockError>,
    ) -> Result<T, ClusterStatsLockCommitError> {
        lock_pessimistic_statement(self.transaction, self.staged, |snapshot, start_ts| {
            let mut snapshot = SnapshotMetaSnapshot::new(snapshot);
            build(&mut snapshot, start_ts).map_err(|error| error.to_string())
        })
        .map_err(pessimistic_error)
    }
}

impl<C: StoreWriteClient, L: StoreWriteLoader, P: StorePdCapability> StatsLockTransaction
    for PessimisticStatsLockTransaction<'_, C, L, P>
{
    type Error = ClusterStatsLockCommitError;

    fn query_locked_tables(&mut self) -> Result<BTreeSet<i64>, Self::Error> {
        query_cluster_locked_tables(&mut self.snapshot()?, self.catalog)
            .map_err(ClusterStatsLockCommitError::Plan)
    }

    fn insert_lock_and_update_meta_version(&mut self, table_id: i64) -> Result<(), Self::Error> {
        self.lock_statement(|snapshot, _| {
            Ok((
                (),
                plan_cluster_insert_lock(snapshot, self.catalog, table_id, self.now)?,
            ))
        })?;
        self.lock_statement(|snapshot, start_ts| {
            Ok((
                (),
                plan_cluster_update_meta_version(snapshot, self.catalog, table_id, start_ts)?,
            ))
        })
    }

    fn lock_delta(&mut self, table_id: i64) -> Result<(i64, i64), Self::Error> {
        query_cluster_lock_delta(&mut self.snapshot()?, self.catalog, table_id)
            .map_err(ClusterStatsLockCommitError::Plan)
    }

    fn update_meta_delta(
        &mut self,
        table_id: i64,
        count_delta: i64,
        modify_count_delta: i64,
    ) -> Result<(), Self::Error> {
        self.lock_statement(|snapshot, start_ts| {
            Ok((
                (),
                plan_cluster_update_meta_delta(
                    snapshot,
                    self.catalog,
                    table_id,
                    count_delta,
                    modify_count_delta,
                    start_ts,
                )?,
            ))
        })
    }

    fn delete_lock(&mut self, table_id: i64) -> Result<(), Self::Error> {
        self.lock_statement(|snapshot, _| {
            Ok((
                (),
                plan_cluster_delete_lock(snapshot, self.catalog, table_id)?,
            ))
        })
    }
}

fn pessimistic_error(error: PessimisticStatementTransactionError) -> ClusterStatsLockCommitError {
    match error {
        PessimisticStatementTransactionError::Build(detail) => {
            ClusterStatsLockCommitError::Other(detail)
        }
        PessimisticStatementTransactionError::Transaction(error) => commit_error(error),
    }
}

fn commit_error(error: LockSqlError) -> ClusterStatsLockCommitError {
    if error.is_result_undetermined() {
        ClusterStatsLockCommitError::Undetermined(
            "the statistics lock transaction returned no commit verdict".to_owned(),
        )
    } else {
        ClusterStatsLockCommitError::Commit(error)
    }
}

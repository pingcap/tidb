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

//! Cluster storage planning for Go `pkg/statistics/handle/lockstats`.

use std::collections::BTreeMap;

use crate::lock_stats::StatsLockTransaction;
use tidb_stats_handle_types::StatsLockTable;

/// Why a `LOCK STATS` / `UNLOCK STATS` statement could not be planned.
#[derive(Debug)]
pub enum ClusterStatsLockError {
    /// The error value Go's `LOCK STATS` / `UNLOCK STATS` planning raises as
    /// it is: `ErrNoDB` for a bare table name with no selected schema,
    /// `ErrTableNotExists`, `ErrUnknownPartition`.
    Mysql(tidb_terror::MysqlError),
    /// Missing table or invalid target.
    Invalid(String),
}

impl std::fmt::Display for ClusterStatsLockError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Mysql(error) => formatter.write_str(&error.message),
            Self::Invalid(detail) => formatter.write_str(detail),
        }
    }
}

impl std::error::Error for ClusterStatsLockError {}

/// A Go error raised as the statement's failure.
impl From<tidb_terror::TerrorError> for ClusterStatsLockError {
    fn from(error: tidb_terror::TerrorError) -> Self {
        Self::Mysql(tidb_terror::MysqlError::from_terror(&error))
    }
}

/// The session-visible error of a refused `LOCK STATS` / `UNLOCK STATS`.
#[must_use]
pub fn cluster_stats_lock_error(
    error: ClusterStatsLockError,
) -> tidb_store_driver_error::sql_query_error::SqlQueryError {
    match error {
        ClusterStatsLockError::Mysql(error) => {
            tidb_store_driver_error::sql_query_error::SqlQueryError::from(error)
        }
        ClusterStatsLockError::Invalid(detail) => {
            tidb_store_driver_error::sql_query_error::SqlQueryError::unknown(detail)
        }
    }
}

/// The tables a `LOCK STATS` / `UNLOCK STATS` statement resolved to: Go's
/// `StatsHandle.LockTables(tables)` / `LockPartitions(tid, tableName,
/// pidNames)` arguments (and their `Unlock` twins).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum StatsLockRequest {
    /// Whole tables, with their partitions (`LockTables` / `RemoveLockedTables`).
    Tables {
        /// `true` for LOCK, `false` for UNLOCK.
        lock: bool,
        /// The tables by physical id.
        tables: BTreeMap<i64, StatsLockTable>,
    },
    /// Named partitions of one table (`LockPartitions` /
    /// `RemoveLockedPartitions`).
    Partitions {
        /// `true` for LOCK, `false` for UNLOCK.
        lock: bool,
        /// The partitioned table's id.
        table_id: i64,
        /// The table's name as the warning displays it.
        displayed: String,
        /// The partitions by physical id, with their names.
        partitions: BTreeMap<i64, String>,
    },
}

/// Drives Go's lockstats policy (`lock_stats`) for a resolved request.
pub(crate) fn apply_stats_lock_request<T: StatsLockTransaction>(
    transaction: &mut T,
    request: &StatsLockRequest,
) -> Result<String, T::Error> {
    match request {
        StatsLockRequest::Partitions {
            lock: true,
            table_id,
            displayed,
            partitions,
        } => {
            crate::lock_stats::add_locked_partitions(transaction, *table_id, displayed, partitions)
        }
        StatsLockRequest::Partitions {
            lock: false,
            table_id,
            displayed,
            partitions,
        } => crate::lock_stats::remove_locked_partitions(
            transaction,
            *table_id,
            displayed,
            partitions,
        ),
        StatsLockRequest::Tables { lock: true, tables } => {
            crate::lock_stats::add_locked_tables(transaction, tables)
        }
        StatsLockRequest::Tables {
            lock: false,
            tables,
        } => crate::lock_stats::remove_locked_tables(transaction, tables),
    }
}

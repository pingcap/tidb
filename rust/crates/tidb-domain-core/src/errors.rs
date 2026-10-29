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

//! The client errors of the domain's cluster services: an analyze, a stats
//! lock or a cluster state change that did not commit, as the connection
//! reports them.

use tidb_store_driver::pessimistic_lock_error::commit_outcome_to_sql_error;
use tidb_store_driver::sql_query_error::{lock_sql_error, SqlQueryError};
use tidb_exec::cluster_stats_lock::ClusterStatsLockError;
use tidb_exec::real_tikv_analyze::ClusterAnalyzeError;
use tidb_exec::real_tikv_stats_lock::ClusterStatsLockCommitError;
use tidb_txnkv::transaction::OptimisticCommitOutcome;

pub fn cluster_analyze_error(error: ClusterAnalyzeError) -> SqlQueryError {
    match error {
        ClusterAnalyzeError::Analyze(error) => {
            let error = tidb_executor::DriverError::from(error).to_mysql_error();
            SqlQueryError::from(error)
        }
        ClusterAnalyzeError::Undetermined(_) => SqlQueryError::result_undetermined(),
        ClusterAnalyzeError::Commit(error) => lock_sql_error(&error),
        ClusterAnalyzeError::Killed(error) => {
            SqlQueryError::from(tidb_error::mysql::MysqlError::from_sql_error(error))
        }
        ClusterAnalyzeError::Other(detail) => SqlQueryError::unknown(detail),
        ClusterAnalyzeError::Mysql(error) => SqlQueryError::from(error),
    }
}

pub fn cluster_stats_lock_error(error: ClusterStatsLockCommitError) -> SqlQueryError {
    match error {
        ClusterStatsLockCommitError::Undetermined(_) => SqlQueryError::result_undetermined(),
        ClusterStatsLockCommitError::Commit(error) => lock_sql_error(&error),
        ClusterStatsLockCommitError::Plan(ClusterStatsLockError::Mysql(error)) => {
            SqlQueryError::from(error)
        }
        ClusterStatsLockCommitError::Plan(error) => SqlQueryError::unknown(error.to_string()),
        ClusterStatsLockCommitError::Other(detail) => SqlQueryError::unknown(detail),
    }
}

/// Returns the client error for a cluster state change that did not commit,
/// preserving the ambiguous outcome instead of reporting a false failure.
pub fn cluster_commit_error(
    outcome: &OptimisticCommitOutcome,
    _subject: &str,
) -> Option<SqlQueryError> {
    commit_outcome_to_sql_error(outcome)
        .err()
        .map(|error| lock_sql_error(&error))
}

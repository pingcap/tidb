// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Pinned Go `LoadStatsFromJSONNoUpdate` storage transactions.
//!
//! This is deliberately not the ANALYZE writer. Go commits every loaded
//! column and index independently, then commits the complete predicate-usage
//! slice, then commits the final table meta update. Historical meta is a later
//! best-effort transaction after each successful item/final-meta write.

use std::fmt;
use std::time::Duration;

use tidb_executor::load_stats::JsonTable;
use tidb_txnkv::pd_capability::CapabilityTimestampSource;
use tidb_txnkv::rpc::UnaryCallContext;
use tidb_txnkv::transaction::{
    OptimisticCommitOutcome, RealOptimisticTransactionOpener, StorePdCapability, StoreWriteClient,
    StoreWriteLoader, MAX_OPTIMISTIC_TRANSACTION_BYTES,
};

use crate::cluster_catalog::load_cluster_catalog;
use crate::cluster_load_stats::{lower_cluster_load_stats, LoadedStatsTable};
use crate::cluster_stats_load::ClusterStatsItem;
use crate::cluster_stats_write::{
    plan_historical_stats_meta_write, plan_loaded_stats_item_write, plan_loaded_stats_meta_write,
    plan_loaded_stats_usage_write, StatsWritePlan,
};
use crate::mysql_bootstrap::utc_now_timestamp;
use crate::pessimistic_lock_error::{commit_outcome_to_sql_error, LockSqlError};
use crate::real_tikv_catalog::TransactionMetaSnapshot;

/// Go `StatsMetaHistorySourceLoadStats`.
pub const LOAD_STATS_HISTORY_SOURCE: &str = "load stats";

/// What durable LOAD STATS storage completed before cache refresh.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ClusterLoadStatsReport {
    /// Physical tables selected by the dump's partition map.
    pub table_count: usize,
    /// Column and index histogram transactions committed.
    pub item_count: usize,
}

/// Why LOAD STATS storage did not complete.
#[derive(Debug)]
pub enum ClusterLoadStatsCommitError {
    /// The commit may have landed, so the server must not report failure.
    Undetermined(String),
    /// A determinate transaction failure with its SQL error identity.
    Commit(LockSqlError),
    /// Resolution, conversion, planning, or transport failed before a verdict.
    Other(String),
}

impl fmt::Display for ClusterLoadStatsCommitError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Undetermined(detail) => {
                write!(formatter, "execution result undetermined: {detail}")
            }
            Self::Commit(error) => formatter.write_str(&error.message),
            Self::Other(detail) => formatter.write_str(detail),
        }
    }
}

impl std::error::Error for ClusterLoadStatsCommitError {}

/// Commits the storage half of pinned Go `LoadStatsFromJSON`.
///
/// `item_history_initialized` supplies Go's `RecordHistoricalStatsMeta(...,
/// enforce=false)` cache gate for column/index transactions. Final meta uses
/// `enforce=true` and therefore does not consult it. History failures remain
/// nonfatal, exactly as the Go handle logs and ignores them.
pub fn commit_cluster_load_stats<C: StoreWriteClient, L: StoreWriteLoader, P: StorePdCapability>(
    opener: &RealOptimisticTransactionOpener<C, L, P>,
    json: &JsonTable,
    timeout: Duration,
    historical_stats_enabled: bool,
    item_history_initialized: impl Fn(i64) -> bool,
) -> Result<ClusterLoadStatsReport, ClusterLoadStatsCommitError> {
    let tables = lower_tables(opener, json, timeout)?;
    let mut report = ClusterLoadStatsReport {
        table_count: tables.len(),
        item_count: 0,
    };
    for table in &tables {
        for item in table.columns.iter().chain(&table.indexes) {
            let version = commit_item(opener, table, item, timeout)?;
            report.item_count += 1;
            if historical_stats_enabled && item_history_initialized(table.physical_id) {
                record_history(opener, table.physical_id, version, timeout);
            }
        }
        commit_usage(opener, table, timeout)?;
        let version = commit_final_meta(opener, table, timeout)?;
        if historical_stats_enabled {
            record_history(opener, table.physical_id, version, timeout);
        }
    }
    Ok(report)
}

fn lower_tables<C: StoreWriteClient, L: StoreWriteLoader, P: StorePdCapability>(
    opener: &RealOptimisticTransactionOpener<C, L, P>,
    json: &JsonTable,
    timeout: Duration,
) -> Result<Vec<LoadedStatsTable>, ClusterLoadStatsCommitError> {
    let mut transaction = opener.begin_read_only().map_err(other)?;
    let tables = {
        let mut snapshot = TransactionMetaSnapshot::new(&mut transaction, timeout);
        let catalog = load_cluster_catalog(&mut snapshot).map_err(other)?;
        lower_cluster_load_stats(&catalog, json).map_err(other)?
    };
    transaction.finish_without_writes().map_err(other)?;
    Ok(tables)
}

fn commit_item<C: StoreWriteClient, L: StoreWriteLoader, P: StorePdCapability>(
    opener: &RealOptimisticTransactionOpener<C, L, P>,
    table: &LoadedStatsTable,
    item: &ClusterStatsItem,
    timeout: Duration,
) -> Result<u64, ClusterLoadStatsCommitError> {
    let mut transaction = begin(opener)?;
    let version = transaction.start_ts();
    let plan = {
        let mut snapshot = TransactionMetaSnapshot::new(&mut transaction, timeout);
        let catalog = load_cluster_catalog(&mut snapshot).map_err(other)?;
        plan_loaded_stats_item_write(
            &mut snapshot,
            &catalog,
            table.physical_id,
            table.count,
            item,
            version,
            utc_now_timestamp(),
        )
        .map_err(other)?
    };
    commit_plan(transaction, plan, timeout, "LOAD STATS item")?;
    Ok(version)
}

fn commit_usage<C: StoreWriteClient, L: StoreWriteLoader, P: StorePdCapability>(
    opener: &RealOptimisticTransactionOpener<C, L, P>,
    table: &LoadedStatsTable,
    timeout: Duration,
) -> Result<(), ClusterLoadStatsCommitError> {
    let mut transaction = begin(opener)?;
    let plan = {
        let mut snapshot = TransactionMetaSnapshot::new(&mut transaction, timeout);
        let catalog = load_cluster_catalog(&mut snapshot).map_err(other)?;
        plan_loaded_stats_usage_write(
            &mut snapshot,
            &catalog,
            table.physical_id,
            &table.predicate_columns,
            utc_now_timestamp(),
        )
        .map_err(other)?
    };
    commit_plan(transaction, plan, timeout, "LOAD STATS usage")
}

fn commit_final_meta<C: StoreWriteClient, L: StoreWriteLoader, P: StorePdCapability>(
    opener: &RealOptimisticTransactionOpener<C, L, P>,
    table: &LoadedStatsTable,
    timeout: Duration,
) -> Result<u64, ClusterLoadStatsCommitError> {
    let mut transaction = begin(opener)?;
    let version = transaction.start_ts();
    let plan = {
        let mut snapshot = TransactionMetaSnapshot::new(&mut transaction, timeout);
        let catalog = load_cluster_catalog(&mut snapshot).map_err(other)?;
        plan_loaded_stats_meta_write(
            &mut snapshot,
            &catalog,
            table.physical_id,
            table.count,
            table.modify_count,
            version,
            utc_now_timestamp(),
        )
        .map_err(other)?
    };
    commit_plan(transaction, plan, timeout, "LOAD STATS meta")?;
    Ok(version)
}

fn record_history<C: StoreWriteClient, L: StoreWriteLoader, P: StorePdCapability>(
    opener: &RealOptimisticTransactionOpener<C, L, P>,
    table_id: i64,
    version: u64,
    timeout: Duration,
) {
    let result = (|| {
        let mut transaction = begin(opener)?;
        let plan = {
            let mut snapshot = TransactionMetaSnapshot::new(&mut transaction, timeout);
            let catalog = load_cluster_catalog(&mut snapshot).map_err(other)?;
            plan_historical_stats_meta_write(
                &mut snapshot,
                &catalog,
                table_id,
                version,
                LOAD_STATS_HISTORY_SOURCE,
                utc_now_timestamp(),
            )
            .map_err(other)?
        };
        commit_plan(transaction, plan, timeout, "LOAD STATS history")
    })();
    let _: Result<(), ClusterLoadStatsCommitError> = result;
}

fn begin<C: StoreWriteClient, L: StoreWriteLoader, P: StorePdCapability>(
    opener: &RealOptimisticTransactionOpener<C, L, P>,
) -> Result<
    tidb_txnkv::transaction::RealOptimisticTransaction<C, L, CapabilityTimestampSource<P>>,
    ClusterLoadStatsCommitError,
> {
    opener
        .begin(usize::MAX, MAX_OPTIMISTIC_TRANSACTION_BYTES)
        .map_err(other)
}

fn commit_plan<C: StoreWriteClient, L: StoreWriteLoader, P: StorePdCapability>(
    transaction: tidb_txnkv::transaction::RealOptimisticTransaction<
        C,
        L,
        CapabilityTimestampSource<P>,
    >,
    plan: StatsWritePlan,
    timeout: Duration,
    operation: &str,
) -> Result<(), ClusterLoadStatsCommitError> {
    if plan.is_empty() {
        transaction.finish_without_writes().map_err(other)?;
        return Ok(());
    }
    let outcome = transaction
        .commit(plan.mutations, &UnaryCallContext::with_timeout(timeout))
        .map_err(other)?;
    classify_commit_outcome(&outcome, operation)
}

fn classify_commit_outcome(
    outcome: &OptimisticCommitOutcome,
    operation: &str,
) -> Result<(), ClusterLoadStatsCommitError> {
    match commit_outcome_to_sql_error(outcome) {
        Ok(()) => Ok(()),
        Err(error) if error.is_result_undetermined() => {
            Err(ClusterLoadStatsCommitError::Undetermined(format!(
                "the {operation} transaction returned no commit verdict"
            )))
        }
        Err(error) => Err(ClusterLoadStatsCommitError::Commit(error)),
    }
}

fn other(error: impl ToString) -> ClusterLoadStatsCommitError {
    ClusterLoadStatsCommitError::Other(error.to_string())
}

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
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use tidb_executor::load_stats::JsonTable;
use tidb_txnkv::transaction::{
    RealOptimisticTransactionOpener, StorePdCapability, StoreWriteClient, StoreWriteLoader,
    MAX_OPTIMISTIC_TRANSACTION_BYTES,
};

use crate::cluster_catalog::load_cluster_catalog;
use crate::cluster_load_stats::{
    load_cluster_stats_target, resolve_cluster_load_stats, ClusterLoadStatsTarget,
    LoadedStatsTable, ResolvedClusterLoadStats,
};
use crate::cluster_stats_load::ClusterStatsItem;
use crate::cluster_stats_write::{
    loaded_stats_item_statements, plan_loaded_stats_item_statement, plan_loaded_stats_meta_write,
    plan_loaded_stats_usage_write,
};
use crate::cluster_table_storage::{
    commit_pessimistic_statement, lock_pessimistic_statement, PessimisticStatementTransactionError,
    SessionTransaction,
};
use crate::mysql_bootstrap::utc_now_timestamp;
use crate::pessimistic_lock_error::LockSqlError;
use crate::real_tikv_catalog::{SnapshotMetaSnapshot, TransactionMetaSnapshot};
use tidb_executor::cluster_storage::MutationBuffer;

/// Go `StatsMetaHistorySourceLoadStats`.
pub const LOAD_STATS_HISTORY_SOURCE: &str = "load stats";

/// What durable LOAD STATS storage completed before cache refresh.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ClusterLoadStatsReport {
    /// Physical tables selected by the dump's partition map.
    pub table_count: usize,
    /// Column and index histogram transactions committed.
    pub item_count: usize,
    physical_ids: Vec<i64>,
}

impl ClusterLoadStatsReport {
    /// Physical table IDs passed to Go's targeted post-LOAD cache update.
    #[must_use]
    pub fn physical_ids(&self) -> &[i64] {
        &self.physical_ids
    }
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
    item_history_initialized: impl Fn(i64) -> bool + Sync,
    concurrency_for_partition: usize,
) -> Result<ClusterLoadStatsReport, ClusterLoadStatsCommitError> {
    let resolved = resolve_tables(opener, json, timeout)?;
    let physical_ids = resolved
        .targets
        .iter()
        .map(|target| target.physical_id)
        .collect::<Vec<_>>();
    let load = |target: &ClusterLoadStatsTarget| {
        let table = load_cluster_stats_target(&resolved.schema, target).map_err(other)?;
        commit_loaded_table(
            opener,
            &table,
            timeout,
            historical_stats_enabled,
            &item_history_initialized,
        )
    };
    let item_count = if resolved.concurrent {
        run_load_workers(&resolved.targets, concurrency_for_partition, &load)?
    } else {
        load(&resolved.targets[0])?
    };
    Ok(ClusterLoadStatsReport {
        table_count: resolved.targets.len(),
        item_count,
        physical_ids,
    })
}

fn commit_loaded_table<C: StoreWriteClient, L: StoreWriteLoader, P: StorePdCapability>(
    opener: &RealOptimisticTransactionOpener<C, L, P>,
    table: &LoadedStatsTable,
    timeout: Duration,
    historical_stats_enabled: bool,
    item_history_initialized: &(impl Fn(i64) -> bool + Sync),
) -> Result<usize, ClusterLoadStatsCommitError> {
    let mut item_count = 0;
    for item in table.columns.iter().chain(&table.indexes) {
        let version = commit_item(opener, table, item, timeout)?;
        item_count += 1;
        if historical_stats_enabled && item_history_initialized(table.physical_id) {
            record_history(opener, table.physical_id, version, timeout);
        }
    }
    commit_usage(opener, table, timeout)?;
    let version = commit_final_meta(opener, table, timeout)?;
    if historical_stats_enabled {
        record_history(opener, table.physical_id, version, timeout);
    }
    Ok(item_count)
}

fn partition_load_concurrency(requested: usize) -> usize {
    let processors = std::thread::available_parallelism().map_or(1, usize::from);
    if requested == 0 {
        processors.div_ceil(2)
    } else {
        requested.min(processors)
    }
}

fn run_load_workers<T: Sync>(
    tasks: &[T],
    requested: usize,
    load: impl Fn(&T) -> Result<usize, ClusterLoadStatsCommitError> + Sync,
) -> Result<usize, ClusterLoadStatsCommitError> {
    let next = AtomicUsize::new(0);
    let loaded_items = AtomicUsize::new(0);
    let first_error = Mutex::new(None);
    let workers = partition_load_concurrency(requested);
    std::thread::scope(|scope| {
        for _ in 0..workers {
            scope.spawn(|| loop {
                if first_error
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .is_some()
                {
                    return;
                }
                let position = next.fetch_add(1, Ordering::Relaxed);
                let Some(task) = tasks.get(position) else {
                    return;
                };
                let result =
                    catch_unwind(AssertUnwindSafe(|| load(task))).unwrap_or_else(|panic| {
                        Err(ClusterLoadStatsCommitError::Other(panic_text(panic)))
                    });
                match result {
                    Ok(count) => {
                        loaded_items.fetch_add(count, Ordering::Relaxed);
                    }
                    Err(error) => {
                        let mut first = first_error
                            .lock()
                            .unwrap_or_else(std::sync::PoisonError::into_inner);
                        if first.is_none() {
                            *first = Some(error);
                        }
                        return;
                    }
                }
            });
        }
    });
    if let Some(error) = first_error
        .into_inner()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
    {
        return Err(error);
    }
    Ok(loaded_items.load(Ordering::Relaxed))
}

fn panic_text(panic: Box<dyn std::any::Any + Send>) -> String {
    if let Some(message) = panic.downcast_ref::<&str>() {
        (*message).to_owned()
    } else if let Some(message) = panic.downcast_ref::<String>() {
        message.clone()
    } else {
        "partition load worker panicked".to_owned()
    }
}

fn resolve_tables<C: StoreWriteClient, L: StoreWriteLoader, P: StorePdCapability>(
    opener: &RealOptimisticTransactionOpener<C, L, P>,
    json: &JsonTable,
    timeout: Duration,
) -> Result<ResolvedClusterLoadStats, ClusterLoadStatsCommitError> {
    let mut transaction = opener.begin_read_only().map_err(other)?;
    let tables = {
        let mut snapshot = TransactionMetaSnapshot::new(&mut transaction, timeout);
        let catalog = load_cluster_catalog(&mut snapshot).map_err(other)?;
        resolve_cluster_load_stats(&catalog, json).map_err(other)?
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
    let transaction = SessionTransaction::begin_pessimistic_with_budget(
        Arc::new(opener.clone()),
        timeout,
        opener.commit_protocol(),
        usize::MAX,
        MAX_OPTIMISTIC_TRANSACTION_BYTES,
    )
    .map_err(other)?;
    let version = transaction.start_ts();
    let staged = MutationBuffer::new();
    for statement in loaded_stats_item_statements(table.physical_id, item).map_err(other)? {
        lock_pessimistic_statement(&transaction, &staged, |snapshot, start_ts| {
            let mut snapshot = SnapshotMetaSnapshot::new(snapshot);
            let catalog = load_cluster_catalog(&mut snapshot).map_err(|error| error.to_string())?;
            let plan = plan_loaded_stats_item_statement(
                &mut snapshot,
                &catalog,
                table.physical_id,
                table.count,
                item,
                start_ts,
                utc_now_timestamp(),
                &statement,
            )
            .map_err(|error| error.to_string())?;
            Ok(((), plan.mutations))
        })
        .map_err(pessimistic_error)?;
    }
    transaction
        .commit(&staged)
        .map(|_| ())
        .map_err(commit_error)?;
    Ok(version)
}

fn commit_usage<C: StoreWriteClient, L: StoreWriteLoader, P: StorePdCapability>(
    opener: &RealOptimisticTransactionOpener<C, L, P>,
    table: &LoadedStatsTable,
    timeout: Duration,
) -> Result<(), ClusterLoadStatsCommitError> {
    let transaction = SessionTransaction::begin_pessimistic_with_budget(
        Arc::new(opener.clone()),
        timeout,
        opener.commit_protocol(),
        usize::MAX,
        MAX_OPTIMISTIC_TRANSACTION_BYTES,
    )
    .map_err(other)?;
    let staged = MutationBuffer::new();
    for column in &table.predicate_columns {
        if column.is_none() {
            continue;
        }
        lock_pessimistic_statement(&transaction, &staged, |snapshot, _start_ts| {
            let mut snapshot = SnapshotMetaSnapshot::new(snapshot);
            let catalog = load_cluster_catalog(&mut snapshot).map_err(|error| error.to_string())?;
            let plan = plan_loaded_stats_usage_write(
                &mut snapshot,
                &catalog,
                table.physical_id,
                std::slice::from_ref(column),
                utc_now_timestamp(),
            )
            .map_err(|error| error.to_string())?;
            Ok(((), plan.mutations))
        })
        .map_err(pessimistic_error)?;
    }
    transaction
        .commit(&staged)
        .map(|_| ())
        .map_err(commit_error)
}

fn commit_final_meta<C: StoreWriteClient, L: StoreWriteLoader, P: StorePdCapability>(
    opener: &RealOptimisticTransactionOpener<C, L, P>,
    table: &LoadedStatsTable,
    timeout: Duration,
) -> Result<u64, ClusterLoadStatsCommitError> {
    commit_pessimistic_statement(opener, timeout, |snapshot, version| {
        let mut snapshot = SnapshotMetaSnapshot::new(snapshot);
        let catalog = load_cluster_catalog(&mut snapshot).map_err(|error| error.to_string())?;
        let plan = plan_loaded_stats_meta_write(
            &mut snapshot,
            &catalog,
            table.physical_id,
            table.count,
            table.modify_count,
            version,
            utc_now_timestamp(),
        )
        .map_err(|error| error.to_string())?;
        Ok((version, plan.mutations))
    })
    .map_err(pessimistic_error)
}

fn record_history<C: StoreWriteClient, L: StoreWriteLoader, P: StorePdCapability>(
    opener: &RealOptimisticTransactionOpener<C, L, P>,
    table_id: i64,
    version: u64,
    timeout: Duration,
) {
    let _ = crate::real_tikv_stats::record_historical_stats_meta(
        opener,
        table_id,
        version,
        LOAD_STATS_HISTORY_SOURCE,
        timeout,
    );
}

fn pessimistic_error(error: PessimisticStatementTransactionError) -> ClusterLoadStatsCommitError {
    match error {
        PessimisticStatementTransactionError::Build(detail) => {
            ClusterLoadStatsCommitError::Other(detail)
        }
        PessimisticStatementTransactionError::Transaction(error) => commit_error(error),
    }
}

fn commit_error(error: LockSqlError) -> ClusterLoadStatsCommitError {
    if error.is_result_undetermined() {
        ClusterLoadStatsCommitError::Undetermined(
            "the LOAD STATS transaction returned no commit verdict".to_owned(),
        )
    } else {
        ClusterLoadStatsCommitError::Commit(error)
    }
}

fn other(error: impl ToString) -> ClusterLoadStatsCommitError {
    ClusterLoadStatsCommitError::Other(error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn partition_load_concurrency_matches_go_default_and_cap() {
        let processors = std::thread::available_parallelism().map_or(1, usize::from);
        assert_eq!(partition_load_concurrency(0), processors.div_ceil(2));
        assert_eq!(partition_load_concurrency(usize::MAX), processors);
    }

    #[test]
    fn partition_load_workers_sum_successful_item_counts() {
        assert_eq!(
            run_load_workers(&[1_usize, 2, 3], 1, |count| Ok(*count)).unwrap(),
            6
        );
    }

    #[test]
    fn partition_load_workers_return_first_error_and_stop_claiming_tasks() {
        let visited = Mutex::new(Vec::new());
        let error = run_load_workers(&[1_usize, 2, 3], 1, |task| {
            visited
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .push(*task);
            if *task == 2 {
                Err(ClusterLoadStatsCommitError::Other("first".to_owned()))
            } else {
                Ok(*task)
            }
        })
        .expect_err("the worker error is returned");
        assert_eq!(error.to_string(), "first");
        assert_eq!(
            *visited
                .into_inner()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
            vec![1, 2]
        );
    }

    #[test]
    fn partition_load_workers_recover_panics_as_errors() {
        let error = run_load_workers(&[()], 1, |_| {
            panic!("PANIC");
        })
        .expect_err("a worker panic is returned as an error");
        assert_eq!(error.to_string(), "PANIC");
    }
}

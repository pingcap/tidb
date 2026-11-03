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
use std::sync::Mutex;
use std::time::Duration;

use tidb_stats_util::JsonTable;
use tidb_store_driver_error::sql_query_error::SqlQueryError;
use tidb_store_driver_txn::transaction::{RealOptimisticTransactionOpener, StoreClient};

use tidb_infoschema::cluster_catalog::load_info_schema;
use crate::cluster_load_stats::{
    load_cluster_stats_target, resolve_cluster_load_stats, ClusterLoadStatsTarget,
    LoadedStatsTable, ResolvedClusterLoadStats,
};
use crate::save::MetaUpdate;
use crate::stats_read_writer::{self, StatsSessionPool};
use tidb_infoschema_issyncer::real_tikv_catalog::TransactionMetaSnapshot;

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
    /// Resolution or conversion failed before any save.
    Other(String),
    /// A statistics-session statement's error, as the statement raised it.
    Sql(SqlQueryError),
}

impl fmt::Display for ClusterLoadStatsCommitError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Other(detail) => formatter.write_str(detail),
            Self::Sql(error) => formatter.write_str(&error.message),
        }
    }
}

impl std::error::Error for ClusterLoadStatsCommitError {}

/// Commits the storage half of pinned Go `LoadStatsFromJSON`.
///
/// `record_history` is Go's `RecordHistoricalStatsMeta` through the stats
/// handle, present when historical stats are enabled; `item_history_initialized`
/// supplies its `RecordHistoricalStatsMeta(...,
/// enforce=false)` cache gate for column/index transactions. Final meta uses
/// `enforce=true` and therefore does not consult it. History failures remain
/// nonfatal, exactly as the Go handle logs and ignores them.
pub fn commit_cluster_load_stats<S: StoreClient>(
    opener: &RealOptimisticTransactionOpener<S>,
    stats_pool: &dyn StatsSessionPool,
    json: &JsonTable,
    timeout: Duration,
    stats_lease: Duration,
    record_history: Option<&(dyn Fn(i64, u64, &str) + Sync)>,
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
        load_stats_from_json(
            stats_pool,
            stats_lease,
            &table,
            record_history,
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

/// Go `statsReadWriter.loadStatsFromJSON`'s saves for one physical table:
/// each histogram (`SaveColOrIdxStatsToStorage`), the predicate-column usage
/// (`SaveColumnStatsUsageToStorage`), then the table's counts
/// (`SaveMetaToStorage`), each in its own statistics-session transaction.
fn load_stats_from_json(
    stats_pool: &dyn StatsSessionPool,
    stats_lease: Duration,
    table: &LoadedStatsTable,
    record_history: Option<&(dyn Fn(i64, u64, &str) + Sync)>,
    item_history_initialized: &(impl Fn(i64) -> bool + Sync),
) -> Result<usize, ClusterLoadStatsCommitError> {
    let mut item_count = 0;
    for item in table.columns.iter().chain(&table.indexes) {
        // The table-level count and modify_count are overridden by the
        // `SaveMetaToStorage` below.
        let version = stats_read_writer::save_col_or_idx_stats_to_storage(
            stats_pool,
            stats_lease,
            table.physical_id,
            table.count,
            0,
            item,
            item.stats_ver,
            false,
        )
        .map_err(ClusterLoadStatsCommitError::Sql)?;
        item_count += 1;
        if let Some(record_history) = record_history
            .filter(|_| version != 0 && item_history_initialized(table.physical_id))
        {
            record_history(table.physical_id, version, LOAD_STATS_HISTORY_SOURCE);
        }
    }
    stats_read_writer::save_column_stats_usage_to_storage(
        stats_pool,
        table.physical_id,
        &table.predicate_columns,
    )
    .map_err(ClusterLoadStatsCommitError::Sql)?;
    let version = stats_read_writer::save_meta_to_storage(
        stats_pool,
        true,
        &[MetaUpdate {
            physical_id: table.physical_id,
            count: table.count,
            modify_count: table.modify_count,
        }],
    )
    .map_err(ClusterLoadStatsCommitError::Sql)?;
    if let Some(record_history) = record_history.filter(|_| version != 0) {
        record_history(table.physical_id, version, LOAD_STATS_HISTORY_SOURCE);
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
            // Each worker commits transactions, which Go runs on a pool
            // goroutine; the default thread stack does not hold that path.
            let spawned = std::thread::Builder::new()
                .name("load-stats-partition".to_owned())
                .stack_size(tidb_worker_stack::WORKER_STACK_BYTES)
                .spawn_scoped(scope, || loop {
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
            if let Err(error) = spawned {
                let mut first = first_error
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                if first.is_none() {
                    *first = Some(ClusterLoadStatsCommitError::Other(format!(
                        "spawn partition load worker: {error}"
                    )));
                }
                break;
            }
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

fn resolve_tables<S: StoreClient>(
    opener: &RealOptimisticTransactionOpener<S>,
    json: &JsonTable,
    timeout: Duration,
) -> Result<ResolvedClusterLoadStats, ClusterLoadStatsCommitError> {
    let mut transaction = opener.begin_read_only().map_err(other)?;
    let tables = {
        let mut snapshot = TransactionMetaSnapshot::new(&mut transaction, timeout);
        let catalog = load_info_schema(&mut snapshot).map_err(other)?;
        resolve_cluster_load_stats(&catalog, json).map_err(other)?
    };
    transaction.finish_without_writes().map_err(other)?;
    Ok(tables)
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

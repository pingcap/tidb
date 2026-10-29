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

//! Go `pkg/statistics/handle/bootstrap.go`: `Domain.initStats`' initial load
//! of the node's statistics cache, full or lite, under the cache's memory
//! quota.

use std::sync::atomic::Ordering;
use std::time::Duration;

use tidb_stats_handle_cache::StatsRefreshSource;
use tidb_txnkv::transaction::{RealOptimisticTransactionOpener, StoreClient};

use tidb_infoschema::cluster_catalog::{load_cluster_catalog, MetaSnapshot};
use tidb_stats_handle_storage::cluster_stats_load::{ClusterStatsLoader, ClusterTableStats};
use tidb_syssession::mysql_system_tables::SystemTableError;
use tidb_infoschema_issyncer::real_tikv_catalog::TransactionMetaSnapshot;
use tidb_stats_handle_syncload::stats_watch::{StatsSnapshot, TableStatsState};
use tidb_stats_handle_syncload::real_tikv_stats::*;

/// The two startup shapes selected by Go `Domain.initStats`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum InitialStatsLoad {
    /// Go `Handle.InitStatsLite`: metadata only for columns and indexes.
    Lite,
    /// Go `Handle.InitStats`: metadata-only columns and fully loaded indexes.
    IndexFull,
}

/// Reads every one of `tables`' lite statistics through one fresh read-only
/// transaction and returns the located `mysql.stats_*` views for later cache
/// updates.
///
/// `mode` selects pinned Go `InitStatsLite` or `InitStats`: the former keeps
/// every payload evicted, while the latter fully loads indexes and retains
/// metadata-only columns. It is selected by `performance.lite-init-stats`,
/// independently of the statistics lease. A table with no
/// `mysql.stats_meta` row becomes [`TableStatsState::Pseudo`].
pub fn load_stats_snapshot_and_loader<S: StoreClient>(
    opener: &RealOptimisticTransactionOpener<S>,
    timeout: Duration,
    tables: &[StatsTarget],
    table_ids: &[i64],
    mode: InitialStatsLoad,
) -> Result<(StatsSnapshot, ClusterStatsLoader), SystemTableError> {
    let mut transaction = opener
        .begin_read_only()
        .map_err(|error| SystemTableError::Snapshot(error.to_string()))?;
    let loaded = {
        let mut snapshot = TransactionMetaSnapshot::new(&mut transaction, timeout);
        let catalog = load_cluster_catalog(&mut snapshot)?;
        let loader = ClusterStatsLoader::locate(&catalog)?;
        let result = load_initial_stats_snapshot(&mut snapshot, &loader, tables, table_ids, mode)?;
        Ok((result, loader))
    };
    transaction
        .finish_without_writes()
        .map_err(|error| SystemTableError::Snapshot(error.to_string()))?;
    loaded
}

/// Go `Handle.InitStatsLite` / `Handle.InitStats` over one consistent
/// metadata snapshot. An empty `table_ids` slice loads every current target;
/// otherwise only the named physical IDs are admitted. Consequently stale
/// statistics rows for dropped tables cannot enter the returned cache.
pub fn load_initial_stats_snapshot<S: MetaSnapshot>(
    snapshot: &mut S,
    loader: &ClusterStatsLoader,
    tables: &[StatsTarget],
    table_ids: &[i64],
    mode: InitialStatsLoad,
) -> Result<StatsSnapshot, SystemTableError> {
    let total_memory = if mode == InitialStatsLoad::IndexFull {
        tidb_memory::mem_total()
            .map_err(|error| SystemTableError::Snapshot(error.to_string()))?
    } else {
        u64::MAX
    };
    let memory_quota = tidb_vardef::STATS_CACHE_MEM_QUOTA.load(Ordering::SeqCst);
    load_initial_stats_snapshot_with_memory_limits(
        snapshot,
        loader,
        tables,
        table_ids,
        mode,
        total_memory,
        memory_quota,
    )
}

/// Go `Handle.InitStats` with its two cache-full inputs made explicit. This
/// keeps the production system-memory probe and global quota outside the
/// deterministic storage-stage policy.
#[allow(clippy::too_many_arguments)]
pub fn load_initial_stats_snapshot_with_memory_limits<S: MetaSnapshot>(
    snapshot: &mut S,
    loader: &ClusterStatsLoader,
    tables: &[StatsTarget],
    table_ids: &[i64],
    mode: InitialStatsLoad,
    total_memory: u64,
    memory_quota: i64,
) -> Result<StatsSnapshot, SystemTableError> {
    if mode == InitialStatsLoad::Lite {
        return load_initial_stats_phase(
            snapshot,
            loader,
            tables,
            table_ids,
            |loader, snapshot, target| {
                loader.load_table_lite(snapshot, target.physical_id, &target.column_types)
            },
        );
    }

    let mut histograms = StatsSnapshot::new();
    let mut consumed = 0u64;
    for target in selected_stats_targets(tables, table_ids) {
        let loaded = if initial_stats_cache_is_full(consumed, total_memory, memory_quota) {
            loader.load_table_lite(snapshot, target.physical_id, &target.column_types)?
        } else {
            loader.load_table_for_init_stats_histograms(
                snapshot,
                target.physical_id,
                &target.column_types,
            )?
        };
        insert_initial_stats_state(&mut histograms, &mut consumed, target, loaded);
    }

    let mut topn = histograms;
    for target in selected_stats_targets(tables, table_ids) {
        if initial_stats_cache_is_full(consumed, total_memory, memory_quota) {
            break;
        }
        let loaded = loader.load_table_for_init_stats_topn(
            snapshot,
            target.physical_id,
            &target.column_types,
        )?;
        insert_initial_stats_state(&mut topn, &mut consumed, target, loaded);
    }

    let mut buckets = topn;
    for target in selected_stats_targets(tables, table_ids) {
        if initial_stats_cache_is_full(consumed, total_memory, memory_quota) {
            break;
        }
        let loaded =
            loader.load_table_for_init_stats(snapshot, target.physical_id, &target.column_types)?;
        insert_initial_stats_state(&mut buckets, &mut consumed, target, loaded);
    }
    Ok(buckets)
}

fn load_initial_stats_phase<S, F>(
    snapshot: &mut S,
    loader: &ClusterStatsLoader,
    tables: &[StatsTarget],
    table_ids: &[i64],
    mut load: F,
) -> Result<StatsSnapshot, SystemTableError>
where
    S: MetaSnapshot,
    F: FnMut(
        &ClusterStatsLoader,
        &mut S,
        &StatsTarget,
    ) -> Result<Option<ClusterTableStats>, SystemTableError>,
{
    let mut result = StatsSnapshot::new();
    for target in tables {
        let table_id = target.physical_id;
        if !table_ids.is_empty() && !table_ids.contains(&table_id) {
            continue;
        }
        let loaded = load(loader, snapshot, target)?;
        let state = match loaded {
            Some(stats) => TableStatsState::Loaded(stats.to_statistics_table(&target.table)),
            None => TableStatsState::Pseudo,
        };
        result.insert(table_id, state);
    }
    Ok(result)
}

fn selected_stats_targets<'a>(
    tables: &'a [StatsTarget],
    table_ids: &'a [i64],
) -> impl Iterator<Item = &'a StatsTarget> {
    tables
        .iter()
        .filter(|target| table_ids.is_empty() || table_ids.contains(&target.physical_id))
}

fn insert_initial_stats_state(
    result: &mut StatsSnapshot,
    consumed: &mut u64,
    target: &StatsTarget,
    loaded: Option<ClusterTableStats>,
) {
    let state = match loaded {
        Some(stats) => TableStatsState::Loaded(stats.to_statistics_table(&target.table)),
        None => TableStatsState::Pseudo,
    };
    let new_memory = initial_stats_state_memory(&state);
    let old_memory = result
        .insert(target.physical_id, state)
        .as_ref()
        .map_or(0, initial_stats_state_memory);
    *consumed = consumed
        .saturating_sub(old_memory)
        .saturating_add(new_memory);
}

fn initial_stats_state_memory(state: &TableStatsState) -> u64 {
    state.loaded().map_or(0, |table| {
        table.memory_usage().total_mem_usage.max(0) as u64
    })
}

fn initial_stats_cache_is_full(consumed: u64, total_memory: u64, memory_quota: i64) -> bool {
    consumed >= total_memory / 4 || (memory_quota != 0 && consumed >= memory_quota.max(0) as u64)
}

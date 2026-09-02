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

//! The production statistics load: one real transaction, one PD timestamp, one
//! consistent view of a table's `mysql.stats_*` rows.
//!
//! `mysql.stats_meta`, `mysql.stats_histograms`, `mysql.stats_buckets`, and
//! `mysql.stats_top_n` are four separate tables that one `ANALYZE` writes in
//! one transaction. Reading them at four timestamps could therefore assemble a
//! histogram whose buckets came from one `ANALYZE` and whose TopN came from
//! the next — a combination that never existed and whose row counts would not
//! add up. Everything below is read at a single `start_ts`.

use std::collections::BTreeMap;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use tidb_datatype::FieldType;
use tidb_executor::cluster_storage::MutationBuffer;
use tidb_model::table_info::TableInfo;
use tidb_stats_handle_cache::{StatsMetaRow, StatsRefreshSource, UpdateError};
use tidb_txnkv::transaction::{
    RealOptimisticTransactionOpener, StorePdCapability, StoreWriteClient, StoreWriteLoader,
    MAX_OPTIMISTIC_TRANSACTION_BYTES,
};

use crate::cluster_catalog::{load_cluster_catalog, MetaSnapshot};
use crate::cluster_stats_load::{ClusterStatsItem, ClusterStatsLoader, ClusterTableStats};
use crate::cluster_stats_write::{
    plan_change_global_stats_id, plan_historical_stats_meta_lock,
    plan_historical_stats_meta_replace, plan_stats_meta_version_refresh, plan_update_stats_version,
};
use crate::cluster_table_storage::{
    commit_pessimistic_statement, lock_pessimistic_statement, PessimisticStatementTransactionError,
    SessionTransaction,
};
use crate::mysql_bootstrap::local_now_datetime6;
use crate::mysql_system_tables::SystemTableError;
use crate::real_tikv_catalog::{SnapshotMetaSnapshot, TransactionMetaSnapshot};
use crate::stats_watch::{SharedStats, StatsSnapshot, TableStatsState};

/// The two startup shapes selected by Go `Domain.initStats`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum InitialStatsLoad {
    /// Go `Handle.InitStatsLite`: metadata only for columns and indexes.
    Lite,
    /// Go `Handle.InitStats`: metadata-only columns and fully loaded indexes.
    IndexFull,
}

/// One cache-refresh target: the complete schema object Go passes through
/// `TableStatsFromStorage`, plus the derived bound-decoding map.
#[derive(Clone, Debug)]
pub struct StatsTarget {
    /// Logical table ID or physical partition ID whose statistics are read.
    pub physical_id: i64,
    /// The current parent-table schema metadata Go resolves for this physical ID.
    pub table: TableInfo,
    /// Declared column types used to decode stored histogram bounds.
    pub column_types: BTreeMap<i64, FieldType>,
}

impl StatsTarget {
    /// Expands one logical table into the IDs Go's statistics cache tracks:
    /// the logical/global ID and every physical partition ID.
    #[must_use]
    pub fn for_table(table: &TableInfo) -> Vec<Self> {
        let column_types = crate::cluster_stats_load::column_types_of(table);
        let mut targets = Vec::with_capacity(
            1 + table
                .partition
                .as_ref()
                .map_or(0, |partition| partition.read().definitions.len()),
        );
        targets.push(Self {
            physical_id: table.id,
            table: table.clone(),
            column_types: column_types.clone(),
        });
        if let Some(partition) = &table.partition {
            for definition in partition.read().definitions.snapshot() {
                targets.push(Self {
                    physical_id: definition.id,
                    table: table.clone(),
                    column_types: column_types.clone(),
                });
            }
        }
        targets
    }
}

/// Reads one table's statistics through one fresh read-only transaction.
///
/// The catalog is loaded at that same timestamp, so the column types the
/// bounds are decoded at are the types the table had when the rows were read.
///
/// `Ok(None)` means the table has no `mysql.stats_meta` row: never analyzed.
pub fn load_table_stats_from_cluster<
    C: StoreWriteClient,
    L: StoreWriteLoader,
    P: StorePdCapability,
>(
    opener: &RealOptimisticTransactionOpener<C, L, P>,
    timeout: Duration,
    schema: &str,
    table: &str,
) -> Result<Option<ClusterTableStats>, SystemTableError> {
    let mut transaction = opener
        .begin_read_only()
        .map_err(|error| SystemTableError::Snapshot(error.to_string()))?;
    let loaded = {
        let mut snapshot = TransactionMetaSnapshot::new(&mut transaction, timeout);
        let catalog = load_cluster_catalog(&mut snapshot)?;
        let info = catalog
            .databases
            .iter()
            .find(|database| database.info.name.lowercase() == schema.to_lowercase())
            .and_then(|database| {
                database
                    .tables
                    .iter()
                    .find(|stored| stored.name.lowercase() == table.to_lowercase())
            })
            .ok_or_else(|| SystemTableError::Missing {
                name: format!("{schema}.{table}"),
            })?;
        let column_types: BTreeMap<i64, FieldType> =
            crate::cluster_stats_load::column_types_of(info);
        let table_id = info.id;
        let loader = ClusterStatsLoader::locate(&catalog)?;
        loader.load_table(&mut snapshot, table_id, &column_types)
    };
    transaction
        .finish_without_writes()
        .map_err(|error| SystemTableError::Snapshot(error.to_string()))?;
    loaded
}

/// Reads one column or index statistics item through one fresh read-only
/// transaction, Go sync-load's storage boundary.
pub fn load_stats_item_from_cluster<
    C: StoreWriteClient,
    L: StoreWriteLoader,
    P: StorePdCapability,
>(
    opener: &RealOptimisticTransactionOpener<C, L, P>,
    timeout: Duration,
    loader: &ClusterStatsLoader,
    table_id: i64,
    is_index: bool,
    id: i64,
    column_type: Option<&FieldType>,
    full_load: bool,
) -> Result<Option<ClusterStatsItem>, SystemTableError> {
    let mut transaction = opener
        .begin_read_only()
        .map_err(|error| SystemTableError::Snapshot(error.to_string()))?;
    let loaded = {
        let mut snapshot = TransactionMetaSnapshot::new(&mut transaction, timeout);
        loader.load_item(
            &mut snapshot,
            table_id,
            is_index,
            id,
            column_type,
            full_load,
        )
    };
    transaction
        .finish_without_writes()
        .map_err(|error| SystemTableError::Snapshot(error.to_string()))?;
    loaded
}

/// Go `storage.ReadColumnDistributionStats` over real TiKV.
///
/// Histogram metadata, TopN, and buckets are read through one read-only
/// transaction, so a concurrent `ANALYZE` cannot splice two generations into
/// one column distribution.  This is a direct foreground read and does not
/// publish anything to the shared statistics cache.
pub fn read_column_distribution_stats_from_cluster<
    C: StoreWriteClient,
    L: StoreWriteLoader,
    P: StorePdCapability,
>(
    opener: &RealOptimisticTransactionOpener<C, L, P>,
    timeout: Duration,
    loader: &ClusterStatsLoader,
    physical_table_id: i64,
    table_info: &TableInfo,
    column_id: i64,
) -> Result<Option<tidb_stats::Column>, SystemTableError> {
    let Some(column_type) = table_info.cols().iter_deref().find_map(|column| {
        let column = column.read();
        (column.id == column_id).then(|| column.field_type.clone())
    }) else {
        return Ok(None);
    };
    let mut transaction = opener
        .begin_read_only()
        .map_err(|error| SystemTableError::Snapshot(error.to_string()))?;
    let loaded = {
        let mut snapshot = TransactionMetaSnapshot::new(&mut transaction, timeout);
        loader.load_column_distribution_stats(
            &mut snapshot,
            physical_table_id,
            column_id,
            table_info,
            &column_type,
        )
    };
    transaction
        .finish_without_writes()
        .map_err(|error| SystemTableError::Snapshot(error.to_string()))?;
    loaded
}

fn load_stats_item_payload_from_cluster<
    C: StoreWriteClient,
    L: StoreWriteLoader,
    P: StorePdCapability,
>(
    opener: &RealOptimisticTransactionOpener<C, L, P>,
    timeout: Duration,
    loader: &ClusterStatsLoader,
    table_id: i64,
    mut item: ClusterStatsItem,
    column_type: Option<&FieldType>,
) -> Result<ClusterStatsItem, SystemTableError> {
    let mut transaction = opener
        .begin_read_only()
        .map_err(|error| SystemTableError::Snapshot(error.to_string()))?;
    let loaded = {
        let mut snapshot = TransactionMetaSnapshot::new(&mut transaction, timeout);
        loader.load_item_payload(&mut snapshot, table_id, &mut item, column_type)
    };
    transaction
        .finish_without_writes()
        .map_err(|error| SystemTableError::Snapshot(error.to_string()))?;
    loaded?;
    Ok(item)
}

/// Go `storage.LoadNeededHistograms`: drain the process-wide asynchronous
/// statistics demand against the latest schema and shared statistics cache.
/// Every item is removed after its one attempt, including storage failures.
pub fn load_needed_histograms_from_cluster<
    C: StoreWriteClient,
    L: StoreWriteLoader,
    P: StorePdCapability,
>(
    opener: &RealOptimisticTransactionOpener<C, L, P>,
    timeout: Duration,
    tables: &[StatsTarget],
    shared: &SharedStats,
    loader: &ClusterStatsLoader,
) -> Result<(), SystemTableError> {
    let needed = &tidb_stats::ASYNC_LOAD_HISTOGRAM_NEEDED_ITEMS;
    for requested in needed.all_items() {
        let item = requested.table_item_id;
        let result: Result<(), SystemTableError> = (|| {
            if skips_internal_column(&item) {
                return Ok(());
            }
            let current = shared.load();
            let Some(current) = current
                .get(&item.table_id)
                .and_then(TableStatsState::loaded)
            else {
                return Ok(());
            };
            if item.is_index {
                if !current.index_load_needed(item.id).1 {
                    return Ok(());
                }
                let Some(metadata) = load_stats_item_from_cluster(
                    opener,
                    timeout,
                    loader,
                    item.table_id,
                    true,
                    item.id,
                    None,
                    false,
                )?
                else {
                    return Ok(());
                };
                let Some(target) = tables
                    .iter()
                    .find(|target| target.physical_id == item.table_id)
                else {
                    return Ok(());
                };
                if !target
                    .table
                    .indices
                    .iter_deref()
                    .any(|index| index.read().id == item.id)
                {
                    return Ok(());
                }
                let loaded = load_stats_item_payload_from_cluster(
                    opener,
                    timeout,
                    loader,
                    item.table_id,
                    metadata,
                    None,
                )?;
                shared.update_item(item.table_id, loaded, &target.table);
                return Ok(());
            }

            let Some(target) = tables
                .iter()
                .find(|target| target.physical_id == item.table_id)
            else {
                return Ok(());
            };
            let Some(column_type) = target.column_types.get(&item.id) else {
                return Ok(());
            };
            let (_, load_needed, analyzed) = current.column_load_needed(item.id, true);
            if !load_needed {
                return Ok(());
            }
            if !analyzed {
                let empty = ClusterStatsItem {
                    id: item.id,
                    is_index: false,
                    stats_ver: 0,
                    flag: 0,
                    load_status: tidb_stats::StatsLoadedStatus::default(),
                    histogram: tidb_stats::Histogram {
                        id: item.id,
                        ..tidb_stats::Histogram::default()
                    },
                    topn: None,
                    cms: None,
                    fm_sketch: None,
                };
                shared.update_item(item.table_id, empty, &target.table);
                return Ok(());
            }
            if let Some(mut loaded) = load_stats_item_from_cluster(
                opener,
                timeout,
                loader,
                item.table_id,
                false,
                item.id,
                Some(column_type),
                false,
            )? {
                if requested.full_load {
                    loaded = load_stats_item_payload_from_cluster(
                        opener,
                        timeout,
                        loader,
                        item.table_id,
                        loaded,
                        Some(column_type),
                    )?;
                }
                shared.update_item(item.table_id, loaded, &target.table);
            }
            Ok(())
        })();
        needed.delete(item);
        result?;
    }
    Ok(())
}

fn skips_internal_column(item: &tidb_model::TableItemID) -> bool {
    !item.is_index && item.id <= 0
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
pub fn load_stats_snapshot_and_loader<
    C: StoreWriteClient,
    L: StoreWriteLoader,
    P: StorePdCapability,
>(
    opener: &RealOptimisticTransactionOpener<C, L, P>,
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
        tidb_util::memory::mem_total()
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

struct ClusterStatsRefreshSource<'a, C, L, P> {
    opener: &'a RealOptimisticTransactionOpener<C, L, P>,
    timeout: Duration,
    loader: &'a ClusterStatsLoader,
    targets: BTreeMap<i64, &'a StatsTarget>,
    current: Arc<StatsSnapshot>,
    lease: Duration,
    load_all: bool,
}

impl<C, L, P> StatsRefreshSource for ClusterStatsRefreshSource<'_, C, L, P>
where
    C: StoreWriteClient,
    L: StoreWriteLoader,
    P: StorePdCapability,
{
    type Error = SystemTableError;

    fn lease(&self) -> Duration {
        self.lease
    }

    fn stats_meta_rows(
        &self,
        after_version: u64,
        physical_ids: &[i64],
    ) -> Result<Vec<StatsMetaRow>, Self::Error> {
        let mut transaction = self
            .opener
            .begin_read_only()
            .map_err(|error| SystemTableError::Snapshot(error.to_string()))?;
        let rows = {
            let mut snapshot = TransactionMetaSnapshot::new(&mut transaction, self.timeout);
            let mut rows = self.loader.load_stats_meta_rows(&mut snapshot)?;
            rows.retain(|row| {
                row.version > after_version
                    && (physical_ids.is_empty() || physical_ids.contains(&row.physical_id))
            });
            rows
        };
        transaction
            .finish_without_writes()
            .map_err(|error| SystemTableError::Snapshot(error.to_string()))?;
        Ok(rows)
    }

    fn table_info_update_ts(&self, physical_id: i64) -> Option<u64> {
        self.targets
            .get(&physical_id)
            .map(|target| target.table.update_ts)
    }

    fn table_stats_from_storage(
        &self,
        physical_id: i64,
    ) -> Result<Option<Arc<tidb_stats::Table>>, Self::Error> {
        let Some(target) = self.targets.get(&physical_id) else {
            return Ok(None);
        };
        let mut transaction = self
            .opener
            .begin_read_only()
            .map_err(|error| SystemTableError::Snapshot(error.to_string()))?;
        let loaded = {
            let mut snapshot = TransactionMetaSnapshot::new(&mut transaction, self.timeout);
            if self.load_all {
                self.loader
                    .load_table(&mut snapshot, physical_id, &target.column_types)?
                    .map(|stats| stats.to_statistics_table(&target.table))
            } else {
                self.loader.load_statistics_table_for_update(
                    &mut snapshot,
                    physical_id,
                    &target.table,
                    &target.column_types,
                    self.current
                        .get(&physical_id)
                        .and_then(TableStatsState::loaded)
                        .map(Arc::as_ref),
                )?
            }
        };
        transaction
            .finish_without_writes()
            .map_err(|error| SystemTableError::Snapshot(error.to_string()))?;
        Ok(loaded)
    }
}

/// Pinned Go `StatsCacheImpl.Update` over the real cluster storage boundary.
/// The cache's five-lease watermark, ordered rows, per-table version/schema
/// skip, metadata-only reuse, and monotonic maximum all come from the shared
/// `tidb-stats-handle-cache` implementation rather than a second snapshot
/// refresh policy.
pub fn update_stats_cache_from_cluster<
    C: StoreWriteClient,
    L: StoreWriteLoader,
    P: StorePdCapability,
>(
    opener: &RealOptimisticTransactionOpener<C, L, P>,
    timeout: Duration,
    tables: &[StatsTarget],
    shared: &SharedStats,
    loader: &ClusterStatsLoader,
    lease: Duration,
    load_all: bool,
    physical_ids: Vec<i64>,
) -> Result<bool, SystemTableError> {
    let current = shared.load();
    let targets = tables
        .iter()
        .map(|target| (target.physical_id, target))
        .collect();
    let tracked_ids = tables
        .iter()
        .map(|target| target.physical_id)
        .collect::<Vec<_>>();
    let source = ClusterStatsRefreshSource {
        opener,
        timeout,
        loader,
        targets,
        current,
        lease,
        load_all,
    };
    shared
        .update_from_source(&source, physical_ids, &tracked_ids, || false)
        .map_err(|error| match error {
            UpdateError::Source(error) => error,
            UpdateError::Cancelled => {
                SystemTableError::Snapshot("statistics update was cancelled".to_owned())
            }
        })
}

/// Pinned Go `statsReadWriter.UpdateStatsMetaVersionForGC` over real TiKV.
///
/// The metadata refresh is the returned transaction result. Historical-meta
/// recording runs afterward in its own best-effort transaction and therefore
/// cannot turn a successful refresh into an error. `record_history` is the
/// already-evaluated Go historical-stats/cache-initialization gate.
pub fn update_stats_meta_version_for_gc<
    C: StoreWriteClient,
    L: StoreWriteLoader,
    P: StorePdCapability,
>(
    opener: &RealOptimisticTransactionOpener<C, L, P>,
    physical_id: i64,
    timeout: Duration,
    record_history: bool,
) -> Result<u64, PessimisticStatementTransactionError> {
    let version = commit_pessimistic_statement(opener, timeout, |snapshot, version| {
        let mut snapshot = SnapshotMetaSnapshot::new(snapshot);
        let catalog = load_cluster_catalog(&mut snapshot).map_err(|error| error.to_string())?;
        let plan = plan_stats_meta_version_refresh(&mut snapshot, &catalog, physical_id, version)
            .map_err(|error| error.to_string())?;
        Ok((version, plan.mutations))
    })?;
    if record_history {
        let _ =
            record_historical_stats_meta(opener, physical_id, version, "schema change", timeout);
    }
    Ok(version)
}

/// Pinned Go `storage.UpdateStatsVersion` over real TiKV.
pub fn update_stats_version<C: StoreWriteClient, L: StoreWriteLoader, P: StorePdCapability>(
    opener: &RealOptimisticTransactionOpener<C, L, P>,
    timeout: Duration,
) -> Result<u64, PessimisticStatementTransactionError> {
    commit_pessimistic_statement(opener, timeout, |snapshot, version| {
        let mut snapshot = SnapshotMetaSnapshot::new(snapshot);
        let catalog = load_cluster_catalog(&mut snapshot).map_err(|error| error.to_string())?;
        let plan = plan_update_stats_version(&mut snapshot, &catalog, version)
            .map_err(|error| error.to_string())?;
        Ok((version, plan.mutations))
    })
}

/// Pinned Go `storage.ChangeGlobalStatsID` over real TiKV.
pub fn change_global_stats_id<C: StoreWriteClient, L: StoreWriteLoader, P: StorePdCapability>(
    opener: &RealOptimisticTransactionOpener<C, L, P>,
    from: i64,
    to: i64,
    timeout: Duration,
) -> Result<(), PessimisticStatementTransactionError> {
    commit_pessimistic_statement(opener, timeout, |snapshot, _version| {
        let mut snapshot = SnapshotMetaSnapshot::new(snapshot);
        let catalog = load_cluster_catalog(&mut snapshot).map_err(|error| error.to_string())?;
        let plan = plan_change_global_stats_id(&mut snapshot, &catalog, from, to)
            .map_err(|error| error.to_string())?;
        Ok(((), plan.mutations))
    })
}

/// Pinned Go `RecordHistoricalStatsMeta` as its own pessimistic transaction.
pub fn record_historical_stats_meta<
    C: StoreWriteClient,
    L: StoreWriteLoader,
    P: StorePdCapability,
>(
    opener: &RealOptimisticTransactionOpener<C, L, P>,
    table_id: i64,
    version: u64,
    source: &str,
    timeout: Duration,
) -> Result<(), PessimisticStatementTransactionError> {
    let transaction = SessionTransaction::begin_pessimistic_with_budget(
        Arc::new(opener.clone()),
        timeout,
        opener.commit_protocol(),
        usize::MAX,
        MAX_OPTIMISTIC_TRANSACTION_BYTES,
    )
    .map_err(|error| PessimisticStatementTransactionError::Build(error.to_string()))?;
    let staged = MutationBuffer::new();
    let (modify_count, count) =
        lock_pessimistic_statement(&transaction, &staged, |snapshot, _start_ts| {
            let mut snapshot = SnapshotMetaSnapshot::new(snapshot);
            let catalog = load_cluster_catalog(&mut snapshot).map_err(|error| error.to_string())?;
            let (counts, plan) =
                plan_historical_stats_meta_lock(&mut snapshot, &catalog, table_id, version)
                    .map_err(|error| error.to_string())?;
            Ok((counts, plan.mutations))
        })?;
    lock_pessimistic_statement(&transaction, &staged, |snapshot, _start_ts| {
        let mut snapshot = SnapshotMetaSnapshot::new(snapshot);
        let catalog = load_cluster_catalog(&mut snapshot).map_err(|error| error.to_string())?;
        let plan = plan_historical_stats_meta_replace(
            &mut snapshot,
            &catalog,
            table_id,
            modify_count,
            count,
            version,
            source,
            local_now_datetime6(),
        )
        .map_err(|error| error.to_string())?;
        Ok(((), plan.mutations))
    })?;
    transaction
        .commit(&staged)
        .map(|_| ())
        .map_err(PessimisticStatementTransactionError::Transaction)
}

#[cfg(test)]
mod tests {
    use tidb_model::go_runtime::GoShared;
    use tidb_model::partition::{PartitionDefinition, PartitionInfo};

    use super::*;

    #[test]
    fn partition_stats_target_keeps_parent_table_info_like_go() {
        let table = TableInfo {
            id: 10,
            update_ts: 99,
            partition: Some(GoShared::new(PartitionInfo {
                definitions: vec![PartitionDefinition {
                    id: 11,
                    ..PartitionDefinition::default()
                }]
                .into(),
                ..PartitionInfo::default()
            })),
            ..TableInfo::default()
        };

        let targets = StatsTarget::for_table(&table);
        assert_eq!(targets.len(), 2);
        assert_eq!(targets[0].physical_id, 10);
        assert_eq!(targets[0].table.id, 10);
        assert_eq!(targets[1].physical_id, 11);
        assert_eq!(targets[1].table.id, 10);
        assert_eq!(targets[1].table.update_ts, 99);
    }

    #[test]
    fn needed_histogram_loading_skips_only_internal_columns() {
        let item = |id, is_index| tidb_model::TableItemID {
            table_id: 10,
            id,
            is_index,
            is_sync_load_failed: false,
        };
        assert!(skips_internal_column(&item(-1, false)));
        assert!(skips_internal_column(&item(0, false)));
        assert!(!skips_internal_column(&item(1, false)));
        assert!(!skips_internal_column(&item(-1, true)));
    }
}

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
use std::sync::Arc;
use std::time::Duration;

use tidb_datatype::FieldType;
use tidb_model::table_info::TableInfo;
use tidb_stats_handle_cache::{StatsMetaRow, StatsRefreshSource, UpdateError};
use tidb_store_driver_txn::transaction::{RealOptimisticTransactionOpener, StoreClient};

use tidb_infoschema::cluster_catalog::MetaSnapshot;
use tidb_stats_handle_storage::cluster_stats_load::{ClusterStatsItem, ClusterStatsLoader};
use tidb_syssession::mysql_system_tables::SystemTableError;
use tidb_infoschema_issyncer::real_tikv_catalog::TransactionMetaSnapshot;
use crate::stats_watch::{SharedStats, TableStatsState};

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
        let column_types = tidb_stats_handle_storage::cluster_stats_load::column_types_of(table);
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

/// Reads one column or index statistics item through one fresh read-only
/// transaction, Go sync-load's storage boundary.
pub fn load_stats_item_from_cluster<S: StoreClient>(
    opener: &RealOptimisticTransactionOpener<S>,
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
pub fn read_column_distribution_stats_from_cluster<S: StoreClient>(
    opener: &RealOptimisticTransactionOpener<S>,
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

fn load_stats_item_payload_from_cluster<S: StoreClient>(
    opener: &RealOptimisticTransactionOpener<S>,
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
pub fn load_needed_histograms_from_cluster<S: StoreClient>(
    opener: &RealOptimisticTransactionOpener<S>,
    timeout: Duration,
    tables: &[StatsTarget],
    shared: &SharedStats,
    loader: &ClusterStatsLoader,
) -> Result<(), SystemTableError> {
    let needed = &tidb_stats_asyncload::ASYNC_LOAD_HISTOGRAM_NEEDED_ITEMS;
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

struct ClusterStatsRefreshSource<'a, S: StoreClient> {
    opener: &'a RealOptimisticTransactionOpener<S>,
    timeout: Duration,
    loader: &'a ClusterStatsLoader,
    targets: BTreeMap<i64, &'a StatsTarget>,
    shared: &'a SharedStats,
    lease: Duration,
}

impl<S> StatsRefreshSource for ClusterStatsRefreshSource<'_, S>
where
    S: StoreClient,
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
            // Go's `statsReadWriter.TableStatsFromStorage` reads the cached
            // table (`statsHandler.Get`), a cached `PseudoTable` included,
            // when it loads, under the handle's lease.
            self.loader.load_statistics_table_for_update(
                &mut snapshot,
                physical_id,
                &target.table,
                &target.column_types,
                self.shared.get(physical_id).as_deref(),
                self.lease,
            )?
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
pub fn update_stats_cache_from_cluster<S: StoreClient>(
    opener: &RealOptimisticTransactionOpener<S>,
    timeout: Duration,
    tables: &[StatsTarget],
    shared: &SharedStats,
    loader: &ClusterStatsLoader,
    lease: Duration,
    physical_ids: Vec<i64>,
) -> Result<bool, SystemTableError> {
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
        shared,
        lease,
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

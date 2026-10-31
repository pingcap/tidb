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

//! Go `Handle`'s planner-side reads over a schema image: the statistics a
//! statement's plan needs (`CollectPredicateColumnsPoint`'s demand,
//! `SyncWaitStatsLoad`'s items), the async-load queue upkeep, and
//! `GetPhysicalTableStats` through the image's schema facts. Go's handle
//! takes the `InfoSchema`; here the image is the infoschema's `Catalog`.

use std::collections::HashMap;
use std::sync::Arc;

use tidb_hack::GoCaseCompare;
use tidb_infoschema::Catalog;

use crate::StatsHandle;

/// The statistics handle's reads of one schema image.
pub trait CatalogStatistics {
    fn clean_needed_statistics_items(&self, handle: &StatsHandle) -> i64;
    fn load_needed_histograms(&self, handle: &StatsHandle, resource_group: &str) -> Result<(), String>;
    fn statistics_demand(
        &self,
        handle: &StatsHandle,
        usage: &tidb_stats::table_statistics::ColumnStatsUsage,
    ) -> HashMap<tidb_model::TableItemID, bool>;
    fn statistics_load_items(
        &self,
        handle: &StatsHandle,
        usage: &tidb_stats::table_statistics::ColumnStatsUsage,
        _determinate: bool,
    ) -> Vec<tidb_model::StatsLoadItem>;
    fn table_statistics_in(
        &self,
        handle: &StatsHandle,
        table_id: i64,
    ) -> Option<Arc<tidb_stats::Table>>;
}

impl CatalogStatistics for Catalog {
    /// Go `storage.CleanFakeItemsForShowHistInFlights`: remove queue entries
    /// whose cache state no longer requires a load and return the number that
    /// remain genuinely needed.
    fn clean_needed_statistics_items(&self, handle: &StatsHandle) -> i64 {
        let needed = &tidb_stats_asyncload::ASYNC_LOAD_HISTOGRAM_NEEDED_ITEMS;
        let mut really_needed = 0_i64;
        for requested in needed.all_items() {
            let item = requested.table_item_id;
            let Some(statistics) = handle.table_statistics(item.table_id, || self.statistics_schema(item.table_id)) else {
                needed.delete(item);
                continue;
            };
            let load_needed = if item.is_index {
                statistics.index_load_needed(item.id).1
            } else {
                let (_, load_needed, analyzed) =
                    statistics.column_load_needed(item.id, requested.full_load);
                load_needed && analyzed
            };
            if load_needed {
                really_needed = really_needed.saturating_add(1);
            } else {
                needed.delete(item);
            }
        }
        really_needed
    }

    /// Go `storage.LoadNeededHistograms`: drains the process-wide asynchronous
    /// demand on the domain tick. Each item is attempted at most once and is
    /// removed even when storage returns an error; a later item remains queued
    /// when an earlier load fails.
    fn load_needed_histograms(&self, handle: &StatsHandle, resource_group: &str) -> Result<(), String> {
        let loader = handle
            .loader
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        let Some(loader) = loader else {
            return Ok(());
        };
        let needed = &tidb_stats_asyncload::ASYNC_LOAD_HISTOGRAM_NEEDED_ITEMS;
        for requested in needed.all_items() {
            let item = requested.table_item_id;
            let valid_metadata = if item.is_index {
                self.kv_table_by_id(item.table_id)
                    .is_some_and(|table| table.indexes().iter().any(|index| index.id == item.id))
            } else {
                item.id > 0
                    && self.kv_table_by_id(item.table_id).is_some_and(|table| {
                        table.columns().iter().any(|column| column.id == item.id)
                    })
            };
            let load_needed = handle
                .table_statistics(item.table_id, || self.statistics_schema(item.table_id))
                .is_some_and(|statistics| {
                    if item.is_index {
                        statistics.index_load_needed(item.id).1
                    } else {
                        // Go also proceeds for an unanalyzed column, to
                        // publish its fake `EmptyColumn`; the loader makes it.
                        statistics.column_load_needed(item.id, true).1
                    }
                });
            if !valid_metadata || !load_needed {
                needed.delete(item);
                continue;
            }
            let requested = tidb_model::StatsLoadItem {
                table_item_id: item,
                full_load: item.is_index || requested.full_load,
            };
            let loaded = loader.load_items(std::slice::from_ref(&requested), resource_group);
            needed.delete(item);
            let tables = loaded?;
            let mut values = handle
                .values
                .write()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            for (table_id, statistics) in tables {
                values.insert(table_id, statistics);
            }
        }
        Ok(())
    }

    /// Go `CollectPredicateColumnsPoint`'s demand after `collectSyncIndices`
    /// and the static-pruning expansion: every column and index the
    /// statement's plan needs statistics of (`true`: the whole payload), whether
    /// or not the shared cache already holds it.
    fn statistics_demand(
        &self,
        handle: &StatsHandle,
        usage: &tidb_stats::table_statistics::ColumnStatsUsage,
    ) -> HashMap<tidb_model::TableItemID, bool> {
        let mut columns = usage.predicate_columns.clone();

        // Go `markAtLeastOneFullStatsLoadForEachTable`: determinate mode
        // keeps at least one analyzed, non-virtual column payload per table
        // unless some column or non-MV index is already full.
        {
            for table_id in &usage.visited_logical_table_ids {
                let Some(statistics) = handle.table_statistics(*table_id, || self.statistics_schema(*table_id)) else {
                    continue;
                };
                if columns.iter().any(|(item, full)| {
                    item.table_id == *table_id
                        && *full
                        && has_analyzed(&statistics, item.id, false)
                }) {
                    continue;
                }
                // Go reads the cached `tblStats.Pseudo` straight off the
                // handle, not `GetStatsTable`'s planner copy: an analyzed
                // table whose count is zero still asks for a load.
                let mut has_full = false;
                statistics.hist_coll.for_each_column(|_, column| {
                    has_full = column.is_full_load();
                    has_full
                });
                statistics.hist_coll.for_each_index(|_, index| {
                    has_full |= index.is_full_load()
                        && !index.info.as_ref().is_some_and(|info| info.mv_index);
                    has_full
                });
                if statistics.hist_coll.pseudo || has_full {
                    continue;
                }
                let Some(table) = self.kv_table_by_id(*table_id) else {
                    continue;
                };
                if let Some(column) = table.columns().iter().find(|column| {
                    column
                        .generated
                        .as_ref()
                        .is_none_or(|generated| generated.stored)
                        && has_analyzed(&statistics, column.id, false)
                }) {
                    columns.insert(
                        tidb_model::TableItemID {
                            table_id: *table_id,
                            id: column.id,
                            is_index: false,
                            is_sync_load_failed: false,
                        },
                        true,
                    );
                }
            }
        }

        let mut items: HashMap<tidb_model::TableItemID, bool> = columns.clone();
        // Go `CollectDependingVirtualCols` is used only to discover expression
        // indexes; virtual columns themselves are not load items.
        let mut index_source_columns = columns.keys().copied().collect::<Vec<_>>();
        for column in columns.keys() {
            let Some(table) = self.kv_table_by_id(column.table_id) else {
                continue;
            };
            let Some(name) = table
                .columns()
                .iter()
                .find(|metadata| metadata.id == column.id)
                .map(|metadata| metadata.name.as_str())
            else {
                continue;
            };
            index_source_columns.extend(table.columns().iter().filter_map(|metadata| {
                let generated = metadata.generated.as_ref()?;
                (!generated.stored
                    && generated
                        .dependencies
                        .iter()
                        .any(|dependency| dependency.go_lower_eq(name)))
                .then_some(tidb_model::TableItemID {
                    table_id: column.table_id,
                    id: metadata.id,
                    is_index: false,
                    is_sync_load_failed: false,
                })
            }));
        }
        for column in index_source_columns {
            let Some(table) = self.kv_table_by_id(column.table_id) else {
                continue;
            };
            let Some(offset) = table
                .columns()
                .iter()
                .position(|metadata| metadata.id == column.id)
            else {
                continue;
            };
            let Some(statistics) = handle.table_statistics(column.table_id, || self.statistics_schema(column.table_id)) else {
                continue;
            };
            // Go `collectSyncIndices`: the handle's cached `tblStats.Pseudo`.
            if statistics.hist_coll.pseudo {
                continue;
            }
            for index in table.indexes() {
                if !index.column_offsets.contains(&offset)
                    || !statistics.index_load_needed(index.id).1
                {
                    continue;
                }
                if usage
                    .kept_index_ids
                    .get(&column.table_id)
                    .is_some_and(|kept| !kept.contains(&index.id))
                {
                    continue;
                }
                items.insert(
                    tidb_model::TableItemID {
                        table_id: column.table_id,
                        id: index.id,
                        is_index: true,
                        is_sync_load_failed: false,
                    },
                    true,
                );
            }
        }

        // Go expands the combined column/index demand after collection. The
        // map is populated only by static-pruning partition data sources.
        let logical_items = items.clone();
        for (item, full_load) in logical_items {
            if let Some(partition_ids) = usage.table_partition_ids.get(&item.table_id) {
                for partition_id in partition_ids {
                    items.insert(
                        tidb_model::TableItemID {
                            table_id: *partition_id,
                            id: item.id,
                            is_index: item.is_index,
                            is_sync_load_failed: false,
                        },
                        full_load,
                    );
                }
            }
        }

        items
    }

    fn statistics_load_items(
        &self,
        handle: &StatsHandle,
        usage: &tidb_stats::table_statistics::ColumnStatsUsage,
        _determinate: bool,
    ) -> Vec<tidb_model::StatsLoadItem> {
        let mut items = self.statistics_demand(handle, usage);
        // Go's stats handle drops requests already satisfied by the shared
        // cache before queueing workers.
        items.retain(|item, full_load| {
            let Some(statistics) = handle.table_statistics(item.table_id, || self.statistics_schema(item.table_id)) else {
                return false;
            };
            // Go `statsSyncLoad.removeHistLoadedColumns`.
            if item.is_index {
                return statistics.index_load_needed(item.id).1;
            }
            statistics.column_load_needed(item.id, *full_load).1
        });

        let mut result = items
            .into_iter()
            .map(|(table_item_id, full_load)| tidb_model::StatsLoadItem {
                table_item_id,
                full_load,
            })
            .collect::<Vec<_>>();
        result.sort_by_key(|item| {
            (
                item.table_item_id.table_id,
                item.table_item_id.is_index,
                item.table_item_id.id,
            )
        });
        result
    }





    /// One table's loaded statistics as `handle` holds them; `None` is Go's
    /// `PseudoTable`.
    fn table_statistics_in(
        &self,
        handle: &StatsHandle,
        table_id: i64,
    ) -> Option<Arc<tidb_stats::Table>> {
        handle.table_statistics(table_id, || self.statistics_schema(table_id))
    }
}

/// Go `ColAndIdxExistenceMap.HasAnalyzed`.
fn has_analyzed(statistics: &tidb_stats::Table, id: i64, is_index: bool) -> bool {
    statistics.existence_map.as_ref().is_some_and(|map| {
        map.read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .has_analyzed(id, is_index)
    })
}

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

//! The statistics a statement's planning reads beside the schema image: Go's
//! `Handle.GetPhysicalTableStats` over the domain's statistics view, the
//! sync and async load of the items the plan needs
//! (`SyncWaitStatsLoad`, `CollectColumnStatsUsage`), and the in-process
//! statistics tests publish.

use crate::*;
use tidb_stats_handle::StatsHandle;

/// Go `PseudoTable`'s schema inputs from a catalog table, whose columns and
/// indexes are all public; a column past the visible ones is hidden.
pub fn pseudo_table_info(table: &crate::KvTable) -> tidb_stats::PseudoTableInfo {
    tidb_stats::PseudoTableInfo {
        id: table.table_id(),
        pk_is_handle: table.pk_handle_offset().is_some(),
        columns: table
            .columns()
            .iter()
            .enumerate()
            .map(|(offset, column)| tidb_stats::PseudoColumnInfo {
                info: tidb_stats::ColumnInfo {
                    id: column.id,
                    name: column.name.go_to_lower(),
                    primary_key: column
                        .field_type
                        .has_flag(tidb_datatype::FieldTypeFlags::PRI_KEY),
                },
                public: true,
                hidden: table.is_hidden(offset),
            })
            .collect(),
        indices: table
            .indexes()
            .iter()
            .map(|index| tidb_stats::PseudoIndexInfo {
                info: tidb_stats::IndexInfo {
                    id: index.id,
                    name: index.name.go_to_lower(),
                    columns: index
                        .column_offsets
                        .iter()
                        .filter_map(|offset| table.columns().get(*offset))
                        .map(|column| column.name.go_to_lower())
                        .collect(),
                    mv_index: table.mv_key_part_source(index.id).is_some(),
                },
                public: true,
            })
            .collect(),
    }
}

impl Catalog {
    /// Go `storage.CleanFakeItemsForShowHistInFlights`: remove queue entries
    /// whose cache state no longer requires a load and return the number that
    /// remain genuinely needed.
    pub fn clean_needed_statistics_items(&self, handle: &StatsHandle) -> i64 {
        let needed = &tidb_stats::ASYNC_LOAD_HISTOGRAM_NEEDED_ITEMS;
        let mut really_needed = 0_i64;
        for requested in needed.all_items() {
            let item = requested.table_item_id;
            let Some(statistics) = handle.table_statistics(item.table_id, || self.statistics_schema(item.table_id)) else {
                needed.delete(item);
                continue;
            };
            let load_needed = if item.is_index {
                statistics.index_is_load_needed(item.id)
            } else {
                statistics.column_is_load_needed(item.id, requested.full_load)
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
    pub fn load_needed_histograms(&self, handle: &StatsHandle, resource_group: &str) -> Result<(), String> {
        let loader = handle
            .loader
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        let Some(loader) = loader else {
            return Ok(());
        };
        let needed = &tidb_stats::ASYNC_LOAD_HISTOGRAM_NEEDED_ITEMS;
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
                        statistics.index_is_load_needed(item.id)
                    } else {
                        statistics.column_is_load_needed(item.id, true)
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
    pub fn statistics_demand(
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
                        && statistics
                            .column_stats_existence
                            .get(&item.id)
                            .copied()
                            .unwrap_or(false)
                }) {
                    continue;
                }
                // Go reads the cached `tblStats.Pseudo` straight off the
                // handle, not `GetStatsTable`'s planner copy: an analyzed
                // table whose count is zero still asks for a load.
                if statistics.is_synthetic_pseudo()
                    || statistics
                        .column_load_status
                        .values()
                        .any(|status| status.is_full_load())
                    || statistics
                        .index_load_status
                        .iter()
                        .any(|(index_id, status)| {
                            status.is_full_load()
                                && self.kv_table_by_id(*table_id).is_none_or(|table| {
                                    table.mv_key_part_source(*index_id).is_none()
                                })
                        })
                {
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
                        && statistics
                            .column_stats_existence
                            .get(&column.id)
                            .copied()
                            .unwrap_or(false)
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
            if statistics.is_synthetic_pseudo() {
                continue;
            }
            for index in table.indexes() {
                if !index.column_offsets.contains(&offset)
                    || !statistics.index_is_load_needed(index.id)
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

    pub fn statistics_load_items(
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
            if item.is_index {
                return statistics.index_is_load_needed(item.id);
            }
            statistics.column_is_load_needed(item.id, *full_load)
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
    #[must_use]
    pub fn table_statistics_in(
        &self,
        handle: &StatsHandle,
        table_id: i64,
    ) -> Option<Arc<tidb_stats::table_statistics::TableStatistics>> {
        handle.table_statistics(table_id, || self.statistics_schema(table_id))
    }

    /// The schema facts the statistics view converts a table's canonical
    /// statistics under (Go `Handle.getStatsByPhysicalID` reads the same
    /// `TableInfo`), for a table or partition id of this image.
    #[must_use]
    pub fn statistics_schema(&self, table_id: i64) -> Option<Arc<StatisticsSchema>> {
        let schemas = self.statistics_schemas.get_or_init(|| {
                let mut schemas = HashMap::new();
                // The shared schema's tables and the session's LOCAL
                // temporary ones, which Go's `TableByID` finds too.
                let entries = self
                    .schema.databases
                    .values()
                    .flat_map(|database| {
                        let database_name = database.name.go_to_lower();
                        database
                            .tables
                            .values()
                            .map(move |entry| (database_name.clone(), entry))
                    })
                    .chain(
                        self.local_temporary
                            .iter()
                            .map(|(key, entry)| (key.database.clone(), entry)),
                    );
                for (database_name, entry) in entries {
                    // Go `Handle.isSystemTable`: `filter.IsSystemSchema(db.Name.L)`.
                    let system_schema = tidb_util::filter::is_system_schema(&database_name);
                    {
                        let TableEntry::Kv(table) = entry.as_ref() else {
                            continue;
                        };
                        let schema = Arc::new(StatisticsSchema {
                            columns: table
                                .columns()
                                .iter()
                                .map(|column| (column.id, column.field_type.is_unsigned()))
                                .collect(),
                            indexes: table
                                .indexes()
                                .iter()
                                .map(|index| (index.id, index.column_offsets.len(), index.unique))
                                .collect(),
                            pseudo: PseudoStatsTable {
                                info: pseudo_table_info(table),
                                partitioned: table.partition().is_some(),
                                cacheable: table.temp_table_type()
                                    == tidb_model::TempTableType::NONE
                                    && !system_schema,
                            },
                        });
                        if let Some(partition) = table.partition() {
                            for definition in &partition.definitions {
                                schemas.insert(definition.id, Arc::clone(&schema));
                            }
                        }
                        schemas.insert(table.table_id(), schema);
                    }
                }
                schemas
            });
        schemas.get(&table_id).cloned()
    }


    /// Replaces the statement-time `information_schema` storage estimates
    /// without moving either catalog epoch. Go keeps these values in the
    /// statement-local `TableSizeStats`; the catalog overlay is only a Rust
    /// executor transport detail.
    pub fn set_table_storage_statistics(
        &mut self,
        table_id: i64,
        table_statistics: (u64, u64, u64, u64),
        partition_statistics: &[(i64, (u64, u64, u64, u64))],
    ) {
        for database in Arc::make_mut(&mut self.schema.databases).values_mut() {
            for entry in Arc::make_mut(database).tables.values_mut() {
                let TableEntry::Kv(table) = Arc::make_mut(entry) else {
                    continue;
                };
                if table.table_id() != table_id {
                    continue;
                }
                let table = Arc::make_mut(table);
                table.set_storage_statistics(table_statistics);
                for (physical_id, statistics) in partition_statistics {
                    table.set_partition_storage_statistics(*physical_id, *statistics);
                }
                return;
            }
        }
    }

    /// Clears statement-derived information-schema storage estimates from a
    /// scratch catalog after a seam provider refused the read. Go's reader
    /// never fails a query over a stats refresh error; the zeroed statement is
    /// the seam fallback for providers that surface their own errors.
    pub fn clear_table_storage_statistics(&mut self) {
        for database in Arc::make_mut(&mut self.schema.databases).values_mut() {
            for entry in Arc::make_mut(database).tables.values_mut() {
                let TableEntry::Kv(table) = Arc::make_mut(entry) else {
                    continue;
                };
                Arc::make_mut(table).clear_storage_statistics();
            }
        }
    }

    /// Go `GetTblInfoForUsedStatsByPhysicalID`'s name: the table's name, then
    /// ` <partition>` for a partition id or ` global` for a partitioned
    /// table's own id, and `tableID <id>` when no table owns the id.
    pub fn used_stats_table_name(&self, physical_id: i64) -> String {
        let Some((_, table)) = self.kv_table_by_physical_id(physical_id) else {
            return format!("tableID {physical_id}");
        };
        let mut name = table.name().clone();
        if let Some(partition) = table.partition() {
            match partition
                .definitions
                .iter()
                .find(|definition| definition.id == physical_id)
            {
                Some(definition) => {
                    name.push(' ');
                    name.push_str(&definition.name);
                }
                None if !partition.definitions.is_empty() => name.push_str(" global"),
                None => {}
            }
        }
        name
    }
}

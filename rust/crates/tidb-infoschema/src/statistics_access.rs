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
                    flag: column.field_type.flags() as usize,
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
                    unique: index.unique,
                },
                public: true,
            })
            .collect(),
    }
}

impl Catalog {
    /// The `TableInfo` facts Go `Handle.getStatsByPhysicalID` reads on a
    /// statistics cache miss, for a table or partition id of this image.
    #[must_use]
    pub fn statistics_schema(&self, table_id: i64) -> Option<Arc<tidb_stats::PseudoStatsTable>> {
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
                    let system_schema = tidb_filter::is_system_schema(&database_name);
                    {
                        let TableEntry::Kv(table) = entry.as_ref() else {
                            continue;
                        };
                        let schema = Arc::new(tidb_stats::PseudoStatsTable {
                            info: pseudo_table_info(table),
                            partitioned: table.partition().is_some(),
                            cacheable: table.temp_table_type() == tidb_model::TempTableType::NONE
                                && !system_schema,
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

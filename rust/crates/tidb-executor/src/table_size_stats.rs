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

//! Go `cache.TableRowStatsCache`'s storage estimates
//! (`pkg/statistics/handle/cache/stats_table_row_cache.go`) over the
//! in-process catalog: what `information_schema.TABLES` and `PARTITIONS`
//! report as `TABLE_ROWS`, `AVG_ROW_LENGTH`, `DATA_LENGTH` and
//! `INDEX_LENGTH`.
//!
//! Go reads `mysql.stats_meta.count` and the column histograms'
//! `tot_col_size`. The in-process store keeps both in the catalog's table
//! statistics, the same source `SHOW STATS_META` reads.

use crate::kv_table::KvTable;
use crate::Catalog;
use tidb_datatype::{UNSPECIFIED_LENGTH, VAR_STORAGE_LEN};

/// `(rows, average row length, data length, index length)`.
pub type StorageEstimate = (u64, u64, u64, u64);

/// One table's estimates: the logical table, then each partition.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct TableStorageEstimate {
    /// The logical table ID.
    pub table_id: i64,
    /// Go `EstimateDataLength` for the table.
    pub table: StorageEstimate,
    /// The per-partition values Go's `PARTITIONS` reader computes.
    pub partitions: Vec<(i64, StorageEstimate)>,
}

/// The estimates of every stored table in `catalog`.
#[must_use]
pub fn table_storage_estimates(catalog: &Catalog) -> Vec<TableStorageEstimate> {
    let stats = CatalogSizeStats { catalog };
    let mut estimates = Vec::new();
    for database in catalog.database_names() {
        for name in catalog.table_names(&database).unwrap_or_default() {
            let Some(crate::TableEntry::Kv(table)) = catalog.table_in(&database, &name) else {
                continue;
            };
            let partitions = table
                .partition()
                .map(|partition| {
                    partition
                        .definitions
                        .iter()
                        .map(|definition| {
                            let rows = stats.table_rows(definition.id);
                            let (data, index) =
                                stats.data_and_index_length(table, definition.id, rows);
                            (definition.id, (rows, average(data, rows), data, index))
                        })
                        .collect()
                })
                .unwrap_or_default();
            estimates.push(TableStorageEstimate {
                table_id: table.table_id,
                table: stats.estimate_data_length(table),
                partitions,
            });
        }
    }
    estimates
}

fn average(data_length: u64, rows: u64) -> u64 {
    if rows == 0 {
        0
    } else {
        data_length / rows
    }
}

/// Go `TableSizeStats` reading the catalog's statistics.
struct CatalogSizeStats<'a> {
    catalog: &'a Catalog,
}

impl CatalogSizeStats<'_> {
    /// Go `GetTableRows`: `stats_meta.count`, zero without a row.
    fn table_rows(&self, physical_id: i64) -> u64 {
        self.catalog
            .table_statistics(physical_id)
            .map_or(0, |statistics| statistics.row_count.max(0) as u64)
    }

    /// Go `GetColLength`: the column histogram's `tot_col_size`.
    fn column_length(&self, physical_id: i64, column_id: i64) -> u64 {
        self.catalog
            .table_statistics(physical_id)
            .and_then(|statistics| {
                statistics
                    .columns
                    .get(&column_id)
                    .map(|column| column.histogram.tot_col_size.max(0) as u64)
            })
            .unwrap_or(0)
    }

    /// Go `EstimateDataLength`: a partitioned table sums its partitions'
    /// rows and data, and adds their local index lengths to its own global
    /// ones.
    fn estimate_data_length(&self, table: &KvTable) -> StorageEstimate {
        let mut rows = self.table_rows(table.table_id);
        let (mut data, mut index) = self.data_and_index_length(table, table.table_id, rows);
        if let Some(partition) = table.partition() {
            rows = 0;
            data = 0;
            for definition in &partition.definitions {
                let partition_rows = self.table_rows(definition.id);
                rows = rows.wrapping_add(partition_rows);
                let (partition_data, partition_index) =
                    self.data_and_index_length(table, definition.id, partition_rows);
                data = data.wrapping_add(partition_data);
                index = index.wrapping_add(partition_index);
            }
        }
        (rows, average(data, rows), data, index)
    }

    /// Go `GetDataAndIndexLength`: a fixed-width column costs its storage
    /// width per row, a variable-width one its histogram's total size; an
    /// index part costs its column's length, or its prefix per row. Global
    /// indexes count at the table level, local ones per partition.
    fn data_and_index_length(&self, table: &KvTable, physical_id: i64, rows: u64) -> (u64, u64) {
        let mut column_lengths = vec![0_u64; table.columns.len()];
        let mut data = 0_u64;
        for (offset, column) in table.columns.iter().enumerate() {
            let storage_length = column.field_type.storage_length();
            let length = if storage_length == VAR_STORAGE_LEN {
                self.column_length(physical_id, column.id)
            } else {
                rows.wrapping_mul(storage_length as u64)
            };
            data = data.wrapping_add(length);
            column_lengths[offset] = length;
        }
        let partitioned = table.partition().is_some();
        let mut index = 0_u64;
        for kv_index in table.indexes() {
            if partitioned {
                if kv_index.global && table.table_id != physical_id {
                    continue;
                }
                if !kv_index.global && table.table_id == physical_id {
                    continue;
                }
            }
            for (position, offset) in kv_index.column_offsets.iter().enumerate() {
                let prefix = kv_index.prefix_length(position);
                let length = if prefix == UNSPECIFIED_LENGTH {
                    column_lengths.get(*offset).copied().unwrap_or(0)
                } else {
                    rows.wrapping_mul(prefix as u64)
                };
                index = index.wrapping_add(length);
            }
        }
        (data, index)
    }
}

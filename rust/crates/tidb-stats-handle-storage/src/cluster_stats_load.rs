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

//! What one `ANALYZE` produced, in the shape Go's `storage/save.go` writes
//! (`ClusterTableStats`, `ClusterStatsItem`), and the statement-local
//! `TableSizeStats` of the information-schema size readers. Reading
//! statistics back is Go's statements in [`crate::read`].

use std::collections::BTreeMap;
use std::sync::{Arc, RwLock};

use tidb_datatype::{
    FieldType,
    UNSPECIFIED_LENGTH, VAR_STORAGE_LEN,
};
use tidb_model::table_info::TableInfo;
use tidb_stats::cmsketch::{
    CmsSketch, TopN,
};
use tidb_stats::histogram::Histogram;
use tidb_stats::{
    ColAndIdxExistenceMap, Column, ColumnInfo, HistColl, Index, StatsLoadedStatus,
    Table,
};


/// One `mysql.stats_meta` row consumed by the information-schema size reader.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TableRowCount {
    /// Physical table ID.
    pub table_id: i64,
    /// Persisted row count.
    pub count: u64,
}

/// One non-index `mysql.stats_histograms` row consumed by the information-schema size reader.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ColumnLength {
    /// Physical table ID.
    pub table_id: i64,
    /// Column histogram ID.
    pub histogram_id: i64,
    /// Persisted total column size. Negative values are clamped to zero.
    pub total_size: i64,
}

/// Per-query row counts and variable-width column sizes used by the
/// information-schema `TABLES` and `PARTITIONS` readers.
///
/// Go's `TableSizeStats` is deliberately not process-global: every statement
/// reads `stats_meta`, and optionally `stats_histograms`, into a fresh value.
#[derive(Clone, Debug, Default)]
pub struct TableSizeStats {
    table_rows: BTreeMap<i64, u64>,
    column_lengths: BTreeMap<(i64, i64), u64>,
}

impl TableSizeStats {
    /// Builds one statement-local value from the two restricted reads.
    #[must_use]
    pub fn from_rows(rows: Vec<TableRowCount>, lengths: Vec<ColumnLength>) -> Self {
        Self {
            table_rows: rows
                .into_iter()
                .map(|row| (row.table_id, row.count))
                .collect(),
            column_lengths: lengths
                .into_iter()
                .map(|row| {
                    (
                        (row.table_id, row.histogram_id),
                        u64::try_from(row.total_size.max(0)).expect("nonnegative i64 fits in u64"),
                    )
                })
                .collect(),
        }
    }

    /// Go `TableSizeStats.GetTableRows`.
    #[must_use]
    pub fn get_table_rows(&self, table_id: i64) -> u64 {
        self.table_rows.get(&table_id).copied().unwrap_or_default()
    }

    /// Go `TableSizeStats.GetColLength`.
    #[must_use]
    fn get_column_length(&self, table_id: i64, histogram_id: i64) -> u64 {
        self.column_lengths
            .get(&(table_id, histogram_id))
            .copied()
            .unwrap_or_default()
    }

    /// Go `TableSizeStats.EstimateDataLength`.
    #[must_use]
    pub fn estimate_data_length(&self, table: &TableInfo) -> (u64, u64, u64, u64) {
        let mut row_count = self.get_table_rows(table.id);
        let (mut data_length, mut index_length) =
            self.get_data_and_index_length(table, table.id, row_count);
        if let Some(partition) = table.get_partition_info() {
            row_count = 0;
            data_length = 0;
            for definition in partition.read().definitions.snapshot() {
                let partition_rows = self.get_table_rows(definition.id);
                row_count = row_count.wrapping_add(partition_rows);
                let (partition_data, partition_index) =
                    self.get_data_and_index_length(table, definition.id, partition_rows);
                data_length = data_length.wrapping_add(partition_data);
                index_length = index_length.wrapping_add(partition_index);
            }
        }
        let average_row_length = if row_count == 0 {
            0
        } else {
            data_length / row_count
        };
        if table.is_sequence() {
            row_count = 1;
        }
        (row_count, average_row_length, data_length, index_length)
    }

    /// Go `TableSizeStats.GetDataAndIndexLength`.
    #[must_use]
    pub fn get_data_and_index_length(
        &self,
        table: &TableInfo,
        physical_id: i64,
        row_count: u64,
    ) -> (u64, u64) {
        let mut column_lengths = vec![0_u64; table.columns.len()];
        let mut data_length = 0_u64;
        for (offset, column) in table.columns.iter_deref().enumerate() {
            let column = column.read();
            if column.state != tidb_model::SchemaState::PUBLIC {
                continue;
            }
            let storage_length = column.field_type.storage_length();
            let length = if storage_length == VAR_STORAGE_LEN {
                self.get_column_length(physical_id, column.id)
            } else {
                row_count.wrapping_mul(storage_length as u64)
            };
            data_length = data_length.wrapping_add(length);
            column_lengths[offset] = length;
        }

        let partitioned = table.get_partition_info().is_some();
        let mut index_length = 0_u64;
        for index in table.indices.iter_deref() {
            let index = index.read();
            if index.state != tidb_model::SchemaState::PUBLIC {
                continue;
            }
            if partitioned {
                if index.global && table.id != physical_id {
                    continue;
                }
                if !index.global && table.id == physical_id {
                    continue;
                }
            }
            for index_column in index.columns.iter_deref() {
                let index_column = index_column.read();
                let length = if index_column.length == UNSPECIFIED_LENGTH {
                    column_lengths[index_column.offset as usize]
                } else {
                    row_count.wrapping_mul(index_column.length as u64)
                };
                index_length = index_length.wrapping_add(length);
            }
        }
        (data_length, index_length)
    }
}

/// One column's or one index's statistics, as one cluster snapshot holds them.
#[derive(Clone, Debug)]
pub struct ClusterStatsItem {
    /// `hist_id`: the column ID, or the index ID when [`Self::is_index`].
    pub id: i64,
    /// Whether this is an index histogram rather than a column histogram.
    pub is_index: bool,
    /// `stats_ver`: which `ANALYZE` generation wrote this.
    pub stats_ver: i64,
    /// `flag`, Go's `statistics.AnalyzeFlag` bitset.
    pub flag: i64,
    /// Go `StatsLoadedStatus`: whether the item has its full payload or only
    /// histogram metadata resident.
    pub load_status: StatsLoadedStatus,
    /// The histogram, with its buckets in ascending bound order and its
    /// counts made cumulative the way Go's in-memory `Bucket.Count` is.
    pub histogram: Histogram,
    /// `mysql.stats_top_n`'s rows, when any.
    pub topn: Option<TopN>,
    /// `mysql.stats_histograms.cm_sketch`, when it holds one.
    pub cms: Option<CmsSketch>,
    /// `mysql.stats_fm_sketch`, loaded only for partition-global merging.
    pub fm_sketch: Option<tidb_stats::FmSketch>,
}

/// Everything one cluster snapshot says about one table's statistics.
#[derive(Clone, Debug)]
pub struct ClusterTableStats {
    /// The physical table ID these rows key on.
    pub table_id: i64,
    /// `mysql.stats_meta.version`, the TSO the stats were last written at.
    pub version: u64,
    /// `mysql.stats_meta.snapshot`, the snapshot TSO whose rows ANALYZE read.
    pub snapshot: u64,
    /// `mysql.stats_meta.modify_count`: rows changed since that write.
    pub modify_count: i64,
    /// `mysql.stats_meta.count`: the table's estimated row count.
    pub row_count: u64,
    /// Go `statistics.Table.LastAnalyzeVersion`: initialized from the analyze
    /// snapshot and advanced by analyzed column/index histogram versions.
    pub last_analyze_version: u64,
    /// `mysql.stats_meta.last_stats_histograms_version`, Go's independent
    /// table-level histogram refresh marker.
    pub last_stats_hist_version: u64,
    /// Column histograms, in `hist_id` order.
    pub columns: Vec<ClusterStatsItem>,
    /// Index histograms, in `hist_id` order.
    pub indexes: Vec<ClusterStatsItem>,
}

impl ClusterTableStats {
    fn analyzed_histogram_version(&self) -> u64 {
        self.columns
            .iter()
            .chain(&self.indexes)
            .filter(|item| item.stats_ver != 0)
            .map(|item| item.histogram.last_update_version)
            .max()
            .unwrap_or_default()
    }

    /// The column histogram for one column ID.
    #[must_use]
    pub fn column(&self, id: i64) -> Option<&ClusterStatsItem> {
        self.columns.iter().find(|item| item.id == id)
    }

    /// The index histogram for one index ID.
    #[must_use]
    pub fn index(&self, id: i64) -> Option<&ClusterStatsItem> {
        self.indexes.iter().find(|item| item.id == id)
    }

    /// Converts the storage row image into Go's canonical `statistics.Table`.
    /// Planner-specific views must be derived from this full table rather
    /// than cached independently.
    #[must_use]
    pub fn to_statistics_table(&self, table_info: &TableInfo) -> Arc<Table> {

        let row_count = stats_meta_count_as_go_int64(self.row_count);
        let stats_version = self
            .columns
            .iter()
            .chain(&self.indexes)
            .map(|item| item.stats_ver)
            .max()
            .unwrap_or_default();
        let mut hist_coll = HistColl::new(
            self.table_id,
            row_count,
            self.modify_count,
            self.columns.len(),
            self.indexes.len(),
        );
        hist_coll.stats_version = i32::try_from(stats_version).unwrap_or_else(|_| {
            if stats_version.is_negative() {
                i32::MIN
            } else {
                i32::MAX
            }
        });
        let mut existence =
            ColAndIdxExistenceMap::new(table_info.columns.len(), table_info.indices.len());

        // Go `statistics.PseudoTable` seeds schema-visible items as known,
        // but not analyzed. Storage may have a real stats_meta row before
        // any column or index histogram rows exist; starting from the schema
        // keeps later load decisions and `ColAndIdxExistenceMap` queries
        // equivalent to that metadata-only table.
        for column in table_info.cols().iter_deref() {
            let column = column.read();
            if column.state == tidb_model::SchemaState::PUBLIC && !column.hidden {
                existence.insert_column(column.id, false);
            }
        }
        for index in table_info.indices.iter_deref() {
            let index = index.read();
            if index.state == tidb_model::SchemaState::PUBLIC {
                existence.insert_index(index.id, false);
            }
        }

        for item in &self.columns {
            let Some(column) = item.to_column(self.table_id, table_info) else {
                continue;
            };
            existence.insert_column(
                item.id,
                item.stats_ver != 0 || item.histogram.ndv > 0 || item.histogram.null_count > 0,
            );
            hist_coll.set_column(item.id, column);
        }
        for item in &self.indexes {
            let Some(index) = item.to_index(self.table_id, table_info) else {
                continue;
            };
            existence.insert_index(item.id, item.stats_ver != 0);
            hist_coll.set_index(item.id, index);
        }

        Arc::new(Table {
            existence_map: Some(Arc::new(RwLock::new(existence))),
            hist_coll,
            version: self.version,
            last_analyze_version: self
                .last_analyze_version
                .max(self.analyzed_histogram_version()),
            last_stats_hist_version: self.last_stats_hist_version.max(self.snapshot),
            table_info_update_ts: table_info.update_ts,
            is_pk_handle: table_info.pk_is_handle,
        })
    }
}

/// Go's load state for one `mysql.stats_histograms` row: an item never
/// analyzed (and, for a column, holding no counted value) is uninitialized;
/// otherwise it is resident when its payload was read, evicted when not.
fn histogram_row_load_status(
    is_index: bool,
    stats_ver: i64,
    histogram: &Histogram,
    full_load: bool,
) -> StatsLoadedStatus {
    let initialized = if is_index {
        stats_ver != 0
    } else {
        stats_ver != 0 || histogram.ndv > 0 || histogram.null_count > 0
    };
    if !initialized {
        StatsLoadedStatus::default()
    } else if full_load {
        StatsLoadedStatus::full_load()
    } else {
        StatsLoadedStatus::all_evicted()
    }
}

/// One `mysql.stats_histograms` row as the metadata-only (`loadAll = false`)
/// load reads it, for a caller that read the row itself: no buckets, TopN or
/// sketch, and an index keeps no `tot_col_size` or `correlation`
/// (`read.go::LoadHistogram`).
#[allow(clippy::too_many_arguments)]
#[must_use]
pub fn histogram_row_item(
    id: i64,
    is_index: bool,
    stats_ver: i64,
    flag: i64,
    ndv: i64,
    null_count: i64,
    version: u64,
    tot_col_size: i64,
    correlation: f64,
) -> ClusterStatsItem {
    let histogram = Histogram {
        id,
        ndv,
        null_count,
        last_update_version: version,
        tot_col_size: if is_index { 0 } else { tot_col_size },
        correlation: if is_index { 0.0 } else { correlation },
        ..Histogram::default()
    };
    ClusterStatsItem {
        id,
        is_index,
        stats_ver,
        flag,
        load_status: histogram_row_load_status(is_index, stats_ver, &histogram, false),
        histogram,
        topn: None,
        cms: None,
        fm_sketch: None,
    }
}

impl ClusterStatsItem {
    pub fn to_column(&self, table_id: i64, table_info: &TableInfo) -> Option<Column> {
        let metadata = table_info.find_column_by_id(self.id)?;
        let is_handle = is_handle_column(table_info, &metadata);
        Some(Column {
            cmsketch: self.cms.clone(),
            fm_sketch: self.fm_sketch.clone(),
            top_n: self.topn.clone(),
            info: Some(metadata),
            histogram: self.histogram.clone(),
            stats_loaded_status: self.load_status,
            physical_id: table_id,
            stats_version: self.stats_ver,
            is_handle,
            ..Column::default()
        })
    }

    pub fn to_index(&self, table_id: i64, table_info: &TableInfo) -> Option<Index> {
        let metadata = table_info.find_index_by_id(self.id)?;
        Some(Index {
            cmsketch: self.cms.clone(),
            fm_sketch: self.fm_sketch.clone(),
            top_n: self.topn.clone(),
            info: Some(metadata),
            histogram: self.histogram.clone(),
            stats_loaded_status: self.load_status,
            stats_version: self.stats_ver,
            physical_id: table_id,
            ..Index::default()
        })
    }
}

/// Go `tblInfo.PKIsHandle && mysql.HasPriKeyFlag(colInfo.GetFlag())`.
fn is_handle_column(table_info: &TableInfo, column: &ColumnInfo) -> bool {
    table_info.pk_is_handle
        && column
            .read()
            .field_type
            .has_flag(tidb_datatype::FieldTypeFlags::PRI_KEY)
}

/// The declared type of each column a table's column histograms are keyed by.
///
/// A column histogram's `hist_id` is the stored column ID, keyed to the
/// analyzed table's own declared column types.
#[must_use]
pub fn column_types_of(table: &TableInfo) -> BTreeMap<i64, FieldType> {
    table
        .cols()
        .iter_deref()
        .map(|column| {
            let column = column.read();
            (column.id, column.field_type.clone())
        })
        .collect()
}

fn stats_meta_count_as_go_int64(count: u64) -> i64 {
    // Go's chunk.Row.GetInt64 reads the same eight-byte slot that its unsigned
    // SQL decoder populated. Preserve that two's-complement result rather
    // than adding a Rust-only saturation policy.
    count as i64
}

#[cfg(test)]
mod tests {
    use tidb_ast::CiString;
    use tidb_datatype::FieldTypeCode;
    use tidb_model::{GoShared, GoSharedPointerSlice};

    use super::*;

    /// Every bound fixture below is the exact `mysql.stats_buckets` blob a
    /// live v8.5.7 playground wrote for `ANALYZE TABLE` over a known
    /// distribution, read back with `hex(lower_bound)`. They are the reason
    /// this module claims to know the encoding rather than assume it.

    #[test]
    fn storage_image_builds_the_canonical_full_statistics_table() {
        let mut primary_type = FieldType::new(FieldTypeCode::LongLong);
        primary_type.set_flag(tidb_datatype::FieldTypeFlags::PRI_KEY);
        let table_info = TableInfo {
            id: 7,
            update_ts: 55,
            pk_is_handle: true,
            columns: GoSharedPointerSlice::from_handles(vec![Some(GoShared::new(
                tidb_model::column::ColumnInfo {
                    id: 1,
                    name: CiString::new("A"),
                    field_type: primary_type,
                    state: tidb_model::SchemaState::PUBLIC,
                    ..tidb_model::column::ColumnInfo::default()
                },
            ))]),
            ..TableInfo::default()
        };
        let stats = ClusterTableStats {
            table_id: 7,
            version: 42,
            snapshot: 40,
            modify_count: 3,
            row_count: 100,
            last_analyze_version: 40,
            last_stats_hist_version: 43,
            columns: vec![
                ClusterStatsItem {
                    id: 1,
                    is_index: false,
                    stats_ver: 2,
                    flag: 1,
                    load_status: StatsLoadedStatus::full_load(),
                    histogram: Histogram {
                        id: 1,
                        ndv: 10,
                        last_update_version: 41,
                        ..Histogram::default()
                    },
                    topn: Some(TopN::new(0)),
                    cms: None,
                    fm_sketch: None,
                },
                // A dropped column's stale mysql.stats_histograms row is not
                // retained in Go's statistics.Table.
                ClusterStatsItem {
                    id: 99,
                    is_index: false,
                    stats_ver: 2,
                    flag: 0,
                    load_status: StatsLoadedStatus::all_evicted(),
                    histogram: Histogram {
                        id: 99,
                        ..Histogram::default()
                    },
                    topn: None,
                    cms: None,
                    fm_sketch: None,
                },
            ],
            indexes: Vec::new(),
        };

        let table = stats.to_statistics_table(&table_info);
        assert_eq!(table.hist_coll.physical_id, 7);
        assert_eq!(table.hist_coll.realtime_count, 100);
        assert_eq!(table.hist_coll.modify_count, 3);
        assert_eq!(table.hist_coll.stats_version, 2);
        assert_eq!(table.version, 42);
        assert_eq!(table.last_analyze_version, 41);
        assert_eq!(table.last_stats_hist_version, 43);
        assert_eq!(table.table_info_update_ts, 55);
        assert!(table.is_pk_handle);
        let column = table.hist_coll.get_column(1).unwrap();
        let column = column.read().unwrap();
        assert_eq!(column.info.as_ref().unwrap().read().name.lowercase(), "a");
        assert!(column.is_handle);
        assert!(column.is_full_load());
        assert!(table.hist_coll.get_column(99).is_none());
        let existence = table.existence_map.as_ref().unwrap().read().unwrap();
        assert!(existence.has_analyzed(1, false));
        assert!(!existence.has(99, false));
    }

    #[test]
    fn stats_meta_unsigned_count_uses_go_get_int64_bits() {
        assert_eq!(stats_meta_count_as_go_int64(i64::MAX as u64), i64::MAX);
        assert_eq!(stats_meta_count_as_go_int64(i64::MAX as u64 + 1), i64::MIN);
        assert_eq!(stats_meta_count_as_go_int64(u64::MAX), -1);
    }
}

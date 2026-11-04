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

//! Average row-size estimation from `pkg/planner/cardinality/row_size.go`.
//!
//! The Go implementation receives a planner context, expression columns, and
//! statistics objects.  Those owners do not exist in the seed planner yet, so
//! this leaf keeps the arithmetic at a typed statistics boundary.  The public
//! API deliberately does not invent a planner context or histogram catalog;
//! the eventual planner can adapt its real owners to these source-shaped
//! inputs without changing the formulas. Source integer fields remain
//! integers at this boundary; conversion happens only where Go converts them
//! to `float64` for arithmetic.

use tidb_planner_property::stats_info::RowSizeColumnStats;





/// A requested planner column and the type-width fallback used for pseudo
/// or missing statistics.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct RowSizeColumn {
    /// Loaded histogram, when one exists.
    pub stats: Option<RowSizeColumnStats>,
    /// `chunk.EstimateTypeWidth` for this column's exact static field type.
    pub estimated_width: f64,
}

impl RowSizeColumn {
    /// Creates a column with loaded statistics.
    #[must_use]
    pub const fn with_stats(stats: RowSizeColumnStats, estimated_width: f64) -> Self {
        Self {
            stats: Some(stats),
            estimated_width,
        }
    }

    /// Creates a pseudo/missing-statistics column.
    #[must_use]
    pub const fn without_stats(estimated_width: f64) -> Self {
        Self {
            stats: None,
            estimated_width,
        }
    }
}

/// Store engines used by `GetTableAvgRowSize`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RowSizeStore {
    /// TiKV row keys carry the 19-byte table/record prefix.
    TiKv,
    /// TiFlash may carry an 8-byte row ID when the handle is absent.
    TiFlash,
    /// TiDB's in-memory/global-view store with no additional row-key accounting.
    TiDb,
    /// Unknown store type with no additional row-key accounting.
    Unspecified,
}

const PSEUDO_COL_SIZE: f64 = 8.0;
const RECORD_ROW_KEY_LEN: f64 = 19.0;

// Go's built-in min/max propagate NaN and choose +0 for max(-0,+0), matching
// math.Min/math.Max at the boundaries used by this file.  Rust's f64 methods
// intentionally have different NaN behavior, so keep these operations local.
fn go_max(x: f64, y: f64) -> f64 {
    if x.is_nan() || y.is_nan() {
        return f64::NAN;
    }
    if x == 0.0 && x == y {
        return if x.is_sign_negative() { y } else { x };
    }
    if x > y { x } else { y }
}

fn round_two(value: f64) -> f64 {
    (value * 100.0).round() / 100.0
}

/// Computes the average encoded column size.
///
/// Direct port of `AvgColSize` in `row_size.go`, including its handle,
/// null-ratio, type-special, and two-decimal-place rules.
#[must_use]
pub fn avg_col_size(column: &RowSizeColumnStats, count: i64, is_key: bool) -> f64 {
    if count == 0 {
        return 0.0;
    }
    if column.is_handle {
        return 8.0;
    }
    let mut not_null_ratio = 1.0;
    if column.total_row_count > 0.0 {
        not_null_ratio = go_max(0.0, 1.0 - column.null_count as f64 / column.total_row_count);
    }
    if column.kind.uses_eight_byte_encoding(is_key) {
        return 8.0 * not_null_ratio;
    }
    go_max(
        0.0,
        round_two(column.total_column_size as f64 / count as f64),
    )
}

/// Computes the average chunk-format column size.
///
/// Direct port of `AvgColSizeChunkFormat`.  Variable columns account for
/// offsets and subtract `log2(avg_size)` for their length field.
#[must_use]
pub fn avg_col_size_chunk_format(column: &RowSizeColumnStats, count: i64) -> f64 {
    if count == 0 {
        return 0.0;
    }
    if let Some(fixed_len) = column.kind.fixed_len() {
        return fixed_len;
    }
    let avg_size = column.total_column_size as f64 / count as f64;
    if avg_size < 1.0 {
        return go_max(0.0, round_two(avg_size)) + 8.0;
    }
    go_max(0.0, round_two(avg_size - avg_size.log2())) + 8.0
}

/// Computes the average `DataInDiskByRows` column size.
///
/// Direct port of `AvgColSizeDataInDiskByRows`, preserving the un-clamped
/// null ratio for fixed-width types and the variable-width length calculation.
#[must_use]
pub fn avg_col_size_data_in_disk_by_rows(column: &RowSizeColumnStats, count: i64) -> f64 {
    if count == 0 {
        return 0.0;
    }
    let mut not_null_ratio = 1.0;
    if column.total_row_count > 0.0 {
        not_null_ratio = 1.0 - column.null_count as f64 / column.total_row_count;
    }
    if let Some(fixed_len) = column.kind.fixed_len() {
        return fixed_len * not_null_ratio;
    }
    let avg_size = column.total_column_size as f64 / count as f64;
    if avg_size < 1.0 {
        return go_max(0.0, round_two(avg_size));
    }
    round_two(avg_size - avg_size.log2())
}

/// Computes average row size for a selected column set.
///
/// This is the context-free equivalent of `GetAvgRowSize`.  `pseudo` models
/// `HistColl.Pseudo`; `realtime_count` models `HistColl.RealtimeCount`.
#[must_use]
pub fn get_avg_row_size(
    columns: &[RowSizeColumn],
    pseudo: bool,
    realtime_count: i64,
    is_encoded_key: bool,
    is_for_scan: bool,
    enable_chunk_rpc: bool,
) -> f64 {
    let mut size = 0.0;
    if pseudo || columns.is_empty() || realtime_count == 0 {
        size = PSEUDO_COL_SIZE * columns.len() as f64;
    } else {
        for column in columns {
            let Some(stats) = column.stats else {
                size += PSEUDO_COL_SIZE;
                continue;
            };
            if !stats.is_handle
                && stats.total_column_size == 0
                && stats.null_count != realtime_count
            {
                size += PSEUDO_COL_SIZE;
            } else if enable_chunk_rpc && !is_for_scan {
                size += avg_col_size_chunk_format(&stats, realtime_count);
            } else {
                size += avg_col_size(&stats, realtime_count, is_encoded_key);
            }
        }
    }
    size = go_max(0.0, size);
    if enable_chunk_rpc && !is_for_scan {
        size + columns.len() as f64 / 8.0
    } else {
        size + columns.len() as f64
    }
}

/// Computes average row size for an index scan.
#[must_use]
pub fn get_index_avg_row_size(
    columns: &[RowSizeColumn],
    pseudo: bool,
    realtime_count: i64,
    is_unique: bool,
    enable_chunk_rpc: bool,
) -> f64 {
    let mut size = get_avg_row_size(
        columns,
        pseudo,
        realtime_count,
        true,
        true,
        enable_chunk_rpc,
    );
    size += RECORD_ROW_KEY_LEN;
    if !is_unique {
        size += 1.0;
    }
    size
}

/// Computes average row size for a table scan.
#[must_use]
pub fn get_table_avg_row_size(
    columns: &[RowSizeColumn],
    pseudo: bool,
    realtime_count: i64,
    store: RowSizeStore,
    handle_in_cols: bool,
    enable_chunk_rpc: bool,
) -> f64 {
    let mut size = get_avg_row_size(
        columns,
        pseudo,
        realtime_count,
        false,
        true,
        enable_chunk_rpc,
    );
    match store {
        RowSizeStore::TiKv => {
            size += RECORD_ROW_KEY_LEN;
            size -= 8.0;
        }
        RowSizeStore::TiFlash if !handle_in_cols => size += 8.0,
        RowSizeStore::TiFlash | RowSizeStore::TiDb | RowSizeStore::Unspecified => {}
    }
    go_max(0.0, size)
}

/// Computes average on-disk row size for a selected column set.
#[must_use]
pub fn get_avg_row_size_data_in_disk_by_rows(
    columns: &[RowSizeColumn],
    pseudo: bool,
    realtime_count: i64,
) -> f64 {
    let mut size = 0.0;
    if pseudo || columns.is_empty() || realtime_count == 0 {
        size = columns
            .iter()
            .map(|column| column.estimated_width)
            .sum::<f64>();
    } else {
        for column in columns {
            let Some(stats) = column.stats else {
                size += column.estimated_width;
                continue;
            };
            if !stats.is_handle
                && stats.total_column_size == 0
                && stats.null_count != realtime_count
            {
                size += column.estimated_width;
            } else {
                size += avg_col_size_data_in_disk_by_rows(&stats, realtime_count);
            }
        }
    }
    go_max(0.0, size + 8.0 * columns.len() as f64)
}

#[cfg(test)]
mod tests {
    use tidb_planner_property::stats_info::RowSizeType;
    use super::{
        RowSizeColumnStats, avg_col_size, avg_col_size_chunk_format,
        avg_col_size_data_in_disk_by_rows,
    };

    /// Go `pkg/planner/cardinality.TestAvgColLen`: the same five column
    /// types are analyzed once, then analyzed after a second row is inserted.
    #[test]
    fn average_column_sizes_match_go_avg_col_len_fixture() {
        let first_analysis = [
            (
                RowSizeColumnStats::new(RowSizeType::Long, 1, 0, 1.0, false),
                1.0,
                8.0,
                8.0,
            ),
            (
                RowSizeColumnStats::new(RowSizeType::Variable, 8, 0, 1.0, false),
                8.0,
                5.0,
                13.0,
            ),
            (
                RowSizeColumnStats::new(RowSizeType::Float, 4, 0, 1.0, false),
                8.0,
                4.0,
                4.0,
            ),
            (
                RowSizeColumnStats::new(RowSizeType::Datetime, 8, 0, 1.0, false),
                8.0,
                8.0,
                8.0,
            ),
            (
                RowSizeColumnStats::new(RowSizeType::Variable, 0, 1, 1.0, false),
                0.0,
                0.0,
                8.0,
            ),
        ];
        for (column, avg, disk, chunk) in first_analysis {
            assert_eq!(avg_col_size(&column, 1, false), avg);
            assert_eq!(avg_col_size_data_in_disk_by_rows(&column, 1), disk);
            assert_eq!(avg_col_size_chunk_format(&column, 1), chunk);
        }

        let after_growth = [
            (
                RowSizeColumnStats::new(RowSizeType::Long, 3, 0, 2.0, false),
                1.5,
                8.0,
                8.0,
            ),
            (
                RowSizeColumnStats::new(RowSizeType::Variable, 21, 0, 2.0, false),
                10.5,
                ((10.5_f64 - 10.5_f64.log2()) * 100.0).round() / 100.0,
                ((10.5_f64 - 10.5_f64.log2()) * 100.0).round() / 100.0 + 8.0,
            ),
            (
                RowSizeColumnStats::new(RowSizeType::Float, 8, 0, 2.0, false),
                8.0,
                4.0,
                4.0,
            ),
            (
                RowSizeColumnStats::new(RowSizeType::Datetime, 16, 0, 2.0, false),
                8.0,
                8.0,
                8.0,
            ),
            (
                RowSizeColumnStats::new(RowSizeType::Variable, 0, 2, 2.0, false),
                0.0,
                0.0,
                8.0,
            ),
        ];
        for (column, avg, disk, chunk) in after_growth {
            assert_eq!(avg_col_size(&column, 2, false), avg);
            assert_eq!(avg_col_size_data_in_disk_by_rows(&column, 2), disk);
            assert_eq!(avg_col_size_chunk_format(&column, 2), chunk);
        }
    }
}

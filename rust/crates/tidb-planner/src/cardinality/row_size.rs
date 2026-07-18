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

/// The MySQL storage types whose row-size encoding has a fixed or special
/// planner width in TiDB.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RowSizeType {
    /// 32-bit floating-point values.
    Float,
    /// 64-bit floating-point values.
    Double,
    /// Duration/time values.
    Duration,
    /// Date values.
    Date,
    /// Datetime values.
    Datetime,
    /// Timestamp values.
    Timestamp,
    /// TINYINT values.
    Tiny,
    /// SMALLINT values.
    Short,
    /// MEDIUMINT values.
    Int24,
    /// INT values.
    Long,
    /// BIGINT values.
    LongLong,
    /// YEAR values.
    Year,
    /// ENUM values.
    Enum,
    /// BIT values.
    Bit,
    /// SET values.
    Set,
    /// DECIMAL values.
    Decimal,
    /// Variable-length string/blob-like values.
    Variable,
}

impl RowSizeType {
    /// Returns the chunk-memory width used by `GetFixedLen`.
    #[must_use]
    pub const fn fixed_len(self) -> Option<f64> {
        match self {
            Self::Float => Some(4.0),
            Self::Tiny
            | Self::Short
            | Self::Int24
            | Self::Long
            | Self::LongLong
            | Self::Double
            | Self::Year
            | Self::Duration => Some(8.0),
            // `unsafe.Sizeof(types.ZeroTime)` in the Go source is 8 bytes.
            Self::Date | Self::Datetime | Self::Timestamp => Some(8.0),
            // `types.MyDecimalStructSize` in the Go source is 40 bytes.
            Self::Decimal => Some(40.0),
            Self::Enum | Self::Bit | Self::Set | Self::Variable => None,
        }
    }

    /// Returns whether `AvgColSize` uses the 8-byte encoded-key/value rule.
    #[must_use]
    pub const fn uses_eight_byte_encoding(self, is_key: bool) -> bool {
        matches!(
            self,
            Self::Float
                | Self::Double
                | Self::Duration
                | Self::Date
                | Self::Datetime
                | Self::Timestamp
        ) || (is_key
            && matches!(
                self,
                Self::Tiny
                    | Self::Short
                    | Self::Int24
                    | Self::Long
                    | Self::LongLong
                    | Self::Year
                    | Self::Enum
                    | Self::Bit
                    | Self::Set
            ))
    }

    /// Returns Go `chunk.EstimateTypeWidth` for a variable type with `flen`.
    ///
    /// Fixed-width types use their chunk width.  A non-positive `flen` falls
    /// back to the source's 32-byte guess.
    #[must_use]
    pub const fn estimate_width(self, flen: i64) -> f64 {
        if let Some(width) = self.fixed_len() {
            return width;
        }
        if flen > 0 {
            if flen <= 32 {
                return flen as f64;
            }
            if flen < 1000 {
                return (32 + (flen - 32) / 2) as f64;
            }
            return (32 + (1000 - 32) / 2) as f64;
        }
        32.0
    }
}

/// The statistics fields consumed by the row-size formulas.
///
/// `total_row_count` mirrors Go's `Column.TotalRowCount()` (`float64`, because
/// histogram and TopN counts are summed as floating-point values). The
/// histogram's source integer fields retain their integer widths; arithmetic
/// conversion happens only where the Go implementation divides them.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct RowSizeColumnStats {
    /// Source histogram type.
    pub kind: RowSizeType,
    /// Whether this column is the handle column.
    pub is_handle: bool,
    /// Source `Histogram.TotColSize`.
    pub total_column_size: i64,
    /// Source `Histogram.NullCount`.
    pub null_count: i64,
    /// Source `Column.TotalRowCount()`.
    pub total_row_count: f64,
}

impl RowSizeColumnStats {
    /// Builds one source-shaped statistics record.
    #[must_use]
    pub const fn new(
        kind: RowSizeType,
        total_column_size: i64,
        null_count: i64,
        total_row_count: f64,
        is_handle: bool,
    ) -> Self {
        Self {
            kind,
            is_handle,
            total_column_size,
            null_count,
            total_row_count,
        }
    }
}

/// A requested planner column and the type-width fallback used for pseudo
/// or missing statistics.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct RowSizeColumn {
    /// Loaded histogram, when one exists.
    pub stats: Option<RowSizeColumnStats>,
    /// `chunk.EstimateTypeWidth` for this column's static type.
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
    if x > y {
        x
    } else {
        y
    }
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

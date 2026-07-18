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

//! Direct translation of `pkg/planner/cardinality/row_size_test.go`.

use tidb_planner::cardinality::row_size::{
    avg_col_size, avg_col_size_chunk_format, avg_col_size_data_in_disk_by_rows, get_avg_row_size,
    get_avg_row_size_data_in_disk_by_rows, get_index_avg_row_size, get_table_avg_row_size,
    RowSizeColumn, RowSizeColumnStats, RowSizeStore, RowSizeType,
};

fn stats(kind: RowSizeType, total_column_size: i64, null_count: i64) -> RowSizeColumnStats {
    RowSizeColumnStats::new(kind, total_column_size, null_count, 1.0, false)
}

fn column(stats: RowSizeColumnStats) -> RowSizeColumn {
    RowSizeColumn::with_stats(stats, stats.kind.estimate_width(100))
}

/// Direct translation of `TestAvgColLen`'s analyzed one-row assertions.
#[test]
fn test_avg_col_len_first_row() {
    let columns = [
        stats(RowSizeType::Long, 1, 0),
        stats(RowSizeType::Variable, 8, 0),
        stats(RowSizeType::Float, 0, 0),
        stats(RowSizeType::Datetime, 0, 0),
        stats(RowSizeType::Variable, 0, 1),
    ];

    assert_eq!(avg_col_size(&columns[0], 1, false), 1.0);
    assert_eq!(avg_col_size_data_in_disk_by_rows(&columns[0], 1), 8.0);
    assert_eq!(avg_col_size_chunk_format(&columns[0], 1), 8.0);

    // VARCHAR includes its one-byte length prefix in TotColSize.
    assert_eq!(avg_col_size(&columns[1], 1, false), 8.0);
    assert_eq!(avg_col_size(&columns[2], 1, false), 8.0);
    assert_eq!(avg_col_size(&columns[3], 1, false), 8.0);
    assert_eq!(avg_col_size_data_in_disk_by_rows(&columns[1], 1), 5.0);
    assert_eq!(avg_col_size_data_in_disk_by_rows(&columns[2], 1), 4.0);
    assert_eq!(avg_col_size_data_in_disk_by_rows(&columns[3], 1), 8.0);
    assert_eq!(avg_col_size_chunk_format(&columns[1], 1), 13.0);
    assert_eq!(avg_col_size_chunk_format(&columns[2], 1), 4.0);
    assert_eq!(avg_col_size_chunk_format(&columns[3], 1), 8.0);
    assert_eq!(avg_col_size_chunk_format(&columns[4], 1), 8.0);
    assert_eq!(avg_col_size_data_in_disk_by_rows(&columns[4], 1), 0.0);
}

/// Direct translation of `TestAvgColLen`'s analyzed two-row assertions.
#[test]
fn test_avg_col_len_second_row() {
    let columns = [
        stats(RowSizeType::Long, 3, 0),
        stats(RowSizeType::Variable, 21, 0),
        stats(RowSizeType::Float, 0, 0),
        stats(RowSizeType::Datetime, 0, 0),
        stats(RowSizeType::Variable, 0, 2),
    ];

    assert_eq!(avg_col_size(&columns[0], 2, false), 1.5);
    assert_eq!(avg_col_size(&columns[1], 2, false), 10.5);
    assert_eq!(avg_col_size(&columns[2], 2, false), 8.0);
    assert_eq!(avg_col_size(&columns[3], 2, false), 8.0);
    assert_eq!(avg_col_size_data_in_disk_by_rows(&columns[0], 2), 8.0);
    assert_eq!(
        avg_col_size_data_in_disk_by_rows(&columns[1], 2),
        ((10.5_f64 - 10.5_f64.log2()) * 100.0).round() / 100.0
    );
    assert_eq!(avg_col_size_data_in_disk_by_rows(&columns[2], 2), 4.0);
    assert_eq!(avg_col_size_data_in_disk_by_rows(&columns[3], 2), 8.0);
    assert_eq!(avg_col_size_chunk_format(&columns[0], 2), 8.0);
    assert_eq!(
        avg_col_size_chunk_format(&columns[1], 2),
        ((10.5_f64 - 10.5_f64.log2()) * 100.0).round() / 100.0 + 8.0
    );
    assert_eq!(avg_col_size_chunk_format(&columns[2], 2), 4.0);
    assert_eq!(avg_col_size_chunk_format(&columns[3], 2), 8.0);
    assert_eq!(avg_col_size_chunk_format(&columns[4], 2), 8.0);
    assert_eq!(avg_col_size_data_in_disk_by_rows(&columns[4], 2), 0.0);
}

#[test]
fn row_size_wrappers_preserve_source_accounting() {
    let columns = [
        column(stats(RowSizeType::Long, 1, 0)),
        column(stats(RowSizeType::Variable, 8, 0)),
    ];

    // Two value flags are added for the normal row format.
    assert_eq!(
        get_avg_row_size(&columns, false, 1, false, true, false),
        11.0
    );
    // Chunk RPC adds one null-bitmap byte per eight columns and uses chunk
    // widths for values.
    assert_eq!(
        get_avg_row_size(&columns, false, 1, false, false, true),
        21.25
    );
    assert_eq!(
        get_avg_row_size(&columns, true, 1, false, true, false),
        18.0
    );
    assert_eq!(
        get_avg_row_size_data_in_disk_by_rows(&columns, false, 1),
        29.0
    );
    assert_eq!(
        get_index_avg_row_size(&columns, false, 1, true, false),
        37.0
    );
    assert_eq!(
        get_index_avg_row_size(&columns, false, 1, false, false),
        38.0
    );
    assert_eq!(
        get_table_avg_row_size(&columns, false, 1, RowSizeStore::TiKv, true, false),
        22.0
    );
    assert_eq!(
        get_table_avg_row_size(&columns, false, 1, RowSizeStore::TiFlash, false, false),
        19.0
    );
}

#[test]
fn row_size_zero_count_and_missing_statistics_boundaries() {
    let handle = RowSizeColumnStats::new(RowSizeType::LongLong, 99, 0, 1.0, true);
    assert_eq!(avg_col_size(&handle, 0, false), 0.0);
    assert_eq!(avg_col_size(&handle, 1, false), 8.0);

    let missing = [RowSizeColumn::without_stats(12.0)];
    assert_eq!(
        get_avg_row_size(&missing, false, 1, false, true, false),
        9.0
    );
    assert_eq!(
        get_avg_row_size_data_in_disk_by_rows(&missing, false, 1),
        20.0
    );

    // Go's max(0, size) clamps negative variable-size estimates before the
    // per-column flag/size record is added.
    let negative = stats(RowSizeType::Variable, -10, 0);
    assert_eq!(avg_col_size(&negative, 1, false), 0.0);
    assert_eq!(avg_col_size_chunk_format(&negative, 1), 8.0);
    assert_eq!(avg_col_size_data_in_disk_by_rows(&negative, 1), 0.0);
}

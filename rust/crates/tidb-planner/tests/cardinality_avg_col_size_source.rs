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

#![allow(missing_docs)]

//! GO PORT of `pkg/planner/cardinality/row_size_test.go:30 TestAvgColLen`
//! (item 7 of the pkg/planner.part1 slice).
//!
//! Go analyzes table t(c1 int, c2 varchar(100), c3 float, c4 datetime,
//! c5 varchar(100)) after one and then two rows and pins
//! `AvgColSize` / `AvgColSizeDataInDiskByRows` / `AvgColSizeChunkFormat`
//! (`pkg/planner/cardinality/row_size.go:119/:169/:149`) against the analyzer's
//! accumulated per-column totals. The Rust formulas live in
//! [`tidb_planner::cardinality::row_size`]; the histogram owner they read is a
//! caller-owned record (`RowSizeColumnStats`), so this port reproduces exactly
//! the fields ANALYZE stored for those two rows:
//!
//! - row 1 `(1, '1234567', 12.3, '2018-03-07 19:00:57', NULL)`:
//!   c1 TotColSize 1 (varint of 1), c2 TotColSize 8 (1 length byte + 7 chars),
//!   c5 all-NULL so avgSize 0.
//! - row 2 `(132, '123456789112', ...)` adds varint(132)=2 bytes to c1
//!   (TotColSize 3 over 2 rows) and 13 bytes to c2 (TotColSize 21).

use tidb_planner::cardinality::row_size::{
    avg_col_size, avg_col_size_chunk_format, avg_col_size_data_in_disk_by_rows, RowSizeColumnStats,
    RowSizeType,
};

fn column(kind: RowSizeType, total_column_size: i64, null_count: i64) -> RowSizeColumnStats {
    RowSizeColumnStats {
        kind,
        is_handle: false,
        total_column_size,
        null_count,
        total_row_count: 1.0,
    }
}

#[test]
fn avg_col_len_matches_analyzed_single_row_shapes() {
    // row_size_test.go:43-58, RealtimeCount = 1 after the first insert/analyze.
    let count = 1;

    // c1 int, non-key encoding stores varint(value) = 1 byte for value 1.
    let c1 = column(RowSizeType::LongLong, 1, 0);
    assert_eq!(avg_col_size(&c1, count, false), 1.0);
    assert_eq!(avg_col_size_data_in_disk_by_rows(&c1, count), 8.0);
    assert_eq!(avg_col_size_chunk_format(&c1, count), 8.0);

    // The size of varchar type is LEN + BYTE, here is 1 + 7 = 8.
    let c2 = column(RowSizeType::Variable, 8, 0);
    assert_eq!(avg_col_size(&c2, count, false), 8.0);
    // float is 8 * notNullRatio via AvgColSize and `unsafe.Sizeof(float32)` =
    // 4 bytes on disk (row_size_test.go:49/:52).
    let c3 = column(RowSizeType::Float, 4, 0);
    assert_eq!(avg_col_size(&c3, count, false), 8.0);
    assert_eq!(avg_col_size_data_in_disk_by_rows(&c3, count), 4.0);
    assert_eq!(avg_col_size_chunk_format(&c3, count), 4.0);
    // datetime uses `unsafe.Sizeof(types.ZeroTime)` = 8 everywhere.
    let c4 = column(RowSizeType::Datetime, 8, 0);
    assert_eq!(avg_col_size(&c4, count, false), 8.0);
    assert_eq!(avg_col_size_data_in_disk_by_rows(&c4, count), 8.0);
    assert_eq!(avg_col_size_chunk_format(&c4, count), 8.0);

    // Second row before re-analyze: varchar(100) now totals 21 bytes over two
    // rows (row_size_test.go:59).
    // Variable-width columns subtract log2(avgSize) for their length field and
    // add the 8-byte offsets header: DataInDisk = round2(10.5 - log2(10.5)),
    // ChunkFormat = that + 8; the NULL-only column contributes offsets only.
    let c2_two_rows = RowSizeColumnStats {
        kind: RowSizeType::Variable,
        is_handle: false,
        total_column_size: 21,
        null_count: 0,
        total_row_count: 2.0,
    };
    let expected_variable = ((10.5_f64 - 10.5_f64.log2()) * 100.0).round() / 100.0;
    assert_eq!(expected_variable, 7.11);
    assert_eq!(
        avg_col_size_data_in_disk_by_rows(&c2_two_rows, 2),
        expected_variable
    );
    assert_eq!(
        avg_col_size_chunk_format(&c2_two_rows, 2),
        expected_variable + 8.0
    );

    // c5 varchar is entirely NULL: NullCount == rows leaves avgSize at 0, so
    // ChunkFormat reports just the 8-byte offset block and DataInDiskByRows
    // reports 0 (row_size_test.go:57-58).
    let c5 = column(RowSizeType::Variable, 0, 1);
    assert_eq!(avg_col_size_chunk_format(&c5, count), 8.0);
    assert_eq!(avg_col_size_data_in_disk_by_rows(&c5, count), 0.0);

    // After the second insert+analyze (RealtimeCount = 2) every pinned number
    // from row_size_test.go:62-75 must still hold with the accumulated sizes.
    let c1 = RowSizeColumnStats {
        kind: RowSizeType::LongLong,
        is_handle: false,
        total_column_size: 3,
        null_count: 0,
        total_row_count: 2.0,
    };
    assert_eq!(avg_col_size(&c1, 2, false), 1.5); // varint(1)+varint(132)
    assert_eq!(avg_col_size_data_in_disk_by_rows(&c1, 2), 8.0);
    assert_eq!(avg_col_size_chunk_format(&c1, 2), 8.0);

    let c2 = RowSizeColumnStats {
        kind: RowSizeType::Variable,
        is_handle: false,
        total_column_size: 21,
        null_count: 0,
        total_row_count: 2.0,
    };
    assert_eq!(avg_col_size(&c2, 2, false), 10.5); // round2(21/2)

    let c3 = RowSizeColumnStats {
        kind: RowSizeType::Float,
        is_handle: false,
        total_column_size: 8,
        null_count: 0,
        total_row_count: 2.0,
    };
    assert_eq!(avg_col_size(&c3, 2, false), 8.0);
    assert_eq!(avg_col_size_data_in_disk_by_rows(&c3, 2), 4.0);
    assert_eq!(avg_col_size_chunk_format(&c3, 2), 4.0);

    let c4 = RowSizeColumnStats {
        kind: RowSizeType::Datetime,
        is_handle: false,
        total_column_size: 16,
        null_count: 0,
        total_row_count: 2.0,
    };
    assert_eq!(avg_col_size(&c4, 2, false), 8.0);
    assert_eq!(avg_col_size_data_in_disk_by_rows(&c4, 2), 8.0);
    assert_eq!(avg_col_size_chunk_format(&c4, 2), 8.0);
}

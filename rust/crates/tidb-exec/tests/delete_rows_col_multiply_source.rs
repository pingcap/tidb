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

//! Source-backed tests for delete row/column work accounting.

use tidb_exec::delete_rows_col_multiply::add_delete_rows_col_multiply;

#[test]
fn delete_rows_col_multiply_preserves_ordinary_and_non_positive_updates() {
    // Source: pkg/executor/delete.go:58-66.
    // Direct Go coverage: pkg/executor/adapter_test.go:934-965
    // (TestDMLRowsColMultiplyRUV2SQLPath), whose single-table DELETE
    // contributes three row columns and whose multi-table DELETE contributes
    // four handle values through this accumulator.
    assert_eq!(add_delete_rows_col_multiply(0, 3), 3);
    assert_eq!(add_delete_rows_col_multiply(3, 4), 7);
    assert_eq!(add_delete_rows_col_multiply(7, 0), 7);
    assert_eq!(add_delete_rows_col_multiply(7, -2), 7);

    // A negative running total is not normalized by the source helper; only
    // the positive-delta upper bound is part of this metric leaf.
    assert_eq!(add_delete_rows_col_multiply(-4, 3), -1);
}

#[test]
fn delete_rows_col_multiply_saturates_only_positive_overflow() {
    // Source: pkg/executor/delete.go:59-65.
    let max = i64::MAX;
    assert_eq!(add_delete_rows_col_multiply(max, 1), max);
    assert_eq!(add_delete_rows_col_multiply(max - 1, 1), max);
    assert_eq!(add_delete_rows_col_multiply(max - 1, 2), max);
    assert_eq!(add_delete_rows_col_multiply(max - 1, -1), max - 1);
}

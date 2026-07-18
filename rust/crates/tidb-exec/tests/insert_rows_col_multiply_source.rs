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

//! Source-backed tests for insert row/column work accounting.

use tidb_exec::insert_rows_col_multiply::rows_col_multiply;

#[test]
fn rows_col_multiply_preserves_zero_and_saturating_boundaries() {
    // Source: pkg/executor/insert_common.go:104-115.
    // Direct Go coverage: pkg/executor/explain_unit_test.go:50-71
    // (TestInsertRowsColMultiplyRUV2Metrics).
    assert_eq!(rows_col_multiply(4, 3), 12);
    assert_eq!(rows_col_multiply(5, 3), 15);
    assert_eq!(rows_col_multiply(0, 3), 0);
    assert_eq!(rows_col_multiply(3, 0), 0);

    let max = i64::MAX as u64;
    assert_eq!(rows_col_multiply(max, 1), i64::MAX);
    assert_eq!(rows_col_multiply(max, 2), i64::MAX);
    assert_eq!(rows_col_multiply(u64::MAX, usize::MAX), i64::MAX);
}

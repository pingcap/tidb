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

//! Insert row/column work accounting from `pkg/executor/insert_common.go`.
//!
//! The source computes a saturating `rowCount * insertColumnCount` value for
//! RUV2 metrics. Metric publication and the live `InsertValues` executor remain
//! external to this dependency-closed arithmetic helper.

/// Computes `row_count * column_count`, saturating at `i64::MAX`.
#[must_use]
pub fn rows_col_multiply(row_count: u64, column_count: usize) -> i64 {
    if row_count == 0 || column_count == 0 {
        return 0;
    }

    let column_count = column_count as u64;
    let max = i64::MAX as u64;
    if row_count > max / column_count {
        return i64::MAX;
    }
    (row_count * column_count) as i64
}

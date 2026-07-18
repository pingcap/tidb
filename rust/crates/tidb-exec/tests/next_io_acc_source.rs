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

//! Source-backed tests for next-input row/cell accounting.

use tidb_exec::next_io_acc::{calc_cell_count, need_next_io_acc, NextIoAccumulator};

#[test]
fn next_io_acc_counts_rows_with_zero_columns_like_source() {
    // Source: pkg/executor/internal/exec/executor.go:42-70.
    // Direct Go coverage: pkg/executor/internal/exec/executor_test.go:35
    // (TestNextIOAccAddInputCountsRowsWithZeroCols), zero-column subtest.
    let mut accumulator = NextIoAccumulator::new();
    accumulator.add_input(3, 0);
    assert_eq!(accumulator.rows(), 3);
    assert_eq!(accumulator.cells(), 0);

    accumulator.add_input(2, -1);
    assert_eq!(accumulator.rows(), 5);
    assert_eq!(accumulator.cells(), 0);
    accumulator.add_input(0, 10);
    accumulator.add_input(-1, 10);
    assert_eq!(accumulator.rows(), 5);
    assert_eq!(accumulator.cells(), 0);
}

#[test]
fn next_io_acc_reset_reuses_state_without_carryover() {
    // Source: pkg/executor/internal/exec/executor.go:48-54, 63-70.
    // Direct Go coverage: pkg/executor/internal/exec/executor_test.go:35
    // (TestNextIOAccAddInputCountsRowsWithZeroCols), accumulator reuse subtest.
    let mut accumulator = NextIoAccumulator::new();
    accumulator.add_input(4, 2);
    assert_eq!((accumulator.rows(), accumulator.cells()), (4, 8));
    accumulator.reset();
    assert_eq!((accumulator.rows(), accumulator.cells()), (0, 0));
    accumulator.add_input(1, 1);
    assert_eq!((accumulator.rows(), accumulator.cells()), (1, 1));
}

#[test]
fn next_io_acc_admission_requires_child_and_tracking_parent() {
    // Source: pkg/executor/internal/exec/executor.go:87-89.
    // Direct Go coverage: pkg/executor/internal/exec/executor_test.go:35
    // (TestNextIOAccAddInputCountsRowsWithZeroCols), admission subtest.
    assert!(!need_next_io_acc(true, false, 0));
    assert!(need_next_io_acc(true, false, 1));
    assert!(!need_next_io_acc(false, true, 0));
    assert!(need_next_io_acc(false, true, 1));
}

#[test]
fn next_io_acc_cell_guard_matches_source() {
    // Source: pkg/executor/internal/exec/executor.go:56-61.
    // Direct Go coverage: pkg/executor/internal/exec/executor_test.go:35
    // (TestNextIOAccAddInputCountsRowsWithZeroCols), rows*cols guard.
    assert_eq!(calc_cell_count(3, 2), 6);
    assert_eq!(calc_cell_count(3, 0), 0);
    assert_eq!(calc_cell_count(0, 3), 0);
    assert_eq!(calc_cell_count(-1, 3), 0);
}

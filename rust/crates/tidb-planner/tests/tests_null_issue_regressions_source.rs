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

//! Documentary gap ports for `pkg/planner/core/tests/null`
//! (`pkg/planner.part15` items 883–885 on `origin/master`).
//!
//! `main_test.go:25 TestMain` bootstraps only (skipped-reason in the batch
//! receipt); both issue regressions execute SQL against a mock store and
//! check plans/results.
//!
//! | Go function | Rust test |
//! | --- | --- |
//! | `null/main_test.go:25 TestMain` | — skipped-reason |
//! | `null/null_test.go:23 TestIssue54803` | [`issue54803_isnull_or_in_aggregation_prunes_to_p0`] |
//! | `null/null_test.go:157 TestIssue56745` | [`issue56745_null_safe_equal_join_with_null_filter_returns_no_rows`] |

/// GO PORT of `pkg/planner/core/tests/null/null_test.go:23 TestIssue54803`.
///
/// Re-derived contract over `t1db47fc1 … PARTITION BY HASH(col_68)
/// PARTITIONS 5` (:24-35):
/// - `SELECT col_68 WHERE ISNULL(col_68) GROUP BY col_68 HAVING ISNULL(…) OR
///   col_68 IN (62,200,196,99)` explains to HashAgg → TableReader root
///   `partition:p0` → Selection `isnull(...)` → TableFullScan (:36-44): a
///   hash-partitioned table whose predicate can only hold NULL rows must
///   prune to partition p0 rather than scanning all five;
/// - adding a TRIM projection + LIMIT 106149535 keeps the same shape with
///   Projection(trim(cast(col_68,var_string(20)))) above Limit above the
///   HashAgg (:45-56);
/// - the tail #55299 piece joins it with prefix-indexed char tables and
///   requires the grouped SELECT over `ISNULL(col_1) OR col_1 IN (...)` to
///   return rows exactly ["0","1"] (comment :57, tail select through :155),
///   pinning aggregate-over-null result ordering.
#[test]
#[ignore = "go-parity-gap: hash-partition pruning inside aggregation planning needs the executor/session stack"]
fn issue54803_isnull_or_in_aggregation_prunes_to_p0() {}

/// GO PORT of `pkg/planner/core/tests/null/null_test.go:157 TestIssue56745`.
///
/// Re-derived contract: clustered-prefix-PK table `lrr(COL1,COL2)` with two
/// rows ('',a)/(test,b) (:162-165); prepared statement joining
/// `t1.col1 <=> t2.col1` filtered by `t1.col1 <=> NULL AND t2.col1 = ?`
/// executed with @a=NULL (:166-168) must COMPLETE returning zero rows — a
/// null-safe-equality join build-side probe must not panic nor wrongly match
/// when the outer filter is provably false.
#[test]
#[ignore = "go-parity-gap: prepare/execute protocol and null-safe join planning unported"]
fn issue56745_null_safe_equal_join_with_null_filter_returns_no_rows() {}

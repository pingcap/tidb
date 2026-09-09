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

//! Documentary gap ports for `pkg/planner/core/casetest/stats_test.go`
//! (`pkg/planner.part9` items 514-515 on `origin/master`, package `casetest`).
//!
//! The Go tests analyze two keyed two-column tables and then inspect the
//! stats of the OPTIMIZED logical plan tree — either directly through
//! `property.ToString(stats.GroupNDVs)` after walking to the aggregation/join
//! nodes, or indirectly through `explain format='brief'` row-count goldens.

/// GO PORT of `pkg/planner/core/casetest/stats_test.go:35 TestGroupNDVs`.
///
/// t1 (5 rows over 4 distinct pairs) and t2 (10 rows over 9) analyzed; the
/// suite loop starts at :56, and per suite SQL the Go test parses,
/// preprocesses, builds, runs `LogicalOptimizeTest` with
/// FlagCollectPredicateColumnsPoint (parse/build :57-67, optimize :68,
/// RecursiveDeriveStats4Test :71), then walks DOWN through
/// Agg/Apply/UnionAll/Join children until DataSource (:74-90) to grab the
/// aggregation's input GroupNDVs and both join sides' GroupNDVs rendered with
/// `property.ToString` and compared against the book (:105-125). Pins where
/// group-NDV information lands on the tree after optimization.
#[test]
#[ignore = "go-parity-gap: needs LogicalOptimize + recursive stat derivation over built plans"]
fn group_ndvs_after_logical_optimize_match_book() {}

/// GO PORT of `pkg/planner/core/casetest/stats_test.go:127 TestNDVGroupCols`.
///
/// Cleaner 4-row/9-row pair of tables plus chunk-RPC stability flag (:139); every
/// suite entry compares only the `explain format='brief'` output (:142-153),
/// i.e. the point is row-count ESTIMATION for aggregations and joins driven
/// by multi-column NDVs.
#[test]
#[ignore = "go-parity-gap: brief-explain estimation outputs need live optimize+cost"]
fn ndv_group_cols_row_count_estimation_golden() {}

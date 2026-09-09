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

//! Documentary gap ports for `pkg/planner/core/cbo_test.go` (item 557) and
//! `pkg/planner/core/enforce_mpp_test.go` TestRowSizeInMPP (item 559),
//! `pkg/planner.part10` on `origin/master`. Both need an analyzed,
//! session-backed store; MPP costing additionally needs virtual TiFlash
//! replica metadata.

/// GO PORT of `pkg/planner/core/cbo_test.go:41 BenchmarkOptimize`.
///
/// Contract (:41-…): seeds t(a int primary key, b int, c varchar(200), d
/// datetime default now, e int, ts timestamp) with 100 batches of 100 rows,
/// creates indexes b/d/e/b_c/ts, analyzes (:55-64), then loops b.N over the
/// optimize of each SQL in a table whose `best` strings pin the winning plan
/// rendering — count/group-by over e picks IndexReader+StreamAgg (:69-72);
/// count with range + group-by mixes HashAgg/IndexLookUp by selectivity
/// (:81-92); range-vs-threshold flips between index lookup and table scan at
/// b<=50 for both count(e) and star projections (:86-96); `1 and t.b <= 50`
/// must not panic and folds to the same table reader shape (:98-100); limit
/// ordering attaches TopN onto index or table paths depending on the select
///ivity (:104-108). The Rust crate carries the join-tier cost model but no
/// access-path optimizer over executed tables.
#[test]
#[ignore = "go-parity-gap: analyzed-table access-path optimization loop needs session/store"]
fn benchmark_optimize_best_plan_shapes_over_seeded_t() {}

/// GO PORT of `pkg/planner/core/enforce_mpp_test.go:30 TestRowSizeInMPP`.
///
/// Contract (:30-64): over analyzed t(a varchar(10), b varchar(20), c
/// varchar(256)) with a virtual TiFlash replica, `tidb_opt_tiflash_concurrency_factor=1`
/// and MPP allowed, the cost parsed from row 0 column 2 of `explain
/// format='verbose'` per single-column projection must be strictly MONOTONE in
/// the projected column width — costs[0] < costs[1] < costs[2] — pinning that
/// TiFlash row-size estimation feeds real costs.
#[test]
#[ignore = "go-parity-gap: verbose-cost rendering plus TiFlash row-size model need session/Domain"]
fn row_size_in_mpp_monotone_over_projected_column_widths() {}

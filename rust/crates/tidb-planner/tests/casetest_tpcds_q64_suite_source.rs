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

//! Documentary gap ports for `pkg/planner/core/casetest/tpcds`
//! (`pkg/planner.part9` items 518-521 on `origin/master`).
//!
//! `main_test.go:32` TestMain registers only the tpcds_suite book and zeroes
//! AsyncCommit SafeWindow/AllowedClockDrift plus enables stats-cache mem
//! quota — bootstrap only, skipped-reason. The query tests need the 13-table
//! TPC-DS schema through a live session with TiFlash replicas and MPP; a
//! prior differential study (`rust/testport/receipts/tpcds_q64.md`) showed
//! the Rust server plans Q64 structurally differently (76 vs Go's 156 MPP
//! plan rows), so no approximated golden is asserted here.

/// GO PORT of `pkg/planner/core/casetest/tpcds/tpcds_test.go:26 TestTPCDSQ64`.
///
/// Creates the 13-table TPC-DS catalog/sales/customer schema (:30-42), sets
/// `tidb_enforce_mpp=ON` with both broadcast thresholds 0 (:43-45) and checks
/// every suite input against its recorded RESULT rows (:60-62). Q64 itself is
/// the CTE-heavy cross-sales star join over catalog_sales/catalog_returns.
#[test]
#[ignore = "go-parity-gap: full MPP planning + execution of Q64 differs structurally on the Rust side (see tpcds_q64.md differential)"]
fn tpcds_q64_result_golden_under_enforced_mpp() {}

/// GO PORT of `pkg/planner/core/casetest/tpcds/tpcds_test.go:67 BenchmarkTPCDSQ64`.
///
/// Same fixture plus LoadTableStats for the thirteen tpcds50.* JSON stat files
/// and tpcds_suite_in.json (:85-98, 13 stat loads + suite input); iterates the
/// EXPLAIN-brief form of Q64.
/// Bench-shaped; also excluded from the assigned gate by `-E 'not test(/bench/)'`.
#[test]
#[ignore = "go-parity-gap: benchmark body runs live MPP explain loops over loaded histograms"]
fn benchmark_tpcds_q64_live_explain_iterations() {}

// GO PORT of `pkg/planner/core/casetest/tpcds/tpcds_test.go:225 TestBenchDaily`.
//
// Only registers `BenchmarkTPCDSQ64` with `benchdaily.Run`; without the
// `-outfile` flag that runner returns immediately (`pkg/util/benchdaily/bench_daily.go:67-69`),
// so under unit-test conditions it performs nothing observable — recorded as
// skipped-reason rather than ported as an empty shell.

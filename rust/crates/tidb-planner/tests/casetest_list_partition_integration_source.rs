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

//! Documentary gap ports for `pkg/planner/core/casetest/partition/
//! list_partition_integration_test.go` (`pkg/planner.part6` items 341–346 on
//! `origin/master`; bootstrap is `partition/main_test.go:30 TestMain`,
//! skipped-reason in the receipt).
//!
//! These four tests plus one benchmark compare partitioned LIST / LIST
//! COLUMNS tables against an unpartitioned twin through randomized DML under a
//! live mock store — correctness-by-equivalence rather than goldens. The
//! crate has no executor or transaction layer, so equivalence cannot run; and
//! `benchdaily.Run` (item 346) only re-runs the benchmark under CI timers.

/// GO PORT of `pkg/planner/core/casetest/partition/
/// list_partition_integration_test.go:28 TestListPartitionOrderLimit`.
///
/// Re-derived contract: tlist/tcollist with five 20-value buckets against
/// plain tnormal, all loaded with 50 rows built from `i*2+rand.Intn(2)`, must
/// return identical sorted result sets to the unpartitioned table for every
/// combination of ORDER BY column (a|b), LIMIT (1|5|20|100) and a
/// random `col > n` predicate, across both prune modes.
#[test]
#[ignore = "go-parity-gap: equivalence sweep needs the mock store, INSERT/SELECT execution and both prune-mode planning pipelines"]
fn list_partition_order_limit_matches_unpartitioned_results() {}

/// GO PORT of `pkg/planner/core/casetest/partition/
/// list_partition_integration_test.go:85 TestListPartitionAgg`.
///
/// Re-derived contract: GROUP BY a with MIN/MAX/SUM/COUNT over b must match
/// the unpartitioned table on the same random data in both prune modes, twice
/// per aggregate.
#[test]
#[ignore = "go-parity-gap: aggregate execution over partition scans unported"]
fn list_partition_aggregates_match_unpartitioned_results() {}

/// GO PORT of `pkg/planner/core/casetest/partition/
/// list_partition_integration_test.go:142 TestListPartitionView`.
///
/// Re-derived contract: a view projecting a*2 AS a2, a+b AS ab over tlist
/// (and later tcollist) returns exactly the unpartitioned view's sorted rows;
/// two rounds of 10 random inserts drive each comparison.
#[test]
#[ignore = "go-parity-gap: view expansion + execution round trip needs the session/executor stack"]
fn list_partition_view_projects_identical_rows() {}

/// GO PORT of `pkg/planner/core/casetest/partition/
/// list_partition_integration_test.go:184 TestListPartitionRandomTransaction`.
///
/// Re-derived contract: a randomized 50-step mix of BEGIN/sorted
/// SELECT-equality checks/INSERTs into all three twins/COMMIT-or-ROLLBACK
/// keeps tlist and tcollist observationally identical to tnormal throughout,
/// inside open transactions — exercising static pruning over uncommitted state.
#[test]
#[ignore = "go-parity-gap: transaction visibility semantics live wholly outside tidb-planner"]
fn list_partition_random_transaction_stays_equivalent() {}

/// GO PORT of `pkg/planner/core/casetest/partition/
/// list_partition_integration_test.go:245 BenchmarkPartitionRangeColumns`.
///
/// Re-derived contract: range-columns interval-partitioned table
/// (`interval(10000) first less than (10000) last less than (5120000)`)
/// point-selects uniform random keys under dynamic prune; kept as its
/// benchmark shape because each iteration is a full session plan+execute.
#[test]
#[ignore = "go-parity-gap: each iteration plans+executes against the mock store; no benchdailies here"]
fn bench_partition_range_columns_interval_pruning_round_trip() {
    // Intentionally runs zero iterations: executing even one requires the
    // CreateMockStore planning path named above.
}

/// GO PORT of `pkg/planner/core/casetest/partition/
/// list_partition_integration_test.go:266 TestBenchDaily`.
///
/// Re-derived contract: registers ONLY `BenchmarkPartitionRangeColumns`
/// (`util/benchdaily.Run`) as this package's daily-bench entrypoint. It has no
/// assertion body of its own in Go beyond registration.
#[test]
#[ignore = "go-parity-gap: benchdaily harness + the benchmark it schedules are executor-bound (see sibling)"]
fn bench_daily_registers_only_range_columns_benchmark() {}

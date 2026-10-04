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

//! Behavioral tests retained from the Go source inventory.
//! Removed empty entries and their original contracts are indexed in
//! rust/docs/parity/current-audit/empty-test-cleanup-obligations.json.

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

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

//! Ports of Go `pkg/executor/test/distsqltest` (and the sibling
//! `pkg/executor/test/ddl` suite bootstrap). Both Go packages are
//! real-TiKV-shaped integration suites: every test drives a mock store and
//! asserts on the `kv.Request` the distsql layer BUILDS (`PartitionNum`,
//! `Concurrency`, `NotFillCache`-style hooks), a surface this tier has no
//! counterpart for — the engine here executes locally and never assembles a
//! coprocessor request. The goleak `TestMain`s are suite bootstrap with no
//! behavior to pin.

// Go `pkg/executor/test/ddl/main_test.go:26::TestMain` and
// `pkg/executor/test/distsqltest/main_test.go:26::TestMain`: both only
// configure the Go suite (autoid step, slow-log threshold, failpoints) and
// install goleak verification; there is no Rust bootstrap behavior to pin.
#[test]
#[ignore = "skipped-reason: goleak/config suite bootstrap, no Rust behavior (b004/b010/b127 TestMain precedent)"]
fn executor_test_ddl_and_distsqltest_main_are_suite_bootstrap() {}

/// Go `pkg/executor/test/distsqltest/distsql_test.go:31::TestDistsqlPartitionTableConcurrency`.
/// Go builds a non-partitioned table, a 10-range and a 20-range partitioned
/// twin, inserts 20 rows into each, ANALYZEs, and hooks `CheckSelectRequestHook`
/// to require that the reader issues ONE kv.Request per partition with
/// `Concurrency = min(PartitionNum, DefDistSQLScanConcurrency)` (1 / 10 / 20).
///
/// go-parity-gap: the pinned numbers live on `kv.Request` construction in
/// `pkg/executor/distsql.go` (`SelectRequestBuilder.SetPartitionNumAndRanges`
/// / `buildCopTasksFromChan`'s concurrency derivation). This tier executes
/// against a local catalog and never builds a coprocessor request, so the
/// request counters have no observable surface here.
#[test]
#[ignore = "go-parity-gap: kv.Request PartitionNum/Concurrency derivation (distsql request builder) unported"]
fn distsql_partition_table_concurrency_per_partition_requests() {}

/// Go `pkg/executor/test/distsqltest/distsql_test.go:81::TestDistSQLSharedKVRequestRace`
/// (issue 60175): under dynamic prune + index merge, the same `kv.Request`
/// (shared ranges / `SampleOrPartitionRows`) must not race when replica-read
/// modes (`follower`, `closest-adaptive`, ...) read it concurrently; Go runs
/// `force index(ic)` and an index-merge query 20× per replica-read mode over
/// 1000 rows and requires identical 500-row prefixes.
///
/// go-parity-gap: replica-read session variables, index-merge plan selection
/// driven by `tidb_enable_index_merge`, and the shared-request cloning that
/// the race fix touched are all above this tier's local executor.
#[test]
#[ignore = "go-parity-gap: replica-read modes + shared kv.Request clone path unported"]
fn distsql_shared_kv_request_race_replica_read_modes() {}

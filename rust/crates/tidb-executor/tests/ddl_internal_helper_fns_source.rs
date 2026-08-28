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

//! Ports of the `pkg/ddl/ddl_test.go` unit-test family (part6 items 301–304
//! of the package's `func Test*`/`func Benchmark*` declarations, sorted by
//! file and line) plus `pkg/ddl/ddl_workerpool_test.go` (item 305), read from
//! `origin/master`.
//!
//! These Go tests call package-INTERNAL helpers directly
//! (`findNextNonTouchedPartitionID`, `mergeContinuousKeyRanges`,
//! `detectAndUpdateJobVersion`, `setGlobalIndexVersion`, the DDL worker
//! pool). None of those helpers is transcreated in this workspace yet, so
//! every port here is an `#[ignore]`d documentary: it carries the re-derived
//! Go contract, citing the Go symbol, and asserts nothing until the helper
//! lands. Nothing is approximated to make a test pass.

use tidb_executor::StmtContext;

/// A `StmtContext` exists so this module keeps a crate-level dependency even
/// when every test in it is an ignored documentary.
#[test]
fn module_compiles_against_the_public_api() {
    let _ctx = StmtContext::for_query();
}

// --- TestFindNextNonTouchedPartitionID (pkg/ddl/ddl_test.go:323) ---
//
// Go builds `pi` with Definitions ids 1..5 and DroppingDefinitions {2, 3}
// (p2/p3 are being reorganized away) and requires
// `findNextNonTouchedPartitionID` (pkg/ddl/index.go:3721) to walk
// Definitions skipping every dropped id: 1->4, 2->4, 3->4, 4->5, 5->0,
// 6->0 (not a partition at all), and with Definitions {1,2,3} plus
// DroppingDefinitions {2,3}, 1->0 (nothing non-touched remains).
//
// go-parity-gap: the helper and its `findNextPartitionID` walker are not
// transcreated anywhere in this workspace.
#[test]
#[ignore = "go-parity-gap: findNextNonTouchedPartitionID (pkg/ddl/index.go:3721) is not transcreated"]
fn find_next_non_touched_partition_id_skips_dropping_definitions() {
    // Contract (pkg/ddl/index.go:3721-3741): with Definitions 1..5 and
    // DroppingDefinitions {2,3}, the next non-touched partition after 1, 2
    // and 3 is 4; after 4 it is 5; after 5 it is 0; an unknown id 6 returns
    // 0; and with Definitions {1,2,3} / DroppingDefinitions {2,3}, id 1 has
    // no non-touched successor (0).
}

// --- TestMergeContinuousKeyRanges (pkg/ddl/ddl_test.go:360) ---
//
// Go builds `[]keyRangeMayExclude` over single-byte keys and requires
// `mergeContinuousKeyRanges` (pkg/ddl/cluster.go:330) to drop every
// `exclude: true` range and coalesce the surviving adjacent ones:
// one excluded range -> empty; one kept range -> itself; two non-excluded
// [1,2)+[3,4) -> [1,4); kept/excluded/kept -> the two kept ones;
// excluded/excluded/kept -> the last; kept/excluded/excluded -> the first;
// excluded/kept/excluded -> the middle.
//
// go-parity-gap: neither `keyRangeMayExclude` nor the merge is transcreated
// (the flashback-cluster key-range planner it serves is not ported).
#[test]
#[ignore = "go-parity-gap: mergeContinuousKeyRanges (pkg/ddl/cluster.go:330) is not transcreated"]
fn merge_continuous_key_ranges_drops_excluded_and_coalesces_rest() {
    // Contract (pkg/ddl/cluster.go:330-368): excluded ranges vanish, the
    // remaining ones merge when adjacent, per the seven cases of
    // pkg/ddl/ddl_test.go:360.
}

// --- TestDetectAndUpdateJobVersion (pkg/ddl/ddl_test.go:475) ---
//
// Go resets `model.JobVerInUse` to V1 and `GlobalIndexV1Supported` to false,
// then runs the cluster-version negotiation: with no peers, the job version
// follows `testargsv1.ForceV1` (V1 when forced, else V2) and the global-index
// flag turns true; with mocked `serverinfo` peers it stays V1 while any peer
// reports an unknown/invalid/pre-8.4 version, upgrades to V2 once all peers
// are >= 8.4.0, and flips `GlobalIndexV1Supported` only once all peers are
// >= 8.5.x — re-evaluated periodically until stable (7 iterations).
//
// go-parity-gap: `detectAndUpdateJobVersion` (pkg/ddl/ddl.go:975), its
// etcd-backed server-info polling and the failpoint hooks are not
// transcreated; only the `JobVersion` enum and the in-use accessor exist
// (tidb-model::job_enums, tested there).
#[test]
#[ignore = "go-parity-gap: detectAndUpdateJobVersion (pkg/ddl/ddl.go:975) and its server-info polling are not transcreated"]
fn detect_and_update_job_version_negotiates_cluster_versions() {
    // Contract (pkg/ddl/ddl.go:975-1042 + pkg/ddl/ddl_test.go:475-584):
    // V1 while any peer is unknown/old, V2 when all peers >= 8.4.0, and
    // GlobalIndexV1Supported only when all peers support global index v1.
}

// --- TestSetGlobalIndexVersionFlag (pkg/ddl/ddl_test.go:586) ---
//
// With `model.SetGlobalIndexV1Supported(false)`, Go's
// `setGlobalIndexVersion` (pkg/ddl/index.go:358) leaves a new global index's
// `GlobalIndexVersion` at 0 even for `Global: true`; with the flag on, the
// same index gets `model.GlobalIndexVersionV1`. The test's table is the zero
// value (non-clustered), the index global and non-unique.
//
// go-parity-gap: the decision function is not transcreated — index creation
// in this tier never sets `global_index_version` — so the observable flag
// change cannot be pinned. The flag accessors themselves
// (`set_global_index_v1_supported`/`get_global_index_v1_supported`) are
// transcreated in tidb-model::index and covered there.
#[test]
#[ignore = "go-parity-gap: setGlobalIndexVersion (pkg/ddl/index.go:358) is not transcreated; index creation never stamps global_index_version"]
fn set_global_index_version_flag_follows_the_supported_switch() {
    // Contract (pkg/ddl/index.go:358-382): supported=false -> version 0;
    // supported=true + global index on a non-clustered table ->
    // GlobalIndexVersionV1.
}

// --- TestDDLWorkerPool (pkg/ddl/ddl_workerpool_test.go:25) ---
//
// Go wraps a `pools.ResourcePool` (capacity 1, idle 2) in the DDL worker
// pool and requires `available()==1` fresh, `==0` after `close()`, and still
// `0` after `put(nil)` (a nil worker is not returned to a closed pool).
//
// go-parity-gap: the DDL-side worker pool over `ngaut/pools` is not
// transcreated. The workspace's `worker_pool` module is a different design —
// a process-global pool for parallel executor sub-tasks with no
// per-DDL-worker resource lifecycle — and cannot answer this contract.
#[test]
#[ignore = "go-parity-gap: the ngaut/pools-backed DDL worker pool (pkg/ddl/ddl_workerpool.go) is not transcreated"]
fn ddl_worker_pool_available_close_and_put_semantics() {
    // Contract (pkg/ddl/ddl_workerpool_test.go:25-40): fresh pool reports
    // its capacity, close drains availability, put(nil) after close is a
    // no-op.
}

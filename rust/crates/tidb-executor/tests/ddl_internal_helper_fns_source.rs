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
//! pool). Helpers that have a direct metadata owner are asserted live below;
//! queue- and cluster-lifecycle helpers remain `#[ignore]`d documentaries.

use tidb_executor::ddl::{merge_continuous_key_ranges, KeyRangeMayExclude};
use tidb_executor::StmtContext;
use tidb_model::{GoSharedSlice, PartitionDefinition, PartitionInfo};
use tidb_txnkv::{Key, KeyRange};

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
#[test]
fn find_next_non_touched_partition_id_skips_dropping_definitions() {
    // Contract (pkg/ddl/index.go:3744-3765): with Definitions 1..5 and
    // DroppingDefinitions {2,3}, the next non-touched partition after 1, 2
    // and 3 is 4; after 4 it is 5; after 5 it is 0; an unknown id 6 returns
    // 0; and with Definitions {1,2,3} / DroppingDefinitions {2,3}, id 1 has
    // no non-touched successor (0).
    let defs = |ids: &[i64]| {
        GoSharedSlice::from_vec(
            ids.iter()
                .map(|id| PartitionDefinition {
                    id: *id,
                    ..Default::default()
                })
                .collect(),
        )
    };
    let partition_info = PartitionInfo {
        definitions: defs(&[1, 2, 3, 4, 5]),
        dropping_definitions: defs(&[2, 3]),
        ..Default::default()
    };
    for (current, expected) in [(1, 4), (2, 4), (3, 4), (4, 5), (5, 0), (6, 0)] {
        assert_eq!(
            partition_info.find_next_non_touched_partition_id(current),
            expected,
            "current partition {current}"
        );
    }
    let no_successor = PartitionInfo {
        definitions: defs(&[1, 2, 3]),
        dropping_definitions: defs(&[2, 3]),
        ..Default::default()
    };
    assert_eq!(no_successor.find_next_non_touched_partition_id(1), 0);
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
#[test]
fn merge_continuous_key_ranges_drops_excluded_and_coalesces_rest() {
    // Contract (pkg/ddl/cluster.go:330-368): excluded ranges vanish, the
    // remaining ones merge when adjacent, per the seven cases of
    // pkg/ddl/ddl_test.go:360.
    let range = |start: u8, end: u8, exclude: bool| KeyRangeMayExclude {
        range: KeyRange::new(Key::from_bytes(vec![start]), Key::from_bytes(vec![end])),
        exclude,
    };
    let output = |ranges: &[KeyRangeMayExclude]| {
        merge_continuous_key_ranges(ranges)
            .into_iter()
            .map(|range| {
                (
                    range.start_key.as_bytes().to_vec(),
                    range.end_key.as_bytes().to_vec(),
                )
            })
            .collect::<Vec<_>>()
    };

    assert_eq!(output(&[range(1, 2, true)]), Vec::<(Vec<u8>, Vec<u8>)>::new());
    assert_eq!(output(&[range(1, 2, false)]), vec![(vec![1], vec![2])]);
    assert_eq!(
        output(&[range(1, 2, false), range(3, 4, false)]),
        vec![(vec![1], vec![4])]
    );
    assert_eq!(
        output(&[range(1, 2, false), range(2, 3, true), range(3, 4, false)]),
        vec![(vec![1], vec![2]), (vec![3], vec![4])]
    );
    assert_eq!(
        output(&[range(1, 2, true), range(2, 3, true), range(3, 4, false)]),
        vec![(vec![3], vec![4])]
    );
    assert_eq!(
        output(&[range(1, 2, false), range(2, 3, true), range(3, 4, true)]),
        vec![(vec![1], vec![2])]
    );
    assert_eq!(
        output(&[range(1, 2, true), range(2, 3, false), range(3, 4, true)]),
        vec![(vec![2], vec![3])]
    );
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

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

//! Behavioral tests retained from Go. Removed documentary entries are
//! indexed in rust/docs/parity/current-audit/comment-test-cleanup-validation.json.

use tidb_executor::ddl::{merge_continuous_key_ranges, KeyRangeMayExclude};
use tidb_model::{GoSharedSlice, PartitionDefinition, PartitionInfo};
use tidb_txnkv::{Key, KeyRange};

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

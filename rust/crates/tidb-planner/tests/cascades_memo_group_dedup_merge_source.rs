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

use tidb_planner::memo_group_id::{GroupId, GroupIdGenerator};

/// GO PORT of
/// `pkg/planner/cascades/memo/group_id_generator_test.go:24
/// TestGroupIDGenerator_NextGroupID`.
///
/// Re-derived contract: `NextGroupID` pre-increments a single-threaded counter
/// so fresh generators yield 1, 2, 3 … (group_id_generator.go:27-30); the Go
/// test pokes the private `id` field to 100 and continues 101, 102, 103
/// (test :35-:41) — mirrored by the crate's explicit-counter constructor,
/// which exists precisely for that observable — and rewrites it to
/// `math.MaxUint64` so the next call wraps to 0 and then 1 (test :42-:47;
/// production uses Go's natural uint64 overflow at
/// group_id_generator.go:27).
#[test]
fn group_id_generator_next_ids_are_one_based_and_wrap_at_uint64_max() {
    let mut g = GroupIdGenerator::new();
    assert_eq!(g.next_group_id(), GroupId::new(1));
    assert_eq!(g.next_group_id(), GroupId::new(2));
    assert_eq!(g.next_group_id(), GroupId::new(3));

    // Adjust the id (Go pokes the private field; the crate exposes an
    // explicit-counter constructor with the same effect).
    let mut g = GroupIdGenerator::from_raw(100);
    assert_eq!(g.next_group_id(), GroupId::new(101));
    assert_eq!(g.next_group_id(), GroupId::new(102));
    assert_eq!(g.next_group_id(), GroupId::new(103));

    let mut g = GroupIdGenerator::from_raw(u64::MAX);
    assert_eq!(g.next_group_id(), GroupId::new(0));
    assert_eq!(g.next_group_id(), GroupId::new(1));
}

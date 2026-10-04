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

use tidb_planner::explore_mark::ExploreMark;

/// GO PORT of `pkg/planner/memo/group_test.go:287 TestExploreMark`.
///
/// Re-derived contract (group.go:30-49): rounds are independent bits; a fresh
/// mark reports nothing explored for any round (:290-292); SetExplored marks
/// only its round (:294-297); SetUnexplored clears only its target while the
/// other round stays set (:299-303). The crate widens no state beyond Go's
/// single-word bitset; out-of-range shifts are checked no-ops on the Rust
/// carrier, which these in-range rounds never touch.
#[test]
fn explore_mark_tracks_two_independent_rounds() {
    let mut mark = ExploreMark::new();
    assert!(!mark.explored(0));
    assert!(!mark.explored(1));

    mark.set_explored(0);
    mark.set_explored(1);
    assert!(mark.explored(0));
    assert!(mark.explored(1));

    mark.set_unexplored(1);
    assert!(mark.explored(0));
    assert!(!mark.explored(1));
}

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

//! Dependency-closed vectors for
//! `pkg/planner/cascades/memo/group_id_generator.go`.
//!
//! The direct Go anchor is `TestGroupIDGenerator_NextGroupID` at
//! `pkg/planner/cascades/memo/group_id_generator_test.go:24`.

use tidb_planner::memo_group_id::{GroupId, GroupIdGenerator};

#[test]
fn group_id_generator_starts_at_one_and_increments() {
    let mut generator = GroupIdGenerator::new();
    assert_eq!(generator.next_group_id(), GroupId::new(1));
    assert_eq!(generator.next_group_id(), GroupId::new(2));
    assert_eq!(generator.next_group_id(), GroupId::new(3));
}

#[test]
fn group_id_generator_wraps_at_u64_max() {
    let mut generator = GroupIdGenerator::from_raw(u64::MAX);
    assert_eq!(generator.next_group_id(), GroupId::new(0));
    assert_eq!(generator.next_group_id().raw(), 1);
}

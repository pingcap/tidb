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

//! Dependency-closed vectors for `pkg/planner/memo/group.go`.
//!
//! The direct Go anchor is `TestExploreMark` at
//! `pkg/planner/memo/group_test.go:287`.

use tidb_planner::explore_mark::ExploreMark;

#[test]
fn source_round_bits_set_clear_and_query() {
    let mut mark = ExploreMark::new();
    assert!(!mark.explored(0));
    assert!(!mark.explored(1));

    mark.set_explored(0);
    mark.set_explored(1);
    assert!(mark.explored(0));
    assert!(mark.explored(1));
    assert_eq!(mark.bits(), 0b11);

    mark.set_unexplored(1);
    assert!(mark.explored(0));
    assert!(!mark.explored(1));
    assert_eq!(mark.bits(), 1);
}

#[test]
fn source_out_of_width_rounds_are_inert() {
    let mut mark = ExploreMark::default();
    mark.set_explored(64);
    assert!(!mark.explored(64));
    mark.set_unexplored(64);
    assert_eq!(mark.bits(), 0);
}

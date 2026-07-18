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

//! Dependency-closed vectors for `pkg/planner/memo/group_expr.go`.
//!
//! The direct Go anchors are `TestNewGroupExpr` at line 27 and
//! `TestGroupExprFingerprint` at line 35 in
//! `pkg/planner/memo/group_expr_test.go`.

use tidb_planner::group_expr::GroupExpr;

#[test]
fn source_new_group_expr_starts_unexplored_and_without_children() {
    let expr = GroupExpr::new([0x01, 0x02, 0x03]);
    assert_eq!(expr.plan_hash(), &[0x01, 0x02, 0x03]);
    assert!(expr.children().is_empty());
    assert!(!expr.explored(0));
}

#[test]
fn source_fingerprint_frames_count_children_and_plan_hash() {
    let mut expr = GroupExpr::new([0xaa, 0xbb]);
    expr.set_children([0x0102_0304_0506_0708]);
    let expected = [0_u8, 1, 1, 2, 3, 4, 5, 6, 7, 8, 0xaa, 0xbb];
    assert_eq!(expr.fingerprint(), expected.as_slice());
    assert_eq!(expr.fingerprint(), expected.as_slice());

    expr.set_explored(2);
    assert!(expr.explored(2));
    expr.add_applied_rule(42);
    assert!(expr.has_applied_rule(42));
    assert!(!expr.has_applied_rule(43));
}

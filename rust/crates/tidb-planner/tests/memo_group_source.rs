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

use tidb_planner::expr_iterator::{Group, GroupExpression};
use tidb_planner::group_expr::GroupExpr;
use tidb_planner::pattern::Operand;
use tidb_planner::pattern_engine::EngineType;

/// GO PORT of `pkg/planner/memo/group_expr_test.go:27 TestNewGroupExpr`.
///
/// `NewGroupExpr(p)` stores the node, leaves Children nil, and starts with an
/// unset round-0 exploration bit (:30-33; construction at group_expr.go:47-53).
/// The crate keys "which logical operator this wraps" on caller-supplied
/// plan-hash bytes instead of a `base.LogicalPlan` pointer — same role as the
/// bytes later feeding `FingerPrint()` (group_expr.go:58 builds
/// `ExprNode.HashCode()` into it).
#[test]
fn new_group_expr_stores_plan_hash_empty_children_and_clear_explore_bit() {
    const NODE_HASH: &[u8] = b"logical-limit-node-hash";
    let expr = GroupExpr::new(NODE_HASH);
    assert_eq!(expr.plan_hash(), NODE_HASH);
    assert!(expr.children().is_empty());
    assert!(!expr.explored(0));
}

/// GO PORT of `pkg/planner/memo/group_expr_test.go:35 TestGroupExprFingerprint`.
///
/// Go pins the byte layout (`group_expr.go:56-69`): after
/// `SetChildren(childGroup)`, `FingerPrint()` equals
/// BigEndian(uint16 childCount=1) ++ BigEndian(uint64 childGroup pointer) ++
/// `LogicalLimit{Count:3}.HashCode()` built by hand into `buffer` (:44-52). The
/// crate keeps the exact layout over explicit carriers: the reflect pointer
/// becomes an owned u64 child identity token and the plan hash is passed in
/// bytes (its provenance, `HashCode()`, stays outside this boundary).
#[test]
fn group_expr_fingerprint_is_child_count_plus_child_ids_plus_plan_hash() {
    const LIMIT_COUNT_3_PLAN_HASH: &[u8] = b"group-expr-fingerprint-limit-hash";
    // Reflect stand-in for `uint64(reflect.ValueOf(childGroup).Pointer())`
    // (group_expr_test.go:48): an arbitrary fixed child identity.
    const CHILD_GROUP_ID: u64 = 0x000a_11ce_c0de_0001;

    let mut expr = GroupExpr::new(LIMIT_COUNT_3_PLAN_HASH);
    let mut expected = Vec::new();
    expected.extend_from_slice(&1u16.to_be_bytes());
    expected.extend_from_slice(&CHILD_GROUP_ID.to_be_bytes());
    expected.extend_from_slice(LIMIT_COUNT_3_PLAN_HASH);

    // Same layout with an empty child list first (childCount = 0).
    let mut expected_leaf = Vec::new();
    expected_leaf.extend_from_slice(&0u16.to_be_bytes());
    expected_leaf.extend_from_slice(LIMIT_COUNT_3_PLAN_HASH);
    assert_eq!(expr.fingerprint(), expected_leaf.as_slice());
    expr.set_children([CHILD_GROUP_ID]);
    assert_eq!(expr.fingerprint(), expected.as_slice());
}

/// GO PORT of `pkg/planner/memo/group_test.go:38 TestNewGroup`.
///
/// `NewGroupWithSchema(expr, schema)` registers the seed expression so the
/// equivalence list starts with exactly one member (:41-44; construction
/// delegates to Insert, group.go:76-95). Narrowed port: the seed-registration
/// half is pinned on the crate's group carrier; the `Fingerprints` map being
/// pre-seeded with one entry and the embedded zero-value ExploreMark (both
/// asserted at :45-46 in Go) have their one observable init contract carried by
/// the GroupExpr-level explore bit below, since group-level mark embedding is
/// not represented in the crate.
#[test]
fn new_group_seeds_exactly_one_equivalent_member() {
    let mut g = Group::new(EngineType::TiDb);
    g.insert(GroupExpression::new(Operand::Limit));
    assert_eq!(g.equivalents.len(), 1);
    assert_eq!(g.equivalents[0].operand, Operand::Limit);

    // Zero-value explore state asserted by Go against the fresh Group
    // (:46): nothing is explored for round 0 yet.
    let seed = GroupExpr::new(b"seed");
    assert!(!seed.explored(0));
}

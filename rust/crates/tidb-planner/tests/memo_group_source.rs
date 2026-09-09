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

//! Port ledger for `pkg/planner/memo/group_expr_test.go` (items 1253-1254)
//! and `pkg/planner/memo/group_test.go` items 1255-1260
//! (`pkg/planner.part21` on `origin/master`).
//!
//! THREE tests are functional ports over [`tidb_planner::group_expr`] and the
//! group scaffolding of [`tidb_planner::expr_iterator`] (Go's
//! `NewGroupExpr`/`GroupExpr.FingerPrint` at pkg/planner/memo/group_expr.go:47,56
//! and zero-value `ExploreMark`, pkg/planner/memo/group.go:32). FIVE are
//! documented gap ports: Go's `Group` (group.go:51) maintains fingerprint-keyed
//! membership (`Fingerprints map[string]*list.Element`, group.go:55) that drives
//! `Insert`/`Delete`/`DeleteAll`/`Exists` dedup semantics (group.go:105-165);
//! the crate's carrier holds a plain `Vec<GroupExpression>` with no membership
//! bookkeeping and no per-operand first-element index (`FirstExpr`,
//! group.go:54), so those contracts have no honest Rust surface yet. The
//! plan-tree-dependent `TestGroupFingerPrint` additionally needs
//! `BuildLogicalPlanForTest` + `memo.Convert2Group`.

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

/// GO PORT of `pkg/planner/memo/group_test.go:49 TestGroupInsert`.
///
/// Re-derived contract: inserting an expression already registered under its
/// own fingerprint returns false — membership lookup runs through `Exists`
/// (group.go:105-123 and :160-165). After externally overriding
/// `selfFingerprint = "1"` (group_test.go:54) the same pointer no longer
/// resolves to a stored key, so Insert returns true and appends a second
/// element. Membership is keyed on GroupExpr.FingerPrint(), never on pointer
/// identity.
#[test]
#[ignore = "go-parity-gap: crate Group has plain Vec membership without fingerprint-keyed dedup or self-fingerprint override hooks"]
fn group_insert_dedups_by_fingerprint_and_accepts_rekeyed_duplicates() {}

/// GO PORT of `pkg/planner/memo/group_test.go:58 TestGroupDelete`.
///
/// Re-derived contract: `Delete` resolves the element through the stored
/// fingerprint (group.go:125-150) shrinking Equivalents 1 -> 0, and a repeated
/// delete of the missing key is a tolerated no-op staying at 0 (:64-68).
#[test]
#[ignore = "go-parity-gap: crate Group has no delete path or fingerprint registry"]
fn group_delete_removes_once_then_tolerates_missing_target() {}

/// GO PORT of `pkg/planner/memo/group_test.go:71 TestGroupDeleteAll`.
///
/// Re-derived contract: a seeded Selection plus inserted Limit/Projection gives
/// three distinct-operand equivalents (:79-81); before the wipe,
/// `GetFirstElem(OperandProjection)` resolves (:83) and `Exists(expr)` holds
/// (:84); `DeleteAll` resets Equivalents/FirstExpr/Fingerprints/SelfFingerprint
/// (group.go:152-158) so the first-element probe turns nil (:86) and membership
/// reports false (:87).
#[test]
#[ignore = "go-parity-gap: crate Group lacks DeleteAll, operand first-element index, and Exists"]
fn group_delete_all_clears_equivalents_first_elem_index_and_membership() {}

/// GO PORT of `pkg/planner/memo/group_test.go:91 TestGroupExists`.
///
/// Re-derived contract: membership follows the fingerprint registry — true for
/// the constructor-seeded expression (:94-96), false again once `Delete` has
/// evicted it (:98-100).
#[test]
#[ignore = "go-parity-gap: crate Group has no Exists probe or delete path"]
fn group_exists_tracks_registered_then_deleted_expression() {}

/// GO PORT of `pkg/planner/memo/group_test.go:101 TestGroupFingerPrint`.
///
/// Re-derived contract over a real built plan (`BuildLogicalPlanForTest` on
/// `select * from t where a > 1 and a < 100`, tree Projection -> Selection ->
/// DataSource, wrapped by `Convert2Group`; :101-133): re-inserting a GroupExpr
/// wrapping the SAME projection with the SAME child group is rejected by
/// fingerprint dedup (count stays 1, :135-139); different children make a new
/// fingerprint (count 2, :141-147); a different operator node makes a new
/// fingerprint (count 3, :149-154); two Selections holding identical conditions
/// in swapped order hash identically because LogicalSelection.HashCode sorts
/// per-condition hashes (pkg/planner/core/operator/logicalop/
/// logical_selection.go, "Sort the conditions..." step), so the reordered copy
/// inserts while a duplicate of either is rejected (count ends at 4,
/// :156-164).
#[test]
#[ignore = "go-parity-gap: needs BuildLogicalPlanForTest+Convert2Group pipeline and fingerprint-keyed Group dedup"]
fn group_finger_print_gates_duplicate_inserts_by_node_children_and_condition_order() {}

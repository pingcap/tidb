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

//! Port ledger for `pkg/planner/cascades/memo` (`pkg/planner.part2` items
//! 65-77 on `origin/master`; note this is the LEGACY cascades memo, not the
//! live `pkg/planner/memo` whose leaves already run in this crate).
//!
//! Exactly one test is a real functional port:
//! `group_id_generator_test.go::TestGroupIDGenerator_NextGroupID`, driven over
//! [`tidb_planner::memo_group_id`], the transcreation of
//! `pkg/planner/cascades/memo/group_id_generator.go`.
//!
//! Every other Go test manipulates the legacy memo's intrusive containers —
//! `container/list` logical-expression runs (group.go:22-26),
//! fingerprint-keyed `hashmap.Map[*GroupExpression, *list.Element]`
//! dedup (group.go:33), pointer-keyed parent maps (group.go:44-53), and the
//! failpoint-guarded `Memo.Init` bottom-up build over real
//! `logicalop.DataSource/LogicalJoin/LogicalLimit/LogicalSort` values — none
//! of which exist on the Rust side (this crate's `expr_iterator`/`group_expr`
//! carry the NEW `pkg/planner/memo` shape, keyed by plan-hash bytes rather
//! than legacy group ids and list elements). They stay documentary gaps; no
//! behavior is approximated. The package bootstrap
//! (`main_test.go:24 TestMain`) is recorded as skipped-reason in the receipt.

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

/// GO PORT of
/// `pkg/planner/cascades/memo/group_and_expr_test.go:30 TestRawHashMap`.
///
/// Re-derived contract: the third-party `zyedidia/generic/hashmap` container
/// that legacy `Group` dedup is built on hashes keys only through their `a
/// uint64` field while equality compares BOTH fields (:18-23 vs :25-28), so
/// `A{1,"1"}` and `A{1,"2"}` coexist as two entries despite colliding hash
/// buckets (Size()==2 after both Puts, test :48), and Get returns the entry
/// matching under full-field equality (:32-36, :49-55). This pins the
/// collision-resolution primitive the whole legacy memo relies on.
#[test]
#[ignore = "go-parity-gap: pins zyedidia/generic/hashmap key-hash/equality split as used by legacy Group.hash2GroupExpr; the crate has no transcreation of that container"]
fn raw_hash_map_hash_key_equality_fields_distinguish_colliding_entries() {
    // Restore: New[A](4, eq=(a.a==b.a && a.s==b.s), hash=t.a); Put {1,"1"},
    // Put {1,"2"}; require Size==2 and per-key Gets return each full value.
}

/// GO PORT of
/// `pkg/planner/cascades/memo/group_and_expr_test.go:60
/// TestGroupExpressionHashCollision`.
///
/// Re-derived contract: two group expressions differing only in child order
/// (`Inputs [child1,child2]` vs `[child2,child1]`) get their `hash64` fields
/// forced equal (:73-74) to simulate a collision; `Group.Insert` still keeps
/// both because dedup keys off hash+equals and `GroupExpression.Equals`
/// compares inputs pairwise in order (group_expr.go:99-129); insertion into
/// root group 5 stamps each expression's back-pointer `.group` (:84-89) while
/// preserving input group ids order-sensitively (:90-100).
#[test]
#[ignore = "go-parity-gap: needs legacy Group.Insert + list-backed equivalence runs + pointer-stamped .group on GroupExpression; unported"]
fn group_expression_hash_collision_keeps_order_distinguished_pair() {
    // Restore: force equal hash64, Insert(a) then Insert(b) both true,
    // root.logicalExpressions.Len()==2, hash2GroupExpr.Get resolves each side.
}

/// GO PORT of
/// `pkg/planner/cascades/memo/group_and_expr_test.go:96
/// TestGroupExpressionDelete`.
///
/// Re-derived contract: after real `Hash64` initialization of two
/// order-differing expressions (:109-116, hashing Inputs groups plus plan via
/// group_expr.go:89-97), inserting both fills the list to 2; deleting an
/// equal-hash but distinct `mock` GE removes nothing (:117-127, dedup by
/// equals mismatch); deleting `a` then `b` drains the list front-first,
/// ending empty via both `Len()` and the exported getter (:128-141;
/// Group.Delete unlinks the map entry and list element at group.go:134-160).
#[test]
#[ignore = "go-parity-gap: needs legacy Group.Delete list/map unlink semantics over real HashEqualer-hashed expressions; unported"]
fn group_expression_delete_removes_only_equals_matches_front_first() {
    // Restore: insert a,b; Delete(mock)->2; Delete(a)->front==b; Delete(b)->0.
}

/// GO PORT of
/// `pkg/planner/cascades/memo/group_and_expr_test.go:140 TestGroupHashEquals`.
///
/// Re-derived contract: `Group.Hash64` feeds only `uint64(groupID)` into the
/// hasher (group.go:84-87), so same-id groups share digests and `Equals`
/// succeeds for any pointer/non-pointer pair with equal ids (:153-156);
/// changing b's id to 2 changes the digest after Reset and breaks every
/// Equals pairing (:157-165). The pointer-vs-value asymmetry —
/// `a.Equals(&b)` true but `a.Equals(b)` false for non-pointer args — comes
/// from Go's `other.(*Group)` assertion failing on value types (group.go:89-
/// 104) and has no carrier here.
#[test]
#[ignore = "go-parity-gap: legacy Group HashEquals dispatch (typed interface assertion incl. nil handling, group.go:89-104) has no Rust carrier"]
fn group_hash_equals_tracks_group_id_only() {
    // Restore: hash pair @id1 -> equal digests + all Equals(true,false) rows;
    // bump id -> digest differs + Equals(false,...).
}

/// GO PORT of
/// `pkg/planner/cascades/memo/group_and_expr_test.go:161
/// TestGroupExpressionHashEquals`.
///
/// Re-derived contract: expressions owned by different root groups (3 vs 4)
/// but identical children+plan hash EQUAL (:178-184: `Hash64` hashes only
/// Inputs' groups and the wrapped plan, group_expr.go:89-97; the owner group
/// is excluded) while direct `Equals` distinguishes owners (:185; group_expr.go:
/// 99-129 compares `.group` pointers); reordering children like join
/// commutativity flips both digest and equality (:186-197).
#[test]
#[ignore = "go-parity-gap: needs legacy GroupExpression.Hash64/Equals over ordered Inputs plus owner-group pointer identity; unported"]
fn group_expression_hash_ignores_owner_group_but_equals_rejects_it() {
    // Restore: same children order -> equal digests, Equals false (owner),
    // Equals(&b) true; swap children -> digests differ, all Equals false.
}

/// GO PORT of
/// `pkg/planner/cascades/memo/group_and_expr_test.go:192
/// TestGroupParentGERefs`.
///
/// Re-derived contract: building join(t1,schema{col1}) ⋈ t2(schema{col2})
/// through `Memo.Init` under the MockPlanSkipMemoDeriveStats failpoint yields
/// 3 groups / 3 id-mapped entries (:216-220), a root group with exactly one
/// root group-expression whose `LogicalPlan.Equals(join)` holds and whose
/// list front matches the hash-map element (:227-235); each child group then
/// carries exactly one parent reference keyed by the parent expression's
/// address (:238-260, `hash2ParentGroupExpr` is keyed by
/// `unsafe.Pointer(j.addr())`, group.go:44-53) and one child expression whose
/// plan equals t1/t2 respectively; globally `hash2GlobalGroupExpr` indexes all
/// three expressions found by value-equality (:262-289).
#[test]
#[ignore = "go-parity-gap: Memo.Init bottom-up build over real logical plans + failpoint MockPlanSkipMemoDeriveStats + pointer-keyed parent maps are unported"]
fn group_parent_ge_refs_bind_child_groups_to_their_owner_expression() {
    // Restore: Init(join); sizes/ids asserts; walk hash2ParentGroupExpr per
    // child; match global hash2GlobalGroupExpr entries via Equals.
}

/// GO PORT of `pkg/planner/cascades/memo/memo_test.go:32 TestMemo`.
///
/// Re-derived contract: `NewMemo().Init(join(t1,t2))` under the skip-stats
/// failpoint produces exactly 3 groups (:46-50), enumeration order assigns
/// post-order group ids 1,2,3 from the shared generator (:51-58; leaf groups
/// first, join's root last; group_id_generator.go:27 plus memo.go:214-232).
#[test]
#[ignore = "go-parity-gap: needs Memo.Init conversion of real logicalop trees under failpoint; Rust carries only the id generator and marks"]
fn memo_init_assigns_post_order_group_ids_to_three_groups() {
    // Restore: Init(join); groups.Len()==3; iterate GetGroups asserting
    // incremental ids starting at 1.
}

/// GO PORT of `pkg/planner/cascades/memo/memo_test.go:58 TestInsertGE`.
///
/// Re-derived contract: inserting a NEW LogicalLimit expression whose input is
/// the current root group appends a fresh group to the memo (:72-79):
/// groups/id-map grow 3→4, ids enumerate 1..4, and the inserted expression's
/// own new group sits at the list back (:80-88;
/// memo.InsertGroupExpression targets nil => creates a group at memo.go:186-
/// 212).
#[test]
#[ignore = "go-parity-gap: needs mm.NewGroupExpression over a real LogicalLimit plus InsertGroupExpression group creation; unported"]
fn insert_ge_appends_new_group_with_sequential_id() {
    // Restore: Init(join); NewGroupExpression(limit,[root]); insert; count 4;
    // Back().GetGroupID()==4.
}

/// GO PORT of `pkg/planner/cascades/memo/memo_test.go:118 TestMergeGroup`.
///
/// Re-derived contract: inserting sort3==sort1-shape into dstG (which held
/// sort2, a superset ordering) finds the existing equivalent instead of
/// inserting (:196-203) and triggers mergeGroup(srcG,dstG): src becomes
/// empty (no parents, no expressions, no operand index) while dst absorbs
/// both parents (hash2ParentGroupExpr size 2 covering srcParentGroup+dstPar
/// ent ids) keeping two logical expressions and one operand-cluster (:205-
/// 245); memo-wide groups stay 5 with global dedup at 4 expressions (:246-
/// 249). childG1's parent references now point only at dstG-side owners
/// (:250-263) per the map rebuild in Group.mergeTo (group.go:241-273) and
/// Memo.mergeGroup (memo.go:270-335).
#[test]
#[ignore = "go-parity-gap: group-merge machinery (mergeGroup/replaceGEChild/Check-invariants over intrusive lists) is entirely outside the crate boundary"]
fn merge_group_unifies_equivalent_sort_children_under_single_group() {
    // Restore: assemble the 6-group memo; InsertGroupExpression(sort3GE,dstG);
    // mirror every size/mask assertion from memo_test.go:196-263.
}

/// GO PORT of `pkg/planner/cascades/memo/memo_test.go:241
/// TestRecursiveMergeGroup`.
///
/// Re-derived contract: when sort2's insertion collapses srcG into dstG, the
/// rewrite recurses upward through limit1's owning group because limit1 and
/// limit2 share offset 1 (:330-338 comment): after both merges dstParentGroup
/// alone remains above (hash2GroupExpr size 1 holding limit2 :374-382),
/// projG2 keeps its single dstG parent ref (:385-393), and childG1's parent
/// set contains only dstG-owned expressions, each still locatable in dstG or
/// projG2 dedup maps (:394-410); memo holds 5 groups / 4 global expressions
/// (:367-370) exactly like Memo.mergeGroup's recursive loop requires.
#[test]
#[ignore = "go-parity-gap: recursive upward merge cascade (limit-offset equality triggering second merge) lives in unported Memo.mergeGroup internals"]
fn recursive_merge_group_cascades_through_limit_parents() {
    // Restore: build src/dst branches sharing childG1 with a projection-only
    // middle group; insert collapsing sort2; assert every mask/size row from
    // memo_test.go:340-410.
}

/// GO PORT of `pkg/planner/cascades/memo/memo_test.go:356
/// TestIteratorLogicalPlan`.
///
/// Re-derived contract: after CopyIn adds t3/t4 to the join children's groups
/// (equivalence classes G2{t1,t3}/G3{t2,t4} :384-390), a hand-built
/// `IteratorLP` walks depth-first yielding join1 first and data sources
/// beneath (:405-431); `GetPlanIDsHash()` values must reproduce hand-computed
/// FNV digests — HashInt(plan-id) per leaf, HashUint64(lhs)+HashUint64(rhs)+
/// HashInt(join-id) at the root (:394-417, hashed via base.HashEqualer
/// primitives at pkg/planner/cascades/base/hash_equaler.go:133/:145) — while
/// TableAsName resolves to "t1"/"t2" on the first-bound alternatives.
#[test]
#[ignore = "go-parity-gap: IteratorLP DFS over legacy memo groups with CopyIn-grown equivalence classes and GetPlanIDsHash composites are unported"]
fn iterator_logical_plan_walks_first_equivalents_with_pinned_plan_hashes() {
    // Restore: Init(join1); CopyIn t3/t4; compute lhs/rhs/jhs digests through
    // new_hash_equaler(); drive iter.Next() and compare.
}

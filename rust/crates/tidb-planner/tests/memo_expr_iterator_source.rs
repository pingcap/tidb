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

//! Port ledger for `pkg/planner/memo/expr_iterator_test.go`
//! (`pkg/planner.part21` items 1249-1252 on `origin/master`).
//!
//! All four Go tests are REAL functional ports over this crate's
//! [`tidb_planner::expr_iterator`] boundary (source-shaped carrier of
//! `pkg/planner/memo/expr_iterator.go`). Go walks intrusive list elements and
//! re-binds child `ExprIter`s lazily (`Next`/`Reset`, expr_iterator.go:40-142);
//! the crate materializes the same pattern-matched combination set eagerly in
//! [`tidb_planner::expr_iterator::ExprIter`] over owned `Group.equivalents`
//! vectors. Because Go's `Group.Insert` anchors same-operand runs around the
//! first-inserted element of each operand (group.go:105-123), operand kinds
//! are contiguous per group, so an eager filter-by-(operand, engine) visit set
//! is identical to Go's lazy run walk; only the enumeration visits it batches
//! differ, never the membership. The Go tests assert exactly operand tags,
//! group identity per visited binding, structural arity, and total match
//! counts — all of which have carriers here. Session/domain/stats-handle
//! teardown (coretestsdk.MockContext + StatsHandle().Close) has no carrier and
//! no observable effect on these assertions; not ported.

use tidb_planner::expr_iterator::{new_expr_iter_from_group_elem, Group, GroupExpression};
use tidb_planner::pattern::Operand;
use tidb_planner::pattern::{build_pattern, Pattern};
use tidb_planner::pattern_engine::{EngineType, EngineTypeSet};

/// Go helper `countMatchedIter` (expr_iterator_test.go:300-313): iterate every
/// equivalent as an iterator root and count all matched bindings.
fn count_matched_iter(group: &Group, pattern: &Pattern) -> usize {
    (0..group.equivalents.len())
        .filter_map(|index| new_expr_iter_from_group_elem(group, index, pattern))
        .map(|iter| iter.len())
        .sum()
}

/// GO PORT of `pkg/planner/memo/expr_iterator_test.go:29
/// TestNewExprIterFromGroupElem`.
///
/// Go builds g0 = [Selection(seed), Limit, Projection, Limit] and g1 =
/// [Selection(seed), Limit, Projection, Limit] (list layout after
/// first-element anchoring, group.go:105-123), joins them under one Join
/// group-expression in g2, then creates `NewExprIterFromGroupElem(g2 front,
/// Join(Projection, Selection))`. Assertions (:56-74): iterator exists, root
/// binding matches OperandJoin with 2 children; child[0] sits on g0's FIRST
/// Projection element with zero children; child[1] sits on g1's FIRST
/// Selection element with zero children. Both groups hold exactly one member
/// of the demanded operand kind, so "anchored at GetFirstElem(...)" pins as
/// "the unique matching equivalent", keyed here by operand tag.
#[test]
fn new_expr_iter_from_group_elem_binds_join_children_at_operand_anchors() {
    let mut g0 = Group::new(EngineType::TiDb);
    g0.insert(GroupExpression::new(Operand::Selection));
    g0.insert(GroupExpression::new(Operand::Limit));
    g0.insert(GroupExpression::new(Operand::Projection));
    g0.insert(GroupExpression::new(Operand::Limit));

    let mut g1 = Group::new(EngineType::TiDb);
    g1.insert(GroupExpression::new(Operand::Selection));
    g1.insert(GroupExpression::new(Operand::Limit));
    g1.insert(GroupExpression::new(Operand::Projection));
    g1.insert(GroupExpression::new(Operand::Limit));

    let join = GroupExpression::new(Operand::Join)
        .with_child(g0.clone())
        .with_child(g1.clone());
    let mut g2 = Group::new(EngineType::TiDb);
    g2.insert(join);

    let pat = build_pattern(
        Operand::Join,
        EngineTypeSet::ALL,
        [
            build_pattern(Operand::Projection, EngineTypeSet::ALL, []),
            build_pattern(Operand::Selection, EngineTypeSet::ALL, []),
        ],
    );

    let iter = new_expr_iter_from_group_elem(&g2, 0, &pat).expect("join expression must match");
    let root = iter.current().expect("iterator starts matched");
    assert_eq!(root.operand, Operand::Join);
    assert_eq!(root.engine, EngineType::TiDb);
    assert_eq!(root.children.len(), 2);

    // children[0] is bound to g0's unique Projection equivalence class member
    // (Go: `g0.GetFirstElem(pattern.OperandProjection)`), leaf with no
    // sub-bindings.
    assert_eq!(root.children[0].operand, Operand::Projection);
    assert_eq!(root.children[0].engine, EngineType::TiDb);
    assert!(root.children[0].children.is_empty());

    // children[1] mirrors it for g1's Selection anchor.
    assert_eq!(root.children[1].operand, Operand::Selection);
    assert_eq!(root.children[1].engine, EngineType::TiDb);
    assert!(root.children[1].children.is_empty());

    assert_eq!(iter.len(), 1);
}

/// GO PORT of `pkg/planner/memo/expr_iterator_test.go:77 TestExprIterNext`.
///
/// Go builds a 3x3 combinatorial memo: g0 holds three distinct Projections
/// interleaved with two Limits, g1 three Selections with two Limits, joined by
/// one Join in g2. Pattern Join(Projection, Selection) over
/// `NewExprIterFromGroupElem(g2 front)` must yield exactly 9 matched
/// bindings (:111 asserts count == 9), every one of them rooted at Join with
/// the projection child on g0 and selection child on g1, both leaves (:96-108).
#[test]
fn expr_iter_next_enumerates_all_nine_projection_selection_combinations() {
    let mut g0 = Group::new(EngineType::TiDb);
    g0.insert(GroupExpression::new(Operand::Projection));
    g0.insert(GroupExpression::new(Operand::Limit));
    g0.insert(GroupExpression::new(Operand::Projection));
    g0.insert(GroupExpression::new(Operand::Limit));
    g0.insert(GroupExpression::new(Operand::Projection));

    let mut g1 = Group::new(EngineType::TiDb);
    g1.insert(GroupExpression::new(Operand::Selection));
    g1.insert(GroupExpression::new(Operand::Limit));
    g1.insert(GroupExpression::new(Operand::Selection));
    g1.insert(GroupExpression::new(Operand::Limit));
    g1.insert(GroupExpression::new(Operand::Selection));

    let join = GroupExpression::new(Operand::Join)
        .with_child(g0.clone())
        .with_child(g1.clone());
    let mut g2 = Group::new(EngineType::TiDb);
    g2.insert(join);

    let pat = build_pattern(
        Operand::Join,
        EngineTypeSet::ALL,
        [
            build_pattern(Operand::Projection, EngineTypeSet::ALL, []),
            build_pattern(Operand::Selection, EngineTypeSet::ALL, []),
        ],
    );

    let mut iter =
        new_expr_iter_from_group_elem(&g2, 0, &pat).expect("join expression must match");
    let mut count = 0;
    while iter.matched() {
        count += 1;
        let root = iter.current().expect("matched position has a binding");
        assert_eq!(root.operand, Operand::Join);
        assert_eq!(root.engine, EngineType::TiDb);
        assert_eq!(root.children.len(), 2);
        assert_eq!(root.children[0].operand, Operand::Projection);
        assert_eq!(root.children[0].engine, EngineType::TiDb);
        assert!(root.children[0].children.is_empty());
        assert_eq!(root.children[1].operand, Operand::Selection);
        assert_eq!(root.children[1].engine, EngineType::TiDb);
        assert!(root.children[1].children.is_empty());
        iter.advance();
    }
    assert_eq!(count, 9);
}

/// GO PORT of `pkg/planner/memo/expr_iterator_test.go:130 TestExprIterReset`.
///
/// Same shape as `TestExprIterNext` but the right-hand Selections each own one
/// child group g2 holding two Limits, and the pattern deepens to
/// Join(Projection, Selection(Limit)). The iterator must bind 18 complete
/// trees (:197 asserts count == 18): 3 projections x 3 selections x 2 limits,
/// where every depth-2 grandchild binding carries OperandLimit with no
/// children of its own (:183-190).
#[test]
fn expr_iter_reset_rebinds_eighteen_deep_bindings_after_full_drain() {
    let mut g0 = Group::new(EngineType::TiDb);
    g0.insert(GroupExpression::new(Operand::Projection));
    g0.insert(GroupExpression::new(Operand::Limit));
    g0.insert(GroupExpression::new(Operand::Projection));
    g0.insert(GroupExpression::new(Operand::Limit));
    g0.insert(GroupExpression::new(Operand::Projection));

    // g2 backs every right-hand selection below.
    let mut g2 = Group::new(EngineType::TiDb);
    g2.insert(GroupExpression::new(Operand::Selection));
    g2.insert(GroupExpression::new(Operand::Limit));
    g2.insert(GroupExpression::new(Operand::Selection));
    g2.insert(GroupExpression::new(Operand::Limit));
    g2.insert(GroupExpression::new(Operand::Selection));

    let make_sel = || GroupExpression::new(Operand::Selection).with_child(g2.clone());
    let mut g1 = Group::new(EngineType::TiDb);
    g1.insert(make_sel());
    g1.insert(GroupExpression::new(Operand::Limit));
    g1.insert(make_sel());
    g1.insert(GroupExpression::new(Operand::Limit));
    g1.insert(make_sel());

    let join = GroupExpression::new(Operand::Join)
        .with_child(g0.clone())
        .with_child(g1.clone());
    let mut g3 = Group::new(EngineType::TiDb);
    g3.insert(join);

    let pat = build_pattern(
        Operand::Join,
        EngineTypeSet::ALL,
        [
            build_pattern(Operand::Projection, EngineTypeSet::ALL, []),
            build_pattern(
                Operand::Selection,
                EngineTypeSet::ALL,
                [build_pattern(Operand::Limit, EngineTypeSet::ALL, [])],
            ),
        ],
    );

    let mut iter =
        new_expr_iter_from_group_elem(&g3, 0, &pat).expect("join expression must match");
    let mut count = 0;
    while iter.matched() {
        count += 1;
        let root = iter.current().expect("matched position has a binding");
        assert_eq!(root.operand, Operand::Join);
        assert_eq!(root.children.len(), 2);
        assert_eq!(root.children[0].operand, Operand::Projection);
        assert!(root.children[0].children.is_empty());
        assert_eq!(root.children[1].operand, Operand::Selection);
        assert_eq!(root.children[1].children.len(), 1);
        assert_eq!(root.children[1].children[0].operand, Operand::Limit);
        assert!(root.children[1].children[0].children.is_empty());
        iter.advance();
    }
    assert_eq!(count, 18);
}

/// GO PORT of
/// `pkg/planner/memo/expr_iterator_test.go:207 TestExprIterWithEngineType`.
///
/// Engine-tagged memo (comment block :255-265): g4[Join -> g3 x2], g3[TiDB]
/// holds two TiKVSingleGathers wrapping g1[TiFlash: Sel, Lim(1), Proj, Lim(2)]
/// and g2[TiKV: Sel, Lim(2), Proj, Lim(3)]. Nine patterns pin engine filtering
/// (:268-298): gather+limit(TiKVOnly)==2, limit(TiFlashOnly)==2,
/// limit(TiKVOrTiFlash)==4, selection(TiFlashOnly)==1,
/// projection(TiKVOnly)==1, join over both sides==4 / mixed==4 /
/// both-any-of-two==16, and childless-gather patterns still match 4 because a
/// pattern node without children imposes no arity constraint (:291-298).
#[test]
fn expr_iter_with_engine_type_filters_groups_by_engine_tags() {
    let mut g1 = Group::new(EngineType::TiFlash);
    g1.insert(GroupExpression::new(Operand::Selection));
    g1.insert(GroupExpression::new(Operand::Limit));
    g1.insert(GroupExpression::new(Operand::Projection));
    g1.insert(GroupExpression::new(Operand::Limit));

    let mut g2 = Group::new(EngineType::TiKv);
    g2.insert(GroupExpression::new(Operand::Selection));
    g2.insert(GroupExpression::new(Operand::Limit));
    g2.insert(GroupExpression::new(Operand::Projection));
    g2.insert(GroupExpression::new(Operand::Limit));

    let flash_gather = GroupExpression::new(Operand::TiKvSingleGather).with_child(g1.clone());
    let mut g3 = Group::new(EngineType::TiDb);
    g3.insert(flash_gather);

    let tikv_gather = GroupExpression::new(Operand::TiKvSingleGather).with_child(g2.clone());
    g3.insert(tikv_gather);

    let join = GroupExpression::new(Operand::Join)
        .with_child(g3.clone())
        .with_child(g3.clone());
    let mut g4 = Group::new(EngineType::TiDb);
    g4.insert(join);

    // p0: gather(TiDB)[limit(TiKV)] -> only the TiKV-backed gather's limits.
    let p0 = build_pattern(
        Operand::TiKvSingleGather,
        EngineTypeSet::TIDB_ONLY,
        [build_pattern(Operand::Limit, EngineTypeSet::TIKV_ONLY, [])],
    );
    assert_eq!(
        count_matched_iter(&g3, &p0),
        2,
        "p0 gather+limit(TiKVOnly)"
    );

    // p1: same but TiFlashOnly -> only the TiFlash-backed gather's limits.
    let p1 = build_pattern(
        Operand::TiKvSingleGather,
        EngineTypeSet::TIDB_ONLY,
        [build_pattern(Operand::Limit, EngineTypeSet::TIFLASH_ONLY, [])],
    );
    assert_eq!(
        count_matched_iter(&g3, &p1),
        2,
        "p1 gather+limit(TiFlashOnly)"
    );

    // p2: TiKVOrTiFlash reaches both gathers' limits.
    let p2 = build_pattern(
        Operand::TiKvSingleGather,
        EngineTypeSet::TIDB_ONLY,
        [build_pattern(
            Operand::Limit,
            EngineTypeSet::TIKV_OR_TIFLASH,
            [],
        )],
    );
    assert_eq!(
        count_matched_iter(&g3, &p2),
        4,
        "p2 gather+limit(TiKVOrTiFlash)"
    );

    // p3/p4: wrong operator inside an accepted engine still fails.
    let p3 = build_pattern(
        Operand::TiKvSingleGather,
        EngineTypeSet::TIDB_ONLY,
        [build_pattern(Operand::Selection, EngineTypeSet::TIFLASH_ONLY, [])],
    );
    assert_eq!(count_matched_iter(&g3, &p3), 1, "p3 sel(TiFlashOnly)");
    let p4 = build_pattern(
        Operand::TiKvSingleGather,
        EngineTypeSet::TIDB_ONLY,
        [build_pattern(Operand::Projection, EngineTypeSet::TIKV_ONLY, [])],
    );
    assert_eq!(count_matched_iter(&g3, &p4), 1, "p4 proj(TiKVOnly)");

    // p5..p7: Joins over (gather x gather) multiply per-side choices.
    let gather_with = |engine_set| {
        build_pattern(
            Operand::TiKvSingleGather,
            EngineTypeSet::TIDB_ONLY,
            [build_pattern(Operand::Limit, engine_set, [])],
        )
    };
    let p5 = build_pattern(
        Operand::Join,
        EngineTypeSet::TIDB_ONLY,
        [gather_with(EngineTypeSet::TIKV_ONLY), gather_with(EngineTypeSet::TIKV_ONLY)],
    );
    assert_eq!(count_matched_iter(&g4, &p5), 4, "p5 join kv/kv");
    let p6 = build_pattern(
        Operand::Join,
        EngineTypeSet::TIDB_ONLY,
        [
            gather_with(EngineTypeSet::TIFLASH_ONLY),
            gather_with(EngineTypeSet::TIKV_ONLY),
        ],
    );
    assert_eq!(count_matched_iter(&g4, &p6), 4, "p6 join flash/kv");
    let p7 = build_pattern(
        Operand::Join,
        EngineTypeSet::TIDB_ONLY,
        [
            gather_with(EngineTypeSet::TIKV_OR_TIFLASH),
            gather_with(EngineTypeSet::TIKV_OR_TIFLASH),
        ],
    );
    assert_eq!(count_matched_iter(&g4, &p7), 16, "p7 join either/either");

    // p8: childless gather patterns impose no arity constraint (:289-298).
    let childless_gather = build_pattern(Operand::TiKvSingleGather, EngineTypeSet::TIDB_ONLY, []);
    let p8 = build_pattern(
        Operand::Join,
        EngineTypeSet::TIDB_ONLY,
        [childless_gather.clone(), childless_gather],
    );
    assert_eq!(count_matched_iter(&g4, &p8), 4, "p8 childless gathers");
}

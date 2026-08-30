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

//! Port ledger for `pkg/planner/core/issuetest/` (`pkg/planner.part11`, Go
//! items 641–646 on `origin/master`).
//!
//! Family contract: panic-regression guards and issue regressions driven
//! through full mock-store sessions; `main_test.go` TestMain only loads the
//! planner_issue golden book + goleak options (bootstrap, no Rust test — see
//! receipt).
//!
//! One item has a live carrier here: the crate transcreates
//! `rule_push_down_sequence.go` traversal as a dependency-closed structural
//! adapter (`crate::push_down_sequence`), so item 644's childless-operator
//! guard is a REAL port over that adapter. The rest are honest gap ports;
//! nothing was approximated to simulate Go behavior.

/// GO PORT of `pkg/planner/core/issuetest/panicrisk_tier2_test.go:60
/// TestPushDownSequenceWithTableDual`.
///
/// Go guard: pushing a Sequence down under shared-CTE execution used to index
/// `Children()[0]` on a CHILDLESS operator — specifically a LogicalTableDual
/// produced by constant-false predicates (`where 1 = 0`) under a join of two
/// CTE refs (`with cte as (select a from t) select * from cte c1 join cte c2
/// on c1.a = c2.a where 1 = 0`, :69-77). Production carrier:
/// `recursiveOptimize` default branch in
/// pkg/planner/core/rule_push_down_sequence.go:67-77 — an operator with
/// `len(children) != 1` while a sequence is pushed must ATTACH the sequence
/// above itself and stop descending (:68-74); exactly-one-child operators
/// push THROUGH (:76-77).
///
/// This port drives the crate's structural adapter
/// (`push_down_sequence::PushDownSequenceSolver`) with the same shapes:
/// 1. the query shape — a LogicalSequence holding one CTE-def leaf plus a
///    two-child main join whose sides are CTE leaves — reorganizes into
///    Sequence{cte_children..., main} without losing any subtree;
/// 2. the crash shape — the same join whose subtree now ends in a childless
///    TableDual-equivalent operator (constant-false product) — must not
///    panic; the sequence attaches ABOVE the childless operator instead;
/// 3. push-through parity — a single-child operator between sequence and
///    dual keeps its position with the sequence wrapped below it.
///
/// The SQL surface (constant-folding to TableDual, shared-CTE MPP execution)
/// remains an external planner owner per the adapter's module header; what is
/// pinned here is exactly the traversal contract that regressed.
#[test]
fn push_down_sequence_attaches_above_childless_table_dual() {
    use tidb_planner::push_down_sequence::{PushDownSequenceSolver, SequenceNodeKind, SequencePlan};

    let solver = PushDownSequenceSolver;
    assert_eq!(
        solver.name(),
        "push_down_sequence",
        "rule registry name matches rule_push_down_sequence.go Name()"
    );

    // A LogicalSequence whose LAST child is the main query (:56-57).
    let sequence_of = |main: SequencePlan| {
        SequencePlan::with_children(
            SequenceNodeKind::Sequence,
            [SequencePlan::new(SequenceNodeKind::Cte), main],
        )
    };
    let cte_ref = || SequencePlan::new(SequenceNodeKind::Cte);

    // Query shape: SELECT * FROM cte c1 JOIN cte c2 ON c1.a=c2.a — the main
    // query ROOT is a two-child join carrying a pushed sequence. The source
    // rule attaches the sequence above such operators and stops descending
    // (:68-74), so the input shape round-trips untouched.
    let join_shape = SequencePlan::with_children(
        SequenceNodeKind::Operator,
        [cte_ref(), cte_ref()],
    );
    let (optimized, changed) = solver.optimize(sequence_of(join_shape.clone()));
    assert!(!changed, "source Optimize reports no direct change");
    assert_eq!(optimized, sequence_of(join_shape));

    // Plain operator subtrees reached WITHOUT a pushed sequence are rebuilt
    // through their children and must compare equal afterwards.
    let bare_tree = SequencePlan::with_children(
        SequenceNodeKind::Operator,
        [
            cte_ref(),
            SequencePlan::with_children(
                SequenceNodeKind::Operator,
                [cte_ref(), cte_ref()],
            ),
        ],
    );
    assert_eq!(solver.optimize(bare_tree.clone()).0, bare_tree);

    // The literal Go-crash route: a single-child operator (a Selection over
    // the constant-false product) sits between the sequence and a CHILDLESS
    // table-dual-equivalent leaf. Traversal must push THROUGH the unary (one
    // child, :76-77) and ATTACH the sequence above the childless leaf —
    // never indexing Children()[0] of it. Expected: Selection now WRAPS
    // Sequence wrapping the dual.
    let unary_over_childless_dual = sequence_of(SequencePlan::with_children(
        SequenceNodeKind::Operator,
        [SequencePlan::new(SequenceNodeKind::Operator)],
    ));
    let expected_unary_wraps_sequence = SequencePlan::with_children(
        SequenceNodeKind::Operator,
        [sequence_of(SequencePlan::new(SequenceNodeKind::Operator))],
    );
    // Must not panic (the Go regression panicked here).
    let optimized = solver.optimize(unary_over_childless_dual).0;
    assert_eq!(
        optimized, expected_unary_wraps_sequence,
        "pushed sequence embeds BELOW the unary parent and attaches above the childless dual"
    );

    // Same attach guard at a MULTI-CHILD operator below the unary: the
    // sequence rides through the unary and lands above the two-child join.
    let unary_over_join = sequence_of(SequencePlan::with_children(
        SequenceNodeKind::Operator,
        [SequencePlan::with_children(
            SequenceNodeKind::Operator,
            [cte_ref(), cte_ref()],
        )],
    ));
    let expected_unary_over_sequence_over_join = SequencePlan::with_children(
        SequenceNodeKind::Operator,
        [sequence_of(SequencePlan::with_children(
            SequenceNodeKind::Operator,
            [cte_ref(), cte_ref()],
        ))],
    );
    let optimized = solver.optimize(unary_over_join).0;
    assert_eq!(optimized, expected_unary_over_sequence_over_join);
}

/// GO PORT of `pkg/planner/core/issuetest/planner_issue_test.go:33
/// TestPlannerIssueRegressions`.
///
/// Re-derived contract: ~950-line batch of issue regressions across access
/// paths, decorrelation, type leakage, plan/cache stability and DML planning
/// (:33-983), each block pinned by exact explain text or result rows —
/// representative blocks: index-lookup-columns-mismatch (:51-81, IndexScan vs
/// TableScan column lists diverge inside IndexLookUp on hash-partitioned t),
/// remove-unnecessary-first-row (:83-107, distinct-cast aggregates drop
/// redundant FirstRow), inl-join-inner-multi-pattern (:109-134),
/// update-join-covering-index (:255+), rollup-having-exists-nil-expression
/// (:527+), instance-plan-cache-with-prepare (:575+),
/// issue-66399 outer-join-eliminate keeps parent join-condition columns
/// (:806+), point-update negative-to-unsigned error codes (:826+),
/// unionscan eliminates TableDual for null comparison (:886+), issue-67802
/// mutable user-var join conditions (:920+), constant-left-nulleq partition
/// pruning (:955+), issue-66706 decimal scale leak through SIGN view
/// predicate (:969-982).
#[test]
#[ignore = "go-parity-gap: needs full optimize+execute stack; ~40 heterogeneous issue blocks"]
fn planner_issue_regressions_batch() {}

/// GO PORT of `pkg/planner/core/issuetest/planner_issue_test.go:985
/// TestOnlyFullGroupCantFeelUnaryConstant`.
///
/// Re-derived contract: ONLY_FULL_GROUP_BY must NOT flag a column selected
/// alongside min(a) when the WHERE contains a unary-minus constant
//  comparison (`where a=-1` / `-1=a` both forms, :992-994): constant
/// propagation folds equality with -1 so a becomes const-evaluable and the
/// aggregate query stays legal, returning NULL rows on empty tables.
#[test]
#[ignore = "go-parity-gap: needs only-full-group-by validation + constant-folding interplay"]
fn only_full_group_by_cannot_feel_unary_constant_in_where() {}

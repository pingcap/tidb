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

//! Documentary gap ports for `pkg/planner/cascades/rule`
//! (`pkg/planner.part2` items 109-117 on `origin/master`).
//!
//! The eight binder tests drive the legacy top-down pattern binder over a
//! live legacy memo (`pkg/planner/cascades/memo`, built through
//! `Memo.Init`/`CopyIn` under the MockPlanSkipMemoDeriveStats failpoint) and
//! compare binding walks plus debug stack traces written into
//! `util.NewStrBuffer`. Neither the binder (`binder.go:148 NewBinder`,
//! `binder.go:201 Next`, dfsMatch at :231, any-matching at :270,
//! printStackInfo at :299) nor its legacy-memo substrate is transcreated
//! here — this crate's memo leaves carry the NEW `pkg/planner/memo`
//! plan-hash shape instead. All eight stay documentary gaps; behavior is not
//! approximated.
//!
//! The ninth Go test of this family,
//! `rule/apply/decorrelateapply/xf_decorrelate_apply_test.go::TestXFDeCorre
//! lateShouldDeleteIntermediaryApply`, calls `t.Skip` in Go itself (:40:
//! "decorrelateapply.XFDeCorrelateSimpleApply rule is not applied in the
//! cascades optimizer fully") so its body never runs upstream; recorded as
//! skipped-reason in the receipt without a Rust twin.

/// GO PORT of `pkg/planner/cascades/rule/binder_test.go:31 TestBinderSuccess`.
///
/// Re-derived contract: over memo(join(t1,t2)) with group ids 1..3 (:39-50),
/// binding the Join-over-{DataSource,DataSource} pattern to the root group
/// expression (rootGE pulled from the list back, :60) yields a holder whose
/// wrapped plan IS join and whose two child holders wrap t1/t2 respectively
/// (:62-66; Binder.Next returns the bound holder tree via dfsMatch/
/// pickGroupExpression, binder.go:201-288).
#[test]
#[ignore = "go-parity-gap: legacy-cascades Memo + Binder machinery (failpoint-built groups over real logical plans, pointer identity checks) are unported"]
fn binder_binds_join_over_datasource_pattern_to_memo_root() {
    // Restore: Init(join); NewBinder(pa, rootGE).Next(); assert holder plan
    // pointer equality with join/t1/t2 as at binder_test.go:59-65.
}

/// GO PORT of `pkg/planner/cascades/rule/binder_test.go:69 TestBinderFail`.
///
/// Re-derived contract: three failed bindings print precise stack traces —
/// DataSource child under a Projection-positioned pattern prints
/// "GE:DataSource_1{}" (:95-98); an unmatched Limit-under-Join position
/// inside an unmatchable root prints "" (:101-116); against a freshly built
/// memo(Projection→Limit→t1), exhaustion prints "GE:Limit_4{GID:1}"
/// (:118-129). Trace lines come from GroupExpression.String over the bsw test
/// writer (binder.go:143-144 bsw field; printStackInfo :299-310).
#[test]
#[ignore = "go-parity-gap: needs Binder's StrBuffer trace output wired to legacy memo internals; unported"]
fn binder_failures_print_expected_stack_traces() {
    // Restore: three NewBinder/Next rounds exactly as binder_test.go:82-137;
    // require.Nil holder each time; flush buf; require exact strings.
}

/// GO PORT of `pkg/planner/cascades/rule/binder_test.go:132 TestBinderTopNode`.
///
/// Re-derived contract: a single-node pattern (no children) binds to the root
/// group expression directly — Next returns non-nil (:151) and the stored
/// holder classifies as OperandJoin via pattern.GetOperand (:152-156).
#[test]
#[ignore = "go-parity-gap: Binder.holder bookkeeping over legacy memo roots is unported"]
fn binder_top_node_single_level_pattern_matches_root() {
    // Restore: single-level pattern; Next() != nil;
    // GetOperand(binder.GetHolder()) == OperandJoin.
}

/// GO PORT of `pkg/planner/cascades/rule/binder_test.go:156 TestBinderOneNode`.
///
/// Re-derived contract: a memo whose whole tree is one join node (single
/// group, groups==1 assertions :166-170) still binds the childless Join
/// pattern from that lone root expression: Next yields a classified holder
/// (:171-180).
#[test]
#[ignore = "go-parity-gap: single-group legacy Memo.Init path plus Binder next-selection are unported"]
fn binder_one_node_memo_binds_childless_join_pattern() {
    // Restore: Init(join-without-children); groups==1; bind; classify holder.
}

/// GO PORT of `pkg/planner/cascades/rule/binder_test.go:176
/// TestBinderSubTreeMatch`.
///
/// Re-derived contract: memo(join3(join1,t-children; join2,t-children)) has 7
/// groups (:192-206); the Join{Join,Join} pattern binds once onto
/// {join3,join1,join2} (:204-217) and a SECOND Next yields nil — subtree
/// exploration below a pinned root expression is the caller's job (:218-230
/// comment); likewise the DataSource-flavored sibling pattern fails against
/// the pinned root GE even though it would match deeper (:216-230 region).
#[test]
#[ignore = "go-parity-gap: pinned-root-expression semantics (Holder anchoring across Next calls) live in unported Binder state"]
fn binder_subtree_match_pins_to_root_expression_once() {
    // Restore: Init(join3); first Next -> join3/join1/join2; second Next ->
    // nil; pa2 bind also nil per binder_test.go:232-240.
}

/// GO PORT of `pkg/planner/cascades/rule/binder_test.go:223
/// TestBinderMultiNext`.
///
/// Re-derived contract: after CopyIn merges t3/t4 into the join children's
/// equivalence classes (:250-256 diagram G2{t1,t3}/G3{t2,t4}), successive
/// Next() calls enumerate all four cartesian child bindings in source order
/// t1t2/t1t4/t3t2/t3t4 (:258-315) and the flushed trace equals the five-line
/// recorded string ending each exhausted sub-bind (:318-324):
/// every completed next leaves the internal stack positioned at the next
/// start point (binder.go dfsSave/dfsRestore flow).
#[test]
#[ignore = "go-parity-gap: multi-equivalent enumeration order and stack-trace restoration need legacy Binder+Memo internals"]
fn binder_multi_next_enumerates_cartesian_equivalents_in_order() {
    // Restore: CopyIn t3/t4; four Next bindings asserting TableAsName pairs;
    // require.Equal on the concatenated five-line buffer string at
    // binder_test.go:318.
}

/// GO PORT of `pkg/planner/cascades/rule/binder_test.go:325 TestBinderAny`.
///
/// Re-derived contract: replacing one child pattern with OperandAny enumerates
/// only ONE binding for that Any side (matched by group, not by concrete
/// expression — anyHasBeenMatched at binder.go:270-276): bindings are
/// t1×t2 then t3×t2 only (:362-406), and the trace pairs show the Any side
/// never revisits its second equivalent (:407-412).
#[test]
#[ignore = "go-parity-gap: Any-pattern first-match-only semantics depend on unported anyHasBeenMatched/Memo state"]
fn binder_any_pattern_consumes_single_equivalent_per_side() {
    // Restore: pattern {Join,{DS,Any}}; Next twice (t2-side fixated); third
    // nil; buffer equals the four-line recorded string at :407-412.
}

/// GO PORT of `pkg/planner/cascades/rule/binder_test.go:413
/// TestBinderMultiAny`.
///
/// Re-derived contract: with BOTH children set to OperandAny, exactly one
/// overall binding survives (first equivalents on both sides, :447-472);
/// the next call immediately exhausts because group-scoped Any matching
/// deliberately ignores remaining expressions — final generated plans embed
/// the group itself rather than any concrete member (:479-505 comment
/// block, closing file body).
#[test]
#[ignore = "go-parity-gap: dual-Any collapse semantics need the same unported Any-matching core"]
fn binder_multi_any_yields_single_binding_from_first_equivalents() {
    // Restore: pattern {Join,{Any,Any}}; one Next asserting t1/t2; second nil;
    // buffer equals recorded strings at binder_test.go:480-505.
}

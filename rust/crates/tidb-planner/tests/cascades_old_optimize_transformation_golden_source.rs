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

//! Documentary gap ports for `pkg/planner/cascades/old`
//! (`pkg/planner.part2` items 78-103 on `origin/master`).
//!
//! The legacy cascades optimizer package owns an end-to-end driver:
//! `Optimizer.onPhasePreprocessing/Exploration` (optimize.go:119/:128),
//! `fillGroupStats` (optimize.go:221), `implGroup` cost-limited search
//! (optimize.go:268), `preparePossibleProperties` (optimize.go:358), rule
//! batches of 38 transformation constructors (transformation_rules.go), and
//! group stringers rendering converted memo groups against the
//! `stringer_suite` / `transformation_rules_suite` books. Every Go test below
//! parses SQL via `BuildLogicalPlanForTest`, converts with
//! `memo.Convert2Group`, and inspects the explored memo — machinery entirely
//! absent from this crate's dependency-closed leaves. All are documentary
//! gaps; none of their behavior is approximated. The package bootstrap
//! (`main_test.go:32 TestMain`: loads both books, goleak) is recorded as
//! skipped-reason in the receipt.

/// GO PORT of
/// `pkg/planner/cascades/old/enforcer_rules_test.go:26 TestGetEnforcerRules`.
///
/// Re-derived contract: `GetEnforcerRules` returns nil for an empty required
/// property (:30-33; enforcer_rules.go:41-50 appends only when SortItems is
/// non-empty); adding one SortItem yields exactly one enforcer whose dynamic
/// type is `*OrderEnforcer` (:35-44).
#[test]
#[ignore = "go-parity-gap: needs old-cascades GetEnforcerRules over property.PhysicalProperty.SortItems plus memo.NewGroupWithSchema; unported"]
fn get_enforcer_rules_appends_order_enforcer_only_for_sort_items() {
    // Restore: empty prop -> nil; one SortItem -> len 1, *OrderEnforcer cast.
}

/// GO PORT of
/// `pkg/planner/cascades/old/enforcer_rules_test.go:42 TestNewProperties`.
///
/// Re-derived contract: `OrderEnforcer.NewProperty` strips order requirements,
/// producing a property with nil SortItems (:49-53;
/// enforcer_rules.go:57-63 — the relaxed child requirement before enforcing
/// sort again above it).
#[test]
#[ignore = "go-parity-gap: OrderEnforcer.NewProperty (enforcer_rules.go:58-63) has no Rust carrier outside crate::enforce's physical-op seam"]
fn order_enforcer_new_property_drops_sort_items() {
    // Restore: build prop+enforcer as in TestGetEnforcerRules; assert newProp.SortItems == nil.
}

/// GO PORT of `pkg/planner/cascades/old/optimize_test.go:38
/// TestImplGroupZeroCost`.
///
/// Re-derived contract: building `select t1.a, t2.a from t t1 left join t t2
/// on t1.a=t2.a where t1.a<1.0`, converting to a memo group, and calling
/// `implGroup(rootGroup, {ExpectedCnt: MaxFloat64}, 0.0)` must return
/// `(nil, nil)` (:52-63): a zero cost limit admits no implementation so the
/// best stays unset (optimize.go:268-340 prunes once candidate cost exceeds
/// the limit).
#[test]
#[ignore = "go-parity-gap: Convert2Group + implGroup cost-limited search need the whole logical-plan builder and implementation-rule set"]
fn impl_group_zero_cost_limit_admits_no_implementation() {
    // Restore: parse/build left-join plan; Convert2Group; implGroup(cost=0);
    // require impl==nil, err==nil.
}

/// GO PORT of `pkg/planner/cascades/old/optimize_test.go:65
/// TestInitGroupSchema`.
///
/// Re-derived contract: converting `select a from t` wraps the plan into a
/// group whose logical property carries the single-column schema (`Prop !=
/// nil`, `Schema.Len()==1`) while stats stay unset before derivation (:81-
/// 90; `Memo.Convert2Group` clones prop without StatsInfo).
#[test]
#[ignore = "go-parity-gap: Convert2Group group-property initialization is unported"]
fn init_group_schema_exposes_column_schema_without_stats() {
    // Restore: build "select a from t"; g:=Convert2Group(logic);
    // g.Prop.Schema.Len()==1; g.Prop.Stats==nil.
}

/// GO PORT of `pkg/planner/cascades/old/optimize_test.go:91
/// TestFillGroupStats`.
///
/// Re-derived contract: after `select * from t t1 join t t2 on t1.a=t2.a`,
/// `Optimizer.fillGroupStats(rootGroup)` recursively populates
/// `rootGroup.Prop.Stats` non-nil (:107-115; optimize.go:221-243 derives
/// bottom-up through group expressions' children groups).
#[test]
#[ignore = "go-parity-gap: fillGroupStats recursive stat derivation over real plans is unported"]
fn fill_group_stats_populates_root_stats_recursively() {
    // Restore: build inner-join plan; Convert2Group; fillGroupStats;
    // Prop.Stats != nil.
}

/// GO PORT of `pkg/planner/cascades/old/optimize_test.go:116
/// TestPreparePossibleProperties`.
///
/// Re-derived contract: restricting exploration rules to RuleEnumeratePaths,
/// preprocessing `select f, sum(a) from t group by f`, then exploring the agg
/// group leaves per-group possible-order info (:161-176): the aggregation's
/// property map holds exactly one order `[f]` while its gathered scan-child
/// group holds two one-column orders, each headed by `a` or `f` (:177-206;
/// preparePossibleProperties merges children orders bottom-up at
/// optimize.go:358-394).
#[test]
#[ignore = "go-parity-gap: needs enumerate-paths exploration over real DataSource plus the possible-properties fixpoint; unported"]
fn prepare_possible_properties_collects_scan_and_agg_orders() {
    // Restore: restricted rule set; preprocess+explore; walk propMap for agg
    // Orders[[f]] and gather Orders[{a},{f}].
}

/// GO PORT of `pkg/planner/cascades/old/optimize_test.go:212
/// TestAppliedRuleSet`.
///
/// Re-derived contract: registering a fake transformation matching any
/// projection records exactly ONE applied hit when exploring `select 1`
/// (:228-239): `OnTransform` increments `appliedTimes` and marks the applied
/// rule (:216-226) while the eraseOld=true result keeps the expression list
/// unchanged but terminates re-exploration of that expression within the
/// round (optimize.go:140-219 exploration loop honors AddAppliedRule marks).
#[test]
#[ignore = "go-parity-gap: pluggable ResetTransformationRules + explore loop bookkeeping are unported"]
fn applied_rule_set_counts_single_fake_projection_hit() {
    // Restore: inject fakeTransformation(OperandProjection, EngineAll);
    // explore select-1 group; appliedTimes==1.
}

/// GO PORT of `pkg/planner/cascades/old/stringer_test.go:35
/// TestGroupStringer`.
///
/// Re-derived contract: with only push-sel-down-gather/table-scan and
/// enumerate-paths enabled, each `stringer_suite_in.json` input rendered by
/// `ToString(ctx.GetEvalCtx(), group)` after preprocessing + exploration +
/// BuildKeyInfo must equal the recorded `stringer_suite_out.json` Result rows
/// (:51-85; stringer.go:28 renders per-group lines with operand name, id,
/// property text). Book files: pkg/planner/cascades/old/testdata/.
#[test]
#[ignore = "go-parity-gap: golden stringer_suite books over live explore pipeline; no renderer or plan converter exists here"]
fn group_stringer_matches_recorded_stringer_suite_goldens() {
    // Restore: for each book case, preprocess/explore/BuildKeyInfo then diff
    // ToString lines against out.json.
}

/// GO PORT of `pkg/planner/cascades/old/transformation_rules_test.go:72
/// TestAggPushDownGather`.
///
/// Re-derived contract (helper testGroupToString :28-70): only
/// NewRulePushAggDownGather (:76) + EnumeratePaths (:79) active; every
/// `transformation_rules_suite` case under this batch renders post-explore
/// group trees byte-equal to the book output, including BuildKeyInfo-driven
/// partialAgg keys (:119 OnRecord compare block at :121-124).
#[test]
#[ignore = "go-parity-gap: transformation_rules_suite goldens over live exploration; unported"]
fn agg_push_down_gather_transformation_goldens() {
    // Restore: testGroupToString-equivalent with NewRulePushAggDownGather +
    // NewRuleEnumeratePaths batch.
}

/// GO PORT of `pkg/planner/cascades/old/transformation_rules_test.go:128
/// TestPredicatePushDown`.
///
/// Same suite shape with the selection-pushdown family PushSelDown over
/// {Sort :133, Projection :134, Aggregation :135, Join :136, UnionAll :137,
/// Window :138}, MergeAdjacentSelection (:139), TransformJoinCondToSel
/// (:142), then pushdown onto TableScan/TiKVSingleGather/IndexScan
/// (:147-149) plus EnumeratePaths (:152) (:130-166).
#[test]
#[ignore = "go-parity-gap: transformation_rules_suite goldens over live exploration; unported"]
fn predicate_push_down_selection_family_goldens() {
    // Restore: same helper with the eight PPD rules (:133-149) +
    // EnumeratePaths.
}

/// GO PORT of `pkg/planner/cascades/old/transformation_rules_test.go:169
/// TestTopNRules`.
///
/// Suite cases with TransformLimitToTopN (:174), limit pushdowns over
/// Projection/OuterJoin/UnionAll (:175-177), topN pushdowns over the same
/// three plus TiKVSingleGather (:181-191), and both MergeAdjacent rules in
/// support (:139/:178 region) (:171-206).
#[test]
#[ignore = "go-parity-gap: transformation_rules_suite goldens over live exploration; unported"]
fn topn_limit_rule_family_goldens() {
    // Restore: same helper with the ten-rule batch (:174-194) + EnumeratePaths.
}

/// GO PORT of `pkg/planner/cascades/old/transformation_rules_test.go:208
/// TestProjectionElimination`.
///
/// Suite cases with EliminateProjection + MergeAdjacentProjection (:210-225).
#[test]
#[ignore = "go-parity-gap: transformation_rules_suite goldens over live exploration; unported"]
fn projection_elimination_goldens() {
    // Restore: two-rule batch through the shared helper.
}

/// GO PORT of `pkg/planner/cascades/old/transformation_rules_test.go:228
/// TestEliminateMaxMin`.
///
/// Suite cases isolating NewRuleEliminateSingleMaxMin at :232 under
/// OperandAggregation (:228-245).
#[test]
#[ignore = "go-parity-gap: transformation_rules_suite goldens over live exploration; unported"]
fn eliminate_max_min_singleton_goldens() {
    // Restore: single-rule batch through the shared helper.
}

/// GO PORT of `pkg/planner/cascades/old/transformation_rules_test.go:247
/// TestMergeAggregationProjection`.
///
/// Suite cases isolating RuleMergeAggregationProjection (:249-263).
#[test]
#[ignore = "go-parity-gap: transformation_rules_suite goldens over live exploration; unported"]
fn merge_aggregation_projection_goldens() {
    // Restore: single-rule batch through the shared helper.
}

/// GO PORT of `pkg/planner/cascades/old/transformation_rules_test.go:266
/// TestMergeAdjacentTopN`.
///
/// Suite cases chaining TransformLimitToTopN (:270), PushTopNDownProjection
/// (:273), MergeAdjacentTopN (:274) and MergeAdjacentProjection (:277)
/// (:266-290).
#[test]
#[ignore = "go-parity-gap: transformation_rules_suite goldens over live exploration; unported"]
fn merge_adjacent_topn_chain_goldens() {
    // Restore: six-rule batch through the shared helper.
}

/// GO PORT of `pkg/planner/cascades/old/transformation_rules_test.go:292
/// TestMergeAdjacentLimit`.
///
/// Suite cases with PushLimitDownProjection + MergeAdjacentLimit (:294-309).
#[test]
#[ignore = "go-parity-gap: transformation_rules_suite goldens over live exploration; unported"]
fn merge_adjacent_limit_pair_goldens() {
    // Restore: two-rule batch through the shared helper.
}

/// GO PORT of `pkg/planner/cascades/old/transformation_rules_test.go:312
/// TestTransformLimitToTableDual`.
///
/// Suite cases isolating RuleTransformLimitToTableDual: impossible ranges
/// collapse to dual under limit semantics; rule constructor at :316
/// (:312-329).
#[test]
#[ignore = "go-parity-gap: transformation_rules_suite goldens over live exploration; unported"]
fn transform_limit_to_table_dual_goldens() {
    // Restore: single-rule batch through the shared helper.
}

/// GO PORT of `pkg/planner/cascades/old/transformation_rules_test.go:331
/// TestPostTransformationRules`.
///
/// Suite cases exercising the POST transformation batch seeded from
/// TransformLimitToTopN (:335) within TransformationRuleBatch at :333
/// (:331-348).
#[test]
#[ignore = "go-parity-gap: transformation_rules_suite goldens over live exploration; unported"]
fn post_transformation_batch_goldens() {
    // Restore: batch(:335-337) through the shared helper.
}

/// GO PORT of `pkg/planner/cascades/old/transformation_rules_test.go:350
/// TestPushLimitDownTiKVSingleGather`.
///
/// Suite cases with PushLimitDownTiKVSingleGather (:354), EliminateProjection
/// (:357) and EnumeratePaths (:360): limit crosses the gather boundary down to
/// scans.
#[test]
#[ignore = "go-parity-gap: transformation_rules_suite goldens over live exploration; unported"]
fn push_limit_down_tikv_single_gather_goldens() {
    // Restore: three-rule batch through the shared helper.
}

/// GO PORT of `pkg/planner/cascades/old/transformation_rules_test.go:375
/// TestEliminateOuterJoin`.
///
/// Suite cases with EliminateOuterJoinBelowAggregation (:379) and
/// EliminateOuterJoinBelowProjection (:382) (:375-395).
#[test]
#[ignore = "go-parity-gap: transformation_rules_suite goldens over live exploration; unported"]
fn eliminate_outer_join_below_shapes_goldens() {
    // Restore: two-rule batch through the shared helper.
}

/// GO PORT of `pkg/planner/cascades/old/transformation_rules_test.go:397
/// TestTransformAggregateCaseToSelection`.
///
/// Suite cases isolating RuleTransformAggregateCaseToSelection under
/// OperandAggregation (:400-401) (:397-414).
#[test]
#[ignore = "go-parity-gap: transformation_rules_suite goldens over live exploration; unported"]
fn transform_aggregate_case_to_selection_goldens() {
    // Restore: single-rule batch through the shared helper.
}

/// GO PORT of `pkg/planner/cascades/old/transformation_rules_test.go:416
/// TestTransformAggToProj`.
///
/// Suite cases isolating RuleTransformAggToProj (:420): aggregate-only-on-
/// distinct-keys shapes become projections inside the memo (:416-436).
#[test]
#[ignore = "go-parity-gap: transformation_rules_suite goldens over live exploration; unported"]
fn transform_agg_to_proj_goldens() {
    // Restore: single-rule batch through the shared helper.
}

/// GO PORT of `pkg/planner/cascades/old/transformation_rules_test.go:438
/// TestDecorrelate`.
///
/// Suite cases pulling selections up through apply and rewriting apply into
/// join under OperandApply: PullSelectionUpApply (:442) +
/// TransformApplyToJoin (:443) (:438-456).
#[test]
#[ignore = "go-parity-gap: transformation_rules_suite goldens over live exploration; unported"]
fn decorrelate_apply_shape_goldens() {
    // Restore: single-rule batch through the shared helper.
}

/// GO PORT of `pkg/planner/cascades/old/transformation_rules_test.go:458
/// TestInjectProj`.
///
/// Suite cases seeding TopNs via TransformLimitToTopN (:462) then injecting
/// projections below aggregations (:466) and below TopNs (:469) (:458-482).
#[test]
#[ignore = "go-parity-gap: transformation_rules_suite goldens over live exploration; unported"]
fn inject_proj_below_agg_and_topn_goldens() {
    // Restore: batch(:462-469) through testGroupToString.
}

/// GO PORT of `pkg/planner/cascades/old/transformation_rules_test.go:484
/// TestMergeAdjacentWindow`.
///
/// Suite cases merging sibling windows: MergeAdjacentProjection (:488) /
/// EliminateProjection (:489) prepare projection clusters while
/// RuleMergeAdjacentWindow sits under OperandWindow (:491-492)
/// (:484-505).
#[test]
#[ignore = "go-parity-gap: transformation_rules_suite goldens over live exploration; unported"]
fn merge_adjacent_window_goldens() {
    // Restore: batch(:486-495) through testGroupToString.
}

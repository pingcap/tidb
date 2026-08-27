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

//! Documentary gap ports for `pkg/planner/core/casetest/rule`
//! (`pkg/planner.part9` items 482-508 on `origin/master`): `dual_test.go`,
//! `rule_cdc_join_reorder_test.go`, `rule_common_handle_ordering_test.go`,
//! `rule_common_handle_range_test.go`, `rule_correlate_test.go`,
//! `rule_derive_topn_from_window_test.go`,
//! `rule_eliminate_empty_selection_test.go`,
//! `rule_eliminate_projection_test.go`,
//! `rule_inject_extra_projection_test.go`, `rule_join_reorder_test.go`,
//! `rule_outer2inner_test.go`, `rule_outer_to_semi_join_test.go`,
//! `rule_predicate_pushdown_test.go`, `rule_predicate_simplification_test.go`.
//!
//! Every one of these tests drives parsed SQL through `testkit` sessions
//! (several through `RunTestUnderCascades[_WithDomain]`, which runs each case
//! twice: cascades engine off/on), compares `EXPLAIN FORMAT='plan_tree'` /
//! `'brief'` goldens loaded from BookKeeper suites
//! (`main_test.go:29-58` registers outer2inner, derive_topn_from_window,
//! join_reorder_suite, predicate_pushdown_suite, predicate_simplification,
//! outer_to_semi_join_suite, correlate_suite, cdc_join_reorder_suite and
//! order_aware_join_reorder_suite books), executes result checks against real
//! mock-store data, and toggles session variables such as
//! `tidb_opt_enable_alternative_logical_plans`,
//! `tidb_opt_enable_advanced_join_reorder`,
//! `tidb_opt_join_reorder_threshold`, `tidb_opt_hash_join_cost_factor` and
//! `tidb_enable_parallel_apply`. The Rust workspace has no SQL-to-plan
//! pipeline, session/executor testkit, TiFlash replica metadata, hint/warning
//! plumbing, or plan-tree explain renderer yet; these are recorded gaps, not
//! approximations.
//!
//! | Go function (`pkg/planner/core/casetest/rule`) | Rust test |
//! | --- | --- |
//! | `dual_test.go:23 TestDual` | [`dual_null_comparisons_fold_to_table_dual`] |
//! | `rule_cdc_join_reorder_test.go:122 TestCDCJoinReorder` | [`cdc_join_reorder_result_matches_pre_cdc_baseline`] |
//! | `rule_cdc_join_reorder_test.go:178 TestJoinReorderPushSelection` | [`join_reorder_push_selection_plans_with_sel_on_top`] |
//! | `rule_cdc_join_reorder_test.go:244 TestDPJoinReorder` | [`dp_join_reorder_result_matches_greedy_baseline`] |
//! | `rule_cdc_join_reorder_test.go:299 TestOrderAwareJoinReorderPushSelection` | [`order_aware_join_reorder_push_selection_keeps_hint_applicable`] |
//! | `rule_cdc_join_reorder_test.go:350 TestOrderAwareJoinReorderAlternativeRound` | [`order_aware_join_reorder_alternative_round_smoke`] |
//! | `rule_cdc_join_reorder_test.go:393 TestDPJoinReorderLeadingHint` | [`dp_join_reorder_leading_hint_warns_inapplicable_for_dp`] |
//! | `rule_common_handle_ordering_test.go:38 TestCommonHandleIndexOrdering` | [`common_handle_index_ordering_avoids_topn_for_pk_suffix_order`] |
//! | `rule_common_handle_range_test.go:29 TestCommonHandleIndexRanges` | [`common_handle_index_ranges_append_clustered_pk_suffix`] |
//! | `rule_common_handle_range_test.go:273 TestCommonHandleIndexRangesWithTupleCompare` | [`common_handle_index_ranges_tuple_compare_spans_appended_handle`] |
//! | `rule_correlate_test.go:29 TestCorrelateNullSemantics` | [`correlate_solver_preserves_three_valued_null_semantics`] |
//! | `rule_correlate_test.go:65 TestCorrelateAlternativeChoosesApply` | [`correlate_alternative_round_prefers_apply_over_index_join`] |
//! | `rule_correlate_test.go:97 TestCorrelatedInApplyEliminatesDistinct` | [`correlated_in_apply_eliminates_inner_distinct_aggregation`] |
//! | `rule_correlate_test.go:154 TestCorrelate` | [`correlate_suite_plan_and_result_golden`] |
//! | `rule_correlate_test.go:226 TestCorrelateParallelApply` | [`correlate_parallel_apply_runs_concurrent_and_matches_serial`] |
//! | `rule_correlate_test.go:285 TestCorrelateWithCostFactors` | [`correlate_cost_factors_let_apply_win_over_penalized_joins`] |
//! | `rule_derive_topn_from_window_test.go:30 TestDerivedTopNSuite` | [`derived_topn_suite_tiflash_pushdown_and_rand_topn_shape`] |
//! | `rule_eliminate_empty_selection_test.go:23 TestEmptySelectionEliminator` | [`empty_selection_eliminator_multi_join_plan_golden`] |
//! | `rule_eliminate_projection_test.go:27 TestEliminateProjectionSuite` | [`eliminate_projection_suite_apply_and_expression_index`] |
//! | `rule_inject_extra_projection_test.go:31 TestWrapCastForAggFuncs` | see `rule_inject_extra_projection_wrap_cast_source.rs` (real port) |
//! | `rule_join_reorder_test.go:48 TestJoinReorderSuite` | [`join_reorder_suite_plain_and_domain_subtests`] |
//! | `rule_outer2inner_test.go:24 TestOuter2InnerSuite` | [`outer2inner_suite_null_reject_conditions_and_golden_book`] |
//! | `rule_outer_to_semi_join_test.go:24 TestOuterToSemiJoin` | [`outer_to_semi_join_suite_plan_and_results`] |
//! | `rule_outer_to_semi_join_test.go:91 TestSemiJoinRewrite` | [`semi_join_rewrite_hint_enables_index_hash_join_delete`] |
//! | `rule_predicate_pushdown_test.go:80 TestPredicatePushdownSuite` | [`predicate_pushdown_suite_constant_propagation_and_pushdown`] |
//! | `rule_predicate_simplification_test.go:24 TestPredicateSimplification` | [`predicate_simplification_suite_plan_cache_aware_golden`] |

/// GO PORT of `pkg/planner/core/casetest/rule/dual_test.go:23 TestDual`.
///
/// Re-derived contract: over `t(id PK auto_increment, d INT)` inside a live
/// cascades session, every contradictory WHERE becomes a root TableDual:
/// `select a from (select d as a from t where d=0) k where k.a=5` explains to
/// exactly `TableDual root  rows:0` (subquery alias both sides) and returns
/// zero rows; `(select 1+2 as a ...) k where k.a=5` keeps a
/// Projection(3->Column) above TableDual; and `d !=/>/>=/</<=/= null` — six
/// comparisons against NULL that are never TRUE — all fold to
/// `TableDual root  rows:0` (dual_test.go:27-52). Needs SQL parsing +
/// constant-folding predicate handling + plan rendering.
#[test]
#[ignore = "go-parity-gap: needs the SQL optimize pipeline plus plan_tree explain rendering of TableDual folding"]
fn dual_null_comparisons_fold_to_table_dual() {}

/// GO PORT of
/// `pkg/planner/core/casetest/rule/rule_cdc_join_reorder_test.go:122
/// TestCDCJoinReorder`.
///
/// Five empty-schema tables t1..t5 with overlapping key values plus ANALYZE;
/// loads the `cdc_join_reorder_suite` book under cascades ON/OFF. Phase 1
/// collects ground-truth results with the old reorder algorithm before any
/// CD-C state; phase 2 replays every input checking the recorded
/// `EXPLAIN FORMAT='plan_tree'` golden and requires the CD-C answers to equal
/// the pre-enablement baseline per case (expectedResults :155, require.Equalf :173).
#[test]
#[ignore = "go-parity-gap: CD-C join reorder planning, analyze'd mock tables and plan_tree goldens all need the unported session/optimize stack"]
fn cdc_join_reorder_result_matches_pre_cdc_baseline() {}

/// GO PORT of
/// `pkg/planner/core/casetest/rule/rule_cdc_join_reorder_test.go:178
/// TestJoinReorderPushSelection`.
///
/// `set @@tidb_opt_join_reorder_through_sel = 1` then five PRIMARY KEY tables
/// joined pairwise; per book entry SET statements execute directly and every
/// other input's explain output is compared positionally, requiring input and
/// output counts to agree exactly (`:210-235`). Pins plans when join reorder
/// must look through Selection nodes.
#[test]
#[ignore = "go-parity-gap: through-selection join reorder decisions run inside the unported optimizer"]
fn join_reorder_push_selection_plans_with_sel_on_top() {}

/// GO PORT of
/// `pkg/planner/core/casetest/rule/rule_cdc_join_reorder_test.go:244
/// TestDPJoinReorder`.
///
/// Same five-table fixture; enables
/// `tidb_opt_enable_advanced_join_reorder=1`. With threshold 0 every ≤5-table
/// group takes greedy; with threshold 10 DP handles them. Each case's
/// plan_tree golden is checked and the DP result MUST equal the greedy
/// greedy baseline collected at :277-280 and compared afterwards (`:294-301`).
#[test]
#[ignore = "go-parity-gap: DP/greedy join reorder algorithms and their cross-validation need live optimization"]
fn dp_join_reorder_result_matches_greedy_baseline() {}

/// GO PORT of
/// `pkg/planner/core/casetest/rule/rule_cdc_join_reorder_test.go:299
/// TestOrderAwareJoinReorderPushSelection`.
///
/// Builds t6..t9 (8000/6000/7000/9000 analyzed rows with hot/cold category
/// skew, helper `prepareOrderAwareJoinReorderTables` :27-72); per case, SET; per case, SET
/// inputs execute while query plans are gold-checked AND `show warnings` for
/// every explained query must NOT contain "leading hint is inapplicable"
/// (`:344`), pinning that order-aware reorder keeps leading hints applicable
/// even with selections on top.
#[test]
#[ignore = "go-parity-gap: large analyzed fixtures + warning-stream assertions live behind the unported hint pipeline"]
fn order_aware_join_reorder_push_selection_keeps_hint_applicable() {}

/// GO PORT of
/// `pkg/planner/core/casetest/rule/rule_cdc_join_reorder_test.go:350
/// TestOrderAwareJoinReorderAlternativeRound`.
///
/// Loads alternative-round tables oa_order_t1..t4, obj, relationship
/// (:74-131, recursive-CTE bulk inserts). Every input either executes (SET)
/// or is planned+explained and recorded (`:369-385`); the suite's own Check
/// step means a passing run proves each shape PLANS without error across the
/// alternative-round replay.
#[test]
#[ignore = "go-parity-gap: the alternative logical-plan round is not implemented in the Rust planner"]
fn order_aware_join_reorder_alternative_round_smoke() {}

/// GO PORT of
/// `pkg/planner/core/casetest/rule/rule_cdc_join_reorder_test.go:393
/// TestDPJoinReorderLeadingHint`.
///
/// Advanced join reorder ON with threshold 10; `SELECT /*+ LEADING(t2, t3) */
/// * FROM t1 JOIN t2 ... JOIN t3 ...` then scans `show warnings` for
/// exactly "leading hint is inapplicable for the DP join reorder algorithm"
/// and requires it found (`:403-415`).
#[test]
#[ignore = "go-parity-gap: leading-hint inapplicability warnings originate in the unported DP reorder path"]
fn dp_join_reorder_leading_hint_warns_inapplicable_for_dp() {}

/// GO PORT of
/// `pkg/planner/core/casetest/rule/rule_common_handle_ordering_test.go:38
/// TestCommonHandleIndexOrdering`.
///
/// Clustered-PK(a1 varchar, a2 int) table with ic(d)/ic_overlap(d,a1)/
/// ic_multi(b,d)/uic(c UNIQUE) plus a prefixed PK(p1(2),p2) table and a
/// single-varchar-PK table; eight cases assert keep order:true / absence of
/// TopN-Sort for ORDER BY on the full clustered handle when the chosen
/// secondary index physically ends with it (e.g. cases 1-3 :63-118),
/// uniqueness/prefix truncation preventing keep order (case 4/7 :120-161), DESC
/// reverse-scan ordering (case 5 :163-180), mixed ASC/DESC falling back to a
/// sort (case 6 :182-194), and exact sorted result rows alongside each
/// explain probe.
#[test]
#[ignore = "go-parity-gap: index keep-order costing over appended common-handle columns needs live findBestTask + explain text"]
fn common_handle_index_ordering_avoids_topn_for_pk_suffix_order() {}

/// GO PORT of
/// `pkg/planner/core/casetest/rule/rule_common_handle_range_test.go:29
/// TestCommonHandleIndexRanges`.
///
/// Clustered PK(tenant,seq) table with ia(a)/ia_overlap(a,tenant)/ub(b
/// unique); predicates on clustered PK columns must extend non-unique
/// secondary-index ranges with the appended full handle: point range
/// `[10 2,10 2]` (:60), open range `(10 1 100,10 1 +inf]` (:72), non-point
/// `(10 1,10 +inf]` (:83), IN-list ranges `[20 2,20 2]` (:94), staying at the
/// declared column when an earlier PK col is unbound (:105), overlap index
/// without re-appending tenant (:117+) — each paired with exact result rows
/// and decimal variants at the tail (:240-266).
#[test]
#[ignore = "go-parity-gap: ranger range-extension over appended handle columns is evaluated during unported access-path building"]
fn common_handle_index_ranges_append_clustered_pk_suffix() {}

/// GO PORT of
/// `pkg/planner/core/casetest/rule/rule_common_handle_range_test.go:273
/// TestCommonHandleIndexRangesWithTupleCompare`.
///
/// Issue #70532 regression: tuple comparison `(a,b,c) > (1,2,3)` on a table
/// with KEY ia(a) and clustered PK(b,c) must DNF-expand into
/// `range:(1 2 3,1 2 +inf], (1 2,1 +inf], (1,+inf]` — reaching INTO the
/// appended handle with matching prefix lengths; a three-column handle
/// reaches one further (`(a,b,c,d) > (1,2,3,4)`, :307-315), each verified by
/// exact explain substring plus ordered result rows.
#[test]
#[ignore = "go-parity-gap: tuple-comparison range derivation lives in the ranger pass behind unported planning"]
fn common_handle_index_ranges_tuple_compare_spans_appended_handle() {}

/// GO PORT of
/// `pkg/planner/core/casetest/rule/rule_correlate_test.go:29
/// TestCorrelateNullSemantics`.
///
/// With `tidb_opt_enable_alternative_logical_plans=ON`: scalar IN over a
/// nullable subquery must return NULL, never 0 — non-null outer/null inner
/// (`tn.a in (select sn.a...)` -> `<nil>`, :41), null outer/non-null inner
/// (:48), and NOT NULL pairs answering 1/1/0 for (1,2,3) IN {1,2} (:57).
/// Guards CorrelateSolver from breaking 3-valued logic of the restored Apply.
#[test]
#[ignore = "go-parity-gap: scalar-IN execution semantics need the executor stack"]
fn correlate_solver_preserves_three_valued_null_semantics() {}

/// GO PORT of
/// `pkg/planner/core/casetest/rule/rule_correlate_test.go:65
/// TestCorrelateAlternativeChoosesApply`.
///
/// Over t1/t2(keyed) with `where b=1 and a in (select a from t2)`: OFF mode
/// decorrelates to IndexJoin (+StreamAgg); ON mode must choose the cheaper
/// Apply+Limit from the correlate round (:86-95) — asserted by scanning
/// `explain format='brief'` operator column for IndexJoin vs Apply — and the
/// answer `1 1` must be identical in both modes (:97-101).
#[test]
#[ignore = "go-parity-gap: alternative-plan cost comparison between Apply and IndexJoin happens in the unported optimizer"]
fn correlate_alternative_round_prefers_apply_over_index_join() {}

/// GO PORT of
/// `pkg/planner/core/casetest/rule/rule_correlate_test.go:97
/// TestCorrelatedInApplyEliminatesDistinct`.
///
/// Three-table contractors/members/employments fixture; blacklists the
/// aggregation_eliminate rule (`insert into mysql.opt_rule_blacklist ...
/// admin reload`, :136-138) and asserts the Apply plan still shows the inner
/// distinct agg (`group by:` + `firstrow(` in operator info, via the
/// explainHasDistinctLikeAgg helper at :211-219);
/// removes the blacklist again and asserts the correlated-IN apply eliminated
/// the inner distinct agg, with count(1)=2 as the result (:144-153).
#[test]
#[ignore = "go-parity-gap: opt_rule_blacklist reload, apply distinct-elimination and explain info columns are unported"]
fn correlated_in_apply_eliminates_inner_distinct_aggregation() {}

/// GO PORT of `pkg/planner/core/casetest/rule/rule_correlate_test.go:154
/// TestCorrelate`.
///
/// Correlate golden book: three keyed two-column tables filled 3/2/2 rows;
/// `tidb_opt_enable_alternative_logical_plans=ON`; every correlate-suite SQL
/// is checked against its recorded `explain format='brief'` plan AND executed
/// results (`:181-188`) under both cascades modes.
#[test]
#[ignore = "go-parity-gap: correlate suite plans/results need session + optimizer + executor"]
fn correlate_suite_plan_and_result_golden() {}

/// GO PORT of
/// `pkg/planner/core/casetest/rule/rule_correlate_test.go:226
/// TestCorrelateParallelApply`.
///
/// Correlate alternative + `tidb_enable_parallel_apply=ON` +
/// concurrency 5: plan shows Apply, EXPLAIN ANALYZE reports
/// `Concurrency:` > 1 for it (:252-267), and parallel+correlate rows equal
/// the serial/no-correlate run of the same query (:269-281).
#[test]
#[ignore = "go-parity-gap: parallel-apply executor concurrency reporting is unported"]
fn correlate_parallel_apply_runs_concurrent_and_matches_serial() {}

/// GO PORT of
/// `pkg/planner/core/casetest/rule/rule_correlate_test.go:285
/// TestCorrelateWithCostFactors`.
///
/// Correlate suite replay with `tidb_opt_hash_join_cost_factor=1000` and
/// `tidb_opt_merge_join_cost_factor=1000` set (:298-300) so penalized
/// hash/merge joins lose to the correlate alternative; per case both brief
/// plan and results match the recorded book entries (:308-315).
#[test]
#[ignore = "go-parity-gap: cost-factor session variables feed the unported cost model during live planning"]
fn correlate_cost_factors_let_apply_win_over_penalized_joins() {}

/// GO PORT of
/// `pkg/planner/core/casetest/rule/rule_derive_topn_from_window_test.go:30
/// TestDerivedTopNSuite`.
///
/// Sub-test `TestPushDerivedTopnFlash` (:41): with
/// tidb_opt_derive_topn=1/enforce_mpp=1 over TiFlash-replica'd t+t3 the
/// derive_topn_from_window book plans are pinned. Sub-test `TestTopNPushdown`
/// (:66): with the flag off, `select rand() ... order by limit 10` rows must
/// be monotonic non-decreasing AND the plan stays root-TopN over
/// Projection(rand()) over TableReader/TableFullScan keep order:false,
/// stats:pseudo (:72-79) — deriving TopN from window must not fire here.
#[test]
#[ignore = "go-parity-gap: TiFlash replica injection, MPP planning and rand() execution are all outside the crate"]
fn derived_topn_suite_tiflash_pushdown_and_rand_topn_shape() {}

/// GO PORT of
/// `pkg/planner/core/casetest/rule/rule_eliminate_empty_selection_test.go:23
/// TestEmptySelectionEliminator`.
///
/// Five generated A/F/G/J/L wide tables; two fully expanded multi-join
/// queries explain to exact plan trees whose Selections survive elimination —
/// HAVING conditions become root Selections above joins with Point_Get builds
/// (:33-77), and an aggregate-with-HAVING right-outer-join tree lands
/// Point_Get(handle:41) below StreamAgg (:78-90). Pins the rule keeps
/// non-empty Selections where the HAVING clause moved them.
#[test]
#[ignore = "go-parity-gap: plan_tree golden comparison over the multi-join optimized tree needs the full rule pipeline"]
fn empty_selection_eliminator_multi_join_plan_golden() {}

/// GO PORT of
/// `pkg/planner/core/casetest/rule/rule_eliminate_projection_test.go:27
/// TestEliminateProjectionSuite`.
///
/// Sub-test `testWithApply` (:35-68): CTE-templated scalar-subquery lookups
/// must return '1' and keep an Apply node in the EXPLAIN tree (projection
/// elimination must not erase the Apply boundary) for both ordered and
/// unordered CTE bodies. Sub-test `testElinimateProjectionWithExpressionIndex`
/// (:70-115): expression-index coalesce keys over USING-joined tables remain
/// stable across 20 repeated runs returning zero rows.
#[test]
#[ignore = "go-parity-gap: CTE + scalar-subquery Apply planning and expression-index indexes are unported surfaces"]
fn eliminate_projection_suite_apply_and_expression_index() {}

/// GO PORT of
/// `pkg/planner/core/casetest/rule/rule_join_reorder_test.go:48
/// TestJoinReorderSuite`.
///
/// Multi-part suite over the join_reorder_suite book. PlainCases run under
/// plain cascades: TestOptEnableHashJoin honors
/// `tidb_opt_enable_hash_join=off` (:53-62), TestJoinOrderHint4DynamicPartitionTable
/// force-prunes partitions dynamically via failpoint (:64-80),
/// TestLeadingHintInapplicableKeepsOtherConds keeps other conditions after an
/// inapplicable leading hint (:81-97), TestLeadingHintWithNonEqJoinUnderOuterJoin
/// applies leading hints around non-equi OR-conditions with distinct plans for
/// hint orders (:98-129), TestOuterJoinReorderNullExtendedNonEqSafety
/// (:129-158); DomainCases re-run partitioned variants under domain control
/// (:160-215). All compare exact plan_tree outputs + warnings.
#[test]
#[ignore = "go-parity-gap: hint-aware join reorder + dynamic partition pruning need the full planner runtime"]
fn join_reorder_suite_plain_and_domain_subtests() {}

/// GO PORT of
/// `pkg/planner/core/casetest/rule/rule_outer2inner_test.go:24
/// TestOuter2InnerSuite`.
///
/// Book-driven outer-to-inner conversion plans plus hand-written probes:
/// ti.Inlj over left-join with OR'd null-filtering predicate keeps IndexJoin
/// (:75-79), structural null-reject proofs (`length(trim(cast(...)))>0` turns
/// LEFT JOIN inner, `coalesce(t2.k,1)>0` stays outer, `1 in (t2.k,...)`
/// semantics at :81-100), issue #66825/#58793/#66833 partial-null-comparison
/// cases keeping `left outer join` intact (:102-146), constant-propagation
/// smoke tests (:150-155), and the tail regression probes reproduced in
/// issues #65166/#58836 windows-over-dual plans (:156-198).
#[test]
#[ignore = "go-parity-gap: null-reject analysis over arbitrary expressions runs inside the unported optimizer"]
fn outer2inner_suite_null_reject_conditions_and_golden_book() {}

/// GO PORT of
/// `pkg/planner/core/casetest/rule/rule_outer_to_semi_join_test.go:24
/// TestOuterToSemiJoin`.
///
/// A/B nullable-pair tables plus t1..t3 single-column NOT NULL tables drive
/// the outer_to_semi_join_suite book: each SQL gets plan + executed-result
/// goldens (:56-64); tail check for issue #68112 reproduces anti-semi-join
/// (`t_inner.id is null` union-all branch) returning `<nil>,<nil>,z` (:67-78).
#[test]
#[ignore = "go-parity-gap: outer→semi-join rewriting plus result execution need the live stack"]
fn outer_to_semi_join_suite_plan_and_results() {}

/// GO PORT of
/// `pkg/planner/core/casetest/rule/rule_outer_to_semi_join_test.go:91
/// TestSemiJoinRewrite`.
///
/// Delete-through-semi-join: `delete from t1 where t1.id in (select
/// /*+ semi_join_rewrite() */ cast(id as char) from t2 where k=1)` must
/// exhibit IndexHashJoin (MustHavePlan, :104) and delete exactly id='1'
/// (:105); re-inserting plus forcing
/// `tidb_opt_enable_semi_join_rewrite=off` while
/// `tidb_opt_enable_alternative_logical_plans=on` STILL yields IndexHashJoin
/// via the alternative round (:109-111).
#[test]
#[ignore = "go-parity-gap: semi_join_rewrite rewrite path is exercised through DELETE plans of the unported optimizer"]
fn semi_join_rewrite_hint_enables_index_hash_join_delete() {}

/// GO PORT of
/// `pkg/planner/core/casetest/rule/rule_predicate_pushdown_test.go:80
/// TestPredicatePushdownSuite`.
///
/// Two book sub-tests. testConstantPropagateWithCollation (:85-105):
/// collation-mixed schemas t/foo/bar/t0..t4/t1_65994/t2_65994 with seeded
/// rows; entries record plan + warnings + RESULTS (executed).
/// testPredicatePushDown (:106+): TiFlash-replica'd crm_rd_150m driving
/// month() pushdown under isolation engines='tiflash' plus further pages of
/// the predicate_pushdown_suite book, comparing plan_tree and warnings for
/// every entry.
#[test]
#[ignore = "go-parity-gap: predicate-pushdown planning over collations/TiFlash isolation engines is unported"]
fn predicate_pushdown_suite_constant_propagation_and_pushdown() {}

/// GO PORT of
/// `pkg/planner/core/casetest/rule/rule_predicate_simplification_test.go:24
/// TestPredicateSimplification`.
///
/// Eleven varied tables (gbk/text/json/expression-index/columnar shapes)
/// with global+session fix-control 44830:ON and non-prepared plan cache ON
/// (:55-70). Every suite entry runs the raw query, records plan_tree,
/// warnings AND whether `@@last_plan_from_cache` flipped (:144-162) — pinning
/// simplification behavior across cached and freshly built plans.
#[test]
#[ignore = "go-parity-gap: plan-cache interaction with predicate simplification is outside the crate today"]
fn predicate_simplification_suite_plan_cache_aware_golden() {}

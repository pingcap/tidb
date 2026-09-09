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

//! Port ledger for `pkg/planner/core/logical_plans_test.go`
//! (`pkg/planner.part12` items 663-706 on `origin/master`, plus item 707 =
//! `pkg/planner/core/main_test.go:30 TestMain`).
//!
//! ALL but one of these tests are DRIVER tests: they parse a statement,
//! wrap it in `resolve.NewNodeW`, run `Preprocess(...)` +
//! `BuildLogicalPlanForTest(ctx, s.GetSCtx(), nodeW, s.GetIS())` or
//! `NewPlanBuilder().Init(sctx, is, hint.NewQBHintHandler(nil)).Build(...)`,
//! then push the result through `logicalOptimize(ctx, flags, p)` /
//! `physicalOptimize` / `TryFastPlan` and compare `ToString(p)` renders,
//! expression stringifications, schema column lists, key info, visit-info
//! privilege records, or access-path candidate lists against goldens loaded
//! from `pkg/planner/core/testdata/plan_suite_unexported.json`
//! (`main_test.go:30-38` registers that BookKeeper suite plus
//! runtime_filter_generator_suite, fts_resolve_index_suite and
//! explain_analyze_ru_suite). The Rust crate owns transcribed operator state
//! and individual rule arithmetic but has no statement pipeline (no
//! Preprocess/name-resolution walk, no `PlanBuilder.Build`, no rule driver,
//! no session/StmtCtx, no plan-tree text renderer), so those surfaces cannot
//! be exercised honestly yet; each is recorded below as an `#[ignore]` gap
//! port whose comment re-derives the pinned contract from the Go source.
//! Item 707 (`main_test.go TestMain`) is bootstrap-only (golden book loading +
//! goleak wrapper): skipped-reason, no Rust twin by design.
//!
//! ONE test is directly portable — `TestAllocID` (:761) — because its whole
//! contract (statement-unique plan ids handed out by one session counter) has
//! an explicit Rust carrier, [`tidb_planner::plan_base::PlanIdAllocator`],
//! used by `BaseLogicalPlan::new`.

use tidb_planner::logical::{data_source::DataSource, BaseLogicalPlan};
use tidb_planner::plan_base::PlanIdAllocator;

/// GO PORT of `pkg/planner/core/logical_plans_test.go:761 TestAllocID`.
///
/// Go builds two DataSources against ONE mock session
/// (`coretestsdk.MockContext()`, :762) with
/// `pA := logicalop.DataSource{}.Init(ctx, 0)` and `pB := ...` (:767-768),
/// asserting `require.Equal(t, pB.ID(), pA.ID()+1)` (:769). `Init` draws each
/// id from `ctx.GetSessionVars().PlanID.Add(1)`, so the pin is: the allocator
/// is shared across operators, monotonic, and consecutive (no gaps between two
/// fresh inits). Rust keeps the same contract explicitly: one
/// [`PlanIdAllocator`] stands in for the session counter (its doc cites the
/// identical Go call) and `DataSource::new` over `BaseLogicalPlan::new`
/// mirrors `DataSource{}.Init`.
#[test]
fn alloc_id_two_inits_get_consecutive_ids_from_one_allocator() {
    let allocator = PlanIdAllocator::new();
    let p_a = DataSource::new(BaseLogicalPlan::new(&allocator, DataSource::TYPE, 0), 48, "t");
    let p_b = DataSource::new(BaseLogicalPlan::new(&allocator, DataSource::TYPE, 0), 48, "t");
    assert_eq!(p_b.base.base.id(), p_a.base.base.id() + 1);
}

/// GO PORT of `pkg/planner/core/logical_plans_test.go:51 TestPredicatePushDown`.
///
/// Golden-driven sweep over `planSuiteUnexportedData` input SQL; every
/// statement is optimized with
/// `FlagPredicatePushDown|FlagDecorrelate|FlagPruneColumns|
/// FlagPruneColumnsAgain|FlagPredicateSimplification` (:64-67) and the whole
/// resulting tree must render exactly like the recorded `ToString(p)` line.
#[test]
#[ignore = "go-parity-gap: needs ParseOneStmt->BuildLogicalPlanForTest->logicalOptimize->ToString over golden data"]
fn predicate_push_down_golden_plans() {}

/// GO PORT of
/// `pkg/planner/core/logical_plans_test.go:74 TestImplicitCastNotNullFlag`.
///
/// Issue #31399: `select count(*) from t3 group by a having bit_and(b) > 1`
/// must keep the implicit cast above `bit_and(b)` NOT NULL-flag-free after
/// PPD/prune optimization — `(AggFuncs[1].Args[0].GetType().GetFlag() &
/// mysql.NotNullFlag) == 0` through Projection>Selection>Aggregation
/// unwrapping (:84-90).
#[test]
#[ignore = "go-parity-gap: requires full select building over schema'd mock tables plus type-flag preservation end-to-end"]
fn implicit_cast_not_null_flag_stays_clear_on_agg_arg() {}

/// GO PORT of
/// `pkg/planner/core/logical_plans_test.go:93 TestEliminateProjectionUnderUnion`.
///
/// `SELECT a FROM t3 JOIN ((SELECT 127 AS IDD FROM t3) UNION ALL (SELECT 1 AS
/// IDD FROM t3)) u ON t3.b = u.IDD`: after constant folding + projection
/// elimination the surviving innermost projection's schema column null-flag
/// and its expression's evaluated null-flag must AGREE (:104-108) — union/all
/// children must not lose or invent NOT NULL.
#[test]
#[ignore = "go-parity-gap: union-all child planning plus projection elimination need the unported optimizer pipeline"]
fn eliminate_projection_under_union_keeps_null_flags_aligned() {}

/// GO PORT of
/// `pkg/planner/core/logical_plans_test.go:112 TestJoinPredicatePushDown`.
///
/// For every golden query the optimized tree must be
/// Projection>Join>[DataSource,DataSource], and the join's LEFT/RIGHT
/// `PushedDownConds` stringified lists must equal the recorded pair
/// (:136-146) — pins which predicates each side of every inner/outer join
/// absorbs.
#[test]
#[ignore = "go-parity-gap: needs join predicate pushdown over built plans and PushedDownConds rendering"]
fn join_predicate_push_down_left_right_conds_golden() {}

/// GO PORT of
/// `pkg/planner/core/logical_plans_test.go:153 TestOuterWherePredicatePushDown`.
///
/// Outer-join WHERE handling: the surviving Selection above the join carries
/// the residual condition while left/right pushed-down cond lists match the
/// golden triple {Sel,Left,Right} (:177-198) for each outer-join query.
#[test]
#[ignore = "go-parity-gap: outer-join WHERE split runs inside the unported predicate-pushdown rule driver"]
fn outer_where_predicate_push_down_sel_and_sides_golden() {}

/// GO PORT of
/// `pkg/planner/core/logical_plans_test.go:202 TestSimplifyOuterJoin`.
///
/// Per golden query: full plan render (:216-223) AND the located
/// `LogicalJoin.JoinType.String()` (found within the first two levels,
/// :224-232) — e.g. left/right/inner decisions after outer-join
/// simplification under PPD+prune flags only.
#[test]
#[ignore = "go-parity-gap: needs the simplify-outer-join rule driven by the unported optimizer"]
fn simplify_outer_join_shape_and_join_type_golden() {}

/// GO PORT of
/// `pkg/planner/core/logical_plans_test.go:241 TestAntiSemiJoinConstFalse`.
///
/// `not exists (... where t1.a=t2.a and t2.b=1 and t2.b=2)` collapses the
/// contradictory filter so the plan becomes exactly
/// `Join{DataScan(t1)->Dual}(test.t.a,test.t.a)->Projection` with JoinType
/// string "anti semi join" (:246-266) — contradiction folding turns the inner
/// side into Dual while decorrelation keeps the equi-key pair.
#[test]
#[ignore = "go-parity-gap: anti-semi conversion + constant-false folding live behind the unported builder/decorrelator"]
fn anti_semi_join_const_false_folds_inner_to_dual() {}

/// GO PORT of
/// `pkg/planner/core/logical_plans_test.go:272 TestDeriveNotNullConds`.
///
/// Golden triple {Plan,Left,Right}: derived IS-NOT-NULL conditions on outer
/// columns must appear among each side's pushed-down conds alongside the
/// rendered plan (:291-310).
#[test]
#[ignore = "go-parity-gap: not-null derivation is a pushdown-rule side effect the Rust pipeline cannot reach yet"]
fn derive_not_null_conditions_golden() {}

/// GO PORT of
/// `pkg/planner/core/logical_plans_test.go:313 TestExtraPKNotNullFlag`.
///
/// `select count(*) from t3`: the implicitly appended extra handle column
/// `_tidb_rowid` (Columns[2]) must carry `mysql.PriKeyFlag|mysql.NotNullFlag`,
/// mirrored by the schema column's RetType flag (:327-341) — the extra PK
/// column stays typed as a not-null key end to end.
#[test]
#[ignore = "go-parity-gap: extra-handle-column synthesis happens inside unported table-scan building"]
fn extra_pk_not_null_flag_on_appended_handle_column() {}

/// GO PORT of
/// `pkg/planner/core/logical_plans_test.go:351 TestGroupByWhenNotExistCols`.
///
/// Under ONLY_FULL_GROUP_BY (session mode toggled in helper
/// `buildLogicPlan4GroupBy` :373-395, mocking the signed table), seven SELECT
/// shapes (alias, table alias, length(a+b) wrappers...) grouping by `b` all
/// fail with `contains nonaggregated column 'test.<qualifier>.a'` matched by
/// regexp (:356-394).
#[test]
#[ignore = "go-parity-gap: ONLY_FULL_GROUP_BY validation needs the unported group-by checker over built aggregates"]
fn group_by_when_not_exist_cols_reports_nonaggregated_column() {}

/// GO PORT of
/// `pkg/planner/core/logical_plans_test.go:402 TestDupRandJoinCondsPushDown`.
///
/// Three pinned shape checks: duplicated `t1.a > rand()` ON conditions BOTH
/// land in OtherConditions unevaluated (:430-433, exact render
/// `[gt(cast(test.t.a, double BINARY), rand()) gt(...)]`); correlated
/// `where t1.a = rand()` splits into per-side projections feeding one EQ
/// other-cond whose args point INTO those projections' last columns
/// (:449-462); and a LEFT JOIN with user-var `@var1` pushes the less-than as
/// an OTHER condition on the preserved side with empty RightConditions
/// (:464-474).
#[test]
#[ignore = "go-parity-gap: nondeterministic-function pushdown policy belongs to the unported expression rewrite over joins"]
fn dup_rand_join_conds_push_down_shapes() {}

/// GO PORT of `pkg/planner/core/logical_plans_test.go:540 TestSubquery`.
///
/// Golden sweep (incl. Preprocess + BuildKeyInfo/Decorrelate/
/// SemiJoinRewrite flags :558-562) over subquery-bearing SQL: EXISTS/IN/
/// scalar forms must optimize into exactly the recorded trees.
#[test]
#[ignore = "go-parity-gap: subquery expansion and semi-join rewrite need the full build pipeline"]
fn subquery_optimized_plans_golden() {}

/// GO PORT of `pkg/planner/core/logical_plans_test.go:568 TestPlanBuilder`.
///
/// Broad builder sweep with cost-model v1 and hash-join concurrency 1 set on
/// the session (:575, :583), prune-columns-only flags; every input's final
/// `ToString` equals its recorded plan — a regression net over dozens of
/// unrelated builder paths (aggregation, distinct, order-by, limit...).
#[test]
#[ignore = "go-parity-gap: the PlanBuilder itself and its session variables are unported"]
fn plan_builder_outputs_golden() {}

/// GO PORT of
/// `pkg/planner/core/logical_plans_test.go:623 TestEagerAggregation`.
///
/// With `AllowAggPushDown=true` (session toggle :631-634) and
/// FlagPushDownAgg among the flags (:640), aggregate functions descend below
/// joins into the recorded positions.
#[test]
#[ignore = "go-parity-gap: aggregation pushdown rule + session var plumbing unported"]
fn eager_aggregation_pushdown_golden() {}

/// GO PORT of
/// `pkg/planner/core/logical_plans_test.go:706 TestProjectionEliminator`.
///
/// Two fixed shapes survive elimination exactly: `(select 1+a as num ...)`
/// stays `DataScan(t)->Projection` (:709-716), and the IN-subquery form folds
/// to `Join{DataScan(t)->Dual->Aggr(firstrow(test.t2.b))}
/// (test.t.a,test.t2.b)->Aggr(count(1))->Projection` (:717-725).
#[test]
#[ignore = "go-parity-gap: needs build+optimize+ToString over the unported pipeline"]
fn projection_eliminator_exact_survivors() {}

/// GO PORT of `pkg/planner/core/logical_plans_test.go:737 TestCS3389`.
///
/// Structural pin for `count(*) where a in (select b ...)`: root Projection
/// non-empty (:748-750), Projection>Aggregation>Join direct chaining with NO
/// intermediate projection between aggregation and join (:751-758) even with
/// JoinReOrder enabled.
#[test]
#[ignore = "go-parity-gap: tree-shape assertion needs the whole optimize stack to produce the tree"]
fn cs3389_no_projection_between_aggregation_and_join() {}

/// GO PORT of `pkg/planner/core/logical_plans_test.go:815 TestValidate`.
///
/// Forty validation rows: row expressions — `(1,2)` operands, wildcard
/// placement, ambiguous aliases/order-by ordinals, unknown HAVING columns,
/// invalid group-func nesting, index-exists checks, DML target resolution —
/// each either builds cleanly or fails with EXACTLY the listed terror
/// (`expression.ErrOperandColumns`, `plannererrors.ErrInvalidWildCard`,
/// `ErrAmbiguous`, `ErrUnknownColumn`, `ErrKeyDoesNotExist`,
/// `ErrInvalidGroupFuncUse`; cases :817-952, compared via terror.Equal
/// :964-973 through Preprocess+BuildLogicalPlanForTest).
#[test]
#[ignore = "go-parity-gap: expression/tableau validation and terror classification live in the unported validator/builder"]
fn validate_statement_errors_match_terrors() {}

/// GO PORT of
/// `pkg/planner/core/logical_plans_test.go:1092 TestVisitInfo`.
///
/// Privilege accounting for ~60 statements (INSERT/DELETE/multi-DELETE/
/// UPDATE/select-with-aggregation/TRUNCATE/DROP/CREATE/create-like/grant/
/// revoke incl. dynamic privileges such as CONNECTION_ADMIN, BACKUP_ADMIN,
/// PLACEMENT_ADMIN, ROLE_ADMIN, SYSTEM_VARIABLES_ADMIN; SHOW/SET/BACKUP/
/// RESTORE/rename/partition-DDL/flush variants :1097-1540): the collected
/// `visitInfo` list — sorted by (privilege,db,table,column) and de-duplicated
/// by `visitInfoArray.Less/Swap` + `unique` (:1558-1596) — must equal the
/// expected privilege tuples, with error identity compared loosely via
/// terror.ErrorEqual (:1580-1588).
#[test]
#[ignore = "go-parity-gap: visitInfo collection lives in PlanBuilder.build* paths and the access checker is unported"]
fn visit_info_privilege_matrix_matches() {}

/// GO PORT of `pkg/planner/core/logical_plans_test.go:1599 TestUnion`.
///
/// UNION queries build via PlanBuilder; failure EXPECTATION itself is part of
/// the golden record (`output[i].Err` :1616-1620); successful builds optimize
/// with the BUILDER's own optFlag (:1624-1626) and must match recorded
/// renders ��� pins UNION/CASE-branch layout incl. Distinct handling.
#[test]
#[ignore = "go-parity-gap: union planning and its error oracle need the unported builder"]
fn union_builder_best_plans_golden() {}

/// GO PORT of
/// `pkg/planner/core/logical_plans_test.go:1640 TestTopNPushDown`.
///
/// ORDER BY+LIMIT statements under the builder-optFlag pipeline must produce
/// the recorded TopN-positioned plans (global TopN vs per-child sort limits).
#[test]
#[ignore = "go-parity-gap: TopN pushdown pass runs inside the unported optimizer"]
fn topn_push_down_golden_plans() {}

/// GO PORT of
/// `pkg/planner/core/logical_plans_test.go:1675 TestNameResolver`.
///
/// Twenty-seven exact-error resolutions (both success and
/// `[planner:1054]Unknown column 'c3' in 'field list'`-style full error
/// STRINGS with code prefixes :1686-1713; covering field-list/group/having/
/// order/on-clause contexts, multi-delete updatable targets, VALUES()
/// pseudo-columns, window-over-group rejections like
/// `[planner:1056]Can't group on 'row_number() over ()'`).
#[test]
#[ignore = "go-parity-gap: name resolution (column/pattern binding with context-specific clause names) is unported"]
fn name_resolver_error_strings_match_exactly() {}

/// GO PORT of `pkg/planner/core/logical_plans_test.go:1765 TestSelectView`.
///
/// Views resolve transparently: `select * from v` and explicit column lists
/// over v plan as `DataScan(t)->Projection` (:1775-1786) — the view definition
/// is spliced in during build.
#[test]
#[ignore = "go-parity-gap: view expansion during plan building is unported"]
fn select_view_expands_to_base_scan() {}

/// GO PORT of
/// `pkg/planner/core/logical_plans_test.go:1805 TestWindowFunction`.
///
/// Window sweep with session vars `{tidb_window_windowing concurrency:1,
/// cost_model_version:1}` (:1814-1818): physical-optimizable windows
/// (`physicalOptimize` via the shared helper :1898-1927) must render recorded
/// logical trees OR fail with the recorded error string, AND the successful
/// ones must re-parse via `stmt.Restore` and re-optimize to the SAME plan
/// (:1875-1887) — an AST round-trip determinism guarantee.
#[test]
#[ignore = "go-parity-gap: window function planning + AST restore round-trip need the full builder/session"]
fn window_function_plans_and_restore_round_trip_golden() {}

/// GO PORT of
/// `pkg/planner/core/logical_plans_test.go:1828 TestWindowParallelFunction`.
///
/// Same corpus as TestWindowFunction but with
/// `tidb_window_concurrency=4` (:1837-1840): parallel-execution planning must
/// yield the same recorded outcomes, proving serial/parallel share logical
/// behavior.
#[test]
#[ignore = "go-parity-gap: parallelism session vars feed the unported window planner"]
fn window_parallel_function_matches_serial_golden() {}

/// GO PORT of
/// `pkg/planner/core/logical_plans_test.go:1990 TestSkylinePruning`.
///
/// Runs under failpoint `forceDynamicPrune=return(true)` (:1994-1997) with
/// dynamic prune mode set (:2113-2114): for each of 25 SQL probes against the
/// signed multi-index table t (and hash-partitioned pt2_global_index with
/// GLOBAL indexes), `skylinePruning(ds, byItemsToProperty(byItems))` — reached
/// through RecursiveDeriveStats and a Sort/Projection-aware descent
/// (:2119-2143) — must retain exactly the named candidate paths, e.g.
/// `PRIMARY_KEY,f,f_g` or `[b_global,g]` for index-merge partials (cases
/// :2007-2088; helper naming :1931-1962).
#[test]
#[ignore = "go-parity-gap: skyline pruning itself and access-path candidates/cost stats are unported here"]
fn skyline_pruning_candidate_paths_golden() {}

/// GO PORT of
/// `pkg/planner/core/logical_plans_test.go:2158 TestFastPlanContextTables`.
///
/// Point-shaped statements get FAST plans: `select * from t where a=1`,
/// `update t set f=0 where a=43215`, `delete from t where a=43215` each route
/// through TryFastPlan and register exactly one touched table `{DB:"test",
/// Table:"t"}` in StmtCtx.Tables (:2170-2186); the range probe `a>1` gets nil
/// plan and EMPTY registration (:2187-2190) — fast-path statistics
/// accounting boundary.
#[test]
#[ignore = "go-parity-gap: TryFastPlan and StmtCtx.Tables registration are unported"]
fn fast_plan_registers_context_tables_only_for_fast_paths() {}

/// GO PORT of
/// `pkg/planner/core/logical_plans_test.go:2204 TestUpdateEQCond`.
///
/// `select t1.a from t t1, t t2 where t1.a = t2.a+1` must become
/// `Join{DataScan(t1)->DataScan(t2)->Projection}
/// (test.t.a,Column#27)->Projection->Projection` (:2211-2217) — the unequal
/// `=` comparison forces an EXTRA projection on t2's side and the join's
/// equal-condition references its synthesized column (#27).
#[test]
#[ignore = "go-parity-gap: needs builder-side per-side projection synthesis driven by non-equi comparisons"]
fn update_eq_condition_adds_projection_side() {}

/// GO PORT of
/// `pkg/planner/core/logical_plans_test.go:2236 TestConflictedJoinTypeHints`.
///
/// `/*+ INL_JOIN(t1) HASH_JOIN(t1) */` on an inner-join query must leave the
/// join with `HintInfo == nil` AND `PreferJoinType == 0` (:2250-2258):
/// conflicting method hints are dropped entirely rather than half-applied.
#[test]
#[ignore = "go-parity-gap: hint parsing/preference resolution feeds the unported builder"]
fn conflicted_join_type_hints_are_dropped() {}

/// GO PORT of
/// `pkg/planner/core/logical_plans_test.go:2264
/// TestSimplyOuterJoinWithOnlyOuterExpr`.
///
/// A RIGHT JOIN whose only condition contains just the OUTER side wrapped in
/// CONCAT_WS must still classify as RightOuterJoin after simplification
/// (:2280-2286) — guards the regression where it degraded to InnerJoin.
#[test]
#[ignore = "go-parity-gap: outer-join simplification classification over built trees is unported"]
fn simply_outer_join_with_only_outer_expr_keeps_right_join() {}

/// GO PORT of
/// `pkg/planner/core/logical_plans_test.go:2292 TestResolvingCorrelatedAggregate`.
///
/// Six scalar-subqueries containing aggregates over the outer table must
/// become Apply{...}->Projection trees with the recorded shapes — including
/// nested `sum(count(a))` double aggregation (:2300-2318) and the merged
/// `sum(a), sum(a), count(a), (select count(a))` case reusing ONE
/// `Aggr(sum(test.t.a),count(test.t.a))` source (:2319-2325) — proving
/// correlated aggregates resolve to the OUTER block's aggregation.
#[test]
#[ignore = "go-parity-gap: correlated aggregate resolution spans builder + decorrelation rules"]
fn resolving_correlated_aggregate_apply_shapes_golden() {}

/// GO PORT of
/// `pkg/planner/core/logical_plans_test.go:2341 TestFastPathInvalidBatchPointGet`.
///
/// Issue #22040: malformed row tuples in IN clauses — `(a,b) in ((1,2),1)`
/// and `(a,b) in (1,2)` (same with indexed f,g) — must NEVER take the batch
/// point-get fast path; TryFastPlan returns nil in all four shapes
/// (:2352-2377).
#[test]
#[ignore = "go-parity-gap: batch point-get arity validation sits inside unported TryFastPlan"]
fn fast_path_rejects_invalid_batch_point_get_tuples() {}

/// GO PORT of
/// `pkg/planner/core/logical_plans_test.go:2386 TestWindowLogicalPlanAmbiguous`.
///
/// Building `select a, max(a) over(), sum(a) over() from t` 100 times in one
/// process (:2394-2405) must render the IDENTICAL plan string every time —
/// plan construction is independent of plan-id/counters drift across reuse.
#[test]
#[ignore = "go-parity-gap: deterministic re-build stability needs the builder and renderer"]
fn window_logical_plan_build_is_repeatable_over_iterations() {}

/// GO PORT of
/// `pkg/planner/core/logical_plans_test.go:2407 TestRemoveOrderbyInSubquery`.
///
/// With session `RemoveOrderbyInSubquery=true` (:2427): top-level ORDER BY
/// survives (`select * from t order by a` keeps Sort, :2432-2436); the scalar
/// `(select 1)` preserves ordering side effects (:2437); a derived table's
/// bare `order by a` is stripped (:2438); but `order by a limit 1` KEEPS
/// Sort+Limit because removing them would change results (:2439-2443).
#[test]
#[ignore = "go-parity-gap: orderby-in-subquery removal happens during subquery planning in the builder"]
fn remove_orderby_in_subquery_respects_limit_semantics() {}

/// GO PORT of
/// `pkg/planner/core/logical_plans_test.go:2445 TestAddLimitForCorrelatedExistsSubquery`.
///
/// With NO_DECORRELATE blocking decorrelation: EXISTS subqueries get a LIMIT 1
/// appended to their Apply arm — `Apply{DataScan(t1)->DataScan(t2)->
/// Sel([eq])->Projection->Limit}` — while the otherwise-identical IN-subquery
/// shape does NOT (:2454-2466): limit insertion is exclusive to existence
/// tests.
#[test]
#[ignore = "go-parity-gap: exists-arm LIMIT injection is a builder/decorrelate interaction, unported"]
fn add_limit_for_correlated_exists_subquery_only() {}

/// GO PORT of `pkg/planner/core/logical_plans_test.go:2473 TestRollupExpand`.
///
/// `select count(a) from t group by a, b with rollup` drives the manual
/// builder and inspects `builder.currentBlockExpand` BEFORE optimization
/// (:2492-2502): GID present, LevelExprs/RollupID2GIDS absent, ModeBitAnd,
/// ExtraGroupingColNames[0]=="gid", DistinctSize==3 over 2 distinct group-by
/// cols. After FlagResolveExpand (:2504-2507): THREE level projections exist;
/// level 0 renders `test.t.a, <nil>->Column#14, <nil>->Column#15, 0->gid`
/// (grouping sets {}, {a}, {a,b} map to gids 0/1/3, :2510-2521); the schema
/// keeps source col `a` NOT NULL, regenerates ex_a/ex_b nullable, gid NOT NULL
/// (:2523-2534); and `GenerateGroupingMarks` accepts grouping(a), grouping(b),
/// grouping(a,b)/grouping(b,a) returning marks sized 1/1/2/2 (:2540-2557).
#[test]
#[ignore = "go-parity-gap: ROLLUP expand synthesis is built by PlanBuilder's currentBlockExpand machinery"]
fn rollup_expand_generates_grouping_sets_levels_and_marks() {}

/// GO PORT of
/// `pkg/planner/core/logical_plans_test.go:2559 TestPruneColumnsForDelete`.
///
/// DELETE plans (result type `physicalop.Delete`) expose OutputNames(),
/// TblColPosInfos layouts — `tid:[start,end]` spans, handle-col indices,
/// index RowLayouts, or "no column-pruning happened" when IndexesRowLayout is
/// nil (:2585-2613) — plus InsidePlan and PrunedOutput texts, all golden per
/// DELETE statement variant.
#[test]
#[ignore = "go-parity-gap: delete DML planning and TblColPosInfo layout computation are unported"]
fn prune_columns_for_delete_layouts_golden() {}

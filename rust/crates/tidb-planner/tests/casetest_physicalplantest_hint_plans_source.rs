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

//! Ports for `pkg/planner/core/casetest/physicalplantest/physical_plan_test.go`
//! (`pkg/planner.part7`, items 361–393 of all 1278 `Test*`/`Benchmark*`
//! declarations under `pkg/planner/` on `origin/master`, sorted by file path
//! then line). Items 361–387 and 389–393 are documentary gaps; item 388
//! (`TestPhysicalApplyIsNotPhysicalJoin`) is a RUNNING port because its whole
//! contract is a Go type-level assertion that Rust already models.
//!
//! The golden families share one shape re-derived from the Go source: boot a
//! mock-store session under both cascade engines
//! (`testkit.RunTestUnderCascades[WithDomain]`, often
//! `mockstore.WithMockTiFlash(n)`), create tables/views plus TiFlash replicas
//! made available through `dom.DDLExecutor().UpdateTableReplicaInfo` or
//! `testkit.SetTiFlashReplica`, load per-test input/output pairs from the
//! physicalplantest testdata books, then either compare
//! `core.ToString(optimized plan)` plus stmt-context warnings, or run
//! `explain format = 'plan_tree' <q>` row-by-row goldens with converted SQL
//! warnings. Some tests additionally pin execution RESULTS (sorted) or
//! hint-restoration equivalence between `core.GenHintsFromPhysicalPlan` and
//! `core.GenHintsFromFlatPlan` over `core.FlattenPhysicalPlan`
//! (`assertSameHints`, :52-63: ElementsMatch on restored hint strings).
//! Every gap below needs the session/executor/Domain stack this crate does
//! not port; none is approximated.

use tidb_planner::physical::{PhysicalApply, PhysicalHashJoin, PhysicalPlan};

/// GO PORT of `physical_plan_test.go:252 TestMPPHints`.
///
/// Re-derived contract: under `WithMockTiFlash(2)` (:315) build table
/// `t(a,b,c)` with `idx_a`/`idx_b` (:261), mark its TiFlash replica available
/// via `UpdateTableReplicaInfo` (:264-266), enable `tidb_allow_mpp` and craft
/// views `v` (group-by agg over t) and `v1` (self join on t) (:262-263); each
/// input is either executed ("set"/"UPDATE" prefixes :291) or checked as an
/// explain plan-tree golden together with the exact SQL warning list
/// (:298-302); a second block recreates `t1(a int primary key)` /
/// `t2(a int, index ia(a))` (:306-307) and replays book section
/// `TestIssue37520` the same way (:315-334).
#[test]
#[ignore = "go-parity-gap: MPP/TiFlash explain goldens need mockstore session + Domain DDL executor + planner.Optimize pipeline"]
fn mpp_hints_plan_tree_goldens_with_views_and_issue_37520() {}

/// GO PORT of `physical_plan_test.go:316 TestMPPHintsScope`.
///
/// Re-derived contract: BEFORE the golden loop, four literals assert the
/// invalid-hint warnings exactly (:324-331): `MPP_1PHASE_AGG()` /
/// `MPP_2PHASE_AGG()` / `shuffle_join(t1,t2)` / `broadcast_join(t1,t2)` each
/// yield one row "Warning 1815 The agg|join can not push down to the MPP
/// side, the <hint>() hint is invalid"; then `alter table t set hypo tiflash
/// replica 1` (:332) enables the hypo-replica path and remaining inputs run
/// as plain query-result goldens plus warnings (:334-354).
#[test]
#[ignore = "go-parity-gap: session warning plumbing + MPP-side hint validation inside the optimize pipeline unported"]
fn mpp_hints_scope_invalid_hints_warn_1815_then_hypo_replica_goldens() {}

/// GO PORT of `physical_plan_test.go:358 TestMPPBCJModel`.
///
/// Re-derived contract: with THREE mpp stores (`WithMockTiFlash(3)` :405) and
/// replica available, broadcast-vs-hash choice follows the comment's exchange
/// size arithmetic (:359-366): broadcast costs Build=2x data while hash costs
/// 4/3x data across build+probe, so hash wins for these data sizes unless the
/// prefer flag changes the comparison; inputs are query/explain goldens plus
/// warnings (:379-404).
#[test]
#[ignore = "go-parity-gap: BCJ cost-model choices are made by the MPP task generator over real fragments"]
fn mpp_bcj_model_three_stores_hash_wins_exchange_size_goldens() {}

/// GO PORT of `physical_plan_test.go:406 TestMPPPreferBCJ`.
///
/// Re-derived contract: t1 gets ONE row, t2 gets EIGHT (:416-417), both get
/// TiFlash replicas plus analyze all columns (:427-443),
/// `tidb_allow_mpp=1; tidb_enforce_mpp=1` (:444); the skewed small-build case
/// must flip to broadcast once
/// `tidb_prefer_broadcast_join_by_exchange_data_size` is ON; set/insert
/// prefixed inputs execute, everything else is a result/explain golden
/// (:451-469).
#[test]
#[ignore = "go-parity-gap: enforce-MPP planning over analyzed stats needs the session/stats stack"]
fn mpp_prefer_broadcast_join_by_exchange_data_size_goldens() {}

/// GO PORT of `physical_plan_test.go:462 TestMPPBCJModelOneTiFlash`.
///
/// Re-derived contract: with ONE mpp store (:523): `GetMPPStoreCount()==1`
/// asserted (:480-484); setting
/// `tidb_prefer_broadcast_join_by_exchange_data_size` to -1 or 2 MUST error
/// (:486-489); zeroing `tidb_broadcast_join_threshold_size/count` disables
/// BCJ while the prefer-flag is OFF (:491-494); remaining inputs are golden
/// queries where broadcast wins at any cost once the flag is ON.
#[test]
#[ignore = "go-parity-gap: MPP store counting + session variable range checks + optimize pipeline unported"]
fn mpp_bcj_model_one_store_prefers_broadcast_and_guards_thresholds() {}

/// GO PORT of `physical_plan_test.go:524 TestMPPRightSemiJoin`.
///
/// Re-derived contract: same two-table skew setup as TestMPPPreferBCJ but
/// sets `tidb_hash_join_version=optimized` (:564) under TiFlash(1) (:579);
/// right-semi joins must keep their probe/build sides when planned on MPP;
/// every non-set/insert input is an explain+warning golden (:567-587).
#[test]
#[ignore = "go-parity-gap: right-semi MPP plan construction needs the fragment/task generator"]
fn mpp_right_semi_join_optimized_hash_join_version_goldens() {}

/// GO PORT of `physical_plan_test.go:580 TestMPPRightOuterJoin`.
///
/// Re-derived contract: t1(a,c) five rows, t2(b,d) three rows including key 7
/// missing from t1 (:591-592), replicas plus analyze plus allow/enforce MPP
/// (:600-623) under TiFlash(3) (:634); right outer join must preserve t2's
/// unmatched rows on MPP; explain+warning goldens for every input (:625-643).
#[test]
#[ignore = "go-parity-gap: right-outer MPP exchange placement needs the fragment/task generator"]
fn mpp_right_outer_join_three_stores_goldens() {}

/// GO PORT of `physical_plan_test.go:636 TestHintScope`.
///
/// Re-derived contract: `tidb_opt_advanced_join_hint=0` disabled (:641);
/// parsed statements optimize against `MockInfoSchema(MockSignedTable,
/// MockUnsignedTable)` (:648) and EVERY case must produce BOTH the golden
/// `core.ToString(best plan)` AND ZERO stmt-context warnings (:663-669),
/// i.e. accepted-but-ignored hints stay silent when advanced hints are off.
#[test]
#[ignore = "go-parity-gap: planner.Optimize end-to-end against MockInfoSchema + stmt warning capture unported"]
fn hint_scope_best_plan_string_without_warnings_under_legacy_join_hints() {}

/// GO PORT of `physical_plan_test.go:671 TestJoinHints`.
///
/// Re-derived contract: per case (:700-724): warnings reset, optimize, then
/// assert best-plan string, at most ONE warning at level Warning whose text
/// matches the book, hints restored from `GenHintsFromPhysicalPlan(p)`
/// equal the golden Hints string (recorded :689-695), and via
/// `assertSameHints` (:52-63) hints regenerated from
/// `FlattenPhysicalPlan(p,false)` are ElementsMatch-equal to the direct ones
/// (:718-720).
#[test]
#[ignore = "go-parity-gap: join-hint resolution + GenHintsFrom{Physical,Flat}Plan equivalence need the optimize pipeline"]
fn join_hints_best_plan_warnings_and_genhints_flatplan_equivalence() {}

/// GO PORT of `physical_plan_test.go:727 TestAggregationHints`.
///
/// Re-derived contract: inputs carry `(SQL string, AggPushDown bool)`
/// (:732-738); HashAggFinal/PartialConcurrency pinned to 1 (:737-739);
/// `sessionVars.AllowAggPushDown` is toggled PER CASE (:750) before
/// optimizing; assertions mirror TestJoinHints minus hint restoration
/// (:757-779).
#[test]
#[ignore = "go-parity-gap: per-case AllowAggPushDown toggling + aggregate-hint warnings need the optimize pipeline"]
fn aggregation_hints_best_plan_and_warnings_per_agg_pushdown_flag() {}

/// GO PORT of `physical_plan_test.go:781 TestSemiJoinRewriteHints`.
///
/// Re-derived contract: table `t(a,b,c)` created (:791); concurrency forced
/// to 1 (:792-794); each golden runs `"explain format = 'plan_tree'" + test`
/// concatenated WITHOUT a space (:822), so inputs begin with a space, plus
/// the single-warning check (:823-828).
#[test]
#[ignore = "go-parity-gap: semi-join-rewrite transform lives behind the optimize pipeline with live sessions"]
fn semi_join_rewrite_hints_plan_tree_and_warning_goldens() {}

/// GO PORT of `physical_plan_test.go:831 TestAggToCopHint`.
///
/// Re-derived contract: only `ta(a int, b int, index(a))` (:842-843);
/// optimizes against the DOMAIN infoschema rather than a mock one (:857);
/// asserts `core.ToString(p)` golden plus first-warning text/level per case
/// (:869-887).
#[test]
#[ignore = "go-parity-gap: agg-to-cop hint cost trade-offs need cop-task cost factors over domain stats"]
fn agg_to_cop_hint_best_plan_and_warning_goldens() {}

/// GO PORT of `physical_plan_test.go:889 TestGroupConcatOrderby`.
///
/// Re-derived contract: failpoint
/// `planner/core/forceDynamicPrune=return(true)` enabled around the body
/// (:880-882); tables `test(id,name)` six rows (:893-902) and RANGE
/// partitioned ptest p0<2,p1<11 (:904-907) with six rows copied in (:908);
/// `tidb_opt_distinct_agg_push_down=1` and `tidb_opt_agg_push_down=1`
/// session-set (:909-910); every case checks BOTH the plan-tree golden AND
/// the sorted query RESULT rows (:919-923).
#[test]
#[ignore = "go-parity-gap: forceDynamicPrune failpoint + execution results over partitioned tables unported"]
fn group_concat_orderby_dynamic_prune_plans_and_results() {}

/// GO PORT of `physical_plan_test.go:932 TestIndexHint` (main body).
///
/// Re-derived contract: per case exactly one-or-zero warnings required
/// depending on the recorded HasWarn bit (:982-987); best-plan string plus
/// restored hints golden plus GenHints flat-plan ElementsMatch equivalence
/// like TestJoinHints (:961-1000).
#[test]
#[ignore = "go-parity-gap: use/ignore-index resolution lives behind the optimize pipeline"]
fn index_hint_best_plan_warn_and_genhints_equivalence() {}

/// GO PORT of `physical_plan_test.go:981 subtest of TestIndexHint`
/// (`ignore long prefix-sharing index keeps shorter sibling`, :981-999).
///
/// Re-derived contract: table `t_issue66875` with `idx_contract_sys_no`
/// (single column) and the prefix-sharing wider sibling
/// `idx_contract_sys_no_delete_flag` (:985-991); FORCE_INDEX naming BOTH
/// indexes combined with IGNORE_INDEX on the wider one must plan onto the
/// SHORTER index — output contains `idx_contract_sys_no`, does NOT contain
/// `idx_contract_sys_no_delete_flag`, and has no TableFullScan (:992-999).
#[test]
#[ignore = "go-parity-gap: FORCE_INDEX+IGNORE_INDEX interaction needs access-path selection over a session infoschema"]
fn index_hint_prefix_sharing_shorter_sibling_forced_by_ignore_index() {}

/// GO PORT of `physical_plan_test.go:1003 TestIndexMergeHint`.
///
/// Re-derived contract: identical skeleton to TestIndexHint but each case
/// FIRST resets statement context via `executor.ResetContextOfStmt`
/// (:1035-1037) so stale index-merge hints cannot leak between cases; HasWarn
/// gate, best plan, restored hints and flat-plan equivalence as above
/// (:1039-1053).
#[test]
#[ignore = "go-parity-gap: index-merge hint parsing needs ResetContextOfStmt + optimize pipeline"]
fn index_merge_hint_best_plan_warn_and_genhints_equivalence() {}

/// GO PORT of `physical_plan_test.go:1056 TestQueryBlockHint`.
///
/// Re-derived contract: qb-name hints (@sel_1 etc.) pin `core.ToString(p)`
/// AND the restored hint list, which must include the block-scoped forms,
/// again verified equivalent between physical-plan and flat-plan generation
/// (:1085-1095).
#[test]
#[ignore = "go-parity-gap: query-block hint propagation needs the name-resolution scopes of the builder"]
fn query_block_hint_plan_and_restored_hints_goldens() {}

/// GO PORT of `physical_plan_test.go:1098 TestInlineProjection`.
///
/// Re-derived contract: two bigint tables each indexed on both columns
/// (:1103-1105); per-case best-plan string pins whether IndexJoin candidates
/// inline their child's Projection; hints golden plus flat-plan equivalence
/// as usual (:1128-1141).
#[test]
#[ignore = "go-parity-gap: projection inlining happens during index-join task construction under the optimizer"]
fn inline_projection_plan_and_hints_goldens_on_indexed_tables() {}

/// GO PORT of `physical_plan_test.go:1144 TestIndexJoinHint`.
///
/// Re-derived contract: wide table `t` with PRIMARY a, UNIQUE b, composite
/// c/d/e, f, g/h, duplicate UNIQUE aliases g_2/g_3, i (:1150-1151);
/// per-case plan string pins which side/index the hinted index join picks;
/// warnings pass through `filterWarnings` (:1170-1180) which DROPS
/// skyline-pruning notices containing "remain after pruning paths for", then
/// `TruncateWarnings(0)` (:1189) before comparing the plan.
#[test]
#[ignore = "go-parity-gap: index-join hint selection + warning filtering need the optimize pipeline"]
fn index_join_hint_plan_and_filtered_skyline_warnings_goldens() {}

/// GO PORT of `physical_plan_test.go:1194 TestHintFromDiffDatabase`.
///
/// Re-derived contract: `test.t1`/`test.t2` created but the session switches
/// to database test2 (:1200-1204); unqualified table-name hints must still
/// bind across the current-database boundary and produce the SAME plans as
/// qualified forms (:1224-1229).
#[test]
#[ignore = "go-parity-gap: cross-database hint binding needs multi-schema infoschema + builder"]
fn hints_apply_from_different_current_database() {}

/// GO PORT of `physical_plan_test.go:1231 TestHJBuildAndProbeHint4DynamicPartitionTable`.
///
/// Re-derived contract: forceDynamicPrune failpoint ON (:1240-1242);
/// hash-partitioned t1(a)/4, t2(a)/5, t3 keyed on b/3 populated identically
/// (:1250-1255), prune mode dynamic (:1257); HJ_BUILD/HJ_PROBE hints pin
/// build/probe side selection per partition scan; each case checks the
/// plan-tree golden AND sorted execution results (:1262-1267).
#[test]
#[ignore = "go-parity-gap: HJ build/probe enforcement under dynamic pruning needs executor results"]
fn hj_build_probe_hints_hash_partitioned_dynamic_prune_goldens() {}

/// GO PORT of `physical_plan_test.go:1270 TestHJBuildAndProbeHint4TiFlash`.
///
/// Re-derived contract: three clustered-pk tables, TiFlash replicas marked
/// available via `SetTiFlashReplica` (:1288-1290), allow+enforce MPP (:1293);
/// HJ_BUILD/_PROBE hints respected when the join runs as MPP exchange
/// operators; plan-tree goldens only, no result comparison (:1296-1305).
#[test]
#[ignore = "go-parity-gap: enforced-MPP joins over TiFlash replicas need the mock TiFlash store stack"]
fn hj_build_probe_hints_tiflash_mpp_enforced_goldens() {}

/// GO PORT of `physical_plan_test.go:1308 TestMPPSinglePartitionType`.
///
/// Re-derived contract: `employee(empid, deptid, salary decimal(10,2))`
/// (:1321-1322) with TiFlash replica (:1324) but `tidb_enforce_mpp=0`
/// (:1323); a mid-book set line executes directly (:1332-1335); remaining
/// cases pin single-partition-type fragmentation (no mixed broadcast/shuffle
/// fragments) via explain goldens (:1336-1340).
#[test]
#[ignore = "go-parity-gap: MPP fragment consistency decisions live in the fragment generator"]
fn mpp_single_partition_type_employee_goldens() {}

/// GO PORT of `physical_plan_test.go:1342 TestCountStarForTiFlash`.
///
/// Re-derived contract: 8-column not-null table plus char pk `t_pick_row_id`
/// (:1354-1356), TiFlash replicas set (:1360-1361), MPP enforced (:1363);
/// count-star pushdown (which columns the TiFlash aggregate actually scans)
/// pinned by plan-tree goldens; second block creates employee (:1369-1370)
/// and replays book section `TestIssues49377Plan` (:1373-1389).
#[test]
#[ignore = "go-parity-gap: count-star TiFlash pushdown goldens need enforced MPP planning"]
fn count_star_tiflash_goldens_with_issues_49377_block() {}

/// GO PORT of `physical_plan_test.go:1391 TestHashAggPushdownToTiFlashCompute`.
///
/// Re-derived contract: two complex PARTITION BY HASH tables tbl_15/tbl_16
/// including prefix indexes and defaults (:1406-1419); global config flips
/// `DisaggregatedTiFlash=true` for the test duration (:1421-1425); static
/// prune mode plus isolation-read engines=tiflash (:1429-1430); hash-agg must
/// survive onto the TiFlash compute layer; plan-tree goldens (:1441-1449).
#[test]
#[ignore = "go-parity-gap: disaggregated-TiFlash config gate + static-partition isolation reads unported"]
fn hash_agg_pushdown_disaggregated_tiflash_static_prune_goldens() {}

/// GO PORT of `physical_plan_test.go:1451 TestPointgetIndexChoosen`.
///
/// Re-derived contract: table with UNIQUE ub(b) and composite UNIQUE
/// ubc(b,c) (:1464-1465); point/range predicates on b alone must hit the
/// SHORTER unique index; golden plan-trees per case (:1473-1477).
#[test]
#[ignore = "go-parity-gap: point-get index choice among unique keys needs access-path costing"]
fn point_get_index_choice_unique_key_pairs_goldens() {}

/// GO PORT of `physical_plan_test.go:1479 TestAlwaysTruePredicateWithSubquery`
/// (issue 46962).
///
/// Re-derived contract: bare two-column table (:1493); each input itself is
/// an EXPLAIN query whose output rows are golden-checked (:1500-1502):
/// TRUE-literal predicates combined with scalar subqueries must NOT collapse
/// into malformed dual plans.
#[test]
#[ignore = "go-parity-gap: constant-folded subquery predicates need the expression rewrite stack"]
fn always_true_predicate_with_subquery_issue_46962_goldens() {}

/// GO PORT of `physical_plan_test.go:1504 TestExplainExpand`.
///
/// Re-derived contract: sales/year/country/product fact table plus generic
/// t/s tables (:1521-1525); FIRST a literal error arm (:1527-1528):
/// GROUP BY country, country, product WITH ROLLUP ORDER BY grouping(year)
/// fails EXACTLY with "[planner:3602] Argument #0 of GROUPING function is not
/// in GROUP BY"; remaining ROLLUP/GROUPING explain shapes are golden rows.
#[test]
#[ignore = "go-parity-gap: GROUPING-with-rollup resolution error + expand operator goldens need the optimize pipeline"]
fn explain_expand_rollup_grouping_error_3602_then_goldens() {}

/// GO PORT of `physical_plan_test.go:1537 TestPhysicalApplyIsNotPhysicalJoin`.
///
/// RUNNING PORT. Go source:
/// `require.NotImplements(t, (*base.PhysicalJoin)(nil), new(physicalop.PhysicalApply))`
/// i.e. PhysicalApply embeds a hash join but deliberately does NOT satisfy
/// the `base.PhysicalJoin` interface (GetJoinType/GetInnerChildIdx,
/// plan_base.go:378-380). In this crate's enum model the same fact is asserted
/// structurally:
/// - [`PhysicalPlan::join_type`] answers Some ONLY for true join variants
///   (src/physical/mod.rs), mirroring the interface boundary Go enforces by
///   making Apply omit those methods.
#[test]
fn physical_apply_is_not_physical_join() {
    let apply = PhysicalPlan::Apply(PhysicalApply::default());
    // Go: new(physicalop.PhysicalApply) does not implement base.PhysicalJoin.
    assert_eq!(apply.join_type(), None);
    assert_eq!(apply.inner_child_idx(), None);

    // And a genuine hash join still DOES answer GetJoinType, showing the
    // boundary separates Apply from the join interface rather than being
    // absent entirely.
    let join = PhysicalPlan::HashJoin(PhysicalHashJoin::default());
    assert_eq!(
        join.join_type(),
        Some(tidb_planner::find_best_task::LogicalJoinType::Inner)
    );
}

/// GO PORT of `physical_plan_test.go:1542 TestRuleAggElimination4Join`.
///
/// Re-derived contract: t1..t3 with composite UNIQUE UK_id1_id2, t4 with a
/// NON-unique KEY of the same shape (:1551-1554); uniqueness-triggered
/// count/agg elimination over joins pinned via cascades_template book
/// goldens (plan trees plus warnings; helper `getCascadesTemplateData`
/// returns testDataMap["cascades_template"] :1598-1600); second block
/// rebuilds t1 with GLOBAL index, NONCLUSTERED pk and RANGE COLUMNS
/// partitioning under static prune mode (:1571-1578 data, DDL :1583, prune
/// mode :1596) replaying book section `TestIssue62331`.
#[test]
#[ignore = "go-parity-gap: unique-key agg elimination goldens need cascade-template data + join optimizer"]
fn rule_agg_elimination_4join_cascades_template_with_issue_62331() {}

/// GO PORT of `physical_plan_test.go:1600 TestLimitPushdown`.
///
/// Re-derived contract: does NOT use RunTestUnderCascades: plain mock store
/// (:1610-1611); injects SYNTHETIC statistics instead of analyzing: HistColl
/// rowCount=5000 with fully-loaded StatsVer2 histogram per column and blob
/// fieldtype per index inserted into `dom.StatsHandle().UpdateStatsCache`
/// (:1618-1650) to avoid analyze-sampling flakiness; LIMIT values then decide
/// TopN-vs-IndexScan vs TableReader; set/UPDATE-prefixed inputs execute,
/// others are plan-tree+warning goldens with NO cascade pair (LoadTestCases
/// without cascades/caller :1656-1663).
#[test]
#[ignore = "go-parity-gap: limit-pushdown cost decisions need injected stats cache + optimize pipeline"]
fn limit_pushdown_injected_histogram_stats_goldens() {}

/// GO PORT of `physical_plan_test.go:1674 TestAllocMPPID`.
///
/// Re-derived contract (fragment.go:192-195): `AllocMPPTaskID(ctx)` returns
/// `StmtCtx.MPPQueryInfo.AllocatedMPPTaskID.Add(1)`; the counter is
/// per-statement (reset when the query finishes), so three successive calls
/// on one fresh `mock.NewContext()` answer EXACTLY 1, 2, 3 (:1678-1680).
/// The Rust workspace ports no MPPQueryInfo counter (no AllocatedMPPTaskID
/// surface anywhere under rust/crates), so the contract is recorded here,
/// not reimplemented.
#[test]
#[ignore = "go-parity-gap: AllocatedMPPTaskID atomic counter on StmtCtx.MPPQueryInfo is outside the ported surface"]
fn alloc_mpp_task_id_increments_one_per_call_from_fresh_context() {}

/// GO PORT of `physical_plan_test.go:1681 TestSemiJoinRewriter`.
///
/// Re-derived contract: `tidb_opt_enable_semi_join_rewrite=on` with t1(int),
/// t2(varchar(10)), t3(int) (:1689-1691); select-from-t1-where-exists over
/// t2 matched on equality rewrites into the EXACT eight golden rows
/// (:1692-1699): inner HashJoin whose BUILD side is a HashAgg firstrow-group
/// over cast(t2.a, double BINARY) feeding TableFullScan, PROBE side carrying
/// t1.a plus its double cast, i.e. semi-join rewrite turns EXISTS into an
/// INNER join with cross-type casts through both children.
#[test]
#[ignore = "go-parity-gap: semi-join rewrite with implicit double casts needs type inference over plans"]
fn semi_join_rewrite_exists_becomes_inner_hash_join_eight_line_golden() {}

/// GO PORT of `physical_plan_test.go:1700 TestDisableReuseChunk`.
///
/// Re-derived contract: saves/restores package var
/// `core.MaxMemoryLimitForOverlongType` (:1706-1712); mediumtext
/// point-select result rows stay correct in BOTH halves (:1715,1725); with
/// the limit set to 0 chunk reuse is DISABLED so `@@last_sql_use_alloc`
/// reports 1 (:1716), and raised to 500GB reuse is allowed again reporting 0
/// (:1726): the variable gates allocation reuse purely by column-width
/// budget.
#[test]
#[ignore = "go-parity-gap: last_sql_use_alloc accounting belongs to the executor's chunk allocator"]
fn disable_reuse_chunk_overlong_mediumtext_controls_last_sql_use_alloc() {}

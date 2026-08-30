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

//! `pkg/planner.part13` DOCUMENTED GAP ports — each function below pins the
//! INTENT of one Go test from `origin/master` that this crate cannot yet
//! exercise for real, so it is `#[ignore]`d instead of approximated.
//!
//! Every entry names its Go file + function and states exactly which surface
//! is missing on the Rust side. Sources read from origin/master in this
//! batch:
//!
//! * `pkg/planner/core/operator/logicalop/logicalop_test/
//!   logical_mem_table_predicate_extractor_test.go` (:74 … :1936) — all 13
//!   extractor tests drive a full PlanBuilder + logical-optimize run over
//!   information_schema / metrics_schema queries via
//!   `getLogicalMemTable` (:52-71) and assert what
//!   `base.MemTablePredicateExtractor.Extract` kept per table type. The
//!   extractor interface AND its ~14 per-table implementations are explicitly
//!   NOT transcreated (`src/logical/mem_table.rs` module header), and no
//!   SQL→plan builder exists in this crate — every assertion below would be a
//!   guess.
//! * `pkg/planner/core/operator/logicalop/logicalop_test/
//!   logical_operator_test.go` (:34, :77, :136, :160) — clone/copy-on-write
//!   aliasing semantics unrepresentable over owned-value operators, plus two
//!   session/golden suites.
//! * `pkg/planner/core/operator/logicalop/logicalop_test/plan_execute_test.go`
//!   (:23 TestIssue58743) — result-set equivalence through the executor.
//! * `pkg/planner/core/operator/physicalop/{fragment_test.go,
//!   physical_batch_point_get_test.go, physical_utils_test.go}` (:25-129) —
//!   MPP fragment/task plumbing and plan-tree flattening are not transcreated;
//!   BatchPointGet pruning lives outside this crate (tidb-executor).
//! * `pkg/planner/core/optimizer_test.go` (:65-649 minus :650) —
//!   MPP decimal/join-key negotiation, TiFlash fine-grained shuffle, hash join
//!   v2 gating: session-level surfaces not ported.
//! * `pkg/planner/core/physical_plan_test.go` (:50-618) — full
//!   parse→Preprocess→planner.Optimize pipelines with mock stores.

/// GO PARITY GAP port of
/// `pkg/planner/core/operator/logicalop/logicalop_test/
/// logical_mem_table_predicate_extractor_test.go:74 TestClusterConfigTableExtractor`.
///
/// go-parity-gap: needs the ClusterConfigTableExtractor's Extract to exist
/// (`memtable_predicate_extractor.go`) plus the SQL→leaf-plan pipeline of
/// getLogicalMemTable (:52-71); both are unported by design. The Go test pins:
/// `type='tikv'` and flipped `'tikv'=type`/`'TiKV'=type` forms fold into
/// node-types {tikv,tidb,pd}; `address` in ('x','y') folds into instances;
/// `guessIsTiDBStore` false-paths and cross-filter interplay; conditions like
/// `nodes=1` mark skipRequest; unrelated predicates push through untouched or
/// turn the leaf into TableDual.
#[test]
#[ignore]
fn cluster_config_table_extractor_folds_type_address_and_node_filters() {}

/// GO PARITY GAP port of `...logical_mem_table_predicate_extractor_test.go:257
/// TestClusterLogTableExtractor`.
///
/// go-parity-gap: same missing extractor surface. Pins time-range folding
/// across `time>..`, `time>=..<..`, literal-vs-column forms; level IN-lists
/// (case-insensitive `warn/WARN`); typed message patterns; numeric sample
/// filters; skipRequest on impossible ranges; dual on constant-false ones.
#[test]
#[ignore]
fn cluster_log_table_extractor_folds_time_level_and_pattern_filters() {}

/// GO PARITY GAP port of `...logical_mem_table_predicate_extractor_test.go:604
/// TestMetricTableExtractor`.
///
/// go-parity-gap: same missing surface. Pins promotion-type constraints
/// (`labels` schema map), time-range normalization into Quantiles/promQL,
/// instance/quantile handling under `metrics_schema` tables and skipRequest
/// rows.
#[test]
#[ignore]
fn metric_table_extractor_folds_promql_time_and_label_filters() {}

/// GO PARITY GAP port of `...logical_mem_table_predicate_extractor_test.go:740
/// TestMetricsSummaryTableExtractor`.
///
/// go-parity-gap: same missing surface (MetricsSummaryTableExtractor folding
/// inst/quantile/time filters across the metrics_summary family).
#[test]
#[ignore]
fn metrics_summary_table_extractor_folds_inst_quantile_time_filters() {}

/// GO PARITY GAP port of `...logical_mem_table_predicate_extractor_test.go:844
/// TestInspectionResultTableExtractor`.
///
/// go-parity-gap: same missing surface (rule/rules to inspect, item filtering,
/// and time-ranges retained for inspection_result queries).
#[test]
#[ignore]
fn inspection_result_table_extractor_folds_rule_item_filters() {}

/// GO PARITY GAP port of `...logical_mem_table_predicate_extractor_test.go:989
/// TestInspectionSummaryTableExtractor`.
///
/// go-parity-gap: same missing surface (quantile+rules+time folding for
/// inspection_summary).
#[test]
#[ignore]
fn inspection_summary_table_extractor_folds_quantile_rules_time() {}

/// GO PARITY GAP port of `...logical_mem_table_predicate_extractor_test.go:1094
/// TestInspectionRuleTableExtractor`.
///
/// go-parity-gap: same missing surface (`inspection_rules`: rule/name lists,
/// type filters, skipRequest negatives).
#[test]
#[ignore]
fn inspection_rule_table_extractor_folds_rule_name_type_filters() {}

/// GO PARITY GAP port of `...logical_mem_table_predicate_extractor_test.go:1137
/// TestTiDBHotRegionsHistoryTableExtractor`.
///
/// go-parity-gap: same missing surface (`TIDB_HOT_REGIONS_HISTORY`: region /
/// store-id / table-name / hot-degree lists and their enumerated value sets;
/// cross-column updates when multiple filters arrive at once :1415+).
#[test]
#[ignore]
fn tidb_hot_regions_history_extractor_folds_region_store_degree_lists() {}

/// GO PARITY GAP port of `...logical_mem_table_predicate_extractor_test.go:1525
/// TestTikvRegionPeersExtractor`.
///
/// go-parity-gap: same missing surface (`TIKV_REGION_PEERS`: db-name,
/// table-name, table-id / is-index / is-leader / is-learner / down-sec pairs,
/// plus probe behavior when region ids repeat across peers).
#[test]
#[ignore]
fn tikv_region_peers_extractor_folds_identity_and_role_columns() {}

/// GO PARITY GAP port of `...logical_mem_table_predicate_extractor_test.go:1663
/// TestColumns`.
///
/// go-parity-gap: exercises extractor.EXTRACT dispatch end-to-end over
/// `COLUMNS`/`STATISTICS` virtual tables with column-name/table-name/schema
/// IN-lists folded against real cataloged views; needs catalog+builder.
#[test]
#[ignore]
fn columns_statistics_extractors_fold_name_list_filters() {}

/// GO PARITY GAP port of `...logical_mem_table_predicate_extractor_test.go:1773
/// TestTikvRegionStatusExtractor`.
///
/// go-parity-gap: same missing surface as TIKV_REGION_PEERS above, for
/// `TIKV_REGION_STATUS` (store/address/region-column pair folding).
#[test]
#[ignore]
fn tikv_region_status_extractor_folds_store_and_region_filters() {}

/// GO PARITY GAP port of `...logical_mem_table_predicate_extractor_test.go:1825
/// TestExtractorInPreparedStmt`.
///
/// go-parity-gap: pins that extractors fold `?` PARAMETER markers only after
/// prepared-statement execution supplies values (`Extract` sees post-exec
/// constants); needs prepare/execute pipeline.
#[test]
#[ignore]
fn extractor_folds_only_post_execution_parameter_values_in_prepared_stmts() {}

/// GO PARITY GAP port of `...logical_mem_table_predicate_extractor_test.go:1936
/// TestInfoSchemaTableExtract`.
///
/// go-parity-gap: pins infoschema-column extraction for
/// CLUSTER_STATEMENTS_SUMMARY(_HISTORY)/CLUSTER_PROCESSLIST/TIDB_TRX
/// (digest-text prefixes, connection/txn id lists, digest matching); needs
/// those extractors' Extract.
#[test]
#[ignore]
fn infoschema_cluster_tables_fold_digest_id_and_processlist_filters() {}

/// GO PARITY GAP port of
/// `pkg/planner/core/operator/logicalop/logicalop_test/logical_operator_test.go:34
/// TestLogicalSchemaClone`.
///
/// go-parity-gap: Go copies the STRUCT (`cloneSp := *sp`) and pins POINTER
/// aliasing: `*schema` shared (append through the clone grows the original),
/// output-names slice shallow, BaseLogicalPlan struct new but children slice
/// SHARED by pointer identity (`sp.Children()[0] == cloneSp.Children()[0]`).
/// Rust plans are owned trees — no `*sp` struct-copy state exists to pin
/// without inventing interior-mutability production code.
#[test]
#[ignore]
fn logical_schema_struct_copy_shares_schema_pointer_and_children() {}

/// GO PARITY GAP port of
/// `...logical_operator_test.go:77 TestLogicalApplyClone`.
///
/// go-parity-gap: same aliasing semantics for LogicalApply.EqualConditions —
/// append within capacity and swap-through-write must stay visible to the
/// ORIGINAL slice because a Go struct copy shares the backing array; the owned
/// Rust operator cannot represent that state.
#[test]
#[ignore]
fn logical_apply_struct_copy_shares_equal_conditions_backing_array() {}

/// GO PARITY GAP port of
/// `...logical_operator_test.go:136 TestReplaceColumnOfExprCopyOnWrite`.
///
/// go-parity-gap: `ruleutil.ReplaceColumnOfExpr`
/// (`pkg/planner/core/rule/util/expression.go_utils`) builds a NEW scalar
/// function whose args point at dstCol while leaving the ORIGINAL untouched;
/// the helper is not transcreated (see `src/logical/apply.rs` module header,
/// "ReplaceExprColumns needs ruleutil.ResolveExprAndReplace").
#[test]
#[ignore]
fn replace_column_of_expr_copies_on_write_instead_of_mutating() {}

/// GO PARITY GAP port of
/// `...logical_operator_test.go:160 TestResolveExprAndReplaceCopyOnWrite`.
///
/// go-parity-gap: `ruleutil.ResolveExprAndReplace(expr, hashCode->col map)`
/// rewrites matches by column HashCode, again copy-on-write; same missing
/// ruleutil surface.
#[test]
#[ignore]
fn resolve_expr_and_replace_rewrites_by_hash_code_without_mutating_source() {}

/// GO PARITY GAP port of
/// `...logical_operator_test.go:199 TestLogicalProjectionPushDownTopN`.
///
/// go-parity-gap: golden plan_tree comparison over a full JSON-expression
/// pushdown query through the optimizer WITH and WITHOUT the topn_push_down
/// rule (opt_rule_blacklist rewrite); needs session planning + explain book.
#[test]
#[ignore]
fn projection_push_down_topn_keeps_order_columns_through_projection_stack() {}

/// GO PARITY GAP port of
/// `...logical_operator_test.go:282 TestLogicalExpandBuildKeyInfo`.
///
/// go-parity-gap: ROLLUP/GROUPING result-count checks plus recorded
/// cascades-suite EXPLAIN goldens (`GetCascadesSuiteData().LoadTestCases`);
/// needs executor result sets and the golden book this crate does not read.
#[test]
#[ignore]
fn rollup_expand_build_key_info_matches_recorded_cascades_explains() {}

/// BOOTSTRAP skipped-reason for
/// `pkg/planner/core/operator/logicalop/logicalop_test/main_test.go:29
/// TestMain` — loads the cascades-suite golden book and installs goleak; no
/// behavior of its own to port. Recorded here rather than as a test; nothing
/// links against it, it exists so the bootstrap decision has a code anchor.
#[allow(dead_code)]
pub const LOGICALOP_TEST_MAIN: () = ();

/// GO PARITY GAP port of
/// `pkg/planner/core/operator/logicalop/logicalop_test/plan_execute_test.go:23
/// TestIssue58743`.
///
/// go-parity-gap: end-to-end statement execution (partitioned clustered table
/// + two CTEs + index-merge hint) asserting an empty-but-clean result;
/// planning+execution pipeline unported.
#[test]
#[ignore]
fn issue_58743_partition_cte_aggregate_executes_without_error() {}

/// GO PARITY GAP port of
/// `pkg/planner/core/operator/physicalop/fragment_test.go:25
/// TestFragmentInitSingleton`.
///
/// go-parity-gap: `Fragment.init(PhysicalHashJoin)` singleton detection over
/// PhysicalExchangeReceiver/Sender children is MPP fragment plumbing this
/// crate has not transcreated (`Fragment`, `mppTaskGenerator` absent).
/// Intent: a join whose receivers feed from PASS-through/BROADCAST senders
/// stays singleton unless BOTH sides carry exchange receivers (:41-53 the
/// r2/r2 case reports singleton=false).
#[test]
#[ignore]
fn fragment_init_reports_singleton_unless_both_join_sides_exchange() {}

/// GO PARITY GAP port of
/// `pkg/planner/core/operator/physicalop/fragment_test.go:56
/// TestFillLocalCTECountsUsesLocalTaskCounts`.
///
/// go-parity-gap: `mppTaskGenerator.fillLocalCTECounts` counting local
/// CTE sinks/sources per TiFlash address (`PhysicalCTESink.CteSinkNum`
/// etc.) — MPP task generator unported. Intent: two CTESink fragments split
/// across tiflash0/tiflash1 with both consumers everywhere yield sink-num 1
/// and source-num 2 per address (:68-92 asserts 1 and 2).
#[test]
#[ignore]
fn fill_local_cte_counts_reads_tasks_local_to_each_tiflash_address() {}

/// GO PARITY GAP port of
/// `pkg/planner/core/operator/physicalop/physical_batch_point_get_test.go:27
/// TestPruneCommonHandleDuplicateValues`.
///
/// go-parity-gap: `BatchPointGetPlan.PrunePartitionsAndValues` dedup runs on
/// the physical BatchPointGetPlan, whose home in this workspace is
/// tidb-executor (kv_table.rs cites `BatchPointGetPlan.
/// PrunePartitionsAndValues`) — OUTSIDE this batch's gate crate. Intent
/// pinned upstream: null-valued keys survive dedup alongside first-seen
/// distinct strings ("b","a","b",nil,"c","a" -> ["b","a","c"]), all-null input
/// prunes to table-dual-free empty handles.
#[test]
#[ignore]
fn batch_point_get_prunes_common_handle_duplicate_index_values() {}

/// GO PARITY GAP port of
/// `...physical_utils_test.go:55 TestTryToGetMppHashAggsForMaxMinCount`.
///
/// go-parity-gap: `tryToGetMppHashAggs` generating PhysicalHashAgg variants
/// by AggMppRunMode (Mpp2Phase for max/min with GROUP BY, MppScalar for
/// max/min-count WITHOUT group-by, never MppTiDB) — MPP agg task generation
/// unported.
#[test]
#[ignore]
fn try_to_get_mpp_hash_aggs_modes_follow_max_min_count_grouping() {}

/// GO PARITY GAP port of
/// `pkg/planner/core/optimizer_test.go:65 TestMPPDecimalConvert`.
///
/// go-parity-gap: `negotiateCommonType(lType, rType)` decimal flen/decimal
/// negotiation (12 matrix rows pinning common type + who converts) is part of
/// the JoinKeyTypeCast/MPP key negotiation, unported. Row set preserved
/// verbatim for the porter: (5,9,5,8)->no conv; (0,8,0,11)->left conv;
/// (5,9,4,9)->both conv dec10; (10,16,0,11)->both conv dec21; (20,20,0,60)->
/// both conv dec65; (0,40,0,60)->none.
#[test]
#[ignore]
fn mpp_decimal_negotiation_picks_common_scale_and_converter_side() {}

/// GO PARITY GAP port of
/// `pkg/planner/core/optimizer_test.go:90 TestMPPJoinKeyTypeConvert`.
///
/// go-parity-gap: same negotiateCommonType surface over int-family pairs
/// (tiny/longlong signed×unsigned → longlong/decimal conversions) plus the
/// three subtests around overlong-type chunk reuse and point-get exact row
/// bounds (:119-269) — all session/stats-backed surfaces.
#[test]
#[ignore]
fn mpp_join_key_type_conversion_matrix_pins_common_types_and_sides() {}

/// GO PARITY GAP port of
/// `pkg/planner/core/optimizer_test.go:277 TestHandleFineGrainedShuffle`.
///
/// go-parity-gap: `handleFineGrainedShuffle` walks a physical tree stamping
/// TiFlashFineGrainedShuffleStreamCount gated by partial sort / hash-
/// partitioned window/agg/join shapes, plus failpoint-driven server-info
/// refresh (`splitTiFlashLogicalCoreCache` subtest :279-315 pins stale-core
/// refresh). Physical tree walker + copr MPP server manager unported.
#[test]
#[ignore]
fn fine_grained_shuffle_stream_count_stamps_only_supported_subtrees() {}

/// GO PARITY GAP port of
/// `pkg/planner/core/optimizer_test.go:600 TestCanTiFlashUseHashJoinV2`.
///
/// go-parity-gap: `PhysicalHashJoin.CanTiFlashUseHashJoinV2` gating on
/// TiFlashHashJoinVersion + spill quotas + NullEQ/cross-join shape; Physical-
/// HashJoin itself is not transcreated. Intent: legacy version, any spill knob
/// enabled, null-eq condition, or cross join forbids V2; optimized version
/// without spill allows it.
#[test]
#[ignore]
fn ti_flash_hash_join_v2_gate_tracks_version_spill_null_eq_and_cross_shape() {}

/// GO PARITY GAP port of
/// `pkg/planner/core/panicrisk_regression_test.go:30
/// TestExtractTablePartitionMalformed`.
///
/// go-parity-gap: `tidbCodecFuncHelper.extractTablePartition` guards
/// `t)(` -style inputs after a historical reversed-slice panic; the codec
/// helper lives with the tidbCodec plan family, unported here. Table of six
/// cases (t)(→t)((, )((, t(p)->t/p, t, t(p, tp)) recorded above verbatim for
/// the porter.
#[test]
#[ignore]
fn encode_record_key_partition_extraction_never_panics_on_malformed_input() {}

/// GO PARITY GAP port of
/// `pkg/planner/core/panicrisk_regression_test.go:55
/// TestSchemaTableSorterKeepsPairsAligned`.
///
/// go-parity-gap: `schemaTableSorter` sorting schema/table PAIRS by (schema,
/// name) without scrambling the pairing after a hand-rolled less-func bug;
/// requires the pair-sorter artifact plus model.TableInfo, unported here.
/// Expected order pinned: (db_a,t1_in_a),(db_a,t2_in_a),(db_b,t_in_b).
#[test]
#[ignore]
fn schema_table_sorter_keeps_each_table_paired_with_its_schema() {}

/// GO PARITY GAP port of
/// `pkg/planner/core/physical_plan_test.go:50 TestAnalyzeBuildSucc`.
///
/// go-parity-gap: planner.Optimize accept/reject decisions for ANALYZE
/// samplerate/samples options ((0.1 succ), (10 samplerate reject),
/// (0.1+100000 samples reject)); ANALYZE plan construction unported.
#[test]
#[ignore]
fn analyze_build_accepts_legal_samplerate_and_rejects_the_rest() {}

/// GO PARITY GAP port of
/// `...physical_plan_test.go:97 TestFullOuterJoinSyntaxUnsupported`.
///
/// go-parity-gap: ErrNotSupportedYet("FULL OUTER JOIN") raised under
/// tidb_enable_full_outer_join=off for FULL OUTER JOIN and LATERAL-in-FULL-
/// OUTER arms, and accepted (plain arm) / still-rejected (lateral arms) with
/// the switch ON; needs the full plan-validate pipeline.
#[test]
#[ignore]
fn full_outer_join_error_gates_track_the_enable_switch_and_lateral_arms() {}

/// GO PARITY GAP port of
/// `...physical_plan_test.go:125 TestAnalyzeSetRate`.
///
/// go-parity-gap: Analyze.Opts[AnalyzeOptSampleRate] equals -1 for bare
/// ANALYZE / samples-only, and 0.1 with samplerate — reads the built ANALYZE
/// plan's option map; unported.
#[test]
#[ignore]
fn analyze_set_rate_records_minus_one_when_no_samplerate_given() {}

/// GO PARITY GAP port of
/// `...physical_plan_test.go:179 TestRequestTypeSupportedOff`.
///
/// go-parity-gap: with a client refusing every request type the planner still
/// emits TableReader(Table(t))->Sel([in(test.t.a, 1, 10, 20)]) (ToString gold);
/// needs session/catalog planning pipeline.
#[test]
#[ignore]
fn unsupported_request_client_still_yields_root_reader_selection_plan() {}

/// GO PARITY GAP port of
/// `...physical_plan_test.go:200 TestDoSubQuery`.
///
/// go-parity-gap: `do 1 in (select a from t)` plans to
/// LeftHashJoin{Dual->PointGet(Handle(t.a)1)}->Projection under cascades AND
/// legacy; stringer-over-full-planning pipeline unported.
#[test]
#[ignore]
fn do_subquery_plans_to_left_hash_join_over_point_get() {}

/// GO PARITY GAP port of
/// `...physical_plan_test.go:229 TestIndexLookupCartesianJoin`.
///
/// go-parity-gap: TIDB_INLJ(t1,t2) on a cartesian join falls back to
/// LeftHashJoin{TableReader->TableReader} warning ErrInternal "TIDB_INLJ hint
/// is inapplicable without column equal ON condition"; needs costing + warning
/// plumbing.
#[test]
#[ignore]
fn index_lookup_hint_on_cartesian_join_warns_and_falls_back_to_hash_join() {}

/// GO PARITY GAP port of
/// `...physical_plan_test.go:249 TestMPPHintsWithBinding`.
///
/// go-parity-gap: global bindings carrying read_from_storage(tiflash) +
/// MPP_1PHASE_AGG/MPP_2PHASE_AGG/shuffle_join/broadcast_join must match and
/// flip last_plan_from_binding; binding store + TiFlash replica mock
/// required.
#[test]
#[ignore]
fn mpp_hints_bind_and_apply_through_global_bindings_round_trip() {}

/// GO PARITY GAP port of
/// `...physical_plan_test.go:295 TestJoinHintCompatibilityWithBinding`.
///
/// go-parity-gap: leading/hash_join hints bind cleanly (no warnings), replay
/// from the binding with last_plan_from_binding=1, and show-global-bindings
/// text round-trips; binding store required.
#[test]
#[ignore]
fn join_hints_compatible_with_global_binding_replay_without_warnings() {}

/// GO PARITY GAP port of
/// `...physical_plan_test.go:322 TestJoinHintCompatibilityWithVariable`.
///
/// go-parity-gap: with tidb_opt_advanced_join_hint=0 the same hints produce a
/// WARNING (legacy path demotes advanced join hints); warning plumbing
/// unported.
#[test]
#[ignore]
fn advanced_join_hint_off_demotes_leading_hash_join_with_warning() {}

/// GO PARITY GAP port of
/// `...physical_plan_test.go:381 TestMPPFullOuterJoinToPB`.
///
/// go-parity-gap: shuffle_join full-outer hash join must keep its left/right
/// conditions and serialize tipb.JoinType_TypeFullOuterJoin to TiFlash
/// (`hashJoin.ToPB`); ToPB serialization unported.
#[test]
#[ignore]
fn mpp_full_outer_shuffle_join_serializes_conditions_into_tipb() {}

/// GO PARITY GAP port of
/// `...physical_plan_test.go:396 TestMPPFullOuterJoinWithoutShuffleHint`.
///
/// go-parity-gap (intent unchanged): broadcast thresholds raised so the
/// same full-outer plan STILL picks MPP shuffle join silently (warnings empty)
/// — planner costing over mock-TiFlash domains unported.
#[test]
#[ignore]
fn full_outer_join_selects_mpp_shuffle_without_explicit_hint_no_warnings() {}

/// GO PARITY GAP port of
/// `...physical_plan_test.go:408 TestHintAlias`.
///
/// go-parity-gap: legacy hint aliases (TIDB_SMJ↔MERGE_JOIN, TIDB_HJ↔HASH_JOIN,
/// TIDB_INLJ↔INL_JOIN) yield IDENTICAL plans regardless of spelling
/// (ToString equality pairwise); needs planning + hint resolution.
#[test]
#[ignore]
fn legacy_and_modern_hint_spellings_produce_identical_plans() {}

/// GO PARITY GAP port of
/// `...physical_plan_test.go:452 TestIndexJoinRowModeWithInnerTopN` (skipped
/// ON-cascades upstream too).
///
/// go-parity-gap: with inl_join_inner_multi_pattern enabled, no index-join
/// variant may take derived-table s (order-by+limit inner) as its inner side;
/// needs the cost-model walk over built joins.
#[test]
#[ignore]
fn index_join_row_mode_never_inner_derived_topn_subtree() {}

/// GO PARITY GAP port of
/// `...physical_plan_test.go:511 TestIndexJoinRowModeWithInnerTopNOuterJoin`.
///
/// go-parity-gap: outer-join twin of the previous arm — LEFT JOIN with the
/// same multi-pattern settings must also avoid index-joins whose inner side
/// carries TopN/Limit; needs costing pipeline.
#[test]
#[ignore]
fn index_join_row_mode_outer_join_also_avoids_inner_topn_subtree() {}

/// GO PARITY GAP port of
/// `...physical_plan_test.go:569 TestIndexJoinHintInSubquery`.
///
/// go-parity-gap: `INL_JOIN(t1, t2@subq)` with QB_NAME(subq) must plan an
/// index join for the semi-apply subquery (advanced join hints); needs the
/// apply-decorrelation + costing pipeline.
#[test]
#[ignore]
fn inl_join_hint_targets_query_block_inside_exists_subquery() {}

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

//! Documentary gap ports for `pkg/planner/core/casetest/mpp`
//! (`pkg/planner.part6` items 307–329 on `origin/master`; family bootstrap is
//! `mpp/main_test.go:30 TestMain`, skipped-reason in the receipt).
//!
//! Every test here runs `testkit.RunTestUnderCascadesWithDomain`: a live mock
//! cluster whose domain can mark tables with virtual TiFlash replicas
//! (`testkit.SetTiFlashReplica`), session engine/MPP variables, and the
//! `integration_suite` golden book
//! (`pkg/planner/core/casetest/mpp/testdata/integration_suite`) whose inputs
//! are whole EXPLAIN statements compared row-by-row. The Rust workspace has
//! neither a session/executor pair nor MPP join costing or exchange
//! selection: the wired physical tree has no ExchangeSender or
//! ExchangeReceiver variants, so these stay documented gaps. Bodies are EMPTY
//! on purpose — asserting them here would mean inventing the plans the golden
//! book records.

/// GO PORT of `pkg/planner/core/casetest/mpp/mpp_test.go:31 TestMPPJoin`.
///
/// Re-derived contract: the star schema (fact_t keyed by int d1_k /
/// decimal(10,2) d2_k / date d3_k plus three analyzed dimension tables), all
/// carrying available TiFlash replicas, must reproduce every recorded plan of
/// the integration_suite book once `tidb_isolation_read_engines='tiflash'`
/// and `tidb_allow_mpp=1` are set; each join key type picks its own exchange
/// shapes through live MPP costing.
#[test]
#[ignore = "go-parity-gap: needs RunTestUnderCascadesWithDomain (mock-store + TiFlash replica meta injection), ANALYZE stats, session isolation variables and the integration_suite golden plans -- none of this surface exists in tidb-planner"]
fn mpp_join_star_schema_golden_plans_under_tiflash_isolation() {}

/// GO PORT of `pkg/planner/core/casetest/mpp/mpp_test.go:78 TestMPPExchangeSender`.
///
/// Re-derived contract (inline golden, issue 36194): for table t with a TiFlash
/// replica, `explain format='plan_tree' select /*+ read_from_storage(tiflash[t]) */ *
/// from t where a + 1 > 20 limit 100` prints Limit above a root TableReader whose
/// child is an mpp[tiflash] ExchangeSender(PassThrough) wrapping a Selection over
/// TableFullScan — i.e. the filter runs below the exchange on TiFlash and the
/// TableReader explains "MppVersion: 3".
#[test]
#[ignore = "go-parity-gap: needs hint-driven storage selection plus physical plan construction and plan_tree printing of ExchangeSender/Limit stacks"]
fn mpp_exchange_sender_places_limit_pushdown_above_passthrough_exchange() {}

/// GO PORT of `pkg/planner/core/casetest/mpp/mpp_test.go:95 TestMPPLeftSemiJoin`.
///
/// Re-derived contract: left semi join against
/// `t(a int not null, b int null)` with `tidb_allow_mpp=1;
/// tidb_enforce_mpp=1` and an injected TiFlash replica must match the
/// integration_suite goldens including per-statement warning snapshots; rows
/// beginning `set`/`UPDATE` execute as statements rather than being planned.
#[test]
#[ignore = "go-parity-gap: enforced-MPP planning with statement-warning capture needs the live session pipeline and golden book"]
fn mpp_left_semi_join_golden_with_enforce_mpp_and_warnings() {}

/// GO PORT of `pkg/planner/core/casetest/mpp/
/// mpp_test.go:139 TestMPPOuterJoinBuildSideForBroadcastJoin`.
///
/// Re-derived contract: two-row build vs three-row probe sides with high
/// broadcast thresholds (`tidb_broadcast_join_threshold_size/count = 10000`)
/// and `tidb_opt_mpp_outer_join_fixed_build_side=0` let the cost model pick
/// broadcast joins whose build side follows row counts; the golden book pins
/// the chosen side for every outer-join variant.
#[test]
#[ignore = "go-parity-gap: outer-join build-side selection by broadcast threshold costs lives in unported findBestTask MPP paths needing analyze stats + replicas"]
fn mpp_outer_join_build_side_for_broadcast_follows_row_counts() {}

/// GO PORT of `pkg/planner/core/casetest/mpp/
/// mpp_test.go:176 TestMPPOuterJoinBuildSideForShuffleJoinWithFixedBuildSide`.
///
/// Re-derived contract: with both thresholds zeroed and
/// `tidb_opt_mpp_outer_join_fixed_build_side=1`, every outer join keeps its
/// declared (left-for-left/right-for-right) shuffle build side even though the
/// other side is smaller.
#[test]
#[ignore = "go-parity-gap: fixed-build-side override inside unported MPP shuffle join planning"]
fn mpp_outer_join_fixed_build_side_forces_shuffle_build_side() {}

/// GO PORT of `pkg/planner/core/casetest/mpp/
/// mpp_test.go:212 TestMPPOuterJoinBuildSideForShuffleJoin`.
///
/// Re-derived contract: the same outer joins as the fixed-build-side case but
/// with `tidb_opt_mpp_outer_join_fixed_build_side=0`, where the cost model may
/// flip each shuffle join's build side to the smaller input.
#[test]
#[ignore = "go-parity-gap: cost-driven build-side choice in unported MPP shuffle join planning"]
fn mpp_outer_join_shuffle_build_side_flips_when_cost_prefers() {}

/// GO PORT of `pkg/planner/core/casetest/mpp/mpp_test.go:249 TestMPPShuffledJoin`.
///
/// Re-derived contract: doubled fact/dimension row counts with thresholds
/// pinned to 1 force hash-partitioned (shuffled) joins across int/decimal/date
/// keys; integration_suite pins every resulting plan tree.
#[test]
#[ignore = "go-parity-gap: threshold-to-shuffle decisions need the unported MPP task/cost machinery plus golden plans"]
fn mpp_shuffled_join_hash_exchanges_pinned_by_thresholds() {}

/// GO PORT of `pkg/planner/core/casetest/mpp/
/// mpp_test.go:303 TestMPPJoinWithCanNotFoundColumnInSchemaColumnsError`.
///
/// Re-derived contract: joining decimal(20,2)-wide with decimal(10,2)-narrow
/// schemas under enforce-mpp exercises the resolve path where a pruned column
/// used to raise `Can't find column Column#N in schema Column: [...]`;
/// integration_suite now pins the healthy plans the fix produces.
#[test]
#[ignore = "go-parity-gap: whole-plan goldens over enforce-MPP column pruning; the error surface itself lives in unported exchange resolution"]
fn mpp_join_decimal_width_gap_does_not_lose_columns_from_schema() {}

/// GO PORT of `pkg/planner/core/casetest/mpp/
/// mpp_test.go:345 TestMPPWithHashExchangeUnderNewCollation`.
///
/// Re-derived contract: utf8mb4_general_ci vs utf8mb4_bin char-keyed indexed
/// tables with `tidb_hash_exchange_with_new_collation=1` decide whether hash
/// exchanges on string columns are permitted per collation; goldens record
/// which keys become broadcasts instead.
#[test]
#[ignore = "go-parity-gap: collation gating of hash exchanges happens in unported MPP exchange resolution"]
fn mpp_hash_exchange_allowed_when_new_collation_flag_set() {}

/// GO PORT of `pkg/planner/core/casetest/mpp/
/// mpp_test.go:384 TestMPPWithBroadcastExchangeUnderNewCollation`.
///
/// Re-derived contract: broadcast joins are legal regardless of collation, so
/// plain utf8mb4_bin string keys keep their broadcast plans; golden-pinned.
#[test]
#[ignore = "go-parity-gap: same missing live-planning surface as the hash-exchange sibling"]
fn mpp_broadcast_exchange_ignores_new_collation_gate() {}

/// GO PORT of `pkg/planner/core/casetest/mpp/mpp_test.go:415 TestMPPAvgRewrite`.
///
/// Re-derived contract: AVG over a decimal(10,2) column pushed to TiFlash is
/// rewritten to SUM/COUNT pairs before crossing an exchange; integration_suite
/// pins the rewritten projection shapes.
#[test]
#[ignore = "go-parity-gap: avg-to-sum/count rewrite under MPP pushdown needs the unported aggregation split rule chain"]
fn mpp_avg_rewrite_pushes_sum_count_pair_over_exchange() {}

/// GO PORT of `pkg/planner/core/casetest/mpp/mpp_test.go:446 TestMppUnionAll`.
///
/// Re-derived contract: UNION ALL over two TiFlash-replica tables under
/// `tidb_enforce_mpp=1` plans every branch into a TiFlash pass-through read;
/// golden-pinned.
#[test]
#[ignore = "go-parity-gap: union branches inside the unported MPP costing loop"]
fn mpp_union_all_branches_read_through_pass_through_exchanges() {}

/// GO PORT of `pkg/planner/core/casetest/mpp/mpp_test.go:478 TestMppJoinDecimal`.
///
/// Re-derived contract: mixed decimal(8,5)/decimal(9,5)/decimal(40,20) scales
/// joining at threshold-1 broadcast sizes; goldens pin each exchange type the
/// decimal comparisons admit.
#[test]
#[ignore = "go-parity-gap: decimal-keyed join costing + exchange selection unported"]
fn mpp_join_decimal_scales_keep_their_exchange_types() {}

/// GO PORT of `pkg/planner/core/casetest/mpp/
/// mpp_test.go:514 TestMppJoinExchangeColumnPrune`.
///
/// Re-derived contract: five-column integer fact joined to one-column tt
/// proves exchange senders only ship the join keys (column pruning below
/// exchanges); goldens pin the narrowed schemas.
#[test]
#[ignore = "go-parity-gap: exchange-level column pruning interacts with the unported physical optimizer driver"]
fn mpp_join_exchange_only_ships_pruned_join_columns() {}

/// GO PORT of `pkg/planner/core/casetest/mpp/
/// mpp_test.go:551 TestMppFineGrainedJoinAndAgg`.
///
/// Re-derived contract: failpoint-injected store instances
/// (`infoschema/mockStoreServerInfo` returning a tiflash+tikv instance list)
/// and `planner/core/mockTiFlashStreamCountUsingMinLogicalCores` returning "8"
/// streams make fine-grained shuffle joins/aggs appear when
/// `tiflash_fine_grained_shuffle_stream_count` derives from logical cores; the
/// goldens pin the fine-grained operator layout.
#[test]
#[ignore = "go-parity-gap: fine-grained-shuffle stream derivation + failpoint-backed instance list need domain plumbing absent from tidb-planner (receiver only exposes the configured count)"]
fn mpp_fine_grained_join_and_agg_use_injected_stream_counts() {}

/// GO PORT of `pkg/planner/core/casetest/mpp/
/// mpp_test.go:600 TestMppAggTopNWithJoin`.
///
/// Re-derived contract: aggregate and TopN over a decimal(6,3) table with an
/// available TiFlash replica choose which stage (root vs mpp) each operator
/// lands on; integration_suite pins every arrangement.
#[test]
#[ignore = "go-parity-gap: agg/topn placement across MPP stages requires unported enforcement+costing"]
fn mpp_agg_topn_with_join_stage_placement_golden() {}

/// GO PORT of `pkg/planner/core/casetest/mpp/mpp_test.go:637 TestRejectSortForMPP`.
///
/// Re-derived contract: sorts that TiFlash cannot satisfy (char(128) ordering)
/// must be rejected/enforced back on root while still keeping MPP for the rest;
/// golden-pinned.
#[test]
#[ignore = "go-parity-gap: sort-property rejection over MPP property enforcement unported"]
fn mpp_reject_sort_keeps_violating_order_at_root() {}

/// GO PORT of `pkg/planner/core/casetest/mpp/
/// mpp_test.go:673 TestPushDownSelectionForMPP`.
///
/// Re-derived contract: with engines `'tiflash,tidb'`, selections distribute
/// between root and mpp[tiflash] scans depending on the predicate's
/// expressibility on TiFlash; goldens pin the split.
#[test]
#[ignore = "go-parity-gap: cross-engine selection distribution in unported physical planning"]
fn mpp_selection_push_down_splits_across_engines() {}

/// GO PORT of `pkg/planner/core/casetest/mpp/
/// mpp_test.go:710 TestPushDownProjectionForMPP`.
///
/// Re-derived contract: projections collapse toward the mpp scan so exchanges
/// carry minimal payloads; golden-pinned shape checks.
#[test]
#[ignore = "go-parity-gap: projection placement across exchanges unported"]
fn mpp_projection_push_down_shape_golden() {}

/// GO PORT of `pkg/planner/core/casetest/mpp/
/// mpp_test.go:746 TestPushDownAggForMPP`.
///
/// Re-derived contract: aggregates decompose into final/partial stages around
/// hash/broadcast exchanges at threshold-1 broadcast sizes; goldens pin the
/// two-phase layouts.
#[test]
#[ignore = "go-parity-gap: two-phase agg decomposition over exchanges unported"]
fn mpp_agg_push_down_two_phase_layout_golden() {}

/// GO PORT of `pkg/planner/core/casetest/mpp/mpp_test.go:782 TestMppVersion`.
///
/// Re-derived contract: `tidb_mpp_version` values gate exchange operator
/// availability and warn ("The version ... invalid") while invalid settings
/// fall back; the issue 52828 block additionally forces a MAX-with-group-by
/// over an IN-subquery join to execute without erroring, proving the MppVersion
/// machinery does not regress it.
#[test]
#[ignore = "go-parity-gap: session-variable gated exchange versions + executor round trip unported"]
fn mpp_version_gates_exchanges_and_warns() {}

/// GO PORT of `pkg/planner/core/casetest/mpp/
/// mpp_test.go:852 TestMPPJoinWithRemoveUselessExchange`.
///
/// Re-derived contract: four primary-key tables whose FK-less self-joins are
/// single-partition show the useless-exchange removal rule dropping redundant
/// exchanges while keeping ones carrying value semantics; goldens include
/// per-plan warnings.
#[test]
#[ignore = "go-parity-gap: remove-useless-exchange rewrite belongs to the unported MPP post-processing"]
fn mpp_join_with_remove_useless_exchange_golden() {}

/// GO PORT of `pkg/planner/core/casetest/mpp/
/// mpp_test.go:906 TestMPPJoinWithoutUselessExchange`.
///
/// Re-derived contract: identical star schema (t1..t3 decimals plus
/// dims/fact_t) but WITHOUT the removal variable enabled, so the same queries
/// keep those exchanges; the differing goldens document exactly what the flag
/// changes.
#[test]
#[ignore = "go-parity-gap: same missing surface as its WithRemoveUselessExchange sibling"]
fn mpp_join_without_remove_useless_exchange_golden() {}

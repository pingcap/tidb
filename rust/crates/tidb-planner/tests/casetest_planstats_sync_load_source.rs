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

//! Documentary gap ports for `pkg/planner/core/casetest/planstats/`
//! (`pkg/planner.part8`, items 465-473 of all 1278 `Test*`/`Benchmark*`
//! declarations under `pkg/planner/` on `origin/master`, sorted by file path
//! then line).
//!
//! Item 465 (`planstats/main_test.go:29 TestMain`) has no Rust test here: it
//! is bootstrap-only - testsetup.SetupForCommonTest, goleak filter list,
//! loads the plan_stats_suite book and wires GenerateOutputIfNeeded -
//! matching the crate's established skipped-reason treatment for TestMain
//! bootstrap (see part7's plancache main_test handling). Items 466-473 all
//! require a Domain-backed StatsHandle: analyze-v2 histograms, lease-based
//! sync/async stats loading with queues and failpoints, stats-cache eviction
//! capacity, and explain rendering of partially loaded stats. None of that
//! machinery exists in the Rust workspace; the ported claims below are kept
//! verbatim as `#[ignore]` gap ports, never approximated.

/// GO PORT of `planstats/plan_stats_test.go:48 TestPlanStatsLoad`.
///
/// Re-derived contract: analyze-version-2 tables t(a int PK, b int, c int,
/// d int, key idx(b)) and range-partitioned pt seeded then analyzed under
/// lease=1 (:56-70); after StatsHandle.Clear+Update each case re-plans via
/// `planner.Optimize` (:116-128) and asserts per operator where FULL column
/// stats landed (countFullStats = histogram len + topN num, :320-330):
/// TableReader loads c but not b (:79-84); partitioned plans load c into
/// EVERY PhysicalUnionAll child (:85-93); join/Apply/>ANY/in/not-in/exists/
/// not-exists arms pin the reader children carrying d or c stats (:98-165);
/// recursive-CTE seed projection's TableReader carries c (:171-183); USE
/// INDEX(idx) IndexLookUp requires pis.StatsInfo().HistColl.GetIdx(1)
/// IsEssentialStatsLoaded (:188-190). Each case clears+updates the stats
/// cache then re-plans via planner.Optimize (:194-211). Issue 48257 tail pins explain
/// "TableReader/data:TableFullScan ... stats:pseudo" presence across sync vs
/// async (sync_wait=0) loads and tidb_opt_objective determinate/moderate
/// flips for t_issue48257/t1_issue48257 (:161-238).
#[test]
#[ignore = "go-parity-gap: Domain StatsHandle lease/sync-load + planner.Optimize stats placement unported"]
fn plan_stats_load_places_full_column_stats_per_physical_operator() {}

/// GO PORT of `planstats/plan_stats_test.go:280 TestPlanStatsLoadForCTE`.
///
/// Re-derived contract: same t + pt setup as TestPlanStatsLoad (:286-308);
/// replays the plan_stats_suite book inputs against live execution
/// (:310-325) - CTE-flavored queries' recorded Result rows must match exactly
/// under cascades x caller variants.
#[test]
#[ignore = "go-parity-gap: plan_stats_suite golden results over executed CTE queries need sessions"]
fn plan_stats_load_for_cte_golden_rows_match_book() {}

/// GO PORT of `planstats/plan_stats_test.go:354 TestPlanStatsLoadTimeout`.
///
/// Re-derived contract: config forces StatsLoadConcurrency=-1 (no worker) +
/// queue size 1 (:334-339); session sets tidb_stats_load_sync_wait=1
/// (:352); one NeededItemTask pre-fills the size-1 channel via AppendNeededItem
/// so sync wait times out immediately (:366-375): with global
/// tidb_stats_load_pseudo_timeout=false `planner.Optimize` ERRORS for the
/// timing-out query (:378-381); with pseudo_timeout=true plus failpoint
/// assertSyncStatsFailed the statement executes fine (:384-387), failpoint
/// assertSyncWaitFailed passes issue 50872's arm (:389-392), and the final
/// optimize yields a PhysicalTableReader whose HistColl countFullStats is 0
/// for both probed columns - pseudo stats fallback (:394-400).
#[test]
#[ignore = "go-parity-gap: stats sync-load timeout queue + pseudo fallback failpoints unported"]
fn plan_stats_load_timeout_errors_then_falls_back_to_pseudo_per_pseudo_timeout() {}

/// GO PORT of `planstats/plan_stats_test.go:420 TestPreparedPlanCacheInvalidatedAfterSyncLoadTimeoutFallback`.
///
/// Re-derived contract: concurrency=-1/queue=1 config again (:421-429);
/// session enables prepared plan cache + tidb_plan_cache_invalidation_on_fresh_stats
/// + sync_wait=1 (:430-434); before b's histogram loads colBStats reports
/// IsAllEvicted (:445-449); first two executes set StmtCtx.IsSyncStatsFailed,
/// warn, keep SessionPlanCache EMPTY (Size()==0) and last_plan_from_cache 0
/// (:451-462); LoadNeededHistograms flips colBStats to IsFullLoad (:464-468);
/// next execute still misses (invalidation on fresh stats, cache Size()==1,
/// :470-472); only the FOLLOWING execute hits ("1", :474-475).
#[test]
#[ignore = "go-parity-gap: evicted->fresh stats invalidation of prepared cache entries needs full stacks"]
fn prepared_plan_cache_invalidated_after_sync_load_timeout_fresh_stats_refill() {}

/// GO PORT of `planstats/plan_stats_test.go:494 TestPlanStatsStatusRecord`.
///
/// Re-derived contract: EnableStatsCacheMemQuota=true globally (:495-498);
/// after a warmed select, StmtCtx.RecordedStatsLoadStatusCnt()==0 (:490);
/// shrinking the stats cache capacity to 1 via SetStatsCacheCapacity(1)
/// (:492) makes the NEXT identical select record every index/column load
/// status as literally "allEvicted" in GetUsedStatsInfo (:497-502).
#[test]
#[ignore = "go-parity-gap: mem-quota eviction statuses recorded into StmtCtx used-stats need stats handle"]
fn plan_stats_status_record_marks_all_evicted_after_capacity_shrink() {}

/// GO PORT of `planstats/plan_stats_test.go:524 TestCollectDependingVirtualCols`.
///
/// Re-derived contract: tables t (three expression indexes over json casts)
/// and t1 (virtual chain vab=a+b, vc=c-5, vvc=b-vc, vvabvvc=vab*vvc plus
/// index expressions ib/icvab/ivvcvab, :511-526) feed neededItems built from
/// the plan_stats_suite book input columns through FindPublicColumnByName
/// (:553-558) then rule.CollectDependingVirtualCols (:561); output col IDs
/// are mapped
/// back to names, sorted, and must equal the book's OutputColNames exactly -
/// direct dependencies only (vvc not collected when only b needed), virtual
/// columns over multi-column dependencies collected once.
#[test]
#[ignore = "go-parity-gap: rule.CollectDependingVirtualCols has no Rust port anywhere in the workspace"]
fn collect_depending_virtual_cols_direct_dependencies_match_book() {}

/// GO PORT of `planstats/plan_stats_test.go:594 TestStatsAnalyzedInDDL`.
///
/// Re-derived contract: tidb_stats_update_during_ddl=1 session var (:584);
/// the plan_stats_suite book alternates select-explain rows with DDL rows
/// (add/drop/modify index & columns on idx_c/idx_bc); after every explain the
/// mysql.stats_histograms version for the exercised index is compared via the
/// getHistID lookup (:606-620): consecutive selects keep the SAME version
/// while any intervening DDL forces a DIFFERENT (re-analyzed) version
/// (:641-652) - DDL-time analysis bumps visible stat versions deterministically.
#[test]
#[ignore = "go-parity-gap: DDL-triggered stats version bumps read from mysql.stats_histograms need Domain"]
fn stats_analyzed_in_ddl_reanalyzed_index_versions_bump_across_ddl_only() {}

/// GO PORT of `planstats/plan_stats_test.go:674 TestPartialStatsInExplain`.
///
/// Re-derived contract: analyzed t/t2/range-partitioned tp at lease=1 then
/// sync-wait disabled (:680-689); with tidb_stats_load_sync_wait=0 explains mark partially
/// loaded operators as "stats:partial[" (tp b=10 case :707-716) and fully
/// loaded ones must NOT print partial markers across the listed explainCase
/// contains/notContains matrix (:706-end incl. pseudo-vs-loaded splits for
/// new partitions).
#[test]
#[ignore = "go-parity-gap: partial-stats explain rendering needs lease-aware stats loading"]
fn partial_stats_in_explain_contains_and_not_contains_matrix() {}

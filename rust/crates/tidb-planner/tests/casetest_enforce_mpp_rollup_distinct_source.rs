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

//! Documentary gap ports for the remainder of
//! `pkg/planner/core/casetest/enforcempp` (`pkg/planner.part4` items 181–187
//! on `origin/master`; part3's sibling module
//! `casetest_enforce_mpp_suite_source.rs` already covers items 174–180 of the
//! same Go file).
//!
//! All seven tests drive whole EXPLAIN/warning goldens out of the
//! `enforce_mpp_suite` book through a live mock-TiFlash session whose table
//! meta carries an available TiFlash replica. Plan rows for `explain`/`desc`
//! inputs are normalized by rewriting every `_[0-9]+` node-id suffix to `_N`
//! (enforce_mpp_test.go:30-46 helper `checkEnforceMPPPlanRows`), and session
//! warnings must equal the book exactly. None of this surface exists here:
//! the crate has no TiFlash-replica meta injection, no MPP task generation,
//! three-stage distinct-agg shaping, shared-CTE execution, rollup MPP rules,
//! or a per-statement warning sink. The package bootstrap
//! `enforcempp/main_test.go:29 TestMain` is item —: skipped-reason bootstrap
//! (loads the `enforce_mpp_suite` book, goleak), documented in the receipt.

/// GO PORT of `pkg/planner/core/casetest/enforcempp/enforce_mpp_test.go:427
/// TestMPPSingleDistinct3Stage`.
///
/// Re-derived contract: over t(a int, b bigint not null, c bigint,
/// d date, e varchar(20) collate utf8mb4_general_ci) carrying one available
/// TiFlash replica assigned straight onto `tbl.Meta().TiFlashReplica`, every
/// suite input must reproduce its plan rows (`set`/`UPDATE` entries are
/// executed, not compared) with warnings matched verbatim; the file-level
/// helpers pin that single count-distinct aggregates lower into a THREE-stage
/// MPP aggregation chain.
#[test]
#[ignore = "go-parity-gap: needs live MPP costing with TiFlash replica meta injection and the three-stage distinct-agg physical shaping"]
fn mpp_single_distinct_three_stage_golden() {}

/// GO PORT of `pkg/planner/core/casetest/enforcempp/enforce_mpp_test.go:474
/// TestMPPMultiDistinct3Stage`.
///
/// Under `tidb_opt_enable_three_stage_multi_distinct_agg=1`,
/// `tidb_isolation_read_engines="tiflash"`, `tidb_enforce_mpp=1`,
/// `tidb_allow_mpp=ON`, table split BETWEEN (0) AND (5000) REGIONS 5 and ten
/// duplicated inserted rows, the three-stage lowering of MULTIPLE distinct
/// aggregates (plus mixed sum(c)) must reproduce the book rows; the source
/// comment (:469-470) records that a post-resolveIndices optimization may
/// inject another projection below the agg without changing output names.
#[test]
#[ignore = "go-parity-gap: multi-distinct three-stage MPP rewrite lives only inside the unported optimizer/implementation set"]
fn mpp_multi_distinct_three_stage_golden() {}

/// GO PORT of `pkg/planner/core/casetest/enforcempp/enforce_mpp_test.go:521
/// TestMPPNullAwareSemiJoinPushDown`.
///
/// NULL-aware anti/semi joins between t(a,b,c) and s(a,b,c), both given
/// TiFlash replicas through `alter table .. set tiflash replica 1` followed
/// by `UpdateTableReplicaInfo(.., true)` (the DDL-path meta variant instead
/// of direct assignment), must reproduce their MPP pushdown plans and
/// warnings from the book.
#[test]
#[ignore = "go-parity-gap: null-aware semi-join MPP pushdown decisions and their warnings are unported"]
fn mpp_null_aware_semi_join_push_down_golden() {}

/// GO PORT of `pkg/planner/core/casetest/enforcempp/enforce_mpp_test.go:568
/// TestMPPSharedCTEScan`.
///
/// With `tidb_enforce_mpp='on'` and
/// `tidb_opt_enable_mpp_shared_cte_execution='on'`, plans over t/s plus
/// TPC-H-shaped part/orders tables (all with replicas enabled through
/// UpdateTableReplicaInfo) must show CTE consumers reading SHARED scans on
/// TiFlash per the book; runs against a plain mockstore session rather than
/// the cascades loop but uses the same `_N` row normalization.
#[test]
#[ignore = "go-parity-gap: shared-CTE MPP execution planning and the mpp_shared_cte_execution switch are unported"]
fn mpp_shared_cte_scan_golden() {}

/// GO PORT of `pkg/planner/core/casetest/enforcempp/enforce_mpp_test.go:640
/// TestRollupMPP`.
///
/// First pins the exact rejection of grouping a column outside its GROUP BY
/// under ROLLUP: explaining `... GROUP BY country, country, product WITH
/// ROLLUP ORDER BY grouping(year)` must fail with
/// `[planner:3602]Argument #0 of GROUPING function is not in GROUP BY`
/// (:686-687). Then, with `tidb_enforce_mpp='on'` and
/// `TiFlashFineGrainedShuffleStreamCount=-1`, GROUP BY ROLLUP aggregates over
/// sales(year, country, product, profit) must produce their MPP plans and
/// warnings from the book.
#[test]
#[ignore = "go-parity-gap: ROLLUP grouping-set planning plus the GROUPING argument check have no end-to-end owner in this crate"]
fn rollup_mpp_grouping_argument_and_plans_golden() {}

/// GO PORT of `pkg/planner/core/casetest/enforcempp/enforce_mpp_test.go:696
/// TestEnforceMPPNewest`.
///
/// Regression window over t1(a int primary key)/t2(a int primary key) with
/// `testkit.SetTiFlashReplica` injections: whatever the current newest
/// enforcement decisions are, they are FROZEN by the book — every input's
/// plan rows (normalized) and warnings must keep matching.
#[test]
#[ignore = "go-parity-gap: needs the live MPP-enforcement decision loop over primary-key tables"]
fn enforce_mpp_newest_cases_golden() {}

/// GO PORT of `pkg/planner/core/casetest/enforcempp/enforce_mpp_test.go:727
/// TestReadCommittedWithTiflash`.
///
/// Under `tx_isolation="READ-COMMITTED"` and
/// `tidb_isolation_read_engines="tidb,tiflash"` inside a transaction: the
/// hinted `/*+ set_var(tidb_enforce_mpp=on) */` join explains as a full MPP
/// tree — TableReader(ExchangeSender(PassThrough(HashJoin … broadcast build
/// over TableRangeScan range:[1,1],[2,2], probe TableFullScan with pushed
/// `in(test.t2.b, 1, 2), not(isnull(test.t2.b))`))), MppVersion: 3 — while
/// the UNHINTED same query keeps a root HashJoin whose build side is
/// Batch_Point_Get handle:[1 2] and whose probe side reads TiFlash, then the
/// transaction commits.
#[test]
#[ignore = "go-parity-gap: set_var hint evaluation, READ-COMMITTED txn context and MPP broadcast-join rendering are unported"]
fn read_committed_isolation_keeps_root_hashjoin_unless_enforced_via_hint() {}

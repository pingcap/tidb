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

//! Documentary gap ports for `pkg/planner/core/casetest/pushdown/`
//! (`pkg/planner.part8`, items 474-480 of all 1278 `Test*`/`Benchmark*`
//! declarations under `pkg/planner/` on `origin/master`, sorted by file path
//! then line).
//!
//! Item 474 (`pushdown/main_test.go:29 TestMain`) has no Rust test here: it is
//! bootstrap-only - testsetup.SetupForCommonTest, goleak filters, loads the
//! integration_suite book consumed by every test in the package - matching the
//! crate's skipped-reason treatment for TestMain bootstrap. Items 475-480 run
//! under `testkit.RunTestUnderCascades(WithDomain)`: each injects a VIRTUAL
//! TiFlash replica into table metadata (`model.TiFlashReplicaInfo{Count:1,
//! Available:true}`), constrains isolation-read engines / mpp-allow /
//! tiflash-cop session vars, and replays the integration_suite book comparing
//! exact explain-plan rows. TiFlash replica metadata, MPP/cop task generation
//! and plan rendering are unported in the Rust workspace; the contracts below
//! stay verbatim `#[ignore]` gap ports. The sibling entry of this Go file,
//! item 481 TestJoinNotSupportedByTiFlash, is already covered by
//! `casetest_pushdown_join_tiflash_source.rs` from batch part9.

/// GO PORT of
/// `pkg/planner/core/casetest/pushdown/push_down_test.go:29 TestPushDownToTiFlashWithKeepOrder`.
///
/// Re-derived contract: t(a int PK, b varchar(20)) with virtual TiFlash
/// replica (:37-42); tidb_allow_tiflash_cop=ON (:35), isolation engines
/// 'tiflash' + tidb_allow_mpp=0 (:46-47); every integration_suite input
/// must explain to its recorded Plan rows exactly (:48-58) - keep-order
/// aggregation/limit shapes push down to TiFlash cop readers instead of
/// degrading when MPP is banned.
#[test]
#[ignore = "go-parity-gap: TiFlash cop reader planning + explain goldens need replica metadata and sessions"]
fn push_down_to_tiflash_with_keep_order_goldens_under_banned_mpp() {}

/// GO PORT of
/// `pkg/planner/core/casetest/pushdown/push_down_test.go:65 TestVirtualColumnIndexPushdown`.
///
/// Re-derived contract: t with virtual generated is_deleted over deleted_at
/// plus composite key k(id, is_deleted) (:68-71); inside a txn inserting one
/// row, `select 1 from t where id=1 and is_deleted=true` MUST contain an
/// IndexRangeScan plan operator (:72-74) - the virtual column resolves onto
/// the stored index columns for point-style access rather than blocking
/// index use.
#[test]
#[ignore = "go-parity-gap: MustHavePlan IndexRangeScan over virtual-column index access needs executed plans"]
fn virtual_column_index_pushdown_keeps_index_range_scan_access() {}

/// GO PORT of
/// `pkg/planner/core/casetest/pushdown/push_down_test.go:78 TestPushDownToTiFlashWithKeepOrderInFastMode`.
///
/// Re-derived contract: identical harness to TestPushDownToTiFlashWithKeepOrder
/// but with @@session.tiflash_fastscan=ON enabled first (:83); the same book
/// replays under fast-scan mode asserting unchanged explain output (:86-109) -
/// fast scan must not alter plan shapes.
#[test]
#[ignore = "go-parity-gap: tiflash_fastscan session gating + cop goldens unported"]
fn push_down_to_tiflash_with_keep_order_in_fast_mode_goldens_unchanged() {}

/// GO PORT of
/// `pkg/planner/core/casetest/pushdown/push_down_test.go:115 TestPushDownProjectionForTiFlash`.
///
/// Re-derived contract: t(id, value decimal(6,3), name char(128)) analyzed;
/// tidb_allow_mpp=OFF + tiflash cop ON (:121-123); SetTiFlashReplica injects
/// the virtual replica (:126); book inputs replay their recorded Plans
/// (:129-139) - projection expressions above TiFlash scans collapse into
/// pushed-down projections per the recorded shapes.
#[test]
#[ignore = "go-parity-gap: projection-pushdown shaping on TiFlash scans needs physical plan builder"]
fn push_down_projection_for_tiflash_golden_plans_after_analyze() {}

/// GO PORT of
/// `pkg/planner/core/casetest/pushdown/push_down_test.go:145 TestPushDownProjectionForTiFlashCoprocessor`.
///
/// Re-derived contract: wide table incl. real/datetime columns, two virtual
/// generated columns c/e (:147); tidb_opt_projection_push_down=1 (:151);
/// virtual replica injected (:154-160); WITHOUT the mpp-ban vars of sibling
/// tests, book explains pin which projections route to cop[tiflash]
/// coprocessor requests (:163-173) - covering keep-real-columns/
/// drop-virtual behavior under forced projection pushdown.
#[test]
#[ignore = "go-parity-gap: projection-to-cop request shaping needs tiflash task generation"]
fn push_down_projection_for_tiflash_coprocessor_golden_column_routing() {}

/// GO PORT of
/// `pkg/planner/core/casetest/pushdown/push_down_test.go:179 TestSelPushDownTiFlash`.
///
/// Re-derived contract: same t + replica + engines/mpp vars as
/// TestPushDownToTiFlashWithKeepOrder (:185-197); selection filters join the
/// pushed-down set - every book SQL explains exactly as recorded (:200-210),
/// pinning that WHERE predicates ride along to TiFlash readers instead of
/// staying root-level Selections.
#[test]
#[ignore = "go-parity-gap: predicate (selection) pushdown decisions over replicas need full planner"]
fn sel_push_down_tiflash_selection_rides_the_pushed_down_readers() {}

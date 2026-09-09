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

//! Ports for `pkg/planner/core/casetest/plan_test.go`
//! (`pkg/planner.part7`, items 394–404 of all 1278 `Test*`/`Benchmark*`
//! declarations under `pkg/planner/` on `origin/master`, sorted by file path
//! then line).
//!
//! Every entry drives whole statements through mock-store sessions:
//! RunTestUnderCascades executes each input, grabs the finished plan from
//! `tk.Session().ShowProcess()`, verifies `core.NormalizePlan(p)` equals
//! `core.NormalizeFlatPlan(core.FlattenPhysicalPlan(p, false))` in BOTH text
//! and digest, decodes the normalized form with
//! `plancodec.DecodeNormalizedPlan` and row-splits via the `getPlanRows`
//! helper (:27-30) against testdata books — or asserts plain execution plans
//! and results. The Rust crate ports none of that normalize/decode/session
//! pipeline; all eleven items are recorded gaps, none approximated. Two items
//! additionally self-skip in Go behind the next-gen kernel gate
//! (`kerneltype.IsNextGen()`), which has no Rust counterpart either.

/// GO PORT of `plan_test.go:49 TestPreferRangeScan`.
///
/// Re-derived contract: non-prepared plan cache disabled because it conflicts
/// with `tidb_opt_prefer_range_scan` (:52); table grown by eleven doubling
/// inserts to 2048 rows and analyzed (:54-67); `tidb_enable_chunk_rpc=on`
/// for stable stats explain (:70); per-case switch sets prefer_range_scan 0
/// then 1 (:91-94), resets PlanID, executes, and pins DECODED normalized-plan
/// rows (row-count equality only, compareStringSlice :32-37) after requiring
/// NormalizePlan equals NormalizeFlatPlan for both payload and digest
/// (:101-104).
#[test]
#[ignore = "go-parity-gap: NormalizePlan/NormalizeFlatPlan + DecodeNormalizedPlan over executed statements need the session stack"]
fn prefer_range_scan_normalize_flatplan_equivalence_goldens() {}

/// GO PORT of `plan_test.go:112 TestPreferRangeScanForDNF`.
///
/// Re-derived contract: fresh store, NO analyze (pseudo stats, :117);
/// with `tidb_opt_prefer_range_scan=1`: DNF arms of pure equalities choose
/// IndexLookUp ((a=1 and b=1) or (a=2 and b=2)); a THIRTY-term equality DNF
/// still chooses IndexLookUp; equal-plus-range arms like
/// (a=1 and b>0) or (a=2 and b<5) also IndexLookUp; ANY arm carrying NOT
/// (:135-137) or a range/mixed predicate (:139-141) falls back to
/// TableReader; flipping the flag OFF sends even the long equality DNF back
/// to TableReader (:144-148).
#[test]
#[ignore = "go-parity-gap: DNF access-path preference under pseudo stats needs the range-builder cost path"]
fn prefer_range_scan_dnf_index_lookup_only_for_pure_equality_arms() {}

/// GO PORT of `plan_test.go:207 TestNormalizedPlan`.
///
/// Re-derived contract: runs shared helper `testNormalizedPlan` (:157-205)
/// under the classic kernel only — self-skips when `kerneltype.IsNextGen()`
/// (:208-210). Helper contract: static prune mode; foreign-key schema t5/t6
/// with cascade actions (:166-171); per case reset PlanID, execute, require
/// NormalizePlan vs flat-plan equivalence AND that `GenHintsFromFlatPlan`
/// over the flat plan does not panic (:183-189), then compare decoded rows
/// against the book.
#[test]
#[ignore = "go-parity-gap: full normalized-plan decode pipeline needs executed plans from a live session"]
fn normalized_plan_decode_rows_goldens_classic_kernel() {}

/// GO PORT of `plan_test.go:214 TestNormalizedPlanForNextGen`.
///
/// Re-derived contract: SAME helper as TestNormalizedPlan but inverse kernel
/// gate — skipped unless `kerneltype.IsNextGen()` (:215-217). Only the
/// kernel type differs; the plan content asserted is identical. No Rust
/// kernel-type gate exists to mirror the skip.
#[test]
#[ignore = "go-parity-gap: same session pipeline as classic variant plus absent kerneltype gate"]
fn normalized_plan_nextgen_kernel_only_variant() {}

/// GO PORT of `plan_test.go:221 TestPlanDigest4InList`.
///
/// Re-derived contract: single-column table t(a) (:225-227): digests must be
/// EQUAL between `a in (1,2)` and `a in (1,2,3)` shapes whether the IN-list
/// is a WHERE filter OR a select-list projection (:231-234 executed pairwise
/// :237-252); issue 66623 arm extends to lengths two through eight with ALL
/// digests equal to the first (:254-277); issue 47634 arm repeats pairwise
/// digest equality for inner joins hinted inl_join(t4|t5) where only the
/// literal 1 vs 2 differs, over clustered-pk t4 and indexed t5 (:279-315).
#[test]
#[ignore = "go-parity-gap: NormalizePlan digests are produced by the live-session planner pipeline"]
fn plan_digest_identical_across_in_list_lengths_issues_66623_and_47634() {}

/// GO PORT of `plan_test.go:317 TestNormalizedPlanForDiffStore`.
///
/// Re-derived contract: t1(pk) populated and its meta hacked with an
/// AVAILABLE TiFlash replica directly (`tbl.Meta().TiFlashReplica =
/// &model.TiFlashReplicaInfo{Count:1, Available:true}` :330-333); inputs hit
/// different stores; per case digest must DIFFER from the previous case's
/// (:389 require.NotEqual) while decoded normalized rows match goldens and
/// flat-plan equivalence holds (:355-365).
#[test]
#[ignore = "go-parity-gap: hacked TiFlash replica metas + cross-store normalized plans unported"]
fn normalized_plan_diff_store_hacked_tiflash_replica_digests_goldens() {}

/// GO PORT of `plan_test.go:398 TestJSONPlanInExplain`.
///
/// Re-derived contract: runs helper `testJSONPlanInExplain` on the classic
/// kernel only (skip if next-gen :399-401); helper body (:407-441): two
/// indexed tables; inputs are explain tidb_json queries whose single JSON
/// cell is unmarshalled into a list of core.ExplainInfoForEncode and compared
/// FIELD BY FIELD — ID, EstRows, ActRows, TaskType, AccessObject,
/// OperatorInfo (:430-437) — against the book.
#[test]
#[ignore = "go-parity-gap: tidb_json explain output needs runtime explain collection during execution"]
fn json_plan_in_explain_encode_fields_goldens_classic_kernel() {}

/// GO PORT of `plan_test.go:405 TestJSONPlanInExplainForNextGen`.
///
/// Re-derived contract: identical helper with the inverse kernel gate
/// (skipped unless next-gen, :406-408). Behavior equals the classic twin.
#[test]
#[ignore = "go-parity-gap: same executor-explain surface plus absent kerneltype gate"]
fn json_plan_in_explain_nextgen_kernel_only_variant() {}

/// GO PORT of `plan_test.go:412 TestHandleEQAll`.
///
/// Re-derived contract: null-semantics pins across two tables (:415-434):
/// t1 rows (7,null),(5,1): 'm' = ALL(subquery over c2) IS NOT UNKNOWN selects
/// both rows regardless of index hints IGNORE_INDEX/USE_INDEX i1; scalar
/// (null = ALL(...)) IS NOT UNKNOWN answers 0 for t1 contents and equally
/// for t2 (7,null),(5,null), while the empty result rows confirm 'm'=ALL
/// evaluates unknown-not-false there; truncate+reload t2 as (7,null)x2:
/// c1 = ALL(select c1) yields 7,7 but c2 = ALL yields no rows; final block
/// t(c)=(1): `(not exists (select 1 from t)) <= all (select c from t)`
/// evaluates 1 in BOTH projection and WHERE positions AND must not produce a
/// TableDual plan (:436-441 MustNotHavePlan).
#[test]
#[ignore = "go-parity-gap: EQ-ALL subquery evaluation with index-hint forcing needs executor + hints"]
fn handle_eq_all_null_semantics_and_no_table_dual_pins() {}

/// GO PORT of `plan_test.go:521 TestCTEErrNotSupportedYet`.
///
/// Re-derived contract: builds pub_branch table plus UNION-ALL view
/// udc_branch_test (three SELECT arms including a correlated LIMIT-1
/// subselect and a _UTF8MB4 literal concat) plus clustered udc_branch_temp;
/// the recursive WITH over the view wrapped as derived table res MUST fail
/// with errno.ErrNotSupportedYet (MustGetErrCode at the end of the test):
/// recursive CTE inside that query shape is explicitly refused rather than
/// mis-planned.
#[test]
#[ignore = "go-parity-gap: recursive CTE through view/union planning errors need the plan builder"]
fn recursive_cte_over_union_view_err_not_supported_yet() {}

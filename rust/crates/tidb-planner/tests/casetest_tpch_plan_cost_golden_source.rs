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

//! Documentary gap ports for `pkg/planner/core/casetest/tpch`
//! (`pkg/planner.part9` items 522-540 on `origin/master`).
//!
//! Every query test loads the tpch_suite book, creates its sub-schema with an
//! injected TiFlash replica, optionally loads `test.<table>.json` histogram
//! stats, and compares either executed results or `explain format='cost_trace'`
//! / `'brief'` goldens; several additionally re-check cost columns via
//! `checkCost` (tpch_test.go:249-259, requiring identical id/estRows/estCost
//! prefixes across cost_trace and verbose explain). The Rust workspace has no
//! session/executor or TiFlash/MPP costing, so all are gap ports. Bootstrap
//! `main_test.go:32` TestMain (tpch_suite book + SafeWindow zeroing +
//! EnableStatsCacheMemQuota) is skipped-reason.

/// GO PORT of `pkg/planner/core/casetest/tpch/tpch_test.go:27 TestQ1`.
///
/// createLineItem (TiFlash replica'd); broadcast thresholds 0; per entry both
/// SQL and RESULT are book-recorded and checked (:35-43).
#[test]
#[ignore = "go-parity-gap: needs live planning+execution over TiFlash-replica'd lineitem"]
fn tpch_q1_result_golden() {}

/// GO PORT of `pkg/planner/core/casetest/tpch/tpch_test.go:54 TestQ2`.
///
/// Sets `tidb_default_string_match_selectivity=0.8` (:57), builds the
/// part/supplier/partsupp/nation/region five-way schema (:58-62) with loaded
/// json stats (:63-67), then pins every query through
/// `explain format='cost_trace'` AND `checkCost` cross-validates
/// id/estRows/estCost against the verbose explain (:249-259).
#[test]
#[ignore = "go-parity-gap: cost_trace rendering plus string-match selectivity injection unported"]
fn tpch_q2_cost_trace_golden() {}

/// GO PORT of `pkg/planner/core/casetest/tpch/tpch_test.go:91 TestQ3`.
///
/// Shared helper testQ3 (:103): customer/orders/lineitem schema; result rows,
/// `for update` lock-plan variants (:133) and an enforced-MPP repeat of the
/// `for update` plan (:134-136) are each compared (:139-141).
#[test]
#[ignore = "go-parity-gap: SELECT FOR UPDATE plan recording and enforce_mpp toggle need the runtime"]
fn tpch_q3_result_and_for_update_golden() {}

/// GO PORT of
/// `pkg/planner/core/casetest/tpch/tpch_test.go:97 TestQ3RCAndDisableTikv`.
///
/// The SAME testQ3 helper with enableRC=true: READ-COMMITTED txn (:109),
/// explicit begin (:110), and isolation engines restricted to `tidb,tiflash`
/// (:111) before replaying the identical golden cycle; committed at the end.
#[test]
#[ignore = "go-parity-gap: RC isolation + engine restriction interplay lives in the unported pipeline"]
fn tpch_q3_rc_and_disabled_tikv_golden() {}

/// GO PORT of `pkg/planner/core/casetest/tpch/tpch_test.go:149 TestQ4`.
///
/// orders+lineitem created (:152-153) with loaded lineitem/orders json stats
/// (:154-155); cost_trace goldens plus checkCost per entry.
#[test]
#[ignore = "go-parity-gap: cost_trace goldens need live planning over loaded stats"]
fn tpch_q4_cost_trace_golden() {}

/// GO PORT of `pkg/planner/core/casetest/tpch/tpch_test.go:179 TestQ5`.
///
/// Five-table nation/region/customer/orders/lineitem/supplier schema
/// (:182-187) with six loaded json stat files (:188-193); cost_trace goldens
/// plus checkCost.
#[test]
#[ignore = "go-parity-gap: same missing cost_trace/live-planning surface"]
fn tpch_q5_cost_trace_golden() {}

/// GO PORT of `pkg/planner/core/casetest/tpch/tpch_test.go:217 TestQ14`.
///
/// String-match selectivity pinned to 0.8 (:220) over lineitem/part created
/// (:221-222) with loaded lineitem stats (:223); entries compare
/// `explain format='brief'` rows.
#[test]
#[ignore = "go-parity-gap: brief-explain goldens need live optimize+cost"]
fn tpch_q14_brief_plan_golden() {}

/// GO PORT of `pkg/planner/core/casetest/tpch/tpch_test.go:261 TestQ9`.
///
/// Six-table part/supplier/partsupp/orders/nation/lineitem schema built
/// (:264-269) with `tidb_default_string_match_selectivity=0.8` (:272); no
/// external stats are loaded; cost_trace goldens plus checkCost.
#[test]
#[ignore = "go-parity-gap: same missing cost_trace surface as tpch_q4_cost_trace_golden"]
fn tpch_q9_cost_trace_golden() {}

/// GO PORT of `pkg/planner/core/casetest/tpch/tpch_test.go:295 TestQ13`.
///
/// customer/orders pair (:298-299) with selectivity 0.8 (:300); NO loaded
/// json stats; cost_trace goldens plus checkCost.
#[test]
#[ignore = "go-parity-gap: same missing cost_trace surface as tpch_q4_cost_trace_golden"]
fn tpch_q13_cost_trace_golden() {}

/// GO PORT of `pkg/planner/core/casetest/tpch/tpch_test.go:324 TestQ18`.
///
/// customer/orders/lineitem trio (:327-329, no json stats load);
/// selectivity defaults; cost_trace goldens plus checkCost.
#[test]
#[ignore = "go-parity-gap: same missing cost_trace surface as tpch_q4_cost_trace_golden"]
fn tpch_q18_cost_trace_golden() {}

/// GO PORT of `pkg/planner/core/casetest/tpch/tpch_test.go:353 TestQ21`.
///
/// selectivity 0.8 (:356); supplier/lineitem/orders/nation schema (:357-360)
/// with four loaded stat files (:361-364); cost_trace goldens plus checkCost.
#[test]
#[ignore = "go-parity-gap: same missing cost_trace surface as tpch_q4_cost_trace_golden"]
fn tpch_q21_cost_trace_golden() {}

/// GO PORT of `pkg/planner/core/casetest/tpch/tpch_test.go:388 TestQ22`.
///
/// selectivity 0.8 (:391) with a nation/customer/orders pairing (:392-393);
/// cost_trace goldens plus checkCost.
#[test]
#[ignore = "go-parity-gap: same missing cost_trace surface as tpch_q4_cost_trace_golden"]
fn tpch_q22_cost_trace_golden() {}

/// GO PORT of `pkg/planner/core/casetest/tpch/tpch_test.go:418
/// BenchmarkTPCHQ1`: bench-shaped Explain/brief loop over the Q1 query with
/// the full TPC-H fixture; excluded from the gate by `-E 'not test(/bench/)'`.
#[test]
#[ignore = "go-parity-gap: benchmark body plans the live fixture repeatedly"]
fn benchmark_tpch_q1_live_loop() {}

/// GO PORT of `pkg/planner/core/casetest/tpch/tpch_test.go:433
/// BenchmarkTPCHQ2`: same pattern over the Q2 five-way join.
#[test]
#[ignore = "go-parity-gap: benchmark body plans the live fixture repeatedly"]
fn benchmark_tpch_q2_live_loop() {}

/// GO PORT of `pkg/planner/core/casetest/tpch/tpch_test.go:454
/// BenchmarkTPCHQ3`: same pattern over the Q3 join.
#[test]
#[ignore = "go-parity-gap: benchmark body plans the live fixture repeatedly"]
fn benchmark_tpch_q3_live_loop() {}

/// GO PORT of `pkg/planner/core/casetest/tpch/tpch_test.go:472
/// BenchmarkTPCHQ4`: same pattern over the Q4 join.
#[test]
#[ignore = "go-parity-gap: benchmark body plans the live fixture repeatedly"]
fn benchmark_tpch_q4_live_loop() {}

/// GO PORT of `pkg/planner/core/catestest/tpch/tpch_test.go:488
/// BenchmarkTPCHQ21`: same pattern over the EXISTS/NOT-EXISTS Q21 join
/// (supplier,lineitem×3,orders,nation), asserting the daily-registered query
/// text stays plannable.
#[test]
#[ignore = "go-parity-gap: benchmark body plans the live fixture repeatedly"]
fn benchmark_tpch_q21_live_loop() {}

// GO PORT of `pkg/planner/core/casetest/tpch/tpch_test.go:507 TestBenchDaily`.
//
// Registers the five benchmarks above with `benchdaily.Run`, which without
// `-outfile` returns immediately (`pkg/util/benchdaily/bench_daily.go:67-69`)
// — nothing observable at unit-test time; skipped-reason.

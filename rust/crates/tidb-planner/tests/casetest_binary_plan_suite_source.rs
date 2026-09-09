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

//! Documentary gap ports for `pkg/planner/core/casetest/binaryplan`
//! (`pkg/planner.part3` items 123–130 on `origin/master`).
//!
//! Every test below drives a live TiDB through `testkit` (mock store +
//! session + executor), reads binary plans back out of
//! `information_schema.(slow_query|statements_summary)`, and decodes them
//! with base64+snappy into `tipb.ExplainData`. None of that machinery —
//! the session/executor stack, the slow-query log file writer, the
//! statement-summary memory store, or the `tipb.ExplainData` codec — is
//! ported into `tidb-planner`, so each ported claim stays `#[ignore]`d
//! and is NOT approximated. The package bootstrap
//! (`main_test.go:30 TestMain`) additionally loads the `binary_plan_suite`
//! testdata book and zeroes async-commit clock-drift config; it has no
//! behavior of its own to assert and is recorded as skipped-reason in the
//! batch receipt.

/// GO PORT of `pkg/planner/core/casetest/binaryplan/binary_plan_core_test.go:37
/// TestBinaryPlanSwitch`.
///
/// Re-derived contract: with `tidb_generate_binary_plan = 1` a
/// >=1s `select sleep(1)` produces a non-empty snappy-compressed base64
/// blob in BOTH `information_schema.slow_query.binary_plan` and
/// `information_schema.statements_summary.binary_plan`, each decoding to
/// `tipb.ExplainData`; with the variable `= 0` both rows are empty. The
/// same test also pins issue:41458's printed plan tree for a four-way
/// self join with `t3.a=1 and t2.a=2`: the operator column sequence is
/// Projection / HashJoin / HashJoin / HashJoin / IndexLookUp(IndexRangeScan+
/// Selection(TableRowIDScan)) x2 / TableReader(Selection(TableFullScan)) x2.
#[test]
#[ignore = "go-parity-gap: needs the live-testkit session/executor, slow-query log and statements_summary stores plus the tipb.ExplainData codec -- none ported into tidb-planner"]
fn binary_plan_switch_gates_slow_log_and_summary_payloads() {
    // Restore when those surfaces land: enable/disable the global variable,
    // decode both stored payloads after enablement, assert empty strings
    // after disablement, then re-check the issue-41458 join shape above the
    // statements_summary plan text.
}

/// GO PORT of `pkg/planner/core/casetest/binaryplan/binary_plan_core_test.go:148
/// TestTooLongBinaryPlan`.
///
/// Re-derived contract: for a query whose encoded binary plan exceeds
/// `stmtsummary.MaxEncodedPlanSizeInBytes` (1024*1024), the slow-query row
/// keeps the full payload (decodes to `ExplainData` with
/// `DiscardedDueToTooLong=false`, `WithRuntimeStats=true`,
/// `Main != nil`) while the statements_summary row records only the
/// `DiscardedDueToTooLong=true` marker with `Main == nil && Ctes == nil`.
/// The oversized plan comes from six-way self joins over 8192 hash
/// partitions in static prune mode.
#[test]
#[ignore = "go-parity-gap: needs live testkit + partitioned-stats planner execution and the tipb.ExplainData fields DiscardedDueToTooLong/WithRuntimeStats/Main/Ctes; the size gate itself lives in the unported stmtsummary/slow-log boundary"]
fn too_long_binary_plan_discarded_only_in_stmt_summary() {
    // Restore: run the 8192-partition six-way join, decode both payloads,
    // assert >MaxEncodedPlanSizeInBytes in the slow log, full Main there,
    // and the discarded-marker shape in statements_summary.
}

/// GO PORT of `pkg/planner/core/casetest/binaryplan/binary_plan_core_test.go:221
/// TestLongBinaryPlan`.
///
/// Re-derived contract: the identical six-way self-join query over 1000
/// hash partitions encodes BELOW `MaxEncodedPlanSizeInBytes`, so both the
/// slow-query and statements_summary rows carry byte-identical payloads
/// that decode to a full `ExplainData` (`DiscardedDueToTooLong=false`,
/// `WithRuntimeStats=true`, `Main != nil`). The Go comment fixes the escape
/// hatch if sizes drift: change the CREATE TABLE partition count, not the
/// assertion.
#[test]
#[ignore = "go-parity-gap: same missing machinery as too_long_binary_plan (live testkit planner + tipb.ExplainData codec); no Rust equivalent of MaxEncodedPlanSizeInBytes comparison exists"]
fn long_binary_plan_under_max_size_kept_in_both_stores() {
    // Restore: execute with sleep(0.3) so the slow log always catches it,
    // then require s1 < max and s1 == s2 across the two information_schema
    // tables.
}

/// GO PORT of
/// `pkg/planner/core/casetest/binaryplan/binary_plan_core_test.go:275
/// TestBinaryPlanOfPreparedStmt`.
///
/// Re-derived contract: the binary plan recorded for `execute stmt using @a`
/// over prepared `select sleep(1), b from t where a > ?` is non-empty,
/// carries runtime stats and a full `Main` in the slow query, and the
/// statements_summary payload equals it byte-for-byte — i.e. prepared-stmt
/// execution also materializes its binary plan like ordinary queries.
#[test]
#[ignore = "go-parity-gap: prepared-statement execution path (prepare/execute protocol, session plan cache) is not ported; decoding requires tipb.ExplainData"]
fn binary_plan_of_prepared_stmt_identical_in_both_stores() {
    // Restore: prepare "select sleep(1), b from t where a > ?", execute with
    // @a=20, compare the two stored payloads.
}

/// GO PORT of `pkg/planner/core/casetest/binaryplan/binary_plan_core_test.go:327
/// TestDecodeBinaryPlan`.
///
/// Re-derived contract: for 16 statements (plain/index-join/recursive-CTE
/// `explain analyze format = 'verbose'` plus static- and dynamic-prune
/// partition variants over a range-partitioned table), every non-empty cell
/// of the verbose EXPLAIN output must equal the corresponding field of
/// `tidb_decode_binary_plan(<slow-log binary_plan>)`, title rows stripped —
/// i.e. the SQL text renderer and the server-side decoder agree exactly.
#[test]
#[ignore = "go-parity-gap: EXPLAIN ANALYZE 'verbose' rendering, tidb_decode_binary_plan built-in and the running executor that produces runtime stats are all unported"]
fn decode_binary_plan_matches_verbose_explain_analyze_rows() {
    // Restore: iterate the 16 case statements, flatten the explain rows and
    // the decoded table rows to whitespace-trimmed tokens, and require them
    // equal per case.
}

/// GO PORT of
/// `pkg/planner/core/casetest/binaryplan/binary_plan_core_test.go:419
/// TestUnnecessaryBinaryPlanInSlowLog`.
///
/// Re-derived contract: with binary-plan generation left OFF and
/// `tidb_slow_log_threshold = 1`, creating a 100-partition hash table and
/// querying it must never write `tidb_decode_binary_plan('')` — the
/// placeholder for an empty binary plan — into the slow-log FILE bytes.
#[test]
#[ignore = "go-parity-gap: no slow-query log file writer exists on the Rust side (the format string the test greps for lives in pkg/util/logutil slow-log emitter)"]
fn unnecessary_binary_plan_never_lands_in_slow_log_text() {
    // Restore: set threshold 1, create/query the partitioned table, read the
    // temp slow-log file and require.NotContains the empty-plan call.
}

/// GO PORT of `pkg/planner/core/casetest/binaryplan/binary_plan_test.go:73
/// TestBinaryPlanInExplainAndSlowLog`.
///
/// Re-derived contract: driven by `RunTestUnderCascades` (both cascades and
/// classic callers) against the `binary_plan_suite` book, each EXPLAIN-input
/// row's returned `binary_plan` string must equal the slow-query row's, and
/// after `simplifyAndCheckBinaryOperator` (binary_plan_test.go:33)
/// simplification every scan-shaped operator (`(Table|Index).*Scan`,
/// `CTEFullScan`, `Point_Get`) must still have `AccessObjects`, root-task
/// operators keep `RootBasicExecInfo` and cop/MPP ones keep `CopExecInfo`
/// whenever `WithRuntimeStats` holds; exec-info, AccessObjects and
/// Memory/DiskBytes fields are zeroed before golden comparison.
#[test]
#[ignore = "go-parity-gap: casetest golden suite over live testkit output (+RunTestUnderCascades dual-caller) and the tipb.ExplainOperator tree are unported"]
fn binary_plan_in_explain_and_slow_log_golden_suite() {
    // Restore: load the binary_plan_suite inputs; per input compare EXPLAIN
    // binary_plan with the slow-log one, run simplifyAndCheckBinaryOperator,
    // and re-diff against the recorded simplified goldens.
}

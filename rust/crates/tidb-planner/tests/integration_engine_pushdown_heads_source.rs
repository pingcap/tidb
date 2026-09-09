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

//! Documentary gap ports for `pkg/planner/core/hint_test.go` TestHintSuite
//! (`pkg/planner.part10` item 584) and `pkg/planner/core/integration_test.go`
//! lines 47-860 (items 585-600 on `origin/master`). All are session-driven
//! integration tests (`testkit.RunTestUnderCascades(WithDomain)`, virtual
//! TiFlash replicas, EXPLAIN renderers or executed result goldens); each entry
//! below re-derives its pinned contract from the Go body.

/// GO PORT of `pkg/planner/core/hint_test.go:31 TestHintSuite`.
///
/// Subtest contracts (:34-…): `set_var(timestamp=…)` hints evaluate per-query
/// without leaking after statement end (:35-44); the same hint through a
/// SESSION BINDING still applies only while bound and reports FoundInBinding
/// (:46-63); set_var + bindings compose without overriding outer system vars
/// (:64-73); security-restricted policies IGNORE restricted binding hints with
/// an exact warning while still reporting binding use (:74-99);
/// `tidb_default_string_match_selectivity` round-trips through set_var hints,
/// session bindings, `show session bindings` serialization and drop-binding
/// (:100-130); set_var inside EXPLAIN keeps the OUTER variable readable
/// (:131-…).
#[test]
#[ignore = "go-parity-gap: set_var hints, bindings and session variables need the full executor stack"]
fn hint_setvar_and_binding_suite() {}

/// GO PORT of `pkg/planner/core/integration_test.go:47
/// TestNoneAccessPathsFoundByIsolationRead`.
///
/// Contract (:47-73): mysql.SystemDB tables are NOT filtered by
/// tidb_isolation_read_engines ('tiflash' still plans stats_meta) while user
/// table t errors exactly `[planner:1815]Internal : No access path for table
/// 't' … valid values can be 'tikv'`; restoring engines replans; INSTANCE
/// config changes never affect already-set isolation reads.
#[test]
#[ignore = "go-parity-gap: isolation-read engine filtering needs access-path planning over sessions"]
fn none_access_paths_found_by_isolation_read_error_shape() {}

/// GO PORT of `pkg/planner/core/integration_test.go:74 TestAggPushDownEngine`.
///
/// Contract (:74-107): approx_count_distinct pushes to TiFlash as nested
/// StreamAgg under batchCop when isolation reads tiflash (:86-91), and degrades
/// to root HashAgg over cop[tikv] full scan when switched back to tikv
/// (:93-101).
#[test]
#[ignore = "go-parity-gap: engine-dependent aggregate pushdown needs TiFlash metadata"]
fn agg_push_down_engine_selects_per_isolation_read() {}

/// GO PORT of `pkg/planner/core/integration_test.go:108 TestPartitionPruningForEQ`.
///
/// Contract (:108-131): for range partitions over weekday(a) (a NON-monotone
/// partition expression), ParseSimpleExpr("a = '2020-01-01 00:00:00'") fed to
/// partitionpruning.PartitionPruning returns exactly ONE partition, index 0 —
/// EQ prunes even without monotonicity.
#[test]
#[ignore = "go-parity-gap: partition pruning driver over PartitionedTable is unported"]
fn partition_pruning_eq_prunes_non_monotone_partition_expr() {}

/// GO PORT of `pkg/planner/core/integration_test.go:132 TestNotReadOnlySQLOnTiFlash`.
///
/// Contract (:132-158): under 'tiflash'-only isolation reads, SELECT FOR
/// UPDATE / INSERT…SELECT / prepared EXECUTE of an INSERT each fail with the
/// exact 1815 message including "check if the query is not readonly and sql
/// mode is strict".
#[test]
#[ignore = "go-parity-gap: non-read-only admission over engine isolation needs sessions"]
fn not_read_only_sql_on_tiflash_rejected() {}

/// GO PORT of `pkg/planner/core/integration_test.go:159 TestTimeToSecPushDownToTiFlash`.
///
/// Contract (:159-187): time_to_sec projects as mpp[tiflash] Projection above
/// TableFullScan under ExchangeSender PassThrough (MppVersion 3).
#[test]
#[ignore = "go-parity-gap: MPP plan_tree rendering needs TiFlash tier"]
fn time_to_sec_push_down_to_tiflash_plan() {}

/// GO PORT of `pkg/planner/core/integration_test.go:188 TestRightShiftPushDownToTiFlash`.
///
/// Same MPP shape for `a >> b` rendered as rightshift(test.t.a, test.t.b)
/// (:188-216).
#[test]
#[ignore = "go-parity-gap: MPP plan_tree rendering needs TiFlash tier"]
fn right_shift_push_down_to_tiflash_plan() {}

/// GO PORT of `pkg/planner/core/integration_test.go:217 TestBitColumnPushDown`.
///
/// Contract (:217-300): correlated subquery over BIT columns plans Apply +
/// StreamAgg + TopN whose results sort-match Go's rows; ascii(a)=65,
/// concat(binary,'A'), BINARY casts and CAST-as-char each push (or stay root)
/// per the recorded brief-explain rows; blacklisting 'bit' from tikv moves the
/// selection to root; collation-sensitive comparisons over bit(8) value 65
/// switch answer with NAMES utf8mb4_bin vs _general_ci.
#[test]
#[ignore = "go-parity-gap: pushdown-blacklist and collation legs need executed sessions"]
fn bit_column_push_down_paths_and_collation_legs() {}

/// GO PORT of `pkg/planner/core/integration_test.go:301 TestSysdatePushDown`.
///
/// Contract (:301-343): sysdate() stays unfolded in selections; setting GLOBAL
/// tidb_sysdate_is_now does NOT affect the existing session while NEW sessions
/// rewrite sysdate→now (failpoint-injected timestamp), and turning it OFF
/// restores.
#[test]
#[ignore = "go-parity-gap: global/session variable interplay with plan rendering needs sessions"]
fn sysdate_push_down_respects_sysdate_is_now_scoping() {}

/// GO PORT of `pkg/planner/core/integration_test.go:344 TestTimeScalarFunctionPushDownResult`.
///
/// Contract (:344-432): hour/month/minute/second/microsecond/dayName/
/// dayOfMonth/dayOfWeek/dayOfYear/Date/Week/time_to_sec-style predicates over
/// datetime literals must return the STORED row — execution-equivalence of
/// pushed temporal functions against literal evaluation.
#[test]
#[ignore = "go-parity-gap: result equivalence needs executed queries"]
fn time_scalar_function_push_down_result_equivalence() {}

/// GO PORT of `pkg/planner/core/integration_test.go:433 TestNumberFunctionPushDown`.
///
/// Contract (:433-502): mod/unhex/oct/sin/asin/cos/acos (and family) filters
/// match constant-computed comparands on signed/unsigned/double columns —
/// pushed numeric builtins execute equivalently to their constant folds.
#[test]
#[ignore = "go-parity-gap: numeric builtin execution needs the engine"]
fn number_function_push_down_results_match_constants() {}

/// GO PORT of `pkg/planner/core/integration_test.go:503 TestScalarFunctionPushDown`.
///
/// Contract (:503-630): right/mod-over-mixed-signedness (four quadrants)/
/// trig-over-cast-id/concat/json... projections render EXACTLY in analyze-brief
/// columns {id, operator-info} while staying pushed — pins per-signature
/// rendering strings like `mod(test.t.id2, test.t.id)` and
/// `sin(cast(test.t.id, double BINARY))`.
#[test]
#[ignore = "go-parity-gap: exact scalar-function rendering under analyze formats needs sessions"]
fn scalar_function_push_down_rendering_quadrants() {}

/// GO PORT of `pkg/planner/core/integration_test.go:631 TestReverseUTF8PushDownToTiFlash`.
///
/// reverse(varchar) projects mpp[tiflash]-side (:631-659).
#[test]
#[ignore = "go-parity-gap: MPP projection rendering needs TiFlash tier"]
fn reverse_utf8_push_down_to_tiflash() {}

/// GO PORT of `pkg/planner/core/integration_test.go:660 TestReversePushDownToTiFlash`.
///
/// reverse(binary(32)) keeps the same MPP shape (:660-688).
#[test]
#[ignore = "go-parity-gap: MPP projection rendering needs TiFlash tier"]
fn reverse_binary_push_down_to_tiflash() {}

/// GO PORT of `pkg/planner/core/integration_test.go:689 TestSpacePushDownToTiFlash`.
///
/// space(int column) projects mpp[tiflash]-side (:689-717).
#[test]
#[ignore = "go-parity-gap: MPP projection rendering needs TiFlash tier"]
fn space_push_down_to_tiflash() {}

/// GO PORT of `pkg/planner/core/integration_test.go:718 TestExplainAnalyzeDML2`.
///
/// Contract cases (:718-818): explain-analyze of DMLs renders runtime stats —
/// plain insert shows auto_id_allocator alloc_cnt: 1; an out-of-range insert
/// rebases (rebase_cnt: 1); null+rebase mixes both; insert-ignore counts 3
/// conflict-checks with the check_insert shape; on-duplicate-update shows
/// count: 2 allocations; replace variants allocate/rebase likewise; prepared
/// EXECUTE keeps the same shape. Each regex must match the analyze output.
#[test]
#[ignore = "go-parity-gap: DML runtime-stat explain needs txn/auto-id plumbing"]
fn explain_analyze_dml_runtime_stat_shapes() {}

/// GO PORT of `pkg/planner/core/integration_test.go:819 TestConflictReadFromStorage`.
///
/// Contract (:819-…): with dynamic pruning forced ON and range-columns
/// partitions over t, mixed READ_FROM_STORAGE hints naming tikv/tiflash PER
/// PARTITION — or whole-table on both sides — plan successfully but emit
/// exactly one `Warning 1815 Storage hints are conflict, you can only specify
/// one storage type of table test.t` warning.
#[test]
#[ignore = "go-parity-gap: conflicting storage-hint warnings need hint parsing over sessions"]
fn conflicting_read_from_storage_hints_warn_once() {}

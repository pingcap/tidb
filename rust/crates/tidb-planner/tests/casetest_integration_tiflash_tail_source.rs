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

//! Port ledger for the tail of `pkg/planner/core/casetest/
//! integration_test.go` (`pkg/planner.part5`, items 284–294 of all `Test*`/
//! `Benchmark*` declarations under `pkg/planner/` on `origin/master`,
//! sorted by file path then line).
//!
//! Family contract: EXPLAIN-shaped golden suites driven through
//! `testdata.LoadTestCases` books (the package's `integration.json`) under
//! the cascades/caller matrix from `RunTestUnderCascades(WithDomain)`,
//! exercising TiFlash isolation-read routing, MPP/fine-grained-shuffle
//! rendering, optimizer fix controls, and assorted regression queries.
//!
//! All eleven items are honest gap ports: this crate exposes planner data
//! structures and cost primitives only — no SQL optimize entry point, no
//! explain renderer, no TiFlash replica injection, no session variable
//! surface — so none of these goldens has an honest carrier here.

/// GO PORT of `integration_test.go:33 TestVerboseExplain`.
///
/// Re-derived contract: after seeding t1/t2/t3 (analyze all columns),
/// adding fake TiFlash replicas on t1/t2/t31240/first_range/partsupp/
/// supplier (:61-65/:82-83), inflating partsupp.RealtimeCount=800000 vs
/// supplier=10000 (:93-96) and pinning tidb_opt_limit_push_down_threshold=0,
/// every query in the integration.json book replays identical plan text;
/// also pins `set @@tidb_enable_chunk_rpc = on` stability note :53.
#[test]
#[ignore = "go-parity-gap: needs live optimize+verbose-explain renderer + tiflash replica injection + golden book"]
fn verbose_explain_golden_matrix_with_tiflash_replicas() {}

/// GO PORT of `integration_test.go:123 TestIsolationReadTiFlashNotChoosePointGet`.
///
/// Re-derived contract: with @@session.tidb_isolation_read_engines="tiflash"
/// over a PK table carrying an available TiFlash replica, the isolated plan
/// must NOT degrade to PointGet — golden book pairs pin the resulting
/// TiFlash TableFullScan/Reader shape (:137-141 loop).
#[test]
#[ignore = "go-parity-gap: needs isolation-read engine filtering + point-get path lowering + golden book"]
fn isolation_read_tiflash_never_lands_on_point_get() {}

/// GO PORT of `integration_test.go:155 TestIsolationReadDoNotFilterSystemDB`.
///
/// Re-derived contract: with tidb_isolation_read_engines="tiflash" globally,
/// queries against system-database memory tables still plan normally (no
/// "table has no replica / engine filtered" refusal) — golden book pins the
/// exempt set (:164-172).
#[test]
#[ignore = "go-parity-gap: needs system-schema planning + engine filter exemption + golden book"]
fn isolation_read_does_not_filter_system_database_tables() {}

/// GO PORT of `integration_test.go:177 TestMergeContinuousSelections`.
///
/// Re-derived contract: wide ts table (nullable + not-null char/varchar
/// columns, int PK) under MPP with a TiFlash replica; chained WHERE
/// predicates collapse into ONE Selection instead of stacked operators —
/// golden book pins merged trees (:192-200).
#[test]
#[ignore = "go-parity-gap: needs selection-merge rule inside end-to-end planning + golden book"]
fn merge_continuous_selections_collapse_stacked_filters() {}

/// GO PORT of `integration_test.go:206 TestTiFlashPartitionTableScan`.
///
/// Re-derived contract: under the `forceDynamicPrune` failpoint (:207), plus
/// dynamic prune mode + tiflash isolation + enforce-mpp + batch-cop=2,
/// range-partitioned rp_t and hash-partitioned hp_t both plan as TiFlash
/// partition-table scans with dynamic-pruning partition lists (golden book,
/// :233-241); drop-table cleanup afterwards.
#[test]
#[ignore = "go-parity-gap: needs dynamic-prune failpoint + mpp partition scan rendering + golden book"]
fn tiflash_partition_table_scan_dynamic_prune_goldens() {}

/// GO PORT of `integration_test.go:245 TestTiFlashFineGrainedShuffle`.
///
/// Re-derived contract: enforce-mpp TiFlash scans through window/hash-agg
/// pipelines record each book query TWICE — once with
/// @@tidb_redact_log=off
/// (identifiers visible) and once with redaction ON (identifiers replaced)
/// — and both outputs replay byte-identically per configuration, pinning
/// fine-grained shuffle operator rows AND their redacted twins (:262-277).
#[test]
#[ignore = "go-parity-gap: needs fine-grained shuffle costing/rendering + redact pipeline + golden book"]
fn tiflash_fine_grained_shuffle_redact_off_and_on_pairs() {}

/// GO PORT of `integration_test.go:281 TestFixControl43817`.
///
/// Re-derived contract: `select * from t1 where t1.a > (select max(a) from
/// t2)` evaluates the non-correlated subquery during optimization with NO
/// error by default (:284); with `set tidb_opt_fix_control="43817:on"` the
/// SAME statement fails with exactly "evaluate non-correlated sub-queries
/// during optimization phase is not allowed by fix-control 43817" (:288,
/// gate at pkg/executor/select.go:599-601 over
/// pkg/planner/util/fixcontrol/get.go:40); turning it off restores silence.
#[test]
#[ignore = "go-parity-gap: fix-control gate lives in unported executor select.go subquery path"]
fn fix_control_43817_gates_optimization_phase_subquery_evaluation() {}

/// GO PORT of `integration_test.go:294 TestFixControl45132`.
///
/// Re-derived contract: 128×7 duplicated rows vs one a=2 row analyzed; base
/// model prefers TableFullScan to avoid double lookups (:306); setting
/// fix-control 45132 to the measured ratio threshold (99) flips the plan to
/// IndexLookup (`EventuallyMustIndexLookup` :309), while 500 and 0 restore
/// TableFullScan (:312-315) — pins the skyline access-row-count knob's
/// monotonic boundary semantics (fixcontrol.Fix45132 const already exists
/// in this crate at src/fix_control.rs:40 but has no consumer surface).
#[test]
#[ignore = "go-parity-gap: skyline-pruning consumer of Fix45132 + analyze stats + plan-choice harness unported"]
fn fix_control_45132_ratio_threshold_flips_index_lookup_boundary() {}

/// GO PORT of `integration_test.go:323 TestTiFlashExtraColumnPrune`.
///
/// Re-derived contract: two-column t1 under tiflash isolation +
/// enforce-mpp; queries that read one column still materialize the extra
/// column needed downstream in the exchanged plan (golden book, :337-344) —
/// pins column pruning NOT dropping columns required across the exchange.
#[test]
#[ignore = "go-parity-gap: needs mpp exchange column tracking + explain renderer + golden book"]
fn tiflash_extra_column_prune_keeps_exchange_required_columns() {}

/// GO PORT of `integration_test.go:352 TestIndexMergeJSONMemberOf2FlakyPart`.
///
/// Re-derived contract: multivalued JSON index
/// iad(a, cast(d->'$.b' as signed array)) under analyze v2: forcing iad on a
/// plain `a = 1` predicate IGNORES the mv-index part (plain Selection over
/// TableFullScan, pinned verbatim :363-367) while adding
/// `2 member of (d->'$.b')` yields a union IndexMerge of IndexRangeScan
/// [1 2,1 2] + TableRowIDScan (:368-372).
#[test]
#[ignore = "go-parity-gap: needs member-of mv-index range building + plan_tree renderer"]
fn index_merge_json_member_of_mv_index_plan_tree_shapes() {}

/// GO PORT of `integration_test.go:375 TestIntegrationRegression`.
///
/// Re-derived contract: batched regressions each with a pinned observable:
/// RecordQPSbyDB config toggled safely via RestoreFunc (:377-381);
/// issue #46556 natural-join-over-view LIKE compiles to HashJoin above
/// zero-row TableDual (verbatim plan :388-395); issue #65325 CASE DEFAULT()
/// order-by returns empty without error :398; issue #67731 decimal-string
/// vs bigint equality `'9007199254740993' = 9007199254740992` is TRUE and
/// the join over them matches :400-409; issue #63949 MustUseIndex abcd
/// honored under tidb_inlj hint :411-413; issue #61669 deep aggregate view
/// join explains non-empty :415-467; issues #60076/#63314 leading-hint join
/// chains keep join keys under always_keep_join_key with verbatim trees
/// :472-508; issue #67366 int-vs-varchar PK join casts probe side and warns
/// `Warning 1105 Implicit type or collation conversion on join keys ...
/// may make indexes unusable` (producer: operator/logicalop/logical_join.go:1924)
/// :509-515; issue #66859 expression-index left join returns `-1 <nil>` row
/// :518-524.
#[test]
#[ignore = "go-parity-gap: nine independent executor+planner regressions need the full SQL stack"]
fn integration_regression_batch_pins_issue_plans_and_rows() {}

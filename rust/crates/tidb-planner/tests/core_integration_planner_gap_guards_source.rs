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

//! Port ledger for `pkg/planner/core/integration_test.go` mixed planner
//! guards (`pkg/planner.part11`, Go items 601, 602, 617, 618, 629, 631–636
//! and 639–640 on `origin/master`).
//!
//! Family contract: end-to-end mock-store tests — hint processing
//! (HYPO_INDEX), cached-table cop routing, read-for-write engine selection,
//! FOR UPDATE lock routing vs TiFlash isolation, virtual/generated column
//! push-down boundaries, statements_summary plan-shape regressions and
//! HAVING-correlated-scalar semantics — all driven through `testkit.TestKit`
//! with EXPLAIN inspection.
//!
//! All fourteen items are honest gap ports: this crate exposes planner data
//! structures, cost primitives and a handful of dependency-closed adapters —
//! no SQL optimize entry point, no executor/session stack, no explain
//! renderer — so none of these behaviors has an honest carrier here.

/// GO PORT of `pkg/planner/core/integration_test.go:847 TestHypoIndexHint`.
///
/// Re-derived contract: HYPO_INDEX(tableName, indexName, cols...) invents a
/// hypothetical index inside plan building (:850-859): a single-col hypo
/// index flips TableFullScan→IndexRangeScan on `a=1`; adding more hypo cols
/// keeps IndexRangeScan as predicates grow (`a=1 and b=1`, plus `c<1`);
/// two hypo indexes on t1/t2 flip a HashJoin into IndexHashJoin (:859).
/// Invalid uses fall back to TableFullScan with Warning 1105 instead of
/// erroring: wrong arity "Invalid HYPO_INDEX hint, valid usage:
/// HYPO_INDEX(tableName, indexName, cols...)" (:863); unknown column
/// "invalid HYPO_INDEX hint: can't find column d in table test.t1" (:865);
/// unknown table in current/current-other schema "'test.tttt' doesn't exist"
/// / "'test1.t1' doesn't exist" (:867,:869).
#[test]
#[ignore = "go-parity-gap: needs hint parser wiring + range-buildable hypothetical indexes + MustHavePlan surface"]
fn hypo_index_hint_plans_and_invalid_usage_warnings() {}

/// GO PORT of `pkg/planner/core/integration_test.go:873
/// TestAggPushToCopForCachedTable`.
///
/// Re-derived contract: for a CACHED table (alter table ... cache) with a
/// nonclustered PK, AGG_TO_COP hint + ignore index(primary) still plans
/// StreamAgg(root) → UnionScan(root) → TableReader → Selection(cop[tikv]) →
/// TableFullScan(cop[tikv]) on first execution (:888-894); after repeated
/// runs the session's StmtCtx.ReadFromTableCache flag becomes true (require.
/// Eventually polling :896-900), i.e. subsequent executions serve from the
/// cached-table layer while keeping identical results ("2").
#[test]
#[ignore = "go-parity-gap: needs cached-table meta + union scan planning + statement-context ReadFromTableCache flag"]
fn agg_to_cop_on_cached_table_routes_through_cache_layer() {}

/// GO PORT of `pkg/planner/core/integration_test.go:1362
/// TestTiFlashReadForWriteStmt`.
///
/// Re-derived contract: @@tidb_enable_tiflash_read_for_write_stmt defaults to
/// 1 and cannot be turned OFF — setting OFF yields Warning 1105 "…is always
/// turned on. This variable has been deprecated…" while the value stays 1
/// (:1373-1381). Engine routing matrix over INSERT..SELECT /
/// REPLACE..SELECT / UPDATE-with-subquery explains read parts (:1387-1444):
/// strict sql_mode keeps reads cop[tikv]; strict+enforce_mpp keeps tikv AND
/// warns "MPP mode may be blocked because the query is not readonly and sql
/// mode is strict." (:1418); non-strict sql_mode allows mpp[tiflash]. A
/// SelectLock-bearing UPDATE warns "MPP mode may be blocked because operator
/// `SelectLock` is not supported now." outside classic kernels (:1439-1443);
/// setting tidb_isolation_read_engines without tiflash blocks MPP with
/// "…'tidb_isolation_read_engines'(value: 'tidb, tikv') not match, need
/// 'tiflash'." (:1457-1463).
#[test]
#[ignore = "go-parity-gap: needs sysvar semantics + read-for-write engine gating across DML explains"]
fn read_for_write_stmt_engine_matrix_and_pinned_on_sysvar() {}

/// GO PORT of `pkg/planner/core/integration_test.go:1466
/// TestPointGetWithSelectLock`.
///
/// Re-derived contract: with SQL_MODE='' and tiflash replicas hacked onto
/// both fixture tables, every point-get/batch-point-get shape carrying FOR
/// UPDATE (composite-PK OR-of-points, single point, unique-key equality/
/// conjunction/OR/in-list) MUST NOT contain "tiflash" in its explain output
/// when engines are 'tidb,tiflash' (:1485-1494 CheckNotContain); adding
/// 'tikv' to the engines list lets them plan within the interactive txn
/// (:1495-1500), and they also work with only 'tidb,tiflash' in auto-commit
/// (:1502-1506). Pins that SELECT ... FOR UPDATE point reads never route to
/// TiFlash.
#[test]
#[ignore = "go-parity-gap: needs FOR UPDATE point-get planning + isolation-engines session state"]
fn point_get_with_select_lock_never_reads_tiflash() {}

/// GO PORT of `pkg/planner/core/integration_test.go:2146 TestVirtualExprPushDown`.
///
/// Re-derived contract: virtual generated columns evaluate AT ROOT for
/// TopN/Projection/Selection regardless of engine (:2160-2200): order by c2
/// limit 2 shows root TopN over cop[tikv] TableFullScan; projection-pushdown
/// mode still computes `plus(c1, c2)` at root; `where c2 > 1` stays a root
/// Selection. Force-index on an expression index works with the matching
/// predicate (rows "2", :2212-2221; issue:67981 IndexLookUp variants pinned
/// verbatim :2224-2245). The same three operators then route over
/// cop[tiflash] scans once replicas/tiflash isolation are set, still
/// evaluating the virtual column at root (:2258-2283). Pins issue #41355:
/// virtual-column expressions block operator-level push-down, scans don't.
#[test]
#[ignore = "go-parity-gap: needs generated-column plan construction + expression-index force paths"]
fn virtual_expr_blocks_operator_pushdown_but_not_scans() {}

/// GO PORT of `pkg/planner/core/integration_test.go:2323 TestIssue41458`.
///
/// Re-derived contract: a four-way self join with two constant-attribute
/// filters must record a plan whose INDENTED operator tree (from
/// information_schema.statements_summary.plan, tab-split second field)
/// matches exactly Projection > HashJoin > HashJoin > HashJoin >
/// IndexLookUp(IndexRangeScan+Selection+TableRowIDScan) ×2 >
/// TableReader(Selection(TableFullScan)) ×2 (:2335-2357) — pins join-order +
/// access-path stability for #41458.
#[test]
#[ignore = "go-parity-gap: needs full optimize pipeline + statements_summary plan serialization"]
fn issue_41458_four_way_join_plan_shape_stability() {}

/// GO PORT of `pkg/planner/core/integration_test.go:2364 TestIssue48257`.
///
/// Re-derived contract: stats-objective interactions (#48257): after analyze,
/// sync-loaded rows show analyzed count 1.00→2.00 as deltas flush;
/// `set tidb_opt_objective='determinate'` reports the ANALYZED count (1.00),
/// while 'moderate' prefers updated deltas (2.00) (:2387-2402). With
/// sync_wait=0 the un-analyzed/pseudo path renders "stats:pseudo" and — in
/// determinate mode without loaded histograms — the pseudo default row count
/// 10000.00 (:2414-2425); LoadNeededHistograms restores 1.00 (:2427-2430).
#[test]
#[ignore = "go-parity-gap: needs stats handle lifecycle + objective-aware cardinality rendering"]
fn issue_48257_stats_objective_governs_row_count_source() {}

/// GO PORT of `pkg/planner/core/integration_test.go:2431 TestIssue54213`.
///
/// Re-derived contract: count(1) over a force_index(tb, ab) LIMIT 100 derived
/// table keeps the pushed-down Limit on BOTH sides of the IndexReader:
/// StreamAgg → Limit(root) → IndexReader(index:Limit) → Limit(cop[tikv],
/// offset:0, count:100) → IndexRangeScan range:[1 1,1 1], stats:pseudo
/// (:2436-2447). Pins #54213 (index-side early stop preserved through
/// aggregation).
#[test]
#[ignore = "go-parity-gap: needs index-reader limit embedding + plan_tree renderer"]
fn issue_54213_limit_survives_index_reader_for_count() {}

/// GO PORT of `pkg/planner/core/integration_test.go:2450 TestIssue54870`.
///
/// Re-derived contract: a NOT NULL STORED-free VIRTUAL `is_deleted`
/// (`deleted_at > '1970-01-01 01:00:01.000'`) inside composite index k(id,
/// is_deleted) must still yield an IndexRangeScan for
/// `where id=1 and is_deleted=true` inside the txn where id=1 was inserted
/// (:2459). Pins #54870: indexed virtual columns keep range access with
/// prepared-style predicates before commit.
#[test]
#[ignore = "go-parity-gap: needs virtual-column index ranges in uncommitted-txn reads"]
fn issue_54870_virtual_column_index_range_scan_in_txn() {}

/// GO PORT of `pkg/planner/core/integration_test.go:2463 TestIssue52472`.
///
/// Re-derived contract: UNION ALL field-type promotion (#52472):
/// int UNION ALL unsigned-int promotes the result column to TypeLonglong
/// (:2476), while literal-0 UNION ALL unsigned-bigint promotes to
/// TypeNewDecimal (:2483) — asserted via rs.Fields()[0].Column.FieldType.
#[test]
#[ignore = "go-parity-gap: needs result-field type derivation for set operations"]
fn issue_52472_union_all_widens_int_unsigned_types() {}

/// GO PORT of `pkg/planner/core/integration_test.go:2488
/// TestTiFlashHashAggPreAggMode`.
///
/// Re-derived contract: @@tiflash_hashagg_preaggregation_mode is an enum
/// sysvar: default 'force_preagg'; legal values force_preagg/auto/
/// force_streaming accepted at SESSION and GLOBAL scope (:2490-2506);
/// anything else errors "incorrect value: `test`.
/// tiflash_hashagg_preaggregation_mode options: force_preagg, auto,
/// force_streaming" (:2508).
#[test]
#[ignore = "go-parity-gap: enum sysvar validation pipeline lives outside this crate"]
fn tiflash_hashagg_preaggregation_mode_enum_sysvar() {}

/// GO PORT of `pkg/planner/core/integration_test.go:2512
/// TestNestedVirtualGeneratedColumnUpdate`.
///
/// Re-derived contract (#2512-title regression): a STORED json column built
/// from `json_merge_patch(ifnull(col6,'{}'), ifnull(col7,'{}'))` feeding two
/// VIRTUAL varchar extractors `left(json_unquote(json_extract(col8,
/// \"$.col9[0]\")),36)`/`$.col10` with an index on the latter must survive
/// INSERT (with DEFAULT placeholders for generated cols) → UPDATE col7 →
/// DELETE (:2521-2526) without mutation-planning panics — nested virtual
/// columns recompute through dependent stored columns.
#[test]
#[ignore = "go-parity-gap: needs nested generated-column write planning + DML executor"]
fn nested_virtual_generated_column_update_cycle() {}

/// GO PORT of `pkg/planner/core/integration_test.go:2560
/// TestCorrelatedScalarSubquery`.
///
/// Re-derived contract: scalar COUNT subquery over LEFT JOIN chains whose
/// WHERE binds outer `cm.hcode = t1.hcode`, wrapped by
/// `WHERE t1.id IN (SELECT MIN(id) FROM t1)` must EXECUTE without "Can't
/// find column Column#17 …" schema-resolution errors (:2567-2584,
/// QueryToErr expects nil) — regression pin for schema column placement in
/// correlated aggregates over outer joins.
#[test]
#[ignore = "go-parity-gap: needs correlated aggregate build + execution over left joins"]
fn correlated_scalar_subquery_schema_resolution_regression() {}

/// GO PORT of `pkg/planner/core/integration_test.go:2586 TestIssue66619`.
///
/// Re-derived contract: correlated scalar COUNT with alias-HAVING returns one
/// row per outer row — `having cnt > 0` maps non-matching outer rows to NULL
/// ("3 <nil>", "4 <nil>") while `having cnt < 1` yields 0 with NULL through
/// HAVING:false→NULL mapping (:2595-2611); plain correlated count yields 0s.
/// NO_DECORRELATE must not change those results (:2600-2601). Also pins
/// issue:66947 direct-having ↔ equivalent derived-filter forms both producing
/// hex '20' for empty-string grouping with `sum(c0) > -1 and char_length(c0)`
/// truthiness (:2617-2623).
#[test]
#[ignore = "go-parity-gap: needs having-over-correlated-scalar executor semantics"]
fn issue_66619_having_over_correlated_count_and_66947_forms() {}

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

//! Ports for `pkg/planner/core/casetest/plancache/`
//! (`pkg/planner.part7`, items 405–420 of all 1278 `Test*`/`Benchmark*`
//! declarations under `pkg/planner/` on `origin/master`, sorted by file path
//! then line).
//!
//! Families covered: `plan_cache_param_test.go` (pure-AST parameterization),
//! `plan_cache_partition_table_test.go` plus `plan_cache_partition_test.go`
//! (prepared and non-prepared plan cache over partitioned tables), and
//! `plan_cache_rebuild_test.go` (instance plan-cache clone validation via Go
//! reflection). Item 405 (`main_test.go:30 TestMain`) has no Rust test here:
//! it is bootstrap-only — loads the plan_cache_suite testdata book, sets up
//! goleak filters and wires golden regeneration into TestMain — so it keeps
//! the crate's established skipped-reason treatment for TestMain bootstrap.
//!
//! The two pure-AST items deserve precision about WHY they are gaps.
//! `ParameterizeAST` (plan_cache_param.go:119-137) walks the parsed statement
//! with a visitor replacing every driver.ValueExpr by a ParamMarkerExpr whose
//! Offset encodes collection order (:60-71) while SKIPPING SelectField
//! expressions, GroupByClause, OrderByClause and Limit (:41-47) plus the
//! format argument of date_format/str_to_date/time_format/from_unixtime
//! (:49-58), then restores the statement with flags
//! RestoreForNonPrepPlanCache|RestoreStringWithoutCharset|
//! RestoreStringSingleQuotes|RestoreNameBackQuotes (:28-31) to produce exact
//! texts like "SELECT * FROM `t` WHERE `a`<?". The Rust parser tier exposes
//! neither that RestoreCtx flag machinery nor that replacement walk;
//! tidb-session's non-prepared cache carries a DIFFERENT key design with
//! parameter-kind suffixes, deliberately not this text. Recorded as gaps,
//! not approximated. Names keep Go's Benchmark shape for the four benchmarks
//! so the batch gate filter `not test(/bench/)` skips them exactly as
//! `go test` skips Benchmarks.

/// GO PORT of `plan_cache_param_test.go:30 TestParameterize`.
///
/// Re-derived contract: fourteen literal rows pin parse-then-ParameterizeAST
/// outputs of (paramSQL, ordered params): select with one comparison yields
/// backquoted column plus single ? collecting int64(10); plain select is
/// unchanged; chained predicates become one ? each IN ORDER; string literals
/// parameterized; projection lists keep literals AND their original
/// spacing/aliases ("select 1, 2, 3 ..." restores as "SELECT 1,2,3 FROM ..."
/// :61-63; mixed quoted aliases stay verbatim :64-72); INSERT values all
/// become (?,?),(?,?) while column names keep un-normalized case (`B`, :73);
/// the SECOND argument of date_format survives as '%Y-%m-%d' while its first
/// becomes ? (:86-88); backquote-needing identifiers preserved
/// (`txu#p#p1`, :89); LIMIT clauses keep literal form (LIMIT 10 / LIMIT
/// 10,20 :92-99).
#[test]
#[ignore = "go-parity-gap: ParameterizeAST needs the AST value-replacement walk + RestoreCtx non-prep-cache flag set"]
fn parameterize_ast_replaces_values_collecting_params_in_order() {}

/// GO PORT of `plan_cache_param_test.go:124 TestGetParamSQLFromASTConcurrently`.
///
/// Re-derived contract: fifty distinct INSERT statements (values i*3,
/// i*3+1, i*3+2) parsed ONCE each, then hammered concurrently — one
/// goroutine per statement times one hundred iterations with randomized
/// sleeps (:143-152); EVERY call of GetParamSQLFromAST must return exactly
/// three datums in order without cross-statement corruption, proving pooled
/// replacer/restorer state never leaks between goroutines while the ORIGINAL
/// ast stays unmutated (plan_cache_param.go:101-108 doc notice) and restored
/// values rebind by ParamMarkerExpr OFFSET as non-prepared cache order.
#[test]
#[ignore = "go-parity-gap: GetParamSQLFromAST's param extraction + restore round-trip is unported"]
fn get_param_sql_from_ast_concurrent_pooled_state_never_leaks() {}

/// GO PORT of `plan_cache_param_test.go:156 BenchmarkParameterizeSelect`.
///
/// Re-derived contract: TPC-C payment SELECT over customer parsed once then
/// `ParameterizeAST` run b.N times; filtered by the gate like go test.
#[test]
#[ignore = "go-parity-gap: benchmark over ParameterizeAST; needs the AST machinery first"]
fn benchmark_parameterize_select() {}

/// GO PORT of `plan_cache_param_test.go:170 BenchmarkParameterizeInsert`.
///
/// Re-derived contract: eight-column history-row INSERT, same loop shape.
#[test]
#[ignore = "go-parity-gap: benchmark over ParameterizeAST; needs the AST machinery first"]
fn benchmark_parameterize_insert() {}

/// GO PORT of `plan_cache_param_test.go:183 BenchmarkGetParamSQL`.
///
/// Re-derived contract: full GetParamSQLFromAST (parameterize + datum copy +
/// restore-with-params) per iteration.
#[test]
#[ignore = "go-parity-gap: benchmark over GetParamSQLFromAST; needs the AST machinery first"]
fn benchmark_get_param_sql() {}

/// GO PORT of `plan_cache_partition_table_test.go:188 TestPartitionVarcharFullCover`.
///
/// Re-derived contract: drives shared harness `testPartitionFullCover`
/// (:43-185, useStringPK=true): five table defs (varchar pk / unique-a /
/// no-key variants with randomly shuffled column/key order, :191-217) times
/// two partition schemes (RANGE COLUMNS(a) split at 'k'/'x', KEY(a)
/// partitions 7, :218-227); collation randomly picked from utf8mb4_bin /
/// unicode_ci / general_ci / 0900_ai_ci / 0900_bin / none which also flips
/// case-sensitivity of point-read verification (:62-77); one THOUSAND seeded
/// random ids inserted in batches (ids >= 'x' skipped for varchar inserts
/// :132-140); per cell runs ALL FOUR readers — preparedStmtPointGet (:297),
/// nonPreparedStmtPointGet (:483), preparedStmtBatchPointGet (:414),
/// nonpreparedStmtBatchPointGet (:514) — asserting hit/miss, prune-aware
/// access objects and sorted result rows under fix control 44262:ON.
#[test]
#[ignore = "go-parity-gap: randomized partition point-get coverage matrix needs prepared-stmt execution over sessions"]
fn partition_varchar_full_cover_randomized_point_reads_four_readers() {}

/// GO PORT of `plan_cache_partition_table_test.go:230 TestPartitionIntFullCover`.
///
/// Re-derived contract: same harness with int PKs capped at maxRange=2000000
/// (ids >= maxRange skipped at insert :136-139) across FIVE schemes: RANGE on
/// a (batch-capable :249-252), RANGE on expression floor(a*0.5)*2 (NOT
/// batch-capable :253-256), HASH(a) partitions 7 (batch-capable),
/// HASH(floor) blocked upstream by canConvertPointGet (:261-263), KEY(a)
/// partitions 7; expected point-get EXPLAIN strings differ per key kind
/// ("handle:" vs "index:a" vs clustered-primary, :238-246).
#[test]
#[ignore = "go-parity-gap: same execution-based coverage matrix; expression-partition BatchPointGet gating unported"]
fn partition_int_full_cover_randomized_point_reads_five_schemes() {}

/// GO PORT of `plan_cache_partition_test.go:26 TestPlanCachePartitionSuite`.
///
/// Re-derived contract: TWO blocks. Block one (:33-105): hash-partitioned t
/// with int pk — fix-control 49736 ON must still cache partition PointGets
/// (@@last_plan_from_cache=1 in both modes :44-52); range-partitioned variant
/// proves SAME-partition rebinds stay cached (@a=4 :58-61), DIFFERENT-
/// partition PointGets are served from cache after issue-fix code changes
/// (@a=2 :62-65); out-of-range @a=2000000 plans Point_Get/partition:dual/
/// handle:2000000 and NEVER caches (:70-75); an IN(?,?,?) spanning partitions
/// misses once then hits (:83-87). Block two partition-batch-point-get-
/// duplicates (:106-148): four-way RANGE on unique key with duplicated values
/// across partitions; Batch_Point_Get_1 operator pinned through a second
/// connection's explain-for-command; duplicate sets hit the cache only when
/// legal, otherwise refuse with warning text "skip plan-cache: plan rebuild
/// failed, rebuild to get an unsafe range, IndexValue length diff" (:145).
#[test]
#[ignore = "go-parity-gap: prepared plan-cache partition rebinding + FoundInPlanCache need the executor session stack"]
fn plan_cache_partition_suite_fix_control_49736_and_duplicate_batch_refusals() {}

/// GO PORT of `plan_cache_partition_test.go:151 TestPlanCachePartitionIndex`.
///
/// Re-derived contract: saves/restores BOTH cache-enablement vars (:153-162);
/// helper pair against KEY-partitioned nonclustered-pk tables. PREPARED arm
/// (:166-189): IN(?,?,?) executions sort-equal real rows, miss then HIT even
/// when parameters move to another key partition (:182-184), explain brief
/// shape IndexLookUp/IndexRangeScan+TableRowIDScan (:176-181). NON-PREPARED
/// arm (:191-258): explain format='plan_cache' pins partition pruning listing
/// (partition:p1,p2 :214-220) and per-partition counts (count(p1)=5,
/// count(p0)=0 :244-246), duplicates-in-list caching, and the decisive
/// boundary that PointGet-shaped single-key selects always take the FAST
/// path so FoundInPlanCache reports FALSE despite earlier caching (:255-258).
#[test]
#[ignore = "go-parity-gap: plan_cache explain formats + fast-path detection need executed sessions"]
fn plan_cache_partition_index_prepared_and_non_prepared_key_partitions() {}

/// GO PORT of `plan_cache_partition_test.go:238 TestPlanCacheFixControlRebuild`.
///
/// Re-derived contract: hash-partitioned t analyzed after ten rows inserted
/// (:245-250); plain parameter flips keep hitting the cache (:252-256);
/// turning fix-control 33031:ON makes the NEXT parameter value MISS with the
/// exact refusal warning "skip plan-cache: plan rebuild failed, Fix33031
/// fix-control set and partitioned table in cached Point Get plan"
/// (:257-263), restoring OFF caches again (:264-268); IDENTICAL cycle
/// repeated for the Batch twin warning "...in cached Batch Point Get plan"
/// over IN(?,?) (:270-285).
#[test]
#[ignore = "go-parity-gap: Fix33031 plan-rebuild refusal path lives behind plan-cache rebuild machinery"]
fn plan_cache_fix_control_33031_rebuild_refusal_point_and_batch() {}

/// GO PORT of `plan_cache_partition_test.go:286 TestPreparedStmtPartitionUnion`.
///
/// Re-derived contract: unique-keyed hash table populated with 100 rows and
/// analyzed, static prune mode set FIRST (:289-297); the OR-list select shows
/// PartitionUnion over per-partition Projection/Batch_Point_Get goldens
/// including pruned empty shapes (:299-305); the PREPARED twin executes
/// identically but NEVER enters the plan cache — refused on every execute
/// with "skip prepared plan-cache: query accesses partitioned tables is
/// un-cacheable if tidb_partition_pruning_mode = 'static'" (:307-320).
#[test]
#[ignore = "go-parity-gap: static-prune PartitionUnion planning + prepared-refusal warnings unported"]
fn prepared_stmt_partition_union_static_mode_always_uncacheable() {}

/// GO PORT of `plan_cache_rebuild_test.go:43 TestPlanCacheClone`.
///
/// Re-derived contract: two sessions share one store; roughly thirty-five
/// cached statements — TableScan variants including pushed arithmetic params
/// (:57-63), IndexScan on secondary index (:66-74), IndexLookUp triples
/// (:77-84), index_lookup_pushdown hint trio (:87-91), USE_INDEX_MERGE
/// disjunctions of two/three arms (:94-99), HashAgg and StreamAgg plain and
/// group-by quartets (:102-110) — each driven through `testCachedPlanClone`
/// (:233-265): execute twice on tk1 to populate the plan cache, clone the
/// CACHED plan for tk2, then walk clone vs source with
/// `checkUnclearPlanCacheClone` (:326+) so ANY field sharing memory between
/// source and clone fails with its dotted path — whitelist excepted
/// (.HandleParams, .IndexValueParams, .Insert.Lists, .accessCols,
/// .PhysicalSchemaProducer.schema, .PruningConds, .PlanPartInfo.Columns,
/// .PlanPartInfo.ColumnNames, .SimpleSchemaProducer.schema, .PkIsHandleCol,
/// JoinKeys, .OtherConditions, .ExtraHandleCol, .PointGetPlan.HandleConstant
/// :245-254); DML clones additionally EXECUTE on tk2 (:256-260).
#[test]
#[ignore = "go-parity-gap: instance plan-cache deep-clone walking needs reflect-driven plan trees"]
fn plan_cache_clone_roundtrip_across_scan_index_merge_agg_shapes() {}

/// GO PORT of `plan_cache_rebuild_test.go:267 TestCheckPlanClone`.
///
/// Re-derived contract: unit-level pins on the checker itself using bare
/// operator structs: identical pointers fail as "same pointer, path
/// *physicalop.PhysicalTableScan"; SHARED slices fail as "same slice
/// pointers, path *.AccessCondition"; a shared ELEMENT inside slices reports
/// "same pointer, path
/// *physicalop.PhysicalTableScan.AccessCondition[0](*expression.Column)"
/// (:272-279); maps and map VALUES caught likewise on
/// PhysicalLock.TblID2Handle paths (:280-290); a shared session ctx surfaces
/// through the embedded base (path ...BasePhysicalPlan.Plan.ctx)
/// (:291-296); struct-tag escape hatch: fields tagged
/// plan-cache-clone:"shallow" MAY share (S.p1 passes, S.p2 does not,
/// :298-308), and clean input returns nil.
#[test]
#[ignore = "go-parity-gap: checkUnclearPlanCacheClone's visited-graph reflection walk has no Rust counterpart"]
fn check_plan_clone_detects_shared_pointers_slices_maps_and_ctx_paths() {}

/// GO PORT of `plan_cache_rebuild_test.go:461 TestFastPointGetClone`.
///
/// Re-derived contract: SOURCE-CODE introspection — reads
/// pkg/planner/core/plan_clone_utils.go (helper readPlanCloneUtils :516-535,
/// fall-back path ../../plan_clone_utils.go), extracts the whole
/// FastClonePointGetForPlanCache function body, then requires EVERY field of
/// physicalop.PointGetPlan to be assigned inside it: cost / PlanCostInit /
/// PlanCost / PlanCostVer2 / accessCols exempt as no-need-to-clone
/// (:468-474), dbName/schema/outputNames/ctx must appear via their SETTER
/// call forms SetDBName( SetSchema( SetOutputNames( SetCtx( (:476-486),
/// everything else must occur as "<field> ="; any missing assignment breaks
/// the invariant that fast-cloned PointGets fully repopulate before reuse.
#[test]
#[ignore = "go-parity-gap: FastClonePointGetForPlanCache + PointGetPlan field surface not ported, nothing to introspect"]
fn fast_point_get_clone_assigns_every_field_of_plan_clone_utils_source() {}

/// GO PORT of `plan_cache_rebuild_test.go:538 BenchmarkPointGetCloneFast`.
///
/// Re-derived contract: optimize select-from-composite-pk-t at (1,1), then
/// b.N iterations of `core.FastClonePointGetForPlanCache(sctx, src, dst)`
/// timing the field-wise fast clone; filtered by the gate like go test.
#[test]
#[ignore = "go-parity-gap: benchmark over FastClonePointGetForPlanCache; needs optimized PointGetPlan objects"]
fn benchmark_point_get_clone_fast() {}

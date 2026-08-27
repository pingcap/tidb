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

//! Documentary gap ports for `pkg/planner/core/casetest/plancache/`
//! (`pkg/planner.part8`, items 421-464 of all 1278 `Test*`/`Benchmark*`
//! declarations under `pkg/planner/` on `origin/master`, sorted by file path
//! then line, chunked in groups of 60):
//!
//! * `plan_cache_rebuild_test.go:560 BenchmarkPointGetClone`
//! * `plan_cache_suite_test.go` items 422-455 plus benchmarks 446-448
//! * `plan_cache_test.go` items 456-459
//! * `plan_cacheable_checker_test.go` items 460-464
//!
//! Every one of these Go tests drives statements through a testkit session:
//! prepare/execute, `planner.Optimize`, `@@last_plan_from_cache`,
//! `explain for connection`, bindings, resource groups and stats-version
//! invalidation. The Rust rewrite keeps plan-cache machinery inside the
//! `tidb-session` crate (`prepared_plan_cache.rs`/`non_prepared_plan_cache.rs`,
//! `pub(crate)` surfaces behind a session dispatch tier) with a different key
//! design; the `tidb-planner` crate has neither a session/testkit stack nor
//! `planner.Optimize`, so no assertion of these contracts can run here.
//! Each item below records its re-derived Go contract verbatim as an
//! `#[ignore]` gap port; nothing is approximated. Benchmarks keep Go's
//! Benchmark name shape so the batch gate filter `not test(/bench/)` skips
//! them exactly like `go test` skips Benchmarks.

/// GO PORT of `plan_cache_rebuild_test.go:560 BenchmarkPointGetClone`.
///
/// Re-derived contract: over a mock store + domain session, plans
/// `select a, b from t where a=1 and b=1` on composite-PK t (:565-570), takes
/// the optimized `physicalop.PointGetPlan` (:573) and loops
/// `src.CloneForPlanCache(sctx)` with `GetPlanCtx()` as destination context
/// (:575-578); the measured unit is that per-clone cost against the fast
/// PointGet clone path.
#[test]
#[ignore = "go-parity-gap: benchmark over PointGetPlan::CloneForPlanCache; needs optimize-over-session to obtain live PointGetPlan objects"]
fn benchmark_point_get_clone() {}

/// GO PORT of `plan_cache_suite_test.go:46 TestInitLRUWithSystemVar`.
///
/// Re-derived contract: setting `tidb_prepared_plan_cache_size = 0` reads back
/// as "1" through the sysvar MinValue clamp (:48-49); the session's
/// `PreparedPlanCacheSize` value must be accepted by
/// `plannercore.NewLRUPlanCache(size, 0, 0, session, false)` returning a
/// non-nil LRU (:50-53).
#[test]
#[ignore = "go-parity-gap: NewLRUPlanCache + PreparedPlanCacheSize sysvar live in the tidb-session tier"]
fn init_lru_with_system_var_clamps_size_to_minimum_one() {}

/// GO PORT of `plan_cache_suite_test.go:57 TestNonPreparedPlanCachePlanString`.
///
/// Re-derived contract: helper parses + Preprocess + `planner.Optimize` and
/// renders `plannercore.ToString(p)` (:62-73). With non-prepared cache ON, the
/// FIRST `select a from t where a < 1` is "IndexReader(Index(t.a)[[-inf,1)])"
/// with cache-miss 0; `< 10` reuses the cached IndexReader shape over range
/// [−inf,10) reporting hit 1 (:68-71, :80); TableReader+Sel filters behave the
/// same ("TableReader(Table(t)->Sel([lt(test.t.b, 1)]))" then `[lt(test.t.b,
/// 10)]`) (:91-93,:102). Flipping global `tidb_redact_log` to MARKER and ON
/// between executions must not change either the rendered string nor the
/// cached-hit answer (:76-77,:82-83,:96-97,:104).
#[test]
#[ignore = "go-parity-gap: planner.Optimize + ToString rendering + FoundInPlanCache need the session pipeline"]
fn non_prepared_plan_cache_plan_string_and_redact_log_stability() {}

/// GO PORT of `plan_cache_suite_test.go:103 TestJSONExtractPlanCache`.
///
/// Re-derived contract: prepared `select id ... where json_unquote(json_extract(doc, ?)) = ?`
/// produces NO skip warning (:111-113), and execute rounds with different
/// @path/@val report cache misses then hits (0,1,1 at :114-121) - JSON path
/// parameters do not block reuse for varchar doc columns. Non-prepared
/// equivalents follow the same miss/hit pattern (:123-127). When doc is a JSON
/// column and the filter is `json_extract(doc,'$.a') is not null`, both
/// executions stay uncached (0,0 :128-131): JSON-typed column filters refuse
/// the cache outright.
#[test]
#[ignore = "go-parity-gap: prepared/non-prepared execution with @@last_plan_from_cache needs sessions"]
fn json_extract_prepared_and_non_prepared_cache_boundaries() {}

/// GO PORT of `plan_cache_suite_test.go:139 TestJSONExtractPlanCacheWithExpressionIndex`.
///
/// Re-derived contract: with expression index idx_a ((cast(json_unquote(json_extract(doc,'$.a'))
/// as char(20)))), the prepared cast(...) = ? statement stays
/// cacheable-warning-free (:153-155) but EVERY execute reports
/// last_plan_from_cache=0 even when only @path differs (:156-160, :163-166):
/// parameters feeding expression-index positions are never reused from cache.
/// Non-prepared queries over the same casts never cache either (:167-171), and
/// group-by/aggregate forms over the same casts always report 0
/// (:177-193).
#[test]
#[ignore = "go-parity-gap: expression-index parameter cache-refusal needs executed prepared plans"]
fn json_extract_expression_index_params_never_reuse_cached_plans() {}

/// GO PORT of `plan_cache_suite_test.go:196 TestNonPreparedPlanCacheInformationSchema`.
///
/// Re-derived contract: over MockInfoSchema{MockSignedTable,MockUnsignedTable}
/// (:203), Preprocess then TWO `planner.Optimize` calls of the identical
/// `select avg(a),avg(b),avg(c) from t` both succeed without error (:208-211)
/// and the second hits the non-prepared cache (`FoundInPlanCache == true`,
/// :212).
#[test]
#[ignore = "go-parity-gap: preprocess/optimize over mock infoschema plus SessionVars.FoundInPlanCache unported"]
fn non_prepared_cache_information_schema_aggregates_hit_on_second_optimize() {}

/// GO PORT of `plan_cache_suite_test.go:216 TestNonPreparedPlanTypeRandomly`.
///
/// Re-derived contract: seven tables typed int/varchar/double/decimal/year/
/// date/datetime seeded with 30 random rows each (:218-238); 200 random
/// filters drawn from >=,<,=,IN across random types (:247-266) must produce
/// equal sorted results across first-execution-with-cache, cached re-run and
/// cache-disabled re-run (:279-287) - randomized type-domain equivalence of
/// cached vs uncached results.
#[test]
#[ignore = "go-parity-gap: randomized execution harness over seven typed tables needs the executor"]
fn non_prepared_plan_type_randomly_results_equal_with_and_without_cache() {}

/// GO PORT of `plan_cache_suite_test.go:290 TestNonPreparedPlanCacheBasically`.
///
/// Re-derived contract: seventeen query shapes over t(a,b,c,d,key(b),key(c,d))
/// (:305-325) each satisfy: result with cache OFF equals the ON-cache first
/// AND second executions; `@@last_plan_from_cache` flips 0-then-1 with the
/// cache enabled and is pinned 0 with it disabled (:308-330).
#[test]
#[ignore = "go-parity-gap: per-query result-equivalence matrix requires executed sessions"]
fn non_prepared_plan_cache_basically_seventeen_query_result_equivalence() {}

/// GO PORT of `plan_cache_suite_test.go:331 TestNonPreparedPlanCacheInternalSQL`.
///
/// Re-derived contract: a user SELECT caches normally (second exec hit=1,
/// :342-344); running the SAME select with `InRestrictedSQL = true`
/// (internal-SQL context via kv.WithInternalSourceType) bypasses the cache -
/// `@@last_plan_from_cache` reads 0 (:345-350); clearing the flag restores
/// hit=1 (:351-353).
#[test]
#[ignore = "go-parity-gap: InRestrictedSQL internal-txn gating of the cache lives in the session tier"]
fn non_prepared_plan_cache_internal_restricted_sql_never_caches() {}

/// GO PORT of `plan_cache_suite_test.go:352 TestPreparedPlanCachePlanSelectionRegressions`.
///
/// Re-derived contract: composite driver running seven regression scenarios,
/// each prepared+executed with plan-cache observability:
/// runPreparedPlanCacheGroupByParamProjection (:486) accepts `group by id,col1`
/// with ? projection; runPreparedPlanCacheRedactExplain (:500) pins the full
/// explain tree incl. redacted in-list `‹40›,‹50›,‹60›` under MARKER while
/// hitting the cache; runPreparedPlanCacheIndexHintRangeScan (:535) keeps
/// RangeScan for `a=? and a=?` but never caches use_index duplicates;
/// runPreparedPlanCacheInvalidRange (:552) plans TableDual_5 for lo>hi ranges
/// uncached; runPreparedPlanCacheLeftJoinRangeScan (:567) pins HashJoin build
/// side with IndexRangeScan inner and caches it; runPreparedPlanCacheInlJoinRangeScan
/// (:604) brief-explains IndexJoin with IndexRangeScan probe and caches;
/// runPreparedPlanCachePointGetSafety (:1211) distinguishes Batch_Point_Get +
/// over-optimization refusal warning, unsafe range-to-PointGet refusals, and
/// Selection-wrapped safe PointGets which DO cache.
#[test]
#[ignore = "go-parity-gap: all seven scenarios need explain-for-connection over executed prepared plans"]
fn prepared_plan_cache_plan_selection_regressions_seven_scenarios() {}

/// GO PORT of `plan_cache_suite_test.go:366 TestPreparedPlanCacheWarningRegressions`.
///
/// Re-derived contract: six warning-pinning scenarios:
/// runPreparedPlanCacheDisableEnable (:581) executes cleanly after the cache
/// is toggled OFF post-prepare; runPreparedPlanCacheLimitWarning (:637) emits
/// "skip prepared plan-cache: limit count is too large" for count>=100000 and
/// flips to "force plan-cache: may use risky cached plan" under fix-control
/// 49736:ON with matching hit/miss answers; runPreparedPlanCacheTypeConversionWarning
/// (:660) warns "'1.0' may be converted to INT" for float params while INT
/// params cache silently with stable IndexRangeScan plans;
/// runPreparedPlanCacheIndexRangeTypeWarning (:700) keeps RangeScan yet warns
/// "'1.1' may be converted to INT"; runPreparedPlanCacheConvFunction
/// (:1548) returns conv(?,16,8) rows then empty sets across cache states;
/// runPreparedPlanCacheForUpdateInTxn (:1974) allows autocommit hits but
/// refuses in-txn `for update` reuses (hit pinned 0 at :1984).
#[test]
#[ignore = "go-parity-gap: warning-stream assertions (show warnings exact texts) need sessions"]
fn prepared_plan_cache_warning_regressions_six_scenarios() {}

/// GO PORT of `plan_cache_suite_test.go:379 TestPreparedPlanCacheBatchPointGetEqAndInFixControl`.
///
/// Re-derived contract: under fix control 44830:ON, mixed EQ+IN predicates on
/// composite PKs plan Batch_Point_Get (:402-409 helper) and DO cache: issues
/// 67852 arms verify changed-EQ-param hits and recovery after duplicate-IN
/// misses (:420-443); IN-leading/mid-position composite-key variants pin the
/// same hit/miss cycle including single-value dedup producing fresh plans
/// (:447-479).
#[test]
#[ignore = "go-parity-gap: Batch/PointGet plan choice under fix-control 44830 needs costing over executed plans"]
fn prepared_plan_cache_batch_point_get_eq_in_fix_control_44830_and_issue_67852() {}

/// GO PORT of `plan_cache_suite_test.go:721 TestPlanCacheWithLimit`.
///
/// Re-derived contract: nine prepared LIMIT/DML-limit shapes (select limit
/// ?, limit 1,? / limit ?,1 / limit ?,?, delete-order-limit, insert-select
/// limits, update limit, union-all branches) each hit the cache when replayed
/// with identical params (row "1") but MISS with changed @a0=6 (rows "0",
/// :755-766); a separate prepare with @a=10001 warns exactly "Warning 1105
/// skip prepared plan-cache: limit count is too large" (:767-773).
#[test]
#[ignore = "go-parity-gap: param-sensitive Limit rebinding needs prepared execution over sessions"]
fn plan_cache_with_limit_param_limit_hits_then_changed_count_misses_and_oversize_refuses() {}

/// GO PORT of `plan_cache_suite_test.go:769 TestPlanCacheWithSubquery`.
///
/// Re-derived contract: six subquery shapes over t(a,b) with their (cacheable,
/// decorrelated) classification (:782-790): EXISTS and IN forms report hit 1;
/// scalar > ANY forms and uncorrelated expressions miss with exact warnings -
/// "query has uncorrelated sub-queries is un-cacheable" for truly
/// uncorrelated bodies, else "PhysicalApply plan is un-cacheable" (:793-812).
/// Switching `tidb_enable_plan_cache_for_subquery = 0` makes every prepare
/// warn "query has sub-queries is un-cacheable" and every double execution
/// stay uncached with EMPTY warning streams (:813-831).
#[test]
#[ignore = "go-parity-gap: Apply-plan cacheability warnings need optimization over sessions"]
fn plan_cache_with_subquery_decorrelated_apply_and_warning_pins() {}

/// GO PORT of `plan_cache_suite_test.go:1078 TestPlanCacheRandomCases`.
///
/// Re-derived contract: twenty rounds (ten under testing.Short) of three
/// randomized corpora - index-merge (:1094-1097), int-conversion and
/// point-get (helpers planCacheIntConvertPrepareData / planCachePointGetPrepareData
/// :851+:881 with unique-key/composite-PK tables randomly re-typed per run) -
/// drive convertQueryToPrepExecStmt (:832) comparing non-prep-cache results,
/// prepared results and cross-checking every row; deterministic-behavior pin
/// is equality across the four engines' replays (:1109-1124, helpers to
/// :1077).
#[test]
#[ignore = "go-parity-gap: randomized prepared-vs-non-prepared corpus harness needs full executor"]
fn plan_cache_random_cases_index_merge_int_convert_point_get_corpora() {}

/// GO PORT of `plan_cache_suite_test.go:1146 TestPlanCacheSubquerySPMEffective`.
///
/// Re-derived contract: four subquery templates carry `/*/` swapped among
/// NO_DECORRELATE hint / binding-using-hint / none (:1154-1160): with hints in
/// the statement text two executions still report cache miss 0 (:1161-1172);
/// with a global binding created BEFORE prepare (binding SQL carrying
/// NO_DECORRELATE) both executions miss (:1176-1189); creating the binding
/// AFTER prepare also invalidates subsequent reuse (:1190-1203).
#[test]
#[ignore = "go-parity-gap: SPM binding interaction with the plan cache needs binding + session stacks"]
fn plan_cache_subquery_spm_no_decorrelate_hints_and_bindings_block_cache_hits() {}

/// GO PORT of `plan_cache_suite_test.go:1256 TestNonPreparedPlanExplainWarning`.
///
/// Re-derived contract: 28 supported and 26 unsupported query shapes over
/// enum/set/json/bit columns, partitioned t1/t2, generated-column t3 and view
/// v (:1277-1335); explains in NINE formats (brief/dot/hint/row/verbose/
/// traditional/binary/tidb_json/cost_trace, :1348-1358) must never emit
/// "plan cache" warnings nor flip @@last_plan_from_cache away from 0 for any
/// form (:1360-1373); unsupported cases explained with format 'plan_cache'
/// expose exactly their listed reasons[idx] on the warning stream
/// (:1336-1346 mapped at :1374-1381) e.g. HAVING/sub-query/JSON-Enum-Set-Bit
/// filters/system-schema/view/null constants/constant-propagation overwrite.
#[test]
#[ignore = "go-parity-gap: nine-format explain matrix + per-reason warning texts need executor explain"]
fn non_prepared_plan_explain_warning_reasons_per_format_matrix() {}

/// GO PORT of `plan_cache_suite_test.go:1394 TestNonPreparedPlanCachePanic`.
///
/// Re-derived contract: for four point-get-ish selects over t with composite
/// PK (c,a) (:1400-1409), parse + Preprocess + `planner.Optimize` MUST simply
/// succeed without panicking for each (:1410-1417) - regression for a
/// nil-map panic in the non-prepared key path.
#[test]
#[ignore = "go-parity-gap: optimizer-crash-freedom check needs parse+preprocess+optimize over sessions"]
fn non_prepared_plan_cache_composite_pk_queries_must_not_panic() {}

/// GO PORT of `plan_cache_suite_test.go:1421 TestNonPreparedPlanCacheAutoStmtRetry`.
///
/// Re-derived contract: tk1 holds a txn updating unique key k 1->3 while tk2
/// inserts a conflicting (3,3) row concurrently (:1433-1449); tk2's write is
/// retried automatically and must ultimately fail containing "Duplicate entry"
/// (:1444-1451) - non-prepared cache plays no role in retry correctness.
#[test]
#[ignore = "go-parity-gap: concurrent auto-retry against a blocking txn needs the txn/executor stack"]
fn non_prepared_plan_cache_auto_stmt_retry_duplicate_entry_after_conflict() {}

/// GO PORT of `plan_cache_suite_test.go:1450 TestNonPreparedPlanCacheRegressions`.
///
/// Re-derived contract: three regressions composed (:1451-1455):
/// runNonPreparedPlanCacheConcurrency (:1457) runs 30 goroutines x 5000
/// iterations of pk-point selects asserting exact rows each time;
/// runNonPreparedPlanCacheASTMutation (:1481) proves a cached plan NEVER
/// reuses mutated AST literals - issue 43667 hook mutates the WHERE constant
/// to 7 during the cached second run and the result must STAY 4;
/// runNonPreparedPlanCacheFieldNameMapping (:1505) checks issue 47133 alias
/// remapping fires FieldName callbacks twice with exact "test.t.user_id"/
/// "test.t.user_personid" strings across a cached replay.
#[test]
#[ignore = "go-parity-gap: concurrency, AST-mutation hook (issue 43667) and FieldName hooks unported"]
fn non_prepared_plan_cache_regressions_concurrency_ast_mutation_field_name_mapping() {}

/// GO PORT of `plan_cache_suite_test.go:1527 TestPlanCacheBindingIgnore`.
///
/// Re-derived contract: same-named tables in test1/test2 both cache (hit rows
/// "1", :1537-1545); after `create global binding using select /*+
/// ignore_plan_cache() */ ...` for EACH database, subsequent executions of the
/// corresponding prepared stmt report 0 twice (:1546-1553) - binding-level
/// ignore_plan_cache disables caching per statement regardless of the other
/// database's behavior.
#[test]
#[ignore = "go-parity-gap: ignore_plan_cache binding propagation needs SPM + sessions"]
fn plan_cache_binding_ignore_hint_refuses_hits_per_database_binding() {}

/// GO PORT of `plan_cache_suite_test.go:1574 TestBuiltinFuncFlen`.
///
/// Re-derived contract: for the 36 listed builtin functions x 6 argument
/// literals (:1578-1585) the query `SELECT c1 from t1 where F(A)` must return
/// IDENTICAL sorted result sets with the non-prepared cache ON versus OFF
/// (:1586-1593) - flen/type inference differences between cached and fresh
/// plans would break result equality (issues 45378/45253 family).
#[test]
#[ignore = "go-parity-gap: 216-cell result-equality sweep needs builtin evaluation through sessions"]
fn builtin_func_flen_results_equal_with_and_without_non_prepared_cache() {}

/// GO PORT of `plan_cache_suite_test.go:1601 TestWarningWithDisablePlanCacheStmt`.
///
/// Re-derived contract: prepares `select * from t` on a hash-partitioned,
/// analyzed table where partitioned tables cannot cache (:1605-1608);
/// show-warnings streams must stay EMPTY across prepare and BOTH executions
/// even though the second reports FoundInPlanCache=true into session vars
/// (:1610-1613) - i.e., the uncacheable-partition skip never surfaces as a
/// warning, and cached metadata does not leak spurious warnings.
#[test]
#[ignore = "go-parity-gap: empty-warning stream around FoundInPlanCache for partitioned prepares needs sessions"]
fn warning_with_disable_plan_cache_stmt_partitioned_table_streams_stay_empty() {}

/// GO PORT of `plan_cache_suite_test.go:1716 TestPlanCacheMVIndexRandomly`.
///
/// Re-derived contract: fix-control 45798:on enables MV-index plan caching;
/// verifyPlanCacheForMVIndex (:1777) drives five rounds per template with
/// random values asserting: plain-vs-cached result equality, index-merge arms
/// keep show-warnings EMPTY across query/prepare/execute/hit stages, and
/// hit=true arms additionally prove an IndexMerge operator appears in
/// explain-for-connection (:1820-1846). Sixteen templates span member-of /
/// json_contains / json_overlaps disjunctions and conjunctions over signed-
/// array, char-array indexes (cases borrowed from TestIndexMergeFromComposedDNFCondition
/// et al., :1726-1776).
#[test]
#[ignore = "go-parity-gap: multivalued-index merge planning + random templates need executor + stats"]
fn plan_cache_mv_index_randomly_fix_control_45798_result_and_warning_pins() {}

/// GO PORT of `plan_cache_suite_test.go:1804 TestPlanCacheMVIndexManually`.
///
/// Re-derived contract: book-driven (GetPlanCacheSuiteData) replay over
/// prepared mv-index queries; every select/execute/show input checks its
/// recorded Result rows byte-exactly, all other inputs MustExec (:1812-1830) -
/// golden pin that manual prepared mv-index usage matches recorded output.
#[test]
#[ignore = "go-parity-gap: plan_cache_suite book goldens over executed mv-index statements unported"]
fn plan_cache_mv_index_manually_golden_book_rows() {}

/// GO PORT of `plan_cache_suite_test.go:1838 BenchmarkPlanCacheBindingMatch`.
///
/// Re-derived cost model: one global binding over t(a,key(a)); loop measures
/// `execute st using @a` end-to-end including binding match + cache lookup
/// (:1845-1852).
#[test]
#[ignore = "go-parity-gap: benchmark over binding-matched cache hits; needs executed sessions"]
fn benchmark_plan_cache_binding_match() {}

/// GO PORT of `plan_cache_suite_test.go:1853 BenchmarkPlanCacheInsert`.
///
/// Re-derived cost model: prepared `insert into t values (1)` executed in a
/// loop (:1859-1865) measuring DML-through-the-prepared-statement path.
#[test]
#[ignore = "go-parity-gap: benchmark over prepared DML; needs executor"]
fn benchmark_plan_cache_insert() {}

/// GO PORT of `plan_cache_suite_test.go:1866 BenchmarkNonPreparedPlanCacheDML`.
///
/// Re-derived cost model: with non-prepared cache ON, repeated
/// insert/update/delete cycles measure the non-prepared DML key path
/// (:1872-1880).
#[test]
#[ignore = "go-parity-gap: benchmark over non-prepared DML cycling; needs executor"]
fn benchmark_non_prepared_plan_cache_dml() {}

/// GO PORT of `plan_cache_suite_test.go:1881 TestIndexRange`.
///
/// Re-derived contract: with the non-prepared cache enabled, `(id = 1 or id =
/// 9223372036854775808)` over bigint AUTO_INCREMENT ids returns ONLY row "1"
/// (the out-of-range literal folds out of the int range instead of erroring,
/// :1889) and `t1.c0 != BIN(-1)` over FLOAT ZEROFILL PK returns row "1"
/// (:1890) - both succeed with sane ranges rather than malformed interval
/// construction.
#[test]
#[ignore = "go-parity-gap: bigint-overflow literal range folding + zerofill float filters need ranger+executor"]
fn index_range_bigint_overflow_literal_folds_out_zerofill_float_bin_filter() {}

/// GO PORT of `plan_cache_suite_test.go:1895 TestPlanCacheDirtyTables`.
///
/// Re-derived contract: for each (t1Dirty,t2Dirty) pair at CACHE-CREATION
/// time, later transactions dirtying the same-or-other tables gate reuse
/// exactly by set equality: a cached plan whose captured dirty-table set
/// equals the current txn's dirty set hits ("1"), ANY difference misses
/// ("0"), across the 4x4 matrix (:1908-1939).
#[test]
#[ignore = "go-parity-gap: dirty-table capture/gating of cached plans lives in the txn-aware session tier"]
fn plan_cache_dirty_tables_hit_only_when_dirty_set_matches_at_creation() {}

/// GO PORT of `plan_cache_suite_test.go:1941 TestInstancePlanCacheAcrossSession`.
///
/// Re-derived contract: with PlanCacheKeyEnableInstancePlanCache{}=true in
/// the statement context (:1942), tk1 caches `select a from t where a < ?`
/// (param 2 then 3, hit "1" :1953); a SECOND session preparing the identical
/// statement shares the instance-level entry: its first execute with @a=4
/// already reports last_plan_from_cache "1" (:1957-1961).
#[test]
#[ignore = "go-parity-gap: instance-scoped cross-session cache sharing needs session manager + contexts"]
fn instance_plan_cache_shared_across_sessions_when_context_gated_on() {}

/// GO PORT of `plan_cache_suite_test.go:1989 TestNonPreparedPlanCacheSupportsFeatures`.
///
/// Re-derived contract: four supported-feature drivers assert cache answers
/// stay consistent WITH feature interactions: max_execution_time hint queries
/// cache on second run (:2010-2018); CREATE BINDING FOR ... maps binding+cache
/// flags independently ("1 0" then "1 1", :2026-2064); set_var bound plans
/// re-create keys when bindings change and preserve "from binding" truth
/// (:2040-2064); an IGNORE_PLAN_CACHE()-bound query never caches while plain
/// replays do (:2068-2087).
#[test]
#[ignore = "go-parity-gap: hint/binding/setvar interplay with @@last_plan_from_binding+_cache needs SPM+sessions"]
fn non_prepared_plan_cache_supports_hints_bindings_setvar_ignore_hint_features() {}

/// GO PORT of `plan_cache_suite_test.go:2114 TestNonPreparedPlanCacheResourceGroup`.
///
/// Re-derived contract: RESOURCE_GROUP(rg1)/RESOURCE_GROUP(rg10) hints set
/// StmtHints.HasResourceGroup + ResourceGroup verbatim each run (:2122-2135)
/// while cached-flag answers move 0->1; once a binding USING .../*+
/// RESOURCE_GROUP(rg2) exists, the BINDING's group wins and the query's own
/// RESOURCE_GROUP(rg1) hint is overridden - HasResourceGroup still true,
/// value "rg2", from-binding 1 (:2138-2154).
#[test]
#[ignore = "go-parity-gap: StmtHints resource-group precedence over query hints via bindings unported"]
fn non_prepared_plan_cache_resource_group_binding_overrides_query_hint() {}

/// GO PORT of `plan_cache_suite_test.go:2157 TestPreparedPlanCacheWorkWithoutMetadataLock`.
///
/// Re-derived contract: with tidb_enable_metadata_lock=off (:2164), prepared
/// `a = ?` executions report hit "1" in autocommit, continue hitting inside a
/// txn (:2174-2183); inserting INTO t breaks the NEXT execution once (dirty
/// table, "0") then the following one caches again "1" (:2184-2187); after
/// rollback cached reuse continues (:2188-2190) - metadata lock absence does
/// not corrupt the cache lifecycle.
#[test]
#[ignore = "go-parity-gap: metadata-lock-free cached lifecycle across txn boundaries needs sessions"]
fn prepared_plan_cache_works_without_metadata_lock_across_txn_and_dirty_insert() {}

/// GO PORT of `plan_cache_suite_test.go:2195 TestPlanCacheSkipStatsOnBinding`.
///
/// Re-derived contract: with tidb_plan_cache_invalidation_on_fresh_stats=ON:
/// Part 1 without bindings ANALYZE busts the cache (miss "0" after analyze,
/// :2208-2222); Part 2 with `create binding using ... /*+ use_index(t,idx_b) */`
/// AND tidb_plan_cache_skip_stats_on_binding=ON the ANALYZE no longer
/// invalidates - from-binding runs keep hitting ("1 1", :2225-2243); Part 3
/// flipping the variable OFF puts stats version back into the key: first run
/// re-keys ("1 0"), warms ("1 1"), and post-ANALYZE misses again ("1 0",
/// :2245-2260); drop binding restores plain prepared behavior.
#[test]
#[ignore = "go-parity-gap: stats-version-in-key gating influenced by bindings + sysvar needs session vars"]
fn plan_cache_skip_stats_on_binding_gates_analyze_invalidation_by_sysvar() {}

/// GO PORT of `plan_cache_test.go:28 TestDropPrepare`.
///
/// Re-derived contract: statement A (`where a = ?`) caches (miss then hit,
/// :41-48); after `deallocate prepare stmt`, preparing a DIFFERENT statement B
/// under the SAME name (`where b = ?`) must NOT inherit A's cached entry
/// (first B execution miss, :59-61); explain-for-connection pins A's plan to
/// IndexLookUp over IndexRangeScan index:a range:[2,2] + TableRowIDScan and
/// B's to index:b range:[3,3], asserting the two trees differ structurally
/// (:52-58,:64-72); B then caches on its own second run (:74-76).
#[test]
#[ignore = "go-parity-gap: deallocate invalidation of the session cache entry + explain goldens need sessions"]
fn drop_prepare_same_name_new_statement_never_inherits_cached_plan() {}

/// GO PORT of `plan_cache_test.go:85 BenchmarkNewPlanCacheKey`.
///
/// Re-derived cost model: PrepareStmt of a 3-param select resolved to its
/// *plannercore.PlanCacheStmt (:94-100); loop constructs
/// `plannercore.NewPlanCacheKey(sctx, stmt)` repeatedly (:103-106),
/// measuring normalized-key construction outside any transaction.
#[test]
#[ignore = "go-parity-gap: benchmark over NewPlanCacheKey; needs session-vars-backed PlanCacheStmt"]
fn benchmark_new_plan_cache_key() {}

/// GO PORT of `plan_cache_test.go:107 BenchmarkNewPlanCacheKeyInTxn`.
///
/// Re-derived cost model: same 1-param prepare (:113-118) but the loop runs
/// INSIDE a transaction that has made t dirty (insert values (3,3), :121-123)
/// - key construction must incorporate txn-dirty state; rolled back at
/// :126.
#[test]
#[ignore = "go-parity-gap: benchmark over in-txn dirty-aware NewPlanCacheKey; needs txn session state"]
fn benchmark_new_plan_cache_key_in_txn() {}

/// GO PORT of `plan_cache_test.go:133 TestBenchDaily`.
///
/// Re-derived contract: registers the five plan-cache benchmarks
/// (BenchmarkNewPlanCacheKey, BenchmarkNewPlanCacheKeyInTxn,
/// BenchmarkPlanCacheBindingMatch, BenchmarkPlanCacheInsert,
/// BenchmarkNonPreparedPlanCacheDML) with benchdaily.Run (:134-140); pure
/// registration - go test skips it absent -bench guards, Rust port mirrors it
/// as documentation-only.
#[test]
#[ignore = "go-parity-gap: benchdaily registration of executors-only benchmarks; no Rust benchdaily surface"]
fn bench_daily_registers_the_five_plan_cache_benchmarks() {}

/// GO PORT of `plan_cacheable_checker_test.go:37 TestFixControl44823`.
///
/// Re-derived contract: default in-list limit 200 refuses prepared caching of
/// a 200-mark IN with exact warning "skip prepared plan-cache: too many values
/// in in-list" and last_plan_from_cache 0 (:43-51); fix control 44823:250
/// silences the warning AND allows the cache hit (:53-63), 44823:0 keeps the
/// allow-hit behavior (:65-71); the NON-prepared cache mirrors the same
/// thresholds against literal IN lists - off-by-default refusal (0), :250 hit
/// (1), :0 hit (1) (:74-95).
#[test]
#[ignore = "go-parity-gap: fix-control 44823 threshold wiring for both caches lives behind sessions"]
fn fix_control_44823_in_list_threshold_gates_prepared_and_non_prepared_caches() {}

/// GO PORT of `plan_cacheable_checker_test.go:93 TestCacheable`.
///
/// Re-derived contract over core.Cacheable/CacheableWithCtx against mocked
/// infoschema: SHOW/LoadData/ImportInto refuse (:108-117); SetOprStmt passes
/// (:119); INSERT-values, INSERT-select and DELETE pass (:124-134); a WHERE
/// FuncCallExpr fails iff the function name belongs to
/// expression.UnCacheableFunctions while ast.Rand passes (:136-142, updated in
/// UPDATE at :191-198 and SELECT at :255-262); WHERE EXISTS-subquery passes
/// with defaults (:144-149); Limit Count/Offset/empty ParamMarkerExpr forms
/// all pass when EnablePlanCacheForParamLimit (:151-176, :295-315); SELECT
/// ExistsSubquery with EnablePlanCacheForSubquery=false refuses with reason
/// EXACTLY "query has sub-queries is un-cacheable" incl. plain SubqueryExpr
/// and CTE-named FROM forms (:266-280), passing again when re-enabled
/// (:281-284); static prune mode + derived-with-CTE parses refuse with reason
/// "query accesses partitioned tables is un-cacheable if
/// tidb_partition_pruning_mode = 'static'" (:286-294); OrderBy ByItem with a
/// ParamMarkerExpr refuses while ValueExpr passes (:317-326); FrameBound
/// ParamMarkerExpr refuses standalone (:328-329); joins over PARTITIONED t1/t2
/// refuse while non-partitioned t3 passes (:331-352).
#[test]
#[ignore = "go-parity-gap: core.Cacheable checker surface lives pub(crate) in the tidb-session crate here, unreachable from tidb-planner"]
fn cacheable_ast_matrix_functions_subqueries_limits_orderby_partitions() {}

/// GO PORT of `plan_cacheable_checker_test.go:370 TestNonPreparedPlanCacheable`.
///
/// Re-derived contract via core.NonPreparedPlanCacheableWithCtx over real DDL:
/// 39 supported SELECT shapes (:388-430) all TRUE; 17 unsupported (having,
/// derived tables, correlated+uncorrelated subqueries incl. partitioned twins,
/// and json_extract guard rows :431-460) all FALSE; 10 supported DML shapes
/// (for update, insert-values/select, update, delete incl. partitioned twins
/// :461-474) all TRUE; issue 46760: with PlanCacheKeyTestIssue46760{}
/// injected, prepare errors once with exact warning "find table test.t failed:
/// mock error" and the executions stop caching (:487-500); issue 49166:
/// `select ... limit 1 into outfile` preparation fails with "This command is
/// not supported in the prepared statement protocol yet" (:503-507).
#[test]
#[ignore = "go-parity-gap: NonPreparedPlanCacheableWithCtx + issue-hook contexts exist only in the session tier"]
fn non_prepared_plan_cacheable_supported_unsupported_and_issue_46760_49166_pins() {}

/// GO PORT of `plan_cacheable_checker_test.go:510 TestPreparedPlanCacheWithCTE`.
///
/// Re-derived contract: non-recursive CTE prepare produces NO warning
/// (:524-532); executes return c1=5 twice with miss-then-hit answers
/// (:534-538); recursive CTE1 with parametrized depth executes to rows 1..3
/// identically with the same miss-then-hit pattern (:540-552) - recursive and
/// non-recursive CTEs over indexed v2 are fully cacheable.
#[test]
#[ignore = "go-parity-gap: CTE-heavy prepared execution @@last_plan_from_cache checks need sessions"]
fn prepared_plan_cache_with_cte_and_recursive_cte_miss_then_hit() {}

/// GO PORT of `plan_cacheable_checker_test.go:554 BenchmarkNonPreparedPlanCacheableChecker`.
///
/// Re-derived cost model: creates t(a,b) and parses ONE representative SELECT
/// through parser.New().ParseOneStmt, then loops the
/// NonPreparedPlanCacheableWithCtx checker call directly (:560-568+) - pure
/// checker throughput, independent of planning.
#[test]
#[ignore = "go-parity-gap: benchmark over NonPreparedPlanCacheableWithCtx; checker surface unported"]
fn benchmark_non_prepared_plan_cacheable_checker() {}

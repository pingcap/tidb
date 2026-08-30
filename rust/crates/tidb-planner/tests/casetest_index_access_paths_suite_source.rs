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

//! Documentary gap ports for `pkg/planner/core/casetest/index`
//! (`pkg/planner.part4` items 208–224 on `origin/master`): thirteen
//! `index_test.go` tests plus the four `index_prune_bench_test.go`
//! benchmarks.
//!
//! All thirteen tests plan through a live mock-store session and compare
//! `explain format='plan_tree'` rows (and sorted result sets) against either
//! inline `testkit.Rows` or the `integration_suite` / `index_range` books;
//! several require DDL-level fixtures (vector/inverted/columnar indexes,
//! partial indexes with WHERE clauses, 54-index multi-tenant schemas) and
//! failpoint callbacks around
//! `planner/core/rule/InjectCheckForIndexPrune`. None of that surface —
//! prefix-index null single-scan, invisible-index admission, MV
//! filters collection, partial-index pruning ranking, prepared-plan-cache
//! bookkeeping — exists as an owner in this crate. The bootstrap
//! `index/main_test.go:29 TestMain` is skipped-reason: loads both books,
//! goleak. Benchmark bodies keep Go's `Benchmark*` shape as
//! `benchmark_*` names so the batch gate filter `not test(/bench/)` skips
//! them exactly as `go test -run` skips Benchmarks.

/// GO PORT of `pkg/planner/core/casetest/index/index_test.go:38
/// TestNullConditionForPrefixIndex`.
///
/// Re-derived contract: over utf8mb4_bin t1(id char(1), c1 varchar(255),
/// c2 text, KEY idx1(c1), KEY idx2(c1,c2(5))), t2(b varchar(10) prefixed-5)
/// and clustered t3(pk(a,b(5))) with `tidb_opt_prefix_index_single_scan=1`,
/// each suite input must reproduce BOTH its plan_tree explain and its sorted
/// result. The tail (:85-101) pins prepared-plan-cache reuse for
/// `select count(1) from t1 where c1 = ? and c2 is not null` — third execute
/// has @@last_plan_from_cache=1 and its connection explain is EXACTLY
/// StreamAgg→IndexReader(index:StreamAgg(StreamAgg(funcs:count(1)) →
/// IndexRangeScan range:["0xfff" -inf,"0xfff" +inf], stats:pseudo).
#[test]
#[ignore = "go-parity-gap: prefix-index IS NULL single-scan planning and prepared-plan-cache accounting need a live session"]
fn null_condition_for_prefix_index_plans_and_plan_cache() {}

/// GO PORT of `pkg/planner/core/casetest/index/index_test.go:95
/// TestInvisibleIndex`.
///
/// t1 with `KEY(a) INVISIBLE`: explaining `SELECT a FROM t1` must show
/// TableReader/TableFullScan; after `tidb_opt_use_invisible_indexes=on` the
/// SAME query must flip to IndexReader/IndexFullScan — two pinned two-row
/// explains (invisible indexes stay out of enumeration until asked for).
#[test]
#[ignore = "go-parity-gap: invisible-index admission switch has no access-path-side owner here"]
fn invisible_index_excluded_until_session_opt_in() {}

/// GO PORT of `pkg/planner/core/casetest/index/index_test.go:112
/// TestRangeDerivation`.
///
/// With `tidb_opt_fix_control="54337:ON"` and
/// `tidb_regard_null_as_point=false`, primary-key pairs (int and char
/// variants) must derive their full access ranges per the `index_range`
/// book's explain plan_tree goldens over t1/t1char/t/tuk.
#[test]
#[ignore = "go-parity-gap: golden explain outputs need live range derivation end to end"]
fn range_derivation_under_fix_control_54337_golden() {}

/// GO PORT of `pkg/planner/core/casetest/index/index_test.go:140
/// TestRowFunctionMatchTheIndexRangeScan`.
///
/// Row-value comparisons `(k1,k2) match the pk1(k1,k2)` index under fix
/// control 54337: plans AND sorted results are golden for every suite input.
#[test]
#[ignore = "go-parity-gap: row-expression-to-range matching runs inside unported ranger wiring"]
fn row_function_matches_index_range_scan_golden() {}

/// GO PORT of `pkg/planner/core/casetest/index/index_test.go:166
/// TestRangeIntersection`.
///
/// Fixture-heavy intersection net (:168-215): t1 fed through eleven
/// cascading INSERT..SELECTs producing NULL-heavy duplicates, a varbinary
/// PKK table, key-partitioned ENUM/SET/string tables, issue-60556 char keys;
/// under fix control 54337 every input pins plan + sorted result.
#[test]
#[ignore = "go-parity-gap: needs live session execution of the DDL/insert fixture and ranger goldens"]
fn range_intersection_fixture_golden() {}

/// GO PORT of `pkg/planner/core/casetest/index/index_test.go:228
/// TestOrderedIndexWithIsNull`.
///
/// `select a from t1 where b is null order by c` over (a int key, b int,
/// c int, index(b,c)) must explain EXACTLY Projection → IndexReader →
/// IndexRangeScan range:[NULL,NULL] keep order:true stats:pseudo; issue
/// #56116 adds t2(unique nullable id) after analyze whose count(*) plan ends
/// in IndexRangeScan range:[NULL,NULL] too.
#[test]
#[ignore = "go-parity-gap: NULL-point ranges on ordered indexes have no explain pipeline here"]
fn ordered_index_with_is_null_builds_null_point_range_keep_order() {}

/// GO PORT of `pkg/planner/core/casetest/index/index_test.go:249
/// TestVectorIndex`.
///
/// Mock-TiFlash topology + failpoint MockCheckColumnarIndexProcess: vector
/// columns b vector / c vector(3) / d vector(4) with HNSW vecIdx1 over
/// vec_cosine_distance(d); USE INDEX ordering queries MUST pick vecIdx1 in
/// both argument orders while L2-distance ordering and extra WHERE
/// predicates must ERROR.
#[test]
#[ignore = "go-parity-gap: ANN/HNSW index path selection and vector distance error surfaces are unported"]
fn vector_index_cosine_ordering_must_use_hnsw_path() {}

/// GO PORT of `pkg/planner/core/casetest/index/index_test.go:274
/// TestInvertedIndex`.
///
/// Columnar inverted indexes idx_a..idx_d over int-ish columns: FORCE INDEX
/// hits each with its matching predicate shape (`a > 0`, `b < 0`, `c = 0`,
/// `d != 0`) while IGNORE INDEX must show no index at all, checked via
/// MustUseIndex/MustNoIndexUsed.
#[test]
#[ignore = "go-parity-gap: inverted-index usage detection requires executor-backed explain checks"]
fn inverted_index_force_and_ignore_honor_columnar_paths() {}

/// GO PORT of `pkg/planner/core/casetest/index/index_test.go:305
/// TestAnalyzeColumnarIndex`.
///
/// After adding HNSW idx((VEC_COSINE_DISTANCE(b))) and INVERTED idx2(c) and
/// `analyze table t` (version 2): warnings pin that columnar indexes are
/// skipped ("Warning 1105 analyzing columnar index is not supported") twice
/// per run, sample-rate notes are recorded verbatim, and stats-handle probes
/// assert the int column HAS histogram while the vector column does NOT.
#[test]
#[ignore = "go-parity-gap: analyze pipeline and statistics-handle probes live outside this crate"]
fn analyze_skips_columnar_indexes_with_pinned_warnings_and_stats() {}

/// GO PORT of `pkg/planner/core/casetest/index/index_test.go:364
/// TestPartialIndexWithPlanCache`.
///
/// Partial indexes idx1(a) WHERE a IS NOT NULL and idx2(b) WHERE b > 10 with
/// prepared statements: the satisfied-precondition statement IS cached
/// (@@last_plan_from_cache=1, connection explain contains idx1) while the
/// unsatisfiable one stays uncached (=0) yet still uses idx2 — asymmetric
/// plan-cache admission pinned through explain-for-connection.
#[test]
#[ignore = "go-parity-gap: prepared plan cache admission plus partial-index qualification need a live session"]
fn partial_index_preconditions_decide_plan_cache_admission() {}

/// GO PORT of `pkg/planner/core/casetest/index/index_test.go:399
/// TestPartialIndexWithIndexPrune`.
///
/// With tidb_opt_index_prune_threshold=0, `InjectCheckForIndexPrune`
/// failpoint callbacks verify prune outcomes per predicate set: unreferenced
/// idx1 pruned while unmatched-constraint idx2 kept, then the mirrored swap
/// with `order by b` and `a is null` inputs cross-checked by explain
/// containment.
#[test]
#[ignore = "go-parity-gap: rule.InjectCheckForIndexPrune hook and AccessPath pruning pass are not transcreated"]
fn index_prune_threshold_zero_prunes_only_unreferenced_partials() {}

/// GO PORT of `pkg/planner/core/casetest/index/index_test.go:603
/// TestForceIndexLimit`.
///
/// issue:54213 regression: `select count(1) from (select /*+ force_index(tb,
/// ab) */ 1 from tb where a=1 and b=1 limit 100) a` must explain EXACTLY
/// StreamAgg(count(1)) ← Limit ← IndexReader(index:Limit) ← Limit cop ←
/// IndexRangeScan(ab) range:[1 1,1 1] — limit pushed INTO the forced index
/// scan side.
#[test]
#[ignore = "go-parity-gap: exact five-row explain tree over a hinted aggregated limit subquery needs a live planner"]
fn force_index_ab_pushes_limit_into_index_range_scan() {}

/// GO PORT of
/// `pkg/planner/core/casetest/index/index_prune_bench_test.go:203
/// BenchmarkIndexPruneSharedPrefixFullQuery`.
///
/// Planning throughput of the five-predicate workspace+IN(obj_type_id...)+
/// numeric equality + ORDER BY label LIMIT query across a 900-row analyzed
/// obj table whose 54 secondary indexes share the clustered-prefix workspace
/// lead, threshold 20.
#[test]
#[ignore = "go-parity-gap: benchmark harness needs mock-store sessions; see family header"]
fn benchmark_index_prune_shared_prefix_full_query() {}

/// GO PORT of
/// `pkg/planner/core/casetest/index/index_prune_bench_test.go:207
/// BenchmarkIndexPruneSharedPrefixFullQueryNoPrune` — same workload with
/// `tidb_opt_index_prune_threshold=-1`, measuring what stage-1 pruning saves.
#[test]
#[ignore = "go-parity-gap: benchmark harness needs mock-store sessions; see family header"]
fn benchmark_index_prune_shared_prefix_full_query_no_prune() {}

/// GO PORT of
/// `pkg/planner/core/casetest/index/index_prune_bench_test.go:211
/// BenchmarkIndexPruneSharedPrefixOrderOnly` — order-only predicate variant,
/// threshold 20.
#[test]
#[ignore = "go-parity-gap: benchmark harness needs mock-store sessions; see family header"]
fn benchmark_index_prune_shared_prefix_order_only() {}

/// GO PORT of
/// `pkg/planner/core/casetest/index/index_prune_bench_test.go:215
/// BenchmarkIndexPruneSharedPrefixOrderOnlyNoPrune` — pruning disabled.
#[test]
#[ignore = "go-parity-gap: benchmark harness needs mock-store sessions; see family header"]
fn benchmark_index_prune_shared_prefix_order_only_no_prune() {}

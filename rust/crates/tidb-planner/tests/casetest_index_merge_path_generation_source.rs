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

//! Documentary gap ports for `pkg/planner/core/casetest/indexmerge`
//! (`pkg/planner.part4` items 225–234 on `origin/master`): three
//! `indexmerge_intersection_test.go` tests, six `indexmerge_path_test.go`
//! tests and one `indexmerge_test.go` test.
//!
//! The family needs three unported pillars: (1) index-merge access-path
//! generation — Go grows `DataSource.PossibleAccessPaths` with
//! `{Idxs:[...],TbFilters:[...]}` partial alternatives during
//! `RecursiveDeriveStats` once `EnableIndexMerge=true`
//! (indexmerge_test.go:77 TestIndexMergePathGeneration), while this crate's
//! `access_path::get_possible_access_paths` enumerates plain table/index
//! alternatives only; (2) MV-index filter mutation splitting —
//! `CollectFilters4MVIndexMutations` / `PrepareIdxColsAndUnwrapArrayType`
//! are not transcreated anywhere in this workspace; (3) live execution for
//! plan-cache bookkeeping (`@@last_plan_from_cache`,
//! `HasPlanForLastExecution("IndexMerge")`) and golden result sets. The
//! bootstrap `indexmerge/main_test.go:30 TestMain` is skipped-reason: loads
//! the index_merge_suite book, goleak.

/// GO PORT of
/// `pkg/planner/core/casetest/indexmerge/indexmerge_intersection_test.go:29
/// TestPlanCacheForIntersectionIndexMerge`.
///
/// Five single-column indexes ia..ie with a use_index_merge prepared
/// statement (`a = 10 and b = ? and c > ? and d is null and e in (0,100)`):
/// FIRST execute must NOT come from cache (@@last_plan_from_cache=0), the
/// second MUST (=1) and stays cached under new parameters; the last
/// execution's plan must contain "IndexMerge".
#[test]
#[ignore = "go-parity-gap: intersection index-merge plan-cache admission needs prepare/execute + session cache plumbing"]
fn plan_cache_admission_for_intersection_index_merge() {}

/// GO PORT of
/// `pkg/planner/core/casetest/indexmerge/indexmerge_intersection_test.go:48
/// TestIndexMergeWithOrderProperty`.
///
/// Nine-key table t (a/b/c/ab/ac/bc/ae/be/abd/cd indexes) plus t2: every
/// suite input's `explain format='plan_tree'` is golden AND `show warnings`
/// must be EMPTY — order-property-aware merge candidate selection never
/// warns.
#[test]
#[ignore = "go-parity-gap: golden explain trees over order-aware index-merge candidates need a live planner"]
fn index_merge_with_order_property_golden_without_warnings() {}

/// GO PORT of
/// `pkg/planner/core/casetest/indexmerge/indexmerge_intersection_test.go:77
/// TestHintForIntersectionIndexMerge`.
///
/// Wide fixture (:84-135): range/range-columns/hash/list partitioned tables,
/// collation-mixed string tables (utf8mb4_bin/ascii_bin/utf8_unicode_ci/
/// gbk_chinese_ci incl. prefixed nonclustered PKs), typed-key t7 and LOB t8;
/// dynamic prune mode plus analyzed stats via
/// statstestutil.HandleNextDDLEventWithTxn; view vh carries the
/// use_index_merge hint set. Every input pins plan + sorted results +
/// NO warnings; the tail pins issue-65791 primary-or-ia merge returning rows
/// `1 10 100 x`,`2 20 200 y` with HasPlanForLastExecution("IndexMerge").
#[test]
#[ignore = "go-parity-gap: partitioned collation-typed intersection merge planning over hints/views is outside the crate surface"]
fn hinted_intersection_index_merge_over_partitioned_and_collation_tables() {}

/// GO PORT of
/// `pkg/planner/core/casetest/indexmerge/indexmerge_path_test.go:43
/// TestCollectFilters4MVIndexMutations`.
///
/// Over t(a int, b int, domains json, images json, KEY a_domains_b(a,
/// (cast(domains as char(253) array)), b)) the query filters
/// `'15975127' member of (domains) AND '15975128' member of (domains) AND
/// a = 1 AND b = 2` split by `CollectFilters4MVIndexMutations` against
/// `PrepareIdxColsAndUnwrapArrayType` columns must yield EXACTLY three
/// access filters ordered eq(a), JSONMemberOf, eq(b); mvColOffset == 1; two
/// condition mutations, both JSONMemberOf scalar functions.
#[test]
#[ignore = "go-parity-gap: CollectFilters4MVIndexMutations / PrepareIdxColsAndUnwrapArrayType are not transcreated"]
fn collect_filters_4_mv_index_mutations_splits_member_of_filters() {}

/// GO PORT of
/// `pkg/planner/core/casetest/indexmerge/indexmerge_path_test.go:117
/// TestMultiMVIndexRandom`.
///
/// Randomized cross-check across signed/unsigned/char(3)/date multi-value
/// array indexes: for twenty queries per fixture (second half OR-shaped),
/// `ignore_index` results must equal `use_index_merge(idx,idx2,idx3,idx4)`
/// results AND the prepared-statement execution of parameterized conds under
/// fix control 45798:on must match too.
#[test]
#[ignore = "go-parity-gap: randomized execution parity requires a working store plus merge-capable planner"]
fn multi_mv_index_random_merge_equals_ignore_index_results() {}

/// GO PORT of
/// `pkg/planner/core/casetest/indexmerge/indexmerge_path_test.go:188
/// TestMVIndexRandom`.
///
/// Single-array-index variant over t(a int, j json, kj((cast(j as T)
/// array))): same ignore-vs-use_index_merge equality per random member-of /
/// json_contains / json_overlaps / numeric conds, plus the parameterized
/// execute equivalence.
#[test]
#[ignore = "go-parity-gap: same missing randomized-execution harness as multi_mv_index_random"]
fn mv_index_random_merge_equals_ignore_index_results() {}

/// GO PORT of
/// `pkg/planner/core/casetest/indexmerge/indexmerge_path_test.go:248
/// TestPlanCacheMVIndex`.
///
/// Multi-json-array-index ti schema (domains/signatures/short_link/
/// long_link/f_item_ids/f_profile_ids/products arrays + plain keys) fed with
/// fifty random rows: fifty check() rounds decide per shape whether the
/// prepared statement hits the cache (member-of-only unions: hitCache=true)
/// or must WARN instead (json_contains/json_overlaps mixes:
/// hitCache=false ⇒ show warnings non-empty) — plan-cache poisoning rules
/// for MV indexes pinned statistically through random values.
#[test]
#[ignore = "go-parity-gap: MV-index plan-cacheability classification runs inside unported path generation"]
fn plan_cache_mv_index_hit_and_miss_shapes() {}

/// GO PORT of
/// `pkg/planner/core/casetest/indexmerge/indexmerge_path_test.go:481
/// TestAnalyzeVectorIndex` (shared harness
/// testAnalyzeTiFlashIndex :472-506).
///
/// vector(2)/vector(3) columns with HNSW idx/idx2 added behind failpoint
/// MockCheckColumnarIndexProcess and an enabled replica: BOTH
/// `analyze table t` and `analyze table t index idx` produce the pinned
/// warning triplets ("analyzing columnar index is not supported, skip idx")
/// alongside sample-rate notes; sorted-warning comparison.
#[test]
#[ignore = "go-parity-gap: analyze pipeline warnings and mock TiFlash DDL topology are unported"]
fn analyze_vector_index_reports_skip_warnings() {}

/// GO PORT of
/// `pkg/planner/core/casetest/indexmerge/indexmerge_path_test.go:486
/// TestAnalyzeColumnarIndex` — inverted idx/idx2 variant of the same
/// harness with identical pinned warning text.
#[test]
#[ignore = "go-parity-gap: same analyze-pipeline surface as analyze_vector_index"]
fn analyze_columnar_index_reports_skip_warnings() {}

/// GO PORT of
/// `pkg/planner/core/casetest/indexmerge/indexmerge_test.go:77
/// TestIndexMergePathGeneration`.
///
/// Pipeline without a store but WITH the real builder: MockContext +
/// MockInfoSchema(MockSignedTable,MockView); parse → Preprocess →
/// PlanBuilder.Build → LogicalOptimizeTest; walk down to the DataSource;
/// enable index merge; after RecursiveDeriveStats the digest
/// `[ {Idxs:[{alternatives…}],TbFilters:[exprs]}, … ]` appended past the
/// pre-existing PossibleAccessPaths must equal the recorded book output for
/// each case (build errors are themselves recorded outputs); whenever the
/// digest is non-empty, RUV2Metrics.PlanDeriveStatsPaths() must equal
/// len(PossibleAccessPaths).
#[test]
#[ignore = "go-parity-gap: no index-merge alternative growth during derive stats, no path-digest renderer, no RUV2Metrics counter on this crate's DataSource"]
fn index_merge_path_generation_digests_match_book() {}

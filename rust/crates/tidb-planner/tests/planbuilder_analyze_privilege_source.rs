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

//! `pkg/planner.part14` DOCUMENTED GAP ports for
//! `pkg/planner/core/planbuilder_test.go` (19 items: tests at :63, :99,
//! :164, :196, :229, :277, :312, :340, :649, :714, :749, :803, :831, :887,
//! :1011, :1052, :1140, :1170, :1227).
//!
//! These tests exercise UNEXPORTED `core` package internals — the
//! `PlanBuilder` rewriter pool, `getPathByIndexName`, `buildShowSchema`,
//! `handleAnalyzeOptions`, `getFullAnalyzeColumnsInfo`, the IMPORT INTO
//! column-assignment checker visitor, `buildRefreshStats`, and the
//! reflection-based `checkDeepClonedCore` pointer-identity walker — through
//! `coretestsdk.MockContext()` sessions. None of these builders exist in
//! this crate. The ordinary SQL→plan builder now resolves index names in
//! `tidb-planner/src/access_path.rs`, but it does not expose Go's unexported
//! `[]*util.AccessPath` helper contract directly. Each item below records its re-derived Go
//! contract as an `#[ignore]` gap port; nothing is approximated.

/// GO PARITY GAP port of `pkg/planner/core/planbuilder_test.go:63
/// TestShow`.
///
/// go-parity-gap: `buildShowSchema` unported. Go walks 23 `ast.ShowStmtType`
/// values and requires every schema column of the built SHOW result schema
/// to carry a positive `RetType.GetFlen()`.
#[test]
#[ignore = "go-parity-gap: buildShowSchema over ast.ShowStmt unported"]
fn show_stmt_result_schema_columns_have_positive_flen() {}

/// GO PARITY GAP port of `pkg/planner/core/planbuilder_test.go:99
/// TestGetPathByIndexName`.
///
/// go-parity-gap: `getPathByIndexName`/`removeIgnoredPaths` over
/// `[]*util.AccessPath` unported as a directly callable helper. Go pins: name `idx` resolves to that
/// index path; prefix `id` ALSO resolves to `idx` (prefix match);
/// `primary` resolves to the int-handle path only when `PKIsHandle`; a
/// missing name yields nil; and with `PKIsHandle=false` `primary` yields
/// nil. The subtest pins that ignoring an EXACT or prefix-resolved long
/// index name removes exactly that path while keeping the shorter sibling
/// (`idx_contract_sys_no` stays when
/// `idx_contract_sys_no_delete_flag`/`..._delete` is ignored, case-insensitively).
#[test]
#[ignore = "go-parity-gap: getPathByIndexName/removeIgnoredPaths over []*util.AccessPath unported"]
fn get_path_by_index_name_prefix_primary_and_ignore_removal() {}

/// GO PARITY GAP port of `pkg/planner/core/planbuilder_test.go:164
/// TestRewriterPool`.
///
/// go-parity-gap: the PlanBuilder rewriter pool (`getExpressionRewriter`,
/// `rewriterCounter`) unported. Go dirties a pooled expression rewriter
/// (asScalar, aggrMap, preprocess hook, insertPlan, disableFoldCounter,
/// ctxStack/ctxNameStk) and requires the NEXT pooled pickup to be fully
/// reset — same rewriter identity, zeroed state, empty stacks.
#[test]
#[ignore = "go-parity-gap: PlanBuilder rewriter pool internals unported"]
fn rewriter_pool_hands_back_a_clean_rewriter() {}

/// GO PARITY GAP port of `pkg/planner/core/planbuilder_test.go:196
/// TestGetInsertColExprDeepCopiesValueExprFieldType`.
///
/// go-parity-gap: `PlanBuilder.getInsertColExpr` unported. Go pins that the
/// produced `expression.Constant` owns a DEEP COPY of the value expression's
/// FieldType: mutating the constant's type to TypeString and dropping
/// NotNullFlag leaves the source `driver.ValueExpr` type at TypeLonglong
/// with NotNullFlag intact.
#[test]
#[ignore = "go-parity-gap: getInsertColExpr builder path unported"]
fn get_insert_col_expr_deep_copies_value_expr_field_type() {}

/// GO PARITY GAP port of `pkg/planner/core/planbuilder_test.go:229
/// TestDisableFold`.
///
/// go-parity-gap: the expression rewrite pipeline (`rewriteExprNode` +
/// `disableFoldCounter`) unported. Go pins that `sin(length("abc"))` folds
/// to a `Constant`, while `benchmark(3, sin(123))` and
/// `pow(length("abc"), benchmark(3, sin(123)))` stay `ScalarFunction`s with
/// the nested shapes preserved (BENCHMARK must not fold to 0) — and the
/// rewriter's disableFoldCounter returns to 0 after the rewrite.
#[test]
#[ignore = "go-parity-gap: rewriteExprNode/disableFoldCounter pipeline unported"]
fn disable_fold_keeps_benchmark_subtree_unfolded() {}

/// GO PARITY GAP port of `pkg/planner/core/planbuilder_test.go:277
/// TestDeepClone`.
///
/// go-parity-gap: Go's `checkDeepClonedCore` reflection walker reports the
/// exact shared-pointer PATH; ownership semantics make the observation
/// unrepresentable over this crate's owned plan values. Go pins four
/// escalation steps on cloned `PhysicalSort`s sharing ByItems: "same slice
/// pointers, path *PhysicalSort.ByItems" → "same pointer, path
/// *PhysicalSort.ByItems[0].Expr" → "same pointer, path
/// *PhysicalSort.ByItems[0].Expr.RetType" → "different values, path
/// *PhysicalSort.ByItems[0].Expr.RetType.tp" → no error once every field is
/// deep-copied.
#[test]
#[ignore = "go-parity-gap: reflect-based shared-pointer path walker unrepresentable over owned plan values"]
fn deep_clone_rejects_shared_pointers_with_exact_paths() {}

/// GO PARITY GAP port of `pkg/planner/core/planbuilder_test.go:312
/// TestTablePlansAndTablePlanInPhysicalTableReaderClone`.
///
/// go-parity-gap: Go requires `Clone` to keep `TablePlan` POINTER-ALIASED to
/// `TablePlans[0]` on the clone (`newTableReader.TablePlan ==
/// newTableReader.TablePlans[0]`); this crate's owned trees cannot alias a
/// child into two fields.
#[test]
#[ignore = "go-parity-gap: pointer aliasing between TablePlan and TablePlans[0] unrepresentable over owned plan trees"]
fn table_reader_clone_keeps_table_plan_aliased_to_first_table_plan() {}

/// GO PARITY GAP port of `pkg/planner/core/planbuilder_test.go:340
/// TestPhysicalPlanClone`.
///
/// go-parity-gap: needs `Clone` over 14 Go operator types plus the
/// reflection deep-clone walker. Go builds table/index scan + reader,
/// index look-up (with PushedDownLimit and ExtraHandleCol), selection,
/// maxOneRow, projection, limit, sort, topN, stream/hash agg (avg +
/// count-distinct descriptors), hash/merge/index joins and requires
/// `checkPhysicalPlanClone` — deep clone with only StatsInfo/session/
/// FieldType pointers whitelisted — to pass for every operator.
#[test]
#[ignore = "go-parity-gap: per-operator Clone over the 14-operator matrix + reflect deep-clone checker unported"]
fn physical_plan_clone_deep_clones_every_operator_family() {}

/// GO PARITY GAP port of `pkg/planner/core/planbuilder_test.go:649
/// TestHandleAnalyzeOptions`.
///
/// go-parity-gap: `handleAnalyzeOptions` unported. Go pins four rejections:
/// TOPN above 100000 ("should not be larger than 100000"), SAMPLERATE above
/// 1 ("should not larger than 1.000000, and should be greater than 0"),
/// BUCKETS above 100000 ("should be positive and not larger than 100000"),
/// and setting both NUMSAMPLES and SAMPLERATE ("can only either set the
/// value of the sample num or set the value of the sample rate").
#[test]
#[ignore = "go-parity-gap: handleAnalyzeOptions over ast.AnalyzeOpt unported"]
fn handle_analyze_options_rejects_out_of_range_values() {}

/// GO PARITY GAP port of `pkg/planner/core/planbuilder_test.go:714
/// TestAnalyzeBucketAndTopNDefaultsFromGlobalVars`.
///
/// go-parity-gap: `handleAnalyzeOptions`/`fillAnalyzeOptions`/
/// `AnalyzeOptionDefault` plus the global `AnalyzeDefaultNumBuckets`/
/// `AnalyzeDefaultNumTopN` variables unported. Go stores 512/150 into the
/// globals and requires the DEFAULTS path to pick them up, an explicit
/// BUCKETS=1024 to override the bucket default while TopN stays 150, and
/// `AnalyzeOptionDefault()` to report the global-derived values.
#[test]
#[ignore = "go-parity-gap: analyze option filling + global default atomics unported"]
fn analyze_bucket_and_topn_defaults_follow_global_vars() {}

/// GO PARITY GAP port of `pkg/planner/core/planbuilder_test.go:749
/// TestGetFullAnalyzeColumnsInfo`.
///
/// go-parity-gap: `PlanBuilder.getFullAnalyzeColumnsInfo` unported. Go pins
/// `AllColumns` returning every table column in order, and `ColumnList`
/// returning exactly the specified columns (id/name/age with an
/// `mustAnalyzedCols` map pre-seeded with column 3).
#[test]
#[ignore = "go-parity-gap: getFullAnalyzeColumnsInfo builder helper unported"]
fn get_full_analyze_columns_info_all_and_column_list() {}

/// GO PARITY GAP port of `pkg/planner/core/planbuilder_test.go:803
/// TestRequireInsertAndSelectPriv`.
///
/// go-parity-gap: `PlanBuilder.requireInsertAndSelectPriv` and `visitInfo`
/// unported. Go requires two `TableName`s (`test.t1`, `Test.T2`) to record
/// FOUR visitInfo entries: InsertPriv then SelectPriv per table, with the
/// schema/table names lower-cased.
#[test]
#[ignore = "go-parity-gap: visitInfo privilege recording unported"]
fn require_insert_and_select_priv_records_one_pair_per_table() {}

/// GO PARITY GAP port of `pkg/planner/core/planbuilder_test.go:831
/// TestBuildRefreshStatsPrivileges`.
///
/// go-parity-gap: `buildRefreshStats` unported. Go pins that REFRESH STATS
/// records exactly ONE SelectPriv visitInfo whose scope follows the
/// statement: table `test.t1` → db test/table t1; `test.*` → db test, no
/// table; `*.*` → neither.
#[test]
#[ignore = "go-parity-gap: buildRefreshStats + visitInfo unported"]
fn build_refresh_stats_privileges_follow_statement_scope() {}

/// GO PARITY GAP port of `pkg/planner/core/planbuilder_test.go:887
/// TestImportIntoCollAssignmentChecker`.
///
/// go-parity-gap: `newImportIntoCollAssignmentChecker` AST visitor
/// unported. Go pins 24 expression cases: user vars (`@a+1`, `@b+@c+@1`,
/// `getvar('c')`) record neededVars; plain column refs and `values(a)`
/// error "COLUMN reference is not supported"; subqueries/exists/in-subquery
/// error "subquery is not supported"; `@@sql_mode` errors on system
/// variables; `@a:=1` errors on variable assignment; `default(t.a)`,
/// window/aggregate/grouping functions error by kind; `getvar` with a
/// non-constant or non-string argument errors; unknown functions error —
/// each message suffixed ", index <i>".
#[test]
#[ignore = "go-parity-gap: IMPORT INTO column-assignment checker AST visitor unported"]
fn import_into_coll_assignment_checker_enforces_expression_rules() {}

/// GO PARITY GAP port of `pkg/planner/core/planbuilder_test.go:1011
/// TestTraffic`.
///
/// go-parity-gap: the Traffic plan builder unported. Go pins that capture /
/// replay / show jobs / cancel jobs build `*Traffic` plans whose
/// `OutputNames()` have 8 columns for `show traffic jobs` (0 for the rest)
/// and whose first visitInfo entry carries the matching dynamic privileges
/// (TRAFFIC_CAPTURE_ADMIN, TRAFFIC_REPLAY_ADMIN, or both).
#[test]
#[ignore = "go-parity-gap: Traffic statement builder + dynamic privilege visitInfo unported"]
fn traffic_stmts_build_traffic_plans_with_dynamic_privs() {}

/// GO PARITY GAP port of `pkg/planner/core/planbuilder_test.go:1052
/// TestBuildAdminAlterDDLJobPlan`.
///
/// go-parity-gap: the ADMIN ALTER DDL JOBS builder unported. Go pins six
/// statements: `thread = 16` → one option AlterDDLJobThread with int64 16;
/// `batch_size = 512` → AlterDDLJobBatchSize 512; `max_write_speed =
/// '10MiB'` → AlterDDLJobMaxWriteSpeed with string "10MiB"; a bare `1024`
/// max_write_speed yields an int64 constant; three options sort by name;
/// and an unknown key `aaa` errors "unsupported admin alter ddl jobs
/// config: aaa" — each with the statement's JobID preserved.
#[test]
#[ignore = "go-parity-gap: AlterDDLJob plan builder unported"]
fn build_admin_alter_ddl_job_plan_options_and_errors() {}

/// GO PARITY GAP port of `pkg/planner/core/planbuilder_test.go:1140
/// TestGetMaxWriteSpeedFromExpression`.
///
/// go-parity-gap: `GetMaxWriteSpeedFromExpression` and the go-units parser
/// unported. Go pins that a random byte value N (0..1PiB) built as an int
/// constant round-trips exactly through the option, and that a string
/// constant "MiB" fails with "parse max_write_speed value error: invalid
/// size: 'MiB'".
#[test]
#[ignore = "go-parity-gap: GetMaxWriteSpeedFromExpression + go-units byte-size parser unported"]
fn get_max_write_speed_from_expression_roundtrips_or_rejects() {}

/// GO PARITY GAP port of `pkg/planner/core/planbuilder_test.go:1170
/// TestProcessNextGenS3Path`.
///
/// go-parity-gap: `checkNextGenS3PathWithSem` unported (the Rust
/// `tidb_util::sem` leaf exposes the SEM toggles only). Go pins, with the
/// global keyspace set to "aaa": S3/OSS URLs carrying an explicit
/// `External-id`/`external_id` error `ErrNotSupportedWithSem` with "IMPORT
/// INTO with explicit external ID"; URLs WITHOUT access-key/secret-key or
/// role-arn error with "IMPORT INTO from S3-like storage without access
/// key/secret access key or role ARN"; and matching-keyspace
/// `external-id=aaa` or credentialed/role-arn URLs pass.
#[test]
#[ignore = "go-parity-gap: checkNextGenS3PathWithSem URL rules unported"]
fn process_next_gen_s3_path_enforces_sem_rules() {}

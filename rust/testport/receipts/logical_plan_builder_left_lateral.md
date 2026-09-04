# LEFT JOIN LATERAL plan building — Rust parity receipt

Comparison source: Go `origin/master` at `f2c346fe4f368ff855e17c1f62e28a89ba7f9723`.
The behavior authority is `d152e4b78d` (`planner: support LEFT JOIN LATERAL`).
This is a Rust-only batch; no Go source or Bazel metadata was edited.

## Complete Go owner inventory

The direct Go owner is `pkg/planner/core`. Before editing, all 107 tracked
top-level artifacts (106 Go files plus `BUILD.bazel`), totaling 73,291 lines,
were enumerated, including production files, unit/integration tests, generated
or build metadata, and every test variant:

```text
BUILD.bazel
access_object.go cbo_test.go columnar_index_utils.go common_plans.go
common_plans_test.go core_init.go encode.go enforce_mpp_test.go
exhaust_physical_plans.go exhaust_physical_plans_test.go expression_codec_fn.go
expression_rewriter.go expression_test.go find_best_task.go find_best_task_test.go
flat_plan.go fts_resolve_index.go fts_resolve_index_test.go fulltext_to_like.go
fulltext_to_like_test.go hint_test.go hint_utils.go index_join_path.go
indexmerge_path.go indexmerge_unfinished_path.go initialize.go integration_test.go
lateral_join_test.go logical_initialize.go logical_plan_builder.go
logical_plans_test.go main_test.go memtable_infoschema_extractor.go
memtable_predicate_extractor.go optimizer.go optimizer_test.go
panicrisk_regression_test.go pb_to_plan.go physical_plan_test.go plan.go
plan_cache.go plan_cache_instance.go plan_cache_instance_test.go
plan_cache_lru.go plan_cache_lru_test.go plan_cache_param.go
plan_cache_rebuild.go plan_cache_utils.go plan_cacheable_checker.go
plan_clone_utils.go plan_cost_ver1.go plan_cost_ver2.go plan_cost_ver2_test.go
plan_replayer_capture_test.go plan_test.go plan_to_pb_test.go planbuilder.go
planbuilder_test.go point_get_plan.go preprocess.go preprocess_test.go
property_cols_prune.go recheck_cte.go resolve_indices.go
rule_aggregation_elimination.go rule_aggregation_push_down.go
rule_aggregation_skew_rewrite.go rule_correlate.go rule_decorrelate.go
rule_derive_topn_from_window.go rule_eliminate_empty_selection.go
rule_eliminate_projection.go rule_eliminate_unionall_dual_item.go
rule_generate_column_substitute.go rule_generate_column_substitute_test.go
rule_inject_extra_projection.go rule_join_elimination.go rule_join_reorder.go
rule_join_reorder_dp.go rule_join_reorder_dp_test.go rule_join_reorder_greedy.go
rule_join_reorder_projection_inline.go rule_outer_to_inner_join.go
rule_predicate_push_down.go rule_push_down_sequence.go
rule_resolve_grouping_expand.go rule_result_reorder.go rule_semi_join_rewrite.go
rule_topn_push_down.go runtime_filter_generator.go
runtime_filter_generator_test.go scalar_subq_expression.go schema_table_key.go
show_predicate_extractor.go stats.go stats_test.go stringer.go stringer_test.go
subquery_plan_builder.go task.go task_heavy_function_optimize_test.go
task_test.go telemetry.go trace.go util.go util_test.go
```

The relevant Go source was read function-by-function in
`logical_plan_builder.go`: `buildJoin`'s LATERAL dispatch and
`buildLateralJoin`'s natural/using rejection, join-type selection, outer-join
flags, schema/name/full-schema construction, nullability reset, ON-condition
attachment, handle-map merge, and hint propagation. The LATERAL cases in
`lateral_join_test.go` and the cardinality/cache cases in `integration_test.go`
were checked as consumers. Go master accepts `LEFT JOIN LATERAL`, rejects only
RIGHT/NATURAL/USING variants, and clears NOT NULL from the inner columns in
both visible Schema and FullSchema.

## Rust change

`PlanBuilder::build_lateral_join` previously rejected LEFT JOIN with a
Rust-only `ErrInvalidLateralJoin`. It now follows Go's branch: constructs a
`LogicalApply` with `LeftOuter`, enables outer-join elimination/semi-join
flags, and resets NOT NULL on the inner portion of both Schema and FullSchema.
The existing ON-condition, output-name, full-schema, handle-map, and hint
paths remain shared with INNER/CROSS LATERAL behavior. RIGHT JOIN, NATURAL, and
USING remain rejected as in Go.

Changed Rust owners:

- `crates/tidb-planner/src/plan_builder/from.rs` — LEFT LATERAL construction;
- `crates/tidb-planner/src/plan_builder/from_tests.rs` — positive nullable
  LEFT LATERAL regression and narrowed RIGHT-only rejection test.

## Regression evidence

```text
cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml --offline --locked \
  -p tidb-planner --lib \
  plan_builder::from_tests::test_left_lateral_builds_a_nullable_outer_apply \
  -- --nocapture
# passed: 1 test

cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml --offline --locked \
  -p tidb-planner --lib \
  plan_builder::from_tests::test_lateral_refuses_the_clauses_go_refuses \
  -- --nocapture
# passed: 1 test
```

The positive regression verifies `LeftOuter`, `IsLateral`, the two outer-join
optimization flags, and nullable inner columns in both Schema and FullSchema.

## Ready validation

The final batch validation is recorded after the commit and includes:

```text
cargo +nightly-2026-08-22 fmt --manifest-path rust/Cargo.toml --all -- --check
# passed
cargo +nightly-2026-08-22 check --manifest-path rust/Cargo.toml --offline --locked \
  -p tidb-planner
# passed; existing warnings only
git diff --check
# passed
PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
TMPDIR=/tmp/tidb-codex make lint
# passed (dashboard linter and Go lint targets)
```

The broader Go testkit LATERAL execution cases and Rust SQL execution remain
separate runtime boundaries; this batch closes the plan-builder LEFT LATERAL
admission and nullability behavior.

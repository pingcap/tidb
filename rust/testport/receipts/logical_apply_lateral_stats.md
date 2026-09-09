# LATERAL Apply cardinality — Rust parity receipt

Comparison source: Go `origin/master` at `f2c346fe4f368ff855e17c1f62e28a89ba7f9723`.
The behavior authority is `c0e4137490` (`planner: correct LATERAL join
cardinality and the Apply cache decision`). This is a Rust-only batch; no Go
source was edited.

## Complete Go owner inventory

Before editing, the direct Go `pkg/planner/core/operator/logicalop` package and
its nested test package were enumerated file-by-file: 43 tracked artifacts,
16,012 lines total. The inventory included every production operator, tests,
test fixtures, Bazel target, generated file, and platform/build variant:

```text
BUILD.bazel
base_logical_plan.go expression_util.go hash64_equals_generated.go
logical_aggregation.go logical_apply.go logical_cte.go logical_cte_table.go
logical_datasource.go logical_expand.go logical_index_scan.go logical_join.go
logical_limit.go logical_lock.go logical_max_one_row.go logical_mem_table.go
logical_mock.go logical_partition_union_all.go logical_plans_misc.go
logical_projection.go logical_schema_producer.go logical_selection.go
logical_sequence.go logical_show.go logical_show_ddl_jobs.go logical_sort.go
logical_table_dual.go logical_table_scan.go logical_tikv_single_gather.go
logical_top_n.go logical_union_all.go logical_union_scan.go logical_window.go
logicalop_test/BUILD.bazel
logicalop_test/hash64_equals_test.go
logicalop_test/logical_mem_table_predicate_extractor_test.go
logicalop_test/logical_operator_test.go logicalop_test/main_test.go
logicalop_test/plan_execute_test.go
logicalop_test/testdata/cascades_suite_in.json
logicalop_test/testdata/cascades_suite_out.json
logicalop_test/testdata/cascades_suite_xut.json
shallow_ref_generated.go
```

The relevant Go `LogicalApply` surface was then read function-by-function:
`Init`, `ExplainInfo`, `ReplaceExprColumns`, `findChildFullSchema`,
`PruneColumns`, `DeriveStats`, `ExtractColGroups`, `ExtractCorrelatedCols`,
`ExtractFD`, `CanPullUpAgg`, `DeCorColFromEqExpr`, and `getGroupNDVs`. The
consumer tests in `pkg/planner/core/integration_test.go` and
`pkg/planner/core/lateral_join_test.go` were also checked, including the
cardinality and LEFT/CROSS/INNER LATERAL cases. The latest Go implementation
uses `EstimateFullJoinRowCount` only when `GetJoinKeys` returns explicit left
keys; a correlated lateral subtree without an explicit key uses
`leftProfile.RowCount * rightProfile.RowCount`, with LEFT OUTER floored at the
outer count.

## Rust change

The Rust recursive stats driver previously rejected keyed lateral Apply nodes
with an `unported_stats` error. It now builds the same
`FullJoinRowCountInput` as the ordinary logical-join path, using the existing
NDV estimator and `TiDBOptJoinReorderThreshold`, and passes the result into
`LogicalApply::derive_stats`. The operator now recognizes exactly Go's
explicit-key branch (not correlation alone), stores the keyed estimate in its
embedded join's `equal_cond_out_cnt`, and documents the product fallback.

Changed Rust owners:

- `crates/tidb-planner/src/logical/rewrite.rs` — recursive Apply estimator;
- `crates/tidb-planner/src/logical/apply.rs` — branch predicate, state, and
  source-aligned documentation;
- `crates/tidb-planner/src/logical/derive_stats_tests.rs` — recursive keyed
  lateral regression;
- `crates/tidb-planner/src/logical/operator_tests.rs` — explicit-key state and
  correlated product-fallback regressions.

## Regression evidence

```text
cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml --offline --locked \
  -p tidb-planner --lib \
  logical::operator_tests::apply_reports_when_the_lateral_estimate_is_mandatory \
  -- --nocapture
# passed: 1 test

cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml --offline --locked \
  -p tidb-planner --lib \
  logical::derive_stats_tests::a_lateral_apply_estimates_explicit_keys_like_a_full_join \
  -- --nocapture
# passed: 1 test
```

The first regression also checks that an explicit keyed estimate is retained
as `equal_cond_out_cnt` and that a correlation without an explicit key does
not request the keyed estimator.

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

The broad Go lateral integration tests and the Rust planner's unrelated
storage/fixture suites remain separate runtime boundaries; this batch closes
the Rust recursive cardinality gap only.

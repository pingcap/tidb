# `pkg/planner/core` common-handle tuple range parity receipt

Go comparison commits: `70cff7d9f663d9f4e86c2f8ff889902f20e2e9df`
(`planner: give every appended handle column a length in selectivity`) and
its range-extension predecessor `12d24af9894`. The Go source was read from
`origin/master`; no Go file was changed.

## Complete inventory

The `pkg/planner/core` tree was walked file by file before editing: 568
artifacts (362 Go production/test files, 60 Bazel build files, 145 JSON
fixtures, and one archive fixture). The related `pkg/planner/cardinality`
tree was also included: 18 artifacts (15 Go files, one build file, and two
JSON fixtures). The existing complete inventories in `planner_empty_range.md`
and `b078.md` remain the authoritative per-file ledgers; this receipt records
the newly compared owners, `core/find_best_task.go`, `core/stats.go`,
`cardinality/selectivity.go`, and the `rule_common_handle_range_test.go`
regression, including their generated/build variants.

## Restored behavior

Go's `fillIndexPath` appends the complete clustered common handle to a
non-unique secondary index when its declared index prefix is fully resolved.
Rust's `find_best_task` now appends those columns and their declared lengths
before ranger construction. The same exclusions as Go are enforced for
unique/primary, global, multi-valued, and columnar indexes, incomplete handle
metadata, duplicate handle columns, and V0 new-collation string handles.

This makes a tuple comparison over `KEY ia(a)` and clustered `PRIMARY KEY
(b,c)` use the physical key `(a,b,c)`, producing the three lexicographic
ranges `(1 2 3,1 2 +inf]`, `(1 2,1 +inf]`, and `(1,+inf]`. A three-column
clustered handle reaches its fourth tuple dimension as well. Residual
predicates and lookup plans remain unchanged.

## Regression coverage

- `tidb-planner::find_best_task::dispatch::tests::a_secondary_index_range_reaches_common_handle_columns`
  constructs the post-rewrite DNF and asserts both the two-column and
  metadata path semantics at the physical scan boundary.
- `tidb-session::tests_explain::common_handle_tuple_comparison_uses_appended_index_ranges`
  executes the SQL shape from Go's `TestCommonHandleIndexRangesWithTupleCompare`,
  checks the EXPLAIN ranges, verifies returned rows, and covers a three-column
  common handle.
- The documentary casetest source remains ignored only for its unavailable
  full Go casetest harness; the active planner/session tests above now cover
  the behavior that was previously marked `go-parity-gap`.

## Ready validation

```text
cargo test --offline --locked -p tidb-planner a_secondary_index_range_reaches_common_handle_columns -- --nocapture
cargo test --offline --locked -p tidb-session common_handle_tuple_comparison_uses_appended_index_ranges -- --nocapture
git diff --check
cargo fmt --all -- --check
PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
  GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
  TMPDIR=/tmp/tidb-codex make lint
```

The Rust-only planner owners, receipt, and ExecPlan were changed; Go,
Bazel, generated, and platform source files were left untouched.

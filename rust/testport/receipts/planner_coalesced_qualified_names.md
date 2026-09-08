# `pkg/planner/core` — coalesced `USING`/`NATURAL JOIN` qualified-name receipt

## Scope and complete owner inventory

This batch follows Go-master (`a0cdff369bd4c7060a840e3943049a79470e8af4`,
2026-09-06) for the name-resolution and column-pruning contract of coalesced
join columns. Before editing, the complete tracked Go package tree was
inventoried with:

```text
git ls-tree -r --name-only master -- pkg/planner/core
git grep -n -E '^func ' master -- 'pkg/planner/core/**/*.go'
```

| Go package tree | Tracked files | Go production | Go tests | non-Go/build/fixture inputs |
| --- | ---: | ---: | ---: | ---: |
| `pkg/planner/core` (including operator subtrees, tests, and testdata) | 544 | 192 | 152 | 59 Bazel/build files, 141 fixture/testdata/golden matches, 3 generated/bootstrap-marked files |

The review covered every listed production and test file, all generated and
platform variants, build inputs, fixtures, and metadata; 2,625 Go function
declarations were inventoried. No Go file was edited. The direct Go owners
are:

* `pkg/planner/core/logical_plan_builder.go:736-1060` (`buildJoin`),
  `:1104-1360` (`buildUsingClause`, `buildNaturalJoin`, and
  `coalesceCommonColumns`), and `:1657-1685`
  (`findColFromNaturalUsingJoin`) — construct the visible schema while
  retaining redundant qualified columns in `FullSchema`/`FullNames`;
* `pkg/planner/core/expression_rewriter.go:2717-2880` — resolves qualified
  references against the join's full name set and remaps redundant columns
  where the Go plan shape permits it; and
* `pkg/planner/core/operator/logicalop/logical_join.go:83-120`, `:794-850`,
  and `:1200+` — defines `FullSchema`/`FullNames`, redundant-column mapping,
  and used-column extraction. Its `planCanResolveUsedCol` recursion treats
  selections, limits, sorts, and max-one-row wrappers as transparent for
  pruning, but treats projections as derived-table boundaries.

## Failure and implementation

The Rust planner resolved every expression against the executable visible
schema. After `JOIN ... USING (a)` or `NATURAL JOIN`, that schema contains one
canonical `a`; a qualified `n2.a` therefore failed with
`UnknownColumn("n2.a")`. Column pruning also passed only visible child schemas
to `LogicalJoin::extract_used_cols`, so the hidden side could be removed before
the qualified reference was evaluated. Qualified wildcard expansion similarly
omitted the redundant side when the current FROM node was the join itself.

The Rust parity change is deliberately Rust-only:

* `rust/crates/tidb-planner/src/plan_builder.rs:699-982,1332-1380` adds an
  optional full schema/name scope to `PlanScopeResolver`; visible names remain
  authoritative for unqualified lookup, with `FullNames` as a qualified-only
  fallback. Plan-aware scalar, projection, sort, selection, and GROUP BY
  rewriting now carry that scope through transparent wrappers.
* `rust/crates/tidb-planner/src/plan_builder/from.rs:1037-1060,1173-1190`
  rewrites plain-join and lateral-apply `ON` expressions with the plan-aware
  resolver.
* `rust/crates/tidb-planner/src/plan_builder.rs:2515-2565` mirrors Go's
  wildcard rule: qualified `join_alias.*` reads `FullSchema` only for a
  direct join/apply node; an inner join wrapped by an `ON` selection remains a
  visible-schema boundary.
* `rust/crates/tidb-planner/src/logical/rewrite.rs:81-110,1076-1095` uses a
  full-capable child schema for join pruning through Go-transparent wrappers,
  while retaining visible schemas at projection/derived-table boundaries.

## Regression coverage

`rust/crates/tidb-session/src/tests_coalesced_joins.rs` adds
`qualified_using_column_survives_projection_pruning`, which asserts that
`SELECT n2.a FROM n1 JOIN n2 USING (a)` returns `1` after pruning. The focused
matrix also covers either qualifier, nested natural joins, row-preserving
outer joins, qualified wildcard rules, join-column order, and the plain-join
boundary.

Focused commands:

```text
cargo +nightly-2026-08-22 test --offline --locked --manifest-path rust/Cargo.toml \
  -p tidb-session --lib tests_coalesced_joins::qualified_using_column_survives_projection_pruning \
  -- --nocapture --test-threads=1
cargo +nightly-2026-08-22 test --offline --locked --manifest-path rust/Cargo.toml \
  -p tidb-session --lib tests_coalesced_joins -- --nocapture --test-threads=1
```

The new regression and seven adjacent parity tests pass individually. The
complete module is **14 passed, 5 failed**; the five failures are existing
GROUP BY alias/error-code and parallel HashAgg-worker baseline failures, not
coalesced qualified-name regressions. Before this batch, the same module
failed on qualified `n2.a`, qualified wildcard expansion, and pruning of a
coalesced child; those cases now pass.

## Ready validation profile

```text
cargo +nightly-2026-08-22 fmt --manifest-path rust/Cargo.toml --all -- --check
git diff --check
cargo +nightly-2026-08-22 check --offline --locked --manifest-path rust/Cargo.toml \
  -p tidb-executor --all-targets
GOPATH=/tmp/tidb-codex-gopath TMPDIR=/tmp/tidb-codex-tmp make lint
```

Formatting, whitespace, and the executor all-target check pass; the existing
workspace warnings remain non-fatal. `make lint` is run again immediately
before the batch commit and passes (including the Go dashboard linter).

## Follow-up: aggregate fields keep their written label (2026-09-09)

Go `buildProjectionField` (`pkg/planner/core/logical_plan_builder.go:1573`)
takes the child-schema origin name only when the field's AST node is a
`*ast.ColumnNameExpr` (`innerNode.(*ast.ColumnNameExpr) && isCol`). The Rust
`ProjectionField` lost that distinction: `extract_agg_funcs_in_select_fields`
(`aggregation.rs:237`) rewrites a field's `expr` to a `#agg#N` marker, which
is an `Expr::Column`, so `projection_field_name` matched the origin-name
branch and returned the aggregation's empty output name. A derived table over
an aggregate therefore became `Column#1` instead of `count(*)`.

`ProjectionField` now records `column_reference` from the AST at construction
(looking through parentheses and a unary `+` with
`inner_from_parentheses_and_unary_plus`), and the naming branch tests it.

The executor's wildcard result naming had the same symptom from the other
side: `result_columns` (`driver/physical_builder.rs`) fell back to
`schema_column_name`, whose `orig_name` is lost when projection elimination
removes the naming projection. Go captures `names := p.OutputNames()` before
optimization (`pkg/planner/optimize.go:525`) and `physical_plan_for_logical`
already stores those names on the physical root; `result_columns` now reads
them before the schema fallback.

Regressions:

- `plan_builder::from_tests::test_derived_aggregate_takes_the_written_field_label`
  pins the derived output name `count(*)`.
- `driver::tests::table_round_trip::count_star_field_keeps_its_written_label`
  failed with `left: ["Column#1"], right: ["count(*)"]` before the executor
  change and passes after.

Ready validation: `tidb-planner` lib 990 passed / 0 failed and its four test
targets 268 / 6 / 3 passed; `tidb-executor` lib 1116 passed / 110 failed (no
new failures; the only diff against the pre-batch list is the fixed case and
the pre-existing flaky `access_cost::index_async_load_queue_tests` pair);
`cargo check --all-targets` for `tidb-planner`, `tidb-executor`, `tidb-exec`,
and `tidb-session`; `cargo fmt --all -- --check` (three pre-existing drift
files only); `git diff --check -- rust`.

## Follow-up: HAVING resolves against the SELECT LIST (2026-09-09)

Go `havingWindowAndOrderbyExprResolver.Leave` (`logical_plan_builder.go:2852`)
falls back to the source plan only for a QUALIFIED name
(`a.curClause == havingClause && v.Name.Table.L != ""`), and `resolveFromPlan`
then requires a SELECT FIELD holding that source column (`:2786`). An
unqualified name never falls back, and a qualified source column the select
list does not project is `ErrUnknownColumn` (1054).

The Rust resolver instead appended any source column as a hidden field, so
`SELECT a FROM ht HAVING b > 0` returned rows where TiDB raises 1054. It now
requires a qualified name that the source knows AND a non-hidden select field
whose column matches it; everything else is
`PlanErrorKind::UnknownColumnInClause`, a new typed planner error mapped to
`DriverError::UnknownColumnInClause` so the 1054 column/clause pair survives
the planner boundary.

Regression: `select_clauses::plain_having_filters_and_sees_only_the_select_list`
failed on `SELECT a FROM ht HAVING b > 0` returning rows and passes after,
covering every captured alias/qualifier/aggregate spelling. Ready validation:
`tidb-executor` lib serialized 1132 passed / 94 failed with that test as the
only removal and no additions; `tidb-planner` all four test targets green;
`cargo fmt --all -- --check` (three pre-existing drift files only);
`git diff --check -- rust`.

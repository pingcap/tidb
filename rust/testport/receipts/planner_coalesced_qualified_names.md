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

## Follow-up: a HAVING scalar subquery no longer leaks its value (2026-09-09)

A correlated scalar subquery in HAVING is lowered by `build_selection` into an
`Apply` whose inner column widens the plan schema without appending a select
field (`lower_scalar_subqueries`). Go's trailing `buildProjection`
(`logical_plan_builder.go:4620`) trims the plan back to the select list, and
the Rust trim was gated on `fields.len() != old_len` alone, so
`SELECT a, b FROM ht HAVING (SELECT y FROM hs WHERE hs.x = ht.b) > 0`
returned `1 | 10 | 5` instead of `1 | 10`. The gate now also compares the
plan's schema width to `old_len`.

Regression:
`select_clauses::a_having_scalar_subquery_does_not_leak_its_value_as_a_result_column`
fails with the extra `5` before the change and passes after. The larger
`an_empty_correlated_having_subquery_is_null_and_drops_its_row` now reaches
its uncorrelated arm (`SELECT a FROM ht HAVING (SELECT count(*) FROM hs) > 0`),
which still fails with `Unsupported("uncorrelated scalar-subquery evaluation is
not available to the planner")`; that is the separate uncorrelated-subquery
boundary, not a regression from this change.

Ready validation from `rust/`:

```text
cargo test -p tidb-executor --lib a_having_scalar_subquery_does_not_leak_its_value_as_a_result_column
# passed: 1
cargo test -p tidb-executor --lib driver::tests::select_clauses
# 11 passed, 1 pre-existing uncorrelated-subquery failure
cargo test -p tidb-executor --lib -- --test-threads=1
# 1,139 passed / 89 failed (baseline 1,138 / 89 plus the new regression)
cargo fmt -p tidb-planner -p tidb-executor -- --check
# three pre-existing drift files only
git diff --check -- rust
# passed
```

## Follow-up: an uncorrelated scalar subquery is lowered, not refused (2026-09-09)

Go's `handleScalarSubquery` (`expression_rewriter.go:1522`) pre-evaluates an
UNCORRELATED scalar subquery: `DoOptimize` builds its physical plan and wraps
it in a `ScalarSubQueryExpr` that runs once per statement. That evaluator is
not ported, so Rust returned `ScalarSubqueryOutcome::EvaluateSeparately` and
`lower_scalar_subqueries` turned it into
`Unsupported("uncorrelated scalar-subquery evaluation is not available to the
planner")` -- every statement with a non-correlated scalar subquery failed.

`handle_scalar_subquery` now lowers every scalar subquery into the
MaxOneRow-guarded left-outer Apply (the same shape the correlated case already
used), and materializes the FROM-less dual's EMPTY schema before building the
Apply so `SELECT (SELECT 1)` keeps a schema. The answer is Go's; the cost is
that the subquery is re-executed once per outer row instead of once per
statement. That performance divergence is bounded and recorded here as the
unported `ScalarSubQueryExpr` boundary.

Regressions (all failed before and pass after):
`driver::field_name_tests::a_folded_literal_is_not_a_written_literal`,
`driver::tests::primary_keys::issue_50051_unsigned_boundaries_range_correctly`,
and `driver::tests::aggregates::explain_distinct_scalar_subquery_with_filter`.
The HAVING test's alias correlation (`hs.x = bb`) now answers `10` like Go and
its assertion was updated; the test still fails on the separate
`ht.b`-in-a-subquery clause attribution (`where clause` vs Go's `having
clause`), which is not a regression.

Ready validation from `rust/`:

```text
cargo test -p tidb-executor --lib -- --test-threads=1
# 1,147 passed / 81 failed (baseline 1,144 / 84; three fixed, no new)
cargo fmt -p tidb-planner -p tidb-executor -- --check
# three pre-existing drift files only
git diff --check -- rust
# passed
```

## Follow-up: the SELECT list lowers every subquery form (2026-09-09)

Go's `expressionRewriter` lowers a direct subquery, a quantified comparison,
an IN, and an EXISTS into an Apply or semi-join wherever they appear. The Rust
projection path (`PlanBuilder::lower_scalar_subqueries`) matched only
`Expr::Subquery`, so `select (c) > all (select c from t) from t` reached
`rewrite_expr_resolved` and failed with "expression form is not yet supported
by the rewriter". The lowerer now handles all four forms, using the same
`handle_*_subquery` handlers the filter path already used.

Regression:
`tests_executor_suite_statements_source::a_select_field_quantified_subquery_is_lowered`
plans and evaluates the `> ALL` form; it failed with the unsupported-rewriter
error before and passes after.

Boundary: a SELECT-field `IN`/`EXISTS` now plans but trips the column-pruning
"unexpected zero-column output schema" panic, so the regression covers the
quantified-comparison form only. `column_name_resolution` still needs window
physical planning for its last assertion.

Ready validation from `rust/`:

```text
cargo test -p tidb-executor --lib a_select_field_quantified_subquery_is_lowered
# passed: 1
cargo test -p tidb-executor --lib -- --test-threads=1
# 1,151 passed / 81 failed (baseline 1,150 / 81 plus the new regression)
cargo check -p tidb-planner
# passed
```

## Follow-up: a left-outer-semi join keeps its marker through pruning (2026-09-09)

Go's `LogicalJoin.PruneColumns` (`logical_join.go:339`) calls `MergeSchema`
(`BuildLogicalJoinSchema`) and then re-appends the left-outer-semi join's
marker column to `parentUsedCols` before `InlineProjection`. The Rust
`PendingColumns::MergeSchema` handler concatenated the CHILDREN's schemas, so
the marker column -- which lives only on the join's own schema -- was dropped
and the join ended with an empty schema (the `noUnexpectedZeroColumnSchema`
panic). A SELECT-field `IN`/`EXISTS` is exactly that shape.

`MergeSchema` now rebuilds the schema the way `BuildLogicalJoinSchema` does
(a semi join outputs the left child's schema; a left-outer-semi join appends
its own last column) and re-appends the marker before inlining.

This also repairs six pre-existing failures, all of which build a
left-outer-semi apply: `aggregates::global_count_over_index_ranges_uses_gos_stream_agg_and_index_reader`,
`subqueries::{correlated_exists_under_or_is_explainable,
nested_in_subquery_under_or_is_explainable,
tpcds_q10_correlated_exists_under_or_is_explainable}`, and
`tests_parallel_apply_sql_source::{ordered_parallel_apply_edge_cases_source,
ordered_parallel_apply_left_outer_semi_source}`. The regression
`a_select_field_quantified_subquery_is_lowered` now covers IN and EXISTS as
well as the quantified comparison.

Ready validation from `rust/`:

```text
cargo test -p tidb-executor --lib a_select_field_quantified_subquery_is_lowered
# passed: 1
cargo test -p tidb-executor --lib -- --test-threads=1
# 1,157 passed / 76 failed (baseline 1,150 / 81): six repaired, one new
# regression, no additions
cargo check -p tidb-planner
# passed
```

## Follow-up: a HAVING name that matches a GROUP BY item resolves through the plan (2026-09-09)

The previous batch narrowed unqualified HAVING names to the SELECT LIST, which
is right for `SELECT a FROM ht HAVING b > 0` (Go's `resolveFieldsFirst` is true
there) but wrong for a GROUP BY column that is not selected. Go's
`havingWindowAndOrderbyExprResolver.Leave`
(`pkg/planner/core/logical_plan_builder.go:2882`) first clears
`resolveFieldsFirst` when the name matches a `GroupBy.Items` entry, then calls
`resolveFromPlan`, whose final step appends an AUXILIARY select field for the
source column (`:2810-2820`). That is how
`select count(*) from t group by a having a > 1` keeps working: the aggregation
carries `a` as a hidden `firstrow()` column.

`resolve_having_and_order_by` now takes the resolved GROUP BY expressions,
matches a HAVING column against them with Go's `ColumnName.Match` semantics,
and, on a match, appends (or reuses) a hidden auxiliary projection field named
by the source `FieldName` and substitutes a `MarkerKind::Column` marker for it.
An unqualified name that matches no GROUP BY item still resolves select-list
first and is still 1054 when the select list does not hold it.

Regression: the new
`plan_builder::aggregation_tests::test_a_having_group_by_column_resolves_through_the_source_plan`
fails before the change with `UnknownColumnInClause { column: "a", clause:
"having clause" }` and passes after. `tidb-planner --lib` is 994 passed / 0
failed; `tidb-executor --lib` serialized is 1171 passed / 69 failed, the same
set as the previous batch (no additions). The executor test
`driver::tests::aggregates::aggregate_having_and_order_by` now clears the
resolution error and fails only on the row ORDER of
`SELECT COUNT(*) FROM g GROUP BY a HAVING a > 1`: the Rust parallel HashAgg
emits final workers in `murmur3(groupKey) % finalConcurrency` bucket order,
while Go's `HashAggFinalWorker.generateResultAndSend` iterates
`partialResultMap.M` (a Go map, so its order is randomized). The test
over-specifies that unordered output; the divergence is recorded here rather
than hidden behind a reordered assertion.

## Follow-up: a computed projection column has no `OrigName` (2026-09-09)

Go `buildProjectionField` (`logical_plan_builder.go:1573`) returns a column
reference UNCHANGED — keeping its own `OrigName` — and builds a computed
field's fresh `Column` with `UniqueID`/`RetType`/`CorrelatedColUniqueID` only,
NO `OrigName`. The alias lives on the `FieldName`. This port wrote
`output.orig_name = name.display_name()` for every projection output, so a
computed column rendered its alias in EXPLAIN (`plus(...)->revenue`,
`revenue:desc`) where Go renders `Column#N`.

`build_projection` now leaves the output column's `OrigName` as the source
column's (direct reference) or empty (computed), matching Go.

Regression: the new
`driver::tests::aggregates::a_computed_projection_column_explains_as_column_not_its_alias`
fails before (`->revenue`) and passes after (`->Column#N`). Ready validation:
`tidb-executor` lib serialized 1204 passed / 48 failed with no additions and
no `statistics_request_tests` failures (an earlier full-suite run showed 13 of
them, which are the documented full-run flake — they pass 16/16 in their own
group); `cargo check --locked --all-targets` clean;
`rustfmt --edition 2021 --check` clean on both changed files;
`git diff --check -- rust`.

Remaining blocker for the tests that assert the exact text
(`grouped_rows_follow_the_reordered_join_tree`, `tpch_q13`,
`tpcc_condition_{four,nine}`): the rendered `Column#N` still carries this
port's allocation id (`Column#12` where Go records `Column#1`), a separate
plan-column-id ordering divergence.

## Follow-up: HAVING subquery clause attribution and the typed unknown-column lift (2026-09-09)

The gap recorded above — `ht.b`-in-a-subquery attributed to `where clause`
instead of Go's `having clause` — is closed.

Go `buildSelect` calls `resolveGbyExprs` only under `if sel.GroupBy != nil`
(`logical_plan_builder.go:4361`), and that call sets
`b.curClause = groupByClause` (`:4067`). The Rust called `resolve_gby_exprs`
unconditionally, so a query with NO GROUP BY still stamped the builder's
`cur_clause` as `GroupBy`. A subquery built later while resolving HAVING then
saw `GroupBy`; its own `build_selection` downgraded that to `Where` because
`cur_clause != Having`. `build_select_body` now skips the call when
`select.group_by` is empty, matching Go's guard.

The clause also has to survive the executor boundary as the typed variant.
`From<EvalError> for PlanError` wrapped every rewriter error in
`PlanErrorKind::Eval`, so the executor lifted an unknown column as
`Exec(Eval(UnknownColumnInClause(..)))` even though the clause name was
correct. The conversion now maps `EvalError::UnknownColumnInClause` to
`PlanError::unknown_column_in_clause`, the same typed error the plan-time
resolution path produces.

Regressions: the new
`plan_builder::tests::a_having_subquery_names_the_having_clause_for_an_unresolved_outer_column`
builds `SELECT a FROM t HAVING (SELECT y FROM hs WHERE hs.x = t.b) > 0` and
asserts `UnknownColumnInClause { column: "t.b", clause: "having clause" }`
(before: `where clause`); the new
`plan_builder::tests::an_unknown_column_from_the_rewriter_keeps_its_clause`
pins the typed `From` conversion. The executor tests
`driver::tests::select_clauses::an_empty_correlated_having_subquery_is_null_and_drops_its_row`
and `driver::tests::subqueries::a_having_subquery_may_only_correlate_to_the_aggregations_output`
now pass. Ready validation: `tidb-planner` lib 997 passed / 0 failed;
`tidb-executor` lib serialized 1215 passed / 37 failed, exactly the two above
removed from the baseline and no additions; `cargo check --locked --all-targets
-p tidb-planner -p tidb-executor` clean; `rustfmt --edition 2021 --check`
clean on both changed files; `git diff --check -- rust` clean.

## Follow-up: a rewritten subquery's new columns lose their names (2026-09-09)

Go `rewriteExprNode` (`expression_rewriter.go:283-299`) defers a reset of the
plan's `OutputNames`: after the expression is rewritten, every column past the
PRE-rewrite schema length is renamed to `types.EmptyName`. Its own comment
gives the reason — `select * from t where t.a in (select t1.a from t1)` leaves
`t1.*` in the plan, and a second subquery naming `t1` would resolve against
the stale names. The Rust kept the appended columns named, so
`SELECT a FROM s WHERE a IN (SELECT a FROM u)` made the IN-to-join rewrite's
inner `u.a` collide with the outer `s.a` and the outer projection's `a` failed
as `UnknownColumnInClause`.

`plan_builder::hide_rewrite_columns(plan, original_len)` now mirrors the Go
defer, and `build_selection` / `build_projection_with_order_by` call it after
each plan-growing expression rewrite (`lower_filter_subquery`,
`lower_scalar_subqueries`).

Regression: the new
`plan_builder::tests::an_in_subquery_hides_the_join_s_inner_column_name`
builds `SELECT a FROM t WHERE a IN (SELECT a FROM hs)` against a catalog whose
`hs.a` shares `t.a`'s name; before the reset the build failed with
`UnknownColumnInClause`, and now the outer projection resolves `t.a`. Ready
validation: `tidb-planner` lib 998 passed / 0 failed; `tidb-executor` lib
serialized 1202 passed / 50 failed, where the 13
`statistics_request_tests` failures are the documented full-run flake (they
pass 16/16 in their own group) and the remaining 37 are the pre-change
baseline; `cargo check --locked --all-targets -p tidb-planner` clean;
`rustfmt --edition 2021 --check` clean on both changed files;
`git diff --check -- rust` clean. The `driver::tests::subqueries::subqueries`
test now clears every IN / NOT IN assertion and stops at the uncorrelated
`EXISTS` arm, which needs Go's separate-subquery evaluation (the recorded
`ScalarSubQueryExpr` boundary).

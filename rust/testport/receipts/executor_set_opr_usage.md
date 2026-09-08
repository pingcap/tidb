# Set-operation usage validation over every child query

Go source: `origin/master` at
`f6c6d1adba7edae5786a4fffbacb31b1ec9c42c6` (2026-09-08). This batch ports the
last reachability gap in `preprocessor.checkSetOprSelectList`
(`pkg/planner/core/preprocess.go:858`) and aligns two `TestUnion2` arms whose
rows Go itself compares as a set.

## Go behavior (the oracle)

`checkSetOprSelectList` walks a `*ast.SetOprSelectList`: for every term except
the last, an `INTO OUTFILE` is `ErrWrongUsage("UNION", "INTO")`, and a term that
is NOT parenthesized and carries its own `LIMIT` or `ORDER BY` is
`ErrWrongUsage("UNION", "LIMIT")` / `("UNION", "ORDER BY")` (both errno 1221).
The preprocessor runs the check through a full `ast.Visitor`, so it fires for a
set operation nested anywhere: a CTE body, a derived table in `FROM`, a scalar
subquery, an `IN`/`EXISTS` subquery, or a parenthesized nested set operation.

`TestUnion2` (`pkg/executor/test/executor/executor_test.go:1106`) compares two
of its arms with `r.Sort().Check(...)` — `SELECT 1 AS c UNION select a FROM t`
and `SELECT 'a' UNION SELECT CONCAT('a', -4)`. A `UNION` is deduplicated by an
aggregation whose group order is unspecified (Go's parallel HashAgg iterates a
Go map), so Go sorts both sides before comparing.

## The Rust implementation

`driver::set_opr::validate_query_usage` recursed only into the outer
`SetOprStmt` and its own `WITH` CTEs. It now runs a `tidb_ast::Visitor` over the
whole cloned `QueryStmt` and calls `validate_set_opr_usage` on every
`SetOprStmt` node, which is Go's full-visitor reachability.

`tests_executor_suite_statements_source::assert_rows_sorted` is the Rust
`r.Sort()`: both sides are sorted before comparing. The two `TestUnion2` arms
above now use it, so the test no longer pins an order the engine never
promised.

## Regressions

- `tests_executor_suite_statements_source::union2_matrix` fails before on both
  counts: `SELECT 1 AS c UNION select a FROM tdec` returned `12.34, 1.00` where
  the sorted expectation is `1.00, 12.34`, and
  `select 1 from (select a from t0 limit 1 union all select a from t0 limit 1) tmp`
  returned rows instead of 1221. Both pass after.

## Validation

Profile: **Ready** for this package batch.

```text
cargo test -p tidb-executor --lib tests_executor_suite_statements_source::union2_matrix
# ok

cargo test -p tidb-executor --lib -- --test-threads=1
# 1217 passed / 35 failed; union2_matrix removed from the baseline, no additions
# (the 13 statistics_request_tests failures are the documented full-run flake)

cargo check --locked --all-targets -p tidb-executor
rustfmt --edition 2021 --check crates/tidb-executor/src/driver/set_opr.rs \
  crates/tidb-executor/src/tests_executor_suite_statements_source.rs
git diff --check -- rust
```

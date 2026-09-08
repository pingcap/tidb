# Point-get admission: prefix indexes and unknown-column clauses

Status: Ready for this focused point-get batch. Two independent Go-master
behaviors were missing; the claim unit is each behavior's Go site, not the
complete `pkg/planner/core` or `pkg/expression` package.

## 1. A unique prefix index must not become a point get

Go source: `pkg/planner/core/find_best_task.go:2204`:
`canConvertPointGet = path.Index.Unique && !path.Index.HasPrefixIndex()` — the
comment above it says "We simply do not build [batch] point get for prefix
indexes."

Rust's `find_best_task/dispatch.rs` checked `source_index.unique` and that the
range covered every index column, but not the declared prefix lengths. A
`UNIQUE KEY uidx (a(3))` therefore became an index `Point_Get`; the executor
builder then refused it with `Unsupported("a retained index point-get requires
a non-prefix unique index")`, so
`SELECT a FROM u WHERE a = 'abcxyz'` errored instead of planning the
IndexLookUp with a residual Selection that Go plans (and answering nothing).

Fix: `declared_index_has_prefix` (any `SourceIndexColumn.length` that is not
`UNSPECIFIED_LENGTH`) now guards the conversion, matching
`!path.Index.HasPrefixIndex()`.

Regression: `driver::tests::index_prefix_reads::
a_unique_prefix_index_does_not_answer_a_point_get` failed with the
`Unsupported` error before the fix and passes after; the other twelve
`index_prefix_reads` tests still pass. The full executor lib suite dropped
from 115 to 113 failures with no new failure.

## 2. Unknown-column errors name their clause

Go source: `pkg/planner/core/planbuilder.go:132` `clauseMsg`, used by
`expression_rewriter.go`'s `ErrUnknownColumn.GenWithStackByArgs(name, clause)`.
The clause text is `field list`, `where clause`, `group statement`,
`order clause`, `having clause`, and so on; the fallback is `expression`.

Rust's plan-aware resolver chain already had `ClauseCode::message()` and set
`cur_clause` at the field-list, where, having, group, and order sites, but
`tidb-expr`'s rewriter always produced `EvalError::UnknownColumn(name)`, which
renders `in 'expression'`. The clause never reached the error.

Fix:
- `EvalError::UnknownColumnInClause(name, clause)` renders the clause.
- `ColumnResolver::clause_message()` defaults to `"expression"`;
  `PlanScopeResolver` overrides it from `PlanBuilder::cur_clause`, and
  `FoldModeResolver` forwards it (the sub-expression decorator previously
  swallowed it, which is what made a WHERE name report `expression`).
- `EvalError::UnknownColumnInClause` maps to 1054
  `Unknown column 'x' in '<clause>'`.

Regression: `driver::tests::select_clauses::an_unknown_column_names_its_clause`
covers `field list`, `where clause`, and `group statement`; it failed with
`in 'expression'` before the fix and passes after. The full planner suite
(989 lib tests) still passes.

One Go behavior remains: a name that is only in `ORDER BY` is resolved by
Go's dedicated `orderByResolver` pass with `curClause = orderByClause`; Rust
appends it as a hidden projection field and reports `field list`. That is
recorded in the physical-plan ExecPlan as remaining work.

## Validation

Profile: **Ready** for this focused batch.

- `cargo test --offline --locked -j12 -p tidb-executor --lib index_prefix_reads`
  — 13 passed.
- `cargo test --offline --locked -j12 -p tidb-executor --lib
  an_unknown_column_names_its_clause` — passed.
- `cargo test --offline --locked -j12 -p tidb-executor --lib` — 1113 passed,
  113 failed, against 1111/115 before the batch; the two fixed tests are the
  delta and no new failing test appeared (the `access_cost` module's two
  order-dependent tests flip between runs and pass in isolation).
- `cargo test --offline --locked -j12 -p tidb-planner --tests` — 989 + 268 +
  6 + 3 passed.
- `cargo fmt` changed files clean; `git diff --check` clean.

No Go, Bazel, Cargo manifest, generated, or fixture file changed.

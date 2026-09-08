# CASE and EXTRACT keep Go's expression names, and Projection text uses Go's renderer

Go source: `origin/master` at
`f6c6d1adba7edae5786a4fffbacb31b1ec9c42c6` (2026-09-08).

## CASE is named `case`, not `case_when`

Go keys the control builtin by `ast.Case`, whose string is `"case"`
(`pkg/parser/ast/functions.go:145`); `caseWhenFunctionClass`
(`pkg/expression/builtin_control.go:316`) and the `funcs` map
(`pkg/expression/builtin.go`) both use that name. The Rust transcreation had
renamed it `case_when`, which broke three things:

- `new_function_impl` looked the name up in the shared `FUNCTIONS` table,
  found no `case_when` entry, and returned `NoDatabaseSelected`. Every rebuild
  of a CASE node therefore failed, so `AggregationPushDownSolver`'s
  projection-crossing arm (`rule_aggregation_push_down.go:555`) refused the
  substitution and left the derived projection below the aggregate.
- `expr_util/fold.rs:195`'s `"case"` arm (a port of Go's `caseWhen_handler`)
  was dead: the node name never matched.
- EXPLAIN printed `case_when(...)` where Go prints `case(...)`.

All `"case_when"` literals in `tidb-expr` are now `"case"`, so the registry
lookup, the fold handler, and the rendered name are Go's.

## EXTRACT is the two-argument `extract` call

Go's parser keeps `EXTRACT(unit FROM value)` as
`FuncCallExpr{FnName: ast.Extract, Args: [TimeUnitExpr, value]}`
(`pkg/parser/expr_cast_parser.go:483`); the rewriter turns the unit into a
VARCHAR constant (`pkg/planner/core/expression_rewriter.go:1838`) and
`extractFunctionClass.getFunction` (`pkg/expression/builtin_time.go:2761`)
types the value per unit. Rust had rewritten it to `unit(value)` — a
Rust-only shape that changed both the expression tree and the plan text
(`year(o_orderdate)` instead of Go's `extract(YEAR, o_orderdate)`).

`rewrite_leaf_call` now builds `extract(<unit constant>, value)` and
`ScalarFunction::eval_by_signature` dispatches the `EXTRACT` name to
`time_fn::dispatch` with the unit read from the first argument, so the same
shared unit implementations (including `calendar::extract_composite` for
`DAY_SECOND`/`YEAR_MONTH`/...) still run. The AST tier's own `Expr::Extract`
arm already evaluated that way.

## Projection expressions render through `StringWithCtx`

Go has two renderers. Conditions (Selection, Join, aggregate arguments,
group-by, by-items) go through `Expression.ExplainInfo`, whose
`Constant.format` (`pkg/expression/explain.go:176`) QUOTES a string constant.
Projection and Expand expressions go through `ExplainExpressionList`
(`explain.go:188`), whose default arm calls `Expression.StringWithCtx`, and
`Constant.StringWithCtx` (`constant.go:181`) prints `TruncatedStringify`
BARE — which is why Go's recorded q14 projection reads
`case(like(test.part.p_type, PROMO%, 92), ..., 0.0000)`.

`plan_trace::physical_expression_text_with_columns` gained an
`ExpressionTextStyle` selector, and `explain::projection_text` uses the
`StringWithCtx` style. Every other operator keeps the `Explain` style, so
Selection text still quotes (`like(..., "%pending%deposits%", 92)`).

## Regressions

- `driver::tests::aggregates::aggregate_push_down_substitutes_child_projection_before_injection`
  failed before with three projections below HashAgg and an injected
  projection reading the derived projection's `Column#12`; it now sees Go's
  layout (`case(eq(test.lineitem.nation, "INDIA"), mul(...), 0)`,
  `mul(...)`, `extract`-free `year(...)`-shaped carriers replaced by the
  substituted base columns) and passes.
- With the rename alone the test still failed on
  `extract(YEAR, test.orders.o_orderdate)`; the rewriter/evaluator change
  makes the plan carry Go's call shape and it passes.
- `cargo test -p tidb-expr --lib` before and after this batch has the same two
  failures, both unrelated and pre-existing:
  `builtin_ext::json::tests::json_schema_valid_resolves_file_and_http_references`
  (needs a live HTTP registry) and
  `simple_expr::tests::build_expression_without_enough_columns` (fails
  identically at the branch tip with the batch reverted).

## Validation

Profile: **Ready** for this package batch.

```text
cargo test -p tidb-executor --lib driver::tests::aggregates::aggregate_push_down_substitutes_child_projection_before_injection
# ok

cargo test -p tidb-executor --lib -- --test-threads=1
# 1224 passed / 28 failed; this test removed from the baseline, no additions

cargo test -p tidb-expr --lib -- --test-threads=1
# 1205 passed / 2 failed (both pre-existing, see above) / 99 ignored

cargo check --locked --all-targets -p tidb-expr -p tidb-planner -p tidb-executor
rustfmt --edition 2021 --check <changed files>
git diff --check -- rust
```

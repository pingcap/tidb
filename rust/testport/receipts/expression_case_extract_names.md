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

## Follow-up: CASE THEN/ELSE branches are cast to the merged control type

`caseWhenFunctionClass.getFunction` (`builtin_control.go:317`) passes the
inferred control type as every result argument's expected type to
`newBaseBuiltinFuncWithFieldTypes`, which wraps each THEN/ELSE argument in
`WrapWithCastAs*` / `BuildCastFunction` (`builtin.go:264-283`). The branch
cast is observable twice: the CASE evaluates to the merged type (a
`DECIMAL` branch beside an integer `ELSE 0` returns a DECIMAL datum), and a
constant branch renders as the cast's folded type in EXPLAIN
(`case(..., 0.0000)`).

Rust computed the merged type with `builtin_return_type("case", ...)` but
never wrapped the result arguments, so the same expression returned
`INT:0` from the integer ELSE and the q14 projection printed `0`. The
rewriter now wraps every THEN index and the trailing ELSE through a
`wrap_case_branch` helper that mirrors Go's per-family cast: `ETInt` uses
`WrapWithCastAsInt` with the merged type, `ETReal`/`ETString`/`ETJson` use
their `WrapWithCastAs*`, and decimal/datetime/timestamp/duration targets use
`BuildCastFunction` with the FULL merged type (`flen`/`decimal`), which is
what makes the constant carry `0.0000`.

The regression extends
`tests::control::case_when_source_vectors_preserve_lazy_truthiness` with
`chunk_e("case when false then 1.5 else 0 end") == "DEC:0.0"`; it returned
`INT:0` before the wrap.

```text
cargo test -p tidb-expr
# 1206 passed; 2 failed; 99 ignored -- the same two pre-existing failures
# recorded above (live-HTTP JSON schema and build_expression_without_enough_columns)

cargo test -p tidb-executor --lib -- --test-threads=1
# 1242 passed; 14 failed; the same pre-existing set

cargo test -p tidb-planner
# 1279 passed; 0 failed
```

## Follow-up: a constant CASE branch folds through the live context

Go's `BuildCastFunctionWithCheck` (`builtin_cast.go:2655`) folds the cast it
just built for every target except JSON, so the wrapped `ELSE 0` becomes the
constant `0.0000` before the CASE node exists. Rust's `build_cast_function`
deliberately leaves conversion casts unfolded so their diagnostics stay with
the live statement context, and the planner's own fold skips lazy
short-circuit parents (`and`/`case`), so the branch kept its `cast(0,
decimal(31,4) BINARY)` shape.

The CASE arm now folds each wrapped branch with the resolver's live
`comparison_context()` in `Normal` mode -- the context the deferred fold would
have used -- reproducing Go's build-time conversion and its warning ownership.
A non-constant branch is untouched, and the CASE node itself still folds
through the existing lazy path. `tpch_q14_matches_recorded_hash_join_plan`'s
projection now reads `case(like(test.part.p_type, PROMO%, 92), mul(...),
0.0000)`; the test still fails only on its remaining recorded-shape gaps
(extra identity projection, `DATE_ADD` literal fold, equal-condition operand
order).

```text
cargo test -p tidb-expr
# 1206 passed; 2 failed; 99 ignored -- the same two pre-existing failures

cargo test -p tidb-executor --lib -- --test-threads=1
# 1242 passed; 14 failed; the same pre-existing set

cargo test -p tidb-planner
# 1279 passed; 0 failed
```

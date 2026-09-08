# `pkg/expression`: `NewFunction` rebuilds a cast through the cast builder

Status: Ready for this focused fix.

## Go behavior (the oracle)

`NewFunction` (`pkg/expression/scalar_function.go:203`) dispatches a cast:

```go
case ast.Cast:
    return BuildCastFunction(ctx, args[0], retType), nil
```

`ColumnSubstituteImpl`/`ColumnSubstituteAll` rebuild a scalar function with
`NewFunction(ctx, sf.FuncName.L, sf.RetType, newArgs...)`, so a predicate that
contains a cast can only be substituted (and pushed) when that rebuild works.
`breakDownPredicates` treats a rebuild failure as `hasFail`, and the predicate
stays above the projection.

## Rust divergence

This port represents casts as dedicated nodes named `cast_decimal`,
`cast_char`, `cast_signed`, ... (`rewriter/result_type.rs:221`) instead of the
single `ast.Cast`. `RealFunctionBuilder::new_function` sent every name to the
builtin registry, which refuses the dedicated cast names (only the generic
`cast` has an explicit refusal), so rebuilding any expression that contains a
cast failed with `NoDatabaseSelected`/`FunctionNotExists` and the substitution
reported `hasFail`.

Observed on `driver::tests::subqueries::correlated_sum_predicate_pulls_above_unique_outer_join`:
the decorrelated predicate
`gt(cast(ps_availqty, decimal(20,0)), mul(0.5, Column#14))` could not be
pushed through the wrapper projection because the `cast_decimal` rebuild
failed.

## Rust change

`RealFunctionBuilder::new_function` routes a `cast*` name with exactly one
argument to `build_cast(arg, ret_type, false)`, mirroring Go's `case ast.Cast`.
Every other name keeps the existing registry path.

## Regression

`expr_util::builder::tests::new_function_routes_a_dedicated_cast_name_to_the_cast_builder`
fails before and passes after.

## Validation

Profile: **Ready** for this package batch.

```text
cargo test -p tidb-expr --lib -- --test-threads=1
# 1,206 passed / 2 failed (the two pre-existing failures:
# json_schema_valid_resolves_file_and_http_references needs HTTP, and
# simple_expr::tests::build_expression_without_enough_columns)
cargo test -p tidb-planner --lib -- --test-threads=1
# 1,002 passed / 0 failed
cargo check --locked --all-targets -p tidb-expr -p tidb-planner -p tidb-executor
rustfmt --edition 2021 --config skip_children=true --check <changed files>
git diff --check -- rust
```

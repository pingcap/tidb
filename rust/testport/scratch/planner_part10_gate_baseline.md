# pkg/planner.part10 (b087) — tidb-planner gate baseline & run log

Baseline (clean tree, branch testport/pod3-b087 @ b77e9332f7), command:

    cargo nextest run --locked -p tidb-planner -E 'not test(/bench/)' --no-fail-fast

Summary line:
    Summary [   3.546s] 1028 tests run: 1028 passed, 461 skipped

Failure set: EMPTY (no `baseline-failing` entries; subset property trivially holds).

Final run after adding the part10 port files:
    Summary [   3.599s] 1035 tests run: 1035 passed, 517 skipped
(+7 running tests = the real ports; +56 newly skipped = documentary gap ports;
 benchmark_optimize_best_plan_shapes_over_seeded_t is excluded by the gate
 filterset exactly like a Go Benchmark is excluded by `go test`.)

Divergences measured this session (kept as #[ignore], production untouched):
1. BETWEEN mixed-type row: `'2001-04-10 12:34:56' between cast(... as datetime)
   and '01-05-01'` answers Int(0); Go answers 1 because wrapExpWithCast()
   casts all three operands to the common comparison type
   (pkg/planner/core/expression_rewriter.go:2746/:2795) before building GE/LE.
   Rust BETWEEN rewrite (rust/crates/tidb-expr/src/rewriter.rs Expr::Between)
   skips that wrapper.
2. build_cast_function nullability: Go's BuildCastFunctionWithCheck deletes
   NotNullFlag on the DeepCopy'd target when the source is nullable
   (pkg/expression/builtin_cast.go:2616-2619); rust/crates/tidb-expr
   src/simple_expr.rs build_cast_function keeps the flag, so the second build's
   ret type reports NOT_NULL where Go's sf2 assertions require it cleared.

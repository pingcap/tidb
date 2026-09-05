# Extremum decimal scale: the winner keeps the winning argument's own decimal

The rung documented in `builtin_ext/compare2.rs` ("separating a typed integer
argument from a folded integer constant") is now closed, with the full-stack
captures that decide it.

## Captures (real TiDB, this branch's Go tree)

Over `create table g (i int, d decimal(10,3))` holding `(-5, 2.500)`, driven
through the session/executor stack (temporary `pkg/session` probe, run and
removed):

| SQL | wire text | datum frac | result ft decimal |
|---|---|---:|---:|
| `select least(i, d) from g` | `-5` | 0 | 3 |
| `select greatest(i, d) from g` | `2.500` | 3 | 3 |
| `select least(1, d) from g` | `1` | 0 | 3 |
| `select least(i, 2.5) from g` | `-5` | 0 | 1 |
| `select least(1, 2.5)` | `1.0` | 1 | 1 |
| `select greatest(1, 2.5)` | `2.5` | 1 | 1 |
| `select least(1, 2.5) from g` | `1.0` | 1 | 1 |

## The Go rule

1. `newBaseBuiltinFuncWithTp` wraps every argument in
   `WrapWithCastAsDecimal`, which casts each argument at that argument's OWN
   decimal: `SetDecimal(0)` for integer arguments, pass-through for decimal
   arguments (`builtin_cast.go:2736-2744`).
2. The signature returns the winning `MyDecimal` RAW
   (`builtinLeastDecimalSig.evalDecimal`, `builtin_compare.go:1006`) —
   `ScalarFunction.EvalDecimal` applies no result-type coercion, so in
   non-folded evaluation the winner keeps its own argument scale: the integer
   column wins at frac 0 (`-5`), the decimal column at frac 3 (`2.500`).
3. `fixFlenAndDecimalForGreatestAndLeast` (`builtin_compare.go:569`) sets the
   RETURN type's decimal to the max over the argument decimals — and a
   fully-constant call is FOLDED at plan time, so the folded constant carries
   that max: `least(1, 2.5)` is `1.0`, `least(1, 2.5, 3.25)` is `1.00`.

The previous Rust behavior rescaled the winner to the max RUNTIME datum
scale in every case: right for all-literal calls (datum scales equal the
literal scales), wrong the moment a typed integer column wins — `-5.000`
where TiDB answers `-5`.

## Fix

- `crates/tidb-expr/src/scalar_function.rs` (the `greatest`/`least` arm):
  passes the per-argument decimals — integer arguments pinned to `0` like
  Go's `SetDecimal(0)`, others their field-type decimal — and whether every
  argument is a strict constant.
- `crates/tidb-expr/src/builtin_ext/compare2.rs`
  (`extremum_numeric`): the decimal branch rescales the winner to the
  WINNING ARGUMENT's own decimal; only when every argument is a strict
  constant does it rescale to the max argument decimal (Go's folded-return
  rule). The values-only fallback (no argument types at all) keeps the
  previous max-datum-scale behavior, which is the same answer for the
  all-literal vectors it was pinned with.

## Fail-before / pass-after

- New test `extremum_over_typed_columns_keeps_the_winner_own_scale`
  (`scalar_function.rs`): column-typed `least(i, d)` over row `(-5, 2.500)`
  must answer a frac-0 `-5`, and mixed `least(1, d)` a frac-0 `1`.
  FAILS before the change (`scale 3 != 0`, verified by temporarily
  restoring the old max-datum-scale rule) and passes after.
- `tidb-expr` full suite: 1208/1208 (excluding the documented network
  flake) — every all-literal capture pin (`least(1, 2.5)` = `1.0`,
  `least(1, 2.555)` = `1.000`, `greatest(2.5, 1.234)` = `2.500`, ...)
  unchanged.
- `tidb-executor` ddl/create/alter/partition filter: 361 passed / 10 failed
  vs the known pre-existing in-flight baseline (which had 11) — no
  regressions.

## Boundary

The captures were taken through Go's real session stack; the Rust port does
not yet model Go's resultset-chunk decimal re-encoding (the wire-level frac
comes from the datum's own scale in both tiers' observable text). The
result-field-type decimal (3 in the column case) is metadata Go carries but
whose padding never reaches the text result for these rows.

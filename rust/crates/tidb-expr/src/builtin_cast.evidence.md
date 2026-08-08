# `builtin_cast.go` lockdown evidence

This receipt owns exactly `pkg/expression/builtin_cast.go`. It is a file-level
seed receipt, not a claim that the whole Go `pkg/expression` package is
transcreated. The direct support surface is
`builtin_cast_test.go` plus `builtin_cast_bench_test.go`.

The source drift gate pins:

| file | bytes | lines | SHA-256 |
| --- | ---: | ---: | --- |
| `builtin_cast.go` | 104638 | 3004 | `12741863129c46a008a9064f94e11bf8be0a20f0b4efd83ef0a9e6b40b731ab5` |
| `builtin_cast_test.go` | 76362 | 1841 | `caa0b678d597d60452e2dda8c6a84f55cd21d6f2920f17661e85ae3d055ad6d7` |
| `builtin_cast_bench_test.go` | 2083 | 66 | `bfeea12e25ddfc8ae854f2670bb9e44d16e82cbe8167923723153c19a933b7ac` |

The unchanged repository Go AST tool is run against an isolated directory
containing only those three exact files. This avoids the package-wide duplicate
obligation ID outside this unit without changing either source or tool. It
finds 1,265 production and 1,922 direct support obligations, 3,187 total.
Every row has exactly one verdict in `builtin_cast.inventory.tsv`.

## Evidence codes

- `P_CAST_EVAL`, `P_GO_CAST_ROWS`: native Rust cast dispatch and boundary rows.
- `P_CAST_REWRITE`: AST rewrite plus the currently supported result metadata.
- `P_NATIVE_JSON`, `P_BINARY_JSON`: `Datum::Json`, including binary opaque input.
- `P_TEMPORAL_WRAP`: native internal `Datum::Time` argument wrapping.
- `P_VECTOR_SOURCE_BOUNDARY`: vector-to-string and explicit rejection of all
  other reachable vector-source targets.
- `U_SIGNATURE_OBJECT`: Go `Clone` and embedded signature-object fields have
  no runtime representation in Rust's enum/free-function dispatch. Structural
  proof: the Rust crate has no `baseBuiltinFunc`/`builtinFunc` trait object;
  compile anchors name the behavior seam instead.
- `U_VECTOR_CAST_TARGET`: `tidb_ast::CastType` has no VectorFloat32 variant, so
  constructing a vector CAST target is impossible in this AST. Vector source
  behavior is separately PORTED and is not hidden under this verdict.
- `D_UNION`: Go explicitly branches on `b.inUnion` (for example
  `if b.inUnion && ... && res < 0`); the Rust `CastType`/rewriter carries no
  union-build flag. The direct union test remains classified here rather than
  silently treated as an ordinary cast.
- `D_ARRAY`: Go's `castAsArrayFunctionClass.verifyArgs` says
  `CAST AS ARRAY can only be used in functional index`; the Rust AST has no
  ARRAY modifier or functional-index build context.
- `D_COLLATION`: Go's `BuildCastCollationFunction` mutates target charset and
  collation at build time. Rust does not expose that helper surface.
- `D_IMPLICIT_EVAL`: Go calls the DAYNAME path a "nasty way" to force implicit
  integer/real evaluation; no equivalent implicit-eval flag exists here.
- `D_CONTROL_PUSH`: Go rewrites hybrid control-function branches through its
  mutable expression tree; this unit has no equivalent mutable planner tree.
- `D_METADATA`, `D_SIGNATURE_MATRIX`: field-length/decimal/sig-object metadata
  rows that are not reproduced by the current Rust builder are explicit gaps.
- `D_VECTOR_ENGINE`, `D_GO_BENCHMARK`: Go chunk-vector execution and Go
  benchmark harnesses have no Rust execution surface; scalar semantics are
  inventoried separately.
- `D_DURATION_ERROR_CONTEXT`: `Datum::Duration` exists, but Go has
  source-specific NULL/warning/error handling (`NumberToDuration`,
  `ParseDuration`, JSON type-code branches). Exposing the common Rust
  conversion also fails the workspace holdout
  `cast_as_time_operand_is_still_unevaluated`, which requires moving and
  regenerating a differential corpus row outside this crate. The attempted
  valid-value seed was reverted and the target remains DECLINED.
- `D_GO_TEST_HOOK`: `newFakeSctx` exists solely to build the Go test context.
- `D_TEMPORAL_RESULT_DOMAIN`: native DATE/DATETIME conversion was implemented
  and mutation-pinned, but the clean workspace differential oracle records Go
  results as `STR` and rejects Rust `TIME`. Public casts therefore retain the
  established string boundary. This also makes Go's hidden stored clock for a
  duration-to-DATE value unrepresentable; the exposure was reverted rather
  than changing the differential owner outside this crate.

DECLINED means the ledger is complete, not that parity is complete. In
particular, the union, ARRAY, temporal-result-domain, metadata, and vector-engine
rows are visible falsifications of any whole-file parity claim. A lockdown with
no oracle movement is still successful; this unit moved reachable DateTime-FSP
parsing, JSON, and vector-source behavior, but does not convert the explicit
gaps into false PORTED claims.

## Validation and mutation receipts

`builtin_cast.mutations.tsv` records boundary mutations. Each KILLED result is
the failing assertion observed with that single semantic mutation applied and
the production source restored afterward. `builtin-cast-lockdown.py` is the
source/hash/AST/verdict/symbol gate; `builtin_cast_lockdown.rs` is the compiler
anchor for every PORTED ledger symbol.

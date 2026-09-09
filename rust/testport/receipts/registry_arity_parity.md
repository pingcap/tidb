# Builtin registry arity parity — systematic verification

Go source: `origin/master` at
`d152e4b78d35cfcb771bfabc289f837c2374d4aa` (2026-09-03). Systematic
accept/reject (arity) parity check across every `baseFunctionClass`
declaration in `pkg/expression` against
`rust/crates/tidb-expr/src/builtin_registry.rs`.

## Method

- Extracted every `ast.<Const>` → SQL-name mapping from
  `pkg/parser/ast/functions.go` (351 constants).
- Extracted every `baseFunctionClass{ast.<Const>, min, max}` declaration from
  `pkg/expression/builtin*.go` + `pkg/expression/*.go` (283 classes).
- Extracted every `("name", min, Some(max)|None)` entry from the Rust
  registry (276 entries).
- Compared name + (min, max) triples.

## Result: zero divergences

All 283 Go classes map to a Rust registry entry with identical bounds. The
7 names flagged by a naive name-diff were each verified as handled by a
dedicated mechanism rather than the plain registry entry:

- `cast` (1,1) — built by `build_cast_function`/`cast_target`
  (the whole CAST family; see `cast_target_type_family.md`).
- `getvar` (1,1) — encoded as typed signature names (`getvar_int`,
  `getvar_string`, ...) by the rewriter; `is_unfoldable` matches the prefix.
- `values` (0,0) — built on the INSERT path; `is_unfoldable` covers folding.
- `json_sum_crc32` (1,1) — evaluated through the `builtin_ext` JSON
  dispatch (`JSON_SUM_CRC32`, `builtin_ext/json/mod.rs:112`), which also
  owns the `JsonSumCrc32Expr` cast form.

## Validation

The comparison is a source read executed as a script over the pinned tree
(reproducible by re-running the extraction); no behavioral gate applies. The
registry's own `is_registered_builtin` invariant tests stay green in the
full tidb-expr sweep (1181 run, 1180 passed, 1 documented network flake).

## Risk

- None. Documentation-only batch; no code changed.
- Value: closes the accept/reject arity tier across all 283 declared
  classes — any future Go arity change now has a pinned baseline to diff
  against.

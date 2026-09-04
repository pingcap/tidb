# Hybrid-type cast push into control functions (`TryPushCastIntoControlFunctionForHybridType`)

Go source: `origin/master` at
`d152e4b78d35cfcb771bfabc289f837c2374d4aa` (2026-09-03). This ports Go
`builtin_cast.go:2898-2960`, the last member of the `BuildCastFunction*`
cluster that the CAST batches (see `cast_target_type_family.md`,
`cast_char_width_estimation.md`) were building toward.

## Go behavior (the oracle)

`BuildCastFunctionWithCheck` first calls
`TryPushCastIntoControlFunctionForHybridType` (builtin_cast.go:2898): when a
numeric-target (`ETInt`/`ETReal`) cast wraps an IF/CASE/ELT scalar function
and one of the VALUE branches has a HYBRID static type (`Enum`/`Set` — `Bit`
excluded per issue 24725), the cast pushes INTO the branches
(`WrapWithCastAsInt(ctx, expr, tp)` / `WrapWithCastAsReal`) and the control
function is REBUILT over the wrapped branches, adopting the rebuilt
signature's ret type. Without the push, `IF(1, e, 'a') + 0` over
`e enum('x','y','z')` flows the enum NAME through the string result and
answers 0; the pushed shape produces the enum's ORDINAL (2).

## The Rust implementation

`build_cast_function` gains the same push for `if`/`case_when`/`elt`:

- Branch positions per shape: `if` → args[1..2]; `case_when` → every result
  branch (odd positions plus the trailing ELSE when present); `elt` →
  args[1..] (the index argument is not a branch).
- The wrap is Go's `WrapWithCastAsInt`/`WrapWithCastAsReal` shape: an
  `ETInt`/`ETReal`-typed branch returns unchanged; otherwise LongLong
  (source flen, decimal 0) / Double (22, unspecified decimal), binary
  charset, `NotNullFlag` inherited from the source, `UnsignedFlag` from the
  TARGET (int) or the source (real). The enum `ENUM_SET_AS_INT` stamp is
  unnecessary in this shape: the built cast node evaluates the ordinal
  through `cast_arg_as_int`'s hybrid short-circuit
  (`builtin_cast.go:146-147` → `builtinCastIntAsIntSig` → `EvalInt` on the
  enum's integer).
- The rebuilt node adopts the re-inferred ret type
  (`infer_type4_control_funcs("if", ...)` /
  `builtin_return_type("case_when", ...)` / `("elt", ...)`); an inference
  failure keeps the unpushed node, which is Go's own `return expr` on error.
- Non-numeric targets and non-control expressions are untouched.

`rewriter::builtin_return_type` is re-exported `pub(crate)` for the rebuild.

## Regressions

- `simple_expr::tests::cast_over_if_pushes_into_hybrid_branches_like_go`
  — FAIL-BEFORE (pre-push the outer node WAS the raw `if`: the enum branch
  stayed an `Enum`-coded leaf). Pins the rebuilt shape (outer `cast_signed`
  over an `if` whose hybrid branch is a `LongLong` cast node) and the value
  (the enum ordinal `2` from a chunk row carrying the enum cell).

## Validation

Profile: **Ready** for this package batch.

```text
cargo +nightly-2026-08-22 fmt --all -- --check
# passed
git diff --check
# passed
RUST_MIN_STACK=67108864 cargo +nightly-2026-08-22 nextest run --offline --locked \
  -p tidb-expr --no-fail-fast
# 1181 run, 1180 passed, 1 failed — only the documented network flake
# (json_schema_valid_resolves_file_and_http_references)
RUST_MIN_STACK=67108864 cargo +nightly-2026-08-22 nextest run --offline --locked \
  -p tidb-expr -E 'test(cast) + test(if)'
# 149 run, 149 passed
cargo +nightly-2026-08-22 clippy --offline --locked -p tidb-expr --all-targets
# clean in touched code (empty-doc-line + unused-variable lints fixed)
```

## Risk

- Correctness: the push fires only for numeric targets over hybrid-typed
  control branches; the rebuilt ret type comes from the same inference Go's
  rebuild uses. Non-hybrid shapes are byte-identical before and after.
- Compatibility: no API change; `builtin_return_type` re-export is
  crate-internal.

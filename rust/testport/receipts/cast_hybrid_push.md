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

## Follow-up: a cast to `BIT` returns the byte carrier, not an integer (2026-09-09)

`mysql.TypeBit`'s eval type is `ETInt`, so `ScalarFunction::coerce_to_ret_type`
kept a cast-to-`BIT` result in the integer family. Go stores a BIT cell as
bytes: `chunk.AppendDatum` maps `KindMysqlBit` to `AppendBytes`, and
`getFixedLen(TypeBit)` is `VarElemLen`, so a BIT chunk column is var-length and
cannot accept `AppendInt64`. A `UNION ALL` of `bit(15)` and `bit(20)` therefore
panicked with `fixed append requires a fixed column` in
`tidb-chunk/src/column.rs`, because the union inserted the `bit(15) -> bit(20)`
cast (`FieldType.Equal` compares flen, `pkg/parser/types/field_type.go:371`)
and the projection wrote an integer into the union's var-length column.

`coerce_to_ret_type` now converts an `Int`/`UInt` result under a `Bit` result
type to `Datum::Bit(BinaryLiteral::from_uint(value, width))`, where `width` is
the target's `(flen + 7) / 8` bytes. This matches Go's recorded
`TestUnionIssue` output `"\x00\x00\x0F", "\x00\x00\xFF", "\x00\xFF\xFF"`; a
value already carrying the bytes is left unchanged.

Regression: the new
`tests::aggregation_arithmetic_cast_source::test_cast_signed_to_bit_returns_zero_padded_bytes`
fails before (returns `Int(15)`) and passes after (`Bit([0, 0, 15])`).
`tidb-executor --lib` serialized is 1173 passed / 68 failed, fixing
`tests_issuetest_b135_source::union_issue_data_type_and_null_arms` with no
additions. `tidb-expr --lib` is 1204 passed / 2 failed, both pre-existing and
unrelated (`build_expression_without_enough_columns` and the documented
network `json_schema_valid_resolves_file_and_http_references`).

## Follow-up: `build_cast` picks the dedicated signature; projection explains its output column (2026-09-09)

Go routes `cast` to `BuildCastFunctionWithCheck`, the dedicated builder that
picks a `cast_*` signature by target type; the bare `cast` name is one of the
four `NewFunction` explicitly refuses. This port's `FunctionBuilder::build_cast`
instead constructed a generic `ScalarFunction` named `cast`, which has no
executor arm, so the aggregation-elimination rule's DECIMAL argument widening
(`rule_aggregation_elimination.rs`) produced a node that failed at run time
with `this scalar function is not yet ported`. Both `build_cast`
implementations now route a `Some(target)` through `dedicated_cast`, the port
of Go's builder; `None` keeps the old generic construction.

`PhysicalProjection.ExplainInfo` uses `expression.ExplainExpressionList`
(`explain.go:188`), which appends `-><output column>` to every expression
except a direct column whose own text already equals its output column. The
Rust printed the bare expression list. `projection_text` now appends the
output column.

Regression: the new
`expr_util::builder::tests::build_cast_uses_the_dedicated_signature_name`
fails before (name `cast`) and passes after (`cast_decimal`). Ready
validation: `tidb-expr` lib 1205 passed / 2 failed, both pre-existing
(`build_expression_without_enough_columns` and the network
`json_schema_valid_resolves_file_and_http_references`);
`tidb-executor` lib serialized 1199 passed / 49 failed, no additions;
`cargo check --locked --all-targets` clean; `rustfmt --edition 2021 --check`
clean on both changed files; `git diff --check -- rust`. The TPCC condition-09
test now clears the unported-function error and fails only on the output
column's NAME (`s1` alias vs Go's `Column#2`), the recorded alias/OrigName
divergence.

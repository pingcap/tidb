# `CAST` target-type production family (parser.y `CastType` parity)

Go source: `origin/master` at
`d152e4b78d35cfcb771bfabc289f837c2374d4aa` (2026-09-03). This batch aligns
the Rust `CAST` target-type production — the family the
`expr-builtin-divergence-inventory.md` audit item 5 called "the flen/flag
each CAST produces (the whole builtin_cast.go getFunction family)" — with
Go's `parser.y` `CastType` rule, which is where the target `FieldType` is
born (`ast.FuncCastExpr.Tp`) before `BuildCastFunctionWithCheck` consumes it.

## Go behavior (the oracle, `parser.y` at master)

Each `CastType` alternative stamps the target type's code, defaults, charset,
and flags:

| Target | Code | flen/decimal | charset/collation | flags |
| --- | --- | --- | --- | --- |
| `BINARY OptFieldLen` | `TypeVarString`, **switching to `TypeString` when a length is given** | flen if given; omitted stays unspecified (`TypeVarString` is absent from `defaultLengthAndDecimalForCast`) | binary | `BinaryFlag` |
| `CHAR OptFieldLen OptBinary` | `TypeVarString` | flen if given | charset from `OptBinary`/session (boundary, below) | `BinaryFlag` only under `BINARY` |
| `DATE` | `TypeDate` | defaults `{10, 0}` | binary | `BinaryFlag` |
| `YEAR` | **`TypeYear`** | — | binary | `BinaryFlag` |
| `DATETIME OptFieldLen` | `TypeDatetime` | `19` → `+1+decimal` | binary | `BinaryFlag` |
| `DECIMAL FloatOpt` | `TypeNewDecimal` | as written | binary | `BinaryFlag` |
| `TIME OptFieldLen` | `TypeDuration` | `10` → `+1+decimal` | binary | `BinaryFlag` |
| `SIGNED OptInteger` | `TypeLonglong` | — | binary | `BinaryFlag` |
| `UNSIGNED OptInteger` | `TypeLonglong` | — | binary | `UnsignedFlag`, `BinaryFlag` |
| `JSON` | `TypeJSON` | — | **utf8mb4 / utf8mb4_bin** | `BinaryFlag`, `ParseToJSONFlag` |
| `DOUBLE` | `TypeDouble` | defaults `{22, -1}` | binary | `BinaryFlag` |
| `FLOAT FloatOpt` | `TypeFloat` (`p >= 25` resolves to `TypeDouble` at parse) | defaults `{12, -1}` / `{22, -1}` | binary | `BinaryFlag` |
| `REAL` | `TypeFloat`/`TypeDouble` by `REAL_AS_FLOAT` (parse-time sql_mode) | as `FLOAT`/`DOUBLE` | binary | `BinaryFlag` |
| `VECTOR ...` | `TypeTiDBVectorFloat32` | as written, decimal 0 | binary | **no `BinaryFlag`** — the one target that sets only `CharsetBin`/`CollationBin` |

Defaults table: `defaultLengthAndDecimalForCast`
(`pkg/parser/mysql/util.go:75-85`).

## The Rust fix (`rewriter::result_type::cast_target`)

- `Binary { len: Some(_) }` now produces `FieldTypeCode::String` (was always
  `VarString`); `len: None` stays `VarString`.
- `Year` now produces `FieldTypeCode::Year` (was `LongLong`; the eval type is
  still `ETInt`, and `build_cast_function`'s code dispatch already maps
  `Year → "cast_year"`, so evaluation is unchanged).
- `Float` now produces `FieldTypeCode::Float` with the `{12, -1}` defaults
  (was folded onto `Double`); `Double` gains the `{22, -1}` defaults. The
  parse-time `REAL`/`FLOAT(p)` folding (`real_as_float`, `p >= 25`) is
  unchanged.
- `Signed`, `Unsigned`/`UnsignedInUnion`, `Decimal`, and `Vector` gain the
  missing binary charset/collation; `Signed`/`Unsigned`/`Decimal`/`Double`/
  `Float`/`Json` gain the missing `BinaryFlag`.
- `Json` gains the utf8mb4/utf8mb4_bin charset/collation.
- `Vector` deliberately sets only the binary charset/collation — the unique
  target without `BinaryFlag` — matching the existing
  `builtin_cast_semantics` pin.

The documented `CHAR ... CHARSET` boundary stands: the charset name rides in
the AST for restore fidelity and is still not modeled into the computed
`FieldType` (the same scope-cut as `Expr::ConvertUsing`).

## Regressions

- `rewriter::result_type::tests::cast_target_types_follow_go_parser_y_cast_rules`
  — FAIL-BEFORE (first assertion already caught `BINARY(5)` as `VarString`
  instead of `TypeString`); pins every row of the table above except the
  `CHAR` charset boundary.

## Validation

Profile: **Ready** for this package batch.

```text
cargo +nightly-2026-08-22 fmt --all -- --check
# passed
git diff --check
# passed
RUST_MIN_STACK=67108864 cargo +nightly-2026-08-22 nextest run --offline --locked \
  -p tidb-expr --no-fail-fast
# 1151 run, 1150 passed, 1 failed — only the documented network flake
# (json_schema_valid_resolves_file_and_http_references); the earlier full run
# also flagged cast_result_types_keep_json_native_and_temporal_fsp, which
# correctly forced the Vector arm down to charset/collation-only.
cargo +nightly-2026-08-22 nextest run --offline --locked \
  -p tidb-exec -p tidb-planner -E 'test(cast) + test(sysvar) + test(variable) + test(restore)'
# 42 run, 42 passed (cast/union-cast pins, sysvar, restore surfaces)
cargo +nightly-2026-08-22 clippy --offline --locked -p tidb-expr --all-targets
# no diagnostics in touched code
```

## Risk

- Correctness: low; the changed fields are the result-type CODE, default
  flen/decimal, charset/collation names, and flags of `CAST` targets — the
  evaluation dispatch is name-driven and unchanged.
- Compatibility: consumers reading the ret-type code/charset (restore,
  protocol metadata) now agree with Go; the one pre-existing pin asserting
  the old no-binary-flag `Vector` shape was already Go-correct and stays
  green.

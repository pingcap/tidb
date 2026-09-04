# `CAST(... AS CHAR)` width production (`adjustRetFtForCastString` parity)

Go source: `origin/master` at
`d152e4b78d35cfcb771bfabc289f837c2374d4aa` (2026-09-03). This batch ports the
string-cast result-width table — the `CHAR` slice of the CAST flen/flag
production family (audit item 5) beyond the target-type codes landed in
`cast_target_type_family.md`.

## Go behavior (the oracle)

`castAsStringFunctionClass.getFunction` calls `adjustRetFtForCastString`
(`pkg/expression/builtin_cast.go`) on the ret type before signature
selection. For a variable-length (`TypeVarString`) target whose flen is
unspecified, the width of the produced value is estimated per argument
family:

- Int: per-type widths — Tiny 3/4, Short 5/6, Int24 8/9, Long 10/11 (unsigned
  first), Longlong always 20 (both range endpoints print 20 chars), Year 4,
  Bit the source flen. Issue 44786: sized by TYPE, not declared flen, so
  `CHAR(1)` cannot truncate `-1`.
- Real: 87 (Float) / 370 (Double) — TiDB formats in `f` notation, so the
  width covers the smallest denormal.
- Decimal: `decimalPrecisionToLength` — precision, +1 when scale > 0, +1 when
  signed (negative sign), minimum 1; unspecified inputs stay unspecified.
- Datetime/Timestamp: `MaxDateWidth` (10) for `TypeDate`, else
  `MaxDatetimeWidthNoFsp` (19), `+1+decimal` when fractional seconds exist.
- Duration: `MaxDurationWidthNoFsp` (10), `+1+decimal` likewise.
- Json: flen `MaxLongBlobWidth` (4294967295) AND the code widens to
  `TypeLongBlob`.
- String: fixed-width sources inherit flen > 0; TinyBlob 255, Blob
  `castBlobFlen` (65535×4), MediumBlob `castMediumBlobFlen` (16777215×4),
  LongBlob 4294967295.

A fixed `TypeString` target returns untouched (Go's early return), a `NULL`
argument returns untouched, and every arm requires
`originalFlen == UnspecifiedLength`.

## The Rust fix

`rewriter::result_type::adjust_ret_ft_for_cast_string` ports the table
verbatim; `build_cast_function` applies it when the target code is
`VarString` (the `CAST(... AS CHAR)` shape) — matching Go's early return for
fixed `TypeString`, so `BINARY(N)` targets are untouched.

The `CHAR ... CHARSET` coercibility/repertoire seam (`isExplicitCharset` →
`CoercibilityExplicit`, repertoire, and `HandleBinaryLiteral`'s
binary-source validation) remains the recorded boundary: it is
collation-resolution architecture, not a width table.

## Regressions

- `simple_expr::tests::cast_as_char_estimates_unspecified_widths_like_go`
  — FAIL-BEFORE (pre-fix the unspecified CHAR target kept flen -1; first
  assertion read `-1` vs `20`). Pins int (20), Int24-unsigned (8), Short (6),
  Year (4), Double (370), Float (87), Decimal(10,2) signed (12), Datetime fsp
  3 (23), Date (10), Duration fsp 2 (13), JSON (4294967295 + `LongBlob`
  code), VarString(7) (7), TinyBlob (255), Blob (262140).

## Validation

Profile: **Ready** for this package batch.

```text
cargo +nightly-2026-08-22 fmt --all -- --check
# passed
git diff --check
# passed
RUST_MIN_STACK=67108864 cargo +nightly-2026-08-22 nextest run --offline --locked \
  -p tidb-expr --no-fail-fast
# 1166 run, 1165 passed, 1 failed — only the documented network flake
# (json_schema_valid_resolves_file_and_http_references)
cargo +nightly-2026-08-22 clippy --offline --locked -p tidb-expr --all-targets
# no diagnostics in touched code
```

## Risk

- Correctness: low; the adjust only fills an unspecified width (and widens
  the JSON code), so every explicitly-sized target is byte-identical before
  and after.
- Compatibility: protocol metadata (ret flen) now agrees with Go for
  unsized `CAST AS CHAR` over every typed source; no API change.

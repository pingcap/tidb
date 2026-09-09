# Clock FSP above `MaxFsp`: the coded `types.ErrTooBigPrecision` (1426)

## Divergence

`NOW(fsp)` / `CURTIME(fsp)` / `UTC_TIME(fsp)` / `UTC_TIMESTAMP(fsp)` /
`SYSDATE(fsp)` with an fsp above `MaxFsp` (6) raised the generic
`Unsupported("bad fractional-seconds-precision argument")` fallback. Go
raises a CODED diagnostic at evaluation time:

- `builtinNowWithArgSig.evalString` (`builtin_time.go:2730`):
  `types.ErrTooBigPrecision.GenWithStackByArgs(fsp, "now", types.MaxFsp)`;
- UTC_TIMESTAMP at `:2600`, UTC_TIME at `:6904`, CURTIME at `:7219` — each
  naming its own function;
- errno `ErrTooBigPrecision` = **1426**, message
  `Too-big precision %d specified for '%-.192s'. Maximum is %d.`.

Go's build-time `CheckFsp` (`pkg/types/fsp.go:38`) is a separate, more
permissive pass: above `MaxFsp` it CLAMPS to `MaxFsp` (so the signature's
return type decimal is capped), -1 is the unspecified marker mapping to 0,
and other negatives error "Invalid fsp %d".

## Fix

- `EvalError::TooBigFsp { fsp, function }`: new variant carrying the fsp and
  the clock function's own name (`now`, `curtime`, `current_time`,
  `utc_time`, `utc_timestamp` — Go names each per its actual spelling).
- `time_fn::parse_fsp_for`: int/uint fsp above 6 raises `TooBigFsp`; fsp
  -1 (unspecified) maps to 0; other negatives keep the refusing form (Go's
  eval-time check only tests the > MaxFsp side; the negative path is a
  build-time concern and remains an honest refusing boundary here).
- `current_time` now carries its function name (`curtime` vs `current_time`)
  so the diagnostic names the spelling the client actually used.
- `driver/errors/exec.rs`: `EvalError::TooBigFsp` maps to
  `MysqlError::coded(1426, "Too-big precision {fsp} specified for
  '{function}'. Maximum is 6.")`.

The VALUES-tier dispatch (which lacks a static type) and the typed tier both
route through the same raise, so AST and chunk tiers observe identical
diagnostics.

## Fail-before / pass-after

New test `clock_fsp_above_max_reports_coded_1426`: `NOW(7)` and
`CURTIME(8)` must raise `TooBigFsp` — FAILS before (generic fallback) and
passes after. Existing pins updated from the generic fallback text to the
coded variant:

- `now(7)`/`now(8)`, `current_time(7)`, `utc_time(7)`, `utc_timestamp(8)`
  (chunk tier, `tests/datetime.rs` and `builtin_string_time_source.rs`);
- negative fsp rows (`now(-1)`, `now(-2)`, `current_time(-1)`) keep the
  refusing form, matching Go's build-time `CheckFsp` "Invalid fsp" split.

## Validation

- `tidb-expr` full suite: 1213/1213 (excluding the documented network flake).
- fmt clean; dead `parse_fsp` wrapper removed after the signature change.

## Go anchors

- `pkg/expression/builtin_time.go:2730 / :2600 / :6904 / :7219`
- `pkg/types/fsp.go:38` (`CheckFsp` build-time clamp/split)
- `pkg/errno` `ErrTooBigPrecision` = 1426

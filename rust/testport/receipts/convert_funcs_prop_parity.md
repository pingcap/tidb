# Charset-transcode classification parity (`convertActionMap` / `func_prop`)

Go source: `origin/master` at
`d152e4b78d35cfcb771bfabc289f837c2374d4aa` (2026-09-03). Verification batch:
the charset-transcode classification decides which builtin arguments get the
implicit UTF-8→charset byte transcode — a missing entry produces wrong bytes
on the wire, so the table is rank-1-adjacent.

## Method

Entry-by-entry diff of Go's `convertActionMap`
(`pkg/expression/builtin_convert_charset.go:300-330`) against
`convert_charset::func_prop`, plus the `isLegacyCharset` five-name set
(`:370-376`) against `is_legacy_charset`.

## Result: full parity

- `funcPropNone` — 27 names (bin, char_func, date_format, oct, space, and the
  22 no-implicit-conversion string functions) — matches.
- `funcPropBinAware` — 17 names (result binary-aware + encrypt functions) —
  matches.
- `funcPropAuto` — 37 names (string functions, operators, string comparing,
  regex, crc32) — matches, with this crate's own `case_when` rebuild name in
  place of Go's `case`.
- `isLegacyCharset` — utf8/utf8mb4/ascii/latin1/binary — matches.

Two verified non-divergences in the same seam:

- Go's `HandleBinaryLiteral` `funcPropAuto` binary-arg→non-binary-result arm
  (`BuildFromBinaryFunction`) is deliberately not modeled at the builder:
  `eval_cast`'s inline binary-source decode + 3854 warning covers the
  observable (see `cast_char_width_estimation.md`'s padding-gate follow-up
  and the recorded `from_binary` wrap experiment in PROGRESS.md).
- `CHAR(n) BINARY` versus `CHAR(n) CHARSET binary` collapse to one AST
  payload exactly as Go's own restore does; only the `BinaryFlag` residue
  differs (recorded in `cast_target_type_family.md`).

## Validation

Source-read verification executed as a script over the pinned tree; the
functional behavior driven by this table is pinned by the existing
`wrap_binary_literals`/`to_binary`/CHAR-charset regressions (all green in the
session sweeps).

## Risk

- None. Documentation-only batch; no code changed.

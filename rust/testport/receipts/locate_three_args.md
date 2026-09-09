# `LOCATE(substr, str, pos)`: the three-argument signatures

## Divergence

The registry declares `("locate", 2, Some(3))` — the 3-argument form was
accepted and type-inferred (the ETInt mask `arg_eval_type.rs` even carries
`b"LOCATE" => 1 << 2` for the position) — but NO eval arm handled it: the
values dispatch matched `vals.len() == 2` only and the scalar collation block
matched `args.len() == 2` only. Any 3-argument call fell through to the
generic not-yet-ported fallback.

## Go oracle

- `builtinLocate3ArgsSig` (`pkg/expression/builtin_string.go:1615`, bytes,
  binary collation) and `builtinLocate3ArgsUTF8Sig` (`:1660`, characters).
- Semantics: the 1-based `pos` converts to a 0-based index BEFORE the bounds
  check; `pos < 1` or a needle that cannot fit at/after `pos` answers 0; an
  EMPTY needle answers `pos` itself (so `LOCATE('', 'abc', 4)` is `4` and
  `LOCATE('', 'abc', 5)` is `0`); the reported position counts from the
  STRING start, not the slice.
- Under a case-insensitive collation Go lowers BOTH strings with
  `strings.ToLower` BEFORE the rune-count bounds, and matches through the
  function's collator.
- `locateFunctionClass.getFunction` declares the position
  `types.ETInt`, so `newBaseBuiltinFuncWithTp` wraps it in
  `WrapWithCastAsInt` — a string position like `'2'` is cast by the ordinary
  CAST-as-signed boundary before the signature runs.

## Fix

- `crates/tidb-expr/src/string_fn.rs`: new `locate_with_position` porting
  both 3-arg signatures — binary (byte windows) vs utf8 (character units,
  CI lowering first via `tidb_mysql::to_lowercase`, matching through the
  collator), NULL propagation on any argument, and the pos bounds exactly as
  Go orders them.
- `crates/tidb-expr/src/scalar_function.rs`: the `locate` arm extended to
  2-or-3 arguments; the 3-argument form applies Go's `WrapWithCastAsInt`
  boundary (`cast_arg_as_int`, using the argument's own static type so an
  UNSIGNED source casts unsigned) before dispatch. `INSTR` stays 2-argument
  (Go has no position signature for it) and keeps its internal swap.
- `crates/tidb-expr/src/func.rs`: the values dispatch grew the
  `vals.len() == 3` LOCATE arm.

## Fail-before / pass-after

New test `locate_with_position_matches_go_three_args_signature`
(`tests/builtin_string_time_source.rs`): Go `TestLocatePosition` rows —
`locate('bar','foobarbar',5)` = 7, non-fitting and `pos < 1` rows = 0,
empty-needle = pos rows, the exact-end match, and the utf8mb4_bin exact
case — FAILS before the arm (generic not-yet-ported fallback) and passes
after. The pre-existing ETInt-cast boundary test
(`a_non_integer_etint_argument_is_cast_before_the_signature_runs`) now also
passes its `locate('b','abcdef','2')` = `INT:2` row in BOTH tiers: the
chunk tier initially failed with the refusing `eval_int` because my first
version intercepted the string position before the wrap pass — fixed by
applying `cast_arg_as_int` in the arm itself, exactly where Go's builder
applies it.

## Validation

- `tidb-expr` full suite: 1212/1212 (excluding the documented network flake).
- fmt clean.

## Go anchors

- `pkg/expression/builtin_string.go:1615-1698` (both 3-arg signatures)
- `pkg/expression/builtin_string.go:1503-1506` (the ETInt tail declaration)

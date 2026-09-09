# `pkg/expression` planner constant-cast warning parity receipt

Status: completed Rust-only alignment for Go's construction-time integer
coercion and warning ownership at Go-master
`f2c346fe4f368ff855e17c1f62e28a89ba7f9723`.

Go's `newBaseBuiltinFuncWithTp` wraps string arguments to ETInt through
`WrapWithCastAsInt` while the live statement context records truncation and
out-of-range diagnostics. Rust's planner resolver folded constant expressions
through `NoColumns`; this preserved the value but discarded warnings before
execution. The same loss affected explicit `CAST(... AS SIGNED/UNSIGNED)` and
the Rust carriers for `VITESS_HASH`, `TIDB_SHARD`, and `FORMAT` precision.

Rust now exposes an opt-in constant-fold mode for the executable planner. It
leaves warning-producing string/byte integer casts and their carrier functions
unfolded when the planner has no statement warning sink; execution then uses
the real `StmtContext` and reports each warning once. Ordinary expression and
DDL folds retain their existing construction-time behavior.

Focused regressions:

- `tests_cast_int_truncation::implicit_integer_cast_keeps_warning_after_plan_fold`
  proves the nested `VITESS_HASH` conversion keeps code 8030, the signed
  complement value, and the wire warning count.
- `tests_cast_int_truncation::a_partly_numeric_string_cast_to_int_warns_1292_and_keeps_the_prefix`
  covers exact and truncated signed casts in strict and non-strict SELECTs.
- `tests_cast_int_truncation::two_bad_casts_in_one_select_leave_two_warnings`,
  `an_unsigned_cast_warns_on_the_same_strings`,
  `signed_string_overflow_reaches_sql_and_implicit_int_consumers`, and
  `a_cast_read_never_fails_the_statement` preserve multiplicity, UNSIGNED, and
  read-side mode invariants.

Validation:

- focused planner/session cast regressions: pass
- `cargo fmt --check` and `git diff --check`: pass
- locked offline `tidb-executor` all-target check: pass
- full Ready lint profile: pass

No Go, generated, platform, Bazel, or module files changed.

# Builtin expression divergence inventory (`pkg/expression` vs `tidb-expr`)

A function-by-function comparison of TiDB's Go builtin evaluation against the
Rust `tidb-expr` crate. Source of truth is Go; nothing here was executed (this
machine cannot run freshly built binaries), so every claim is a source reading
with a concrete distinguishing input derived from the Go control flow.

**Status: IN PROGRESS.** See "Resume here" at the bottom for exactly where a
later unit should pick up.

## Structural note that explains most of what follows

Go selects a *signature* at build time from the argument `FieldType` flags
(`getFunction` in each `*FunctionClass`), and the chosen signature then fixes
both the evaluation domain and the result's signedness/flen. `tidb-expr` splits
this in two: `builtin_arithmetic.rs` does the *type* inference, while
`ops.rs::eval_binary_full` re-dispatches at run time on the *`Datum` kind* of
the values. The two dispatches are not the same function of the input, so any
place where Go's signature choice depends on a flag that the runtime `Datum`
does not carry (most importantly `UnsignedFlag` on a `Decimal`/`Real` operand)
is a candidate divergence. Findings A and B below are both instances.

## Ranked findings

### A. `DIV` on a decimal operand reports "Division by 0" where Go reports overflow (or returns a value)

- Go: `pkg/expression/builtin_arithmetic.go:926` `builtinArithmeticIntDivideDecimalSig.evalInt`
- Rust: `rust/crates/tidb-expr/src/ops.rs:708` (`decimal_binary`, `IntDiv` arm)
- Helper: `rust/crates/tidb-datatype/src/decimal.rs:690` `Decimal::div_rem`

`div_rem` returns `None` for **two** different conditions — division by zero
(`other.is_zero()`, line 691) and *a quotient too wide for `i64`*
(`q_digits.parse().ok()?`, line 699). `ops.rs` collapses both into
`ctx.handle_division_by_zero()`.

Go separates them. `evalInt` computes `types.DecimalDiv` first; a zero divisor
comes back as `types.ErrDivByZero` and takes the division-by-zero path
(line 938), while an out-of-`BIGINT` quotient is caught later by `ToInt`/`ToUint`
and raised as `types.ErrOverflow` (lines 965, 972) — an unconditional error,
never a warning.

Distinguishing inputs:

| expression | Go | Rust |
| --- | --- | --- |
| `SELECT 99999999999999999999 DIV 1.0;` (20 nines) | `ERROR 1690 (22003): BIGINT value is out of range in '(99999999999999999999 DIV 1.0)'` | non-strict: warning `1365 Division by 0` + `NULL`; strict: `ERROR 1365 Division by 0` |
| `SELECT 18446744073709551615 DIV 1.5;` | `12297829382473034410` (Go stamps `UnsignedFlag` because the LHS literal is unsigned, then reads the quotient back with `ToUint`, line 956) | non-strict: warning `1365` + `NULL`; strict: `ERROR 1365` |

The second row is the worse one: a **wrong value** (`NULL` for a well-defined
result) on an input with no error in it at all.

Consequence rank: (1) wrong value, and (2) error-vs-warning inverted.

**Partial fix applied** in this branch: `decimal_binary`'s `IntDiv` arm now
tests `b.is_zero()` itself, so a `None` from `div_rem` can only mean the
quotient overflowed and is reported as `EvalError::IntOverflow`. That converts
row 1 to matching behaviour. Row 2 (the unsigned-flag-dependent `ToUint` read)
is **not** fixed — see finding B.

### B. `DIV`'s result signedness ignores `UnsignedFlag` once either operand is decimal/real

- Go: `pkg/expression/builtin_arithmetic.go:853` (flag stamped on `bf.tp`) and
  `:952-967` (`isLHSUnsigned || isRHSUnsigned` -> `c.ToUint()`)
- Rust: `rust/crates/tidb-expr/src/ops.rs:709` (`Datum::Int(q)`, unconditionally
  signed) and `:936` (the `float_binary` `IntDiv` arm)

Go's rule for `DIV` is: **either** operand unsigned makes the *result* unsigned,
and the quotient is then read back with `ToUint`, which accepts the whole
`[0, 2^64)` range. The Rust decimal path always produces `Datum::Int`, so the
representable range is `[-2^63, 2^63)` and everything above `i64::MAX` is lost
(currently as finding A's mis-reported error).

Go also has a special case with no Rust counterpart at
`builtin_arithmetic.go:960-964`: when the unsigned result lands in `(-1, 0]`,
`ToUint` overflows, `ToInt` truncates to `0` with `ErrTruncated`, and Go
returns **`0` with no error at all**.

Distinguishing input: `SELECT CAST(1 AS UNSIGNED) DIV -3.0;`
The exact quotient is `-0.3333`; Go takes the `v == 0 && err == ErrTruncated`
branch and returns `0`. Rust's `div_rem` truncates to `0` too but types it
`Datum::Int(0)` — value agrees here, so this one is a *type* divergence only.
The value divergence is the `> i64::MAX` case in finding A row 2.

Not fixed: the runtime `Datum` reaching `decimal_binary` does not carry the
argument `FieldType` flags, so this needs the signedness to be threaded from
`infer_arithmetic_type`'s already-correct `"intdiv"` arm
(`rust/crates/tidb-expr/src/builtin_arithmetic.rs:329`) into the evaluator.
That is an evaluator-plumbing change, not a one-line fix.

Consequence rank: (1) wrong value above `i64::MAX`; (3) wrong result type
otherwise.

## Verified-equal inventory

(Functions read on both sides and found to agree. A later unit can trust these
and skip them.)

| Function | Go | Rust | Note |
| --- | --- | --- | --- |
| `+` integer, all four signedness pairs | `builtin_arithmetic.go:229` | `ops.rs:567` `integer_add` | Each of Go's four `switch` cases maps to the matching checked op; overflow is `ErrOverflow` in both, never a wrap. |
| `-` integer, all four signedness pairs | `builtin_arithmetic.go:463`, `overflowCheck` at `:491` | `ops.rs:595` `minus_overflows` | Branch-for-branch port. `forceToSigned`/`NO_UNSIGNED_SUBTRACTION` is unmodelled in Rust and defaulted off — noted as unverified below, not as a divergence. |
| `*` integer | `builtin_arithmetic.go:687` (unsigned sig) and `:705` (signed sig) | `ops.rs:471` | Go's two sigs are selected by `HasUnsignedFlag(lhs) \|\| HasUnsignedFlag(rhs)`, which is exactly the Rust `unsigned` predicate here; both reduce to `checked_mul` on the respective type. |
| `/` promotes to decimal even for `Int`/`Int` | `builtin_arithmetic.go:740` (no `ETInt` arm at all) | `ops.rs:389` | Both always produce a decimal; result scale `scale(a) + div_precision_increment` matches `setType4DivDecimal:151`. |
| `/` by zero | `builtin_arithmetic.go:811` | `ops.rs:395` | Both route through the division-by-zero handler and yield `NULL`. |

## Resume here

Scope items covered so far: (1) arithmetic — partially: integer `+ - *`,
`DIV`, `/`. **Not yet examined at all**: integer `MOD` signedness pairs, decimal
`MOD`, real `MOD`, the `setFlenDecimal4RealOrDecimal` flen/decimal rules against
`builtin_arithmetic.rs:135`, and scope items 2 through 7 (comparison/coercion,
control flow, string/collation, `CAST`, math/rounding, temporal).

## Unverified

- Nothing in this document was executed. `cargo check`/`cargo clippy` are the
  only gates this machine can run.
- Go's `sql_mode` interactions (`NO_UNSIGNED_SUBTRACTION`, strict vs non-strict
  for the division handler) were read, not exercised.

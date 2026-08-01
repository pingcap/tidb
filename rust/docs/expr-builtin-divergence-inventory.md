# Builtin expression divergence inventory (`pkg/expression` vs `tidb-expr`)

A function-by-function comparison of TiDB's Go builtin evaluation against the
Rust `tidb-expr` crate. Go source is the oracle. **Nothing here was executed** —
this machine cannot run freshly built binaries — so every claim is a source
reading, and every distinguishing input is derived from Go's control flow rather
than observed. `cargo check`/`cargo clippy`/`cargo fmt` are the only gates run.

**Status: PARTIAL.** See [Resume here](#resume-here) for exactly where the sweep
stopped and what is untouched.

---

## The structural cause behind most findings

Go selects a *signature* at build time from the argument `FieldType`s
(`getFunction` on each `*FunctionClass`), and that choice fixes the evaluation
domain, the result's signedness, and its flen/decimal. `tidb-expr` splits this
in two:

- `builtin_arithmetic.rs` / `builtin_compare.rs` / `rewriter.rs` infer the
  result **type** from `FieldType`s (mostly faithfully), and
- `ops.rs::eval_binary_full`, `math_fn/mod.rs`, `string_fn.rs` evaluate the
  **value** by re-dispatching on the runtime `Datum` kind.

These are not the same function of the input. Wherever Go's signature choice
depends on something a runtime `Datum` does not carry — `UnsignedFlag` on a
decimal/real operand, a column's *declared* `flen`/`decimal` as opposed to the
digits actually present, the derived collation's binary-ness — the two dispatches
can disagree. Findings **B**, **C**, **D** and **E** are all instances of exactly
this. A later unit looking for more findings should hunt for that shape first.

---

## Ranked findings

Rank key: (1) wrong VALUE; (2) error-vs-warning inverted; (3) wrong result TYPE
or flen; (4) diagnostics only.

### A — rank 1+2 — `DIV` on a decimal operand reports "Division by 0" where Go reports overflow

- Go: `pkg/expression/builtin_arithmetic.go:926` `builtinArithmeticIntDivideDecimalSig.evalInt`
- Rust: `rust/crates/tidb-expr/src/ops.rs:708` (`decimal_binary`, `IntDiv` arm)
- Helper: `rust/crates/tidb-datatype/src/decimal.rs:690` `Decimal::div_rem`

`div_rem` answers `None` for **two** unrelated conditions: a zero divisor
(`other.is_zero()`, line 691) and a quotient too wide for `i64`
(`q_digits.parse().ok()?`, line 699). `ops.rs` collapsed both into
`ctx.handle_division_by_zero()`.

Go keeps them apart. `evalInt` runs `types.DecimalDiv` first; a zero divisor
returns `types.ErrDivByZero` and takes the division-by-zero path (line 938),
while an out-of-`BIGINT` quotient is caught afterwards by `ToInt`/`ToUint` and
raised as `types.ErrOverflow` (lines 965, 972) — **unconditionally an error**,
never downgraded to a warning by any sql_mode.

| expression | Go | Rust (before fix) |
| --- | --- | --- |
| `SELECT 99999999999999999999 DIV 1.0;` | `ERROR 1690 (22003): BIGINT value is out of range in '(99999999999999999999 DIV 1.0)'` | non-strict: warning `1365 Division by 0` + `NULL`; strict: `ERROR 1365 Division by 0` |
| `SELECT 18446744073709551615 DIV 1.5;` | `12297829382473034410` — Go stamps `UnsignedFlag` (line 853, the LHS literal is unsigned) and reads the quotient back with `ToUint` (line 956), which spans the whole `u64` range | non-strict: warning `1365` + `NULL`; strict: `ERROR 1365` |

Row 2 is the worse case: a **wrong value** — `NULL` for a well-defined result —
on an input containing no error at all.

**FIXED (partially), this branch.** `decimal_binary`'s `IntDiv` arm now tests
`b.is_zero()` itself, so a `None` from `div_rem` can only mean the quotient
overflowed, and it is reported as `EvalError::IntOverflow`. That makes row 1
agree with Go. Row 2 still returns an error rather than the value — it needs
finding B.

### B — rank 1/3 — `DIV`'s result signedness ignores `UnsignedFlag` once an operand is decimal

- Go: `pkg/expression/builtin_arithmetic.go:853` (flag stamped on `bf.tp`),
  `:952-967` (`isLHSUnsigned || isRHSUnsigned` -> `c.ToUint()`)
- Rust: `rust/crates/tidb-expr/src/ops.rs:709` (`Datum::Int(q)`, always signed)

Go's `DIV` rule: **either** operand unsigned makes the result unsigned, and the
quotient is then read with `ToUint`, spanning `[0, 2^64)`. The Rust decimal path
always yields `Datum::Int`, capping the representable range at `i64::MAX`.

This is inconsistent *within Rust itself*: the sibling float path already
captures the same distinction at `ops.rs:921`
(`let unsigned_div = matches!(l, Datum::UInt(_)) || matches!(r, Datum::UInt(_))`)
and produces `Datum::UInt`. The decimal path does not, and it cannot simply copy
that line, because `Decimal::div_rem` returns an `i64` quotient — the wider value
is unrepresentable before `tidb-datatype` changes. (That crate is owned by
another unit this session, so this was left alone deliberately.)

Go also has a special case with no Rust counterpart, `builtin_arithmetic.go:960`:
when an unsigned result lands in `(-1, 0]`, `ToUint` overflows, `ToInt`
truncates to `0` with `ErrTruncated`, and Go returns **`0` with no error at
all**. Distinguishing input `SELECT CAST(1 AS UNSIGNED) DIV -3.0;` — exact
quotient `-0.3333`; Go returns `0`; Rust's `div_rem` also truncates to `0` but
types it `Datum::Int(0)`, so here only the *type* differs.

**NOT FIXED.** Needs the signedness threaded from `infer_arithmetic_type`'s
already-correct `"intdiv"` arm (`builtin_arithmetic.rs:329`) into the evaluator,
plus a `u64`-capable quotient.

### C — rank 1 — `ROUND`/`TRUNCATE` on a decimal ignore the result type's decimal cap when the scale argument is non-constant

- Go: `pkg/expression/builtin_math.go` `builtinRoundWithFracDecSig.evalDecimal`
  (`val.Round(to, min(int(frac), b.tp.GetDecimal()), ModeHalfUp)`) and
  `builtinTruncateDecimalSig.evalDecimal`
  (`x.Round(result, min(int(d), b.getRetTp().GetDecimal()), ModeTruncate)`);
  the cap comes from `calculateDecimal4RoundAndTruncate`
- Rust: `rust/crates/tidb-expr/src/math_fn/mod.rs:740`
  (`let target_scale = d.clamp(i32::MIN as i64, 30) as i32;`)

`calculateDecimal4RoundAndTruncate` returns:

- `0` when the result eval type is `ETInt`, or the function has one argument;
- **`args[0].GetType().GetDecimal()`** when the scale argument is *not* a
  constant;
- otherwise the constant scale, floored at `0` and capped at
  `mysql.MaxDecimalScale` (30).

Rust caps at 30 and nothing else. The two agree for every *constant* scale, and
disagree whenever the scale is a column or any other non-constant expression
whose value exceeds the first argument's declared scale.

```sql
CREATE TABLE t (d DECIMAL(10,2), f INT);
INSERT INTO t VALUES (1.23, 5);
SELECT ROUND(d, f), TRUNCATE(d, f) FROM t;
```

| | Go | Rust |
| --- | --- | --- |
| `ROUND(d, f)` | `1.23` (cap = `d`'s scale, 2) | `1.23000` |
| `TRUNCATE(d, f)` | `1.23` | `1.23000` |

Numerically equal, but the **text the client receives differs**, which is a wrong
returned value over both the text and binary protocols, and a different
`decimals` byte in the column definition.

**NOT FIXED** — the evaluator has no access to the argument `FieldType`.

### D — rank 1 — `LIKE` counts `_` in characters even for a binary operand

- Go: `pkg/expression/builtin_like.go` `builtinLikeSig.evalInt` ->
  `b.collator().Pattern()`; for collation `binary` that is
  `pkg/util/stringutil/string_util.go:203` `CompilePatternInnerBinary` +
  `:281` `DoMatchBinary`, both **byte**-wise. The non-binary collators use
  `:155` `CompilePatternInner` + `:289` `DoMatch`, which are rune-wise.
- Rust: `rust/crates/tidb-expr/src/like.rs:94` `like_match_by` —
  `let text: Vec<char> = text.chars().collect();`, always rune-wise. There is
  no byte-wise variant; `like_match` and `like_match_with_collation` both
  funnel here.

Go therefore has two matchers and picks between them by the derived collation;
Rust has one.

```sql
CREATE TABLE t (b BLOB);
INSERT INTO t VALUES ('é');           -- 2 bytes in UTF-8
SELECT b LIKE '_' FROM t;
```

Go: `0` — `_` consumes one **byte**, and 2 bytes remain unmatched against a
1-token pattern. Rust: `1` — `é` is one `char`.

Equivalently `SELECT _binary'é' LIKE _binary'_';`.

**NOT FIXED** — needs a byte-wise `compile`/`match` pair plus the derived
collation at the call site.

### E — rank 3 — `FLOOR`/`CEIL` pick the decimal-vs-int result type from the runtime digits, not the declared width

- Go: `pkg/expression/builtin_math.go` `getEvalTp4FloorAndCeil` —
  `if fieldTp.GetFlen()-fieldTp.GetDecimal() > mysql.MaxIntWidth-2 { retTp = ETDecimal }`
- Rust: `rust/crates/tidb-expr/src/math_fn/mod.rs:613-625` — `integer_digits`
  computed from `d.coefficient_digits().len() - d.storage_scale()`, i.e. from
  the **value**

Go's cutoff reads the column's *declared* integer width, so it is a property of
the schema and constant across rows. Rust's reads the digits the row actually
holds, so it varies row by row.

```sql
CREATE TABLE t (d DECIMAL(20,0));
INSERT INTO t VALUES (5);
SELECT FLOOR(d) FROM t;
```

Go: result type `DECIMAL` (declared width 20 > 18), value `5`.
Rust: `integer_digits` is 1, so `Datum::Int(5)` — result type `BIGINT`.

The printed text is `5` either way; the divergence is the result-set column type
(`MYSQL_TYPE_NEWDECIMAL` vs `MYSQL_TYPE_LONGLONG`), which changes the binary
protocol encoding and what a client driver hands back to the application. The
Rust code's own comment claims this preserves the source boundary; it does so
only for literals wide enough to carry their own digits (`9223372036854775807.0`
does), not for narrow values in wide columns.

**NOT FIXED** — same `FieldType`-plumbing dependency as B and C.

### F — rank 2 — `SUBSTRING`'s position/length arguments refused instead of cast

- Go: `pkg/expression/builtin_string.go` `substringFunctionClass.getFunction`
  builds every signature through `newBaseBuiltinFuncWithTp(..., types.ETInt, ...)`
  for arguments 2 and 3, so a non-integer argument is **cast** before
  `builtinSubstring2ArgsSig`/`...3ArgsSig`'s `evalString` ever runs.
- Rust: `rust/crates/tidb-expr/src/string_fn.rs:131` matched only
  `Datum::Int(pos)` and fell through to
  `Err(EvalError::Unsupported("bad SUBSTRING arguments"))`.

`SELECT SUBSTRING('hello', '2');` — Go: `ello`. Rust: a statement error. Same
for a `Datum::UInt` position (`SUBSTRING('hello', CAST(2 AS UNSIGNED))`), a
decimal position, and any of those in the length slot.

**FIXED, this branch.** Both arguments now go through `crate::cast::to_i64_signed`
— the same helper `LEFT`/`RIGHT` (`string_fn.rs:106`) already use for the
identical Go `ETInt` cast.

### G — rank 4 — `CONCAT` has no `max_allowed_packet` guard

- Go: `pkg/expression/builtin_string.go` `builtinConcatSig.evalString` —
  `if uint64(len(s)+len(d)) > b.maxAllowedPacket { return "", true, handleAllowedPacketOverflowed(...) }`,
  which appends warning `1301 Result of concat() was larger than max_allowed_packet`
  and returns `NULL`.
- Rust: `rust/crates/tidb-expr/src/string_fn.rs` `concat` — no size check.

A `CONCAT` result over `max_allowed_packet` (default 64 MiB) is `NULL` + warning
in Go and a full-length string in Rust. Listed for completeness; it needs
session state the value evaluator does not carry, and the same guard is missing
from the other packet-limited string builtins.

---

## Verified-equal inventory

Read on both sides during this sweep and found to agree. A later unit can skip
these.

### Arithmetic

| Function | Go | Rust | Why equal |
| --- | --- | --- | --- |
| `+` integer, all 4 signedness pairs | `builtin_arithmetic.go:229` | `ops.rs:567` `integer_add` | Each of Go's four `switch` cases maps to the matching checked op; overflow is `ErrOverflow` in both, never a silent wrap. |
| `-` integer, all 4 signedness pairs | `builtin_arithmetic.go:463`; `overflowCheck` at `:491` | `ops.rs:595` `minus_overflows` | Branch-for-branch port. `forceToSigned`/`NO_UNSIGNED_SUBTRACTION` is unmodelled and defaulted off — listed under Unverified, not as a divergence. |
| `*` integer | `builtin_arithmetic.go:687` (unsigned sig), `:705` (signed sig) | `ops.rs:471` | Go's two sigs are selected by `HasUnsignedFlag(lhs) \|\| HasUnsignedFlag(rhs)`, which is exactly the Rust `unsigned` predicate; both reduce to `checked_mul` on the respective type. |
| `+ - * /` real | `builtin_arithmetic.go:319`, `:400`, `:651`, `:779` | `ops.rs:925-935` | Go rejects a non-finite result (`mathutil.IsFinite` for `+`, `math.IsInf` for `*` and `/`); with finite operands the two predicates cannot disagree, and `finite_float` covers both. |
| `%` real | `builtin_arithmetic.go:1076` | `ops.rs:952` | `math.Mod` and Rust `%` are both C `fmod`. Go has no overflow check; a finite `fmod` is always finite, so `finite_float` never fires. |
| `/` always promotes to decimal, even `Int`/`Int` | `builtin_arithmetic.go:740` — the class has no `ETInt` arm | `ops.rs:389` | Result scale `scale(a) + div_precision_increment` matches `setType4DivDecimal:151`. |
| `/` by zero | `builtin_arithmetic.go:811` | `ops.rs:395` | Both route through the division-by-zero handler and yield `NULL`. |
| `%` integer, all 4 signedness pairs | `builtin_arithmetic.go:1143`, `:1181`, `:1224`, `:1267` (four distinct sigs) | `ops.rs:518-534` | Go's four sigs are reproduced as the four `(Integer, Integer)` arms; both keep the **dividend's** sign and make the result unsigned iff the LHS is (`getFunction:1037`). |
| `%` by zero | `builtin_arithmetic.go:1092`, `:1124` | `ops.rs:511`, `:715`, `:953` | Division-by-zero handler + `NULL` on all three domains. |

### Comparison and coercion

| Rule | Go | Rust | Why equal |
| --- | --- | --- | --- |
| string vs string -> `ETString`, collation compare | `getBaseCmpType`, `builtin_compare.go:1415` | `ops.rs:241-247` `string_cmp_operand` + `string_compare` | PAD SPACE / NO PAD is delegated to `Collation::compare` on both sides. |
| int vs string -> `ETReal` (fallthrough) | `builtin_compare.go:1428` | `ops.rs:354-366` | Both coerce via the MySQL numeric-prefix scan. |
| decimal vs string -> `ETReal` | `builtin_compare.go:1419` | `ops.rs:354` | Same path. |
| int vs decimal -> `ETDecimal` | `builtin_compare.go:1421` | `ops.rs:407` `decimal_binary` | Int promotes to a scale-0 decimal; exact compare. |
| ENUM/SET vs string -> `ETString` (compares the LABEL) | `builtin_compare.go:1415` (`IsStringKind` is true for `ETString`, which `TypeEnum`/`TypeSet` evaluate to) | `ops.rs:1212-1213` | `string_cmp_operand` returns the enum/set *name*, so both compare labels, not ordinals. |
| ENUM/SET vs int -> `ETInt` (compares the ORDINAL) | `builtin_compare.go:1417` (`lft.Hybrid()`) | `ops.rs:432` `integer_of` | An `Int` operand yields `None` from `string_cmp_operand`, dropping the pair to the integer residue where the enum reads as its ordinal. |
| JSON on either side -> `ETJson` | `GetAccurateCmpType`, `builtin_compare.go:1439` | `ops.rs:226` | Intercepted above the string branch on both sides; ordering is type-precedence-first in both. |
| `<=>` (`nulleq`) NULL rule | `builtin_compare.go` `nulleq` sigs | `ops.rs:195-199`, `:667-672`, `:905-909` | `NULL <=> NULL` is `1`, `NULL <=> x` is `0`, in the integer, decimal and float domains alike. |
| result `FieldType` of every comparison | `generateCmpSigs` (`ETInt` base, `SetFlen(1)`, `IsBooleanFlag`) | `builtin_compare.rs:47-55` | Flen 1 and the boolean flag are both set. |

### Math and rounding

| Function | Go | Rust | Why equal |
| --- | --- | --- | --- |
| `types.Round` (real) | `pkg/types/helper.go` `Round` + `RoundFloat` = `math.RoundToEven` | `math_fn/mod.rs` `go_round_float` (`round_ties_even`) | Ties to even on both sides, including the `IsInf(tmp)` early return and the `IsNaN(result) -> 0` tail. |
| `types.Truncate` (real) | `pkg/types/helper.go` `Truncate` | `math_fn/mod.rs` `go_truncate_float` | Including the `shift == 0.0 -> 0.0` branch and the NaN passthrough. |
| `math.Pow10` | Go `src/math/pow10.go` | `math_fn/mod.rs` `go_pow10` | Table-lookup port, deliberately reproducing Go's 1-ULP imprecision rather than `powi`. |
| `ROUND(int_expr)` 1-arg | `builtinRoundIntSig` (`builtin_math.go:393`) — a plain passthrough | `math_fn/mod.rs:700`, `[v] if round => (v, 0)` then the `Datum::Int` arm | Both return the argument unchanged as `Int`. |
| `TRUNCATE(int, d)` for `d < 0`, incl. `i64::MIN` and `d <= -19` | `builtinTruncateIntSig` — `d == mathutil.MinInt -> 0`, else `int64(math.Pow10(-d))` | `go_truncate_int` — `checked_neg`/`checked_pow`, `None -> 0` | Go's `int64(1e19)` saturates to `i64::MIN`, making `x / shift * shift` zero for any in-range `x`; Rust's `None -> 0` produces the same answer for every `d <= -19` and for `i64::MIN`. |
| `TRUNCATE(uint, d)` at the `u64` boundary | `builtinTruncateUintSig` — operates on the original `uint64` | `go_truncate_uint` | Neither routes through `f64`, so the exact low digits survive. |
| `TRUNCATE(x, unsigned_d)` returns `x` unchanged | `builtinTruncateIntSig`/`...UintSig` inspect `HasUnsignedFlag(args[1])` **before** the value | `math_fn/mod.rs:698` `unsigned_integer_scale` | Both make the decision on the scale's *type*, not its value. |
| `ROUND`/`TRUNCATE` of a string/temporal argument | `roundFunctionClass`/`truncateFunctionClass` coerce a non-int/non-decimal argument to `ETReal` | `math_fn/mod.rs` `other =>` arm | Both produce a `DOUBLE`, via the shared numeric-prefix coercion. |
| `ROUND(dec, const_frac)` incl. negative and `> 30` | `calculateDecimal4RoundAndTruncate` + `min(frac, tp.decimal)` | `math_fn/mod.rs:740` | For a **constant** scale the two caps coincide: `< 0` gives Go `min(frac, 0) == frac`, and `> 30` gives `min(frac, 30) == 30`. Only the non-constant case diverges (finding C). |

### Strings

| Function | Go | Rust | Why equal |
| --- | --- | --- | --- |
| `SUBSTRING(s, pos[, len])` index arithmetic | `builtinSubstring2ArgsSig`/`...3ArgsSig` | `string_fn.rs:139-160` | `pos == 0`, `pos == -len`, `pos < -len`, `len <= 0`, and `pos + len` overflowing `i64` all produce the same answer; Rust's `checked_add` reproduces Go's `end < pos` wrap branch. |
| `SUBSTRING` byte-vs-character units | Go picks `...Sig` vs `...UTF8Sig` by charset | `string_fn.rs` `StrUnits` | The unit is carried by `StrUnits`, so both bodies are the same arithmetic over the selected unit. |
| `LOCATE`/`INSTR` binary vs UTF-8 signature | `locateFunctionClass.getFunction` picks by `bf.collation == charset.CollationBin` | `string_fn.rs:205` `locate` + `:231` `locate_collation` | Binary gives a 1-based **byte** offset with no folding; non-binary a 1-based **character** offset. Empty needle is `1` in both. |
| `TRIM` (1/2/3-arg), whole-`remstr` stripping | `trimLeft`/`trimRight` (`builtin_string.go`) | `string_fn.rs:1718` `trim_value` | Both strip whole occurrences repeatedly, never per character. |
| `TRIM` with an empty `remstr` | `strings.TrimPrefix(str, "")` returns `str`, so Go's loop exits immediately | `string_fn.rs:1727` explicit guard | No-op on both sides (Rust needs the explicit guard only because `trim_start_matches` would loop). |
| `TRIM(str)` 1-arg | `strings.Trim(d, spaceChars)`, `spaceChars = " "` (`builtin_string.go:1862`) | caller resolves `remstr` to a single space | Trimming all leading/trailing spaces one at a time is the same set as a single-space cutset trim. |
| `TRIM` NULL propagation incl. a NULL `remstr` in the 3-arg form | `builtinTrim3ArgsSig` returns `isRemStrNull` | `trim_value`'s `(Some, Some)` destructure | `NULL` in both. |
| `CONCAT` with a NULL argument | `builtinConcatSig` returns on the first `isNull` | `string_fn.rs` `concat` | `NULL` if any argument is `NULL` (packet limit aside — finding G). |
| `LIKE` escape semantics | `CompilePatternInner`'s `case escapeRune` (`string_util.go:166`) | `like.rs:43` `compile_like` | `\c` is a literal `c` for any `c`; a **trailing** escape character is a literal instance of itself in both. |
| `LIKE ... ESCAPE ''` | Go sets `escapeRune = rune(0)`, so only a literal NUL escapes and `\` is an ordinary character | `like.rs:46` `Some(0) => None` | Same observable behaviour for any pattern without an embedded NUL. |
| `LIKE` `%%` collapse and `%_` -> `_%` swap | `string_util.go:174-185` | not performed | Both are semantics-preserving optimisations; the match results are identical. |

### Cast

| Direction | Go | Rust | Why equal |
| --- | --- | --- | --- |
| `CAST(unsigned_int AS SIGNED)` | `castAsInt` passes the `int64` bits through | `cast.rs:120` `*i as i64` | `CAST(18446744073709551615 AS SIGNED)` is `-1` in both. |
| `CAST(-5 AS UNSIGNED)` | low 64 bits preserved | `cast.rs:143` | `18446744073709551611` in both — not an error, not a display-only wrap. |
| `CAST(decimal AS SIGNED/UNSIGNED)` rounding mode | `builtinCastDecimalAsIntSig` rounds `ModeHalfUp` **before** converting | `cast.rs:121`, `:149` (`round_to_i64_saturating` / `round_to_u64_saturating`, ties away from zero) | Half-up on both. |
| `CAST(real AS SIGNED/UNSIGNED)` rounding mode | `ConvertFloatToInt`/`ConvertFloatToUint` -> `RoundFloat` = `math.RoundToEven` | `cast.rs:122`, `:168` (`round_ties_even`) | Ties to even on both — the deliberate asymmetry against the decimal path is reproduced. |
| `CAST(negative_real AS UNSIGNED)` | `ConvertFloatToUint` clamps to `0` under default flags | `cast.rs:169-172` | `0` in both, unlike the integer source's bit reinterpretation. |

---

## Resume here

**Scope covered.**

1. *Arithmetic* — **done** for `+ - * / DIV %` across integer (all four
   signedness pairs), decimal and real, plus division-by-zero on each. Findings
   A, B. **Not done:** the `setFlenDecimal4RealOrDecimal` /
   `setType4DivDecimal` / `setType4ModRealOrDecimal` flen-and-decimal rules were
   read on the Go side (`builtin_arithmetic.go:106`, `:143`, `:983`) but only
   spot-checked against `builtin_arithmetic.rs:135`, `:206`, `:230`. That is the
   single highest-value place to resume — it is pure type inference, so it can
   be compared statically with no oracle at all.
2. *Comparison and coercion* — **mostly done**: every `getBaseCmpType` branch,
   the ENUM/SET and JSON cases, `<=>`. **Not done:** `GetAccurateCmpType`'s
   const-refinement arms (`builtin_compare.go:1454-1483`) beyond the
   already-known decimal-vs-const-string one; the temporal-column-vs-constant
   arm; `getCmpTp4MinMax` for `GREATEST`/`LEAST`.
3. *Control flow* — **barely started.** `InferType4ControlFuncs`
   (`builtin_control.go`) and `rewriter.rs:443` were read side by side but not
   compared case by case. Go's `len(notNullFields) == 1` shortcut (copy that one
   field type verbatim) versus Rust's unconditional `agg_field_type` +
   `set_numeric_len_from_args` is an unexamined suspect. `CASE`/`IF` branch
   laziness and `NULLIF`'s NULL rule were not looked at at all.
4. *Strings* — **partly done:** `SUBSTRING`, `LEFT`/`RIGHT`, `LOCATE`/`INSTR`,
   `TRIM` (all three arities), `CONCAT`, `LIKE`. Findings D, F, G. **Not done:**
   `REPLACE`, `CHAR` vs `VARCHAR` padding on comparison and on read-back,
   `LPAD`/`RPAD` truncation, `STRCMP`'s collation, `ELT`/`FIELD`/`MAKE_SET`,
   `EXPORT_SET`, and every packet-limited builtin besides `CONCAT`.
5. *Cast* — **partly done:** to/from signed and unsigned across int, decimal and
   real. **Not done:** the flen/flag each `CAST` *produces* (the whole
   `builtin_cast.go` `getFunction` family), `CAST` to and from `CHAR(n)`,
   `BINARY(n)`, `DECIMAL(p,s)`, `JSON`, and all temporal targets; the
   `inUnion` flag, which Go consults in every `...AsIntSig` and which Rust does
   not model at all.
6. *Math and rounding* — **done** for `ROUND`, `TRUNCATE`, `FLOOR`, `CEIL`
   including their return types. Findings C, E. **Not done:** `ABS`'s
   signedness, `MOD` as a function call (as opposed to the operator), `POW`,
   `EXP`, `LOG` overflow diagnostics, `RAND`'s seeding, `CRC32`, `CONV`.
7. *Temporal arithmetic* — **not started.** `DATE_ADD`/`DATE_SUB` per unit,
   `DATEDIFF`, `TIMESTAMPDIFF`, `EXTRACT`, fractional-second carry: none of it
   was opened.

**Suggested order for the next unit:** (1) the arithmetic flen/decimal rules,
because they are statically comparable; (2) temporal arithmetic, because it is
completely unswept and fraction carry is a classic divergence site;
(3) control-flow type inference.

---

## Unverified

- **Nothing in this document was executed.** Every "Go returns X" is read off
  Go's control flow. The two fixes below were gated only by `cargo check`,
  `cargo clippy` and `cargo fmt`; no test was run, because no freshly built
  binary can start on this machine.
- The existing oracles would not have caught most of this anyway: the
  integration replay compares rejected-vs-accepted and never error text, it
  observes warnings on 28 of 4,906 statements, and `gorun` under-reports integer
  display width. Findings C and E are invisible to all three.
- sql_mode interactions were read, not exercised: `NO_UNSIGNED_SUBTRACTION`
  (`forceToSigned` in `overflowCheck`), and strict vs non-strict for the
  division-by-zero and overflow handlers.
- Go's `inUnion` flag on the cast signatures is unmodelled in Rust and was not
  investigated; it changes `CAST(negative AS UNSIGNED)` to `0` inside a `UNION`.

## Changes made on this branch

- `rust/crates/tidb-expr/src/ops.rs` — finding A's partial fix.
- `rust/crates/tidb-expr/src/string_fn.rs` — finding F's fix.

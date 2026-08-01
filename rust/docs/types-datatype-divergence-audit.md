# `pkg/types` vs `tidb-datatype`: semantic divergence audit

File-by-file semantic comparison of Go `pkg/types` against
`rust/crates/tidb-datatype`. A finding carries a Go `file:line`, a Rust
`file:line`, and a concrete distinguishing input. Areas checked and found
equal are listed too — that inventory tells the next reader where not to look.

**Execution constraint.** Nothing can be run on this machine: `syspolicyd` is
wedged and every freshly created executable hangs at `_dyld_start`. `cargo
check` and `cargo clippy` work; `cargo test`, `gorun`, `goeval` and Go test
binaries do not. **Every finding below is derived by reading source on both
sides and none has been confirmed by execution.** Where a claim depends on
behaviour I could not read out of the source with certainty, it says so.

Standing oracle limit that motivates this audit: the integration replay
compares rejected-vs-accepted only, never error text, and observes warnings on
28 of 4,906 statements. It would not have caught most of what is below.

## Ranking

1. silent wrong VALUE returned
2. error-vs-warning inverted (a failing statement can start succeeding)
3. accept-vs-refuse
4. message or code differences

## Summary

31 findings. Sections: `D*` Datum/conversion/context, `T*` Time/Duration,
`M*` MyDecimal, `F*` FieldType/Set/Enum. Counts and the unaudited list are at
the bottom.

The worst three:

- **F1** — a `utf8mb4_0900_bin` CHAR or VARCHAR column wrote a restored-data
  payload TiDB never writes, so the index and row bytes were mutually
  undecodable between the two engines. Two independent bugs in one boolean.
  **Fixed** in this branch.
- **D1** — `CAST(TIME '11:59:59.999999' AS SIGNED)` returns `115960` instead of
  `120000`: the port rounds the rendered decimal instead of the temporal value,
  so the fractional carry never propagates through the sexagesimal fields. The
  sibling entry point three files over already does this correctly and
  documents the same number.
- **T1/T2** — a duration with trailing garbage or an invalid minute silently
  becomes a *different duration* rather than the parsed value or NULL:
  `'11:22:33abc'` is `11:22:33` in Go and `00:00:11` here; `'10:70:00'` is NULL
  in Go and `00:00:10` here.

Two fixes landed in this branch (commits `4d2b945d4d`, `441fc392c9`); both are
literal transcriptions of a Go predicate, both ship a regression test, and
**neither test has been run**. Everything else is written up rather than
half-implemented.

---

## D1 (rank 1) — `Datum::to_i64` does not round a TIME/DATETIME before rendering it as a number

`CAST(<temporal> AS SIGNED)` in MySQL rounds the *temporal value* to `fsp = 0`
first, so a fractional second carries through the sexagesimal fields. Go does
that with `RoundFrac`. `Datum::to_i64` instead renders the unrounded value as a
decimal and rounds the decimal, which produces a number with `60` in the
seconds field.

- Go: `pkg/types/datum.go:2009-2034` (`toSignedInteger`, `KindMysqlTime` /
  `KindMysqlDuration`). The source comment states the contract outright:
  `// 2011-11-10 11:59:59.999999 -> 20111110120000`.
  Confirmed by the expression layer: `pkg/expression/builtin_cast.go:2013`
  (`builtinCastTimeAsIntSig`: `val.RoundFrac(tc, types.DefaultFsp)` then
  `t.ToNumber().ToInt()`) and `pkg/expression/builtin_cast.go:2163`
  (`builtinCastDurationAsIntSig`, same shape).
- Rust: `rust/crates/tidb-datatype/src/datum/convert.rs:127-128` —
  `Self::Time(value) => decimal_to_i64(value.to_number())`,
  `Self::Duration(value) => decimal_to_i64(value.to_number())`. No
  `round_frac`. `to_number()`
  (`rust/crates/tidb-datatype/src/mysql_time.rs:396`,
  `rust/crates/tidb-datatype/src/duration.rs:194`) emits the fraction, and
  `decimal_to_i64` (`datum/convert.rs:340`) rounds the *decimal* through
  `Decimal::round_to_i64` (`decimal.rs:812`, half away from zero on the digit
  string).

Distinguishing inputs:

| input | Go / MySQL | Rust `Datum::to_i64` |
| --- | --- | --- |
| `CAST(TIME '11:59:59.999999' AS SIGNED)` | `120000` | `115960` |
| `CAST(TIMESTAMP '2011-11-10 11:59:59.999999' AS SIGNED)` | `20111110120000` | `20111110115960` |
| `CAST(TIME '11:11:11.999999' AS SIGNED)` | `111112` | `111112` (agrees — no carry) |

Reachability: `tidb-expr`'s `to_i64_signed`
(`rust/crates/tidb-expr/src/cast.rs:117-127`) handles `Int/UInt/Decimal/Real/
String/Bytes` itself and sends everything else — including `Time` and
`Duration` — to `other.to_i64()`. So this is on the live `CAST(... AS SIGNED)`
path, not a dormant helper.

This is a bug inside `tidb-datatype`, not a missing design: the *other*
conversion entry point already does it right and even documents the same
number. `rust/crates/tidb-datatype/src/datum_convert.rs:216-236`
(`convert_to_signed`) calls `value.round_frac(crate::DEFAULT_FSP, &Utc)` for
`Time` and `value.round_frac(crate::DEFAULT_FSP)` for `Duration`, with the
comment "`11:59:59.999999` becomes 120000, not 115960". The two paths
disagree with each other.

Not fixed here. `Time::round_frac` needs a timezone
(`mysql_time.rs:442`; Go's `Time.RoundFrac` reaches the location through
`GoTime(ctx.Location())`) and `Datum::to_i64()` takes no context, so the fix is
either a context parameter or a documented UTC choice matching what
`convert_to_signed` already does. That is a signature change across callers in
two crates, and with nothing runnable here I will not land it blind.

Note in passing: Go's own signed and unsigned paths are asymmetric —
`convertToUint` (`pkg/types/datum.go:1339-1355`) uses `dec.Round(dec, 0,
ModeHalfUp)` on the rendered number rather than `RoundFrac`, so
`CAST(TIME '11:59:59.999999' AS UNSIGNED)` really is `115960`. Rust's
`convert_to_unsigned` (`datum_convert.rs:283-284`) mirrors that correctly.
`Datum::to_i64` is applying the *unsigned* rule on the signed path.

## D2 (rank 1) — `Datum::to_i64` treats a hex/bit LITERAL like a BIT column value

Go splits the two kinds. `KindMysqlBit` (a stored `BIT(n)` column value)
reinterprets the low 64 bits; `KindBinaryLiteral` (a `0x…` / `b'…'` literal)
goes through the bounded conversion and saturates.

- Go: `pkg/types/datum.go:1982-1988` — `ToInt64` special-cases
  `KindMysqlBit` only (`int64(uintVal)`); every other kind falls to
  `toSignedInteger`, whose `KindBinaryLiteral, KindMysqlBit` arm
  (`pkg/types/datum.go:2055-2061`) applies
  `ConvertUintToInt(val, math.MaxInt64, TypeLonglong)`
  (`pkg/types/convert.go:138-144`), which returns `MaxInt64` plus `ErrOverflow`.
- Rust: `rust/crates/tidb-datatype/src/datum/convert.rs:139-147` — a single
  arm `Self::BinaryLiteral(value) | Self::Bit(value)` doing
  `outcome.value() as i64`, an unconditional wrapping reinterpretation.

Distinguishing input: a `BinaryLiteral` payload of eight `0xFF` bytes
(`0xFFFFFFFFFFFFFFFF`).

- Go `Datum{k: KindBinaryLiteral}.ToInt64` → `9223372036854775807` with
  `ErrOverflow`.
- Rust `Datum::BinaryLiteral(…).to_i64()` → `-1`, no event.
- For `KindMysqlBit` both sides give `-1` with no error, so Rust is correct for
  half the pair and wrong for the other half.

Unverified: whether TiDB's expression layer keeps a hex literal as
`KindBinaryLiteral` all the way to this call, or folds it to a `UInt` datum
first. The Datum-level divergence is unambiguous from the source; the
SQL-level reachability is not something I could establish without running.

## D3 (rank 3) — `Datum::compare` REFUSES a non-UTF-8 string operand where Go compares it

Go's datum comparison operates on raw bytes throughout. The Rust port converts
through `std::str::from_utf8` and propagates the error, so a `binary`- or
`latin1`-charset value that is not valid UTF-8 turns a comparison into a hard
error instead of a comparison.

- Go: `pkg/types/datum.go:836-838` (`compareFloat64`, `KindString, KindBytes`
  → `StrToFloat(ctx, d.GetString(), false)`), and
  `pkg/types/convert.go:703-758` (`getValidFloatPrefix` scans bytes and stops
  at the first non-numeric byte, returning prefix `"0"` plus a truncation
  event). Also `pkg/types/datum.go:998-1000` and `925-927` for the
  time/duration string arms.
- Rust: `rust/crates/tidb-datatype/src/datum/compare.rs:245-247`
  (`numeric_bytes_to_float`: `str_to_float(std::str::from_utf8(bytes)?, …)`),
  `:145` (`compare_string`, `Time` arm), `:256` (`compare_time_bytes`),
  and `:122-123`.

Distinguishing input: `Datum::Bytes(vec![0xFF])` compared against
`Datum::Int(0)`.

- Go: `compareInt64` → default → `compareFloat64` → `StrToFloat` yields `0.0`
  with a truncation event → result `0` (equal), which under a
  warning-disposition context is just a warning.
- Rust: `compare_i64` → `compare_f64` → `numeric_bytes_to_float` →
  `Utf8Error` → `Err(DatumValueError)`; no ordering is produced at all.

Any `VARBINARY`/`BLOB`/`latin1` value holding a byte outside ASCII reaches
this. The same shape applies to `Datum::String` via `as_utf8()?`.

## D4 (rank 3) — `Datum::compare` returns `Err` where Go returns an ORDERING *and* an error

Go's comparison helpers that parse a string return both the comparison result
and the parse error; the parse failure leaves a zero value that the comparison
still uses. Under a warning-disposition context the caller keeps the ordering.
Rust returns `Err` with no ordering, so a lenient caller loses the answer.

- Go: `pkg/types/datum.go:875-877` — `dt, err := ParseDatetime(ctx, s);
  return d.GetMysqlTime().Compare(dt), errors.Trace(err)`. `parseTime`
  (`pkg/types/time.go:2012-2033`) returns `NewTime(ZeroCoreTime, tp,
  DefaultFsp)` alongside the error, so `dt` is the zero datetime. Same pattern
  at `:878-880` (duration), `:998-1000`, `:871-874` (decimal).
- Rust: `rust/crates/tidb-datatype/src/datum/compare.rs:144-154` and
  `:249-260` — `parse_datetime(...).map_err(...)` then `?`.

Distinguishing input: `Time('2011-01-01 00:00:00')` compared with
`String('not a date')`.

- Go: ordering `Greater` (zero datetime sorts first) plus the parse error.
- Rust: `Err(DatumValueError::Comparison(...))`, no ordering.

## D5 (rank 2/3) — `Datum::compare` has no context: flags, timezone and warning sink are all hardcoded

`Datum.Compare` in Go takes a `types.Context` and threads it into every nested
conversion. The Rust `Datum::compare` takes only a `Collation`. Three
consequences, all in one place:

- Rust: `rust/crates/tidb-datatype/src/datum/compare.rs:56` — signature
  `compare(&self, other: &Self, comparer: Collation)`.
- Go: `pkg/types/datum.go:738` — `Compare(ctx Context, ad *Datum, comparer
  collate.Collator)`.

**(a) Zero-date / invalid-date flags are pinned.** `compare.rs:146` and `:257`
call `parse_datetime(text, &chrono_tz::UTC, true, false)` — i.e. always
`allow_zero_in_date = true`, `allow_invalid_date = false`. Go's
`ParseDatetime(ctx, s)` runs `t.Check(ctx)`, which consults
`FlagIgnoreZeroInDateErr` and `FlagIgnoreInvalidDateErr`
(`pkg/types/context.go:150-176`). Distinguishing input: comparing
`Time` against `String('2020-02-31')` under `sql_mode = 'ALLOW_INVALID_DATES'`
— Go accepts and compares, Rust refuses.

**(b) The timezone is pinned to UTC.** Go uses `ctx.Location()`.

**(c) Truncation events are dropped, not warned.** Every nested conversion in
Go passes through `ctx.HandleTruncate`, which appends a warning under
`FlagTruncateAsWarning`. `numeric_bytes_to_float` (`compare.rs:245`) does
`Ok(str_to_float(...).value)` — the `Converted::event` is discarded on the
floor. `compare_f64`'s decimal arm (`:124`) likewise uses the infallible
`to_f64()` where Go's `MyDecimal.ToFloat64()` returns an error.

Consequence: a comparison that MySQL accompanies with `Warning 1292 Truncated
incorrect DOUBLE value: 'abc'` produces no warning at all. This is exactly the
class the integration replay cannot see (28 of 4,906 statements observe
warnings).

## D6 (rank 2) — `Datum::to_decimal` for a float source discards `FromString`'s error

- Go: `pkg/types/datum.go:1941-1944` (`ConvertDatumToDecimal`) — `err =
  dec.FromFloat64(d.GetFloat64())`, and the error is returned
  (`:1966`). `FromFloat64` formats with `'g'`/`-1` and calls `FromString`,
  which reports `ErrOverflow` when the value does not fit `MyDecimal`'s
  65-digit/30-scale envelope.
- Rust: `rust/crates/tidb-datatype/src/datum/convert.rs:220-223` —
  `Decimal::from_signed_literal(&value.to_string())`, and
  `from_signed_literal` (`decimal.rs:136-138`) is
  `Self::parse_mysql(text).0` — it **throws away** `parse_mysql`'s
  `Option<DecimalParseError>`. `event: None` unconditionally.

Distinguishing input: `Datum::Real(1e308).to_decimal()`. Go returns the
saturated decimal *plus* `ErrOverflow`; Rust returns a value with no event, so
a strict-mode statement that MySQL rejects proceeds.

Second, smaller divergence in the same two lines: Go reads a `KindFloat32`
datum through `GetFloat32()` (`pkg/types/datum.go:181-183`), which rounds the
stored `f64` to `f32` and widens it back, because `SetFloat32FromF64`
(`:192-196`) stores a raw `f64`. Rust's `to_decimal` uses the stored value
directly while its own `to_f64` (`datum/convert.rs:169`) *does* apply the
`as f32` round-trip — so the two Rust accessors disagree with each other.
Distinguishing input: a `Float32` datum built from the `f64` `3.1` —
`to_f64()` gives `3.0999999046325684`, `to_decimal()` gives `3.1`; Go gives
`3.0999999046325684` for both.

## D7 (rank 1, now FIXED) — `str_to_int`/`str_to_uint` accepted a bare sign as the function-cast prefix

Go's `getValidIntPrefix` `isFuncCast` arm advances the valid length **only on a
digit**, so `[+-]` at offset 0 is skipped without counting and an operand with
no digit yields prefix `"0"`.

- Go: `pkg/types/convert.go:393-415`.
- Rust (before the fix): `rust/crates/tidb-datatype/src/convert.rs:602-615` —
  a `take_while` that *kept* the sign, so the prefix for `"-"` was `"-"`,
  `"-".parse::<i64>()` failed, and the error arm saturated to `i64::MIN`.

Distinguishing inputs: `str_to_int("-", true)` → Go `0` + truncation warning,
Rust `-9223372036854775808` + an overflow event. `str_to_int("+", true)` → Go
`0`, Rust `i64::MAX`. `str_to_uint("+", true)` → Go `0`, Rust `u64::MAX`.
Same for `"-abc"`, `"+-1"`.

A second bug in the same expression: the truncation test was
`integer.len() != input.len()`, comparing the *substituted* prefix against the
input, so `"-"` (prefix `"0"`, both length 1) reported **no** truncation at all
where Go raises `ErrTruncatedWrongVal("INTEGER", "-")`.

Fixed in this branch (commit `4d2b945d4d`) by extracting
`function_cast_integer_prefix`, which reproduces Go's scan byte for byte and
returns the consumed-all flag separately, plus a regression test
(`function_cast_integer_prefix_needs_a_digit`). **The test has not been run** —
nothing executes here. `cargo check -p tidb-datatype --all-targets` and
`cargo clippy -p tidb-datatype --all-targets` are clean (EXIT=0 each), and
`cargo fmt --all --check` is clean.

Reachability today: `str_to_int(_, true)` has no in-tree caller —
`tidb-expr`'s cast path uses its own `str_int_prefix`
(`rust/crates/tidb-expr/src/cast.rs:244`), which happens to get the lone-sign
case right. So this was a latent trap in a public API rather than a live wrong
answer.

## D8 (rank 4) — error-code precedence inverted on the signed string→int path

When both the string parse and the range clamp have something to report, Go
keeps the **parse** error on the signed path and the **clamp** error on the
unsigned path. Rust uses one helper for both and always keeps the clamp error.

- Go signed: `pkg/types/datum.go:2002-2008` — `if err == nil { err = err2 }`,
  i.e. `StrToInt`'s error wins.
- Go unsigned: `pkg/types/datum.go:1329-1335` — the same idiom with the
  operands swapped, i.e. `ConvertUintToUint`'s error wins.
- Rust: `rust/crates/tidb-datatype/src/datum_convert.rs:1096-1101` —
  `prefer_event(first, second) = second.or(first)`, applied to both
  (`:198-215` signed, `:984-991` unsigned).

Distinguishing input: `Datum::String("999abc")` converted to a signed
`TINYINT`. Go reports `1292 Truncated incorrect INTEGER value: '999abc'`;
Rust reports `1690 TINYINT value is out of range in '999'`. Both saturate to
`127`, and both codes are in `HandleTruncate`'s allowlist so the
error-vs-warning disposition is the same — this is a message/code difference
only.

## D9 (rank 4) — `getValidFloatPrefix`'s NUL-byte error argument

Go truncates the *subject string* at the NUL before formatting the message
(`pkg/types/convert.go:740-742` reassigns `s = s[:validLen]`, and `:755` uses
that `s`). Rust's `valid_float_prefix`
(`rust/crates/tidb-datatype/src/convert.rs:395-398`) tracks the effective
length separately and leaves the caller holding the full input for the message.

Distinguishing input: `"\x0012"`. Go's warning text is `Truncated incorrect
DOUBLE value: ''`; Rust's caller renders the whole operand. Value and
disposition are identical.

---

# Time / Duration (`time.go`, `core_time.go`, `fsp.go`)

Produced by a parallel source-read sub-audit under the same no-execution
constraint. I spot-verified the two rank-1 entries below against the Go source
myself (`T1`'s `matchDuration` tail and `T4`'s `GetFsp` arithmetic); the rest
carry the sub-audit's file:line evidence and have **not** been independently
re-derived.

## T1 (rank 1) — trailing garbage after a duration: Go keeps the parsed clock, Rust re-parses the leading digits

- Go: `pkg/types/time.go:1777-1781` — after a successful match,
  `if err == nil && len(rest) > 0 { return Duration{d, fsp}, false,
  ErrTruncatedWrongVal }`. The parsed value survives; only a warning rides
  along.
- Rust: `rust/crates/tidb-datatype/src/duration.rs:655-659` treats
  `index != input.len()` as `InvalidFormat`, and the fallback at `:692-719`
  re-parses only the leading digit run through the compact `HHMMSS` split.

Distinguishing inputs:

| input | Go | Rust |
| --- | --- | --- |
| `'11:22:33abc'` fsp 0 | `11:22:33` + warning 1292 | `00:00:11` |
| `'12:34:56.7890 xyz'` | NULL (`time.go:1751`, `charsLen >= 12`) | `00:00:12` |

## T2 (rank 1) — invalid minute/second: Go returns NULL, Rust returns a value

- Go: `pkg/types/time.go:1760-1762` — `checkHHMMSS` failure returns
  `ZeroDuration, isNull=true`.
- Rust: `rust/crates/tidb-datatype/src/duration.rs:619-621` returns
  `InvalidFormat`, which falls into the same `:692-719` re-parse.

Distinguishing input: `'10:70:00'` → Go NULL, Rust `00:00:10`.

## T3 (rank 1) — a trailing bare `.` is legal in Go and rejected in Rust

- Go: `pkg/types/time.go:1703-1720` — `matchFrac` uses `parser.Digit(rest, 0)`
  (`pkg/util/parser/parser.go:104`), and zero digits is legal, so frac is 0 and
  there is no error at all.
- Rust: `rust/crates/tidb-datatype/src/duration.rs:624-631` —
  `if start == index { return Err(InvalidFormat) }`.

Distinguishing input: `'11:22:33.'` → Go `11:22:33` with **no** warning, Rust
`00:00:11` with a truncation event.

## T4 (rank 1) — `GetFsp` counts BYTES after the dot, Rust counts digits

- Go: `pkg/types/time.go:569-581` — `fsp = len(s) - index - 1`, capped at 6.
- Rust: `rust/crates/tidb-datatype/src/mysql_time.rs:737-747` —
  `take_while(is_ascii_digit).count()`.

Distinguishing inputs:

| input | Go | Rust |
| --- | --- | --- |
| `ParseDatetime('2020-01-01 10:00:00.5x')` | fsp 4 → `2020-01-01 10:00:00.5000` | fsp 1 → `…10:00:00.5` |
| `'2020-01-01 12:00:00.1+05:00'` | fsp 6 → `2020-01-01 07:00:00.100000` | fsp 1 → `2020-01-01 07:00:00.1` |

Also on the live path via `rust/crates/tidb-expr/src/cast.rs:457`.

## T5 (rank 1) — fractional-overflow carry bypasses the session zone and the calendar check

- Go: `pkg/types/time.go:1186-1193` — `t1, err := tmp.GoTime(ctx.Location())`
  then `FromGoTime(t1.Add(gotime.Second))`; **errors** when `GoTime` fails.
- Rust: `rust/crates/tidb-datatype/src/time_parse.rs:614-616` —
  `core = core.add_duration(1_000_000_000)`; never errors, never consults the
  zone.

Distinguishing inputs:

- tz `America/Los_Angeles`, `'2021-03-14 01:59:59.9999999'` fsp 6 → Go
  `2021-03-14 03:00:00` (the carry crosses the spring-forward gap); Rust
  `2021-03-14 02:00:00`, a wall clock that does not exist in that zone.
- `'2017-00-05 23:59:59.9999999'` fsp 6 → Go `ErrWrongValue`; Rust returns a
  value (a 2016-12 date out of `calc_daynr`).

## T6 (rank 1) — `Duration.RoundFrac` halfway direction is wrong for negatives

- Go: `pkg/types/time.go:1536-1555` rounds through `gotime.Time.Round`, whose
  documented halfway rule is round **up** (toward +∞), not away from zero.
- Rust: `rust/crates/tidb-datatype/src/duration.rs:912-931` —
  `if value >= 0 {(v+half)/unit} else {(v-half)/unit}`, away from zero.

Distinguishing input: `TIME '-00:00:00.0015'` (nanoseconds `-1_500_000`),
fsp 6 → 3 → Go `-00:00:00.001`, Rust `-00:00:00.002`.

## T7 (rank 2) — no `ErrTimestampInDSTTransition` path in the string parser

- Go: `pkg/types/time.go:2012-2034` plus `adjustTimestampErrForDST`
  (`:2036-2052`) — a TIMESTAMP string landing in a DST gap returns the
  **adjusted value** together with `ErrTimestampInDSTTransition`, which callers
  such as `Time.Convert` (`time.go:464-470`) downgrade to a warning.
- Rust: `rust/crates/tidb-datatype/src/time_parse.rs:501-504` —
  `time.validate(...)?` propagates a hard `Err`. The adjustment exists only in
  `mysql_time.rs:355-362` (`convert_kind`), never in the string parser.

Distinguishing input: tz `America/Los_Angeles`,
`ParseTime('2018-03-11 02:00:16', TIMESTAMP, 0)` → Go `2018-03-11 03:00:00` +
warning; Rust `Err(NonexistentLocalTime)`, no value.

## T8 (rank 2) — `ParseTimeFromNum(0)` drops the zero-date error

- Go: `pkg/types/time.go:2083-2098` — for `num == 0`, when
  `!ctx.Flags().IgnoreZeroDateErr()` it returns `ErrTruncatedWrongVal`.
- Rust: `rust/crates/tidb-datatype/src/time_parse.rs:769-774` — unconditional
  `Ok(zero, truncated: false)`; the flag is not even a parameter.

Distinguishing input: numeric literal `0` into a `DATE` column under
`NO_ZERO_DATE` + strict mode → Go errors, Rust stores a silent zero.

## T9 (rank 3) — `str_to_date` hardcodes `allow_zero_in_date = true`

- Go: `pkg/types/time.go:2938-2963` — `t.Check(typeCtx)` consults
  `ctx.Flags().IgnoreZeroInDate()`.
- Rust: `rust/crates/tidb-datatype/src/str_to_date.rs:75` —
  `result.validate(true, allow_invalid_date, timezone)`; `Time::str_to_date`
  (`:53-58`) has no such parameter.

Distinguishing input: `sql_mode='NO_ZERO_IN_DATE'`,
`STR_TO_DATE('2013-05','%Y-%m')` → Go NULL, Rust `2013-05-00`.

## T10 (rank 3) — `ctx[token] = 0` on date exhaustion not ported

- Go: `pkg/types/time.go:3021-3024` records `ctx[token] = 0` when the input
  runs out mid-format; `mysqlTimeFix` (`:2972-2978`) then errors when `%p`
  appears with `%H`, or when `Hour() == 0`.
- Rust: `rust/crates/tidb-datatype/src/str_to_date.rs:124-126` returns early
  with nothing recorded, so `fix_meridiem` (`:338-354`) sees `None`.

Distinguishing inputs: `STR_TO_DATE('11:30:45', '%H:%i:%s %p')` → Go NULL,
Rust `0000-00-00 11:30:45`. `STR_TO_DATE('', '%p')` → Go NULL, Rust
`0000-00-00 00:00:00`.

## T11 (rank 3) — `%.` uses `is_ascii_punctuation` instead of `unicode.IsPunct`

- Go: `pkg/types/time.go:3534-3543` — `skipAllPunct` → `unicode.IsPunct`,
  which **excludes** the ASCII symbols `+ < = > ^ \` | ~ $`.
- Rust: `rust/crates/tidb-datatype/src/str_to_date.rs:235-237` —
  `char::is_ascii_punctuation`, which includes them and excludes Latin-1
  punctuation.

Distinguishing inputs (divergent in both directions):
`STR_TO_DATE('2013+5','%Y%.%c')` → Go NULL, Rust `2013-05-00`.
`STR_TO_DATE('2013¿5','%Y%.%c')` → Go `2013-05-00`, Rust error.

## T12 (rank 3) — float-string path hardcodes `allow_invalid_date = false`

- Go: `pkg/types/time.go:1050` — `ParseDatetimeFromNum(ctx, numOfTime)`, whose
  `t.Check(ctx)` uses the session flags.
- Rust: `rust/crates/tidb-datatype/src/time_parse.rs:561` —
  `parse_time_from_num(number, DateTime, fsp, true, false, timezone)`, literals
  rather than the caller's flags.

Distinguishing input: `sql_mode='ALLOW_INVALID_DATES'`,
`ParseTimeFromFloatString('20200231', DATETIME, 0)` → Go `2020-02-31`, Rust
`Err(InvalidDate)`.

## T13–T16 (rank 4 / structural)

- **T13** `Time::validate` (`mysql_time.rs:646-648`) replaces Go's
  `compareTime(t, MaxDatetime) > 0` (`time.go:2167-2177`) with
  `year > 9999 || month > 12`, so a microsecond in `1_000_000..1_048_575`
  escapes: `from_date_checked(9999,12,31,23,59,59,1_000_000)` → Go
  `ErrWrongValue`, Rust `Ok`.
- **T14** Go's DST-adjusted `Convert` returns `Time{FromGoTime(tAdj)}`
  (`time.go:467`), whose low 4 bits are zero, so the result's type reverts to
  DATETIME and fsp to 0. Rust `convert_kind` (`mysql_time.rs:355-362`) keeps
  `Timestamp` and the fsp. Same calendar value, different type metadata.
- **T15** `ToPackedUint` (`time.go:646-657`) never validates; Rust
  `to_packed_uint` (`mysql_time.rs:669-681` → `packed_time.rs:70-90`) rejects
  `hour > 23` / `year > 9999` / `microsecond > 999_999`, turning an infallible
  Go call into a fallible one.
- **T16 (reachability unverified)** `AdjustedGoTime` (`time.go:191-209`) works
  on the *normalized* `time.Date` result, so a `CoreTime` with hour ≥ 24 (e.g.
  `2020-03-28 26:45` in `Europe/Amsterdam`) normalizes into the DST gap and
  yields `2020-03-29 03:00 CEST`; Rust `adjusted_datetime`
  (`core_time.rs:145-150` → `:298-309`) returns `InvalidCalendar` first. No
  non-synthetic way to build such a `CoreTime` was found.

**Adjacent, out of scope:** `rust/crates/tidb-expr/src/time_fn/calendar.rs:1230`
holds a *second, independent* `STR_TO_DATE` implementation that never calls
`Time::str_to_date`. T9–T11 describe the `tidb-datatype` copy only; the
expression copy needs its own pass.

---

# MyDecimal (`mydecimal.go`)

Produced by a parallel source-read sub-audit under the same no-execution
constraint. I spot-verified M1, M3 and M5 against the Go source myself; the
rest carry the sub-audit's evidence and have not been independently re-derived.

**Framing that matters more than any single finding:** there are **two** Rust
ports of this one Go file.

- `rust/crates/tidb-datatype/src/mydecimal.rs` — a word-buffer, line-for-line
  port carrying `FromString` / `Round` / `Shift` / `ToString` / `FromInt` /
  `FromUint` only.
- `rust/crates/tidb-datatype/src/decimal.rs` — a digit-string reimplementation
  carrying the `*_mysql` methods: arithmetic, `ToBin`/`FromBin`, hashing,
  exponent parsing. **`Datum::Decimal` holds this one.**

Where the two disagree, `mydecimal.rs` is almost always the one matching Go.
Every finding below except M8 and M10 is against `decimal.rs`, i.e. against the
implementation the value path actually uses.

## M1 (rank 1) — `shift_mysql` keeps a rounding carry Go throws away

- Go: `pkg/types/mydecimal.go:599-606` — after the truncating `Round`, Go tests
  the *digit geometry*, `if digitEnd <= digitBegin { *d = zeroMyDecimal;
  return ErrTruncated }`. `digitBegin`/`digitEnd` are the **pre-round** bounds,
  so a carry that lifted the value into a surviving position is discarded.
- Rust: `rust/crates/tidb-datatype/src/decimal.rs:667-670` — tests only
  `if rounded.is_zero()`, so the carried-up value survives.

Distinguishing input: `Decimal::parse_mysql("9e-82")`.

- Go: `Shift(-82)` → `wordsFrac = 10 > 9`, `diff = 1`,
  `Round(d, -1, HalfUp)` turns `9` into `10`, then `digitEnd 8 <= digitBegin 8`
  → **`0`** with `ErrTruncated`.
- Rust: rounds `0.<81 zeros>9` half-up at 81 digits → **`1e-81`** with
  `Truncated`.

Also reproducible at the reduced word limit the fixtures use:
`from_signed_literal("999.123").shift_mysql_with_word_limit(-21, 2)` → Rust
`0.000000000000000001`, Go (`wordBufLen = 2`) `0`.

Control: `mydecimal.rs:632-639` has Go's check and is correct.

## M2 (rank 1) — `parse_mysql` returns `BadNumber`+0 where Go escalates to `ErrOverflow`+max-decimal

- Go: `pkg/types/mydecimal.go:498-510` — on a `strToInt` error Go does **not**
  stop. It zeroes `d`, keeps the clamped exponent (`strToInt` returns
  `MaxInt64`/`MinInt64` *with* `ErrBadNumber`, `pkg/types/helper.go:176-183`),
  and the following `exponent > math.MaxInt32/2` test then rewrites `d` through
  `maxDecimal(81, 0, d)` with `err = ErrOverflow`.
- Rust: `rust/crates/tidb-datatype/src/decimal.rs:227-230` —
  `Some(BadNumber) => return (Self::from_int(0), Some(BadNumber))`, an early
  return that skips both exponent-bound tests.

Distinguishing input: `"1e9223372036854775808"` (19-digit exponent: fits `u64`,
exceeds `i64::MAX`) → Go `999…9` (81 nines) + `ErrOverflow`; Rust `0` +
`BadNumber`. Code-only variant: `"1e-9223372036854775809"` → Go `0` +
`ErrTruncated`, Rust `0` + `BadNumber`.

The existing fixture `("1e18446744073709551620", "0", BadNumber)` passes
because *that* exponent trips `uintCutOff` and `strToInt` returns 0; only
exponents in `(i64::MAX, u64::MAX]` expose the bug. Control:
`mydecimal.rs:859-886` reproduces Go exactly.

## M3 (rank 1) — `from_f64` renders positionally instead of Go's `%g`

- Go: `pkg/types/mydecimal.go:1165-1167` — `FromFloat64` is
  `strconv.FormatFloat(f, 'g', -1, 64)` then `FromString`, so large and small
  magnitudes arrive as **exponent** text and take `FromString`'s `Shift` path.
- Rust: `rust/crates/tidb-datatype/src/decimal.rs:276-283` —
  `value.to_string()`, and Rust's `Display for f64` **never** emits exponent
  form; `convert_scientific_notation` (`convert.rs:260`) is then a no-op. The
  value always takes `FromString`'s digit-count path instead.

Distinguishing inputs:

| input | Go | Rust |
| --- | --- | --- |
| `1e-73` | exact `0.<72 zeros>1` (`Shift` puts all nine words in the fraction) | **`0`** (`words_int 1 + words_frac 9 > 9` → truncate to 72 fraction digits) |
| `1e81` | `999…9` (81 nines) + `ErrOverflow` | **`0`** (82-digit positional text; the overflow branch keeps the **last** 81 digits, all zeros) |

Secondary: `from_f64` returns `Option<Self>` (`None` only for non-finite) and
discards Go's `ErrTruncated`/`ErrOverflow` entirely. This compounds with D6
above, where `Datum::to_decimal` also drops the parse error.

## M4 (rank 2) — `div_mysql` / `rem_mysql` have no truncation/overflow channel

- Go: `pkg/types/mydecimal.go:2281` — `fixWordCntError` inside `doDivMod`,
  plus the mod branch's `ErrOverflow`/`ErrTruncated` at `:2434-2447`.
- Rust: `rust/crates/tidb-datatype/src/decimal.rs:731-759` (`div_mysql`) and
  `:711-726` (`rem_mysql`) return `Option<Decimal>`; `None` only for
  divide-by-zero.

Distinguishing input:
`10000000000000000000.000000000000000000000000000000 /
3.000000000000000000000000000000` (20 integer digits, scale 30 both sides).
Go: `fixWordCntError(3, 8)` → `(3, 6, ErrTruncated)`, `digitsFrac` clamped to
54, **`ErrTruncated` returned**. Rust: `storage_scale = 72`, `Some(value)`,
**no warning**. The value printed at scale 30 is identical, so this is
warning-loss rather than a wrong number.

## M5 (rank 1 in-function, rank 4 in practice) — `ModeCeiling` scans every discarded digit; Go scans one

- Go: `pkg/types/mydecimal.go:901-909` — the non-word-aligned branch
  (`frac % 9 != 0`) computes `digAfterScale`, the **single** digit at position
  `frac`, and increments only on that. Digits beyond it are ignored. Go's own
  source carries `/* TODO - fix this code as it won't work for CEILING mode */`
  immediately above. The word-aligned branch (`:871-881`) *does* scan later
  words — the inconsistency is Go's.
- Rust: `rust/crates/tidb-datatype/src/decimal.rs:1046` —
  `discarded_nonzero = digits[split..].bytes().any(|d| d != b'0')`, i.e. the
  aligned behaviour for all `frac`.

Distinguishing input: `1.0001` at `frac = 1`. Go `Round(&to, 1, ModeCeiling)` →
**`1.0`**; Rust `round_ceiling_to_scale(1)` → **`1.1`**.

Why the `go_round_with_ceil` fixture misses it: every case there has a nonzero
digit exactly at the cut (`15.17`@1, `123456789.987654321`@1), where the
one-digit and all-digits rules agree.

Practical blast radius: `grep -rn ModeCeiling pkg/` finds only
`mydecimal_test.go` and `mydecimal_benchmark_test.go` — no production caller.

## M6 (rank 1 in-function, unreachable via SQL) — add/sub overflow test is result-based

- Go: `pkg/types/mydecimal.go:1909-1926` — `wordsIntTo = max(wordsInt1,
  wordsInt2)`, then `if x > wordMax-1 { wordsIntTo++ }` where `x` is the
  *leading word* of the wider operand. An operand heuristic that over-reports.
- Rust: `rust/crates/tidb-datatype/src/decimal.rs:467-487` derives `words_int`
  from the actual result.

Distinguishing input: `999999999` followed by 72 zeros (81 digits, so
`wordBuf[0] == 999999999`) `+ 1` → Go `ErrOverflow` with the result overwritten
by 81 nines; Rust the exact sum with no warning. Requires 81 integer digits,
above `DECIMAL(65)`, so unreachable through ordinary SQL.

## M7 (rank 3) — `from_bin` discards Go's `binSize` on a corrupt payload

- Go: `pkg/types/mydecimal.go:1532-1534, 1544-1546, 1557-1559, 1568-1570` —
  each corruption check does `*d = zeroMyDecimal; return binSize,
  ErrBadNumber`, so the caller still gets the consumed length and a usable
  zero.
- Rust: `rust/crates/tidb-datatype/src/decimal.rs:1974, 1987, 2001, 2013` —
  `return Err(DecimalCodecError::BadNumber)`; no size, no value.

Distinguishing input: any `{precision: 10, frac: 0}` payload whose first full
word decodes above `999999999`. Go's row decoder can advance the cursor past
the field; the Rust caller cannot.

## M8 (rank 3) — `mydecimal.rs` trims ASCII whitespace where Go trims Unicode whitespace

- Go: `pkg/types/mydecimal.go:527` and `pkg/types/helper.go:134` both use
  `strings.TrimSpace`, whose `unicode.IsSpace` includes vertical tab `\x0b`.
- Rust: `rust/crates/tidb-datatype/src/mydecimal.rs:279-289` —
  `trim_ascii_space` uses `is_ascii_whitespace`, which **excludes** `\x0b`.
  Used at `:225` (exponent) and `:887` (trailing garbage).

Distinguishing inputs: `"1\x0b"` → Go `1` with no error, Rust `1` +
`Truncated`. `"1e\x0b5"` → Go `100000` with no error, Rust `1` + `Truncated`.
`decimal.rs:1347` uses `str::trim` (Unicode) and matches Go.

## M9 / M10 (rank 4)

- **M9** `DecimalMul` overflow loses Go's `-0`. Go assigns `to.negative`
  *before* the `err == ErrOverflow` early return
  (`pkg/types/mydecimal.go:2070-2075`), so `ToString` emits `-0`; Rust
  `decimal.rs:523-525` builds `Decimal::new(false, "0", 0)`. Input
  `(-999…9, 81 digits) * (999…9, 81 digits)`. Unreachable at `DECIMAL(65)`.
- **M10** Error identity for the no-digits case. Go
  (`mydecimal.go:415, 443`) returns `ErrTruncatedWrongVal("DECIMAL", str)`
  (MySQL 1292); `mydecimal.rs:772, 802` collapses it to
  `DecimalError::BadNumber`, which Go uses for a *different* condition, so
  `"abc"` and `"1e18446744073709551620"` become indistinguishable.
  `decimal.rs:154, 184` keeps a distinct `TruncatedWrongValue` and is faithful.

## Decimal areas explicitly left uncertain

Recorded so nobody re-derives them believing they are clean:

- **`mul_mysql` omits Go's `notFixedDec` (31) clamp.** `mydecimal.go:2071` is
  `min(df1+df2, 31)`; `decimal.rs:552-553` is `min(df1+df2, words_frac*9)`.
  For `0.1234567890123456 * 0.1234567890123456` Go's `digitsFrac` is 31 and
  Rust's is 32. `Display`, `to_bin(prec, 30)` and `Round(30)` were all traced
  and converge; **no distinguishing observable input was constructed.** A real
  code-level divergence with unproven consequence — `storage_string()` is the
  likeliest place it would surface.
- **`ToFloat64`'s ≤12-digit fast path.** Go (`mydecimal.go:1188-1208`) sums
  base-1e9 words in `f64` then does `math.Round(f*unit)/unit`; Rust
  `decimal.rs:967-971` always parses the canonical text. Believed equivalent
  (`0.1` checked by hand); a double-rounding case was not exhaustively ruled
  out and no differential sweep could be run.
- **`div_round` (`decimal.rs:783-789`)** documents rounding but truncates via
  `digit_divmod`. The *code* matches Go `DecimalDiv` (which truncates), so this
  is doc drift, not a semantic divergence. Separately `target_scale <
  self.scale` underflows `u32` and panics; no Go counterpart, robustness only.
- **`Decimal::from_literal`** applies no nine-word bound, so
  `MyDecimalWords::from_decimal` can hit its `Overflow` branch and keep the
  **first** 81 integer digits where Go's `FromString` keeps the **last** 81.
  `from_literal` has no Go counterpart, so it is not called a divergence.

---

# FieldType, Set/Enum (`field_type.go`, `field_type_builder.go`, `etc.go`, `set.go`, `enum.go`)

Produced by a parallel source-read sub-audit under the same no-execution
constraint. I independently verified F1 against the Go source (it is the worst
finding in this document and is now fixed); the rest carry the sub-audit's
evidence.

## F1 (rank 1, now FIXED) — `utf8mb4_0900_bin` columns emitted restored data Go never writes

Two independent bugs in one predicate, both on the live storage-encoding path.

- Go: `pkg/types/etc.go:145-155` —
  `useNewCollate && IsNonBinaryStr(ft) && (!collate.IsBinCollation(c) ||
  IsTypeVarchar(tp)) && c != "utf8mb4_0900_bin"`. `IsBinCollation`
  (`pkg/util/collate/collate.go:356-360`) **includes** `utf8mb4_0900_bin`, and
  the trailing guard then overrides the VARCHAR exemption as well.
- Rust (before the fix):
  `rust/crates/tidb-datatype/src/field_type/mod.rs:915-928` — the bin-collation
  set was `Binary | AsciiBin | Latin1Bin | Utf8Bin | Utf8Mb4Bin`, **missing
  `Utf8Mb40900Bin`**, and the trailing guard was absent entirely, so even
  adding the variant would still have left VARCHAR wrong because
  `is_type_varchar()` short-circuits first.

Distinguishing inputs:

| column | Go | Rust (before) |
| --- | --- | --- |
| `CHAR(10) COLLATE utf8mb4_0900_bin` | `false` | `true` |
| `VARCHAR(10) COLLATE utf8mb4_0900_bin` | `false` (trailing guard) | `true` |

Consequence: `rust/crates/tidb-codec/src/rowcodec.rs:610`,
`rust/crates/tidb-tablecodec/src/table_index.rs:876,918,961` and
`table_row.rs:380` all gate restored-data emission on this predicate. Rust
wrote an extra payload TiDB never writes, so the index and row bytes are
mutually undecodable between the two engines for any such column.

Fixed in this branch (commit `441fc392c9`) by transcribing Go's boolean
literally — early return for `Utf8Mb40900Bin`, then the full `IsBinCollation`
membership — plus three rows appended to
`source_need_restored_data_rows`. Note `rust/crates/tidb-datatype/src/collation.rs:301-306`
already had a *correct* `is_bin_collation`; the field-type predicate simply was
not using it. **The test has not been run.** `cargo check` and `cargo clippy` on
`tidb-datatype` are clean (EXIT=0 each), as is `cargo check -p tidb-tablecodec
-p tidb-codec` (EXIT=0), and `cargo fmt --all --check` is clean.

## F2 (rank 1) — `FieldTypeBuilder::new()` starts at flen/decimal `-1`; Go's starts at `0`

- Go: `pkg/types/field_type_builder.go:23-25` — `&FieldTypeBuilder{}` holds a
  **zero-value** `FieldType`, i.e. `flen = 0, decimal = 0`
  (`pkg/parser/types/field_type.go:44-62`). It is Go's *other* constructor,
  `parser.NewFieldType` (`:151-157`), that seeds `-1`.
- Rust: `rust/crates/tidb-datatype/src/field_type/builder.rs:31-35` seeds from
  `FieldType::parser(...)` → `flen = -1, decimal = -1` (`mod.rs:542-557`). The
  doc comment claims "the source zero-value field type"; it is not.

Distinguishing input (live): `newReturnFieldTypeForBaseBuiltinFunc(ETInt)` —
Go (`pkg/expression/builtin.go:153`) sets type, flag and flen and never touches
decimal, leaving `decimal = 0`. Rust
(`rust/crates/tidb-expr/src/builtin_arithmetic.rs:110-114`) leaves
`decimal = -1`, and nothing on the int `plus/minus/mul/intdiv/mod` paths
overwrites it. So `SELECT 1+1` produces a result `FieldType` with `decimal = 0`
in Go and `decimal = -1` in Rust — and that value reaches the protocol
column-definition `decimals` byte.

Second distinguishing input:
`NewFieldTypeBuilder().SetType(mysql.TypeVarchar).BuildP().CompactStr()` → Go
`"varchar(0)"`; the Rust equivalent → `"varchar(5)"` (the `-1` gets substituted
with the default flen).

## F3 (rank 1, reachability uncertain) — `is_binary_string()` answers "binary" for an EMPTY collation name

- Go: `pkg/types/etc.go:125-127` — `IsBinaryStr` compares the collation
  **string** to `"binary"`.
- Rust: `rust/crates/tidb-datatype/src/field_type/mod.rs:898-900` reads a cached
  `Collation` **enum**. `FieldType::parser()` (`mod.rs:542-557`) seeds that enum
  to `Collation::Binary` while `collation_name` is `""`, and
  `From<JsonFieldType>` (`mod.rs:1424-1425`) falls back to `Collation::Binary`
  for any name `Collation::from_name` rejects, including `""`.

Distinguishing input: a `ColumnInfo` whose `FieldType` JSON is
`{"Tp":15,"Charset":"utf8mb4","Collate":""}` (a legacy column). Go:
`IsBinaryStr = false`, `IsNonBinaryStr = true`, `NeedRestoredData = true`.
Rust: `is_binary_string() = true`, `is_character_string() = false`,
`need_restored_data() = false`. Inverted, again on the encoding path.

Left unfixed: whether an empty `Collate` occurs in currently-written meta could
not be confirmed, and the fix is a representation change (keep the name, or
carry an explicit "unrecognised" variant) rather than a boolean edit. The
*unregistered-name* variant of this is not reachable — the 16 collations in the
Rust enum exactly cover TiDB's new-collation set.

## F4 (rank 4, latent) — `source_string()` / `Display` pins the display-width switch to the wrong value

- Go: `pkg/parser/types/field_type.go:541-542` — `String()` calls
  `CompactStr()`, which reads the process global
  `TiDBStrictIntegerDisplayWidth`, set from `DeprecateIntegerDisplayWidth`,
  whose shipped default is `true` (`pkg/config/config.go:1279`, wired at
  `cmd/tidb-server/main.go:1154`).
- Rust: `rust/crates/tidb-datatype/src/field_type/mod.rs:1158-1159` —
  `vec![self.compact_str(false)]`, permanently the non-strict branch, which
  **emits** the width.

Distinguishing input: a `BIGINT` field type with `flen = 22` and `BinaryFlag`.
A real tidb-server prints `bigint BINARY` (confirmed against recorded
`tests/integrationtest/r/**`: `cast(..., bigint BINARY)`); Rust prints
`bigint(22) BINARY`. Latent, not live: `type_desc`/`info_schema_str` callers
correctly pass `STRICT_INTEGER_DISPLAY_WIDTH`
(`rust/crates/tidb-session/src/show.rs:35,165`, `infoschema.rs:852,1000`), and
`source_string` has no production caller.

## F5 / F6 / F7 (rank 4)

- **F5** `default_field_type_for_value` gets `±Inf` flen wrong. Go
  (`pkg/types/field_type.go:273-278`) uses
  `strconv.FormatFloat(x, 'f', -1, 64)`, rendering `+Inf` (4 bytes) / `-Inf`
  (4) / `NaN` (3). Rust
  (`rust/crates/tidb-datatype/src/field_type/value.rs:125-130`) uses bare
  `value.to_string()` → `"inf"` (3). Input `math.Inf(1)` → Go `flen = 4`, Rust
  `flen = 3`. The sibling `parser_default_field_type_for_value`
  (`value.rs:280-285`) routes through `go_fixed_shortest_f64`
  (`value.rs:341-351`) and is correct, so the two twins disagree. Test/bench
  callers only today.
- **F6** `restore_as_cast_type` refuses to emit an empty CHARSET clause Go
  emits. Go `pkg/parser/types/field_type.go:642-645` writes
  `" CHARSET " + ft.charset` whenever the charset is neither `binary` nor
  `utf8mb4` — an empty charset passes. Rust
  (`field_type/mod.rs:1249-1255`) adds `&& !self.charset_name.is_empty()`.
  Input `FieldType{tp: VarString, charset: "", collate: ""}` with
  `explicitCharset = true` → Go `"CHAR CHARSET "`, Rust `"CHAR"`. No parser
  path producing an empty cast charset was found, and Go's own output is
  degenerate.
- **F7** `SetElems(nil)` round-trips as `[]` instead of `null`. Go
  (`pkg/parser/types/field_type.go:303-305, 761-773`) leaves `elems == nil`,
  marshalling to `"Elems":null`; Rust (`field_type/mod.rs:1025-1028`)
  unconditionally sets `elems_present = true`, so `:1407` emits `"Elems":[]`.
  Byte-level meta divergence only — Go's unmarshal accepts both. The
  `elems_present` mechanism is otherwise a correct model of Go's nil-vs-empty
  distinction.

Deliberate and documented, not a finding: `parse_set_value` returns
`TooManyElements` (`enum_set.rs:289-291`) where Go panics with an
index-out-of-range on >64 elements (`pkg/types/set.go:120-125`). Unreachable —
MySQL caps SET at 64.

## Not ported at all

No Rust counterpart found anywhere under `rust/`:
`InferParamTypeFromDatum` / `InferParamTypeFromUnderlyingValue` /
`hasVariantFieldLength` (the modern spelling of `DefaultParamTypeForValue` —
prepared statements bind by text substitution instead,
`rust/crates/tidb-session/src/prepared_statements.rs:218`);
`CheckModifyTypeCompatible`, `needReorgToChange`, `checkTypeChangeSupported`,
`ConvertBetweenCharAndVarchar`, `IsVarcharTooBigFieldLength`; `IsTypeBit(ft)`;
`KindStr`.

---

# Verified-equal inventory

Checked function by function and found semantically equivalent. Listed so the
next reader does not re-derive them.

## Datum, comparison, conversion, context

**`pkg/types/compare.go` → `compare.rs` — the whole file.**
`VecCompareUU/II/UI/IU` and `CompareInt`. The mixed-sign branches are
equivalent by construction: Go's `isUnsigned0 && !isUnsigned1` guard
(`compare.go:96`) and Rust's `left.cmp(&(value as u64))` after the `value < 0`
check (`compare.rs:53-56`) agree on every input including
`(i64::MIN as u64, i64::MAX)`.

**`pkg/types/truncate.go` → `truncate.rs` — the whole file.**
The ten-code allowlist (`ErrTruncatedWrongValue`, `ErrDataTooLong`,
`ErrTruncatedWrongValueForField`, `ErrWarnDataOutOfRange`, `ErrDataOutOfRange`,
`ErrBadNumber`, `ErrWrongValueForType`, `ErrDatetimeFunctionOverflow`,
`WarnDataTruncated`, `ErrIncorrectDatetimeValue`) is identical and in the same
role, and the disposition precedence (ignore beats warn beats return) matches
`truncate.go:45-52`. Note `ErrOverflow` is `ErrDataOutOfRange` and IS in the
allowlist — overflow and truncation share one disposition, which is why D8 is
rank 4 and not rank 2.

**`pkg/types/context.go::Flags` → `conversion_context/flags.rs`.**
All ten bits, same ordinal positions, same accessor semantics.
`DEFAULT_STATEMENT_FLAGS` equals `DefaultStmtFlags`
(`StrictFlags | FlagAllowNegativeToUnsigned | FlagIgnoreZeroDateErr`).

**`pkg/types/convert.go` bound tables → `convert.rs:114-151`.**
`IntegerUnsignedUpperBound` / `IntegerSignedUpperBound` /
`IntegerSignedLowerBound` agree per type, including the `ENUM` 65535 quirk and
`SET`/`BIT` mapping to `MaxUint64`.

**`ConvertFloatToInt` / `ConvertIntToInt` / `ConvertUintToInt` /
`ConvertIntToUint` / `ConvertUintToUint`** (`convert.go:109-166` →
`convert.rs:154-231`). Boundary behaviour verified at the exact-equality case
`val == float64(upperBound)` (returns the bound with **no** error on both
sides) and at `float64(i64::MIN)` (in range on both sides).

**`ConvertFloatToUint`** (`convert.go:169-183` → `convert.rs:234-257`).
Go's `big.Float(...).Uint64()` `acc == big.Below` test and Rust's
`rounded >= u64::MAX as f64` test select the same inputs, including the largest
`f64` strictly below `2^64`. Rust additionally guards `!is_finite()`, where Go
would rely on `big.Float` — a robustness improvement, not a behaviour change
for finite input.

**`convertScientificNotation`** (`convert.go:187-230` → `convert.rs:260-287`).
The four Go branches collapse to Rust's two by construction and produce the
same string on every single-dot input, which is all `ConvertDecimalToUint` can
supply. (Go additionally panics on a no-dot negative exponent such as
`"123e-5"` — `f[point+1:]` slices past the end — but nothing reaches
`convertScientificNotation` with an exponent, so this is dead on both sides.)

**`convertDecimalStrToUint`** (`convert.go:232-270` → `convert.rs:291-327`),
including the `upperBound - round` pre-adjustment and the string-length
comparison that avoids float precision loss.

**`floatStrToIntStr` / `roundIntStr`** (`convert.go:419-552` →
`convert.rs:417-540`), including the `intCnt > 21 || intCnt < 0` early
overflow detection and the `9` → `10` carry-extension in `roundIntStr`.

**`getValidFloatPrefix`** (`convert.go:703-758` → `convert.rs:359-414`) — the
accept/reject decision and the returned prefix match on every case I traced:
`"1.1."`, `"1e1.1"`, `"1e5e"`, `"+.e"`, trailing `e` (Go returns `s[:i]` with
**no** truncation event; Rust does the same at `:388-393`), embedded NUL, and
`"123."`. Only the error's subject string differs (D9).

**`StrToFloat`** (`convert.go:555-574` → `convert.rs:717-730`). The
`ErrRange`→`±MaxFloat64` clamp is equivalent: Go gets `(±Inf, ErrRange)` from
`strconv.ParseFloat` and clamps; Rust gets `Ok(±inf)` from `f64::from_str` and
clamps on `is_infinite()`. Underflow agrees too — I checked Go's `atof64`
slow path sets `ovf` only on overflow, so `"1e-400"` is `(0, nil)` on both
sides, not the error/silence split it first appeared to be.

**`StrToDuration`'s 12-digit datetime sniff** (`convert.go:326-349` →
`convert.rs:748-778`). Go's `len(str) − sign − len(str[dotIdx:])` and Rust's
`unsigned.find('.')` compute the same integer-digit count.

**`NumberToDuration`** (`convert.go:352-381` → `convert.rs:781-827`),
including the asymmetry that only the positive branch attempts the
`>= 10000000000` datetime fallback. (Rust's `number.abs()` at `:786` would
panic on `i64::MIN` in a debug build where Go's explicit
`number < -TimeMaxValue` does not; I could not establish that `i64::MIN`
reaches this function.)

**`ConvertJSONToInt` / `ConvertJSONToFloat` / `ConvertJSONToDecimal`**
(`convert.go:582-700` → `convert.rs:853-1110`): the non-numeric type-code set,
the `JSONLiteralNil` → truncation rule, `JSONLiteralFalse` → 0, everything
else → 1, and the string arm's negative/non-negative split between `StrToInt`
and `StrToUint`.

**`BinaryJSON.IsZero`** (`json_binary.go:185-199`) is
`CompareBinaryJSON(bj, jsonZero) == 0`, and `Datum::to_bool`'s JSON arm
(`datum/convert.rs:90-96`) is the same comparison — including the
counterintuitive results that JSON `null` and JSON `false` are both truthy.

**`Datum.Compare`'s NULL / MinNotNull / MaxValue lattice.** Go reaches the
sentinel answers through the right-hand-kind dispatch
(`datum.go:743-760` plus the `KindNull, KindMinNotNull` / `KindMaxValue` arms
repeated in every helper); Rust hoists them into
`compare_sentinel_order` (`datum/compare.rs:40-49`) before dispatch. I traced
every combination of `{Null, MinNotNull, MaxValue} × {each scalar kind, each
other sentinel}` in both directions and they agree, including the JSON
interaction: JSON `<` NULL in both orders, via Go's leading swap
(`datum.go:739-742`) and Rust's mirror at `compare.rs:57-65`.

**`Datum.Compare`'s int/uint mixed pairs.** `compareInt64` / `compareUint64`
(`datum.go:792-822`) versus `compare_i64` / `compare_u64`
(`datum/compare.rs:93-115`). Go's explicit `d.GetUint64() > math.MaxInt64`
guard and Rust's promotion to a `u64` comparison agree on every pair.

**`GetBinaryLiteral4Cmp` leading-zero stripping.** Go strips at the datum
(`datum.go:308-322`, keeping one byte for all-zero); Rust strips inside
`BinaryLiteral::compare_bytes` / `compare_bytes_of`
(`binary_literal.rs:67-75`) and inside `to_int` (`:114-123`), so the
`compare_f64` arm that Go feeds through `4Cmp` gets the same integer.

**`compareMysqlEnum` / `compareMysqlSet`.** The two Go bodies are identical
(`datum.go:933-944`, `:966-977`); Rust folds them into `compare_named_number`
(`datum/compare.rs:187-201`) with the same name-vs-number split.

**`Datum::to_f64`** for `Time`/`Duration` — Go does **not** round these
(`datum.go:2082-2087`, plain `ToNumber().ToFloat64()`), and neither does Rust
(`datum/convert.rs:174-181`). The missing round in D1 is specific to the
integer path.

**`Datum::to_bool`** for every kind except the JSON arm noted above:
`datum.go:1883-1930` versus `datum/convert.rs:33-104`, including that
`KindFloat32` is tested through `GetFloat64` (raw bits, no `f32` round-trip) on
both sides.

**`Datum::convert_to_signed` / `convert_to_unsigned`** (`datum.go:1314-1382`
→ `datum_convert.rs:185-326`) — the per-kind dispatch, the bound application,
and the `RoundFrac`-on-signed / `Round`-on-unsigned asymmetry described in D1.
Only the event precedence differs (D8).

## Time / Duration

Compared function by function with no distinguishing input found.

- **Splitting and acceptance:** `ParseDateFormat` ↔ `parse_date_format`
  (including the `i < len(format)-1` last-byte rule, consecutive-separator
  consumption, and `"2011-11-11x"` → `["2011","11","11x"]`),
  `isValidSeparator`, `isPunctuation` (`helper.go:122`, byte for byte),
  `isDigit`, `splitDateTime`, `GetFracIndex`, and `GetTimezone` (all four
  `validIdxCombinations` shapes; the `tzMinute != "" && tzSep == ""` no-absorb
  guard maps exactly to `minute.is_some() && !has_colon`).
- **Datetime parsing:** the `noAbsorb` rule; the separator-less length table
  5/6/7/8/9/10/11/12/14 including `hhmmss` only for 11/12/14 and the
  `%2d`-prefix fraction→clock promotion for 5/6/8 and 9/10; `adjustYear`; the
  `len(seps[0]) <= 2 && !isFloat` re-adjust with its all-zero + empty-frac
  exception; `case 2 → error`; `seps[:6]` truncation with warning; multiple
  fractional dots (`"2020-10-10 10:10:10.123.456"` → `.456` on both); leading
  `+`/`-` rejection; embedded `T`.
- **Timezone suffix application:** the
  `deltaHour > 14 || deltaMinute > 59 || (14 && != 0) || (-00:00)` rejection,
  `FixedZone` → session-zone re-projection, and `!hhmmss` → error.
- **fsp:** `CheckFsp`, `ParseFrac` (including the `(tmp+5)/10` half-up and the
  `>= 10^fsp` overflow signal), `alignFrac`, `Time.RoundFrac` (both branches
  including the `t2.Day()-1 > 0` / `clock_micros >= 86400e6` refusal;
  `"2020-01-01 23:59:59.9999999"` agrees at fsp 6 and at fsp 0), and the
  `RoundFrac`/`TruncateFrac` free functions.
- **Duration:** `matchHHMMSSCompact` (`"1122"` → `00:11:22`, `"112"` →
  `00:01:12`), `matchHHMMSSDelimited`, `matchDayHHMMSS` (`"1 11:22"` →
  `35:22:00`, `"11 22"` → `286:00:00`), `hhmmssAddOverflow` carry including the
  `mod[0] = -1` hour non-wrap, the `> TimeMaxHour` saturation to
  `MaxTime`/`MinTime` with `ErrTruncatedWrongVal` (`"839:00:00"` →
  `838:59:59` + warning), `TruncateOverflowMySQLTime`,
  `canFallbackToDateTime` (its punctuation set is a correct enumeration of
  `unicode.IsPunct` over bytes 0–255), the datetime-fallback result
  (`"20121231115959.999"` fsp 0 → `12:00:00`), `splitDuration`,
  `Duration.String`/`ToNumber`, `DurationFormat`.
- **Zero/invalid-date checks:** `checkDateType` / `checkMonthDay` /
  `checkDatetimeType` ↔ `Time::validate` / `validate_clock` — the all-zero
  short-circuit, `!allowZeroInDate && (month==0||day==0)`, `month > 12`, the
  leap-year `maxDay` including Feb-29 under `allowInvalidDate`, and
  hour/minute/second `>= 24/60/60`. Modulo T13.
- **TIMESTAMP epoch range:** `checkTimestampType` (`time.go:2200-2226`) ↔
  `mysql_time.rs:625-634`. **Inclusive at both ends on both sides**:
  `1970-01-01 00:00:01.000000` UTC accepted, `…00:00:00.999999` rejected,
  `2038-01-19 03:14:07.999999` accepted, `03:14:08.000000` rejected.
- **Timezone semantics:** `CoreTime.GoTime` (DST gap → error on both),
  `AdjustedGoTime` (all 14 source DST rows for `Australia/Lord_Howe`,
  `Europe/Vilnius`, `Europe/Amsterdam`), `ConvertTimeZone`, `FromGoTime`
  (the +500 ns rounding is present). The fall-back-ambiguous instant selection
  (`core_time.rs:504-545`) is a correct reimplementation of Go's `time.Date`
  offset-lookup rule, including the Los Angeles / London asymmetry.
- **Core calendar:** `calcDaynr`, `getDateFromDaynr`, `calcDaysInYear`,
  `calcWeekday`, `weekMode`, `calcWeek`, `Week`/`YearWeek`/`YearDay`,
  `isLeapYear`, `GetLastDay`, `compareTime`, `datetimeToUint64`,
  `calcTimeDiffInternal`, `timestampDiff`, `mixDateAndDuration`,
  `getFixDays`/`AddDate`.
- **Codec:** `ToPackedUint`/`FromPackedUint` bit layout
  (`(year*13+month)<<5|day`, `<<17`, `hour<<12|minute<<6|second`,
  `<<24|micro`) modulo T15, and the Go raw `uint64` `fspTt` encoding.
- **`DateFormat` specifiers:** `%b %M %m %c %D %d %e %j %H %k %h %I %l %i %p
  %r %T %S %s %f %U %u %V %v %a %W %w %X %x %Y %y` and the unknown-specifier
  passthrough — including the `%X`/`%x` negative-year `4294967295` output and
  the `%b`/`%M` month-0 error.
- **`StrToDate` specifiers:** the same parser table (`%b %c %d %e %f %h %H %I
  %i %j %k %l %M %m %p %r %s %S %T %Y %y %# %. %@`, everything else
  literal-matched), `parseNDigits`, `%r`/`%T` incremental-update semantics and
  their end-of-line tolerance, the `%j` 3-digit/non-zero rule, month-name
  matching, and `mysqlTimeFix` 12-hour normalization. Modulo T9/T10/T11.
- **Numeric parsing:** `parseDateTimeFromNum` ↔ `normalize_numeric_datetime`
  at every boundary (`< 101`, `<= 691231`, `< 700101`, `<= 991231`,
  `<= 99991231`, `< 101000000`, `<= 691231235959`, `< 700101000000`,
  `<= 991231235959`, `>= 10000101000000`), plus `ParseTimeFromFloat64` and
  `ParseTimeFromDecimal`.
- **Interval parsing:** `ParseDurationValue` / `parseSingleTimeValue` /
  `parseTimeValue` / `ExtractDurationValue`; `IsClockUnit` / `IsDateUnit` /
  `IsMicrosecondUnit` / `IsDateFormat`; `ExtractDatetimeNum` /
  `ExtractDurationNum`.

## MyDecimal

**`mydecimal.rs` ↔ Go — this port is in very good shape.**
`MyDecimal` struct layout and `MyDecimalStructSize` (40); `digitsToWords`
(including the negative-argument truncation that makes the `div9` table and the
plain formula agree); `countLeadingZeroes`; `countTrailingZeroes`;
`fixWordCntError`; `add`; `maxDecimal`; `removeLeadingZeros`; `ToString`
(including the `fill` / leading-`0` / `digitsInt == 0` handling); `FromInt`
(including `i64::MIN`); `FromUint`; `digitBounds`; `doMiniLeftShift`;
`doMiniRightShift`; `Shift` (the `wordsFrac < lack` ⟺ `wordsInt > wordBufLen`
overflow condition, mini-shift alignment, word move, gap fill); `Round` (all
three modes, negative `frac`, the `frac == wordsFracTo*9` aligned branch, the
carry-and-shift path including `ErrOverflow`, the zero-with-proper-scale path,
and `mod9[to.digitsInt]` ≡ `digits_int % 9` over the whole `int8` domain);
`FromString` (sign, all-space input, `endIdx+1 <= len(str)` ≡
`end_idx < len`, `fixWordCntError` disposition, backward-integer /
forward-left-aligned-fraction word packing, the `MaxInt32/2` and `MinInt32/2`
exponent thresholds, `allZero → negative=false`, `resultFrac = digitsFrac`);
`strToInt` (`uintCutOff`/`intCutOff`/`hasNum`, clamped value with
`ErrBadNumber`). Except M8's whitespace point.

**`decimal.rs` ↔ Go:** `fixWordCntError`; `digitsToWords`; `DecimalBinSize`
(including the negative-`xInt`/`xFrac` `ErrBadNumber` path); `readWord`;
`writeWord`; `countLeadingZeroes`; `removeLeadingZeros`;
**`WriteBin`/`ToBin`** (the `digitsIntFrom+fracSizeFrom == 0` mask reset, the
`intSize` mask fill, the three-way `fracSize` comparison and its truncation
sub-test, the trailing-partial-word `lim`/`dig2bytes` walk, the tail fill
bounded by `originIntSize+originFracSize`, the final `bin[0] ^= 0x80`);
**`FromBin`** (mask derivation, `binSize > 40` rejection, short-payload zero
fill, both `fixWordCntError` branches, the `powers10[leadingDigits+1]` and
`> wordMax` validations, leading-zero-word `digitsInt` decrements, trailing
partial-word scaling) except M7's return shape; `ToHashKey`/`HashKeySize`
(precision from stripped leading/trailing zeros, `prec == 0 → 1`,
`ErrTruncated` suppression, appended `digitsFrac` byte); `PrecisionAndFrac`;
**`ToInt`** (verified at `±i64::MAX`, `±i64::MIN`, `i64::MIN-1`, `2^64`; the
`x == MinInt64` positive trap; Overflow-before-Truncated precedence);
**`ToUint`** (negative → Overflow, `MaxUint64` saturation, Truncated);
`Round`/`ModeHalfUp` and `ModeTruncate` as `round_to_scale`/`truncate_to_scale`
(verified on `ROUND(12345,-2)`, `ROUND(12365,-2)`, `ROUND(12345,-5)`,
`ROUND(92345,-5)`, `ROUND(600,-3)`, `ROUND(6000,-4)`, and Go's
`digitsInt+frac < 0` early-zero falling out of the digit math without a special
case); `Shift`'s overflow condition and bound-stripping (everything but M1);
`DecimalMul`'s word loop (`add2`/`add` carry chain, both `idxTo < 0` overflow
returns, the `-0.000` check, leading-zero-word compaction); `DecimalMod` value
and sign; `DecimalDiv`'s fraction-word budget
(`digitsToWords(frac1+frac2+fracIncr)*9` ≡
`word_scale(frac1+frac2+adjusted_increment)`, verified on `1e-15 / 1e9` down to
the exact quotient word); `maxDecimal`/`NewMaxOrMinDec` including
`precision == 0`; `MarshalJSON`/`UnmarshalJSON` object shape; `Compare`/`Ord`.

## FieldType, Set/Enum

- **`fieldTypeMergeRules`: all 841 cells compared programmatically — 0
  differences.** The Go table was parsed out of `field_type.go:443-1459` and the
  `MERGE_RULES` literal out of `aggregate.rs:10-127`, then diffed cell by cell.
  That covers DECIMAL×DOUBLE→DOUBLE, every temporal×string pair, the whole
  ENUM/SET/BIT/JSON/NEWDATE/DATETIME/VECTOR rows and columns, and the
  blob-widening rows.
- `getFieldTypeIndex` / `type_index`, including Go's map-miss-returns-0
  behaviour for unregistered type bytes.
- `mergeTypeFlag` (the `uint`-vs-`u32` mask width is immaterial for the two
  bits involved).
- `AggFieldType`: accumulator seeding, the order of `isMixedSign` versus
  `SetType`/`SetFlag`, the `bumpRange` predicate including the `TypeBit`
  disjunct, the full promotion ladder
  Tiny→Short→Int24→Long→LongLong→NewDecimal with Year falling through, and the
  trailing no-op unsigned re-add.
- `AggregateEvalType` + `mergeEvalType`: the `TypeNull` skip that also skips
  the `lft` update, `gotFirst`, `gotBinString`, and both output flags via
  `SetTypeFlag`. Plus `SetTypeFlag` itself.
- `defaultLengthAndDecimal` (all 24 entries + the `(-1,-1)` miss) and
  `defaultLengthAndDecimalForCast` (all 9 entries + miss).
- `NewFieldType` / `DefaultCharsetForType` / `minFlenAndDecimalForType`,
  including Year being in the integer set for the flen default and
  `utf8mb4`/`utf8mb4_bin` for exactly {VarString, String, Varchar}.
- `EvalType()` per MySQL type including the `EnumSetAsIntFlag` case; and the
  whole `EvalType` enum — discriminants, `IsStringKind` (VectorFloat32
  included), `IsVectorKind`, and `String()` (`ETDuration` → `"Time"`).
- `CompactStr`: enum/set rendering, temporal `(M)`, float/double `(M,D)` gated
  on `isDecimalNotDefault`, decimal's unconditional `(M,D)`,
  bit/varchar/char/var_string `(M)`, the tinyint display-width exception
  (`zerofill || flen==1`), the other integers' zerofill gate, year's raw
  unsubstituted flen, vector's `!= -1` gate, and null's `"(0)"`.
- `InfoSchemaStr` and `ColumnInfo.GetTypeDesc` (`unsigned` excluded for
  BIT/YEAR; `zerofill` excluded for YEAR; `zerofill` on `type_desc` but not on
  `info_schema_str`).
- `Restore()`: per-type precision/scale selection, the UNSIGNED / ZEROFILL /
  BINARY clauses and their conditions, CHARACTER SET uppercased versus COLLATE
  not uppercased, and the `''` escaping for enum/set elements.
- `TypeToStr` / `StrToType` — all 28 map entries plus the text→blob /
  char→binary aliasing and the `var_string`-is-not-aliased subtlety.
- `format.OutputFormat` escape table (`\0`, `''`, `\n`, `\r`).
- `StorageLength` including `dig2bytes`, `digitsPerWord=9`, `wordSize=4`.
- `Equal` / `PartialEqual` including the NotNull special case and the
  varchar↔var_string equivalence.
- `HasCharset`, `IsVarLengthType`, and the whole
  `IsTypeBlob/Char/Varchar/Unspecified/Prefixable/Fractionable/Time/Float/
  Integer/StoredAsInteger/Numeric/Temporal/IsString` family.
- `SetFlenUnderLimit` / `SetDecimalUnderLimit` /
  `UpdateFlenAndDecimalUnderLimit` (the Rust rewrite is algebraically identical
  to Go's `deltaFlen += MaxDecimalScale` form).
- `MarshalJSON` field names and the use of the **raw** `tp` rather than
  `GetType()` for ARRAY types.
- `DefaultTypeForValue`, every arm except F5's `±Inf` detail — including three
  easy-to-get-wrong quirks the port got right: the `*MyDecimal` double
  `SetFlenUnderLimit` being equivalent to `min(L+1, 65)`; `Duration`'s flen
  being **overwritten** with `fsp+1` rather than incremented; and
  `BinaryLiteral`'s add-BinaryFlag-then-delete-it dance. Also HexLiteral
  `len*3`, BitLiteral `len*3`, Enum/Set `len(Name)`, JSON's
  `utf8mb4`/`utf8mb4_bin`, and the default arm. The `test_driver` twin
  (`parser_default_field_type_for_value`) likewise: BitLiteral `len(x)`,
  BinaryLiteral→`TypeBit` with `len*8`, unclamped decimal, no NotNull flag.
- `StrLenOfInt64Fast` / `StrLenOfUint64Fast` including `0` → 1 and `i64::MIN`
  → 20.
- `ParseEnum` / `ParseEnumName` / `ParseEnumValue`: exact message text, the
  `ErrTruncated` (1265) identity, the `number == 0 || number > len` boundary,
  and the collator-based name compare.
- `ParseSet` / `ParseSetName` / `ParseSetValue`: the **empty-string → zeroSet**
  special case, dedup-by-collator-key via `marked`, the leftover-names error,
  `1 << i` for `i >= 64` yielding 0 (Go's shift semantics reproduced by
  `checked_shl().unwrap_or(0)`), the leftover-bits error, and the fact that SET
  errors are plain errors while ENUM errors are `ErrTruncated`.
- `parse_go_uint64_base_zero` versus `strconv.ParseUint(s, 0, 64)`: base
  prefixes (`0b`/`0o`/`0x`/leading-`0`), the `"0"` and `"0x"`/`"0b"` edge
  cases, `underscoreOK`, sign rejection, and overflow rejection.
- `enum_set_display_length` (ENUM = max element length; SET = sum + n−1)
  versus `pkg/parser/ddl_fieldtype_parser.go:577-597`, including n=0 and n=1.
- `STRICT_INTEGER_DISPLAY_WIDTH = true` matches the shipped
  `deprecate-integer-display-length` default.
- `TryToFixFlenOfDatetime` as reimplemented at
  `rust/crates/tidb-executor/src/driver/set_opr.rs:172-178`.

---

# Coverage, counts, and what is unverified

**Counts.** 31 findings across four surfaces:

| surface | rank 1 | rank 2 | rank 3 | rank 4 | total |
| --- | --- | --- | --- | --- | --- |
| Datum / conversion / context (D) | 3 | 1 | 2 | 3 | 9 |
| Time / Duration (T) | 6 | 2 | 4 | 4 | 16 |
| MyDecimal (M) | 4 | 1 | 2 | 3 | 10 |
| FieldType / Set / Enum (F) | 3 | 0 | 0 | 4 | 7 |

(Findings that are rank 1 in-function but unreachable through SQL — M6, M9 —
are counted at their in-function rank and flagged in place. D7 and F1 are
counted although they are fixed in this branch.)

**Audited.** `compare.go`, `convert.go`, `truncate.go`, `context.go`,
`etc.go`; the `Datum.Compare` family and the
`ToBool`/`ToInt64`/`ToFloat64`/`ToDecimal` family plus the integer arms of
`ConvertTo` in `datum.go`; the whole of `mydecimal.go`; `time.go`,
`core_time.go`, `fsp.go`; `field_type.go`, `field_type_builder.go`, `set.go`,
`enum.go`.

**Not audited.** The `json_*.go` family (`json_binary.go`,
`json_binary_functions.go`, `json_path_expr.go`, `json_constants.go`),
`binary_literal.go` beyond the arms the Datum comparison reaches,
`vector.go`/`vector_functions.go`, `overflow.go`, `helper.go`,
`explain_format.go`, `field_name.go`, and the whole string/charset/collation
side (`charset.rs`, `collation.rs`, the encoding tables). Treat their absence
from the findings list as "not yet audited", **not** "clean".

**What is unverified because nothing can execute here.** Everything. No test
was run, no binary was executed, no differential sweep was performed. The two
fixes in this branch (`4d2b945d4d`, `441fc392c9`) each ship a regression test
that has **never been run**; they compile and lint clean and nothing more.
Concretely, the following remain open questions that one execution each would
settle:

- Whether `Datum::BinaryLiteral` (D2) ever reaches `to_i64` from real SQL, or
  is folded to `UInt` upstream.
- Whether an empty `Collate` string (F3) occurs in currently-written column
  meta.
- Whether the `mul_mysql` `notFixedDec` clamp (M-uncertain) has any observable
  consequence at all — three renderings were traced and all converged.
- Whether `ToFloat64`'s ≤12-digit fast path (M-uncertain) has a
  double-rounding case.
- Whether an hour-≥-24 `CoreTime` (T16) can be built non-synthetically.

**Structural observations for whoever picks these up.**

1. The Rust tree carries **two** decimal implementations —
   `mydecimal.rs` (a faithful word-buffer port) and `decimal.rs` (a
   digit-string reimplementation). `Datum::Decimal` holds the latter, and every
   decimal finding except M8 and M10 is against the latter. Any claim that
   "`MyDecimal` is ported" has to say which of the two the value path uses.
2. The tree also carries **two** `STR_TO_DATE` implementations —
   `tidb-datatype/src/str_to_date.rs` and
   `tidb-expr/src/time_fn/calendar.rs:1230`, which never calls the first.
   T9–T11 describe only the former.
3. Several divergences are the same shape: a Rust entry point drops the Go
   `Context` and hardcodes what it carried. D5 (comparison), T9, T12
   (`allow_zero_in_date` / `allow_invalid_date`), and D1's blocker
   (`Time::round_frac` needs a timezone) are all instances. A single decision
   about how `tidb-datatype` threads conversion context would close a cluster,
   not one bug.

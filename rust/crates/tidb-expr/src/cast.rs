// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

//! `CAST(expr AS type)` / `CONVERT(expr, type)` evaluation
//! ([`eval_cast`], dispatched from `crate::eval_in`'s `Expr::Cast` arm) and
//! `CONVERT(expr USING charset)` (a plain stringification passthrough,
//! handled directly in `crate::eval_in`'s own `Expr::ConvertUsing` arm —
//! this crate has no charset domain at all).
//!
//! JSON targets retain their native datum domain. DATE/DATETIME keep the
//! evaluator's established string result boundary; the lockdown receipt
//! records the native temporal integration gap explicitly. Every rule here
//! (string-to-number prefix
//! parsing width, rounding tie-breaking per source type, `UNSIGNED`'s
//! negative-float-clamps-to-zero rule, `DECIMAL`'s precision clamp,
//! `BINARY`'s NUL-padding) was confirmed via `goeval`, not assumed — see
//! each function's own doc for the specific probe.

use crate::coerce::coerce_str;
use crate::time_fn::calendar::{format_ymd_result, parse_date_ymd};
use crate::Decimal;
use crate::{Datum, EvalError};
use tidb_ast::CastType;

/// Evaluates a [`CastType`] against an already-evaluated, non-`NULL`
/// operand (`NULL` is handled by the caller — every target type maps
/// `NULL` to `NULL`, so there's no per-type NULL case to write here).
///
/// `source` is the operand's static `FieldType` where the caller knows it, and
/// `None` where it does not. Go picks the cast SIGNATURE from that type
/// (`builtinCastIntAsTimeSig` vs `builtinCastStringAsTimeSig` vs ...), and the
/// datum kind is only a proxy for it — a proxy with exactly one hole, `YEAR`,
/// whose values are ordinary `Datum::Int`s that Go nonetheless converts by a
/// rule of their own. See [`cast_to_time`].
pub(crate) fn eval_cast(
    cast_type: &CastType,
    v: Datum,
    source: Option<&tidb_datatype::FieldType>,
    ctx: &dyn crate::Columns,
) -> Result<Datum, EvalError> {
    if v.is_range_sentinel() {
        return Err(EvalError::Unsupported("range sentinel cast operand"));
    }
    if matches!(v, Datum::VectorFloat32(_))
        && !matches!(cast_type, CastType::Char { .. } | CastType::Binary { .. })
    {
        return Err(EvalError::Unsupported(
            "a vector can only be cast to string or vector",
        ));
    }
    match cast_type {
        CastType::Signed => {
            report_int_truncation(&v, ctx)?;
            report_signed_overflow(&v, ctx);
            Ok(Datum::Int(to_i64_signed_in(&v, &ctx.time_zone())))
        }
        CastType::Unsigned => {
            report_int_truncation(&v, ctx)?;
            Ok(Datum::UInt(to_u64_unsigned(&v, ctx)))
        }
        CastType::Char { len, .. } => {
            let text = string_source_text(&v, source)?;
            Ok(Datum::new_string(match len {
                Some(n) => {
                    report_data_too_long(ctx, text.chars().count(), *n as usize);
                    text.chars().take(*n as usize).collect()
                }
                None => text,
            }))
        }
        CastType::Binary { len } => {
            // Go's binary cast is byte-oriented and preserves arbitrary
            // octets.  Do not route an already-byte-valued operand through
            // UTF-8 decoding: `CAST('你好world' AS BINARY(5))` deliberately
            // keeps the first five bytes, even though that suffix is not a
            // complete UTF-8 sequence (see `TestCastFunctions`).
            // The YEAR-zero rendering is the SAME signature's, because a
            // BINARY target only changes `b.tp`'s charset: Go still picks
            // `builtinCastIntAsStringSig` from the ARGUMENT's ETInt eval type
            // and only then pads. Captured: `hex(cast(y as binary))` over a
            // zero YEAR is `30303030`, i.e. `"0000"`.
            let bytes = match year_zero_string(&v, source) {
                Some(text) => text.into_bytes(),
                None => datum_binary_bytes(&v)?,
            };
            Ok(Datum::new_bytes(match len {
                Some(n) => {
                    // A binary target measures in BYTES, not characters:
                    // Go's `chs == CharsetBin` arm sets
                    // `characterLen = len(s)`.
                    report_data_too_long(ctx, bytes.len(), *n as usize);
                    // Go `padZeroForBinaryType` (`builtin_cast.go:2249`)
                    // refuses to BUILD a pad wider than `max_allowed_packet`,
                    // answering NULL with the 1301 warning instead. The test
                    // is on the declared width, before any allocation, and
                    // that ordering is the whole point: `cast("a" as
                    // binary(4294967295))` (`expression/issues`) otherwise
                    // materializes four gigabytes of zeros -- 109 SECONDS of
                    // the topic's 125, for a statement TiDB rejects outright.
                    if bytes.len() < *n as usize && *n as u64 > ctx.max_allowed_packet() {
                        ctx.handle_allowed_packet_overflowed("cast_as_binary")?;
                        return Ok(Datum::Null);
                    }
                    binary_pad_truncate(&bytes, *n as usize)
                }
                None => bytes,
            }))
        }
        CastType::Decimal { flen, scale } => {
            let source = to_decimal_for_cast(&v);
            let produced = source.cast_to_precision(*flen, *scale);
            report_decimal_production(ctx, &source, &produced, *flen, *scale);
            Ok(Datum::Decimal(produced))
        }
        CastType::Date => cast_to_time(&v, source, ctx, tidb_datatype::TimeType::Date, 0),
        CastType::DateTime { fsp } => cast_to_time(
            &v,
            source,
            ctx,
            tidb_datatype::TimeType::DateTime,
            i64::from(fsp.unwrap_or(0)),
        ),
        CastType::Year => cast_to_year(&v),
        CastType::Double | CastType::Float => Ok(Datum::Real(to_f64_for_cast(&v))),
        CastType::Time { .. } => Err(EvalError::Unsupported("CAST AS TIME")),
        CastType::Json => crate::builtin_ext::cast_as_json(&v),
    }
}

/// Go `types.ProduceStrWithSpecifiedTp` (`pkg/types/datum.go:1289-1304`),
/// warning half: a value the target width cannot hold raises
/// `ErrDataTooLong` (1406) "Data Too Long, field len %d, data len %d".
///
/// `data_len` is what Go's `characterLen` counts, which is the SOURCE's own
/// length in the target's unit -- runes for a character target, bytes for a
/// binary one -- NOT the truncated result's. Captured:
/// `CAST('中文abc' AS CHAR(2))` warns `field len 2, data len 5` while
/// `CAST('中文abc' AS BINARY(4))` warns `field len 4, data len 9`.
///
/// Go's one exception, the whitespace-only overflow that downgrades to a
/// 1265 `Data truncated`, needs `tp.GetType() == TypeVarchar`; a CAST target
/// is `TypeVarString`, so it cannot apply here. Captured:
/// `CAST('ab   ' AS CHAR(2))` warns 1406, not 1265.
fn report_data_too_long(ctx: &dyn crate::Columns, data_len: usize, field_len: usize) {
    if data_len > field_len {
        ctx.append_warning(
            1406,
            &format!("Data Too Long, field len {field_len}, data len {data_len}"),
        );
    }
}

/// Go `types.ProduceDecWithSpecifiedTp` (`pkg/types/datum.go:1629-1666`),
/// warning half. Two mutually exclusive events, in Go's own `else if` order:
///
///  * the rounded value no longer fits `flen - scale` integer digits, so it
///    is clamped to the max/min decimal and `ErrOverflow` (1690) reports
///    `DECIMAL value is out of range in '(flen, scale)'`;
///  * otherwise, if rounding to `scale` CHANGED the value, `ErrTruncatedWrongVal`
///    (1292) reports `Truncated incorrect DECIMAL value: '<original>'` -- with
///    the ORIGINAL text, not the rounded one.
///
/// The overflow arm suppresses the truncation arm even when both are true.
/// Captured: `CAST(1234.56 AS DECIMAL(4,1))`, which both overflows AND loses
/// a digit to rounding, warns 1690 ONLY -- that case is what makes the `else`
/// load-bearing, and turning it into a second `if` is the mutation the corpus
/// now catches. `CAST(123.456 AS DECIMAL(10,2))` warns 1292 with `'123.456'`.
///
/// Go's guard is `flen != UnspecifiedLength && decimal != UnspecifiedLength`;
/// [`tidb_ast::CastType::Decimal`] carries `flen == 0` for unspecified, which
/// is the same gate [`Decimal::cast_to_precision`] uses to skip the clamp.
fn report_decimal_production(
    ctx: &dyn crate::Columns,
    source: &Decimal,
    produced: &Decimal,
    flen: u32,
    scale: u32,
) {
    if flen == 0 {
        return;
    }
    let rounded = source.round_to_scale(scale as i32);
    let int_digits = rounded.coefficient_digits().len() as u32 - rounded.storage_scale();
    if int_digits > flen.saturating_sub(scale) {
        ctx.append_warning(
            1690,
            &format!("DECIMAL value is out of range in '({flen}, {scale})'"),
        );
    } else if source.storage_scale() > scale && produced != source {
        ctx.append_warning(
            1292,
            &format!("Truncated incorrect DECIMAL value: '{source}'"),
        );
    }
}

fn datum_sql_string(value: &Datum) -> Result<String, EvalError> {
    value
        .sql_string()
        .map_err(|_| EvalError::Unsupported("invalid UTF-8 string coercion"))
}

/// Go `builtinCastIntAsStringSig.evalString`'s last rendering rule
/// (`pkg/expression/builtin_cast.go:1098`):
///
/// ```go
/// if tp.GetType() == mysql.TypeYear && res == "0" {
///     res = "0000"
/// }
/// ```
///
/// `tp` is `b.args[0].GetType(ctx)` -- the SOURCE's static type, not the
/// datum's. A zero YEAR is a `Datum::Int(0)` indistinguishable from a
/// `BIGINT` zero, and the two render differently: `CAST(y AS CHAR)` is
/// `'0000'` where `CAST(i AS CHAR)` is `'0'`. Captured over
/// `t(y year, i int)` holding `(0, 0)`:
///
/// ```text
/// select cast(y as char), length(cast(y as char)), cast(i as char) from t;
/// 0000    4    0
/// ```
///
/// `Some` only for that one value: every other YEAR is already its own four
/// digits (the domain is `0` and `1901..=2155`), which is why Go tests the
/// RENDERED text rather than the integer.
fn year_zero_string(value: &Datum, source: Option<&tidb_datatype::FieldType>) -> Option<String> {
    if source.map(tidb_datatype::FieldType::code) != Some(tidb_datatype::FieldTypeCode::Year) {
        return None;
    }
    matches!(value, Datum::Int(0) | Datum::UInt(0)).then(|| "0000".to_owned())
}

/// [`year_zero_string`] over the ordinary string rendering.
fn string_source_text(
    value: &Datum,
    source: Option<&tidb_datatype::FieldType>,
) -> Result<String, EvalError> {
    match year_zero_string(value, source) {
        Some(text) => Ok(text),
        None => datum_sql_string(value),
    }
}

/// Returns the byte payload used by Go's `builtinCast*AsStringSig` binary
/// target.  String/bytes datums already carry the source bytes; only numeric
/// values need SQL stringification first.
fn datum_binary_bytes(value: &Datum) -> Result<Vec<u8>, EvalError> {
    match value {
        Datum::String(value) => Ok(value.bytes().to_vec()),
        Datum::Bytes(value) => Ok(value.clone()),
        _ => Ok(datum_sql_string(value)?.into_bytes()),
    }
}

/// `SIGNED`'s own coercion: `Int` is unchanged; `Decimal`/`Float` round to
/// the nearest integer (ties away from zero for `Decimal`, ties to EVEN
/// for `Float` — a real asymmetry, matching the `~` bitwise operator's own
/// established rule, confirmed via `goeval`: `CAST(2.5e0 AS SIGNED)` is
/// `2`, `CAST(1.5 AS SIGNED)` — a `DECIMAL` literal — is also `2`, but
/// `CAST(0.5e0 AS SIGNED)` is `0`, the even neighbor); either CLAMPS
/// (never errors) on overflow past `i64`, confirmed via `goeval`:
/// `CAST(1e300 AS SIGNED)` is `9223372036854775807`. `Str` parses a
/// leading `[+-]?digits` prefix ONLY (no `.`, no exponent — confirmed via
/// `goeval`: `CAST('3.5abc' AS SIGNED)` sees just `3`, `CAST('.5' AS
/// SIGNED)` sees no digits at all), defaulting to `0` if no digit is
/// found, and ALSO saturates past `i64` range rather than replicating
/// real TiDB's own exotic bit-reinterpretation overflow behavior there (a
/// KNOWN, deliberately excluded divergence for a value nobody writes
/// intentionally — see [`str_int_prefix`]'s own doc).
pub(crate) fn to_i64_signed(v: &Datum) -> i64 {
    to_i64_signed_in(v, &tidb_datatype::SessionTimeZone::utc())
}

/// [`to_i64_signed`] with the session's `time_zone`, which Go's
/// `toSignedInteger` hands to `Time.RoundFrac` -- load-bearing only when a
/// DATETIME's fractional carry lands on a DST transition instant.
pub(crate) fn to_i64_signed_in(v: &Datum, zone: &tidb_datatype::SessionTimeZone) -> i64 {
    match v {
        Datum::Int(i) => *i,
        Datum::UInt(i) => *i as i64,
        Datum::Decimal(d) => d.round_to_i64_saturating(),
        Datum::Real(f) => f.round_ties_even() as i64,
        Datum::String(s) => s.as_utf8().map(str_int_prefix).unwrap_or(0),
        Datum::Bytes(s) => std::str::from_utf8(s).map(str_int_prefix).unwrap_or(0),
        Datum::Null | Datum::MinNotNull | Datum::MaxValue => unreachable!("guarded by caller"),
        other => other.to_i64_in(zone).map_or(0, |converted| converted.value),
    }
}

/// `UNSIGNED`'s own coercion. Integer and integer-string sources preserve
/// the low 64 bits, so `CAST(-5 AS UNSIGNED)` is the genuine
/// `18446744073709551611` UInt64 value.
///
/// The DECIMAL and the FLOAT source do NOT agree about a negative value, and
/// that disagreement is Go's, not an inconsistency to smooth over:
///
///  * `MyDecimal.ToUint` (`ConvertDecimalToUint`) returns 0 for a negative
///    value, plus a truncation event. Captured: `cast(-1.5 as unsigned)` is 0
///    with `1292 Truncated incorrect DECIMAL value: '-1.5'`.
///  * `ConvertFloatToUint` (`pkg/types/convert.go:169-183`) rounds first, and
///    for a negative result takes the `AllowNegativeToUnsigned` arm --
///    `return uint64(int64(val))`, the low 64 bits, exactly like the integer
///    source above -- beside an overflow event. Captured:
///    `cast(-1.5e0 as unsigned)` is 18446744073709551614 with `1690 constant
///    -2 overflows bigint`, `cast(-1e0 as unsigned)` is 18446744073709551615,
///    and `cast(-1e300 as unsigned)` is 9223372036854775808 (Go's
///    out-of-range `int64(...)` conversion lands on `i64::MIN`, which is what
///    Rust's saturating `as i64` gives too).
///
/// `-0.4` is the boundary the two share: it ROUNDS to `-0.0`, which is not
/// `< 0`, so both answer 0 with no event at all.
///
/// The result is [`Datum::UInt`], so downstream comparisons and arithmetic
/// retain the domain instead of silently reinterpreting it as signed display
/// text.
fn to_u64_unsigned(v: &Datum, ctx: &dyn crate::Columns) -> u64 {
    match v {
        // TiDB's integer cast reuses the low 64 bits for an ETInt source.
        // That is observable for `CAST(-5 AS UNSIGNED)`, which is
        // 18446744073709551611 rather than an error or a display-only wrap.
        // Go's `builtinCastTimeAsIntSig`/`builtinCastDurationAsIntSig`
        // produce a plain `int64` and the UNSIGNED target only reinterprets
        // its bits, so a temporal source takes the SIGNED path -- including
        // its `RoundFrac(DefaultFsp)`, which `convertToUint`'s own temporal
        // arm (a different caller) does NOT do.
        Datum::Int(_)
        | Datum::String(_)
        | Datum::Bytes(_)
        | Datum::Time(_)
        | Datum::Duration(_) => to_i64_signed_in(v, &ctx.time_zone()) as u64,
        Datum::UInt(i) => *i,
        // A decimal rounds half-up then converts through the full u64 range
        // (Go `MyDecimal.ToUint`): a negative value becomes 0, and a magnitude in
        // `(i64::MAX, u64::MAX]` — the upper half of `UNSIGNED BIGINT` — is kept
        // rather than saturated at `i64::MAX` by the signed path.
        Datum::Decimal(d) => {
            // Go's `convertDecimalStrToUint` reports the clamp, and the
            // message carries the decimal's ORIGINAL text -- `'-2.0'`, not the
            // rounded `-2`. The test is on the ROUNDED value, which is why
            // `cast(-0.4 as unsigned)` is a silent 0 (it rounds to `-0`) while
            // `cast(-1.5 as unsigned)` warns. Captured (`gorun`, default
            // sql_mode): `select cast(-2.0 as unsigned)` -> 0 with
            // `1292 Truncated incorrect DECIMAL value: '-2.0'`;
            // `cast(1.5 as unsigned)` -> 2 with no warning at all.
            if d.round_to_i64_saturating() < 0 {
                ctx.append_warning(1292, &format!("Truncated incorrect DECIMAL value: '{d}'"));
            }
            d.round_to_u64_saturating()
        }
        // A real rounds half-to-even then converts across the full u64 range
        // (Go `ConvertFloatToUint`), so its own upper half is kept too -- and
        // its NEGATIVE half is kept as the low 64 bits rather than clamped.
        Datum::Real(f) => real_to_u64_saturating(*f, ctx),
        Datum::Null | Datum::MinNotNull | Datum::MaxValue => unreachable!("guarded by caller"),
        other => other
            .to_decimal()
            .map_or(0, |converted| converted.value.round_to_u64_saturating()),
    }
}

/// `CAST(real AS UNSIGNED)`: round half to even (Go `RoundFloat` =
/// `math.RoundToEven`, the same rounding the signed real path uses), then Go
/// `ConvertFloatToUint` across the full `u64` range. A magnitude past
/// `u64::MAX` saturates to `u64::MAX` (`ConvertFloatToUint`'s `upperBound`
/// clamp). Routing through the signed path instead would lose the upper half
/// of `UNSIGNED BIGINT` at `i64::MAX`.
///
/// A NEGATIVE rounded value takes Go's `AllowNegativeToUnsigned` arm
/// (`convert.go:171-176`): `uint64(int64(val))`, the SAME low-64-bit
/// reinterpretation an integer source gets -- see [`to_u64_unsigned`]'s doc
/// for the captures, and for why the DECIMAL source really does answer 0 here
/// while this one does not.
fn real_to_u64_saturating(f: f64, ctx: &dyn crate::Columns) -> u64 {
    let rounded = f.round_ties_even();
    if rounded < 0.0 {
        // Go raises the overflow event on this arm and returns the value
        // anyway. `overflow(val, tp)` prints the ROUNDED value with `%v`,
        // which for a float64 is `strconv.FormatFloat(f, 'g', -1, 64)`.
        // Captured under the DEFAULT (strict) sql_mode, where this is still a
        // WARNING rather than a statement error, because
        // `builtinCastRealAsIntSig` routes it through `HandleOverflow`.
        ctx.append_warning(
            1690,
            &format!(
                "constant {} overflows bigint",
                tidb_datatype::format_float_g_shortest(rounded)
            ),
        );
        // Rust's saturating `as i64` reproduces Go's out-of-range `int64(...)`
        // landing on `i64::MIN`, which is what makes `cast(-1e300 as
        // unsigned)` 9223372036854775808 rather than 0.
        (rounded as i64) as u64
    } else {
        // Rust's float-to-int cast saturates: an in-range integral float is
        // exact, a magnitude past `u64::MAX` clamps to `u64::MAX`, and `NaN`
        // maps to 0 (already excluded by the caller's NULL guard).
        rounded as u64
    }
}

/// Scans a MySQL-style INTEGER numeric prefix: optional leading
/// whitespace, optional sign, then a run of ASCII digits — stopping at
/// the first non-digit (no `.`, no exponent; see [`to_i64_signed`]'s own
/// doc for the confirming probe). `0` if no digit is found. Saturates to
/// `i64::MIN`/`MAX` on overflow rather than replicating real TiDB's own
/// exotic bit-reinterpretation for a string whose digit run exceeds even
/// `u64` range (confirmed via `goeval`: `CAST('99999999999999999999' AS
/// SIGNED)` — twenty `9`s — is `-1` in real TiDB, a `u64::MAX` value
/// bit-reinterpreted as `i64`; this project deliberately does not
/// replicate that, saturating to `i64::MAX` instead — a principled,
/// documented divergence for a value nobody writes intentionally, not an
/// oversight).
/// Go `types.getValidIntPrefix`'s `isFuncCast` arm, reporting ONLY whether
/// the scan consumed the whole string. Go scans BYTES and advances the valid
/// length only on a digit, so a lone sign leaves length zero:
/// `[+-]?` at offset 0 is skipped without counting, every following ASCII
/// digit sets the length to `i + 1`, and the first other byte stops the scan.
///
/// Returned separately from [`str_int_prefix`] because the two answers have
/// different lifetimes in Go too: the prefix VALUE is returned to the caller
/// unconditionally, while the truncation event goes through
/// `Context.HandleTruncate` and may be discarded, warned, or raised.
fn int_prefix_consumed_all(s: &str) -> bool {
    // Go `StrToInt`/`StrToUint` trim BOTH ends before scanning, so trailing
    // space is not a truncation; `CAST('  12  ' AS SIGNED)` is exact.
    let trimmed = s.trim();
    let mut valid_len = 0;
    for (i, byte) in trimmed.bytes().enumerate() {
        if (byte == b'+' || byte == b'-') && i == 0 {
            continue;
        }
        if byte.is_ascii_digit() {
            valid_len = i + 1;
            continue;
        }
        break;
    }
    valid_len != 0 && valid_len == trimmed.len()
}

/// Applies the statement's truncation level when `CAST(<string> AS
/// SIGNED/UNSIGNED)` did not consume the whole operand, which is the point
/// Go's `getValidIntPrefix` calls `Context.HandleTruncate`.
///
/// The `CAST(<number> AS SIGNED)` clamp's own warning, which is the only
/// thing that says the value saturated:
///
///  * `builtinCastRealAsIntSig` (`builtin_cast.go:1367`) returns
///    `ConvertFloatToInt`'s `ErrOverflow` (1690)
///    "constant %v overflows bigint" -- printing the ROUNDED value.
///  * `builtinCastDecimalAsIntSig` (`:1566`) aliases its own `ErrOverflow`
///    to `ErrTruncatedWrongVal` (1292)
///    "Truncated incorrect DECIMAL value: '%v'" -- printing the ORIGINAL
///    decimal, not the rounded one.
///
/// Both are WARNINGS in the default (strict) sql_mode, captured: reads never
/// fail. Go compares against `float64(upperBound)`, so the check is exact
/// only in `f64`: `CAST(9223372036854775806.9e0 AS SIGNED)` is `i64::MAX`
/// with NO warning, because the bound itself rounds up to the same `f64`.
/// Go's `val >= float64(upperBound)` spares exactly that equal case and
/// `val < float64(lowerBound)` is strict, which is why the range below is
/// INCLUSIVE at both ends.
///
/// `RoundFloat` is `math.RoundToEven`, mirrored here, but NO input can
/// observe it: an `f64` keeps a fractional part only below 2^52, while this
/// arm fires only past 2^63, so the rounding is the identity for both the
/// comparison and the printed text. It is kept because Go rounds; a fixture
/// that pins it cannot exist.
///
/// SIGNED only. Go's UNSIGNED target takes the other branch of the same
/// signature (`ConvertFloatToUint`/`MyDecimal.ToUint`), whose warning
/// [`to_u64_unsigned`] already raises -- calling both would double it.
fn report_signed_overflow(v: &Datum, ctx: &dyn crate::Columns) {
    match v {
        Datum::Real(value) | Datum::Float32(value) => {
            let rounded = value.round_ties_even();
            if !(i64::MIN as f64..=i64::MAX as f64).contains(&rounded) {
                ctx.append_warning(
                    1690,
                    &format!(
                        "constant {} overflows bigint",
                        tidb_datatype::format_float_g_shortest(rounded)
                    ),
                );
            }
        }
        Datum::Decimal(value) if value.round_to_i64().is_none() => ctx.append_warning(
            1292,
            &format!("Truncated incorrect DECIMAL value: '{value}'"),
        ),
        _ => {}
    }
}

/// Only a string-valued operand reaches Go's `builtinCastStringAsIntSig`;
/// the numeric signatures have their own, overflow-shaped diagnostic, in
/// [`report_signed_overflow`].
pub(crate) fn report_int_truncation(v: &Datum, ctx: &dyn crate::Columns) -> Result<(), EvalError> {
    let text = match v {
        Datum::String(value) => value.as_utf8().ok(),
        Datum::Bytes(value) => std::str::from_utf8(value).ok(),
        _ => None,
    };
    match text {
        Some(text) if !int_prefix_consumed_all(text) => ctx.handle_truncate(&format!(
            "Truncated incorrect INTEGER value: '{}'",
            text.trim()
        )),
        _ => Ok(()),
    }
}

fn str_int_prefix(s: &str) -> i64 {
    let s = s.trim_start();
    let (negative, rest) = match s.strip_prefix('-') {
        Some(r) => (true, r),
        None => (false, s.strip_prefix('+').unwrap_or(s)),
    };
    let digits: String = rest.chars().take_while(char::is_ascii_digit).collect();
    if digits.is_empty() {
        return 0;
    }
    match digits.parse::<i64>() {
        Ok(mag) => {
            if negative {
                -mag
            } else {
                mag
            }
        }
        Err(_) => {
            if negative {
                i64::MIN
            } else {
                i64::MAX
            }
        }
    }
}

/// `CHAR(N)`'s own truncation is handled inline in [`eval_cast`] (keeps
/// the first `N` characters, never pads); this is `BINARY(N)`'s own
/// FIXED-WIDTH behavior — truncates the same way if longer, but PADS
/// with `\0` bytes if shorter, confirmed via `goeval`:
/// `CAST('hi' AS BINARY(5))` is `"hi\0\0\0"`, 5 bytes exactly. MySQL
/// `BINARY` counts BYTES, not characters; the byte-preserving `Datum::Bytes`
/// result keeps the same behavior for non-UTF-8 truncation boundaries too.
fn binary_pad_truncate(s: &[u8], n: usize) -> Vec<u8> {
    let mut bytes: Vec<u8> = s.iter().copied().take(n).collect();
    bytes.resize(n, 0);
    bytes
}

/// Coerces an arbitrary source value to [`Decimal`] for `CAST(... AS
/// DECIMAL(...))`'s own operand — a WIDER domain than `crate::ops`'s own
/// `to_decimal`, which only ever sees `Int`/`Decimal` (guarded there by
/// `eval_binary`'s upstream `Str`/`Float` interception; `CAST` has no
/// such guard, so its operand can be any value).
fn to_decimal_for_cast(v: &Datum) -> Decimal {
    match v {
        Datum::Decimal(d) => d.clone(),
        Datum::Int(i) => Decimal::from_int(*i),
        Datum::UInt(i) => Decimal::from_uint(*i),
        // `f64`'s own `Display` never uses scientific notation (confirmed
        // directly against `1e300`/`1e-300`), so this always lands on
        // `decimal_prefix`'s exact, no-exponent path — never its lossy
        // `f64`-round-trip fallback.
        Datum::Real(f) => decimal_prefix(&f.to_string()),
        Datum::String(s) => s
            .as_utf8()
            .map(decimal_prefix)
            .unwrap_or_else(|_| Decimal::from_int(0)),
        Datum::Bytes(s) => std::str::from_utf8(s)
            .map(decimal_prefix)
            .unwrap_or_else(|_| Decimal::from_int(0)),
        Datum::Null | Datum::MinNotNull | Datum::MaxValue => unreachable!("guarded by caller"),
        other => other
            .to_decimal()
            .map_or_else(|_| Decimal::from_int(0), |converted| converted.value),
    }
}

/// `to_f64_for_cast`/`to_decimal_for_cast`'s shared string scan: a FULLER
/// numeric prefix than [`str_int_prefix`]'s own digit-run-only scan —
/// optional whitespace, sign, digits, optional `.` + digits, optional
/// exponent (confirmed via `goeval`: `CAST('3.5abc' AS DECIMAL)` sees
/// `3.5abc`'s leading `3.5`, and `CAST('1e2' AS DECIMAL)` is `100`, both
/// stopping at the first character that doesn't extend the number). Exact
/// digit-string arithmetic when there's no exponent (the common case,
/// covering every value this crate's `Decimal` itself can ever produce);
/// an exponent suffix falls back to an `f64` round-trip — a narrow,
/// accepted precision-loss divergence for that one sub-case (real MySQL's
/// own decimal parser handles an exponent exactly; this crate's does
/// not).
fn decimal_prefix(s: &str) -> Decimal {
    let s = s.trim_start();
    let (negative, rest) = match s.strip_prefix('-') {
        Some(r) => (true, r),
        None => (false, s.strip_prefix('+').unwrap_or(s)),
    };
    let int_digits: String = rest.chars().take_while(char::is_ascii_digit).collect();
    let after_int = &rest[int_digits.len()..];
    let (frac_digits, after_frac) = match after_int.strip_prefix('.') {
        Some(r) => {
            let f: String = r.chars().take_while(char::is_ascii_digit).collect();
            let len = f.len();
            (f, &r[len..])
        }
        None => (String::new(), after_int),
    };
    if int_digits.is_empty() && frac_digits.is_empty() {
        return Decimal::from_int(0);
    }
    let base = if int_digits.is_empty() {
        format!("0.{frac_digits}")
    } else if frac_digits.is_empty() {
        int_digits.clone()
    } else {
        format!("{int_digits}.{frac_digits}")
    };
    let exponent = exponent_prefix(after_frac);
    if exponent != 0 {
        let base_f: f64 = base.parse().unwrap_or(0.0);
        let sign = if negative { -1.0 } else { 1.0 };
        let scaled = sign * base_f * 10f64.powi(exponent);
        // `f64`'s own `Display` never uses scientific notation, so this
        // recursive call always lands on the `exponent == 0` fast path
        // above — no risk of looping.
        return decimal_prefix(&scaled.to_string());
    }
    let mut d = Decimal::from_literal(&base);
    if negative {
        d = d.negate();
    }
    d
}

/// Scans an optional `e`/`E` exponent suffix (`[eE][+-]?digits`),
/// returning `0` if the text doesn't start with one — [`decimal_prefix`]'s
/// own helper.
fn exponent_prefix(s: &str) -> i32 {
    let Some(rest) = s.strip_prefix(['e', 'E']) else {
        return 0;
    };
    let (negative, rest) = match rest.strip_prefix('-') {
        Some(r) => (true, r),
        None => (false, rest.strip_prefix('+').unwrap_or(rest)),
    };
    let digits: String = rest.chars().take_while(char::is_ascii_digit).collect();
    if digits.is_empty() {
        return 0;
    }
    let mag: i32 = digits.parse().unwrap_or(0);
    if negative {
        -mag
    } else {
        mag
    }
}

/// `DOUBLE`/`FLOAT`'s own coercion: `Int`/`Decimal`/`Float` promote the
/// same way `crate::ops::to_f64` already does for binary arithmetic; a
/// `Str` source reuses [`decimal_prefix`]'s own numeric-prefix scan
/// (matching `DECIMAL`'s own string coercion, NOT `SIGNED`'s narrower
/// digit-run-only one — confirmed via `goeval`: `CAST('3.5e1abc' AS
/// DOUBLE)` is `35`, consuming the `.` and exponent `SIGNED`'s own scan
/// would stop before).
fn to_f64_for_cast(v: &Datum) -> f64 {
    match v {
        Datum::Int(i) => *i as f64,
        Datum::UInt(i) => *i as f64,
        Datum::Decimal(d) => d.to_f64(),
        Datum::Real(f) => *f,
        Datum::String(s) => s.as_utf8().map(decimal_prefix).map_or(0.0, |d| d.to_f64()),
        Datum::Bytes(s) => std::str::from_utf8(s)
            .map(decimal_prefix)
            .map_or(0.0, |d| d.to_f64()),
        Datum::Null | Datum::MinNotNull | Datum::MaxValue => unreachable!("guarded by caller"),
        other => other.to_f64().map_or(0.0, |converted| converted.value),
    }
}

/// `CAST(... AS DATE)` and `CAST(... AS DATETIME)`: Go
/// `builtinCastStringAsTimeSig.evalTime`.
///
/// The whole body is Go's, in order: `types.ParseTime` under the STATEMENT's
/// type flags, then `handleInvalidTimeError` on failure, then the separate
/// `NO_ZERO_DATE` rejection of an all-zero result, then the DATE truncation
/// of the clock fields.
///
/// # Why this does not use this crate's own date parser
///
/// It used to, and that was two sources of truth for one table. Go asks
/// `Time.Check` the zero-in-date and invalid-date questions with the
/// statement's flags; `time_fn::calendar::parse_date_ymd` asks NEITHER and
/// rejects a zero month unconditionally, so `CAST('2024-00-01' AS DATE)`
/// answered NULL where TiDB answers `2024-00-01`, and every failing cast
/// answered NULL with NO warning where TiDB warns 1292.
/// `tidb_datatype::parse_time` is the faithful port of Go's `ParseTime`,
/// flags included, and is the same parser the WRITE path converts through.
/// `parse_date_ymd` stays strict for its own callers, which Go does NOT
/// relax (see its doc).
///
/// # The flags are the READ path's, not the write path's
///
/// Go `ResetContextOfStmt`'s `*ast.SelectStmt` arm sets `IgnoreZeroInDate`
/// UNCONDITIONALLY -- a zero-in-date reads back intact even under the default
/// mode that refuses to STORE one -- and takes `IgnoreInvalidDateErr` from
/// `ALLOW_INVALID_DATES` alone. With `TruncateAsWarning` also set, a bad
/// value is a warning plus NULL, never a statement failure: READS NEVER FAIL,
/// in any sql_mode.
fn cast_to_time(
    v: &Datum,
    source: Option<&tidb_datatype::FieldType>,
    ctx: &dyn crate::Columns,
    kind: tidb_datatype::TimeType,
    fsp: i64,
) -> Result<Datum, EvalError> {
    let Some(time) = cast_to_time_value(v, source, ctx, kind, Some(fsp))? else {
        return Ok(Datum::Null);
    };
    // The evaluator's public differential protocol still represents DATE and
    // DATETIME CAST results as strings. Keep the one YEAR-source exception
    // whose zero month/day fields need the typed value for later comparisons.
    if year_source_value(v, source).is_some() {
        return Ok(Datum::Time(time));
    }
    let core = time.core_time();
    Ok(if kind == tidb_datatype::TimeType::Date {
        format_ymd_result(
            i64::from(core.year()),
            u32::from(core.month()),
            u32::from(core.day()),
            None,
        )
    } else {
        Datum::new_string(time.to_string())
    })
}

/// Go's repeated DATE-target rule: preserve the calendar fields and clear the
/// clock before the typed value leaves the cast signature.
fn truncate_clock_for_date(
    mut time: tidb_datatype::Time,
    kind: tidb_datatype::TimeType,
) -> tidb_datatype::Time {
    if kind != tidb_datatype::TimeType::Date {
        return time;
    }
    let core = time.core_time();
    time.set_core_time(tidb_datatype::CoreTime::from_date(
        core.year() as u16,
        core.month(),
        core.day(),
        0,
        0,
        0,
        0,
    ));
    time
}

/// The `types.Time` Go's chosen `builtinCast*AsTimeSig` produces, BEFORE this
/// tier renders it as a string. `None` is Go's NULL (any warning already
/// raised).
///
/// [`cast_to_time`] is this plus the rendering; the argument-cast seam
/// ([`crate::arg_eval_type`]) is this WITHOUT it, because the rendering is
/// lossy in exactly the place Go's `ETDatetime` argument layer is not: a
/// `types.Time` carries a zero month or day as a stored field, while
/// `format_ymd*_result` collapses any zero year to the literal `0000-00-00`
/// and answers NULL outside `1..=9999`.
fn cast_to_time_value(
    v: &Datum,
    source: Option<&tidb_datatype::FieldType>,
    ctx: &dyn crate::Columns,
    kind: tidb_datatype::TimeType,
    fsp: Option<i64>,
) -> Result<Option<tidb_datatype::Time>, EvalError> {
    // A `YEAR` source is the one case the datum kind cannot speak for. Go
    // `builtinCastIntAsTimeSig.evalTime` (`builtin_cast.go:1127-1131`) asks the
    // ARGUMENT'S TYPE, not the integer's digits:
    //
    //   if b.args[0].GetType(ctx).GetType() == mysql.TypeYear {
    //       res, err = types.ParseTimeFromYear(val)
    //   } else {
    //       res, err = types.ParseTimeFromNum(typeCtx(ctx), val, ...)
    //   }
    //
    // and `types.ParseTimeFromYear` (`time.go:2072-2081`) INJECTS the value as
    // the year FIELD -- `FromDate(int(year), 0, 0, 0, 0, 0, 0)`, so `2018` is
    // `2018-00-00 00:00:00` -- with `0` mapping to the zero date typed
    // `mysql.TypeDate`. Routing that same `2018` through `ParseTimeFromNum`,
    // which reads an int as a packed `YYYYMMDD`, FAILS and yields NULL. Every
    // other INT source keeps `ParseTimeFromNum` below.
    if let Some(year) = year_source_value(v, source) {
        let time = tidb_datatype::parse_time_from_year(year)
            .map_err(|_| EvalError::Unsupported("a YEAR value outside the year range"))?;
        return Ok(Some(time));
    }
    // A DURATION source is the second kind whose text cannot speak for it. Go
    // `builtinCastDurationAsTimeSig.evalTime` (`builtin_cast.go:2275-2291`)
    // never parses `20:00:01` as a wall clock; it calls
    // `val.ConvertToTimeWithTimestamp(tc, b.tp.GetType(), ts)`, which takes
    // the CALENDAR DATE of the statement's own timestamp and mixes the
    // elapsed time into it (`types/time.go:1500-1507`). Routing the text
    // through `ParseTime` instead reads the `20` as a YEAR.
    //
    // Neither half of this is visible in the recorded corpus: every recorded
    // statement that reaches it has the OTHER argument winning, so any wrong
    // conversion still prints the recorded answer. Both are pinned by
    // `a_duration_beside_a_temporal_literal_lands_on_the_statement_date` in
    // `tidb-session`, which puts the duration on the winning side and then
    // moves the session zone across the date line.
    //
    // The two date-mode flags SURVIVED their own mutation (hardcoding both to
    // `false` moves nothing): `mixDateAndDuration` always starts from a real
    // calendar date, so no zero or invalid component can arise for them to
    // rule on. They are passed because Go passes its `ctx`, not because a
    // value distinguishes them.
    if let Datum::Duration(duration) = v {
        let modes = ctx.date_modes();
        let (utc_secs, nanos, tz_offset) = ctx
            .now()
            .ok_or(EvalError::Unsupported("no statement clock for a TIME cast"))?;
        // Go reads the calendar date of `ts.In(ctx.Location())`; `now`'s third
        // field is that location's offset AT that instant, so a fixed offset
        // names the same civil day without re-resolving the zone.
        let zone = chrono::FixedOffset::east_opt(tz_offset).ok_or(EvalError::Unsupported(
            "session time-zone offset out of range",
        ))?;
        let Some(stamp) = chrono::DateTime::from_timestamp(utc_secs, nanos) else {
            return Ok(None);
        };
        return Ok(duration
            .convert_to_time(
                stamp.with_timezone(&zone),
                kind,
                !modes.no_zero_in_date,
                modes.allow_invalid_dates,
            )
            .and_then(|time| match fsp {
                Some(fsp) => time.round_frac(fsp, &ctx.time_zone()),
                None => Ok(time),
            })
            .ok());
    }
    let Some(s) = coerce_str(v)? else {
        return Ok(None);
    };
    let modes = ctx.date_modes();
    // Go routes each source TYPE to its own parser, not its text: only the
    // STRING/BYTES signatures (`builtinCastStringAsTimeSig`) parse the wall-
    // clock text through `ParseTime`. An INT source takes `ParseTimeFromNum`,
    // and a REAL/DECIMAL source takes `ParseTimeFromFloatString`, both of which
    // read the value as TiDB's packed `YYYYMMDD[HHMMSS]` NUMBER -- not as a
    // free-form date string. Funnelling a decimal through the string parser is
    // what made `cast(121212.1111 as datetime)` absorb `.1111` as a clock
    // (`2012-12-12 11:11:00`) and `cast(111.1 as datetime)` fail outright,
    // where TiDB answers `2012-12-12 00:00:00` and `2000-01-11 00:00:00`
    // (`expression/cast`). The parser choice mirrors `Datum::convert_to_time`,
    // the faithful write-path port.
    let parsed = parse_time_by_source(
        v,
        &s,
        kind,
        fsp,
        modes.allow_invalid_dates,
        &ctx.time_zone(),
    );
    let Ok(time) = parsed else {
        invalid_time_warning(ctx, &s);
        return Ok(None);
    };
    // Go's SECOND check is the STRING signature's ALONE
    // (`builtinCastStringAsTimeSig`: `res.IsZero() && HasNoZeroDateMode()`).
    // The INT/REAL/DECIMAL signatures have no such rejection -- a numeric zero
    // is the zero time, not NULL (Go `#11203`), so `cast(0 as datetime)` and a
    // `0`-valued double/decimal column read `0000-00-00 00:00:00`, matching
    // `expression/cast`. Gating this on the text sources keeps the numeric
    // sources on Go's own no-rejection path.
    if matches!(v, Datum::String(_) | Datum::Bytes(_)) && time.is_zero() && modes.no_zero_date {
        invalid_time_warning(ctx, &s);
        return Ok(None);
    }
    Ok(Some(truncate_clock_for_date(time, kind)))
}

/// Go's `WrapWithCastAsTime(ctx, expr, types.NewFieldType(mysql.TypeDatetime))`
/// (`pkg/expression/builtin_cast.go:2817`), applied to an argument's VALUE
/// because this tier has no build-time expression rewrite to hang the cast on.
///
/// Go's early return is the whole of the special-casing:
///
/// ```go
/// exprTp := expr.GetType(ctx.GetEvalCtx()).GetType()
/// if tp.GetType() == exprTp {
///     return expr
/// } else if (exprTp == mysql.TypeDate || exprTp == mysql.TypeTimestamp) && tp.GetType() == mysql.TypeDatetime {
///     return expr
/// }
/// ```
///
/// -- i.e. a DATE, DATETIME or TIMESTAMP expression is handed through
/// untouched. In this tier those three are exactly the expressions that
/// evaluate to a [`Datum::Time`], so ONE pass-through arm covers all of Go's
/// early return; everything else goes through the cast Go builds, which is
/// [`cast_to_time_value`] (the same function `CAST(x AS DATETIME)` uses,
/// YEAR hole and all).
pub(crate) fn cast_arg_as_datetime(
    v: &Datum,
    source: Option<&tidb_datatype::FieldType>,
    ctx: &dyn crate::Columns,
) -> Result<Datum, EvalError> {
    if matches!(v, Datum::Time(_) | Datum::Null) {
        return Ok(v.clone());
    }
    Ok(
        cast_to_time_value(v, source, ctx, tidb_datatype::TimeType::DateTime, None)?
            .map_or(Datum::Null, Datum::Time),
    )
}

/// Go `WrapWithCastAsInt(ctx, expr, nil)` (`builtin_cast.go:2666-2698`),
/// applied to an argument's VALUE for the same reason
/// [`cast_arg_as_datetime`] is.
///
/// Go's own body, in full:
///
/// ```go
/// if expr.GetType(ctx.GetEvalCtx()).GetType() == mysql.TypeEnum {
///     ... expr.GetType(ctx.GetEvalCtx()).AddFlag(mysql.EnumSetAsIntFlag)
/// }
/// if expr.GetType(ctx.GetEvalCtx()).EvalType() == types.ETInt {
///     return expr
/// }
/// tp := types.NewFieldType(mysql.TypeLonglong)
/// ...
/// if targetType == nil {
///     tp.AddFlag(expr.GetType(ctx.GetEvalCtx()).GetFlag() & mysql.UnsignedFlag)
/// }
/// return BuildCastFunction(ctx, expr, tp)
/// ```
///
/// Three of Go's rules land here, and every one of them is a KIND test, not a
/// per-builtin condition:
///
///  * **The early return.** `EvalType() == types.ETInt` covers the integer
///    types and, through `FieldType.EvalType`'s own switch
///    (`pkg/parser/types/field_type.go:417-441`), `mysql.TypeBit` and
///    `mysql.TypeYear` as well. In this tier those are exactly the arguments
///    that evaluate to an [`Datum::Int`]/[`Datum::UInt`] — a `YEAR` reaches
///    the signature as its integer either way, so unlike the ETDatetime rung
///    the static type buys NOTHING here.
///
///  * **The hybrid short-circuit.** `mysql.TypeEnum` gets
///    `EnumSetAsIntFlag`, which flips its `EvalType()` to `ETInt` and takes
///    the early return with the ORDINAL. `mysql.TypeSet` does NOT get the
///    flag, but the cast Go then builds routes it right back to the same
///    reading: `castAsIntFunctionClass.getFunction` opens with
///    `if args[0].GetType(ctx.GetEvalCtx()).Hybrid() || IsBinaryLiteral(args[0]) {
///    sig = &builtinCastIntAsIntSig{bf} }` (`builtin_cast.go:146-147`), whose
///    body is `b.args[0].EvalInt`. ENUM, SET, BIT and a bit/hex LITERAL
///    therefore all reach the signature as their ordinal or bit integer, and
///    [`Datum::to_i64_in`](tidb_datatype::Datum::to_i64_in) already reads all
///    four that way -- so the ordinary cast below is already Go's answer for
///    them and needs no arm of its own.
///
///  * **The unsigned inheritance.** `targetType` is `nil` at every
///    `newBaseBuiltinFuncWithTp` call site (`builtin.go:202`), so the built
///    cast is `UNSIGNED` exactly when the SOURCE type is, and that flag is
///    what `builtinTruncateIntSig` reads back out of
///    `b.args[1].GetType(ctx).GetFlag()` (`builtin_math.go:2166`). A tier
///    without the source type therefore answers SIGNED, which is Go's answer
///    for every argument that is not an unsigned non-integer.
///
/// Confirmed against real TiDB (`gorun`) over an `enum('x','y','z')` holding
/// `'y'`, a `set('a','b','c')` holding `'a,c'` and a `bit(8)` holding
/// `b'00000011'`: `make_set(e,'p','q','r')` is `q` (ordinal 2),
/// `make_set(s,'p','q','r')` is `p,r` (bits 5) and `make_set(b,'p','q','r')`
/// is `p,q` (bits 3).
pub(crate) fn cast_arg_as_int(
    v: &Datum,
    source: Option<&tidb_datatype::FieldType>,
    ctx: &dyn crate::Columns,
) -> Result<Datum, EvalError> {
    if matches!(v, Datum::Int(_) | Datum::UInt(_) | Datum::Null) {
        return Ok(v.clone());
    }
    let cast = if source.is_some_and(tidb_datatype::FieldType::is_unsigned) {
        CastType::Unsigned
    } else {
        CastType::Signed
    };
    eval_cast(&cast, v.clone(), source, ctx)
}

/// Go `WrapWithCastAsString(ctx, expr)` (`builtin_cast.go:2769-2813`), applied
/// to an argument's VALUE for the same reason [`cast_arg_as_datetime`] is.
///
/// Go's body is one early return and then a pile of RESULT-TYPE arithmetic:
///
/// ```go
/// exprTp := expr.GetType(ctx.GetEvalCtx())
/// if exprTp.EvalType() == types.ETString {
///     return expr
/// }
/// argLen := exprTp.GetFlen()
/// ... // argLen adjustments, then charset/collation on the built `tp`
/// return BuildCastFunction(ctx, expr, tp)
/// ```
///
/// Everything after the early return sets `tp`'s FLEN and CHARSET, which are
/// metadata: none of the `argLen` arms can truncate, because every one of them
/// is at least as wide as the rendering it describes (`mysql.MaxIntWidth` for
/// an integer, `GetFlen()+3` for a decimal, `-1` -- unspecified -- for a
/// float). So at the VALUE seam this cast is the early return plus "render
/// the value's text", and the two things worth transcribing are which values
/// take the early return and what BIT renders as.
///
///  * **The early return** is `EvalType() == types.ETString`, and
///    `FieldType.EvalType` (`pkg/parser/types/field_type.go:436-441`) puts
///    `mysql.TypeEnum` and `mysql.TypeSet` there unless they carry
///    `EnumSetAsIntFlag` -- a flag only `WrapWithCastAsInt` ever adds. An
///    ENUM or SET argument is therefore NOT wrapped, and the signature body
///    reads it with `EvalString`, which is its NAME. Captured from real TiDB
///    (`gorun`) over an `enum('{}','[1]','x')` holding `'{}'`: `quote(e)` is
///    `'{}'` and `ltrim(e)` is `{}` -- the name, never the ordinal `1`. This
///    is the exact OPPOSITE of [`cast_arg_as_int`]'s hybrid arm, where the
///    same column reaches the signature as its ordinal.
///
///  * **BIT is the one hybrid that is NOT string-typed**: `mysql.TypeBit` is
///    `ETInt`, so it does not take the early return. The cast Go then builds
///    lands on `castAsStringFunctionClass.getFunction`'s own hybrid arm
///    (`builtin_cast.go:315-321`), whose `castBitAsUnBinary` test is false
///    here because `WrapWithCastAsString` already set the target charset to
///    `charset.CharsetBin` for `TypeBit` (`:2801-2804`) -- so the signature
///    is `builtinCastStringAsStringSig` and the value is the bit's RAW BYTES,
///    not its decimal digits. Captured over a `bit(8)` holding `b'11111111'`:
///    `hex(ltrim(b))` is `FF`, and `hex(quote(b))` is `27EFBFBD27` -- one
///    0xFF byte that `Quote`'s own `[]rune` conversion then replaces.
///
/// `source` is unused: unlike [`cast_arg_as_datetime`]'s `YEAR` and
/// [`cast_arg_as_int`]'s `UNSIGNED`, nothing this cast produces depends on a
/// fact the datum does not already carry.
pub(crate) fn cast_arg_as_string(
    v: &Datum,
    _source: Option<&tidb_datatype::FieldType>,
    _ctx: &dyn crate::Columns,
) -> Result<Datum, EvalError> {
    match v {
        // Go's early return: every `types.ETString` eval type, which is every
        // string kind plus the two string-typed hybrids. Passing the datum
        // through UNCHANGED (rather than flattening it to bytes here) is what
        // keeps a `Datum::String`'s collation and a `Datum::Bytes`'s binary
        // signature readable by the body -- see `crate::string_signature`.
        Datum::Null
        | Datum::String(_)
        | Datum::Bytes(_)
        | Datum::Enum(..)
        | Datum::Set(..)
        | Datum::BinaryLiteral(_) => Ok(v.clone()),
        // The BIT arm above: raw bytes under the binary charset Go's `tp`
        // was given, which in this tier is `Datum::Bytes`.
        Datum::Bit(bits) => Ok(Datum::new_bytes(bits.as_bytes().to_vec())),
        // Everything else takes one of `castAsStringFunctionClass`'s
        // per-source signatures, all of which render the value's own text
        // under the connection charset -- which is exactly what
        // `crate::coerce::coerce_str_bytes` already is.
        _ => Ok(crate::coerce::coerce_str_bytes(v)?.map_or(Datum::Null, Datum::new_string)),
    }
}

/// The integer a `YEAR`-typed operand carries, or `None` when the operand is
/// not a `YEAR` at all.
///
/// The type test is Go's own (`b.args[0].GetType(ctx).GetType() ==
/// mysql.TypeYear`); the kind test is this tier's, because a `YEAR` expression
/// always evaluates to an integer and anything else under a `YEAR` field type
/// is a value this tier produced, not one Go's `EvalInt` could have returned.
fn year_source_value(v: &Datum, source: Option<&tidb_datatype::FieldType>) -> Option<i64> {
    if source?.code() != tidb_datatype::FieldTypeCode::Year {
        return None;
    }
    match v {
        Datum::Int(value) => Some(*value),
        Datum::UInt(value) => i64::try_from(*value).ok(),
        _ => None,
    }
}

/// Parses one cast operand into a `Time`, choosing the parser by SOURCE TYPE
/// the way Go's per-signature `builtinCast*AsTimeSig` split does (see
/// [`cast_to_time`]'s doc for why the text is not enough). The read-path flags
/// are the string signature's own: `allow_zero_in_date` is UNCONDITIONALLY
/// `true` (a SELECT reads a zero-in-date back intact), and `allow_invalid_date`
/// follows `ALLOW_INVALID_DATES`. `Err(())` is Go's parse failure, which the
/// caller turns into a 1292 warning plus NULL.
///
/// The parser routing mirrors `Datum::convert_to_time`, the faithful write-path
/// port: INT/UINT -> `parse_time_from_num`, DECIMAL -> `parse_time_from_decimal`,
/// REAL/FLOAT -> `parse_time_from_float64`. A UINT beyond `i64::MAX` cannot be a
/// packed datetime and is a parse failure. The float/decimal parsers classify
/// DATE-vs-DATETIME by digit count, so the target `kind` is re-imposed with
/// `set_kind` -- exactly what `convert_to_time` does after those two parsers.
fn parse_time_by_source(
    v: &Datum,
    text: &str,
    kind: tidb_datatype::TimeType,
    fsp: Option<i64>,
    allow_invalid: bool,
    zone: &tidb_datatype::SessionTimeZone,
) -> Result<tidb_datatype::Time, ()> {
    match v {
        Datum::Int(value) => tidb_datatype::parse_time_from_num(
            *value,
            kind,
            fsp.unwrap_or(0),
            true,
            allow_invalid,
            zone,
        )
        .map(|parsed| parsed.time)
        .map_err(|_| ()),
        Datum::UInt(value) => {
            let signed = i64::try_from(*value).map_err(|_| ())?;
            tidb_datatype::parse_time_from_num(
                signed,
                kind,
                fsp.unwrap_or(0),
                true,
                allow_invalid,
                zone,
            )
            .map(|parsed| parsed.time)
            .map_err(|_| ())
        }
        Datum::Decimal(value) => {
            let mut time = tidb_datatype::parse_time_from_decimal(value, true, allow_invalid, zone)
                .map_err(|_| ())?;
            time.set_kind(kind);
            match fsp {
                Some(fsp) => time.round_frac(fsp, zone).map_err(|_| ()),
                None => Ok(time),
            }
        }
        Datum::Real(value) => real_to_time(*value, kind, fsp.unwrap_or(0), allow_invalid, zone),
        Datum::Float32(value) => real_to_time(*value, kind, fsp.unwrap_or(0), allow_invalid, zone),
        Datum::Time(value) => {
            let mut time = *value;
            time.set_kind(kind);
            match fsp {
                Some(fsp) => time.round_frac(fsp, zone).map_err(|_| ()),
                None => Ok(time),
            }
        }
        // STRING/BYTES and every other coercible source keep Go's
        // `builtinCastStringAsTimeSig` path: parse the wall-clock TEXT.
        //
        // The zone is the SESSION's, as Go's `builtinCastStringAsTimeSig` passes
        // `ctx.TypeCtx()`. It does more than TIMESTAMP's range check: a literal
        // whose fraction is wider than `fsp` ROUNDS, and Go applies that carry to
        // the INSTANT in `ctx.Location()`, so a carry landing on a DST transition
        // moves the wall clock by the offset change too. CAPTURED from real TiDB:
        // `cast('2011-03-13 01:59:59.9999999' as datetime)` is
        // `2011-03-13 02:00:00` under `time_zone='UTC'` and `03:00:00` under
        // `'America/Los_Angeles'` (02:00 does not exist there), and
        // `cast('2011-11-06 01:59:59.9999999' as datetime)` is `02:00:00` under
        // UTC and `01:00:00` there (the repeated hour). Hardcoding UTC returned
        // the UTC answer for every session.
        _ => tidb_datatype::parse_time(
            text,
            kind,
            fsp.unwrap_or_else(|| i64::from(tidb_datatype::get_fsp(text))),
            false,
            true,
            allow_invalid,
            zone,
        )
        .map(|parsed| parsed.time)
        .map_err(|_| ()),
    }
}

/// REAL/FLOAT source shared by `Real` and `Float32`: Go
/// `builtinCastRealAsTimeSig` reads the float's packed-number form. `0.0` is
/// the zero time rather than a failure (Go's `#11203` guard); the float parser
/// already returns the zero time for a `0` integer part, so no special case is
/// needed here.
fn real_to_time(
    value: f64,
    kind: tidb_datatype::TimeType,
    fsp: i64,
    allow_invalid: bool,
    zone: &tidb_datatype::SessionTimeZone,
) -> Result<tidb_datatype::Time, ()> {
    let mut time =
        tidb_datatype::parse_time_from_float64(value, true, allow_invalid, zone).map_err(|_| ())?;
    time.set_kind(kind);
    time.round_frac(fsp, zone).map_err(|_| ())
}

/// Go `handleInvalidTimeError` on the read path: `ErrWrongValue` (1292)
/// becomes a warning and the cast yields NULL.
fn invalid_time_warning(ctx: &dyn crate::Columns, input: &str) {
    ctx.append_warning(1292, &format!("Incorrect datetime value: '{input}'"));
}

/// `CAST(... AS YEAR)`: the operand's calendar year if it parses as a
/// date-shaped string (confirmed via `goeval`: `CAST('2021-01-01' AS
/// YEAR)` is `2021`), else a plain `SIGNED`-style integer coercion
/// (confirmed via `goeval`: `CAST('99' AS YEAR)` is `99` — NOT the
/// two-digit-year century pivot the `YEAR` COLUMN TYPE applies at
/// storage time, a genuinely separate rule this scalar CAST does not
/// share).
fn cast_to_year(v: &Datum) -> Result<Datum, EvalError> {
    if let Some(s) = coerce_str(v)? {
        if let Some((y, _, _)) = parse_date_ymd(&s) {
            return Ok(Datum::Int(y));
        }
    }
    Ok(Datum::Int(to_i64_signed(v)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::context::NoColumns;

    #[test]
    fn cast_decimal_as_unsigned_keeps_the_upper_half_of_unsigned_bigint() {
        // The wired bug: routing a decimal through the signed path saturated the
        // upper half of UNSIGNED BIGINT at i64::MAX (9223372036854775807). Go
        // rounds half-up then MyDecimal.ToUint, keeping the full u64 range.
        assert_eq!(
            to_u64_unsigned(
                &Datum::Decimal(Decimal::from_literal("10000000000000000000")),
                &NoColumns
            ),
            10_000_000_000_000_000_000,
            "one past i64::MAX is kept, not saturated"
        );
        assert_eq!(
            to_u64_unsigned(
                &Datum::Decimal(Decimal::from_literal("18446744073709551615")),
                &NoColumns
            ),
            u64::MAX
        );
        // Half-up rounding and the negative-to-zero rule are unchanged.
        assert_eq!(
            to_u64_unsigned(&Datum::Decimal(Decimal::from_literal("5.6")), &NoColumns),
            6
        );
        assert_eq!(
            to_u64_unsigned(
                &Datum::Decimal(Decimal::from_literal("5.6").negate()),
                &NoColumns
            ),
            0
        );
        // A signed-integer source still reinterprets its low 64 bits:
        // CAST(-5 AS UNSIGNED) stays 18446744073709551611, unaffected by the fix.
        assert_eq!(
            to_u64_unsigned(&Datum::Int(-5), &NoColumns),
            18_446_744_073_709_551_611
        );
    }

    #[test]
    fn cast_real_as_unsigned_keeps_the_upper_half_of_unsigned_bigint() {
        // The sibling wired bug: a real routed through the signed path saturated
        // the upper half of UNSIGNED BIGINT at i64::MAX (9223372036854775807).
        // Go rounds half-to-even (RoundFloat) then ConvertFloatToUint across the
        // full u64 range. 1e19 is exactly representable in f64.
        assert_eq!(
            to_u64_unsigned(&Datum::Real(1.0e19), &NoColumns),
            10_000_000_000_000_000_000,
            "a real past i64::MAX is kept, not saturated at i64::MAX"
        );
        // A magnitude past u64::MAX saturates to MaxUint64 (upperBound clamp).
        assert_eq!(to_u64_unsigned(&Datum::Real(1.0e30), &NoColumns), u64::MAX);
        // Half-to-even rounding (Go RoundFloat = math.RoundToEven), the same rule
        // the signed real path uses: 2.5 -> 2, 3.5 -> 4.
        assert_eq!(to_u64_unsigned(&Datum::Real(2.5), &NoColumns), 2);
        assert_eq!(to_u64_unsigned(&Datum::Real(3.5), &NoColumns), 4);
        // A negative real does NOT clamp to zero: Go's
        // `AllowNegativeToUnsigned` arm returns `uint64(int64(val))`, the same
        // low-64-bit reinterpretation the integer source above gets. This
        // assertion used to read `, 0)` and pinned the WRONG answer.
        // Captured (`goeval`): `cast(-1.5e0 as unsigned)` ->
        // 18446744073709551614, `cast(-1e0 as unsigned)` ->
        // 18446744073709551615, `cast(-1e300 as unsigned)` ->
        // 9223372036854775808.
        assert_eq!(
            to_u64_unsigned(&Datum::Real(-1.5), &NoColumns),
            18_446_744_073_709_551_614
        );
        assert_eq!(
            to_u64_unsigned(&Datum::Real(-1.0), &NoColumns),
            18_446_744_073_709_551_615
        );
        assert_eq!(
            to_u64_unsigned(&Datum::Real(-5.6), &NoColumns),
            18_446_744_073_709_551_610
        );
        assert_eq!(
            to_u64_unsigned(&Datum::Real(-1.0e300), &NoColumns),
            9_223_372_036_854_775_808
        );
        // -0.4 ROUNDS to -0.0, which is not `< 0`, so it is the one negative
        // input that really is 0 -- with no warning either.
        assert_eq!(to_u64_unsigned(&Datum::Real(-0.4), &NoColumns), 0);
        // The DECIMAL source keeps Go's own opposite rule: negative -> 0.
        assert_eq!(
            to_u64_unsigned(
                &Datum::Decimal(Decimal::from_literal("1.5").negate()),
                &NoColumns
            ),
            0
        );
    }

    /// `CAST(str AS DATETIME)` rounds in the SESSION zone, not in UTC.
    ///
    /// Go's `builtinCastStringAsTimeSig` passes `ctx.TypeCtx()`, whose
    /// location the fractional-carry arm of `parseDatetime` applies the carry
    /// in. CAPTURED from real TiDB, both instants chosen so the carry lands
    /// exactly on a DST transition:
    ///
    /// ```text
    /// select cast('2011-03-13 01:59:59.9999999' as datetime)
    ///   time_zone='UTC'                 2011-03-13 02:00:00
    ///   time_zone='America/Los_Angeles' 2011-03-13 03:00:00
    /// select cast('2011-11-06 01:59:59.9999999' as datetime)
    ///   time_zone='UTC'                 2011-11-06 02:00:00
    ///   time_zone='America/Los_Angeles' 2011-11-06 01:00:00
    /// ```
    ///
    /// A four-zone probe over ordinary instants shows NO difference at all,
    /// which is why this pin uses the transition instants: an invariance
    /// probe here is a false negative.
    #[test]
    fn a_string_cast_to_datetime_rounds_in_the_session_zone() {
        use crate::Columns as _;
        struct Zoned(tidb_datatype::SessionTimeZone);
        impl crate::Columns for Zoned {
            fn get(&self, _: &[String]) -> Option<Datum> {
                None
            }
            fn time_zone(&self) -> tidb_datatype::SessionTimeZone {
                self.0.clone()
            }
        }
        let utc = Zoned(tidb_datatype::SessionTimeZone::utc());
        let la = Zoned(tidb_datatype::SessionTimeZone::Named(
            chrono_tz::America::Los_Angeles,
        ));
        for (input, in_utc, in_la) in [
            (
                "2011-03-13 01:59:59.9999999",
                "2011-03-13 02:00:00",
                "2011-03-13 03:00:00",
            ),
            (
                "2011-11-06 01:59:59.9999999",
                "2011-11-06 02:00:00",
                "2011-11-06 01:00:00",
            ),
        ] {
            for (ctx, expected) in [(&utc, in_utc), (&la, in_la)] {
                let got = cast_to_time(
                    &Datum::new_string(input.to_string()),
                    None,
                    ctx,
                    tidb_datatype::TimeType::DateTime,
                    0,
                )
                .unwrap_or_else(|error| panic!("{input}: {error:?}"));
                assert_eq!(
                    render_time(&got),
                    expected,
                    "{input} in {:?}",
                    ctx.time_zone()
                );
            }
        }
    }

    fn render_time(v: &Datum) -> String {
        match v {
            Datum::Time(time) => time.to_string(),
            Datum::String(text) => crate::coerce::string_text(text)
                .expect("temporal CAST text is valid UTF-8")
                .to_owned(),
            Datum::Null => "NULL".to_owned(),
            other => panic!("a temporal cast produced an unexpected {other:?}"),
        }
    }

    fn datetime_fsp(v: Datum, fsp: i64) -> String {
        render_time(
            &cast_to_time(&v, None, &NoColumns, tidb_datatype::TimeType::DateTime, fsp)
                .expect("cast"),
        )
    }

    fn datetime(v: Datum) -> String {
        datetime_fsp(v, 0)
    }

    /// A DECIMAL source is read as TiDB's packed `YYYYMMDD[HHMMSS]` NUMBER
    /// (Go `builtinCastDecimalAsTimeSig` -> `ParseTimeFromFloatString`), NOT as
    /// wall-clock text. Funnelling `121212.1111` through the STRING parser made
    /// it absorb the `.1111` fraction as a clock (`2012-12-12 11:11:00`) where
    /// TiDB reads the whole-date number `121212` and answers midnight
    /// (`expression/cast`: `cast(d2 as datetime)` over `121212.1111`).
    #[test]
    fn a_decimal_source_reads_the_packed_number_not_the_wall_clock_text() {
        assert_eq!(
            datetime(Datum::Decimal(Decimal::from_literal("121212.1111"))),
            "2012-12-12 00:00:00",
        );
        // A number shorter than a full date is zero-padded YYMMDD, so `111`
        // is `00-01-11` -> `2000-01-11`; the string parser rejected it as NULL.
        assert_eq!(
            datetime(Datum::Decimal(Decimal::from_literal("111.1"))),
            "2000-01-11 00:00:00",
        );
        // A month of 13 is still an invalid date -> NULL, unchanged.
        assert_eq!(
            datetime(Datum::Decimal(Decimal::from_literal("1311.1"))),
            "NULL",
        );
    }

    /// A REAL/DOUBLE source takes the same packed-number reading
    /// (Go `builtinCastRealAsTimeSig`), so `1122.1` is `00-11-22`.
    #[test]
    fn a_real_source_reads_the_packed_number() {
        assert_eq!(datetime(Datum::Real(1122.1)), "2000-11-22 00:00:00",);
        assert_eq!(datetime(Datum::Float32(1122.1)), "2000-11-22 00:00:00",);
    }

    /// An INTEGER source is `ParseTimeFromNum` (Go `builtinCastIntAsTimeSig`):
    /// `20170118` is the packed date, no fractional text to misread.
    #[test]
    fn an_integer_source_reads_the_packed_number() {
        assert_eq!(datetime(Datum::Int(20_170_118)), "2017-01-18 00:00:00",);
    }

    /// Go's numeric cast signatures have NO `NO_ZERO_DATE` rejection (the
    /// `#11203` guard: a zero number is the zero time, never NULL), unlike the
    /// STRING signature. Under the default SQL mode -- which DOES carry
    /// `NO_ZERO_DATE` ([`NoColumns`] answers `TIDB_DEFAULT_SQL_MODE`) -- a zero
    /// INT/REAL/DECIMAL therefore reads `0000-00-00 00:00:00`, matching
    /// `expression/cast`'s `(0, 0, 0)` row, while a zero-date STRING is still
    /// rejected to NULL.
    #[test]
    fn a_numeric_zero_is_the_zero_time_not_null() {
        let zero = "0000-00-00 00:00:00";
        assert_eq!(datetime(Datum::Int(0)), zero, "int 0");
        assert_eq!(datetime(Datum::UInt(0)), zero, "uint 0");
        assert_eq!(datetime(Datum::Real(0.0)), zero, "real 0");
        assert_eq!(
            datetime(Datum::Decimal(Decimal::from_literal("0"))),
            zero,
            "decimal 0"
        );
        // The STRING signature keeps Go's zero-date rejection under NO_ZERO_DATE.
        assert_eq!(
            datetime(Datum::new_string("0000-00-00 00:00:00".to_string())),
            "NULL",
            "a zero-date STRING is still rejected"
        );
    }

    /// The STRING path is unchanged: a wall-clock literal still parses as text.
    #[test]
    fn a_string_source_is_unchanged() {
        assert_eq!(
            datetime(Datum::new_string("2017-01-18 12:34:56".to_string())),
            "2017-01-18 12:34:56",
        );
    }

    #[test]
    fn temporal_target_fsp_rounds_at_boundaries() {
        let text = |s: &str| Datum::new_string(s.to_owned());
        assert_eq!(
            datetime_fsp(text("2020-02-03 11:22:33.987654"), 3),
            "2020-02-03 11:22:33.988"
        );
        assert_eq!(
            datetime_fsp(text("2020-01-01 23:59:59.5"), 0),
            "2020-01-02 00:00:00"
        );
        assert_eq!(
            datetime_fsp(text("2020-01-01 23:59:59.5"), 1),
            "2020-01-01 23:59:59.5"
        );
        let through_dispatch = eval_cast(
            &CastType::DateTime { fsp: Some(3) },
            text("2020-02-03 11:22:33.987654"),
            None,
            &NoColumns,
        )
        .expect("CAST dispatch");
        assert_eq!(render_time(&through_dispatch), "2020-02-03 11:22:33.988");
    }

    #[test]
    fn temporal_cast_preserves_value_and_zero_date_fields() {
        let got = cast_to_time(
            &Datum::new_string("0000-01-02 03:04:05".to_owned()),
            None,
            &NoColumns,
            tidb_datatype::TimeType::DateTime,
            0,
        )
        .expect("cast");
        assert!(matches!(got, Datum::String(_)));
        assert_eq!(render_time(&got), "0000-01-02 03:04:05");

        let date = cast_to_time(
            &Datum::new_string("2020-01-01 10:30:00".to_owned()),
            None,
            &NoColumns,
            tidb_datatype::TimeType::Date,
            0,
        )
        .expect("cast");
        assert_eq!(render_time(&date), "2020-01-01");
    }

    #[test]
    fn duration_to_date_keeps_go_visible_date() {
        struct AtMidnight;
        impl crate::Columns for AtMidnight {
            fn get(&self, _: &[String]) -> Option<Datum> {
                None
            }
            fn now(&self) -> Option<(i64, u32, i32)> {
                Some((1_785_974_400, 0, 0))
            }
        }
        let duration = Datum::Duration(
            tidb_datatype::MySqlDuration::new(12, 34, 56, 789_000, 3).expect("duration"),
        );
        let date = cast_to_time(
            &duration,
            None,
            &AtMidnight,
            tidb_datatype::TimeType::Date,
            0,
        )
        .expect("cast");
        assert_eq!(render_time(&date), "2026-08-06");
    }
}

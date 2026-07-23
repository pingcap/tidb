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
//! `TIME`/`JSON` targets are deliberately `Unsupported` — see
//! `tidb_ast::CastType`'s own doc for why (no `TIME`/`JSON` value domain
//! exists in this crate). Every other rule here (string-to-number prefix
//! parsing width, rounding tie-breaking per source type, `UNSIGNED`'s
//! negative-float-clamps-to-zero rule, `DECIMAL`'s precision clamp,
//! `BINARY`'s NUL-padding) was confirmed via `goeval`, not assumed — see
//! each function's own doc for the specific probe.

use crate::coerce::coerce_str;
use crate::time_fn::calendar::{
    format_ymd_result, format_ymdhms_result, parse_date_ymd, parse_time_hms,
};
use crate::Decimal;
use crate::{Datum, EvalError};
use tidb_ast::CastType;

/// Evaluates a [`CastType`] against an already-evaluated, non-`NULL`
/// operand (`NULL` is handled by the caller — every target type maps
/// `NULL` to `NULL`, so there's no per-type NULL case to write here).
pub(crate) fn eval_cast(cast_type: &CastType, v: Datum) -> Result<Datum, EvalError> {
    if v.is_range_sentinel() {
        return Err(EvalError::Unsupported("range sentinel cast operand"));
    }
    match cast_type {
        CastType::Signed => Ok(Datum::Int(to_i64_signed(&v))),
        CastType::Unsigned => Ok(Datum::UInt(to_u64_unsigned(&v))),
        CastType::Char { len, .. } => {
            let text = datum_sql_string(&v)?;
            Ok(Datum::new_string(match len {
                Some(n) => text.chars().take(*n as usize).collect(),
                None => text,
            }))
        }
        CastType::Binary { len } => {
            // Go's binary cast is byte-oriented and preserves arbitrary
            // octets.  Do not route an already-byte-valued operand through
            // UTF-8 decoding: `CAST('你好world' AS BINARY(5))` deliberately
            // keeps the first five bytes, even though that suffix is not a
            // complete UTF-8 sequence (see `TestCastFunctions`).
            let bytes = datum_binary_bytes(&v)?;
            Ok(Datum::new_bytes(match len {
                Some(n) => binary_pad_truncate(&bytes, *n as usize),
                None => bytes,
            }))
        }
        CastType::Decimal { flen, scale } => Ok(Datum::Decimal(
            to_decimal_for_cast(&v).cast_to_precision(*flen, *scale),
        )),
        CastType::Date => cast_to_date(&v),
        CastType::DateTime { .. } => cast_to_datetime(&v),
        CastType::Year => cast_to_year(&v),
        CastType::Double | CastType::Float => Ok(Datum::Real(to_f64_for_cast(&v))),
        CastType::Time { .. } => Err(EvalError::Unsupported("CAST AS TIME")),
        CastType::Json => Err(EvalError::Unsupported("CAST AS JSON")),
    }
}

fn datum_sql_string(value: &Datum) -> Result<String, EvalError> {
    value
        .sql_string()
        .map_err(|_| EvalError::Unsupported("invalid UTF-8 string coercion"))
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
    match v {
        Datum::Int(i) => *i,
        Datum::UInt(i) => *i as i64,
        Datum::Decimal(d) => d.round_to_i64_saturating(),
        Datum::Real(f) => f.round_ties_even() as i64,
        Datum::String(s) => s.as_utf8().map(str_int_prefix).unwrap_or(0),
        Datum::Bytes(s) => std::str::from_utf8(s).map(str_int_prefix).unwrap_or(0),
        Datum::Null | Datum::MinNotNull | Datum::MaxValue => unreachable!("guarded by caller"),
        other => other.to_i64().map_or(0, |converted| converted.value),
    }
}

/// `UNSIGNED`'s own coercion. Integer and integer-string sources preserve
/// the low 64 bits, so `CAST(-5 AS UNSIGNED)` is the genuine
/// `18446744073709551611` UInt64 value. A decimal source rounds half-up then
/// converts across the full `u64` range (Go `MyDecimal.ToUint`: negative -> 0);
/// a float source rounds half-to-even then converts across the same full `u64`
/// range (Go `ConvertFloatToUint`: negative -> 0). The result is
/// [`Datum::UInt`], so downstream comparisons and arithmetic retain the domain
/// instead of silently reinterpreting it as signed display text.
fn to_u64_unsigned(v: &Datum) -> u64 {
    match v {
        // TiDB's integer cast reuses the low 64 bits for an ETInt source.
        // That is observable for `CAST(-5 AS UNSIGNED)`, which is
        // 18446744073709551611 rather than an error or a display-only wrap.
        Datum::Int(_) | Datum::String(_) | Datum::Bytes(_) => to_i64_signed(v) as u64,
        Datum::UInt(i) => *i,
        // A decimal rounds half-up then converts through the full u64 range
        // (Go `MyDecimal.ToUint`): a negative value becomes 0, and a magnitude in
        // `(i64::MAX, u64::MAX]` — the upper half of `UNSIGNED BIGINT` — is kept
        // rather than saturated at `i64::MAX` by the signed path.
        Datum::Decimal(d) => d.round_to_u64_saturating(),
        // A real rounds half-to-even then converts across the full u64 range
        // (Go `ConvertFloatToUint`), so its own upper half is kept too.
        Datum::Real(f) => real_to_u64_saturating(*f),
        Datum::Null | Datum::MinNotNull | Datum::MaxValue => unreachable!("guarded by caller"),
        other => other
            .to_decimal()
            .map_or(0, |converted| converted.value.round_to_u64_saturating()),
    }
}

/// `CAST(real AS UNSIGNED)`: round half to even (Go `RoundFloat` =
/// `math.RoundToEven`, the same rounding the signed real path uses), then Go
/// `ConvertFloatToUint` across the full `u64` range. A negative value is `0`
/// under the default flags, and a magnitude past `u64::MAX` saturates to
/// `u64::MAX` (`ConvertFloatToUint`'s `upperBound` clamp). Routing through the
/// signed path instead would lose the upper half of `UNSIGNED BIGINT` at
/// `i64::MAX`.
fn real_to_u64_saturating(f: f64) -> u64 {
    let rounded = f.round_ties_even();
    if rounded < 0.0 {
        // A negative rounded value clamps to zero (goeval-confirmed for the
        // real source, unlike an integer source's low-64-bit reinterpretation).
        0
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

/// `CAST(... AS DATE)`: parses the operand's calendar date, IGNORING any
/// trailing time-of-day (exactly like every other DATE-truncating
/// function in `crate::time_fn::calendar` already does — confirmed via `goeval`:
/// `CAST('2021-01-01 10:30:00' AS DATE)` is `2021-01-01`, the time simply
/// dropped, not validated). `NULL` if the operand doesn't coerce to a
/// string or doesn't parse as a date.
fn cast_to_date(v: &Datum) -> Result<Datum, EvalError> {
    Ok(match coerce_str(v)?.and_then(|s| parse_date_ymd(&s)) {
        Some((y, m, d)) => format_ymd_result(y, m, d, None),
        None => Datum::Null,
    })
}

/// `CAST(... AS DATETIME)`: like [`cast_to_date`], but ALSO parses a
/// trailing time-of-day, defaulting to midnight if the input has none —
/// mirrors `time_fn::calendar::date_add`'s own date/time-suffix splitting exactly.
/// A time suffix that's PRESENT but doesn't parse as `HH:MM:SS` makes the
/// WHOLE cast `NULL` (confirmed via `goeval`), not just the time part.
fn cast_to_datetime(v: &Datum) -> Result<Datum, EvalError> {
    let Some(s) = coerce_str(v)? else {
        return Ok(Datum::Null);
    };
    let trimmed = s.trim();
    let (date_str, time_suffix) = trimmed
        .split_once(char::is_whitespace)
        .map_or((trimmed, None), |(d, t)| (d, Some(t)));
    let Some((y, m, d)) = parse_date_ymd(date_str) else {
        return Ok(Datum::Null);
    };
    let (h, mi, sec) = match time_suffix {
        Some(t) => match parse_time_hms(t.trim_start()) {
            Some(hms) => hms,
            None => return Ok(Datum::Null),
        },
        None => (0, 0, 0),
    };
    Ok(format_ymdhms_result(y, m, d, h, mi, sec))
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

    #[test]
    fn cast_decimal_as_unsigned_keeps_the_upper_half_of_unsigned_bigint() {
        // The wired bug: routing a decimal through the signed path saturated the
        // upper half of UNSIGNED BIGINT at i64::MAX (9223372036854775807). Go
        // rounds half-up then MyDecimal.ToUint, keeping the full u64 range.
        assert_eq!(
            to_u64_unsigned(&Datum::Decimal(Decimal::from_literal(
                "10000000000000000000"
            ))),
            10_000_000_000_000_000_000,
            "one past i64::MAX is kept, not saturated"
        );
        assert_eq!(
            to_u64_unsigned(&Datum::Decimal(Decimal::from_literal(
                "18446744073709551615"
            ))),
            u64::MAX
        );
        // Half-up rounding and the negative-to-zero rule are unchanged.
        assert_eq!(
            to_u64_unsigned(&Datum::Decimal(Decimal::from_literal("5.6"))),
            6
        );
        assert_eq!(
            to_u64_unsigned(&Datum::Decimal(Decimal::from_literal("5.6").negate())),
            0
        );
        // A signed-integer source still reinterprets its low 64 bits:
        // CAST(-5 AS UNSIGNED) stays 18446744073709551611, unaffected by the fix.
        assert_eq!(to_u64_unsigned(&Datum::Int(-5)), 18_446_744_073_709_551_611);
    }

    #[test]
    fn cast_real_as_unsigned_keeps_the_upper_half_of_unsigned_bigint() {
        // The sibling wired bug: a real routed through the signed path saturated
        // the upper half of UNSIGNED BIGINT at i64::MAX (9223372036854775807).
        // Go rounds half-to-even (RoundFloat) then ConvertFloatToUint across the
        // full u64 range. 1e19 is exactly representable in f64.
        assert_eq!(
            to_u64_unsigned(&Datum::Real(1.0e19)),
            10_000_000_000_000_000_000,
            "a real past i64::MAX is kept, not saturated at i64::MAX"
        );
        // A magnitude past u64::MAX saturates to MaxUint64 (upperBound clamp).
        assert_eq!(to_u64_unsigned(&Datum::Real(1.0e30)), u64::MAX);
        // Half-to-even rounding (Go RoundFloat = math.RoundToEven), the same rule
        // the signed real path uses: 2.5 -> 2, 3.5 -> 4.
        assert_eq!(to_u64_unsigned(&Datum::Real(2.5)), 2);
        assert_eq!(to_u64_unsigned(&Datum::Real(3.5)), 4);
        // A negative real clamps to zero under the default flags.
        assert_eq!(to_u64_unsigned(&Datum::Real(-5.6)), 0);
        assert_eq!(to_u64_unsigned(&Datum::Real(-0.4)), 0);
    }
}

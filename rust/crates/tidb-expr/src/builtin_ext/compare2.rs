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

//! `compare2` family builtins — see `super`'s doc for the dispatch contract
//! and `rust/PARALLEL.md` for ownership. Every builtin here is a faithful
//! port of its Go implementation in `pkg/expression/builtin_*.go`, cited
//! per function.

use std::cmp::Ordering;
use std::net::{Ipv4Addr, Ipv6Addr};
use std::str::FromStr;

use tidb_ast::BinaryOp;

use crate::coerce::{coerce_str, integer_cmp, integer_of};
use crate::ops::{to_decimal, to_f64};
use crate::{eval_binary, Datum, EvalError};

/// Dispatches this family's builtins; `None` if `name` isn't one of them.
pub(crate) fn dispatch(name: &str, vals: &[Datum]) -> Option<Result<Datum, EvalError>> {
    match (name, vals.len()) {
        ("LEAST", _) => Some(extremum(vals, Ordering::Less)),
        ("GREATEST", _) => Some(extremum(vals, Ordering::Greater)),
        ("INTERVAL", n) if n >= 2 => Some(interval(vals)),
        ("ISNULL", 1) => Some(Ok(Datum::Int(i64::from(matches!(vals[0], Datum::Null))))),
        ("INET_ATON", 1) => Some(inet_aton(&vals[0])),
        ("INET_NTOA", 1) => Some(inet_ntoa(&vals[0])),
        ("INET6_ATON", 1) => Some(inet6_aton(&vals[0])),
        ("INET6_NTOA", 1) => Some(inet6_ntoa(&vals[0])),
        ("IS_IPV4", 1) => Some(is_ipv4_value(&vals[0])),
        ("IS_IPV4_MAPPED", 1) => Some(is_ipv4_mapped_value(&vals[0])),
        ("IS_IPV4_COMPAT", 1) => Some(is_ipv4_compat_value(&vals[0])),
        ("IS_IPV6", 1) => Some(is_ipv6_value(&vals[0])),
        _ => None,
    }
}

/// LEAST/GREATEST: `NULL` if any argument is `NULL`, else the extreme value
/// by `want` (`Less` for LEAST, `Greater` for GREATEST) — a MIXED
/// Int/Decimal/Float argument list promotes through `eval_binary`'s own
/// comparison (confirmed via goeval: `GREATEST(1.5e2, 3.14, 2)` — Float,
/// Decimal, Int all in one call — is `FLOAT:150`, not an error), so no
/// per-type-pair matching is hand-rolled here. When a string is present,
/// Go's FieldType aggregation selects the string signature and stringifies
/// every argument before comparing it; the byte-preserving scalar path below
/// keeps that source boundary without inventing a numeric coercion. Port of
/// the signatures built by `leastFunctionClass` and
/// `greatestFunctionClass` in `pkg/expression/builtin_compare.go`.
fn extremum(vals: &[Datum], want: Ordering) -> Result<Datum, EvalError> {
    if vals.is_empty() {
        return Err(EvalError::Unsupported("bad function arity"));
    }
    if vals.contains(&Datum::Null) {
        return Ok(Datum::Null);
    }
    // A string operand makes aggregateType choose ETString in Go, so numeric
    // values are first rendered with EvalString and then compared under the
    // string collation. This directly covers TestGreatestLeastFunc's
    // `("123a", "b", "c", 12)` row: GREATEST is `"c"`, LEAST is `"12"`.
    // Keep raw bytes for existing String/Bytes values; only scalar numerics
    // need textual rendering here.
    if vals
        .iter()
        .any(|v| matches!(v, Datum::String(_) | Datum::Bytes(_)))
    {
        let mut best = extremum_string_value(&vals[0])?;
        for v in &vals[1..] {
            let candidate = extremum_string_value(v)?;
            if (want == Ordering::Greater && candidate > best)
                || (want == Ordering::Less && candidate < best)
            {
                best = candidate;
            }
        }
        return Ok(Datum::new_string(best));
    }
    let op = if want == Ordering::Greater {
        BinaryOp::Gt
    } else {
        BinaryOp::Lt
    };
    let mut best = vals[0].clone();
    for v in &vals[1..] {
        if eval_binary(op, v.clone(), best.clone())? == Datum::Int(1) {
            best = v.clone();
        }
    }
    // The RESULT promotes to the widest type among ALL arguments (Float >
    // Decimal > Int, same hierarchy `+`/`-` use) — not just whichever raw
    // value happened to win the comparison (a real bug caught by the
    // differential corpus, not assumed correct on the first attempt:
    // `LEAST(1.5e2, 3.14, 2)` is `FLOAT:2`, not `INT:2`, even though the
    // winning argument `2` was written as a bare Int literal).
    if vals.iter().any(|v| matches!(v, Datum::Real(_))) {
        return Ok(Datum::Real(to_f64(best)));
    }
    if vals.iter().any(|v| matches!(v, Datum::Decimal(_))) {
        return Ok(Datum::Decimal(to_decimal(best)));
    }
    // TiDB's common numeric type for a mixed signed/unsigned integer list is
    // DECIMAL. Preserve that result domain even when the winning value is an
    // integer: GREATEST(1, CAST(2 AS UNSIGNED)) is DEC:2, not UINT:2.
    if vals.iter().any(|v| matches!(v, Datum::Int(_)))
        && vals.iter().any(|v| matches!(v, Datum::UInt(_)))
    {
        return Ok(Datum::Decimal(to_decimal(best)));
    }
    Ok(best)
}

fn extremum_string_value(value: &Datum) -> Result<Vec<u8>, EvalError> {
    Ok(match value {
        Datum::String(value) => value.bytes().to_vec(),
        Datum::Bytes(value) => value.clone(),
        Datum::Int(value) => value.to_string().into_bytes(),
        Datum::UInt(value) => value.to_string().into_bytes(),
        Datum::Decimal(value) => value.to_string().into_bytes(),
        Datum::Real(value) => value.to_string().into_bytes(),
        Datum::Null => return Err(EvalError::Unsupported("NULL string operand")),
        Datum::MinNotNull | Datum::MaxValue => {
            return Err(EvalError::Unsupported("range sentinel string operand"));
        }
    })
}

/// `INTERVAL(n, n1, n2, ...)`: return the zero-based position of the first
/// boundary greater than `n`, or the number of boundaries when none is
/// greater. Port of `builtinIntervalIntSig.evalInt` and
/// `builtinIntervalRealSig.evalInt` in `pkg/expression/builtin_compare.go`.
///
/// Like TiDB, an integer-only call uses its exact integer signature; the
/// presence of any decimal, float, or string selects the lossy real
/// signature. A NULL target is `-1`; NULL boundaries participate only in the
/// nullable signature and are skipped. The binary search intentionally keeps
/// TiDB's documented precondition that non-NULL boundaries are sorted.
fn interval(vals: &[Datum]) -> Result<Datum, EvalError> {
    if vals.iter().any(Datum::is_range_sentinel) {
        return Err(EvalError::Unsupported("range sentinel INTERVAL argument"));
    }
    if vals[0] == Datum::Null {
        return Ok(Datum::Int(-1));
    }
    let nullable = vals.iter().any(|v| matches!(v, Datum::Null));
    if vals
        .iter()
        .all(|v| matches!(v, Datum::Int(_) | Datum::UInt(_) | Datum::Null))
    {
        let target = match integer_of(&vals[0])? {
            Some(value) => value,
            None => unreachable!("all-int guard"),
        };
        let index = if nullable {
            vals[1..]
                .iter()
                .position(|v| {
                    integer_of(v)
                        .expect("range sentinels rejected above")
                        .is_some_and(|boundary| integer_cmp(target, boundary).is_lt())
                })
                .unwrap_or(vals.len() - 1)
        } else {
            vals[1..].partition_point(|v| {
                integer_of(v)
                    .expect("range sentinels rejected above")
                    .is_some_and(|boundary| integer_cmp(boundary, target).is_le())
            })
        };
        return Ok(Datum::Int(index as i64));
    }

    let target = interval_real(&vals[0]);
    let index = if nullable {
        let mut index = vals.len() - 1;
        for (offset, boundary) in vals[1..].iter().enumerate() {
            if !matches!(boundary, Datum::Null) && target < interval_real(boundary) {
                index = offset;
                break;
            }
        }
        index
    } else {
        vals[1..].partition_point(|boundary| interval_real(boundary) <= target)
    };
    Ok(Datum::Int(index as i64))
}

/// TiDB's real signature evaluates every argument as `ETReal`. `Datum` has
/// no type metadata, so this local conversion is limited to the scalar types
/// it can represent; the string path implements MySQL's numeric-prefix rule
/// required by `TestIntervalFunc` (`'b'` becomes `0`).
fn interval_real(value: &Datum) -> f64 {
    match value {
        Datum::Int(value) => *value as f64,
        Datum::UInt(value) => *value as f64,
        Datum::Decimal(value) => value.to_f64(),
        Datum::Real(value) => *value,
        Datum::String(value) => value.as_utf8().map(mysql_real_prefix).unwrap_or(0.0),
        Datum::Bytes(value) => std::str::from_utf8(value)
            .map(mysql_real_prefix)
            .unwrap_or(0.0),
        Datum::Null | Datum::MinNotNull | Datum::MaxValue => {
            unreachable!("non-scalar boundaries are rejected before conversion")
        }
    }
}

/// Parses the numeric prefix MySQL uses when an `ETString` is evaluated as a
/// real. No prefix (including an empty string) is numeric zero.
fn mysql_real_prefix(text: &str) -> f64 {
    let text = text.trim_start();
    let bytes = text.as_bytes();
    let mut end = 0;
    if matches!(bytes.first(), Some(b'+' | b'-')) {
        end = 1;
    }
    let integer_start = end;
    while matches!(bytes.get(end), Some(b'0'..=b'9')) {
        end += 1;
    }
    let mut has_digits = end > integer_start;
    if matches!(bytes.get(end), Some(b'.')) {
        let decimal = end;
        end += 1;
        let fractional_start = end;
        while matches!(bytes.get(end), Some(b'0'..=b'9')) {
            end += 1;
        }
        has_digits |= end > fractional_start;
        if !has_digits {
            end = decimal;
        }
    }
    if has_digits && matches!(bytes.get(end), Some(b'e' | b'E')) {
        let exponent = end;
        end += 1;
        if matches!(bytes.get(end), Some(b'+' | b'-')) {
            end += 1;
        }
        let exponent_start = end;
        while matches!(bytes.get(end), Some(b'0'..=b'9')) {
            end += 1;
        }
        if exponent_start == end {
            end = exponent;
        }
    }
    if !has_digits {
        return 0.0;
    }
    // `types.StrToFloat` saturates an overflowing `ParseFloat` result to the
    // corresponding finite bound after recording the truncation warning. The
    // INTERVAL test runs with truncation ignored, so the value used for the
    // comparison is still `MAXFLOAT`/`-MAXFLOAT`, not zero. Rust's parser
    // reports the overflow as an error, but this prefix is already known to
    // be syntactically numeric; therefore every parse error here is that
    // range condition.
    text[..end].parse().unwrap_or_else(|_| {
        if text.as_bytes().first() == Some(&b'-') {
            -f64::MAX
        } else {
            f64::MAX
        }
    })
}

/// `INET_ATON(expr)`: decimal dotted IPv4 to an unsigned 32-bit integer,
/// including TiDB/MySQL's one-, two-, and three-component shorthand. Port of
/// `builtinInetAtonSig.evalInt` in `pkg/expression/builtin_miscellaneous.go`.
/// Invalid non-NULL input is an evaluation error in TiDB's strict-context
/// unit test; this family has no matching generic SQL-error variant, so it is
/// surfaced as `Unsupported` until the frozen `EvalError` domain grows one.
fn inet_aton(value: &Datum) -> Result<Datum, EvalError> {
    let Some(text) = coerce_str(value)? else {
        return Ok(Datum::Null);
    };
    if text.is_empty() || text.ends_with('.') {
        return Err(EvalError::Unsupported("invalid INET_ATON address"));
    }
    let mut result = 0_u64;
    let mut byte_result = 0_u64;
    let mut dots = 0_u8;
    for byte in text.bytes() {
        match byte {
            b'0'..=b'9' => {
                byte_result = byte_result * 10 + u64::from(byte - b'0');
                if byte_result > 255 {
                    return Err(EvalError::Unsupported("invalid INET_ATON address"));
                }
            }
            b'.' => {
                dots += 1;
                if dots > 3 {
                    return Err(EvalError::Unsupported("invalid INET_ATON address"));
                }
                result = (result << 8) + byte_result;
                byte_result = 0;
            }
            _ => return Err(EvalError::Unsupported("invalid INET_ATON address")),
        }
    }
    if dots == 1 {
        result <<= 8;
    }
    if dots <= 2 {
        result <<= 8;
    }
    Ok(Datum::UInt((result << 8) + byte_result))
}

/// `INET_NTOA(expr)`: unsigned 32-bit integer to canonical dotted IPv4.
/// Port of `builtinInetNtoaSig.evalString` in
/// `pkg/expression/builtin_miscellaneous.go`. The scalar domain has no
/// planning-time `ETInt` cast, so only its already-integer values are
/// representable faithfully; other types remain honestly unsupported.
fn inet_ntoa(value: &Datum) -> Result<Datum, EvalError> {
    let value = match value {
        Datum::Null => return Ok(Datum::Null),
        Datum::Int(value) => *value as u64,
        Datum::UInt(value) => *value,
        _ => return Err(EvalError::Unsupported("INET_NTOA non-integer argument")),
    };
    let Ok(value) = u32::try_from(value) else {
        return Ok(Datum::Null);
    };
    Ok(Datum::new_string(format!(
        "{}.{}.{}.{}",
        value >> 24,
        (value >> 16) & 0xff,
        (value >> 8) & 0xff,
        value & 0xff
    )))
}

/// `INET6_ATON(expr)`: parse an IPv4 or IPv6 spelling into the raw network
/// byte representation used by TiDB's binary `ETString` signature.  Go's
/// `net.ParseIP` always returns a 16-byte value for a colon-containing
/// spelling, including IPv4-mapped and IPv4-compatible forms; the source
/// then keeps the four-byte representation only for a plain dotted IPv4
/// input.  Port of `builtinInet6AtonSig.evalString` in
/// `pkg/expression/builtin_miscellaneous.go`.
fn inet6_aton(value: &Datum) -> Result<Datum, EvalError> {
    let text = match value {
        Datum::Null => return Ok(Datum::Null),
        // `EvalString` in the Go signature preserves raw bytes.  IP syntax is
        // ASCII, so invalid UTF-8 is simply the same parse failure rather than
        // a lossy replacement conversion.
        Datum::String(value) => std::str::from_utf8(value.bytes()),
        Datum::Bytes(value) => std::str::from_utf8(value),
        _ => return inet6_aton_text(&coerce_str(value)?.expect("non-NULL scalar")),
    }
    .map_err(|_| EvalError::Unsupported("invalid INET6_ATON address"))?;
    inet6_aton_text(text)
}

fn inet6_aton_text(text: &str) -> Result<Datum, EvalError> {
    if text.is_empty() {
        return Err(EvalError::Unsupported("invalid INET6_ATON address"));
    }
    // Keep the source's four-byte result only when the original spelling is
    // plain IPv4.  `Ipv6Addr::from_str` handles all colon-containing forms,
    // including embedded IPv4 and mapped IPv4, and its octets are exactly the
    // bytes Go copies from `ip.To16()`/`ip.To4()`.
    if !text.contains(':') {
        if let Ok(ip) = Ipv4Addr::from_str(text) {
            return Ok(Datum::new_bytes(ip.octets()));
        }
    }
    Ipv6Addr::from_str(text)
        .map(|ip| Datum::new_bytes(ip.octets()))
        .map_err(|_| EvalError::Unsupported("invalid INET6_ATON address"))
}

/// `INET6_NTOA(expr)`: render a four- or sixteen-byte binary string as the
/// canonical textual IPv4/IPv6 spelling.  Go first asks `net.IP.String()` to
/// format the bytes and then prefixes a sixteen-byte mapped IPv4 result with
/// `::ffff:`; Rust's `Ipv6Addr` formatter emits that same canonical mapped
/// spelling.  Any other byte length is SQL `NULL`, matching the source's
/// `net.ParseIP(ip) == nil` branch.  Port of
/// `builtinInet6NtoaSig.evalString` in `pkg/expression/builtin_miscellaneous.go`.
fn inet6_ntoa(value: &Datum) -> Result<Datum, EvalError> {
    let bytes = match value {
        Datum::Null => return Ok(Datum::Null),
        Datum::String(value) => value.bytes().to_vec(),
        Datum::Bytes(value) => value.clone(),
        // The Go function's argument is ETString, so numeric constants are
        // first rendered by EvalString and then interpreted as raw bytes.
        _ => coerce_str(value)?.expect("non-NULL scalar").into_bytes(),
    };
    match bytes.len() {
        4 => Ok(Datum::new_string(
            Ipv4Addr::new(bytes[0], bytes[1], bytes[2], bytes[3]).to_string(),
        )),
        16 => Ok(Datum::new_string(
            Ipv6Addr::from(<[u8; 16]>::try_from(bytes).unwrap()).to_string(),
        )),
        _ => Ok(Datum::Null),
    }
}

/// `IS_IPV4(expr)`: strict four-component decimal IPv4 predicate. Port of
/// `builtinIsIPv4Sig.evalInt` and its `isIPv4` helper in
/// `pkg/expression/builtin_miscellaneous.go`.
fn is_ipv4_value(value: &Datum) -> Result<Datum, EvalError> {
    let Some(value) = coerce_str(value)? else {
        return Ok(Datum::Null);
    };
    Ok(Datum::Int(i64::from(is_ipv4(&value))))
}

fn is_ipv4(value: &str) -> bool {
    let mut dots = 0;
    let mut component = 0_u16;
    let mut previous_dot = true;
    for byte in value.bytes() {
        match byte {
            b'0'..=b'9' => {
                // Only whether the component exceeds 255 matters. Saturate
                // rather than letting an arbitrarily long invalid component
                // overflow Rust's debug arithmetic before we reject it.
                component = component
                    .saturating_mul(10)
                    .saturating_add(u16::from(byte - b'0'));
                previous_dot = false;
            }
            b'.' => {
                dots += 1;
                if dots > 3 || component > 255 || previous_dot {
                    return false;
                }
                component = 0;
                previous_dot = true;
            }
            _ => return false,
        }
    }
    dots == 3 && component <= 255 && !previous_dot
}

/// `IS_IPV4_MAPPED(expr)`: true only for a sixteen-byte binary payload whose
/// first twelve bytes are the IPv4-mapped prefix (`::ffff:`).  The Go
/// signature receives an ETString and tests the raw bytes directly; keeping
/// this helper byte-oriented is important because arbitrary SQL strings are
/// allowed to contain invalid UTF-8.  Port of
/// `builtinIsIPv4MappedSig.evalInt` in `pkg/expression/builtin_miscellaneous.go`.
fn is_ipv4_mapped_value(value: &Datum) -> Result<Datum, EvalError> {
    let Some(bytes) = eval_string_bytes(value)? else {
        return Ok(Datum::Null);
    };
    let mapped = bytes.len() == 16 && bytes[..12] == [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0xff, 0xff];
    Ok(Datum::Int(i64::from(mapped)))
}

/// `IS_IPV4_COMPAT(expr)`: true only for a sixteen-byte binary payload whose
/// first twelve bytes are all zero (`::/96`, excluding the mapped `::ffff:`
/// prefix by construction).  Port of `builtinIsIPv4CompatSig.evalInt` in
/// `pkg/expression/builtin_miscellaneous.go`.
fn is_ipv4_compat_value(value: &Datum) -> Result<Datum, EvalError> {
    let Some(bytes) = eval_string_bytes(value)? else {
        return Ok(Datum::Null);
    };
    let compat = bytes.len() == 16 && bytes[..12] == [0; 12];
    Ok(Datum::Int(i64::from(compat)))
}

/// Evaluates the ETString argument used by the Go IPv4 binary predicates
/// without decoding or replacing arbitrary bytes.  Numeric constants still
/// follow the normal EvalString coercion used by the source signature.
fn eval_string_bytes(value: &Datum) -> Result<Option<Vec<u8>>, EvalError> {
    match value {
        Datum::Null => Ok(None),
        Datum::String(value) => Ok(Some(value.bytes().to_vec())),
        Datum::Bytes(value) => Ok(Some(value.clone())),
        _ => Ok(coerce_str(value)?.map(|text| text.into_bytes())),
    }
}

/// `IS_IPV6(expr)`: true for a parseable IPv6 address, including an IPv4
/// mapped spelling, but false for a pure IPv4 address. Port of
/// `builtinIsIPv6Sig.evalInt` in `pkg/expression/builtin_miscellaneous.go`.
fn is_ipv6_value(value: &Datum) -> Result<Datum, EvalError> {
    let Some(value) = coerce_str(value)? else {
        return Ok(Datum::Null);
    };
    Ok(Datum::Int(i64::from(
        Ipv6Addr::from_str(&value).is_ok() && !is_ipv4(&value),
    )))
}

#[cfg(test)]
mod tests {
    use super::dispatch;
    use crate::Datum;
    use crate::Decimal;

    fn call(name: &str, vals: &[Datum]) -> Datum {
        dispatch(name, vals)
            .expect("name/arity should dispatch to compare2")
            .expect("Go vector should evaluate")
    }

    fn s(value: &str) -> Datum {
        Datum::new_string(value.to_string())
    }

    /// `TestIntervalFunc` vectors that fit the current signed `Datum` domain.
    #[test]
    fn interval_go_vectors() {
        let cases: &[(Vec<Datum>, i64)] = &[
            (vec![Datum::Null, Datum::Int(1), Datum::Int(2)], -1),
            (vec![Datum::Int(1), Datum::Int(2), Datum::Int(3)], 0),
            (vec![Datum::Int(2), Datum::Int(1), Datum::Int(3)], 1),
            (vec![Datum::Int(3), Datum::Int(1), Datum::Int(2)], 2),
            (vec![Datum::Int(0), s("b"), s("1"), s("2")], 1),
            (vec![s("a"), s("b"), s("1"), s("2")], 1),
            (
                vec![
                    Datum::Int(23),
                    Datum::Int(1),
                    Datum::Int(23),
                    Datum::Int(23),
                    Datum::Int(23),
                    Datum::Int(30),
                    Datum::Int(44),
                    Datum::Int(200),
                ],
                4,
            ),
            (
                vec![
                    Datum::Int(23),
                    Datum::Decimal(Decimal::from_literal("1.7")),
                    Datum::Decimal(Decimal::from_literal("15.3")),
                    Datum::Decimal(Decimal::from_literal("23.1")),
                    Datum::Int(30),
                    Datum::Int(44),
                    Datum::Int(200),
                ],
                2,
            ),
            (
                vec![
                    Datum::Int(9_007_199_254_740_992),
                    Datum::Int(9_007_199_254_740_993),
                ],
                0,
            ),
            (
                vec![
                    Datum::UInt(9_223_372_036_854_775_808),
                    Datum::UInt(9_223_372_036_854_775_809),
                ],
                0,
            ),
            (
                vec![Datum::Int(i64::MAX), Datum::UInt(9_223_372_036_854_775_808)],
                0,
            ),
            (
                vec![
                    Datum::Int(-9_223_372_036_854_775_807),
                    Datum::UInt(9_223_372_036_854_775_808),
                ],
                0,
            ),
            (
                vec![Datum::UInt(9_223_372_036_854_775_806), Datum::Int(i64::MAX)],
                0,
            ),
            (
                vec![
                    Datum::UInt(9_223_372_036_854_775_806),
                    Datum::Int(-9_223_372_036_854_775_807),
                ],
                1,
            ),
            (vec![Datum::Int(-1), Datum::Int(2333), Datum::Null], 0),
            (
                vec![Datum::Int(1), Datum::Null, Datum::Null, Datum::Null],
                3,
            ),
            (
                vec![
                    Datum::Int(1),
                    Datum::Null,
                    Datum::Null,
                    Datum::Null,
                    Datum::Int(2),
                ],
                3,
            ),
            (
                vec![Datum::Int(9_007_199_254_740_992), s("9007199254740993")],
                1,
            ),
            (
                vec![s("9007199254740992"), Datum::Int(9_007_199_254_740_993)],
                1,
            ),
            (vec![s("9007199254740992"), s("9007199254740993")], 1),
            // Go's StrToFloat saturates an overflowing real conversion to
            // MAXFLOAT (with truncation ignored by TestIntervalFunc). The
            // old Rust fallback returned zero, placing this target before a
            // 1e308 boundary instead of after it.
            (vec![s("1e999"), Datum::Real(1e308)], 1),
        ];
        for (args, want) in cases {
            assert_eq!(call("INTERVAL", args), Datum::Int(*want));
        }
    }

    /// `TestInetAton` exact valid/NULL vectors. Its malformed-input vectors
    /// assert a strict-context TiDB error, which is still an error here.
    #[test]
    fn inet_aton_go_vectors() {
        let cases = [
            (Datum::Null, Datum::Null),
            (s("255.255.255.255"), Datum::UInt(4_294_967_295)),
            (s("0.0.0.0"), Datum::UInt(0)),
            (s("127.0.0.1"), Datum::UInt(2_130_706_433)),
            (s("113.14.22.3"), Datum::UInt(1_896_748_547)),
            (s("127"), Datum::UInt(127)),
            (s("127.255"), Datum::UInt(2_130_706_687)),
            (s("127.2.1"), Datum::UInt(2_130_837_505)),
        ];
        for (arg, want) in cases {
            assert_eq!(call("INET_ATON", &[arg]), want);
        }
        for invalid in ["", "0.0.0.256", "127,256", "123.2.1.", "127.0.0.1.1"] {
            assert!(dispatch("INET_ATON", &[s(invalid)]).unwrap().is_err());
        }
    }

    /// `TestInetNtoa` vectors, including values outside the IPv4 range.
    #[test]
    fn inet_ntoa_go_vectors() {
        let cases = [
            (
                Datum::Int(167_773_449),
                Datum::new_string("10.0.5.9".to_string()),
            ),
            (
                Datum::Int(2_063_728_641),
                Datum::new_string("123.2.0.1".to_string()),
            ),
            (Datum::Int(0), Datum::new_string("0.0.0.0".to_string())),
            (Datum::Int(545_460_846_593), Datum::Null),
            (Datum::Int(-1), Datum::Null),
            (
                Datum::Int(4_294_967_295),
                Datum::new_string("255.255.255.255".to_string()),
            ),
            (Datum::Null, Datum::Null),
        ];
        for (arg, want) in cases {
            assert_eq!(call("INET_NTOA", &[arg]), want);
        }
    }

    /// `TestInet6AtoN` exact source vectors.  The result is binary even when
    /// the input is ordinary dotted IPv4 text: a plain IPv4 spelling uses
    /// four bytes, while every colon-containing spelling uses sixteen.
    #[test]
    fn inet6_aton_go_vectors() {
        let cases = [
            ("0.0.0.0", Datum::new_bytes([0, 0, 0, 0])),
            ("10.0.5.9", Datum::new_bytes([0x0a, 0, 5, 9])),
            (
                "fdfe::5a55:caff:fefa:9089",
                Datum::new_bytes([
                    0xfd, 0xfe, 0, 0, 0, 0, 0, 0, 0x5a, 0x55, 0xca, 0xff, 0xfe, 0xfa, 0x90, 0x89,
                ]),
            ),
            (
                "::ffff:1.2.3.4",
                Datum::new_bytes([0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0xff, 0xff, 1, 2, 3, 4]),
            ),
            ("", Datum::Null),
            ("Not IP address", Datum::Null),
            ("1.0002.3.4", Datum::Null),
            ("1.2.256", Datum::Null),
            (
                "::ffff:255.255.255.255",
                Datum::new_bytes([
                    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
                ]),
            ),
        ];
        for (text, want) in cases {
            match want {
                Datum::Null => assert!(dispatch("INET6_ATON", &[s(text)]).unwrap().is_err()),
                want => assert_eq!(call("INET6_ATON", &[s(text)]), want),
            }
        }
        assert_eq!(call("INET6_ATON", &[Datum::Null]), Datum::Null);
    }

    /// `TestInet6NtoA` exact source vectors, including invalid byte lengths
    /// and the NULL input path.
    #[test]
    fn inet6_ntoa_go_vectors() {
        let cases = [
            (Datum::new_bytes([0, 0, 0, 0]), "0.0.0.0"),
            (Datum::new_bytes([0x0a, 0, 5, 9]), "10.0.5.9"),
            (
                Datum::new_bytes([
                    0xfd, 0xfe, 0, 0, 0, 0, 0, 0, 0x5a, 0x55, 0xca, 0xff, 0xfe, 0xfa, 0x90, 0x89,
                ]),
                "fdfe::5a55:caff:fefa:9089",
            ),
            (
                Datum::new_bytes([0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0xff, 0xff, 1, 2, 3, 4]),
                "::ffff:1.2.3.4",
            ),
            (
                Datum::new_bytes([
                    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
                ]),
                "::ffff:255.255.255.255",
            ),
        ];
        for (value, want) in cases {
            assert_eq!(call("INET6_NTOA", &[value]), s(want));
        }
        for bytes in [Vec::new(), vec![0x0a, 0, 5], vec![0; 15]] {
            assert_eq!(call("INET6_NTOA", &[Datum::new_bytes(bytes)]), Datum::Null);
        }
        assert_eq!(call("INET6_NTOA", &[Datum::Null]), Datum::Null);
    }

    /// `TestIsIPv4` and `TestIsIPv6` vectors, plus their NULL checks.
    #[test]
    fn is_ip_go_vectors() {
        for (ip, want) in [
            ("192.168.1.1", 1),
            ("255.255.255.255", 1),
            ("10.t.255.255", 0),
            ("10.1.2.3.4", 0),
            ("2001:250:207:0:0:eef2::1", 0),
            ("::ffff:1.2.3.4", 0),
            ("1...1", 0),
            ("192.168.1.", 0),
            (".168.1.2", 0),
            ("168.1.2", 0),
            ("1.2.3.4.5", 0),
        ] {
            assert_eq!(call("IS_IPV4", &[s(ip)]), Datum::Int(want));
        }
        assert_eq!(call("IS_IPV4", &[Datum::Null]), Datum::Null);
        for (ip, want) in [
            ("2001:250:207:0:0:eef2::1", 1),
            ("2001:0250:0207:0001:0000:0000:0000:ff02", 1),
            ("2001:250:207::eff2::1，", 0),
            ("192.168.1.1", 0),
            ("::ffff:1.2.3.4", 1),
        ] {
            assert_eq!(call("IS_IPV6", &[s(ip)]), Datum::Int(want));
        }
        assert_eq!(call("IS_IPV6", &[Datum::Null]), Datum::Null);
    }

    /// `TestIsIPv4Mapped` and `TestIsIPv4Compat` operate on raw ETString
    /// bytes, not textual IPv6 spellings.  Keep every source row here,
    /// including the malformed lengths and the SQL NULL path.
    #[test]
    fn is_ipv4_binary_predicate_go_vectors() {
        let mapped_cases = [
            (vec![], 0),
            (vec![0x10, 0x10, 0x10, 0x10], 0),
            (
                vec![0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0xff, 0xff, 1, 2, 3, 4],
                1,
            ),
            (
                vec![0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0xff, 0xff, 1, 2, 3, 4],
                0,
            ),
            (vec![0, 1, 2, 3, 4, 5, 6], 0),
            // Go's EvalString preserves arbitrary bytes; invalid UTF-8 is
            // still just a non-matching binary payload.
            (vec![0xff; 16], 0),
        ];
        for (bytes, want) in mapped_cases {
            assert_eq!(
                call("IS_IPV4_MAPPED", &[Datum::new_bytes(bytes)]),
                Datum::Int(want)
            );
        }
        assert_eq!(call("IS_IPV4_MAPPED", &[Datum::Null]), Datum::Null);

        let compat_cases = [
            (vec![], 0),
            (vec![0x10, 0x10, 0x10, 0x10], 0),
            (vec![0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 2, 3, 4], 1),
            (vec![0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 1, 2, 3, 4], 0),
            (
                vec![0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0xff, 0xff, 1, 2, 3, 4],
                0,
            ),
            (vec![0, 1, 2, 3, 4, 5, 6], 0),
            (vec![0xff; 16], 0),
        ];
        for (bytes, want) in compat_cases {
            assert_eq!(
                call("IS_IPV4_COMPAT", &[Datum::new_bytes(bytes)]),
                Datum::Int(want)
            );
        }
        assert_eq!(call("IS_IPV4_COMPAT", &[Datum::Null]), Datum::Null);
    }

    /// `builtin*IsNullSig.evalInt` in `builtin_op.go` has the same result
    /// for every represented eval type.
    #[test]
    fn isnull_values() {
        for value in [
            Datum::Int(0),
            s(""),
            Datum::Decimal(Decimal::from_literal("0.0")),
            Datum::Real(0.0),
        ] {
            assert_eq!(call("ISNULL", &[value]), Datum::Int(0));
        }
        assert_eq!(call("ISNULL", &[Datum::Null]), Datum::Int(1));
    }
}

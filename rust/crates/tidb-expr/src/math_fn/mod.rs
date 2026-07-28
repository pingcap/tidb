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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Source-family implementation for the currently translated portion of
//! `pkg/expression/builtin_math.go`.
//!
//! One dispatch owns RAND's AST identity, ABS, SIGN, CEIL/FLOOR,
//! ROUND/TRUNCATE, CONV, CRC32, and the existing transcendental functions.
//! It intentionally does not claim the unimplemented remainder of the Go
//! source. Arguments are still evaluated by `crate::func::eval_func` before
//! this dispatch; RAND additionally receives the original argument AST so its
//! constant-versus-row-dependent generator identity remains unchanged.

use std::cmp::Ordering;

use tidb_ast::Expr;

use crate::coerce::coerce_str;
use crate::ops::{finite_float, to_f64, to_f64_with_mysql_string};
use crate::{Columns, Datum, EvalError, MysqlRng};

/// Dispatches translated `builtin_math.go` functions, or returns `None` when
/// the name belongs to another source family.
pub(crate) fn dispatch(
    name: &str,
    args: &[Expr],
    vals: &[Datum],
    cols: &dyn Columns,
    function_key: Option<usize>,
) -> Option<Result<Datum, EvalError>> {
    // RAND is the one arm that needs the argument AST (constant-versus-row
    // generator identity), the session `Columns`, and the per-call
    // `function_key`; every other math builtin is pure over `vals` and lives
    // in [`dispatch_values`] so the chunk-row bridge can reuse it.
    if name == "RAND" {
        return Some(eval_rand(args, vals, cols, function_key));
    }
    dispatch_values(name, vals)
}

/// The values-only subset of [`dispatch`]: every math builtin whose result is
/// a pure function of its already-evaluated arguments. Shared by the
/// AST-level `eval_func` path and `crate::func::eval_func_values` (the
/// `ScalarFunction`/chunk-row bridge).
pub(crate) fn dispatch_values(name: &str, vals: &[Datum]) -> Option<Result<Datum, EvalError>> {
    let result = match name {
        "ABS" => abs(vals),
        "SIGN" => sign(vals),
        "CEIL" | "CEILING" => ceil_floor(vals, true),
        "FLOOR" => ceil_floor(vals, false),
        "ROUND" => round_or_truncate(vals, true),
        "TRUNCATE" => round_or_truncate(vals, false),
        "SQRT" => sqrt(vals),
        "POW" | "POWER" => pow(vals),
        "EXP" => exp(vals),
        "LN" => ln(vals),
        "LOG" => log(vals),
        "LOG2" => log2(vals),
        "LOG10" => log10(vals),
        "PI" => pi(vals),
        "SIN" => sin(vals),
        "COS" => cos(vals),
        "TAN" => tan(vals),
        "ASIN" => asin(vals),
        "ACOS" => acos(vals),
        "ATAN" => atan(vals),
        "ATAN2" => atan2(vals),
        "COT" => cot(vals),
        "RADIANS" => radians(vals),
        "DEGREES" => degrees(vals),
        "CONV" if vals.len() == 3 => conv(vals),
        "CRC32" if vals.len() == 1 => crc32(vals),
        _ => return None,
    };
    Some(result)
}

/// `CONV(n, from_base, to_base)`: reinterprets `n`'s digits in `from_base`
/// and re-emits them (uppercase) in `to_base`. Ported from `builtinConvSig`
/// in `pkg/expression/builtin_math.go`: a NEGATIVE base means signed
/// (`from_base < 0` interprets the value as signed and clamps to
/// `i64` range; `to_base < 0` renders the result signed with a `-` sign);
/// the value is carried through an unsigned 64-bit two's-complement wrap.
/// Bases must be `2..=36` after taking their absolute value, else `NULL`.
/// A leading `+`/`-` sign is honored; an empty valid prefix yields `"0"`.
/// `NULL` if any argument is `NULL`.
pub(crate) fn conv(vals: &[Datum]) -> Result<Datum, EvalError> {
    let (Some(n), Datum::Int(from), Datum::Int(to)) = (coerce_str(&vals[0])?, &vals[1], &vals[2])
    else {
        return Ok(Datum::Null);
    };
    let (mut from, mut to) = (*from, *to);
    let signed = from < 0;
    let ignore_sign = to < 0;
    if signed {
        from = -from;
    }
    if ignore_sign {
        to = -to;
    }
    if !(2..=36).contains(&from) || !(2..=36).contains(&to) {
        return Ok(Datum::Null);
    }
    let prefix = conv_valid_prefix(n.trim(), from as u32);
    if prefix.is_empty() {
        return Ok(Datum::new_string("0".to_string()));
    }
    let (mut negative, digits) = match prefix.strip_prefix('-') {
        Some(rest) => (true, rest),
        None => (false, prefix.as_str()),
    };
    let mut val: u64 = 0;
    for c in digits.chars() {
        // `c` is guaranteed a valid `from`-base digit by `conv_valid_prefix`.
        val = val
            .wrapping_mul(u64::from(from as u32))
            .wrapping_add(u64::from(c.to_digit(from as u32).unwrap()));
    }
    // Signed clamping to the i64 range, mirroring the Go `conv` helper.
    if signed {
        const ABS_I64_MIN: u64 = 1 << 63; // -math.MinInt64
        if negative && val > ABS_I64_MIN {
            val = ABS_I64_MIN;
        }
        if !negative && val > i64::MAX as u64 {
            val = i64::MAX as u64;
        }
    }
    if negative {
        val = val.wrapping_neg();
    }
    // Recompute the sign from the (possibly wrapped) bit pattern.
    negative = (val as i64) < 0;
    if ignore_sign && negative {
        val = val.wrapping_neg();
    }
    let mut out = to_radix_upper(val, to as u32);
    if negative && ignore_sign {
        out.insert(0, '-');
    }
    Ok(Datum::new_string(out))
}

/// The longest valid `CONV` prefix in `base` (a port of
/// `expression.getValidPrefix`): a leading `+`/`-` at position 0 is allowed
/// (a leading `+` is dropped), then valid base-`base` digits until the first
/// invalid character.
pub(crate) fn conv_valid_prefix(s: &str, base: u32) -> String {
    let mut valid_len = 0;
    for (i, c) in s.char_indices() {
        if c == '+' || c == '-' {
            if i != 0 {
                break;
            }
        } else if c.is_digit(base) {
            valid_len = i + c.len_utf8();
        } else {
            break;
        }
    }
    let prefix = &s[..valid_len];
    prefix.strip_prefix('+').unwrap_or(prefix).to_string()
}

/// Renders `value` in `radix` (2..=36) with uppercase digits.
fn to_radix_upper(mut value: u64, radix: u32) -> String {
    if value == 0 {
        return "0".to_string();
    }
    const DIGITS: &[u8] = b"0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ";
    let mut out = Vec::new();
    while value > 0 {
        out.push(DIGITS[(value % u64::from(radix)) as usize]);
        value /= u64::from(radix);
    }
    out.reverse();
    String::from_utf8(out).unwrap()
}

/// `CRC32(str)`: the IEEE CRC-32 checksum (polynomial `0xEDB88320`) as an
/// unsigned integer; `NULL` propagates.
pub(crate) fn crc32(vals: &[Datum]) -> Result<Datum, EvalError> {
    let Some(s) = coerce_str(&vals[0])? else {
        return Ok(Datum::Null);
    };
    let mut crc: u32 = 0xFFFF_FFFF;
    for &byte in s.as_bytes() {
        crc ^= u32::from(byte);
        for _ in 0..8 {
            let mask = (crc & 1).wrapping_neg();
            crc = (crc >> 1) ^ (0xEDB8_8320 & mask);
        }
    }
    Ok(Datum::UInt(u64::from(!crc)))
}

fn abs(vals: &[Datum]) -> Result<Datum, EvalError> {
    match vals {
        [Datum::Null] => Ok(Datum::Null),
        // `builtinAbsIntSig` returns TiDB's BIGINT overflow error for the
        // one signed value that cannot be represented by its positive
        // counterpart.  Do not use `wrapping_abs`: that would silently turn
        // ABS(MININT) back into MININT and diverge from the source contract.
        [Datum::Int(value)] => value
            .checked_abs()
            .map(Datum::Int)
            .ok_or(EvalError::IntOverflow),
        [Datum::UInt(value)] => Ok(Datum::UInt(*value)),
        [Datum::Decimal(value)] => Ok(Datum::Decimal(value.abs())),
        [Datum::Real(value)] => Ok(Datum::Real(value.abs())),
        _ => Err(EvalError::Unsupported("bad function arity")),
    }
}

fn sign(vals: &[Datum]) -> Result<Datum, EvalError> {
    match vals {
        [Datum::Null] => Ok(Datum::Null),
        [Datum::Int(value)] => Ok(Datum::Int(value.signum())),
        [Datum::UInt(value)] => Ok(Datum::Int(i64::from(*value != 0))),
        [Datum::Decimal(value)] => Ok(Datum::Int(value.signum())),
        [Datum::Real(value)] => Ok(Datum::Int(sign_of_real(*value))),
        // `signFunctionClass` selects ETReal for strings. Preserve MySQL's
        // numeric-prefix coercion even though this compact evaluator has no
        // warning channel.
        [Datum::String(_)] => Ok(Datum::Int(sign_of_real(to_f64_with_mysql_string(&vals[0])))),
        _ => Err(EvalError::Unsupported("bad function arity")),
    }
}

/// Coerces one function argument to `f64`: `NULL` propagates (the `Ok(None)`
/// case, for the caller to turn into `Datum::Null`). This is a port of the
/// `EvalReal` argument coercion used by the signatures in
/// `pkg/expression/builtin_math.go`: string arguments use MySQL's numeric
/// prefix rule (so `SQRT('4')` is 2 and `SIN('abc')` is 0, with a warning in
/// a real session).  This value-only evaluator has no warning channel, but
/// must preserve the result value rather than rejecting the expression.
fn numeric_arg(v: &Datum) -> Result<Option<f64>, EvalError> {
    match v {
        Datum::Null => Ok(None),
        Datum::String(_) | Datum::Bytes(_) => Ok(Some(to_f64_with_mysql_string(v))),
        Datum::Int(_) | Datum::UInt(_) | Datum::Decimal(_) | Datum::Real(_) => {
            Ok(Some(to_f64(v.clone())))
        }
        Datum::MinNotNull | Datum::MaxValue => {
            Err(EvalError::Unsupported("range sentinel numeric argument"))
        }
        other => other
            .to_f64()
            .map(|converted| Some(converted.value))
            .map_err(|_| EvalError::Unsupported("numeric argument conversion")),
    }
}

/// `NULL` if `x <= 0` (MySQL's own domain check for `LN`/`LOG`/`LOG2`/
/// `LOG10`), else `Ok(x.ln())`.
fn checked_ln(x: f64) -> Datum {
    if x <= 0.0 {
        Datum::Null
    } else {
        Datum::Real(x.ln())
    }
}

fn sqrt(vals: &[Datum]) -> Result<Datum, EvalError> {
    let [v] = vals else {
        return Err(EvalError::Unsupported("bad function arity"));
    };
    Ok(match numeric_arg(v)? {
        // MySQL's own domain check: NULL for a negative argument, not an
        // error (the OPPOSITE convention from POW/EXP's NaN-is-an-error
        // rule below).
        Some(x) if x < 0.0 => Datum::Null,
        Some(x) => Datum::Real(x.sqrt()),
        None => Datum::Null,
    })
}

fn ln(vals: &[Datum]) -> Result<Datum, EvalError> {
    let [v] = vals else {
        return Err(EvalError::Unsupported("bad function arity"));
    };
    Ok(match numeric_arg(v)? {
        Some(x) => checked_ln(x),
        None => Datum::Null,
    })
}

/// `LOG(x)` (1 argument, natural log — identical to `LN`) or `LOG(base,
/// x)` (2 arguments, log base `base` of `x`).
fn log(vals: &[Datum]) -> Result<Datum, EvalError> {
    match vals {
        [_] => ln(vals),
        [b, x] => {
            let (Some(base), Some(x)) = (numeric_arg(b)?, numeric_arg(x)?) else {
                return Ok(Datum::Null);
            };
            Ok(if base <= 0.0 || base == 1.0 || x <= 0.0 {
                Datum::Null
            } else {
                Datum::Real(x.log(base))
            })
        }
        _ => Err(EvalError::Unsupported("bad function arity")),
    }
}

fn log2(vals: &[Datum]) -> Result<Datum, EvalError> {
    let [v] = vals else {
        return Err(EvalError::Unsupported("bad function arity"));
    };
    Ok(match numeric_arg(v)? {
        Some(x) if x <= 0.0 => Datum::Null,
        Some(x) => Datum::Real(x.log2()),
        None => Datum::Null,
    })
}

fn log10(vals: &[Datum]) -> Result<Datum, EvalError> {
    let [v] = vals else {
        return Err(EvalError::Unsupported("bad function arity"));
    };
    Ok(match numeric_arg(v)? {
        Some(x) if x <= 0.0 => Datum::Null,
        Some(x) => Datum::Real(x.log10()),
        None => Datum::Null,
    })
}

fn pow(vals: &[Datum]) -> Result<Datum, EvalError> {
    let [base, exp] = vals else {
        return Err(EvalError::Unsupported("bad function arity"));
    };
    let (Some(b), Some(e)) = (numeric_arg(base)?, numeric_arg(exp)?) else {
        return Ok(Datum::Null);
    };
    finite_float(b.powf(e))
}

fn exp(vals: &[Datum]) -> Result<Datum, EvalError> {
    let [v] = vals else {
        return Err(EvalError::Unsupported("bad function arity"));
    };
    match numeric_arg(v)? {
        Some(x) => finite_float(x.exp()),
        None => Ok(Datum::Null),
    }
}

/// `PI()`: a niladic function returning the constant.
fn pi(vals: &[Datum]) -> Result<Datum, EvalError> {
    match vals {
        [] => Ok(Datum::Real(std::f64::consts::PI)),
        _ => Err(EvalError::Unsupported("bad function arity")),
    }
}

/// The shape shared by every unary trig/`RADIANS`/`DEGREES` function with
/// NO explicit MySQL domain check (unlike `ASIN`/`ACOS` below): `NULL` if
/// the argument is `NULL`, else `f(x)` wrapped through `finite_float`.
fn unary_finite(vals: &[Datum], f: impl FnOnce(f64) -> f64) -> Result<Datum, EvalError> {
    let [v] = vals else {
        return Err(EvalError::Unsupported("bad function arity"));
    };
    match numeric_arg(v)? {
        Some(x) => finite_float(f(x)),
        None => Ok(Datum::Null),
    }
}

fn sin(vals: &[Datum]) -> Result<Datum, EvalError> {
    unary_finite(vals, f64::sin)
}

fn cos(vals: &[Datum]) -> Result<Datum, EvalError> {
    unary_finite(vals, f64::cos)
}

fn tan(vals: &[Datum]) -> Result<Datum, EvalError> {
    unary_finite(vals, f64::tan)
}

fn cot(vals: &[Datum]) -> Result<Datum, EvalError> {
    unary_finite(vals, |x| 1.0 / x.tan())
}

fn radians(vals: &[Datum]) -> Result<Datum, EvalError> {
    unary_finite(vals, f64::to_radians)
}

fn degrees(vals: &[Datum]) -> Result<Datum, EvalError> {
    unary_finite(vals, f64::to_degrees)
}

/// `NULL` if `x` isn't in `[-1, 1]` (MySQL's own domain check for
/// `ASIN`/`ACOS`, mirroring `SQRT`'s), else `Ok(f(x))`.
fn asin_acos_domain(x: f64, f: impl FnOnce(f64) -> f64) -> Datum {
    if (-1.0..=1.0).contains(&x) {
        Datum::Real(f(x))
    } else {
        Datum::Null
    }
}

fn asin(vals: &[Datum]) -> Result<Datum, EvalError> {
    let [v] = vals else {
        return Err(EvalError::Unsupported("bad function arity"));
    };
    Ok(match numeric_arg(v)? {
        Some(x) => asin_acos_domain(x, f64::asin),
        None => Datum::Null,
    })
}

fn acos(vals: &[Datum]) -> Result<Datum, EvalError> {
    let [v] = vals else {
        return Err(EvalError::Unsupported("bad function arity"));
    };
    Ok(match numeric_arg(v)? {
        Some(x) => asin_acos_domain(x, f64::acos),
        None => Datum::Null,
    })
}

/// `ATAN(x)` (1 argument) or `ATAN(y, x)` (2 arguments, exactly `ATAN2(y,
/// x)` — same argument order, confirmed via `goeval`, not assumed).
fn atan(vals: &[Datum]) -> Result<Datum, EvalError> {
    match vals {
        [_] => unary_finite(vals, f64::atan),
        [_, _] => atan2(vals),
        _ => Err(EvalError::Unsupported("bad function arity")),
    }
}

fn atan2(vals: &[Datum]) -> Result<Datum, EvalError> {
    let [y, x] = vals else {
        return Err(EvalError::Unsupported("bad function arity"));
    };
    let (Some(y), Some(x)) = (numeric_arg(y)?, numeric_arg(x)?) else {
        return Ok(Datum::Null);
    };
    finite_float(y.atan2(x))
}

/// `randFunctionClass` / `builtinRandSig` / `builtinRandWithSeedFirstGenSig`
/// from `pkg/expression/builtin_math.go`. Constant `RAND(N)` owns one
/// statement-scoped generator per AST occurrence; nonconstant inputs start a
/// fresh generator for every row evaluation.
fn eval_rand(
    args: &[Expr],
    vals: &[Datum],
    cols: &dyn Columns,
    function_key: Option<usize>,
) -> Result<Datum, EvalError> {
    match (args, vals) {
        ([], []) => eval_rand_values(&[], cols, function_key, false),
        ([arg], [value]) => eval_rand_values(
            std::slice::from_ref(value),
            cols,
            function_key,
            is_constant_expr(arg),
        ),
        _ => Err(EvalError::Unsupported("bad function arity")),
    }
}

/// The value-level half of [`eval_rand`], shared with the chunk-row bridge
/// (`ScalarFunction::eval`), which has no `tidb_ast::Expr` to classify --
/// its caller passes the constant-vs-row identity it already knows instead.
pub(crate) fn eval_rand_values(
    vals: &[Datum],
    cols: &dyn Columns,
    function_key: Option<usize>,
    arg_is_constant: bool,
) -> Result<Datum, EvalError> {
    match vals {
        [] => cols
            .rand_next()
            .map(Datum::Real)
            .ok_or(EvalError::Unsupported("RAND requires a session")),
        [value] => {
            let seed = rand_seed(value)?;
            if arg_is_constant {
                let key = function_key.ok_or(EvalError::Unsupported(
                    "RAND requires a stable function identity",
                ))?;
                Ok(Datum::Real(
                    cols.rand_seeded_next(key, seed)
                        .unwrap_or_else(|| MysqlRng::new_with_seed(seed).gen()),
                ))
            } else {
                Ok(Datum::Real(MysqlRng::new_with_seed(seed).gen()))
            }
        }
        _ => Err(EvalError::Unsupported("bad function arity")),
    }
}

fn rand_seed(value: &Datum) -> Result<i64, EvalError> {
    match value {
        Datum::Null => Ok(0),
        Datum::Int(value) => Ok(*value),
        Datum::UInt(value) => Ok(*value as i64),
        Datum::Decimal(value) => value.round_to_i64().ok_or(EvalError::IntOverflow),
        Datum::Real(value) => Ok(*value as i64),
        Datum::String(value) => Ok(value
            .as_utf8()
            .map_err(|_| EvalError::Unsupported("invalid UTF-8 string datum"))?
            .trim()
            .parse::<f64>()
            .unwrap_or(0.0) as i64),
        Datum::Bytes(value) => Ok(std::str::from_utf8(value)
            .map_err(|_| EvalError::Unsupported("invalid UTF-8 byte datum"))?
            .trim()
            .parse::<f64>()
            .unwrap_or(0.0) as i64),
        Datum::MinNotNull | Datum::MaxValue => {
            Err(EvalError::Unsupported("range sentinel RAND seed"))
        }
        other => other
            .to_i64()
            .map(|converted| converted.value)
            .map_err(|_| EvalError::Unsupported("RAND seed conversion")),
    }
}

/// This AST-only classifier mirrors the build-time distinction TiDB's
/// function builder makes between a `Constant` and a row-dependent
/// expression. The parser represents a constant arithmetic tree directly,
/// so recurse through its structural wrappers as well.
fn is_constant_expr(expr: &Expr) -> bool {
    match expr {
        Expr::Int(_)
        | Expr::Decimal(_)
        | Expr::Float(_)
        | Expr::Hex(_)
        | Expr::Bit(_)
        | Expr::String(_)
        | Expr::Null => true,
        Expr::Paren(expr) | Expr::Unary(_, expr) => is_constant_expr(expr),
        Expr::Binary(_, left, right) => is_constant_expr(left) && is_constant_expr(right),
        _ => false,
    }
}

/// CEIL/CEILING (`ceiling: true`) or FLOOR (`false`): `Int` is unchanged;
/// `Decimal` computes the EXACT ceiling/floor ([`Decimal::ceil_floor`]).
/// TiDB's builder keeps a decimal result when the argument's declared
/// integer width exceeds `mysql.MaxIntWidth - 2` (18 digits), even when the
/// exact rounded value happens to fit `i64`; this preserves the source
/// `getEvalTp4FloorAndCeil` type boundary rather than inferring the return
/// domain from the runtime magnitude. Narrower decimals collapse to `Int`.
/// `Float`
/// stays `Float` — the OPPOSITE convention from `Decimal`'s own
/// int-collapsing rule, also confirmed via `goeval`, not assumed.
fn ceil_floor(vals: &[Datum], ceiling: bool) -> Result<Datum, EvalError> {
    let [v] = vals else {
        return Err(EvalError::Unsupported("bad function arity"));
    };
    Ok(match v {
        Datum::Null => Datum::Null,
        Datum::Int(i) => Datum::Int(*i),
        Datum::UInt(i) => Datum::UInt(*i),
        Datum::Decimal(d) => {
            let r = d.ceil_floor(ceiling);
            // `getEvalTp4FloorAndCeil` chooses ETDecimal from FieldType's
            // declared integer width, not from the rounded value. A literal
            // such as `9223372036854775807.0` therefore remains DECIMAL even
            // though its exact ceiling fits i64. `Decimal` retains the same
            // width information losslessly as coefficient digits minus its
            // storage scale; 18 is TiDB's `mysql.MaxIntWidth - 2` cutoff.
            let integer_digits = d
                .coefficient_digits()
                .len()
                .saturating_sub(d.storage_scale() as usize)
                .max(1);
            if integer_digits > 18 {
                Datum::Decimal(r)
            } else {
                match r.round_to_i64() {
                    Some(i) => Datum::Int(i),
                    None => Datum::Decimal(r),
                }
            }
        }
        Datum::Real(f) => Datum::Real(if ceiling { f.ceil() } else { f.floor() }),
        Datum::Float32(f) => Datum::Float32(if ceiling { f.ceil() } else { f.floor() }),
        // `ceilFunctionClass`/`floorFunctionClass` choose their real
        // signatures for strings. Preserve the resulting FLOAT type in
        // addition to the numeric-prefix coercion: CEIL('1.23') is 2.0,
        // unlike CEIL(Decimal('1.23')) which has a DECIMAL signature.
        Datum::String(_) | Datum::Bytes(_) => {
            let f = to_f64_with_mysql_string(v);
            Datum::Real(if ceiling { f.ceil() } else { f.floor() })
        }
        Datum::MinNotNull | Datum::MaxValue => {
            return Err(EvalError::Unsupported("range sentinel numeric argument"));
        }
        other => {
            let f = other
                .to_f64()
                .map_err(|_| EvalError::Unsupported("numeric argument conversion"))?
                .value;
            Datum::Real(if ceiling { f.ceil() } else { f.floor() })
        }
    })
}

fn sign_of_real(value: f64) -> i64 {
    match value.partial_cmp(&0.0) {
        Some(Ordering::Greater) => 1,
        Some(Ordering::Less) => -1,
        _ => 0,
    }
}

/// `ROUND(x)`/`ROUND(x, d)` (`round: true`) or `TRUNCATE(x, d)` (`false`,
/// always 2 arguments — confirmed via `goeval`: unlike `ROUND`, `TRUNCATE`
/// has no 1-arg form). `NULL` if any argument is `NULL`. Per-type rule,
/// each confirmed via `goeval` then cross-checked against the real
/// `pkg/expression/builtin_math.go`/`pkg/types/helper.go` sources (not
/// assumed to match `CEIL`/`FLOOR`'s rule, which is different):
/// - `Int` stays `Int` for the 1-arg `ROUND` form (a plain passthrough,
///   matching `builtinRoundIntSig`). The 2-arg forms round-trip through
///   `f64` exactly like real TiDB does (`int64(types.Round(float64(x),
///   d))`/the analogous truncating-division path for `TRUNCATE`) — exact
///   for any `x` within `f64`'s 53-bit exact-integer range, and losing
///   precision near `i64::MAX`/`MIN` the SAME way the reference
///   implementation does (both are IEEE-754 `f64`), rather than being
///   invented to be more precise than the system being modeled.
/// - `Decimal` NEVER collapses to `Int` (unlike `CEIL`/`FLOOR` — confirmed
///   `ROUND(3.14159)` is `DEC:3`, not `INT:3`) and rounds ties AWAY from
///   zero (`ModeHalfUp`/`ModeTruncate`), clamped to MySQL's `DECIMAL` max
///   scale (30) for a positive `d`.
/// - `Float` rounds/truncates via Go's `types.Round`/`types.Truncate`
///   ported bit-for-bit (see [`go_round_float`]/[`go_truncate_float`]):
///   `ROUND` ties TO EVEN — the OPPOSITE tie-breaking rule from `Decimal`
///   (matching the bitwise-conversion precedent), not a "more correct"
///   decimal-aware rounding, since the reference implementation is
///   deliberately this simple and occasionally imprecise.
fn round_or_truncate(vals: &[Datum], round: bool) -> Result<Datum, EvalError> {
    if vals.iter().any(Datum::is_range_sentinel) {
        return Err(EvalError::Unsupported(
            "range sentinel ROUND/TRUNCATE argument",
        ));
    }
    if vals.contains(&Datum::Null) {
        return Ok(Datum::Null);
    }
    // The integer TRUNCATE signatures inspect the scale FieldType before
    // evaluating its value.  An unsigned scale is therefore always
    // non-negative, even when its u64 bit pattern would become negative if
    // narrowed to i64 (for example CAST(18446744073709551615 AS UNSIGNED));
    // Go returns the integer input unchanged in that case.  Keep this type
    // boundary explicit instead of letting the value-only scale cast invent
    // a signed negative precision.
    let unsigned_integer_scale = !round && matches!(vals.get(1), Some(Datum::UInt(_)));
    let (v, d) = match vals {
        [v] if round => (v, 0i64),
        [v, d] => match d {
            Datum::Int(d) => (v, *d),
            Datum::UInt(d) => (v, *d as i64),
            _ => return Err(EvalError::Unsupported("non-integer scale argument")),
        },
        _ => return Err(EvalError::Unsupported("bad function arity")),
    };
    Ok(match v {
        Datum::Int(i) => {
            if unsigned_integer_scale {
                Datum::Int(*i)
            } else {
                Datum::Int(if round {
                    go_round_float(*i as f64, d) as i64
                } else {
                    go_truncate_int(*i, d)
                })
            }
        }
        Datum::UInt(i) => {
            if unsigned_integer_scale {
                Datum::UInt(*i)
            } else {
                Datum::UInt(if round {
                    go_round_float(*i as f64, d) as u64
                } else {
                    // `builtinTruncateUintSig` operates on the original
                    // uint64, not through `EvalReal`/f64.  The distinction is
                    // observable at the u64 boundary: converting
                    // 18446744073709551615 to f64 first loses enough low
                    // digits to change the exact -10 result.
                    go_truncate_uint(*i, d)
                })
            }
        }
        Datum::Decimal(dec) => {
            // MySQL clamps a positive scale to DECIMAL's max (30); a
            // negative scale is used as-is (confirmed via `goeval`:
            // `ROUND(12345, -2)` is `12300`, not clamped).
            let target_scale = d.clamp(i32::MIN as i64, 30) as i32;
            Datum::Decimal(if round {
                dec.round_to_scale(target_scale)
            } else {
                dec.truncate_to_scale(target_scale)
            })
        }
        Datum::Real(f) => Datum::Real(if round {
            go_round_float(*f, d)
        } else {
            go_truncate_float(*f, d)
        }),
        Datum::Float32(f) => Datum::Float32(if round {
            go_round_float(*f, d)
        } else {
            go_truncate_float(*f, d)
        }),
        Datum::String(_) | Datum::Bytes(_) => {
            return Err(EvalError::Unsupported("string operand"));
        }
        Datum::Null | Datum::MinNotNull | Datum::MaxValue => unreachable!("guarded above"),
        other => {
            let f = other
                .to_f64()
                .map_err(|_| EvalError::Unsupported("numeric operand conversion"))?
                .value;
            Datum::Real(if round {
                go_round_float(f, d)
            } else {
                go_truncate_float(f, d)
            })
        }
    })
}

/// Go's `math.Pow10(n)` (`src/math/pow10.go`) ported bit-for-bit: a
/// table-lookup, NOT `10f64.powi(n)` — the two disagree by 1 ULP across most
/// of the exponent range (confirmed by dumping both sides' bit patterns for
/// every `n` in `-400..=400` and diffing, not assumed), since Go's table is
/// a fast approximation rather than always the correctly-rounded result.
/// `go_round_float`/`go_truncate_float` need this EXACT (occasionally
/// imprecise) value, not a more accurate one, to reproduce the reference
/// implementation's own rounding decisions bit-for-bit.
fn go_pow10(n: i64) -> f64 {
    const POW10_TAB: [f64; 32] = [
        1e00, 1e01, 1e02, 1e03, 1e04, 1e05, 1e06, 1e07, 1e08, 1e09, 1e10, 1e11, 1e12, 1e13, 1e14,
        1e15, 1e16, 1e17, 1e18, 1e19, 1e20, 1e21, 1e22, 1e23, 1e24, 1e25, 1e26, 1e27, 1e28, 1e29,
        1e30, 1e31,
    ];
    const POW10_POSTAB32: [f64; 10] = [
        1e00, 1e32, 1e64, 1e96, 1e128, 1e160, 1e192, 1e224, 1e256, 1e288,
    ];
    const POW10_NEGTAB32: [f64; 11] = [
        1e-00, 1e-32, 1e-64, 1e-96, 1e-128, 1e-160, 1e-192, 1e-224, 1e-256, 1e-288, 1e-320,
    ];
    if (0..=308).contains(&n) {
        let n = n as usize;
        POW10_POSTAB32[n / 32] * POW10_TAB[n % 32]
    } else if (-323..=0).contains(&n) {
        let n = (-n) as usize;
        POW10_NEGTAB32[n / 32] / POW10_TAB[n % 32]
    } else if n > 0 {
        f64::INFINITY
    } else {
        0.0
    }
}

/// Go's `types.Round(f, dec)` (`pkg/types/helper.go`) ported bit-for-bit:
/// multiply by `10^dec` ([`go_pow10`]), round the intermediate to the
/// nearest EVEN integer, divide back — deliberately simple and occasionally
/// imprecise, matching the reference implementation exactly rather than a
/// "more correct" decimal-aware approach. An infinite intermediate (e.g.
/// huge `dec`) returns `f` unchanged; a `NaN` result (e.g. `f == 0.0` with
/// `dec` large enough that `shift` is infinite, giving `0 * inf`) becomes
/// `0.0` — both are Go's own explicit fallbacks, not invented here. Also
/// used for `ROUND(int, d)`, which real TiDB implements as this same `f64`
/// round-trip (`int64(types.Round(float64(val), d))`).
fn go_round_float(f: f64, dec: i64) -> f64 {
    let shift = go_pow10(dec);
    let tmp = f * shift;
    if tmp.is_infinite() {
        return f;
    }
    let result = tmp.round_ties_even() / shift;
    if result.is_nan() {
        0.0
    } else {
        result
    }
}

/// Go's `types.Truncate(f, dec)` (`pkg/types/helper.go`) ported bit-for-bit:
/// same shape as [`go_round_float`] but truncates (`f64::trunc`) instead of
/// rounding, with an extra guard for `shift == 0.0` (`dec` negative enough
/// that `10^dec` underflows to `0.0`) — Go returns `f` unchanged for a `NaN`
/// input and `0.0` for everything else in that case.
fn go_truncate_float(f: f64, dec: i64) -> f64 {
    let shift = go_pow10(dec);
    let tmp = f * shift;
    if tmp.is_infinite() || tmp.is_nan() {
        return f;
    }
    if shift == 0.0 {
        return if f.is_nan() { f } else { 0.0 };
    }
    tmp.trunc() / shift
}

/// `TRUNCATE(int, d)` ported from Go's `builtinTruncateIntSig`: a
/// non-negative `d` is a no-op (an integer has nothing past the decimal
/// point to truncate away); a negative `d` zeroes out the low `-d` decimal
/// digits via truncating integer division — exact integer arithmetic, no
/// `f64` round-trip (unlike `ROUND`). A `-d` past `i64`'s own range of exact
/// powers of ten (`> 18`) falls back to `0`, matching what real MySQL's
/// float-round-tripped `shift` would produce for any in-range value at that
/// magnitude anyway, without replicating Go's own undefined float-to-int
/// overflow behavior at that boundary.
fn go_truncate_int(val: i64, dec: i64) -> i64 {
    if dec >= 0 {
        return val;
    }
    let shift = dec
        .checked_neg()
        .and_then(|n| u32::try_from(n).ok())
        .and_then(|n| 10i64.checked_pow(n));
    match shift {
        Some(shift) => val / shift * shift,
        None => 0,
    }
}

/// `builtinTruncateUintSig`'s exact unsigned integer path.  As with the Go
/// signature, a non-negative scale is a no-op and a negative scale divides by
/// an integer power of ten before multiplying it back.  Powers that do not
/// fit in `u64` produce zero for every in-range input, matching the quotient
/// boundary without routing through lossy floating-point conversion.
fn go_truncate_uint(val: u64, dec: i64) -> u64 {
    if dec >= 0 {
        return val;
    }
    let shift = dec
        .checked_neg()
        .and_then(|n| u32::try_from(n).ok())
        .and_then(|n| 10u64.checked_pow(n));
    match shift {
        Some(shift) => val / shift * shift,
        None => 0,
    }
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;

    use super::{cot, dispatch, log2, sin, sqrt};
    use crate::{Columns, Datum, EvalError};
    use tidb_ast::Expr;

    struct RandColumns {
        seeded: Cell<Option<(usize, i64)>>,
    }

    impl Columns for RandColumns {
        fn get(&self, _: &[String]) -> Option<Datum> {
            None
        }

        fn rand_seeded_next(&self, key: usize, seed: i64) -> Option<f64> {
            self.seeded.set(Some((key, seed)));
            Some(0.25)
        }
    }

    #[test]
    fn rand_dispatch_retains_constant_ast_and_function_identity() {
        let columns = RandColumns {
            seeded: Cell::new(None),
        };
        assert_eq!(
            dispatch(
                "RAND",
                &[Expr::Int("7".to_string())],
                &[Datum::Int(7)],
                &columns,
                Some(41),
            ),
            Some(Ok(Datum::Real(0.25)))
        );
        assert_eq!(columns.seeded.get(), Some((41, 7)));
    }

    #[test]
    fn rand_values_shares_the_ast_paths_identity_semantics() {
        use super::eval_rand_values;

        // The chunk bridge has no `Expr` to classify, so its caller passes
        // constant-vs-row as a plain bool; a constant argument still needs
        // the stable identity to reach the seeded generator.
        let columns = RandColumns {
            seeded: Cell::new(None),
        };
        assert_eq!(
            eval_rand_values(&[Datum::Int(7)], &columns, Some(41), true),
            Ok(Datum::Real(0.25))
        );
        assert_eq!(columns.seeded.get(), Some((41, 7)));

        // A nonconstant argument (or a missing identity) never touches the
        // seeded generator -- it always starts a fresh one.
        let columns = RandColumns {
            seeded: Cell::new(None),
        };
        assert!(matches!(
            eval_rand_values(&[Datum::Int(7)], &columns, None, false),
            Ok(Datum::Real(_))
        ));
        assert_eq!(columns.seeded.get(), None);

        // The zero-argument form reads the session's running generator.
        assert_eq!(
            eval_rand_values(&[], &columns, None, false),
            Err(EvalError::Unsupported("RAND requires a session"))
        );
    }

    #[test]
    fn real_math_signatures_coerce_mysql_string_prefixes() {
        assert_eq!(
            sqrt(&[Datum::new_string("4".to_owned())]),
            Ok(Datum::Real(2.0))
        );
        assert_eq!(
            sin(&[Datum::new_string("not numeric".to_owned())]),
            Ok(Datum::Real(0.0))
        );
        assert_eq!(
            log2(&[Datum::new_string("4abc".to_owned())]),
            Ok(Datum::Real(2.0))
        );
        assert_eq!(
            log2(&[Datum::new_string("abc".to_owned())]),
            Ok(Datum::Null)
        );
    }

    #[test]
    fn cot_preserves_go_overflow_after_string_coercion() {
        assert_eq!(
            cot(&[Datum::new_string("tidb".to_owned())]),
            Err(EvalError::FloatOverflow)
        );
    }
}

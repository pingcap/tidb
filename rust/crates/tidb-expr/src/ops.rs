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

//! Unary and binary operator evaluation over [`Datum`] — arithmetic,
//! bitwise, comparison, and logical (three-valued) semantics, for both the
//! `Int` and `Decimal` domains.

use tidb_ast::{BinaryOp, UnaryOp};

use crate::coerce::{
    bool_int, integer_bits, integer_cmp, integer_of, integer_to_decimal, integer_to_f64, truthy_of,
    Integer,
};
use crate::{Datum, Decimal, EvalError};
use tidb_datatype::{div_int64, div_int_with_uint, div_uint_with_int};

mod real_coerce;
pub(crate) use real_coerce::*;

pub(crate) fn eval_unary(
    op: UnaryOp,
    v: Datum,
    ctx: &dyn crate::context::Columns,
) -> Result<Datum, EvalError> {
    use UnaryOp::*;
    // Every unary operator applied to NULL is NULL.
    if v == Datum::Null {
        return Ok(Datum::Null);
    }
    if v.is_range_sentinel() {
        return Err(EvalError::Unsupported("range sentinel expression operand"));
    }
    // Logical NOT is three-valued truthiness, shared by Int and Decimal.
    if let Not | NotKeyword = op {
        return match truthy_of(&v)? {
            Some(t) => Ok(bool_int(!t)),
            None => Ok(Datum::Null),
        };
    }
    match v {
        // Go's unary classes never SEE a string argument: `getFunction` names
        // the argument's eval type and `newBaseBuiltinFuncWithTp` wraps the
        // argument in that cast before any signature runs.
        //
        //  * `bitNegFunctionClass` fixes `types.ETInt`
        //    (`pkg/expression/builtin_op.go:800`), so `~'3'` is `~3`.
        //  * `unaryMinusFunctionClass`'s default arm (`:1053-1076`) picks
        //    `ETDecimal` only for a decimal or temporal argument and `ETReal`
        //    for everything else, a string included -- so `-'3'` is the REAL
        //    -3, not a decimal.
        //  * UNARY PLUS is not a function class at all; TiDB's parser hands
        //    back the operand untouched, so `+'3'` is still the STRING '3'.
        //
        // Captured (`goeval`): `-'3'` -> FLOAT:-3, `+'3'` -> STR:3,
        // `~'3'` -> UINT:18446744073709551612.
        Datum::String(_) | Datum::Bytes(_) => match op {
            Plus => Ok(v),
            Minus => Ok(Datum::Real(-to_f64_with_mysql_string(&v, ctx)?)),
            BitNeg => {
                crate::cast::report_int_truncation(&v, ctx)?;
                Ok(Datum::UInt(!(crate::cast::to_i64_signed(&v) as u64)))
            }
            Not | NotKeyword => unreachable!("handled above"),
        },
        Datum::Decimal(d) => match op {
            Plus => Ok(Datum::Decimal(d)),
            Minus => Ok(Datum::Decimal(d.negate())),
            // `~x` rounds to the nearest integer first (ties away from zero),
            // then flips the bits exactly like the `Int` case.
            BitNeg => d
                .round_to_i64()
                .map(|i| Datum::UInt(!(i as u64)))
                .ok_or(EvalError::IntOverflow),
            Not | NotKeyword => unreachable!("handled above"),
        },
        // Negating a finite f64 is always finite, so no overflow check is
        // needed there — only `~` can fail (out-of-`i64`-range).
        Datum::Real(f) => match op {
            Plus => Ok(Datum::Real(f)),
            Minus => Ok(Datum::Real(-f)),
            // `~x` rounds to the nearest integer first — but TIES TO
            // EVEN, the OPPOSITE tie-breaking rule from `Decimal`'s own
            // `~` (ties away from zero) — a real, easy-to-miss asymmetry
            // confirmed via `goeval`, not assumed: `~2.5` is `-3` (2.5
            // rounds to the even 2, `~2` is `-3`), not `-4` (which
            // away-from-zero rounding to 3 would give).
            BitNeg => f64_to_i64(f.round_ties_even())
                .map(|i| Datum::UInt(!(i as u64)))
                .ok_or(EvalError::IntOverflow),
            Not | NotKeyword => unreachable!("handled above"),
        },
        Datum::Float32(f) => match op {
            Plus => Ok(Datum::Float32(f)),
            Minus => Ok(Datum::Float32(-f)),
            BitNeg => f64_to_i64(f.round_ties_even())
                .map(|i| Datum::UInt(!(i as u64)))
                .ok_or(EvalError::IntOverflow),
            Not | NotKeyword => unreachable!("handled above"),
        },
        Datum::Int(i) => Ok(match op {
            Plus => Datum::Int(i),
            // Negating the one signed magnitude without an i64 counterpart
            // promotes to DECIMAL, rather than wrapping back to Int::MIN.
            // This is the second unary-minus step in `--9223372036854775808`.
            Minus if i == i64::MIN => Datum::Decimal(Decimal::from_uint(1_u64 << 63)),
            Minus => Datum::Int(-i),
            BitNeg => Datum::UInt(!(i as u64)),
            Not | NotKeyword => unreachable!("handled above"),
        }),
        Datum::UInt(i) => Ok(match op {
            Plus => Datum::UInt(i),
            // A unary minus keeps an ordinary unsigned magnitude in the
            // signed domain when it fits, preserves the one representable
            // `-2^63` boundary, and otherwise promotes to DECIMAL instead
            // of wrapping through an invented UInt result. This is the
            // parser/evaluator outcome TiDB exposes for `-u64::MAX`.
            Minus if i <= i64::MAX as u64 => Datum::Int(-(i as i64)),
            Minus if i == (1_u64 << 63) => Datum::Int(i64::MIN),
            Minus => Datum::Decimal(Decimal::from_uint(i).negate()),
            BitNeg => Datum::UInt(!i),
            Not | NotKeyword => unreachable!("handled above"),
        }),
        Datum::Null => unreachable!("handled above"),
        Datum::MinNotNull | Datum::MaxValue => unreachable!("rejected above"),
        other => {
            let decimal = other
                .to_decimal()
                .map_err(|_| EvalError::Unsupported("numeric unary operand"))?
                .value;
            match op {
                Plus => Ok(Datum::Decimal(decimal)),
                Minus => Ok(Datum::Decimal(decimal.negate())),
                BitNeg => decimal
                    .round_to_i64()
                    .map(|i| Datum::UInt(!(i as u64)))
                    .ok_or(EvalError::IntOverflow),
                Not | NotKeyword => unreachable!("handled above"),
            }
        }
    }
}

/// Evaluates a binary operation with the session's explicit decimal-division
/// scale increment. Context-free callers must use [`eval_binary`], which
/// preserves TiDB's default of 4.
pub(crate) fn eval_binary_with_div_precision(
    op: BinaryOp,
    l: Datum,
    r: Datum,
    div_precision_increment: u32,
    ctx: &dyn crate::context::Columns,
) -> Result<Datum, EvalError> {
    eval_binary_full(
        op,
        l,
        r,
        div_precision_increment,
        DERIVATION_FREE_COLLATION,
        ctx,
    )
}

/// [`eval_binary`] for a caller that HAS the statement context, so a
/// string-versus-number comparison can raise its own 1292.
///
/// The nested comparisons in `IN`, `BETWEEN` and `CASE ... WHEN` reach the
/// same `getBaseCmpType` ETReal coercion as a top-level `=`, and TiDB warns
/// there too (captured: `SELECT 1 IN ('12abc')` records 1292). Routing them
/// through [`eval_binary`], whose resolver is `NoColumns`, silently dropped
/// those warnings.
pub(crate) fn eval_binary_in(
    op: BinaryOp,
    l: Datum,
    r: Datum,
    ctx: &dyn crate::context::Columns,
) -> Result<Datum, EvalError> {
    eval_binary_with_div_precision(op, l, r, ctx.div_precision_increment(), ctx)
}

/// The collation an evaluation with no expression-level derivation behind it
/// runs under: `utf8mb4_bin`, this tier's connection collation.
///
/// This is the AST evaluator's answer (`tidb_expr::eval_in` walks a parsed
/// `Expr` with no built expression tree, so it has no derived collation to
/// consult) and the answer for a hand-assembled function node. Every path that
/// goes through the rewriter -- which is every table-backed query -- carries a
/// real derived collation instead; see [`crate::collation_derive`].
pub(crate) const DERIVATION_FREE_COLLATION: tidb_datatype::Collation =
    tidb_datatype::Collation::Utf8Mb4Bin;

/// [`eval_binary_with_div_precision`] under an explicitly derived collation.
///
/// A string-vs-string comparison consults `collation`, which the expression
/// rewriter aggregated from the operands (Go: the comparison's own result type
/// carries the collation `builtinCompareStringSig` compares under). Every other
/// operand pairing ignores it, exactly as in Go.
/// The text `Context.HandleTruncate` names in a `1292 Truncated incorrect
/// <TYPE> value: '<text>'` warning: Go's message carries the operand's own
/// bytes, lossily decoded here for the same reason [`bytes_to_f64`] scans
/// them raw -- the message is diagnostic text, not a value.
fn string_operand_text(d: &Datum) -> String {
    match d {
        Datum::String(s) => String::from_utf8_lossy(s.bytes()).into_owned(),
        Datum::Bytes(s) => String::from_utf8_lossy(s).into_owned(),
        _ => String::new(),
    }
}

pub(crate) fn eval_binary_full(
    op: BinaryOp,
    l: Datum,
    r: Datum,
    div_precision_increment: u32,
    collation: tidb_datatype::Collation,
    ctx: &dyn crate::context::Columns,
) -> Result<Datum, EvalError> {
    use BinaryOp::*;
    if l.is_range_sentinel() || r.is_range_sentinel() {
        return Err(EvalError::Unsupported("range sentinel expression operand"));
    }
    // `<=>` never propagates NULL.  Handle its NULL cases before selecting a
    // comparison type, matching `compareFunctionClass` in
    // `pkg/expression/builtin_compare.go`; this also lets `NULL <=> '1'`
    // return false instead of being rejected by the mixed-string guard below.
    if op == NullEq {
        match (&l, &r) {
            (Datum::Null, Datum::Null) => return Ok(Datum::Int(1)),
            (Datum::Null, _) | (_, Datum::Null) => return Ok(Datum::Int(0)),
            _ => {}
        }
    }
    // Go builds AND/OR/XOR with ETInt arguments, so each operand first takes
    // MySQL's numeric-prefix truthiness path. This must precede the ordinary
    // string comparison branch: string-vs-string is a binary collation
    // comparison for `=`, `<`, etc., but never for a logical operator.
    match op {
        LogicAnd => return logic_and(l, r),
        LogicOr => return logic_or(l, r),
        LogicXor => return logic_xor(l, r),
        _ => {}
    }
    // A JSON operand compares in the JSON domain, and it has to be intercepted
    // HERE -- above the string branch -- because Go's `GetCmpFunction` picks
    // the JSON comparer as soon as EITHER side is ETJson, coercing the other
    // side to JSON rather than the reverse. The ordering itself is Go's
    // `CompareBinaryJSON` (`pkg/types/json_binary_functions.go`), reached
    // through `Datum.compareMysqlJSON`, which is what `Datum::compare` already
    // ports: values of different JSON types order by TYPE PRECEDENCE first, so
    // over `'"a"','"B"','1','{"a":1}','[1,2]'` the minimum is the number `1`
    // and the maximum is an array -- exactly what TiDB records for issue
    // 31640's `select min(a)/max(a) from t` (`tests/integrationtest`).
    //
    // Until this arm existed the pair fell through every guard below to the
    // integer-only path, whose `unreachable!` then aborted the process. The
    // comment on that `unreachable!` claimed the upstream guards excluded
    // everything; they covered Str/Float/Decimal and NOT Json.
    if matches!(l, Datum::Json(_)) || matches!(r, Datum::Json(_)) {
        if !matches!(op, Eq | Ge | Gt | Le | Lt | Ne | NullEq) {
            return Err(EvalError::Unsupported("JSON operand"));
        }
        if l == Datum::Null || r == Datum::Null {
            return Ok(Datum::Null);
        }
        let ordering = l
            .compare(&r, collation)
            .map_err(|_| EvalError::Unsupported("JSON comparison"))?;
        return Ok(ordering_to_bool(op, ordering));
    }
    // Two strings compare under the collation the expression derivation
    // aggregated for THIS comparison (byte order and PAD SPACE for
    // `utf8mb4_bin`, case folding for a `_ci` collation, NO PAD for `binary`).
    //
    // ONLY a comparison takes this branch. An arithmetic or bitwise operator
    // over two strings is not a collation question at all -- Go casts both
    // arguments into the signature's numeric domain first (see the
    // string-operand arm further down), so `'1231' % '12'` is 7, not a
    // collation comparison that has no definition for `%`.
    let comparison = matches!(op, Eq | Ge | Gt | Le | Lt | Ne | NullEq);
    if comparison {
        if let (Some(a), Some(b)) = (
            string_cmp_operand(&l, comparison),
            string_cmp_operand(&r, comparison),
        ) {
            return string_compare(op, a, b, collation);
        }
    }
    // `Raw` and `VectorFloat32` are the two kinds that no dispatch below
    // claims, and they are rejected HERE -- as one guard, above every
    // numeric path -- rather than at each of the places they would otherwise
    // land, because those places do not fail alike:
    //
    //   * a `Div` or a `Decimal` operand reaches [`to_decimal`], whose
    //     fallback `.expect()` PANICS on either kind;
    //   * a comparison against a string reaches
    //     [`to_f64_with_mysql_string`], which used to substitute `0.0` for
    //     either kind -- a wrong ANSWER, which is worse -- and now returns
    //     the same statement error this guard does;
    //   * everything else reaches the integer residue at the bottom.
    //
    // One guard makes all three the same statement error. `Raw` compared
    // with `Raw` (or with a string) is deliberately still handled by the
    // branch just above, since `as_raw_bytes` gives it real byte semantics.
    //
    // Go reaches neither kind by this route: `KindRaw` is internal encoding
    // state that `Datum.Compare` answers 0 for out of its `default` arm, and
    // a vector column's comparisons go through `compareVectorFloat32`, which
    // this evaluator has not ported. Returning an error says exactly that,
    // and says it without taking the process down.
    if matches!(l, Datum::Raw(_) | Datum::VectorFloat32(_))
        || matches!(r, Datum::Raw(_) | Datum::VectorFloat32(_))
    {
        return Err(EvalError::UnsupportedOperandPair(l.kind(), r.kind()));
    }
    // A datetime/date value compares in the TIME domain against another
    // temporal value or a STRING: Go's `getBaseCmpType` gives ETString for a
    // pair whose eval types are both string-kind (datetime IS string-kind),
    // and `GetAccurateCmpType` then upgrades ETString-with-a-time to
    // ETDatetime, so `'2024-12-31'` is parsed into a Time first rather than
    // compared by its NUMERIC PREFIX (2024.0) -- the silent wrong-row bug for
    // the `WHERE created <= 'date'` every application writes.
    //
    // Against a NUMBER, Go compares in the REAL domain instead:
    // `getBaseCmpType(ETDatetime, ETInt)` is ETReal, so `datetime_col <
    // 20231310` is `20230809000000 < 20231310`, NOT a datetime parse of
    // 20231310 that fails and drops the row. A numeric CONSTANT that DOES
    // convert to a datetime has already been rewritten to a datetime constant
    // by `refine_comparisons` (Go's `refineNumericConstantCmpDatetime`), so it
    // reaches here as a `Time` and takes this datetime path; every numeric
    // operand that remains -- a non-convertible constant, or a bigint column
    // -- is one Go also compares as real, and so falls through to the
    // `numeric_context_value` promotion below.
    // (Time ARITHMETIC -- `created + 1` -- falls through the same way, which
    // is also what Go's non-comparison paths do.)
    let numeric_partner = |value: &Datum| {
        matches!(
            value,
            Datum::Int(_) | Datum::UInt(_) | Datum::Decimal(_) | Datum::Real(_) | Datum::Float32(_)
        )
    };
    if matches!(op, Eq | Ge | Gt | Le | Lt | Ne | NullEq)
        && (matches!(l, Datum::Time(_)) || matches!(r, Datum::Time(_)))
        && !(matches!(l, Datum::Time(_)) && numeric_partner(&r))
        && !(matches!(r, Datum::Time(_)) && numeric_partner(&l))
    {
        if l == Datum::Null || r == Datum::Null {
            return Ok(if op == NullEq {
                Datum::Int(0)
            } else {
                Datum::Null
            });
        }
        return time_compare(op, &l, &r, ctx);
    }
    // A `TIME` compared with a string compares in the DURATION domain: Go's
    // `RefineComparedConstant` turns the string constant into a duration, so
    // `tm = '1:00:00'` is TRUE for `01:00:00` (verified via `gorun`). Without
    // this the numeric substitution below would compare `10000` against the
    // string's NUMERIC PREFIX (1), i.e. silently drop the row.
    if matches!(op, Eq | Ge | Gt | Le | Lt | Ne | NullEq) {
        let text_side = |value: &Datum| match value {
            Datum::String(value) => Some(String::from_utf8_lossy(value.bytes()).into_owned()),
            Datum::Bytes(value) => Some(String::from_utf8_lossy(value).into_owned()),
            _ => None,
        };
        let pair = match (&l, &r) {
            (Datum::Duration(a), other) => text_side(other).map(|text| (*a, text, false)),
            (other, Datum::Duration(b)) => text_side(other).map(|text| (*b, text, true)),
            _ => None,
        };
        if let Some((duration, text, reversed)) = pair {
            let ordering = match duration.compare_string(&text) {
                Ok(ordering) if reversed => ordering.reverse(),
                Ok(ordering) => ordering,
                Err(_) => {
                    ctx.append_warning(1292, &format!("Incorrect time value: '{text}'"));
                    return Ok(if op == NullEq {
                        Datum::Int(0)
                    } else {
                        Datum::Null
                    });
                }
            };
            return Ok(ordering_to_bool(op, ordering));
        }
    }
    // Every remaining use of a temporal operand -- arithmetic, and comparing
    // two `TIME`s -- evaluates it in its NUMERIC context, which is what Go's
    // `numericContextResultType` (`pkg/expression/builtin_arithmetic.go:80`)
    // gives a temporal type: ETDecimal when it carries fractional seconds,
    // ETInt otherwise. So `DATETIME '2020-01-02 03:04:05' + 0` is
    // `20200102030405`, `TIME '01:00:00' * 0` is `0`, and a `DATETIME(6)` keeps
    // its 6 fractional digits as a decimal (all three verified via `gorun`).
    // Substituting the numeric value here rather than adding a temporal arm to
    // each operator is what keeps the promotion hierarchy below single-sourced.
    if matches!(l, Datum::Time(_) | Datum::Duration(_))
        || matches!(r, Datum::Time(_) | Datum::Duration(_))
    {
        return eval_binary_full(
            op,
            numeric_context_value(l),
            numeric_context_value(r),
            div_precision_increment,
            collation,
            ctx,
        );
    }
    // `getBaseCmpType` in `builtin_compare.go` selects ETReal whenever a
    // string is compared with a numeric value.  Thus both operands use the
    // same MySQL numeric-prefix coercion as `EvalReal`; this is comparison
    // semantics only, not a claim that arbitrary string arithmetic is in
    // scope for this compact value evaluator.
    if matches!(l, Datum::String(_) | Datum::Bytes(_))
        || matches!(r, Datum::String(_) | Datum::Bytes(_))
    {
        if matches!(op, Eq | Ge | Gt | Le | Lt | Ne | NullEq) {
            if l == Datum::Null || r == Datum::Null {
                return Ok(Datum::Null);
            }
            return real_compare(
                op,
                to_f64_with_mysql_string(&l, ctx)?,
                to_f64_with_mysql_string(&r, ctx)?,
            );
        }
        // `Datum::Bytes` is the AST tier's BINARY LITERAL carrier
        // (`binary_literal.rs::bytes_to_value` turns `0x...`/`b'...'` into
        // one), and Go's `numericContextResultType` gives a CONSTANT binary
        // literal `ETInt`, not the `ETReal` every other string takes
        // (`pkg/expression/builtin_arithmetic.go:91`) -- `0x20000000000000 + 1`
        // is 9007199254740993, not 1. Coercing those bytes as TEXT here would
        // trade a refusal for a wrong answer, so they stay refused; the chunk
        // rewriter, which carries the literal as `Datum::BinaryLiteral` and so
        // keeps its integer identity, already answers correctly.
        //
        // Nothing else reaches here as `Bytes`: a VARCHAR *and* a VARBINARY
        // column both read back as `Datum::String` (the latter merely collated
        // `binary`), which is also what makes `binary '3' + 1` FLOAT:4 in Go.
        if matches!(l, Datum::Bytes(_)) || matches!(r, Datum::Bytes(_)) {
            return Err(EvalError::Unsupported("binary literal operand"));
        }
        // Every other operator reaches its signature with the string ALREADY
        // cast. Go's arithmetic and bitwise classes each read
        // `numericContextResultType` (`pkg/expression/builtin_arithmetic.go:80`)
        // for both arguments, pick one signature, and let
        // `newBaseBuiltinFuncWithTp` wrap each argument in that signature's
        // `argTps` cast. A string's numeric context is ALWAYS `ETReal`
        // (`:94-100`) -- the `ETInt` shortcut at `:91` is for a CONSTANT binary
        // literal (`0x1234`, `b'11'`) or a `BIT` column, and those arrive here
        // as their own `Datum` kinds, not as `String`/`Bytes`. So the cast a
        // string takes depends only on the OPERATOR:
        //
        //  * `& | ^ << >>` -- `bitAndFunctionClass` and friends fix `ETInt`
        //    arguments unconditionally (`builtin_op.go`), so `'3.7' & 1` sees
        //    `StrToInt`'s truncated 3, not a rounded 4.
        //  * `DIV` -- `arithmeticIntDivideFunctionClass` uses `ETInt` arguments
        //    only when BOTH sides are `ETInt`, and `ETDecimal` otherwise, so
        //    `'7.9' DIV 2` is 3.
        //  * `+ - * / %` -- `ETReal` wins as soon as either side is `ETReal`.
        //
        // Only the STRING operands are converted; a numeric partner already
        // promotes correctly through the hierarchy below, so this stays one
        // conversion rather than a second, rival promotion table.
        //
        // Captured (`goeval`): `'3'+1` FLOAT:4, `'3'/2` FLOAT:1.5,
        // `'3' DIV 2` INT:1, `'3'%2` FLOAT:1, `'3'&1` UINT:1, `'3'<<2` UINT:12,
        // `1.5&'3'` UINT:2, `'12abc'+1` FLOAT:13, `'abc'+1` FLOAT:1
        // (+ warning 1292 `Truncated incorrect DOUBLE value: 'abc'`),
        // `'abc' DIV 2` INT:0 (+ the DECIMAL-worded 1292).
        let cast_string = |d: Datum| -> Result<Datum, EvalError> {
            if !matches!(d, Datum::String(_) | Datum::Bytes(_)) {
                return Ok(d);
            }
            match op {
                BitAnd | BitOr | BitXor | LeftShift | RightShift => {
                    crate::cast::report_int_truncation(&d, ctx)?;
                    Ok(Datum::Int(crate::cast::to_i64_signed(&d)))
                }
                IntDiv => {
                    let converted = d
                        .to_decimal()
                        .map_err(|_| EvalError::Unsupported("string operand"))?;
                    if converted.event.is_some() {
                        // Go's `builtinCastStringAsDecimalSig` routes
                        // `StrToDecimal`'s truncation through
                        // `Context.HandleTruncate`, and the message names
                        // DECIMAL rather than the DOUBLE the real cast names.
                        ctx.handle_truncate(&format!(
                            "Truncated incorrect DECIMAL value: '{}'",
                            string_operand_text(&d)
                        ))?;
                    }
                    Ok(Datum::Decimal(converted.value))
                }
                _ => Ok(Datum::Real(to_f64_with_mysql_string(&d, ctx)?)),
            }
        };
        // Neither converted operand is a string, so the recursion is one deep.
        return eval_binary_full(
            op,
            cast_string(l)?,
            cast_string(r)?,
            div_precision_increment,
            collation,
            ctx,
        );
    }
    // `Float` dominates `Decimal` in MySQL's promotion hierarchy — the
    // OPPOSITE of how `Decimal` dominates `Int` below — so this check
    // must run before the `Div`/`Decimal` dispatch, not after: an
    // Int/Float or Decimal/Float pair promotes BOTH operands to `f64`,
    // not to `Decimal` (confirmed via goeval: `1.5e2 + 3.14` is
    // `FLOAT:153.14`, not a `Decimal`).
    // `Float32` is the same ETReal domain as `Real`, just the 4-byte storage a
    // `FLOAT` column reads back as; leaving it out of this dispatch dropped it
    // through to the integer-only path below, whose `unreachable!` then
    // panicked on any FLOAT-vs-integer comparison or arithmetic.
    if matches!(l, Datum::Real(_) | Datum::Float32(_))
        || matches!(r, Datum::Real(_) | Datum::Float32(_))
    {
        return float_binary(op, l, r, ctx);
    }
    // `/` always promotes both operands to Decimal and produces a Decimal
    // result — even for two Int operands, MySQL's `/` never yields an Int
    // (confirmed via goeval: `1 / 2` is `DEC:0.5000`) — so it's intercepted
    // here, before the Int-only/decimal-only dispatch below would otherwise
    // only reach it when a Decimal operand was ALREADY present.
    if op == Div {
        if l == Datum::Null || r == Datum::Null {
            return Ok(Datum::Null);
        }
        let a = to_decimal(l);
        let b = to_decimal(r);
        if b.is_zero() {
            ctx.handle_division_by_zero()?;
            return Ok(Datum::Null);
        }
        let target_scale = a.scale() + effective_div_precision_increment(div_precision_increment);
        return Ok(match a.true_div(&b, target_scale) {
            Some(q) => Datum::Decimal(q),
            None => Datum::Null,
        });
    }
    // A Decimal operand (an Int operand promotes to a scale-0 decimal, MySQL's
    // implicit rule) arithmetics/compares exactly; handles its own NullEq.
    if matches!(l, Datum::Decimal(_)) || matches!(r, Datum::Decimal(_)) {
        return decimal_binary(op, l, r, ctx);
    }
    if op == NullEq {
        return null_safe_eq(l, r);
    }
    // By this point `l`/`r` should only be an integral value or `Null`: `Str`
    // is guarded out at the very top, `Float`/`Decimal`/`Div`/`Json`/temporal
    // are all intercepted above, and `integer_of` maps the remaining integral
    // kinds -- `Enum`, `Set`, `Bit`, `BinaryLiteral` -- to their unsigned
    // numeric value the way Go's `Datum.GetInt64`/`GetUint64` do.
    //
    // "Should" is not "does". `integer_of` still answers `None` for `Raw` and
    // `VectorFloat32`, and `Err` for the `MinNotNull`/`MaxValue` range
    // sentinels; none of those has a dispatch above. The predecessor of this
    // code asserted the guards were exhaustive with an `unreachable!`, and
    // that assertion was false twice -- for `Float32` and for `Json` -- each
    // time turning one user query into a process abort that killed every
    // other connection. So the residue returns a statement error naming BOTH
    // kinds instead of panicking.
    if l == Datum::Null || r == Datum::Null {
        return Ok(Datum::Null);
    }
    // `integer_of`'s own `Err` (the range sentinels) keeps its existing
    // message; only the `None` residue, which used to panic, is new.
    let (a, b) = match (integer_of(&l)?, integer_of(&r)?) {
        (Some(a), Some(b)) => (a, b),
        _ => return Err(EvalError::UnsupportedOperandPair(l.kind(), r.kind())),
    };
    integer_binary(op, a, b, ctx)
}

fn integer_binary(
    op: BinaryOp,
    a: Integer,
    b: Integer,
    ctx: &dyn crate::context::Columns,
) -> Result<Datum, EvalError> {
    use BinaryOp::*;
    let lhs_unsigned = matches!(a, Integer::Unsigned(_));
    let rhs_unsigned = matches!(b, Integer::Unsigned(_));
    let unsigned = lhs_unsigned || rhs_unsigned;
    let bits_a = integer_bits(a);
    let bits_b = integer_bits(b);
    Ok(match op {
        Plus => return integer_add(a, b),
        // `-` reports `ErrOverflow` exactly where Go `builtinArithmeticMinusIntSig`
        // does — via [`minus_overflows`], a line-for-line port of Go's
        // `overflowCheck` (verified against goeval across every branch). A
        // non-overflowing result keeps its wrapped two's-complement value, typed
        // by `unsigned`.
        Minus => {
            if minus_overflows(lhs_unsigned, rhs_unsigned, bits_a as i64, bits_b as i64) {
                return Err(EvalError::IntOverflow);
            }
            integer_result(unsigned, bits_a.wrapping_sub(bits_b))
        }
        // `*` matches Go's two sigs, selected by whether either operand is
        // unsigned (`getFunction`: `HasUnsignedFlag(lhs) || HasUnsignedFlag(rhs)`
        // == this `unsigned`). `builtinArithmeticMultiplyIntUnsignedSig` multiplies
        // the u64 bit patterns and errors when the product wraps
        // (`unsignedA != 0 && result/unsignedA != unsignedB`); `...MultiplyIntSig`
        // multiplies as i64 (`a != 0 && result/a != b`, plus the `MinInt64 * -1`
        // case). Both are exactly `checked_mul` on the respective type.
        Mul => {
            if unsigned {
                return bits_a
                    .checked_mul(bits_b)
                    .map(Datum::UInt)
                    .ok_or(EvalError::IntOverflow);
            }
            return (bits_a as i64)
                .checked_mul(bits_b as i64)
                .map(Datum::Int)
                .ok_or(EvalError::IntOverflow);
        }
        // `DIV`/`MOD` by zero yield NULL in MySQL. `DIV` truncates toward zero.
        IntDiv => {
            if bits_b == 0 {
                ctx.handle_division_by_zero()?;
                Datum::Null
            } else {
                // Go selects a different checked helper for every signedness
                // pair.  In particular, a mixed signed/unsigned quotient is
                // an unsigned result and rejects a negative quotient instead
                // of dividing the raw two's-complement bit patterns.
                let quotient = match (a, b) {
                    (Integer::Unsigned(lhs), Integer::Unsigned(rhs)) => lhs / rhs,
                    (Integer::Unsigned(lhs), Integer::Signed(rhs)) => {
                        div_uint_with_int(lhs, rhs).map_err(|_| EvalError::IntOverflow)?
                    }
                    (Integer::Signed(lhs), Integer::Unsigned(rhs)) => {
                        div_int_with_uint(lhs, rhs).map_err(|_| EvalError::IntOverflow)?
                    }
                    (Integer::Signed(lhs), Integer::Signed(rhs)) => {
                        return div_int64(lhs, rhs)
                            .map(Datum::Int)
                            .map_err(|_| EvalError::IntOverflow);
                    }
                };
                Datum::UInt(quotient)
            }
        }
        Mod => {
            if bits_b == 0 {
                ctx.handle_division_by_zero()?;
                Datum::Null
            } else {
                // MOD's result flag follows the left operand only.  Go's
                // mixed-sign implementations also preserve the dividend sign
                // instead of taking a remainder over raw unsigned bits.
                match (a, b) {
                    (Integer::Unsigned(lhs), Integer::Unsigned(rhs)) => Datum::UInt(lhs % rhs),
                    (Integer::Unsigned(lhs), Integer::Signed(rhs)) => {
                        Datum::UInt(lhs % rhs.unsigned_abs())
                    }
                    (Integer::Signed(lhs), Integer::Unsigned(rhs)) => {
                        let remainder = if lhs < 0 {
                            -((lhs.unsigned_abs() % rhs) as i64)
                        } else {
                            (lhs as u64 % rhs) as i64
                        };
                        Datum::Int(remainder)
                    }
                    (Integer::Signed(lhs), Integer::Signed(rhs)) => {
                        Datum::Int(lhs.wrapping_rem(rhs))
                    }
                }
            }
        }
        BitAnd => Datum::UInt(bits_a & bits_b),
        BitOr => Datum::UInt(bits_a | bits_b),
        BitXor => Datum::UInt(bits_a ^ bits_b),
        LeftShift => Datum::UInt(shift_left(bits_a, bits_b)),
        RightShift => Datum::UInt(shift_right(bits_a, bits_b)),
        Eq => bool_int(integer_cmp(a, b).is_eq()),
        Ge => bool_int(integer_cmp(a, b).is_ge()),
        Gt => bool_int(integer_cmp(a, b).is_gt()),
        Le => bool_int(integer_cmp(a, b).is_le()),
        Lt => bool_int(integer_cmp(a, b).is_lt()),
        Ne => bool_int(!integer_cmp(a, b).is_eq()),
        Div => unreachable!("handled above"),
        LogicAnd | LogicOr | LogicXor | NullEq => unreachable!("handled above"),
    })
}

fn integer_result(unsigned: bool, bits: u64) -> Datum {
    if unsigned {
        Datum::UInt(bits)
    } else {
        Datum::Int(bits as i64)
    }
}

/// Integer `+` with TiDB's overflow rule (`builtinArithmeticPlusIntSig`): the
/// result is `UNSIGNED` when either operand is, and any result past that type's
/// range is `ErrOverflow`, never a silent wrap. Go errors in every signedness
/// case rather than adding the raw two's-complement bits, so each case maps to a
/// checked operation: a mixed sum underflows past `0` when a negative addend
/// exceeds the unsigned operand, or overflows past `u64::MAX`.
fn integer_add(a: Integer, b: Integer) -> Result<Datum, EvalError> {
    match (a, b) {
        (Integer::Signed(x), Integer::Signed(y)) => x
            .checked_add(y)
            .map(Datum::Int)
            .ok_or(EvalError::IntOverflow),
        (Integer::Unsigned(x), Integer::Unsigned(y)) => x
            .checked_add(y)
            .map(Datum::UInt)
            .ok_or(EvalError::IntOverflow),
        (Integer::Unsigned(x), Integer::Signed(y)) | (Integer::Signed(y), Integer::Unsigned(x)) => {
            let sum = if y < 0 {
                x.checked_sub(y.unsigned_abs())
            } else {
                x.checked_add(y.unsigned_abs())
            };
            sum.map(Datum::UInt).ok_or(EvalError::IntOverflow)
        }
    }
}

/// A line-for-line port of Go `builtinArithmeticMinusIntSig.overflowCheck`:
/// `true` when `a - b` overflows the result type. `a`/`b` are the operands
/// reinterpreted as `i64` (Go passes the raw `int64` bits). `signed` is
/// `!lhs_unsigned && !rhs_unsigned` — Go's `forceToSigned` is the
/// `NO_UNSIGNED_SUBTRACTION` sql_mode, which this context-free layer does not
/// model, so this is the default (mode off). The branch structure and the final
/// condition mirror Go exactly; verified against goeval across every branch.
fn minus_overflows(lhs_unsigned: bool, rhs_unsigned: bool, a: i64, b: i64) -> bool {
    let signed = !lhs_unsigned && !rhs_unsigned;
    let res = a.wrapping_sub(b);
    let (ua, ub) = (a as u64, b as u64);
    let mut res_unsigned = false;
    if lhs_unsigned {
        if rhs_unsigned {
            if ua < ub {
                if res >= 0 {
                    return true;
                }
            } else {
                res_unsigned = true;
            }
        } else if b >= 0 {
            if ua > ub {
                res_unsigned = true;
            }
        } else if ua > u64::MAX - b.unsigned_abs() {
            // Go `testIfSumOverflowsUll(ua, uint64(-b))`.
            return true;
        } else {
            res_unsigned = true;
        }
    } else if rhs_unsigned {
        // Go `uint64(a - math.MinInt64) < ub`.
        if (a.wrapping_sub(i64::MIN) as u64) < ub {
            return true;
        }
    } else if a > 0 && b < 0 {
        res_unsigned = true;
    } else if a < 0 && b > 0 && res >= 0 {
        return true;
    }
    (!signed && !res_unsigned && res < 0)
        || (signed && res_unsigned && (res as u64) > i64::MAX as u64)
}

/// Evaluates a context-free binary operation with TiDB's default
/// `div_precision_increment` of 4.
pub(crate) fn eval_binary(op: BinaryOp, l: Datum, r: Datum) -> Result<Datum, EvalError> {
    eval_binary_with_div_precision(op, l, r, 4, &crate::context::NoColumns)
}

/// TiDB preserves a declared decimal division result scale when the session
/// value is zero: `SET div_precision_increment = 0; SELECT 8 / 7` still
/// renders `1.1429`, while values 1 through 30 use their exact increment.
/// This comes from the source divide builtin's result-type construction plus
/// its runtime `DecimalDiv` call (`builtin_arithmetic.go:745,810`), not from
/// treating the stored TypeUnsigned value as anything other than zero.
pub(crate) const fn effective_div_precision_increment(raw: u32) -> u32 {
    if raw == 0 {
        4
    } else {
        raw
    }
}

/// Decimal arithmetic and comparison: an `Int` operand promotes to a scale-0
/// decimal (MySQL's implicit rule), and `+`/`-`/`*` and every comparison are
/// exact (see [`Decimal`]). `NullEq` has its own NULL rule; every other
/// operator here is `NULL` if either operand is `NULL` — including `DIV`/
/// `MOD` by zero, matching the `Int` case. `/` itself never reaches this
/// function — `eval_binary` intercepts it earlier, since it must promote
/// even a pure `Int`/`Int` pair to `Decimal`.
fn decimal_binary(
    op: BinaryOp,
    l: Datum,
    r: Datum,
    ctx: &dyn crate::context::Columns,
) -> Result<Datum, EvalError> {
    use BinaryOp::*;
    if op == NullEq {
        return Ok(match (&l, &r) {
            (Datum::Null, Datum::Null) => Datum::Int(1),
            (Datum::Null, _) | (_, Datum::Null) => Datum::Int(0),
            _ => bool_int(to_decimal(l) == to_decimal(r)),
        });
    }
    if l == Datum::Null || r == Datum::Null {
        return Ok(Datum::Null);
    }
    let a = to_decimal(l);
    let b = to_decimal(r);
    Ok(match op {
        Plus => {
            let (sum, warning) = a.add_mysql(&b);
            if warning == Some(tidb_datatype::DecimalCodecWarning::Overflow) {
                return Err(EvalError::DecimalOverflow);
            }
            Datum::Decimal(sum)
        }
        Minus => {
            let (difference, warning) = a.sub_mysql(&b);
            if warning == Some(tidb_datatype::DecimalCodecWarning::Overflow) {
                return Err(EvalError::DecimalOverflow);
            }
            Datum::Decimal(difference)
        }
        Mul => {
            let (product, warning) = a.mul_mysql(&b);
            if warning == Some(tidb_datatype::DecimalCodecWarning::Overflow) {
                return Err(EvalError::DecimalOverflow);
            }
            Datum::Decimal(product)
        }
        Eq => bool_int(a == b),
        Ge => bool_int(a >= b),
        Gt => bool_int(a > b),
        Le => bool_int(a <= b),
        Lt => bool_int(a < b),
        Ne => bool_int(a != b),
        Div => unreachable!("handled above"),
        // `div_rem` answers `None` for two unrelated conditions: a zero divisor
        // and a quotient too wide for `i64`. Go
        // (`builtinArithmeticIntDivideDecimalSig.evalInt`,
        // `builtin_arithmetic.go:926`) keeps them apart — a zero divisor comes
        // back from `DecimalDiv` as `ErrDivByZero` and goes to the
        // division-by-zero handler, while an out-of-`BIGINT` quotient is caught
        // later by `ToInt`/`ToUint` and raised as an unconditional
        // `ErrOverflow`, never downgraded to a warning. Testing the divisor
        // here is what lets the remaining `None` mean overflow and only
        // overflow.
        IntDiv => {
            if b.is_zero() {
                ctx.handle_division_by_zero()?;
                Datum::Null
            } else {
                match a.div_rem(&b) {
                    Some((q, _)) => Datum::Int(q),
                    None => return Err(EvalError::IntOverflow),
                }
            }
        }
        Mod => match a.rem_mysql(&b) {
            Some(r) => Datum::Decimal(r),
            None => {
                ctx.handle_division_by_zero()?;
                Datum::Null
            }
        },
        // Bitwise/shift operators work on integers in MySQL, so a decimal
        // operand rounds to the nearest `i64` first (ties away from zero),
        // same as unary `~` above.
        BitAnd | BitOr | BitXor | LeftShift | RightShift => {
            let (ai, bi) = match (a.round_to_i64(), b.round_to_i64()) {
                (Some(x), Some(y)) => (x, y),
                _ => return Err(EvalError::IntOverflow),
            };
            match op {
                BitAnd => Datum::UInt((ai as u64) & (bi as u64)),
                BitOr => Datum::UInt((ai as u64) | (bi as u64)),
                BitXor => Datum::UInt((ai as u64) ^ (bi as u64)),
                LeftShift => Datum::UInt(shift_left(ai as u64, bi as u64)),
                RightShift => Datum::UInt(shift_right(ai as u64, bi as u64)),
                _ => unreachable!("guarded by outer match"),
            }
        }
        LogicAnd | LogicOr | LogicXor | NullEq => unreachable!("handled by caller"),
    })
}

/// Coerces a non-`NULL` value to [`Decimal`] (an `Int` promotes to scale 0);
/// `Str`/`Float` are unreachable here — `eval_binary` guards both out
/// before dispatching to decimal handling (`Float` takes priority over
/// `Decimal`, so a `Float` operand never reaches this function at all).
/// Also reused by `func::extremum` (only when no argument is `Float`, so
/// the same invariant holds there too).
/// Compares in the time domain, parsing the non-`Time` side.
///
/// Captured from TiDB: `'2024-12-31'` against a DATETIME column means that
/// date's midnight, a bare number `20241231` parses as a date too, and a
/// string that is not a datetime at all filters every row with warning 1292
/// `Incorrect datetime value` -- the comparison itself yields NULL.
fn time_compare(
    op: BinaryOp,
    l: &Datum,
    r: &Datum,
    ctx: &dyn crate::context::Columns,
) -> Result<Datum, EvalError> {
    let ordering = match (l, r) {
        (Datum::Time(a), Datum::Time(b)) => a.compare(*b),
        (Datum::Time(a), other) => match time_compare_ordering(*a, other, ctx)? {
            Some(ordering) => ordering,
            None => return Ok(Datum::Null),
        },
        (other, Datum::Time(b)) => match time_compare_ordering(*b, other, ctx)? {
            Some(ordering) => ordering.reverse(),
            None => return Ok(Datum::Null),
        },
        _ => unreachable!("one side is a Time"),
    };
    Ok(ordering_to_bool(op, ordering))
}

/// A resolved ordering read as the comparison operator's boolean result.
fn ordering_to_bool(op: BinaryOp, ordering: std::cmp::Ordering) -> Datum {
    use BinaryOp::*;
    match op {
        Eq | NullEq => bool_int(ordering.is_eq()),
        Ne => bool_int(!ordering.is_eq()),
        Lt => bool_int(ordering.is_lt()),
        Le => bool_int(ordering.is_le()),
        Gt => bool_int(ordering.is_gt()),
        Ge => bool_int(ordering.is_ge()),
        _ => unreachable!("only comparisons resolve to an ordering"),
    }
}

/// A temporal value in the numeric context Go's `numericContextResultType`
/// (`pkg/expression/builtin_arithmetic.go:80`) gives it: a DECIMAL when it
/// carries fractional seconds, an INT otherwise. Every other datum is already
/// in its own numeric domain and passes through.
fn numeric_context_value(value: Datum) -> Datum {
    let (number, fsp) = match value {
        Datum::Time(time) => (time.to_number(), time.fsp()),
        Datum::Duration(duration) => (duration.to_number(), duration.fsp()),
        other => return other,
    };
    if fsp > 0 {
        return Datum::Decimal(number);
    }
    // `to_number` of an fsp-0 temporal is integral, so truncation is exact.
    Datum::Int(number.to_i64_trunc().0)
}

/// `time` compared against a non-time datum, parsed into the time domain.
/// `None` is an unparseable value, which warns 1292 and compares as NULL.
fn time_compare_ordering(
    time: tidb_datatype::Time,
    other: &Datum,
    ctx: &dyn crate::context::Columns,
) -> Result<Option<std::cmp::Ordering>, EvalError> {
    let text = match other {
        Datum::String(value) => String::from_utf8_lossy(value.bytes()).into_owned(),
        Datum::Bytes(value) => String::from_utf8_lossy(value).into_owned(),
        // Go parses a numeric operand through its digits, so 20241231 is a
        // date and 20240615100000 a datetime.
        Datum::Int(value) => value.to_string(),
        Datum::UInt(value) => value.to_string(),
        Datum::Decimal(value) => value.to_string(),
        Datum::Real(value) => value.to_string(),
        _ => {
            return Err(EvalError::Unsupported(
                "comparing a time with this datum kind",
            ))
        }
    };
    // The session zone only matters for a Timestamp reading, which this
    // comparison does not shift; UTC keeps the parse deterministic.
    match time.compare_string(&text, true, true, &chrono_tz::Tz::UTC) {
        Ok(ordering) => Ok(Some(ordering)),
        Err(_) => {
            ctx.append_warning(1292, &format!("Incorrect datetime value: '{text}'"));
            Ok(None)
        }
    }
}

/// AUDIT (with [`to_f64`] and [`to_f64_with_mysql_string`] below): the
/// `unreachable!` and `.expect()` here are the ones a caller can reach with a
/// datum, so the claim is spelled out rather than asserted.
///
/// `Null` and the sentinels are rejected by [`eval_binary_full`]'s own early
/// returns, `String`/`Bytes` by its top guard, and `Raw`/`VectorFloat32` by
/// the guard added for them; every remaining kind -- including `Enum`,
/// `Set`, `Bit`, `BinaryLiteral`, `Time`, `Duration`, `Json` -- has an arm in
/// `Datum::to_decimal`, so the `.expect()` cannot fire. `extremum` is the
/// other caller, and it reaches these only after `eval_binary` has already
/// accepted the same values.
///
/// This is a proof about CALLERS, so it lapses if someone calls these from
/// somewhere new -- which is exactly what happened to
/// [`to_f64_with_mysql_string`], reached from `math_fn`, `string_fn`,
/// `builtin_ext::info` and `builtin_ext::compare2` on datums these guards
/// never saw. That function no longer relies on a caller proof at all: it
/// returns a `Result` and lets `Datum::to_f64` decide, per its own audit
/// table. `to_decimal` still rests on the argument above.
pub(crate) fn to_decimal(v: Datum) -> Decimal {
    match v {
        Datum::Decimal(d) => d,
        Datum::Int(i) => integer_to_decimal(Integer::Signed(i)),
        Datum::UInt(i) => integer_to_decimal(Integer::Unsigned(i)),
        Datum::String(_)
        | Datum::Bytes(_)
        | Datum::Real(_)
        | Datum::Null
        | Datum::MinNotNull
        | Datum::MaxValue => {
            unreachable!("guarded by caller")
        }
        other => {
            other
                .to_decimal()
                .expect("numeric caller must supply a decimal-convertible datum")
                .value
        }
    }
}

/// The bytes `value` contributes to a STRING-domain comparison, or `None` when
/// it belongs to some other comparison domain.
///
/// A hex/bit LITERAL (`x'..'`, `0x..`, `b'..'`) is a string operand, not a
/// number: Go's `DefaultTypeForValue` gives its `HexLiteral`/`BitLiteral` arms
/// `TypeVarString` with the binary charset (`pkg/types/field_type.go`), so
/// `GetAccurateCmpType` selects the ETString comparer whenever the other
/// operand is also a string, and `Datum.compareBinaryLiteral`
/// (`pkg/types/datum.go`) then compares the literal's RAW BYTES against a
/// `KindString`/`KindBytes` operand -- falling back to the literal's INTEGER
/// value only for every other operand kind. Both halves live here: the literal
/// joins the byte comparison only against another byte operand, and only under
/// a comparison operator, so arithmetic (`x'01020304' + 0` is `16909060`) and
/// numeric-operand comparison (`x'41' > 64` is 1) still take its integer value
/// through `coerce::integer_of` further down `eval_binary_full`.
///
/// The collation is NOT forced to `binary`: a hex literal is only
/// `CoercibilityCoercible` (Go `deriveCoercibilityForConstant`), so an
/// `CoercibilityImplicit` column wins the aggregation -- confirmed via `gorun`,
/// for `varchar(8) collate utf8mb4_general_ci c` holding `'AB'`,
/// `c = x'6162'` is 1.
///
/// `Datum::Bit` -- a BIT COLUMN's value rather than a literal -- deliberately
/// has no arm: `TypeBit`'s eval type is ETInt, so Go compares a bit column
/// NUMERICALLY even against a string (confirmed via `gorun`: for `bit(16) b`
/// holding `b'0100000101000010'`, `b = 'AB'` is 0 while `b = 16706` is 1).
/// `Enum`/`Set` DO get an arm, and it is their NAME, not their ordinal.
/// Go's `compareMysqlEnum`/`compareMysqlSet` compare against a `KindString`,
/// `KindBytes`, `KindMysqlEnum` or `KindMysqlSet` operand through the
/// COLLATOR on the element spelling, and drop to the ordinal only in the
/// `default` arm -- which is where a numeric operand lands, and where
/// [`integer_of`] already puts them. Captured via `gorun` for
/// `enum('a','b','c') e` holding `'b'` and `set('x','y') s` holding `'x'`:
/// `e = 'b'` is 1, `e < 'c'` is 1, `e = 'zzz'` is 0, `s = 'x'` is 1, and
/// `e = e2` is 1 for a DIFFERENTLY ORDERED `enum('b','z')` also holding
/// `'b'` -- ordinal 2 against ordinal 1, equal only by name. The numeric
/// readings stay: `e = 2` is 1 and `e <=> 3` is 0.
fn string_cmp_operand(value: &Datum, comparison: bool) -> Option<&[u8]> {
    match value {
        Datum::BinaryLiteral(literal) if comparison => Some(literal.as_bytes()),
        Datum::Enum(value, _) if comparison => Some(value.name().as_bytes()),
        Datum::Set(value, _) if comparison => Some(value.name().as_bytes()),
        other => other.as_raw_bytes(),
    }
}

/// Compares two strings under `collation`.
///
/// The PAD SPACE vs NO PAD rule is the collation's own (`Collation::compare`
/// transcreates each collator's `Compare`, including `utf8mb4_bin`'s trailing-
/// space trim and `binary`'s lack of one), so this function no longer decides
/// it. Only comparison operators are defined on strings here.
fn string_compare(
    op: BinaryOp,
    a: &[u8],
    b: &[u8],
    collation: tidb_datatype::Collation,
) -> Result<Datum, EvalError> {
    use BinaryOp::*;
    let ord = collation.compare(a, b);
    Ok(match op {
        Eq | NullEq => bool_int(ord == std::cmp::Ordering::Equal),
        Ne => bool_int(ord != std::cmp::Ordering::Equal),
        Lt => bool_int(ord == std::cmp::Ordering::Less),
        Le => bool_int(ord != std::cmp::Ordering::Greater),
        Gt => bool_int(ord == std::cmp::Ordering::Greater),
        Ge => bool_int(ord != std::cmp::Ordering::Less),
        _ => return Err(EvalError::Unsupported("string arithmetic")),
    })
}

/// MySQL shifts operate on 64-bit unsigned values; a shift amount `>= 64`
/// yields 0.
fn shift_left(a: u64, b: u64) -> u64 {
    if b >= 64 {
        0
    } else {
        a << b
    }
}

fn shift_right(a: u64, b: u64) -> u64 {
    if b >= 64 {
        0
    } else {
        a >> b
    }
}

/// FALSE dominates; otherwise NULL propagates if either side is unknown.
/// Also called directly from `crate::eval_in`'s `BETWEEN` handling (`x >= lo
/// AND x <= hi`), not just from `eval_binary`'s `LogicAnd` arm.
pub(crate) fn logic_and(l: Datum, r: Datum) -> Result<Datum, EvalError> {
    Ok(match (truthy_of(&l)?, truthy_of(&r)?) {
        (Some(false), _) | (_, Some(false)) => Datum::Int(0),
        (Some(true), Some(true)) => Datum::Int(1),
        _ => Datum::Null,
    })
}

fn logic_or(l: Datum, r: Datum) -> Result<Datum, EvalError> {
    // TRUE dominates; otherwise NULL propagates if either side is unknown.
    Ok(match (truthy_of(&l)?, truthy_of(&r)?) {
        (Some(true), _) | (_, Some(true)) => Datum::Int(1),
        (Some(false), Some(false)) => Datum::Int(0),
        _ => Datum::Null,
    })
}

fn logic_xor(l: Datum, r: Datum) -> Result<Datum, EvalError> {
    Ok(match (truthy_of(&l)?, truthy_of(&r)?) {
        (Some(a), Some(b)) => bool_int(a ^ b),
        _ => Datum::Null,
    })
}

/// Called from `eval_binary`'s own `NullEq` arm, after its `Str`/`Float`/
/// `Decimal`/`Json`/temporal guards have run.
///
/// The comment this replaces claimed the survivors "can only be `Int` or
/// `Null`". That was FALSE, and asserted without a capture. `Enum`, `Set`,
/// `Bit` and `BinaryLiteral` all reach here -- none is a string by
/// `as_raw_bytes`, and none has a numeric guard above -- and every one of
/// them hit the `unreachable!` and ABORTED THE PROCESS. `e <=> 2` on an
/// `enum` column was a one-query kill switch for the whole server.
///
/// The four are integral values, which is exactly what [`integer_of`]
/// already says about them, matching the `default` arms of Go's
/// `compareMysqlEnum`/`compareMysqlSet`/`compareBinaryLiteral` (all of which
/// fall through to a numeric comparison for a non-string operand). Captured
/// via `gorun` for `enum('a','b','c') e` = `'b'`, `set('x','y') s` = `'x'`,
/// `bit(8) b` = `b'00000010'`: `e <=> 2` is 1, `e <=> 5` is 0,
/// `e <=> null` is 0, `s <=> 1` is 1, `b <=> 2` is 1.
///
/// (`e <=> 'b'` is 1 too, but that pair never arrives here -- comparing an
/// enum with a string is a NAME comparison resolved by
/// [`string_cmp_operand`] further up.)
///
/// `<=>` is the one comparison with no NULL result: an operand that is NULL
/// makes the answer 0, or 1 when both are, and never propagates.
fn null_safe_eq(l: Datum, r: Datum) -> Result<Datum, EvalError> {
    match (&l, &r) {
        (Datum::Null, Datum::Null) => return Ok(Datum::Int(1)),
        (Datum::Null, _) | (_, Datum::Null) => return Ok(Datum::Int(0)),
        _ => {}
    }
    match (integer_of(&l)?, integer_of(&r)?) {
        (Some(a), Some(b)) => Ok(bool_int(integer_cmp(a, b).is_eq())),
        // The residue is an error, not a panic, for the reason recorded on
        // `eval_binary_full`'s own residue: the previous assertion that this
        // point was unreachable was wrong, and being wrong cost the process.
        _ => Err(EvalError::UnsupportedOperandPair(l.kind(), r.kind())),
    }
}

#[cfg(test)]
mod tests {
    use super::{
        bytes_to_f64, eval_binary, eval_binary_full, to_f64_with_mysql_string,
        DERIVATION_FREE_COLLATION,
    };
    use crate::{Datum, Decimal, EvalError};
    use tidb_ast::BinaryOp;

    /// A `Columns` resolver that keeps every warning it is handed, so a test
    /// can assert the CODE, the exact MESSAGE, and the COUNT.
    #[derive(Default)]
    struct WarningLog(std::cell::RefCell<Vec<(u16, String)>>);

    impl crate::Columns for WarningLog {
        fn get(&self, _: &[String]) -> Option<Datum> {
            None
        }
        fn append_warning(&self, code: u16, message: &str) {
            self.0.borrow_mut().push((code, message.to_owned()));
        }
    }

    impl WarningLog {
        fn taken(&self) -> Vec<(u16, String)> {
            self.0.take()
        }
    }

    fn truncated(text: &str) -> (u16, String) {
        (1292, format!("Truncated incorrect DOUBLE value: '{text}'"))
    }

    /// The 1292 warning Go raises while coercing a string into the ETReal
    /// domain, which this crate had no channel for until `Columns` was
    /// threaded into the values-only dispatch.
    ///
    /// Every expectation is a `gorun` capture against a real session, read off
    /// `SHOW WARNINGS`. The three properties that matter are all pinned here,
    /// because getting the value right while getting any of them wrong still
    /// diverges on the recorded warning line:
    ///
    ///  * the CODE and the exact MESSAGE, including that the quoted text is
    ///    the TRIMMED operand;
    ///  * WHICH operands raise it -- a full numeric string, a trailing `.`,
    ///    a trailing `e`, and the EMPTY string all raise NOTHING, and the
    ///    empty string is silent only because the engine reaches this
    ///    coercion with `isFuncCast` set;
    ///  * the COUNT: one per coercion, never deduplicated, so a two-operand
    ///    expression over two bad strings raises TWO.
    #[test]
    fn string_to_real_coercion_raises_go_truncation_warnings() {
        let log = WarningLog::default();
        let s = |text: &str| Datum::new_string(text.to_owned());

        assert_eq!(to_f64_with_mysql_string(&s("12abc"), &log), Ok(12.0));
        assert_eq!(log.taken(), vec![truncated("12abc")]);

        // The message names the operand AFTER `strings.TrimSpace`, and a
        // padded but otherwise complete number raises nothing at all.
        assert_eq!(to_f64_with_mysql_string(&s(" 12abc "), &log), Ok(12.0));
        assert_eq!(log.taken(), vec![truncated("12abc")]);
        assert_eq!(to_f64_with_mysql_string(&s(" 12 "), &log), Ok(12.0));
        assert_eq!(log.taken(), vec![]);

        // Silent rows. `12.` and `1e` are Go's own two "shorter prefix, no
        // error" returns from `getValidFloatPrefix`, and the empty string is
        // the `isFuncCast` exemption.
        for quiet in ["12", "12.5", "12.", "1e", ""] {
            assert!(to_f64_with_mysql_string(&s(quiet), &log).is_ok());
            assert_eq!(log.taken(), vec![], "{quiet:?}");
        }

        // A prefix with no digits at all still reads 0 and still warns, and
        // an out-of-range exponent saturates to f64::MAX with the SAME
        // message rather than a distinct overflow one.
        assert_eq!(to_f64_with_mysql_string(&s("abc"), &log), Ok(0.0));
        assert_eq!(log.taken(), vec![truncated("abc")]);
        assert_eq!(to_f64_with_mysql_string(&s("1e400"), &log), Ok(f64::MAX));
        assert_eq!(log.taken(), vec![truncated("1e400")]);

        // A comparison raises it only when the string actually meets a
        // NUMBER:  picks ETReal for that pair, while a
        // string/string comparison stays in the string domain and warns
        // nothing (captured both ways).
        assert_eq!(
            eval_binary_full(
                BinaryOp::Eq,
                s("12abc"),
                Datum::Int(12),
                4,
                DERIVATION_FREE_COLLATION,
                &log,
            ),
            Ok(Datum::Int(1))
        );
        assert_eq!(log.taken(), vec![truncated("12abc")]);
        assert_eq!(
            eval_binary_full(
                BinaryOp::Eq,
                s("12abc"),
                s("3xyz"),
                4,
                DERIVATION_FREE_COLLATION,
                &log,
            ),
            Ok(Datum::Int(0))
        );
        assert_eq!(log.taken(), vec![]);

        // One warning per coercion, never deduplicated: both arguments of a
        // two-argument builtin raise their own.
        assert_eq!(
            crate::math_fn::dispatch_values("POW", &[s("2abc"), s("3xyz")], &log)
                .expect("dispatches"),
            Ok(Datum::Real(8.0))
        );
        assert_eq!(log.taken(), vec![truncated("2abc"), truncated("3xyz")]);

        // The same sink serves every family that coerces, not just math:
        // FIELD, FORMAT, INTERVAL and FORMAT_BYTES each raise their own.
        assert!(crate::string_fn::field(&[Datum::Int(1), s("12abc")], &log).is_ok());
        assert_eq!(log.taken(), vec![truncated("12abc")]);
        assert!(crate::string_fn::format_num(&[s("12abc"), Datum::Int(2)], &log).is_ok());
        assert_eq!(log.taken(), vec![truncated("12abc")]);
        assert!(
            crate::builtin_ext::dispatch("INTERVAL", &[Datum::Int(1), s("12abc")], &log)
                .expect("dispatches")
                .is_ok()
        );
        assert_eq!(log.taken(), vec![truncated("12abc")]);
        assert!(
            crate::builtin_ext::dispatch("FORMAT_BYTES", &[s("12abc")], &log)
                .expect("dispatches")
                .is_ok()
        );
        assert_eq!(log.taken(), vec![truncated("12abc")]);
    }

    /// `MIN`/`MAX` over a `json` column -- TiDB's own issue-31640 case, whose
    /// recorded answers are `min(a)` = `1` and `max(a)` = `[3, 4]` over
    /// `'"a"','"B"','"c"','"D"','{"a":1}','1','{"b":2}','[1,2]','[3,4]'`
    /// (`tests/integrationtest/r/expression/issues.result`). Aggregation
    /// compares through `compare_datums` -> `eval_binary`, so before the JSON
    /// arm existed both operands fell past every guard into the integer-only
    /// path and its `unreachable!` ABORTED the process -- which is what made
    /// `expression/issues` and `expression/json` crashing topics.
    ///
    /// The ordering asserted here is Go `CompareBinaryJSON`'s type precedence
    /// (number < string < object < array), not merely "does not panic": a JSON
    /// comparison silently answering by some other rule would change which row
    /// `MIN` returns.
    #[test]
    fn json_operands_compare_by_gos_json_type_precedence_instead_of_aborting() {
        use tidb_datatype::BinaryJSON;
        let json = |text: &str| Datum::Json(BinaryJSON::parse(text).unwrap());
        // Number is the lowest precedence, array the highest.
        for (lower, higher) in [
            ("1", r#""a""#),
            (r#""a""#, r#"{"a": 1}"#),
            (r#"{"a": 1}"#, "[1, 2]"),
            ("1", "[3, 4]"),
        ] {
            assert_eq!(
                eval_binary(BinaryOp::Lt, json(lower), json(higher)),
                Ok(Datum::Int(1)),
                "{lower} < {higher}"
            );
            assert_eq!(
                eval_binary(BinaryOp::Gt, json(lower), json(higher)),
                Ok(Datum::Int(0)),
                "{lower} > {higher}"
            );
        }
        // Within one type the values themselves order: `"B"` before `"a"` by
        // byte order, and `[1, 2]` before `[3, 4]` element-wise.
        assert_eq!(
            eval_binary(BinaryOp::Lt, json(r#""B""#), json(r#""a""#)),
            Ok(Datum::Int(1))
        );
        assert_eq!(
            eval_binary(BinaryOp::Lt, json("[1, 2]"), json("[3, 4]")),
            Ok(Datum::Int(1))
        );
        assert_eq!(
            eval_binary(BinaryOp::Eq, json(r#"{"a": 1}"#), json(r#"{"a": 1}"#)),
            Ok(Datum::Int(1))
        );
        // A JSON operand under an ARITHMETIC operator is refused, not guessed.
        assert!(matches!(
            eval_binary(BinaryOp::Plus, json("1"), Datum::Int(1)),
            Err(EvalError::Unsupported(_))
        ));
    }

    /// The third escape from the same catch-all, closed before it happened.
    ///
    /// `Float32` and `Json` each reached the integer-only residue and aborted
    /// the PROCESS -- every connection, not just the offending one. `Raw` and
    /// `VectorFloat32` are the kinds still unclaimed by any dispatch, and
    /// they were on exactly the same track. What each one hit differed by
    /// operator, which is why the guard is one check rather than several:
    /// `Div`/`Decimal` panicked inside `to_decimal`'s `.expect`, a
    /// string comparison silently substituted `0.0` and returned a WRONG
    /// answer, and everything else hit the `unreachable!`.
    ///
    /// Assertions are on the error, not merely on "did not panic": the
    /// string-comparison case in particular used to RETURN, so a test that
    /// only checked for absence of a panic would have passed against the bug.
    #[test]
    fn unclaimed_datum_kinds_report_an_error_instead_of_aborting_or_guessing() {
        use tidb_datatype::{DatumKind, VectorFloat32};
        let raw = || Datum::Raw(vec![0x08, 0x02]);
        let vector = || Datum::VectorFloat32(VectorFloat32::must_create(vec![1.0, 2.0]));
        for (name, operand, kind) in [
            ("raw", raw as fn() -> Datum, DatumKind::Raw),
            ("vector", vector as fn() -> Datum, DatumKind::VectorFloat32),
        ] {
            for op in [
                BinaryOp::Eq,
                BinaryOp::Lt,
                BinaryOp::Plus,
                BinaryOp::Div,
                BinaryOp::Mul,
            ] {
                assert_eq!(
                    eval_binary(op, operand(), Datum::Int(1)),
                    Err(EvalError::UnsupportedOperandPair(kind, DatumKind::Int)),
                    "{name} {op:?} int"
                );
                assert_eq!(
                    eval_binary(op, Datum::Decimal(Decimal::from_int(3)), operand()),
                    Err(EvalError::UnsupportedOperandPair(DatumKind::Decimal, kind)),
                    "decimal {op:?} {name}"
                );
            }
        }
        // A vector compared with a STRING is the silent-wrong-answer case:
        // `to_f64_with_mysql_string` used to read the vector as `0.0`, so
        // `vector = '0'` answered TRUE. It is an error now.
        assert_eq!(
            eval_binary(BinaryOp::Eq, vector(), Datum::Bytes(b"0".to_vec()),),
            Err(EvalError::UnsupportedOperandPair(
                DatumKind::VectorFloat32,
                DatumKind::Bytes
            ))
        );
        // CONTROL: `Raw` keeps the byte semantics `as_raw_bytes` gives it, so
        // raw-vs-raw and raw-vs-string comparisons must be unaffected by the
        // guard above them.
        assert_eq!(eval_binary(BinaryOp::Eq, raw(), raw()), Ok(Datum::Int(1)));
        assert_eq!(
            eval_binary(BinaryOp::Eq, raw(), Datum::Bytes(vec![0x08, 0x02])),
            Ok(Datum::Int(1))
        );
        assert_eq!(
            eval_binary(BinaryOp::Lt, raw(), Datum::Bytes(vec![0x09])),
            Ok(Datum::Int(1))
        );
    }

    /// The SAME unclaimed kinds, reached through the FUNCTION callers rather
    /// than through `eval_binary`.
    ///
    /// `eval_binary`'s guard above is a proof about one caller, and
    /// `to_f64_with_mysql_string` has four others that guard nothing:
    /// `math_fn`, `string_fn`, `builtin_ext::info` and
    /// `builtin_ext::compare2`. Every one of them used to read a vector as
    /// `0.0` -- `FORMAT_BYTES(vec)` answered `0 bytes`. TiDB errors on all of
    /// these (`SQRT(VEC_FROM_TEXT('[1,2]'))`, `FIELD(1, ...)`,
    /// `INTERVAL(1, ...)`, `FORMAT_BYTES(...)`, `FORMAT(..., 2)` -- captured,
    /// each one `ERR`), and the enum/ordinal controls beside them must keep
    /// their VALUES.
    #[test]
    fn function_callers_reject_the_unclaimed_kinds_too() {
        use tidb_datatype::{Collation, MysqlEnum, VectorFloat32};
        let vector = || Datum::VectorFloat32(VectorFloat32::must_create(vec![1.0, 2.0]));
        assert!(to_f64_with_mysql_string(&vector(), &crate::NoColumns).is_err());
        assert!(to_f64_with_mysql_string(&Datum::Raw(vec![0x08]), &crate::NoColumns).is_err());
        // `Null` and the range sentinels are errors in Go's `ToFloat64` as
        // well, and are no longer an `unreachable!` betting on the caller.
        assert!(to_f64_with_mysql_string(&Datum::Null, &crate::NoColumns).is_err());
        assert!(to_f64_with_mysql_string(&Datum::MaxValue, &crate::NoColumns).is_err());

        for name in ["SQRT", "EXP", "LN", "SIN", "CEIL", "FLOOR"] {
            assert!(
                crate::math_fn::dispatch_values(name, &[vector()], &crate::NoColumns)
                    .expect("dispatches")
                    .is_err(),
                "{name}(vector)"
            );
        }
        assert!(crate::string_fn::field(&[Datum::Int(1), vector()], &crate::NoColumns).is_err());
        assert!(
            crate::string_fn::format_num(&[vector(), Datum::Int(2)], &crate::NoColumns).is_err()
        );
        assert!(crate::string_fn::format_num(
            &[Datum::Raw(vec![0x08]), Datum::Int(2)],
            &crate::NoColumns
        )
        .is_err());
        assert!(
            crate::builtin_ext::dispatch("FORMAT_BYTES", &[vector()], &crate::NoColumns)
                .expect("dispatches")
                .is_err()
        );
        assert!(crate::builtin_ext::dispatch(
            "INTERVAL",
            &[Datum::Int(1), vector()],
            &crate::NoColumns
        )
        .expect("dispatches")
        .is_err());

        // CONTROLS: the kinds that DO convert keep TiDB's value. `'8'` is
        // ordinal 2 of `enum('9','8','7')`, and `gorun` reads it as the
        // ordinal, never as the name -- `SQRT(e)` is `SQRT(2)`.
        let e = || Datum::Enum(MysqlEnum::new("8", 2), Collation::Utf8Mb4Bin);
        assert_eq!(to_f64_with_mysql_string(&e(), &crate::NoColumns), Ok(2.0));
        assert_eq!(
            crate::math_fn::dispatch_values("SQRT", &[e()], &crate::NoColumns).expect("dispatches"),
            Ok(Datum::Real(2.0f64.sqrt()))
        );
        // `FORMAT_BYTES(e)` is `2 bytes`, not `0 bytes`.
        assert_eq!(
            crate::builtin_ext::dispatch("FORMAT_BYTES", &[e()], &crate::NoColumns)
                .expect("dispatches")
                .map(|value| value.sql_string().unwrap()),
            Ok("2 bytes".to_owned())
        );
        // `FORMAT` reads the same ETReal: TiDB answers `2.00` for the enum's
        // ordinal, and for a temporal argument it formats that argument's
        // NUMBER, never its text. `FORMAT(CAST('2021-01-01' AS DATETIME),2)`
        // is `20,210,101,000,000.00` (a DATE column, whose number carries no
        // time part, gives `20,210,101.00`) -- captured, where rendering the
        // text `'2021-01-01'` used to answer `2.00`.
        assert_eq!(
            crate::string_fn::format_num(&[e(), Datum::Int(2)], &crate::NoColumns)
                .map(|value| value.sql_string().unwrap()),
            Ok("2.00".to_owned())
        );
        let date = Datum::Time(
            tidb_datatype::str_to_datetime("2021-01-01", 0, &chrono_tz::Tz::UTC)
                .expect("literal date")
                .value,
        );
        assert_eq!(
            crate::string_fn::format_num(&[date, Datum::Int(2)], &crate::NoColumns)
                .map(|value| value.sql_string().unwrap()),
            Ok("20,210,101,000,000.00".to_owned())
        );
    }

    /// `enum`/`set` comparisons, both halves of Go's split.
    ///
    /// Against a NUMBER the ordinal is compared; against a STRING (or another
    /// enum/set) the element NAME is, through the collator. Go's
    /// `compareMysqlEnum`/`compareMysqlSet` make that split explicitly and
    /// this evaluator made neither side of it: the numeric side reached
    /// `null_safe_eq`'s `unreachable!` and ABORTED THE PROCESS on
    /// `e <=> 2`, and the name side fell to the string-vs-numeric rule,
    /// which read `'b'` as the numeric prefix `0.0` and answered `e = 'b'`
    /// FALSE.
    ///
    /// Every expectation below is a `gorun` capture over
    /// `enum('a','b','c') e` holding `'b'`, a second `enum('b','z') e2` also
    /// holding `'b'`, and `set('x','y') s` holding `'x'`.
    #[test]
    fn enum_and_set_compare_by_name_against_text_and_by_ordinal_against_numbers() {
        use tidb_datatype::{Collation, MysqlEnum, MysqlSet};
        // 'b' is ordinal 2 in enum('a','b','c') but ordinal 1 in enum('b','z').
        let e = || Datum::Enum(MysqlEnum::new("b", 2), Collation::Utf8Mb4Bin);
        let e2 = || Datum::Enum(MysqlEnum::new("b", 1), Collation::Utf8Mb4Bin);
        let s = || Datum::Set(MysqlSet::new("x", 1), Collation::Utf8Mb4Bin);
        let text = |t: &str| Datum::Bytes(t.as_bytes().to_vec());

        // Against text: by NAME. `e = e2` is the sharpest case -- ordinal 2
        // against ordinal 1, equal only because both spell "b".
        for (op, l, r, want) in [
            (BinaryOp::Eq, e(), text("b"), 1),
            (BinaryOp::NullEq, e(), text("b"), 1),
            (BinaryOp::Lt, e(), text("c"), 1),
            (BinaryOp::Eq, e(), text("zzz"), 0),
            (BinaryOp::Eq, e(), e2(), 1),
            (BinaryOp::Eq, s(), text("x"), 1),
        ] {
            assert_eq!(
                eval_binary(op, l.clone(), r.clone()),
                Ok(Datum::Int(want)),
                "{l:?} {op:?} {r:?}"
            );
        }

        // Against a number: by ORDINAL, and `<=>` never yields NULL.
        for (op, l, r, want) in [
            (BinaryOp::Eq, e(), Datum::Int(2), 1),
            (BinaryOp::NullEq, e(), Datum::Int(2), 1),
            (BinaryOp::NullEq, e(), Datum::Int(5), 0),
            (BinaryOp::NullEq, e(), Datum::Null, 0),
            (BinaryOp::NullEq, s(), Datum::Int(1), 1),
            (BinaryOp::Eq, s(), Datum::Int(1), 1),
        ] {
            assert_eq!(
                eval_binary(op, l.clone(), r.clone()),
                Ok(Datum::Int(want)),
                "{l:?} {op:?} {r:?}"
            );
        }

        // A BIT column stays numeric even against text -- `TypeBit`'s eval
        // type is ETInt -- so it deliberately gains no name arm. `b <=> 2`
        // is 1 where it used to abort.
        let bit = || Datum::Bit(tidb_datatype::BinaryLiteral::from(vec![0x02_u8]));
        assert_eq!(
            eval_binary(BinaryOp::NullEq, bit(), Datum::Int(2)),
            Ok(Datum::Int(1))
        );
        assert_eq!(
            eval_binary(BinaryOp::NullEq, bit(), Datum::Null),
            Ok(Datum::Int(0))
        );
    }

    #[test]
    fn mixed_string_number_comparisons_use_mysql_real_prefix() {
        for (text, expected) in [
            ("123", 123.0),
            ("  -2.5tail", -2.5),
            (".5tail", 0.5),
            ("1e2tail", 100.0),
            ("1e", 1.0),
            ("not a number", 0.0),
        ] {
            assert_eq!(
                bytes_to_f64(text.as_bytes(), &crate::NoColumns),
                expected,
                "{text}"
            );
        }
        assert_eq!(bytes_to_f64(b"1e999", &crate::NoColumns), f64::MAX);
        assert_eq!(bytes_to_f64(b"-1e999", &crate::NoColumns), -f64::MAX);
        // A `binary`/`latin1` payload is not UTF-8, and TiDB still reads its
        // numeric prefix: `SELECT ABS(0x3132FF)` answers 12, captured from a
        // running session. Reading it as 0.0 was a silent wrong answer.
        assert_eq!(bytes_to_f64(&[b'1', b'2', 0xFF], &crate::NoColumns), 12.0);
        assert_eq!(bytes_to_f64(&[0xFF], &crate::NoColumns), 0.0);
        for datum in [
            Datum::Bytes(vec![b'4', 0xFF]),
            Datum::new_string(vec![b'4', 0xFF]),
        ] {
            assert_eq!(to_f64_with_mysql_string(&datum, &crate::NoColumns), Ok(4.0));
        }
        assert_eq!(
            eval_binary(
                BinaryOp::Eq,
                Datum::new_string("12x".to_owned()),
                Datum::Int(12),
            ),
            Ok(Datum::Int(1))
        );
        assert_eq!(
            eval_binary(
                BinaryOp::NullEq,
                Datum::Null,
                Datum::new_string("1".to_owned())
            ),
            Ok(Datum::Int(0))
        );
        for sentinel in [Datum::min_not_null(), Datum::max_value()] {
            assert_eq!(
                eval_binary(BinaryOp::NullEq, Datum::Null, sentinel.clone()),
                Err(EvalError::Unsupported("range sentinel expression operand"))
            );
            assert_eq!(
                eval_binary(BinaryOp::Eq, sentinel, Datum::new_string("1")),
                Err(EvalError::Unsupported("range sentinel expression operand"))
            );
        }
    }

    /// Int-vs-Decimal comparison is EXACT — both promote to Decimal (MySQL's
    /// implicit rule), never lossily through `f64`. Two integers 1 apart that
    /// share a single `f64` must still compare unequal. An explicit REAL operand,
    /// by contrast, forces the lossy `f64` comparison (Real dominates Decimal in
    /// the promotion hierarchy). goeval-verified: `... = ...806.0` -> 0 (decimal),
    /// `... = ...806e0` -> 1 (real).
    #[test]
    fn int_vs_decimal_comparison_is_exact_not_lossy_through_f64() {
        let big_int = Datum::Int(9223372036854775807);
        let near_decimal = Datum::Decimal(Decimal::from_literal("9223372036854775806.0"));
        assert_eq!(
            eval_binary(BinaryOp::Eq, big_int.clone(), near_decimal.clone()),
            Ok(Datum::Int(0)),
            "differ by 1 -> not equal, despite sharing one f64"
        );
        assert_eq!(
            eval_binary(BinaryOp::Gt, big_int, near_decimal),
            Ok(Datum::Int(1))
        );
        // 2^53 + 1 vs 2^53 (the f64 integer-precision boundary) is also exact.
        assert_eq!(
            eval_binary(
                BinaryOp::Eq,
                Datum::Int(9007199254740993),
                Datum::Decimal(Decimal::from_literal("9007199254740992.0")),
            ),
            Ok(Datum::Int(0))
        );
        // Contrast: an explicit REAL operand IS lossy — both round to the same
        // f64 and compare equal, matching MySQL's Real-dominates hierarchy.
        assert_eq!(
            eval_binary(
                BinaryOp::Eq,
                Datum::Int(9223372036854775807),
                Datum::Real(9223372036854775806.0),
            ),
            Ok(Datum::Int(1)),
        );
    }

    /// Decimal `/` result scale = left-operand scale + `div_precision_increment`
    /// (default 4), with the last digit ROUNDED (not truncated), trailing zeros
    /// kept, and `/0` -> NULL. Authoritative goeval values, rendered through
    /// `to_string` so the scale is checked, not just the value.
    #[test]
    fn decimal_division_scale_and_rounding_match_go() {
        fn div(l: Datum, r: Datum) -> String {
            match eval_binary(BinaryOp::Div, l, r) {
                Ok(Datum::Decimal(d)) => d.to_string(),
                Ok(Datum::Null) => "NULL".to_owned(),
                other => panic!("unexpected division result: {other:?}"),
            }
        }
        let dec = |t: &str| Datum::Decimal(Decimal::from_literal(t));
        assert_eq!(div(Datum::Int(1), Datum::Int(3)), "0.3333");
        assert_eq!(div(Datum::Int(2), Datum::Int(3)), "0.6667"); // last digit rounds up
        assert_eq!(div(dec("1.0"), Datum::Int(3)), "0.33333"); // scale 1 + 4
        assert_eq!(div(Datum::Int(10), Datum::Int(3)), "3.3333");
        assert_eq!(div(Datum::Int(7), Datum::Int(2)), "3.5000"); // trailing zeros kept
        assert_eq!(div(Datum::Int(1), Datum::Int(30000)), "0.0000");
        assert_eq!(div(Datum::Int(22), Datum::Int(7)), "3.1429"); // rounds up
        assert_eq!(div(dec("0.1"), dec("0.3")), "0.33333");
        assert_eq!(div(Datum::Int(5), Datum::Int(3)), "1.6667");
        assert_eq!(div(Datum::Int(1), Datum::Int(0)), "NULL".to_owned());
    }

    #[test]
    fn signed_int_division_rejects_the_only_overflow_case() {
        assert_eq!(
            eval_binary(BinaryOp::IntDiv, Datum::Int(i64::MIN), Datum::Int(-1)),
            Err(EvalError::IntOverflow)
        );
        assert_eq!(
            eval_binary(BinaryOp::Mod, Datum::Int(i64::MIN), Datum::Int(-1)),
            Ok(Datum::Int(0))
        );
    }

    /// Direct rows from `builtin_arithmetic_test.go::{TestArithmeticIntDivide,
    /// TestArithmeticMod}`.  The source has separate evaluator signatures for
    /// every signedness pair; these assertions keep that distinction visible
    /// in the compact Datum evaluator instead of silently dividing raw bits.
    #[test]
    fn integer_division_matches_go_signedness_helpers() {
        for (lhs, rhs, expected) in [
            (Datum::Int(13), Datum::Int(11), Ok(Datum::Int(1))),
            (Datum::Int(-13), Datum::Int(11), Ok(Datum::Int(-1))),
            (Datum::UInt(13), Datum::UInt(11), Ok(Datum::UInt(1))),
            (Datum::UInt(13), Datum::Int(11), Ok(Datum::UInt(1))),
            (Datum::UInt(1), Datum::Int(-2), Ok(Datum::UInt(0))),
            (Datum::Int(13), Datum::UInt(11), Ok(Datum::UInt(1))),
            (Datum::Int(-1), Datum::UInt(11), Ok(Datum::UInt(0))),
            (Datum::Int(13), Datum::Int(0), Ok(Datum::Null)),
        ] {
            assert_eq!(eval_binary(BinaryOp::IntDiv, lhs, rhs), expected);
        }
        assert_eq!(
            eval_binary(BinaryOp::IntDiv, Datum::UInt(1), Datum::Int(-1)),
            Err(EvalError::IntOverflow)
        );
        assert_eq!(
            eval_binary(BinaryOp::IntDiv, Datum::Int(-13), Datum::UInt(11)),
            Err(EvalError::IntOverflow)
        );
    }

    #[test]
    fn integer_mod_preserves_go_dividend_sign_and_result_flag() {
        for (lhs, rhs, expected) in [
            (Datum::Int(13), Datum::Int(11), Datum::Int(2)),
            (Datum::Int(-13), Datum::Int(11), Datum::Int(-2)),
            (Datum::Int(13), Datum::Int(-11), Datum::Int(2)),
            (Datum::UInt(13), Datum::UInt(11), Datum::UInt(2)),
            (Datum::UInt(13), Datum::Int(-11), Datum::UInt(2)),
            (Datum::Int(-22), Datum::UInt(10), Datum::Int(-2)),
            (Datum::Int(i64::MIN), Datum::UInt(3), Datum::Int(-2)),
            (Datum::Int(13), Datum::Int(0), Datum::Null),
        ] {
            assert_eq!(eval_binary(BinaryOp::Mod, lhs, rhs), Ok(expected));
        }
    }

    /// `builtinArithmeticPlusIntSig`: integer `+` reports `ErrOverflow` in every
    /// signedness case rather than silently adding the raw two's-complement bits.
    /// (`-`/`*` overflow is tracked separately, behind Go-vector verification.)
    #[test]
    fn integer_plus_reports_overflow_like_go_instead_of_wrapping() {
        for (lhs, rhs, expected) in [
            // signed + signed
            (
                Datum::Int(i64::MAX),
                Datum::Int(1),
                Err(EvalError::IntOverflow),
            ),
            (
                Datum::Int(i64::MIN),
                Datum::Int(-1),
                Err(EvalError::IntOverflow),
            ),
            (
                Datum::Int(i64::MAX),
                Datum::Int(-1),
                Ok(Datum::Int(i64::MAX - 1)),
            ),
            // unsigned + unsigned
            (
                Datum::UInt(u64::MAX),
                Datum::UInt(1),
                Err(EvalError::IntOverflow),
            ),
            (
                Datum::UInt(u64::MAX),
                Datum::UInt(0),
                Ok(Datum::UInt(u64::MAX)),
            ),
            // mixed -> unsigned result: a negative addend can underflow past 0,
            // and a positive addend can overflow past u64::MAX.
            (Datum::UInt(1), Datum::Int(-2), Err(EvalError::IntOverflow)),
            (Datum::UInt(5), Datum::Int(-2), Ok(Datum::UInt(3))),
            (
                Datum::UInt(u64::MAX),
                Datum::Int(1),
                Err(EvalError::IntOverflow),
            ),
            // commutative: signed + unsigned matches unsigned + signed.
            (Datum::Int(-2), Datum::UInt(5), Ok(Datum::UInt(3))),
            (Datum::Int(-2), Datum::UInt(1), Err(EvalError::IntOverflow)),
        ] {
            assert_eq!(eval_binary(BinaryOp::Plus, lhs, rhs), expected);
        }
    }

    /// Signed-signed `-`/`*` overflow is `ErrOverflow` (goeval-confirmed for the
    /// signed domain: `-9223372036854775808 - 1` and `9223372036854775807 * 2`
    /// both ERR); a non-overflowing signed result, including a negative, passes.
    /// The unsigned/mixed cases have their own dedicated vector tests.
    #[test]
    fn signed_minus_and_mul_report_overflow_like_go() {
        for (op, lhs, rhs, expected) in [
            (
                BinaryOp::Minus,
                Datum::Int(i64::MIN),
                Datum::Int(1),
                Err(EvalError::IntOverflow),
            ),
            (
                BinaryOp::Minus,
                Datum::Int(i64::MAX),
                Datum::Int(-1),
                Err(EvalError::IntOverflow),
            ),
            (
                BinaryOp::Minus,
                Datum::Int(2),
                Datum::Int(5),
                Ok(Datum::Int(-3)),
            ),
            (
                BinaryOp::Mul,
                Datum::Int(i64::MAX),
                Datum::Int(2),
                Err(EvalError::IntOverflow),
            ),
            (
                BinaryOp::Mul,
                Datum::Int(i64::MIN),
                Datum::Int(-1),
                Err(EvalError::IntOverflow),
            ),
            (
                BinaryOp::Mul,
                Datum::Int(-2),
                Datum::Int(3),
                Ok(Datum::Int(-6)),
            ),
        ] {
            assert_eq!(eval_binary(op, lhs, rhs), expected);
        }
    }

    /// Unsigned/mixed `-` overflow, the literal `minus_overflows` port. Expected
    /// values are the authoritative goeval results (goeval errors exactly when
    /// TiDB does, and prints an unsigned result as its signed-bit form — so e.g.
    /// `INT:9223372036854775807` is the unsigned value here). A non-overflowing
    /// mixed/unsigned difference is UNSIGNED (result type lhs||rhs).
    #[test]
    fn unsigned_and_mixed_minus_match_go_overflow_check() {
        for (lhs, rhs, expected) in [
            // unsigned - unsigned
            (Datum::UInt(5), Datum::UInt(2), Ok(Datum::UInt(3))),
            (Datum::UInt(2), Datum::UInt(5), Err(EvalError::IntOverflow)),
            (
                Datum::UInt(9223372036854775808),
                Datum::UInt(1),
                Ok(Datum::UInt(9223372036854775807)),
            ),
            // unsigned - signed
            (Datum::UInt(5), Datum::Int(2), Ok(Datum::UInt(3))),
            (Datum::UInt(2), Datum::Int(5), Err(EvalError::IntOverflow)),
            (Datum::UInt(2), Datum::Int(-5), Ok(Datum::UInt(7))),
            (
                Datum::UInt(u64::MAX),
                Datum::Int(-1),
                Err(EvalError::IntOverflow),
            ),
            // signed - unsigned
            (Datum::Int(5), Datum::UInt(2), Ok(Datum::UInt(3))),
            (Datum::Int(2), Datum::UInt(5), Err(EvalError::IntOverflow)),
            (Datum::Int(-5), Datum::UInt(2), Err(EvalError::IntOverflow)),
            // corner: ua < ub yet the two's-complement res >= 0 (b's bits are a
            // huge unsigned), Go's inner `if res >= 0 { return true }`.
            (
                Datum::UInt(0),
                Datum::UInt(9223372036854775809),
                Err(EvalError::IntOverflow),
            ),
            // s-u boundary: `uint64(a - MinInt64) == ub` does not overflow, but the
            // resulting difference is negative -> overflow.
            (
                Datum::Int(-1),
                Datum::UInt(9223372036854775807),
                Err(EvalError::IntOverflow),
            ),
            // s-u that stays in range -> unsigned 0.
            (
                Datum::Int(9223372036854775807),
                Datum::UInt(9223372036854775807),
                Ok(Datum::UInt(0)),
            ),
            // uu equal -> 0 (resUnsigned, no overflow).
            (
                Datum::UInt(u64::MAX),
                Datum::UInt(u64::MAX),
                Ok(Datum::UInt(0)),
            ),
        ] {
            assert_eq!(eval_binary(BinaryOp::Minus, lhs, rhs), expected);
        }
    }

    /// Unsigned/mixed `*` matches Go `builtinArithmeticMultiplyIntUnsignedSig`
    /// (either operand unsigned -> unsigned result, multiplied as u64 bit
    /// patterns, overflow when the product wraps). Authoritative goeval values;
    /// an unsigned result prints as its signed-bit form (`u64::MAX` -> `INT:-1`).
    #[test]
    fn unsigned_and_mixed_mul_match_go_overflow_check() {
        for (lhs, rhs, expected) in [
            // unsigned * unsigned
            (Datum::UInt(3), Datum::UInt(4), Ok(Datum::UInt(12))),
            (
                Datum::UInt(u64::MAX),
                Datum::UInt(1),
                Ok(Datum::UInt(u64::MAX)),
            ),
            (Datum::UInt(0), Datum::UInt(5), Ok(Datum::UInt(0))),
            (
                Datum::UInt(4294967296),
                Datum::UInt(4294967296),
                Err(EvalError::IntOverflow),
            ),
            // 2^32 * (2^32 - 1) = 2^64 - 2^32, still within u64 (goeval INT:-4294967296).
            (
                Datum::UInt(4294967296),
                Datum::UInt(4294967295),
                Ok(Datum::UInt(18446744069414584320)),
            ),
            // mixed (either operand unsigned -> unsigned result): a negative
            // operand's bits become a huge unsigned factor and overflow.
            (Datum::Int(3), Datum::UInt(4), Ok(Datum::UInt(12))),
            (Datum::UInt(4), Datum::Int(3), Ok(Datum::UInt(12))),
            (Datum::Int(-3), Datum::UInt(4), Err(EvalError::IntOverflow)),
            (Datum::UInt(4), Datum::Int(-3), Err(EvalError::IntOverflow)),
            (
                Datum::Int(-1),
                Datum::UInt(9223372036854775808),
                Err(EvalError::IntOverflow),
            ),
        ] {
            assert_eq!(eval_binary(BinaryOp::Mul, lhs, rhs), expected);
        }
    }

    #[test]
    fn decimal_mul_uses_bounded_mydecimal_semantics() {
        let huge = Datum::Decimal(Decimal::from_signed_literal(&format!(
            "1{}",
            "0".repeat(60)
        )));
        assert_eq!(
            eval_binary(BinaryOp::Mul, huge.clone(), huge),
            Err(EvalError::DecimalOverflow)
        );

        let left = Datum::Decimal(Decimal::from_signed_literal(
            "-0.0000000000000000000000000000000000000000000000000017382578996420603",
        ));
        let right = Datum::Decimal(Decimal::from_signed_literal(
            "-13890436710184412000000000000000000000000000000000000000000000000000000000000",
        ));
        assert_eq!(
            eval_binary(BinaryOp::Mul, left, right),
            Ok(Datum::Decimal(Decimal::from_signed_literal(
                "0.000000000000000000000000000000"
            )))
        );
    }
}

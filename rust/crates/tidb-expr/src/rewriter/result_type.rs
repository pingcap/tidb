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

//! What TYPE a rewritten expression reports, split out of `rewriter.rs`.
//!
//! The rewriter has two jobs: build the expression tree, and stamp each node
//! with the field type Go's `getFunction` would have given it. This module is
//! the second job -- the per-builtin result-type table
//! ([`builtin_return_type`]), the literal type rules Go's
//! `types.DefaultTypeForValue` applies, and the flen/decimal arithmetic those
//! two share (`concat_flen`, `string_cast_flen`, `set_numeric_len_from_args`
//! and friends).
//!
//! It is kept apart from the tree building because a wrong type here is a
//! chunk-cell panic rather than a wrong answer, so the rules deserve to be
//! read as one table instead of scattered through the rewrite arms.

use super::Expression;
use tidb_datatype::{Datum, FieldType, FieldTypeCode};

use crate::builtin_ext::{GlCmpStringMode, GlSignature};
use crate::scalar_function::ScalarFunction;
use crate::EvalError;

mod crypto;
pub(super) use crypto::returns_binary_string;

pub(super) fn restore_char_result_charset(func: &mut ScalarFunction) -> Result<(), EvalError> {
    if func.func_name.lowercase() != "char_func" {
        return Ok(());
    }
    let charset = match func.args.last() {
        Some(Expression::Constant(constant)) if constant.value == Datum::Null => None,
        Some(Expression::Constant(constant)) => constant.value.sql_string().ok(),
        _ => return Err(EvalError::Unsupported("CHAR charset argument")),
    };
    let Some(ft) = func.ret_type.as_mut() else {
        return Ok(());
    };
    if let Some(charset) = charset {
        // go's charFunctionClass.getFunction does NOT validate the charset:
        // an unknown name reaches `GetCharsetInfo` at EXEC time and answers
        // the bare 1105 `Unknown charset <name>` there -- with the argument
        // coercion's truncation warnings already in the statement buffer.
        // Failing here at plan time lost that ordering (and answered the
        // wrong message). An unknown name just keeps the default charset;
        // the error belongs to the eval below.
        if let Ok(collation) = tidb_charset::get_default_collation(&charset) {
            ft.set_charset_name(charset);
            ft.set_collation_name(collation);
        } else {
            super::set_binary_charset(ft);
        }
    } else {
        super::set_binary_charset(ft);
    }
    Ok(())
}

/// Go `types.SetBinChsClnFlag`: stamp binary charset, collation and flag.
pub(super) fn set_binary_charset(ft: &mut FieldType) {
    ft.set_charset_name("binary");
    // `set_collation_name` resolves the cached `Collation` enum that
    // `is_binary_string` reads, so naming the collation once is enough; there
    // is no second call here to keep in sync.
    ft.set_collation_name("binary");
    ft.add_flags(tidb_datatype::FieldTypeFlags::BINARY);
}

/// Go `preprocessor.checkFuncCastExpr` plus the duration precision check in
/// `expressionRewriter.checkTimePrecision`. These reject the statement before
/// the cast argument is evaluated.
pub(super) fn check_func_cast_expr(cast: &tidb_ast::CastExpr) -> Result<(), crate::EvalError> {
    use tidb_ast::CastType;

    let invalid = |code, message| crate::EvalError::InvalidTypeDeclaration { code, message };
    match cast.cast_type {
        CastType::Decimal { flen, scale } if flen < scale => Err(invalid(
            1427,
            format!(
                "For float(M,D), double(M,D) or decimal(M,D), M must be >= D (column '{}').",
                cast.expr.restore()
            ),
        )),
        CastType::Decimal { flen, .. } if flen > 65 => Err(invalid(
            1426,
            format!(
                "Too-big precision {flen} specified for '{}'. Maximum is 65.",
                cast.expr.restore()
            ),
        )),
        CastType::Decimal { scale, .. } if scale > 30 => Err(invalid(
            1425,
            format!(
                "Too big scale {scale} specified for column '{}'. Maximum is 30.",
                cast.expr.restore()
            ),
        )),
        CastType::DateTime { fsp: Some(fsp) } | CastType::Time { fsp: Some(fsp) } if fsp > 6 => {
            Err(invalid(
                1426,
                format!("Too-big precision {fsp} specified for 'CAST'. Maximum is 6."),
            ))
        }
        _ => Ok(()),
    }
}

/// Go `preprocessor` accepts `NAME_CONST` only when the name is a literal and
/// the value is a literal or unary expression. This check precedes name/type
/// resolution, so invalid source shapes always report 1210.
pub(super) fn validate_name_const_args(args: &[tidb_ast::Expr]) -> Result<(), crate::EvalError> {
    use tidb_ast::Expr;

    if args.len() != 2 {
        return Err(crate::EvalError::WrongParameterCount("name_const"));
    }
    let literal = |expr: &Expr| {
        matches!(
            expr,
            Expr::Null
                | Expr::Int(_)
                | Expr::Decimal(_)
                | Expr::Float(_)
                | Expr::Hex(_)
                | Expr::Bit(_)
                | Expr::String(_)
                | Expr::RawString(_)
                | Expr::CharsetString { .. }
                | Expr::CharsetBinary { .. }
                | Expr::Bool(_)
        )
    };
    if literal(&args[0]) && (literal(&args[1]) || matches!(args[1], Expr::Unary(_, _))) {
        Ok(())
    } else {
        Err(crate::EvalError::IncorrectArguments(
            "Incorrect arguments to NAME_CONST".to_owned(),
        ))
    }
}

pub(super) fn cast_target(cast_type: &tidb_ast::CastType) -> Option<(&'static str, FieldType)> {
    use tidb_ast::CastType;
    let name = match cast_type {
        CastType::Signed => "cast_signed",
        CastType::Unsigned => "cast_unsigned",
        CastType::UnsignedInUnion => "cast_unsigned_in_union",
        CastType::Char { .. } => "cast_char",
        CastType::Binary { .. } => "cast_binary",
        CastType::Decimal { .. } => "cast_decimal",
        CastType::Date => "cast_date",
        CastType::DateTime { .. } => "cast_datetime",
        CastType::Time { .. } => "cast_time",
        CastType::Year => "cast_year",
        CastType::Double => "cast_double",
        // go's FLOAT(p<=24) cast is its OWN signature (`builtinCastRealAsFloatSig`-shape):
        // its out-of-range behavior differs from DOUBLE -- a TEXT source raises
        // 1690 `constant 1e+300 overflows float` while a REAL source answers 0 --
        // so it cannot share the cast_double node, whose DOUBLE arm clamps.
        CastType::Float => "cast_float",
        CastType::Json => "cast_json",
        CastType::Vector { .. } => "cast_vector",
    };
    let ft = match cast_type {
        // parser.y `"SIGNED" OptInteger`: TypeLonglong + CharsetBin/
        // CollationBin/BinaryFlag.
        CastType::Signed => {
            let mut ft = FieldType::new(FieldTypeCode::LongLong);
            set_binary_charset(&mut ft);
            ft
        }
        // parser.y `"UNSIGNED" OptInteger`: TypeLonglong +
        // UnsignedFlag|BinaryFlag + the binary charset. (`UnsignedInUnion`
        // is this crate's internal union-projection carrier, not a parser
        // target; it keeps the same shape as `Unsigned`.)
        CastType::Unsigned | CastType::UnsignedInUnion => {
            let mut ft = FieldType::new(FieldTypeCode::LongLong);
            ft.add_flags(tidb_datatype::FieldTypeFlags::UNSIGNED);
            set_binary_charset(&mut ft);
            ft
        }
        CastType::Char { len, charset } => {
            let mut ft = FieldType::new(FieldTypeCode::VarString);
            if let Some(len) = len {
                ft.set_flen(i64::from(*len));
            }
            // Go's `CHAR OptFieldLen OptBinary` rule: the `BINARY` suffix is
            // `IsBinary` (BinaryFlag + binary charset/collation); a named
            // charset resolves its default collation via
            // `charset.GetDefaultCollation` (parser.y:9971). The no-clause
            // shape takes the session charset — the recorded boundary here.
            match charset.as_deref() {
                Some("BINARY") => set_binary_charset(&mut ft),
                Some(name) => {
                    let collation = tidb_charset::get_default_collation(name).ok()?;
                    ft.set_charset_name(name.to_owned());
                    ft.set_collation_name(collation);
                }
                None => {}
            }
            ft
        }
        // parser.y `"BINARY" OptFieldLen`: TypeVarString, but a GIVEN length
        // switches the code to TypeString; the binary charset/collation/flag
        // are always set. An omitted length keeps TypeVarString, whose
        // missing default in `defaultLengthAndDecimalForCast` leaves flen
        // unspecified (no truncation).
        CastType::Binary { len } => {
            let mut ft = FieldType::new(match len {
                Some(_) => FieldTypeCode::String,
                None => FieldTypeCode::VarString,
            });
            set_binary_charset(&mut ft);
            if let Some(len) = len {
                ft.set_flen(i64::from(*len));
            }
            ft
        }
        // parser.y `"DECIMAL" FloatOpt`: TypeNewDecimal + the binary
        // charset/collation/flag.
        CastType::Decimal { flen, scale } => {
            let mut ft = FieldType::new(FieldTypeCode::NewDecimal);
            ft.set_flen(i64::from(*flen));
            ft.set_decimal(i64::from(*scale));
            set_binary_charset(&mut ft);
            ft
        }
        CastType::Date => {
            let mut ft = FieldType::new(FieldTypeCode::Date);
            ft.set_flen(10);
            ft.set_decimal(0);
            set_binary_charset(&mut ft);
            ft
        }
        CastType::DateTime { fsp } => {
            let decimal = i64::from(fsp.unwrap_or(0));
            let mut ft = FieldType::new(FieldTypeCode::Datetime);
            ft.set_flen(if decimal > 0 { 20 + decimal } else { 19 });
            ft.set_decimal(decimal);
            set_binary_charset(&mut ft);
            ft
        }
        CastType::Time { fsp } => {
            let decimal = i64::from(fsp.unwrap_or(0));
            let mut ft = FieldType::new(FieldTypeCode::Duration);
            ft.set_flen(if decimal > 0 { 11 + decimal } else { 10 });
            ft.set_decimal(decimal);
            set_binary_charset(&mut ft);
            ft
        }
        // parser.y `"YEAR"`: TypeYear + the binary charset — the eval type
        // is still ETInt, so the evaluation arm is unchanged.
        CastType::Year => {
            let mut ft = FieldType::new(FieldTypeCode::Year);
            set_binary_charset(&mut ft);
            ft
        }
        // parser.y `"DOUBLE"`: TypeDouble + the
        // defaultLengthAndDecimalForCast defaults {22, -1} + binary.
        CastType::Double => {
            let mut ft = FieldType::new(FieldTypeCode::Double);
            ft.set_flen(22);
            ft.set_decimal(tidb_datatype::UNSPECIFIED_LENGTH);
            set_binary_charset(&mut ft);
            ft
        }
        // parser.y `"FLOAT" FloatOpt`: TypeFloat (p <= 24; p >= 25 folds to
        // `CastType::Double` at parse time) + the {12, -1} defaults + binary.
        CastType::Float => {
            let mut ft = FieldType::new(FieldTypeCode::Float);
            ft.set_flen(12);
            ft.set_decimal(tidb_datatype::UNSPECIFIED_LENGTH);
            set_binary_charset(&mut ft);
            ft
        }
        // parser.y `"JSON"`: TypeJSON + BinaryFlag|ParseToJSONFlag + the
        // default utf8mb4 charset/collation.
        CastType::Json => {
            let mut ft = FieldType::new(FieldTypeCode::Json);
            // expr_cast_parser.go applies the JSON-specific CAST defaults.
            ft.set_flen(4_194_304);
            ft.set_decimal(0);
            ft.add_flags(tidb_datatype::FieldTypeFlags::PARSE_TO_JSON);
            ft.add_flags(tidb_datatype::FieldTypeFlags::BINARY);
            ft.set_charset_name("utf8mb4");
            ft.set_collation_name("utf8mb4_bin");
            ft
        }
        // parser.y `"VECTOR" OptVectorElementType OptFieldLen`:
        // TypeTiDBVectorFloat32. Unlike the other targets, this rule sets
        // only CharsetBin/CollationBin — it does NOT add BinaryFlag.
        CastType::Vector { dimensions } => {
            let mut ft = FieldType::new(FieldTypeCode::VectorFloat32);
            if let Some(dimensions) = dimensions {
                ft.set_flen(i64::from(*dimensions));
            }
            ft.set_decimal(0);
            ft.set_charset_name("binary");
            ft.set_collation_name("binary");
            ft
        }
    };
    Some((name, ft))
}

/// Go `adjustRetFtForCastString` (`builtin_cast.go`): a `CAST(... AS CHAR)`
/// target with an unspecified width takes the width of the value the cast
/// produces, per the argument's family (and `CAST(json AS CHAR)` widens the
/// code to `TypeLongBlob`). Fixed `TypeString` targets return untouched, so
/// the caller gates on `VarString` — Go's own early return.
pub(crate) fn adjust_ret_ft_for_cast_string(ret_ft: &mut FieldType, arg_ft: &FieldType) {
    use tidb_datatype::FieldTypeFlags;
    const MAX_TINY_BLOB_SIZE: i64 = 255;
    const CAST_BLOB_FLEN: i64 = 65_535 * 4;
    const CAST_MEDIUM_BLOB_FLEN: i64 = 16_777_215 * 4;
    const MAX_LONG_BLOB_SIZE: i64 = 4_294_967_295;
    const UNSPECIFIED: i64 = tidb_datatype::UNSPECIFIED_LENGTH;

    // Go: `if retFt.GetType() == mysql.TypeString { return }` — only variable
    // length string types are estimated; every estimation arm also requires
    // `originalFlen == UnspecifiedLength`.
    if ret_ft.code() != FieldTypeCode::VarString || ret_ft.flen() != UNSPECIFIED {
        return;
    }
    if arg_ft.code() == FieldTypeCode::Null {
        return;
    }
    match arg_ft.eval_type() {
        tidb_datatype::EvalType::Int => {
            let flen = match arg_ft.code() {
                // issue 44786: sizing an int by its type width, not its
                // declared flen, so `CHAR(1)` cannot truncate `-1`.
                FieldTypeCode::Tiny => {
                    if arg_ft.has_flag(FieldTypeFlags::UNSIGNED) {
                        3
                    } else {
                        4
                    }
                }
                FieldTypeCode::Short => {
                    if arg_ft.has_flag(FieldTypeFlags::UNSIGNED) {
                        5
                    } else {
                        6
                    }
                }
                FieldTypeCode::Int24 => {
                    if arg_ft.has_flag(FieldTypeFlags::UNSIGNED) {
                        8
                    } else {
                        9
                    }
                }
                FieldTypeCode::Long => {
                    if arg_ft.has_flag(FieldTypeFlags::UNSIGNED) {
                        10
                    } else {
                        11
                    }
                }
                // BIGINT is 20 either way: both range endpoints print 20
                // characters.
                FieldTypeCode::LongLong => 20,
                FieldTypeCode::Year => 4,
                FieldTypeCode::Bit => arg_ft.flen(),
                // Go asserts Enum/Set never reach here (their
                // EnumSetAsIntFlag path is removed upstream of the call)
                // and leaves the target untouched otherwise.
                _ => return,
            };
            ret_ft.set_flen(flen);
        }
        tidb_datatype::EvalType::Real => {
            // TiDB formats float/double in `f` notation, so the width covers
            // the smallest denormal: 87 for float, 370 for double.
            let flen = match arg_ft.code() {
                FieldTypeCode::Float => 87,
                FieldTypeCode::Double => 370,
                _ => return,
            };
            ret_ft.set_flen(flen);
        }
        tidb_datatype::EvalType::Decimal => {
            let precision = arg_ft.flen();
            let scale = arg_ft.decimal();
            if precision == UNSPECIFIED || scale == UNSPECIFIED {
                return;
            }
            let mut ret = precision;
            if scale > 0 {
                ret += 1;
            }
            if !arg_ft.has_flag(FieldTypeFlags::UNSIGNED) && precision > 0 {
                ret += 1; // for negative sign
            }
            if ret == 0 {
                ret = 1;
            }
            ret_ft.set_flen(ret);
        }
        tidb_datatype::EvalType::Datetime | tidb_datatype::EvalType::Timestamp => {
            let mut flen = if arg_ft.code() == FieldTypeCode::Date {
                10 // MaxDateWidth
            } else {
                19 // MaxDatetimeWidthNoFsp
            };
            if arg_ft.decimal() > 0 {
                flen += 1 + arg_ft.decimal();
            }
            ret_ft.set_flen(flen);
        }
        tidb_datatype::EvalType::Duration => {
            let mut flen = 10; // MaxDurationWidthNoFsp
            if arg_ft.decimal() > 0 {
                flen += 1 + arg_ft.decimal();
            }
            ret_ft.set_flen(flen);
        }
        tidb_datatype::EvalType::Json => {
            ret_ft.set_flen(MAX_LONG_BLOB_SIZE);
            ret_ft.set_code(FieldTypeCode::LongBlob);
        }
        tidb_datatype::EvalType::VectorFloat32 => {}
        tidb_datatype::EvalType::String => {
            let flen = match arg_ft.code() {
                FieldTypeCode::String | FieldTypeCode::Varchar | FieldTypeCode::VarString => {
                    if arg_ft.flen() > 0 {
                        arg_ft.flen()
                    } else {
                        return;
                    }
                }
                FieldTypeCode::TinyBlob => MAX_TINY_BLOB_SIZE,
                FieldTypeCode::Blob => CAST_BLOB_FLEN,
                FieldTypeCode::MediumBlob => CAST_MEDIUM_BLOB_FLEN,
                FieldTypeCode::LongBlob => MAX_LONG_BLOB_SIZE,
                _ => return,
            };
            ret_ft.set_flen(flen);
        }
    }
}

/// Go `types.DefaultTypeForValue` for a `*MyDecimal`: the printed length plus
/// one for the decimal point, and the literal's own fractional digits.
pub(super) fn decimal_literal_type(value: &tidb_datatype::Decimal) -> FieldType {
    let mut ft = FieldType::new(FieldTypeCode::NewDecimal);
    ft.set_flen_under_limit(value.to_string().chars().count() as i64);
    ft.set_decimal_under_limit(i64::from(value.scale()));
    ft.set_flen_under_limit(ft.flen() + 1);
    set_binary_charset(&mut ft);
    ft.add_flags(tidb_datatype::FieldTypeFlags::NOT_NULL);
    ft
}

/// Go `types.DefaultTypeForValue` for an `int64`/`uint64`: a `BIGINT` as wide
/// as the value's printed digits, with `UnsignedFlag` set exactly when the
/// literal did not fit the signed domain.
///
/// The flen counts the digits of the VALUE, not of the source text, because
/// Go measures `StrLenOfInt64Fast(x)` after the parse -- so `007` is flen 1.
pub(super) fn int_literal_type(printed_len: usize, unsigned: bool) -> FieldType {
    let mut ft = FieldType::new(FieldTypeCode::LongLong);
    ft.set_flen(printed_len as i64);
    ft.set_decimal(0);
    set_binary_charset(&mut ft);
    ft.add_flags(tidb_datatype::FieldTypeFlags::NOT_NULL);
    if unsigned {
        ft.add_flags(tidb_datatype::FieldTypeFlags::UNSIGNED);
    }
    ft
}

/// Go `types.DefaultTypeForValue` for a `float64`: a binary, NOT NULL
/// `DOUBLE` as wide as the shortest printed form and of unspecified scale.
pub(super) fn float_literal_type(value: f64) -> FieldType {
    let mut ft = FieldType::new(FieldTypeCode::Double);
    ft.set_flen(tidb_goruntime::go_strconv::format_float(value, b'f', -1, 64).len() as i64);
    ft.set_decimal(-1);
    set_binary_charset(&mut ft);
    ft.add_flags(tidb_datatype::FieldTypeFlags::NOT_NULL);
    ft
}

/// Go `types.DefaultTypeForValue` for a `nil`: the only literal that is not
/// NOT NULL, typed `NULL`, zero wide, binary.
pub(super) fn null_literal_type() -> FieldType {
    let mut ft = FieldType::new(FieldTypeCode::Null);
    ft.set_flen(0);
    ft.set_decimal(0);
    set_binary_charset(&mut ft);
    ft
}

/// Go `types.DefaultTypeForValue` for a `bool`: a one-wide, binary, NOT NULL
/// `BIGINT` that remembers it was boolean.
pub(super) fn bool_literal_type() -> FieldType {
    let mut ft = FieldType::new(FieldTypeCode::LongLong);
    ft.set_flen(1);
    ft.set_decimal(0);
    set_binary_charset(&mut ft);
    ft.add_flags(
        tidb_datatype::FieldTypeFlags::NOT_NULL | tidb_datatype::FieldTypeFlags::IS_BOOLEAN,
    );
    ft
}

/// Go `types.DefaultTypeForValue` for a `BitLiteral`/`HexLiteral`: a binary
/// `VarString` three bytes wide per literal byte. Only the hex form is
/// unsigned, which is what makes `0x41 + 0` read the bytes as a number.
pub(super) fn binary_literal_type(byte_len: usize, unsigned: bool) -> FieldType {
    let mut ft = FieldType::new(FieldTypeCode::VarString);
    ft.set_flen(byte_len as i64 * 3);
    ft.set_decimal(0);
    set_binary_charset(&mut ft);
    ft.add_flags(tidb_datatype::FieldTypeFlags::NOT_NULL);
    if unsigned {
        ft.add_flags(tidb_datatype::FieldTypeFlags::UNSIGNED);
    }
    ft
}

/// The return type Go's function class gives `name` over `args`, `None` when
/// the count or the class refuses them.
#[cfg(test)]
pub(crate) fn builtin_return_type(name: &str, args: &[Expression]) -> Option<FieldType> {
    crate::builtin_registry::verify_args_by_count(name, args.len()).ok()?;
    let f = crate::builtin::funcs(name)?
        .get_function(&crate::builtin::ColumnsBuildContext(&tidb_exprctx::NoColumns), args.to_vec())
        .ok()?;
    Some(f.get_ret_tp())
}

fn datetime_return_type(fsp: i64, not_null: bool) -> Option<FieldType> {
    if !(0..=6).contains(&fsp) {
        return None;
    }
    let mut result = FieldType::new(FieldTypeCode::Datetime);
    result.set_decimal(fsp);
    result.set_flen(19 + if fsp == 0 { 0 } else { fsp + 1 });
    if not_null {
        result.add_flags(tidb_datatype::FieldTypeFlags::NOT_NULL);
    }
    set_binary_charset(&mut result);
    Some(result)
}

fn date_return_type(args: &[Expression], arity: usize) -> Option<FieldType> {
    if args.len() != arity {
        return None;
    }
    let mut result = FieldType::new(FieldTypeCode::Date);
    result.set_decimal(0);
    result.set_flen(10);
    set_binary_charset(&mut result);
    Some(result)
}

fn duration_return_type(fsp: i64, base_flen: i64) -> Option<FieldType> {
    if !(0..=6).contains(&fsp) {
        return None;
    }
    let mut result = FieldType::new(FieldTypeCode::Duration);
    result.set_decimal(fsp);
    result.set_flen(base_flen + if fsp == 0 { 0 } else { fsp + 1 });
    set_binary_charset(&mut result);
    Some(result)
}

pub(crate) fn str_to_date_return_type(args: &[Expression]) -> Option<FieldType> {
    let [_, format] = args else {
        return None;
    };
    let Expression::Constant(format) = format else {
        return datetime_return_type(6, false);
    };
    if matches!(&format.value, Datum::Null) {
        return datetime_return_type(0, false);
    }
    let format = format.value.sql_string().ok()?;
    let fsp = if format.contains("%f") { 6 } else { 0 };
    match tidb_datatype::get_format_type(&format) {
        (true, false) => duration_return_type(fsp, 10),
        (false, true) => date_return_type(args, 2),
        _ => datetime_return_type(fsp, false),
    }
}

pub(crate) fn date_arithmetic_return_type(unit: &str, args: &[Expression]) -> Option<FieldType> {
    use tidb_datatype::EvalType;
    let [date, amount] = args else {
        return None;
    };
    let date_type = date.static_type()?;
    let clock_unit = matches!(
        unit.to_ascii_uppercase().as_str(),
        "MICROSECOND"
            | "SECOND"
            | "MINUTE"
            | "HOUR"
            | "SECOND_MICROSECOND"
            | "MINUTE_MICROSECOND"
            | "HOUR_MICROSECOND"
            | "DAY_MICROSECOND"
            | "MINUTE_SECOND"
            | "HOUR_SECOND"
            | "DAY_SECOND"
            | "HOUR_MINUTE"
            | "DAY_MINUTE"
            | "DAY_HOUR"
    );
    let date_unit = matches!(
        unit.to_ascii_uppercase().as_str(),
        "DAY"
            | "WEEK"
            | "MONTH"
            | "QUARTER"
            | "YEAR"
            | "DAY_MICROSECOND"
            | "DAY_SECOND"
            | "DAY_MINUTE"
            | "DAY_HOUR"
            | "YEAR_MONTH"
    );
    let code = if date_type.code() == FieldTypeCode::Date {
        if clock_unit {
            FieldTypeCode::Datetime
        } else {
            FieldTypeCode::Date
        }
    } else {
        match date_type.eval_type() {
            EvalType::Duration if date_unit && !unit.eq_ignore_ascii_case("DAY_MICROSECOND") => {
                FieldTypeCode::Datetime
            }
            EvalType::Duration => FieldTypeCode::Duration,
            EvalType::Datetime | EvalType::Timestamp => FieldTypeCode::Datetime,
            _ => FieldTypeCode::VarString,
        }
    };
    let mut result = FieldType::new(code);
    match code {
        FieldTypeCode::Date => {
            result.set_flen(10);
            result.set_decimal(0);
        }
        FieldTypeCode::Datetime | FieldTypeCode::Duration => {
            let fsp = i64::from(crate::time_fn::calendar::date_add_result_fsp(
                unit,
                Some(date_type),
                amount.static_type(),
            )?);
            result.set_decimal(fsp);
            result.set_flen(
                if code == FieldTypeCode::Datetime {
                    19
                } else {
                    10
                } + if fsp == 0 { 0 } else { fsp + 1 },
            );
            set_binary_charset(&mut result);
        }
        FieldTypeCode::VarString => {
            result.set_flen(29);
            result.set_decimal(0);
        }
        _ => unreachable!("DATE_ADD result type is closed"),
    }
    Some(result)
}

const MAX_LONG_BLOB_WIDTH: i64 = 4_294_967_295;

/// The flen an argument carries once `WrapWithCastAsString` has wrapped it,
/// which is the width `CONCAT` actually sums.
///
/// Two Go functions decide it in sequence (`pkg/expression/builtin_cast.go`).
/// `WrapWithCastAsString` asks for a width: a string argument is not wrapped
/// at all and keeps its own flen, an integer widens to `MaxIntWidth` whatever
/// its declared display width (a `BIT(n)` instead asks for its byte count),
/// a decimal gains three for the sign, the point and a leading zero, and a
/// float or double asks for nothing because `CAST(f AS CHAR)`'s printed width
/// is not predictable. `setCastFlen`-style sizing on the cast's own return
/// type then fills in every width still unasked-for, which is where the
/// remaining types get theirs.
///
/// Captured from Go, one `CONCAT` argument each: `DOUBLE` 370, `FLOAT` 87,
/// `DATE` 10, `DATETIME` 19, `DATETIME(3)` 23, `TIME(2)` 13, `TINYINT(4)` 20,
/// `BIT(8)` 1, `DECIMAL(10,2)` 13, `VARCHAR(16)` 16.
///
/// Not modeled: a DECIMAL whose own flen is unspecified, where Go computes
/// `decimalPrecisionToLength`. That leaves the unspecified path below, which
/// is Go's answer for an argument of genuinely unknown width.
pub(crate) fn string_cast_flen(ft: &FieldType) -> i64 {
    use tidb_datatype::EvalType;
    const UNSPECIFIED: i64 = tidb_datatype::UNSPECIFIED_LENGTH;

    let eval_type = ft.eval_type();
    let requested = match eval_type {
        // Already a string: no cast is inserted at all.
        EvalType::String => return ft.flen(),
        EvalType::Int if ft.code() == FieldTypeCode::Bit => (ft.flen() + 7) / 8,
        EvalType::Int => 20,
        EvalType::Decimal if ft.flen() != UNSPECIFIED => ft.flen() + 3,
        EvalType::Real => UNSPECIFIED,
        _ => ft.flen(),
    };
    if requested != UNSPECIFIED {
        return requested;
    }

    // The cast's return-type sizing, reached only for a width nobody asked for.
    let with_fraction = |base: i64| {
        if ft.decimal() > 0 {
            base + 1 + ft.decimal()
        } else {
            base
        }
    };
    match eval_type {
        // 87 and 370 are Go's own worst-case widths for `%f`-formatted
        // f32/f64, not MySQL's 12/22 -- TiDB never uses scientific notation.
        EvalType::Real if ft.code() == FieldTypeCode::Float => 87,
        EvalType::Real => 370,
        EvalType::Datetime | EvalType::Timestamp if ft.code() == FieldTypeCode::Date => 10,
        EvalType::Datetime | EvalType::Timestamp => with_fraction(19),
        EvalType::Duration => with_fraction(10),
        EvalType::Json => MAX_LONG_BLOB_WIDTH,
        _ => UNSPECIFIED,
    }
}

/// Go `resolveType4Extremum` (`pkg/expression/builtin_compare.go:451`)
/// followed by the `argTp` overrides both `greatestFunctionClass.getFunction`
/// and `leastFunctionClass.getFunction` open with -- together, the whole of
/// how those two pick ONE of their eight signatures:
///
/// ```go
/// resFieldType, fieldTimeType, cmpStringMode := resolveType4Extremum(ctx.GetEvalCtx(), args)
/// resTp := resFieldType.EvalType()
/// argTp := resTp
/// if cmpStringMode != GLCmpStringDirectly {
///     argTp = types.ETString
/// } else if resTp == types.ETJson {
///     unsupportedJSONComparison(ctx, args); argTp = types.ETString; resTp = types.ETString
/// }
/// ```
///
/// The decisive line is `argTp := resTp`: the comparison domain is the
/// AGGREGATE of the argument FieldTypes, never the runtime value that happens
/// to arrive. That is the whole difference between `greatest(e, 2)` over an
/// `enum('{}','[1]','x')` answering `{}` (an ETString signature, the integer
/// stringified) and answering `2` (an integer comparison against the enum's
/// ordinal). Go's own arms then collapse onto three implementations: ETString
/// has the three [`GlCmpStringMode`] variants, ETDatetime/ETTimestamp is
/// `builtinGreatestTimeSig` keyed on [`GlSignature::ret_date`], and every
/// other arm -- ETInt, ETReal, ETDecimal, ETDuration, ETVectorFloat32 --
/// compares the already-cast arguments and returns the winner unchanged.
///
/// `None` means an argument carries no static type here, which is this tier's
/// "I cannot name Go's signature", never a claim that Go would refuse the
/// call.
pub fn gl_signature(args: &[Expression]) -> Option<GlSignature> {
    let typed: Vec<FieldType> = args
        .iter()
        .filter_map(|arg| arg.static_type().cloned())
        .collect();
    if typed.len() != args.len() || typed.is_empty() {
        return None;
    }
    let aggregated = tidb_datatype::agg_field_type(&typed);
    let cmp_string_mode = gl_cmp_string_mode(&aggregated, &typed);
    // `fieldTimeType`: `GLRetDate` only for an aggregate that is literally
    // `mysql.TypeDate`, which is what `builtinGreatestTimeSig`'s `cmpAsDate`
    // carries into `getAccurateTimeTypeForGLRet`.
    let ret_date = aggregated.code() == FieldTypeCode::Date;
    let arg_type = if cmp_string_mode == GlCmpStringMode::Directly {
        match aggregated.eval_type() {
            // `unsupportedJSONComparison` warns and then compares as text.
            tidb_datatype::EvalType::Json => tidb_datatype::EvalType::String,
            other => other,
        }
    } else {
        tidb_datatype::EvalType::String
    };
    Some(GlSignature {
        arg_type,
        cmp_string_mode,
        ret_date,
    })
}

/// Go `resolveType4Extremum`'s `cmpStringMode` half: when GREATEST/LEAST's
/// arguments aggregate to a STRING kind that is not itself temporal, and at
/// least one argument IS a date or datetime, Go compares every argument as a
/// parsed time instead of as text.
fn gl_cmp_string_mode(aggregated: &FieldType, typed: &[FieldType]) -> GlCmpStringMode {
    if !aggregated.eval_type().is_string_kind() || aggregated.code().is_type_temporal() {
        return GlCmpStringMode::Directly;
    }
    // Go scans for a temporal argument but PREFERS a DATETIME one, so a
    // (DATE, DATETIME, string) list compares as datetime rather than as date.
    let mut temporal: Option<FieldTypeCode> = None;
    for ft in typed {
        if ft.code().is_type_temporal()
            && (temporal.is_none() || ft.code() == FieldTypeCode::Datetime)
        {
            temporal = Some(ft.code());
        }
    }
    match temporal {
        Some(FieldTypeCode::Date) => GlCmpStringMode::AsDate,
        Some(code) if code.is_temporal_with_date() => GlCmpStringMode::AsDatetime,
        // A DURATION is temporal but carries no date, so Go leaves the mode
        // alone and the plain string signature runs.
        _ => GlCmpStringMode::Directly,
    }
}

#[cfg(test)]
#[path = "result_type_tests.rs"]
mod tests;

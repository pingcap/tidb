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

use super::{Constant, Expression};
use tidb_datatype::{Datum, FieldType, FieldTypeCode};

use super::control_type::set_numeric_len_from_args;
use crate::builtin_ext::{GlCmpStringMode, GlSignature};

mod crypto;
pub(super) use crypto::returns_binary_string;

/// Go `types.SetBinChsClnFlag`: the binary charset/collation plus the binary
/// flag every non-string literal type carries.
/// The builtins whose result is a BINARY string: Go's `getFunction` for each
/// ends with `types.SetBinChsClnFlag(bf.tp)` (`unhexFunctionClass`,
/// `base64FunctionClass.fromBase64` in `pkg/expression/builtin_string.go`;
/// `inet6AtonFunctionClass` in `pkg/expression/builtin_miscellaneous.go`).
///
/// This is the single list both places that force it consult -- the result
/// type built in [`builtin_return_type`] and the re-assertion after generic
/// collation derivation -- so the two cannot drift apart.
/// The result type code Go's own `getFunction` gives a builtin, for the
/// builtins where [`builtin_return_type`] deliberately reports something
/// else.
///
/// There is exactly one such family today, and its divergence is stated in
/// full at its arm in [`builtin_return_type`]: this crate has no BinaryJSON
/// cell, so a JSON-returning builtin is typed `VarString` and evaluates to
/// the canonical JSON TEXT. That keeps the VALUES byte-identical to TiDB's,
/// and it is the right trade for evaluation -- but it also ERASES a fact some
/// callers need, which is what TiDB itself would call the result.
///
/// `pkg/ddl/index.go`'s `checkIndexColumn` is such a caller. It refuses an
/// expression index whose result type is JSON (3753) or BLOB/TEXT (3757), so
/// the refusal it computes depends on the Go type and not on the cell type
/// this crate stores the value in. Reading `static_type()` there would answer
/// `VarString` for `json_extract` and accept an index TiDB refuses.
///
/// `None` means Go and this crate agree, and the expression's own
/// `static_type()` is Go's answer too.
///
/// Measured, `select <expr>` against TiDB through the DDL probe:
///
/// ```text
/// json_extract(j,'$.a')  json BINARY    tp=245 flen=16777216
/// json_unquote(j)        longtext       tp=251 flen=4294967295
/// json_pretty(j)         longtext       tp=251 flen=67108864
/// json_type(j)           var_string(51) tp=253   -- no divergence
/// json_quote(s)          var_string(122)tp=253   -- no divergence
/// json_valid(j)          bigint(20)     tp=8     -- no divergence
/// cast(j as json)        json BINARY    tp=245 flen=4194304
/// ```
///
/// Only the CODE is kept, because only the code is what the refusal reads;
/// the flen would be a second fact to hold correct with no reader for it.
#[must_use]
pub fn go_result_type_code(name: &str) -> Option<FieldTypeCode> {
    match name {
        // Go's `MysqlJson` group. `cast_json` is `CAST(x AS JSON)`, which
        // this crate lowers to a one-argument function of that name.
        "json_extract"
        | "json_object"
        | "json_array"
        | "json_keys"
        | "json_set"
        | "json_insert"
        | "json_replace"
        | "json_remove"
        | "json_array_append"
        | "json_array_insert"
        | "json_merge"
        | "json_merge_preserve"
        | "json_merge_patch"
        | "json_search"
        | "cast_json" => Some(FieldTypeCode::Json),
        // Go `jsonUnquoteFunctionClass`/`jsonPrettyFunctionClass`: an
        // `ETString` result widened to `mysql.MaxBlobWidth`+ and therefore
        // reported as LONGTEXT, which is a BLOB-class type to
        // `types.IsTypeBlob`.
        "json_unquote" | "json_pretty" => Some(FieldTypeCode::LongBlob),
        _ => None,
    }
}

pub(super) fn set_binary_charset(ft: &mut FieldType) {
    ft.set_charset_name("binary");
    // `set_collation_name` resolves the cached `Collation` enum that
    // `is_binary_string` reads, so naming the collation once is enough; there
    // is no second call here to keep in sync.
    ft.set_collation_name("binary");
    ft.add_flags(tidb_datatype::FieldTypeFlags::BINARY);
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

/// The result type of a builtin this rewriter is willing to build.
///
/// A chunk cell is sized from its column's type, so a wrong static type is a
/// panic rather than a wrong answer -- which is why this rewriter builds ONLY
/// the functions whose result type Go fixes to one thing, and refuses the
/// rest instead of falling back to a placeholder. Go's own per-class type
/// inference (`getFunction` on each `functionClass`) is the full version of
/// this table; the deferred names are listed with it.
///
/// NOT BUILT here, and refused (each needs more than a fixed result type):
/// `CAST`/`CONVERT` take a target type, not a value, argument;
/// `GROUP_CONCAT` is an aggregate; `DATE_ADD`-family take an `Expr::Interval`
/// argument that is not an expression at all.
///
pub(super) fn builtin_return_type(name: &str, args: &[Expression]) -> Option<FieldType> {
    let mut ft = builtin_return_type_before_ret_tp(name, args)?;
    promote_wide_string_result(&mut ft);
    Some(ft)
}

/// Go `baseBuiltinFunc.getRetTp` (`pkg/expression/builtin.go`), the step every
/// builtin's declared type passes through on its way out of `getFunction`.
///
/// It is NOT an argument rule and it is not per-builtin: a string-kind result
/// whose flen has grown past a BLOB boundary stops calling itself a
/// `VarString`. That is the whole of Go's "TEXT promotion" -- there is no
/// separate "a TEXT argument makes a TEXT result" rule, which is why
/// `REVERSE(text_column)` is still a `var_string(65535)` in Go (65535 is one
/// short of the MEDIUM boundary) while `FROM_BASE64(text_column)`, whose flen
/// rule triples it to 196605, is a `mediumblob`.
///
/// Both thresholds are `>=`, and both are Go's own constants:
/// `mysql.MaxBlobWidth` and the bare 65536 `getRetTp` spells out.
fn promote_wide_string_result(ft: &mut FieldType) {
    /// Go's literal `65536` in `getRetTp` -- one past `mysql.MaxBlobSize`.
    const MEDIUM_BLOB_BOUNDARY: i64 = 65536;
    if ft.eval_type() != tidb_datatype::EvalType::String {
        return;
    }
    if ft.flen() >= MAX_BLOB_WIDTH {
        ft.set_code(FieldTypeCode::LongBlob);
    } else if ft.flen() >= MEDIUM_BLOB_BOUNDARY {
        ft.set_code(FieldTypeCode::MediumBlob);
    }
}

/// The width Go's `getFunction` reads out of `args[i]` for an argument the
/// builtin declared as `types.ETString`.
///
/// `newBaseBuiltinFuncWithTp` REPLACES each element of the caller's `args`
/// slice with its cast-wrapped self before returning, so every
/// `args[0].GetType()` a string builtin reads afterwards is the width of the
/// argument AS A STRING, not its declared one. That is the same quantity
/// `CONCAT` sums, so this is [`string_cast_flen`] and not `flen()` -- which is
/// what makes `LOWER(int_column)` 20 rather than 11 and `REVERSE(a_double)`
/// 370 rather than unspecified.
fn str_arg_flen(args: &[Expression], i: usize) -> i64 {
    args.get(i)
        .and_then(Expression::static_type)
        .map_or(tidb_datatype::UNSPECIFIED_LENGTH, string_cast_flen)
}

/// The width of `args[i]` BEFORE any cast wrapping, for the two builtins whose
/// `getFunction` reads the argument type before it calls
/// `newBaseBuiltinFuncWithTp` (`UNHEX`) or declares the argument `ETInt`
/// (`HEX`'s numeric branch, where `WrapWithCastAsInt` copies the source flen
/// through unchanged and so cannot move it).
/// The eval type `HEX` and `UNHEX` branch on, defaulting to the STRING branch
/// for an argument this tier could not type -- Go always has one, so the
/// default only decides whether the call is BUILT, never what Go would say.
fn arg_eval_type(args: &[Expression], i: usize) -> tidb_datatype::EvalType {
    args.get(i)
        .and_then(Expression::static_type)
        .map_or(tidb_datatype::EvalType::String, FieldType::eval_type)
}

fn raw_arg_flen(args: &[Expression], i: usize) -> i64 {
    args.get(i)
        .and_then(Expression::static_type)
        .map_or(tidb_datatype::UNSPECIFIED_LENGTH, FieldType::flen)
}

/// Go `getExpressionFsp` for `timeFunctionClass`: a constant uses the digits
/// written after its first decimal point (capped at `MaxFsp`); a non-constant
/// inherits the argument type's scale after the implicit TIME cast.
fn time_argument_fsp(arg: &Expression) -> i64 {
    const MAX_FSP: i64 = 6;
    if let Expression::Constant(constant) = arg {
        return constant.value.sql_string().ok().map_or(0, |value| {
            value.find('.').map_or(0, |dot| {
                (value.len() - dot - 1).min(MAX_FSP as usize) as i64
            })
        });
    }
    arg.static_type()
        .map_or(0, FieldType::decimal)
        .clamp(0, MAX_FSP)
}

/// `timeFunctionClass.getFunction` + `setDecimalAndFlenForTime`.
fn time_return_type(args: &[Expression]) -> Option<FieldType> {
    let [arg] = args else {
        return None;
    };
    let fsp = time_argument_fsp(arg);
    let mut ft = FieldType::new(FieldTypeCode::Duration);
    ft.set_decimal(fsp);
    ft.set_flen(10 + if fsp == 0 { 0 } else { fsp + 1 });
    set_binary_charset(&mut ft);
    Some(ft)
}

fn add_sub_time_return_type(args: &[Expression]) -> Option<FieldType> {
    let [left, right] = args else {
        return None;
    };
    let fsp = time_argument_fsp(left).max(time_argument_fsp(right)).min(6);
    let code = match left.static_type()?.code() {
        FieldTypeCode::Datetime | FieldTypeCode::Timestamp => FieldTypeCode::Datetime,
        FieldTypeCode::Duration => FieldTypeCode::Duration,
        _ => FieldTypeCode::String,
    };
    let mut result = FieldType::new(code);
    match code {
        FieldTypeCode::Datetime | FieldTypeCode::Duration => {
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
        FieldTypeCode::String => {
            result.set_flen(26);
            result.set_decimal(tidb_datatype::UNSPECIFIED_LENGTH);
        }
        _ => unreachable!("ADDTIME result type is closed"),
    }
    Some(result)
}

fn date_add_return_type(name: &str, args: &[Expression]) -> Option<FieldType> {
    use tidb_datatype::EvalType;

    let [date, amount] = args else {
        return None;
    };
    let unit = name
        .strip_prefix("date_add_")
        .or_else(|| name.strip_prefix("date_sub_"))?;
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

/// Go `roundFunctionClass.getFunction` / `truncateFunctionClass.getFunction`.
///
/// These functions preserve the first argument's numeric domain. For decimal
/// values, a row-dependent scale cannot widen the result beyond the input's
/// declared scale; a constant scale fixes the result scale at build time.
fn round_truncate_return_type(name: &str, args: &[Expression]) -> Option<FieldType> {
    use tidb_datatype::{EvalType, MAX_DECIMAL_SCALE, UNSPECIFIED_LENGTH};

    let value_type = args.first()?.static_type()?;
    let eval_type = match value_type.eval_type() {
        EvalType::Int => EvalType::Int,
        EvalType::Decimal => EvalType::Decimal,
        _ => EvalType::Real,
    };
    let mut result = match eval_type {
        EvalType::Int => FieldType::new(FieldTypeCode::LongLong),
        EvalType::Decimal => FieldType::new(FieldTypeCode::NewDecimal),
        EvalType::Real => FieldType::new(FieldTypeCode::Double),
        _ => unreachable!("normalized above"),
    };
    if value_type.is_unsigned() {
        result.add_flags(tidb_datatype::FieldTypeFlags::UNSIGNED);
    }
    if eval_type != EvalType::Decimal {
        return Some(result);
    }

    let decimal = if args.len() <= 1 {
        0
    } else if let Expression::Constant(constant) = &args[1] {
        constant
            .eval()
            .ok()
            .and_then(|value| {
                crate::cast::cast_arg_as_int(
                    &value,
                    constant.ret_type.as_ref(),
                    &crate::context::NoColumns,
                )
                .ok()
            })
            .and_then(|value| crate::arg_eval_type::eval_int(&value).ok().flatten())
            .filter(|decimal| *decimal >= 0)
            .map_or(0, |decimal| decimal.min(MAX_DECIMAL_SCALE))
    } else {
        value_type.decimal()
    };
    result.set_decimal_under_limit(decimal);

    let flen = if name == "round" {
        let mut flen = value_type.flen();
        if decimal != UNSPECIFIED_LENGTH {
            flen = if value_type.decimal() == UNSPECIFIED_LENGTH {
                flen.saturating_add(decimal)
            } else {
                flen.saturating_add((decimal - value_type.decimal()).max(0))
            };
        }
        flen
    } else {
        value_type
            .flen()
            .saturating_sub(value_type.decimal())
            .saturating_add(decimal)
    };
    result.set_flen_under_limit(flen);
    Some(result)
}

/// Go `getEvalTp4FloorAndCeil` plus `setFlag4FloorAndCeil`.
fn ceil_floor_return_type(args: &[Expression]) -> Option<FieldType> {
    use tidb_datatype::EvalType;

    let source = args.first()?.static_type()?;
    let mut result = match source.eval_type() {
        EvalType::Int => FieldType::new(FieldTypeCode::LongLong),
        EvalType::Decimal if source.flen() - source.decimal() > 18 => {
            let mut decimal = FieldType::new(FieldTypeCode::NewDecimal);
            decimal.set_flen_under_limit(source.flen());
            decimal.set_decimal(0);
            decimal
        }
        EvalType::Decimal => FieldType::new(FieldTypeCode::LongLong),
        _ => FieldType::new(FieldTypeCode::Double),
    };
    if matches!(
        source.code(),
        FieldTypeCode::Long | FieldTypeCode::LongLong | FieldTypeCode::NewDecimal
    ) && source.is_unsigned()
    {
        result.add_flags(tidb_datatype::FieldTypeFlags::UNSIGNED);
    }
    Some(result)
}

fn builtin_return_type_before_ret_tp(name: &str, args: &[Expression]) -> Option<FieldType> {
    let text = || {
        let mut ft = FieldType::new(FieldTypeCode::VarString);
        ft.set_decimal(tidb_datatype::UNSPECIFIED_LENGTH);
        ft
    };
    let int = || FieldType::new(FieldTypeCode::LongLong);
    let real = || FieldType::new(FieldTypeCode::Double);
    let vector = || FieldType::new(FieldTypeCode::VectorFloat32);
    if name.starts_with("date_add_") || name.starts_with("date_sub_") {
        return date_add_return_type(name, args);
    }
    Some(match name {
        // ---------------------------------------------------------------
        // The STRING family's flen, `pkg/expression/builtin_string.go`.
        //
        // Go sizes each of these from its ARGUMENTS, and the width reaches the
        // client as the result set's `ColumnLength`. It also decides, through
        // [`promote_wide_string_result`], whether the result is still a
        // `var_string` or has become a MEDIUM/LONG blob -- which is what
        // `pkg/ddl/index.go`'s `checkIndexColumn` reads when it refuses an
        // expression index. Every number below is Go's own, cross-checked
        // against `pkg/expression/typeinfer_test.go`'s
        // `createTestCase4StrFuncs` golden table.
        // ---------------------------------------------------------------
        // `bf.tp.SetFlen(args[0].GetType().GetFlen())`, the argument's own
        // width passed straight through. `TRANSLATE` (`translateFunctionClass`)
        // is the same rule and joins them here.
        "upper" | "ucase" | "lower" | "lcase" | "reverse" | "ltrim" | "rtrim" | "left" | "right"
        | "substring" | "substr" | "mid" | "substring_index" | "translate"
        | "regexp_substr" | "regexp_replace" => {
            ft_with_flen(text(), str_arg_flen(args, 0))
        }
        // `trimFunctionClass` is the same rule for the ONE- and THREE-argument
        // forms and, uniquely, sets no flen at all for the two-argument
        // `TRIM(remstr FROM str)` form -- so that one stays unspecified. Go's
        // omission, not ours.
        "trim" => {
            if args.len() == 2 {
                text()
            } else {
                ft_with_flen(text(), str_arg_flen(args, 0))
            }
        }
        // A fixed `mysql.MaxBlobWidth`, which the promotion above then turns
        // into a LONGTEXT/LONGBLOB result.
        "repeat" | "space" | "insert_func" | "format" => ft_with_flen(text(), MAX_BLOB_WIDTH),
        // A fixed 64, the widest a 64-bit word can print in base 2.
        // `convFunctionClass` (`builtin_math.go`) shares the constant.
        "bin" | "oct" | "conv" => ft_with_flen(text(), 64),
        // `replaceFunctionClass.fixLength`: the subject's width, grown by the
        // replacement's excess over the needle once per whole needle that
        // fits. Ported as the integer arithmetic Go writes, guards included.
        "replace" if args.len() == 3 => {
            let mut char_len = str_arg_flen(args, 0);
            let old_len = str_arg_flen(args, 1);
            let diff = str_arg_flen(args, 2) - old_len;
            if diff > 0 && old_len > 0 {
                char_len += (char_len / old_len) * diff;
            }
            ft_with_flen(text(), char_len)
        }
        // `getFlen4LpadAndRpad`: the PAD LENGTH argument decides, and only when
        // it is a constant -- anything else, an overflowing one, or a NULL is
        // `mysql.MaxBlobWidth`. The chosen width is then multiplied by four
        // (the widest a character can encode to) and clamped.
        "lpad" | "rpad" if args.len() == 3 => {
            let flen = lpad_rpad_flen(&args[1]).saturating_mul(4);
            ft_with_flen(text(), if flen > MAX_BLOB_WIDTH { MAX_BLOB_WIDTH } else { flen })
        }
        // `eltFunctionClass`: the widest selectable value, where an argument of
        // UNKNOWN width does not widen but RESETS -- Go's condition is
        // `flen == UnspecifiedLength || flen > bf.tp.GetFlen()`, so a later
        // unsized argument makes the whole result unsized again.
        "elt" if args.len() >= 2 => {
            let mut flen = tidb_datatype::UNSPECIFIED_LENGTH;
            for i in 1..args.len() {
                let arg_flen = str_arg_flen(args, i);
                if arg_flen == tidb_datatype::UNSPECIFIED_LENGTH || arg_flen > flen {
                    flen = arg_flen;
                }
            }
            ft_with_flen(text(), flen)
        }
        // `exportSetFunctionClass`: sixty-four copies of the wider of the ON
        // and OFF strings, sixty-three separators between them, times four for
        // the character width. The separator defaults to one character when
        // the argument is absent.
        "export_set" if args.len() >= 3 => {
            let value_flen = str_arg_flen(args, 1).max(str_arg_flen(args, 2));
            let separator_flen = if args.len() > 3 {
                str_arg_flen(args, 3)
            } else {
                1
            };
            ft_with_flen(text(), (value_flen * 64 + separator_flen * 63) * 4)
        }
        // `makeSetFunctionClass.getFlen`: with a CONSTANT bit mask Go sizes
        // only the members that mask selects, plus one comma between each
        // adjacent pair; otherwise it sums every member and adds
        // `len(args) - 2` commas.
        "make_set" if args.len() >= 2 => {
            let flen = make_set_flen(args);
            ft_with_flen(text(), if flen > MAX_BLOB_WIDTH { MAX_BLOB_WIDTH } else { flen })
        }
        // `charFunctionClass`: four bytes per code point. The trailing
        // argument is the USING charset (see `build.rs`'s `CHAR_FUNC` arm),
        // which is not a code point, so Go's `len(args) - 1` counts it out.
        "char_func" if !args.is_empty() => {
            ft_with_flen(text(), 4 * (args.len() as i64 - 1))
        }
        // `quoteFunctionClass`: every character could need a backslash, plus
        // the two surrounding quotes.
        "quote" if args.len() == 1 => {
            let arg_flen = str_arg_flen(args, 0);
            let flen = if arg_flen == tidb_datatype::UNSPECIFIED_LENGTH {
                tidb_datatype::UNSPECIFIED_LENGTH
            } else {
                2 * arg_flen + 2
            };
            ft_with_flen(text(), if flen > MAX_BLOB_WIDTH { MAX_BLOB_WIDTH } else { flen })
        }
        // `hexFunctionClass` picks its argument's EVAL type and so its width
        // rule: a string-ish argument is hexed as up to four bytes per
        // character, two hex digits each, while a numeric one is declared
        // `ETInt` -- and `WrapWithCastAsInt` copies the source flen through
        // unchanged, so its own width is what gets doubled.
        "hex" if args.len() == 1 => {
            use tidb_datatype::EvalType;
            // An argument with no static type takes the STRING branch, which
            // is where an unknown width lands anyway: Go always has a type
            // here, so this is only about not refusing the call outright.
            let flen = match arg_eval_type(args, 0) {
                EvalType::Int | EvalType::Real | EvalType::Decimal => {
                    scale_flen(raw_arg_flen(args, 0), 2)
                }
                _ => scale_flen(str_arg_flen(args, 0), 8),
            };
            ft_with_flen(text(), flen)
        }
        // Go `md5FunctionClass` (`pkg/expression/builtin_encryption.go:581`):
        // connection-charset text of flen 32, the width of one hexed MD5
        // digest -- the same shape as its `SHA`/`SHA1`/`SHA2` siblings below,
        // which is why it does not belong in the unsized string family above.
        "md5" if args.len() == 1 => {
            let mut ft = text();
            ft.set_flen(32);
            ft
        }
        // `CONCAT`/`CONCAT_WS` are the two of that family that SIZE their
        // result, and the size reaches the client as the column metadata's
        // `ColumnLength`. See [`concat_flen`].
        "concat" => {
            let mut ft = text();
            ft.set_flen(concat_flen(args, 0));
            ft
        }
        "concat_ws" if !args.is_empty() => {
            // The separator lands between the remaining arguments, so it is
            // counted `len(args) - 2` times rather than once.
            let separators = i64::from(u32::try_from(args.len().saturating_sub(2)).ok()?);
            let separator_flen = separators * string_cast_flen(args[0].static_type()?);
            ft_with_flen(text(), clamp_blob_width(concat_flen(args, 1) + separator_flen))
        }
        // Go `unhexFunctionClass`/`base64FunctionClass.fromBase64` (see
        // `types.SetBinChsClnFlag` in `pkg/expression/builtin_string.go`):
        // unlike the plain string-in-string-out family above, these two
        // return a BINARY-collated `VarString`, which is what makes
        // `CHAR_LENGTH(UNHEX(...))` count bytes rather than characters.
        // `unhexFunctionClass` is the ONE builtin in the family that reads its
        // argument's type BEFORE calling `newBaseBuiltinFuncWithTp`, so the
        // width it halves is the DECLARED one and not the cast-as-string one:
        // `UNHEX(int_column)` is 6, from an `int`'s eleven digits, and not 40.
        "unhex" if args.len() == 1 => {
            use tidb_datatype::EvalType;
            let raw = raw_arg_flen(args, 0);
            let flen = if raw == tidb_datatype::UNSPECIFIED_LENGTH {
                tidb_datatype::UNSPECIFIED_LENGTH
            } else {
                match arg_eval_type(args, 0) {
                    EvalType::Int | EvalType::Real | EvalType::Decimal => (raw + 1) / 2,
                    _ => (raw * 4 + 1) / 2,
                }
            };
            let mut ft = ft_with_flen(text(), flen);
            set_binary_charset(&mut ft);
            ft
        }
        // `base64FunctionClass`'s decode half: three payload bytes per four
        // base64 characters, so the widest inflation is threefold.
        "from_base64" if args.len() == 1 => {
            let flen = scale_flen(str_arg_flen(args, 0), 3);
            let mut ft = ft_with_flen(
                text(),
                if flen > MAX_BLOB_WIDTH {
                    MAX_BLOB_WIDTH
                } else {
                    flen
                },
            );
            set_binary_charset(&mut ft);
            ft
        }
        // The date/time family. `TIME()` and `DATE()` retain their native
        // temporal cell domains; the remaining values this crate produces
        // are formatted strings or integers.
        "time" => time_return_type(args)?,
        "date" if args.len() == 1 => {
            let mut ft = FieldType::new(FieldTypeCode::Date);
            ft.set_decimal(0);
            ft.set_flen(10);
            ft
        }
        "now" | "current_timestamp" | "localtime" | "localtimestamp" | "utc_timestamp"
        | "curdate" | "current_date" | "utc_date"
        | "curtime" | "current_time" | "utc_time" | "monthname" | "dayname" | "last_day"
        | "sec_to_time" | "maketime" | "makedate" | "from_days" | "date_format" | "str_to_date" => text(),
        "addtime" | "subtime" => add_sub_time_return_type(args)?,
        "timestamp" | "timestampadd" | "sysdate" => text(),
        "month" | "day" | "dayofmonth" | "dayofweek" | "dayofyear" | "weekday" | "quarter"
        | "week" | "weekofyear" | "yearweek" | "year" | "hour" | "minute" | "second"
        | "microsecond" | "time_to_sec" | "to_days" | "period_add" | "period_diff"
        | "unix_timestamp" | "datediff"
        // `EXTRACT`'s composite units (`HOUR_MINUTE`, `DAY_SECOND`, ...) are
        // sugared into these single-argument function names (see the
        // `Expr::Extract` arm below) and, like every other EXTRACT unit,
        // always return an integer (`ExtractDatetimeNum`/
        // `ExtractDurationNum` in `pkg/types/time.go`).
        | "year_month" | "day_hour" | "day_minute" | "day_second" | "day_microsecond"
        | "hour_minute" | "hour_second" | "hour_microsecond" | "minute_second"
        | "minute_microsecond" | "second_microsecond" => int(),
        // `pkg/expression/builtin_vec.go`: vectors are accepted as either
        // stored vector cells or text casts, but the scalar return domain is
        // fixed by each function class.
        "vec_dims" if args.len() == 1 => int(),
        "vec_l1_distance" | "vec_l2_distance" | "vec_negative_inner_product"
        | "vec_cosine_distance" if args.len() == 2 => real(),
        "vec_l2_norm" if args.len() == 1 => real(),
        "vec_from_text" if args.len() == 1 => vector(),
        "vec_as_text" if args.len() == 1 => text(),
        // The math family. Go types these from the ARGUMENT, and the captured
        // types are what a chunk cell must be sized for: `ABS` and `MOD`
        // preserve the argument domain, `CEIL`/`FLOOR` return an integer for
        // an integer or decimal argument but stay real for a real one, and
        // `ROUND`/`TRUNCATE` keep the decimal domain. `CEIL`/`FLOOR` keep a
        // decimal only when its declared integer width exceeds 18 digits;
        // narrower decimals use the integer signatures.
        "abs" | "mod" => arg_numeric_type(args)?,
        "ceil" | "ceiling" | "floor" => ceil_floor_return_type(args)?,
        // `ROUND`/`TRUNCATE` read the FIRST argument alone -- `argTp :=
        // args[0].GetType(ctx.GetEvalCtx()).EvalType()` (`builtin_math.go:272`
        // and `:2036`) -- because the second is the SCALE, declared
        // `types.ETInt` and cast (`crate::arg_eval_type`) rather than
        // promoted. Ranking it with the value would let a STRING scale drag
        // the result into the real domain: captured from real TiDB (`gorun`),
        // `round(3.14159,'100')` is `3.141590000000000000000000000000`, the
        // same decimal as `round(3.14159,100)`, not `3.14159`.
        "round" | "truncate" => round_truncate_return_type(name, args)?,
        // Always real, whatever went in.
        "sqrt" | "pow" | "power" | "exp" | "ln" | "log" | "log2" | "log10" | "pi" | "sin"
        | "cos" | "tan" | "asin" | "acos" | "atan" | "atan2" | "cot" | "radians" | "degrees"
        | "rand" => FieldType::new(FieldTypeCode::Double),
        "sign" | "crc32" => int(),
        // The JSON family's value slice: JSON evaluated as VALUES. Go types
        // the first group `MysqlJson` and the second group as strings/ints.
        //
        // DOCUMENTED DIVERGENCE, the same one the temporal casts carry: this
        // crate has no BinaryJSON value, so a JSON-returning builtin produces
        // its canonical JSON TEXT (`format_json`, which is
        // `BinaryJSON.MarshalJSON`'s exact spelling -- byte-sorted keys,
        // `, ` / `: ` separators). The VALUE therefore matches TiDB
        // byte for byte; the reported column type is `VarString` where TiDB
        // says `JSON`. Typing it as Go does would put a string into a JSON
        // cell, which panics rather than mistyping.
        //
        // `JSON_TABLE` is deliberately NOT listed: it is a table function,
        // not a scalar, so it keeps falling through to the refusal below.
        //
        // The type Go would have reported is not thrown away with the
        // divergence: [`go_result_type_code`] keeps it, for the one caller
        // that needs Go's ANSWER rather than this crate's cell type.
        "json_extract" | "json_object" | "json_array" | "json_keys" | "json_quote"
        | "json_unquote" | "json_type" | "json_set" | "json_insert" | "json_replace"
        | "json_remove" | "json_array_append" | "json_array_insert" | "json_merge"
        | "json_merge_preserve" | "json_merge_patch" => text(),
        "json_contains" | "json_length" | "json_depth" => int(),
        // `ast.JSONValid` is in Go's `booleanFunctions` map
        // (`json_contains`/`_length`/`_depth` are not), so its `ETInt` result
        // carries `IsBooleanFlag`: `JSON_ARRAY(json_valid('{}'))` is `[true]`.
        // `json_valid` HAS an evaluator here (`builtin_ext::json::report`), so
        // the flag is observable and the existing result-type arm is unchanged
        // except for the flag.
        //
        // `ast.JSONSchemaValid` is ALSO a Go boolean function, but this tier has
        // NO evaluator for it, so it deliberately keeps NO result-type arm:
        // giving it one lets the rewriter build a ScalarFunction that an
        // expression index would accept and then fail to populate. The flag is
        // unobservable on a value that can never be produced, and refusing at the
        // rewrite door (the Go-code-1105 wrong-refuse pinned as the safe
        // direction by `tests_expression_indexes::every_ga_function_passes_the_gate`)
        // is the correct behaviour.
        "json_valid" => {
            let mut ft = int();
            ft.add_flags(tidb_datatype::FieldTypeFlags::IS_BOOLEAN);
            ft
        }
        // Go `loadFileFunctionClass.getFunction`: an `ETString` result of
        // flen 64 in the connection charset. The VALUE is always NULL.
        "load_file" if args.len() == 1 => {
            let mut ft = text();
            ft.set_flen(64);
            ft
        }
        // NOT the same merge a CASE uses, even though both are "combine the
        // argument types". Go `greatestFunctionClass.getFunction` takes only
        // the EVAL TYPE from `resolveType4Extremum` and then picks one
        // signature per eval type -- `builtinGreatestRealSig` returns an
        // `f64`, so a FLOAT argument comes back as DOUBLE. A CASE instead
        // keeps the merged FIELD type (`types.AggFieldType`), where FLOAT
        // beside an integer stays FLOAT. Captured from Go over a FLOAT column
        // holding 12.191: `greatest(c,0)` prints 12.190999984741211 while
        // `case ... then c else 0 end` prints 12.191.
        "greatest" | "least" => extremum_return_type(args)?,
        // `NULLIF` keeps its first argument's type; the second only decides
        // whether the result is NULL.
        "nullif" => args.first()?.static_type()?.clone(),
        // `ANY_VALUE` is the identity on both value AND type: Go
        // `anyValueFunctionClass.getFunction` clones the argument's whole
        // `FieldType` over the builder's (`*bf.tp = *ft`), so the charset,
        // collation, flen and decimal all pass through untouched.
        "any_value" if args.len() == 1 => args.first()?.static_type()?.clone(),
        // Reading a user variable: Go's `BuildGetVarFunction` picks one of its
        // typed `GETVAR` signatures from the type the session currently holds
        // for the name, so the CHOICE is made before the rewriter runs and
        // arrives encoded in the name -- the same "build-time decision lives
        // in the function name" shape `cast_*` and `date_add_*` use. The
        // declared type must agree with the value the evaluator returns,
        // because the chunk tier appends into a column of exactly this type.
        "getvar_int" if args.len() == 1 => int(),
        "getvar_uint" if args.len() == 1 => {
            let mut ft = int();
            ft.add_flags(tidb_datatype::FieldTypeFlags::UNSIGNED);
            ft
        }
        "getvar_real" if args.len() == 1 => FieldType::new(FieldTypeCode::Double),
        "getvar_decimal" if args.len() == 1 => FieldType::new(FieldTypeCode::NewDecimal),
        "getvar_string" if args.len() == 1 => text(),
        // `SETVAR` reports -- and stores -- its value argument's type.
        "setvar" if args.len() == 2 => args[1].static_type().cloned().unwrap_or_else(text),
        // Go reads these from `SessionVars`; each returns a string of flen 64
        // (`databaseFunctionClass`, `versionFunctionClass`,
        // `currentUserFunctionClass`, `currentRoleFunctionClass`,
        // `userFunctionClass`, `currentResourceGroupFunctionClass` in
        // `pkg/expression/builtin_info.go` -- every one of them ends in
        // `bf.tp.SetFlen(64)`). `SCHEMA` is an alias of `DATABASE` and
        // `SESSION_USER`/`SYSTEM_USER` of `USER`, sharing the same class and
        // so the same width. `CURRENT_RESOURCE_GROUP` is deliberately absent:
        // it has the same flen 64, but no evaluator arm exists for it, so it
        // stays refused rather than typed-then-unevaluable.
        "database" | "schema" | "version" | "current_user" | "current_role" | "user"
        | "session_user" | "system_user" => {
            let mut ft = text();
            ft.set_flen(64);
            ft
        }
        // Go sizes this VarString from `printer.GetTiDBInfo()`.
        "tidb_version" if args.is_empty() => ft_with_flen(
            text(),
            i64::try_from(tidb_util::printer::get_tidb_info(&Default::default()).len())
                .unwrap_or(i64::MAX),
        ),
        // Go `connectionIDFunctionClass` fixes an unsigned `LongLong`.
        "connection_id" => {
            let mut ft = int();
            ft.add_flags(tidb_datatype::FieldTypeFlags::UNSIGNED);
            ft
        }
        // Go `rowCountFunctionClass` (`pkg/expression/builtin_info.go`): a
        // plain signed `LongLong` with no flen of its own -- unlike
        // `LAST_INSERT_ID` below, `ROW_COUNT()` is signed, because a failed
        // statement reports -1.
        "row_count" if args.is_empty() => int(),
        // Go `nextValFunctionClass` / `lastValFunctionClass` /
        // `setValFunctionClass` (`pkg/expression/builtin_info.go`): each fixes
        // a signed `LongLong`. The sequence NAME travels as the first argument,
        // a string constant the rewriter substitutes for the column reference
        // the parser produced -- see the `nextval` arm of
        // `rewrite_expr_resolved`.
        // `NEXTVAL`/`LASTVAL` are flen 10 (`builtin_info.go:1503,1576`);
        // `SETVAL` instead copies its VALUE argument's flen
        // (`builtin_info.go:1643`, `args[1]`), so `SETVAL(s, 1000)` is 4 wide.
        "nextval" | "lastval" if args.len() == 1 => {
            let mut ft = int();
            ft.set_flen(10);
            ft
        }
        "setval" if args.len() == 2 => {
            let mut ft = int();
            if let Some(arg) = args[1].static_type() {
                ft.set_flen(arg.flen());
            }
            ft
        }
        // Go `lastInsertIDFunctionClass` adds `mysql.UnsignedFlag`, while
        // `newBaseBuiltinFuncWithTp` stamps every integer result with
        // `mysql.BinaryFlag`. Both arities therefore expose the same binary
        // unsigned BIGINT metadata.
        "last_insert_id" if args.len() <= 1 => {
            let mut ft = int();
            ft.add_flags(
                tidb_datatype::FieldTypeFlags::UNSIGNED
                    | tidb_datatype::FieldTypeFlags::BINARY,
            );
            ft
        }
        // Go `bitCountFunctionClass` (`pkg/expression/builtin_other.go`):
        // int in, int out, flen 2 (a 64-bit word has at most 64 set bits).
        "bit_count" if args.len() == 1 => {
            let mut ft = int();
            ft.set_flen(2);
            ft
        }
        // Go `isNullFunctionClass` (`pkg/expression/builtin_op.go`): the
        // one-digit boolean shape. This is the FUNCTION spelling `ISNULL(x)`;
        // the `x IS NULL` operator is rewritten onto the same name by the
        // `Expr::Is` arm below, so both spellings share this type.
        "isnull" if args.len() == 1 => {
            let mut ft = int();
            ft.set_flen(1);
            // `ast.IsNull` is in Go's `booleanFunctions` map.
            ft.add_flags(tidb_datatype::FieldTypeFlags::IS_BOOLEAN);
            ft
        }
        // The hash family (`pkg/expression/builtin_encryption.go`). Each
        // returns hex TEXT in the CONNECTION charset -- explicitly NOT
        // binary, unlike `UNHEX`/`FROM_BASE64` above -- with a fixed flen:
        // `sha1FunctionClass` 40 (one SHA-1 digest hexed) and
        // `sha2FunctionClass` 128 (sized for its widest variant, SHA-512).
        // Go gives both SHA-1 and SM3 flen 40. SM3 actually emits 64 hex
        // digits, but the 40-byte metadata is observable and source-owned.
        "sha" | "sha1" | "sm3" if args.len() == 1 => {
            let mut ft = text();
            ft.set_flen(40);
            ft
        }
        "sha2" if args.len() == 2 => {
            let mut ft = text();
            ft.set_flen(128);
            ft
        }
        // Go `ordFunctionClass` (`pkg/expression/builtin_string.go`): string
        // in, int out, flen 10 -- the code point of the first CHARACTER under
        // the argument's charset, so a binary argument yields its first byte.
        "ord" if args.len() == 1 => {
            let mut ft = int();
            ft.set_flen(10);
            ft
        }
        // Go `toBase64FunctionClass`: connection-charset text whose flen is
        // derived from the argument's via `base64NeededEncodedLength`, and
        // stays unspecified when the argument's is (which is the case for
        // every expression this tier types as `text()`).
        "to_base64" if args.len() == 1 => {
            let mut ft = text();
            ft.set_flen(base64_needed_encoded_length(str_arg_flen(args, 0)));
            ft
        }
        // The IP-address family (`pkg/expression/builtin_miscellaneous.go`).
        // `INET_ATON` is UNSIGNED -- that flag is the whole reason
        // `INET_ATON('255.255.255.255')` reports 4294967295 rather than a
        // negative number once it is widened.
        "inet_aton" if args.len() == 1 => {
            let mut ft = int();
            ft.set_flen(21);
            ft.add_flags(tidb_datatype::FieldTypeFlags::UNSIGNED);
            ft
        }
        "inet_ntoa" if args.len() == 1 => {
            let mut ft = text();
            ft.set_flen(93);
            ft.set_decimal(0);
            ft
        }
        // `INET6_ATON` returns raw address BYTES, so Go stamps it binary
        // (`types.SetBinChsClnFlag`) exactly as it does `UNHEX`; `INET6_NTOA`
        // returns printable text in the connection charset.
        "inet6_aton" if args.len() == 1 => {
            let mut ft = text();
            set_binary_charset(&mut ft);
            ft.set_flen(16);
            ft.set_decimal(0);
            ft
        }
        "inet6_ntoa" if args.len() == 1 => {
            let mut ft = text();
            ft.set_flen(117);
            ft.set_decimal(0);
            ft
        }
        // The IP-address predicates: one-digit boolean ints. `ast.IsIPv4`,
        // `IsIPv4Compat`, `IsIPv4Mapped` and `IsIPv6` are in Go's
        // `booleanFunctions` map, so their result carries `IsBooleanFlag` and a
        // `JSON_ARRAY(is_ipv4(...))` element is JSON `true`/`false`.
        "is_ipv4" | "is_ipv4_compat" | "is_ipv4_mapped" | "is_ipv6" if args.len() == 1 => {
            let mut ft = int();
            ft.set_flen(1);
            ft.add_flags(tidb_datatype::FieldTypeFlags::IS_BOOLEAN);
            ft
        }
        // `IS_UUID` is a one-digit boolean int too, but it is NOT in Go's
        // `booleanFunctions` map, so it stays an ordinary integer (a
        // `JSON_ARRAY(is_uuid(...))` element is `1`/`0`, matching TiDB).
        "is_uuid" if args.len() == 1 => {
            let mut ft = int();
            ft.set_flen(1);
            ft
        }
        // `NAME_CONST(label, value)` is the identity on both value AND type:
        // Go `nameConstFunctionClass.getFunction`
        // (`pkg/expression/builtin_miscellaneous.go:1259`) clones the SECOND
        // argument's whole `FieldType` over the builder's (`*bf.tp =
        // *args[1].GetType(...)`), exactly as `ANY_VALUE` does with its only
        // argument. The first argument is the column label, not part of the
        // value.
        "name_const" if args.len() == 2 => args[1].static_type()?.clone(),
        // Go `uuidTimestampFunctionClass`
        // (`builtin_miscellaneous.go:1679`): an `ETDecimal` of flen 18 and
        // six fractional digits -- microsecond resolution on a Unix epoch.
        "uuid_timestamp" if args.len() == 1 => {
            let mut ft = FieldType::new(FieldTypeCode::NewDecimal);
            ft.set_flen(18);
            ft.set_decimal_under_limit(6);
            ft
        }
        // Go `uuidToBinFunctionClass` (`builtin_miscellaneous.go:1814`): the
        // raw sixteen address bytes, so `types.SetBinChsClnFlag` -- the same
        // treatment `UNHEX` and `INET6_ATON` above get, and for the same
        // reason (the result is not text in any charset).
        "uuid_to_bin" if args.len() == 1 || args.len() == 2 => {
            let mut ft = text();
            set_binary_charset(&mut ft);
            ft.set_flen(16);
            ft.set_decimal(0);
            ft
        }
        // Go `binToUUIDFunctionClass` (`builtin_miscellaneous.go:1898`): the
        // printable 36-character dashed spelling in the CONNECTION charset,
        // sized 32 -- Go's own width, which counts the hex digits and not the
        // four dashes.
        "bin_to_uuid" if args.len() == 1 || args.len() == 2 => {
            let mut ft = text();
            ft.set_flen(32);
            ft.set_decimal(0);
            ft
        }
        // Go `tidbShardFunctionClass` (`builtin_miscellaneous.go:1982`) and
        // `vitessHashFunctionClass` (`:1760`): both are UNSIGNED ints with
        // `types.SetBinChsClnFlag`. `TIDB_SHARD` is flen 4 because its value
        // is a Vitess hash taken mod 256; `VITESS_HASH` is the full 64-bit
        // digest, flen 20.
        "tidb_shard" if args.len() == 1 => {
            let mut ft = int();
            ft.set_flen(4);
            ft.add_flags(tidb_datatype::FieldTypeFlags::UNSIGNED);
            set_binary_charset(&mut ft);
            ft
        }
        "vitess_hash" if args.len() == 1 => {
            let mut ft = int();
            ft.set_flen(20);
            ft.add_flags(tidb_datatype::FieldTypeFlags::UNSIGNED);
            set_binary_charset(&mut ft);
            ft
        }
        // Go `passwordFunctionClass` (`builtin_encryption.go:487`):
        // `mysql.PWDHashLen + 1` = 41, the 40 hex digits of the double-SHA-1
        // digest plus the leading `*`.
        "password" if args.len() == 1 => {
            let mut ft = text();
            ft.set_flen(41);
            ft
        }
        // Go `encodeFunctionClass`/`decodeFunctionClass`: the stream cipher is
        // length-preserving, so both report argument 0's own flen and NEITHER
        // sets the binary charset -- Go leaves them in the connection charset
        // even though `ENCODE`'s output is arbitrary bytes.
        "encode" | "decode" if args.len() == 2 => {
            let mut ft = text();
            if let Some(arg) = args[0].static_type() {
                ft.set_flen(arg.flen());
            }
            ft
        }
        "random_bytes" => crypto::random_bytes_return_type(text()),
        "compress" if args.len() == 1 => {
            crypto::compress_return_type(str_arg_flen(args, 0), text())
        }
        "aes_encrypt" => {
            let mut ft = text();
            let input = raw_arg_flen(args, 0);
            ft.set_flen(16 * (input / 16 + 1));
            set_binary_charset(&mut ft);
            ft
        }
        "aes_decrypt" => {
            let mut ft = text();
            ft.set_flen(raw_arg_flen(args, 0));
            set_binary_charset(&mut ft);
            ft
        }
        // Go `uncompressFunctionClass` (`builtin_encryption.go:911`): flen
        // `mysql.MaxBlobWidth` (16777216) with `types.SetBinChsClnFlag`, since
        // the inflated payload is whatever bytes were compressed.
        "uncompress" if args.len() == 1 => {
            let mut ft = text();
            set_binary_charset(&mut ft);
            ft.set_flen(MAX_BLOB_WIDTH);
            ft
        }
        // Go `uncompressedLengthFunctionClass` (`builtin_encryption.go:972`):
        // an int of flen 10 read out of the payload's 4-byte length prefix.
        "uncompressed_length" if args.len() == 1 => {
            let mut ft = int();
            ft.set_flen(10);
            ft
        }
        "validate_password_strength" if args.len() == 1 => ft_with_flen(int(), 21),
        // Go `uuidVersionFunctionClass`: an int with flen 10, NOT the boolean
        // shape above -- it reports the version nibble, not a yes/no.
        "uuid_version" if args.len() == 1 => {
            let mut ft = int();
            ft.set_flen(10);
            ft
        }
        // Go `intervalFunctionClass` (`pkg/expression/builtin_compare.go`):
        // always an int (the index of the containing bucket), with no flen of
        // its own. Whether the arguments are compared as ints or as reals is
        // an ARGUMENT-side choice that does not reach the result type.
        "interval" if args.len() >= 2 => int(),
        // These information functions return connection-charset text with no fixed flen.
        "format_bytes" | "format_nano_time" if args.len() == 1 => text(),
        "tidb_decode_plan" | "tidb_decode_binary_plan" if args.len() == 1 => text(),
        // Go `timeFormatFunctionClass` (`pkg/expression/builtin_time.go`):
        // a DURATION and a format string in, text out, with the flen sized
        // from the FORMAT argument's -- `(flen + 1) / 2 * 11`, an upper bound
        // on how much one format specifier can expand.
        "time_format" if args.len() == 2 => {
            let mut ft = text();
            let format_flen = args[1].static_type()?.flen();
            if format_flen != tidb_datatype::UNSPECIFIED_LENGTH {
                ft.set_flen((format_flen + 1) / 2 * 11);
            }
            ft
        }
        // Go `toSecondsFunctionClass`: datetime in, plain int out. It sits
        // beside `TO_DAYS` in the int family above but arrives here because
        // it takes the zero-date `calcDaynr` path in `time_fn::calendar`.
        "to_seconds" if args.len() == 1 => int(),
        // `JSON_SEARCH` is typed `ETJson` by Go; it carries the same
        // JSON-as-canonical-text divergence documented for the JSON family
        // above, so its result cell is a string here.
        "json_search" if args.len() >= 3 => text(),
        // String in, number out. The unlisted widths below -- `CHAR_LENGTH`,
        // `CHARACTER_LENGTH`, `LOCATE`, `POSITION` and `FIELD` -- are
        // `mysql.MaxIntWidth`, which is what `newReturnFieldTypeForBaseBuiltinFunc`
        // already gives every `ETInt` result, so they need no arm of their own.
        // `OCTET_LENGTH` is `lengthFunctionClass` and `POSITION` is
        // `locateFunctionClass` (`builtin.go`'s `funcs` map), so each shares
        // its alias's width.
        "length" | "octet_length" | "bit_length" => ft_with_flen(int(), 10),
        "ascii" | "find_in_set" => ft_with_flen(int(), 3),
        "instr" => ft_with_flen(int(), 11),
        "strcmp" => ft_with_flen(int(), 2),
        "char_length" | "character_length" | "locate" | "position" | "field" => int(),
        // `regexpInStrFunctionClass` fixes the integer result width to
        // `mysql.MaxIntWidth`.
        "regexp_instr" => ft_with_flen(int(), 20),
        // Go `likeFunctionClass`/`regexpLikeFunctionClass`: a one-digit
        // boolean.
        // `ast.Like` and `ast.Regexp` are in Go's `booleanFunctions` map, so
        // these operator forms are boolean-flagged. (`ilike` is not a Go boolean
        // function but is a pre-existing alias of `like`.)
        "like" | "ilike" | "regexp" | "regexp_like" => {
            let mut ft = int();
            ft.set_flen(1);
            ft.add_flags(tidb_datatype::FieldTypeFlags::IS_BOOLEAN);
            ft
        }
        // Go runs ONE inference for the whole control family
        // (`InferType4ControlFuncs`), and the eval type it returns is what
        // SELECTS the signature -- so the type decides the value here, not
        // just the printed width. It lives in [`super::control_type`].
        //
        // `IF`'s first argument is the condition, so only its two result
        // branches carry the type; `CASE WHEN`'s caller has already reduced
        // its flattened `cond, result, ..., else` list to the THEN/ELSE
        // branches (the `Expr::Case` arm of `rewrite_expr_resolved`), which
        // is Go's own `thenArgs`.
        "if" if args.len() == 3 => super::control_type::infer_type4_control_funcs("if", &args[1..])?,
        "case_when" | "ifnull" | "coalesce" => {
            super::control_type::infer_type4_control_funcs(name, args)?
        }
        _ => return None,
    })
}

/// A width multiplied by a per-character factor, with Go's one sentinel: an
/// argument of unknown width leaves the result unknown rather than scaling
/// `-1` into a nonsense number. `HEX`, `UNHEX` and `FROM_BASE64` each spell
/// this guard out; sharing it is what keeps the three from drifting.
fn scale_flen(flen: i64, factor: i64) -> i64 {
    if flen == tidb_datatype::UNSPECIFIED_LENGTH {
        tidb_datatype::UNSPECIFIED_LENGTH
    } else {
        flen.saturating_mul(factor)
    }
}

/// Go `getFlen4LpadAndRpad` (`pkg/expression/builtin_string.go`): the pad
/// length is knowable only from a CONSTANT argument, and a NULL, an
/// unevaluable one, or one wider than `mysql.MaxBlobWidth` all mean "assume
/// the widest".
fn lpad_rpad_flen(length: &Expression) -> i64 {
    let Expression::Constant(constant) = length else {
        return MAX_BLOB_WIDTH;
    };
    let value = match &constant.value {
        Datum::Int(v) => *v,
        Datum::UInt(v) => i64::try_from(*v).unwrap_or(MAX_BLOB_WIDTH),
        // Go evaluates the constant AS AN INT, so a NULL or a literal that is
        // no integer at all takes the `isNull || err != nil` branch.
        _ => return MAX_BLOB_WIDTH,
    };
    if value > MAX_BLOB_WIDTH {
        MAX_BLOB_WIDTH
    } else {
        value
    }
}

/// Go `makeSetFunctionClass.getFlen`.
///
/// A constant bit mask is the interesting half: Go sizes only the members that
/// mask actually selects and puts `count - 1` commas between them, so
/// `MAKE_SET(1, text_col, x)` is as wide as `text_col` alone. Everything else
/// -- a non-constant mask, a NULL one, one that will not evaluate -- falls to
/// the sum of every member plus `len(args) - 2` commas.
fn make_set_flen(args: &[Expression]) -> i64 {
    let member_flen = |i: usize| str_arg_flen(args, i);
    if let Expression::Constant(constant) = &args[0] {
        let bits = match &constant.value {
            Datum::Int(v) => Some(*v),
            Datum::UInt(v) => Some(*v as i64),
            _ => None,
        };
        if let Some(bits) = bits {
            let mut flen = 0_i64;
            let mut count = 0_i64;
            for i in 1..args.len() {
                // Go's `1 << uint(i-1)` is 0 once the shift reaches the word
                // width, so a 65th member is never selected.
                let bit = u32::try_from(i - 1)
                    .ok()
                    .and_then(|shift| 1_i64.checked_shl(shift))
                    .unwrap_or(0);
                if bits & bit != 0 {
                    flen += member_flen(i);
                    count += 1;
                }
            }
            if count > 0 {
                flen += count - 1;
            }
            return flen;
        }
    }
    let mut flen = 0_i64;
    for i in 1..args.len() {
        flen += member_flen(i);
    }
    flen + args.len() as i64 - 2
}

/// The flen `TO_BASE64` reports for an argument of flen `n`.
///
/// Port of `base64NeededEncodedLength` (`pkg/expression/builtin_string.go`),
/// including its two sentinels: an unspecified argument length stays
/// unspecified, and a length whose encoding would overflow a signed word
/// reports -1 (which is the same value as `UNSPECIFIED_LENGTH`, exactly as in
/// Go -- both mean "no usable bound"). The `+ (length - 1) / 76` term is the
/// newline every 76 output characters that MySQL's base64 inserts.
pub(super) fn base64_needed_encoded_length(n: i64) -> i64 {
    if n == tidb_datatype::UNSPECIFIED_LENGTH || n > 6_827_690_988_321_067_803 {
        return tidb_datatype::UNSPECIFIED_LENGTH;
    }
    let length = (n + 2) / 3 * 4;
    length + (length - 1) / 76
}

/// The result type of `GREATEST`/`LEAST`.
///
/// Go `greatestFunctionClass.getFunction` (`pkg/expression/builtin_compare.go`)
/// reduces `resolveType4Extremum` to an EVAL type and dispatches one signature
/// per eval type, so the result carries the eval type's canonical field type
/// rather than any argument's own: the `ETReal` arm is
/// `builtinGreatestRealSig`, whose `evalReal` is an `f64`, so a FLOAT argument
/// widens to DOUBLE here even though the same pair of branches inside a CASE
/// stays FLOAT.
/// `pkg/parser/mysql.MaxBlobWidth` and `MaxLongBlobWidth`.
const MAX_BLOB_WIDTH: i64 = 16_777_216;
const MAX_LONG_BLOB_WIDTH: i64 = 4_294_967_295;

fn ft_with_flen(mut ft: FieldType, flen: i64) -> FieldType {
    ft.set_flen(flen);
    ft
}

fn clamp_blob_width(flen: i64) -> i64 {
    if flen >= MAX_BLOB_WIDTH {
        MAX_BLOB_WIDTH
    } else {
        flen
    }
}

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
fn string_cast_flen(ft: &FieldType) -> i64 {
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

/// Go `concatFunctionClass.getFunction`, over `args[skip..]`.
///
/// The result's flen is the sum of the arguments' string-cast widths, clamped
/// at `MaxBlobWidth`. An argument of unknown width does NOT simply make the
/// whole thing unknown: Go restarts the running sum at `MaxBlobWidth` and then
/// still adds that argument's negative flen, so a single unsized argument
/// leaves `MaxBlobWidth - 1` unless a later argument pushes the sum back over
/// the clamp. That is Go's arithmetic verbatim, oddity included -- it is what
/// the client is told, so it is what we must say.
fn concat_flen(args: &[Expression], skip: usize) -> i64 {
    let mut flen = 0_i64;
    for arg in args.iter().skip(skip) {
        let arg_flen = arg
            .static_type()
            .map_or(tidb_datatype::UNSPECIFIED_LENGTH, string_cast_flen);
        if arg_flen < 0 {
            flen = MAX_BLOB_WIDTH;
        }
        flen += arg_flen;
    }
    clamp_blob_width(flen)
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

fn extremum_return_type(args: &[Expression]) -> Option<FieldType> {
    let typed: Vec<&FieldType> = args
        .iter()
        .filter_map(Expression::static_type)
        .filter(|ft| ft.code() != FieldTypeCode::Null)
        .collect();
    let first = (*typed.first()?).clone();
    // `GetAccurateCmpType` selects ETVectorFloat32 as soon as either side is
    // vector, and `greatestFunctionClass`/`leastFunctionClass` use that
    // comparison domain as their return type too. The accompanying cast turns
    // text arguments into vectors before evaluation.
    if typed
        .iter()
        .any(|ft| ft.eval_type() == tidb_datatype::EvalType::VectorFloat32)
    {
        return Some(FieldType::new(FieldTypeCode::VectorFloat32));
    }
    if typed
        .iter()
        .all(|ft| ft.eval_type() == tidb_datatype::EvalType::String)
    {
        // Go's ETString return type is `newReturnFieldTypeForBaseBuiltinFunc`'s
        // `mysql.TypeVarString` (`builtin.go`), never an argument's own code.
        // Keeping the argument's code is a survivable approximation for a
        // CHAR/VARCHAR/BLOB argument, whose width and collation it also
        // carries -- but an ENUM or SET column type declares a NAME/VALUE
        // cell, and `builtinGreatestStringSig` writes a plain string into it.
        // `GREATEST(e, e)` over one `enum('{}','[1]','x')` is TiDB's `{}`; a
        // declared ENUM result made this tier read that `{}` back as an
        // element name missing its 8-byte ordinal prefix.
        if typed.iter().any(|ft| ft.code() != first.code())
            || matches!(
                first.code(),
                FieldTypeCode::Enum | FieldTypeCode::Set | FieldTypeCode::Json
            )
        {
            let mut merged = FieldType::new(FieldTypeCode::VarString);
            merged.set_decimal(tidb_datatype::UNSPECIFIED_LENGTH);
            return Some(merged);
        }
        return Some(first);
    }
    if typed.iter().all(|ft| {
        matches!(
            ft.eval_type(),
            tidb_datatype::EvalType::Int
                | tidb_datatype::EvalType::Decimal
                | tidb_datatype::EvalType::Real
        )
    }) {
        let owned: Vec<Expression> = typed
            .iter()
            .map(|ft| Expression::Constant(Constant::new(Datum::Null, (*ft).clone())))
            .collect();
        let mut merged = arg_numeric_type(&owned)?;
        // Go takes GREATEST/LEAST's result type from `aggregateType`, not
        // from a pairwise rank (`builtin_compare.go:452`), and `AggFieldType`
        // does one thing the rank cannot: a MIXED-SIGN pair of same-width
        // integers is promoted one rank, and LONGLONG's next rank is DECIMAL
        // (`types/field_type.go:77-97`). That is what lets
        // `GREATEST(CAST(9223372036854775808 AS UNSIGNED), 1)` answer
        // 9223372036854775808 -- the signed 64-bit domain has no room for it,
        // and an ETInt signature compares the two as signed and picks the 1.
        let aggregated = tidb_datatype::agg_field_type(
            &typed.iter().map(|ft| (*ft).clone()).collect::<Vec<_>>(),
        );
        if merged.eval_type() == tidb_datatype::EvalType::Int
            && aggregated.eval_type() == tidb_datatype::EvalType::Decimal
        {
            merged = FieldType::new(FieldTypeCode::NewDecimal);
        }
        set_numeric_len_from_args(&mut merged, &typed);
        // `getFunction`'s ETInt arm: `bf.tp.AddFlag(resFieldType.GetFlag())`
        // (`builtin_compare.go:523`), which is the ONLY arm that carries the
        // aggregate's flags onto the result -- so an all-unsigned GREATEST
        // reports UNSIGNED and every other arm does not.
        if merged.eval_type() == tidb_datatype::EvalType::Int && aggregated.is_unsigned() {
            merged = merged.with_added_flags(tidb_datatype::FieldTypeFlags::UNSIGNED);
        }
        return Some(merged);
    }
    // Go's ETDatetime/ETTimestamp and ETDuration arms
    // (`builtin_compare.go:543-553`): when every argument is temporal the
    // aggregate is temporal too, and the signature returns a TIME, not text.
    // `AggFieldType` is what widens a DATE beside a DATETIME, which is why
    // `LEAST(date, datetime)` reads `2020-01-01 00:00:00` and not
    // `2020-01-01`.
    if typed.iter().all(|ft| {
        matches!(
            ft.eval_type(),
            tidb_datatype::EvalType::Datetime
                | tidb_datatype::EvalType::Timestamp
                | tidb_datatype::EvalType::Duration
        )
    }) {
        let aggregated = tidb_datatype::agg_field_type(
            &typed.iter().map(|ft| (*ft).clone()).collect::<Vec<_>>(),
        );
        if aggregated.code().is_type_temporal() {
            return Some(aggregated);
        }
    }
    // Everything left is a MIXTURE of kinds, which Go handles the same way it
    // handles the uniform cases: `resolveType4Extremum` aggregates the
    // argument FieldTypes and `getFunction` picks one signature per EVAL type.
    // A mixture whose aggregate is string-kind -- a string beside a number, a
    // DATE beside a string (which additionally selects the compare-as-time
    // signature, see [`gl_cmp_string_mode`]), or the JSON case Go also folds
    // onto `ETString` -- therefore returns a string, where this table used to
    // refuse the whole shape and no such call ran at all.
    let aggregated =
        tidb_datatype::agg_field_type(&typed.iter().map(|ft| (*ft).clone()).collect::<Vec<_>>());
    if aggregated.eval_type().is_string_kind() {
        let mut merged = FieldType::new(FieldTypeCode::VarString);
        merged.set_decimal(tidb_datatype::UNSPECIFIED_LENGTH);
        return Some(merged);
    }
    if typed.iter().any(|ft| ft.code() != first.code()) {
        return None;
    }
    Some(first)
}

/// The numeric type a type-preserving math builtin reports, which Go takes
/// from its argument: an integer argument keeps the integer domain, a decimal
/// keeps the decimal one, and a real keeps the real one. A pair of arguments
/// (`MOD`, `ATAN2`) takes the wider of the two, which is the same order Go's
/// `setFlenDecimal4Int`-family builders use.
fn arg_numeric_type(args: &[Expression]) -> Option<FieldType> {
    use tidb_datatype::EvalType;
    let mut best: Option<FieldType> = None;
    for arg in args {
        let ft = arg.static_type()?;
        let rank = |ft: &FieldType| match ft.eval_type() {
            EvalType::Int => 0,
            EvalType::Decimal => 1,
            _ => 2,
        };
        if best.as_ref().is_none_or(|current| rank(ft) > rank(current)) {
            best = Some(ft.clone());
        }
    }
    let best = best?;
    Some(match best.eval_type() {
        EvalType::Int => FieldType::new(FieldTypeCode::LongLong),
        EvalType::Decimal => FieldType::new(FieldTypeCode::NewDecimal),
        // A string argument is read as a number, which Go does as a real.
        _ => FieldType::new(FieldTypeCode::Double),
    })
}

#[cfg(test)]
#[path = "result_type_tests.rs"]
mod tests;

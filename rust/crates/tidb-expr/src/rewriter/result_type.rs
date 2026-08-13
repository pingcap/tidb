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

fn builtin_return_type_before_ret_tp(name: &str, args: &[Expression]) -> Option<FieldType> {
    let text = || {
        let mut ft = FieldType::new(FieldTypeCode::VarString);
        ft.set_decimal(tidb_datatype::UNSPECIFIED_LENGTH);
        ft
    };
    let int = || FieldType::new(FieldTypeCode::LongLong);
    let real = || FieldType::new(FieldTypeCode::Double);
    let vector = || FieldType::new(FieldTypeCode::VectorFloat32);
    // `date_add_<unit>`/`date_sub_<unit>` carry the INTERVAL unit in the name
    // (see the `Expr::Func` arm of `rewrite_expr_resolved`). Real TiDB types
    // these from the date argument (DATE in, DATE out; DATETIME in, DATETIME
    // out), which this tier renders as the same formatted string the row path
    // produces — the documented temporal-as-string divergence.
    if name.starts_with("date_add_") || name.starts_with("date_sub_") {
        return Some(text());
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
        | "substring" | "substr" | "mid" | "substring_index" | "translate" => {
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
        // The date/time family. `TIME()` is the native Duration arm above;
        // the remaining temporal values this crate produces are formatted
        // strings or integers, so their reported column type retains the
        // documented temporal-as-string divergence.
        "time" => time_return_type(args)?,
        "now" | "current_timestamp" | "localtime" | "localtimestamp" | "utc_timestamp"
        | "curdate" | "current_date" | "utc_date"
        | "curtime" | "current_time" | "utc_time" | "monthname" | "dayname" | "last_day"
        | "sec_to_time" | "maketime" | "makedate" | "from_days" | "date_format" | "str_to_date"
        // `ADDTIME`/`SUBTIME` return one of Go's THREE result types
        // (`getBf4TimeAddSub`: DATETIME, TIME or STRING, chosen from the
        // first argument's own type), `TIMESTAMP` a DATETIME and
        // `TIMESTAMPADD`/`SYSDATE` a VarString and a DATETIME. All five land
        // on the same documented divergence as the rest of this family: the
        // value is the formatted string, so the reported type is text.
        | "addtime" | "subtime" | "timestamp" | "timestampadd" | "sysdate" => text(),
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
        // `ROUND`/`TRUNCATE` keep the decimal domain.
        "abs" | "mod" => arg_numeric_type(args)?,
        "ceil" | "ceiling" | "floor" => match arg_numeric_type(args)?.eval_type() {
            tidb_datatype::EvalType::Real => FieldType::new(FieldTypeCode::Double),
            _ => int(),
        },
        // `ROUND`/`TRUNCATE` read the FIRST argument alone -- `argTp :=
        // args[0].GetType(ctx.GetEvalCtx()).EvalType()` (`builtin_math.go:272`
        // and `:2036`) -- because the second is the SCALE, declared
        // `types.ETInt` and cast (`crate::arg_eval_type`) rather than
        // promoted. Ranking it with the value would let a STRING scale drag
        // the result into the real domain: captured from real TiDB (`gorun`),
        // `round(3.14159,'100')` is `3.141590000000000000000000000000`, the
        // same decimal as `round(3.14159,100)`, not `3.14159`.
        "round" | "truncate" => match arg_numeric_type(&args[..1])?.eval_type() {
            tidb_datatype::EvalType::Real => FieldType::new(FieldTypeCode::Double),
            tidb_datatype::EvalType::Int if name == "round" => int(),
            _ => FieldType::new(FieldTypeCode::NewDecimal),
        },
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
mod time_source_tests {
    use super::*;
    use crate::NoColumns;
    use tidb_ast::Expr;
    use tidb_chunk::chunk::Chunk;

    fn string_arg(value: &str) -> Expression {
        let mut ft = FieldType::new(FieldTypeCode::VarString);
        ft.set_flen(value.len() as i64);
        Expression::Constant(Constant::new(Datum::new_string(value.to_owned()), ft))
    }

    fn chunk_time(value: &str) -> Datum {
        let expression = Expr::Func {
            name: "time".to_owned(),
            args: vec![Expr::String(value.to_owned())],
            origin_position: 0,
        };
        let rewritten = crate::rewriter::rewrite_expr(&expression).unwrap();
        let mut chunk = Chunk::new_empty(&[]);
        chunk.set_num_virtual_rows(1);
        rewritten.eval(&NoColumns, chunk.get_row(0)).unwrap()
    }

    /// Exact metadata half of Go `TestTime`: the four positive spellings,
    /// the negative maximum duration, and the integer-zero build boundary.
    #[test]
    fn test_time() {
        for (value, fsp, flen) in [
            ("2003-12-31 01:02:03", 0, 10),
            ("2003-12-31 01:02:03.000123", 6, 17),
            ("01:02:03.000123", 6, 17),
            ("01:02:03", 0, 10),
            ("-838:59:59.000000", 6, 17),
        ] {
            let result = builtin_return_type("time", &[string_arg(value)]).unwrap();
            assert_eq!(result.code(), FieldTypeCode::Duration);
            assert_eq!(result.charset_name(), "binary");
            assert_eq!(result.collation_name(), "binary");
            assert!(result.has_flag(tidb_datatype::FieldTypeFlags::BINARY));
            assert_eq!(result.decimal(), fsp);
            assert_eq!(result.flen(), flen);
            let Datum::Duration(duration) = chunk_time(value) else {
                panic!("TIME must evaluate into its declared duration domain")
            };
            let expected = value
                .split_once(char::is_whitespace)
                .map_or(value, |(_, time)| time);
            assert_eq!(duration.to_string(), expected);
        }

        let zero = Expression::Constant(Constant::new(
            Datum::Int(0),
            FieldType::new(FieldTypeCode::LongLong),
        ));
        let result = builtin_return_type("time", &[zero]).unwrap();
        assert_eq!(result.code(), FieldTypeCode::Duration);
        assert_eq!(result.decimal(), 0);
        assert_eq!(result.flen(), 10);
        assert!(builtin_return_type("time", &[]).is_none());
    }
}

#[cfg(test)]
mod vector_result_type_tests {
    use super::*;

    fn string_arg() -> Expression {
        Expression::Constant(Constant::new(
            Datum::new_string("[1,2]"),
            FieldType::new(FieldTypeCode::VarString),
        ))
    }

    #[test]
    fn vector_builtin_result_domains_match_their_function_classes() {
        let vector = Expression::Constant(Constant::new(
            Datum::Null,
            FieldType::new(FieldTypeCode::VectorFloat32),
        ));
        assert_eq!(
            builtin_return_type("vec_dims", std::slice::from_ref(&vector))
                .expect("VEC_DIMS type")
                .code(),
            FieldTypeCode::LongLong
        );
        assert_eq!(
            builtin_return_type("vec_l2_distance", &[vector.clone(), vector.clone()])
                .expect("VEC_L2_DISTANCE type")
                .code(),
            FieldTypeCode::Double
        );
        assert_eq!(
            builtin_return_type("vec_from_text", &[string_arg()])
                .expect("VEC_FROM_TEXT type")
                .code(),
            FieldTypeCode::VectorFloat32
        );
        assert_eq!(
            builtin_return_type("vec_as_text", &[vector])
                .expect("VEC_AS_TEXT type")
                .code(),
            FieldTypeCode::VarString
        );
    }
}

#[cfg(test)]
mod info_source_tests {
    use std::cell::Cell;

    use super::*;
    use crate::Columns;

    struct LastInsertColumns {
        previous: u64,
        published: Cell<Option<u64>>,
    }

    impl Columns for LastInsertColumns {
        fn get(&self, _: &[String]) -> Option<Datum> {
            None
        }

        fn last_insert_id(&self) -> Option<u64> {
            Some(self.previous)
        }

        fn set_last_insert_id(&self, value: u64) {
            self.published.set(Some(value));
        }
    }

    /// Go `TestLastInsertID`: both arities' wire metadata and the complete
    /// source value matrix, including float rounding and two's-complement
    /// publication of negative and maximum unsigned arguments.
    #[test]
    fn test_last_insert_id() {
        let one_arg = [Expression::Constant(Constant::new(
            Datum::Int(1),
            FieldType::new(FieldTypeCode::LongLong),
        ))];
        for args in [&[][..], &one_arg[..]] {
            let result_type = builtin_return_type("last_insert_id", args).unwrap();
            assert_eq!(result_type.code(), FieldTypeCode::LongLong);
            assert_eq!(result_type.charset_name(), "binary");
            assert_eq!(result_type.collation_name(), "binary");
            assert!(result_type.has_flag(tidb_datatype::FieldTypeFlags::BINARY));
            assert!(result_type.is_unsigned());
            assert_eq!(result_type.flen(), 20);
        }

        for (previous, args, expected, published) in [
            (0, vec![Datum::Int(1)], 1, Some(1)),
            (0, vec![Datum::Real(1.1)], 1, Some(1)),
            (0, vec![Datum::UInt(u64::MAX)], u64::MAX, Some(u64::MAX)),
            (0, vec![Datum::Int(-1)], u64::MAX, Some(u64::MAX)),
            (1, vec![], 1, None),
            (u64::MAX, vec![], u64::MAX, None),
        ] {
            let ctx = LastInsertColumns {
                previous,
                published: Cell::new(None),
            };
            assert_eq!(
                crate::func::eval_func_values_in("LAST_INSERT_ID", &args, &ctx)
                    .expect("LAST_INSERT_ID must be dispatched")
                    .expect("source row must evaluate"),
                Datum::UInt(expected)
            );
            assert_eq!(ctx.published.get(), published);
        }
    }
}

#[cfg(test)]
mod concat_flen_tests {
    use super::*;

    fn arg(code: FieldTypeCode, flen: i64, decimal: i64) -> Expression {
        let mut ft = FieldType::new(code);
        ft.set_flen(flen);
        ft.set_decimal(decimal);
        Expression::Constant(Constant::new(Datum::Null, ft))
    }

    fn flen_of(name: &str, args: &[Expression]) -> i64 {
        builtin_return_type(name, args)
            .unwrap_or_else(|| panic!("{name} has no return type"))
            .flen()
    }

    const UNSPECIFIED: i64 = tidb_datatype::UNSPECIFIED_LENGTH;

    fn varchar(flen: i64) -> Expression {
        arg(FieldTypeCode::Varchar, flen, UNSPECIFIED)
    }
    fn bigint() -> Expression {
        arg(FieldTypeCode::LongLong, 11, 0)
    }

    /// Every number below is Go's own `GetFlen()` for the built expression,
    /// captured by running `expression.NewFunction` over these exact argument
    /// FieldTypes. `CONCAT`'s width reaches the client as the result set's
    /// `ColumnLength`, so an unspecified flen here is wire-visible.
    #[test]
    fn concat_sums_the_string_cast_widths_go_sums() {
        assert_eq!(flen_of("concat", &[varchar(16)]), 16);
        assert_eq!(flen_of("concat", &[varchar(16), varchar(16)]), 32);
        // Any integer widens to MaxIntWidth, whatever its display width.
        assert_eq!(flen_of("concat", &[bigint(), bigint()]), 40);
        assert_eq!(flen_of("concat", &[arg(FieldTypeCode::Tiny, 4, 0)]), 20);
        // BIT is the exception: its BYTE count, because TiKV needs the real
        // length when it evaluates things like ASCII(bit).
        assert_eq!(flen_of("concat", &[arg(FieldTypeCode::Bit, 8, 0)]), 1);
        // Sign, decimal point and a leading zero.
        assert_eq!(
            flen_of("concat", &[arg(FieldTypeCode::NewDecimal, 10, 2)]),
            13
        );
        // TiDB prints reals in full, never scientific notation, so the
        // worst-case widths are far wider than MySQL's 12/22.
        assert_eq!(
            flen_of(
                "concat",
                &[arg(FieldTypeCode::Float, UNSPECIFIED, UNSPECIFIED)]
            ),
            87
        );
        assert_eq!(
            flen_of(
                "concat",
                &[arg(FieldTypeCode::Double, UNSPECIFIED, UNSPECIFIED)]
            ),
            370
        );
        assert_eq!(
            flen_of(
                "concat",
                &[
                    varchar(16),
                    arg(FieldTypeCode::Double, UNSPECIFIED, UNSPECIFIED)
                ]
            ),
            386
        );
        assert_eq!(
            flen_of(
                "concat",
                &[arg(FieldTypeCode::Date, UNSPECIFIED, UNSPECIFIED)]
            ),
            10
        );
        assert_eq!(
            flen_of("concat", &[arg(FieldTypeCode::Datetime, UNSPECIFIED, 0)]),
            19
        );
        assert_eq!(
            flen_of("concat", &[arg(FieldTypeCode::Datetime, UNSPECIFIED, 3)]),
            23
        );
        assert_eq!(
            flen_of("concat", &[arg(FieldTypeCode::Duration, UNSPECIFIED, 2)]),
            13
        );
    }

    /// The separator is counted `len(args) - 2` times, so a single value
    /// argument contributes no separator at all.
    #[test]
    fn concat_ws_counts_the_separator_between_values_only() {
        assert_eq!(flen_of("concat_ws", &[varchar(3), varchar(16)]), 16);
        assert_eq!(
            flen_of("concat_ws", &[varchar(3), varchar(16), varchar(16)]),
            35
        );
        assert_eq!(
            flen_of("concat_ws", &[varchar(3), bigint(), bigint(), bigint()]),
            66
        );
        assert_eq!(
            flen_of(
                "concat_ws",
                &[
                    varchar(3),
                    arg(FieldTypeCode::Double, UNSPECIFIED, UNSPECIFIED),
                    varchar(16)
                ]
            ),
            389
        );
    }

    /// Go restarts the running sum at MaxBlobWidth for an argument of unknown
    /// width and then still ADDS that argument's -1, so a lone unsized
    /// argument leaves MaxBlobWidth - 1 rather than MaxBlobWidth. Porting the
    /// arithmetic rather than the intent is what keeps us on Go's number.
    #[test]
    fn concat_clamps_at_max_blob_width_the_way_go_does() {
        let unsized_arg = arg(FieldTypeCode::Varchar, UNSPECIFIED, UNSPECIFIED);
        assert_eq!(
            flen_of("concat", std::slice::from_ref(&unsized_arg)),
            MAX_BLOB_WIDTH - 1
        );
        // A second argument pushes the sum back over the clamp.
        assert_eq!(
            flen_of("concat", &[unsized_arg, varchar(16)]),
            MAX_BLOB_WIDTH
        );
    }
}

/// Go's `createTestCase4StrFuncs` (`pkg/expression/typeinfer_test.go`), which
/// is TiDB's own golden table for what a string builtin reports: it builds a
/// logical plan for `select <expr> from t` over a fixed schema and asserts the
/// output column's type byte, charset, flag, flen and decimal.
///
/// Only the TYPE BYTE and the FLEN are re-asserted here -- the charset and
/// flag are `derive_collation`'s answer, tested with that -- and only for the
/// builtins this rewriter builds. Every expected number below is copied from
/// that table rather than recomputed, so a rule that merely looks right does
/// not pass.
#[cfg(test)]
mod go_string_flen_tests {
    use super::*;

    const UNSPECIFIED: i64 = tidb_datatype::UNSPECIFIED_LENGTH;

    fn typed(code: FieldTypeCode, flen: i64, decimal: i64) -> Expression {
        let mut ft = FieldType::new(code);
        ft.set_flen(flen);
        ft.set_decimal(decimal);
        Expression::Constant(Constant::new(Datum::Null, ft))
    }

    fn literal(value: Datum, code: FieldTypeCode, flen: i64) -> Expression {
        let mut ft = FieldType::new(code);
        ft.set_flen(flen);
        ft.set_decimal(UNSPECIFIED);
        Expression::Constant(Constant::new(value, ft))
    }

    /// `c_char char(20)`.
    fn c_char() -> Expression {
        typed(FieldTypeCode::String, 20, UNSPECIFIED)
    }
    /// `c_binary binary(20)`.
    fn c_binary() -> Expression {
        typed(FieldTypeCode::String, 20, UNSPECIFIED)
    }
    /// `c_int_d int`.
    fn c_int_d() -> Expression {
        typed(FieldTypeCode::Long, 11, 0)
    }
    /// `c_double_d double`.
    fn c_double_d() -> Expression {
        typed(FieldTypeCode::Double, UNSPECIFIED, UNSPECIFIED)
    }
    /// `c_float_d float`.
    fn c_float_d() -> Expression {
        typed(FieldTypeCode::Float, UNSPECIFIED, UNSPECIFIED)
    }
    /// `c_decimal decimal(6, 3)`.
    fn c_decimal() -> Expression {
        typed(FieldTypeCode::NewDecimal, 6, 3)
    }
    /// `c_text_d text`.
    fn c_text_d() -> Expression {
        typed(FieldTypeCode::Blob, 65535, UNSPECIFIED)
    }
    /// `c_datetime datetime(2)`.
    fn c_datetime() -> Expression {
        typed(FieldTypeCode::Datetime, UNSPECIFIED, 2)
    }
    /// `c_set set('a', 'b', 'c')`.
    fn c_set() -> Expression {
        typed(FieldTypeCode::Set, 5, UNSPECIFIED)
    }
    /// `c_enum enum('a', 'b', 'c')`.
    fn c_enum() -> Expression {
        typed(FieldTypeCode::Enum, 1, UNSPECIFIED)
    }

    #[track_caller]
    fn assert_go(name: &str, args: &[Expression], code: FieldTypeCode, flen: i64) {
        let ft =
            builtin_return_type(name, args).unwrap_or_else(|| panic!("{name} has no return type"));
        assert_eq!((ft.code(), ft.flen()), (code, flen), "{name}");
    }

    /// The rule shape that reaches the widest part of the family:
    /// `bf.tp.SetFlen(args[0].GetType().GetFlen())` read AFTER the argument was
    /// wrapped in `WrapWithCastAsString`, which is why an `int` argument is 20
    /// and a `double` one 370.
    #[test]
    fn arg_zero_width_family_matches_go() {
        for name in [
            "lower",
            "upper",
            "lcase",
            "ucase",
            "reverse",
            "ltrim",
            "rtrim",
            "left",
            "right",
            "substr",
            "substring",
            "mid",
            "substring_index",
        ] {
            // Go: lower(c_int_d) / reverse(c_int_d) / left(c_int_d, c_int_d).
            assert_go(name, &[c_int_d(), c_int_d()], FieldTypeCode::VarString, 20);
            assert_go(name, &[c_char(), c_int_d()], FieldTypeCode::VarString, 20);
            assert_go(name, &[c_binary(), c_int_d()], FieldTypeCode::VarString, 20);
        }
        // `TRIM` is the same rule in its one- and three-argument forms; its
        // two-argument form is the exception, asserted separately below.
        assert_go("trim", &[c_int_d()], FieldTypeCode::VarString, 20);
        assert_go(
            "trim",
            &[c_char(), c_char(), c_int_d()],
            FieldTypeCode::VarString,
            20,
        );
        // Go: reverse over the remaining column kinds.
        assert_go("reverse", &[c_float_d()], FieldTypeCode::VarString, 87);
        assert_go("reverse", &[c_double_d()], FieldTypeCode::VarString, 370);
        assert_go("reverse", &[c_decimal()], FieldTypeCode::VarString, 9);
        assert_go("reverse", &[c_set()], FieldTypeCode::VarString, 5);
        assert_go("reverse", &[c_enum()], FieldTypeCode::VarString, 1);
    }

    /// A TEXT argument is the boundary case the expression-index refusals turn
    /// on: 65535 is ONE SHORT of `getRetTp`'s MEDIUM threshold, so Go reports a
    /// `var_string(65535)` and not a `mediumtext`. Go's own row.
    #[test]
    fn a_text_argument_stays_var_string_in_go() {
        assert_go("reverse", &[c_text_d()], FieldTypeCode::VarString, 65535);
        assert_go("lower", &[c_text_d()], FieldTypeCode::VarString, 65535);
        // MEDIUMTEXT and LONGTEXT do cross it, which is what makes
        // `index i((lower(mediumtext_col)))` a 3757 rather than an accept.
        assert_go(
            "lower",
            &[typed(FieldTypeCode::MediumBlob, 16_777_215, UNSPECIFIED)],
            FieldTypeCode::MediumBlob,
            16_777_215,
        );
        assert_go(
            "lower",
            &[typed(
                FieldTypeCode::LongBlob,
                MAX_LONG_BLOB_WIDTH,
                UNSPECIFIED,
            )],
            FieldTypeCode::LongBlob,
            MAX_LONG_BLOB_WIDTH,
        );
        // And a `varchar(0)` argument keeps a ZERO-width result, which is the
        // third refusal (3761).
        assert_go(
            "lower",
            &[typed(FieldTypeCode::Varchar, 0, UNSPECIFIED)],
            FieldTypeCode::VarString,
            0,
        );
    }

    /// `getRetTp`'s two thresholds are both `>=`, and both are exact: a result
    /// of exactly 65536 IS a mediumblob and one of exactly `MaxBlobWidth` IS a
    /// longblob. Asserted on the boundary itself because an off-by-one there
    /// is invisible in every other row -- no column width in Go's own golden
    /// table lands on either number.
    #[test]
    fn the_promotion_thresholds_are_inclusive() {
        let at = |flen: i64| {
            builtin_return_type("lower", &[typed(FieldTypeCode::Varchar, flen, UNSPECIFIED)])
                .unwrap()
                .code()
        };
        assert_eq!(at(65535), FieldTypeCode::VarString);
        assert_eq!(at(65536), FieldTypeCode::MediumBlob);
        assert_eq!(at(MAX_BLOB_WIDTH - 1), FieldTypeCode::MediumBlob);
        assert_eq!(at(MAX_BLOB_WIDTH), FieldTypeCode::LongBlob);
    }

    /// `TRIM(remstr FROM str)` is the one form in the family whose
    /// `getFunction` sets no flen at all.
    #[test]
    fn two_argument_trim_has_no_width_in_go() {
        assert_go(
            "trim",
            &[c_char(), c_char()],
            FieldTypeCode::VarString,
            UNSPECIFIED,
        );
    }

    /// The fixed-width members, each of which `getRetTp` then promotes.
    #[test]
    fn fixed_width_members_match_go() {
        assert_go(
            "space",
            &[c_int_d()],
            FieldTypeCode::LongBlob,
            MAX_BLOB_WIDTH,
        );
        assert_go(
            "repeat",
            &[c_char(), c_int_d()],
            FieldTypeCode::LongBlob,
            MAX_BLOB_WIDTH,
        );
        assert_go(
            "insert_func",
            &[c_char(), c_int_d(), c_int_d(), c_char()],
            FieldTypeCode::LongBlob,
            MAX_BLOB_WIDTH,
        );
        assert_go(
            "format",
            &[c_double_d(), c_double_d()],
            FieldTypeCode::LongBlob,
            MAX_BLOB_WIDTH,
        );
        for name in ["bin", "oct"] {
            assert_go(name, &[c_int_d()], FieldTypeCode::VarString, 64);
            assert_go(name, &[c_text_d()], FieldTypeCode::VarString, 64);
        }
        assert_go(
            "conv",
            &[c_char(), c_int_d(), c_int_d()],
            FieldTypeCode::VarString,
            64,
        );
    }

    /// `replaceFunctionClass.fixLength`. Go's own row is
    /// `replace(1234, 2, 55)` -> 20, where every literal widens to
    /// `MaxIntWidth` first and the excess term is therefore zero.
    #[test]
    fn replace_matches_go() {
        assert_go(
            "replace",
            &[
                literal(Datum::Int(1234), FieldTypeCode::LongLong, 4),
                literal(Datum::Int(2), FieldTypeCode::LongLong, 1),
                literal(Datum::Int(55), FieldTypeCode::LongLong, 2),
            ],
            FieldTypeCode::VarString,
            20,
        );
        assert_go(
            "replace",
            &[c_binary(), c_int_d(), c_int_d()],
            FieldTypeCode::VarString,
            20,
        );
        // The excess term itself: a 20-wide subject, a 2-wide needle and a
        // 5-wide replacement is 20 + (20/2)*3.
        assert_go(
            "replace",
            &[
                c_char(),
                typed(FieldTypeCode::Varchar, 2, UNSPECIFIED),
                typed(FieldTypeCode::Varchar, 5, UNSPECIFIED),
            ],
            FieldTypeCode::VarString,
            50,
        );
    }

    /// `getFlen4LpadAndRpad`: only a constant pad length is knowable, and it is
    /// multiplied by four. Go's rows.
    #[test]
    fn lpad_and_rpad_match_go() {
        let twelve = literal(Datum::Int(12), FieldTypeCode::LongLong, 2);
        let go = literal(Datum::Bytes(b"go".to_vec()), FieldTypeCode::VarString, 2);
        for name in ["lpad", "rpad"] {
            assert_go(
                name,
                &[
                    literal(Datum::Bytes(b"TiDB".to_vec()), FieldTypeCode::VarString, 4),
                    twelve.clone(),
                    go.clone(),
                ],
                FieldTypeCode::VarString,
                48,
            );
            // A NON-constant length is `mysql.MaxBlobWidth` before the times
            // four, and the clamp brings it back to `MaxBlobWidth`.
            assert_go(
                name,
                &[c_char(), c_int_d(), c_char()],
                FieldTypeCode::LongBlob,
                MAX_BLOB_WIDTH,
            );
        }
    }

    /// `eltFunctionClass`: the widest selectable value, where an argument of
    /// unknown width RESETS rather than widens.
    #[test]
    fn elt_matches_go() {
        assert_go(
            "elt",
            &[c_int_d(), c_char(), c_char(), c_char()],
            FieldTypeCode::VarString,
            20,
        );
        assert_go(
            "elt",
            &[c_int_d(), c_char(), c_int_d()],
            FieldTypeCode::VarString,
            20,
        );
        assert_go(
            "elt",
            &[c_int_d(), c_char(), c_double_d(), c_int_d()],
            FieldTypeCode::VarString,
            370,
        );
        assert_go(
            "elt",
            &[c_int_d(), c_char(), c_double_d(), c_int_d(), c_binary()],
            FieldTypeCode::VarString,
            370,
        );
        // The reset: a trailing argument of unknown width takes the result
        // back to unknown, which is Go's `flen == UnspecifiedLength ||` arm.
        assert_go(
            "elt",
            &[
                c_int_d(),
                c_char(),
                typed(FieldTypeCode::Varchar, UNSPECIFIED, UNSPECIFIED),
            ],
            FieldTypeCode::VarString,
            UNSPECIFIED,
        );
    }

    /// `exportSetFunctionClass`: sixty-four values and sixty-three separators,
    /// times four. Go's three rows, all over TEXT columns.
    #[test]
    fn export_set_matches_go() {
        assert_go(
            "export_set",
            &[c_double_d(), c_text_d(), c_text_d()],
            FieldTypeCode::MediumBlob,
            16_777_212,
        );
        assert_go(
            "export_set",
            &[c_double_d(), c_text_d(), c_text_d(), c_text_d()],
            FieldTypeCode::LongBlob,
            33_291_780,
        );
        assert_go(
            "export_set",
            &[c_double_d(), c_text_d(), c_text_d(), c_text_d(), c_int_d()],
            FieldTypeCode::LongBlob,
            33_291_780,
        );
    }

    /// `makeSetFunctionClass.getFlen`, both halves: a constant mask sizes only
    /// the members it selects, and anything else sums them all.
    #[test]
    fn make_set_matches_go() {
        assert_go(
            "make_set",
            &[c_int_d(), c_text_d()],
            FieldTypeCode::VarString,
            65535,
        );
        assert_go(
            "make_set",
            &[c_int_d(), c_text_d(), c_binary()],
            FieldTypeCode::MediumBlob,
            65556,
        );
        // Go's `make_set(1, c_text_d, 0x40)`: mask 1 selects the FIRST member
        // only, so the binary literal contributes nothing.
        assert_go(
            "make_set",
            &[
                literal(Datum::Int(1), FieldTypeCode::LongLong, 1),
                c_text_d(),
                literal(Datum::Bytes(vec![0x40]), FieldTypeCode::VarString, 3),
            ],
            FieldTypeCode::VarString,
            65535,
        );
    }

    /// `charFunctionClass`: four bytes per code point, the trailing USING
    /// charset argument excluded.
    #[test]
    fn char_matches_go() {
        let charset = literal(
            Datum::Bytes(b"binary".to_vec()),
            FieldTypeCode::VarString,
            6,
        );
        assert_go(
            "char_func",
            &[c_int_d(), charset.clone()],
            FieldTypeCode::VarString,
            4,
        );
        assert_go(
            "char_func",
            &[c_int_d(), c_int_d(), charset],
            FieldTypeCode::VarString,
            8,
        );
    }

    /// Go's `CONCAT`/`CONCAT_WS` rows over LITERALS, which only add up once a
    /// literal carries the width `types.DefaultTypeForValue` gives it. They
    /// are the check that the literal rule and the sum rule agree.
    #[test]
    fn concat_over_literals_matches_go() {
        let lit = |bytes: &[u8]| {
            literal(
                Datum::Bytes(bytes.to_vec()),
                FieldTypeCode::VarString,
                bytes.len() as i64,
            )
        };
        // Go: CONCAT('T', 'i', 'DB') -> 4.
        assert_go(
            "concat",
            &[lit(b"T"), lit(b"i"), lit(b"DB")],
            FieldTypeCode::VarString,
            4,
        );
        // Go: CONCAT_WS('-', 'T', 'i', 'DB') -> 6, the separator counted twice.
        assert_go(
            "concat_ws",
            &[lit(b"-"), lit(b"T"), lit(b"i"), lit(b"DB")],
            FieldTypeCode::VarString,
            6,
        );
        // Go: CONCAT_WS(',', 'TiDB', c_binary) -> 25.
        assert_go(
            "concat_ws",
            &[lit(b","), lit(b"TiDB"), c_binary()],
            FieldTypeCode::VarString,
            25,
        );
    }

    /// The INT half of the same file. Go fixes a width on each of these too,
    /// and it is not `MaxIntWidth` -- `LENGTH` is 10, `ASCII` 3, `STRCMP` 2.
    /// Go's own rows, from the same golden table.
    #[test]
    fn string_builtins_returning_an_int_match_go() {
        for (name, args, flen) in [
            ("bit_length", vec![c_char()], 10),
            ("ascii", vec![c_char()], 3),
            ("ord", vec![c_char()], 10),
            ("instr", vec![c_char(), c_char()], 11),
            ("strcmp", vec![c_char(), c_char()], 2),
            ("find_in_set", vec![c_int_d(), c_text_d()], 3),
            // Go's `MaxIntWidth` rows, which the ETInt default already gives.
            ("char_length", vec![c_char()], 20),
            ("character_length", vec![c_char()], 20),
            ("locate", vec![c_char(), c_char()], 20),
            ("field", vec![c_double_d(), c_text_d()], 20),
        ] {
            assert_go(name, &args, FieldTypeCode::LongLong, flen);
        }
        // `LENGTH`'s own row is not in the golden table; `lengthFunctionClass`
        // sets 10 and `OCTET_LENGTH` is the same class.
        assert_go("length", &[c_char()], FieldTypeCode::LongLong, 10);
        assert_go("octet_length", &[c_char()], FieldTypeCode::LongLong, 10);
    }

    /// `quoteFunctionClass`: `2 * flen + 2`. Go's rows.
    #[test]
    fn quote_matches_go() {
        assert_go("quote", &[c_int_d()], FieldTypeCode::VarString, 42);
        assert_go("quote", &[c_float_d()], FieldTypeCode::VarString, 176);
        assert_go("quote", &[c_double_d()], FieldTypeCode::VarString, 742);
    }

    /// `HEX` splits on the argument's eval type: a string is hexed four bytes
    /// per character at two digits each, a number is doubled from its DECLARED
    /// width. Go's `hex(c_char)` 160 and `hex(c_int_d)` 22.
    #[test]
    fn hex_matches_go() {
        assert_go("hex", &[c_char()], FieldTypeCode::VarString, 160);
        assert_go("hex", &[c_int_d()], FieldTypeCode::VarString, 22);
    }

    /// `UNHEX` is the family's one reader of the UNCAST argument type, which is
    /// why Go's `unhex(c_int_d)` is 6 and not 40.
    #[test]
    fn unhex_matches_go() {
        assert_go("unhex", &[c_int_d()], FieldTypeCode::VarString, 6);
        assert_go("unhex", &[c_char()], FieldTypeCode::VarString, 40);
    }

    /// `FROM_BASE64` triples, `TO_BASE64` grows by `base64NeededEncodedLength`.
    /// Go's rows, including the two whose triple crosses the MEDIUM boundary.
    #[test]
    fn base64_matches_go() {
        assert_go("from_base64", &[c_int_d()], FieldTypeCode::VarString, 60);
        assert_go("from_base64", &[c_float_d()], FieldTypeCode::VarString, 261);
        assert_go(
            "from_base64",
            &[c_double_d()],
            FieldTypeCode::VarString,
            1110,
        );
        assert_go("from_base64", &[c_decimal()], FieldTypeCode::VarString, 27);
        assert_go("from_base64", &[c_datetime()], FieldTypeCode::VarString, 66);
        assert_go("from_base64", &[c_char()], FieldTypeCode::VarString, 60);
        assert_go("from_base64", &[c_set()], FieldTypeCode::VarString, 15);
        assert_go("from_base64", &[c_enum()], FieldTypeCode::VarString, 3);
        assert_go(
            "from_base64",
            &[c_text_d()],
            FieldTypeCode::MediumBlob,
            196_605,
        );
        assert_go("to_base64", &[c_binary()], FieldTypeCode::VarString, 28);
    }
}

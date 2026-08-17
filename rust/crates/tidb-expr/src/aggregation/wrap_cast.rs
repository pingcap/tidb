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

//! SEED of `pkg/expression/builtin_cast.go`'s `WrapWithCastAs*` family
//! (`:2666`-`:2886`), the one piece of that file
//! [`super::BaseFuncDesc::wrap_cast_for_agg_args`], `typeInfer4BitFuncs` and
//! `typeInfer4JsonObjectAgg` cannot be written without.
//!
//! | Go symbol | line | Rust |
//! | --- | --- | --- |
//! | `WrapWithCastAsInt` | 2666 | [`wrap_with_cast_as_int`] |
//! | `WrapWithCastAsReal` | 2703 | [`wrap_with_cast_as_real`] |
//! | `minimalDecimalLenForHoldingInteger` | 2713 | `minimal_decimal_len_for_holding_integer` |
//! | `WrapWithCastAsDecimal` | 2736 | [`wrap_with_cast_as_decimal`] |
//! | `WrapWithCastAsString` | 2769 | [`wrap_with_cast_as_string`] |
//! | `WrapWithCastAsTime` | 2817 | [`wrap_with_cast_as_time`] |
//! | `WrapWithCastAsDuration` | 2853 | [`wrap_with_cast_as_duration`] |
//! | `WrapWithCastAsJSON` | 2873 | [`wrap_with_cast_as_json`] |
//! | `WrapWithCastAsVectorFloat32` | 2883 | [`wrap_with_cast_as_vector_float32`] |
//!
//! This module claims NOTHING else in `builtin_cast.go`. When that file is
//! ported as a unit these functions move there and this module becomes a
//! re-export.
//!
//! # Narrowings, each named
//!
//! - **`BuildCastFunction` is [`crate::simple_expr::build_cast_function`].**
//!   That is this crate's node builder for a cast: it picks the signature
//!   name from the TARGET type's code, which is the same discrimination Go's
//!   `castAs*` selection performs. It returns an error for a target type
//!   whose cast signature is not ported; every wrapper below propagates that
//!   error rather than silently returning the unwrapped expression, because
//!   an unwrapped argument would make the aggregate read the WRONG eval type.
//! - **`WrapWithCastAsDecimal`'s constant-refinement tail is dropped**
//!   (`:2836`-`:2845`). Go evaluates the freshly built cast node
//!   (`castExpr.EvalDecimal`) when it is `ConstStrict` and narrows the node's
//!   flen/decimal to the actual precision of the result. It needs an
//!   `EvalContext` and a decimal evaluation of a node this crate can only
//!   evaluate through the full builtin dispatch; the result differs only in
//!   the DISPLAY metadata of a constant argument, never in the value. It is
//!   dropped, not approximated.
//! - **`WrapWithCastAsString`'s `CoercibilityExplicit` branch reads the
//!   argument's FIELD TYPE charset/collation** rather than a separate
//!   `collationInfo`. Go's `expr.CharsetAndCollation()` returns the derived
//!   collation, which for an EXPLICIT-coercibility node (a `COLLATE` clause)
//!   is exactly what the node's own field type carries in this crate, since
//!   [`crate::rewriter`] writes the derived collation back into the type.
//! - **`WrapWithCastAsInt`'s ENUM flag mutation is applied to a CLONE.** Go
//!   deep-clones a `*Column`/`*CorrelatedColumn` before adding
//!   `EnumSetAsIntFlag` and mutates the shared field type in place for every
//!   other node kind. Rust takes the expression by value, so the clone is
//!   free and the in-place mutation of a shared type cannot happen at all --
//!   this is strictly safer, and observable only if a caller relied on the
//!   flag appearing on an aliased `*ScalarFunction`'s type, which no
//!   aggregate path does.

use crate::context::{Columns, EvalError};
use crate::expression::Expression;
use crate::simple_expr::build_cast_function;
use tidb_datatype::{
    EvalType, FieldType, FieldTypeCode, FieldTypeFlags, MAX_DECIMAL_WIDTH, UNSPECIFIED_LENGTH,
};

/// `mysql.MaxIntWidth`.
pub(super) const MAX_INT_WIDTH: i64 = 20;
/// `mysql.MaxRealWidth`.
pub(super) const MAX_REAL_WIDTH: i64 = 23;
/// `mysql.MaxDateWidth` (`YYYY-MM-DD`).
const MAX_DATE_WIDTH: i64 = 10;
/// `mysql.MaxDatetimeWidthNoFsp` (`YYYY-MM-DD HH:MM:SS`).
const MAX_DATETIME_WIDTH_NO_FSP: i64 = 19;
/// `mysql.MaxDurationWidthNoFsp` (`HH:MM:SS`).
const MAX_DURATION_WIDTH_NO_FSP: i64 = 10;
/// `mysql.MaxBlobWidth`.
pub(super) const MAX_BLOB_WIDTH: i64 = 16_777_216;
/// `mysql.MaxFieldCharLength`.
pub(super) const MAX_FIELD_CHAR_LENGTH: i64 = 255;
/// `mysql.NotFixedDec`.
pub(super) const NOT_FIXED_DEC: i64 = 31;
/// `types.MinFsp`.
const MIN_FSP: i64 = 0;
/// `types.MaxFsp`.
const MAX_FSP: i64 = 6;
/// `types.UnspecifiedFsp`, which shares the `-1` sentinel with
/// `types.UnspecifiedLength`.
const UNSPECIFIED_FSP: i64 = -1;
/// The flen `WrapWithCastAsJSON` hardcodes (`:2879`).
const JSON_CAST_FLEN: i64 = 12_582_912;

/// Go `types.SetBinChsClnFlag`: the `binary` pseudo-charset/collation plus the
/// binary flag, which every numeric aggregate result carries.
pub(super) fn set_bin_chs_cln_flag(field_type: &mut FieldType) {
    field_type.set_charset_name("binary");
    field_type.set_collation_name("binary");
    field_type.add_flags(FieldTypeFlags::BINARY);
}

/// The argument's static type, defaulting to the `NULL` type when the node
/// carries none (Go's nil `RetType`, which is unreachable for a built node).
pub(super) fn type_of(expr: &Expression) -> FieldType {
    expr.static_type()
        .cloned()
        .unwrap_or_else(|| FieldType::new(FieldTypeCode::Null))
}

/// Replaces the expression's own result type in place, which is how Go
/// mutates `expr.GetType(ctx).AddFlag(...)`.
fn ret_type_mut(expr: &mut Expression) -> Option<&mut FieldType> {
    match expr {
        Expression::Column(c) => c.ret_type.as_mut(),
        Expression::Constant(c) => c.ret_type.as_mut(),
        Expression::CorrelatedColumn(c) => c.column.ret_type.as_mut(),
        Expression::ScalarFunction(f) => f.ret_type.as_mut(),
    }
}

/// Go `WrapWithCastAsInt` (`builtin_cast.go:2666`).
///
/// `target_type` is Go's nullable `targetType`: `None` inherits the source's
/// UNSIGNED flag, `Some` takes the target's.
pub fn wrap_with_cast_as_int(
    mut expr: Expression,
    target_type: Option<&FieldType>,
) -> Result<Expression, EvalError> {
    if type_of(&expr).code() == FieldTypeCode::Enum {
        // Go deep-clones a column before touching the flag; taking `expr` by
        // value already gives us an unaliased node.
        if let Some(ft) = ret_type_mut(&mut expr) {
            ft.add_flags(FieldTypeFlags::ENUM_SET_AS_INT);
        }
    }
    let source = type_of(&expr);
    if source.eval_type() == EvalType::Int {
        return Ok(expr);
    }
    let mut tp = FieldType::new(FieldTypeCode::LongLong);
    tp.set_flen(source.flen());
    tp.set_decimal(0);
    set_bin_chs_cln_flag(&mut tp);
    tp.add_flags(source.flags() & FieldTypeFlags::NOT_NULL);
    match target_type {
        None => tp.add_flags(source.flags() & FieldTypeFlags::UNSIGNED),
        Some(target) => tp.add_flags(target.flags() & FieldTypeFlags::UNSIGNED),
    }
    build_cast_function(expr, tp)
}

/// Go `WrapWithCastAsReal` (`builtin_cast.go:2703`).
pub fn wrap_with_cast_as_real(expr: Expression) -> Result<Expression, EvalError> {
    let source = type_of(&expr);
    if source.eval_type() == EvalType::Real {
        return Ok(expr);
    }
    let mut tp = FieldType::new(FieldTypeCode::Double);
    tp.set_flen(MAX_REAL_WIDTH);
    tp.set_decimal(UNSPECIFIED_LENGTH);
    set_bin_chs_cln_flag(&mut tp);
    tp.add_flags(source.flags() & (FieldTypeFlags::UNSIGNED | FieldTypeFlags::NOT_NULL));
    build_cast_function(expr, tp)
}

/// Go `minimalDecimalLenForHoldingInteger` (`builtin_cast.go:2713`).
const fn minimal_decimal_len_for_holding_integer(code: FieldTypeCode) -> i64 {
    match code {
        FieldTypeCode::Tiny => 3,
        FieldTypeCode::Short => 5,
        FieldTypeCode::Int24 => 8,
        FieldTypeCode::Long => 10,
        FieldTypeCode::LongLong => 20,
        FieldTypeCode::Year => 4,
        _ => MAX_INT_WIDTH,
    }
}

/// Go `WrapWithCastAsDecimal` (`builtin_cast.go:2736`), without the
/// constant-refinement tail (see the module header).
pub fn wrap_with_cast_as_decimal(expr: Expression) -> Result<Expression, EvalError> {
    let source = type_of(&expr);
    if source.eval_type() == EvalType::Decimal {
        return Ok(expr);
    }
    let mut tp = FieldType::new(FieldTypeCode::NewDecimal);
    tp.set_flen_under_limit(source.flen());
    tp.set_decimal_under_limit(source.decimal());
    if source.eval_type() == EvalType::Int {
        tp.set_flen(minimal_decimal_len_for_holding_integer(source.code()));
        tp.set_decimal(0);
    }
    if tp.flen() == UNSPECIFIED_LENGTH || tp.flen() > MAX_DECIMAL_WIDTH {
        tp.set_flen(MAX_DECIMAL_WIDTH);
    }
    set_bin_chs_cln_flag(&mut tp);
    tp.add_flags(source.flags() & (FieldTypeFlags::UNSIGNED | FieldTypeFlags::NOT_NULL));
    build_cast_function(expr, tp)
}

/// Go `WrapWithCastAsString` (`builtin_cast.go:2769`).
///
/// `connection` is `ctx.GetCharsetInfo()`.
pub fn wrap_with_cast_as_string(
    expr: Expression,
    connection: (&str, &str),
) -> Result<Expression, EvalError> {
    let source = type_of(&expr);
    if source.eval_type() == EvalType::String {
        return Ok(expr);
    }
    let mut arg_len = source.flen();
    // A DECIMAL rendering needs room for the point, the sign and a leading
    // zero. FLOAT/DOUBLE lengths are not accurate, so they are left alone
    // here and cleared below.
    if source.code() == FieldTypeCode::NewDecimal && arg_len != UNSPECIFIED_FSP {
        arg_len += 3;
    }
    if source.eval_type() == EvalType::Int {
        arg_len = MAX_INT_WIDTH;
        // TiKV needs a BIT's real byte length while evaluating e.g. ascii(bit).
        if source.code() == FieldTypeCode::Bit {
            arg_len = (source.flen() + 7) / 8;
        }
    }
    if matches!(source.code(), FieldTypeCode::Float | FieldTypeCode::Double) {
        arg_len = -1;
    }
    let mut tp = FieldType::new(FieldTypeCode::VarString);
    if crate::collation_derive::coercibility_of(&expr)
        == crate::expr_collation::Coercibility::EXPLICIT
    {
        tp.set_charset_name(source.charset_name().to_owned());
        tp.set_collation_name(source.collation_name().to_owned());
    } else if source.code() == FieldTypeCode::Bit {
        // An implicit BIT-to-string cast produces binary.
        tp.set_charset_name("binary");
        tp.set_collation_name("binary");
    } else {
        tp.set_charset_name(connection.0.to_owned());
        tp.set_collation_name(connection.1.to_owned());
    }
    tp.set_flen(arg_len);
    tp.set_decimal(UNSPECIFIED_LENGTH);
    build_cast_function(expr, tp)
}

/// Go `WrapWithCastAsTime` (`builtin_cast.go:2817`).
///
/// Go mutates the caller's `*types.FieldType`; Rust takes it by value and
/// returns the wrapped expression, so the mutation is not observable outside.
pub fn wrap_with_cast_as_time(
    expr: Expression,
    mut tp: FieldType,
) -> Result<Expression, EvalError> {
    let source = type_of(&expr);
    let source_code = source.code();
    if tp.code() == source_code {
        return Ok(expr);
    }
    if matches!(source_code, FieldTypeCode::Date | FieldTypeCode::Timestamp)
        && tp.code() == FieldTypeCode::Datetime
    {
        return Ok(expr);
    }
    match source.eval_type() {
        EvalType::Int => tp.set_decimal(MIN_FSP),
        EvalType::String | EvalType::Real | EvalType::Json => tp.set_decimal(MAX_FSP),
        EvalType::Datetime | EvalType::Timestamp | EvalType::Duration => {
            tp.set_decimal(source.decimal());
        }
        EvalType::Decimal => {
            tp.set_decimal(source.decimal());
            if tp.decimal() > MAX_FSP {
                tp.set_decimal(MAX_FSP);
            }
        }
        EvalType::VectorFloat32 => {}
    }
    match tp.code() {
        FieldTypeCode::Date => tp.set_flen(MAX_DATE_WIDTH),
        FieldTypeCode::Datetime | FieldTypeCode::Timestamp => {
            tp.set_flen(MAX_DATETIME_WIDTH_NO_FSP);
            if tp.decimal() > 0 {
                tp.set_flen(tp.flen() + 1 + tp.decimal());
            }
        }
        _ => {}
    }
    set_bin_chs_cln_flag(&mut tp);
    build_cast_function(expr, tp)
}

/// Go `WrapWithCastAsDuration` (`builtin_cast.go:2853`).
pub fn wrap_with_cast_as_duration(expr: Expression) -> Result<Expression, EvalError> {
    let source = type_of(&expr);
    if source.code() == FieldTypeCode::Duration {
        return Ok(expr);
    }
    let mut tp = FieldType::new(FieldTypeCode::Duration);
    match source.code() {
        FieldTypeCode::Datetime | FieldTypeCode::Timestamp | FieldTypeCode::Date => {
            tp.set_decimal(source.decimal());
        }
        _ => tp.set_decimal(MAX_FSP),
    }
    tp.set_flen(MAX_DURATION_WIDTH_NO_FSP);
    if tp.decimal() > 0 {
        tp.set_flen(tp.flen() + 1 + tp.decimal());
    }
    build_cast_function(expr, tp)
}

/// Go `WrapWithCastAsJSON` (`builtin_cast.go:2873`).
pub fn wrap_with_cast_as_json(expr: Expression) -> Result<Expression, EvalError> {
    let source = type_of(&expr);
    if source.code() == FieldTypeCode::Json && source.flags() & FieldTypeFlags::PARSE_TO_JSON == 0 {
        return Ok(expr);
    }
    let mut tp = FieldType::new(FieldTypeCode::Json);
    tp.set_flags(FieldTypeFlags::BINARY);
    tp.set_flen(JSON_CAST_FLEN);
    tp.set_charset_name("utf8mb4");
    tp.set_collation_name("utf8mb4_bin");
    build_cast_function(expr, tp)
}

/// Go `WrapWithCastAsVectorFloat32` (`builtin_cast.go:2883`).
pub fn wrap_with_cast_as_vector_float32(expr: Expression) -> Result<Expression, EvalError> {
    if type_of(&expr).code() == FieldTypeCode::VectorFloat32 {
        return Ok(expr);
    }
    build_cast_function(expr, FieldType::new(FieldTypeCode::VectorFloat32))
}

/// Go `expression.BuildCastFunction(ctx, expr, tp)` for a target the caller
/// already fully described. Exposed so `typeInfer4GroupConcat` can reproduce
/// its literal `BuildCastFunction(ctx, a.Args[i], tp)` call.
pub fn build_cast_to(expr: Expression, target: FieldType) -> Result<Expression, EvalError> {
    build_cast_function(expr, target)
}

/// The connection charset/collation pair a `BuildContext` supplies, defaulted
/// through `charset.GetDefaultCharsetAndCollate()` exactly as
/// `typeInfer4GroupConcat` does when the session reports empty strings.
pub(super) fn connection_charset(ctx: &impl Columns) -> (String, String) {
    let (chs, coll) = ctx.connection_charset_info();
    if chs.is_empty() || coll.is_empty() {
        return ("utf8mb4".to_owned(), "utf8mb4_bin".to_owned());
    }
    (chs.to_owned(), coll.to_owned())
}

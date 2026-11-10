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

//! Go `pkg/expression`: scalar expressions, their builtin function classes
//! and signatures, and their evaluation.
//!
//! There is one evaluator, as in Go. A parsed [`tidb_ast::Expr`] is rewritten
//! into an [`Expression`] tree ([`rewriter`]), each function node is built by
//! its Go function class ([`builtin`]) -- which infers the result type, wraps
//! the arguments in the casts its declared eval types ask for and picks the
//! signature -- and the tree is evaluated over a row or a chunk
//! ([`scalar_function`]). [`eval_in`] is Go's `EvalSimpleAst`: that same
//! rewrite over no schema, built and folded in the evaluation context,
//! followed by one evaluation.

pub mod param_errors;
pub mod expression_errors;
pub mod explain;
pub mod wrap_cast;
mod arg_eval_type;
mod binary_literal;
mod build;
pub(crate) mod builtin;
pub mod builtin_arithmetic;
#[cfg(test)]
mod builtin_cast_semantics;
mod chunk_executor;
pub mod builtin_compare;
mod builtin_cast;
mod builtin_control;
mod builtin_encryption;
mod builtin_ext;
mod builtin_json;
mod builtin_other;
mod builtin_regexp;
mod builtin_string;
mod builtin_time;
mod builtin_vec;
mod builtin_fts;
mod builtin_grouping;
mod builtin_ilike;
mod builtin_like;
mod builtin_info;
mod builtin_math;
mod builtin_miscellaneous;
pub mod builtin_op;
pub mod builtin_registry;
pub mod cast;
pub use cast::{convert_json_2_tp, err_invalid_json_for_func_index};
mod coerce;
pub mod collation_derive;
pub mod column;
pub mod constant;
pub mod value_expr;
pub use value_expr::ValueExprDatum;
pub mod constant_fold;
pub mod constant_propagation;
mod go_flate;
pub use constant_fold::{
    derive_constant_null_flag, fold_constant_in_mode,
    fold_constant_in_mode_preserving_warning_casts, ConstantFoldMode,
};
pub mod convert_charset;
mod eval_error_sql;
pub use eval_error_sql::{EvalErrorSql, ToTerror};
pub mod evaluator;
pub mod expr_collation;
pub mod expr_util;
pub mod expression;
mod field_name;
pub mod fts;
mod func;
mod grouping;
pub mod grouping_sets;
pub mod infer_pushdown;
mod like;
mod math_fn;
pub mod memory_usage;
pub mod new_function;
pub use new_function::{
    new_function, new_function_base, new_function_impl, new_function_internal,
    new_function_try_fold, new_function_with_init, scalar_funcs_2_exprs, type_infer_for_null,
    ScalarFunctionCallBack,
};
mod ops;
pub mod distsql_builtin;
pub mod pb_predicate;
pub mod pushdown_catalog;
mod regexp;
pub mod rewriter;
mod row;
pub mod scalar_function;
pub mod scalar_subquery;
pub mod schema;
pub mod simple_expr;
mod string_fn;
pub use string_fn::quote_str;
mod string_packet;
mod string_signature;
mod time_fn;
mod time_literal;

pub use field_name::{find_field_name, find_field_name_idx_by_col_name, NonUniqueFieldName};

pub use build::{BuildContext, BuiltStringLength, StringLengthFunction, StringLengthSignature};
pub use coerce::{truthy_in, truthy_of};
pub(crate) use tidb_exprctx::{BlockEncryptionMode, Columns, ErrorLevel, EvalError, JsonError, NoColumns, SequenceEvalError};
#[cfg(test)]
pub(crate) use tidb_exprctx::ZonedNoColumns;
pub use grouping::{GroupingFunction, GroupingMetadata, GroupingMetadataError, GroupingMode};
pub use like::{ilike_match, like_match_with_collation};
pub use regexp::regexp_match_bin_collation;
pub use row::{compare_datums, compare_datums_with_collation};
pub(crate) use tidb_datatype::{Datum, Decimal};
use tidb_mathutil::MysqlRng;

use tidb_ast::Expr;


use ops::{
    effective_div_precision_increment, eval_binary, eval_binary_with_div_precision, eval_unary,
};

/// Evaluates a constant expression, or returns why it is out of scope.
pub fn eval(expr: &Expr) -> Result<Datum, EvalError> {
    eval_in(expr, &NoColumns)
}

/// Mirrors Go `expression.IsValidCurrentTimestampExpr` from
/// `pkg/expression/helper.go`.
///
/// The predicate is used while validating a temporal column's DEFAULT AST,
/// before the expression is lowered into an executable evaluator. Go accepts
/// only a `CURRENT_TIMESTAMP` function call: a bare call is valid when the
/// destination has no fractional-second precision, while an explicit first
/// integer argument is valid only when it exactly matches the destination
/// field type's decimal/FSP metadata. Additional arguments are intentionally
/// ignored here, matching Go's direct `Args[0]` read; malformed first
/// arguments simply fail the predicate.
#[must_use]
pub fn is_valid_current_timestamp_expr(
    expr: &Expr,
    field_type: Option<&tidb_datatype::FieldType>,
) -> bool {
    let Expr::Func { name, args, .. } = expr else {
        return false;
    };
    if !name.eq_ignore_ascii_case("CURRENT_TIMESTAMP") {
        return false;
    }

    match args.first() {
        None => field_type.is_none_or(|field_type| field_type.decimal() <= 0),
        Some(Expr::Int(digits)) => {
            let Some(field_type) = field_type else {
                return false;
            };
            let Ok(fsp) = digits.parse::<i64>() else {
                return false;
            };
            fsp == field_type.decimal()
        }
        Some(_) => false,
    }
}

/// The AST/value boundary for Go `expression.GetTimeValue`.
///
/// Go's helper is used while constructing temporal defaults, so it accepts
/// both raw sentinel strings (`CURRENT_TIMESTAMP`/`CURRENT_DATE`) and parser
/// value expressions. Rust represents the latter with [`Expr`]:
/// `String`/`Int`/`Null` stand in for `driver.ValueExpr`, `RawString` is the
/// untyped Go `string` case, `Func` is an AST function call, and `Unary` is
/// the small arithmetic form the source helper evaluates before parsing.
/// Unknown AST forms preserve Go's zero-value datum (`NULL`) rather than
/// pretending to evaluate a wider build-context surface.
pub fn get_time_value(
    cols: &dyn Columns,
    expr: &Expr,
    kind: tidb_datatype::TimeType,
    fsp: i64,
    explicit_timezone: Option<&tidb_datatype::SessionTimeZone>,
) -> Result<Datum, EvalError> {
    let parse_zone = explicit_timezone
        .cloned()
        .unwrap_or_else(|| cols.time_zone());
    let modes = cols.date_modes();

    let parse_text = |text: &str| {
        tidb_datatype::parse_time(
            text,
            kind,
            fsp,
            false,
            !modes.no_zero_in_date,
            modes.allow_invalid_dates,
            &parse_zone,
        )
        .map(|parsed| parsed.time)
        .map_err(|error| EvalError::TruncatedWrongValue(error.to_string()))
    };
    let parse_number = |number: i64| {
        tidb_datatype::parse_time_from_num(
            number,
            kind,
            fsp,
            !modes.no_zero_in_date,
            modes.allow_invalid_dates,
            number == 0 || !modes.no_zero_date,
            &parse_zone,
        )
        .map(|parsed| parsed.time)
        .map_err(|error| EvalError::TruncatedWrongValue(error.to_string()))
    };

    let time = match expr {
        // `GetTimeValue(ctx, string, ...)`: the two clock sentinels are
        // interpreted before ordinary text parsing.
        Expr::RawString(text) if text.eq_ignore_ascii_case("CURRENT_TIMESTAMP") => {
            current_time_value(cols, kind, fsp)?
        }
        Expr::RawString(text) if text.eq_ignore_ascii_case("CURRENT_DATE") => {
            current_date_value(cols, kind, fsp)?
        }
        Expr::RawString(text) if text == "0000-00-00 00:00:00" => {
            // Go logs (rather than returns) the zero-date parse error here;
            // the value remains the parser's zero temporal value.
            tidb_datatype::parse_time_from_num(
                0,
                kind,
                fsp,
                !modes.no_zero_in_date,
                modes.allow_invalid_dates,
                true,
                &parse_zone,
            )
            .map(|parsed| parsed.time)
            .map_err(|error| EvalError::TruncatedWrongValue(error.to_string()))?
        }
        Expr::RawString(text) => parse_text(text)?,

        // `*driver.ValueExpr` source cases.
        Expr::String(text) => parse_text(text)?,
        Expr::Int(digits) => {
            let number = digits.parse::<i64>().map_err(|_| EvalError::IntOverflow)?;
            parse_number(number)?
        }
        Expr::Null => return Ok(Datum::Null),

        // `*ast.FuncCallExpr` returns a string marker, not a parsed temporal
        // value; this is what DEFAULT-expression construction stores.
        Expr::Func { name, .. }
            if name.eq_ignore_ascii_case("CURRENT_TIMESTAMP")
                || name.eq_ignore_ascii_case("CURRENT_DATE") =>
        {
            return Ok(Datum::new_string_datum(name.to_ascii_uppercase()));
        }
        Expr::Func { .. } => {
            return Err(EvalError::Unsupported("default value expression"));
        }

        // `*ast.UnaryOperationExpr`: evaluate the simple expression and then
        // feed its signed integer representation to ParseTimeFromNum.
        Expr::Unary(_, _) => {
            let value = eval_in(expr, cols)?;
            parse_number(crate::cast::to_i64_signed(&value))?
        }

        // Go's type switch returns the zero datum for every other `any` value.
        _ => return Ok(Datum::Null),
    };
    Ok(Datum::new_time(time))
}

fn current_time_value(
    cols: &dyn Columns,
    kind: tidb_datatype::TimeType,
    fsp: i64,
) -> Result<tidb_datatype::Time, EvalError> {
    use chrono::{Datelike, Timelike};

    let normalized_fsp = tidb_datatype::check_fsp(fsp)
        .map_err(|error| EvalError::TruncatedWrongValue(error.to_string()))?;
    let (seconds, nanos, _) = cols.now().ok_or(EvalError::Unsupported(
        "no statement clock for GetTimeValue",
    ))?;
    let instant = chrono::DateTime::<chrono::Utc>::from_timestamp(seconds, nanos)
        .ok_or(EvalError::Unsupported("statement clock is out of range"))?
        .with_timezone(&cols.time_zone());
    let quantum = 10_u32.pow((9 - normalized_fsp) as u32);
    let nanos = (instant.nanosecond() / quantum) * quantum;
    tidb_datatype::Time::from_date_checked(
        instant.year(),
        instant.month() as i32,
        instant.day() as i32,
        instant.hour() as i32,
        instant.minute() as i32,
        instant.second() as i32,
        (nanos / 1_000) as i32,
        kind,
        normalized_fsp,
    )
    .map_err(|error| EvalError::TruncatedWrongValue(error.to_string()))
}

fn current_date_value(
    cols: &dyn Columns,
    kind: tidb_datatype::TimeType,
    fsp: i64,
) -> Result<tidb_datatype::Time, EvalError> {
    let current = current_time_value(cols, kind, fsp)?;
    let core = current.core_time();
    tidb_datatype::Time::from_date_checked(
        core.year(),
        core.month() as i32,
        core.day() as i32,
        0,
        0,
        0,
        0,
        kind,
        fsp,
    )
    .map_err(|error| EvalError::TruncatedWrongValue(error.to_string()))
}

/// Evaluates one already-built expression against the caller's statement
/// context and the single virtual row used for a column-free expression.
///
/// DDL constant folding lives in crates that should not depend directly on
/// `tidb-chunk`; keeping the virtual-row detail here also ensures every such
/// fold uses the same row shape instead of inventing a second evaluator.
pub fn eval_expression_once(
    expression: &expression::Expression,
    ctx: &dyn Columns,
) -> Result<Datum, EvalError> {
    let mut dual = tidb_chunk::chunk::Chunk::new_empty_chunk(&[]);
    dual.set_num_virtual_rows(1);
    expression.eval(ctx, dual.get_row(0))
}

/// Applies a binary operator to already-evaluated operands. Exposed so callers
/// that intercept some sub-expressions (e.g. aggregates during grouping) can
/// still reuse the operator semantics.
pub fn apply_binary(op: tidb_ast::BinaryOp, l: Datum, r: Datum) -> Result<Datum, EvalError> {
    eval_binary(op, l, r)
}

/// Moves a temporal value by `INTERVAL amount unit`, `sign` being `1` to add
/// and `-1` to subtract -- `DATE_ADD`/`DATE_SUB`'s own calendar arithmetic
/// applied to already-evaluated operands.
///
/// Exposed for the window executor's `RANGE BETWEEN INTERVAL n unit ...`
/// frame, whose boundary is the current row's `ORDER BY` key moved by the
/// interval; it must be the SAME arithmetic `DATE_ADD` performs, month-end
/// clamping and out-of-range `NULL` included.
pub fn date_add_interval(
    unit: &str,
    date: &Datum,
    amount: &Datum,
    sign: i64,
) -> Result<Datum, EvalError> {
    time_fn::calendar::date_add(unit, date, amount, sign)
}

/// Applies TiDB's byte-preserving `CONCAT` coercion to already-evaluated
/// values without round-tripping them through literal AST nodes.
pub fn concat_values(values: &[Datum]) -> Result<Datum, EvalError> {
    string_fn::concat(values)
}

/// Applies a binary operator with the current session's explicit
/// `div_precision_increment`. Every table-backed scalar, grouped, and window
/// division path calls this rather than relying on [`apply_binary`]'s
/// context-free default.
pub fn apply_binary_with_div_precision(
    op: tidb_ast::BinaryOp,
    l: Datum,
    r: Datum,
    div_precision_increment: u32,
    ctx: &dyn tidb_exprctx::context::Columns,
) -> Result<Datum, EvalError> {
    eval_binary_with_div_precision(op, l, r, div_precision_increment, ctx)
}

/// Applies a unary operator to an already-evaluated operand.
pub fn apply_unary(
    op: tidb_ast::UnaryOp,
    v: Datum,
    ctx: &dyn tidb_exprctx::context::Columns,
) -> Result<Datum, EvalError> {
    eval_unary(op, v, ops::Operand::Literal, ctx)
}

/// `AVG`'s `SUM / COUNT`, exposed so `tidb-exec` can compute it without
/// reimplementing decimal division: an `Int` sum promotes to decimal (scale
/// 0, MySQL's implicit rule, same as every other decimal op); the result
/// scale grows by MySQL's `div_precision_increment` past the sum's own scale,
/// and is ROUNDED to that scale (ties away from zero) via true division — unlike `DIV`/`MOD`,
/// which truncate exactly and need no such growth. A `Float` sum instead
/// divides via plain native `f64` division — MySQL's `div_precision_increment`
/// scale growth is a `DECIMAL`-specific rule that doesn't apply to `AVG`
/// over a real `FLOAT`/`DOUBLE` column (confirmed via `gorun`: `AVG` there
/// is exactly `sum / count`, not assumed to match the `Decimal` rule).
/// `count` must be positive (an empty group is the caller's job to turn
/// into `NULL` before calling this, same as `SUM`).
pub fn avg_of(sum: Datum, count: i64) -> Result<Datum, EvalError> {
    avg_of_with_div_precision(sum, count, 4)
}

/// The session-aware form of [`avg_of`]. `AVG` uses the same
/// `div_precision_increment` as scalar `/`, so callers with a SQL session
/// must pass its current value explicitly.
pub fn avg_of_with_div_precision(
    sum: Datum,
    count: i64,
    div_precision_increment: u32,
) -> Result<Datum, EvalError> {
    let d = match sum {
        Datum::Real(f) => return Ok(Datum::Real(f / count as f64)),
        Datum::Float32(f) => return Ok(Datum::Float32(f / count as f64)),
        Datum::Decimal(d) => d,
        Datum::Int(i) => Decimal::from_int(i),
        Datum::UInt(i) => Decimal::from_uint(i),
        Datum::String(_) | Datum::Bytes(_) | Datum::Null | Datum::MinNotNull | Datum::MaxValue => {
            return Err(EvalError::Unsupported("AVG of non-numeric"));
        }
        other => {
            other
                .to_decimal()
                .map_err(|_| EvalError::Unsupported("AVG of non-numeric"))?
                .value
        }
    };
    let target_scale = d.scale() + effective_div_precision_increment(div_precision_increment);
    Ok(Datum::Decimal(d.div_round(count, target_scale)))
}

/// Fits a value into a `DECIMAL(precision, scale)` column for storage:
/// rounds a numeric value to `scale` and range-checks its integer part
/// (see [`Decimal::fit_precision_scale`]). Returns the rounded value, or
/// `None` when the integer part overflows (the caller turns that into a
/// column-out-of-range error). `NULL` and any non-numeric value pass
/// through unchanged — coercing those is outside this width-check's scope.
/// Used by `tidb_exec`'s `INSERT`/`UPDATE` column-width validation.
pub fn fit_decimal_column(value: Datum, precision: u32, scale: u32) -> Option<Datum> {
    match value {
        Datum::Decimal(d) => d.fit_precision_scale(precision, scale).map(Datum::Decimal),
        Datum::Int(i) => Decimal::from_int(i)
            .fit_precision_scale(precision, scale)
            .map(Datum::Decimal),
        Datum::UInt(i) => Decimal::from_uint(i)
            .fit_precision_scale(precision, scale)
            .map(Datum::Decimal),
        other => Some(other),
    }
}

/// Evaluates an expression, resolving column references via `cols`.
pub fn eval_in(expr: &Expr, cols: &dyn Columns) -> Result<Datum, EvalError> {
    let built = rewriter::rewrite_expr_resolved(expr, &rewriter::ContextResolver(cols))?;
    eval_expression_once(&built, cols)
}

#[cfg(test)]
mod tests;

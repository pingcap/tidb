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

//! `pkg/expression/builtin_compare.go`: the comparison, COALESCE, GREATEST,
//! LEAST and INTERVAL function classes.
//!
//! [`CompareFunctionClass`] is Go `compareFunctionClass`: `refineArgs`, then
//! `GetAccurateCmpType` over the refined arguments, then `generateCmpSigs`.

use crate::builtin_arithmetic::new_return_field_type_for_base_builtin_func;
use crate::builtin_ext::GlCmpStringMode;
use crate::constant::Constant;
use tidb_exprctx::context::{Columns, EvalError};
use crate::expression::Expression;
use tidb_datatype::{Datum, EvalType, FieldType, FieldTypeCode, FieldTypeFlags};

/// The result `FieldType` Go's `compareFunctionClass.getFunction` derives, for
/// the comparison scalar-function `name` (`eq`/`nulleq`/`ne`/`lt`/`le`/`gt`/
/// `ge`). Returns `None` for any other function name.
#[must_use]
pub fn infer_compare_type(name: &str) -> Option<FieldType> {
    match name {
        "eq" | "nulleq" | "ne" | "lt" | "le" | "gt" | "ge" => {
            let mut ret = new_return_field_type_for_base_builtin_func(EvalType::Int);
            // generateCmpSigs: bf.tp.SetFlen(1).
            ret.set_flen(1);
            // All comparison names are in Go's booleanFunctions map.
            ret.add_flags(FieldTypeFlags::IS_BOOLEAN);
            Some(ret)
        }
        _ => None,
    }
}

/// The comparison operators `refineArgs` runs for, and the operator each
/// becomes when the constant is on the LEFT (Go `symmetricOp`,
/// `pkg/expression/builtin_compare.go`).
fn symmetric_op(name: &str) -> Option<&'static str> {
    Some(match name {
        "lt" => "gt",
        "gt" => "lt",
        "le" => "ge",
        "ge" => "le",
        "eq" => "eq",
        "ne" => "ne",
        "nulleq" => "nulleq",
        _ => return None,
    })
}

/// Go `types.StrToFloat`'s truncation diagnostic, raised through the
/// statement's own `HandleTruncate` policy.
///
/// `Err` is the strict-mode outcome, and it is NOT propagated as a statement
/// error by the caller: Go's `RefineComparedConstant` answers `return con,
/// false` for every conversion error (`builtin_compare.go:1596`), leaving the
/// comparison unrefined. That is what the `?`-free `is_err()` checks below
/// reproduce.
fn note_string_truncation(ctx: &dyn Columns, datum: &Datum) -> Result<(), ()> {
    // Go `Datum.Compare` reaches the string arm for `KindString`/`KindBytes`
    // only.
    let bytes = match datum {
        Datum::String(value) => value.bytes(),
        Datum::Bytes(value) => value.as_slice(),
        _ => {
            // Go raises this only through the float-prefix scan, which
            // only a string source reaches. A DECIMAL or REAL constant
            // converts to int without it -- captured: `a > 1.5` and
            // `a > 1e100` warn zero times.
            return Ok(());
        }
    };
    let text = String::from_utf8_lossy(bytes);
    let text = tidb_datatype::float_warning_input(&text);
    if tidb_datatype::str_to_float(text, false).event.is_none() {
        return Ok(());
    }
    crate::expression_errors::handle_truncate(ctx, &format!("Truncated incorrect DOUBLE value: '{text}'"))
        .map_err(|_| ())
}

/// Go compares the converted datum with the original through Datum.Compare.
/// Its NULL and decimal ordering must not be replaced by a float conversion.
/// String operands additionally report the comparison's truncation warning.
///
/// This is where the reported statement's second warning comes from, NOT
/// from the `Floor` fold: for `'10ab'` the converted int (10) and the string
/// read as a double (10.0) are EQUAL, so `RefineComparedConstant` returns at
/// `builtin_compare.go:1602` and never reaches the fold at all. The fold is
/// reached only when they differ -- `a > '3.5'` becomes `gt(a, 3)` -- and it
/// contributes a third warning only when the string also truncates
/// (captured: `a > '3.5abc'` warns three times, `a > '3.5'` zero).
fn compare_int_with_constant(
    ctx: &dyn Columns,
    int_datum: &Datum,
    original: &Datum,
) -> Result<std::cmp::Ordering, ()> {
    note_string_truncation(ctx, original)?;
    int_datum
        .compare(original, tidb_charset::Collation::Binary)
        .map_err(|_| ())
}

/// Go `RefineComparedConstant` (`pkg/expression/builtin_compare.go:1574`):
/// the non-int `con` re-expressed in the int column's own type, so the
/// comparison that survives is int-to-int.
///
/// `None` leaves the comparison unchanged. A returned constant carries Go's
/// `isExceptional` bit, which the caller combines with the column nullability.
fn refine_compared_constant(
    ctx: &dyn Columns,
    target: &FieldType,
    con: &Constant,
    op: &str,
) -> Option<(Constant, bool)> {
    let value = con.eval_in(ctx).ok()?;
    let target = if target.code() == FieldTypeCode::Bit {
        &FieldType::new(FieldTypeCode::LongLong)
    } else {
        target
    };
    // Go disables negative-to-unsigned wrapping only for the initial probe.
    let (integer, exceptional) = convert_refinement_constant(
        ctx,
        target,
        con,
        &value,
        ctx.type_flags().with_allow_negative_to_unsigned(false),
    )?;
    if exceptional {
        return Some((integer, true));
    }
    if compare_int_with_constant(ctx, &integer.value, &con.value).ok()? == std::cmp::Ordering::Equal
    {
        return Some((integer, false));
    }
    match op {
        "lt" | "ge" | "le" | "gt" => {
            // Go builds and folds the typed CEIL/FLOOR signature. Decimal
            // inputs must retain their precision, including values above 2^53.
            let name = if matches!(op, "lt" | "ge") {
                "ceil"
            } else {
                "floor"
            };
            let Expression::Constant(folded) = crate::new_function::new_function(
                ctx,
                name,
                FieldType::new(FieldTypeCode::Unspecified),
                vec![Expression::Constant(con.clone())],
            )
            .ok()?
            else {
                return None;
            };
            // tryToConvertConstantInt returns an existing ETInt unchanged,
            // even when it lies outside the narrower column's integer range.
            if folded.get_type(ctx).ok()?.eval_type() == EvalType::Int {
                return Some((folded, false));
            }
            let value = folded.eval_in(ctx).ok()?;
            convert_refinement_constant(ctx, target, &folded, &value, ctx.type_flags())
        }
        "eq" | "nulleq" => {
            match con.get_type(ctx).ok()?.eval_type() {
                EvalType::Real | EvalType::Decimal => return Some((con.clone(), true)),
                EvalType::String => {}
                _ => return None,
            }
            let warnings = crate::constant::ConversionWarnings(ctx);
            let zone = ctx.time_zone();
            let context = tidb_datatype::ConversionContext::new(
                ctx.type_flags(),
                tidb_datatype::ConversionLocation::from_time_zone(&zone),
                &warnings,
            );
            let converted = value
                .convert_to_in_context(&FieldType::new(FieldTypeCode::Double), &context, &zone)
                .ok()?;
            if converted.error.is_some() {
                return None;
            }
            let Datum::Real(value) = converted.value else {
                return None;
            };
            if value != value.trunc() {
                Some((con.clone(), true))
            } else {
                Some((integer, false))
            }
        }
        _ => None,
    }
}

/// Shared Datum.ConvertTo and Constant construction from Go's refinement
/// helpers. Overflow preserves the saturated boundary but drops SubqueryRefID;
/// successful conversion retains it. Other errors leave the caller unchanged.
fn convert_refinement_constant(
    ctx: &dyn Columns,
    target: &FieldType,
    con: &Constant,
    value: &Datum,
    flags: tidb_datatype::ConversionFlags,
) -> Option<(Constant, bool)> {
    let warnings = crate::constant::ConversionWarnings(ctx);
    let zone = ctx.time_zone();
    let context = tidb_datatype::ConversionContext::new(
        flags,
        tidb_datatype::ConversionLocation::from_time_zone(&zone),
        &warnings,
    );
    let converted = value.convert_to_in_context(target, &context, &zone).ok()?;
    let exceptional = if let Some(error) = converted.error {
        if !tidb_datatype::types_errors::ERR_OVERFLOW.equal(Some(&error)) {
            return None;
        }
        true
    } else {
        false
    };
    let mut refined = Constant::new(converted.value, target.clone());
    refined.deferred_expr = con.deferred_expr.clone();
    refined.param_marker = con.param_marker;
    if !exceptional {
        refined.subquery_ref_id = con.subquery_ref_id;
    }
    Some((refined, exceptional))
}

/// Go `compareFunctionClass.refineNumericConstantCmpDatetime`
/// (`builtin_compare.go:1876`, guarded by `matchRefineRule3Pattern` at the
/// `refineArgs` call site, `:1802-1808`): `datetime/timestamp non-constant
/// [cmp] numeric constant` (or its mirror). The numeric constant is converted
/// to a `DATETIME`; on success it is REWRITTEN to that datetime constant, so
/// the comparison that survives is datetime-to-datetime. On any conversion
/// error or a NULL result the constant is left exactly as written -- Go's
/// `return args` -- and the pair then compares as REAL (`getBaseCmpType`'s
/// ETReal for datetime-vs-number), which the value evaluator reaches through
/// its `numeric_context_value` promotion.
///
/// Only `mysql.TypeTimestamp`/`mysql.TypeDatetime` match Go's
/// `matchRefineRule3Pattern`; a `DATE` or `YEAR` non-constant does not, and
/// compares as real (DATE) or datetime-via-YEAR-cast (YEAR, a separate rule)
/// respectively. Returns whether rule 3 claimed this comparison.
fn refine_numeric_constant_cmp_datetime(
    left: &mut Expression,
    right: &mut Expression,
    ctx: &dyn Columns,
) -> bool {
    // The datetime side must be a non-constant DATETIME/TIMESTAMP expression;
    // the other side a constant whose eval type is Int/Real/Decimal.
    let is_datetime_expr = |expr: &Expression| {
        expr.static_type().is_some_and(|ft| {
            matches!(
                ft.code(),
                FieldTypeCode::Datetime | FieldTypeCode::Timestamp
            )
        }) && !matches!(expr, Expression::Constant(_))
    };
    let numeric_const = |expr: &Expression| {
        matches!(expr, Expression::Constant(_))
            && matches!(
                expr.static_type().map(FieldType::eval_type),
                Some(EvalType::Int | EvalType::Real | EvalType::Decimal)
            )
    };

    let constant = if is_datetime_expr(left) && numeric_const(right) {
        right
    } else if is_datetime_expr(right) && numeric_const(left) {
        left
    } else {
        return false;
    };
    let Expression::Constant(con) = constant else {
        return false;
    };
    let Ok(value) = con.eval_in(ctx) else {
        return true;
    };
    if value.is_null() {
        return true;
    }
    // Conversion failure leaves the numeric constant unchanged, as Go does.
    // Date modes and location come from this statement, even for an unfolded
    // temporal function with no column dependencies.
    let target = FieldType::new(FieldTypeCode::Datetime);
    let warnings = crate::constant::ConversionWarnings(ctx);
    let zone = ctx.time_zone();
    let context = tidb_datatype::ConversionContext::new(
        ctx.type_flags(),
        tidb_datatype::ConversionLocation::from_time_zone(&zone),
        &warnings,
    );
    if let Ok(converted) = value.convert_to_in_context(&target, &context, &zone) {
        if converted.error.is_none() && !converted.value.is_null() {
            *constant = Expression::Constant(Constant::new(converted.value, target));
        }
    }
    true
}

/// Applies the comparison refinement that does not require a statement
/// context while an AST binary expression is being built.
///
/// Go's `compareFunctionClass.getFunction` runs `refineArgs` before
/// `generateCmpSigs`, but the context-free Rust rewriter cannot call the full
/// refinement pass: temporal conversion needs the session timezone and string
/// conversion may need to report warnings.  The integer/non-integer constant
/// rule is independent of both.  Running that rule here preserves Go's
/// rounded integer constants (for example `a < '1.0'` -> `a < 1`) before
/// comparison signature casts are selected.
pub(crate) fn refine_integer_comparison_for_rewrite(name: &str, arguments: &mut [Expression]) {
    refine_integer_comparison_for_rewrite_with_context(name, arguments, &tidb_exprctx::context::NoColumns);
}

/// The same context-free integer comparison rewrite, but with the live
/// statement context Go's `compareFunctionClass.getFunction` owns. This keeps
/// the refined integer constant while retaining the two 1292 diagnostics
/// raised by `RefineComparedConstant` during construction.
pub(crate) fn refine_integer_comparison_for_rewrite_with_context(
    name: &str,
    arguments: &mut [Expression],
    ctx: &dyn Columns,
) {
    let Some(mirrored) = symmetric_op(name) else {
        return;
    };
    let [left, right] = arguments else {
        return;
    };
    let eval_type = |expression: &Expression| expression.static_type().map(FieldType::eval_type);
    let left_is_int = eval_type(left) == Some(EvalType::Int);
    let right_is_int = eval_type(right) == Some(EvalType::Int);
    refine_int_operand_constant(left, right, left_is_int, right_is_int, name, mirrored, ctx);
}

/// Go expressionRewriter.inToExpression refines integer IN candidates before
/// deciding whether every equality can share a single comparison signature.
pub(crate) fn refine_integer_in_arguments(
    args: &mut [Expression],
    ctx: &dyn Columns,
) -> Result<(), EvalError> {
    let Some((left, candidates)) = args.split_first_mut() else {
        return Ok(());
    };
    let Some(target) = left.static_type() else {
        return Ok(());
    };
    if target.eval_type() != EvalType::Int {
        return Ok(());
    }
    for candidate in candidates {
        let Expression::Constant(constant) = candidate else {
            continue;
        };
        if ctx.use_plan_cache()
            && (constant.param_marker.is_some() || constant.deferred_expr.is_some())
        {
            if constant.get_type(ctx)?.eval_type() == EvalType::Int {
                continue;
            }
            skip_plan_cache_for_comparison(ctx, constant, "INT");
            crate::expr_util::remove_mutable_const(std::slice::from_mut(candidate), ctx)?;
        }
        let Expression::Constant(constant) = candidate else {
            unreachable!()
        };
        if let Some((refined, false)) = refine_compared_constant(ctx, target, constant, "eq") {
            *constant = refined;
        }
    }
    Ok(())
}

// Go `allowCmpArgsRefining4PlanCache`'s skip: `ctx.SetSkipPlanCache(
// fmt.Sprintf("'%v' may be converted to %v", con.StringWithCtx(...), tp))`.
fn skip_plan_cache_for_comparison(ctx: &dyn Columns, constant: &Constant, target: &str) {
    let text = crate::explain::physical_expression_text_with_columns(
        ctx,
        &Expression::Constant(constant.clone()),
        &[],
        crate::explain::ExpressionTextStyle::StringWithCtx,
    )
    .unwrap_or_default();
    ctx.set_skip_plan_cache(&format!("'{text}' may be converted to {target}"));
}

// Go allowCmpArgsRefining4PlanCache: only the three value-dependent
// conversions may refine a mutable operand, and they must disable caching.
fn allow_comparison_refinement(ctx: &dyn Columns, args: &[Expression]) -> Result<bool, EvalError> {
    if !crate::expr_util::maybe_over_optimized_4_plan_cache(ctx.use_plan_cache(), args) {
        return Ok(true);
    }
    for (index, argument) in args.iter().enumerate() {
        let Expression::Constant(constant) = argument else {
            continue;
        };
        let other = &args[1 - index];
        let other_type = match other {
            Expression::Constant(value) => Some(value.get_type(ctx)?),
            _ => other.static_type().map(std::borrow::Cow::Borrowed),
        };
        let Some(other_type) = other_type else {
            continue;
        };
        let constant_type = constant.get_type(ctx)?.eval_type();
        let target = if other_type.code() == FieldTypeCode::Year
            || (other_type.eval_type() == EvalType::Int
                && matches!(
                    constant_type,
                    EvalType::String | EvalType::Real | EvalType::Decimal
                )) {
            "INT"
        } else if !matches!(other, Expression::Constant(_))
            && matches!(
                other_type.code(),
                FieldTypeCode::Datetime | FieldTypeCode::Timestamp
            )
            && matches!(
                constant_type,
                EvalType::Int | EvalType::Real | EvalType::Decimal
            )
        {
            "datetime"
        } else {
            continue;
        };
        skip_plan_cache_for_comparison(ctx, constant, target);
        return Ok(true);
    }
    Ok(false)
}

/// Go `handleDurationTypeComparisonForNullEq`: when a plain non-NULL
/// constant cannot be cast to the other operand's `TIME` domain, preserve
/// MySQL's `NULL <=> non-NULL` result by replacing the comparison with
/// `0 <=> 1`. The cast is deliberately performed once while the expression
/// is built, so its truncation warning is not repeated for every input row.
fn rewrite_invalid_duration_null_eq(
    left: &mut Expression,
    right: &mut Expression,
    ctx: &dyn Columns,
) -> Result<bool, EvalError> {
    let duration_nonconstant = |expression: &Expression| {
        !matches!(expression, Expression::Constant(_))
            && expression.static_type().map(FieldType::code) == Some(FieldTypeCode::Duration)
    };
    let invalid = match (&*left, &*right) {
        (Expression::Constant(constant), other)
            if constant.deferred_expr.is_none()
                && constant.param_marker.is_none()
                && !matches!(constant.value, Datum::Null)
                && duration_nonconstant(other) =>
        {
            matches!(
                crate::cast::cast_arg_as_duration(
                    &constant.value,
                    constant.get_static_type(),
                    ctx,
                )?,
                Datum::Null
            )
        }
        (other, Expression::Constant(constant))
            if constant.deferred_expr.is_none()
                && constant.param_marker.is_none()
                && !matches!(constant.value, Datum::Null)
                && duration_nonconstant(other) =>
        {
            matches!(
                crate::cast::cast_arg_as_duration(
                    &constant.value,
                    constant.get_static_type(),
                    ctx,
                )?,
                Datum::Null
            )
        }
        _ => false,
    };
    if invalid {
        *left = Expression::Constant(Constant::new_zero());
        *right = Expression::Constant(Constant::new_one());
    }
    Ok(invalid)
}

/// `refineArgs`' own body, over the two arguments of one comparison. `name` is
/// the operator, `mirrored` its `symmetricOp`.
fn refine_args(
    left: &mut Expression,
    right: &mut Expression,
    name: &str,
    mirrored: &str,
    ctx: &dyn Columns,
) {
    let op = mirrored;
    // Rule 3 first, matching `refineArgs`' order: a datetime/timestamp
    // non-constant expression compared with a numeric constant that converts
    // to a datetime. Go returns immediately once this fires, so the int arm
    // below never also runs on the same comparison.
    if refine_numeric_constant_cmp_datetime(left, right, ctx) {
        return;
    }

    // Go reads BOTH argument types once, at the top of `refineArgs`
    // (`:1779-1783`), and every later arm tests those captured values -- not
    // the type of whatever constant an earlier arm may have substituted. That
    // ordering is what keeps the two rules below disjoint; see
    // [`adjust_year_int_constant`].
    let eval_type = |e: &Expression| e.static_type().map(FieldType::eval_type);
    let left_is_int = eval_type(left) == Some(EvalType::Int);
    let right_is_int = eval_type(right) == Some(EvalType::Int);
    let is_year =
        |e: &Expression| e.static_type().map(FieldType::code) == Some(FieldTypeCode::Year);
    let (left_is_year, right_is_year) = (is_year(left), is_year(right));

    refine_int_operand_constant(left, right, left_is_int, right_is_int, name, op, ctx);
    adjust_year_int_constant(
        left,
        right,
        (left_is_int, right_is_int),
        (left_is_year, right_is_year),
    );
    refine_unsigned_arguments(left, right, name, mirrored, ctx);
}

/// Go compareFunctionClass.refineArgsByUnsignedFlag. The signed EvalInt
/// carrier is deliberate: large unsigned constants have a negative carrier.
fn refine_unsigned_arguments(
    left: &mut Expression,
    right: &mut Expression,
    name: &str,
    mirrored: &str,
    ctx: &dyn Columns,
) {
    if [left.static_type(), right.static_type()]
        .iter()
        .any(|field| field.is_none_or(|field| field.eval_type() != EvalType::Int))
    {
        return;
    }
    fn column_of(expression: &Expression) -> Option<&FieldType> {
        match expression {
            Expression::Column(column) => column.ret_type.as_ref(),
            Expression::CorrelatedColumn(column) => column.column.ret_type.as_ref(),
            _ => None,
        }
    }
    let (column, constant, column_first, operator) = match (&*left, &*right) {
        (column, Expression::Constant(constant)) => (column_of(column), constant, true, name),
        (Expression::Constant(constant), column) => (column_of(column), constant, false, mirrored),
        _ => return,
    };
    let (Some(column), Some(constant_type)) = (column, constant.ret_type.as_ref()) else {
        return;
    };
    let mut empty = tidb_chunk::chunk::Chunk::new_empty_chunk(&[]);
    empty.set_num_virtual_rows(1);
    let Ok(Datum::Int(value)) = constant.eval_typed_as_on_row(ctx, empty.get_row(0), EvalType::Int)
    else {
        return;
    };
    if value > 0 {
        return;
    }
    let not_null = column.flags() & FieldTypeFlags::NOT_NULL != 0;
    let replace = if constant_type.is_unsigned() && !column.is_unsigned() {
        ((operator == "eq" && not_null) || operator == "nulleq")
            && value as u64 > tidb_datatype::integer_signed_upper_bound(column.code()) as u64
    } else if column.is_unsigned() && not_null && !constant_type.is_unsigned() {
        !(value == 0 && matches!(operator, "le" | "gt" | "nulleq" | "eq" | "ne"))
    } else {
        false
    };
    if replace {
        let one = Expression::Constant(Constant::new_one());
        let zero = Expression::Constant(Constant::new_zero());
        (*left, *right) = if column_first {
            (one, zero)
        } else {
            (zero, one)
        };
    }
}

/// `int non-constant [cmp] non-int constant` (`builtin_compare.go:1811-1833`)
/// and its mirror through `symmetricOp` (`:1836-1854`). The constant side
/// keeps the operator seen from the COLUMN's side, which is why the mirrored
/// call passes `mirrored`.
fn refine_int_operand_constant(
    left: &mut Expression,
    right: &mut Expression,
    left_is_int: bool,
    right_is_int: bool,
    name: &str,
    mirrored: &str,
    ctx: &dyn Columns,
) {
    let constant_on_left = matches!(left, Expression::Constant(_));
    let (column, constant, op) = match (&mut *left, &mut *right) {
        (column, Expression::Constant(constant))
            if left_is_int && !right_is_int && !matches!(column, Expression::Constant(_)) =>
        {
            (column, constant, name)
        }
        (Expression::Constant(constant), column)
            if right_is_int && !left_is_int && !matches!(column, Expression::Constant(_)) =>
        {
            (column, constant, mirrored)
        }
        _ => return,
    };
    let Some(target) = column.static_type().cloned() else {
        return;
    };
    if let Some((refined, exceptional)) = refine_compared_constant(ctx, &target, constant, op) {
        if !exceptional {
            *constant = refined;
            return;
        }
        // Mutable values cannot be replaced by a plan-time truth value, and
        // nullable operands must retain NULL propagation (Go refineArgs).
        if target.flags() & FieldTypeFlags::NOT_NULL == 0
            || constant.param_marker.is_some()
            || constant.deferred_expr.is_some()
        {
            return;
        }
        let negative_infinite = match &refined.value {
            Datum::Int(value) => Some(value & 1 == 0),
            Datum::UInt(value) => Some(value & 1 == 0),
            _ => None,
        };
        let swap = !matches!(name, "eq" | "nulleq")
            && negative_infinite.is_some_and(|negative| negative != constant_on_left);
        *left = Expression::Constant(Constant::new(
            Datum::Int(i64::from(swap)),
            FieldType::new(FieldTypeCode::LongLong),
        ));
        *right = Expression::Constant(Constant::new(
            Datum::Int(i64::from(!swap)),
            FieldType::new(FieldTypeCode::LongLong),
        ));
    }
}

/// `int constant [cmp] year type` and `year type [cmp] int constant`
/// (`builtin_compare.go:1856-1873`), the arms that make `year_col < 30` mean
/// `year_col < 2030`:
///
/// ```go
/// // int constant [cmp] year type
/// if arg0IsCon && arg0IsInt && arg1Type.GetType() == mysql.TypeYear && !arg0.Value.IsNull() {
///     adjusted, failed := types.AdjustYear(arg0.Value.GetInt64(), false)
///     if failed == nil {
///         arg0.Value.SetInt64(adjusted)
///         finalArg0 = arg0
///     }
/// }
/// // year type [cmp] int constant
/// if arg1IsCon && arg1IsInt && arg0Type.GetType() == mysql.TypeYear && !arg1.Value.IsNull() {
///     ...
/// }
/// ```
///
/// # Why the value evaluator cannot do this on its own
///
/// A YEAR column reaches it as a plain `Datum::Int`, indistinguishable from a
/// `BIGINT`'s, so `y < 30` decided every row by `2018 < 30` and returned only
/// the zero year where TiDB returns the whole table. The two-digit window is
/// a property of the COLUMN'S TYPE, and the type exists only here.
///
/// The two arms cannot both fire on one comparison, and neither can fire on a
/// constant [`refine_int_operand_constant`] just substituted: Go tests the
/// ORIGINAL argument's eval type (`arg1IsInt`), which is `ETString` exactly
/// when that substitution happened. `y = '18'` is therefore adjusted once, by
/// `convert_to_year` inside [`refine_compared_constant`], and `y = 18` once,
/// here.
///
/// `failed == nil` is the whole gate. Go's `AdjustYear` CLAMPS an out-of-range
/// year and reports `ErrWarnDataOutOfRange` alongside it; the caller keeps the
/// clamped value only when there was no error, so a year outside 1901..=2155
/// leaves the comparison exactly as written -- which is what
/// [`tidb_datatype::adjust_year`]'s `Err` says without a clamp to discard.
fn adjust_year_int_constant(
    left: &mut Expression,
    right: &mut Expression,
    (left_is_int, right_is_int): (bool, bool),
    (left_is_year, right_is_year): (bool, bool),
) {
    if left_is_int && right_is_year {
        adjust_year_in_place(left);
    }
    if right_is_int && left_is_year {
        adjust_year_in_place(right);
    }
}

/// Go `arg.Value.SetInt64(types.AdjustYear(arg.Value.GetInt64(), false))`,
/// applied only when the side is a non-NULL constant.
///
/// Go reads the datum with `GetInt64`, which on a `KindUint64` returns the
/// same 64 bits reinterpreted; both integer datums are read that way here.
/// Any other datum shape under an `ETInt` static type is a hybrid
/// (`BIT`/`ENUM`/`SET`) whose `GetInt64` reads Go's unrelated `i` field, so
/// this leaves it alone rather than inventing a year for it.
fn adjust_year_in_place(side: &mut Expression) {
    let Expression::Constant(constant) = side else {
        return;
    };
    let value = match &constant.value {
        Datum::Int(value) => *value,
        Datum::UInt(value) => *value as i64,
        _ => return,
    };
    if let Ok(adjusted) = tidb_datatype::adjust_year(value, false) {
        constant.value = Datum::Int(adjusted);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn comparisons_are_boolean_longlong_flen1() {
        for name in ["eq", "nulleq", "ne", "lt", "le", "gt", "ge"] {
            let ret = infer_compare_type(name).unwrap();
            assert_eq!(ret.code(), FieldTypeCode::LongLong, "{name}");
            assert_eq!(ret.flen(), 1, "{name}");
            assert!(!ret.is_unsigned(), "{name}");
            assert_ne!(ret.flags() & FieldTypeFlags::IS_BOOLEAN, 0, "{name}");
            assert_ne!(ret.flags() & FieldTypeFlags::BINARY, 0, "{name}");
        }
    }

    #[test]
    fn non_comparison_name_is_none() {
        assert!(infer_compare_type("plus").is_none());
        assert!(infer_compare_type("and").is_none());
    }

    /// A warning sink, because the count and the text of what the refinement
    /// raises IS half of what is being ported: the values below all round
    /// trip, so a version that silently skipped the two `StrToFloat` calls
    /// would pass every value assertion here.
    #[derive(Default)]
    struct Sink {
        warnings: std::cell::RefCell<Vec<String>>,
    }

    impl Columns for Sink {
        fn get(&self, _: &[String]) -> Option<Datum> {
            None
        }
        fn append_warning(&self, code: u16, message: &str) {
            self.warnings.borrow_mut().push(format!("{code} {message}"));
        }
    }

    fn int_column() -> Expression {
        let mut column = crate::column::Column::new(1, FieldType::new(FieldTypeCode::Long));
        column.index = 0;
        Expression::Column(column)
    }

    fn duration_column() -> Expression {
        let mut column = crate::column::Column::new(2, FieldType::new(FieldTypeCode::Duration));
        column.index = 0;
        Expression::Column(column)
    }

    fn date_column() -> Expression {
        let mut column = crate::column::Column::new(3, FieldType::new(FieldTypeCode::Date));
        column.index = 0;
        Expression::Column(column)
    }

    fn string_constant(text: &str) -> Expression {
        Expression::Constant(Constant::new(
            Datum::new_string_datum(text),
            FieldType::new(FieldTypeCode::Varchar),
        ))
    }

    #[test]
    fn numeric_datetime_refinement_matches_go_shapes_and_date_flags() {
        struct DateContext(u8, tidb_datatype::SessionTimeZone);
        impl Columns for DateContext {
            fn get(&self, _: &[String]) -> Option<Datum> {
                None
            }
            fn time_zone(&self) -> tidb_datatype::SessionTimeZone {
                self.1.clone()
            }
            fn type_flags(&self) -> tidb_datatype::ConversionFlags {
                tidb_datatype::DEFAULT_STATEMENT_FLAGS
                    .with_ignore_zero_date_err(self.0 & 1 != 0)
                    .with_ignore_zero_in_date_err(self.0 & 1 != 0)
                    .with_ignore_invalid_date_err(self.0 & 2 != 0)
            }
            fn append_warning(&self, code: u16, message: &str) {
                panic!("unexpected warning {code}: {message}");
            }
        }
        // Go oracle: all four modes, both operand orders, temporal node kinds,
        // and integer/decimal/real/NULL controls. REAL conversion is rejected
        // by Datum.ConvertTo, even though the refinement rule considers it.
        for mode in 0..4 {
            for code in [
                FieldTypeCode::Datetime,
                FieldTypeCode::Timestamp,
                FieldTypeCode::Date,
            ] {
                let field = FieldType::new(code);
                let column = crate::column::Column::new(1, field.clone());
                let time = tidb_datatype::Time::new(
                    tidb_datatype::CoreTime::from_date(2024, 8, 9, 0, 0, 0, 0),
                    match code {
                        FieldTypeCode::Timestamp => tidb_datatype::TimeType::Timestamp,
                        FieldTypeCode::Date => tidb_datatype::TimeType::Date,
                        _ => tidb_datatype::TimeType::DateTime,
                    },
                    0,
                )
                .unwrap();
                for (shape, temporal) in [
                    ("column", Expression::Column(column.clone())),
                    (
                        "correlated",
                        Expression::CorrelatedColumn(crate::column::CorrelatedColumn::new(column)),
                    ),
                    (
                        "scalar",
                        crate::wrap_cast::build_cast_function(
                            &crate::wrap_cast::UnfoldedBuildContext,
                            Expression::Column(crate::column::Column::new(
                                2,
                                FieldType::new(FieldTypeCode::LongLong),
                            )),
                            field.clone(),
                        )
                        .unwrap(),
                    ),
                    (
                        "constant",
                        Expression::Constant(Constant::new(Datum::new_time(time), field.clone())),
                    ),
                ] {
                    for (value, source_code, expected) in [
                        (
                            Datum::Int(20240809),
                            FieldTypeCode::LongLong,
                            Some("2024-08-09 00:00:00"),
                        ),
                        (
                            Datum::Int(0),
                            FieldTypeCode::LongLong,
                            (mode & 1 != 0).then_some("0000-00-00 00:00:00"),
                        ),
                        (
                            Datum::Int(20240001),
                            FieldTypeCode::LongLong,
                            (mode & 1 != 0).then_some("2024-00-01 00:00:00"),
                        ),
                        (
                            Datum::Int(20240231),
                            FieldTypeCode::LongLong,
                            (mode & 2 != 0).then_some("2024-02-31 00:00:00"),
                        ),
                        (Datum::Int(20241301), FieldTypeCode::LongLong, None),
                        (
                            Datum::Decimal(
                                tidb_datatype::Decimal::parse_mysql("20240809235959.9").0,
                            ),
                            FieldTypeCode::NewDecimal,
                            Some("2024-08-10 00:00:00"),
                        ),
                        (
                            Datum::Decimal(tidb_datatype::Decimal::parse_mysql("0.01").0),
                            FieldTypeCode::NewDecimal,
                            Some("0000-00-00 00:00:00"),
                        ),
                        (
                            Datum::Decimal(tidb_datatype::Decimal::parse_mysql("0.1").0),
                            FieldTypeCode::NewDecimal,
                            (mode & 1 != 0).then_some("0000-00-00 00:00:00"),
                        ),
                        (Datum::Real(20240809.0), FieldTypeCode::Double, None),
                        (Datum::Null, FieldTypeCode::LongLong, None),
                    ] {
                        for flip in [false, true] {
                            let mut numeric =
                                Constant::new(value.clone(), FieldType::new(source_code));
                            numeric.subquery_ref_id = 77;
                            let numeric = Expression::Constant(numeric);
                            let (mut left, mut right) = if flip {
                                (numeric, temporal.clone())
                            } else {
                                (temporal.clone(), numeric)
                            };
                            refine_args(
                                &mut left,
                                &mut right,
                                "lt",
                                "gt",
                                &DateContext(mode, tidb_datatype::SessionTimeZone::utc()),
                            );
                            let Expression::Constant(actual) = (if flip { &left } else { &right })
                            else {
                                panic!("numeric constant lost")
                            };
                            let expected = if code == FieldTypeCode::Date || shape == "constant" {
                                None
                            } else {
                                expected
                            };
                            let case =
                                format!("mode={mode} {code:?} {shape} {value:?} flip={flip}");
                            if let Some(expected) = expected {
                                assert_eq!(
                                    actual.ret_type.as_ref().unwrap().code(),
                                    FieldTypeCode::Datetime,
                                    "{case}"
                                );
                                assert_eq!(actual.value.sql_string().unwrap(), expected, "{case}");
                                assert_eq!(actual.subquery_ref_id, 0, "{case}");
                            } else {
                                assert_eq!(actual.value, value, "{case}");
                                assert_eq!(actual.subquery_ref_id, 77, "{case}");
                            }
                        }
                    }
                }
            }
        }
        for (zone, spring, autumn) in [
            (
                tidb_datatype::SessionTimeZone::utc(),
                "2011-03-13 02:00:00",
                "2011-11-06 02:00:00",
            ),
            (
                tidb_datatype::SessionTimeZone::Named(chrono_tz::America::Los_Angeles),
                "2011-03-13 03:00:00",
                "2011-11-06 01:00:00",
            ),
        ] {
            for (text, expected) in [("20110313015959.9", spring), ("20111106015959.9", autumn)] {
                for flip in [false, true] {
                    let temporal = Expression::Column(crate::column::Column::new(
                        1,
                        FieldType::new(FieldTypeCode::Datetime),
                    ));
                    let numeric = Expression::Constant(Constant::new(
                        Datum::Decimal(tidb_datatype::Decimal::parse_mysql(text).0),
                        FieldType::new(FieldTypeCode::NewDecimal),
                    ));
                    let (mut left, mut right) = if flip {
                        (numeric, temporal)
                    } else {
                        (temporal, numeric)
                    };
                    refine_args(
                        &mut left,
                        &mut right,
                        "lt",
                        "gt",
                        &DateContext(0, zone.clone()),
                    );
                    let Expression::Constant(actual) = (if flip { &left } else { &right }) else {
                        panic!("constant lost")
                    };
                    assert_eq!(
                        actual.value.sql_string().unwrap(),
                        expected,
                        "{zone:?} {text} flip={flip}"
                    );
                }
            }
        }
    }

    #[test]
    fn decimal_refinement_preserves_exact_boundaries_and_go_result_types() {
        // TestRefinePrecisionOracle: 11 source cases across all seven operators.
        for (text, target_code, unsigned, floor, ceil, exceptional) in [
            (
                "9007199254740993.1",
                FieldTypeCode::LongLong,
                false,
                "9007199254740993",
                "9007199254740994",
                false,
            ),
            (
                "-9007199254740993.1",
                FieldTypeCode::LongLong,
                false,
                "-9007199254740994",
                "-9007199254740993",
                false,
            ),
            (
                "9223372036854775806.1",
                FieldTypeCode::LongLong,
                false,
                "9223372036854775806",
                "9223372036854775807",
                false,
            ),
            (
                "-9223372036854775807.1",
                FieldTypeCode::LongLong,
                false,
                "-9223372036854775808",
                "-9223372036854775807",
                false,
            ),
            (
                "18446744073709551614.1",
                FieldTypeCode::LongLong,
                true,
                "18446744073709551614",
                "18446744073709551615",
                false,
            ),
            ("127.1", FieldTypeCode::Tiny, false, "127", "128", false),
            ("-128.1", FieldTypeCode::Tiny, false, "-129", "-128", false),
            ("255.1", FieldTypeCode::Tiny, true, "255", "256", false),
            ("-0.1", FieldTypeCode::Tiny, true, "0", "0", true),
            (
                "9007199254740993.0",
                FieldTypeCode::LongLong,
                false,
                "9007199254740993",
                "9007199254740993",
                false,
            ),
            (
                "9223372036854775808.1",
                FieldTypeCode::LongLong,
                false,
                "9223372036854775807",
                "9223372036854775807",
                true,
            ),
        ] {
            let source = FieldType::new(FieldTypeCode::NewDecimal)
                .with_flen((text.trim_start_matches('-').len() - 1) as i64)
                .with_decimal(1);
            let mut constant = Constant::new(
                Datum::Decimal(tidb_datatype::Decimal::parse_mysql(text).0),
                source,
            );
            constant.subquery_ref_id = 77;
            let target = FieldType::new(target_code).with_unsigned(unsigned);
            for op in ["lt", "le", "gt", "ge", "eq", "ne", "nulleq"] {
                let sink = Sink::default();
                let (actual, is_exceptional) =
                    refine_compared_constant(&sink, &target, &constant, op)
                        .unwrap_or_else(|| (constant.clone(), false));
                let unchanged =
                    !exceptional && !text.ends_with(".0") && matches!(op, "eq" | "ne" | "nulleq");
                let expected = if unchanged {
                    text
                } else if matches!(op, "lt" | "ge") {
                    ceil
                } else {
                    floor
                };
                assert_eq!(actual.value.sql_string().unwrap(), expected, "{text} {op}");
                assert_eq!(
                    is_exceptional,
                    exceptional || (unchanged && op != "ne"),
                    "{text} {op}"
                );
                assert_eq!(
                    actual.ret_type.as_ref().unwrap().code(),
                    if unchanged {
                        FieldTypeCode::NewDecimal
                    } else if exceptional {
                        target_code
                    } else {
                        FieldTypeCode::LongLong
                    },
                    "{text} {op}"
                );
                assert_eq!(
                    matches!(actual.value, Datum::UInt(_)),
                    !unchanged
                        && unsigned
                        && (exceptional || target_code == FieldTypeCode::LongLong),
                    "{text} {op}"
                );
                assert_eq!(
                    actual.subquery_ref_id,
                    if exceptional { 0 } else { 77 },
                    "{text} {op}"
                );
                assert!(sink.warnings.borrow().is_empty());
            }
        }
    }

    #[test]
    fn unsigned_refinement_matches_go_across_widths_nullability_and_operand_order() {
        // TestUnsignedRefinementOracle: each pair covers lt/le/gt/ge/eq/ne/nulleq,
        // column first then constant first. Go returns these same masks for all
        // five integer widths and both ordinary and correlated columns.
        let masks = [
            [
                "------- -------",
                "------- -------",
                "------- -------",
                "------- -------",
                "------- -------",
                "------Y ------Y",
                "------Y ------Y",
                "------- -------",
            ],
            ["------- -------"; 8],
            [
                "------- -------",
                "------- -------",
                "------- -------",
                "------- -------",
                "------- -------",
                "----Y-Y ----Y-Y",
                "----Y-Y ----Y-Y",
                "------- -------",
            ],
            [
                "YYYYYYY YYYYYYY",
                "Y--Y--- -YY----",
                "------- -------",
                "------- -------",
                "------- -------",
                "------- -------",
                "------- -------",
                "------- -------",
            ],
        ];
        let values = [
            Datum::Int(-1),
            Datum::Int(0),
            Datum::Int(1),
            Datum::UInt(0),
            Datum::UInt(128),
            Datum::UInt(1 << 63),
            Datum::UInt(u64::MAX),
            Datum::Null,
        ];
        let operators = ["lt", "le", "gt", "ge", "eq", "ne", "nulleq"];
        for code in [
            FieldTypeCode::Tiny,
            FieldTypeCode::Short,
            FieldTypeCode::Int24,
            FieldTypeCode::Long,
            FieldTypeCode::LongLong,
        ] {
            for (flags, masks) in masks.iter().enumerate() {
                let mut column_type = FieldType::new(code);
                if flags & 1 != 0 {
                    column_type.add_flags(FieldTypeFlags::UNSIGNED);
                }
                if flags & 2 != 0 {
                    column_type.add_flags(FieldTypeFlags::NOT_NULL);
                }
                for (value, mask) in values.iter().zip(masks) {
                    let mut constant_type = FieldType::new(FieldTypeCode::LongLong);
                    if matches!(value, Datum::UInt(_)) {
                        constant_type.add_flags(FieldTypeFlags::UNSIGNED);
                    }
                    for correlated in [false, true] {
                        for (flip, mask) in mask.split_whitespace().enumerate() {
                            for (op, expected) in operators.iter().zip(mask.bytes()) {
                                let column = crate::column::Column::new(1, column_type.clone());
                                let column = if correlated {
                                    Expression::CorrelatedColumn(
                                        crate::column::CorrelatedColumn::new(column),
                                    )
                                } else {
                                    Expression::Column(column)
                                };
                                let constant = Expression::Constant(Constant::new(
                                    value.clone(),
                                    constant_type.clone(),
                                ));
                                let (left, right) = if flip == 0 {
                                    (column, constant)
                                } else {
                                    (constant, column)
                                };
                                let (actual, warnings) = refine(op, left, right);
                                assert!(warnings.is_empty());
                                let Expression::ScalarFunction(function) = actual else {
                                    panic!("comparison")
                                };
                                let replaced = function
                                    .args
                                    .iter()
                                    .all(|arg| matches!(arg, Expression::Constant(_)));
                                assert_eq!(replaced, expected == b'Y', "{code:?} flags={flags} value={value:?} correlated={correlated} flip={flip} op={op}");
                                if replaced {
                                    for (index, argument) in function.args.iter().enumerate() {
                                        let Expression::Constant(constant) = argument else {
                                            unreachable!()
                                        };
                                        assert_eq!(
                                            constant.value,
                                            Datum::Int(i64::from(index == flip))
                                        );
                                        let field = constant.ret_type.as_ref().unwrap();
                                        assert_eq!(field.code(), FieldTypeCode::Tiny);
                                        assert!(field.is_unsigned());
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    /// `<int column> <op> <constant>`, refined, as `(refined arg, warnings)`.
    fn refine(op: &str, left: Expression, right: Expression) -> (Expression, Vec<String>) {
        let sink = Sink::default();
        let expr = crate::new_function::new_function_base(
            &sink,
            op,
            infer_compare_type(op).unwrap(),
            vec![left, right],
        )
        .unwrap();
        (expr, sink.warnings.into_inner())
    }

    fn constant_of(expr: &Expression, index: usize) -> Datum {
        let Expression::ScalarFunction(function) = expr else {
            panic!("expected a comparison, got {expr:?}");
        };
        match &function.args[index] {
            Expression::Constant(constant) => constant.value.clone(),
            other => panic!("argument {index} is not a constant: {other:?}"),
        }
    }

    #[test]
    fn invalid_duration_constants_are_rewritten_once_on_either_side() {
        for (left, right) in [
            (duration_column(), string_constant("not-a-time")),
            (string_constant("not-a-time"), duration_column()),
        ] {
            let (expr, warnings) = refine("nulleq", left, right);
            assert_eq!(constant_of(&expr, 0), Datum::Int(0));
            assert_eq!(constant_of(&expr, 1), Datum::Int(1));
            assert_eq!(warnings.len(), 1, "{warnings:?}");
        }

        let (valid, warnings) = refine("nulleq", duration_column(), string_constant("10:00:00"));
        assert_eq!(
            constant_of(&valid, 1),
            Datum::Duration(tidb_datatype::MySqlDuration::from_raw_parts(
                10 * 3_600 * 1_000_000_000,
                6,
            ))
        );
        assert!(warnings.is_empty(), "{warnings:?}");

        let null = Expression::Constant(Constant::new_null());
        let (null_comparison, warnings) = refine("nulleq", duration_column(), null);
        assert_eq!(constant_of(&null_comparison, 1), Datum::Null);
        assert!(warnings.is_empty(), "{warnings:?}");
    }

    #[test]
    fn temporal_comparison_folds_string_constants_as_datetime() {
        for (left, right, constant_index) in [
            (date_column(), string_constant("1995-03-13"), 1),
            (string_constant("1995-03-13"), date_column(), 0),
        ] {
            let (expr, warnings) = refine("lt", left, right);
            assert!(warnings.is_empty(), "{warnings:?}");
            let Expression::ScalarFunction(comparison) = expr else {
                panic!("comparison was not a scalar function");
            };
            let Expression::Constant(constant) = &comparison.args[constant_index] else {
                panic!("string comparison operand was not folded");
            };
            assert_eq!(
                constant.value.sql_string().unwrap(),
                "1995-03-13 00:00:00.000000"
            );
            let target = constant.ret_type.as_ref().unwrap();
            assert_eq!(target.code(), FieldTypeCode::Datetime);
            assert_eq!(target.flen(), 26);
            assert_eq!(target.decimal(), 6);
        }

        // Go's BuildCastFunction folds this invalid non-JSON cast while the
        // comparison signature is built. The folded NULL keeps the source's
        // one 1292 warning, but moves it to construction time rather than
        // leaving a Rust-only cast node for per-row evaluation.
        let (invalid, warnings) = refine("lt", date_column(), string_constant("not-a-datetime"));
        assert_eq!(constant_of(&invalid, 1), Datum::Null);
        assert_eq!(warnings.len(), 1, "{warnings:?}");
    }

    #[test]
    fn duration_refinement_propagates_strict_conversion_errors() {
        struct StrictSink;
        impl Columns for StrictSink {
            fn get(&self, _: &[String]) -> Option<Datum> {
                None
            }
            fn truncate_level(&self) -> tidb_exprctx::context::ErrorLevel {
                tidb_exprctx::context::ErrorLevel::Error
            }
        }

        assert!(matches!(
            crate::new_function::new_function_base(
                &StrictSink,
                "nulleq",
                infer_compare_type("nulleq").unwrap(),
                vec![duration_column(), string_constant("not-a-time")],
            ),
            Err(EvalError::TruncatedWrongValue(_))
        ));
    }

    #[test]
    fn a_decimal_column_compared_with_bigint_casts_bigint_to_decimal_20() {
        let mut decimal = crate::column::Column::new(1, FieldType::new(FieldTypeCode::NewDecimal));
        decimal.index = 0;
        let mut integer = crate::column::Column::new(2, FieldType::new(FieldTypeCode::LongLong));
        integer.index = 1;
        let (expr, warnings) = refine(
            "ne",
            Expression::Column(decimal),
            Expression::Column(integer),
        );
        assert!(warnings.is_empty());
        let Expression::ScalarFunction(comparison) = expr else {
            panic!("comparison was not a scalar function");
        };
        let Expression::ScalarFunction(cast) = &comparison.args[1] else {
            panic!("BIGINT operand was not cast: {:?}", comparison.args[1]);
        };
        assert_eq!(cast.func_name.lowercase(), "cast");
        let target = cast.ret_type.as_ref().unwrap();
        assert_eq!(target.code(), FieldTypeCode::NewDecimal);
        assert_eq!(target.flen(), 20);
        assert_eq!(target.decimal(), 0);
        assert_ne!(target.flags() & FieldTypeFlags::BINARY, 0);
    }

    #[test]
    fn decimal_comparison_folds_integer_literals_with_go_precision() {
        for (value, precision, unsigned) in [
            (Datum::Int(24), 2, false),
            (Datum::Int(-24), 2, false),
            (Datum::Int(i64::MIN), 19, false),
            (Datum::UInt(u64::MAX), 20, true),
        ] {
            for literal_on_left in [false, true] {
                let decimal = Expression::Column(crate::column::Column::new(
                    1,
                    FieldType::new(FieldTypeCode::NewDecimal),
                ));
                let flags = FieldTypeFlags::NOT_NULL
                    | if unsigned {
                        FieldTypeFlags::UNSIGNED
                    } else {
                        0
                    };
                let integer = Expression::Constant(Constant::new(
                    value.clone(),
                    FieldType::new(FieldTypeCode::LongLong).with_flags(flags),
                ));
                let (left, right) = if literal_on_left {
                    (integer, decimal)
                } else {
                    (decimal, integer)
                };
                let (expression, warnings) = refine("lt", left, right);
                assert!(warnings.is_empty());
                let Expression::ScalarFunction(function) = expression else {
                    panic!("comparison")
                };
                let Expression::Constant(constant) = &function.args[usize::from(!literal_on_left)]
                else {
                    panic!("folded decimal")
                };
                let field_type = constant.ret_type.as_ref().unwrap();
                assert_eq!(field_type.code(), FieldTypeCode::NewDecimal);
                assert_eq!(field_type.flen(), precision);
                assert_eq!(field_type.decimal(), 0);
                assert_eq!(field_type.flags() & FieldTypeFlags::UNSIGNED != 0, unsigned);
                assert_ne!(field_type.flags() & FieldTypeFlags::BINARY, 0);
                let Datum::Decimal(converted) = &constant.value else {
                    panic!("decimal datum")
                };
                assert_eq!(converted.to_string(), value.sql_string().unwrap());
            }
        }
        struct Parameter(Datum);
        impl crate::Columns for Parameter {
            fn get(&self, _: &[String]) -> Option<Datum> {
                None
            }
            fn param_value(&self, order: usize) -> Result<Datum, EvalError> {
                if order == 0 {
                    Ok(self.0.clone())
                } else {
                    Err(EvalError::Unsupported("unbound prepared parameter"))
                }
            }
        }
        for deferred in [false, true] {
            let mut parameter =
                Constant::new(Datum::Int(1), FieldType::new(FieldTypeCode::LongLong));
            parameter.param_marker = Some(crate::constant::ParamMarker { order: 0 });
            if deferred {
                let child = Expression::Constant(parameter.clone());
                parameter.param_marker = None;
                parameter.deferred_expr = Some(Box::new(child));
            }
            let cast = crate::wrap_cast::wrap_with_cast_as_decimal(&crate::builtin::UnfoldedBuildContext, Expression::Constant(parameter))
                .unwrap();
            assert!(
                matches!(&cast, Expression::ScalarFunction(function) if function.func_name.lowercase() == "cast")
            );
            for value in [Datum::Int(2), Datum::Int(-2), Datum::Null] {
                let expected = match value {
                    Datum::Int(value) => Datum::Decimal(tidb_datatype::Decimal::from_int(value)),
                    _ => Datum::Null,
                };
                assert_eq!(
                    crate::eval_expression_once(&cast, &Parameter(value)).unwrap(),
                    expected
                );
            }
            assert!(crate::eval_expression_once(&cast, &crate::NoColumns).is_err());
        }
    }

    /// The reported statement, structurally: `a > '10ab'` compares INT TO INT
    /// afterwards, and the two warnings are the conversion and the compare --
    /// NOT the `Floor` fold, which this input never reaches because 10 and
    /// 10.0 are equal (`builtin_compare.go:1600-1609`).
    #[test]
    fn an_int_column_gt_a_partial_numeric_string_becomes_gt_an_int() {
        let (expr, warnings) = refine("gt", int_column(), string_constant("10ab"));
        assert_eq!(constant_of(&expr, 1), Datum::Int(10));
        assert_eq!(
            warnings,
            [
                "1292 Truncated incorrect DOUBLE value: '10ab'",
                "1292 Truncated incorrect DOUBLE value: '10ab'",
            ]
        );
    }

    /// The `Floor`/`Ceil` fold, which is reached only when the converted int
    /// and the constant DIFFER, and which rounds by operator so the refined
    /// comparison selects exactly the rows the written one did.
    ///
    /// Captured from `gorun` over `(1),(2),(3),(10),(20)`: `a > '3.5'` answers
    /// 10 and 20 (so the constant is 3, not 4) and warns zero times, because
    /// `'3.5'` is a complete float prefix.
    #[test]
    fn an_inexact_constant_rounds_by_operator() {
        for (op, expected) in [("gt", 3), ("le", 3), ("lt", 4), ("ge", 4)] {
            let (expr, warnings) = refine(op, int_column(), string_constant("3.5"));
            assert_eq!(constant_of(&expr, 1), Datum::Int(expected), "{op}");
            assert!(warnings.is_empty(), "{op} -> {warnings:?}");
        }
    }

    /// The fold contributes a THIRD warning when the string both truncates
    /// and is inexact -- its own string->double coercion. Captured:
    /// `a > '3.5abc'` warns three times.
    #[test]
    fn a_truncating_inexact_string_warns_three_times() {
        let (expr, warnings) = refine("gt", int_column(), string_constant("3.5abc"));
        assert_eq!(constant_of(&expr, 1), Datum::Int(3));
        assert_eq!(warnings.len(), 3, "{warnings:?}");
    }

    /// The constant on the LEFT takes the operator seen from the COLUMN's
    /// side (Go `symmetricOp`), so `'3.5' < a` refines like `a > '3.5'`.
    #[test]
    fn a_constant_on_the_left_uses_the_symmetric_operator() {
        let (expr, _) = refine("lt", string_constant("3.5"), int_column());
        assert_eq!(constant_of(&expr, 0), Datum::Int(3));
    }

    /// Nothing to refine: two constants are not the `int non-constant [cmp]
    /// non-int constant` shape, and an int constant is already int.
    #[test]
    fn only_a_non_int_constant_against_an_int_non_constant_is_refined() {
        let (expr, warnings) = refine("gt", string_constant("10ab"), string_constant("10ab"));
        assert_eq!(constant_of(&expr, 1), Datum::new_string_datum("10ab"));
        assert!(warnings.is_empty(), "{warnings:?}");

        let int_constant = Expression::Constant(Constant::new(
            Datum::Int(10),
            FieldType::new(FieldTypeCode::LongLong),
        ));
        let (expr, warnings) = refine("gt", int_column(), int_constant);
        assert_eq!(constant_of(&expr, 1), Datum::Int(10));
        assert!(warnings.is_empty(), "{warnings:?}");
    }

    /// `EQ` against an inexact value is Go's `isExceptional`; nullable columns
    /// retain the comparison so NULL still propagates. The per-row comparison
    /// computes the right (empty) answer -- captured: `a = '3.5'` returns no
    /// row and warns zero times.
    #[test]
    fn an_inexact_equality_casts_the_unrefined_constant_like_go() {
        let (expr, _) = refine("eq", int_column(), string_constant("3.5"));
        assert_eq!(constant_of(&expr, 1), Datum::Real(3.5));
    }

    #[test]
    fn exceptional_integer_comparisons_fold_only_for_not_null_columns() {
        for (op, literal, expected) in [
            ("eq", "150.5", false),
            ("nulleq", "150.5", false),
            ("lt", "999999999999999999999999", true),
            ("gt", "-999999999999999999999999", true),
        ] {
            for mirrored in [false, true] {
                let field_type =
                    FieldType::new(FieldTypeCode::Long).with_flags(FieldTypeFlags::NOT_NULL);
                let column = Expression::Column(crate::column::Column::new(1, field_type));
                let constant = string_constant(literal);
                let (left, right, operator) = if mirrored {
                    (constant, column, symmetric_op(op).unwrap())
                } else {
                    (column, constant, op)
                };
                let (expr, _) = refine(operator, left, right);
                assert!(crate::constant_fold::folds_to_constant(&expr), "{expr:?}");
                assert_eq!(
                    crate::eval_expression_once(&expr, &Sink::default()).unwrap(),
                    Datum::Int(i64::from(expected))
                );
            }
            let (nullable, _) = refine(op, int_column(), string_constant(literal));
            assert!(!crate::constant_fold::folds_to_constant(&nullable));
        }
    }

    /// An overflowing constant is Go's other `isExceptional`; left unrefined
    /// for the same reason, and captured as warning-free
    /// (`a > 1e100` warns zero times).
    #[test]
    fn an_overflowing_constant_is_left_unrefined() {
        let constant = Expression::Constant(Constant::new(
            Datum::Real(1e100),
            FieldType::new(FieldTypeCode::Double),
        ));
        let (expr, warnings) = refine("gt", int_column(), constant);
        assert_eq!(constant_of(&expr, 1), Datum::Real(1e100));
        assert!(warnings.is_empty(), "{warnings:?}");
    }
}

// ***** GetAccurateCmpType *****

/// One operand of a comparison, as much of it as `GetAccurateCmpType` reads.
///
/// Go passes whole `Expression`s and asks them three questions: their
/// `FieldType`, whether they are a `*Constant`, and whether they are a
/// `*Column` (`isTemporalColumn`). Carrying those three answers keeps the
/// port a pure function of what Go actually consults.
#[derive(Debug, Clone, Copy)]
pub struct CmpOperand<'a> {
    /// Go `expr.GetType(ctx)`.
    pub field_type: &'a FieldType,
    /// Go `_, ok := expr.(*Constant)`.
    pub is_constant: bool,
    /// Go `_, ok := expr.(*Column)`, which `isTemporalColumn` also requires.
    pub is_column: bool,
}

impl<'a> CmpOperand<'a> {
    /// A non-constant column operand, the shape a scanned column takes.
    #[must_use]
    pub const fn column(field_type: &'a FieldType) -> Self {
        Self {
            field_type,
            is_constant: false,
            is_column: true,
        }
    }

    /// A literal operand.
    #[must_use]
    pub const fn constant(field_type: &'a FieldType) -> Self {
        Self {
            field_type,
            is_constant: true,
            is_column: false,
        }
    }

    /// Go `isTemporalColumn(ctx, expr)`: a COLUMN whose type carries a date,
    /// or a `DURATION` one.
    const fn is_temporal_column(self) -> bool {
        self.is_column
            && (self.field_type.code().is_temporal_with_date()
                || matches!(self.field_type.code(), FieldTypeCode::Duration))
    }
}

/// Go `getBaseCmpType(lhs, rhs, lft, rft)` (`builtin_compare.go`): the
/// comparison domain two operand TYPES share, before `GetAccurateCmpType`'s
/// overrides.
fn base_cmp_type(left: &FieldType, right: &FieldType) -> EvalType {
    let (mut lhs, mut rhs) = (left.eval_type(), right.eval_type());
    let (left_code, right_code) = (left.code(), right.code());
    if left_code == FieldTypeCode::Unspecified || right_code == FieldTypeCode::Unspecified {
        if left_code == right_code {
            return EvalType::String;
        }
        if left_code == FieldTypeCode::Unspecified {
            lhs = rhs;
        } else {
            rhs = lhs;
        }
    }
    // A HYBRID type reads as an integer here even though its own eval type
    // may not be one, which is what puts `ENUM`/`SET` alongside `BIT`.
    let left_int = lhs == EvalType::Int || left.is_hybrid();
    let right_int = rhs == EvalType::Int || right.is_hybrid();
    if lhs.is_string_kind() && rhs.is_string_kind() {
        EvalType::String
    } else if left_int && right_int {
        EvalType::Int
    } else if (lhs == EvalType::Decimal && rhs == EvalType::String)
        || (lhs == EvalType::String && rhs == EvalType::Decimal)
    {
        EvalType::Real
    } else if (left_int || lhs == EvalType::Decimal) && (right_int || rhs == EvalType::Decimal) {
        EvalType::Decimal
    } else if (left_code.is_temporal_with_date() && right_code == FieldTypeCode::Year)
        || (left_code == FieldTypeCode::Year && right_code.is_temporal_with_date())
    {
        EvalType::Datetime
    } else {
        EvalType::Real
    }
}

/// Go `expression.GetAccurateCmpType(ctx, lhs, rhs)`: the eval type a
/// comparison of these two operands is EVALUATED in, which is also the domain
/// each side is cast into before the comparison runs.
///
/// This is not the comparison's RESULT type -- that is always the ETInt of
/// [`infer_compare_type`]. It is the domain, and any caller that has to
/// reproduce the comparison's own equality relation (rather than just run it)
/// needs it: two values distinct in their own type can be EQUAL here, since
/// `blob <cmp> int` compares as REAL and `'gO'` and `'W'` are both `0`.
#[must_use]
pub fn get_accurate_cmp_type(left: CmpOperand<'_>, right: CmpOperand<'_>) -> EvalType {
    let (left_type, right_type) = (left.field_type, right.field_type);
    let (left_eval, right_eval) = (left_type.eval_type(), right_type.eval_type());
    let (left_code, right_code) = (left_type.code(), right_type.code());
    let mut cmp_type = base_cmp_type(left_type, right_type);
    if left_eval == EvalType::VectorFloat32 || right_eval == EvalType::VectorFloat32 {
        cmp_type = EvalType::VectorFloat32;
    } else if (left_eval.is_string_kind() && left_code == FieldTypeCode::Json)
        || (right_eval.is_string_kind() && right_code == FieldTypeCode::Json)
    {
        cmp_type = EvalType::Json;
    } else if cmp_type == EvalType::String
        && (left_code.is_type_time() || right_code.is_type_time())
    {
        // `date[time] <cmp> date[time]`, and `string <cmp> date[time]`.
        cmp_type = if left_code == right_code {
            left_eval
        } else {
            EvalType::Datetime
        };
    } else if left_code == FieldTypeCode::Duration && right_code == FieldTypeCode::Duration {
        cmp_type = EvalType::Duration;
    } else if cmp_type == EvalType::Real || cmp_type == EvalType::String {
        if (left_eval == EvalType::Decimal
            && !left.is_constant
            && right_eval.is_string_kind()
            && right.is_constant)
            || (right_eval == EvalType::Decimal
                && !right.is_constant
                && left_eval.is_string_kind()
                && left.is_constant)
        {
            // A non-constant DECIMAL against a constant string compares as
            // DECIMAL rather than REAL, so the precision survives.
            cmp_type = EvalType::Decimal;
        } else if (left.is_temporal_column() && right.is_constant)
            || (right.is_temporal_column() && left.is_constant)
        {
            // Go narrows only the DURATION case here; a date-bearing column
            // against a non-temporal constant keeps the REAL it already has.
            let column = if left.is_column { left } else { right };
            if column.field_type.code() == FieldTypeCode::Duration {
                cmp_type = EvalType::Duration;
            }
        }
    }
    cmp_type
}

/// Go `coalesceFunctionClass`.
pub(crate) struct CoalesceFunctionClass;

impl crate::builtin::FunctionClass for CoalesceFunctionClass {
    fn get_function(
        &self,
        ctx: &dyn crate::builtin::BuildContext,
        args: Vec<Expression>,
    ) -> Result<crate::builtin::BaseBuiltinFunc, EvalError> {
        use tidb_proto::tipb::ScalarFuncSig;

        // COALESCE is NOT NULL when any argument is.
        let flag = args.iter().fold(0, |flag, arg| {
            flag | (crate::wrap_cast::type_of(arg).flags() & FieldTypeFlags::NOT_NULL)
        });
        let mut result_field_type =
            crate::builtin_control::infer_type_4_control_funcs(ctx, "coalesce", &args)?;
        result_field_type.add_flags(flag);
        let ret_eval_tp = result_field_type.eval_type();
        let field_eval_tps = vec![ret_eval_tp; args.len()];
        let mut bf = crate::builtin::new_base_builtin_func_with_tp(
            ctx,
            "coalesce",
            args,
            ret_eval_tp,
            &field_eval_tps,
        )?;
        let decimal = result_field_type.decimal();
        bf.tp = result_field_type;
        bf.pb_code = match ret_eval_tp {
            EvalType::Int => ScalarFuncSig::CoalesceInt,
            EvalType::Real => ScalarFuncSig::CoalesceReal,
            EvalType::Decimal => ScalarFuncSig::CoalesceDecimal,
            EvalType::String => ScalarFuncSig::CoalesceString,
            EvalType::Datetime | EvalType::Timestamp => {
                bf.tp.set_decimal(decimal);
                ScalarFuncSig::CoalesceTime
            }
            EvalType::Duration => {
                bf.tp.set_decimal(decimal);
                ScalarFuncSig::CoalesceDuration
            }
            EvalType::Json => ScalarFuncSig::CoalesceJson,
            // Go sets no pb code for `builtinCoalesceVectorFloat32Sig`.
            EvalType::VectorFloat32 => {
                bf.kernel = Some(crate::scalar_function::Kernel::Coalesce(EvalType::VectorFloat32));
                ScalarFuncSig::Unspecified
            }
        };
        Ok(bf)
    }
}

/// Go `resolveType4Extremum`: the aggregated type, whether it is a DATE
/// (`GLRetDate`), and how string arguments compare.
fn resolve_type_4_extremum(args: &[Expression]) -> (FieldType, bool, GlCmpStringMode) {
    let arg_types: Vec<FieldType> = args.iter().map(crate::wrap_cast::type_of).collect();
    let agg_type = tidb_datatype::agg_field_type(&arg_types);
    let mut cmp_string_mode = GlCmpStringMode::Directly;
    if agg_type.eval_type().is_string_kind() {
        let mut temporal_item: Option<FieldTypeCode> = None;
        for item in &arg_types {
            if item.code().is_type_temporal()
                && (temporal_item.is_none() || item.code() == FieldTypeCode::Datetime)
            {
                temporal_item = Some(item.code());
            }
        }
        if let Some(temporal_item) = temporal_item {
            if !agg_type.code().is_type_temporal() && temporal_item.is_temporal_with_date() {
                cmp_string_mode = if temporal_item == FieldTypeCode::Date {
                    GlCmpStringMode::AsDate
                } else {
                    GlCmpStringMode::AsDatetime
                };
            }
        }
    }
    let ret_date = agg_type.code() == FieldTypeCode::Date;
    (agg_type, ret_date, cmp_string_mode)
}

/// Go `unsupportedJSONComparison`.
fn unsupported_json_comparison(ctx: &dyn crate::builtin::BuildContext, args: &[Expression]) {
    if args
        .iter()
        .any(|arg| crate::wrap_cast::type_of(arg).code() == FieldTypeCode::Json)
    {
        ctx.append_warning(&crate::expression_errors::ERR_UNSUPPORTED_JSON_COMPARISON);
    }
}

/// Go `fixFlenAndDecimalForGreatestAndLeast`.
fn fix_flen_and_decimal_for_greatest_and_least(args: &[Expression]) -> (i64, i64) {
    args.iter().fold((0, 0), |(flen, decimal), arg| {
        let arg_type = crate::wrap_cast::type_of(arg);
        (flen.max(arg_type.flen()), decimal.max(arg_type.decimal()))
    })
}

/// The signatures of `GREATEST` or of `LEAST`, in the order of Go's
/// `getFunction` switch.
struct ExtremumSigs {
    int: tidb_proto::tipb::ScalarFuncSig,
    real: tidb_proto::tipb::ScalarFuncSig,
    decimal: tidb_proto::tipb::ScalarFuncSig,
    cmp_string_as_date: tidb_proto::tipb::ScalarFuncSig,
    cmp_string_as_time: tidb_proto::tipb::ScalarFuncSig,
    string: tidb_proto::tipb::ScalarFuncSig,
    duration: tidb_proto::tipb::ScalarFuncSig,
    date: tidb_proto::tipb::ScalarFuncSig,
    time: tidb_proto::tipb::ScalarFuncSig,
    vector_float32: crate::scalar_function::Kernel,
}

/// Go `greatestFunctionClass.getFunction` and `leastFunctionClass.getFunction`,
/// which differ in their signatures only.
fn extremum_function(
    ctx: &dyn crate::builtin::BuildContext,
    func_name: &str,
    args: Vec<Expression>,
    sigs: &ExtremumSigs,
) -> Result<crate::builtin::BaseBuiltinFunc, EvalError> {
    let (res_field_type, ret_date, cmp_string_mode) = resolve_type_4_extremum(&args);
    let mut res_tp = res_field_type.eval_type();
    let mut arg_tp = res_tp;
    if cmp_string_mode != GlCmpStringMode::Directly {
        arg_tp = EvalType::String;
    } else if res_tp == EvalType::Json {
        unsupported_json_comparison(ctx, &args);
        arg_tp = EvalType::String;
        res_tp = EvalType::String;
    }
    let arg_tps = vec![arg_tp; args.len()];
    let mut bf =
        crate::builtin::new_base_builtin_func_with_tp(ctx, func_name, args, res_tp, &arg_tps)?;
    bf.pb_code = match arg_tp {
        EvalType::Int => {
            bf.tp.add_flags(res_field_type.flags());
            sigs.int
        }
        EvalType::Real => sigs.real,
        EvalType::Decimal => sigs.decimal,
        EvalType::String => match cmp_string_mode {
            GlCmpStringMode::AsDate => sigs.cmp_string_as_date,
            GlCmpStringMode::AsDatetime => sigs.cmp_string_as_time,
            GlCmpStringMode::Directly => sigs.string,
        },
        EvalType::Duration => sigs.duration,
        EvalType::Datetime | EvalType::Timestamp => {
            if ret_date {
                sigs.date
            } else {
                sigs.time
            }
        }
        // Go sets no pb code for the vector signatures.
        EvalType::VectorFloat32 => {
            bf.kernel = Some(sigs.vector_float32);
            tidb_proto::tipb::ScalarFuncSig::Unspecified
        }
        EvalType::Json => {
            return Err(EvalError::Unsupported("unsupported type json during evaluation"))
        }
    };
    let (flen, decimal) = fix_flen_and_decimal_for_greatest_and_least(&bf.args);
    bf.tp = bf.get_ret_tp();
    bf.tp.set_flen_under_limit(flen);
    bf.tp.set_decimal_under_limit(decimal);
    Ok(bf)
}

/// Go `greatestFunctionClass`.
pub(crate) struct GreatestFunctionClass;

impl crate::builtin::FunctionClass for GreatestFunctionClass {
    fn get_function(
        &self,
        ctx: &dyn crate::builtin::BuildContext,
        args: Vec<Expression>,
    ) -> Result<crate::builtin::BaseBuiltinFunc, EvalError> {
        use tidb_proto::tipb::ScalarFuncSig;
        extremum_function(
            ctx,
            "greatest",
            args,
            &ExtremumSigs {
                int: ScalarFuncSig::GreatestInt,
                real: ScalarFuncSig::GreatestReal,
                decimal: ScalarFuncSig::GreatestDecimal,
                cmp_string_as_date: ScalarFuncSig::GreatestCmpStringAsDate,
                cmp_string_as_time: ScalarFuncSig::GreatestCmpStringAsTime,
                string: ScalarFuncSig::GreatestString,
                duration: ScalarFuncSig::GreatestDuration,
                date: ScalarFuncSig::GreatestDate,
                time: ScalarFuncSig::GreatestTime,
                vector_float32: crate::scalar_function::Kernel::Method(crate::scalar_function::greatest_method),
            },
        )
    }
}

/// Go `leastFunctionClass`.
pub(crate) struct LeastFunctionClass;

impl crate::builtin::FunctionClass for LeastFunctionClass {
    fn get_function(
        &self,
        ctx: &dyn crate::builtin::BuildContext,
        args: Vec<Expression>,
    ) -> Result<crate::builtin::BaseBuiltinFunc, EvalError> {
        use tidb_proto::tipb::ScalarFuncSig;
        extremum_function(
            ctx,
            "least",
            args,
            &ExtremumSigs {
                int: ScalarFuncSig::LeastInt,
                real: ScalarFuncSig::LeastReal,
                decimal: ScalarFuncSig::LeastDecimal,
                cmp_string_as_date: ScalarFuncSig::LeastCmpStringAsDate,
                cmp_string_as_time: ScalarFuncSig::LeastCmpStringAsTime,
                string: ScalarFuncSig::LeastString,
                duration: ScalarFuncSig::LeastDuration,
                date: ScalarFuncSig::LeastDate,
                time: ScalarFuncSig::LeastTime,
                vector_float32: crate::scalar_function::Kernel::Method(crate::scalar_function::least_method),
            },
        )
    }
}

/// Go `intervalFunctionClass`.
pub(crate) struct IntervalFunctionClass;

impl crate::builtin::FunctionClass for IntervalFunctionClass {
    fn get_function(
        &self,
        ctx: &dyn crate::builtin::BuildContext,
        args: Vec<Expression>,
    ) -> Result<crate::builtin::BaseBuiltinFunc, EvalError> {
        use tidb_proto::tipb::ScalarFuncSig;
        let all_int = args
            .iter()
            .all(|arg| crate::wrap_cast::type_of(arg).eval_type() == EvalType::Int);
        let arg_tp = if all_int { EvalType::Int } else { EvalType::Real };
        let arg_tps = vec![arg_tp; args.len()];
        let mut bf = crate::builtin::new_base_builtin_func_with_tp(
            ctx,
            "interval",
            args,
            EvalType::Int,
            &arg_tps,
        )?;
        bf.pb_code = if all_int {
            ScalarFuncSig::IntervalInt
        } else {
            ScalarFuncSig::IntervalReal
        };
        Ok(bf)
    }
}

/// Go `compareFunctionClass`, one per comparison operator.
pub(crate) struct CompareFunctionClass {
    /// Go `funcName`.
    pub(crate) func_name: &'static str,
}

/// The comparison at `expr` rebuilt through its function class in `ctx`,
/// for a test whose resolver built it without a statement context.
#[cfg(test)]
pub(crate) fn refine_comparison(expr: &mut Expression, ctx: &dyn Columns) -> Result<(), EvalError> {
    let Expression::ScalarFunction(function) = expr else {
        return Ok(());
    };
    let name = function.func_name.lowercase().to_owned();
    if symmetric_op(&name).is_none() || function.args.len() != 2 {
        return Ok(());
    }
    let class = crate::builtin::funcs(&name).expect("a comparison class");
    let f = class.get_function(
        &crate::builtin::ColumnsBuildContext(ctx),
        std::mem::take(&mut function.args),
    )?;
    *function = crate::new_function::scalar_function_of(
        &name,
        FieldType::new(FieldTypeCode::Unspecified),
        f,
    );
    Ok(())
}

/// [`refine_comparison`] over every comparison of the tree at `expr`.
#[cfg(test)]
pub(crate) fn refine_comparisons(expr: &mut Expression, ctx: &dyn Columns) -> Result<(), EvalError> {
    if let Expression::ScalarFunction(function) = expr {
        for arg in &mut function.args {
            refine_comparisons(arg, ctx)?;
        }
    }
    refine_comparison(expr, ctx)
}

impl CompareFunctionClass {
    /// Go `refineArgs`.
    fn refine_args(
        &self,
        ctx: &dyn crate::builtin::BuildContext,
        args: &mut Vec<Expression>,
    ) -> Result<(), EvalError> {
        let name = self.func_name;
        let Some(columns) = ctx.columns() else {
            refine_integer_comparison_for_rewrite(name, args);
            return Ok(());
        };
        if !allow_comparison_refinement(columns, args)? {
            return Ok(());
        }
        crate::expr_util::remove_mutable_const(args, columns)?;
        let [left, right] = args.as_mut_slice() else {
            unreachable!("verifyArgs admits two arguments")
        };
        if name == "nulleq" && rewrite_invalid_duration_null_eq(left, right, columns)? {
            return Ok(());
        }
        let mirrored = symmetric_op(name).expect("a comparison operator");
        refine_args(left, right, name, mirrored, columns);
        Ok(())
    }
}

impl crate::builtin::FunctionClass for CompareFunctionClass {
    fn get_function(
        &self,
        ctx: &dyn crate::builtin::BuildContext,
        mut args: Vec<Expression>,
    ) -> Result<crate::builtin::BaseBuiltinFunc, EvalError> {
        use tidb_proto::tipb::ScalarFuncSig as Sig;

        self.refine_args(ctx, &mut args)?;
        let arg_types: Vec<FieldType> = args.iter().map(crate::wrap_cast::type_of).collect();
        let operand = |index: usize| CmpOperand {
            field_type: &arg_types[index],
            // Go's `NewFunction` folds a wholly-constant subtree before
            // `GetAccurateCmpType` sees it; the Rust rewriter keeps that
            // subtree as a scalar node.
            is_constant: crate::constant_fold::folds_to_constant(&args[index]),
            is_column: matches!(args[index], Expression::Column(_)),
        };
        let tp = get_accurate_cmp_type(operand(0), operand(1));

        // Go `generateCmpSigs`.
        let mut bf = crate::builtin::new_base_builtin_func_with_tp(
            ctx,
            self.func_name,
            args,
            EvalType::Int,
            &[tp, tp],
        )?;
        if tp == EvalType::Json {
            // In compare, if we cast string to JSON, we shouldn't parse it.
            for arg in &mut bf.args {
                crate::expr_util::predicates::disable_parse_json_flag_4_expr(arg);
            }
        }
        bf.tp.set_flen(1);
        // lt, le, gt, eq, ge, ne, nulleq
        let sigs = match tp {
            EvalType::Int => [
                Sig::LtInt, Sig::LeInt, Sig::GtInt, Sig::EqInt, Sig::GeInt, Sig::NeInt,
                Sig::NullEqInt,
            ],
            EvalType::Real => [
                Sig::LtReal, Sig::LeReal, Sig::GtReal, Sig::EqReal, Sig::GeReal, Sig::NeReal,
                Sig::NullEqReal,
            ],
            EvalType::Decimal => [
                Sig::LtDecimal, Sig::LeDecimal, Sig::GtDecimal, Sig::EqDecimal, Sig::GeDecimal,
                Sig::NeDecimal, Sig::NullEqDecimal,
            ],
            EvalType::String => [
                Sig::LtString, Sig::LeString, Sig::GtString, Sig::EqString, Sig::GeString,
                Sig::NeString, Sig::NullEqString,
            ],
            EvalType::Duration => [
                Sig::LtDuration, Sig::LeDuration, Sig::GtDuration, Sig::EqDuration,
                Sig::GeDuration, Sig::NeDuration, Sig::NullEqDuration,
            ],
            EvalType::Datetime | EvalType::Timestamp => [
                Sig::LtTime, Sig::LeTime, Sig::GtTime, Sig::EqTime, Sig::GeTime, Sig::NeTime,
                Sig::NullEqTime,
            ],
            EvalType::Json => [
                Sig::LtJson, Sig::LeJson, Sig::GtJson, Sig::EqJson, Sig::GeJson, Sig::NeJson,
                Sig::NullEqJson,
            ],
            EvalType::VectorFloat32 => [
                Sig::LtVectorFloat32, Sig::LeVectorFloat32, Sig::GtVectorFloat32,
                Sig::EqVectorFloat32, Sig::GeVectorFloat32, Sig::NeVectorFloat32,
                Sig::NullEqVectorFloat32,
            ],
        };
        bf.pb_code = match self.func_name {
            "lt" => sigs[0],
            "le" => sigs[1],
            "gt" => sigs[2],
            "eq" => sigs[3],
            "ge" => sigs[4],
            "ne" => sigs[5],
            "nulleq" => sigs[6],
            other => unreachable!("{other} is not a comparison operator"),
        };
        Ok(bf)
    }
}

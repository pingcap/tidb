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

//! `pkg/expression/builtin_compare.go`: the comparison operators' result-type
//! derivation (`compareFunctionClass.getFunction` / `generateCmpSigs`),
//! transcreated as [`infer_compare_type`].
//!
//! In Go the comparison operand type (`GetAccurateCmpType`) only selects the
//! *signature* (which eval path compares the operands); the *result*
//! `FieldType` is always the ETInt base type from
//! `newReturnFieldTypeForBaseBuiltinFunc` (TypeLonglong, BinaryFlag, flen
//! MaxIntWidth) with `bf.tp.SetFlen(1)`, plus `mysql.IsBooleanFlag` because
//! every comparison name is in the `booleanFunctions` map
//! (`function_traits.go`).
//!
//! `refineArgs`' constant refinement (`int non-constant <cmp> non-int
//! constant`) IS ported, as [`refine_comparisons`] over the built tree --
//! see that function for the Go it follows and for what within `refineArgs`
//! is still deferred. The JSON `DisableParseJSONFlag4Expr` tweak, which also
//! touches only the args, is not ported.

use crate::builtin_arithmetic::new_return_field_type;
use crate::constant::Constant;
use crate::context::Columns;
use crate::expression::Expression;
use tidb_datatype::{
    Datum, EvalType, FieldType, FieldTypeCode, FieldTypeFlags, ScalarConversionEvent,
};

/// The result `FieldType` Go's `compareFunctionClass.getFunction` derives, for
/// the comparison scalar-function `name` (`eq`/`nulleq`/`ne`/`lt`/`le`/`gt`/
/// `ge`). Returns `None` for any other function name.
#[must_use]
pub fn infer_compare_type(name: &str) -> Option<FieldType> {
    match name {
        "eq" | "nulleq" | "ne" | "lt" | "le" | "gt" | "ge" => {
            let mut ret = new_return_field_type(EvalType::Int);
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
    let text = text.trim();
    if tidb_datatype::str_to_float(text, false).event.is_none() {
        return Ok(());
    }
    ctx.handle_truncate(&format!("Truncated incorrect DOUBLE value: '{text}'"))
        .map_err(|_| ())
}

/// Go `Datum.compareString`'s default arm (`pkg/types/datum.go:887-892`) for
/// an INT receiver: the string operand is read as a double -- raising the
/// truncation a SECOND time -- and the two are compared as doubles
/// (`compareFloat64`, which widens the int the same way).
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
    let left = int_datum.to_f64().map_err(|_| ())?.value;
    let right = original.to_f64().map_err(|_| ())?.value;
    left.partial_cmp(&right).ok_or(())
}

/// Go `RefineComparedConstant` (`pkg/expression/builtin_compare.go:1574`):
/// the non-int `con` re-expressed in the int column's own type, so the
/// comparison that survives is int-to-int.
///
/// `None` is Go's `return con, false` -- the comparison is left exactly as
/// written. Both of Go's `isExceptional` outcomes (an overflowing constant,
/// and an `EQ`/`NullEQ` against a value with a non-zero fraction) answer
/// `None` here rather than Go's `[]Expression{NewZero(), NewOne()}` rewrite.
/// That rewrite is a pure OPTIMIZATION -- Go itself applies it only when the
/// column carries `NotNullFlag` (`:1815-1820`), and the unrefined per-row
/// comparison computes the same rows either way -- so leaving it out costs
/// speed, never an answer.
fn refine_compared_constant(
    ctx: &dyn Columns,
    target: &FieldType,
    con: &Constant,
    op: &str,
) -> Option<Constant> {
    if con.value.is_null() {
        return None;
    }
    // `:1580-1582`: a BIT column is refined against LONGLONG instead.
    let target = if matches!(target.code(), FieldTypeCode::Bit) {
        &FieldType::new(FieldTypeCode::LongLong)
    } else {
        target
    };

    // `:1585-1587`: AllowNegativeToUnsigned off, so an underflow saturates at
    // 0 instead of wrapping.
    let flags = tidb_datatype::DEFAULT_STATEMENT_FLAGS.with_allow_negative_to_unsigned(false);
    let converted = con.value.convert_to(target, flags).ok()?;
    if matches!(converted.event, Some(ScalarConversionEvent::Overflow(_))) {
        return None;
    }
    // The FIRST warning: the string->int conversion runs the float-prefix
    // scan (`getValidIntPrefix`'s non-cast arm), which raises 1292.
    note_string_truncation(ctx, &con.value).ok()?;
    let int_datum = converted.value;

    // Go carries `DeferredExpr`/`ParamMarker`/`SubqueryRefID` across onto the
    // refined constant (`:1602-1608`); only the value and type change.
    let int_constant = |value: Datum| {
        let mut refined = Constant::new(value, target.clone());
        refined.deferred_expr = con.deferred_expr.clone();
        refined.param_marker = con.param_marker;
        refined.subquery_ref_id = con.subquery_ref_id;
        refined
    };

    // `:1600-1609`: an exact constant is already the answer.
    let ordering = compare_int_with_constant(ctx, &int_datum, &con.value).ok()?;
    if ordering == std::cmp::Ordering::Equal {
        return Some(int_constant(int_datum));
    }

    match op {
        // `:1613-1619`: the operator picks the rounding direction, so that
        // `a < 1.1` and `a >= 1.1` mean `a < 2`/`a >= 2` while `a <= 1.1`
        // and `a > 1.1` mean `a <= 1`/`a > 1`.
        "lt" | "ge" | "le" | "gt" => {
            let value = con.value.to_f64().ok()?.value;
            // Reading the string as a double is the fold's own coercion, and
            // it raises the truncation once more when the string is partial.
            note_string_truncation(ctx, &con.value).ok()?;
            let folded = if matches!(op, "lt" | "ge") {
                value.ceil()
            } else {
                value.floor()
            };
            // Go `tryToConvertConstantInt` (`:1516`): the folded REAL becomes
            // the column's int type, and an overflow there leaves the
            // comparison unrefined for the same reason as above.
            let folded = Datum::Real(folded).convert_to(target, flags).ok()?;
            if folded.event.is_some() {
                return None;
            }
            Some(int_constant(folded.value))
        }
        // `:1622-1657`: Go's `EQ`/`NullEQ` case switches on the CONSTANT's
        // own eval type, and the two halves are not the same rule.
        //
        // ```go
        // case opcode.NullEQ, opcode.EQ:
        //     switch con.GetType(ctx.GetEvalCtx()).EvalType() {
        //     case types.ETReal, types.ETDecimal:
        //         return con, true
        //     case types.ETString:
        //         ...
        //         doubleDatum, err = dt.ConvertTo(evalCtx.TypeCtx(), types.NewFieldType(mysql.TypeDouble))
        //         if err != nil { return con, false }
        //         if doubleDatum.GetFloat64() != math.Trunc(doubleDatum.GetFloat64()) {
        //             return con, true
        //         }
        //         return &Constant{Value: intDatum, RetType: &targetFieldType, ...}, false
        //     }
        // ```
        //
        // A REAL/DECIMAL constant is Go's `isExceptional` -- `int = 1.1` is
        // definitely false -- which this port answers by leaving the
        // comparison alone (see the doc above). A STRING constant is NOT:
        // when the string reads as a WHOLE double it is refined to
        // `intDatum`, and Go's own comment says which target that arm exists
        // for:
        //
        // ```text
        // 2. When `targetFieldType.GetType()` is `TypeYear`, we can not compare `doubleDatum` with `intDatum` directly,
        //    because we'll convert values in the ranges '0' to '69' and '70' to '99' to YEAR values in the ranges
        //    2000 to 2069 and 1970 to 1999.
        // 3. Suppose the value of `con` is 2, when `targetFieldType.GetType()` is `TypeYear`, the value of `doubleDatum`
        //    will be 2.0 and the value of `intDatum` will be 2002 in this case.
        // ```
        //
        // On an ordinary INT target the arm is unreachable for a whole
        // string: `intDatum` and the string then compare EQUAL and
        // `RefineComparedConstant` has already returned above. A YEAR target
        // is the case where they differ WITHOUT the constant being inexact,
        // because `convert_to_year` applied the two-digit window -- `y = '18'`
        // converts to 2018 while the string reads as 18.0.
        "eq" | "nulleq" => {
            // A `None` ret_type is a nil `*types.FieldType`, which Go never
            // builds here; it takes the unrefined answer.
            if con.ret_type.as_ref().map(FieldType::eval_type) != Some(EvalType::String) {
                return None;
            }
            // The fold's own string->double coercion, raising the truncation
            // once more when the string is partial -- the same second warning
            // the `Floor`/`Ceil` arm above raises.
            let value = con.value.to_f64().ok()?.value;
            note_string_truncation(ctx, &con.value).ok()?;
            if value != value.trunc() {
                return None;
            }
            Some(int_constant(int_datum))
        }
        // Go's switch has no `NE` arm: a `!=` whose constant is inexact
        // falls through to `return con, false`.
        _ => None,
    }
}

/// Whether `expr` reads a column, i.e. is NOT a constant expression.
///
/// Go's `refineArgs` distinguishes its two sides by `args[i].(*Constant)`,
/// and a `CAST` of a literal is a folded `*Constant` there. This tier folds
/// a constant subtree in place (`constant_fold`) but does NOT rewrite the
/// node into a `Constant`, so `cast('2023-08-09' as datetime)` stays a
/// `ScalarFunction`. Matching Go's constant/non-constant split therefore
/// means asking whether the expression depends on a row -- a column-free
/// datetime expression (a folded `CAST`) is the constant side and does not
/// trigger rule 3, which is why `cast(...) > 20230809` compares as REAL
/// (both constants) while `datetime_col > 20230809` compares as datetime.
fn reads_column(expr: &Expression) -> bool {
    match expr {
        Expression::Column(_) | Expression::CorrelatedColumn(_) => true,
        Expression::Constant(_) => false,
        Expression::ScalarFunction(function) => function.args.iter().any(reads_column),
    }
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
fn refine_numeric_constant_cmp_datetime(left: &mut Expression, right: &mut Expression) -> bool {
    // The datetime side must be a non-constant DATETIME/TIMESTAMP expression;
    // the other side a constant whose eval type is Int/Real/Decimal.
    let is_datetime_expr = |expr: &Expression| {
        expr.static_type().is_some_and(|ft| {
            matches!(
                ft.code(),
                FieldTypeCode::Datetime | FieldTypeCode::Timestamp
            )
        }) && reads_column(expr)
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
    if con.value.is_null() {
        return true;
    }
    // Go `dt.ConvertTo(ctx.TypeCtx(), NewFieldType(TypeDatetime))`: an invalid
    // datetime (e.g. `20231310`, month 13) errors and leaves the comparison
    // unrefined. TypeDatetime carries no zone dependence, so the default UTC
    // conversion matches.
    let target = FieldType::new(FieldTypeCode::Datetime);
    let flags = tidb_datatype::DEFAULT_STATEMENT_FLAGS;
    match con.value.convert_to(&target, flags) {
        Ok(converted) if !converted.value.is_null() => {
            *constant = Expression::Constant(Constant::new(converted.value, target));
        }
        _ => {}
    }
    true
}

/// Go `generateCmpSigs`' argument wrapping for the ONE operand pair whose
/// comparison domain the value evaluator cannot recover from the datums:
/// a `DATE`/`DATETIME`/`TIMESTAMP` expression compared with a `YEAR` one.
///
/// `getBaseCmpType` (`builtin_compare.go:1424-1427`) is where the pair is
/// decided, and this is its LAST arm before the ETReal fallback:
///
/// ```go
/// } else if lft != nil && rft != nil && (types.IsTemporalWithDate(lft.GetType()) && rft.GetType() == mysql.TypeYear ||
///     lft.GetType() == mysql.TypeYear && types.IsTemporalWithDate(rft.GetType())) {
///     return types.ETDatetime
/// }
/// ```
///
/// `types.IsTemporalWithDate` is `IsTypeTime` (`pkg/types/etc.go:119-121`):
/// `TypeDatetime`, `TypeDate`, `TypeTimestamp` and nothing else.
/// `GetAccurateCmpType` leaves that ETDatetime alone -- its own arms fire only
/// for JSON, vector, ETString or ETReal -- and `generateCmpSigs` then calls
/// `newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETInt, tp, tp)` with
/// `tp == ETDatetime`, which runs `WrapWithCastAsTime` over both arguments.
///
/// `WrapWithCastAsTime` (`builtin_cast.go:2817-2823`) inserts NOTHING on the
/// temporal side -- `tp.GetType() == exprTp` for a DATETIME, and the explicit
/// `(exprTp == mysql.TypeDate || exprTp == mysql.TypeTimestamp) && tp.GetType()
/// == mysql.TypeDatetime` early return covers the other two -- so this wraps
/// only the YEAR side, exactly as Go does. The cast itself is the YEAR-source
/// branch of `cast::eval_cast`'s time path.
///
/// # Why the value evaluator cannot do this on its own
///
/// A YEAR column reaches it as a plain `Datum::Int`, indistinguishable from a
/// `BIGINT`'s -- and `getBaseCmpType` sends DATETIME-vs-BIGINT to ETReal, the
/// opposite domain. Only the FIELD TYPE separates the two, and the field type
/// exists only here. Left alone, the comparison decided every row by
/// `20000503164444 < 2018`, i.e. by the operator alone: `<` returned no rows
/// and `>` returned all of them, with no error and no warning.
fn wrap_year_operand_for_datetime_compare(left: &mut Expression, right: &mut Expression) {
    let code = |expr: &Expression| expr.static_type().map(FieldType::code);
    let is_temporal_with_date = |expr: &Expression| {
        matches!(
            code(expr),
            Some(FieldTypeCode::Datetime | FieldTypeCode::Date | FieldTypeCode::Timestamp)
        )
    };
    let is_year = |expr: &Expression| code(expr) == Some(FieldTypeCode::Year);

    let year_side = if is_temporal_with_date(left) && is_year(right) {
        right
    } else if is_year(left) && is_temporal_with_date(right) {
        left
    } else {
        return;
    };
    let mut target = FieldType::new(FieldTypeCode::Datetime);
    // `WrapWithCastAsTime`'s own width/scale for an ETInt source:
    // `tp.SetDecimal(types.MinFsp)` then `tp.SetFlen(mysql.MaxDatetimeWidthNoFsp)`.
    target.set_decimal(0);
    target.set_flen(19);
    let arg = std::mem::replace(
        year_side,
        Expression::Constant(Constant::new(
            Datum::Null,
            FieldType::new(FieldTypeCode::Null),
        )),
    );
    *year_side = Expression::ScalarFunction(crate::expression::ScalarFunction::new(
        tidb_ast::CiString::new("cast_datetime"),
        target,
        vec![arg],
    ));
}

/// Go `newBaseBuiltinFuncWithTp(..., ETDecimal, ETDecimal)` wrapping the
/// integer side of a DECIMAL-vs-INT comparison with
/// `WrapWithCastAsDecimal`. The integer width comes from the MySQL type, not
/// from the value currently stored in the row (`BIGINT` therefore needs
/// DECIMAL(20,0)).
fn wrap_integer_operand_for_decimal_compare(left: &mut Expression, right: &mut Expression) {
    let eval_type = |expression: &Expression| expression.static_type().map(FieldType::eval_type);
    let integer = match (eval_type(left), eval_type(right)) {
        (Some(EvalType::Decimal), Some(EvalType::Int))
            if !matches!(right, Expression::Constant(_)) =>
        {
            right
        }
        (Some(EvalType::Int), Some(EvalType::Decimal))
            if !matches!(left, Expression::Constant(_)) =>
        {
            left
        }
        _ => return,
    };
    let Some(source_type) = integer.static_type().cloned() else {
        return;
    };
    let precision = match source_type.code() {
        FieldTypeCode::Tiny => 3,
        FieldTypeCode::Short => 5,
        FieldTypeCode::Int24 => 8,
        FieldTypeCode::Long => 10,
        FieldTypeCode::LongLong => 20,
        FieldTypeCode::Year => 4,
        _ => return,
    };
    let mut target = FieldType::new(FieldTypeCode::NewDecimal);
    target.set_flen(precision);
    target.set_decimal(0);
    target.add_flags(
        FieldTypeFlags::BINARY
            | (source_type.flags() & (FieldTypeFlags::UNSIGNED | FieldTypeFlags::NOT_NULL)),
    );
    let argument = std::mem::replace(
        integer,
        Expression::Constant(Constant::new(
            Datum::Null,
            FieldType::new(FieldTypeCode::Null),
        )),
    );
    *integer = Expression::ScalarFunction(crate::expression::ScalarFunction::new(
        tidb_ast::CiString::new("cast_decimal"),
        target,
        vec![argument],
    ));
}

/// Go `compareFunctionClass.refineArgs` (`builtin_compare.go:1778`), applied
/// to every comparison in an already-built expression tree.
///
/// Go runs this inside `getFunction` (`:1984`), where the comparison is
/// constructed. This tier builds comparisons in several places (binary
/// operators, `BETWEEN`, the simple `CASE`), none of which holds an
/// evaluation context, so the refinement runs as one pass over the finished
/// tree instead. It reads only the two arguments -- exactly what `refineArgs`
/// reads -- so the placement changes nothing about the result.
///
/// Ported: the `int non-constant [cmp] non-int constant` arm (`:1811-1833`)
/// and its mirror through `symmetricOp` (`:1836-1854`), and the
/// numeric-constant-vs-datetime rule (`refineNumericConstantCmpDatetime`,
/// `:1802-1808`) -- see [`refine_numeric_constant_cmp_datetime`].
/// DEFERRED, each an independent rule of the same function: the `YEAR`
/// adjustment (`:1856-1871`), the `NullEQ`-vs-`DURATION` rewrite
/// (`:1795-1799`), `refineArgsByUnsignedFlag` (`:1919`), and the plan-cache
/// guard `allowCmpArgsRefining4PlanCache` (`:1789`) -- which matters only once
/// refined plans are cached across parameter values.
pub fn refine_comparisons(expr: &mut Expression, ctx: &dyn Columns) {
    let Expression::ScalarFunction(function) = expr else {
        return;
    };
    for arg in &mut function.args {
        refine_comparisons(arg, ctx);
    }
    let name = function.func_name.lowercase();
    let Some(mirrored) = symmetric_op(name) else {
        return;
    };
    let [left, right] = function.args.as_mut_slice() else {
        return;
    };
    refine_args(left, right, name, mirrored, ctx);
    // Go's own order: `getFunction` runs `refineArgs` first and only then
    // derives the comparison type -- and the argument casts that go with it --
    // from the arguments that survived it (`builtin_compare.go:1984-1989`).
    wrap_integer_operand_for_decimal_compare(left, right);
    wrap_year_operand_for_datetime_compare(left, right);
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
    if refine_numeric_constant_cmp_datetime(left, right) {
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
    let (column, constant, op) = match (left, right) {
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
    if let Some(refined) = refine_compared_constant(ctx, &target, constant, op) {
        *constant = refined;
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

    fn string_constant(text: &str) -> Expression {
        Expression::Constant(Constant::new(
            Datum::new_string(text),
            FieldType::new(FieldTypeCode::Varchar),
        ))
    }

    /// `<int column> <op> <constant>`, refined, as `(refined arg, warnings)`.
    fn refine(op: &str, left: Expression, right: Expression) -> (Expression, Vec<String>) {
        let sink = Sink::default();
        let mut expr = Expression::ScalarFunction(crate::expression::ScalarFunction::new(
            tidb_ast::CiString::new(op),
            infer_compare_type(op).unwrap(),
            vec![left, right],
        ));
        refine_comparisons(&mut expr, &sink);
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
        assert_eq!(constant_of(&expr, 1), Datum::new_string("10ab"));
        assert!(warnings.is_empty(), "{warnings:?}");

        let int_constant = Expression::Constant(Constant::new(
            Datum::Int(10),
            FieldType::new(FieldTypeCode::LongLong),
        ));
        let (expr, warnings) = refine("gt", int_column(), int_constant);
        assert_eq!(constant_of(&expr, 1), Datum::Int(10));
        assert!(warnings.is_empty(), "{warnings:?}");
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
        assert_eq!(cast.func_name.lowercase(), "cast_decimal");
        let target = cast.ret_type.as_ref().unwrap();
        assert_eq!(target.code(), FieldTypeCode::NewDecimal);
        assert_eq!(target.flen(), 20);
        assert_eq!(target.decimal(), 0);
        assert_ne!(target.flags() & FieldTypeFlags::BINARY, 0);
    }

    /// `EQ` against an inexact value is Go's `isExceptional`, which this port
    /// answers by leaving the comparison alone. The per-row comparison still
    /// computes the right (empty) answer -- captured: `a = '3.5'` returns no
    /// row and warns zero times.
    #[test]
    fn an_inexact_equality_is_left_unrefined() {
        let (expr, _) = refine("eq", int_column(), string_constant("3.5"));
        assert_eq!(constant_of(&expr, 1), Datum::new_string("3.5"));
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

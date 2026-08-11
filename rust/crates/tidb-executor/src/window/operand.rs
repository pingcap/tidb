// Copyright 2025 PingCAP, Inc.
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

//! How ONE operand of a window value function is read.
//!
//! Go splits this across two files that only make sense together:
//! `WrapCastForAggArgs` (`expression/aggregation/base_func.go:496-538`) wraps
//! each argument in a cast chosen by the RESULT type's eval kind, and
//! `buildValueEvaluator` (`executor/aggfuncs/func_value.go:233`) then reads
//! the wrapped argument through that same eval kind. This tier evaluates
//! arguments to datums first, so the two collapse into one conversion --
//! which is why the cast wrappers' many DECLINE-TO-WRAP shortcuts live here
//! rather than at build time.
//!
//! `LAG`/`LEAD`'s written default takes the same road, except for the one
//! step `buildLeadLag` adds for a CONSTANT: `Value.ConvertTo(RetTp)`, which
//! applies the merged type's full display metadata. [`column_free`] is the
//! test that decides which of the two a default gets.

use tidb_datatype::{Datum, EvalType, FieldType, FieldTypeCode, FieldTypeFlags, TimeType};

/// Converts a value into `target`'s DOMAIN, ignoring its display metadata --
/// Go's `buildValueEvaluator(RetTp)`, which reads an argument through the
/// merged type's eval kind (`EvalString`, `EvalDecimal`, ...) rather than
/// through a width-and-scale-applying conversion. That distinction is
/// visible: `LAG(int_col, 1, 1.5)` returns `10`, not the scale-padded
/// `10.0` the full `DECIMAL(12,1)` would produce.
///
/// A TEMPORAL operand is read even more literally than that. Go does not hand
/// the raw operand to the evaluator at all: `WrapCastForAggArgs`
/// (`aggregation/base_func.go:496-538`) wraps it FIRST, and the two temporal
/// wrappers decline to wrap far more often than they wrap --
///
/// ```go
/// func WrapWithCastAsTime(ctx BuildContext, expr Expression, tp *types.FieldType) Expression {
///     exprTp := expr.GetType(ctx.GetEvalCtx()).GetType()
///     if tp.GetType() == exprTp {
///         return expr
///     } else if (exprTp == mysql.TypeDate || exprTp == mysql.TypeTimestamp) && tp.GetType() == mysql.TypeDatetime {
///         return expr
///     }
///     switch x := expr.GetType(ctx.GetEvalCtx()).EvalType(); x {
///     ...
///     case types.ETDatetime, types.ETTimestamp, types.ETDuration:
///         tp.SetDecimal(expr.GetType(ctx.GetEvalCtx()).GetDecimal())
/// ```
///
/// -- and `WrapWithCastAsDuration` returns a `TIME` operand unwrapped the same
/// way. So a temporal operand keeps its own SCALE, and (through the second
/// shortcut) its own KIND: TiDB answers `2020-01-01 10:20:30.123456` for
/// `LAG(datetime6_col, 1)` and a bare `2020-03-04` for the DATE operand of a
/// `LAG(date_col, 1, time3_col)` whose result type is `datetime(3)` -- both
/// captured via `gorun`. Where the wrappers DO wrap, the cast's fsp is taken
/// from the SOURCE, never from the merged result, which is why
/// `LAG(time3_col, 1, date_col)` answers `... 01:02:03.456` and not the
/// result type's six digits.
///
/// This applies to the DEFAULT operand too, and for the same reason: only a
/// CONSTANT default gets `Value.ConvertTo(RetTp)` in `buildLeadLag`, and a
/// constant reaches here as a string or a number rather than as a temporal
/// datum.
pub(super) fn coerce_to_domain(value: Datum, target: &FieldType) -> Datum {
    let temporal_target = matches!(
        target.eval_type(),
        EvalType::Datetime | EvalType::Timestamp | EvalType::Duration
    );
    if temporal_target {
        let unwrapped = match &value {
            Datum::Time(time) => {
                time.kind() == time_kind_of(target.code())
                    // `WrapWithCastAsTime`'s second shortcut.
                    || (target.code() == FieldTypeCode::Datetime
                        && matches!(time.kind(), TimeType::Date | TimeType::Timestamp))
            }
            // `WrapWithCastAsDuration`'s only shortcut.
            Datum::Duration(_) => target.code() == FieldTypeCode::Duration,
            _ => false,
        };
        if unwrapped {
            return value;
        }
    }
    let mut domain = FieldType::new(target.code());
    domain.set_flen(tidb_datatype::UNSPECIFIED_LENGTH);
    // The cast Go builds when it does wrap takes its fsp from the SOURCE. The
    // two wrappers' tables agree on every temporal source, and a temporal
    // result type can only be reached from a temporal operand, so the
    // temporal rows are the whole reachable table.
    domain.set_decimal(match (&value, temporal_target) {
        (Datum::Time(time), true) => i64::from(time.fsp()),
        (Datum::Duration(duration), true) => duration.fsp(),
        _ => tidb_datatype::UNSPECIFIED_FSP,
    });
    if target.is_unsigned() {
        domain.add_flags(FieldTypeFlags::UNSIGNED);
    }
    coerce_to_type(value, &domain)
}

/// Whether an expression reads no COLUMN, which is exactly when Go's
/// `FoldConstant` has already collapsed it into an `*expression.Constant` by
/// the time `buildLeadLag` type-asserts on it. A non-deterministic call is
/// still folded (into a constant carrying a deferred expression), which is
/// why `LAG(datetime6_col, 1, NOW())` pads its default to `.000000` exactly
/// like a written literal would -- captured from TiDB via `gorun`.
pub(super) fn column_free(expr: &tidb_expr::expression::Expression) -> bool {
    use tidb_expr::expression::Expression;
    match expr {
        Expression::Column(_) | Expression::CorrelatedColumn(_) => false,
        Expression::Constant(_) => true,
        Expression::ScalarFunction(function) => function.args.iter().all(column_free),
    }
}

/// The `TimeType` a temporal field-type code stores, so a `DATE` operand and
/// a `DATE` result compare equal the way Go compares `tp.GetType()` with the
/// expression's own `GetType()`.
fn time_kind_of(code: FieldTypeCode) -> TimeType {
    match code {
        FieldTypeCode::Date => TimeType::Date,
        FieldTypeCode::Timestamp => TimeType::Timestamp,
        _ => TimeType::DateTime,
    }
}

/// Converts a value into `target` exactly, leaving it untouched when the
/// conversion fails (Go's `buildLeadLag` keeps the original constant when
/// `ConvertTo` errors).
pub(super) fn coerce_to_type(value: Datum, target: &FieldType) -> Datum {
    if value.is_null() {
        return value;
    }
    // An INTEGER result is never range-converted, in EITHER role. Go's
    // `value4Int` stores what `EvalInt` handed it -- the raw 64-bit pattern
    // -- and appends it with `AppendInt64`, leaving the RESULT column's
    // UNSIGNED flag to decide how it prints; and a constant default whose
    // `ConvertTo` overflows is KEPT as written and then read the same way.
    // Both roads end at the bit pattern, never at a saturated bound, and the
    // merged type is exactly where the two signednesses meet:
    // `LAG(bigint_unsigned_not_null, 1, other_unsigned_not_null)` merges to a
    // SIGNED result (Go's `SetFlag(NotNullFlag)` drops UNSIGNED) and TiDB
    // answers `-6`/`-1`, not the clamped `9223372036854775807`.
    if target.eval_type() == EvalType::Int {
        match (&value, target.is_unsigned()) {
            (Datum::UInt(bits), false) => return Datum::Int(*bits as i64),
            (Datum::Int(bits), true) => return Datum::UInt(*bits as u64),
            (Datum::Int(_) | Datum::UInt(_), _) => return value,
            // A non-integer datum still converts: Go reaches an integer
            // result from one only through `ConvertTo`/`EvalInt`, both of
            // which round rather than reinterpret.
            _ => {}
        }
    }
    match value.convert_to(target, tidb_datatype::DEFAULT_STATEMENT_FLAGS) {
        Ok(converted) => converted.value,
        Err(_) => value,
    }
}

/// Go's `WrapCastForAggArgs` does not hand `WrapWithCastAsTime` a scratch
/// type -- it hands it `a.RetTp`, the RESULT type, and that function writes
/// into it:
///
/// ```go
/// case types.ETDatetime, types.ETTimestamp, types.ETDuration:
///     tp.SetDecimal(expr.GetType(ctx.GetEvalCtx()).GetDecimal())
/// ...
/// case mysql.TypeDatetime, mysql.TypeTimestamp:
///     tp.SetFlen(mysql.MaxDatetimeWidthNoFsp)
///     if tp.GetDecimal() > 0 {
///         tp.SetFlen(tp.GetFlen() + 1 + tp.GetDecimal())
///     }
/// ```
///
/// So an argument the wrapper decides to wrap NARROWS the merged result to
/// its own scale. `desc` over a view of
/// `lag(datetime6_col, 1, time3_col)` reports `datetime(3)`, not the
/// `datetime(6)` `InferType4ControlFuncs` returned (captured via `gorun`) --
/// while the `datetime(6)` operand itself still answers with all six digits,
/// because it is the argument that was NOT wrapped.
///
/// Only a `DATETIME`/`TIMESTAMP` result can be rewritten this way. Go's flen
/// switch has a `DATE` arm as well, but a `DATE` result means both operands
/// were dates, and a date operand is never wrapped into a date result.
pub(super) fn wrap_cast_rewrites_a_temporal_result(
    result: &mut FieldType,
    arg_types: &[Option<FieldType>],
) {
    if !matches!(result.eval_type(), EvalType::Datetime | EvalType::Timestamp) {
        return;
    }
    for arg in arg_types.iter().flatten() {
        // Go's loop skips a NULL-typed argument outright, and
        // `WrapWithCastAsTime` returns the argument unwrapped -- writing
        // nothing -- for the same two shortcuts [`coerce_to_domain`] reads.
        if arg.code() == FieldTypeCode::Null
            || arg.code() == result.code()
            || (result.code() == FieldTypeCode::Datetime
                && matches!(arg.code(), FieldTypeCode::Date | FieldTypeCode::Timestamp))
        {
            continue;
        }
        result.set_decimal(match arg.eval_type() {
            EvalType::Int => 0,
            EvalType::String | EvalType::Real | EvalType::Json => 6,
            EvalType::Datetime | EvalType::Timestamp | EvalType::Duration => arg.decimal(),
            EvalType::Decimal => arg.decimal().min(6),
            EvalType::VectorFloat32 => result.decimal(),
        });
        let decimal = result.decimal();
        result.set_flen(if decimal > 0 { 19 + 1 + decimal } else { 19 });
    }
}

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

//! Go `expression.CompareFunc` for RANGE frame bounds.

use tidb_chunk::{column::Column, row::Row};
use tidb_datatype::{
    compare_int, get_last_day, CoreTime, Datum, FieldTypeCode, MyDecimal, Time, TimeType,
    MYDECIMAL_STRUCT_SIZE,
};
use tidb_driver_error::ExecError;
use tidb_executor_aggfuncs::agg_func::AggFuncUpdateContext;
use tidb_expr::{constant::Constant, expression::Expression, scalar_function::ScalarFunction};
use tidb_planner_core_operator_logicalop::window::FrameBound;

/// Prepared native operands for one RANGE comparison. Other expressions
/// use the ordinary evaluator in the same order as the Go comparison.
pub(crate) enum RangeComparison {
    Integer(IntegerOperand, IntegerOperand),
    Decimal(DecimalOperand, DecimalOperand),
    Time(TimeOperand, TimeOperand),
    Expression,
}

impl RangeComparison {
    pub(crate) fn for_bound(bound: &FrameBound, end: bool) -> Vec<Self> {
        bound
            .compare_cols
            .iter()
            .zip(&bound.calc_funcs)
            .map(|(key, bound)| {
                let (lhs, rhs) = if end { (bound, key) } else { (key, bound) };
                Self::prepare(lhs, rhs)
            })
            .collect()
    }

    fn prepare(lhs: &Expression, rhs: &Expression) -> Self {
        if let Some((lhs, rhs)) = IntegerOperand::prepare(lhs).zip(IntegerOperand::prepare(rhs)) {
            return Self::Integer(lhs, rhs);
        }
        if let Some((lhs, rhs)) = DecimalOperand::prepare(lhs).zip(DecimalOperand::prepare(rhs)) {
            return Self::Decimal(lhs, rhs);
        }
        if let Some((lhs, rhs)) = TimeOperand::prepare(lhs).zip(TimeOperand::prepare(rhs)) {
            return Self::Time(lhs, rhs);
        }
        Self::Expression
    }

    pub(crate) fn compares_same_column(&self) -> bool {
        match self {
            Self::Integer(lhs, rhs) => lhs.same_column(rhs),
            Self::Decimal(lhs, rhs) => {
                lhs.index == rhs.index && lhs.offset.is_none() && rhs.offset.is_none()
            }
            Self::Time(lhs, rhs) => {
                lhs.index == rhs.index && lhs.offset.is_none() && rhs.offset.is_none()
            }
            Self::Expression => false,
        }
    }

    #[inline]
    pub(crate) fn compare(
        &self,
        ctx: &AggFuncUpdateContext,
        lhs: &Expression,
        rhs: &Expression,
        lhs_row: Row<'_>,
        rhs_row: Row<'_>,
    ) -> Result<i64, ExecError> {
        self.compare_tracked(ctx, lhs, rhs, lhs_row, rhs_row, &mut true)
    }

    #[inline]
    pub(crate) fn compare_tracked(
        &self,
        ctx: &AggFuncUpdateContext,
        lhs: &Expression,
        rhs: &Expression,
        lhs_row: Row<'_>,
        rhs_row: Row<'_>,
        native: &mut bool,
    ) -> Result<i64, ExecError> {
        if let Self::Integer(lhs_int, rhs_int) = self {
            if lhs_row == rhs_row
                && lhs_int.same_column(rhs_int)
                && lhs_int.read(ctx, lhs_row).is_some()
            {
                return Ok(0);
            }
            if let (Some(left), Some(right)) =
                (lhs_int.read(ctx, lhs_row), rhs_int.read(ctx, rhs_row))
            {
                return Ok(match (left, right) {
                    (Some(left), Some(right)) => {
                        compare_int(left, lhs_int.unsigned, right, rhs_int.unsigned)
                    }
                    _ => left.cmp(&right) as i64,
                });
            }
        }
        if let Self::Decimal(lhs, rhs) = self {
            if let Some(left) = lhs.read(lhs_row) {
                if lhs_row == rhs_row && self.compares_same_column() {
                    return Ok(0);
                }
                if let Some(right) = rhs.read(rhs_row) {
                    return Ok(match (left, right) {
                        (Some(left), Some(right)) => compare_decimal(left, right),
                        (None, None) => 0,
                        (None, Some(_)) => -1,
                        (Some(_), None) => 1,
                    });
                }
            }
        }
        if let Self::Time(lhs, rhs) = self {
            if let Some(left) = lhs.read(lhs_row) {
                if lhs_row == rhs_row && self.compares_same_column() {
                    return Ok(0);
                }
                if let Some(right) = rhs.read(rhs_row) {
                    return Ok(left.cmp(&right) as i64);
                }
            }
        }
        *native = false;
        let left = lhs.eval(ctx, lhs_row)?;
        let right = rhs.eval(ctx, rhs_row)?;
        Ok(match (&left, &right) {
            (Datum::Null, Datum::Null) => 0,
            (Datum::Null, _) => -1,
            (_, Datum::Null) => 1,
            (Datum::Int(l), Datum::Int(r)) => l.cmp(r) as i64,
            (Datum::UInt(l), Datum::UInt(r)) => l.cmp(r) as i64,
            (Datum::Int(l), Datum::UInt(r)) => compare_int(*l, false, *r as i64, true),
            (Datum::UInt(l), Datum::Int(r)) => compare_int(*l as i64, true, *r, false),
            (Datum::Decimal(l), Datum::Decimal(r)) => l.cmp(r) as i64,
            (Datum::Time(l), Datum::Time(r)) => l.compare(*r) as i64,
            _ => tidb_expr::compare_datums_with_collation(
                &left,
                &right,
                tidb_expr::collation_derive::collation_of_node(lhs),
            )? as i64,
        })
    }
}

pub(crate) struct IntegerOperand {
    index: usize,
    unsigned: bool,
    offset: Offset,
}

enum Offset {
    None,
    Add(i64),
    Subtract(i64),
}

impl IntegerOperand {
    fn same_column(&self, other: &Self) -> bool {
        self.index == other.index
            && self.unsigned == other.unsigned
            && matches!((&self.offset, &other.offset), (Offset::None, Offset::None))
    }

    fn column(expression: &Expression) -> Option<Self> {
        let Expression::Column(column) = expression else {
            return None;
        };
        let ty = column.ret_type.as_ref()?;
        let unsigned = match ty.code() {
            FieldTypeCode::Tiny
            | FieldTypeCode::Short
            | FieldTypeCode::Int24
            | FieldTypeCode::Long
            | FieldTypeCode::LongLong => ty.is_unsigned(),
            FieldTypeCode::Year => false,
            _ => return None,
        };
        Some(Self {
            index: usize::try_from(column.index).ok()?,
            unsigned,
            offset: Offset::None,
        })
    }

    fn prepare(expression: &Expression) -> Option<Self> {
        let Expression::ScalarFunction(function) = expression else {
            return Self::column(expression);
        };
        let [column, Expression::Constant(literal)] = function.args.as_slice() else {
            return None;
        };
        if literal.deferred_expr.is_some() || literal.param_marker.is_some() {
            return None;
        }
        let ty = literal.ret_type.as_ref()?;
        if ty.code() != FieldTypeCode::LongLong || ty.is_unsigned() {
            return None;
        }
        let Datum::Int(value) = literal.value else {
            return None;
        };
        if value < 0 {
            return None;
        }
        let mut operand = Self::column(column)?;
        if column.static_type()?.code() != FieldTypeCode::LongLong {
            return None;
        }
        let result = function.ret_type.as_ref()?;
        if result.code() != FieldTypeCode::LongLong || result.is_unsigned() != operand.unsigned {
            return None;
        }
        operand.offset = match function.pb_code().as_str_name() {
            "PlusInt" => Offset::Add(value),
            "MinusInt" => Offset::Subtract(value),
            _ => return None,
        };
        Some(operand)
    }

    /// Outer None requests regular evaluation, including its overflow errors.
    /// Inner None is a SQL NULL candidate or bound.
    #[inline]
    fn read(&self, ctx: &AggFuncUpdateContext, row: Row<'_>) -> Option<Option<i64>> {
        let chunk = row.chunk().filter(|chunk| self.index < chunk.num_cols())?;
        let column = chunk.column(self.index);
        if column.type_size() != 8 {
            return None;
        }
        let index = row.idx();
        if column.is_null(index) {
            return Some(None);
        }
        let value = column.get_int64(index);
        let result = match self.offset {
            Offset::None => value,
            Offset::Add(n) if self.unsigned => (value as u64).checked_add(n as u64)? as i64,
            Offset::Subtract(n) if self.unsigned => {
                if ctx.no_unsigned_subtraction() {
                    return None;
                }
                (value as u64).checked_sub(n as u64)? as i64
            }
            Offset::Add(n) => value.checked_add(n)?,
            Offset::Subtract(n) => value.checked_sub(n)?,
        };
        Some(Some(result))
    }
}

fn fixed_literal(expression: &Expression) -> Option<&Constant> {
    let Expression::Constant(literal) = expression else {
        return None;
    };
    (literal.deferred_expr.is_none() && literal.param_marker.is_none()).then_some(literal)
}

fn column_index(
    expression: &Expression,
    supported: impl FnOnce(FieldTypeCode) -> bool,
) -> Option<usize> {
    let Expression::Column(column) = expression else {
        return None;
    };
    if !supported(column.ret_type.as_ref()?.code()) {
        return None;
    }
    usize::try_from(column.index).ok()
}

#[inline]
fn read_fixed<T>(
    row: Row<'_>,
    index: usize,
    width: i64,
    read: impl FnOnce(&Column, usize) -> Option<T>,
) -> Option<Option<T>> {
    let chunk = row.chunk().filter(|chunk| index < chunk.num_cols())?;
    let column = chunk.column(index);
    if column.type_size() != width {
        return None;
    }
    if column.is_null(row.idx()) {
        return Some(None);
    }
    read(&column, row.idx()).map(Some)
}

pub(crate) struct DecimalOperand {
    index: usize,
    offset: Option<i32>,
}

impl DecimalOperand {
    fn prepare(expression: &Expression) -> Option<Self> {
        let decimal = |code| code == FieldTypeCode::NewDecimal;
        let Expression::ScalarFunction(function) = expression else {
            return Some(Self {
                index: column_index(expression, decimal)?,
                offset: None,
            });
        };
        let [column, literal] = function.args.as_slice() else {
            return None;
        };
        if !decimal(function.ret_type.as_ref()?.code()) {
            return None;
        }
        let literal = fixed_literal(literal)?;
        let literal_type = literal.ret_type.as_ref()?;
        let offset = match &literal.value {
            Datum::Int(n)
                if literal_type.code() == FieldTypeCode::LongLong
                    && !literal_type.is_unsigned() =>
            {
                i32::try_from(*n).ok()?
            }
            Datum::Decimal(operand) if literal_type.code() == FieldTypeCode::NewDecimal => {
                let (coefficient, scale) = operand.coefficient_i128()?;
                if scale != 0 {
                    return None;
                }
                i32::try_from(coefficient).ok()?
            }
            _ => return None,
        };
        if !(0..1_000_000_000).contains(&offset) {
            return None;
        }
        let offset = match function.pb_code().as_str_name() {
            "PlusDecimal" => offset,
            "MinusDecimal" => -offset,
            _ => return None,
        };
        Some(Self {
            index: column_index(column, decimal)?,
            offset: Some(offset),
        })
    }

    #[inline]
    fn read(&self, row: Row<'_>) -> Option<Option<MyDecimal>> {
        read_fixed(
            row,
            self.index,
            MYDECIMAL_STRUCT_SIZE as i64,
            |column, index| {
                let value = column.get_my_decimal(index);
                match self.offset {
                    Some(offset) => value.checked_add_integer(offset),
                    None => Some(value),
                }
            },
        )
    }
}

#[inline]
fn compare_decimal(left: MyDecimal, right: MyDecimal) -> i64 {
    if left.is_negative() != right.is_negative()
        && left.to_i128_scaled().is_some_and(|(value, _)| value == 0)
        && right.to_i128_scaled().is_some_and(|(value, _)| value == 0)
    {
        return 0;
    }
    left.compare(&right) as i64
}

pub(crate) struct TimeOperand {
    index: usize,
    offset: Option<TimeOffset>,
}

struct TimeOffset {
    seconds: i32,
    fsp: i64,
    microsecond_quantum: u32,
}

impl TimeOperand {
    fn prepare(expression: &Expression) -> Option<Self> {
        let Expression::ScalarFunction(function) = expression else {
            let temporal = |code| {
                matches!(
                    code,
                    FieldTypeCode::Date | FieldTypeCode::Datetime | FieldTypeCode::Timestamp
                )
            };
            return Some(Self {
                index: column_index(expression, temporal)?,
                offset: None,
            });
        };
        Self::prepare_offset(function)
    }

    fn prepare_offset(function: &ScalarFunction) -> Option<Self> {
        let [column, amount, unit] = function.args.as_slice() else {
            return None;
        };
        if !fixed_literal(unit)?
            .value
            .as_raw_bytes()?
            .eq_ignore_ascii_case(b"SECOND")
        {
            return None;
        }
        let amount = fixed_literal(amount)?;
        let amount_type = amount.ret_type.as_ref()?;
        if amount_type.code() != FieldTypeCode::LongLong || amount_type.is_unsigned() {
            return None;
        }
        let Datum::Int(seconds) = amount.value else {
            return None;
        };
        let seconds = i32::try_from(seconds).ok()?;
        if !(0..86_400).contains(&seconds) {
            return None;
        }
        let seconds = match function.pb_code().as_str_name() {
            "AddDateDatetimeInt" => seconds,
            "SubDateDatetimeInt" => -seconds,
            _ => return None,
        };
        let ty = function.ret_type.as_ref()?;
        let fsp = ty.decimal();
        if ty.code() != FieldTypeCode::Datetime || !(0..=6).contains(&fsp) {
            return None;
        }
        Some(Self {
            index: column_index(column, |code| code == FieldTypeCode::Datetime)?,
            offset: Some(TimeOffset {
                seconds,
                fsp,
                microsecond_quantum: 10u32.pow(6 - fsp as u32),
            }),
        })
    }

    #[inline]
    fn read(&self, row: Row<'_>) -> Option<Option<u64>> {
        read_fixed(row, self.index, 8, |column, index| {
            let raw = column.get_uint64(index);
            match &self.offset {
                Some(offset) => offset.shift(Time::from_go_raw(raw).ok()?),
                // Time::compare ignores the kind and FSP bits. An out-of-range
                // FSP still goes through the checked expression getter.
                None => (raw & 0xf != 0xf).then_some(raw >> 4),
            }
        })
    }
}

impl TimeOffset {
    #[inline]
    fn shift(&self, time: Time) -> Option<u64> {
        let core = time.core_time();
        let (year, month, day) = (core.year(), core.month(), core.day());
        let (hour, minute, second, micros) = (
            core.hour(),
            core.minute(),
            core.second(),
            core.microsecond(),
        );
        if time.kind() != TimeType::DateTime
            || i64::from(time.fsp()) != self.fsp
            || !(1..=9999).contains(&year)
            || !(1..=12).contains(&month)
            || day == 0
            || day > get_last_day(year, month)
            || hour >= 24
            || minute >= 60
            || second >= 60
            || micros >= 1_000_000
            || micros % self.microsecond_quantum != 0
        {
            return None;
        }
        // Within a civil day, integer seconds cannot change the calendar
        // date or fractional precision. Let the existing evaluator handle
        // date boundaries, zero dates and precision conversion.
        let seconds =
            i32::from(hour) * 3600 + i32::from(minute) * 60 + i32::from(second) + self.seconds;
        if !(0..86_400).contains(&seconds) {
            return None;
        }
        Some(
            CoreTime::from_date(
                year as u16,
                month,
                day,
                (seconds / 3600) as u8,
                (seconds / 60 % 60) as u8,
                (seconds % 60) as u8,
                micros,
            )
            .raw()
                >> 4,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tidb_chunk::chunk::Chunk;
    use tidb_datatype::{FieldType, FieldTypeFlags};
    use tidb_expr::{column::Column as ExprColumn, new_function_base};
    use tidb_exprctx::{Columns, NoColumns};

    struct SignedSubtraction;

    impl Columns for SignedSubtraction {
        fn get(&self, _: &[String]) -> Option<Datum> {
            None
        }

        fn no_unsigned_subtraction(&self) -> bool {
            true
        }
    }

    fn assert_comparisons(
        typed: &RangeComparison,
        lhs: &Expression,
        rhs: &Expression,
        chunk: &Chunk,
    ) {
        for l in 0..chunk.num_rows() {
            for r in 0..chunk.num_rows() {
                let (lrow, rrow) = (chunk.get_row(l), chunk.get_row(r));
                let expected =
                    RangeComparison::Expression.compare(&NoColumns, lhs, rhs, lrow, rrow);
                let actual = typed.compare(&NoColumns, lhs, rhs, lrow, rrow);
                assert_eq!(
                    format!("{actual:?}"),
                    format!("{expected:?}"),
                    "rows {l}, {r}"
                );
            }
        }
    }

    #[test]
    fn decimal_bounds_keep_fractional_scale_and_wide_integer_words() {
        let mut field = FieldType::new(FieldTypeCode::NewDecimal);
        field.set_flen(65);
        field.set_decimal(6);
        let mut column = ExprColumn::new(1, field.clone());
        column.index = 0;
        let key = Expression::Column(column);
        let mut chunk = Chunk::new_chunk_with_capacity(&[field.clone()], 8);
        chunk.append_null(0);
        for text in [
            "0",
            "-0.125",
            "0.125000",
            "-6.5",
            "6.5",
            "999999999.5",
            "500000000000000000000000000000000000000000.0001",
        ] {
            let (value, error) = MyDecimal::from_string(text.as_bytes());
            assert!(error.is_none());
            chunk.append_my_decimal(0, &value);
        }
        for name in ["plus", "minus"] {
            let bound = new_function_base(
                &NoColumns,
                name,
                field.clone(),
                vec![
                    key.clone(),
                    Expression::Constant(Constant::new(
                        Datum::Int(5),
                        FieldType::new(FieldTypeCode::LongLong),
                    )),
                ],
            )
            .unwrap();
            let typed = RangeComparison::prepare(&key, &bound);
            assert!(matches!(typed, RangeComparison::Decimal(..)));
            assert_comparisons(&typed, &key, &bound, &chunk);
        }
    }

    #[test]
    fn datetime_bounds_keep_fractional_seconds_and_fall_back_at_midnight() {
        let mut field = FieldType::new(FieldTypeCode::Datetime);
        field.set_decimal(6);
        let mut column = ExprColumn::new(1, field.clone());
        column.index = 0;
        let key = Expression::Column(column);
        let same_day =
            Time::from_date_checked(2024, 2, 29, 11, 59, 50, 125001, TimeType::DateTime, 6)
                .unwrap();
        let midnight =
            Time::from_date_checked(2024, 2, 29, 23, 59, 50, 125001, TimeType::DateTime, 6)
                .unwrap();
        let shifted =
            Time::from_date_checked(2024, 2, 29, 12, 0, 22, 125001, TimeType::DateTime, 6).unwrap();
        let mut chunk = Chunk::new_chunk_with_capacity(&[field.clone()], 4);
        chunk.append_time(0, same_day);
        chunk.append_time(0, midnight);
        chunk.append_null(0);
        chunk.append_time(0, shifted);
        let args = vec![
            key.clone(),
            Expression::Constant(Constant::new(
                Datum::Int(32),
                FieldType::new(FieldTypeCode::LongLong),
            )),
            Expression::Constant(Constant::new(
                Datum::new_string_datum("SECOND"),
                FieldType::new(FieldTypeCode::Varchar),
            )),
        ];
        let bound = new_function_base(&NoColumns, "date_add", field, args).unwrap();
        let typed = RangeComparison::prepare(&key, &bound);
        assert!(matches!(typed, RangeComparison::Time(..)));
        assert_comparisons(&typed, &key, &bound, &chunk);

        let mut native = true;
        let order = typed
            .compare_tracked(
                &NoColumns,
                &key,
                &bound,
                chunk.get_row(3),
                chunk.get_row(0),
                &mut native,
            )
            .unwrap();
        assert_eq!(order, 0);
        assert!(native);
        typed
            .compare_tracked(
                &NoColumns,
                &key,
                &bound,
                chunk.get_row(0),
                chunk.get_row(1),
                &mut native,
            )
            .unwrap();
        assert!(!native);
    }

    #[test]
    fn typed_bounds_match_expression_errors_nulls_and_signedness() {
        for unsigned in [false, true] {
            let mut field = FieldType::new(FieldTypeCode::LongLong);
            if unsigned {
                field.add_flags(FieldTypeFlags::UNSIGNED);
            }
            let mut column = ExprColumn::new(1, field.clone());
            column.index = 0;
            let key = Expression::Column(column);
            let mut chunk = Chunk::new_chunk_with_capacity(&[field.clone()], 6);
            chunk.append_null(0);
            for value in [i64::MIN, -1, 0, 1, i64::MAX] {
                chunk.append_int64(0, value);
            }

            for name in ["plus", "minus"] {
                let bound = new_function_base(
                    &NoColumns,
                    name,
                    field.clone(),
                    vec![
                        key.clone(),
                        Expression::Constant(Constant::new(
                            Datum::Int(1),
                            FieldType::new(FieldTypeCode::LongLong),
                        )),
                    ],
                )
                .unwrap();
                for (lhs, rhs) in [(&key, &bound), (&bound, &key)] {
                    let typed = RangeComparison::prepare(lhs, rhs);
                    assert!(matches!(typed, RangeComparison::Integer(..)));
                    for ctx in [&NoColumns as &dyn Columns, &SignedSubtraction] {
                        for l in 0..chunk.num_rows() {
                            for r in 0..chunk.num_rows() {
                                let (lrow, rrow) = (chunk.get_row(l), chunk.get_row(r));
                                let expected = (|| -> Result<i64, ExecError> {
                                    let left = lhs.eval(ctx, lrow)?;
                                    let right = rhs.eval(ctx, rrow)?;
                                    Ok(tidb_expr::compare_datums_with_collation(
                                        &left,
                                        &right,
                                        tidb_expr::collation_derive::collation_of_node(lhs),
                                    )? as i64)
                                })()
                                .map_err(|e| format!("{e:?}"));
                                let actual = typed
                                    .compare(ctx, lhs, rhs, lrow, rrow)
                                    .map_err(|e| format!("{e:?}"));
                                assert_eq!(
                                    actual, expected,
                                    "{name} unsigned={unsigned} left={l} right={r}"
                                );
                            }
                        }
                    }
                }
            }
        }
    }
}

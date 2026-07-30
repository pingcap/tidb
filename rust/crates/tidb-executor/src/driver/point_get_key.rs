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

//! The key a point plan looks a row up by.
//!
//! Mirrors `getPointGetValue` and `checkCanConvertInPointGet` in
//! `pkg/planner/core/point_get_plan.go`.
//!
//! A point plan replaces the comparison with a KEY LOOKUP, so the constant
//! written in the `WHERE` has to be moved into the COLUMN's domain first --
//! `pk = 1.0` looks up handle `1`, not "no handle at all". Go's rule is a
//! single one for every column type and every point plan (`PointGet`,
//! `Batch_Point_Get`, handle or unique index):
//!
//!  1. convert the constant to the column's field type, and
//!  2. require the converted value to compare EQUAL to the original.
//!
//! When either step fails the point plan is ABANDONED -- Go returns `nil`
//! from `getNameValuePairs`/`newBatchPointGetPlan` and the statement falls
//! back to an ordinary scan, whose comparison then decides the rows. That is
//! why a non-representable constant needs no special case here: `pk = 1.5`
//! becomes a scan, and the scan's `=` returns no row on its own.
//!
//! The failure mode this replaces was returning "point plan, zero rows" for
//! every non-integer constant, which silently dropped the row for
//! `pk = 1.0`, `pk = 1e0`, `pk = '1'` and `pk IN (1.0, 2.0)`.

use tidb_datatype::{Datum, FieldType, FieldTypeCode};

/// Go `checkCanConvertInPointGet`: pairings whose conversion is meaningful
/// for storage but wrong for key equality, so no point plan may be built.
fn can_convert_in_point_get(column: &FieldType, value: &Datum) -> bool {
    if column.eval_type() == tidb_datatype::EvalType::String
        && matches!(
            value,
            Datum::Int(_) | Datum::UInt(_) | Datum::Float32(_) | Datum::Real(_) | Datum::Decimal(_)
        )
    {
        // Column type is String and constant type is numeric.
        return false;
    }
    if column.code() == FieldTypeCode::Bit && matches!(value, Datum::String(_)) {
        // Column type is Bit and constant type is string.
        return false;
    }
    true
}

/// Go `getPointGetValue`: the constant in the column's domain, or `None`
/// when this statement may not use a point plan at all.
pub(crate) fn point_get_value(column: &FieldType, value: &Datum) -> Option<Datum> {
    if value.is_null() {
        return None;
    }
    if !can_convert_in_point_get(column, value) {
        return None;
    }
    let converted = value
        .convert_to(column, tidb_datatype::STRICT_FLAGS)
        .ok()?
        .value;
    // "The converted result must be same as original datum." A comparison in
    // the ORIGINAL datum's domain, exactly as Go's `dVal.Compare(&d)`: this
    // is what separates `1.0` (equal to `1`, so a point get on handle 1) from
    // `1.5` (not equal to `2`, so no point plan).
    match converted.compare(value, column.collation()) {
        Ok(std::cmp::Ordering::Equal) => Some(converted),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tidb_datatype::Decimal;

    fn int_column() -> FieldType {
        FieldType::new(FieldTypeCode::LongLong)
    }

    fn decimal(text: &str) -> Datum {
        Datum::Decimal(Decimal::from_literal(text))
    }

    #[test]
    fn a_decimal_constant_with_a_zero_fraction_names_the_integer_handle() {
        assert_eq!(
            point_get_value(&int_column(), &decimal("1.0")),
            Some(Datum::Int(1))
        );
        assert_eq!(
            point_get_value(&int_column(), &decimal("1.00")),
            Some(Datum::Int(1))
        );
    }

    #[test]
    fn a_non_representable_constant_abandons_the_point_plan() {
        // NOT "handle 2" and NOT "no rows": `None` means "use a scan", whose
        // own comparison returns the empty result Go returns.
        assert_eq!(point_get_value(&int_column(), &decimal("1.5")), None);
        assert_eq!(point_get_value(&int_column(), &decimal("0.5")), None);
        assert_eq!(point_get_value(&int_column(), &Datum::Real(1.5)), None);
    }

    #[test]
    fn float_and_string_constants_name_the_integer_handle() {
        assert_eq!(
            point_get_value(&int_column(), &Datum::Real(1.0)),
            Some(Datum::Int(1))
        );
        assert_eq!(
            point_get_value(&int_column(), &Datum::new_string("1")),
            Some(Datum::Int(1))
        );
        assert_eq!(
            point_get_value(&int_column(), &Datum::new_string("1.5")),
            None
        );
    }

    #[test]
    fn an_integer_constant_passes_through_unchanged() {
        assert_eq!(
            point_get_value(&int_column(), &Datum::Int(7)),
            Some(Datum::Int(7))
        );
    }

    #[test]
    fn a_negative_constant_never_names_an_unsigned_handle() {
        let mut unsigned = FieldType::new(FieldTypeCode::LongLong);
        unsigned.add_flags(tidb_datatype::FieldTypeFlags::UNSIGNED);
        // Saturation to 0 is not equal to -1, so the point plan is abandoned
        // and the scan decides -- Go's same `cmp != 0` rejection.
        assert_eq!(point_get_value(&unsigned, &Datum::Int(-1)), None);
        assert_eq!(point_get_value(&unsigned, &decimal("-1.5")), None);
    }

    #[test]
    fn a_numeric_constant_never_keys_a_string_column() {
        let column = FieldType::new(FieldTypeCode::Varchar);
        assert_eq!(point_get_value(&column, &Datum::Int(1)), None);
        assert_eq!(point_get_value(&column, &decimal("1.0")), None);
    }

    #[test]
    fn a_null_constant_is_never_a_point_key() {
        assert_eq!(point_get_value(&int_column(), &Datum::Null), None);
    }
}

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

/// Whether a parameter that fails its column conversion provably matches no
/// stored value, so the statement's answer is the empty set without any
/// storage read. Go serves such an EXECUTE from its re-optimized plan
/// (`GetPlanFromPlanCache` misses into `generateNewPlan`,
/// `pkg/planner/core/plan_cache.go`), and that fresh point/range plan reads
/// nothing -- the empty set is the same observable answer, served without
/// re-planning. A string longer than the column's character capacity can
/// never compare equal to a stored value: PAD SPACE collations fold trailing
/// spaces first, so those are discounted before the length test; a non-ASCII
/// payload stays with the ordinary planner because byte length is not char
/// length.
pub(crate) fn names_no_rows(column: &FieldType, value: &Datum) -> bool {
    let payload = match value {
        Datum::String(string) => string.bytes(),
        Datum::Bytes(bytes) => bytes,
        _ => return false,
    };
    if column.eval_type() != tidb_datatype::EvalType::String {
        return false;
    }
    if !payload.iter().all(u8::is_ascii) {
        return false;
    }
    let mut significant = payload.len();
    if tidb_datatype::is_pad_space_collation(column.collation().name()) {
        significant -= payload.iter().rev().take_while(|byte| **byte == b' ').count();
    }
    column.flen() >= 0 && significant > column.flen() as usize
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

    fn varchar_column(flen: i64) -> FieldType {
        tidb_datatype::FieldTypeBuilder::new()
            .with_code(FieldTypeCode::Varchar)
            .flen_set(flen)
            .charset_set("utf8mb4")
            .collation_set("utf8mb4_bin")
            .build()
    }

    #[test]
    fn a_string_longer_than_the_column_names_no_rows() {
        // The workload binds an 18-char id number to custno varchar(10): no
        // stored value can compare equal, so the empty set is the answer.
        assert!(names_no_rows(
            &varchar_column(10),
            &Datum::new_string("310110194401061214")
        ));
        assert!(!names_no_rows(&varchar_column(10), &Datum::new_string("1002041840")));
    }

    #[test]
    fn trailing_spaces_fold_under_pad_space_collations() {
        // utf8mb4_bin is PAD SPACE: 'stored' + spaces equals 'stored', so a
        // payload whose significant part fits must still be read.
        let value = format!("{}{}", "0123456789", " ".repeat(8));
        assert!(!names_no_rows(&varchar_column(10), &Datum::new_string(value)));
        let longer = format!("{}{}", "01234567890", " ".repeat(8));
        assert!(names_no_rows(&varchar_column(10), &Datum::new_string(longer)));
    }

    #[test]
    fn a_multibyte_payload_stays_with_the_planner() {
        // Byte length is not char length outside ASCII, so no verdict.
        assert!(!names_no_rows(
            &varchar_column(1),
            &Datum::new_string("你好")
        ));
    }

    #[test]
    fn non_string_domains_are_left_to_the_planner() {
        // An integer that fails its conversion may still be a saturation or
        // rounding question; only provable string overlength short-circuits.
        assert!(!names_no_rows(&int_column(), &Datum::new_string("1")));
        assert!(!names_no_rows(&int_column(), &Datum::Int(-1)));
    }
}

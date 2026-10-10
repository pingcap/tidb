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
//! Go's conversion rule (`getPointGetValue` / `checkCanConvertInPointGet`)
//! lives with the planner in [`tidb_planner::point_get_value`], where a cached
//! fast plan's rebuild applies it too. This module adds the executor-side
//! answers for a constant that cannot be a key: a value outside the column's
//! domain, or one longer than its capacity, matches no stored row.

use tidb_datatype::{Datum, FieldType};
use tidb_planner::point_get_value::can_convert_in_point_get;
pub(crate) use tidb_planner::point_get_value::point_get_value;

/// Whether a constant cannot be represented in its column's domain.
///
/// Go `getNameValuePairs` returns the pair with `isTableDual` when
/// `d.ConvertTo` reports `types.ErrOverflow`, and `tryPointGetPlan` then plans
/// a `TableDual`: a value outside the column's domain can equal no stored
/// value, so the statement's answer is the empty set. Rust's callers use this
/// to answer the same empty set without a storage read. The event, not the
/// value, is the signal: `convert_to` clamps to a representable value and
/// records the overflow.
pub(crate) fn point_get_value_overflowed(column: &FieldType, value: &Datum) -> bool {
    if value.is_null() || !can_convert_in_point_get(column, value) {
        return false;
    }
    match value.convert_to(column, tidb_datatype::STRICT_FLAGS) {
        Ok(converted) => matches!(
            converted.event,
            Some(tidb_datatype::ScalarConversionEvent::Overflow(_))
        ),
        Err(_) => false,
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
        significant -= payload
            .iter()
            .rev()
            .take_while(|byte| **byte == b' ')
            .count();
    }
    column.flen() >= 0 && significant > column.flen() as usize
}

#[cfg(test)]
mod tests {
    use super::*;
    use tidb_datatype::{Decimal, FieldTypeCode};

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

    #[test]
    fn an_out_of_range_constant_overflows_its_column_domain() {
        // Go `getNameValuePairs` returns `isTableDual` on `ErrOverflow`; the
        // event is the Rust signal because `convert_to` clamps and records it.
        assert!(point_get_value_overflowed(
            &int_column(),
            &Datum::new_string("99999999999999999999999999")
        ));
        assert!(point_get_value_overflowed(
            &int_column(),
            &Datum::Real(1e300)
        ));
        assert!(!point_get_value_overflowed(
            &int_column(),
            &Datum::new_string("1")
        ));
        assert!(!point_get_value_overflowed(&int_column(), &Datum::Int(1)));
        assert!(!point_get_value_overflowed(&int_column(), &Datum::Null));
        // A too-long string is a TRUNCATION, not an overflow, and Go's
        // `ErrTruncatedWrongVal` arm keeps comparing it.
        assert!(!point_get_value_overflowed(
            &varchar_column(3),
            &Datum::new_string("abcdef")
        ));
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
        assert!(!names_no_rows(
            &varchar_column(10),
            &Datum::new_string("1002041840")
        ));
    }

    #[test]
    fn trailing_spaces_fold_under_pad_space_collations() {
        // utf8mb4_bin is PAD SPACE: 'stored' + spaces equals 'stored', so a
        // payload whose significant part fits must still be read.
        let value = format!("{}{}", "0123456789", " ".repeat(8));
        assert!(!names_no_rows(
            &varchar_column(10),
            &Datum::new_string(value)
        ));
        let longer = format!("{}{}", "01234567890", " ".repeat(8));
        assert!(names_no_rows(
            &varchar_column(10),
            &Datum::new_string(longer)
        ));
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

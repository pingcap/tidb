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

//! Go `getPointGetValue` and `checkCanConvertInPointGet`
//! (`pkg/planner/core/point_get_plan.go`): the constant a point plan looks a
//! key up by, moved into the key column's domain.
//!
//! A point plan replaces the comparison with a KEY LOOKUP, so the constant
//! written in the `WHERE` has to be moved into the COLUMN's domain first --
//! `pk = 1.0` looks up handle `1`, not "no handle at all". Go's rule is one
//! for every column type and every point plan (`PointGet`,
//! `Batch_Point_Get`, handle or unique index):
//!
//!  1. convert the constant to the column's field type, and
//!  2. require the converted value to compare EQUAL to the original.
//!
//! When either step fails the point plan is ABANDONED and the statement falls
//! back to an ordinary scan, whose comparison then decides the rows. A cached
//! fast plan applies the same rule to each execution's parameter
//! (`convertConstant2Datum`), and a parameter that fails it rebuilds no plan.

use tidb_datatype::{Datum, FieldType, FieldTypeCode};

/// Go `checkCanConvertInPointGet`: pairings whose conversion is meaningful
/// for storage but wrong for key equality, so no point plan may be built.
#[must_use]
pub fn can_convert_in_point_get(column: &FieldType, value: &Datum) -> bool {
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
#[must_use]
pub fn point_get_value(column: &FieldType, value: &Datum) -> Option<Datum> {
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

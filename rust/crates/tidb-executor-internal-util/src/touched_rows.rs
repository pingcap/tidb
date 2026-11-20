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

//! Go `touched_rows.go`.

use tidb_chunk::column::Column;
use tidb_datatype::{EvalType, FieldType, FieldTypeCode};
use tidb_driver_error::ExecError;

/// Go `touchedBitmapMarker`.
struct TouchedBitmapMarker<'a> {
    bitmap: &'a mut [u8],
    stride: usize,
    bit_byte_offset: usize,
    bit_mask: u8,
}

impl TouchedBitmapMarker<'_> {
    fn mark(&mut self, update_ordinal: usize) {
        self.bitmap[update_ordinal * self.stride + self.bit_byte_offset] |= self.bit_mask;
    }
}

/// Go `MarkTouchedRowsByColumn`: compares old/new chunk columns and marks
/// touched bits for changed update rows. The comparison deliberately uses
/// binary semantics for string-like values to match row-update
/// touched-column detection instead of SQL collation equality.
///
/// Go spells one loop per value type; each compares two non-NULL cells of
/// that type, and a NULL on exactly one side is a change.
///
/// # Errors
/// The field type's eval type has no comparison.
#[allow(clippy::too_many_arguments)]
pub fn mark_touched_rows_by_column(
    update_rows: &[usize],
    update_touched_bitmap: &mut [u8],
    update_touched_stride: usize,
    update_bit_pos: usize,
    old_col: &Column,
    new_col: &Column,
    field_type: &FieldType,
    not_null: bool,
    comparison_context: &str,
) -> Result<(), ExecError> {
    if update_rows.is_empty() {
        return Ok(());
    }
    let mut marker = TouchedBitmapMarker {
        bitmap: update_touched_bitmap,
        stride: update_touched_stride,
        bit_byte_offset: update_bit_pos >> 3,
        bit_mask: 1 << (update_bit_pos & 7),
    };
    let unsigned = tidb_mysql::types::has_unsigned_flag(field_type.flags() as usize);
    let differs: Box<dyn Fn(usize) -> bool + '_> = match field_type.eval_type() {
        // BIT evaluates as an integer but can be stored as variable-length byte
        // literals. NULL values occupy no data bytes in that layout, so an int64
        // slice would address the wrong rows. Fixed-length projection columns
        // keep the integer path.
        EvalType::Int if field_type.code() == FieldTypeCode::Bit && (!old_col.is_fixed() || !new_col.is_fixed()) => {
            Box::new(|row| old_col.get_raw(row).as_ref() != new_col.get_raw(row).as_ref())
        }
        EvalType::Int if unsigned => Box::new(|row| old_col.get_uint64(row) != new_col.get_uint64(row)),
        EvalType::Int => Box::new(|row| old_col.get_int64(row) != new_col.get_int64(row)),
        EvalType::Real if field_type.code() == FieldTypeCode::Float => {
            Box::new(|row| old_col.get_float32(row) != new_col.get_float32(row))
        }
        EvalType::Real => Box::new(|row| old_col.get_float64(row) != new_col.get_float64(row)),
        EvalType::Decimal => Box::new(|row| {
            old_col.get_my_decimal(row).compare(&new_col.get_my_decimal(row)) != std::cmp::Ordering::Equal
        }),
        // Go `collate.GetBinaryCollator().Compare`: the bytes.
        EvalType::String => match field_type.code() {
            FieldTypeCode::Enum => Box::new(|row| old_col.get_enum(row).name_bytes() != new_col.get_enum(row).name_bytes()),
            FieldTypeCode::Set => Box::new(|row| old_col.get_set(row).name_bytes() != new_col.get_set(row).name_bytes()),
            _ => Box::new(|row| old_col.get_string(row).as_bytes() != new_col.get_string(row).as_bytes()),
        },
        EvalType::Datetime | EvalType::Timestamp => Box::new(|row| old_col.get_time(row) != new_col.get_time(row)),
        // Go `GoDurations()`: the stored nanosecond counts.
        EvalType::Duration => Box::new(|row| old_col.get_int64(row) != new_col.get_int64(row)),
        EvalType::Json | EvalType::VectorFloat32 => {
            Box::new(|row| old_col.get_raw(row).as_ref() != new_col.get_raw(row).as_ref())
        }
    };
    let _ = comparison_context;
    for (update_ordinal, &row_idx) in update_rows.iter().enumerate() {
        if !not_null {
            let old_is_null = old_col.is_null(row_idx);
            let new_is_null = new_col.is_null(row_idx);
            if old_is_null || new_is_null {
                if old_is_null != new_is_null {
                    marker.mark(update_ordinal);
                }
                continue;
            }
        }
        if differs(row_idx) {
            marker.mark(update_ordinal);
        }
    }
    Ok(())
}

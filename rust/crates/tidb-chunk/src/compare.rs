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

//! `pkg/util/chunk/compare.go`: ordering over chunk cells.
//!
//! Three surfaces, ported whole:
//!
//! * [`get_compare_func`] -- Go `GetCompareFunc`, the per-field-type
//!   cell-vs-cell comparator that sorting, merge joins and window frames use.
//! * [`compare`] -- Go `Compare`, a cell against a [`Datum`], which is the
//!   ranger/index-bound direction and follows the DATUM's kind, not a field
//!   type.
//! * [`crate::chunk::Chunk::lower_bound`]/[`crate::chunk::Chunk::upper_bound`]
//!   -- Go's binary searches over a non-decreasing column.
//!
//! Two behaviours worth stating, because both are easy to guess wrong:
//!
//! * The string comparator is COLLATION-AWARE, not a raw byte compare. Go
//!   routes it through `types.CompareString` -> `collate.GetCollator(collation)`
//!   with the FIELD TYPE's collation, and `types.NewFieldType(mysql.TypeString)`
//!   defaults to `utf8mb4_bin` (measured), which already trims trailing spaces.
//!   Only the blob family defaults to `binary`.
//! * NULL sorts FIRST: `cmpNull` returns -1 when the left cell is null, and two
//!   nulls are equal. `Compare`'s datum direction agrees (a null datum is less
//!   than any non-null cell) and additionally orders the `MinNotNull`/`MaxValue`
//!   range sentinels.
//!
//! `TiDBVectorFloat32` uses the datatype layer's source-compatible
//! lexicographic vector comparison over serialized variable-length cells.

use crate::column::{Column, name_value_parts};
use crate::column_slot::ColumnSlot;
use crate::row::Row;
use std::cmp::Ordering;
use tidb_datatype::{
    compare_binary_json, get_collator, BinaryJSON, BinaryLiteral, Collator, Datum, Decimal, FieldType,
    FieldTypeCode, MYDECIMAL_STRUCT_SIZE, MyDecimal, VectorFloat32Ref,
};

/// Go `chunk.CompareFunc`: compares column `l_col` of `l` with column `r_col`
/// of `r`. The two columns must have the same type.
pub type CompareFunc =
    Box<dyn for<'a> Fn(Row<'a>, usize, Row<'a>, usize) -> Ordering + Send + Sync>;

/// A compiled comparator over columns whose storage is already borrowed.
///
/// Sort keeps these borrows for the whole `sort_unstable_by` call. That is
/// equivalent to Go's direct `chunk.Row` access, while avoiding a fresh
/// `ColumnSlot` read (and, for shared variable-width columns, a cell copy) on
/// every comparison.
pub type ColumnCompareFunc = Box<dyn Fn(&Column, usize, &Column, usize) -> Ordering + Send + Sync>;

/// Compares two non-NULL variable-width string cells in already borrowed columns.
#[inline]
pub fn compare_column_strings(
    collator: Collator,
    left: &Column,
    left_row: usize,
    right: &Column,
    right_row: usize,
) -> Ordering {
    match (
        left.borrow_nonnull_var_cell(left_row),
        right.borrow_nonnull_var_cell(right_row),
    ) {
        (Some(left), Some(right)) => collator.compare(left, right),
        (Some(left), None) => collator.compare(left, &right.get_bytes(right_row)),
        (None, Some(right)) => collator.compare(&left.get_bytes(left_row), right),
        (None, None) => compare_locked_strings(collator, left, left_row, right, right_row),
    }
}

fn compare_locked_strings(
    collator: Collator,
    left: &Column,
    left_row: usize,
    right: &Column,
    right_row: usize,
) -> Ordering {
    let left_bytes = left.data.read();
    let left_cell = &left_bytes[left.var_byte_range(left_row)];
    if let Some(right_bytes) = right.data.try_read() {
        return collator.compare(left_cell, &right_bytes[right.var_byte_range(right_row)]);
    }
    // A waiting writer can prevent another reader even on the same backing.
    // Keep the first cell stable and release its lock before waiting.
    let left_cell = left_cell.to_vec();
    drop(left_bytes);
    collator.compare(&left_cell, &right.get_bytes(right_row))
}

/// The null pre-check for two already-borrowed columns.
fn column_null_order(
    left: &Column,
    left_row: usize,
    right: &Column,
    right_row: usize,
) -> Option<Ordering> {
    let (left_null, right_null) = (left.is_null(left_row), right.is_null(right_row));
    if left_null || right_null {
        Some(cmp_null(left_null, right_null))
    } else {
        None
    }
}

/// Compiles Go `keyCmpFuncs` for column storage that the caller has already
/// borrowed.
///
/// This has the same type and NULL ordering as [`get_compare_func`]. It is a
/// separate surface because a `Row` intentionally reopens its `ColumnSlot` on
/// each typed accessor, whereas blocking operators such as Sort can retain a
/// read view while they compare the same columns repeatedly.
#[must_use]
pub fn get_column_compare_func(field_type: &FieldType) -> Option<ColumnCompareFunc> {
    let func: ColumnCompareFunc = match field_type.code() {
        FieldTypeCode::Tiny
        | FieldTypeCode::Short
        | FieldTypeCode::Int24
        | FieldTypeCode::Long
        | FieldTypeCode::LongLong
        | FieldTypeCode::Year => {
            if field_type.is_unsigned() {
                Box::new(|left, left_row, right, right_row| {
                    column_null_order(left, left_row, right, right_row).unwrap_or_else(|| {
                        left.get_uint64(left_row).cmp(&right.get_uint64(right_row))
                    })
                })
            } else {
                Box::new(|left, left_row, right, right_row| {
                    column_null_order(left, left_row, right, right_row).unwrap_or_else(|| {
                        left.get_int64(left_row).cmp(&right.get_int64(right_row))
                    })
                })
            }
        }
        FieldTypeCode::Float => Box::new(|left, left_row, right, right_row| {
            column_null_order(left, left_row, right, right_row).unwrap_or_else(|| {
                cmp_float(
                    f64::from(left.get_float32(left_row)),
                    f64::from(right.get_float32(right_row)),
                )
            })
        }),
        FieldTypeCode::Double => Box::new(|left, left_row, right, right_row| {
            column_null_order(left, left_row, right, right_row).unwrap_or_else(|| {
                cmp_float(left.get_float64(left_row), right.get_float64(right_row))
            })
        }),
        FieldTypeCode::String
        | FieldTypeCode::VarString
        | FieldTypeCode::Varchar
        | FieldTypeCode::Blob
        | FieldTypeCode::TinyBlob
        | FieldTypeCode::MediumBlob
        | FieldTypeCode::LongBlob => {
            let collator = get_collator(field_type.collation_name());
            Box::new(move |left, left_row, right, right_row| {
                column_null_order(left, left_row, right, right_row).unwrap_or_else(|| {
                    compare_column_strings(collator, left, left_row, right, right_row)
                })
            })
        }
        FieldTypeCode::Date | FieldTypeCode::Datetime | FieldTypeCode::Timestamp => {
            Box::new(|left, left_row, right, right_row| {
                column_null_order(left, left_row, right, right_row)
                    .unwrap_or_else(|| left.get_time(left_row).compare(right.get_time(right_row)))
            })
        }
        FieldTypeCode::Duration => Box::new(|left, left_row, right, right_row| {
            column_null_order(left, left_row, right, right_row).unwrap_or_else(|| {
                left.get_duration(left_row, 0)
                    .nanoseconds()
                    .cmp(&right.get_duration(right_row, 0).nanoseconds())
            })
        }),
        FieldTypeCode::NewDecimal => Box::new(cmp_my_decimal_column),
        FieldTypeCode::Set | FieldTypeCode::Enum => Box::new(cmp_name_value_column),
        FieldTypeCode::Bit => Box::new(cmp_bit_column),
        FieldTypeCode::Json => Box::new(|left, left_row, right, right_row| {
            if let Some(order) = cmp_borrowed_json(left, left_row, right, right_row) {
                return order;
            }
            column_null_order(left, left_row, right, right_row).unwrap_or_else(|| {
                compare_binary_json(&left.get_json(left_row), &right.get_json(right_row))
            })
        }),
        FieldTypeCode::VectorFloat32 => Box::new(cmp_vector_column),
        FieldTypeCode::Null => Box::new(|_, _, _, _| Ordering::Equal),
        _ => return None,
    };
    Some(func)
}

/// Go's `cmp.Compare` for floats: a NaN is less than every non-NaN and equal to
/// another NaN, and `-0.0 == 0.0`.
///
/// This is NOT `f64::total_cmp`, which orders `-0.0 < 0.0` and splits NaNs by
/// sign bit.
#[must_use]
pub fn cmp_float(left: f64, right: f64) -> Ordering {
    match (left.is_nan(), right.is_nan()) {
        (true, true) => Ordering::Equal,
        (true, false) => Ordering::Less,
        (false, true) => Ordering::Greater,
        (false, false) => {
            if left < right {
                Ordering::Less
            } else if left > right {
                Ordering::Greater
            } else {
                Ordering::Equal
            }
        }
    }
}

/// Go `cmpNull`: a null cell sorts before a non-null one, two nulls are equal.
fn cmp_null(l_null: bool, r_null: bool) -> Ordering {
    match (l_null, r_null) {
        (true, true) => Ordering::Equal,
        (true, false) => Ordering::Less,
        // Go's `cmpNull` is only reached when at least one side is null, so the
        // remaining cases are "right is null" and return 1.
        (false, _) => Ordering::Greater,
    }
}

/// The null pre-check every typed comparator opens with; `None` means both
/// cells are non-null and the caller should compare values.
fn null_order(l: Row<'_>, l_col: usize, r: Row<'_>, r_col: usize) -> Option<Ordering> {
    let (l_null, r_null) = (l.is_null(l_col), r.is_null(r_col));
    if l_null || r_null {
        Some(cmp_null(l_null, r_null))
    } else {
        None
    }
}

/// Go `GetCompareFunc`: the comparator for `field_type`, or `None` for a type
/// with no ordering (Go returns a nil `CompareFunc`).
#[must_use]
pub fn get_compare_func(field_type: &FieldType) -> Option<CompareFunc> {
    let func: CompareFunc = match field_type.code() {
        FieldTypeCode::Tiny
        | FieldTypeCode::Short
        | FieldTypeCode::Int24
        | FieldTypeCode::Long
        | FieldTypeCode::LongLong
        | FieldTypeCode::Year => {
            if field_type.is_unsigned() {
                Box::new(cmp_uint64)
            } else {
                Box::new(cmp_int64)
            }
        }
        FieldTypeCode::Float => Box::new(cmp_float32),
        FieldTypeCode::Double => Box::new(cmp_float64),
        FieldTypeCode::String
        | FieldTypeCode::VarString
        | FieldTypeCode::Varchar
        | FieldTypeCode::Blob
        | FieldTypeCode::TinyBlob
        | FieldTypeCode::MediumBlob
        | FieldTypeCode::LongBlob => gen_cmp_string_func(field_type.collation_name().to_owned()),
        FieldTypeCode::Date | FieldTypeCode::Datetime | FieldTypeCode::Timestamp => {
            Box::new(cmp_time)
        }
        FieldTypeCode::Duration => Box::new(cmp_duration),
        FieldTypeCode::NewDecimal => Box::new(cmp_my_decimal),
        FieldTypeCode::Set | FieldTypeCode::Enum => Box::new(cmp_name_value),
        FieldTypeCode::Bit => Box::new(cmp_bit),
        FieldTypeCode::Json => Box::new(cmp_json),
        FieldTypeCode::VectorFloat32 => Box::new(cmp_vector_float32),
        FieldTypeCode::Null => Box::new(cmp_null_const),
        _ => return None,
    };
    Some(func)
}

/// Go `cmpInt64`.
fn cmp_int64(l: Row<'_>, l_col: usize, r: Row<'_>, r_col: usize) -> Ordering {
    null_order(l, l_col, r, r_col).unwrap_or_else(|| l.get_int64(l_col).cmp(&r.get_int64(r_col)))
}

/// Go `cmpUint64`.
fn cmp_uint64(l: Row<'_>, l_col: usize, r: Row<'_>, r_col: usize) -> Ordering {
    null_order(l, l_col, r, r_col).unwrap_or_else(|| l.get_uint64(l_col).cmp(&r.get_uint64(r_col)))
}

/// Go `genCmpStringFunc`: closes over the FIELD TYPE's collation name and
/// resolves the collator on every call, as `types.CompareString` does.
fn gen_cmp_string_func(collation: String) -> CompareFunc {
    Box::new(move |l, l_col, r, r_col| {
        cmp_string_with_collation_info(l, l_col, r, r_col, &collation)
    })
}

/// Go `cmpStringWithCollationInfo`.
fn cmp_string_with_collation_info(
    l: Row<'_>,
    l_col: usize,
    r: Row<'_>,
    r_col: usize,
    collation: &str,
) -> Ordering {
    null_order(l, l_col, r, r_col).unwrap_or_else(|| {
        let left = l.get_bytes(l_col);
        let right = r.get_bytes(r_col);
        get_collator(collation).compare(left.as_ref(), right.as_ref())
    })
}

/// Go `cmpFloat32`: both sides widen to `float64` before comparing.
fn cmp_float32(l: Row<'_>, l_col: usize, r: Row<'_>, r_col: usize) -> Ordering {
    null_order(l, l_col, r, r_col).unwrap_or_else(|| {
        cmp_float(
            f64::from(l.get_float32(l_col)),
            f64::from(r.get_float32(r_col)),
        )
    })
}

/// Go `cmpFloat64`.
fn cmp_float64(l: Row<'_>, l_col: usize, r: Row<'_>, r_col: usize) -> Ordering {
    null_order(l, l_col, r, r_col)
        .unwrap_or_else(|| cmp_float(l.get_float64(l_col), r.get_float64(r_col)))
}

/// Go `cmpMyDecimal`.
fn cmp_my_decimal(l: Row<'_>, l_col: usize, r: Row<'_>, r_col: usize) -> Ordering {
    if let (Some(left), Some(right)) = (owned_column(l, l_col), owned_column(r, r_col)) {
        return cmp_my_decimal_column(left, l.idx(), right, r.idx());
    }
    null_order(l, l_col, r, r_col)
        .unwrap_or_else(|| l.get_my_decimal(l_col).compare(&r.get_my_decimal(r_col)))
}

#[inline]
fn owned_column(row: Row<'_>, col: usize) -> Option<&Column> {
    row.chunk().expect("empty Row has no Chunk").column_slots()[col].owned_column()
}

fn cmp_my_decimal_column(left: &Column, l_row: usize, right: &Column, r_row: usize) -> Ordering {
    column_null_order(left, l_row, right, r_row).unwrap_or_else(|| {
        if std::ptr::eq(left, right) {
            return left.with_my_decimal_data(|data| {
                cmp_decimal_cells(decimal_cell(data, l_row), decimal_cell(data, r_row))
            });
        }
        if let (Some(left), Some(right)) = (left.data.try_borrow(), right.data.try_borrow()) {
            return cmp_decimal_cells(decimal_cell(left, l_row), decimal_cell(right, r_row));
        }
        left.get_my_decimal(l_row)
            .compare(&right.get_my_decimal(r_row))
    })
}

#[inline]
fn decimal_cell(data: &[u8], row: usize) -> &[u8; MYDECIMAL_STRUCT_SIZE] {
    let start = row * MYDECIMAL_STRUCT_SIZE;
    data[start..start + MYDECIMAL_STRUCT_SIZE].try_into().expect("fixed element")
}

#[inline]
fn cmp_decimal_cells(left: &[u8; MYDECIMAL_STRUCT_SIZE], right: &[u8; MYDECIMAL_STRUCT_SIZE]) -> Ordering {
    MyDecimal::compare_packed(left, right).expect("chunk decimal cell holds a valid MyDecimal")
}

/// Go `cmpTime`.
fn cmp_time(l: Row<'_>, l_col: usize, r: Row<'_>, r_col: usize) -> Ordering {
    null_order(l, l_col, r, r_col).unwrap_or_else(|| l.get_time(l_col).compare(r.get_time(r_col)))
}

/// Go `cmpDuration`: compares the raw nanosecond counts, ignoring fsp (Go
/// reads both cells with `GetDuration(col, 0)`).
fn cmp_duration(l: Row<'_>, l_col: usize, r: Row<'_>, r_col: usize) -> Ordering {
    null_order(l, l_col, r, r_col).unwrap_or_else(|| {
        l.get_duration(l_col, 0)
            .nanoseconds()
            .cmp(&r.get_duration(r_col, 0).nanoseconds())
    })
}

/// Go `cmpNameValue`: an ENUM/SET cell is ordered by its NUMERIC value only --
/// the element name stored beside it is never consulted, so this comparator is
/// collation-independent.
fn cmp_name_value(l: Row<'_>, l_col: usize, r: Row<'_>, r_col: usize) -> Ordering {
    if let (Some(left), Some(right)) = (owned_column(l, l_col), owned_column(r, r_col)) {
        return cmp_name_value_column(left, l.idx(), right, r.idx());
    }
    null_order(l, l_col, r, r_col).unwrap_or_else(|| {
        l.get_name_value_number(l_col)
            .cmp(&r.get_name_value_number(r_col))
    })
}

fn cmp_name_value_column(left: &Column, l_row: usize, right: &Column, r_row: usize) -> Ordering {
    column_null_order(left, l_row, right, r_row).unwrap_or_else(|| {
        if std::ptr::eq(left, right) {
            let offsets = left.offsets.as_slice();
            let data = left.data.read();
            let l_value = name_value_parts(&data[offsets.cell_range(l_row)]).1;
            let r_value = name_value_parts(&data[offsets.cell_range(r_row)]).1;
            return l_value.cmp(&r_value);
        }
        left.get_name_value_number(l_row)
            .cmp(&right.get_name_value_number(r_row))
    })
}

/// Go `cmpBit`: `types.BinaryLiteral.Compare`, which strips leading zero bytes
/// and orders by LENGTH first -- NOT `bytes.Compare`.
fn cmp_bit(l: Row<'_>, l_col: usize, r: Row<'_>, r_col: usize) -> Ordering {
    if let (Some(left), Some(right)) = (owned_column(l, l_col), owned_column(r, r_col)) {
        return cmp_bit_column(left, l.idx(), right, r.idx());
    }
    null_order(l, l_col, r, r_col).unwrap_or_else(|| {
        let left_bytes = l.get_bytes(l_col);
        let right_bytes = r.get_bytes(r_col);
        compare_bit_bytes(&left_bytes, &right_bytes)
    })
}

fn cmp_bit_column(left: &Column, l_row: usize, right: &Column, r_row: usize) -> Ordering {
    column_null_order(left, l_row, right, r_row).unwrap_or_else(|| {
        if std::ptr::eq(left, right) {
            let offsets = left.offsets.as_slice();
            let data = left.data.read();
            return compare_bit_bytes(
                &data[offsets.cell_range(l_row)],
                &data[offsets.cell_range(r_row)],
            );
        }
        let left_bytes = left.get_bytes(l_row);
        if let Some(right_bytes) = right.data.try_read() {
            return compare_bit_bytes(&left_bytes, &right_bytes[right.var_byte_range(r_row)]);
        }
        let stable_left = left_bytes.to_vec();
        drop(left_bytes);
        compare_bit_bytes(&stable_left, &right.get_bytes(r_row))
    })
}

#[inline]
fn compare_bit_bytes(left: &[u8], right: &[u8]) -> Ordering {
    if left.len() == right.len() {
        compare_bytes(left, right)
    } else {
        BinaryLiteral::compare_bytes(left, right)
    }
}

/// Go `cmpJSON`.
fn cmp_json(l: Row<'_>, l_col: usize, r: Row<'_>, r_col: usize) -> Ordering {
    let cmp = || {
        let left = l.chunk()?.column_slots().get(l_col)?.owned_column()?;
        let right = r.chunk()?.column_slots().get(r_col)?.owned_column()?;
        cmp_borrowed_json(left, l.idx(), right, r.idx())
    };
    if let Some(order) = cmp() {
        return order;
    }
    null_order(l, l_col, r, r_col)
        .unwrap_or_else(|| compare_binary_json(&l.get_json(l_col), &r.get_json(r_col)))
}

fn cmp_borrowed_json(
    left: &Column,
    left_row: usize,
    right: &Column,
    right_row: usize,
) -> Option<Ordering> {
    let (left_cell, left_range) = left.borrow_cell_with_range(left_row)?;
    let (right_cell, right_range) = right.borrow_cell_with_range(right_row)?;
    BinaryJSON::compare_encoded_with_validation(
        left_cell,
        right_cell,
        || {
            left.data
                .cached_json_layout(left_range)
                .map(|layout| layout.is_canonical())
        },
        || {
            right
                .data
                .cached_json_layout(right_range)
                .map(|layout| layout.is_canonical())
        },
    )
}

fn cmp_json_datum(column: &Column, row: usize, target: &BinaryJSON) -> Ordering {
    let compare = || {
        let (cell, range) = column.borrow_cell_with_range(row)?;
        target.compare_encoded_left(cell, || {
            column
                .data
                .cached_json_layout(range)
                .map(|layout| layout.is_canonical())
        })
    };
    compare().unwrap_or_else(|| compare_binary_json(&column.get_json(row), target))
}

/// Go `cmpNullConst`: every cell of a `NULL`-typed column is equal, WITHOUT a
/// null pre-check.
fn cmp_null_const(_l: Row<'_>, _l_col: usize, _r: Row<'_>, _r_col: usize) -> Ordering {
    Ordering::Equal
}

/// Go `cmpVectorFloat32`.
fn cmp_vector_float32(l: Row<'_>, l_col: usize, r: Row<'_>, r_col: usize) -> Ordering {
    let left = &l.chunk().expect("empty Row has no Chunk").column_slots()[l_col];
    let right = &r.chunk().expect("empty Row has no Chunk").column_slots()[r_col];
    if let (Some(left), Some(right)) = (left.owned_column(), right.owned_column()) {
        return cmp_vector_column(left, l.idx(), right, r.idx());
    }
    cmp_aliased_vector_float32(left, l.idx(), right, r.idx())
}

#[cold]
fn cmp_aliased_vector_float32(
    left: &ColumnSlot,
    left_row: usize,
    right: &ColumnSlot,
    right_row: usize,
) -> Ordering {
    {
        let left_column = left.read();
        if left.same_identity(right) {
            return cmp_vectors_in_one_column(&left_column, left_row, right_row);
        }
        // A writer can hold the other owner while waiting for this one.
        // Only keep both guards when the second read can proceed immediately.
        if let Some(order) = right.try_read().and_then(|right_column| {
            try_cmp_vectors_in_distinct_columns(&left_column, left_row, &right_column, right_row)
        }) {
            return order;
        }
    }
    let left_null = left.read().is_null(left_row);
    let right_null = right.read().is_null(right_row);
    if left_null || right_null {
        return cmp_null(left_null, right_null);
    }
    let left_value = left.read().get_vector_float32(left_row);
    left_value.compare(&right.read().get_vector_float32(right_row))
}

#[inline]
fn cmp_vectors_in_one_column(column: &Column, left_row: usize, right_row: usize) -> Ordering {
    let bitmap = &*column.null_bitmap;
    let is_null = |row: usize| bitmap[row / 8] & (1 << (row & 7)) == 0;
    let (left_null, right_null) = (is_null(left_row), is_null(right_row));
    if left_null || right_null {
        return cmp_null(left_null, right_null);
    }
    let offsets = column.offsets.as_slice();
    let bytes = column.data.read();
    cmp_vector_bytes(
        &bytes[offsets.cell_range(left_row)],
        &bytes[offsets.cell_range(right_row)],
    )
}

fn try_cmp_vectors_in_distinct_columns(
    left: &Column,
    left_row: usize,
    right: &Column,
    right_row: usize,
) -> Option<Ordering> {
    if let Some(order) = column_null_order(left, left_row, right, right_row) {
        return Some(order);
    }
    let left_bytes = left.borrow_nonnull_var_cell(left_row)?;
    let right_bytes = right.borrow_nonnull_var_cell(right_row)?;
    Some(cmp_vector_bytes(left_bytes, right_bytes))
}

#[inline]
fn cmp_vector_column(left: &Column, left_row: usize, right: &Column, right_row: usize) -> Ordering {
    if std::ptr::eq(left, right) {
        return cmp_vectors_in_one_column(left, left_row, right_row);
    }
    try_cmp_vectors_in_distinct_columns(left, left_row, right, right_row)
        .unwrap_or_else(|| cmp_shared_vector_column(left, left_row, right, right_row))
}

#[cold]
fn cmp_shared_vector_column(
    left: &Column,
    left_row: usize,
    right: &Column,
    right_row: usize,
) -> Ordering {
    left.get_vector_float32(left_row)
        .compare(&right.get_vector_float32(right_row))
}

#[inline]
fn cmp_vector_bytes(left: &[u8], right: &[u8]) -> Ordering {
    VectorFloat32Ref::compare_encoded(left, right)
        .unwrap_or_else(|error| panic!("invalid VectorFloat32 chunk cell: {error}"))
}

#[inline]
fn vector_view(bytes: &[u8]) -> VectorFloat32Ref<'_> {
    VectorFloat32Ref::from_encoded(bytes)
        .unwrap_or_else(|error| panic!("invalid VectorFloat32 chunk cell: {error}"))
}

fn decimal_for_comparison(value: &Decimal) -> MyDecimal {
    if value.storage_scale() == value.scale() {
        if let Ok(decimal) = value.to_my_decimal() {
            return decimal;
        }
    }
    // Preserve the display-scale and overflow behavior of the text path.
    MyDecimal::from_string(value.to_string().as_bytes()).0
}

pub(crate) fn search_column_datum<const UPPER: bool>(
    column: &Column,
    rows: usize,
    selection: Option<&[usize]>,
    target: &Datum,
) -> (usize, bool) {
    let physical_row = |row| selection.map_or(row, |selection| selection[row]);
    match target {
        Datum::Decimal(value) => {
            let target = decimal_for_comparison(value);
            column.with_my_decimal_data(|data| {
                search_bound::<UPPER>(rows, |row| {
                    target.compare_with_packed(decimal_cell(data, physical_row(row)))
                        .expect("chunk decimal cell holds a valid MyDecimal")
                        .reverse()
                })
            })
        }
        Datum::Bytes(target) => search_byte_bound::<UPPER>(column, rows, physical_row, target),
        Datum::BinaryLiteral(target) | Datum::Bit(target) => {
            search_byte_bound::<UPPER>(column, rows, physical_row, target.as_bytes())
        }
        Datum::Enum(target, _) => {
            search_name_value_bound::<UPPER>(column, rows, physical_row, target.value())
        }
        Datum::Set(target, _) => {
            search_name_value_bound::<UPPER>(column, rows, physical_row, target.value())
        }
        _ => search_bound::<UPPER>(rows, |row| {
            compare_column_datum(column, physical_row(row), target)
        }),
    }
}

fn search_byte_bound<const UPPER: bool>(
    column: &Column,
    rows: usize,
    physical_row: impl Fn(usize) -> usize,
    target: &[u8],
) -> (usize, bool) {
    let offsets = column.offsets.as_slice();
    let data = column.data.read();
    search_bound::<UPPER>(rows, |row| {
        compare_bytes(&data[offsets.cell_range(physical_row(row))], target)
    })
}

fn search_name_value_bound<const UPPER: bool>(
    column: &Column,
    rows: usize,
    physical_row: impl Fn(usize) -> usize,
    target: u64,
) -> (usize, bool) {
    let offsets = column.offsets.as_slice();
    let data = column.data.read();
    search_bound::<UPPER>(rows, |row| {
        name_value_parts(&data[offsets.cell_range(physical_row(row))]).1.cmp(&target)
    })
}

#[inline]
fn search_bound<const UPPER: bool>(
    rows: usize,
    compare: impl Fn(usize) -> Ordering,
) -> (usize, bool) {
    if UPPER {
        return (search(rows, |row| compare(row) == Ordering::Greater), false);
    }
    let mut high = rows - 1;
    let mut high_order = compare(high);
    if high_order == Ordering::Less {
        return (rows, false);
    }
    // The initial probe already established that the last row qualifies.
    // Keep its comparison outcome and reuse the probe that moved the bound.
    let mut low = 0;
    while low < high {
        let middle = low + (high - low) / 2;
        let order = compare(middle);
        if order == Ordering::Less {
            low = middle + 1;
        } else {
            high = middle;
            high_order = order;
        }
    }
    (high, high_order == Ordering::Equal)
}

#[inline]
fn compare_bytes(left: &[u8], right: &[u8]) -> Ordering {
    if let ([left], [right]) = (left, right) {
        return left.cmp(right);
    }
    if let (Ok(left), Ok(right)) = (<&[u8; 8]>::try_from(left), <&[u8; 8]>::try_from(right)) {
        return u64::from_be_bytes(*left).cmp(&u64::from_be_bytes(*right));
    }
    left.cmp(right)
}

#[inline]
fn compare_cell_bytes(column: &Column, row: usize, target: &[u8]) -> Ordering {
    let data = column.data.read();
    compare_bytes(&data[column.var_byte_range(row)], target)
}

/// Go `chunk.Compare`: compares the cell at `col_idx` of `row` with the datum
/// `ad`, dispatching on the DATUM's kind.
///
/// Two properties that differ from [`get_compare_func`] and are load-bearing
/// for range building:
///
/// * There is no null pre-check on the CELL. Only a `Null`/`MinNotNull` datum
///   consults `row.IsNull`; every other kind reads the cell's value directly.
/// * A BIT/binary-literal/bytes datum uses a RAW byte compare, where
///   [`get_compare_func`]'s `Bit` arm uses `BinaryLiteral::compare`. The two
///   disagree on e.g. `0x00_01` vs `0x01`, and Go has the same split.
///
/// Go's `default` arm returns 0 for any kind it does not name.
#[must_use]
#[inline]
pub fn compare(row: Row<'_>, col_idx: usize, ad: &Datum) -> Ordering {
    match ad {
        Datum::MaxValue => Ordering::Less,
        Datum::Raw(_) => Ordering::Equal,
        _ => {
            let slot = &row.chunk().expect("empty Row has no Chunk").column_slots()[col_idx];
            if let Some(column) = slot.owned_column() {
                compare_column_datum(column, row.idx(), ad)
            } else {
                compare_column_datum(&slot.read(), row.idx(), ad)
            }
        }
    }
}

#[inline]
fn compare_column_datum(column: &Column, row: usize, ad: &Datum) -> Ordering {
    match ad {
        Datum::Null => {
            if column.is_null(row) {
                Ordering::Equal
            } else {
                Ordering::Greater
            }
        }
        Datum::MinNotNull => {
            if column.is_null(row) {
                Ordering::Less
            } else {
                Ordering::Greater
            }
        }
        Datum::MaxValue => Ordering::Less,
        Datum::Int(value) => column.get_int64(row).cmp(value),
        Datum::UInt(value) => column.get_uint64(row).cmp(value),
        // Go rounds the datum through float32 before widening both operands.
        Datum::Float32(value) => cmp_float(
            f64::from(column.get_float32(row)),
            f64::from(*value as f32),
        ),
        Datum::Real(value) => cmp_float(column.get_float64(row), *value),
        Datum::String(value) => {
            let bytes = column.get_bytes(row);
            get_collator(value.collation().name()).compare(bytes.as_ref(), value.bytes())
        }
        Datum::Bytes(value) => compare_cell_bytes(column, row, value),
        Datum::BinaryLiteral(value) | Datum::Bit(value) => {
            compare_cell_bytes(column, row, value.as_bytes())
        }
        Datum::Decimal(value) => column.with_my_decimal_data(|data| {
            value.compare_chunk_decimal(decimal_cell(data, row))
                .expect("chunk decimal cell holds a valid MyDecimal")
                .reverse()
        }),
        Datum::Duration(value) => column.get_int64(row).cmp(&value.nanoseconds()),
        Datum::Enum(value, _) => column.get_name_value_number(row).cmp(&value.value()),
        Datum::Set(value, _) => column.get_name_value_number(row).cmp(&value.value()),
        Datum::Json(value) => cmp_json_datum(column, row, value),
        Datum::VectorFloat32(value) => {
            let bytes = column.get_bytes(row);
            vector_view(&bytes).compare(&value.as_view())
        }
        Datum::Time(value) => column.get_time(row).compare(*value),
        Datum::Raw(_) => Ordering::Equal,
    }
}

/// Go's `sort.Search`: the smallest `i` in `0..n` for which `pred(i)` holds, or
/// `n` when none does. This is Go's loop, literally.
///
/// [`crate::chunk::Chunk::lower_bound`]'s `match` flag looks like it depends on
/// WHICH rows this bisection happens to probe, but it does not, and a mutation
/// probe confirmed it: replacing this loop with a linear scan leaves every
/// fixture answer, `match` included, unchanged. The reason is that the loop
/// only ever lowers `j` to an `h` whose `pred(h)` was true, so a returned index
/// below `n` was necessarily probed -- and for a monotone predicate over a
/// sorted column that index is equal to the probe exactly when the value is
/// present. So `match` means "the value is in the column", for any correct
/// search.
pub(crate) fn search(n: usize, mut pred: impl FnMut(usize) -> bool) -> usize {
    let (mut i, mut j) = (0usize, n);
    while i < j {
        let h = (i + j) / 2;
        if pred(h) {
            j = h;
        } else {
            i = h + 1;
        }
    }
    i
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::chunk::Chunk;
    use tidb_datatype::{
        time_from_days, BinaryJSON, BinaryJSONValue, MyDecimal, MySqlDuration, MysqlEnum, MysqlSet,
    };

    #[test]
    fn compare_string_cells_with_shallow_aliases() {
        use crate::mutrow::MutRow;
        use tidb_datatype::get_collator_with_mode;

        let field_type = FieldType::new(FieldTypeCode::Varchar);
        let fields = std::slice::from_ref(&field_type);
        let mut source = Chunk::new_chunk_with_capacity(fields, 2);
        source.append_bytes(0, b"abcdefghi  ");
        source.append_bytes(0, b"abcdefghi\0");
        let mut alias = MutRow::mut_row_from_types(fields);
        alias.shallow_copy_partial_row(0, &mut source, 0);

        let alias = alias.to_row().chunk().unwrap().column(0);
        let source = source.column(0);
        assert!(source.data.backing_ptr_eq(&alias.data));

        let padded = get_collator_with_mode(true, "utf8mb4_bin");
        let binary = get_collator_with_mode(true, "binary");
        assert_eq!(compare_column_strings(padded, &source, 0, &alias, 0), Ordering::Equal);
        assert_eq!(compare_column_strings(padded, &source, 1, &alias, 0), Ordering::Greater);
        assert_eq!(compare_column_strings(binary, &source, 1, &alias, 0), Ordering::Less);

        let mut owned = Column::new_column(&field_type, 1);
        owned.append_bytes(b"abcdefghi");
        assert_eq!(compare_column_strings(padded, &owned, 0, &alias, 0), Ordering::Equal);
        assert_eq!(compare_column_strings(binary, &alias, 0, &owned, 0), Ordering::Greater);
    }

    /// Go `chunk_test.go`'s `newAllTypes`: one field type per chunk column
    /// kind, in Go's order. The unsigned BIGINT, SET and ENUM entries carry
    /// `UnsignedFlag` and (for SET/ENUM) the two elements `a`, `b`.
    fn new_all_types() -> Vec<FieldType> {
        let mut ret = vec![
            FieldType::new(FieldTypeCode::Tiny),
            FieldType::new(FieldTypeCode::Short),
            FieldType::new(FieldTypeCode::Int24),
            FieldType::new(FieldTypeCode::Long),
            FieldType::new(FieldTypeCode::LongLong),
        ];
        ret.push(FieldType::new(FieldTypeCode::LongLong).with_unsigned(true));
        ret.extend([
            FieldType::new(FieldTypeCode::Year),
            FieldType::new(FieldTypeCode::Float),
            FieldType::new(FieldTypeCode::Double),
            FieldType::new(FieldTypeCode::String),
            FieldType::new(FieldTypeCode::VarString),
            FieldType::new(FieldTypeCode::Varchar),
            FieldType::new(FieldTypeCode::Blob),
            FieldType::new(FieldTypeCode::TinyBlob),
            FieldType::new(FieldTypeCode::MediumBlob),
            FieldType::new(FieldTypeCode::LongBlob),
            FieldType::new(FieldTypeCode::Date),
            FieldType::new(FieldTypeCode::Datetime),
            FieldType::new(FieldTypeCode::Timestamp),
            FieldType::new(FieldTypeCode::Duration),
            FieldType::new(FieldTypeCode::NewDecimal),
        ]);
        ret.push(
            FieldType::new(FieldTypeCode::Set)
                .with_unsigned(true)
                .with_elems(["a", "b"]),
        );
        ret.push(
            FieldType::new(FieldTypeCode::Enum)
                .with_unsigned(true)
                .with_elems(["a", "b"]),
        );
        ret.extend([
            FieldType::new(FieldTypeCode::Bit),
            FieldType::new(FieldTypeCode::Json),
        ]);
        ret
    }

    fn json_int(value: i64) -> BinaryJSON {
        BinaryJSON::from_typed_value(&BinaryJSONValue::Int64(value)).expect("an int is valid JSON")
    }

    #[test]
    fn json_comparison_rechecks_wide_cells_after_writes_and_reset() {
        let field = FieldType::new(FieldTypeCode::Json);
        let mut chunk = Chunk::new_chunk_with_capacity(std::slice::from_ref(&field), 2);
        let compare = get_column_compare_func(&field).unwrap();
        let text = "界".repeat(2048);
        let left = BinaryJSON::parse(&format!("\"z{text}\"")).unwrap();
        let right = BinaryJSON::parse(&format!("\"a{text}x\"")).unwrap();
        let read_order = |chunk: &Chunk| {
            let column = chunk.column(0);
            let column_order = compare(&column, 0, &column, 1);
            assert_eq!(
                cmp_json(chunk.get_row(0), 0, chunk.get_row(1), 0),
                column_order,
            );
            column_order
        };

        chunk.append_json(0, &left);
        chunk.append_json(0, &right);
        assert_eq!(read_order(&chunk), Ordering::Greater);

        let mut invalid = right.encoded();
        *invalid.last_mut().unwrap() = 0xff;
        chunk.column_mut(0).set_raw(1, &invalid);
        assert_eq!(read_order(&chunk), Ordering::Less);

        chunk.reset();
        chunk.append_json(0, &left);
        chunk.append_json(0, &right);
        assert_eq!(read_order(&chunk), Ordering::Greater);
    }

    #[test]
    fn json_datum_comparison_reuses_and_invalidates_the_row_layout() {
        let field = FieldType::new(FieldTypeCode::Json);
        let mut chunk = Chunk::new_chunk_with_capacity(&[field], 1);
        let fill = "界".repeat(Column::MIN_RETAINED_CELL_BYTES / 3 + 32);
        let left = BinaryJSON::parse(&format!("\"z{fill}\"")).unwrap();
        let right = BinaryJSON::parse(&format!("\"a{fill}x\"")).unwrap();
        let unchecked = BinaryJSON::from_encoded_parts(left.type_code(), left.value());
        chunk.append_json(0, &unchecked);
        let target = Datum::Json(right);
        let read_layout = |chunk: &Chunk| {
            let column = chunk.column(0);
            let (_, range) = column.borrow_cell_with_range(0).unwrap();
            column
                .data
                .known_json_layout(&range)
                .map(|layout| layout.is_canonical())
        };

        assert_eq!(read_layout(&chunk), None);
        assert_eq!(compare(chunk.get_row(0), 0, &target), Ordering::Greater);
        assert_eq!(read_layout(&chunk), Some(true));

        let mut invalid = left.encoded();
        *invalid.last_mut().unwrap() = 0xff;
        chunk.column_mut(0).set_raw(0, &invalid);
        assert_eq!(read_layout(&chunk), None);
        assert_eq!(compare(chunk.get_row(0), 0, &target), Ordering::Less);
        assert_eq!(read_layout(&chunk), Some(false));
    }

    /// Appends the `k`-th value of Go's `TestCopyTo` ladder to every column.
    fn append_kth_row(chk: &mut Chunk, all_types: &[FieldType], k: i64) {
        for (i, tp) in all_types.iter().enumerate() {
            match tp.code() {
                FieldTypeCode::Tiny
                | FieldTypeCode::Short
                | FieldTypeCode::Int24
                | FieldTypeCode::Long
                | FieldTypeCode::LongLong
                | FieldTypeCode::Year => {
                    if tp.is_unsigned() {
                        chk.append_uint64(i, k as u64);
                    } else {
                        chk.append_int64(i, k);
                    }
                }
                FieldTypeCode::Float => chk.append_float32(i, k as f32),
                FieldTypeCode::Double => chk.append_float64(i, k as f64),
                FieldTypeCode::String
                | FieldTypeCode::VarString
                | FieldTypeCode::Varchar
                | FieldTypeCode::Blob
                | FieldTypeCode::TinyBlob
                | FieldTypeCode::MediumBlob
                | FieldTypeCode::LongBlob => chk.append_string(i, k.to_string()),
                FieldTypeCode::Date | FieldTypeCode::Datetime | FieldTypeCode::Timestamp => {
                    chk.append_time(i, time_from_days(2000 * 365 + k));
                }
                FieldTypeCode::Duration => chk.append_duration(
                    i,
                    MySqlDuration::from_nanoseconds(k * 1_000_000_000, 0).expect("in range"),
                ),
                FieldTypeCode::NewDecimal => chk.append_my_decimal(i, &MyDecimal::from_int(k)),
                FieldTypeCode::Set => chk.append_set(i, &MysqlSet::new("a", k as u64)),
                FieldTypeCode::Enum => chk.append_enum(i, &MysqlEnum::new("a", k as u64)),
                FieldTypeCode::Bit => chk.append_bytes(i, &[k as u8]),
                FieldTypeCode::Json => chk.append_json(i, &json_int(k)),
                other => panic!("type not handled: {other:?}"),
            }
        }
    }

    /// Go `TestCompare` (`pkg/util/chunk/chunk_test.go`): for EVERY column
    /// kind, a chunk of three rows -- null, small, big -- must order
    /// null < small < big and be reflexive.
    #[test]
    fn go_test_compare() {
        let all_types = new_all_types();
        let mut chk = Chunk::new_chunk_with_capacity(&all_types, 32);
        for i in 0..all_types.len() {
            chk.append_null(i);
        }
        // The "small" row: Go uses 0/-1 per signedness, "0", day 2000*365,
        // zero duration, decimal 0, element value 0, one zero byte, JSON 0.
        for (i, tp) in all_types.iter().enumerate() {
            match tp.code() {
                FieldTypeCode::Tiny
                | FieldTypeCode::Short
                | FieldTypeCode::Int24
                | FieldTypeCode::Long
                | FieldTypeCode::LongLong
                | FieldTypeCode::Year => {
                    if tp.is_unsigned() {
                        chk.append_uint64(i, 0);
                    } else {
                        chk.append_int64(i, -1);
                    }
                }
                FieldTypeCode::Float => chk.append_float32(i, 0.0),
                FieldTypeCode::Double => chk.append_float64(i, 0.0),
                FieldTypeCode::String
                | FieldTypeCode::VarString
                | FieldTypeCode::Varchar
                | FieldTypeCode::Blob
                | FieldTypeCode::TinyBlob
                | FieldTypeCode::MediumBlob
                | FieldTypeCode::LongBlob => chk.append_string(i, "0"),
                FieldTypeCode::Date | FieldTypeCode::Datetime | FieldTypeCode::Timestamp => {
                    chk.append_time(i, time_from_days(2000 * 365));
                }
                FieldTypeCode::Duration => {
                    chk.append_duration(i, MySqlDuration::from_nanoseconds(0, 0).expect("zero"));
                }
                FieldTypeCode::NewDecimal => chk.append_my_decimal(i, &MyDecimal::from_int(0)),
                FieldTypeCode::Set => chk.append_set(i, &MysqlSet::new("a", 0)),
                FieldTypeCode::Enum => chk.append_enum(i, &MysqlEnum::new("a", 0)),
                FieldTypeCode::Bit => chk.append_bytes(i, &[0]),
                FieldTypeCode::Json => chk.append_json(i, &json_int(0)),
                other => panic!("type not handled: {other:?}"),
            }
        }
        // The "big" row. Note the unsigned column takes MaxUint64, which is
        // the whole point: read as a SIGNED int it would be -1 and sort BELOW
        // the small row's 0.
        for (i, tp) in all_types.iter().enumerate() {
            match tp.code() {
                FieldTypeCode::Tiny
                | FieldTypeCode::Short
                | FieldTypeCode::Int24
                | FieldTypeCode::Long
                | FieldTypeCode::LongLong
                | FieldTypeCode::Year => {
                    if tp.is_unsigned() {
                        chk.append_uint64(i, u64::MAX);
                    } else {
                        chk.append_int64(i, 1);
                    }
                }
                FieldTypeCode::Float => chk.append_float32(i, 1.0),
                FieldTypeCode::Double => chk.append_float64(i, 1.0),
                FieldTypeCode::String
                | FieldTypeCode::VarString
                | FieldTypeCode::Varchar
                | FieldTypeCode::Blob
                | FieldTypeCode::TinyBlob
                | FieldTypeCode::MediumBlob
                | FieldTypeCode::LongBlob => chk.append_string(i, "1"),
                FieldTypeCode::Date | FieldTypeCode::Datetime | FieldTypeCode::Timestamp => {
                    chk.append_time(i, time_from_days(2001 * 365));
                }
                FieldTypeCode::Duration => chk.append_duration(
                    i,
                    MySqlDuration::from_nanoseconds(1_000_000_000, 0).expect("one second"),
                ),
                FieldTypeCode::NewDecimal => chk.append_my_decimal(i, &MyDecimal::from_int(1)),
                FieldTypeCode::Set => chk.append_set(i, &MysqlSet::new("b", 1)),
                FieldTypeCode::Enum => chk.append_enum(i, &MysqlEnum::new("b", 1)),
                FieldTypeCode::Bit => chk.append_bytes(i, &[1]),
                FieldTypeCode::Json => chk.append_json(i, &json_int(1)),
                other => panic!("type not handled: {other:?}"),
            }
        }

        let (row_null, row_small, row_big) = (chk.get_row(0), chk.get_row(1), chk.get_row(2));
        for (i, tp) in all_types.iter().enumerate() {
            let cmp_func = get_compare_func(tp).expect("every chunk column kind has a comparator");
            assert_eq!(
                cmp_func(row_null, i, row_null, i),
                Ordering::Equal,
                "col {i}"
            );
            assert_eq!(
                cmp_func(row_null, i, row_small, i),
                Ordering::Less,
                "col {i}"
            );
            assert_eq!(
                cmp_func(row_small, i, row_null, i),
                Ordering::Greater,
                "col {i}"
            );
            assert_eq!(
                cmp_func(row_small, i, row_small, i),
                Ordering::Equal,
                "col {i}"
            );
            assert_eq!(
                cmp_func(row_small, i, row_big, i),
                Ordering::Less,
                "col {i}"
            );
            assert_eq!(
                cmp_func(row_big, i, row_small, i),
                Ordering::Greater,
                "col {i}"
            );
            assert_eq!(cmp_func(row_big, i, row_big, i), Ordering::Equal, "col {i}");
        }
    }

    /// Go `TestCopyTo` (`pkg/util/chunk/chunk_test.go`): a `CopyConstruct`ed
    /// chunk must compare EQUAL to its source cell by cell, over 100 rows of
    /// every column kind plus a leading all-null row.
    #[test]
    fn go_test_copy_to() {
        let all_types = new_all_types();
        let mut chk = Chunk::new_chunk_with_capacity(&all_types, 101);
        for i in 0..all_types.len() {
            chk.append_null(i);
        }
        for k in 0..100 {
            append_kth_row(&mut chk, &all_types, k);
        }

        let ck1 = chk.copy_construct();
        for k in 0..101 {
            let row = chk.get_row(k);
            let r1 = ck1.get_row(k);
            for (i, tp) in all_types.iter().enumerate() {
                let cmp_func = get_compare_func(tp).expect("comparator");
                assert_eq!(cmp_func(row, i, r1, i), Ordering::Equal, "row {k} col {i}");
            }
        }
    }

    #[test]
    fn vector_float32_comparators_cover_null_empty_and_lexicographic_values() {
        use tidb_datatype::VectorFloat32;

        let field = FieldType::new(FieldTypeCode::VectorFloat32);
        let mut chunk = Chunk::new_chunk_with_capacity(&[field.clone(), field.clone()], 4);
        chunk.append_null(0);
        chunk.append_vector_float32(0, &VectorFloat32::default());
        chunk.append_vector_float32(0, &VectorFloat32::must_create(vec![1.0, 2.0]));
        chunk.append_vector_float32(0, &VectorFloat32::must_create(vec![1.0, 3.0]));

        let compare_cells = get_compare_func(&field).expect("vector comparator");
        assert_eq!(
            compare_cells(chunk.get_row(0), 0, chunk.get_row(1), 0),
            Ordering::Less
        );
        assert_eq!(
            compare_cells(chunk.get_row(1), 0, chunk.get_row(2), 0),
            Ordering::Less
        );
        assert_eq!(
            compare_cells(chunk.get_row(2), 0, chunk.get_row(3), 0),
            Ordering::Less
        );
        assert_eq!(
            compare(
                chunk.get_row(2),
                0,
                &Datum::VectorFloat32(VectorFloat32::must_create(vec![1.0, 2.0])),
            ),
            Ordering::Equal
        );

        chunk.make_ref(0, 1);
        assert_eq!(
            compare_cells(chunk.get_row(2), 0, chunk.get_row(3), 1),
            Ordering::Less
        );
    }

    #[test]
    fn aliased_wire_vector_comparison_preserves_null_and_late_order() {
        use crate::codec::Codec;
        use crate::mutrow::MutRow;
        use tidb_datatype::VectorFloat32;

        let field = FieldType::new(FieldTypeCode::VectorFloat32);
        let fields = [
            FieldType::new(FieldTypeCode::Varchar),
            field.clone(),
            field.clone(),
            field.clone(),
            field.clone(),
        ];
        let mut source = Chunk::new_chunk_with_capacity(&fields, 3);
        source.append_bytes(0, b"xx");
        source.append_null(1);
        source.append_null(2);
        source.append_null(3);
        source.append_null(4);
        for tail in [1.0, 2.0] {
            source.append_bytes(0, b"x");
            let mut values = vec![0.5; 1024];
            values[1023] = tail;
            let vector = VectorFloat32::must_create(values);
            source.append_vector_float32(1, &vector);
            source.append_vector_float32(2, &vector);
            source.append_null(3);
            source.append_null(4);
        }

        let codec = Codec::new(fields.to_vec());
        let frame = codec.encode(&source).into();
        let mut chunk = Chunk::new_chunk_with_capacity(&fields, 3);
        assert!(codec
            .try_decode_bytes_to_chunk(&frame, &mut chunk)
            .unwrap()
            .is_empty());
        assert_ne!(chunk.column(1).get_bytes(1).as_ptr().align_offset(4), 0);
        let compare_cells = get_compare_func(&field).unwrap();
        let check = |chunk: &Chunk| {
            for (left, right, order) in [
                (0, 0, Ordering::Equal),
                (0, 1, Ordering::Less),
                (1, 2, Ordering::Less),
                (2, 1, Ordering::Greater),
            ] {
                assert_eq!(compare_cells(chunk.get_row(left), 1, chunk.get_row(right), 2), order);
            }
        };
        chunk.make_ref(1, 3);
        chunk.make_ref(2, 4);
        check(&chunk);
        chunk.make_ref(1, 2);
        check(&chunk);

        let mut shallow = MutRow::mut_row_from_types(&fields);
        shallow.shallow_copy_partial_row(0, &mut source, 1);
        source.make_ref(1, 3);
        source.make_ref(2, 4);
        assert!(source.column(1).has_mutably_shared_bytes());
        check(&source);
        source.make_ref(1, 2);
        check(&source);
    }

    #[test]
    fn compare_decimal_keeps_the_displayed_value_for_hidden_fraction_words() {
        let field = FieldType::new(FieldTypeCode::NewDecimal);
        let mut chunk = Chunk::new_chunk_with_capacity(&[field], 1);
        let decimal = Decimal::from_literal("8")
            .true_div(&Decimal::from_literal("7"), 7)
            .unwrap();
        assert!(decimal.storage_scale() > decimal.scale());
        let (visible, _) = MyDecimal::from_string(decimal.to_string().as_bytes());
        chunk.append_my_decimal(0, &visible);
        let target = Datum::Decimal(decimal);
        assert_eq!(
            compare(chunk.get_row(0), 0, &target),
            Ordering::Equal
        );
        assert_eq!(chunk.lower_bound(0, &target), (0, true));
        assert_eq!(chunk.upper_bound(0, &target), 1);
    }

    #[test]
    fn bit_bounds_follow_selection_on_an_aliased_column() {
        let field = FieldType::new(FieldTypeCode::Bit);
        let mut source = Chunk::new_chunk_with_capacity(&[field], 5);
        for byte in [5, 128, 1, 128, 127] {
            source.append_bytes(0, &[byte]);
        }
        let mut selected = source.prune(&[0, 0]);
        selected.set_sel(Some(vec![2, 0, 4, 1, 3]));
        let target = Datum::Bit(BinaryLiteral::from(vec![128]));
        assert_eq!(selected.lower_bound(1, &target), (3, true));
        assert_eq!(selected.upper_bound(1, &target), 5);

        let absent = Datum::Bit(BinaryLiteral::from(vec![126]));
        assert_eq!(selected.lower_bound(1, &absent), (2, false));
        assert_eq!(selected.upper_bound(1, &absent), 2);
        assert_eq!(
            cmp_bit(selected.get_row(0), 0, selected.get_row(3), 1),
            Ordering::Less,
        );
    }
}

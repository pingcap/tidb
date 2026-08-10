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
//! * [`Chunk::lower_bound`]/[`Chunk::upper_bound`] -- Go's binary searches over
//!   a non-decreasing column.
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

use crate::chunk::Chunk;
use crate::row::Row;
use std::cmp::Ordering;
use tidb_datatype::{
    compare_binary_json, get_collator, Datum, FieldType, FieldTypeCode, MyDecimal,
};

/// Go `chunk.CompareFunc`: compares column `l_col` of `l` with column `r_col`
/// of `r`. The two columns must have the same type.
pub type CompareFunc =
    Box<dyn for<'a> Fn(Row<'a>, usize, Row<'a>, usize) -> Ordering + Send + Sync>;

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
    null_order(l, l_col, r, r_col)
        .unwrap_or_else(|| l.get_my_decimal(l_col).compare(&r.get_my_decimal(r_col)))
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
    null_order(l, l_col, r, r_col)
        .unwrap_or_else(|| l.get_name_value(l_col).1.cmp(&r.get_name_value(r_col).1))
}

/// Go `cmpBit`: `types.BinaryLiteral.Compare`, which strips leading zero bytes
/// and orders by LENGTH first -- NOT `bytes.Compare`.
fn cmp_bit(l: Row<'_>, l_col: usize, r: Row<'_>, r_col: usize) -> Ordering {
    null_order(l, l_col, r, r_col).unwrap_or_else(|| {
        let left_bytes = l.get_bytes(l_col);
        let right_bytes = r.get_bytes(r_col);
        let left = tidb_datatype::BinaryLiteral::from(left_bytes.as_ref());
        let right = tidb_datatype::BinaryLiteral::from(right_bytes.as_ref());
        left.compare(&right)
    })
}

/// Go `cmpJSON`.
fn cmp_json(l: Row<'_>, l_col: usize, r: Row<'_>, r_col: usize) -> Ordering {
    null_order(l, l_col, r, r_col)
        .unwrap_or_else(|| compare_binary_json(&l.get_json(l_col), &r.get_json(r_col)))
}

/// Go `cmpNullConst`: every cell of a `NULL`-typed column is equal, WITHOUT a
/// null pre-check.
fn cmp_null_const(_l: Row<'_>, _l_col: usize, _r: Row<'_>, _r_col: usize) -> Ordering {
    Ordering::Equal
}

/// Go `cmpVectorFloat32`.
fn cmp_vector_float32(l: Row<'_>, l_col: usize, r: Row<'_>, r_col: usize) -> Ordering {
    null_order(l, l_col, r, r_col).unwrap_or_else(|| {
        l.get_vector_float32(l_col)
            .compare(&r.get_vector_float32(r_col))
    })
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
pub fn compare(row: Row<'_>, col_idx: usize, ad: &Datum) -> Ordering {
    match ad {
        Datum::Null => {
            if row.is_null(col_idx) {
                Ordering::Equal
            } else {
                Ordering::Greater
            }
        }
        Datum::MinNotNull => {
            if row.is_null(col_idx) {
                Ordering::Less
            } else {
                Ordering::Greater
            }
        }
        Datum::MaxValue => Ordering::Less,
        Datum::Int(value) => row.get_int64(col_idx).cmp(value),
        Datum::UInt(value) => row.get_uint64(col_idx).cmp(value),
        // Go's `Datum.GetFloat32` rounds the DATUM through `float32`, then
        // widens both operands to `float64` for `cmp.Compare`.
        Datum::Float32(value) => cmp_float(
            f64::from(row.get_float32(col_idx)),
            f64::from(*value as f32),
        ),
        Datum::Real(value) => cmp_float(row.get_float64(col_idx), *value),
        Datum::String(value) => {
            let bytes = row.get_bytes(col_idx);
            get_collator(value.collation().name()).compare(bytes.as_ref(), value.bytes())
        }
        Datum::Bytes(value) => row.get_bytes(col_idx).as_ref().cmp(value.as_slice()),
        Datum::BinaryLiteral(value) | Datum::Bit(value) => {
            row.get_bytes(col_idx).as_ref().cmp(value.as_bytes())
        }
        // Go compares two `*types.MyDecimal`. This port's decimal DATUM is the
        // text-shaped `Decimal`, so it is re-parsed into a `MyDecimal` -- the
        // canonical text round-trips exactly, and the comparison itself is then
        // Go's `MyDecimal.Compare`.
        Datum::Decimal(value) => {
            let (right, _) = MyDecimal::from_string(value.to_string().as_bytes());
            row.get_my_decimal(col_idx).compare(&right)
        }
        Datum::Duration(value) => row
            .get_duration(col_idx, 0)
            .nanoseconds()
            .cmp(&value.nanoseconds()),
        Datum::Enum(value, _) => row.get_enum(col_idx).value().cmp(&value.value()),
        Datum::Set(value, _) => row.get_set(col_idx).value().cmp(&value.value()),
        Datum::Json(value) => compare_binary_json(&row.get_json(col_idx), value),
        Datum::VectorFloat32(value) => row.get_vector_float32(col_idx).compare(value),
        Datum::Time(value) => row.get_time(col_idx).compare(*value),
        Datum::Raw(_) => Ordering::Equal,
    }
}

/// Go's `sort.Search`: the smallest `i` in `0..n` for which `pred(i)` holds, or
/// `n` when none does. This is Go's loop, literally.
///
/// [`Chunk::lower_bound`]'s `match` flag looks like it depends on WHICH rows
/// this bisection happens to probe, but it does not, and a mutation probe
/// confirmed it: replacing this loop with a linear scan leaves every fixture
/// answer, `match` included, unchanged. The reason is that the loop only ever
/// lowers `j` to an `h` whose `pred(h)` was true, so a returned index below `n`
/// was necessarily probed -- and for a monotone predicate over a sorted column
/// that index is equal to the probe exactly when the value is present. So
/// `match` means "the value is in the column", for any correct search.
fn sort_search(n: usize, mut pred: impl FnMut(usize) -> bool) -> usize {
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

impl Chunk {
    /// Go `LowerBound`: on the non-decreasing column `col_idx`, the smallest
    /// index whose value is not less than `d`, plus whether a PROBED row was
    /// equal to `d`.
    ///
    /// Go reads the last row before searching, so this panics on an empty
    /// chunk exactly as Go does.
    #[must_use]
    pub fn lower_bound(&self, col_idx: usize, d: &Datum) -> (usize, bool) {
        if compare(self.get_row(self.num_rows() - 1), col_idx, d) == Ordering::Less {
            return (self.num_rows(), false);
        }
        let mut matched = false;
        let index = sort_search(self.num_rows(), |i| {
            let ordering = compare(self.get_row(i), col_idx, d);
            if ordering == Ordering::Equal {
                matched = true;
            }
            ordering != Ordering::Less
        });
        (index, matched)
    }

    /// Go `UpperBound`: on the non-decreasing column `col_idx`, the smallest
    /// index whose value is larger than `d`.
    #[must_use]
    pub fn upper_bound(&self, col_idx: usize, d: &Datum) -> usize {
        sort_search(self.num_rows(), |i| {
            compare(self.get_row(i), col_idx, d) == Ordering::Greater
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tidb_datatype::{
        time_from_days, BinaryJSON, BinaryJSONValue, MyDecimal, MySqlDuration, MysqlEnum, MysqlSet,
    };

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
        let mut chk = Chunk::new_with_capacity(&all_types, 32);
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
        let mut chk = Chunk::new_with_capacity(&all_types, 101);
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
        let mut chunk = Chunk::new_with_capacity(std::slice::from_ref(&field), 4);
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
    }
}

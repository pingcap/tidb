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

//! `pkg/util/chunk/row.go`: the `Row`, a cursor into one row of a [`Chunk`].
//!
//! Go's `Row` is `{c *Chunk, idx int}` -- a lightweight pointer plus index.
//! This port models it as a borrow, `Row<'a>`, so a row cannot outlive its
//! chunk.
//!
//! Ported: the accessors a simple query needs -- `chunk`, `idx`, `len`,
//! `get_int64`/`get_uint64`/`get_float32`/`get_float64`, `get_bytes`/`get_raw`,
//! `get_time`/`get_duration`, VectorFloat32, JSON, decimal, and `is_null`.
//! DEFERRED (documented): `GetDatumRow`, `CopyConstruct`, and
//! a `str`-typed `GetString` (pending the crate-wide bytes-vs-str policy).

use crate::chunk::Chunk;
use tidb_datatype::{
    Datum, Decimal, FieldType, FieldTypeCode, GoString, MyDecimal, MySqlDuration, Time,
    VectorFloat32,
};

/// Go `chunk.RowSize = unsafe.Sizeof(Row{})`: what one retained row CURSOR
/// costs, which memory-tracked operators add per row on top of the chunk
/// bytes (Go `sort_partition.go`'s `chunk.RowSize*rowNum`).
///
/// Go's `Row` is `{c *Chunk, idx int}` and this one is `{chunk: &Chunk, idx:
/// usize}` -- two words either way, so the two constants agree.
pub const ROW_SIZE: i64 = size_of::<Row<'static>>() as i64;

/// Go `chunk.Row`: a cursor to one row of a [`Chunk`].
#[derive(Clone, Copy, Debug)]
pub struct Row<'a> {
    chunk: Option<&'a Chunk>,
    idx: usize,
}

/// Go compares two `Row`s with `==` on `{c *Chunk, idx int}`, which is chunk
/// POINTER identity plus the index -- not a value comparison of the rows' data.
/// `iterator.go`'s `row != it.End()` loop condition and `iterator_test.go`'s
/// `require.Equal(t, rows[i], it.Current())` both rely on exactly that.
impl PartialEq for Row<'_> {
    fn eq(&self, other: &Self) -> bool {
        match (self.chunk, other.chunk) {
            (None, None) => self.idx == other.idx,
            (Some(left), Some(right)) => std::ptr::eq(left, right) && self.idx == other.idx,
            _ => false,
        }
    }
}

impl Eq for Row<'_> {}

impl<'a> Row<'a> {
    /// Builds a row cursor at physical index `idx` of `chunk`.
    #[must_use]
    pub(crate) fn new(chunk: &'a Chunk, idx: usize) -> Self {
        Row {
            chunk: Some(chunk),
            idx,
        }
    }

    /// Go's zero `Row{}` sentinel. It is distinct from a valid row of a
    /// zero-column chunk, whose column count is also zero.
    #[must_use]
    pub const fn empty() -> Self {
        Row {
            chunk: None,
            idx: 0,
        }
    }

    /// Go `Chunk`: the chunk this row belongs to.
    #[must_use]
    pub fn chunk(&self) -> &'a Chunk {
        self.chunk.expect("empty Row has no Chunk")
    }

    /// Go `Idx`: the (physical) row index within the chunk.
    #[must_use]
    pub fn idx(&self) -> usize {
        self.idx
    }

    /// Go `Len`: the number of columns.
    #[must_use]
    pub fn len(&self) -> usize {
        self.chunk.map_or(0, Chunk::num_cols)
    }

    /// Go `IsEmpty`: only the zero `Row{}` sentinel is empty. A valid row of a
    /// zero-column/virtual chunk is not empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.chunk.is_none()
    }

    /// Go `GetInt64`.
    #[must_use]
    pub fn get_int64(&self, col_idx: usize) -> i64 {
        self.chunk().columns()[col_idx].get_int64(self.idx)
    }

    /// Go `GetUint64`.
    #[must_use]
    pub fn get_uint64(&self, col_idx: usize) -> u64 {
        self.chunk().columns()[col_idx].get_uint64(self.idx)
    }

    /// Go `GetFloat32`.
    #[must_use]
    pub fn get_float32(&self, col_idx: usize) -> f32 {
        self.chunk().columns()[col_idx].get_float32(self.idx)
    }

    /// Go `GetFloat64`.
    #[must_use]
    pub fn get_float64(&self, col_idx: usize) -> f64 {
        self.chunk().columns()[col_idx].get_float64(self.idx)
    }

    /// Go `GetTime`.
    #[must_use]
    pub fn get_time(&self, col_idx: usize) -> Time {
        self.chunk().columns()[col_idx].get_time(self.idx)
    }

    /// Go `GetDuration`: reads the cell's nanoseconds and stamps `fill_fsp` on
    /// (the column stores no fsp).
    #[must_use]
    pub fn get_duration(&self, col_idx: usize, fill_fsp: i64) -> MySqlDuration {
        self.chunk().columns()[col_idx].get_duration(self.idx, fill_fsp)
    }

    /// Go `GetMyDecimal`.
    #[must_use]
    pub fn get_my_decimal(&self, col_idx: usize) -> MyDecimal {
        self.chunk().columns()[col_idx].get_my_decimal(self.idx)
    }

    /// Go `GetEnum`.
    #[must_use]
    pub fn get_enum(&self, col_idx: usize) -> tidb_datatype::MysqlEnum {
        self.chunk().columns()[col_idx].get_enum(self.idx)
    }

    /// Go `GetSet`.
    #[must_use]
    pub fn get_set(&self, col_idx: usize) -> tidb_datatype::MysqlSet {
        self.chunk().columns()[col_idx].get_set(self.idx)
    }

    /// Go `GetBytes`: the raw bytes of a variable-length column's cell.
    #[must_use]
    pub fn get_bytes(&self, col_idx: usize) -> &'a [u8] {
        self.chunk().columns()[col_idx].get_bytes(self.idx)
    }

    /// Go `GetRaw`: the raw element bytes for either column kind.
    #[must_use]
    pub fn get_raw(&self, col_idx: usize) -> &'a [u8] {
        self.chunk().columns()[col_idx].get_raw(self.idx)
    }

    /// Go `GetJSON`.
    #[must_use]
    pub fn get_json(&self, col_idx: usize) -> tidb_datatype::BinaryJSON {
        self.chunk().columns()[col_idx].get_json(self.idx)
    }

    /// Go `GetVectorFloat32`.
    #[must_use]
    pub fn get_vector_float32(&self, col_idx: usize) -> VectorFloat32 {
        self.chunk().columns()[col_idx].get_vector_float32(self.idx)
    }

    /// Go `getNameValue`: the `(name, value)` pair an ENUM/SET cell stores.
    #[must_use]
    pub fn get_name_value(&self, col_idx: usize) -> (GoString, u64) {
        self.chunk().columns()[col_idx].get_name_value(self.idx)
    }

    /// Go `IsNull`.
    #[must_use]
    pub fn is_null(&self, col_idx: usize) -> bool {
        self.chunk().columns()[col_idx].is_null(self.idx)
    }

    /// Go `GetDatum` (`DatumWithBuffer`): read the cell at `col_idx` as a
    /// [`Datum`], interpreted by `field_type`.
    ///
    /// Ported for the column kinds whose storage exists: NULL, the signed/
    /// unsigned integer family and `Year`, `Float`/`Double`, and the string/blob
    /// family (as a collation-tagged string), `Date`/`Datetime`/`Timestamp`
    /// (as a Time datum), `Duration` (fsp filled from the field type's
    /// decimal, matching Go), and `Bit` (the cell's own bytes, Go
    /// `SetMysqlBit`). The remaining types (VectorFloat32) land with their
    /// column getters; reaching one here panics rather than returning a wrong or
    /// silently-null datum.
    #[must_use]
    pub fn get_datum(&self, col_idx: usize, field_type: &FieldType) -> Datum {
        if self.is_null(col_idx) {
            return Datum::Null;
        }
        match field_type.code() {
            FieldTypeCode::Tiny
            | FieldTypeCode::Short
            | FieldTypeCode::Int24
            | FieldTypeCode::Long
            | FieldTypeCode::LongLong => {
                if field_type.is_unsigned() {
                    Datum::UInt(self.get_uint64(col_idx))
                } else {
                    Datum::Int(self.get_int64(col_idx))
                }
            }
            // Year is always read as a signed int64 regardless of the unsigned
            // flag (matches Go's DatumWithBuffer note).
            FieldTypeCode::Year => Datum::Int(self.get_int64(col_idx)),
            FieldTypeCode::Float => Datum::Float32(f64::from(self.get_float32(col_idx))),
            FieldTypeCode::Double => Datum::Real(self.get_float64(col_idx)),
            FieldTypeCode::Varchar
            | FieldTypeCode::VarString
            | FieldTypeCode::String
            | FieldTypeCode::Blob
            | FieldTypeCode::TinyBlob
            | FieldTypeCode::MediumBlob
            | FieldTypeCode::LongBlob => {
                let mut d = Datum::Null;
                d.set_string(self.get_bytes(col_idx).to_vec(), field_type.collation());
                d
            }
            // Go `d.SetMysqlBit(r.GetBytes(colIdx))`: a BIT column is a
            // VARIABLE-length chunk column (`getFixedLen` has no BIT arm), and
            // its cell is the binary literal's own bytes.
            FieldTypeCode::Bit => {
                Datum::Bit(tidb_datatype::BinaryLiteral::from(self.get_bytes(col_idx).to_vec()))
            }
            // Go `GetJSON`: the cell's first byte is the type code and the
            // rest is the value, which is what `BinaryJSON` stores.
            FieldTypeCode::Json => {
                let cell = self.get_bytes(col_idx);
                let (type_code, value) = cell
                    .split_first()
                    .expect("a JSON cell always carries its type code");
                Datum::Json(tidb_datatype::BinaryJSON::from_encoded_parts(
                    *type_code, value,
                ))
            }
            // Go `GetEnum`/`GetSet`: the cell is the 8-byte native value
            // followed by the element name, and the datum carries the column's
            // collation so name comparison stays collation-aware.
            FieldTypeCode::Enum => Datum::new_enum(
                self.get_enum(col_idx),
                field_type.collation(),
            ),
            FieldTypeCode::Set => Datum::new_set(
                self.get_set(col_idx),
                field_type.collation(),
            ),
            FieldTypeCode::Date | FieldTypeCode::Datetime | FieldTypeCode::Timestamp => {
                Datum::Time(self.get_time(col_idx))
            }
            // Go passes tp.GetDecimal() as the fill fsp.
            FieldTypeCode::Duration => {
                Datum::Duration(self.get_duration(col_idx, field_type.decimal()))
            }
            // Go additionally calls SetLength(tp.GetFlen()) and
            // SetFrac(tp.GetDecimal()) here. The value and its own fractional
            // digits round-trip exactly through the canonical decimal text
            // (matching Go's unspecified-decimal branch, which uses the stored
            // digitsFrac). DEFERRED, documented: the explicit
            // SetFrac(tp.GetDecimal()) display-metadata override, which only
            // differs when a column's declared scale disagrees with the scale
            // actually stored in the cell -- reproducing it needs a
            // frac-metadata setter that does not round the value.
            FieldTypeCode::NewDecimal => {
                let text = String::from_utf8(self.get_my_decimal(col_idx).to_string_bytes())
                    .expect("decimal text is ASCII");
                Datum::Decimal(Decimal::from_literal(&text))
            }
            FieldTypeCode::VectorFloat32 => Datum::VectorFloat32(self.get_vector_float32(col_idx)),
            other => panic!(
                "chunk Row::get_datum: column type {other:?} not yet supported (deferred with its column getter)"
            ),
        }
    }
}

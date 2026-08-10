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
//! `get_int64`/`get_uint64`/`get_float32`/`get_float64`,
//! `get_string`/`get_bytes`/`get_raw_len`/`get_raw`,
//! `get_time`/`get_duration`, VectorFloat32, JSON, decimal, and `is_null`.
//! Byte cells use guard-backed views because a MutRow may share their backing.

use crate::chunk::Chunk;
use crate::CellBytes;
use tidb_datatype::{
    deserialize_vector_float32, Datum, Decimal, EvalType, FieldType, FieldTypeCode, GoString,
    MyDecimal, MySqlDuration, Time, VectorFloat32, MYDECIMAL_STRUCT_SIZE, UNSPECIFIED_LENGTH,
};

/// Go `chunk.RowSize = unsafe.Sizeof(Row{})`: what one retained row CURSOR
/// costs, which memory-tracked operators add per row on top of the chunk
/// bytes (Go `sort_partition.go`'s `chunk.RowSize*rowNum`).
///
/// Go's `Row` is `{c *Chunk, idx int}` and this one is `{chunk: &Chunk, idx:
/// usize}` -- two words either way, so the two constants agree.
pub const ROW_SIZE: i64 = size_of::<Row<'static>>() as i64;

/// A typed chunk cell that cannot be safely materialized as a [`Datum`].
///
/// Trusted, source-shaped getters retain Go's panic contract. Network and
/// storage boundaries use [`Row::try_get_datum_row`] so malformed payloads
/// become ordinary query errors instead of unwinding the server.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RowDatumError {
    /// The caller supplied fewer field types than the row contains.
    MissingFieldType {
        /// First column without a field type.
        column: usize,
        /// Number of field types supplied by the caller.
        available: usize,
    },
    /// The requested column does not exist in this row.
    ColumnOutOfRange {
        /// Requested column ordinal.
        column: usize,
        /// Number of columns in this row.
        columns: usize,
    },
    /// The cell's bytes do not represent the declared field type.
    InvalidCell {
        /// Column ordinal.
        column: usize,
        /// Declared field type.
        field_type: FieldTypeCode,
        /// Validation failure.
        message: String,
    },
}

impl std::fmt::Display for RowDatumError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::MissingFieldType { column, available } => write!(
                formatter,
                "chunk row column {column} has no field type ({available} supplied)"
            ),
            Self::ColumnOutOfRange { column, columns } => write!(
                formatter,
                "chunk row column {column} is outside {columns} columns"
            ),
            Self::InvalidCell {
                column,
                field_type,
                message,
            } => write!(
                formatter,
                "chunk row column {column} ({field_type:?}) has an invalid payload: {message}"
            ),
        }
    }
}

impl std::error::Error for RowDatumError {}

/// Go `chunk.Row`: a cursor to one row of a [`Chunk`].
#[derive(Clone, Copy, Debug)]
pub struct Row<'a> {
    chunk: Option<&'a Chunk>,
    idx: usize,
}

/// An independently owned copy of one Go `chunk.Row`.
///
/// Go's `Row.CopyConstruct` returns a `Row` pointing at a newly allocated
/// one-row `Chunk`; garbage collection keeps that chunk alive. Rust makes the
/// same ownership explicit so the copied row can safely outlive its source.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OwnedRow {
    chunk: Chunk,
}

impl OwnedRow {
    /// Borrows the copied row.
    #[must_use]
    pub fn as_row(&self) -> Row<'_> {
        self.chunk.get_row(0)
    }

    /// Borrows the one-row chunk that owns the copied row.
    #[must_use]
    pub const fn chunk(&self) -> &Chunk {
        &self.chunk
    }

    /// Transfers ownership of the copied row's one-row chunk.
    #[must_use]
    pub fn into_chunk(self) -> Chunk {
        self.chunk
    }
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

    /// Go `Chunk`: the chunk this row belongs to, or `None` for the zero
    /// `Row{}` sentinel (Go returns a nil `*Chunk` in that case).
    #[must_use]
    pub const fn chunk(&self) -> Option<&'a Chunk> {
        self.chunk
    }

    fn expect_chunk(&self) -> &'a Chunk {
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
        self.expect_chunk().column(col_idx).get_int64(self.idx)
    }

    /// Go `GetUint64`.
    #[must_use]
    pub fn get_uint64(&self, col_idx: usize) -> u64 {
        self.expect_chunk().column(col_idx).get_uint64(self.idx)
    }

    /// Go `GetFloat32`.
    #[must_use]
    pub fn get_float32(&self, col_idx: usize) -> f32 {
        self.expect_chunk().column(col_idx).get_float32(self.idx)
    }

    /// Go `GetFloat64`.
    #[must_use]
    pub fn get_float64(&self, col_idx: usize) -> f64 {
        self.expect_chunk().column(col_idx).get_float64(self.idx)
    }

    /// Go `GetTime`.
    #[must_use]
    pub fn get_time(&self, col_idx: usize) -> Time {
        self.expect_chunk().column(col_idx).get_time(self.idx)
    }

    /// Go `GetDuration`: reads the cell's nanoseconds and stamps `fill_fsp` on
    /// (the column stores no fsp).
    #[must_use]
    pub fn get_duration(&self, col_idx: usize, fill_fsp: i64) -> MySqlDuration {
        self.expect_chunk()
            .column(col_idx)
            .get_duration(self.idx, fill_fsp)
    }

    /// Go `GetMyDecimal`.
    #[must_use]
    pub fn get_my_decimal(&self, col_idx: usize) -> MyDecimal {
        self.expect_chunk().column(col_idx).get_my_decimal(self.idx)
    }

    /// Go `GetEnum`.
    #[must_use]
    pub fn get_enum(&self, col_idx: usize) -> tidb_datatype::MysqlEnum {
        self.expect_chunk().column(col_idx).get_enum(self.idx)
    }

    /// Go `GetSet`.
    #[must_use]
    pub fn get_set(&self, col_idx: usize) -> tidb_datatype::MysqlSet {
        self.expect_chunk().column(col_idx).get_set(self.idx)
    }

    /// Go `GetBytes`: the raw bytes of a variable-length column's cell.
    #[must_use]
    pub fn get_bytes(&self, col_idx: usize) -> CellBytes<'a> {
        self.expect_chunk().column_slots()[col_idx].get_bytes(self.idx)
    }

    /// Go `GetString`: a byte-preserving string view of a variable-length
    /// column's cell.
    #[must_use]
    pub fn get_string(&self, col_idx: usize) -> GoString {
        self.expect_chunk().column(col_idx).get_string(self.idx)
    }

    /// Go `GetRawLen`: the encoded cell width for either column kind.
    #[must_use]
    pub fn get_raw_len(&self, col_idx: usize) -> usize {
        self.expect_chunk().column(col_idx).raw_len(self.idx)
    }

    /// Go `GetRaw`: the raw element bytes for either column kind.
    #[must_use]
    pub fn get_raw(&self, col_idx: usize) -> CellBytes<'a> {
        self.expect_chunk().column_slots()[col_idx].get_raw(self.idx)
    }

    /// Go `GetJSON`.
    #[must_use]
    pub fn get_json(&self, col_idx: usize) -> tidb_datatype::BinaryJSON {
        self.expect_chunk().column(col_idx).get_json(self.idx)
    }

    /// Go `GetVectorFloat32`.
    #[must_use]
    pub fn get_vector_float32(&self, col_idx: usize) -> VectorFloat32 {
        self.expect_chunk()
            .column(col_idx)
            .get_vector_float32(self.idx)
    }

    /// Go `getNameValue`: the `(name, value)` pair an ENUM/SET cell stores.
    #[must_use]
    pub fn get_name_value(&self, col_idx: usize) -> (GoString, u64) {
        self.expect_chunk().column(col_idx).get_name_value(self.idx)
    }

    /// Go `IsNull`.
    #[must_use]
    pub fn is_null(&self, col_idx: usize) -> bool {
        self.expect_chunk().column(col_idx).is_null(self.idx)
    }

    /// Go `GetDatumRow`: materializes every column with the corresponding
    /// field type.
    #[must_use]
    pub fn get_datum_row(&self, field_types: &[FieldType]) -> Vec<Datum> {
        let mut datums = vec![Datum::Null; self.len()];
        self.get_datum_row_with_buffer(field_types, &mut datums);
        datums
    }

    /// Fallible typed row materialization for untrusted wire or disk input.
    pub fn try_get_datum_row(
        &self,
        field_types: &[FieldType],
    ) -> Result<Vec<Datum>, RowDatumError> {
        if field_types.len() < self.len() {
            return Err(RowDatumError::MissingFieldType {
                column: field_types.len(),
                available: field_types.len(),
            });
        }
        (0..self.len())
            .map(|column| self.try_get_datum(column, &field_types[column]))
            .collect()
    }

    /// Go `GetDatumRowWithBuffer`: overwrites the caller's reusable datum
    /// buffer in place and returns that same buffer.
    pub fn get_datum_row_with_buffer<'b>(
        &self,
        field_types: &[FieldType],
        datums: &'b mut [Datum],
    ) -> &'b mut [Datum] {
        for (column, datum) in datums.iter_mut().enumerate() {
            self.datum_with_buffer(column, &field_types[column], datum);
        }
        datums
    }

    /// Go `GetDatum` (`DatumWithBuffer`): read the cell at `col_idx` as a
    /// [`Datum`], interpreted by `field_type`.
    ///
    /// Ported for the column kinds whose storage exists: NULL, the signed/
    /// unsigned integer family and `Year`, `Float`/`Double`, and the string/blob
    /// family (as a collation-tagged string), `Date`/`Datetime`/`Timestamp`
    /// (as a Time datum), `Duration` (fsp filled from the field type's
    /// decimal, matching Go), and `Bit` (the cell's own bytes, Go
    /// `SetMysqlBit`). A source type with no switch arm leaves a caller-supplied
    /// datum unchanged; [`Row::get_datum`] therefore returns its initialized
    /// NULL value for that same type.
    #[must_use]
    pub fn get_datum(&self, col_idx: usize, field_type: &FieldType) -> Datum {
        let mut datum = Datum::Null;
        self.datum_with_buffer(col_idx, field_type, &mut datum);
        datum
    }

    /// Fallible [`Row::get_datum`] for an untrusted typed cell.
    pub fn try_get_datum(
        &self,
        col_idx: usize,
        field_type: &FieldType,
    ) -> Result<Datum, RowDatumError> {
        let mut datum = Datum::Null;
        self.try_datum_with_buffer(col_idx, field_type, &mut datum)?;
        Ok(datum)
    }

    /// Go `DatumWithBuffer`: materializes one cell into caller-owned datum
    /// storage. Existing contents are overwritten for every supported kind.
    pub fn datum_with_buffer(&self, col_idx: usize, field_type: &FieldType, datum: &mut Datum) {
        if self.is_null(col_idx) {
            *datum = Datum::Null;
            return;
        }
        let materialized = match field_type.code() {
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
                let mut value = Datum::Null;
                value.set_string(self.get_bytes(col_idx).to_vec(), field_type.collation());
                value
            }
            FieldTypeCode::Bit => Datum::Bit(tidb_datatype::BinaryLiteral::from(
                self.get_bytes(col_idx).to_vec(),
            )),
            FieldTypeCode::Json => {
                let cell = self.get_bytes(col_idx);
                let (type_code, value) = cell
                    .split_first()
                    .expect("a JSON cell always carries its type code");
                Datum::Json(tidb_datatype::BinaryJSON::from_encoded_parts(
                    *type_code, value,
                ))
            }
            FieldTypeCode::Enum => Datum::new_enum(self.get_enum(col_idx), field_type.collation()),
            FieldTypeCode::Set => Datum::new_set(self.get_set(col_idx), field_type.collation()),
            FieldTypeCode::Date | FieldTypeCode::Datetime | FieldTypeCode::Timestamp => {
                Datum::Time(self.get_time(col_idx))
            }
            FieldTypeCode::Duration => {
                Datum::Duration(self.get_duration(col_idx, field_type.decimal()))
            }
            FieldTypeCode::NewDecimal => {
                let stored = self.get_my_decimal(col_idx);
                let fraction = if field_type.decimal() == UNSPECIFIED_LENGTH {
                    i64::from(stored.digits_frac())
                } else {
                    field_type.decimal()
                };
                Datum::Decimal(
                    Decimal::from_my_decimal(&stored)
                        .with_declared_shape(field_type.flen(), fraction),
                )
            }
            FieldTypeCode::VectorFloat32 => Datum::VectorFloat32(self.get_vector_float32(col_idx)),
            _ => return,
        };
        *datum = materialized;
    }

    /// Checked `DatumWithBuffer` boundary used by response and spill readers.
    pub fn try_datum_with_buffer(
        &self,
        col_idx: usize,
        field_type: &FieldType,
        datum: &mut Datum,
    ) -> Result<(), RowDatumError> {
        if col_idx >= self.len() {
            return Err(RowDatumError::ColumnOutOfRange {
                column: col_idx,
                columns: self.len(),
            });
        }
        if self.is_null(col_idx) {
            *datum = Datum::Null;
            return Ok(());
        }
        match field_type.code() {
            FieldTypeCode::Tiny
            | FieldTypeCode::Short
            | FieldTypeCode::Int24
            | FieldTypeCode::Long
            | FieldTypeCode::LongLong
            | FieldTypeCode::Year
            | FieldTypeCode::Double => {
                let _: [u8; 8] = self.try_fixed(col_idx, field_type)?;
            }
            FieldTypeCode::Float => {
                let _: [u8; 4] = self.try_fixed(col_idx, field_type)?;
            }
            FieldTypeCode::Varchar
            | FieldTypeCode::VarString
            | FieldTypeCode::String
            | FieldTypeCode::Blob
            | FieldTypeCode::TinyBlob
            | FieldTypeCode::MediumBlob
            | FieldTypeCode::LongBlob
            | FieldTypeCode::Bit => {}
            FieldTypeCode::Json => {
                let cell = self.get_bytes(col_idx);
                let (type_code, value) = cell
                    .split_first()
                    .ok_or_else(|| self.invalid_cell(col_idx, field_type, "empty JSON cell"))?;
                tidb_datatype::BinaryJSON::from_raw(*type_code, value.to_vec())
                    .map_err(|error| self.invalid_cell(col_idx, field_type, error))?;
            }
            FieldTypeCode::Enum | FieldTypeCode::Set => {
                let cell = self.get_bytes(col_idx);
                if !cell.is_empty() {
                    cell.split_at_checked(8).ok_or_else(|| {
                        self.invalid_cell(
                            col_idx,
                            field_type,
                            "name/value cell is shorter than its 8-byte value prefix",
                        )
                    })?;
                }
            }
            FieldTypeCode::Date | FieldTypeCode::Datetime | FieldTypeCode::Timestamp => {
                let raw = u64::from_ne_bytes(self.try_fixed(col_idx, field_type)?);
                Time::from_go_raw(raw)
                    .map_err(|error| self.invalid_cell(col_idx, field_type, error))?;
            }
            FieldTypeCode::Duration => {
                let nanoseconds = i64::from_ne_bytes(self.try_fixed(col_idx, field_type)?);
                MySqlDuration::from_nanoseconds(nanoseconds, field_type.decimal())
                    .map_err(|error| self.invalid_cell(col_idx, field_type, error))?;
            }
            FieldTypeCode::NewDecimal => {
                let raw: [u8; MYDECIMAL_STRUCT_SIZE] = self.try_fixed(col_idx, field_type)?;
                MyDecimal::from_raw_bytes(raw)
                    .map_err(|error| self.invalid_cell(col_idx, field_type, error))?;
            }
            FieldTypeCode::VectorFloat32 => {
                let cell = self.get_bytes(col_idx);
                deserialize_vector_float32(cell.as_ref())
                    .map_err(|error| self.invalid_cell(col_idx, field_type, error))?;
            }
            _ => {}
        }
        self.datum_with_buffer(col_idx, field_type, datum);
        Ok(())
    }

    fn try_fixed<const N: usize>(
        &self,
        col_idx: usize,
        field_type: &FieldType,
    ) -> Result<[u8; N], RowDatumError> {
        let cell = self.get_raw(col_idx);
        cell.as_ref().try_into().map_err(|_| {
            self.invalid_cell(
                col_idx,
                field_type,
                format!("expected {N} bytes, got {}", cell.len()),
            )
        })
    }

    fn invalid_cell(
        &self,
        col_idx: usize,
        field_type: &FieldType,
        message: impl std::fmt::Display,
    ) -> RowDatumError {
        RowDatumError::InvalidCell {
            column: col_idx,
            field_type: field_type.code(),
            message: message.to_string(),
        }
    }

    /// Go `Row.CopyConstruct`: deep-copy this physical row into an independently
    /// owned one-row chunk.
    #[must_use]
    pub fn copy_construct(&self) -> OwnedRow {
        let mut chunk = self.expect_chunk().renew_with_capacity(1, 1);
        chunk.append_row(*self);
        OwnedRow { chunk }
    }

    /// Go `Row.ToString`: render every field into one byte-authoritative row.
    ///
    /// The result is [`GoString`] because source string, ENUM, and SET cells
    /// may contain arbitrary bytes. Numeric, temporal, JSON, and vector text is
    /// ASCII and follows the corresponding source value formatter.
    #[must_use]
    pub fn to_string(&self, field_types: &[FieldType]) -> GoString {
        assert!(
            field_types.len() >= self.len(),
            "Row::to_string needs one field type per column"
        );
        let mut output = Vec::new();
        for (col_idx, field_type) in field_types.iter().take(self.len()).enumerate() {
            if self.is_null(col_idx) {
                output.extend_from_slice(b"NULL");
            } else {
                match field_type.eval_type() {
                    EvalType::Int => {
                        output.extend_from_slice(self.get_int64(col_idx).to_string().as_bytes());
                    }
                    EvalType::String => match field_type.code() {
                        FieldTypeCode::Enum => {
                            let value = self.get_enum(col_idx);
                            output.extend_from_slice(value.name_bytes());
                        }
                        FieldTypeCode::Set => {
                            let value = self.get_set(col_idx);
                            output.extend_from_slice(value.name_bytes());
                        }
                        _ => output.extend_from_slice(self.get_string(col_idx).as_bytes()),
                    },
                    EvalType::Datetime | EvalType::Timestamp => {
                        output.extend_from_slice(self.get_time(col_idx).to_string().as_bytes());
                    }
                    EvalType::Decimal => {
                        output.extend_from_slice(&self.get_my_decimal(col_idx).to_string_bytes());
                    }
                    EvalType::Duration => {
                        output.extend_from_slice(
                            self.get_duration(col_idx, field_type.decimal())
                                .to_string()
                                .as_bytes(),
                        );
                    }
                    EvalType::Json => {
                        output.extend_from_slice(self.get_json(col_idx).to_string().as_bytes());
                    }
                    EvalType::Real => {
                        let value = match field_type.code() {
                            FieldTypeCode::Float => {
                                Datum::Float32(f64::from(self.get_float32(col_idx)))
                            }
                            FieldTypeCode::Double => Datum::Real(self.get_float64(col_idx)),
                            _ => unreachable!("only FLOAT and DOUBLE have real eval type"),
                        };
                        output.extend_from_slice(
                            &value
                                .sql_bytes()
                                .expect("a numeric datum always has a text form"),
                        );
                    }
                    EvalType::VectorFloat32 => {
                        output.extend_from_slice(
                            self.get_vector_float32(col_idx).to_string().as_bytes(),
                        );
                    }
                }
            }
            if col_idx + 1 != self.len() {
                output.extend_from_slice(b", ");
            }
        }
        output.into()
    }
}

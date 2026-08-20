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

//! `pkg/util/chunk/column.go`: the columnar `Column`.
//!
//! A `Column` stores one output column of a chunk. Fixed-length elements live
//! back-to-back in `data` (element `i` at `data[i*elem_len ..]`); a per-row
//! `null_bitmap` (bit 0 = null, 1 = not-null) records nullity; variable-length
//! columns additionally use `offsets`.
//!
//! Ported here: the fixed-length core -- the null bitmap and the
//! `int64`/`uint64`/`float32`/`float64` append/get path, plus `is_null`,
//! `is_fixed`, `rows`, `reset`, and `copy_construct`. Go writes fixed elements
//! through an `unsafe.Pointer` cast (native-endian); this port uses
//! `to_ne_bytes`/`from_ne_bytes`, which is the same in-memory layout on a given
//! target.
//!
//! Also ported: variable-length append/get (string/bytes via `offsets`) and the
//! `getFixedLen`/`NewColumn(FieldType)` type dispatch.
//!
//! Also ported: the typed append/get for Time (stored as the packed 8-byte
//! `types.Time` bit pattern, `Time::go_raw`) and Duration (stored as the
//! `int64` nanosecond count; fsp is supplied by the reader, matching Go
//! `AppendDuration`/`GetDuration`).
//!
//! `MyDecimal` cells use Go's raw
//! 40-byte in-memory `types.MyDecimal` struct
//! (`digitsInt`/`digitsFrac`/`resultFrac`/`negative` + 9 base-1e9 `int32`
//! words). The Rust `tidb_datatype::Decimal` is a decimal-digit-string
//! representation and cannot round-trip that 40-byte layout byte-for-byte;
//! the layout-faithful `MyDecimal` provided by `tidb-datatype`.
//!
//! Also ported: the Enum/Set name-value cells (Go `appendNameValue`/
//! `getNameValue`: the 8-byte native-endian value followed by the element
//! name, in one variable-length row).
//!
//! The resize/reserve/null-mutation families and JSON/VectorFloat32 typed
//! storage are implemented below. Go strings remain arbitrary bytes, so the
//! direct `GetString` surface returns [`GoString`] instead of Rust `str`.

use tidb_datatype::{
    deserialize_vector_float32, EvalType, FieldType, FieldTypeCode, GoString, GoStringSource,
    MyDecimal, MySqlDuration, MysqlEnum, MysqlSet, Time, VectorFloat32, MYDECIMAL_STRUCT_SIZE,
};

use crate::column_view::{ColumnBytes, ColumnBytesStorage};
use crate::shared_bytes::SharedBytes;

/// Go `VarElemLen` (`= -1`): the sentinel element length of a variable-length
/// column.
pub const VAR_ELEM_LEN: i64 = -1;

/// Go `sizeTime` = `sizeof(types.Time)`. A `types.Time` is a single `CoreTime`
/// (`uint64`), so it is 8 bytes. (For chunk-codec cross-language fidelity this
/// must equal Go's `sizeTime`; it does.)
pub const SIZE_TIME: i64 = 8;

/// Go `types.MyDecimalStructSize` (`= 40`): the fixed element width of a
/// `NewDecimal` column.
pub const MY_DECIMAL_STRUCT_SIZE: i64 = 40;

/// Go `estimatedElemLen`: initial bytes reserved per variable-length row.
pub const ESTIMATED_ELEM_LEN: usize = 8;

/// Go's accepted 64-bit `Column` payload size, excluding backing allocations.
const GO_COLUMN_PAYLOAD_BYTES: i64 = 112;

/// Go `getFixedLen`: the fixed element width for a column of `field_type`, or
/// [`VAR_ELEM_LEN`] when the type is variable-length.
#[must_use]
pub fn get_fixed_len(field_type: &FieldType) -> i64 {
    match field_type.code() {
        FieldTypeCode::Float => 4,
        FieldTypeCode::Tiny
        | FieldTypeCode::Short
        | FieldTypeCode::Int24
        | FieldTypeCode::Long
        | FieldTypeCode::LongLong
        | FieldTypeCode::Double
        | FieldTypeCode::Year
        | FieldTypeCode::Duration => 8,
        FieldTypeCode::Date | FieldTypeCode::Datetime | FieldTypeCode::Timestamp => SIZE_TIME,
        FieldTypeCode::NewDecimal => MY_DECIMAL_STRUCT_SIZE,
        _ => VAR_ELEM_LEN,
    }
}

/// Go `chunk.Column`: a single columnar column of values.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Column {
    pub(crate) length: usize,
    /// Bit `i` records row `i`: 0 = null, 1 = not-null (Go `nullBitmap`).
    pub(crate) null_bitmap: Vec<u8>,
    /// Row `i` of a variable-length column starts at `data[offsets[i]]`
    /// (Go `offsets`; empty for a fixed-length column).
    pub(crate) offsets: Vec<i64>,
    /// The packed element bytes (Go `data`).
    pub(crate) data: SharedBytes,
    /// Scratch buffer sized to one fixed element. `None` identifies a
    /// variable-length column, including the zero-width fixed edge case.
    pub(crate) elem_buf: Option<Vec<u8>>,
    /// Go `avoidReusing`: keep this column out of the allocator's reuse pool.
    pub avoid_reusing: bool,
}

impl Column {
    /// Go `Chunk.MemoryUsage`'s per-column term:
    /// `unsafe.Sizeof(*col) + cap(nullBitmap) + cap(offsets)*8 + cap(data) +
    /// cap(elemBuf)`.
    ///
    /// The public TiDB contract uses Go's accepted 64-bit payload constant;
    /// Rust layout and synchronization bookkeeping are implementation details.
    #[must_use]
    pub fn memory_usage(&self) -> i64 {
        let of = |n: usize| i64::try_from(n).unwrap_or(i64::MAX);
        GO_COLUMN_PAYLOAD_BYTES
            + of(self.null_bitmap.capacity())
            + of(self.offsets.capacity() * 8)
            + of(self.data.capacity())
            + of(self.elem_buf.as_ref().map_or(0, Vec::capacity))
    }

    /// Go `newFixedLenColumn`: a fixed-length column whose elements are
    /// `elem_len` bytes, with initial data capacity for `capacity` rows.
    #[must_use]
    pub fn new_fixed_len(elem_len: usize, capacity: usize) -> Self {
        Column {
            elem_buf: Some(vec![0; elem_len]),
            data: SharedBytes::with_capacity(elem_len * capacity),
            null_bitmap: Vec::with_capacity((capacity + 7) >> 3),
            offsets: Vec::new(),
            length: 0,
            avoid_reusing: false,
        }
    }

    /// Go `NewColumn`: a column sized for `field_type`, with initial capacity
    /// for `capacity` rows.
    #[must_use]
    pub fn new_column(field_type: &FieldType, capacity: usize) -> Self {
        match get_fixed_len(field_type) {
            VAR_ELEM_LEN => Column::new_var_len(capacity),
            elem_len => Column::new_fixed_len(elem_len as usize, capacity),
        }
    }

    /// Go `newColumn(ts, capacity)`: a column whose element width is `type_size`
    /// ([`VAR_ELEM_LEN`] for a variable-length one), sized for `capacity` rows.
    ///
    /// This is the constructor `renewColumns` uses, where the shape comes from
    /// an existing column's `typeSize` rather than from a field type.
    #[must_use]
    pub fn new_column_with_type_size(type_size: i64, capacity: usize) -> Self {
        if type_size == VAR_ELEM_LEN {
            Column::new_var_len(capacity)
        } else {
            Column::new_fixed_len(type_size as usize, capacity)
        }
    }

    /// Go `NewEmptyColumn`: a column typed for `field_type` but with no
    /// preallocated data/bitmap capacity.
    #[must_use]
    pub fn new_empty_column(field_type: &FieldType) -> Self {
        match get_fixed_len(field_type) {
            VAR_ELEM_LEN => Column {
                offsets: vec![0],
                ..Column::default()
            },
            elem_len => Column {
                elem_buf: Some(vec![0; elem_len as usize]),
                ..Column::default()
            },
        }
    }

    /// Go `newVarLenColumn`: a variable-length column. The leading `0` offset is
    /// always present so slicing `data` is uniform.
    #[must_use]
    pub fn new_var_len(capacity: usize) -> Self {
        let mut offsets = Vec::with_capacity(capacity + 1);
        offsets.push(0);
        let data_capacity = ESTIMATED_ELEM_LEN
            .checked_mul(capacity)
            .expect("variable-length column capacity overflow");
        Column {
            elem_buf: None,
            data: SharedBytes::with_capacity(data_capacity),
            null_bitmap: Vec::with_capacity((capacity + 7) >> 3),
            offsets,
            length: 0,
            avoid_reusing: false,
        }
    }

    /// Go `IsFixed`: whether elements have a fixed length (i.e. `elemBuf != nil`).
    #[must_use]
    pub fn is_fixed(&self) -> bool {
        self.elem_buf.is_some()
    }

    /// Go `typeSize`: the fixed element size, or [`VAR_ELEM_LEN`] for var-length.
    #[must_use]
    pub fn type_size(&self) -> i64 {
        self.elem_buf
            .as_ref()
            .filter(|buffer| !buffer.is_empty())
            .map_or(VAR_ELEM_LEN, |buffer| buffer.len() as i64)
    }

    /// Go `GetNullBitmapCap`.
    #[must_use]
    pub fn null_bitmap_capacity(&self) -> usize {
        self.null_bitmap.capacity()
    }

    /// Go `GetOffsetCap`.
    #[must_use]
    pub fn offset_capacity(&self) -> usize {
        self.offsets.capacity()
    }

    /// Go `GetDataCap`.
    #[must_use]
    pub fn data_capacity(&self) -> usize {
        self.data.capacity()
    }

    /// Go allocator's `cap(col.elemBuf)` type-eligibility check.
    #[must_use]
    pub(crate) fn elem_buffer_capacity(&self) -> usize {
        self.elem_buf.as_ref().map_or(0, Vec::capacity)
    }

    pub(crate) fn elem_buffer_len(&self) -> usize {
        self.elem_buf.as_ref().map_or(0, Vec::len)
    }

    /// Go `Rows`: the number of rows currently stored.
    #[must_use]
    pub fn rows(&self) -> usize {
        self.length
    }

    /// Go `IsNull`: whether row `row_idx` is null.
    #[must_use]
    pub fn is_null(&self, row_idx: usize) -> bool {
        let null_byte = self.null_bitmap[row_idx / 8];
        null_byte & (1 << (row_idx & 7)) == 0
    }

    /// Go `appendNullBitmap`: extend the bitmap for the next row, marking it
    /// not-null when `not_null` is set (a null leaves the bit at 0).
    pub fn append_null_bitmap(&mut self, not_null: bool) {
        let idx = self.length >> 3;
        if idx >= self.null_bitmap.len() {
            self.null_bitmap.push(0);
        }
        if not_null {
            let pos = self.length & 7;
            self.null_bitmap[idx] |= 1 << pos;
        }
    }

    /// Go `Reserve`: preserve current content while reserving additional
    /// bitmap/data/offset capacity.
    pub fn reserve(&mut self, more_null_bitmap: usize, more_data: usize, more_offsets: usize) {
        self.null_bitmap.reserve(more_null_bitmap);
        self.data.reserve(more_data);
        self.offsets.reserve(more_offsets);
    }

    /// Go `CalculateLenDeltaForAppendCellNTimesForNullBitMap`.
    #[must_use]
    pub fn null_bitmap_len_delta_for_append_cell_n_times(&self, times: usize) -> usize {
        ((self.length + times + 7) >> 3).saturating_sub(self.null_bitmap.len())
    }

    /// Go `CalculateLenDeltaForAppendCellNTimesForFixedElem`.
    #[must_use]
    pub fn fixed_len_delta_for_append_cell_n_times(src: &Column, times: usize) -> usize {
        src.elem_buffer_len().saturating_mul(times)
    }

    /// Go `CalculateLenDeltaForAppendCellNTimesForVarElem`.
    #[must_use]
    pub fn var_len_delta_for_append_cell_n_times(src: &Column, row: usize, times: usize) -> usize {
        let cell_len = usize::try_from(src.offsets[row + 1] - src.offsets[row])
            .expect("column offsets are non-decreasing");
        cell_len.saturating_mul(times)
    }

    /// Go `AppendCellNTimes`: append one source cell `times` times, preserving
    /// both bytes and nullity.
    pub fn append_cell_n_times(&mut self, src: &Column, row: usize, times: usize) {
        if times == 0 {
            return;
        }
        let not_null = !src.is_null(row);
        if times == 1 {
            self.append_null_bitmap(not_null);
        } else {
            self.append_multi_same_null_bitmap(not_null, times);
        }
        if self.is_fixed() {
            let elem_len = src.elem_buffer_len();
            let start = row * elem_len;
            let cell = src.data.read()[start..start + elem_len].to_vec();
            for _ in 0..times {
                self.data.extend_from_slice(&cell);
            }
        } else {
            let start = usize::try_from(src.offsets[row]).expect("non-negative offset");
            let end = usize::try_from(src.offsets[row + 1]).expect("non-negative offset");
            let cell = src.data.read()[start..end].to_vec();
            for _ in 0..times {
                self.data.extend_from_slice(&cell);
                self.offsets.push(self.data.len() as i64);
            }
        }
        self.length += times;
    }

    /// Go `reset` (lowercase): drop all rows but keep the element type. A
    /// var-length column keeps its leading `0` offset.
    pub fn reset(&mut self) {
        self.length = 0;
        self.null_bitmap.clear();
        if !self.offsets.is_empty() {
            self.offsets.truncate(1);
        } else if !self.is_fixed() {
            self.offsets.push(0);
        }
        self.data.reset();
    }

    /// Go exported `Reset(EvalType)`: clear the column and reset its physical
    /// element shape for the requested evaluation type.
    pub fn reset_for_eval_type(&mut self, eval_type: EvalType) {
        match eval_type {
            EvalType::Int => self.resize_int64(0, false),
            EvalType::Real => self.resize_float64(0, false),
            EvalType::Decimal => self.resize_decimal(0, false),
            EvalType::String => self.reserve_string(0),
            EvalType::Datetime | EvalType::Timestamp => self.resize_time(0, false),
            EvalType::Duration => self.resize_go_duration(0, false),
            EvalType::Json => self.reserve_json(0),
            EvalType::VectorFloat32 => self.reserve_vector_float32(0),
        }
    }

    /// Go `finishAppendFixed`: commit the scratch `elem_buf` as one not-null row.
    fn finish_append_fixed(&mut self) {
        self.data.extend_from_slice(
            self.elem_buf
                .as_deref()
                .expect("fixed append requires a fixed column"),
        );
        self.append_null_bitmap(true);
        self.length += 1;
    }

    fn write_elem_buf(&mut self, bytes: &[u8]) {
        let elem_buf = self
            .elem_buf
            .as_mut()
            .expect("fixed append requires a fixed column");
        let copied = elem_buf.len().min(bytes.len());
        elem_buf[..copied].copy_from_slice(&bytes[..copied]);
    }

    /// Go `AppendInt64`.
    pub fn append_int64(&mut self, value: i64) {
        self.write_elem_buf(&value.to_ne_bytes());
        self.finish_append_fixed();
    }

    /// Go `AppendUint64`.
    pub fn append_uint64(&mut self, value: u64) {
        self.write_elem_buf(&value.to_ne_bytes());
        self.finish_append_fixed();
    }

    /// Go `AppendFloat32`.
    pub fn append_float32(&mut self, value: f32) {
        self.write_elem_buf(&value.to_ne_bytes());
        self.finish_append_fixed();
    }

    /// Go `AppendFloat64`.
    pub fn append_float64(&mut self, value: f64) {
        self.write_elem_buf(&value.to_ne_bytes());
        self.finish_append_fixed();
    }

    /// Go `AppendTime`: append a `types.Time` value as one fixed 8-byte row.
    ///
    /// Go stores the raw in-memory `types.Time` (a single packed `uint64`
    /// `CoreTime` with the type/fsp metadata in the low 4 bits) via an
    /// `unsafe.Pointer` cast; `Time::go_raw` is that exact bit pattern.
    pub fn append_time(&mut self, t: Time) {
        self.write_elem_buf(&t.go_raw().to_ne_bytes());
        self.finish_append_fixed();
    }

    /// Go `AppendDuration`: append a duration as its `int64` nanosecond count
    /// (Go `int64(dur.Duration)`). Fsp is ignored, exactly as in Go.
    pub fn append_duration(&mut self, dur: MySqlDuration) {
        self.append_int64(dur.nanoseconds());
    }

    /// Go `AppendMyDecimal`: append a decimal as the raw 40-byte
    /// `types.MyDecimal` struct (Go writes it through `unsafe.Pointer`; the
    /// bytes are identical).
    pub fn append_my_decimal(&mut self, dec: &MyDecimal) {
        self.write_elem_buf(&dec.to_raw_bytes());
        self.finish_append_fixed();
    }

    /// Go `GetDecimal`: the decimal in the specific row.
    ///
    /// # Panics
    /// Panics if the stored bytes are not a valid `MyDecimal`; every value
    /// written by [`Column::append_my_decimal`] round-trips.
    #[must_use]
    pub fn get_my_decimal(&self, row_id: usize) -> MyDecimal {
        let bytes: [u8; MYDECIMAL_STRUCT_SIZE] = self.fixed_elem::<MYDECIMAL_STRUCT_SIZE>(row_id);
        MyDecimal::from_raw_bytes(bytes).expect("chunk decimal cell holds a valid MyDecimal")
    }

    /// Go `GetTime`: the `types.Time` in the specific row.
    ///
    /// # Panics
    /// Panics if the stored bits are not a valid packed `types.Time`; every
    /// value written by [`Column::append_time`] round-trips.
    #[must_use]
    pub fn get_time(&self, row_id: usize) -> Time {
        Time::from_go_raw(u64::from_ne_bytes(self.fixed_elem::<8>(row_id)))
            .expect("chunk Time cell holds a valid packed types.Time")
    }

    /// Go `GetDuration`: the duration in the specific row, with `fill_fsp`
    /// stamped on as the fractional-second precision (the column itself does
    /// not store fsp).
    ///
    #[must_use]
    pub fn get_duration(&self, row_id: usize, fill_fsp: i64) -> MySqlDuration {
        MySqlDuration::from_raw_parts(self.get_int64(row_id), fill_fsp)
    }

    /// Go `appendNameValue`: a name/value cell is the 8-byte native-endian
    /// `uint64` value followed by the name bytes, in one variable-length row.
    /// This is the layout `Column::get_name_value` and the chunk-codec
    /// decoder both read.
    fn append_name_value(&mut self, name: &[u8], value: u64) {
        debug_assert!(
            !self.is_fixed(),
            "append_name_value on a fixed-length column"
        );
        self.data.extend_from_slice(&value.to_ne_bytes());
        self.data.extend_from_slice(name);
        self.finish_append_var();
    }

    /// Go `AppendEnum`.
    pub fn append_enum(&mut self, value: &MysqlEnum) {
        self.append_name_value(value.name_bytes(), value.value());
    }

    /// Go `AppendSet`.
    pub fn append_set(&mut self, value: &MysqlSet) {
        self.append_name_value(value.name_bytes(), value.value());
    }

    /// Go `getNameValue`: an empty cell is the zero pair, exactly as in Go;
    /// otherwise the leading 8 bytes are the value and the rest is the name.
    ///
    /// # Panics
    /// Panics on a non-empty cell shorter than the 8-byte value prefix, which
    /// is not producible by [`Column::append_enum`]/[`Column::append_set`].
    #[must_use]
    pub fn get_name_value(&self, row_id: usize) -> (GoString, u64) {
        let cell = self.get_bytes(row_id);
        if cell.is_empty() {
            return (GoString::default(), 0);
        }
        let (value_bytes, name_bytes) = cell
            .split_at_checked(8)
            .expect("a name/value cell carries its 8-byte value prefix");
        let value = u64::from_ne_bytes(value_bytes.try_into().expect("eight bytes"));
        (GoString::from(name_bytes), value)
    }

    /// Go `GetEnum`.
    #[must_use]
    pub fn get_enum(&self, row_id: usize) -> MysqlEnum {
        let (name, value) = self.get_name_value(row_id);
        MysqlEnum::new(name, value)
    }

    /// Go `GetSet`.
    #[must_use]
    pub fn get_set(&self, row_id: usize) -> MysqlSet {
        let (name, value) = self.get_name_value(row_id);
        MysqlSet::new(name, value)
    }

    /// Go `AppendNull`: leave the null bit unset, then keep element positions
    /// consistent -- a fixed column appends the (zeroed) scratch element; a
    /// var-length column repeats the last offset (a zero-width element).
    pub fn append_null(&mut self) {
        self.append_null_bitmap(false);
        if self.is_fixed() {
            self.data.extend_from_slice(
                self.elem_buf
                    .as_deref()
                    .expect("fixed column has an element buffer"),
            );
        } else {
            self.offsets.push(self.offsets[self.length]);
        }
        self.length += 1;
    }

    /// Go `AppendNNulls`.
    pub fn append_n_nulls(&mut self, n: usize) {
        if n == 0 {
            return;
        }
        self.append_multi_same_null_bitmap(false, n);
        if self.is_fixed() {
            for _ in 0..n {
                self.data.extend_from_slice(
                    self.elem_buf
                        .as_deref()
                        .expect("fixed column has an element buffer"),
                );
            }
        } else {
            let current = self.offsets[self.length];
            self.offsets.extend(std::iter::repeat_n(current, n));
        }
        self.length += n;
    }

    /// Go `finishAppendVar`: commit the bytes appended to `data` as one not-null
    /// variable-length row (records the new end offset).
    fn finish_append_var(&mut self) {
        self.append_null_bitmap(true);
        self.offsets.push(self.data.len() as i64);
        self.length += 1;
    }

    /// Go `AppendString`: append a string's bytes as one row.
    pub fn append_string(&mut self, value: impl GoStringSource) {
        debug_assert!(!self.is_fixed(), "append_string on a fixed-length column");
        self.data.extend_from_slice(value.as_go_bytes());
        self.finish_append_var();
    }

    /// Go `AppendBytes`: append raw bytes as one row.
    pub fn append_bytes(&mut self, bytes: &[u8]) {
        debug_assert!(!self.is_fixed(), "append_bytes on a fixed-length column");
        self.data.extend_from_slice(bytes);
        self.finish_append_var();
    }

    /// Go `AppendVectorFloat32`: append the vector's serialized little-endian
    /// image as one variable-length cell.
    pub fn append_vector_float32(&mut self, value: &VectorFloat32) {
        debug_assert!(
            !self.is_fixed(),
            "append_vector_float32 on a fixed-length column"
        );
        let mut encoded = Vec::new();
        value.serialize_to(&mut encoded);
        self.data.extend_from_slice(&encoded);
        self.finish_append_var();
    }

    /// Go `Column.AppendJSON`.
    pub fn append_json(&mut self, value: &tidb_datatype::BinaryJSON) {
        self.append_bytes(&value.encoded());
    }

    /// Go `GetBytes`: the raw bytes of a variable-length row.
    ///
    /// TiDB strings are arbitrary byte sequences. The returned guard behaves
    /// like a byte slice while keeping shared storage stable for its borrow.
    #[must_use]
    pub fn get_bytes(&self, row_id: usize) -> ColumnBytes<'_> {
        let start = self.offsets[row_id] as usize;
        let end = self.offsets[row_id + 1] as usize;
        ColumnBytes {
            storage: ColumnBytesStorage::Borrowed(self.data.read()),
            start,
            end,
        }
    }

    /// Go `Column.GetString`, represented with [`GoString`] so arbitrary Go
    /// string bytes are not rejected as invalid UTF-8.
    #[must_use]
    pub fn get_string(&self, row_id: usize) -> GoString {
        GoString::from_bytes(self.get_bytes(row_id).as_ref().to_vec())
    }

    /// Go `GetJSON`: the cell's first byte is the JSON type code and the rest
    /// is the value, which is exactly what a `BinaryJSON` carries.
    #[must_use]
    pub fn get_json(&self, row_id: usize) -> tidb_datatype::BinaryJSON {
        let cell = self.get_bytes(row_id);
        let (type_code, value) = cell
            .split_first()
            .expect("a JSON cell always carries its type code");
        tidb_datatype::BinaryJSON::from_encoded_parts(*type_code, value)
    }

    /// Go `GetVectorFloat32`: deserialize one vector cell. A malformed cell
    /// panics, matching Go's `panic(err)` path. A valid leading vector is
    /// accepted even when the cell has a suffix, matching the source decoder.
    #[must_use]
    pub fn get_vector_float32(&self, row_id: usize) -> VectorFloat32 {
        let cell = self.get_bytes(row_id);
        let (value, _) = deserialize_vector_float32(cell.as_ref())
            .unwrap_or_else(|error| panic!("invalid VectorFloat32 chunk cell: {error}"));
        value
    }

    /// Go `GetRaw`: the raw element bytes of a row, for either column kind.
    #[must_use]
    pub fn get_raw(&self, row_id: usize) -> ColumnBytes<'_> {
        if self.is_fixed() {
            let elem_len = self.elem_buffer_len();
            let start = row_id * elem_len;
            ColumnBytes {
                storage: ColumnBytesStorage::Borrowed(self.data.read()),
                start,
                end: start + elem_len,
            }
        } else {
            self.get_bytes(row_id)
        }
    }

    /// Go `GetRawLength`.
    #[must_use]
    pub fn raw_len(&self, row_id: usize) -> usize {
        if self.is_fixed() {
            self.elem_buffer_len()
        } else {
            usize::try_from(self.offsets[row_id + 1] - self.offsets[row_id])
                .expect("column offsets are non-decreasing")
        }
    }

    /// Go `SetRaw`: copy as many input bytes as fit in the existing cell.
    /// Short input preserves the old tail; long input is truncated.
    pub fn set_raw(&mut self, row_id: usize, bytes: &[u8]) {
        let start = usize::try_from(self.offsets[row_id]).expect("non-negative offset");
        let end = usize::try_from(self.offsets[row_id + 1]).expect("non-negative offset");
        self.data.copy_from_slice(start..end, bytes);
    }

    /// Go `GetInt64`.
    #[must_use]
    pub fn get_int64(&self, row_id: usize) -> i64 {
        i64::from_ne_bytes(self.fixed_elem::<8>(row_id))
    }

    /// Go `GetUint64`.
    #[must_use]
    pub fn get_uint64(&self, row_id: usize) -> u64 {
        u64::from_ne_bytes(self.fixed_elem::<8>(row_id))
    }

    /// Go `GetFloat32`.
    #[must_use]
    pub fn get_float32(&self, row_id: usize) -> f32 {
        f32::from_ne_bytes(self.fixed_elem::<4>(row_id))
    }

    /// Go `GetFloat64`.
    #[must_use]
    pub fn get_float64(&self, row_id: usize) -> f64 {
        f64::from_ne_bytes(self.fixed_elem::<8>(row_id))
    }

    /// Mutate all `i64` cells through an aligned, borrow-scoped slice.
    /// Changes commit only on normal return; a panic leaves packed bytes unchanged.
    pub fn with_int64s_mut<R>(&mut self, mutate: impl FnOnce(&mut [i64]) -> R) -> R {
        self.with_typed_values_mut(
            8,
            |bytes| i64::from_ne_bytes(bytes.try_into().expect("eight bytes")),
            |value, encoded| encoded.extend_from_slice(&value.to_ne_bytes()),
            mutate,
        )
    }

    /// Mutate all `u64` cells through an aligned, borrow-scoped slice.
    /// Changes commit only on normal return; a panic leaves packed bytes unchanged.
    pub fn with_uint64s_mut<R>(&mut self, mutate: impl FnOnce(&mut [u64]) -> R) -> R {
        self.with_typed_values_mut(
            8,
            |bytes| u64::from_ne_bytes(bytes.try_into().expect("eight bytes")),
            |value, encoded| encoded.extend_from_slice(&value.to_ne_bytes()),
            mutate,
        )
    }

    /// Mutate all `f32` cells through an aligned, borrow-scoped slice.
    /// Changes commit only on normal return; a panic leaves packed bytes unchanged.
    pub fn with_float32s_mut<R>(&mut self, mutate: impl FnOnce(&mut [f32]) -> R) -> R {
        self.with_typed_values_mut(
            4,
            |bytes| f32::from_ne_bytes(bytes.try_into().expect("four bytes")),
            |value, encoded| encoded.extend_from_slice(&value.to_ne_bytes()),
            mutate,
        )
    }

    /// Mutate all `f64` cells through an aligned, borrow-scoped slice.
    /// Changes commit only on normal return; a panic leaves packed bytes unchanged.
    pub fn with_float64s_mut<R>(&mut self, mutate: impl FnOnce(&mut [f64]) -> R) -> R {
        self.with_typed_values_mut(
            8,
            |bytes| f64::from_ne_bytes(bytes.try_into().expect("eight bytes")),
            |value, encoded| encoded.extend_from_slice(&value.to_ne_bytes()),
            mutate,
        )
    }

    /// Mutate all Go duration nanosecond values through an aligned slice.
    /// Changes commit only on normal return; a panic leaves packed bytes unchanged.
    pub fn with_go_durations_mut<R>(&mut self, mutate: impl FnOnce(&mut [i64]) -> R) -> R {
        self.with_int64s_mut(mutate)
    }

    /// Mutate all decimal cells through decoded `MyDecimal` values.
    /// Changes commit only on normal return; a panic leaves packed bytes unchanged.
    pub fn with_decimals_mut<R>(&mut self, mutate: impl FnOnce(&mut [MyDecimal]) -> R) -> R {
        self.with_typed_values_mut(
            MYDECIMAL_STRUCT_SIZE,
            |bytes| {
                let raw: [u8; MYDECIMAL_STRUCT_SIZE] =
                    bytes.try_into().expect("decimal cell width");
                MyDecimal::from_raw_bytes(raw).expect("chunk decimal cell holds a valid MyDecimal")
            },
            |value, encoded| encoded.extend_from_slice(&value.to_raw_bytes()),
            mutate,
        )
    }

    /// Mutate all packed time cells through decoded `Time` values.
    /// Changes commit only on normal return; a panic leaves packed bytes unchanged.
    pub fn with_times_mut<R>(&mut self, mutate: impl FnOnce(&mut [Time]) -> R) -> R {
        self.with_typed_values_mut(
            SIZE_TIME as usize,
            |bytes| {
                let raw = u64::from_ne_bytes(bytes.try_into().expect("time cell width"));
                Time::from_go_raw(raw).expect("chunk Time cell holds a valid packed types.Time")
            },
            |value, encoded| encoded.extend_from_slice(&value.go_raw().to_ne_bytes()),
            mutate,
        )
    }

    /// Mutate one cell's bytes without retaining a lock across user code.
    /// Changes commit only on normal return; a panic leaves packed bytes unchanged.
    pub fn with_cell_bytes_mut<R>(
        &mut self,
        row_id: usize,
        mutate: impl FnOnce(&mut [u8]) -> R,
    ) -> R {
        let (start, end) = if self.is_fixed() {
            let width = self.elem_buffer_len();
            (row_id * width, (row_id + 1) * width)
        } else {
            (
                usize::try_from(self.offsets[row_id]).expect("non-negative offset"),
                usize::try_from(self.offsets[row_id + 1]).expect("non-negative offset"),
            )
        };
        let mut cell = self.data.read()[start..end].to_vec();
        let result = mutate(&mut cell);
        self.data.copy_from_slice(start..end, &cell);
        result
    }

    fn with_typed_values_mut<T, R>(
        &mut self,
        width: usize,
        mut decode: impl FnMut(&[u8]) -> T,
        mut encode: impl FnMut(&T, &mut Vec<u8>),
        mutate: impl FnOnce(&mut [T]) -> R,
    ) -> R {
        let byte_len = self
            .length
            .checked_mul(width)
            .expect("typed column view size overflow");
        let snapshot = self.data.snapshot();
        assert!(
            byte_len <= snapshot.len(),
            "typed column view exceeds packed data"
        );
        let mut values: Vec<T> = snapshot[..byte_len]
            .chunks_exact(width)
            .map(&mut decode)
            .collect();
        let result = mutate(&mut values);
        let mut encoded = Vec::with_capacity(byte_len);
        for value in &values {
            encode(value, &mut encoded);
        }
        assert_eq!(encoded.len(), byte_len, "typed encoder changed cell width");
        self.data.copy_from_slice(0..byte_len, &encoded);
        result
    }

    /// Go `resize`, shared by the exported fixed-width resize helpers.
    fn resize_fixed(&mut self, n: usize, type_size: usize, is_null: bool) {
        let data_len = n.checked_mul(type_size).expect("column size overflow");
        self.data.resize_preserving(data_len);
        if !is_null {
            self.data.fill(0);
        }

        self.null_bitmap.resize((n + 7) >> 3, 0);
        self.null_bitmap.fill(if is_null { 0 } else { 0xff });
        if !is_null && n & 7 != 0 {
            let last = self
                .null_bitmap
                .last_mut()
                .expect("non-zero row count has a bitmap byte");
            *last = ((1u16 << (n & 7)) - 1) as u8;
        }

        // Go re-slices `elemBuf` here.  In particular, resizing an existing
        // fixed-width column does not zero the append scratch buffer: a later
        // AppendNull copies the last scratch value into the null cell.  Rust's
        // `Vec::resize` preserves the bytes when the length is unchanged,
        // which is the ordinary same-evaluation-type path.
        self.elem_buf
            .get_or_insert_with(Vec::new)
            .resize(type_size, 0);
        // Go's fixed-width resize does not touch `offsets`.  A column that was
        // previously variable-width therefore retains that slice header; it
        // is reused if the column later becomes variable-width again.
        self.length = n;
    }

    /// Go `reserve`, shared by variable-width Reserve* helpers.
    fn reserve_var(&mut self, n: usize, estimated_size: usize) {
        let data_capacity = n
            .checked_mul(estimated_size)
            .expect("column reserve size overflow");
        if self.data.capacity() < data_capacity {
            self.data = SharedBytes::with_capacity(data_capacity);
        } else {
            self.data.reset();
        }
        let bitmap_capacity = (n + 7) >> 3;
        if self.null_bitmap.capacity() < bitmap_capacity {
            self.null_bitmap = Vec::with_capacity(bitmap_capacity);
        } else {
            self.null_bitmap.clear();
        }
        let offset_capacity = n.checked_add(1).expect("offset capacity overflow");
        if self.offsets.capacity() < offset_capacity {
            self.offsets = Vec::with_capacity(offset_capacity);
        }
        if self.offsets.is_empty() {
            self.offsets.push(0);
        } else {
            // Go uses `offsets[:1]`, preserving the first header word rather
            // than manufacturing a new zero when the backing is reusable.
            self.offsets.truncate(1);
        }
        self.elem_buf = None;
        self.length = 0;
    }

    /// Go `ResizeInt64`.
    pub fn resize_int64(&mut self, n: usize, is_null: bool) {
        self.resize_fixed(n, 8, is_null);
    }

    /// Go `ResizeUint64`.
    pub fn resize_uint64(&mut self, n: usize, is_null: bool) {
        self.resize_fixed(n, 8, is_null);
    }

    /// Go `ResizeFloat32`.
    pub fn resize_float32(&mut self, n: usize, is_null: bool) {
        self.resize_fixed(n, 4, is_null);
    }

    /// Go `ResizeFloat64`.
    pub fn resize_float64(&mut self, n: usize, is_null: bool) {
        self.resize_fixed(n, 8, is_null);
    }

    /// Go `ResizeDecimal`.
    pub fn resize_decimal(&mut self, n: usize, is_null: bool) {
        self.resize_fixed(n, MYDECIMAL_STRUCT_SIZE, is_null);
    }

    /// Go `ResizeGoDuration`.
    pub fn resize_go_duration(&mut self, n: usize, is_null: bool) {
        self.resize_fixed(n, 8, is_null);
    }

    /// Go `ResizeTime`.
    pub fn resize_time(&mut self, n: usize, is_null: bool) {
        self.resize_fixed(n, SIZE_TIME as usize, is_null);
    }

    /// Go `ReserveString`.
    pub fn reserve_string(&mut self, n: usize) {
        self.reserve_var(n, ESTIMATED_ELEM_LEN);
    }

    /// Go `ReserveStringWithSizeHint`.
    pub fn reserve_string_with_size_hint(&mut self, n: usize, size: usize) {
        self.reserve_var(n, size);
    }

    /// Go `ReserveBytes`.
    pub fn reserve_bytes(&mut self, n: usize) {
        self.reserve_var(n, ESTIMATED_ELEM_LEN);
    }

    /// Go `ReserveJSON`.
    pub fn reserve_json(&mut self, n: usize) {
        self.reserve_var(n, ESTIMATED_ELEM_LEN);
    }

    /// Go `ReserveVectorFloat32`.
    pub fn reserve_vector_float32(&mut self, n: usize) {
        self.reserve_var(n, ESTIMATED_ELEM_LEN);
    }

    /// Go `ReserveSet`.
    pub fn reserve_set(&mut self, n: usize) {
        self.reserve_var(n, ESTIMATED_ELEM_LEN);
    }

    /// Go `ReserveEnum`.
    pub fn reserve_enum(&mut self, n: usize) {
        self.reserve_var(n, ESTIMATED_ELEM_LEN);
    }

    /// Go `SetNull` (`is_null=true` clears the not-null bit).
    pub fn set_null(&mut self, row_id: usize, is_null: bool) {
        let mask = 1u8 << (row_id & 7);
        if is_null {
            self.null_bitmap[row_id >> 3] &= !mask;
        } else {
            self.null_bitmap[row_id >> 3] |= mask;
        }
    }

    /// Go `SetNulls`: set the half-open row range `[begin, end)`.
    pub fn set_nulls(&mut self, begin: usize, end: usize, is_null: bool) {
        assert!(begin <= end && end <= self.length);
        for row in begin..end {
            self.set_null(row, is_null);
        }
    }

    /// Reads the `N` element bytes of a fixed-length row.
    fn fixed_elem<const N: usize>(&self, row_id: usize) -> [u8; N] {
        let start = row_id * N;
        self.data.read()[start..start + N]
            .try_into()
            .expect("fixed element")
    }

    /// Go `CopyConstruct` (the `dst == nil` branch): a deep copy.
    #[must_use]
    pub fn copy_construct(&self) -> Column {
        Column {
            length: self.length,
            null_bitmap: self.null_bitmap.clone(),
            offsets: self.offsets.clone(),
            data: self.data.deep_copy(),
            elem_buf: self.elem_buf.clone(),
            // Go intentionally does not propagate `avoidReusing`: the copy
            // owns all buffers even when the source was a zero-copy codec
            // view, so it is safe to return to the allocator.
            avoid_reusing: false,
        }
    }

    fn copy_construct_into(&self, destination: Option<Column>) -> Column {
        let Some(mut destination) = destination else {
            return self.copy_construct();
        };
        // Go's supplied-destination branch overwrites every data/shape field
        // but deliberately leaves `avoidReusing` untouched.
        destination.length = self.length;
        destination.null_bitmap.clear();
        destination.null_bitmap.extend_from_slice(&self.null_bitmap);
        destination.offsets.clear();
        destination.offsets.extend_from_slice(&self.offsets);
        let data = self.data.snapshot();
        destination.data.reset();
        destination.data.extend_from_slice(&data);
        destination.elem_buf = self.elem_buf.clone();
        destination
    }

    /// Go `CopyReconstruct`: deep-copy only the selected rows, reusing `dst`
    /// storage when supplied. `None` selection is the ordinary deep-copy path.
    #[must_use]
    pub fn copy_reconstruct(&self, sel: Option<&[usize]>, dst: Option<Column>) -> Column {
        let Some(sel) = sel else {
            return self.copy_construct_into(dst);
        };
        if sel.len() == self.length && sel.windows(2).all(|pair| pair[0] <= pair[1]) {
            return self.copy_construct_into(dst);
        }

        let mut destination =
            dst.unwrap_or_else(|| Column::new_column_with_type_size(self.type_size(), sel.len()));
        destination.reset();
        if self.is_fixed() {
            // Go allocates a fresh scratch element in this branch while
            // retaining dst.offsets and dst.avoidReusing.
            destination.elem_buf = Some(vec![0; self.elem_buffer_len()]);
        } else {
            destination.elem_buf = None;
            if destination.offsets.is_empty() {
                destination.offsets.push(0);
            }
        }
        for &row in sel {
            destination.append_cell_from(self, row);
        }
        destination
    }

    /// Go `appendCellByCell`: append `src`'s cell at `row_idx` (value and
    /// nullity) as a new row of this column. `self` and `src` must be the same
    /// element kind.
    pub(crate) fn append_cell_from(&mut self, src: &Column, row_idx: usize) {
        self.append_null_bitmap(!src.is_null(row_idx));
        if src.is_fixed() {
            let elem_len = src.elem_buffer_len();
            let offset = row_idx * elem_len;
            let data = src.data.read();
            self.data
                .extend_from_slice(&data[offset..offset + elem_len]);
        } else {
            let start = src.offsets[row_idx] as usize;
            let end = src.offsets[row_idx + 1] as usize;
            let data = src.data.read();
            self.data.extend_from_slice(&data[start..end]);
            self.offsets.push(self.data.len() as i64);
        }
        self.length += 1;
    }

    /// Append one cell after its source owner has been unlocked. This is the
    /// same physical operation as [`Column::append_cell_from`], with nullity,
    /// shape, and bytes prepared while the source was borrowed.
    pub(crate) fn append_prepared_cell(
        &mut self,
        not_null: bool,
        source_is_fixed: bool,
        cell: &[u8],
    ) {
        self.append_null_bitmap(not_null);
        self.data.extend_from_slice(cell);
        if !source_is_fixed {
            self.offsets.push(self.data.len() as i64);
        }
        self.length += 1;
    }

    /// Go `diskFormatRow.toRow`'s not-null branch: append `cell` as one row's
    /// raw bytes, whichever kind of column this is.
    ///
    /// Go writes a fixed-length cell by pointing `elemBuf` at it and calling
    /// `finishAppendFixed`; the cell is the column's element length, so the
    /// result is the same as appending the bytes directly.
    pub(crate) fn append_raw_cell(&mut self, cell: &[u8]) {
        if self.is_fixed() {
            self.append_null_bitmap(true);
            self.data.extend_from_slice(cell);
            self.length += 1;
        } else {
            self.append_bytes(cell);
        }
    }

    /// Go `nullCount`: the number of null rows currently stored.
    #[must_use]
    pub fn null_count(&self) -> usize {
        let mut cnt = 0;
        let mut i = 0;
        while i + 8 <= self.length {
            // 0 is null and 1 is not null.
            cnt += 8 - self.null_bitmap[i >> 3].count_ones() as usize;
            i += 8;
        }
        while i < self.length {
            if self.is_null(i) {
                cnt += 1;
            }
            i += 1;
        }
        cnt
    }

    /// Go `MergeNulls`: the result row is not-null only when every input row
    /// is not-null.
    pub fn merge_nulls(&mut self, columns: &[&Column]) {
        assert!(self.is_fixed(), "result column should be fixed-length type");
        for column in columns {
            assert_eq!(
                self.length, column.length,
                "all merged columns must have the same length"
            );
        }
        for column in columns {
            for (left, right) in self.null_bitmap.iter_mut().zip(&column.null_bitmap) {
                *left &= *right;
            }
        }
    }

    /// Go `DestroyDataForTest`: overwrite every occupied byte so tests can
    /// prove a supposed copy does not retain the source backing.
    pub fn destroy_data_for_test(&mut self) {
        for index in 0..self.data.len() {
            self.data
                .set(index, (index as u8).wrapping_mul(31).wrapping_add(0xa5));
        }
    }

    /// Go `ContainsVeryLargeElement`.
    #[must_use]
    pub fn contains_very_large_element(&self) -> bool {
        if self.length == 0 || self.is_fixed() {
            return false;
        }
        if self.offsets[self.length] <= i64::from(u32::MAX) {
            return false;
        }
        self.offsets[..=self.length]
            .windows(2)
            .any(|pair| pair[1] - pair[0] > i64::from(u32::MAX))
    }

    /// Go `reconstruct`: compact this column in place so that row `n` becomes
    /// what row `sel[n]` used to be. `sel` must be ascending, which is what
    /// every caller (`Chunk.sel` filtering) produces; the compaction copies
    /// backwards over itself and relies on `dst <= src`.
    pub fn reconstruct(&mut self, sel: &[usize]) {
        if self.is_fixed() {
            let elem_len = self.elem_buffer_len();
            for (dst, &src) in sel.iter().enumerate() {
                let idx = dst >> 3;
                let pos = dst & 7;
                if self.is_null(src) {
                    self.null_bitmap[idx] &= !(1u8 << pos);
                } else {
                    self.data
                        .copy_within(src * elem_len..src * elem_len + elem_len, dst * elem_len);
                    self.null_bitmap[idx] |= 1u8 << pos;
                }
            }
            self.data.truncate(sel.len() * elem_len);
        } else {
            let mut tail = 0usize;
            for (dst, &src) in sel.iter().enumerate() {
                let idx = dst >> 3;
                let pos = dst & 7;
                if self.is_null(src) {
                    self.null_bitmap[idx] &= !(1u8 << pos);
                    self.offsets[dst + 1] = tail as i64;
                } else {
                    let start = self.offsets[src] as usize;
                    let end = self.offsets[src + 1] as usize;
                    self.data.copy_within(start..end, tail);
                    tail += end - start;
                    self.offsets[dst + 1] = tail as i64;
                    self.null_bitmap[idx] |= 1u8 << pos;
                }
            }
            self.data.truncate(tail);
            self.offsets.truncate(sel.len() + 1);
        }
        self.length = sel.len();

        // clean nullBitmap
        self.null_bitmap.truncate((sel.len() + 7) >> 3);
        let idx = sel.len() >> 3;
        if idx < self.null_bitmap.len() {
            let pos = sel.len() & 7;
            self.null_bitmap[idx] &= ((1u16 << pos) - 1) as u8;
        }
    }

    /// Go `appendMultiSameNullBitmap`: extend the bitmap by `num` rows that all
    /// share the same nullity.
    pub(crate) fn append_multi_same_null_bitmap(&mut self, not_null: bool, num: usize) {
        let num_new_bytes = ((self.length + num + 7) >> 3) - self.null_bitmap.len();
        let b = if not_null { 0xffu8 } else { 0u8 };
        for _ in 0..num_new_bytes {
            self.null_bitmap.push(b);
        }
        if !not_null {
            return;
        }
        // 1. Set all the remaining bits in the last slot of the old bitmap to 1.
        let num_remaining_bits = self.length % 8;
        let bit_mask = !(((1u16 << num_remaining_bits) - 1) as u8);
        self.null_bitmap[self.length / 8] |= bit_mask;
        // 2. Set all the redundant bits in the last slot of the new bitmap to 0.
        let num_redundant_bits = self.null_bitmap.len() * 8 - self.length - num;
        let bit_mask = ((1u16 << (8 - num_redundant_bits)) as u8).wrapping_sub(1);
        let last = self.null_bitmap.len() - 1;
        self.null_bitmap[last] &= bit_mask;
    }

    /// Go `CopyExpectedRowsWithRowIDFunc`: append to this column the rows of
    /// `src` in `start..end` whose `selected` flag equals `expected_result`,
    /// reading `src` at `row_id_fn(i)`.
    pub(crate) fn copy_expected_rows_with_row_id_func(
        &mut self,
        src: &Column,
        selected: &[bool],
        expected_result: bool,
        start: usize,
        end: usize,
        row_id_fn: impl Fn(usize) -> usize,
    ) {
        for (i, sel) in selected.iter().enumerate().take(end).skip(start) {
            if *sel != expected_result {
                continue;
            }
            self.append_cell_from(src, row_id_fn(i));
        }
    }

    /// Go `CopyRows`: append to this column the `src` rows named by `selected`.
    pub(crate) fn copy_rows_from(&mut self, src: &Column, selected: &[usize]) {
        for &row_id in selected {
            self.append_cell_from(src, row_id);
        }
    }

    /// Go `copySameOuterRows`' per-column body: append `num_rows` copies of the
    /// `src` block that starts at `row_idx`. For a fixed-length column this is
    /// the contiguous run `row_idx..row_idx+num_rows`; Go relies on all outer
    /// rows in the source being identical, so the run reads as `num_rows`
    /// repeats of the same value.
    pub(crate) fn copy_same_rows_from(&mut self, src: &Column, row_idx: usize, num_rows: usize) {
        self.append_multi_same_null_bitmap(!src.is_null(row_idx), num_rows);
        self.length += num_rows;
        if src.is_fixed() {
            let elem_len = src.elem_buffer_len();
            let start = row_idx * elem_len;
            let end = start + num_rows * elem_len;
            let cells = src.data.read()[start..end].to_vec();
            self.data.extend_from_slice(&cells);
        } else {
            let start = src.offsets[row_idx] as usize;
            let end = src.offsets[row_idx + num_rows] as usize;
            let cells = src.data.read()[start..end].to_vec();
            self.data.extend_from_slice(&cells);
            let elem_len = src.offsets[row_idx + 1] - src.offsets[row_idx];
            for _ in 0..num_rows {
                let last = *self.offsets.last().expect("var-len column keeps offset 0");
                self.offsets.push(last + elem_len);
            }
        }
    }

    /// The row count as the crate's chunk-level copies track it.
    pub(crate) fn length(&self) -> usize {
        self.length
    }
}

/// Go `AppendCellFromRawData`: append one cell from a join row's packed raw
/// stream and return the offset immediately after it.
///
/// Fixed-width cells occupy exactly the destination column's element width.
/// Variable-width cells carry a native-endian uint32 length prefix followed by
/// that many payload bytes. Nullity is intentionally not changed; the join
/// probe appends its null bitmap bit separately before calling this function.
pub fn append_cell_from_raw_data(
    destination: &mut Column,
    row_data: &[u8],
    current_offset: usize,
) -> usize {
    if destination.is_fixed() {
        let width = destination.elem_buffer_len();
        let end = current_offset
            .checked_add(width)
            .expect("fixed raw-cell offset overflow");
        destination
            .data
            .extend_from_slice(&row_data[current_offset..end]);
        destination.length += 1;
        return end;
    }

    let length_end = current_offset
        .checked_add(4)
        .expect("variable raw-cell length offset overflow");
    let length = u32::from_ne_bytes(
        row_data[current_offset..length_end]
            .try_into()
            .expect("four-byte variable cell length"),
    ) as usize;
    let end = length_end
        .checked_add(length)
        .expect("variable raw-cell offset overflow");
    destination
        .data
        .extend_from_slice(&row_data[length_end..end]);
    destination.offsets.push(destination.data.len() as i64);
    destination.length += 1;
    end
}

#[cfg(test)]
#[path = "column_tests.rs"]
mod tests;

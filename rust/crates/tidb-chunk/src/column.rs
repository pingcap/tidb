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
//! DEFERRED (documented, later tranches): the `Resize*` family;
//! `Reset(EvalType)`; the `Reserve`/`resize` capacity helpers;
//! `SetNull(s)`/`nullCount`; a `str`-typed `GetString`; and the `Chunk`/`Row`
//! containers built on `Column`. JSON and VectorFloat32 typed storage is
//! implemented as variable-length source-format cells.

use tidb_datatype::{
    deserialize_vector_float32, EvalType, FieldType, FieldTypeCode, GoString, MyDecimal,
    MySqlDuration, MysqlEnum, MysqlSet, Time, VectorFloat32, MYDECIMAL_STRUCT_SIZE,
};

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
    pub(crate) data: Vec<u8>,
    /// Scratch buffer sized to one fixed element; empty for a var-length column
    /// (Go `elemBuf`).
    pub(crate) elem_buf: Vec<u8>,
    /// Go `avoidReusing`: keep this column out of the allocator's reuse pool.
    pub avoid_reusing: bool,
}

impl Column {
    /// Go `Chunk.MemoryUsage`'s per-column term:
    /// `unsafe.Sizeof(*col) + cap(nullBitmap) + cap(offsets)*8 + cap(data) +
    /// cap(elemBuf)`.
    ///
    /// The struct's own size stands in for Go's `unsafe.Sizeof(*col)`; the
    /// field list is the same one Go sums, in the same order, so the two
    /// numbers agree whenever the two layouts do.
    #[must_use]
    pub fn memory_usage(&self) -> i64 {
        let of = |n: usize| i64::try_from(n).unwrap_or(i64::MAX);
        of(size_of::<Column>())
            + of(self.null_bitmap.capacity())
            + of(self.offsets.capacity() * 8)
            + of(self.data.capacity())
            + of(self.elem_buf.capacity())
    }

    /// Go `newFixedLenColumn`: a fixed-length column whose elements are
    /// `elem_len` bytes, with initial data capacity for `capacity` rows.
    #[must_use]
    pub fn new_fixed_len(elem_len: usize, capacity: usize) -> Self {
        Column {
            elem_buf: vec![0; elem_len],
            data: Vec::with_capacity(elem_len * capacity),
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
                elem_buf: vec![0; elem_len as usize],
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
            elem_buf: Vec::new(),
            data: Vec::with_capacity(data_capacity),
            null_bitmap: Vec::with_capacity((capacity + 7) >> 3),
            offsets,
            length: 0,
            avoid_reusing: false,
        }
    }

    /// Go `IsFixed`: whether elements have a fixed length (i.e. `elemBuf != nil`).
    #[must_use]
    pub fn is_fixed(&self) -> bool {
        !self.elem_buf.is_empty()
    }

    /// Go `typeSize`: the fixed element size, or [`VAR_ELEM_LEN`] for var-length.
    #[must_use]
    pub fn type_size(&self) -> i64 {
        if self.elem_buf.is_empty() {
            VAR_ELEM_LEN
        } else {
            self.elem_buf.len() as i64
        }
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
        self.elem_buf.capacity()
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
        src.elem_buf.len().saturating_mul(times)
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
            let elem_len = src.elem_buf.len();
            let start = row * elem_len;
            for _ in 0..times {
                self.data
                    .extend_from_slice(&src.data[start..start + elem_len]);
            }
        } else {
            let start = usize::try_from(src.offsets[row]).expect("non-negative offset");
            let end = usize::try_from(src.offsets[row + 1]).expect("non-negative offset");
            for _ in 0..times {
                self.data.extend_from_slice(&src.data[start..end]);
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
        self.data.clear();
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
        self.data.extend_from_slice(&self.elem_buf);
        self.append_null_bitmap(true);
        self.length += 1;
    }

    /// Go `AppendInt64`.
    pub fn append_int64(&mut self, value: i64) {
        self.elem_buf.copy_from_slice(&value.to_ne_bytes());
        self.finish_append_fixed();
    }

    /// Go `AppendUint64`.
    pub fn append_uint64(&mut self, value: u64) {
        self.elem_buf.copy_from_slice(&value.to_ne_bytes());
        self.finish_append_fixed();
    }

    /// Go `AppendFloat32`.
    pub fn append_float32(&mut self, value: f32) {
        self.elem_buf.copy_from_slice(&value.to_ne_bytes());
        self.finish_append_fixed();
    }

    /// Go `AppendFloat64`.
    pub fn append_float64(&mut self, value: f64) {
        self.elem_buf.copy_from_slice(&value.to_ne_bytes());
        self.finish_append_fixed();
    }

    /// Go `AppendTime`: append a `types.Time` value as one fixed 8-byte row.
    ///
    /// Go stores the raw in-memory `types.Time` (a single packed `uint64`
    /// `CoreTime` with the type/fsp metadata in the low 4 bits) via an
    /// `unsafe.Pointer` cast; `Time::go_raw` is that exact bit pattern.
    pub fn append_time(&mut self, t: Time) {
        self.elem_buf.copy_from_slice(&t.go_raw().to_ne_bytes());
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
        self.elem_buf.copy_from_slice(&dec.to_raw_bytes());
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
    /// # Panics
    /// Panics if `fill_fsp` is out of range (Go's `types.Duration.Fsp` is an
    /// unchecked `int`; `MySqlDuration` validates). `UNSPECIFIED_FSP` (`-1`)
    /// maps to the default fsp.
    #[must_use]
    pub fn get_duration(&self, row_id: usize, fill_fsp: i64) -> MySqlDuration {
        MySqlDuration::from_nanoseconds(self.get_int64(row_id), fill_fsp)
            .expect("valid fill_fsp for chunk duration cell")
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
            self.data.extend_from_slice(&self.elem_buf);
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
                self.data.extend_from_slice(&self.elem_buf);
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
    pub fn append_string(&mut self, str: &str) {
        debug_assert!(!self.is_fixed(), "append_string on a fixed-length column");
        self.data.extend_from_slice(str.as_bytes());
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
        value.serialize_to(&mut self.data);
        self.finish_append_var();
    }

    /// Go `GetBytes`: the raw bytes of a variable-length row.
    ///
    /// TiDB strings are arbitrary byte sequences, so this returns `&[u8]`; a
    /// `str`-typed `GetString` waits on the crate-wide bytes-vs-str policy.
    #[must_use]
    pub fn get_bytes(&self, row_id: usize) -> &[u8] {
        let start = self.offsets[row_id] as usize;
        let end = self.offsets[row_id + 1] as usize;
        &self.data[start..end]
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
    /// panics, matching Go's `panic(err)` path. The cell boundary must contain
    /// exactly one vector image; accepting a suffix would silently hide a
    /// corrupt chunk offset table.
    #[must_use]
    pub fn get_vector_float32(&self, row_id: usize) -> VectorFloat32 {
        let cell = self.get_bytes(row_id);
        let (value, remaining) = deserialize_vector_float32(cell)
            .unwrap_or_else(|error| panic!("invalid VectorFloat32 chunk cell: {error}"));
        assert!(
            remaining.is_empty(),
            "VectorFloat32 chunk cell has {} trailing bytes",
            remaining.len()
        );
        value
    }

    /// Go `GetRaw`: the raw element bytes of a row, for either column kind.
    #[must_use]
    pub fn get_raw(&self, row_id: usize) -> &[u8] {
        if self.is_fixed() {
            let elem_len = self.elem_buf.len();
            &self.data[row_id * elem_len..row_id * elem_len + elem_len]
        } else {
            self.get_bytes(row_id)
        }
    }

    /// Go `GetRawLength`.
    #[must_use]
    pub fn raw_len(&self, row_id: usize) -> usize {
        if self.is_fixed() {
            self.elem_buf.len()
        } else {
            usize::try_from(self.offsets[row_id + 1] - self.offsets[row_id])
                .expect("column offsets are non-decreasing")
        }
    }

    /// Go `SetRaw`. The caller must provide exactly the existing variable-cell
    /// width; this assertion turns Go's documented precondition into a checked
    /// Rust boundary.
    pub fn set_raw(&mut self, row_id: usize, bytes: &[u8]) {
        assert!(!self.is_fixed(), "SetRaw requires a variable-length column");
        let start = usize::try_from(self.offsets[row_id]).expect("non-negative offset");
        let end = usize::try_from(self.offsets[row_id + 1]).expect("non-negative offset");
        assert_eq!(bytes.len(), end - start, "SetRaw width must not change");
        self.data[start..end].copy_from_slice(bytes);
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

    /// Go `resize`, shared by the exported fixed-width resize helpers.
    fn resize_fixed(&mut self, n: usize, type_size: usize, is_null: bool) {
        let data_len = n.checked_mul(type_size).expect("column size overflow");
        self.data.resize(data_len, 0);
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
        self.elem_buf.resize(type_size, 0);
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
            self.data = Vec::with_capacity(data_capacity);
        } else {
            self.data.clear();
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
        self.elem_buf.clear();
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
        self.data[start..start + N]
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
            data: self.data.clone(),
            elem_buf: self.elem_buf.clone(),
            // Go intentionally does not propagate `avoidReusing`: the copy
            // owns all buffers even when the source was a zero-copy codec
            // view, so it is safe to return to the allocator.
            avoid_reusing: false,
        }
    }

    /// Go `CopyReconstruct`: deep-copy only the selected rows, reusing `dst`
    /// storage when supplied. `None` selection is the ordinary deep-copy path.
    #[must_use]
    pub fn copy_reconstruct(&self, sel: Option<&[usize]>, dst: Option<Column>) -> Column {
        let Some(sel) = sel else {
            return self.copy_construct();
        };
        if sel.len() == self.length && sel.windows(2).all(|pair| pair[0] <= pair[1]) {
            return self.copy_construct();
        }

        let mut destination =
            dst.unwrap_or_else(|| Column::new_column_with_type_size(self.type_size(), sel.len()));
        destination.reset();
        if destination.type_size() != self.type_size() {
            destination = Column::new_column_with_type_size(self.type_size(), sel.len());
        }
        for &row in sel {
            destination.append_cell_from(self, row);
        }
        destination.avoid_reusing = false;
        destination
    }

    /// Go `appendCellByCell`: append `src`'s cell at `row_idx` (value and
    /// nullity) as a new row of this column. `self` and `src` must be the same
    /// element kind.
    pub(crate) fn append_cell_from(&mut self, src: &Column, row_idx: usize) {
        self.append_null_bitmap(!src.is_null(row_idx));
        if src.is_fixed() {
            let elem_len = src.elem_buf.len();
            let offset = row_idx * elem_len;
            self.data
                .extend_from_slice(&src.data[offset..offset + elem_len]);
        } else {
            let start = src.offsets[row_idx] as usize;
            let end = src.offsets[row_idx + 1] as usize;
            self.data.extend_from_slice(&src.data[start..end]);
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
        for (index, byte) in self.data.iter_mut().enumerate() {
            *byte = (index as u8).wrapping_mul(31).wrapping_add(0xa5);
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
            let elem_len = self.elem_buf.len();
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
            let elem_len = src.elem_buf.len();
            let start = row_idx * elem_len;
            let end = start + num_rows * elem_len;
            self.data.extend_from_slice(&src.data[start..end]);
        } else {
            let start = src.offsets[row_idx] as usize;
            let end = src.offsets[row_idx + num_rows] as usize;
            self.data.extend_from_slice(&src.data[start..end]);
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

#[cfg(test)]
mod tests {
    use super::*;

    /// Stands in for Go's `rand` in the reconstruct tests: those tests draw a
    /// fresh random selection and null pattern on every run, so a fixed
    /// generator run over several seeds keeps the same coverage while staying
    /// reproducible when it fails.
    struct Rng(u64);

    impl Rng {
        fn next_u64(&mut self) -> u64 {
            self.0 ^= self.0 << 13;
            self.0 ^= self.0 >> 7;
            self.0 ^= self.0 << 17;
            self.0
        }

        /// Go `rand.Intn(10)`.
        fn intn10(&mut self) -> u64 {
            self.next_u64() % 10
        }

        /// Go `rand.Int63()`.
        fn int63(&mut self) -> i64 {
            (self.next_u64() >> 1) as i64
        }
    }

    /// Go `TestReconstructFixedLen` (`pkg/util/chunk/column_test.go:432`).
    #[test]
    fn reconstruct_fixed_len() {
        for seed in 1..=8u64 {
            let mut rng = Rng(seed);
            let mut col = Column::new_column(
                &FieldType::new(tidb_datatype::FieldTypeCode::LongLong),
                1024,
            );
            let mut results: Vec<i64> = Vec::with_capacity(1024);
            let mut nulls: Vec<bool> = Vec::with_capacity(1024);
            let mut sel: Vec<usize> = Vec::with_capacity(1024);
            for i in 0..1024 {
                if rng.intn10() < 6 {
                    sel.push(i);
                }
                if rng.intn10() < 2 {
                    col.append_null();
                    nulls.push(true);
                    results.push(0);
                    continue;
                }
                let v = rng.int63();
                col.append_int64(v);
                results.push(v);
                nulls.push(false);
            }

            col.reconstruct(&sel);
            let mut null_cnt = 0;
            for (n, &i) in sel.iter().enumerate() {
                if nulls[i] {
                    null_cnt += 1;
                    assert!(col.is_null(n), "seed {seed}: row {n} should be null");
                } else {
                    assert_eq!(results[i], col.get_int64(n), "seed {seed}: row {n}");
                }
            }
            assert_eq!(col.null_count(), null_cnt);
            assert_eq!(sel.len(), col.length);

            for i in 0..128i64 {
                if i % 2 == 0 {
                    col.append_null();
                } else {
                    col.append_int64(i * i * i);
                }
            }

            assert_eq!(sel.len(), col.length - 128);
            assert_eq!(null_cnt + 128 / 2, col.null_count());
            for i in 0..128usize {
                if i % 2 == 0 {
                    assert!(col.is_null(sel.len() + i));
                } else {
                    let v = i as i64;
                    assert_eq!(v * v * v, col.get_int64(sel.len() + i));
                    assert!(!col.is_null(sel.len() + i));
                }
            }
        }
    }

    /// Go `TestReconstructVarLen` (`pkg/util/chunk/column_test.go:488`).
    #[test]
    fn reconstruct_var_len() {
        for seed in 1..=8u64 {
            let mut rng = Rng(seed);
            let mut col = Column::new_column(
                &FieldType::new(tidb_datatype::FieldTypeCode::VarString),
                1024,
            );
            let mut results: Vec<String> = Vec::with_capacity(1024);
            let mut nulls: Vec<bool> = Vec::with_capacity(1024);
            let mut sel: Vec<usize> = Vec::with_capacity(1024);
            for i in 0..1024 {
                if rng.intn10() < 6 {
                    sel.push(i);
                }
                if rng.intn10() < 2 {
                    col.append_null();
                    nulls.push(true);
                    results.push(String::new());
                    continue;
                }
                let v = rng.int63().to_string();
                col.append_string(&v);
                results.push(v);
                nulls.push(false);
            }

            col.reconstruct(&sel);
            let mut null_cnt = 0;
            for (n, &i) in sel.iter().enumerate() {
                if nulls[i] {
                    null_cnt += 1;
                    assert!(col.is_null(n), "seed {seed}: row {n} should be null");
                } else {
                    assert_eq!(
                        results[i].as_bytes(),
                        col.get_bytes(n),
                        "seed {seed}: row {n}"
                    );
                }
            }
            assert_eq!(col.null_count(), null_cnt);
            assert_eq!(sel.len(), col.length);

            for i in 0..128usize {
                if i % 2 == 0 {
                    col.append_null();
                } else {
                    col.append_string(&(i * i * i).to_string());
                }
            }

            assert_eq!(sel.len(), col.length - 128);
            assert_eq!(null_cnt + 128 / 2, col.null_count());
            for i in 0..128usize {
                if i % 2 == 0 {
                    assert!(col.is_null(sel.len() + i));
                } else {
                    assert_eq!(
                        (i * i * i).to_string().as_bytes(),
                        col.get_bytes(sel.len() + i)
                    );
                    assert!(!col.is_null(sel.len() + i));
                }
            }
        }
    }

    #[test]
    fn fixed_int64_append_get_null() {
        let mut c = Column::new_fixed_len(8, 4);
        assert!(c.is_fixed());
        assert_eq!(c.type_size(), 8);
        c.append_int64(10);
        c.append_null();
        c.append_int64(-3);
        assert_eq!(c.rows(), 3);
        assert_eq!(c.get_int64(0), 10);
        assert!(!c.is_null(0));
        assert!(c.is_null(1));
        assert!(!c.is_null(2));
        assert_eq!(c.get_int64(2), -3);
    }

    #[test]
    fn null_bitmap_spans_multiple_bytes() {
        let mut c = Column::new_fixed_len(8, 16);
        for i in 0..10 {
            if i % 2 == 0 {
                c.append_int64(i);
            } else {
                c.append_null();
            }
        }
        assert_eq!(c.rows(), 10);
        for i in 0..10 {
            assert_eq!(c.is_null(i as usize), i % 2 != 0, "row {i}");
        }
    }

    #[test]
    fn float_and_uint_roundtrip() {
        let mut f = Column::new_fixed_len(8, 2);
        f.append_float64(3.5);
        f.append_float64(-1.25);
        assert_eq!(f.get_float64(0), 3.5);
        assert_eq!(f.get_float64(1), -1.25);

        let mut f32c = Column::new_fixed_len(4, 1);
        f32c.append_float32(2.5);
        assert_eq!(f32c.get_float32(0), 2.5);

        let mut u = Column::new_fixed_len(8, 1);
        u.append_uint64(u64::MAX);
        assert_eq!(u.get_uint64(0), u64::MAX);
    }

    #[test]
    fn reset_clears_rows_keeps_kind() {
        let mut c = Column::new_fixed_len(8, 2);
        c.append_int64(7);
        c.reset();
        assert_eq!(c.rows(), 0);
        assert!(c.is_fixed());
        c.append_int64(9);
        assert_eq!(c.get_int64(0), 9);
    }

    #[test]
    fn var_len_column_shape() {
        let c = Column::new_var_len(4);
        assert!(!c.is_fixed());
        assert_eq!(c.type_size(), VAR_ELEM_LEN);
    }

    #[test]
    fn var_len_append_get_string_bytes_null() {
        let mut c = Column::new_var_len(4);
        c.append_string("hello");
        c.append_null();
        c.append_bytes(&[0x00, 0xff, 0x10]); // non-UTF8 binary
        c.append_string("");
        assert_eq!(c.rows(), 4);
        assert_eq!(c.get_bytes(0), b"hello");
        assert!(!c.is_null(0));
        // Null row has zero width and is flagged null.
        assert!(c.is_null(1));
        assert_eq!(c.get_bytes(1), b"");
        assert_eq!(c.get_bytes(2), &[0x00, 0xff, 0x10]);
        assert_eq!(c.get_raw(2), &[0x00, 0xff, 0x10]);
        assert_eq!(c.get_bytes(3), b"");
        assert!(!c.is_null(3)); // empty string is NOT null
    }

    /// The append side must produce the exact cell bytes Go's
    /// `Column.AppendEnum`/`AppendSet` produce, because the chunk-codec
    /// decoder (`tidb-codec`'s `decode_column_datums`) reads that layout: an
    /// 8-byte native-endian value followed by the element name.
    ///
    /// Captured from a real TiDB via a throwaway
    /// `TestZZDumpTablesPriv` (`go test -tags=intest ./pkg/executor/`), which
    /// printed `chunk.NewColumn(...).GetRaw(0)` for both types.
    #[test]
    fn enum_and_set_cells_are_the_bytes_go_writes() {
        use tidb_datatype::FieldTypeCode;
        let mut enums = Column::new_column(&FieldType::new(FieldTypeCode::Enum), 4);
        enums.append_enum(&MysqlEnum::new("bb", 2));
        assert_eq!(
            enums.get_raw(0),
            &[0x02, 0, 0, 0, 0, 0, 0, 0, b'b', b'b'],
            "Go printed: 02 00 00 00 00 00 00 00 62 62"
        );

        // `mysql.tables_priv`.`Table_priv` spells GRANT OPTION `Grant`, and its
        // element list puts it at bit 6, so `Select,Grant` is 1|64 = 0x41.
        let mut sets = Column::new_column(&FieldType::new(FieldTypeCode::Set), 4);
        sets.append_set(&MysqlSet::new("Select,Grant", 1 | 64));
        let mut expected = vec![0x41, 0, 0, 0, 0, 0, 0, 0];
        expected.extend_from_slice(b"Select,Grant");
        assert_eq!(sets.get_raw(0), expected.as_slice());
    }

    #[test]
    fn enum_and_set_cells_round_trip_including_the_empty_and_null_ones() {
        use tidb_datatype::FieldTypeCode;
        let mut c = Column::new_column(&FieldType::new(FieldTypeCode::Set), 4);
        // Go's `getNameValue` answers the zero pair for a zero-width cell, and
        // an empty SET (`Value == 0`) is written with no name -- but Go still
        // writes the 8-byte prefix, so the cell is 8 bytes, not empty.
        c.append_set(&MysqlSet::new("", 0));
        c.append_null();
        c.append_set(&MysqlSet::new("Select,Update", 1 | 4));
        assert_eq!(c.get_set(0), MysqlSet::new("", 0));
        assert_eq!(c.get_raw(0).len(), 8);
        assert!(c.is_null(1));
        // A null cell is zero-width, which is exactly the case Go's
        // `getNameValue` short-circuits.
        assert_eq!(c.get_name_value(1), (GoString::default(), 0));
        assert_eq!(c.get_set(2), MysqlSet::new("Select,Update", 5));

        let mut e = Column::new_column(&FieldType::new(FieldTypeCode::Enum), 2);
        e.append_enum(&MysqlEnum::new("N", 1));
        e.append_enum(&MysqlEnum::new("Y", 2));
        assert_eq!(e.get_enum(0), MysqlEnum::new("N", 1));
        assert_eq!(e.get_enum(1), MysqlEnum::new("Y", 2));

        let mut raw = Column::new_column(&FieldType::new(FieldTypeCode::Enum), 2);
        raw.append_enum(&MysqlEnum::new(vec![0xff], 1));
        raw.append_set(&MysqlSet::new(vec![0xfe], 2));
        assert_eq!(raw.get_enum(0).name_bytes(), &[0xff]);
        assert_eq!(raw.get_set(1).name_bytes(), &[0xfe]);
    }

    #[test]
    fn get_raw_fixed_and_var() {
        let mut f = Column::new_fixed_len(8, 1);
        f.append_int64(0x0102_0304_0506_0708);
        assert_eq!(f.get_raw(0), &0x0102_0304_0506_0708i64.to_ne_bytes());

        let mut v = Column::new_var_len(1);
        v.append_bytes(b"abc");
        assert_eq!(v.get_raw(0), b"abc");
    }

    #[test]
    fn copy_construct_is_deep() {
        let mut c = Column::new_fixed_len(8, 2);
        c.append_int64(42);
        let d = c.copy_construct();
        assert_eq!(d.rows(), 1);
        assert_eq!(d.get_int64(0), 42);
    }

    #[test]
    fn time_append_get_null_roundtrip() {
        use tidb_datatype::{CoreTime, TimeType};
        let mut c = Column::new_column(&FieldType::new(FieldTypeCode::Datetime), 4);
        assert_eq!(c.type_size(), SIZE_TIME);
        let dt = Time::new(
            CoreTime::from_date(2026, 7, 25, 12, 34, 56, 654_321),
            TimeType::DateTime,
            6,
        )
        .unwrap();
        let ts = Time::new(
            CoreTime::from_date(1999, 12, 31, 23, 59, 59, 0),
            TimeType::Timestamp,
            0,
        )
        .unwrap();
        let date = Time::new(
            CoreTime::from_date(2000, 2, 29, 0, 0, 0, 0),
            TimeType::Date,
            0,
        )
        .unwrap();
        c.append_time(dt);
        c.append_null();
        c.append_time(ts);
        c.append_time(date);
        assert_eq!(c.rows(), 4);
        assert_eq!(c.get_time(0), dt);
        assert!(c.is_null(1));
        assert!(!c.is_null(2));
        assert_eq!(c.get_time(2), ts);
        assert_eq!(c.get_time(3), date);
        // The stored bytes are exactly Go's packed uint64 (native-endian).
        assert_eq!(c.get_raw(0), &dt.go_raw().to_ne_bytes());
    }

    #[test]
    fn duration_append_get_null_roundtrip() {
        let mut c = Column::new_column(&FieldType::new(FieldTypeCode::Duration), 4);
        assert_eq!(c.type_size(), 8);
        let d = MySqlDuration::new(11, 22, 33, 456_789, 6).unwrap();
        let neg = d.negated();
        c.append_duration(d);
        c.append_null();
        c.append_duration(neg);
        assert_eq!(c.rows(), 3);
        // Append ignores fsp; the reader supplies it (Go GetDuration fillFsp).
        assert_eq!(c.get_duration(0, 6), d);
        assert_eq!(c.get_duration(0, 3).nanoseconds(), d.nanoseconds());
        assert_eq!(c.get_duration(0, 3).fsp(), 3);
        assert!(c.is_null(1));
        assert_eq!(c.get_duration(2, 6), neg);
        // Stored as Go's int64 nanoseconds.
        assert_eq!(c.get_int64(0), d.nanoseconds());
    }

    #[test]
    fn fixed_len_type_dispatch() {
        use tidb_datatype::FieldTypeCode;
        let ft = |c| FieldType::new(c);
        assert_eq!(get_fixed_len(&ft(FieldTypeCode::Float)), 4);
        assert_eq!(get_fixed_len(&ft(FieldTypeCode::Long)), 8);
        assert_eq!(get_fixed_len(&ft(FieldTypeCode::LongLong)), 8);
        assert_eq!(get_fixed_len(&ft(FieldTypeCode::Double)), 8);
        assert_eq!(get_fixed_len(&ft(FieldTypeCode::Duration)), 8);
        assert_eq!(get_fixed_len(&ft(FieldTypeCode::Datetime)), SIZE_TIME);
        assert_eq!(
            get_fixed_len(&ft(FieldTypeCode::NewDecimal)),
            MY_DECIMAL_STRUCT_SIZE
        );
        assert_eq!(get_fixed_len(&ft(FieldTypeCode::VarString)), VAR_ELEM_LEN);
        assert_eq!(get_fixed_len(&ft(FieldTypeCode::Blob)), VAR_ELEM_LEN);
    }

    #[test]
    fn new_column_from_field_type() {
        use tidb_datatype::FieldTypeCode;
        let mut int_col = Column::new_column(&FieldType::new(FieldTypeCode::Long), 4);
        assert!(int_col.is_fixed());
        assert_eq!(int_col.type_size(), 8);
        int_col.append_int64(5);
        assert_eq!(int_col.get_int64(0), 5);

        let mut str_col = Column::new_column(&FieldType::new(FieldTypeCode::VarString), 4);
        assert!(!str_col.is_fixed());
        str_col.append_string("x");
        assert_eq!(str_col.get_bytes(0), b"x");

        let empty_fixed = Column::new_empty_column(&FieldType::new(FieldTypeCode::Float));
        assert!(empty_fixed.is_fixed());
        assert_eq!(empty_fixed.type_size(), 4);
        let empty_var = Column::new_empty_column(&FieldType::new(FieldTypeCode::Blob));
        assert!(!empty_var.is_fixed());
    }

    #[test]
    fn copy_construct_owns_buffers_and_clears_zero_copy_reuse_guard() {
        let mut source = Column::new_var_len(2);
        source.append_string("owned value");
        source.avoid_reusing = true;

        let copied = source.copy_construct();
        assert_eq!(copied.get_bytes(0), b"owned value");
        assert!(!copied.avoid_reusing);
        assert!(source.avoid_reusing);
    }

    #[test]
    fn resize_reserve_and_eval_type_reset_match_go_shapes() {
        let mut column = Column::new_column(&FieldType::new(FieldTypeCode::LongLong), 2);
        column.resize_int64(4, false);
        assert_eq!(column.rows(), 4);
        assert_eq!(column.null_bitmap, vec![0x0f]);
        assert_eq!(column.data.len(), 32);
        assert!(column.data.iter().all(|byte| *byte == 0));

        column.resize_uint64(11, false);
        assert_eq!(column.null_bitmap, vec![0xff, 0x07]);
        column.resize_uint64(7, true);
        assert_eq!(column.null_bitmap, vec![0]);

        column.reset_for_eval_type(EvalType::Duration);
        assert!(column.is_fixed());
        assert_eq!(column.type_size(), 8);
        assert_eq!(column.rows(), 0);
        column.append_duration(MySqlDuration::from_nanoseconds(7, 0).unwrap());
        assert_eq!(column.data.len(), 8);

        column.reset_for_eval_type(EvalType::String);
        assert!(!column.is_fixed());
        assert_eq!(column.offsets, vec![0]);
        column.append_string("x");
        assert_eq!(column.get_bytes(0), b"x");
    }

    /// Go `Column.resize` re-slices rather than recreating the append scratch
    /// and leaves the unrelated offsets slice alone.  Both details are
    /// observable after an evaluation-type transition.
    #[test]
    fn resize_preserves_scratch_and_offset_headers() {
        let mut fixed = Column::new_fixed_len(8, 2);
        let scratch = 0x0102_0304_0506_0708_i64;
        fixed.append_int64(scratch);
        fixed.resize_int64(0, false);
        fixed.append_null();
        assert!(fixed.is_null(0));
        assert_eq!(fixed.get_raw(0), &scratch.to_ne_bytes());

        let mut changing = Column::new_var_len(2);
        changing.offsets = vec![7, 9];
        changing.resize_int64(0, false);
        assert_eq!(changing.offsets, vec![7, 9]);
        changing.reserve_string(1);
        assert_eq!(changing.offsets, vec![7]);
    }

    #[test]
    fn reserve_preserves_content_and_typed_reserve_clears_rows() {
        let mut column = Column::new_var_len(0);
        column.append_string("alpha");
        let bitmap = column.null_bitmap.clone();
        let offsets = column.offsets.clone();
        let data = column.data.clone();
        column.reserve(10, 10, 10);
        assert_eq!(column.null_bitmap, bitmap);
        assert_eq!(column.offsets, offsets);
        assert_eq!(column.data, data);
        assert!(column.null_bitmap.capacity() >= bitmap.len() + 10);
        assert!(column.offsets.capacity() >= offsets.len() + 10);
        assert!(column.data.capacity() >= data.len() + 10);

        column.reserve_string_with_size_hint(9, 36);
        assert_eq!(column.rows(), 0);
        assert_eq!(column.offsets, vec![0]);
        assert!(column.data_capacity() >= 9 * 36);
        assert!(column.offset_capacity() >= 10);
        assert!(column.null_bitmap_capacity() >= 2);
    }

    #[test]
    fn append_cell_n_times_and_copy_reconstruct_cover_fixed_var_and_null() {
        let mut fixed = Column::new_fixed_len(8, 4);
        fixed.append_int64(11);
        fixed.append_null();
        fixed.append_int64(33);

        let mut fixed_copy = Column::new_fixed_len(8, 0);
        fixed_copy.append_cell_n_times(&fixed, 0, 3);
        fixed_copy.append_cell_n_times(&fixed, 1, 2);
        assert_eq!(fixed_copy.rows(), 5);
        assert_eq!(fixed_copy.get_int64(0), 11);
        assert_eq!(fixed_copy.get_int64(2), 11);
        assert!(fixed_copy.is_null(3));
        assert!(fixed_copy.is_null(4));

        let mut variable = Column::new_var_len(4);
        variable.append_string("a");
        variable.append_null();
        variable.append_string("ccc");
        let selected = variable.copy_reconstruct(Some(&[2, 0, 1]), None);
        assert_eq!(selected.get_bytes(0), b"ccc");
        assert_eq!(selected.get_bytes(1), b"a");
        assert!(selected.is_null(2));
        assert_eq!(selected.offsets, vec![0, 3, 4, 4]);
    }

    #[test]
    fn set_null_ranges_and_merge_nulls_match_rowwise_and() {
        let mut left = Column::new_fixed_len(8, 16);
        let mut right = Column::new_fixed_len(8, 16);
        let mut result = Column::new_fixed_len(8, 16);
        left.resize_int64(11, false);
        right.resize_int64(11, false);
        result.resize_int64(11, false);
        left.set_nulls(1, 4, true);
        right.set_nulls(3, 8, true);
        result.merge_nulls(&[&left, &right]);
        for row in 0..11 {
            assert_eq!(
                result.is_null(row),
                left.is_null(row) || right.is_null(row),
                "row {row}"
            );
        }
        assert_eq!(result.null_count(), 7);
    }

    /// Go `TestLargeStringColumnOffset` (`pkg/util/chunk/column_test.go`): a
    /// var-length column's offsets are 64-BIT. A 6M string field at a batch
    /// size of 1024 puts the offset past 6GB, which an `int32` offset would
    /// silently wrap.
    #[test]
    fn go_test_large_string_column_offset() {
        let mut col = Column::new_var_len(1);
        col.offsets[0] = 6 << 30;
        assert_eq!(col.offsets[0], 6_i64 << 30);
    }

    /// Go `TestJSONColumn` (`pkg/util/chunk/column_test.go`): 1024 distinct
    /// JSON objects round-trip through the column, and reading them back
    /// through the COLUMN and through a `Row` agrees, printed form included.
    #[test]
    fn go_test_json_column() {
        let field = FieldType::new(FieldTypeCode::Json);
        let mut chk = crate::chunk::Chunk::new_with_capacity(&[field], 1024);
        for i in 0..1024 {
            let json = tidb_datatype::BinaryJSON::parse(&format!("{{\"{i}\":{i}}}"))
                .expect("valid JSON object");
            chk.append_json(0, &json);
        }

        let mut it = crate::iterator::Iterator4Chunk::new(&chk);
        let mut i = 0;
        let mut row = crate::iterator::ChunkIterator::begin(&mut it);
        while row.is_some() {
            let j1 = chk.column(0).get_json(i);
            let j2 = row.expect("not end").get_json(0);
            assert_eq!(j2.to_string(), j1.to_string());
            assert_eq!(j1.to_string(), format!("{{\"{i}\": {i}}}"));
            i += 1;
            row = crate::iterator::ChunkIterator::next_row(&mut it);
        }
        assert_eq!(i, 1024);
    }
}

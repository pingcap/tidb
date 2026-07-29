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
//! DEFERRED -- `MyDecimal` cells: Go stores a `NewDecimal` cell as the raw
//! 40-byte in-memory `types.MyDecimal` struct
//! (`digitsInt`/`digitsFrac`/`resultFrac`/`negative` + 9 base-1e9 `int32`
//! words). The Rust `tidb_datatype::Decimal` is a decimal-digit-string
//! representation and cannot round-trip that 40-byte layout byte-for-byte;
//! faking a different layout would silently break Go/Rust chunk fidelity, so
//! `AppendMyDecimal`/`GetDecimal` wait until a layout-faithful `MyDecimal`
//! exists.
//!
//! Also ported: the Enum/Set name-value cells (Go `appendNameValue`/
//! `getNameValue`: the 8-byte native-endian value followed by the element
//! name, in one variable-length row).
//!
//! DEFERRED (documented, later tranches): the typed appends and `Resize*` for
//! `MyDecimal` (see above), JSON, and `VectorFloat32`;
//! `Reset(EvalType)`; the `Reserve`/`resize` capacity helpers;
//! `SetNull(s)`/`nullCount`; a `str`-typed `GetString`; and the `Chunk`/`Row`
//! containers built on `Column`.

use tidb_datatype::{
    FieldType, FieldTypeCode, MyDecimal, MySqlDuration, MysqlEnum, MysqlSet, Time,
    MYDECIMAL_STRUCT_SIZE,
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
#[derive(Clone, Debug, Default)]
pub struct Column {
    length: usize,
    /// Bit `i` records row `i`: 0 = null, 1 = not-null (Go `nullBitmap`).
    null_bitmap: Vec<u8>,
    /// Row `i` of a variable-length column starts at `data[offsets[i]]`
    /// (Go `offsets`; empty for a fixed-length column).
    offsets: Vec<i64>,
    /// The packed element bytes (Go `data`).
    data: Vec<u8>,
    /// Scratch buffer sized to one fixed element; empty for a var-length column
    /// (Go `elemBuf`).
    elem_buf: Vec<u8>,
    /// Go `avoidReusing`: keep this column out of the allocator's reuse pool.
    pub avoid_reusing: bool,
}

impl Column {
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
        Column {
            elem_buf: Vec::new(),
            data: Vec::new(),
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
    fn append_name_value(&mut self, name: &str, value: u64) {
        debug_assert!(
            !self.is_fixed(),
            "append_name_value on a fixed-length column"
        );
        self.data.extend_from_slice(&value.to_ne_bytes());
        self.data.extend_from_slice(name.as_bytes());
        self.finish_append_var();
    }

    /// Go `AppendEnum`.
    pub fn append_enum(&mut self, value: &MysqlEnum) {
        self.append_name_value(value.name(), value.value());
    }

    /// Go `AppendSet`.
    pub fn append_set(&mut self, value: &MysqlSet) {
        self.append_name_value(value.name(), value.value());
    }

    /// Go `getNameValue`: an empty cell is the zero pair, exactly as in Go;
    /// otherwise the leading 8 bytes are the value and the rest is the name.
    ///
    /// # Panics
    /// Panics on a non-empty cell shorter than the 8-byte value prefix, or one
    /// whose name bytes are not UTF-8 -- neither is producible by
    /// [`Column::append_enum`]/[`Column::append_set`].
    #[must_use]
    pub fn get_name_value(&self, row_id: usize) -> (String, u64) {
        let cell = self.get_bytes(row_id);
        if cell.is_empty() {
            return (String::new(), 0);
        }
        let (value_bytes, name_bytes) = cell
            .split_at_checked(8)
            .expect("a name/value cell carries its 8-byte value prefix");
        let value = u64::from_ne_bytes(value_bytes.try_into().expect("eight bytes"));
        let name = std::str::from_utf8(name_bytes)
            .expect("a name/value cell holds a UTF-8 element name")
            .to_owned();
        (name, value)
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
        self.clone()
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
}

#[cfg(test)]
mod tests {
    use super::*;

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
        assert_eq!(c.get_name_value(1), (String::new(), 0));
        assert_eq!(c.get_set(2), MysqlSet::new("Select,Update", 5));

        let mut e = Column::new_column(&FieldType::new(FieldTypeCode::Enum), 2);
        e.append_enum(&MysqlEnum::new("N", 1));
        e.append_enum(&MysqlEnum::new("Y", 2));
        assert_eq!(e.get_enum(0), MysqlEnum::new("N", 1));
        assert_eq!(e.get_enum(1), MysqlEnum::new("Y", 2));
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
}

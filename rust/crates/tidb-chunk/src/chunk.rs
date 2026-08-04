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

//! `pkg/util/chunk/chunk.go`: the `Chunk`, a batch of rows in columnar layout.
//!
//! A `Chunk` holds one [`Column`] per output field; row `i` is the `i`-th cell
//! of every column. Executors produce chunks and expression evaluation reads
//! rows out of them (see [`crate::row::Row`]).
//!
//! Ported: construction from field types, `num_cols`/`num_rows` (selection
//! aware), `column`(`_mut`), `get_row`, `reset`, `set_num_virtual_rows`,
//! `capacity`, the by-column typed append helpers, and `append_row`/
//! `append_partial_row` (row copy via `Column::append_cell_from`).
//!
//! DEFERRED (documented): the `requiredRows`/`IsFull` growth policy,
//! `GrowAndReset`, `CopyConstructSel` and other selection transforms, the chunk
//! pool/allocator, disk spilling (`chunk_in_disk`), and the exotic-typed append
//! helpers that depend on `VectorFloat32` column support (Time and
//! Duration are ported; see `column.rs` for the `MyDecimal` layout deferral).

use crate::column::Column;
use crate::row::Row;
use tidb_datatype::{Datum, FieldType, MyDecimal, MySqlDuration, Time};

/// Go `chunk.Chunk`: a columnar batch of rows.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Chunk {
    /// Go `sel`: the selected physical row indices, or `None` when all rows are
    /// selected.
    pub(crate) sel: Option<Vec<usize>>,
    pub(crate) columns: Vec<Column>,
    /// Go `numVirtualRows`: the row count when the chunk holds no columns.
    pub(crate) num_virtual_rows: usize,
    /// Go `capacity`: the max rows this chunk was sized for.
    pub(crate) capacity: usize,
    /// Go `requiredRows`: how many rows the parent executor wants.
    pub(crate) required_rows: usize,
    /// Go `inCompleteChunk`: some columns are intentionally unfilled.
    in_complete_chunk: bool,
}

impl Chunk {
    /// Go `New`: a chunk for `fields`, capped at `min(capacity, max_chunk_size)`
    /// rows, with `required_rows = max_chunk_size`.
    #[must_use]
    pub fn new(fields: &[FieldType], capacity: usize, max_chunk_size: usize) -> Self {
        let capacity = capacity.min(max_chunk_size);
        Chunk {
            sel: None,
            columns: fields
                .iter()
                .map(|f| Column::new_column(f, capacity))
                .collect(),
            num_virtual_rows: 0,
            capacity,
            required_rows: max_chunk_size,
            in_complete_chunk: false,
        }
    }

    /// Go `NewChunkWithCapacity`.
    #[must_use]
    pub fn new_with_capacity(fields: &[FieldType], capacity: usize) -> Self {
        Chunk::new(fields, capacity, capacity)
    }

    /// Go `NewEmptyChunk`: columns typed for `fields` with no preallocation.
    #[must_use]
    pub fn new_empty(fields: &[FieldType]) -> Self {
        Chunk {
            columns: fields.iter().map(Column::new_empty_column).collect(),
            ..Chunk::default()
        }
    }

    /// Go `NumCols`.
    #[must_use]
    pub fn num_cols(&self) -> usize {
        self.columns.len()
    }

    /// Go `Chunk.MemoryUsage`: the bytes this chunk's columns hold, summed
    /// over `Column::memory_usage`.
    ///
    /// This is the number Go's memory-tracked operators consume per chunk, so
    /// it is capacity-based rather than length-based: an operator that keeps a
    /// chunk keeps its whole allocation, not just the rows in use.
    #[must_use]
    pub fn memory_usage(&self) -> i64 {
        self.columns.iter().map(Column::memory_usage).sum()
    }

    /// Go `NumRows`: the logical row count (selection aware; virtual for a
    /// column-less or incomplete chunk).
    #[must_use]
    pub fn num_rows(&self) -> usize {
        if let Some(sel) = &self.sel {
            return sel.len();
        }
        if self.in_complete_chunk || self.num_cols() == 0 {
            return self.num_virtual_rows;
        }
        self.columns[0].rows()
    }

    /// Go `Column`: the column at `col_idx`.
    #[must_use]
    pub fn column(&self, col_idx: usize) -> &Column {
        &self.columns[col_idx]
    }

    /// A mutable borrow of the column at `col_idx`.
    pub fn column_mut(&mut self, col_idx: usize) -> &mut Column {
        &mut self.columns[col_idx]
    }

    /// Go `GetRow`: the logical row at `idx`, mapped through the selection.
    #[must_use]
    pub fn get_row(&self, idx: usize) -> Row<'_> {
        let physical = match &self.sel {
            Some(sel) => sel[idx],
            None => idx,
        };
        Row::new(self, physical)
    }

    /// Go `numVirtualRows`: the field itself, which the join copy helpers and
    /// their tests assert on directly.
    #[must_use]
    pub fn num_virtual_rows(&self) -> usize {
        self.num_virtual_rows
    }

    /// Go `SetNumVirtualRows`.
    pub fn set_num_virtual_rows(&mut self, num_virtual_rows: usize) {
        self.num_virtual_rows = num_virtual_rows;
    }

    /// Go `Capacity`.
    #[must_use]
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// Go `Reset`: clear all rows, keeping the columns' element types so the
    /// memory can be reused.
    pub fn reset(&mut self) {
        self.sel = None;
        for col in &mut self.columns {
            col.reset();
        }
        self.num_virtual_rows = 0;
    }

    /// Go `appendSel`: when appending to column 0 of a selection-carrying chunk,
    /// record the new physical row as selected.
    ///
    /// Column 0 is only consulted when a selection is present (Go's
    /// `colIdx == 0 && c.sel != nil`); a column-less chunk never carries a
    /// selection, so this must not touch `columns[0]` otherwise.
    fn append_sel(&mut self, col_idx: usize) {
        if col_idx == 0 {
            if let Some(sel) = &mut self.sel {
                let len = self.columns[0].rows();
                sel.push(len);
            }
        }
    }

    /// Go `AppendNull`.
    pub fn append_null(&mut self, col_idx: usize) {
        self.append_sel(col_idx);
        self.columns[col_idx].append_null();
    }

    /// Go `AppendInt64`.
    pub fn append_int64(&mut self, col_idx: usize, value: i64) {
        self.append_sel(col_idx);
        self.columns[col_idx].append_int64(value);
    }

    /// Go `AppendUint64`.
    pub fn append_uint64(&mut self, col_idx: usize, value: u64) {
        self.append_sel(col_idx);
        self.columns[col_idx].append_uint64(value);
    }

    /// Go `AppendFloat64`.
    pub fn append_float64(&mut self, col_idx: usize, value: f64) {
        self.append_sel(col_idx);
        self.columns[col_idx].append_float64(value);
    }

    /// Go `AppendTime`.
    pub fn append_time(&mut self, col_idx: usize, value: Time) {
        self.append_sel(col_idx);
        self.columns[col_idx].append_time(value);
    }

    /// Go `AppendDuration` (fsp is ignored, as in Go).
    pub fn append_duration(&mut self, col_idx: usize, value: MySqlDuration) {
        self.append_sel(col_idx);
        self.columns[col_idx].append_duration(value);
    }

    /// Go `AppendMyDecimal`.
    pub fn append_my_decimal(&mut self, col_idx: usize, value: &MyDecimal) {
        self.append_sel(col_idx);
        self.columns[col_idx].append_my_decimal(value);
    }

    /// Go `AppendString`.
    pub fn append_string(&mut self, col_idx: usize, value: &str) {
        self.append_sel(col_idx);
        self.columns[col_idx].append_string(value);
    }

    /// Go `AppendBytes`.
    pub fn append_bytes(&mut self, col_idx: usize, value: &[u8]) {
        self.append_sel(col_idx);
        self.columns[col_idx].append_bytes(value);
    }

    /// Go `AppendJSON`: a JSON cell is the var-length byte string
    /// `type code || value`, exactly the encoding `BinaryJSON` carries on the
    /// wire and in a row value.
    pub fn append_json(&mut self, col_idx: usize, value: &tidb_datatype::BinaryJSON) {
        self.append_bytes(col_idx, &value.encoded());
    }

    /// Go `AppendEnum`.
    pub fn append_enum(&mut self, col_idx: usize, value: &tidb_datatype::MysqlEnum) {
        self.append_sel(col_idx);
        self.columns[col_idx].append_enum(value);
    }

    /// Go `AppendSet`.
    pub fn append_set(&mut self, col_idx: usize, value: &tidb_datatype::MysqlSet) {
        self.append_sel(col_idx);
        self.columns[col_idx].append_set(value);
    }

    /// Go `AppendDatum`: append a [`Datum`] value into column `col_idx`,
    /// dispatching on its kind (the inverse of [`Row::get_datum`]).
    ///
    /// A `Datum::Decimal` carries the digit-string `Decimal`, so it reaches
    /// the raw 40-byte cell through `MyDecimal::from_string` over its
    /// canonical text -- the same text `Row::get_datum` reads back out. A
    /// value too large for the `MyDecimal` buffer panics rather than being
    /// silently truncated into the cell; callers holding a `MyDecimal`
    /// already should use the exact [`Chunk::append_my_decimal`].
    ///
    /// Supports the kinds whose column storage exists (NULL, int/uint, real/
    /// float32, string/bytes, binary literal, time, duration, decimal, JSON,
    /// enum, set). Other kinds panic, pending their column support.
    pub fn append_datum(&mut self, col_idx: usize, datum: &Datum) {
        match datum {
            Datum::Null => self.append_null(col_idx),
            Datum::Int(i) => self.append_int64(col_idx, *i),
            Datum::UInt(u) => self.append_uint64(col_idx, *u),
            Datum::Real(f) => self.append_float64(col_idx, *f),
            Datum::Float32(f) => {
                self.append_sel(col_idx);
                self.columns[col_idx].append_float32(*f as f32);
            }
            Datum::String(s) => self.append_bytes(col_idx, s.bytes()),
            Datum::Bytes(b) => self.append_bytes(col_idx, b),
            // A hex or bit literal lives in a binary `VarString` column, so
            // its cell is the literal's own bytes -- which is how Go stores
            // `KindBinaryLiteral`/`KindMysqlBit` in a chunk too.
            Datum::BinaryLiteral(literal) | Datum::Bit(literal) => {
                self.append_bytes(col_idx, literal.as_bytes());
            }
            Datum::Json(value) => self.append_json(col_idx, value),
            Datum::Enum(value, _) => self.append_enum(col_idx, value),
            Datum::Set(value, _) => self.append_set(col_idx, value),
            Datum::Time(t) => self.append_time(col_idx, *t),
            Datum::Duration(d) => self.append_duration(col_idx, *d),
            Datum::Decimal(dec) => {
                let text = dec.to_string();
                let (value, err) = MyDecimal::from_string(text.as_bytes());
                assert!(
                    err.is_none(),
                    "Chunk::append_datum: decimal {text} does not fit a MyDecimal cell ({err:?})"
                );
                self.append_my_decimal(col_idx, &value);
            }
            other => panic!(
                "Chunk::append_datum: datum {other:?} not yet supported (pending its column storage)"
            ),
        }
    }

    /// Go `AppendPartialRow`: append `row`'s cells into this chunk's columns
    /// starting at `col_off`.
    pub fn append_partial_row(&mut self, col_off: usize, row: Row<'_>) {
        self.append_sel(col_off);
        for (i, src_col) in row.chunk().columns.iter().enumerate() {
            self.columns[col_off + i].append_cell_from(src_col, row.idx());
        }
    }

    /// Go `AppendRow`: append a whole row (from another chunk) to this chunk.
    pub fn append_row(&mut self, row: Row<'_>) {
        self.append_partial_row(0, row);
        self.num_virtual_rows += 1;
    }

    /// Go `CopyConstruct`: a new chunk with a deep copy of this chunk's data.
    #[must_use]
    pub fn copy_construct(&self) -> Chunk {
        Chunk {
            sel: self.sel.clone(),
            columns: self.columns.iter().map(Column::copy_construct).collect(),
            num_virtual_rows: self.num_virtual_rows,
            capacity: self.capacity,
            required_rows: self.required_rows,
            in_complete_chunk: self.in_complete_chunk,
        }
    }

    /// The chunk's columns (for row accessors within the crate).
    pub(crate) fn columns(&self) -> &[Column] {
        &self.columns
    }

    /// Go `c.sel != nil`: whether a selection vector is installed.
    pub(crate) fn has_sel(&self) -> bool {
        self.sel.is_some()
    }

    /// Go `numVirtualRows += n`.
    pub(crate) fn add_virtual_rows(&mut self, n: usize) {
        self.num_virtual_rows += n;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tidb_datatype::FieldTypeCode;

    fn int_str_fields() -> Vec<FieldType> {
        vec![
            FieldType::new(FieldTypeCode::Long),
            FieldType::new(FieldTypeCode::VarString),
        ]
    }

    /// Go's own `Chunk.MemoryUsage` for the same chunks, captured from
    /// `pkg/util/chunk` in-process (`chunk.New(fields, cap, 1024)`):
    ///
    /// | chunk              | Go    |
    /// |--------------------|-------|
    /// | 1 bigint, cap 0    | 120   |
    /// | 1 bigint, cap 32   | 380   |
    /// | 1 bigint, cap 1024 | 8440  |
    ///
    /// A fixed-length column agrees exactly, which pins both the per-column
    /// struct size (112 bytes either way) and the capacity terms.
    #[test]
    fn memory_usage_of_a_fixed_length_column_matches_go() {
        let bigint = vec![FieldType::new(FieldTypeCode::LongLong)];
        assert_eq!(Chunk::new(&bigint, 0, 1024).memory_usage(), 120);
        assert_eq!(Chunk::new(&bigint, 32, 1024).memory_usage(), 380);
        assert_eq!(Chunk::new(&bigint, 1024, 1024).memory_usage(), 8440);
    }

    /// A VARIABLE-length column does NOT agree with Go, and the gap is in the
    /// chunk port's allocation strategy rather than in this accounting: Go's
    /// `newVarLenColumn` pre-reserves `data` for `estimatedElemLen*capacity`
    /// bytes while [`Column::new_var_len`] starts `data` empty. Go reports 636
    /// for a fresh single-VARCHAR chunk at capacity 32; the port reports the
    /// bytes it actually holds, which is less because it actually allocated
    /// less. Accounting the Go number here would over-count memory this
    /// process never took.
    #[test]
    fn memory_usage_of_a_var_length_column_reports_what_was_actually_allocated() {
        let varchar = vec![FieldType::new(FieldTypeCode::VarString)];
        let chk = Chunk::new(&varchar, 32, 1024);
        // 112 struct + 4 null bitmap + 33*8 offsets + 0 data + 0 elemBuf.
        assert_eq!(chk.memory_usage(), 112 + 4 + 33 * 8);
        assert!(chk.memory_usage() < 636, "Go's number for the same chunk");
    }

    /// The tracked number must GROW as rows land, or an operator that fills a
    /// chunk would be accounted as if it were still empty.
    #[test]
    fn memory_usage_grows_past_the_initial_capacity() {
        let fields = int_str_fields();
        let mut chk = Chunk::new(&fields, 8, 1024);
        let empty = chk.memory_usage();
        for i in 0..64 {
            chk.append_int64(0, i);
            chk.append_string(1, "abcdefgh");
        }
        assert!(chk.memory_usage() > empty);
    }

    #[test]
    fn build_and_read_rows() {
        let fields = int_str_fields();
        let mut chk = Chunk::new_with_capacity(&fields, 8);
        assert_eq!(chk.num_cols(), 2);
        assert_eq!(chk.num_rows(), 0);

        chk.append_int64(0, 10);
        chk.append_string(1, "a");
        chk.append_null(0);
        chk.append_string(1, "b");
        assert_eq!(chk.num_rows(), 2);

        let r0 = chk.get_row(0);
        assert_eq!(r0.len(), 2);
        assert_eq!(r0.get_int64(0), 10);
        assert!(!r0.is_null(0));
        assert_eq!(r0.get_bytes(1), b"a");

        let r1 = chk.get_row(1);
        assert!(r1.is_null(0));
        assert_eq!(r1.get_bytes(1), b"b");
    }

    #[test]
    fn append_row_copies_between_chunks() {
        let fields = int_str_fields();
        let mut src = Chunk::new_with_capacity(&fields, 4);
        src.append_int64(0, 7);
        src.append_string(1, "hi");

        let mut dst = Chunk::new_with_capacity(&fields, 4);
        dst.append_row(src.get_row(0));
        assert_eq!(dst.num_rows(), 1);
        let r = dst.get_row(0);
        assert_eq!(r.get_int64(0), 7);
        assert_eq!(r.get_bytes(1), b"hi");
    }

    #[test]
    fn reset_reuses_columns() {
        let fields = int_str_fields();
        let mut chk = Chunk::new_with_capacity(&fields, 4);
        chk.append_int64(0, 1);
        chk.append_string(1, "x");
        chk.reset();
        assert_eq!(chk.num_rows(), 0);
        chk.append_int64(0, 2);
        chk.append_string(1, "y");
        assert_eq!(chk.get_row(0).get_int64(0), 2);
    }

    #[test]
    fn get_datum_by_type() {
        use tidb_datatype::Datum;
        let fields = vec![
            FieldType::new(FieldTypeCode::Long),
            FieldType::new(FieldTypeCode::VarString),
            FieldType::new(FieldTypeCode::Double),
        ];
        let mut chk = Chunk::new_with_capacity(&fields, 4);
        chk.append_int64(0, 42);
        chk.append_string(1, "hi");
        chk.append_float64(2, 2.5);
        // second row: null int
        chk.append_null(0);
        chk.append_string(1, "");
        chk.append_float64(2, 0.0);

        let r0 = chk.get_row(0);
        assert_eq!(r0.get_datum(0, &fields[0]), Datum::Int(42));
        assert_eq!(r0.get_datum(2, &fields[2]), Datum::Real(2.5));
        match r0.get_datum(1, &fields[1]) {
            Datum::String(_) => {}
            other => panic!("expected string datum, got {other:?}"),
        }
        // null cell -> Datum::Null regardless of type
        assert_eq!(chk.get_row(1).get_datum(0, &fields[0]), Datum::Null);
    }

    /// A decimal datum must survive `append_datum` -> `get_datum` unchanged,
    /// which is the path an INSERT of a decimal literal takes.
    #[test]
    fn decimal_datum_round_trips_through_append_datum() {
        use tidb_datatype::{Decimal, FieldTypeCode};
        let ft = FieldType::new(FieldTypeCode::NewDecimal);
        let mut chunk = Chunk::new(std::slice::from_ref(&ft), 4, 8);
        for text in ["1.50", "-273.15", "0", "12345678901234567890.123456789"] {
            chunk.append_datum(0, &Datum::Decimal(Decimal::from_literal(text)));
        }
        chunk.append_null(0);

        let texts: Vec<String> = (0..4)
            .map(|i| match chunk.get_row(i).get_datum(0, &ft) {
                Datum::Decimal(d) => d.to_string(),
                other => panic!("expected a decimal, got {other:?}"),
            })
            .collect();
        assert_eq!(
            texts,
            ["1.50", "-273.15", "0", "12345678901234567890.123456789"]
        );
        assert!(chunk.get_row(4).is_null(0));
    }

    #[test]
    fn decimal_cells_round_trip_as_raw_struct_bytes() {
        use tidb_datatype::{FieldTypeCode, MyDecimal};
        let ft = FieldType::new(FieldTypeCode::NewDecimal);
        let mut chk = Chunk::new_with_capacity(std::slice::from_ref(&ft), 4);
        let a = MyDecimal::from_int(12345);
        let b = MyDecimal::from_int(-7);
        chk.append_my_decimal(0, &a);
        chk.append_null(0);
        chk.append_my_decimal(0, &b);
        assert_eq!(chk.num_rows(), 3);

        // The cell is the exact 40-byte struct.
        assert_eq!(chk.get_row(0).get_my_decimal(0), a);
        assert_eq!(chk.get_row(2).get_my_decimal(0), b);
        assert_eq!(chk.column(0).get_raw(0), &a.to_raw_bytes()[..]);
        // And reads back as a decimal datum with the same text.
        match chk.get_row(0).get_datum(0, &ft) {
            Datum::Decimal(d) => assert_eq!(d.to_string(), "12345"),
            other => panic!("expected decimal datum, got {other:?}"),
        }
        assert_eq!(chk.get_row(1).get_datum(0, &ft), Datum::Null);
    }

    #[test]
    fn time_duration_datum_roundtrip() {
        use tidb_datatype::{CoreTime, Datum, TimeType};
        let fields = vec![
            FieldType::new(FieldTypeCode::Datetime),
            FieldType::new(FieldTypeCode::Duration).with_decimal(3),
        ];
        let t = Time::new(
            CoreTime::from_date(2026, 7, 25, 8, 30, 15, 500_000),
            TimeType::DateTime,
            6,
        )
        .unwrap();
        let d = MySqlDuration::new(1, 2, 3, 400_000, 3).unwrap();

        let mut chk = Chunk::new_with_capacity(&fields, 4);
        chk.append_datum(0, &Datum::Time(t));
        chk.append_datum(1, &Datum::Duration(d));
        chk.append_datum(0, &Datum::Null);
        chk.append_datum(1, &Datum::Null);

        let r0 = chk.get_row(0);
        assert_eq!(r0.get_time(0), t);
        assert_eq!(r0.get_datum(0, &fields[0]), Datum::Time(t));
        // Duration fsp is refilled from the field type's decimal (Go
        // tp.GetDecimal()), matching what was appended here.
        assert_eq!(r0.get_duration(1, 3), d);
        assert_eq!(r0.get_datum(1, &fields[1]), Datum::Duration(d));
        let r1 = chk.get_row(1);
        assert_eq!(r1.get_datum(0, &fields[0]), Datum::Null);
        assert_eq!(r1.get_datum(1, &fields[1]), Datum::Null);
    }

    /// Go `pkg/util/chunk/chunk_test.go`'s `newAllTypes`, ported WHOLE: every
    /// field type the chunk tests build a column for, in Go's own order.
    ///
    /// The point of the whole table is that a column's SHAPE (fixed vs
    /// variable length) and the datum kind its cell reads back as must agree
    /// for EVERY type, not for the ones someone remembered. A single wrong
    /// pairing is either a panic (an 8-byte append into a var-length column,
    /// or `append_bytes` into a fixed one) or a silently wrong value.
    fn go_all_types() -> Vec<FieldType> {
        use tidb_datatype::FieldTypeCode as C;
        vec![
            FieldType::new(C::Tiny),
            FieldType::new(C::Short),
            FieldType::new(C::Int24),
            FieldType::new(C::Long),
            FieldType::new(C::LongLong),
            FieldType::new(C::LongLong).with_unsigned(true),
            FieldType::new(C::Year),
            FieldType::new(C::Float),
            FieldType::new(C::Double),
            FieldType::new(C::String),
            FieldType::new(C::VarString),
            FieldType::new(C::Varchar),
            FieldType::new(C::Blob),
            FieldType::new(C::TinyBlob),
            FieldType::new(C::MediumBlob),
            FieldType::new(C::LongBlob),
            FieldType::new(C::Date),
            FieldType::new(C::Datetime),
            FieldType::new(C::Timestamp),
            FieldType::new(C::Duration),
            FieldType::new(C::NewDecimal),
            FieldType::new(C::Set)
                .with_unsigned(true)
                .with_elems(["a", "b"]),
            FieldType::new(C::Enum)
                .with_unsigned(true)
                .with_elems(["a", "b"]),
            FieldType::new(C::Bit),
            FieldType::new(C::Json),
        ]
    }

    /// The value Go's `TestCompare`/`TestCopyTo` append for each type, as the
    /// datum this port's `append_datum` takes.
    fn go_all_types_value(field_type: &FieldType, k: u64) -> Datum {
        use tidb_datatype::Collation;
        // The same collation `Row::get_datum` stamps on an enum/set datum.
        fn collation_of(field_type: &FieldType) -> Collation {
            Collation::from_name(field_type.collation_name()).unwrap_or(Collation::Binary)
        }
        use tidb_datatype::{
            BinaryJSON, BinaryLiteral, CoreTime, Decimal, FieldTypeCode as C, MysqlEnum, MysqlSet,
            TimeType,
        };
        match field_type.code() {
            C::Tiny | C::Short | C::Int24 | C::Long | C::LongLong | C::Year => {
                if field_type.is_unsigned() {
                    Datum::UInt(k)
                } else {
                    Datum::Int(k as i64)
                }
            }
            C::Float => Datum::Float32(k as f64),
            C::Double => Datum::Real(k as f64),
            C::String
            | C::VarString
            | C::Varchar
            | C::Blob
            | C::TinyBlob
            | C::MediumBlob
            // Go appends the text and reads it back with `d.SetString(...,
            // tp.GetCollate())`, so the round-tripped datum is a
            // collation-tagged string.
            | C::LongBlob => {
                let mut d = Datum::Null;
                d.set_string(k.to_string().into_bytes(), collation_of(field_type));
                d
            }
            C::Date | C::Datetime | C::Timestamp => Datum::Time(
                Time::new(
                    CoreTime::from_date(2000, 1, 1, 0, 0, u8::try_from(k).unwrap(), 0),
                    match field_type.code() {
                        C::Date => TimeType::Date,
                        C::Timestamp => TimeType::Timestamp,
                        _ => TimeType::DateTime,
                    },
                    0,
                )
                .unwrap(),
            ),
            C::Duration => {
                Datum::Duration(MySqlDuration::new(0, 0, i64::try_from(k).unwrap(), 0, 0).unwrap())
            }
            C::NewDecimal => Datum::Decimal(Decimal::from_literal(&k.to_string())),
            // Go appends `types.Set{Name: "a", Value: k}` verbatim, without
            // asking the field type's elems to agree.
            C::Set => Datum::Set(
                MysqlSet::new("a".to_owned(), k),
                collation_of(field_type),
            ),
            C::Enum => Datum::Enum(
                MysqlEnum::new("a".to_owned(), k),
                collation_of(field_type),
            ),
            // Go: `chunk.AppendBytes(i, []byte{byte(k)})` -- a BIT cell is the
            // literal's own bytes in a VARIABLE-length column.
            C::Bit => Datum::Bit(BinaryLiteral::from(vec![u8::try_from(k & 0xff).unwrap()])),
            C::Json => Datum::Json(BinaryJSON::parse(&k.to_string()).unwrap()),
            other => panic!("type not handled: {other:?}"),
        }
    }

    /// Every type in Go's `newAllTypes` table survives
    /// `append_datum` -> `get_datum` with its kind and value intact, and a
    /// NULL cell in each reads back NULL.
    #[test]
    fn every_go_all_types_column_round_trips_a_datum() {
        let fields = go_all_types();
        let mut chunk = Chunk::new(&fields, 8, 128);
        for (i, field_type) in fields.iter().enumerate() {
            chunk.append_null(i);
            for k in 0..3u64 {
                chunk.append_datum(i, &go_all_types_value(field_type, k));
            }
        }
        for (i, field_type) in fields.iter().enumerate() {
            assert_eq!(
                chunk.get_row(0).get_datum(i, field_type),
                Datum::Null,
                "{field_type:?}"
            );
            for k in 0..3u64 {
                let expected = go_all_types_value(field_type, k);
                let actual = chunk
                    .get_row(usize::try_from(k).unwrap() + 1)
                    .get_datum(i, field_type);
                assert_eq!(actual, expected, "{field_type:?} at k={k}");
            }
        }
    }

    #[test]
    fn empty_chunk_virtual_rows() {
        let mut chk = Chunk::new_empty(&[]);
        assert_eq!(chk.num_cols(), 0);
        chk.set_num_virtual_rows(5);
        assert_eq!(chk.num_rows(), 5);
    }
}

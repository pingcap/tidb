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
//! helpers that depend on Decimal/JSON/Enum/Set column support (Time and
//! Duration are ported; see `column.rs` for the `MyDecimal` layout deferral).

use crate::column::Column;
use crate::row::Row;
use tidb_datatype::{Datum, FieldType, MySqlDuration, Time};

/// Go `chunk.Chunk`: a columnar batch of rows.
#[derive(Clone, Debug, Default)]
pub struct Chunk {
    /// Go `sel`: the selected physical row indices, or `None` when all rows are
    /// selected.
    sel: Option<Vec<usize>>,
    columns: Vec<Column>,
    /// Go `numVirtualRows`: the row count when the chunk holds no columns.
    num_virtual_rows: usize,
    /// Go `capacity`: the max rows this chunk was sized for.
    capacity: usize,
    /// Go `requiredRows`: how many rows the parent executor wants.
    required_rows: usize,
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

    /// Go `AppendDatum`: append a [`Datum`] value into column `col_idx`,
    /// dispatching on its kind (the inverse of [`Row::get_datum`]).
    ///
    /// Supports the kinds whose column storage exists (NULL, int/uint, real/
    /// float32, string/bytes, time, duration). Other kinds panic, pending
    /// their column support.
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
            Datum::Time(t) => self.append_time(col_idx, *t),
            Datum::Duration(d) => self.append_duration(col_idx, *d),
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

    #[test]
    fn empty_chunk_virtual_rows() {
        let mut chk = Chunk::new_empty(&[]);
        assert_eq!(chk.num_cols(), 0);
        chk.set_num_virtual_rows(5);
        assert_eq!(chk.num_rows(), 5);
    }
}

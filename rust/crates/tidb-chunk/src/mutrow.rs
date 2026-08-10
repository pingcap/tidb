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

//! `pkg/util/chunk/mutrow.go`: the MUTABLE one-row chunk.
//!
//! A `MutRow` is a `Chunk` whose every column holds exactly one row, built by
//! hand rather than through the append path so the single cell can be
//! overwritten in place. Partition pruning
//! (`pkg/table/tables/partition.go`, `rule_partition_processor.go`) and ranger
//! detachment (`pkg/util/ranger/detacher.go`) both evaluate expressions against
//! one, which is why it exists at this tier.
//!
//! Go models it as `type MutRow Row`, i.e. `{c *Chunk, idx int}` with `idx`
//! always 0. This port OWNS the chunk (`MutRow { chunk: Chunk }`) and hands out
//! a borrowed [`Row`] from [`MutRow::to_row`], which is the same thing with the
//! lifetime made explicit.
//!
//! Go's `MutRowFromValues`/`SetValue` take `any` and `MutRowFromDatums`/
//!   `SetDatum` take a `types.Datum`, the former reached through
//!   `Datum.GetValue()`. This port has one value type, so
//!   [`MutRow::from_datums`] covers both constructors; `SetValue` and
//!   `SetDatum` stay separate because their GROWTH rules differ (see
//!   [`MutRow::set_value`]).

use crate::chunk::Chunk;
use crate::column::{Column, MY_DECIMAL_STRUCT_SIZE, SIZE_TIME};
use crate::column_slot::ColumnSlot;
use crate::row::Row;
use crate::shared_bytes::SharedBytes;
use tidb_datatype::{
    BinaryJSON, Datum, FieldType, FieldTypeCode, MyDecimal, MySqlDuration, Time, TimeType,
    VectorFloat32,
};

/// Go `chunk.MutRow`: a mutable single-row chunk.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct MutRow {
    chunk: Chunk,
}

impl MutRow {
    /// Go `ToRow`: read the mutable row as an ordinary [`Row`].
    #[must_use]
    pub fn to_row(&self) -> Row<'_> {
        self.chunk.get_row(0)
    }

    /// Go `Len`: the number of columns.
    #[must_use]
    pub fn len(&self) -> usize {
        self.chunk.num_cols()
    }

    /// Whether the row has no columns.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Go `MutRowFromDatums` (and `MutRowFromValues`, which differs only in
    /// taking Go's `any` where this takes the same value as a [`Datum`]).
    #[must_use]
    pub fn from_datums(datums: &[Datum]) -> MutRow {
        MutRow::from_columns(datums.iter().map(make_mut_row_column).collect())
    }

    /// Go `MutRowFromTypes`: every column initialized to its type's zero value.
    #[must_use]
    pub fn from_types(field_types: &[FieldType]) -> MutRow {
        MutRow::from_columns(field_types.iter().map(zero_column_for_type).collect())
    }

    /// Go's `&Chunk{columns: ...}`: the one-row chunk behind every MutRow
    /// constructor.
    fn from_columns(columns: Vec<Column>) -> MutRow {
        let mut chunk = Chunk::default();
        chunk.columns = columns.into_iter().map(ColumnSlot::new).collect();
        MutRow { chunk }
    }

    /// Go `SetRow`: overwrite every column with `row`'s cells.
    ///
    /// A NULL source cell leaves the cleaned (empty/stale) destination cell
    /// NULL, exactly as in Go: the `continue` skips the write and the not-null
    /// bit is never set.
    pub fn set_row(&mut self, row: Row<'_>) {
        let source = row.chunk().expect("cannot copy the empty Row sentinel");
        for col_idx in 0..source.num_cols() {
            let (is_null, elem_len, cell) = {
                let source_column = source.column(col_idx);
                let raw = source_column.get_raw(row.idx());
                let cell = raw.to_vec();
                (
                    source_column.is_null(row.idx()),
                    source_column.elem_buffer_len(),
                    cell,
                )
            };
            let mut target = self.chunk.column_mut(col_idx);
            clean_col_of_mut_row(&mut target);
            if is_null {
                continue;
            }
            if elem_len > 0 {
                // Go copies with `copy(dst, src)`, which stops at the shorter
                // of the two.
                let n = target.data.len().min(cell.len());
                target.data.copy_from_slice(0..n, &cell[..n]);
            } else {
                set_mut_row_bytes(&mut target, &cell);
            }
            target.null_bitmap[0] = 1;
        }
    }

    /// Go `SetValues`.
    pub fn set_values(&mut self, values: &[Datum]) {
        for (i, value) in values.iter().enumerate() {
            self.set_value(i, value);
        }
    }

    /// Go `SetValue`: write `value` into column `col_idx` IN PLACE.
    ///
    /// Unlike [`MutRow::set_datum`] this never grows a fixed-width buffer --
    /// Go writes straight through `binary.LittleEndian.PutUint64(col.data, ..)`,
    /// which panics on a buffer shorter than the value. The caller is expected
    /// to have built the column from the matching type.
    ///
    /// [`Datum::Null`] stands for Go's `nil`: the column is cleaned and left
    /// NULL. The kinds Go's `any` switch has no arm for (`MinNotNull`,
    /// `MaxValue`, `Raw`) fall through to Go's implicit default: the cell is
    /// cleaned and then marked NOT NULL without any value being written.
    ///
    /// # Panics
    /// Panics when the column's buffer is too small for a fixed-width value,
    /// or when a bytes-shaped value reaches a fixed-length column -- both of
    /// which are Go panics too.
    pub fn set_value(&mut self, col_idx: usize, value: &Datum) {
        let mut column = self.chunk.column_mut(col_idx);
        clean_col_of_mut_row(&mut column);
        if matches!(value, Datum::Null) {
            return;
        }
        match value {
            Datum::Int(i) => put_uint64(&mut column, *i as u64),
            Datum::UInt(u) => put_uint64(&mut column, *u),
            Datum::Real(f) => put_uint64(&mut column, f.to_bits()),
            Datum::Float32(f) => {
                column
                    .data
                    .copy_from_slice(0..4, &(*f as f32).to_bits().to_le_bytes());
            }
            Datum::String(s) => set_mut_row_bytes(&mut column, s.bytes()),
            Datum::Bytes(b) => set_mut_row_bytes(&mut column, b),
            Datum::BinaryLiteral(l) | Datum::Bit(l) => {
                set_mut_row_bytes(&mut column, l.as_bytes());
            }
            Datum::Duration(d) => {
                column
                    .data
                    .copy_from_slice(0..8, &d.nanoseconds().to_ne_bytes());
            }
            Datum::Decimal(d) => {
                column.data.copy_from_slice(
                    0..MY_DECIMAL_STRUCT_SIZE as usize,
                    &my_decimal_of(d).to_raw_bytes(),
                );
            }
            Datum::Time(t) => {
                column
                    .data
                    .copy_from_slice(0..SIZE_TIME as usize, &t.go_raw().to_ne_bytes());
            }
            Datum::Enum(e, _) => {
                set_mut_row_name_value(&mut column, e.name_bytes(), e.value());
            }
            Datum::Set(s, _) => {
                set_mut_row_name_value(&mut column, s.name_bytes(), s.value());
            }
            Datum::Json(j) => set_mut_row_json(&mut column, j),
            Datum::VectorFloat32(v) => set_mut_row_bytes(&mut column, &v.serialize()),
            // Go's `any` switch has no arm for these, so nothing is written.
            Datum::Null | Datum::MinNotNull | Datum::MaxValue | Datum::Raw(_) => {}
        }
        column.null_bitmap[0] = 1;
    }

    /// Go `SetDatums`.
    pub fn set_datums(&mut self, datums: &[Datum]) {
        for (i, datum) in datums.iter().enumerate() {
            self.set_datum(i, datum);
        }
    }

    /// Go `SetDatum`: write `datum` into column `col_idx`, GROWING the buffer
    /// of a fixed-width kind when the column was built var-length (Go's
    /// `if len(col.data) < 8 { col.data = make([]byte, 8) }` family).
    ///
    /// Note what Go's growth leaves behind on a var-length column: `offsets[1]`
    /// is NOT updated, so `GetRaw`/`GetBytes` still report an empty cell while
    /// the typed getter reads the value back. This port reproduces that.
    pub fn set_datum(&mut self, col_idx: usize, datum: &Datum) {
        if matches!(datum, Datum::MinNotNull | Datum::MaxValue | Datum::Raw(_)) {
            self.chunk.columns[col_idx] = ColumnSlot::new(make_mut_row_column(datum));
            return;
        }
        let mut column = self.chunk.column_mut(col_idx);
        clean_col_of_mut_row(&mut column);
        if matches!(datum, Datum::Null) {
            return;
        }
        match datum {
            // Go writes `d.GetUint64()` for all three kinds, which is the
            // datum's raw 64-bit payload -- the float's bit pattern included.
            Datum::Int(_) | Datum::UInt(_) | Datum::Real(_) => {
                grow_data_to(&mut column, 8);
                let bits = match datum {
                    Datum::Int(i) => *i as u64,
                    Datum::UInt(u) => *u,
                    Datum::Real(f) => f.to_bits(),
                    _ => unreachable!("matched above"),
                };
                column.data.copy_from_slice(0..8, &bits.to_le_bytes());
            }
            Datum::Float32(f) => {
                grow_data_to(&mut column, 4);
                column
                    .data
                    .copy_from_slice(0..4, &(*f as f32).to_bits().to_le_bytes());
            }
            Datum::String(s) => set_mut_row_bytes(&mut column, s.bytes()),
            Datum::Bytes(b) => set_mut_row_bytes(&mut column, b),
            Datum::BinaryLiteral(l) | Datum::Bit(l) => {
                set_mut_row_bytes(&mut column, l.as_bytes());
            }
            Datum::Time(t) => {
                grow_data_to(&mut column, SIZE_TIME as usize);
                column
                    .data
                    .copy_from_slice(0..SIZE_TIME as usize, &t.go_raw().to_ne_bytes());
            }
            Datum::Duration(d) => {
                grow_data_to(&mut column, 8);
                column
                    .data
                    .copy_from_slice(0..8, &d.nanoseconds().to_ne_bytes());
            }
            Datum::Decimal(d) => {
                grow_data_to(&mut column, MY_DECIMAL_STRUCT_SIZE as usize);
                column.data.copy_from_slice(
                    0..MY_DECIMAL_STRUCT_SIZE as usize,
                    &my_decimal_of(d).to_raw_bytes(),
                );
            }
            Datum::Json(j) => set_mut_row_json(&mut column, j),
            Datum::VectorFloat32(v) => set_mut_row_bytes(&mut column, &v.serialize()),
            Datum::Enum(e, _) => {
                set_mut_row_name_value(&mut column, e.name_bytes(), e.value());
            }
            Datum::Set(s, _) => {
                set_mut_row_name_value(&mut column, s.name_bytes(), s.value());
            }
            // Go's `default` arm REPLACES the column with a freshly built one
            // and then sets the not-null bit on the column it just replaced
            // away -- so the new column keeps whatever nullity
            // `makeMutRowColumn` gave it, which for these sentinel kinds is
            // NULL. Returning early is that behaviour, written out.
            Datum::Null | Datum::MinNotNull | Datum::MaxValue | Datum::Raw(_) => unreachable!(),
        }
        column.null_bitmap[0] = 1;
    }

    /// Go `ShallowCopyPartialRow`: place `row`'s cells into the columns
    /// starting at `col_idx`.
    ///
    /// The mutable source borrow permits lazy promotion of only the aliased
    /// byte backing; ordinary columns remain lock-free `Vec` storage.
    pub fn shallow_copy_partial_row(&mut self, col_idx: usize, source: &mut Chunk, row_idx: usize) {
        for i in 0..source.columns.len() {
            let prepared = {
                let mut source_column = source.column_mut(i);
                prepare_shallow_column(&mut source_column, row_idx)
            };
            let mut target = self.chunk.column_mut(col_idx + i);
            target.null_bitmap[0] = u8::from(prepared.not_null);
            target.data = prepared.data;
            if let Some(length) = prepared.variable_length {
                target.offsets[1] = length;
            }
        }
    }
}

struct PreparedShallowColumn {
    not_null: bool,
    data: SharedBytes,
    variable_length: Option<i64>,
}

fn prepare_shallow_column(source: &mut Column, row_idx: usize) -> PreparedShallowColumn {
    let not_null = !source.is_null(row_idx);
    if source.is_fixed() {
        let elem_len = source.elem_buffer_len();
        let offset = row_idx * elem_len;
        PreparedShallowColumn {
            not_null,
            data: source.data.share_range(offset, offset + elem_len),
            variable_length: None,
        }
    } else {
        let start = source.offsets[row_idx] as usize;
        let end = source.offsets[row_idx + 1] as usize;
        let data = source.data.share_range(start, end);
        PreparedShallowColumn {
            not_null,
            variable_length: Some(data.len() as i64),
            data,
        }
    }
}

/// Go `cleanColOfMutRow`: zero every offset and clear the null bit, so the
/// cell reads back as an empty NULL until something writes it.
fn clean_col_of_mut_row(column: &mut Column) {
    for offset in &mut column.offsets {
        *offset = 0;
    }
    column.null_bitmap[0] = 0;
}

/// Go `newMutRowFixedLenColumn`: one not-null fixed-width row.
fn new_mut_row_fixed_len_column(elem_size: usize) -> Column {
    Column {
        length: 1,
        elem_buf: Some(vec![0; elem_size]),
        data: SharedBytes::zeros(elem_size),
        null_bitmap: vec![1],
        offsets: Vec::new(),
        avoid_reusing: false,
    }
}

/// Go `newMutRowVarLenColumn`: one not-null variable-width row of `val_size`
/// bytes. (Go carves the data and the 1-byte null bitmap out of a single
/// `val_size+1` allocation; the two fields are what matter.)
fn new_mut_row_var_len_column(val_size: usize) -> Column {
    Column {
        length: 1,
        elem_buf: None,
        data: SharedBytes::zeros(val_size),
        null_bitmap: vec![1],
        offsets: vec![0, val_size as i64],
        avoid_reusing: false,
    }
}

/// Go `makeMutRowUint64Column`.
fn make_mut_row_uint64_column(value: u64) -> Column {
    let mut column = new_mut_row_fixed_len_column(8);
    column.data.copy_from_slice(0..8, &value.to_ne_bytes());
    column
}

/// Go `makeMutRowBytesColumn`.
fn make_mut_row_bytes_column(bytes: &[u8]) -> Column {
    let mut column = new_mut_row_var_len_column(bytes.len());
    column.data.copy_from_slice(0..bytes.len(), bytes);
    column
}

/// The 40-byte `MyDecimal` behind a `Datum::Decimal`.
///
/// The datum carries the digit-string `Decimal`, so it reaches the raw cell
/// through its canonical text -- the same route [`crate::chunk::Chunk::append_datum`]
/// takes, and the same text `Row::get_datum` reads back out.
///
/// # Panics
/// Panics on a value too large for a `MyDecimal` buffer, rather than
/// truncating it silently into the cell.
fn my_decimal_of(decimal: &tidb_datatype::Decimal) -> MyDecimal {
    let text = decimal.to_string();
    let (value, error) = MyDecimal::from_string(text.as_bytes());
    assert!(
        error.is_none(),
        "MutRow: decimal {text} does not fit a MyDecimal cell ({error:?})"
    );
    value
}

/// Go `makeMutRowColumn`: a one-row column holding `value`.
fn make_mut_row_column(value: &Datum) -> Column {
    match value {
        // Go's `case nil` -- which `Datum.GetValue()` also produces for the
        // range sentinels and for raw bytes -- builds an empty bytes column
        // and then clears its null bit.
        Datum::Null | Datum::MinNotNull | Datum::MaxValue | Datum::Raw(_) => {
            let mut column = make_mut_row_bytes_column(&[]);
            column.null_bitmap[0] = 0;
            column
        }
        Datum::Int(i) => make_mut_row_uint64_column(*i as u64),
        Datum::UInt(u) => make_mut_row_uint64_column(*u),
        Datum::Real(f) => make_mut_row_uint64_column(f.to_bits()),
        Datum::Float32(f) => {
            let mut column = new_mut_row_fixed_len_column(4);
            column
                .data
                .copy_from_slice(0..4, &(*f as f32).to_bits().to_ne_bytes());
            column
        }
        Datum::String(s) => make_mut_row_bytes_column(s.bytes()),
        Datum::Bytes(b) => make_mut_row_bytes_column(b),
        Datum::BinaryLiteral(l) | Datum::Bit(l) => make_mut_row_bytes_column(l.as_bytes()),
        Datum::Decimal(d) => {
            let mut column = new_mut_row_fixed_len_column(MY_DECIMAL_STRUCT_SIZE as usize);
            column.data.copy_from_slice(
                0..MY_DECIMAL_STRUCT_SIZE as usize,
                &my_decimal_of(d).to_raw_bytes(),
            );
            column
        }
        Datum::Time(t) => {
            let mut column = new_mut_row_fixed_len_column(SIZE_TIME as usize);
            column
                .data
                .copy_from_slice(0..SIZE_TIME as usize, &t.go_raw().to_ne_bytes());
            column
        }
        Datum::Json(j) => {
            let data_len = j.value().len() + 1;
            let mut column = new_mut_row_var_len_column(data_len);
            column.data.set(0, j.type_code());
            column.data.copy_from_slice(1..data_len, j.value());
            column
        }
        Datum::VectorFloat32(v) => make_mut_row_bytes_column(&v.serialize()),
        Datum::Duration(d) => {
            let mut column = new_mut_row_fixed_len_column(8);
            column
                .data
                .copy_from_slice(0..8, &d.nanoseconds().to_ne_bytes());
            column
        }
        Datum::Enum(e, _) => make_name_value_column(e.name_bytes(), e.value()),
        Datum::Set(s, _) => make_name_value_column(s.name_bytes(), s.value()),
    }
}

/// Go's `types.Enum`/`types.Set` arm of `makeMutRowColumn`: the 8-byte value
/// followed by the element name.
fn make_name_value_column(name: &[u8], value: u64) -> Column {
    let mut column = new_mut_row_var_len_column(name.len() + 8);
    column.data.copy_from_slice(0..8, &value.to_ne_bytes());
    column.data.copy_from_slice(8..8 + name.len(), name);
    column
}

/// Go `zeroValForType` fused with the `makeMutRowColumn` call that always
/// follows it: the zero-valued one-row column for `field_type`.
///
/// The fusion is what lets the `NewDecimal` arm stay faithful. Go's zero
/// decimal is `types.NewDecFromInt(0)`, whose `digitsInt` is 9 (one whole
/// base-1e9 word) -- NOT the 1 that parsing the text `"0"` produces. Routing it
/// through a `Datum::Decimal` would lose that, because a `Datum` carries the
/// digit string, not the 40-byte struct.
fn zero_column_for_type(field_type: &FieldType) -> Column {
    use FieldTypeCode as C;
    match field_type.code() {
        C::Float => make_mut_row_column(&Datum::Float32(0.0)),
        C::Double => make_mut_row_column(&Datum::Real(0.0)),
        C::Tiny | C::Short | C::Int24 | C::Long | C::LongLong | C::Year => {
            if field_type.is_unsigned() {
                make_mut_row_column(&Datum::UInt(0))
            } else {
                make_mut_row_column(&Datum::Int(0))
            }
        }
        // Go returns `""` here and `[]byte{}` for the blob family; both reach
        // `makeMutRowBytesColumn` with an empty slice.
        C::String
        | C::VarString
        | C::Varchar
        | C::Blob
        | C::TinyBlob
        | C::MediumBlob
        | C::LongBlob
        // `types.BinaryLiteral{}` -- an empty, NOT-NULL cell.
        | C::Bit => make_mut_row_bytes_column(&[]),
        C::Duration => make_mut_row_column(&Datum::Duration(
            MySqlDuration::from_nanoseconds(0, 0).expect("zero duration"),
        )),
        // Go `types.NewDecFromInt(0)`.
        C::NewDecimal => {
            let mut column = new_mut_row_fixed_len_column(MY_DECIMAL_STRUCT_SIZE as usize);
            column.data.copy_from_slice(
                0..MY_DECIMAL_STRUCT_SIZE as usize,
                &MyDecimal::from_int(0).to_raw_bytes(),
            );
            column
        }
        C::Date | C::Datetime | C::Timestamp => {
            let kind = match field_type.code() {
                C::Date => TimeType::Date,
                C::Timestamp => TimeType::Timestamp,
                _ => TimeType::DateTime,
            };
            make_mut_row_column(&Datum::Time(
                Time::new(tidb_datatype::CoreTime::default(), kind, 0).expect("zero time"),
            ))
        }
        C::Set => make_name_value_column(b"", 0),
        C::Enum => make_name_value_column(b"", 0),
        // Go `types.CreateBinaryJSON(nil)`: the JSON literal `null`.
        C::Json => make_mut_row_column(&Datum::Json(
            BinaryJSON::parse("null").expect("the JSON null literal"),
        )),
        C::VectorFloat32 => {
            make_mut_row_column(&Datum::VectorFloat32(VectorFloat32::init(0)))
        }
        // Go's `default: return nil` -- a NULL cell.
        _ => make_mut_row_column(&Datum::Null),
    }
}

/// Go's `if len(col.data) < n { col.data = make([]byte, n) }` growth guard,
/// shared by every fixed-width arm of `SetDatum`.
fn grow_data_to(column: &mut Column, n: usize) {
    if column.data.len() < n {
        column.data = SharedBytes::zeros(n);
    }
}

/// Go's `binary.LittleEndian.PutUint64(col.data, x)` in `SetValue`.
///
/// # Panics
/// Panics on a buffer shorter than 8 bytes, as Go's `PutUint64` does.
fn put_uint64(column: &mut Column, value: u64) {
    column.data.copy_from_slice(0..8, &value.to_le_bytes());
}

/// Go `setMutRowBytes`: fit `bytes` into the cell, reslicing the existing
/// buffer when it is big enough and otherwise allocating a fresh one (which,
/// as in Go, also replaces the 1-byte null bitmap with a cleared one -- every
/// caller sets the not-null bit afterwards).
fn set_mut_row_bytes(column: &mut Column, bytes: &[u8]) {
    if column.data.len() >= bytes.len() {
        column.data.truncate(bytes.len());
    } else {
        column.data = SharedBytes::zeros(bytes.len());
        column.null_bitmap = vec![0];
    }
    column.data.copy_from_slice(0..bytes.len(), bytes);
    column.offsets[1] = bytes.len() as i64;
}

/// Go `setMutRowNameValue`.
fn set_mut_row_name_value(column: &mut Column, name: &[u8], value: u64) {
    let data_len = name.len() + 8;
    if column.data.len() >= data_len {
        column.data.truncate(data_len);
    } else {
        column.data = SharedBytes::zeros(data_len);
        column.null_bitmap = vec![0];
    }
    column.data.copy_from_slice(0..8, &value.to_le_bytes());
    column.data.copy_from_slice(8..data_len, name);
    column.offsets[1] = data_len as i64;
}

/// Go `setMutRowJSON`: the type code byte followed by the value bytes.
fn set_mut_row_json(column: &mut Column, value: &BinaryJSON) {
    let data_len = value.value().len() + 1;
    if column.data.len() >= data_len {
        column.data.truncate(data_len);
    } else {
        column.data = SharedBytes::zeros(data_len);
        column.null_bitmap = vec![0];
    }
    column.data.set(0, value.type_code());
    column.data.copy_from_slice(1..data_len, value.value());
    column.offsets[1] = data_len as i64;
}

#[cfg(test)]
mod tests {
    use super::*;
    use tidb_datatype::{
        BinaryLiteral, Collation, CoreTime, Decimal, FieldTypeCode as C, MysqlEnum, MysqlSet,
    };

    /// Every line was printed by the REAL Go `pkg/util/chunk` MutRow; see
    /// `rust/difftests/chunk-tests/fixtures/generate_mutrow_vectors.go` for the
    /// generator that reproduces it.
    const GO_VECTORS: &str =
        include_str!("../../../difftests/chunk-tests/fixtures/mutrow_vectors.tsv");

    fn hex(bytes: impl AsRef<[u8]>) -> String {
        bytes.as_ref().iter().map(|b| format!("{b:02x}")).collect()
    }

    /// The Go fixture's lines for one case, in column order.
    fn go_case(name: &str) -> Vec<(bool, bool, String)> {
        let rows: Vec<_> = GO_VECTORS
            .lines()
            .filter_map(|line| {
                let mut parts = line.split('\t');
                (parts.next()? == name).then(|| {
                    let _col = parts.next().expect("col idx");
                    let is_null = parts.next().expect("isnull") == "1";
                    let is_fixed = parts.next().expect("isfixed") == "1";
                    (is_null, is_fixed, parts.next().expect("hex").to_owned())
                })
            })
            .collect();
        assert!(!rows.is_empty(), "no Go fixture lines for case {name}");
        rows
    }

    /// A single-field Go fixture line, for the `_typed` read-back pins.
    fn go_typed(name: &str) -> String {
        go_case(name).pop().expect("one line").2
    }

    /// Asserts every column of `mut_row` against the Go image of `name`.
    #[track_caller]
    fn assert_matches_go(name: &str, mut_row: &MutRow) {
        let expected = go_case(name);
        assert_eq!(expected.len(), mut_row.len(), "{name}: column count");
        let row = mut_row.to_row();
        for (i, (is_null, is_fixed, bytes)) in expected.iter().enumerate() {
            assert_eq!(row.is_null(i), *is_null, "{name} col {i}: nullity");
            assert_eq!(
                mut_row.chunk.column(i).is_fixed(),
                *is_fixed,
                "{name} col {i}: fixed-vs-variable shape"
            );
            assert_eq!(hex(row.get_raw(i)), *bytes, "{name} col {i}: cell bytes");
        }
    }

    fn ft(code: C) -> FieldType {
        FieldType::new(code)
    }

    fn a_time() -> Time {
        Time::new(
            CoreTime::from_date(2024, 3, 17, 4, 5, 6, 789_000),
            TimeType::DateTime,
            6,
        )
        .expect("a valid datetime")
    }

    fn a_duration() -> MySqlDuration {
        MySqlDuration::from_nanoseconds(12_345_000_000_000, 0).expect("3h25m45s")
    }

    fn dec(text: &str) -> Datum {
        Datum::Decimal(Decimal::from_literal(text))
    }

    fn json(text: &str) -> Datum {
        Datum::Json(BinaryJSON::parse(text).expect("valid JSON"))
    }

    fn string(text: &str) -> Datum {
        let mut d = Datum::Null;
        d.set_string(text.as_bytes().to_vec(), Collation::Utf8Mb4Bin);
        d
    }

    /// The Go generator's `theTypes()`, in the same order.
    fn the_types() -> Vec<FieldType> {
        vec![
            ft(C::Float),
            ft(C::Double),
            ft(C::Tiny),
            ft(C::LongLong),
            ft(C::LongLong).with_unsigned(true),
            ft(C::Year),
            ft(C::Varchar),
            ft(C::String),
            ft(C::VarString),
            ft(C::Blob),
            ft(C::TinyBlob),
            ft(C::MediumBlob),
            ft(C::LongBlob),
            ft(C::Duration),
            ft(C::NewDecimal),
            ft(C::Date),
            ft(C::Datetime),
            ft(C::Timestamp),
            ft(C::Bit),
            ft(C::Set),
            ft(C::Enum),
            ft(C::Json),
            ft(C::VectorFloat32),
            ft(C::Geometry),
        ]
    }

    /// The Go generator's `theDatums()`, in the same order.
    fn the_datums() -> Vec<Datum> {
        vec![
            Datum::Null,
            Datum::Int(-1),
            Datum::Int(42),
            Datum::UInt(u64::MAX),
            Datum::Real(3.5),
            Datum::Float32(1.5),
            string("hello"),
            string(""),
            Datum::Bytes(vec![0x00, 0x01, 0xff]),
            Datum::BinaryLiteral(BinaryLiteral::from(vec![0xab, 0xcd])),
            Datum::Duration(a_duration()),
            dec("123.456"),
            Datum::Time(a_time()),
            Datum::Enum(MysqlEnum::new("abc", 2), Collation::Utf8Mb4Bin),
            Datum::Set(MysqlSet::new("a,b", 3), Collation::Utf8Mb4Bin),
            json(r#"{"a":1}"#),
            Datum::VectorFloat32(VectorFloat32::must_create(vec![1.5, -2.25, 3.0])),
            Datum::MinNotNull,
            Datum::MaxValue,
        ]
    }

    /// `MutRowFromDatums` builds the byte image Go's `makeMutRowColumn` does,
    /// for every kind -- including the NULL/sentinel arms, which come out as
    /// an empty NULL cell rather than as nothing at all.
    #[test]
    fn from_datums_matches_go() {
        assert_matches_go("from_datums", &MutRow::from_datums(&the_datums()));
    }

    /// `MutRowFromTypes` hits every `zeroValForType` arm. The `NewDecimal`
    /// entry is the one that catches a shortcut: Go's zero decimal is
    /// `NewDecFromInt(0)`, whose `digitsInt` is 9, not the 1 that parsing "0"
    /// would give.
    #[test]
    fn from_types_matches_go() {
        assert_matches_go("from_types", &MutRow::from_types(&the_types()));
    }

    /// The field types the `set_datum` sweep is built on.
    fn set_datum_types() -> Vec<FieldType> {
        vec![
            ft(C::Varchar),
            ft(C::LongLong),
            ft(C::Double),
            ft(C::Datetime),
            ft(C::Duration),
            ft(C::NewDecimal),
            ft(C::Json),
            ft(C::Enum),
            ft(C::Set),
            ft(C::Blob),
            ft(C::Float),
            ft(C::Bit),
            ft(C::VectorFloat32),
        ]
    }

    #[test]
    fn set_datum_matches_go() {
        let datums = vec![
            string("a longer string than the zero value"),
            Datum::Int(-7),
            Datum::Real(-0.125),
            Datum::Time(a_time()),
            Datum::Duration(a_duration()),
            dec("-9876.54321"),
            json(r#"[1,"x"]"#),
            Datum::Enum(MysqlEnum::new("zz", 7), Collation::Utf8Mb4Bin),
            Datum::Set(MysqlSet::new("a", 1), Collation::Utf8Mb4Bin),
            Datum::Bytes(vec![0xde, 0xad, 0xbe, 0xef]),
            Datum::Float32(-2.5),
            Datum::BinaryLiteral(BinaryLiteral::from(vec![0x01, 0x02, 0x03])),
            // Go's `types.InitVectorFloat32(3)`: three zero elements.
            Datum::VectorFloat32(VectorFloat32::init(3)),
        ];
        let mut mut_row = MutRow::from_types(&set_datum_types());
        mut_row.set_datums(&datums);
        assert_matches_go("set_datum", &mut_row);

        // `cleanColOfMutRow`: a var-length cell reads back EMPTY because the
        // offsets are zeroed, while a fixed cell keeps its stale bytes.
        mut_row.set_datums(&vec![Datum::Null; datums.len()]);
        assert_matches_go("set_datum_null", &mut_row);
    }

    /// Go `TestIssue29947`: `SetDatum(NULL)` must mark the cell null and zero
    /// its offsets WITHOUT reallocating either buffer.
    ///
    /// The bug it guards is a NULL write that drops the typed buffer, after
    /// which the next non-NULL write to that column lands in a wrongly sized
    /// cell. Reallocation is invisible to a value-level assertion, so the
    /// buffer LENGTHS are what this pins.
    #[test]
    fn set_datum_null_does_not_reallocate() {
        let types = the_types();
        let mut mut_row = MutRow::from_types(&types);
        let before: Vec<_> = mut_row
            .chunk
            .column_slots()
            .iter()
            .map(|slot| {
                let column = slot.read();
                (column.data.clone(), column.elem_buf.clone())
            })
            .collect();
        for (i, (data, elem_buf)) in before.iter().enumerate() {
            mut_row.set_datum(i, &Datum::Null);
            let column = mut_row.chunk.column(i);
            assert!(column.is_null(0), "col {i}: must read back NULL");
            assert!(
                column.offsets.iter().all(|&off| off == 0),
                "col {i}: offsets must be zeroed"
            );
            assert_eq!(&column.data, data, "col {i}: data buffer changed");
            assert_eq!(&column.elem_buf, elem_buf, "col {i}: elem buffer changed");
        }
    }

    /// `setMutRowBytes`' grow / reslice / regrow rule.
    #[test]
    fn set_bytes_grow_and_shrink_matches_go() {
        let mut mut_row = MutRow::from_types(&[ft(C::Varchar)]);
        mut_row.set_datum(0, &string("0123456789abcdef"));
        assert_matches_go("bytes_grow", &mut_row);
        mut_row.set_datum(0, &string("xy"));
        assert_matches_go("bytes_shrink", &mut_row);
        mut_row.set_datum(0, &string("0123456789abcdefghij"));
        assert_matches_go("bytes_regrow", &mut_row);
    }

    /// `SetDatum`'s fixed-width growth on a column built VARIABLE-length.
    ///
    /// The raw cell stays EMPTY -- Go grows `col.data` but never touches
    /// `offsets[1]` -- while the typed getter reads the value back. Both
    /// halves are pinned, because a port that "helpfully" fixed the offset
    /// would pass the second assertion and fail the first.
    #[test]
    fn set_datum_grows_a_var_len_column_like_go() {
        /// One grow case: its fixture name, the datum written, and the typed
        /// getter that reads it back.
        type GrowCase = (&'static str, Datum, fn(Row<'_>) -> String);
        let cases: Vec<GrowCase> = vec![
            ("grow_int", Datum::Int(0x0102_0304_0506_0708), |r| {
                r.get_int64(0).to_string()
            }),
            ("grow_float32", Datum::Float32(7.25), |r| {
                r.get_float32(0).to_string()
            }),
            ("grow_time", Datum::Time(a_time()), |r| {
                r.get_time(0).to_string()
            }),
            ("grow_duration", Datum::Duration(a_duration()), |r| {
                r.get_duration(0, 0).nanoseconds().to_string()
            }),
            ("grow_decimal", dec("1.5"), |r| {
                String::from_utf8(r.get_my_decimal(0).to_string_bytes()).expect("ASCII")
            }),
        ];
        for (name, datum, read) in cases {
            let mut mut_row = MutRow::from_types(&[ft(C::Varchar)]);
            mut_row.set_datum(0, &datum);
            assert_matches_go(name, &mut_row);
            assert_eq!(
                read(mut_row.to_row()),
                go_typed(&format!("{name}_typed")),
                "{name}: typed read-back"
            );
        }
    }

    /// `SetValue`: the in-place sibling of `SetDatum`, plus the nil arm that
    /// leaves a cleaned cell NULL.
    #[test]
    fn set_value_matches_go() {
        let types = vec![
            ft(C::LongLong),
            ft(C::LongLong),
            ft(C::Double),
            ft(C::Float),
            ft(C::Varchar),
            ft(C::Blob),
            ft(C::Duration),
            ft(C::NewDecimal),
            ft(C::Datetime),
            ft(C::Enum),
            ft(C::Set),
            ft(C::Json),
            ft(C::Bit),
        ];
        let mut mut_row = MutRow::from_types(&types);
        mut_row.set_values(&[
            Datum::Int(-3),
            Datum::UInt(9),
            Datum::Real(2.5),
            Datum::Float32(-1.25),
            string("str"),
            Datum::Bytes(vec![0x10, 0x20]),
            Datum::Duration(a_duration()),
            dec("0.001"),
            Datum::Time(a_time()),
            Datum::Enum(MysqlEnum::new("e", 4), Collation::Utf8Mb4Bin),
            Datum::Set(MysqlSet::new("s", 5), Collation::Utf8Mb4Bin),
            json("true"),
            Datum::BinaryLiteral(BinaryLiteral::from(vec![0x7f])),
        ]);
        assert_matches_go("set_value", &mut_row);

        mut_row.set_value(4, &Datum::Null);
        mut_row.set_value(0, &Datum::Null);
        assert_matches_go("set_value_nil", &mut_row);
    }

    #[test]
    fn enum_and_set_mutations_preserve_non_utf8_name_bytes() {
        let mut mut_row = MutRow::from_datums(&[
            Datum::Enum(MysqlEnum::new(vec![0xff], 1), Collation::Binary),
            Datum::Set(MysqlSet::new(vec![0xfe], 1), Collation::Binary),
        ]);
        assert_eq!(mut_row.to_row().get_enum(0).name_bytes(), &[0xff]);
        assert_eq!(mut_row.to_row().get_set(1).name_bytes(), &[0xfe]);

        mut_row.set_datum(
            0,
            &Datum::Enum(MysqlEnum::new(vec![0xfe], 2), Collation::Binary),
        );
        mut_row.set_value(
            1,
            &Datum::Set(MysqlSet::new(vec![0xff], 2), Collation::Binary),
        );
        assert_eq!(mut_row.to_row().get_enum(0).name_bytes(), &[0xfe]);
        assert_eq!(mut_row.to_row().get_set(1).name_bytes(), &[0xff]);
    }

    /// `SetRow` copies a real chunk row in, NULL cell included, and `Clone`
    /// is deep.
    #[test]
    fn set_row_and_clone_match_go() {
        let types = vec![
            ft(C::LongLong),
            ft(C::Varchar),
            ft(C::Datetime),
            ft(C::Varchar),
        ];
        let mut chunk = Chunk::new_with_capacity(&types, 4);
        chunk.append_int64(0, 1234);
        chunk.append_string(1, "row-source-value");
        chunk.append_time(2, a_time());
        chunk.append_null(3);

        let mut mut_row = MutRow::from_types(&types);
        mut_row.set_row(chunk.get_row(0));
        assert_matches_go("set_row", &mut_row);

        let cloned = mut_row.clone();
        assert_matches_go("set_row_clone", &cloned);
        // Deep, not aliased: overwriting the original leaves the clone alone.
        mut_row.set_datum(1, &string("overwritten"));
        assert_matches_go("set_row_clone", &cloned);
    }

    /// `ShallowCopyPartialRow` at a column offset, over a fixed cell, a
    /// variable cell, and a NULL one.
    #[test]
    fn shallow_copy_partial_row_matches_go() {
        let source_types = vec![ft(C::LongLong), ft(C::Varchar), ft(C::Varchar)];
        let mut chunk = Chunk::new_with_capacity(&source_types, 4);
        chunk.append_int64(0, -5);
        chunk.append_string(1, "shallow");
        chunk.append_null(2);
        chunk.append_int64(0, 6);
        chunk.append_string(1, "second");
        chunk.append_string(2, "notnull");

        let dest_types = vec![
            ft(C::Varchar),
            ft(C::LongLong),
            ft(C::Varchar),
            ft(C::Varchar),
        ];
        let mut mut_row = MutRow::from_types(&dest_types);
        mut_row.set_datum(0, &string("kept"));
        mut_row.shallow_copy_partial_row(1, &mut chunk, 1);
        assert_matches_go("shallow_copy", &mut_row);

        let mut second = MutRow::from_types(&dest_types);
        second.shallow_copy_partial_row(1, &mut chunk, 0);
        assert_matches_go("shallow_copy_null", &second);

        chunk.column_mut(0).with_int64s_mut(|values| values[0] = 42);
        assert_eq!(second.to_row().get_int64(1), 42);

        chunk
            .column_mut(1)
            .with_cell_bytes_mut(0, |cell| cell.copy_from_slice(b"SHALLOW"));
        assert_eq!(second.to_row().get_bytes(2), b"SHALLOW");

        chunk.column_mut(1).reset();
        chunk.column_mut(1).append_string("RESET!!");
        assert_eq!(second.to_row().get_bytes(2), b"RESET!!");

        let alias_row = second.to_row();
        let guarded_cell = alias_row.get_bytes(2);
        chunk.column_mut(1).append_bytes(guarded_cell.as_ref());
        assert_eq!(guarded_cell, b"RESET!!");
        assert_eq!(chunk.get_row(1).get_bytes(1), b"RESET!!");
        drop(guarded_cell);

        let grown_source = "source growth detaches this backing".repeat(2);
        chunk.column_mut(1).reset();
        chunk.column_mut(1).append_string(&grown_source);
        assert_eq!(chunk.get_row(0).get_bytes(1), grown_source.as_bytes());
        assert_eq!(second.to_row().get_bytes(2), b"RESET!!");

        second.set_datum(2, &string("a value that forces detached storage"));
        assert_eq!(chunk.get_row(0).get_bytes(1), grown_source.as_bytes());
    }
}

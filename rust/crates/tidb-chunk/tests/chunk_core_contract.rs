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

//! Public boundary tests for accepted `pkg/util/chunk/chunk.go` and `row.go`.

use tidb_chunk::chunk::{Chunk, ZERO_CAPACITY};
use tidb_chunk::row::Row;
use tidb_datatype::{
    BinaryJSON, CoreTime, Datum, FieldType, FieldTypeCode, FieldTypeFlags, GoString, MyDecimal,
    MySqlDuration, MysqlEnum, MysqlSet, Time, TimeType, VectorFloat32,
};

#[test]
fn public_zero_capacity_is_the_source_builder_sentinel() {
    assert_eq!(ZERO_CAPACITY, 0);

    assert!(Row::empty().chunk().is_none());
    let constructed = Chunk::new(&[], 0, 0);
    assert!(constructed.get_row(0).chunk().is_some());
}

#[test]
fn copy_construct_of_zero_value_becomes_initialized_empty() {
    let mut source = Chunk::default();
    source.set_num_virtual_rows(3);

    let mut copied = source.copy_construct();
    assert_eq!(copied.num_rows(), 3);

    // Go CopyConstruct assigns make([]*Column, len(nil)). The resulting
    // zero-length slice is non-nil, so Reset clears virtual rows.
    copied.reset();
    assert_eq!(copied.num_rows(), 0);

    // The initialized empty schema also keeps renewal capacity rather than
    // taking the literal nil-column early return.
    let renewed = copied.renew_with_capacity(7, 11);
    assert_eq!(renewed.capacity(), 7);
    assert_eq!(renewed.required_rows(), 11);
}

#[test]
fn chunk_core_state_append_identity_and_transform_contract() {
    let fields = [
        FieldType::new(FieldTypeCode::LongLong),
        FieldType::new(FieldTypeCode::VarString),
    ];
    let mut source = tidb_chunk::chunk::Chunk::new(&fields, 8, 3);
    assert_eq!(source.capacity(), 3);
    assert_eq!(source.required_rows(), 3);
    assert_eq!(source.num_cols(), 2);
    assert!(source.memory_usage() > 0);

    source.set_required_rows(2, 3);
    assert_eq!(source.required_rows(), 2);
    source.set_required_rows(0, 3);
    assert_eq!(source.required_rows(), 3);
    source.set_incomplete_chunk(true);
    source.set_num_virtual_rows(4);
    assert_eq!(source.num_rows(), 4);
    source.set_incomplete_chunk(false);
    source.set_num_virtual_rows(0);

    for (value, text) in [(10, b"ten".as_slice()), (20, b"twenty"), (30, b"thirty")] {
        source.append_int64(0, value);
        source.append_bytes(1, text);
    }
    assert!(source.is_full());

    source.set_sel(Some(vec![2, 0]));
    assert_eq!(source.num_rows(), 2);
    assert_eq!(source.get_row(0).get_int64(0), 30);
    assert_eq!(source.get_row(1).get_bytes(1).as_ref(), b"ten");

    let selected = source.copy_construct_sel();
    assert!(selected.sel().is_none());
    assert_eq!(selected.num_rows(), 2);
    assert_eq!(selected.get_row(0).get_int64(0), 30);
    assert_eq!(selected.get_row(1).get_int64(0), 10);

    let mut physical_range = Chunk::new_with_capacity(&fields, 4);
    physical_range.append_range_from(&source, 0, 2);
    assert_eq!(physical_range.get_row(0).get_int64(0), 10);
    assert_eq!(physical_range.get_row(1).get_int64(0), 20);

    let logical_rows = [source.get_row(0), source.get_row(1)];
    let projected_fields = [
        FieldType::new(FieldTypeCode::VarString),
        FieldType::new(FieldTypeCode::LongLong),
    ];
    let mut projected = Chunk::new_with_capacity(&projected_fields, 4);
    assert_eq!(
        projected.append_rows_by_col_idxs(&logical_rows, Some(&[1, 0])),
        4
    );
    assert_eq!(projected.get_row(0).get_bytes(0).as_ref(), b"thirty");
    assert_eq!(projected.get_row(0).get_int64(1), 30);

    source.reconstruct();
    assert!(source.sel().is_none());
    assert_eq!(source.get_row(0).get_int64(0), 30);
    source.truncate_to(1);
    assert_eq!(source.num_rows(), 1);
    source.grow_and_reset(8);
    assert_eq!(source.num_rows(), 0);

    let int_fields = [
        FieldType::new(FieldTypeCode::LongLong),
        FieldType::new(FieldTypeCode::LongLong),
    ];
    let mut aliases = Chunk::new_with_capacity(&int_fields, 2);
    aliases.append_int64(0, 7);
    aliases.append_int64(1, 9);
    aliases.make_ref(0, 1);
    assert!(aliases.columns_share_identity(0, &aliases, 1));
    let pruned = aliases.prune(&[1, 0]);
    assert!(pruned.columns_share_identity(0, &pruned, 1));

    let same = aliases.column_handle(0);
    assert!(aliases.set_col(1, same).is_none());
    let mut other = Chunk::new_with_capacity(&int_fields[..1], 2);
    other.append_int64(0, 42);
    Chunk::make_ref_to(&mut aliases, 1, &mut other, 0).unwrap();
    assert!(aliases.columns_share_identity(1, &other, 0));

    aliases.set_sel(Some(vec![0]));
    assert!(Chunk::make_ref_to(&mut aliases, 0, &mut other, 0).is_err());
    aliases.set_sel(None);
    aliases.swap_column(0, 1).unwrap();
    aliases.swap_columns(&mut other);
}

#[test]
fn packed_raw_cell_contract() {
    let raw_fields = [
        FieldType::new(FieldTypeCode::LongLong),
        FieldType::new(FieldTypeCode::VarString),
    ];
    let mut raw = Chunk::new_with_capacity(&raw_fields, 1);
    let fixed = 17_i64.to_ne_bytes();
    assert_eq!(
        tidb_chunk::column::append_cell_from_raw_data(&mut raw.column_mut(0), &fixed, 0),
        fixed.len()
    );
    let mut variable = 3_u32.to_ne_bytes().to_vec();
    variable.extend_from_slice(b"raw");
    assert_eq!(
        tidb_chunk::column::append_cell_from_raw_data(&mut raw.column_mut(1), &variable, 0),
        variable.len()
    );
    assert_eq!(raw.get_row(0).get_int64(0), 17);
    assert_eq!(raw.get_row(0).get_bytes(1).as_ref(), b"raw");
}

#[test]
fn row_core_datum_contract() {
    let fields = [
        FieldType::new(FieldTypeCode::LongLong).with_added_flags(FieldTypeFlags::UNSIGNED),
        FieldType::new(FieldTypeCode::VarString),
        FieldType::new(FieldTypeCode::Year),
        FieldType::new(FieldTypeCode::Double),
    ];
    let mut chunk = Chunk::new_with_capacity(&fields, 2);
    chunk.append_uint64(0, u64::MAX);
    chunk.append_bytes(1, &[0xff, b'x']);
    chunk.append_int64(2, -1);
    chunk.append_float64(3, 3.5);
    for column in 0..fields.len() {
        chunk.append_null(column);
    }

    let row = chunk.get_row(0);
    assert_eq!(Row::chunk(&row).map(Chunk::num_cols), Some(4));
    assert_eq!(row.idx(), 0);
    assert_eq!(row.len(), 4);
    assert!(!row.is_empty());
    assert_eq!(row.get_uint64(0), u64::MAX);
    assert_eq!(row.get_string(1).as_bytes(), &[0xff, b'x']);
    assert_eq!(row.get_raw_len(2), 8);
    assert_eq!(row.get_float64(3), 3.5);

    let datums = row.get_datum_row(&fields);
    assert_eq!(datums[0], Datum::UInt(u64::MAX));
    assert_eq!(datums[2], Datum::Int(-1));
    assert_eq!(datums[3], Datum::Real(3.5));

    let mut reused = Datum::Int(99);
    tidb_chunk::row::Row::datum_with_buffer(&chunk.get_row(1), 0, &fields[0], &mut reused);
    assert_eq!(reused, Datum::Null);
    Row::datum_with_buffer(&row, 3, &fields[3], &mut reused);
    assert_eq!(reused, Datum::Real(3.5));
}

#[test]
fn copied_row_owns_one_independent_row() {
    let fields = [
        FieldType::new(FieldTypeCode::LongLong),
        FieldType::new(FieldTypeCode::VarString),
    ];
    let mut source = Chunk::new_with_capacity(&fields, 4);
    source.append_int64(0, 7);
    source.append_bytes(1, b"first");
    source.append_int64(0, 42);
    source.append_bytes(1, b"second");

    let copied = tidb_chunk::row::Row::copy_construct(&source.get_row(1));
    assert_eq!(copied.chunk().num_rows(), 1);
    assert_eq!(copied.chunk().capacity(), 1);
    assert_eq!(copied.chunk().required_rows(), 1);
    assert_eq!(copied.as_row().get_int64(0), 42);
    assert_eq!(copied.as_row().get_bytes(1).as_ref(), b"second");

    source.reset();
    source.append_int64(0, 99);
    source.append_bytes(1, b"replacement");
    assert_eq!(copied.as_row().get_int64(0), 42);
    assert_eq!(copied.as_row().get_bytes(1).as_ref(), b"second");

    let copied_chunk = copied.into_chunk();
    assert_eq!(copied_chunk.get_row(0).get_int64(0), 42);
}

#[test]
fn row_and_chunk_text_match_the_accepted_source_oracle() {
    let fields = [
        FieldType::new(FieldTypeCode::Float),
        FieldType::new(FieldTypeCode::Double),
        FieldType::new(FieldTypeCode::String),
        FieldType::new(FieldTypeCode::Date),
        FieldType::new(FieldTypeCode::LongLong),
    ];
    let zero_date = Time::new(CoreTime::default(), TimeType::Date, 0).unwrap();
    let zero_datetime = Time::new(CoreTime::default(), TimeType::DateTime, 0).unwrap();
    let mut chunk = Chunk::new_with_capacity(&fields, 2);

    chunk.append_float32(0, 1.0);
    chunk.append_float64(1, 1.0);
    chunk.append_string(2, "1");
    chunk.append_time(3, zero_date);
    chunk.append_int64(4, 1);

    chunk.append_float32(0, 2.0);
    chunk.append_float64(1, 2.0);
    chunk.append_string(2, "2");
    chunk.append_time(3, zero_datetime);
    chunk.append_int64(4, 2);

    assert_eq!(
        chunk.get_row(0).to_string(&fields).as_bytes(),
        b"1, 1, 1, 0000-00-00, 1"
    );
    assert_eq!(
        tidb_chunk::chunk::Chunk::to_string(&chunk, &fields).as_bytes(),
        b"1, 1, 1, 0000-00-00, 1\n2, 2, 2, 0000-00-00 00:00:00, 2\n"
    );
}

#[test]
fn row_text_covers_every_eval_type_and_preserves_source_bytes() {
    let fields = [
        FieldType::new(FieldTypeCode::LongLong),
        FieldType::new(FieldTypeCode::VarString),
        FieldType::new(FieldTypeCode::Enum),
        FieldType::new(FieldTypeCode::Set),
        FieldType::new(FieldTypeCode::Datetime),
        FieldType::new(FieldTypeCode::Timestamp),
        FieldType::new(FieldTypeCode::NewDecimal),
        FieldType::new(FieldTypeCode::Duration).with_decimal(3),
        FieldType::new(FieldTypeCode::Json),
        FieldType::new(FieldTypeCode::Float),
        FieldType::new(FieldTypeCode::Double),
        FieldType::new(FieldTypeCode::VectorFloat32),
    ];
    let mut chunk = Chunk::new_with_capacity(&fields, 2);
    let zero_datetime = Time::new(CoreTime::default(), TimeType::DateTime, 0).unwrap();
    let decimal = MyDecimal::from_string(b"-12.340").0;
    let duration = MySqlDuration::new(1, 2, 3, 450_000, 3).unwrap();
    let json = BinaryJSON::parse(r#"{"a":1}"#).unwrap();
    let vector = VectorFloat32::must_create(vec![-1.25, 0.0, 3.5]);

    chunk.append_int64(0, -7);
    chunk.append_bytes(1, &[0xff, 0x00, b'x']);
    chunk.append_enum(2, &MysqlEnum::new(GoString::from(vec![0xfe, b'e']), 1));
    chunk.append_set(3, &MysqlSet::new(GoString::from(vec![b'a', 0x00, b'b']), 3));
    chunk.append_time(4, zero_datetime);
    chunk.append_time(5, zero_datetime);
    chunk.append_my_decimal(6, &decimal);
    chunk.append_duration(7, duration);
    chunk.append_json(8, &json);
    chunk.append_float32(9, f32::INFINITY);
    chunk.append_float64(10, f64::NAN);
    chunk.append_vector_float32(11, &vector);

    for column in 0..fields.len() {
        chunk.append_null(column);
    }

    let mut expected = Vec::new();
    expected.extend_from_slice(b"-7, ");
    expected.extend_from_slice(&[0xff, 0x00, b'x']);
    expected.extend_from_slice(b", ");
    expected.extend_from_slice(&[0xfe, b'e']);
    expected.extend_from_slice(b", a\0b, 0000-00-00 00:00:00, 0000-00-00 00:00:00");
    expected.extend_from_slice(b", -12.340, 01:02:03.450, {\"a\": 1}, +Inf, NaN, [-1.25,0,3.5]");
    assert_eq!(
        tidb_chunk::row::Row::to_string(&chunk.get_row(0), &fields).as_bytes(),
        expected
    );

    let null_row = std::iter::repeat_n("NULL", fields.len())
        .collect::<Vec<_>>()
        .join(", ");
    assert_eq!(
        chunk.get_row(1).to_string(&fields).as_bytes(),
        null_row.as_bytes()
    );

    chunk.set_sel(Some(vec![1, 0]));
    let mut expected_chunk = null_row.into_bytes();
    expected_chunk.push(b'\n');
    expected_chunk.extend_from_slice(&expected);
    expected_chunk.push(b'\n');
    assert_eq!(chunk.to_string(&fields).as_bytes(), expected_chunk);
}

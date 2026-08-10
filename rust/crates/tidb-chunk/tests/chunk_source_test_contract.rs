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

//! Public semantic boundary for accepted `pkg/util/chunk/chunk_test.go`.

use tidb_chunk::chunk::Chunk;
use tidb_datatype::{BinaryJSON, Datum, FieldType, FieldTypeCode, MyDecimal};

fn source_fields() -> Vec<FieldType> {
    vec![
        FieldType::new(FieldTypeCode::LongLong),
        FieldType::new(FieldTypeCode::LongLong),
        FieldType::new(FieldTypeCode::VarString),
        FieldType::new(FieldTypeCode::VarString),
        FieldType::new(FieldTypeCode::NewDecimal),
        FieldType::new(FieldTypeCode::Json),
    ]
}

fn source_rows(fields: &[FieldType]) -> Chunk {
    let mut chunk = Chunk::new_with_capacity(fields, 10);
    for row in 0..10_i64 {
        let text = format!("{row}.12345");
        chunk.append_null(0);
        chunk.append_int64(1, row);
        chunk.append_string(2, &text);
        chunk.append_bytes(3, text.as_bytes());
        chunk.append_my_decimal(4, &MyDecimal::from_string(text.as_bytes()).0);
        chunk.append_json(
            5,
            &BinaryJSON::parse(&format!(r#""{text}""#)).expect("JSON string"),
        );
    }
    chunk
}

fn assert_source_row(chunk: &Chunk, row: usize) {
    let text = format!("{row}.12345");
    let value = chunk.get_row(row);
    assert!(value.is_null(0));
    assert_eq!(value.get_int64(0), 0);
    assert_eq!(value.get_int64(1), row as i64);
    assert_eq!(value.get_string(2).as_bytes(), text.as_bytes());
    assert_eq!(value.get_bytes(3).as_ref(), text.as_bytes());
    assert_eq!(
        String::from_utf8(value.get_my_decimal(4).to_string_bytes()).expect("decimal ASCII"),
        text
    );
    assert_eq!(value.get_json(5).to_string(), format!(r#""{text}""#));
}

#[test]
fn source_row_batch_append_and_projection_contract() {
    let fields = source_fields();
    let source = source_rows(&fields);
    assert_eq!(source.num_cols(), 6);
    assert_eq!(source.num_rows(), 10);
    for row in 0..10 {
        assert_source_row(&source, row);
    }

    let rows = (0..10).map(|row| source.get_row(row)).collect::<Vec<_>>();
    let mut appended = Chunk::new_with_capacity(&fields, 10);
    appended.append_rows(&rows);
    for row in 0..10 {
        assert_source_row(&appended, row);
    }

    let projected_fields = [
        FieldType::new(FieldTypeCode::Json),
        FieldType::new(FieldTypeCode::VarString),
        FieldType::new(FieldTypeCode::NewDecimal),
    ];
    let mut projected = Chunk::new_with_capacity(&projected_fields, 10);
    assert_eq!(
        projected.append_rows_by_col_idxs(&rows, Some(&[5, 3, 4])),
        30
    );
    assert_eq!(projected.num_rows(), 10);
    for row in 0..10 {
        let text = format!("{row}.12345");
        let value = projected.get_row(row);
        assert_eq!(value.get_json(0).to_string(), format!(r#""{text}""#));
        assert_eq!(value.get_bytes(1).as_ref(), text.as_bytes());
        assert_eq!(
            String::from_utf8(value.get_my_decimal(2).to_string_bytes()).expect("decimal ASCII"),
            text
        );
    }

    let single_field = [FieldType::new(FieldTypeCode::LongLong)];
    let mut single_source = Chunk::new_with_capacity(&single_field, 1);
    single_source.append_int64(0, 1);
    let mut partial = Chunk::new_with_capacity(
        &[
            FieldType::new(FieldTypeCode::LongLong),
            FieldType::new(FieldTypeCode::LongLong),
        ],
        1,
    );
    partial.append_partial_row(0, single_source.get_row(0));
    partial.append_partial_row(1, single_source.get_row(0));
    assert_eq!(partial.num_rows(), 1);
    assert_eq!(partial.get_row(0).get_int64(0), 1);
    assert_eq!(partial.get_row(0).get_int64(1), 1);

    let mut virtual_rows = Chunk::new_empty(&[]);
    assert_eq!(
        virtual_rows.append_rows_by_col_idxs(&rows[..3], Some(&[])),
        0
    );
    assert_eq!(virtual_rows.num_rows(), 3);
}

#[test]
fn source_size_selection_append_and_truncate_contract() {
    let field = FieldType::new(FieldTypeCode::LongLong);
    let mut sized = Chunk::new(std::slice::from_ref(&field), 10, 10);
    assert_eq!(sized.required_rows(), 10);
    for _ in 0..10 {
        sized.append_int64(0, 1);
    }
    assert!(sized.is_full());

    sized.grow_and_reset(13);
    assert_eq!(sized.required_rows(), 13);
    for required in 1..20 {
        sized.set_required_rows(required, 13);
        assert_eq!(sized.required_rows(), required.min(13) as usize);
    }
    sized.set_required_rows(-1, 13);
    assert_eq!(sized.required_rows(), 13);

    let mut selected = Chunk::new_with_capacity(std::slice::from_ref(&field), 8);
    let mut selection = Vec::new();
    for row in 0_usize..8 {
        selected.append_int64(0, row as i64);
        if row % 2 == 0 {
            selection.push(row);
        }
    }
    selected.set_sel(Some(selection));
    assert_eq!(selected.num_rows(), 4);
    selected.append_int64(0, 99);
    assert_eq!(selected.sel().and_then(|sel| sel.last()), Some(&8));
    assert_eq!(selected.num_rows(), 5);

    let fields = [
        FieldType::new(FieldTypeCode::Float),
        FieldType::new(FieldTypeCode::VarString),
    ];
    let mut source = Chunk::new_with_capacity(&fields, 16);
    for row in 0..8 {
        source.append_float32(0, 12.8);
        source.append_string(1, "abc");
        source.append_null(0);
        source.append_null(1);
        assert_eq!(source.num_rows(), (row + 1) * 2);
    }
    source.truncate_to(16);
    source.truncate_to(14);
    source.truncate_to(12);
    assert_eq!(source.num_rows(), 12);
    for row in (0..12).step_by(2) {
        assert_eq!(source.get_row(row).get_float32(0), 12.8);
        assert_eq!(source.get_row(row).get_bytes(1).as_ref(), b"abc");
        assert!(source.get_row(row + 1).is_null(0));
        assert!(source.get_row(row + 1).is_null(1));
    }

    let mut appended = Chunk::new_with_capacity(&fields, 24);
    appended.append_range_from(&source, 0, source.num_rows());
    appended.append_own_range(2, 6);
    assert_eq!(appended.num_rows(), 16);
}

#[test]
fn source_copy_decimal_memory_and_identity_contract() {
    let fields = [
        FieldType::new(FieldTypeCode::LongLong),
        FieldType::new(FieldTypeCode::VarString),
    ];
    let mut source = Chunk::new_with_capacity(&fields, 2);
    source.append_int64(0, 7);
    source.append_string(1, "seven");
    let copied = source.copy_construct();
    source.reset();
    source.append_int64(0, 9);
    source.append_string(1, "nine");
    assert_eq!(copied.get_row(0).get_int64(0), 7);
    assert_eq!(copied.get_row(0).get_bytes(1).as_ref(), b"seven");

    let decimal_type = FieldType::new(FieldTypeCode::NewDecimal)
        .with_flen(4)
        .with_decimal(2);
    let mut decimal = Chunk::new_with_capacity(std::slice::from_ref(&decimal_type), 1);
    decimal.append_my_decimal(0, &MyDecimal::from_string(b"1.01").0);
    match decimal.get_row(0).get_datum(0, &decimal_type) {
        Datum::Decimal(value) => {
            assert_eq!(value.to_string(), "1.01");
            assert_eq!(value.declared_shape(), Some((4, 2)));
        }
        other => panic!("expected decimal, got {other:?}"),
    }

    assert_eq!(
        Chunk::new(&[FieldType::new(FieldTypeCode::LongLong)], 32, 1024).memory_usage(),
        380
    );
    assert_eq!(
        Chunk::new(&[FieldType::new(FieldTypeCode::VarString)], 32, 1024).memory_usage(),
        636
    );

    let mut aliases = Chunk::new_with_capacity(&fields, 1);
    aliases.append_int64(0, 1);
    aliases.append_string(1, "one");
    aliases.make_ref(0, 1);
    assert!(aliases.columns_share_identity(0, &aliases, 1));
    aliases
        .column_mut(1)
        .with_int64s_mut(|values| values[0] = 42);
    assert_eq!(aliases.column(0).get_int64(0), 42);

    let mut other = Chunk::new_with_capacity(&fields, 1);
    other.append_int64(0, 3);
    other.append_string(1, "three");
    aliases.make_ref_to(0, &mut other, 1).unwrap();
    assert!(aliases.columns_share_identity(0, &other, 1));
    aliases.swap_column(0, 1).unwrap();
    assert!(aliases.columns_share_identity(1, &other, 1));
}

#[test]
fn benchmark_semantic_workload_contract() {
    let fields = [
        FieldType::new(FieldTypeCode::LongLong),
        FieldType::new(FieldTypeCode::VarString),
    ];
    let mut chunk = Chunk::new_with_capacity(&fields, 1_024);
    for row in 0..1_000_i64 {
        chunk.append_int64(0, row);
        chunk.append_string(1, "abcd");
    }
    assert_eq!(chunk.num_rows(), 1_000);
    assert_eq!(chunk.get_row(999).get_int64(0), 999);
    assert_eq!(chunk.get_row(999).get_bytes(1).as_ref(), b"abcd");
    let initial_memory = chunk.memory_usage();

    let rows = (0..1_000).map(|row| chunk.get_row(row)).collect::<Vec<_>>();
    let mut batch = Chunk::new_with_capacity(&fields, 1_000);
    batch.append_rows(&rows);
    assert_eq!(batch.num_rows(), 1_000);
    assert_eq!(batch.get_row(999).get_int64(0), 999);

    chunk.grow_and_reset(1_024);
    assert_eq!(chunk.num_rows(), 0);
    assert!(chunk.memory_usage() >= initial_memory);
}

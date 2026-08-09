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

//! Public boundary tests for source-visible `Row` and `Chunk` datum behavior.

use tidb_chunk::chunk::Chunk;
use tidb_chunk::row::Row;
use tidb_datatype::{Datum, FieldType, FieldTypeCode};

#[test]
fn append_datum_range_sentinels_are_noops() {
    let field = FieldType::new(FieldTypeCode::VarString);
    let mut chunk = Chunk::new_with_capacity(std::slice::from_ref(&field), 4);
    chunk.append_bytes(0, b"seed");

    let rows_before = chunk.num_rows();
    let cell_before = chunk.get_row(0).get_raw(0).to_vec();
    let memory_before = chunk.memory_usage();

    for sentinel in [Datum::MinNotNull, Datum::MaxValue] {
        Chunk::append_datum(&mut chunk, 0, &sentinel);
    }

    assert_eq!(chunk.num_rows(), rows_before);
    assert_eq!(chunk.get_row(0).get_raw(0).as_ref(), cell_before);
    assert_eq!(chunk.memory_usage(), memory_before);

    chunk.set_sel(Some(vec![0]));
    let selection_before = chunk.sel().map(<[usize]>::to_vec);
    for sentinel in [Datum::MinNotNull, Datum::MaxValue] {
        Chunk::append_datum(&mut chunk, 0, &sentinel);
    }
    assert_eq!(chunk.num_rows(), rows_before);
    assert_eq!(chunk.sel().map(<[usize]>::to_vec), selection_before);
}

#[test]
fn unsupported_field_type_preserves_datum_buffer() {
    let storage_type = FieldType::new(FieldTypeCode::LongLong);
    let mut chunk = Chunk::new_with_capacity(std::slice::from_ref(&storage_type), 2);
    chunk.append_int64(0, 42);
    chunk.append_null(0);

    let value_row = chunk.get_row(0);
    let null_row = chunk.get_row(1);
    for code in [
        FieldTypeCode::Unspecified,
        FieldTypeCode::NewDate,
        FieldTypeCode::Geometry,
        FieldTypeCode::Null,
        FieldTypeCode::Unknown(0xee),
    ] {
        let unsupported_type = FieldType::new(code);
        let mut reusable = Datum::Bytes(vec![0xff, 0x00]);
        Row::datum_with_buffer(&value_row, 0, &unsupported_type, &mut reusable);
        assert_eq!(reusable, Datum::Bytes(vec![0xff, 0x00]), "{code:?}");
        assert_eq!(
            Row::get_datum(&value_row, 0, &unsupported_type),
            Datum::Null,
            "{code:?}"
        );

        reusable = Datum::Int(99);
        Row::datum_with_buffer(&null_row, 0, &unsupported_type, &mut reusable);
        assert_eq!(reusable, Datum::Null, "{code:?}");
    }
}

#[test]
fn row_string_preserves_source_bytes_and_row_index() {
    let field = FieldType::new(FieldTypeCode::VarString);
    let mut chunk = Chunk::new_with_capacity(std::slice::from_ref(&field), 2);
    chunk.append_bytes(0, b"wrong-row");
    chunk.append_bytes(0, &[0xff, 0x00, b'x']);
    chunk.set_sel(Some(vec![1]));

    let row = chunk.get_row(0);
    assert_eq!(Row::get_string(&row, 0).as_bytes(), &[0xff, 0x00, b'x']);
}

#[test]
fn row_raw_len_preserves_source_width_and_row_index() {
    let fields = [
        FieldType::new(FieldTypeCode::LongLong),
        FieldType::new(FieldTypeCode::VarString),
    ];
    let mut chunk = Chunk::new_with_capacity(&fields, 2);
    chunk.append_int64(0, 1);
    chunk.append_bytes(1, b"x");
    chunk.append_int64(0, 2);
    chunk.append_bytes(1, b"second");
    chunk.set_sel(Some(vec![1]));

    let row = chunk.get_row(0);
    assert_eq!(Row::get_raw_len(&row, 0), size_of::<i64>());
    assert_eq!(Row::get_raw_len(&row, 1), b"second".len());
}

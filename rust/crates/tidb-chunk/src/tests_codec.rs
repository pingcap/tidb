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

//! Ports of `pkg/util/chunk/codec_test.go`.

use tidb_datatype::{
    BinaryJSON, BinaryJSONValue, FieldType, FieldTypeCode, MyDecimal,
};

use crate::codec::{estimate_type_width, Codec};
use crate::chunk::Chunk;

fn codec_field_types() -> Vec<FieldType> {
    vec![
        FieldType::new(FieldTypeCode::LongLong),
        FieldType::new(FieldTypeCode::LongLong),
        FieldType::new(FieldTypeCode::Varchar),
        FieldType::new(FieldTypeCode::Varchar),
        FieldType::new(FieldTypeCode::NewDecimal),
        FieldType::new(FieldTypeCode::Json),
    ]
}

/// Go `TestCodec` (codec_test.go): encode a mixed chunk and decode it back.
#[test]
fn codec_encode_decode_round_trip() {
    let num_rows = 10;
    let col_types = codec_field_types();

    let mut old_chk = Chunk::new_with_capacity(&col_types, num_rows);
    for i in 0..num_rows {
        let text = format!("{i}.12345");
        old_chk.append_null(0);
        old_chk.append_int64(1, i as i64);
        old_chk.append_string(2, text.as_bytes());
        old_chk.append_string(3, text.as_bytes());
        let decimal = MyDecimal::from_string(text.as_bytes()).0;
        old_chk.append_my_decimal(4, &decimal);
        let json = create_string_json(&text);
        old_chk.append_json(5, &json);
    }

    let codec = Codec::new(col_types.clone());
    let buffer = codec.encode(&old_chk);

    let mut new_chk = Chunk::new_with_capacity(&col_types, num_rows);
    let remained = codec.decode_to_chunk(&buffer, &mut new_chk);

    assert!(remained.is_empty());
    assert_eq!(new_chk.num_cols(), col_types.len());
    assert_eq!(new_chk.num_rows(), num_rows);
    for i in 0..num_rows {
        let row = new_chk.get_row(i);
        let text = format!("{i}.12345");
        assert!(row.is_null(0));
        assert!(!row.is_null(1));
        assert!(!row.is_null(2));
        assert!(!row.is_null(3));
        assert!(!row.is_null(4));
        assert!(!row.is_null(5));

        assert_eq!(row.get_int64(1), i as i64);
        assert_eq!(row.get_string(2), text.as_str());
        assert_eq!(row.get_string(3), text.as_str());
        let expected_decimal = MyDecimal::from_string(text.as_bytes()).0;
        assert_eq!(row.get_my_decimal(4), expected_decimal);
        assert_eq!(row.get_json(5), create_string_json(&text));
    }
}

/// Go `types.CreateBinaryJSON(str)`: a binary JSON string value.
fn create_string_json(text: &str) -> BinaryJSON {
    BinaryJSON::from_typed_value(&BinaryJSONValue::String(text.to_owned()))
        .expect("string json value")
}

/// Go `TestEstimateTypeWidth` (codec_test.go).
#[test]
fn estimate_type_width_matches_go() {
    // Fixed-width type.
    let col_type = FieldType::new(FieldTypeCode::LongLong);
    assert_eq!(estimate_type_width(&col_type), 8);

    // colLen <= 32.
    let mut col_type = FieldType::new(FieldTypeCode::String);
    col_type.set_flen(31);
    assert_eq!(estimate_type_width(&col_type), 31);

    // colLen < 1000.
    let mut col_type = FieldType::new(FieldTypeCode::String);
    col_type.set_flen(999);
    assert_eq!(estimate_type_width(&col_type), 515);

    // colLen >= 1000.
    let mut col_type = FieldType::new(FieldTypeCode::String);
    col_type.set_flen(2000);
    assert_eq!(estimate_type_width(&col_type), 516);

    // Value after guessing: no length information.
    let col_type = FieldType::new(FieldTypeCode::String);
    assert_eq!(estimate_type_width(&col_type), 32);
}

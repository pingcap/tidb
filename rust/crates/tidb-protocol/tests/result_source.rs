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

#![allow(missing_docs)]

use tidb_protocol::{
    append_length_encoded_bytes, append_length_encoded_int, dump_column, dump_column_with_default,
    dump_flag, dump_type, encode_text_row, is_string_column_type, ColumnDefault, ColumnInfo,
    PacketError, TYPE_ENUM, TYPE_NEW_DATE, TYPE_SET, TYPE_STRING,
};

fn source_column(default_value: Option<ColumnDefault>) -> ColumnInfo {
    ColumnInfo {
        schema: "testSchema".to_owned(),
        table: "testTable".to_owned(),
        org_table: "testOrgTable".to_owned(),
        name: "testName".to_owned(),
        org_name: "testOrgName".to_owned(),
        column_length: 1,
        charset: 106,
        flag: 0,
        decimal: 1,
        type_code: TYPE_NEW_DATE,
        default_value,
    }
}

#[test]
fn test_dump_column() {
    let mut got = Vec::new();
    dump_column(
        &mut got,
        &source_column(Some(ColumnDefault::Bytes(vec![5, 2]))),
    );
    let expected = vec![
        0x03, b'd', b'e', b'f', 0x0a, b't', b'e', b's', b't', b'S', b'c', b'h', b'e', b'm', b'a',
        0x09, b't', b'e', b's', b't', b'T', b'a', b'b', b'l', b'e', 0x0c, b't', b'e', b's', b't',
        b'O', b'r', b'g', b'T', b'a', b'b', b'l', b'e', 0x08, b't', b'e', b's', b't', b'N', b'a',
        b'm', b'e', 0x0b, b't', b'e', b's', b't', b'O', b'r', b'g', b'N', b'a', b'm', b'e', 0x0c,
        0x6a, 0x00, 0x01, 0x00, 0x00, 0x00, 0x0e, 0x00, 0x00, 0x01, 0x00, 0x00,
    ];
    assert_eq!(got, expected);

    assert_eq!(dump_flag(TYPE_SET, 0), 1 << 11);
    assert_eq!(dump_flag(TYPE_ENUM, 0), 1 << 8);
    assert_eq!(dump_flag(TYPE_STRING, 0), 0);
    assert_eq!(dump_type(TYPE_SET), TYPE_STRING);
    assert_eq!(dump_type(TYPE_ENUM), TYPE_STRING);
    assert_eq!(dump_type(TYPE_NEW_DATE), TYPE_NEW_DATE);
}

#[test]
fn test_dump_column_with_default() {
    let mut got = Vec::new();
    dump_column_with_default(
        &mut got,
        &source_column(Some(ColumnDefault::Text("test".to_owned()))),
    );
    let mut expected = vec![
        0x03, b'd', b'e', b'f', 0x0a, b't', b'e', b's', b't', b'S', b'c', b'h', b'e', b'm', b'a',
        0x09, b't', b'e', b's', b't', b'T', b'a', b'b', b'l', b'e', 0x0c, b't', b'e', b's', b't',
        b'O', b'r', b'g', b'T', b'a', b'b', b'l', b'e', 0x08, b't', b'e', b's', b't', b'N', b'a',
        b'm', b'e', 0x0b, b't', b'e', b's', b't', b'O', b'r', b'g', b'N', b'a', b'm', b'e', 0x0c,
        0x6a, 0x00, 0x01, 0x00, 0x00, 0x00, 0x0e, 0x00, 0x00, 0x01, 0x00, 0x00,
    ];
    expected.extend_from_slice(&[0x04, b't', b'e', b's', b't']);
    assert_eq!(got, expected);
}

#[test]
fn test_column_name_limit() {
    let mut column = source_column(None);
    column.name = "a".repeat(300);
    let mut got = Vec::new();
    dump_column(&mut got, &column);

    let mut expected = vec![
        0x03, b'd', b'e', b'f', 0x0a, b't', b'e', b's', b't', b'S', b'c', b'h', b'e', b'm', b'a',
        0x09, b't', b'e', b's', b't', b'T', b'a', b'b', b'l', b'e', 0x0c, b't', b'e', b's', b't',
        b'O', b'r', b'g', b'T', b'a', b'b', b'l', b'e', 0xfc, 0x00, 0x01,
    ];
    expected.extend(std::iter::repeat_n(b'a', 256));
    expected.extend_from_slice(&[
        0x0b, b't', b'e', b's', b't', b'O', b'r', b'g', b'N', b'a', b'm', b'e', 0x0c, 0x6a, 0x00,
        0x01, 0x00, 0x00, 0x00, 0x0e, 0x00, 0x00, 0x01, 0x00, 0x00,
    ]);
    assert_eq!(got, expected);
}

#[test]
fn test_dump_length_encoded_int() {
    let cases = [
        (0, vec![0x00]),
        (250, vec![0xfa]),
        (251, vec![0xfc, 0xfb, 0x00]),
        (513, vec![0xfc, 0x01, 0x02]),
        (197_121, vec![0xfd, 0x01, 0x02, 0x03]),
        (
            578_437_695_752_307_201,
            vec![0xfe, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08],
        ),
    ];
    for (value, expected) in cases {
        let mut got = Vec::new();
        append_length_encoded_int(&mut got, value);
        assert_eq!(got, expected);
    }
}

#[test]
fn test_dump_text_value_framing() {
    assert_eq!(
        encode_text_row(&[None, Some(b"10"), Some(b"11")]),
        vec![0xfb, 0x02, b'1', b'0', 0x02, b'1', b'1']
    );

    let mut got = Vec::new();
    append_length_encoded_bytes(&mut got, Some(&[0xd2, 0xbb]));
    assert_eq!(got, vec![0x02, 0xd2, 0xbb]);

    assert!(is_string_column_type(tidb_protocol::TYPE_VARCHAR));
    assert!(is_string_column_type(
        tidb_protocol::TYPE_TIDB_VECTOR_FLOAT32
    ));
    assert!(!is_string_column_type(8));
}

#[test]
fn typed_result_boundaries_remain_explicit() {
    // Keep the source-owned framing test honest: this leaf accepts already
    // formatted bytes and does not pretend to implement Go Datum conversion.
    let error = PacketError::PayloadLengthOverflow { length: 1 << 24 };
    assert!(error.to_string().contains("three-byte"));
}

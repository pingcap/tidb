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
    encode_eof_packet, encode_ok_packet, encode_text_result_set, ColumnInfo, EofPacket, OkPacket,
    ResultSetError, ResultSetOptions, TYPE_NEW_DATE,
};

fn source_column() -> ColumnInfo {
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
        default_value: None,
    }
}

#[test]
fn test_ok_eof() {
    let ok = encode_ok_packet(&OkPacket {
        status_flags: 2,
        protocol_41: true,
        ..OkPacket::default()
    });
    assert_eq!(ok, vec![0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00]);

    let eof_as_ok = encode_eof_packet(&EofPacket {
        status_flags: 2,
        deprecate_eof: true,
        protocol_41: true,
        ..EofPacket::default()
    });
    assert_eq!(eof_as_ok, vec![0xfe, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00]);

    let legacy_eof = encode_eof_packet(&EofPacket {
        status_flags: 2,
        deprecate_eof: false,
        protocol_41: true,
        ..EofPacket::default()
    });
    assert_eq!(legacy_eof, vec![0xfe, 0x00, 0x00, 0x02, 0x00]);
}

#[test]
fn test_ok_info_is_length_encoded() {
    let packet = encode_ok_packet(&OkPacket {
        info: b"done".to_vec(),
        protocol_41: true,
        ..OkPacket::default()
    });
    assert_eq!(
        packet,
        vec![0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x04, b'd', b'o', b'n', b'e']
    );
}

#[test]
fn text_result_set_sequence_matches_write_chunks() {
    let packets = encode_text_result_set(
        &[source_column()],
        &[vec![Some(b"10".to_vec())], vec![None]],
        ResultSetOptions {
            status_flags: 2,
            ..ResultSetOptions::default()
        },
    )
    .unwrap();

    assert_eq!(packets[0], vec![0x01]);
    assert_eq!(packets[1][0], 0x03); // lenenc "def" metadata prefix
    assert_eq!(packets[2], vec![0xfe, 0x00, 0x00, 0x02, 0x00]);
    assert_eq!(packets[3], vec![0x02, b'1', b'0']);
    assert_eq!(packets[4], vec![0xfb]);
    assert_eq!(packets[5], packets[2]);
}

#[test]
fn text_result_set_deprecate_eof_uses_ok_shaped_terminal_packet() {
    let packets = encode_text_result_set(
        &[source_column()],
        &[],
        ResultSetOptions {
            status_flags: 2,
            deprecate_eof: true,
            protocol_41: true,
            ..ResultSetOptions::default()
        },
    )
    .unwrap();

    assert_eq!(packets.len(), 3);
    assert_eq!(packets[2], vec![0xfe, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00]);
}

#[test]
fn text_result_set_rejects_row_column_mismatch() {
    let error = encode_text_result_set(&[source_column()], &[vec![]], ResultSetOptions::default())
        .unwrap_err();
    assert_eq!(
        error,
        ResultSetError::RowColumnCount {
            row: 0,
            expected: 1,
            actual: 0,
        }
    );
}

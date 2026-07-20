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
    decode_prepared_statement_close, decode_prepared_statement_execute,
    encode_binary_signed_longlong_row, encode_prepared_statement_prepare_response,
    BinarySignedLongLongResultSetStream, ColumnInfo, PreparedParameterType, PreparedParameterTypes,
    PreparedStatementError, PreparedValue, ResultSetOptions, TYPE_LONGLONG,
};

fn longlong_column(name: &str) -> ColumnInfo {
    ColumnInfo {
        schema: "sysbench_rs".to_owned(),
        table: "t".to_owned(),
        org_table: "t".to_owned(),
        name: name.to_owned(),
        org_name: name.to_owned(),
        column_length: 20,
        charset: 63,
        flag: 0,
        decimal: 0,
        type_code: TYPE_LONGLONG,
        default_value: None,
    }
}

fn execute_payload(statement_id: u32, new_types: bool, value: i64) -> Vec<u8> {
    let mut packet = Vec::new();
    packet.extend_from_slice(&statement_id.to_le_bytes());
    packet.push(0); // no cursor
    packet.extend_from_slice(&1_u32.to_le_bytes());
    packet.push(0); // one non-NULL parameter
    packet.push(u8::from(new_types));
    if new_types {
        packet.extend_from_slice(&[0x08, 0]); // signed MYSQL_TYPE_LONGLONG
    }
    packet.extend_from_slice(&value.to_le_bytes());
    packet
}

#[test]
fn execute_decodes_one_signed_bigint_and_retains_type_reuse_representation() {
    let first = decode_prepared_statement_execute(&execute_payload(7, true, -42), 1, None).unwrap();
    assert_eq!(first.statement_id, 7);
    assert_eq!(first.cursor_flags, 0);
    assert_eq!(
        first.parameter_types,
        PreparedParameterTypes::New(vec![PreparedParameterType::SignedLongLong])
    );
    assert_eq!(first.values, vec![PreparedValue::SignedLongLong(-42)]);

    let second = decode_prepared_statement_execute(
        &execute_payload(7, false, i64::MAX),
        1,
        Some(&[PreparedParameterType::SignedLongLong]),
    )
    .unwrap();
    assert_eq!(second.parameter_types, PreparedParameterTypes::Reuse);
    assert_eq!(second.values, vec![PreparedValue::SignedLongLong(i64::MAX)]);
}

#[test]
fn execute_rejects_every_unowned_packet_variant_without_fallback() {
    let cases = [
        (
            "truncated fixed header",
            vec![1, 0, 0],
            None,
            PreparedStatementError::Truncated {
                field: "statement ID",
                required: 4,
                available: 3,
            },
        ),
        (
            "wrong parameter count",
            execute_payload(1, true, 1),
            None,
            PreparedStatementError::UnsupportedParameterCount { count: 2 },
        ),
        (
            "zero statement id",
            execute_payload(0, true, 1),
            None,
            PreparedStatementError::ZeroStatementId,
        ),
        (
            "cursor",
            {
                let mut packet = execute_payload(1, true, 1);
                packet[4] = 1;
                packet
            },
            None,
            PreparedStatementError::UnsupportedCursorFlag(1),
        ),
        (
            "non-one iteration count",
            {
                let mut packet = execute_payload(1, true, 1);
                packet[5..9].copy_from_slice(&2_u32.to_le_bytes());
                packet
            },
            None,
            PreparedStatementError::UnsupportedIterationCount(2),
        ),
        (
            "null",
            {
                let mut packet = execute_payload(1, true, 1);
                packet[9] = 1;
                packet
            },
            None,
            PreparedStatementError::NullParameter { parameter: 0 },
        ),
        (
            "missing type reuse",
            execute_payload(1, false, 1),
            None,
            PreparedStatementError::MissingPreviousTypeVector,
        ),
        (
            "unsigned",
            {
                let mut packet = execute_payload(1, true, 1);
                packet[12] = 0x80;
                packet
            },
            None,
            PreparedStatementError::UnsignedParameter { parameter: 0 },
        ),
        (
            "non bigint",
            {
                let mut packet = execute_payload(1, true, 1);
                packet[11] = 0x0f;
                packet
            },
            None,
            PreparedStatementError::UnsupportedParameterType {
                parameter: 0,
                type_code: 0x0f,
            },
        ),
        (
            "truncated value",
            execute_payload(1, true, 1)[..15].to_vec(),
            None,
            PreparedStatementError::Truncated {
                field: "signed BIGINT value",
                required: 8,
                available: 2,
            },
        ),
    ];
    for (name, packet, types, expected) in cases {
        let parameter_count = match &expected {
            PreparedStatementError::UnsupportedParameterCount { count } => *count,
            _ => 1,
        };
        assert_eq!(
            decode_prepared_statement_execute(&packet, parameter_count, types),
            Err(expected),
            "{name}"
        );
    }
}

#[test]
fn prepare_metadata_and_silent_close_match_mysql_packet_contract() {
    let packets = encode_prepared_statement_prepare_response(
        7,
        &[longlong_column("id")],
        &[longlong_column("value")],
        ResultSetOptions {
            status_flags: 2,
            ..ResultSetOptions::default()
        },
    )
    .unwrap();
    assert_eq!(packets.len(), 5);
    assert_eq!(packets[0], vec![0, 7, 0, 0, 0, 1, 0, 1, 0, 0, 0, 0]);
    assert_eq!(packets[1][0], 0x03); // length-encoded catalog "def"
    assert_eq!(packets[2], vec![0xfe, 0, 0, 2, 0]);
    assert_eq!(packets[3][0], 0x03);
    assert_eq!(packets[4], packets[2]);
    assert_eq!(decode_prepared_statement_close(&7_u32.to_le_bytes()), Ok(7));
    assert_eq!(
        decode_prepared_statement_close(&[7, 0, 0]),
        Err(PreparedStatementError::Truncated {
            field: "statement ID",
            required: 4,
            available: 3,
        })
    );
    assert_eq!(
        decode_prepared_statement_close(&[7, 0, 0, 0, 1]),
        Err(PreparedStatementError::TrailingBytes { bytes: 1 })
    );
}

#[test]
fn binary_rows_use_the_result_null_bitmap_offset_and_little_endian_cells() {
    assert_eq!(
        encode_binary_signed_longlong_row(&[-2, i64::MAX]),
        vec![
            0, 0, 0xfe, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
            0xff, 0xff, 0x7f,
        ]
    );

    let mut stream = BinarySignedLongLongResultSetStream::new(
        vec![longlong_column("value")],
        ResultSetOptions {
            status_flags: 2,
            ..ResultSetOptions::default()
        },
    )
    .unwrap();
    assert_eq!(stream.metadata_packets().unwrap().len(), 3);
    assert_eq!(
        stream.row_packet(&[-42]).unwrap(),
        vec![0, 0, 0xd6, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff]
    );
    assert_eq!(stream.finish_packet().unwrap(), vec![0xfe, 0, 0, 2, 0]);
}

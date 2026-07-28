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

use tidb_datatype::{Decimal, PackedTime};
use tidb_protocol::{
    decode_prepared_statement_close, decode_prepared_statement_execute, encode_binary_datetime,
    encode_binary_result_row, encode_binary_signed_longlong_row, encode_binary_time,
    encode_prepared_statement_prepare_response, BinaryDateTimeType, BinaryResultCell,
    BinaryResultSetStream, ColumnInfo, PreparedParameterType, PreparedParameterTypes,
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

// One-parameter execute packet carrying an explicit type pair and raw value
// bytes, so a test can drive any admitted integer width.
fn execute_payload_typed(
    statement_id: u32,
    type_code: u8,
    flag: u8,
    value_bytes: &[u8],
) -> Vec<u8> {
    let mut packet = Vec::new();
    packet.extend_from_slice(&statement_id.to_le_bytes());
    packet.push(0); // no cursor
    packet.extend_from_slice(&1_u32.to_le_bytes());
    packet.push(0); // one non-NULL parameter
    packet.push(1); // new parameter types follow
    packet.push(type_code);
    packet.push(flag);
    packet.extend_from_slice(value_bytes);
    packet
}

#[test]
fn execute_decodes_each_signed_integer_width_with_sign_extension() {
    // Go ExecBinaryParam widens int8/int16/int32/int64 to one int64 datum;
    // TypeTiny=1, Short/Year=2, Int24/Long=4, Longlong=8 wire bytes.
    let cases: &[(u8, &[u8], i64)] = &[
        (0x01, &[0xff], -1),                               // TypeTiny
        (0x01, &[0x7f], 127),                              // TypeTiny max
        (0x02, &[0x00, 0x80], -32768),                     // TypeShort min
        (0x0d, &[0xff, 0x7f], 32767),                      // TypeYear (2 bytes)
        (0x03, &[0xff, 0xff, 0xff, 0xff], -1),             // TypeLong
        (0x09, &[0x78, 0x56, 0x34, 0x12], 0x1234_5678),    // TypeInt24 (4 wire bytes)
        (0x08, &[0x00, 0, 0, 0, 0, 0, 0, 0x80], i64::MIN), // TypeLonglong
    ];
    for (type_code, value_bytes, expected) in cases {
        let decoded = decode_prepared_statement_execute(
            &execute_payload_typed(7, *type_code, 0, value_bytes),
            1,
            None,
        )
        .unwrap();
        assert_eq!(
            decoded.values,
            vec![PreparedValue::SignedLongLong(*expected)],
            "type {type_code:#x}"
        );
    }
    // The remembered type vector names the concrete width for later reuse.
    let tiny =
        decode_prepared_statement_execute(&execute_payload_typed(7, 0x01, 0, &[0x01]), 1, None)
            .unwrap();
    assert_eq!(
        tiny.parameter_types,
        PreparedParameterTypes::New(vec![PreparedParameterType::SignedTiny])
    );
}

#[test]
fn execute_decodes_a_string_parameter() {
    // TYPE_VARCHAR (0x0f) carries a length-encoded string; utf8 identity for this
    // node, so the raw bytes pass through (ExecBinaryParam's string arm).
    let decoded = decode_prepared_statement_execute(
        &execute_payload_typed(7, 0x0f, 0, &[0x03, b'a', b'b', b'c']),
        1,
        None,
    )
    .unwrap();
    assert_eq!(decoded.values, vec![PreparedValue::String(b"abc".to_vec())]);
    assert_eq!(
        decoded.parameter_types,
        PreparedParameterTypes::New(vec![PreparedParameterType::String])
    );
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
            // A multi-marker execute is now decoded, so the only rejected
            // count is zero: every admitted template binds at least one value.
            "zero parameter count",
            execute_payload(1, true, 1),
            None,
            PreparedStatementError::UnsupportedParameterCount { count: 0 },
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
            // A NULL parameter carries NO bytes in the value section, so a
            // packet that both marks it NULL and sends eight value bytes is
            // malformed. (The NULL parameter itself is now a value -- see
            // `execute_decodes_every_parameter_family`.)
            "null bit with value bytes",
            {
                let mut packet = execute_payload(1, true, 1);
                packet[9] = 1;
                packet
            },
            None,
            PreparedStatementError::TrailingBytes { bytes: 8 },
        ),
        (
            "missing type reuse",
            execute_payload(1, false, 1),
            None,
            PreparedStatementError::MissingPreviousTypeVector,
        ),
        (
            // Only the unsigned bit is a real flag; any other bit is an
            // encoding this leaf does not model. (The unsigned bit itself is
            // now admitted -- see `execute_decodes_every_parameter_family`.)
            "unmodelled type flag",
            {
                let mut packet = execute_payload(1, true, 1);
                packet[12] = 0x40;
                packet
            },
            None,
            PreparedStatementError::InvalidField {
                field: "parameter type flags",
                value: 0x40,
            },
        ),
        (
            // TYPE_GEOMETRY (0xff): outside every family Go's ExecBinaryParam
            // builds a datum for, so it still fails closed. (TYPE_DOUBLE used
            // to be this example and is admitted now.)
            "unsupported type",
            {
                let mut packet = execute_payload(1, true, 1);
                packet[11] = 0xff;
                packet
            },
            None,
            PreparedStatementError::UnsupportedParameterType {
                parameter: 0,
                type_code: 0xff,
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

    let mut stream = BinaryResultSetStream::new(
        vec![longlong_column("value")],
        ResultSetOptions {
            status_flags: 2,
            ..ResultSetOptions::default()
        },
    )
    .unwrap();
    assert_eq!(stream.metadata_packets().unwrap().len(), 3);
    assert_eq!(
        stream
            .row_packet(&[BinaryResultCell::LongLong(-42)])
            .unwrap(),
        vec![0, 0, 0xd6, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff]
    );
    assert_eq!(stream.finish_packet().unwrap(), vec![0xfe, 0, 0, 2, 0]);
}

// -----------------------------------------------------------------------------
// Binary result cells: mixed signed-BIGINT and string rows
// -----------------------------------------------------------------------------

#[test]
fn a_mixed_int_and_string_binary_row_matches_the_wire_format() {
    // Byte layout per TiDB's DumpBinaryRow (pkg/server/internal/column/column.go):
    // OKHeader (0x00), one null-bitmap byte (ceil((2+2)/8)=1, all zero), then the
    // TypeLonglong cell as dump.Uint64 (8 LE bytes) and the string cell as
    // dump.LengthEncodedString.
    let row = encode_binary_result_row(&[
        BinaryResultCell::LongLong(100),
        BinaryResultCell::String(b"hello".to_vec()),
    ]);
    let mut expected = vec![0x00, 0x00];
    expected.extend_from_slice(&100_i64.to_le_bytes());
    expected.push(5); // length-encoded length of "hello"
    expected.extend_from_slice(b"hello");
    assert_eq!(row, expected);
}

#[test]
fn an_int_only_binary_result_row_matches_the_signed_longlong_encoder() {
    // The generalized encoder must be byte-identical to the existing int path
    // when every cell is a signed BIGINT.
    let cells = [
        BinaryResultCell::LongLong(-2),
        BinaryResultCell::LongLong(i64::MAX),
    ];
    assert_eq!(
        encode_binary_result_row(&cells),
        encode_binary_signed_longlong_row(&[-2, i64::MAX])
    );
}

#[test]
fn an_empty_string_cell_is_a_zero_length_length_encoded_string() {
    let row = encode_binary_result_row(&[BinaryResultCell::String(Vec::new())]);
    // header, one null-bitmap byte (ceil((1+2)/8)=1), then lenenc length 0.
    assert_eq!(row, vec![0x00, 0x00, 0x00]);
}

#[test]
fn a_null_cell_sets_its_bitmap_bit_and_writes_no_value_bytes() {
    // Per TiDB DumpBinaryRow: the first two bitmap bits are reserved, so the
    // single column occupies bit 2 -> the one null-bitmap byte is 1 << 2 = 0x04,
    // and a NULL contributes no value bytes.
    let row = encode_binary_result_row(&[BinaryResultCell::Null]);
    assert_eq!(row, vec![0x00, 0x04]);
}

#[test]
fn a_null_cell_among_values_marks_only_its_own_column() {
    // Columns 0 (Long, bit 2) and 1 (Null, bit 3): only bit 3 is set (0x08), the
    // non-null Long still dumps its four little-endian value bytes.
    let row = encode_binary_result_row(&[BinaryResultCell::Long(5), BinaryResultCell::Null]);
    let mut expected = vec![0x00, 0x08];
    expected.extend_from_slice(&5_u32.to_le_bytes());
    assert_eq!(row, expected);
}

#[test]
fn a_string_cell_length_prefix_grows_past_the_one_byte_boundary() {
    // 251 bytes crosses the length-encoded-int boundary into the 0xfc + u16 form.
    let text = vec![b'x'; 251];
    let row = encode_binary_result_row(&[BinaryResultCell::String(text.clone())]);
    let mut expected = vec![0x00, 0x00, 0xfc];
    expected.extend_from_slice(&251_u16.to_le_bytes());
    expected.extend_from_slice(&text);
    assert_eq!(row, expected);
}

#[test]
fn the_null_bitmap_reserves_two_low_bits_at_the_seven_column_boundary() {
    // With 7 columns the two reserved low bits push the bitmap into a second
    // byte: ceil((7 + 2) / 8) = 2, versus ceil(7 / 8) = 1 without the reserve.
    let cells = vec![BinaryResultCell::LongLong(0); 7];
    let row = encode_binary_result_row(&cells);
    // header(1) + bitmap(2) + 7 * 8-byte int cells.
    assert_eq!(row.len(), 1 + 2 + 7 * 8);
    assert_eq!(
        &row[..3],
        &[0x00, 0x00, 0x00],
        "header then a two-byte bitmap"
    );
}

fn varstring_column(name: &str) -> ColumnInfo {
    ColumnInfo {
        schema: "sysbench_rs".to_owned(),
        table: "t".to_owned(),
        org_table: "t".to_owned(),
        name: name.to_owned(),
        org_name: name.to_owned(),
        column_length: 120,
        charset: 46,
        flag: 0,
        decimal: 0,
        type_code: 0xfe, // TypeString (CHAR)
        default_value: None,
    }
}

fn newdecimal_column(name: &str) -> ColumnInfo {
    ColumnInfo {
        schema: "sysbench_rs".to_owned(),
        table: "t".to_owned(),
        org_table: "t".to_owned(),
        name: name.to_owned(),
        org_name: name.to_owned(),
        column_length: 22,
        charset: 63,
        flag: 0,
        decimal: 4,
        type_code: 0xf6, // TypeNewDecimal
        default_value: None,
    }
}

#[test]
fn a_decimal_cell_dumps_length_encoded_mydecimal_string() {
    // Go DumpBinaryRow TypeNewDecimal: LengthEncodedString(GetMyDecimal(i).String())
    // with no EncodeData. The MyDecimal.String() outputs below were generated from
    // pkg/types: "1234.5678" -> "1234.5678", "010.500" -> "10.500", "-0.5" -> "-0.5".
    let row = |text| {
        encode_binary_result_row(&[BinaryResultCell::NewDecimal(Decimal::from_literal(text))])
    };
    assert_eq!(
        row("1234.5678"),
        vec![0x00, 0x00, 0x09, b'1', b'2', b'3', b'4', b'.', b'5', b'6', b'7', b'8']
    );
    assert_eq!(
        row("010.500"),
        vec![0x00, 0x00, 0x06, b'1', b'0', b'.', b'5', b'0', b'0']
    );
    assert_eq!(row("-0.5"), vec![0x00, 0x00, 0x04, b'-', b'0', b'.', b'5']);
}

#[test]
fn the_stream_admits_a_decimal_column_and_frames_a_row() {
    let mut stream = BinaryResultSetStream::new(
        vec![longlong_column("id"), newdecimal_column("amount")],
        ResultSetOptions::default(),
    )
    .expect("LONGLONG + NewDecimal columns are admitted");
    assert_eq!(stream.metadata_packets().unwrap().len(), 4);
    let row = stream
        .row_packet(&[
            BinaryResultCell::LongLong(7),
            BinaryResultCell::NewDecimal(Decimal::from_literal("1234.5678")),
        ])
        .expect("a longlong + decimal row frames");
    let mut expected = vec![0x00, 0x00]; // header + 1 null-bitmap byte (ceil((2+2)/8))
    expected.extend_from_slice(&7_i64.to_le_bytes());
    expected.push(9);
    expected.extend_from_slice(b"1234.5678");
    assert_eq!(row, expected);
    // A string cell for a decimal column is a mismatch (cell_matches is exact).
    let mut mismatch = BinaryResultSetStream::new(
        vec![newdecimal_column("amount")],
        ResultSetOptions::default(),
    )
    .unwrap();
    mismatch.metadata_packets().unwrap();
    assert_eq!(
        mismatch.row_packet(&[BinaryResultCell::String(b"1.0".to_vec())]),
        Err(PreparedStatementError::MismatchedBinaryResultCell {
            column: 0,
            type_code: 0xf6,
        })
    );
}

#[test]
fn the_stream_admits_a_string_column_and_frames_a_mixed_row() {
    let mut stream = BinaryResultSetStream::new(
        vec![longlong_column("k"), varstring_column("c")],
        ResultSetOptions::default(),
    )
    .expect("LONGLONG + string columns are admitted");
    // metadata: column count + two column defs + the legacy metadata EOF
    // (deprecate_eof is false by default).
    assert_eq!(stream.metadata_packets().unwrap().len(), 4);
    let row = stream
        .row_packet(&[
            BinaryResultCell::LongLong(7),
            BinaryResultCell::String(b"abc".to_vec()),
        ])
        .expect("a mixed row frames");
    let mut expected = vec![0x00, 0x00]; // header + 1 null-bitmap byte (ceil((2+2)/8))
    expected.extend_from_slice(&7_i64.to_le_bytes());
    expected.push(3);
    expected.extend_from_slice(b"abc");
    assert_eq!(row, expected);
}

#[test]
fn the_stream_rejects_a_cell_that_does_not_match_its_column_type() {
    let mut stream =
        BinaryResultSetStream::new(vec![varstring_column("c")], ResultSetOptions::default())
            .unwrap();
    stream.metadata_packets().unwrap();
    assert_eq!(
        stream.row_packet(&[BinaryResultCell::LongLong(1)]),
        Err(PreparedStatementError::MismatchedBinaryResultCell {
            column: 0,
            type_code: 0xfe,
        })
    );
}

#[test]
fn the_stream_rejects_an_unsupported_result_column_type() {
    let mut column = varstring_column("c");
    column.type_code = 12; // TypeDatetime, temporal cell source not ported yet
    assert_eq!(
        BinaryResultSetStream::new(vec![column], ResultSetOptions::default())
            .map(|_| ())
            .unwrap_err(),
        PreparedStatementError::UnsupportedBinaryResultColumn {
            column: 0,
            type_code: 12,
        }
    );
}

#[test]
fn each_integer_cell_dumps_its_dump_binary_row_width() {
    // Per DumpBinaryRow + dump.Uint16/32/64: TypeTiny=1 byte, Short/Year=2 LE,
    // Int24/Long=4 LE, Longlong=8 LE. Header + one all-zero null-bitmap byte
    // precede the single cell.
    let row = |cell| encode_binary_result_row(&[cell]);
    assert_eq!(row(BinaryResultCell::Tiny(-1)), vec![0x00, 0x00, 0xff]);
    assert_eq!(
        row(BinaryResultCell::Short(-1)),
        vec![0x00, 0x00, 0xff, 0xff]
    );
    assert_eq!(
        row(BinaryResultCell::Long(-1)),
        vec![0x00, 0x00, 0xff, 0xff, 0xff, 0xff]
    );
    assert_eq!(
        row(BinaryResultCell::LongLong(-1)),
        vec![0x00, 0x00, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff]
    );
    // 300 = 0x012C: as a 1-byte Tiny it truncates to 0x2C, exactly like
    // Go byte(GetInt64).
    assert_eq!(row(BinaryResultCell::Tiny(300)), vec![0x00, 0x00, 0x2c]);
}

#[test]
fn float_and_double_cells_dump_ieee754_little_endian_bits() {
    assert_eq!(encode_binary_result_row(&[BinaryResultCell::Float(1.5)]), {
        let mut e = vec![0x00, 0x00];
        e.extend_from_slice(&1.5f32.to_bits().to_le_bytes());
        e
    });
    assert_eq!(
        encode_binary_result_row(&[BinaryResultCell::Double(1.5)]),
        {
            let mut e = vec![0x00, 0x00];
            e.extend_from_slice(&1.5f64.to_bits().to_le_bytes());
            e
        }
    );
}

#[test]
fn the_stream_admits_the_whole_numeric_family_and_rejects_unported_types() {
    // Tiny, Short, Year, Int24, Long, Longlong, Float, Double, NewDecimal (246) —
    // all sourced.
    for tp in [1u8, 2, 13, 9, 3, 8, 4, 5, 246] {
        let mut c = longlong_column("n");
        c.type_code = tp;
        assert!(BinaryResultSetStream::new(vec![c], ResultSetOptions::default()).is_ok());
    }
    // Datetime (12), Timestamp (7), Enum (0xf7) are fail closed pending a
    // temporal / EncodeData cell source.
    for tp in [12u8, 7, 0xf7] {
        let mut c = longlong_column("n");
        c.type_code = tp;
        assert!(BinaryResultSetStream::new(vec![c], ResultSetOptions::default()).is_err());
    }
}

#[test]
fn binary_time_matches_go_dump_binary_time_vectors() {
    // Vectors from pkg/server/internal/dump/dump_test.go TestDumpBinaryTime.
    assert_eq!(encode_binary_time(0), vec![0]);
    assert_eq!(
        encode_binary_time(-1),
        vec![12, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
    );
    // 1ns + 86_400_000 microseconds = 1min 26s + 400000us fractional.
    let ns = 1 + 86_400 * 1_000 * 1_000;
    assert_eq!(
        encode_binary_time(ns),
        vec![12, 0, 0, 0, 0, 0, 0, 1, 26, 128, 26, 6, 0]
    );
    // A whole-second duration drops the micros: type byte 8, then the 9-byte
    // body (`data[:9]` in Go) ending in the seconds field. Go's own vectors
    // never hit this branch, so this extends the coverage.
    assert_eq!(
        encode_binary_time(2 * 1_000_000_000),
        vec![8, 0, 0, 0, 0, 0, 0, 0, 2]
    );
}

#[test]
fn binary_datetime_matches_go_dump_binary_datetime_vectors() {
    // Vectors generated from pkg/server/internal/dump/dump.go BinaryDateTime,
    // one per branch of Go's `t.Type()` switch.
    let packed = |year, month, day, hour, minute, second, microsecond| {
        PackedTime::from_parts(year, month, day, hour, minute, second, microsecond)
            .expect("valid packed time")
    };

    // DATETIME with microseconds: length 11, LE u16 year, m/d/h/mi/s, LE u32 micros.
    assert_eq!(
        encode_binary_datetime(
            packed(2017, 1, 5, 23, 59, 59, 575_601),
            BinaryDateTimeType::Datetime
        ),
        vec![11, 225, 7, 1, 5, 23, 59, 59, 113, 200, 8, 0]
    );
    // DATETIME with HH:MM:SS but no micros: length 7.
    assert_eq!(
        encode_binary_datetime(
            packed(2017, 1, 5, 23, 59, 59, 0),
            BinaryDateTimeType::Datetime
        ),
        vec![7, 225, 7, 1, 5, 23, 59, 59]
    );
    // DATETIME with an all-zero time: length 4, date only.
    assert_eq!(
        encode_binary_datetime(packed(2017, 1, 5, 0, 0, 0, 0), BinaryDateTimeType::Datetime),
        vec![4, 225, 7, 1, 5]
    );
    // Zero DATETIME: a single 0 length byte.
    assert_eq!(
        encode_binary_datetime(PackedTime::ZERO, BinaryDateTimeType::Datetime),
        vec![0]
    );
    // TIMESTAMP renders byte-for-byte like DATETIME.
    assert_eq!(
        encode_binary_datetime(
            packed(1999, 12, 31, 23, 59, 59, 999_999),
            BinaryDateTimeType::Timestamp
        ),
        vec![11, 207, 7, 12, 31, 23, 59, 59, 63, 66, 15, 0]
    );
    assert_eq!(
        encode_binary_datetime(
            packed(2000, 1, 1, 0, 0, 1, 0),
            BinaryDateTimeType::Timestamp
        ),
        vec![7, 208, 7, 1, 1, 0, 0, 1]
    );
    // DATE emits only YYYY-MM-DD (length 4).
    assert_eq!(
        encode_binary_datetime(packed(2020, 6, 15, 0, 0, 0, 0), BinaryDateTimeType::Date),
        vec![4, 228, 7, 6, 15]
    );
    // DATE discards any time/microsecond bits, exactly as Go's switch does.
    assert_eq!(
        encode_binary_datetime(
            packed(2020, 6, 15, 10, 20, 30, 123_456),
            BinaryDateTimeType::Date
        ),
        vec![4, 228, 7, 6, 15]
    );
    // Zero DATE: a single 0 length byte.
    assert_eq!(
        encode_binary_datetime(PackedTime::ZERO, BinaryDateTimeType::Date),
        vec![0]
    );
}

/// Go `ExecBinaryParam` builds one datum per parameter family. Each family
/// this leaf decodes is driven here from a real execute packet.
#[test]
fn execute_decodes_every_parameter_family() {
    // A NULL parameter is a bitmap bit and no value bytes at all.
    let mut null_packet = Vec::new();
    null_packet.extend_from_slice(&1_u32.to_le_bytes());
    null_packet.push(0);
    null_packet.extend_from_slice(&1_u32.to_le_bytes());
    null_packet.push(1); // the one parameter is NULL
    null_packet.push(1); // new types follow
    null_packet.extend_from_slice(&[0x08, 0]);
    assert_eq!(
        decode_prepared_statement_execute(&null_packet, 1, None)
            .expect("a NULL parameter decodes")
            .values,
        vec![PreparedValue::Null]
    );

    // Every other family carries its own value bytes.
    let cases: Vec<(&str, u8, u8, Vec<u8>, PreparedValue)> = vec![
        (
            "unsigned tiny",
            0x01,
            0x80,
            vec![0xff],
            PreparedValue::UnsignedLongLong(255),
        ),
        (
            "signed tiny",
            0x01,
            0,
            vec![0xff],
            PreparedValue::SignedLongLong(-1),
        ),
        (
            "unsigned bigint",
            0x08,
            0x80,
            u64::MAX.to_le_bytes().to_vec(),
            PreparedValue::UnsignedLongLong(u64::MAX),
        ),
        (
            "float",
            0x04,
            0,
            1.5_f32.to_bits().to_le_bytes().to_vec(),
            PreparedValue::Float(1.5),
        ),
        (
            "double",
            0x05,
            0,
            2.5_f64.to_bits().to_le_bytes().to_vec(),
            PreparedValue::Double(2.5),
        ),
        (
            "decimal",
            0xf6,
            0,
            {
                let digits = b"12.345";
                let mut encoded = vec![digits.len() as u8];
                encoded.extend_from_slice(digits);
                encoded
            },
            PreparedValue::Decimal(b"12.345".to_vec()),
        ),
    ];
    for (name, type_code, flag, value_bytes, expected) in cases {
        let packet = execute_payload_typed(1, type_code, flag, &value_bytes);
        assert_eq!(
            decode_prepared_statement_execute(&packet, 1, None)
                .unwrap_or_else(|error| panic!("{name} should decode: {error:?}"))
                .values,
            vec![expected],
            "{name}"
        );
    }
}

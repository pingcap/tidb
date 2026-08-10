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

//! Byte-for-byte parity checks for the binary prepared-statement parameter
//! splitter against Go `parseBinaryParams` (`pkg/server/conn_stmt_params.go`)
//! and its test `pkg/server/conn_stmt_params_test.go` (`TestParseExecArgs`,
//! `TestParseExecArgsMalformedLengthEncodedParam`, `TestParseExecArgsForDecimal`).
//!
//! `parseBinaryParams` is the splitter: it produces raw `BinaryParam` byte
//! slices before typed interpretation. These tests assert that package
//! boundary directly; the prepared-statement tests exercise the downstream
//! typed consumer.

#![allow(missing_docs)]

use tidb_protocol::{
    parse_binary_params, parse_length_encoded_int, BinaryParam, BinaryParamError, TYPE_NULL,
};

fn param(tp: u8, is_unsigned: bool, is_null: bool, val: &[u8]) -> BinaryParam {
    BinaryParam {
        tp,
        is_unsigned,
        is_null,
        val: val.to_vec(),
    }
}

// The single-parameter shape shared by every TestParseExecArgs case: one
// non-bound argument, a one-byte NULL bitmap, and a `[type, flag]` pair.
fn split_one(param_types: &[u8], param_values: &[u8]) -> Result<BinaryParam, BinaryParamError> {
    let parsed = parse_binary_params(1, &[None], &[0x0], param_types, param_values, "utf8mb4")?;
    assert_eq!(parsed.len(), 1);
    Ok(parsed.into_iter().next().unwrap())
}

#[test]
fn fixed_width_integer_params_slice_their_declared_width() {
    // TestParseExecArgs int-overflow inputs: TypeTiny/Short/Long carry 1/2/4
    // little-endian bytes. The splitter tags the raw bytes; the -1 the Go test
    // asserts is ExecBinaryParam's later signed interpretation.
    assert_eq!(
        split_one(&[1, 0], &[0xff]).unwrap(),
        param(1, false, false, &[0xff])
    );
    assert_eq!(
        split_one(&[2, 0], &[0xff, 0xff]).unwrap(),
        param(2, false, false, &[0xff, 0xff])
    );
    assert_eq!(
        split_one(&[3, 0], &[0xff, 0xff, 0xff, 0xff]).unwrap(),
        param(3, false, false, &[0xff, 0xff, 0xff, 0xff])
    );
}

#[test]
fn the_unsigned_flag_bit_is_recorded() {
    // paramTypes second byte 0x80 marks the value unsigned.
    assert_eq!(
        split_one(&[1, 0x80], &[0xff]).unwrap(),
        param(1, true, false, &[0xff])
    );
}

#[test]
fn temporal_params_read_one_leading_length_byte() {
    // Datetime with microseconds: leading 0x0b = 11 value bytes follow.
    assert_eq!(
        split_one(
            &[12, 0],
            &[0x0b, 0xda, 0x07, 0x0a, 0x11, 0x13, 0x1b, 0x1e, 0x01, 0x00, 0x00, 0x00]
        )
        .unwrap(),
        param(
            12,
            false,
            false,
            &[0xda, 0x07, 0x0a, 0x11, 0x13, 0x1b, 0x1e, 0x01, 0x00, 0x00, 0x00]
        )
    );
    // Date: leading 0x04 = 4 value bytes.
    assert_eq!(
        split_one(&[10, 0], &[0x04, 0xda, 0x07, 0x0a, 0x11]).unwrap(),
        param(10, false, false, &[0xda, 0x07, 0x0a, 0x11])
    );
    // Datetime with only HH:MM:SS: leading 0x07 = 7 value bytes.
    assert_eq!(
        split_one(&[7, 0], &[0x07, 0xda, 0x07, 0x0a, 0x11, 0x13, 0x1b, 0x1e]).unwrap(),
        param(7, false, false, &[0xda, 0x07, 0x0a, 0x11, 0x13, 0x1b, 0x1e])
    );
    // A zero temporal value: leading 0x00 = no value bytes.
    assert_eq!(
        split_one(&[12, 0], &[0x00]).unwrap(),
        param(12, false, false, &[])
    );
    // A zero Duration value likewise.
    assert_eq!(
        split_one(&[11, 0], &[0x00]).unwrap(),
        param(11, false, false, &[])
    );
}

#[test]
fn decimal_and_string_params_read_a_length_encoded_int() {
    // TestParseExecArgsForDecimal "1": TypeNewDecimal (0xf6), lenenc length 1.
    assert_eq!(
        split_one(&[0xf6, 0], &[0x1, b'1']).unwrap(),
        param(0xf6, false, false, b"1")
    );
    // String group (VarString 0xfd): lenenc length 3, decoded (utf8 identity).
    assert_eq!(
        split_one(&[0xfd, 0], &[0x03, b'a', b'b', b'c']).unwrap(),
        param(0xfd, false, false, b"abc")
    );
    // A length-encoded NULL marker (0xfb) surfaces as an is_null parameter.
    assert_eq!(
        split_one(&[0xfd, 0], &[0xfb]).unwrap(),
        param(0xfd, false, true, &[])
    );
}

#[test]
fn a_declared_type_null_param_is_null_with_no_bytes() {
    // The switch's TypeNull arm: length 0, is_null true.
    assert_eq!(
        split_one(&[TYPE_NULL, 0], &[]).unwrap(),
        param(TYPE_NULL, false, true, &[])
    );
}

#[test]
fn the_null_bitmap_yields_a_type_null_param() {
    // Bit 0 set in the bitmap marks parameter 0 absent; Go emits BinaryParam{Tp:
    // TypeNull} with the is_null field left false (the tag itself is the signal).
    let parsed = parse_binary_params(1, &[None], &[0x1], &[1, 0], &[], "utf8mb4").unwrap();
    assert_eq!(parsed, vec![param(TYPE_NULL, false, false, &[])]);
}

#[test]
fn a_send_long_data_bound_param_is_used_directly() {
    // A value delivered earlier via COM_STMT_SEND_LONG_DATA: declared String
    // (0xfe) keeps its type and passes through the utf8-identity decoder.
    let bound: &[u8] = b"xyz";
    let parsed = parse_binary_params(1, &[Some(bound)], &[0x0], &[0xfe, 0], &[], "utf8mb4").unwrap();
    assert_eq!(parsed, vec![param(0xfe, false, false, b"xyz")]);
}

#[test]
fn positions_advance_across_multiple_params() {
    // Two params share one value buffer; the second must start where the first
    // ended: Tiny(1 byte) then Short(2 bytes).
    let parsed =
        parse_binary_params(2, &[None, None], &[0x0], &[1, 0, 2, 0], &[0x05, 0x06, 0x07], "utf8mb4")
        .unwrap();
    assert_eq!(
        parsed,
        vec![
            param(1, false, false, &[0x05]),
            param(2, false, false, &[0x06, 0x07]),
        ]
    );
}

#[test]
fn malformed_packets_are_rejected() {
    // The "For error test" cases in TestParseExecArgs, plus the malformed
    // length-encoded cases from TestParseExecArgsMalformedLengthEncodedParam.
    // Timestamp/Duration whose leading length byte overruns the buffer.
    assert_eq!(
        split_one(&[7, 0], &[10]).unwrap_err(),
        BinaryParamError::MalformedPacket
    );
    assert_eq!(
        split_one(&[11, 0], &[10]).unwrap_err(),
        BinaryParamError::MalformedPacket
    );
    assert_eq!(
        split_one(&[11, 0], &[8, 2]).unwrap_err(),
        BinaryParamError::MalformedPacket
    );
    // Truncated length-encoded uint64 header (0xfe with no following bytes).
    assert_eq!(
        split_one(&[0xfd, 0], &[0xfe]).unwrap_err(),
        BinaryParamError::MalformedPacket
    );
    // Overflowing length-encoded uint64 (1 << 63): must reject, never panic.
    assert_eq!(
        split_one(
            &[0xfd, 0],
            &[0xfe, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x80]
        )
        .unwrap_err(),
        BinaryParamError::MalformedPacket
    );
}

#[test]
fn an_unknown_field_type_is_reported() {
    // TypeDecimal (0) is TypeUnspecified, handled; a truly unmapped code such as
    // 0x9f is errUnknownFieldType. (0xff Geometry IS mapped, so pick an unused.)
    let error = split_one(&[0x9f, 0], &[0x00]).unwrap_err();
    assert_eq!(
        error,
        BinaryParamError::UnknownFieldType { type_code: 0x9f }
    );
    assert_eq!(error.to_string(), "stmt unknown field type 159");
    assert_eq!(error.mysql_error_code(), Some(8051));
    assert_eq!(BinaryParamError::MalformedPacket.mysql_error_code(), None);
    assert_eq!(
        BinaryParamError::MalformedPacket.to_string(),
        "malform packet error"
    );
}

#[test]
fn length_encoded_int_matches_the_mysql_widths() {
    // Directly exercise the ported util.ParseLengthEncodedInt widths.
    assert_eq!(parse_length_encoded_int(&[0x00]), Some((0, false, 1)));
    assert_eq!(parse_length_encoded_int(&[0xfa]), Some((250, false, 1)));
    assert_eq!(parse_length_encoded_int(&[0xfb]), Some((0, true, 1)));
    assert_eq!(
        parse_length_encoded_int(&[0xfc, 0x01, 0x02]),
        Some((0x0201, false, 3))
    );
    assert_eq!(
        parse_length_encoded_int(&[0xfd, 0x01, 0x02, 0x03]),
        Some((0x0003_0201, false, 4))
    );
    assert_eq!(
        parse_length_encoded_int(&[0xfe, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08]),
        Some((0x0807_0605_0403_0201, false, 9))
    );
    // Truncation returns None (Go io.EOF).
    assert_eq!(parse_length_encoded_int(&[]), None);
    assert_eq!(parse_length_encoded_int(&[0xfc, 0x01]), None);
    assert_eq!(parse_length_encoded_int(&[0xfe, 0x01]), None);
}

/// Go `TestParseExecArgsAndEncode` (`pkg/server/conn_stmt_params_test.go:319`):
/// a parameter from a client whose charset is not UTF-8 is decoded through the
/// connection's `InputDecoder` before it becomes a value.
///
/// Both Go rows use the same gbk bytes `b2 e2 ca d4` for `测试`, once as an
/// inline `TypeVarchar` value (length-prefixed) and once as a
/// `COM_STMT_SEND_LONG_DATA` bound value declared `TypeString`. The decode
/// happens on both paths, so both are asserted; the two rows after them pin
/// the boundary of the decode, which is what makes the charset argument
/// load-bearing rather than decorative.
#[test]
fn a_gbk_client_string_param_is_decoded_to_utf8() {
    const GBK_TEST: &[u8] = &[0xb2, 0xe2, 0xca, 0xd4];
    let expected = "测试".as_bytes();

    // Row 1: inline TypeVarchar value, `[len, bytes...]`.
    let mut values = vec![u8::try_from(GBK_TEST.len()).unwrap()];
    values.extend_from_slice(GBK_TEST);
    let parsed = parse_binary_params(1, &[None], &[0x0], &[15, 0], &values, "gbk").unwrap();
    assert_eq!(parsed[0].val, expected, "an inline gbk varchar parameter");

    // Row 2: the value arrived through COM_STMT_SEND_LONG_DATA, declared
    // TypeString, so the bound branch decodes it too.
    let parsed = parse_binary_params(1, &[Some(GBK_TEST)], &[0x0], &[254, 0], &[], "gbk").unwrap();
    assert_eq!(parsed[0].val, expected, "a bound gbk string parameter");

    // The same bytes on a utf8mb4 connection are NOT transformed
    // (`FindEncodingTakeUTF8AsNoop`), which is why the charset must travel
    // with the parameters rather than being assumed.
    let parsed = parse_binary_params(1, &[None], &[0x0], &[15, 0], &values, "utf8mb4").unwrap();
    assert_eq!(parsed[0].val, GBK_TEST, "utf8mb4 input is a no-op");

    // A BLOB parameter is not in the string group at all: Go never gives it
    // to the decoder, so its bytes stay raw even on a gbk connection.
    let parsed = parse_binary_params(1, &[None], &[0x0], &[252, 0], &values, "gbk").unwrap();
    assert_eq!(parsed[0].val, GBK_TEST, "a BLOB parameter is not decoded");
}

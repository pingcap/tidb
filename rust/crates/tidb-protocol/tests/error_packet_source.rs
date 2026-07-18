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

use tidb_protocol::{encode_error_packet, ErrorPacket, ERR_HEADER};

#[test]
fn write_error_protocol_41_matches_go_field_order() {
    // Source: pkg/server/conn.go::clientConn.writeError. The Go writer emits
    // the ERR header, little-endian code, '#', SQLSTATE, then message bytes.
    let packet = ErrorPacket::new(1105, b"HY000", b"unknown error", true);
    assert_eq!(
        encode_error_packet(&packet),
        [
            ERR_HEADER, 0x51, 0x04, b'#', b'H', b'Y', b'0', b'0', b'0', b'u', b'n', b'k', b'n',
            b'o', b'w', b'n', b' ', b'e', b'r', b'r', b'o', b'r',
        ]
    );
}

#[test]
fn write_error_legacy_omits_protocol_41_sqlstate() {
    // Companion source coverage: pkg/parser/mysql/error_test.go::TestSQLError
    // constructs both mapped and custom SQLError values before conn.go writes
    // their wire representation.
    let packet = ErrorPacket::new(1047, b"08S01", b"Unknown command", false);
    assert_eq!(
        encode_error_packet(&packet),
        [
            ERR_HEADER, 0x17, 0x04, b'U', b'n', b'k', b'n', b'o', b'w', b'n', b' ', b'c', b'o',
            b'm', b'm', b'a', b'n', b'd'
        ]
    );
}

#[test]
fn write_error_preserves_custom_code_state_and_message_bytes() {
    // `mysql.NewErrf` accepts an arbitrary code and message; no errno table or
    // UTF-8 replacement belongs in this protocol leaf.
    let packet = ErrorPacket::new(0, [0xff, 0x00, b'X'], [0x80, 0x00, b'!'], true);
    assert_eq!(
        encode_error_packet(&packet),
        [ERR_HEADER, 0x00, 0x00, b'#', 0xff, 0x00, b'X', 0x80, 0x00, b'!']
    );
}

#[test]
fn write_error_legacy_does_not_emit_state_even_if_custom_bytes_exist() {
    let packet = ErrorPacket::new(1, *b"#HY", b"message", false);
    assert_eq!(
        encode_error_packet(&packet),
        [ERR_HEADER, 0x01, 0x00, b'm', b'e', b's', b's', b'a', b'g', b'e']
    );
}

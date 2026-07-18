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
    encode_error_packet, error_packet_from_descriptor, ErrorDescriptor, ErrorKind,
    MYSQL_ERR_BAD_FIELD, MYSQL_ERR_DUP_ENTRY, MYSQL_ERR_NOT_SUPPORTED_YET, MYSQL_ERR_PARSE,
    MYSQL_ERR_UNKNOWN,
};

#[test]
fn terror_mysql_mapping_preserves_code_state_and_message() {
    // Source: pkg/parser/terror/terror.go::ToSQLError and
    // pkg/parser/mysql/error.go::NewErrf. The converter consumes an already
    // rendered message and only supplies the registered errno/SQLSTATE.
    let descriptor = ErrorDescriptor::new(ErrorKind::UnknownColumn, b"unknown column");
    let packet = error_packet_from_descriptor(&descriptor, true);
    assert_eq!(packet.code, MYSQL_ERR_BAD_FIELD);
    assert_eq!(packet.state, b"42S22");
    assert_eq!(packet.message, b"unknown column");
    assert!(packet.protocol_41);
}

#[test]
fn protocol_41_and_legacy_inputs_only_change_packet_state_flag() {
    let descriptor = ErrorDescriptor::new(ErrorKind::Parse, b"syntax error");
    let protocol_41 = error_packet_from_descriptor(&descriptor, true);
    let legacy = error_packet_from_descriptor(&descriptor, false);

    assert!(protocol_41.protocol_41);
    assert!(!legacy.protocol_41);
    assert_eq!(protocol_41.code, MYSQL_ERR_PARSE);
    assert_eq!(legacy.code, MYSQL_ERR_PARSE);
    assert_eq!(protocol_41.state, legacy.state);
    assert_eq!(
        encode_error_packet(&protocol_41),
        [
            0xff, 0x28, 0x04, b'#', b'4', b'2', b'0', b'0', b'0', b's', b'y', b'n', b't', b'a',
            b'x', b' ', b'e', b'r', b'r', b'o', b'r'
        ]
    );
    assert_eq!(
        encode_error_packet(&legacy),
        [
            0xff, 0x28, 0x04, b's', b'y', b'n', b't', b'a', b'x', b' ', b'e', b'r', b'r', b'o',
            b'r'
        ]
    );
}

#[test]
fn mapped_exec_categories_use_only_existing_mysql_codes() {
    assert_eq!(
        error_packet_from_descriptor(
            &ErrorDescriptor::new(ErrorKind::DuplicateKey, b"duplicate"),
            true,
        )
        .code,
        MYSQL_ERR_DUP_ENTRY
    );
    assert_eq!(
        error_packet_from_descriptor(
            &ErrorDescriptor::new(ErrorKind::NotSupportedYet, b"not supported"),
            true,
        )
        .code,
        MYSQL_ERR_NOT_SUPPORTED_YET
    );
}

#[test]
fn categories_without_an_exact_source_mapping_are_explicit_unknown() {
    // Foreign-key child/parent failures use different Go errno values. The
    // generic ExecError variant must not guess one, nor should protocol
    // framing manufacture context that the session has not supplied.
    let descriptor = ErrorDescriptor::new(ErrorKind::ForeignKeyViolation, [0xff, 0x00, b'!']);
    let packet = error_packet_from_descriptor(&descriptor, true);
    assert_eq!(packet.code, MYSQL_ERR_UNKNOWN);
    assert_eq!(packet.state, b"HY000");
    assert_eq!(packet.message, [0xff, 0x00, b'!']);
}

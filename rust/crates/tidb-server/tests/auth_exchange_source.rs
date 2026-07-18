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

use tidb_server::{
    decode_client_packet, AuthExchangeError, AuthMoreData, AuthSwitchRequest,
    AUTH_MORE_DATA_PREFIX, AUTH_SWITCH_REQUEST,
};

#[test]
fn auth_switch_request_preserves_go_plugin_and_challenge_field_order() {
    // Source: pkg/privilege/conn/conn.go:19-30 and pkg/server/conn.go:276-326.
    // The request is only a wire envelope; the selected plugin owns the
    // challenge and the later client response.
    let request = AuthSwitchRequest::new("caching_sha2_password", [1_u8, 2, 3]).expect("request");
    assert_eq!(
        request.encode_payload(),
        [
            AUTH_SWITCH_REQUEST,
            b'c',
            b'a',
            b'c',
            b'h',
            b'i',
            b'n',
            b'g',
            b'_',
            b's',
            b'h',
            b'a',
            b'2',
            b'_',
            b'p',
            b'a',
            b's',
            b's',
            b'w',
            b'o',
            b'r',
            b'd',
            0,
            1,
            2,
            3,
            0,
        ]
    );
    let framed = request.encode_packet(7).expect("frame");
    assert_eq!(&framed[..4], &[27, 0, 0, 7]);
    assert_eq!(
        AuthSwitchRequest::parse_payload(&request.encode_payload()),
        Ok(request)
    );
}

#[test]
fn auth_more_data_adds_only_the_protocol_prefix() {
    // Source: pkg/privilege/conn/conn.go:19-30 and pkg/server/conn.go:2901-2907.
    let more = AuthMoreData::new([0x10_u8, 0x00, 0xff]);
    assert_eq!(
        more.encode_payload(),
        [AUTH_MORE_DATA_PREFIX, 0x10, 0x00, 0xff]
    );
    let framed = more.encode_packet(4).expect("frame");
    assert_eq!(&framed[..4], &[4, 0, 0, 4]);
    assert_eq!(
        AuthMoreData::parse_payload(&more.encode_payload()),
        Ok(more)
    );
    assert_eq!(
        AuthMoreData::parse_payload(&[AUTH_SWITCH_REQUEST]),
        Err(AuthExchangeError::UnexpectedHeader {
            expected: AUTH_MORE_DATA_PREFIX,
            received: Some(AUTH_SWITCH_REQUEST),
        })
    );
}

#[test]
fn client_auth_response_is_opaque_and_framed_without_verification() {
    // Source: pkg/privilege/conn/conn.go:19-30. ReadPacket returns the
    // plugin response bytes; password comparison belongs to AuthenticateUser
    // and is intentionally not implemented by this transport leaf.
    let framed = [3_u8, 0, 0, 9, 0xde, 0xad, 0xbe];
    let response = decode_client_packet(&framed, 9).expect("client packet");
    assert_eq!(response.bytes, [0xde, 0xad, 0xbe]);
}

#[test]
fn auth_switch_parser_requires_both_nul_terminated_fields() {
    assert_eq!(
        AuthSwitchRequest::parse_payload(&[AUTH_SWITCH_REQUEST, b'a']),
        Err(AuthExchangeError::Malformed(
            "auth switch plugin is not NUL terminated",
        ))
    );
    assert_eq!(
        AuthSwitchRequest::parse_payload(&[AUTH_SWITCH_REQUEST, b'a', 0, b'x']),
        Err(AuthExchangeError::Malformed(
            "auth switch challenge is not NUL terminated",
        ))
    );
    assert_eq!(
        AuthSwitchRequest::new("a\0b", Vec::<u8>::new()),
        Err(AuthExchangeError::EmbeddedNul("client auth plugin"))
    );
}

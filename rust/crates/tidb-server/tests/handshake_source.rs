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

use std::io::Cursor;

use tidb_protocol::PacketReader;
use tidb_server::handshake::{
    negotiate_capabilities, parse_response, parse_response_header, AuthHandshake,
    AuthHandshakePacket, AuthHandshakePhase, AuthPluginAction, AUTH_NATIVE_PASSWORD,
    CLIENT_CONNECT_ATTRS, CLIENT_CONNECT_WITH_DB, CLIENT_PLUGIN_AUTH, CLIENT_PROTOCOL_41,
    CLIENT_SECURE_CONNECTION, CLIENT_SSL, DEFAULT_COLLATION_ID, SERVER_STATUS_AUTOCOMMIT,
};
use tidb_server::handshake::{HandshakeError, InitialHandshake};

fn lenenc(value: usize) -> Vec<u8> {
    if value <= 250 {
        vec![value as u8]
    } else if value <= u16::MAX as usize {
        let mut bytes = vec![0xfc];
        bytes.extend_from_slice(&(value as u16).to_le_bytes());
        bytes
    } else {
        panic!("test helper only supports short lengths")
    }
}

fn response_header(capability: u32, collation: u8) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(32);
    bytes.extend_from_slice(&capability.to_le_bytes());
    bytes.extend_from_slice(&0_u32.to_le_bytes());
    bytes.push(collation);
    bytes.extend_from_slice(&[0; 23]);
    bytes
}

#[test]
fn initial_handshake_matches_go_field_order_and_packet_framing() {
    let handshake = InitialHandshake {
        connection_id: 1,
        salt: (1..=20).collect(),
        capability: 0x1234_5678,
        collation: 0,
        status_flags: SERVER_STATUS_AUTOCOMMIT,
        server_version: "5.7.25-TiDB".to_owned(),
        auth_plugin: "mysql_native_password".to_owned(),
    };
    let payload = handshake.encode_payload().expect("payload");
    assert_eq!(payload[0], 10);
    assert_eq!(&payload[1..13], b"5.7.25-TiDB\0");
    assert_eq!(&payload[13..17], &1_u32.to_le_bytes());
    assert_eq!(&payload[17..25], &[1, 2, 3, 4, 5, 6, 7, 8]);
    assert_eq!(payload[25], 0);
    assert_eq!(&payload[26..28], &0x5678_u16.to_le_bytes());
    assert_eq!(payload[28], DEFAULT_COLLATION_ID);
    assert_eq!(&payload[29..31], &SERVER_STATUS_AUTOCOMMIT.to_le_bytes());
    assert_eq!(&payload[31..33], &0x1234_u16.to_le_bytes());
    assert_eq!(payload[33], 21);
    assert_eq!(&payload[34..44], &[0; 10]);
    assert_eq!(
        &payload[44..57],
        &[9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 0]
    );
    assert!(payload.ends_with(b"mysql_native_password\0"));

    let packet = handshake.encode_packet().expect("packet");
    let mut reader = PacketReader::new(Cursor::new(packet));
    assert_eq!(reader.read_packet().expect("packet payload"), payload);
}

#[test]
fn initial_handshake_rejects_unsafe_salt_and_terminated_strings() {
    let mut handshake = InitialHandshake {
        connection_id: 1,
        salt: vec![0; 7],
        capability: 0,
        collation: DEFAULT_COLLATION_ID,
        status_flags: 0,
        server_version: "v".to_owned(),
        auth_plugin: "auth".to_owned(),
    };
    assert!(matches!(
        handshake.encode_payload(),
        Err(HandshakeError::InvalidSaltLength(7))
    ));
    handshake.salt = vec![0; 8];
    handshake.server_version.push('\0');
    assert!(matches!(
        handshake.encode_payload(),
        Err(HandshakeError::EmbeddedNul("server version"))
    ));
}

#[test]
fn response_parser_preserves_source_header_body_and_attributes() {
    let capability = CLIENT_PROTOCOL_41
        | CLIENT_SECURE_CONNECTION
        | CLIENT_CONNECT_WITH_DB
        | CLIENT_PLUGIN_AUTH
        | CLIENT_CONNECT_ATTRS;
    let mut data = response_header(capability, 45);
    data.extend_from_slice(b"root\0");
    data.push(3);
    data.extend_from_slice(b"abc");
    data.extend_from_slice(b"test\0mysql_native_password\0");
    let attrs = [2, b'a', b'b', 2, b'c', b'd'];
    data.extend_from_slice(&lenenc(attrs.len()));
    data.extend_from_slice(&attrs);

    let response = parse_response(&data).expect("response");
    assert_eq!(response.capability, capability);
    assert_eq!(response.collation, 45);
    assert_eq!(response.user, "root");
    assert_eq!(response.db_name, "test");
    assert_eq!(response.auth_plugin, "mysql_native_password");
    assert_eq!(response.auth, b"abc");
    assert_eq!(response.attrs.get("ab"), Some(&"cd".to_owned()));
}

#[test]
fn response_header_rejects_short_packets_and_capability_negotiation_is_intersection() {
    assert!(parse_response_header(&[0]).is_err());
    assert_eq!(
        negotiate_capabilities(CLIENT_PROTOCOL_41 | CLIENT_PLUGIN_AUTH, CLIENT_PROTOCOL_41)
            .expect("protocol 4.1 negotiation"),
        CLIENT_PROTOCOL_41
    );
    assert!(matches!(
        negotiate_capabilities(CLIENT_PLUGIN_AUTH, u32::MAX),
        Err(HandshakeError::MissingCapability(CLIENT_PROTOCOL_41))
    ));
}

#[test]
fn response_parser_reports_malformed_lengths_without_panicking() {
    let capability = CLIENT_PROTOCOL_41 | CLIENT_SECURE_CONNECTION;
    let valid_prefix = response_header(capability, 0);
    for end in 0..=valid_prefix.len() + 5 {
        let mut data = valid_prefix[..end.min(valid_prefix.len())].to_vec();
        if end > valid_prefix.len() {
            data.extend_from_slice(&b"root\0"[..(end - valid_prefix.len()).min(5)]);
        }
        let result = std::panic::catch_unwind(|| parse_response(&data));
        assert!(result.is_ok(), "parser panicked for {data:?}");
    }

    let mut truncated = response_header(capability, 0);
    truncated.extend_from_slice(b"root\0");
    assert!(parse_response(&truncated).is_err());

    let attrs_capability = CLIENT_PROTOCOL_41 | CLIENT_CONNECT_ATTRS;
    let mut malformed_attrs = response_header(attrs_capability, 0);
    malformed_attrs.extend_from_slice(b"root\0\0");
    malformed_attrs.extend_from_slice(&[0xfc, 0xff]);
    assert!(parse_response(&malformed_attrs).is_err());
}

#[test]
fn auth_handshake_preserves_raw_response_and_defers_identity_plugin_selection() {
    // Source: pkg/server/conn.go:593-714, 721-755, 939-1040.  Parsing and
    // capability intersection are separate from user lookup/password
    // verification; the raw auth response remains available to that owner.
    let capability = CLIENT_PROTOCOL_41 | CLIENT_SECURE_CONNECTION | CLIENT_PLUGIN_AUTH;
    let mut packet = response_header(capability, 45);
    packet.extend_from_slice(b"root\0");
    packet.push(3);
    packet.extend_from_slice(b"abc");
    packet.extend_from_slice(b"mysql_native_password\0");
    let raw_packet = packet.clone();

    let mut handshake = AuthHandshake::new(capability, AUTH_NATIVE_PASSWORD);
    let request = match handshake.receive_packet(&packet).expect("auth response") {
        AuthHandshakePacket::Authentication(request) => request,
        AuthHandshakePacket::TlsRequest { .. } => panic!("full response became TLS request"),
    };
    assert_eq!(request.raw_packet, raw_packet);
    assert_eq!(request.response.auth, b"abc");
    assert_eq!(request.negotiated_capability, capability);
    assert_eq!(
        handshake.phase(),
        &AuthHandshakePhase::AuthenticationPending(Box::new(request.clone()))
    );
    assert_eq!(
        handshake
            .auth_plugin_action(None)
            .expect("matching advertised plugin"),
        AuthPluginAction::UseClientPlugin
    );
    assert_eq!(
        handshake
            .auth_plugin_action(Some("caching_sha2_password"))
            .expect("identity plugin switch"),
        AuthPluginAction::RequestSwitch {
            plugin: "caching_sha2_password".to_owned()
        }
    );
}

#[test]
fn auth_handshake_models_legacy_plugin_fallback_without_authentication() {
    // Source: pkg/server/conn.go:749-755 and 1007-1040.  A legacy client
    // falls back only to mysql_native_password; a configured non-native
    // plugin is an explicit unsupported boundary, not a fake success.
    let capability = CLIENT_PROTOCOL_41 | CLIENT_SECURE_CONNECTION;
    let mut packet = response_header(capability, 0);
    packet.extend_from_slice(b"root\0");
    packet.push(0);

    let mut handshake = AuthHandshake::new(capability, AUTH_NATIVE_PASSWORD);
    handshake.receive_packet(&packet).expect("legacy response");
    assert_eq!(
        handshake.auth_plugin_action(None).expect("native fallback"),
        AuthPluginAction::NativePasswordFallback
    );
    assert_eq!(
        handshake
            .auth_plugin_action(Some(AUTH_NATIVE_PASSWORD))
            .expect("native fallback"),
        AuthPluginAction::NativePasswordFallback
    );
    assert_eq!(
        handshake
            .auth_plugin_action(Some("caching_sha2_password"))
            .expect("legacy rejection"),
        AuthPluginAction::RejectLegacyClient {
            required_plugin: "caching_sha2_password".to_owned()
        }
    );
}

#[test]
fn auth_handshake_requires_transport_owner_for_ssl_request() {
    // Source: pkg/server/conn.go:627-655.  The parser recognizes an exact
    // SSLRequest and exposes the bytes; it does not pretend to perform TLS.
    let capability = CLIENT_PROTOCOL_41 | CLIENT_SSL;
    let packet = response_header(capability, 45);
    let mut handshake = AuthHandshake::new(capability, AUTH_NATIVE_PASSWORD);

    let tls = match handshake.receive_packet(&packet).expect("SSLRequest") {
        AuthHandshakePacket::TlsRequest {
            negotiated_capability,
            collation,
            raw_packet,
        } => {
            assert_eq!(negotiated_capability, capability);
            assert_eq!(collation, 45);
            assert_eq!(raw_packet, packet);
            raw_packet
        }
        AuthHandshakePacket::Authentication(_) => panic!("SSLRequest parsed as auth response"),
    };
    assert_eq!(
        handshake.phase(),
        &AuthHandshakePhase::TlsRequested {
            negotiated_capability: capability,
            collation: 45,
        }
    );
    assert!(matches!(
        handshake.receive_packet(&tls),
        Err(tidb_server::handshake::HandshakeError::InvalidState(_))
    ));
    handshake
        .tls_established()
        .expect("transport TLS transition");

    let response_capability = capability | CLIENT_SECURE_CONNECTION | CLIENT_PLUGIN_AUTH;
    let mut response = response_header(response_capability, 45);
    response.extend_from_slice(b"root\0");
    response.push(0);
    response.extend_from_slice(b"mysql_native_password\0");
    assert!(matches!(
        handshake.receive_packet(&response),
        Ok(AuthHandshakePacket::Authentication(_))
    ));
}

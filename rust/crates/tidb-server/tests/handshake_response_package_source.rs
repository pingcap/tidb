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

//! Complete-package contract tests for `pkg/server/internal/handshake`.

use std::collections::HashMap;

use tidb_server::handshake::{
    CLIENT_CONNECT_ATTRS, CLIENT_CONNECT_WITH_DB, CLIENT_PLUGIN_AUTH, CLIENT_PROTOCOL_41,
    CLIENT_SECURE_CONNECTION, CLIENT_ZSTD_COMPRESSION_ALGORITHM,
};
use tidb_server::{parse_response, HandshakeResponse41};

fn response_header(capability: u32, collation: u8) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(32);
    bytes.extend_from_slice(&capability.to_le_bytes());
    bytes.extend_from_slice(&0_u32.to_le_bytes());
    bytes.push(collation);
    bytes.extend_from_slice(&[0; 23]);
    bytes
}

fn lenenc_string(value: &[u8]) -> Vec<u8> {
    assert!(value.len() <= 250);
    let mut encoded = Vec::with_capacity(value.len() + 1);
    encoded.push(value.len() as u8);
    encoded.extend_from_slice(value);
    encoded
}

#[test]
fn response41_default_matches_go_zero_value_semantics() {
    let response = HandshakeResponse41::default();

    assert!(response.attrs.is_empty());
    assert!(response.user.is_empty());
    assert!(response.db_name.is_empty());
    assert!(response.auth_plugin.is_empty());
    assert!(response.auth.is_empty());
    assert_eq!(response.zstd_level, 0_i32);
    assert_eq!(response.capability, 0_u32);
    assert_eq!(response.collation, 0_u8);
}

#[test]
fn response41_owns_every_field_and_clones_without_aliasing() {
    let response = HandshakeResponse41 {
        attrs: HashMap::from([("program_name".to_owned(), "mysql".to_owned())]),
        user: "root".to_owned(),
        db_name: "test".to_owned(),
        auth_plugin: "mysql_native_password".to_owned(),
        auth: vec![1, 2, 3],
        zstd_level: 17_i32,
        capability: 0x1234_5678,
        collation: 45,
    };
    let mut clone = response.clone();
    clone
        .attrs
        .insert("program_name".to_owned(), "changed".to_owned());
    clone.user.push_str("-changed");
    clone.db_name.push_str("-changed");
    clone.auth_plugin.push_str("-changed");
    clone.auth.push(4);

    assert_eq!(response.attrs["program_name"], "mysql");
    assert_eq!(response.user, "root");
    assert_eq!(response.db_name, "test");
    assert_eq!(response.auth_plugin, "mysql_native_password");
    assert_eq!(response.auth, [1, 2, 3]);
    assert_eq!(response.zstd_level, 17_i32);
    assert_eq!(response.capability, 0x1234_5678);
    assert_eq!(response.collation, 45);
}

#[test]
fn parser_populates_the_complete_response41_contract() {
    let capability = CLIENT_PROTOCOL_41
        | CLIENT_SECURE_CONNECTION
        | CLIENT_CONNECT_WITH_DB
        | CLIENT_PLUGIN_AUTH
        | CLIENT_CONNECT_ATTRS
        | CLIENT_ZSTD_COMPRESSION_ALGORITHM;
    let mut packet = response_header(capability, 45);
    packet.extend_from_slice(b"root\0");
    packet.push(3);
    packet.extend_from_slice(&[1, 2, 3]);
    packet.extend_from_slice(b"test\0mysql_native_password\0");
    let mut attrs = lenenc_string(b"program_name");
    attrs.extend_from_slice(&lenenc_string(b"mysql"));
    packet.extend_from_slice(&lenenc_string(&attrs));
    packet.push(u8::MAX);

    assert_eq!(
        parse_response(&packet).expect("complete HandshakeResponse41"),
        HandshakeResponse41 {
            attrs: HashMap::from([("program_name".to_owned(), "mysql".to_owned())]),
            user: "root".to_owned(),
            db_name: "test".to_owned(),
            auth_plugin: "mysql_native_password".to_owned(),
            auth: vec![1, 2, 3],
            zstd_level: i32::from(u8::MAX),
            capability,
            collation: 45,
        }
    );
}

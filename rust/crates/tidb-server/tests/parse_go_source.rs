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

use std::sync::Arc;
use tidb_server::handshake::{
    parse_response, parse_response_body_into_with_attrs_state, parse_response_header_into,
    parse_response_with_attrs_state, ConnectionAttrsState, CLIENT_CONNECT_ATTRS,
    CLIENT_CONNECT_WITH_DB, CLIENT_PLUGIN_AUTH, CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA,
    CLIENT_PROTOCOL_41, CLIENT_SECURE_CONNECTION, CLIENT_ZSTD_COMPRESSION_ALGORITHM,
};
use tidb_server::HandshakeResponse41;

fn header(capability: u32) -> Vec<u8> {
    let mut packet = capability.to_le_bytes().to_vec();
    packet.extend_from_slice(&0_u32.to_le_bytes());
    packet.push(0);
    packet.extend_from_slice(&[0; 23]);
    packet
}

fn lenenc(value: usize) -> Vec<u8> {
    match value {
        0..=250 => vec![value as u8],
        251..=0xffff => {
            let mut bytes = vec![0xfc];
            bytes.extend_from_slice(&(value as u16).to_le_bytes());
            bytes
        }
        0x1_0000..=0xff_ffff => vec![0xfd, value as u8, (value >> 8) as u8, (value >> 16) as u8],
        _ => panic!("test helper length is too large"),
    }
}

fn response_with_attrs(attrs: &[u8]) -> Vec<u8> {
    let mut packet = header(CLIENT_PROTOCOL_41 | CLIENT_CONNECT_ATTRS);
    packet.extend_from_slice(b"root\0\0");
    packet.extend_from_slice(&lenenc(attrs.len()));
    packet.extend_from_slice(attrs);
    packet
}

/// `parseAttrs` uses Go's default `ConnectAttrsSize` of 4096 bytes. The
/// aggregate counts decoded key/value bytes, not length-encoding overhead.
#[test]
fn default_attribute_limit_truncates_at_the_first_overflowing_pair() {
    let mut attrs = vec![1, b'k'];
    attrs.extend_from_slice(&lenenc(4096));
    attrs.extend(std::iter::repeat_n(b'v', 4096));

    let response = parse_response(&response_with_attrs(&attrs)).expect("Go returns nil error");
    assert_eq!(response.attrs.len(), 1);
    assert_eq!(
        response.attrs.get("_truncated").map(String::as_str),
        Some("4097")
    );
    assert!(!response.attrs.contains_key("k"));
}

/// `HandshakeResponseBody` logs and ignores a decoded-attribute error. The
/// handshake remains valid and no partial attribute map is installed.
#[test]
fn malformed_attribute_rows_are_ignored_after_the_frame_is_valid() {
    let malformed = [2, b'a'];
    let response = parse_response(&response_with_attrs(&malformed))
        .expect("Go swallows parseAttrs errors after validating the outer frame");
    assert!(response.attrs.is_empty());
}

/// Go strings preserve arbitrary wire bytes. A parser may reject them later,
/// but parsing must not replace `0xff` with UTF-8 U+FFFD.
#[test]
fn handshake_identity_preserves_non_utf8_wire_bytes() {
    let mut packet = header(CLIENT_PROTOCOL_41);
    packet.extend_from_slice(&[0xff, 0, 0]);
    let response = parse_response(&packet).expect("Go accepts byte strings");
    assert_eq!(response.user.as_bytes(), &[0xff]);
}

/// Go's plugin branch deliberately handles an unterminated final plugin name
/// as an unexpected packet: it leaves AuthPlugin empty and advances zero
/// bytes rather than turning the otherwise complete response into an error.
#[test]
fn unterminated_final_auth_plugin_is_ignored() {
    let mut packet = header(CLIENT_PROTOCOL_41 | CLIENT_PLUGIN_AUTH);
    packet.extend_from_slice(b"root\0\0caching_sha2_password");
    let response = parse_response(&packet).expect("Go returns nil error");
    assert!(response.auth_plugin.is_empty());
}

#[test]
fn header_and_body_mutation_order_matches_go_on_failure() {
    let mut response = HandshakeResponse41 {
        capability: 7,
        collation: 9,
        user: "old-user".into(),
        auth: vec![0xaa],
        ..HandshakeResponse41::default()
    };
    assert!(parse_response_header_into(&mut response, &[0]).is_err());
    assert_eq!(response.capability, 7);
    assert_eq!(response.collation, 9);

    let capability = CLIENT_PROTOCOL_41 | CLIENT_SECURE_CONNECTION;
    let complete_header = header(capability);
    assert_eq!(
        parse_response_header_into(&mut response, &complete_header).expect("complete header"),
        32
    );
    assert_eq!(response.capability, capability);
    assert_eq!(response.collation, 0);

    let mut truncated_auth = complete_header;
    truncated_auth.extend_from_slice(b"new-user\0");
    truncated_auth.extend_from_slice(&[3, 1, 2]);
    let state = ConnectionAttrsState::new(4096);
    assert!(
        parse_response_body_into_with_attrs_state(&mut response, &truncated_auth, 32, &state)
            .is_err()
    );
    assert_eq!(response.user.as_bytes(), b"new-user");
    assert_eq!(
        response.auth,
        [0xaa],
        "failed auth slice must not overwrite the old value"
    );
}

#[test]
fn header_boundary_reads_exactly_the_source_fields() {
    let capability = 0x7856_3412;
    let mut exact = header(capability);
    exact[4..8].copy_from_slice(&[0xff; 4]);
    exact[8] = 45;
    exact[9] = 99;
    let mut response = HandshakeResponse41::default();
    assert!(parse_response_header_into(&mut response, &exact[..31]).is_err());
    assert_eq!(
        parse_response_header_into(&mut response, &exact).unwrap(),
        32
    );
    assert_eq!(response.capability, capability);
    assert_eq!(response.collation, 45);
}

#[test]
fn null_auth_and_single_byte_no_auth_marker_preserve_go_semantics() {
    let state = ConnectionAttrsState::new(4096);
    let mut response = HandshakeResponse41 {
        capability: CLIENT_PROTOCOL_41 | CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA,
        auth: vec![7, 8],
        ..HandshakeResponse41::default()
    };

    let mut null_auth = header(response.capability);
    null_auth.extend_from_slice(b"root\0");
    null_auth.push(0xfb);
    parse_response_body_into_with_attrs_state(&mut response, &null_auth, 32, &state)
        .expect("NULL length leaves the existing auth value untouched");
    assert_eq!(response.auth, [7, 8]);

    let mut marker_only = header(response.capability);
    marker_only.extend_from_slice(b"root\0");
    marker_only.push(1);
    parse_response_body_into_with_attrs_state(&mut response, &marker_only, 32, &state)
        .expect("Go advances two bytes without reading the absent filler");
    assert_eq!(response.auth, [7, 8]);

    let capability = response.capability | CLIENT_PLUGIN_AUTH;
    response.capability = capability;
    let mut marker_with_filler = header(capability);
    marker_with_filler.extend_from_slice(b"root\0");
    marker_with_filler.extend_from_slice(&[1, 0xff]);
    marker_with_filler.extend_from_slice(b"plugin\0");
    parse_response_body_into_with_attrs_state(&mut response, &marker_with_filler, 32, &state)
        .expect("the byte after marker 1 is skipped without inspection");
    assert_eq!(response.auth_plugin.as_bytes(), b"plugin");
}

#[test]
fn attribute_policy_warnings_and_metrics_match_go_boundaries() {
    let attrs = [2, b'a', b'b', 2, b'c', b'd', 2, b'e', b'f', 2, b'g', b'h'];
    let state = ConnectionAttrsState::new(5);
    let response = parse_response_with_attrs_state(&response_with_attrs(&attrs), &state)
        .expect("framed attributes");
    assert_eq!(response.attrs.get("ab").map(String::as_str), Some("cd"));
    assert_eq!(
        response.attrs.get("_truncated").map(String::as_str),
        Some("4")
    );
    assert!(!response.attrs.contains_key("ef"));
    assert_eq!(state.lost(), 1);
    assert_eq!(state.longest_seen(), 8);
    assert_eq!(
        response.attr_warnings,
        ["session connection attributes truncated: total size 8 bytes exceeds performance_schema_session_connect_attrs_size (5), 4 bytes were discarded"]
    );

    let disabled = ConnectionAttrsState::new(0);
    let response = parse_response_with_attrs_state(&response_with_attrs(&attrs), &disabled)
        .expect("disabled collection");
    assert!(response.attrs.is_empty());
    assert_eq!(disabled.lost(), 0);
    assert_eq!(disabled.longest_seen(), 0);

    let exact = ConnectionAttrsState::new(4);
    let response = parse_response_with_attrs_state(
        &response_with_attrs(&[2, b'a', b'b', 2, b'c', b'd']),
        &exact,
    )
    .expect("aggregate exactly at the configured limit");
    assert_eq!(response.attrs.get("ab").map(String::as_str), Some("cd"));
    assert!(!response.attrs.contains_key("_truncated"));
    assert_eq!(exact.lost(), 0);
}

#[test]
fn raw_attribute_bytes_null_lengths_and_warning_order_are_preserved() {
    let attrs = [1, b'_', 1, 0xff, 0xfb, 0xfb];
    let state = ConnectionAttrsState::new(-1);
    let response = parse_response_with_attrs_state(&response_with_attrs(&attrs), &state)
        .expect("Go accepts NULL key/value lengths as empty strings");
    assert_eq!(response.raw_attrs.get(b"_".as_slice()), Some(&vec![0xff]));
    assert_eq!(response.raw_attrs.get(b"".as_slice()), Some(&Vec::new()));
    assert_eq!(
        response.attr_warnings,
        ["custom connection attributes with leading underscore are deprecated and will be rejected in a future release"]
    );
}

#[test]
fn sixty_four_kib_metric_boundary_does_not_update_longest_seen() {
    let mut attrs = vec![0];
    attrs.extend_from_slice(&lenenc(65_536));
    attrs.extend(std::iter::repeat_n(0, 65_536));
    let state = ConnectionAttrsState::new(-1);
    let response = parse_response_with_attrs_state(&response_with_attrs(&attrs), &state)
        .expect("exact auto-size limit is accepted");
    assert_eq!(
        response.raw_attrs.get(b"".as_slice()).map(Vec::len),
        Some(65_536)
    );
    assert_eq!(state.longest_seen(), 0);
    assert_eq!(state.lost(), 0);
}

#[test]
fn every_auth_encoding_width_and_mode_matches_go() {
    let lenenc_cases = [
        (vec![0], Vec::new()),
        (vec![3, 1, 2, 3], vec![1, 2, 3]),
        (vec![0xfc, 3, 0, 1, 2, 3], vec![1, 2, 3]),
        (vec![0xfd, 3, 0, 0, 1, 2, 3], vec![1, 2, 3]),
        (vec![0xfe, 3, 0, 0, 0, 0, 0, 0, 0, 1, 2, 3], vec![1, 2, 3]),
    ];
    for (encoded, expected) in lenenc_cases {
        let mut packet = header(CLIENT_PROTOCOL_41 | CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA);
        packet.extend_from_slice(b"root\0");
        packet.extend_from_slice(&encoded);
        assert_eq!(parse_response(&packet).expect("lenenc auth").auth, expected);
    }

    let mut ff_packet = header(CLIENT_PROTOCOL_41 | CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA);
    ff_packet.extend_from_slice(b"root\0");
    ff_packet.push(0xff);
    ff_packet.extend(0_u8..=254);
    assert_eq!(
        parse_response(&ff_packet)
            .expect("Go treats 0xff as inline length 255")
            .auth
            .len(),
        255
    );

    let mut secure = header(CLIENT_PROTOCOL_41 | CLIENT_SECURE_CONNECTION);
    secure.extend_from_slice(b"root\0\x03abc");
    assert_eq!(parse_response(&secure).expect("secure auth").auth, b"abc");

    let mut legacy = header(CLIENT_PROTOCOL_41);
    legacy.extend_from_slice(b"root\0abc\0");
    assert_eq!(parse_response(&legacy).expect("legacy auth").auth, b"abc");
}

#[test]
fn optional_database_plugin_and_zstd_fields_follow_source_order() {
    let capability = CLIENT_PROTOCOL_41
        | CLIENT_CONNECT_WITH_DB
        | CLIENT_PLUGIN_AUTH
        | CLIENT_ZSTD_COMPRESSION_ALGORITHM;
    let mut packet = header(capability);
    packet.extend_from_slice(b"root\0\0db\0plugin\0");
    packet.push(22);
    let response = parse_response(&packet).expect("complete optional fields");
    assert_eq!(response.db_name.as_bytes(), b"db");
    assert_eq!(response.auth_plugin.as_bytes(), b"plugin");
    assert_eq!(response.zstd_level, 22);

    let mut absent_db = header(CLIENT_PROTOCOL_41 | CLIENT_CONNECT_WITH_DB);
    absent_db.extend_from_slice(b"root\0\0");
    assert!(parse_response(&absent_db)
        .expect("absent optional db")
        .db_name
        .is_empty());

    let mut malformed_db = header(CLIENT_PROTOCOL_41 | CLIENT_CONNECT_WITH_DB);
    malformed_db.extend_from_slice(b"root\0\0unterminated");
    assert!(parse_response(&malformed_db).is_err());

    let mut missing_zstd = header(CLIENT_PROTOCOL_41 | CLIENT_ZSTD_COMPRESSION_ALGORITHM);
    missing_zstd.extend_from_slice(b"root\0\0");
    assert!(parse_response(&missing_zstd).is_err());

    let mut attrs_short_circuit =
        header(CLIENT_PROTOCOL_41 | CLIENT_CONNECT_ATTRS | CLIENT_ZSTD_COMPRESSION_ALGORITHM);
    attrs_short_circuit.extend_from_slice(b"root\0\0");
    assert!(parse_response(&attrs_short_circuit).is_ok());
}

#[test]
fn null_attribute_frame_preserves_existing_map_and_outer_errors_remain_errors() {
    let state = ConnectionAttrsState::new(4096);
    let capability = CLIENT_PROTOCOL_41 | CLIENT_CONNECT_ATTRS;
    let mut response = HandshakeResponse41 {
        capability,
        attrs: std::collections::HashMap::from([("old".to_owned(), "value".to_owned())]),
        raw_attrs: std::collections::HashMap::from([(b"old".to_vec(), b"value".to_vec())]),
        ..HandshakeResponse41::default()
    };
    let mut null_frame = header(capability);
    null_frame.extend_from_slice(b"root\0\0");
    null_frame.push(0xfb);
    parse_response_body_into_with_attrs_state(&mut response, &null_frame, 32, &state)
        .expect("NULL outer length skips attribute mutation");
    assert_eq!(response.attrs.get("old").map(String::as_str), Some("value"));

    let mut truncated_frame = header(capability);
    truncated_frame.extend_from_slice(b"root\0\0\x06\x01a");
    assert!(parse_response(&truncated_frame).is_err());

    let mut over_hard_limit = header(capability);
    over_hard_limit.extend_from_slice(b"root\0\0");
    over_hard_limit.extend_from_slice(&[0xfd, 1, 0, 16]);
    assert!(parse_response(&over_hard_limit).is_err());

    let mut exact_hard_limit = vec![0];
    exact_hard_limit.extend_from_slice(&lenenc((1 << 20) - 5));
    exact_hard_limit.extend(std::iter::repeat_n(0, (1 << 20) - 5));
    let state = ConnectionAttrsState::new(0);
    parse_response_with_attrs_state(&response_with_attrs(&exact_hard_limit), &state)
        .expect("the 1 MiB outer frame is admitted before policy decoding");
}

#[test]
fn warning_combination_duplicate_keys_and_longest_seen_cas_match_go() {
    let mut attrs = Vec::new();
    for (key, value) in [("_custom", "a"), ("dup", "first"), ("dup", "last")] {
        attrs.extend_from_slice(&lenenc(key.len()));
        attrs.extend_from_slice(key.as_bytes());
        attrs.extend_from_slice(&lenenc(value.len()));
        attrs.extend_from_slice(value.as_bytes());
    }
    let state = ConnectionAttrsState::new(1_000);
    let response = parse_response_with_attrs_state(&response_with_attrs(&attrs), &state)
        .expect("valid attributes");
    assert_eq!(response.attrs.get("dup").map(String::as_str), Some("last"));
    assert_eq!(response.attr_warnings.len(), 1);
    let first_longest = state.longest_seen();
    assert!(first_longest > 0);

    let smaller = [1, b'a', 1, b'b'];
    parse_response_with_attrs_state(&response_with_attrs(&smaller), &state)
        .expect("smaller aggregate");
    assert_eq!(state.longest_seen(), first_longest);

    state.set_limit(5);
    let response = parse_response_with_attrs_state(&response_with_attrs(&attrs), &state)
        .expect("truncated custom attrs");
    assert_eq!(state.lost(), 1);
    assert_eq!(response.attr_warnings.len(), 2);
    assert!(response.attr_warnings[0].starts_with("custom connection attributes"));
    assert!(response.attr_warnings[1].starts_with("session connection attributes truncated"));
}

#[test]
fn zero_attribute_limit_skips_decoding() {
    let state = ConnectionAttrsState::new(0);
    let response = parse_response_with_attrs_state(&response_with_attrs(&[2, b'a']), &state)
        .expect("Go returns before decoding when collection is disabled");
    assert!(response.attrs.is_empty());
    assert_eq!(state.longest_seen(), 0);
}

#[test]
fn standard_underscore_attributes_do_not_warn() {
    let mut attrs = Vec::new();
    for key in [
        "_client_name",
        "_client_version",
        "_os",
        "_pid",
        "_platform",
    ] {
        attrs.extend_from_slice(&lenenc(key.len()));
        attrs.extend_from_slice(key.as_bytes());
        attrs.extend_from_slice(&[1, b'v']);
    }
    let state = ConnectionAttrsState::new(-1);
    let response = parse_response_with_attrs_state(&response_with_attrs(&attrs), &state)
        .expect("standard attributes");
    assert!(response.attr_warnings.is_empty());
}

#[test]
fn negative_attribute_limit_normalizes_to_sixty_four_kib() {
    let mut attrs = vec![0];
    attrs.extend_from_slice(&lenenc(65_537));
    attrs.extend(std::iter::repeat_n(0, 65_537));
    let state = ConnectionAttrsState::new(-7);
    let response = parse_response_with_attrs_state(&response_with_attrs(&attrs), &state)
        .expect("every negative value uses Go's autosize cap");
    assert_eq!(
        response.attrs.get("_truncated").map(String::as_str),
        Some("65537")
    );
    assert_eq!(state.lost(), 1);
}

#[test]
fn concurrent_longest_seen_updates_converge_on_the_maximum() {
    let state = Arc::new(ConnectionAttrsState::new(1_000));
    let mut threads = Vec::new();
    for size in [1_usize, 17, 3, 63, 8, 127, 32, 255] {
        let state = Arc::clone(&state);
        threads.push(std::thread::spawn(move || {
            let mut attrs = vec![0];
            attrs.extend_from_slice(&lenenc(size));
            attrs.extend(std::iter::repeat_n(b'x', size));
            parse_response_with_attrs_state(&response_with_attrs(&attrs), &state).unwrap();
        }));
    }
    for thread in threads {
        thread.join().unwrap();
    }
    assert_eq!(state.longest_seen(), 255);
}

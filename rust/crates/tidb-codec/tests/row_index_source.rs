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

//! Source-exact coverage for `pkg/tablecodec/rowindexcodec`.

use tidb_codec::{decode_table_id, encode_int, get_key_kind, KeyKind};

#[test]
fn get_key_kind_executes_the_complete_original_source_table() {
    let cases: &[(&str, &[u8], KeyKind)] = &[
        (
            "row",
            &[116, 128, 0, 0, 0, 0, 0, 0, 0, 95, 114],
            KeyKind::Row,
        ),
        (
            "index",
            &[
                116, 128, 0, 0, 0, 0, 0, 0, 0, 95, 105, 128, 0, 0, 0, 0, 0, 0, 0,
            ],
            KeyKind::Index,
        ),
        ("empty", b"", KeyKind::Unknown),
        // A Go nil slice and an empty slice have the same observable bytes at
        // this API boundary, so both original assertions execute as `&[]`.
        ("nil", &[], KeyKind::Unknown),
    ];

    for (name, key, expected) in cases {
        assert_eq!(get_key_kind(key), *expected, "source row {name}");
    }
}

#[test]
fn get_key_kind_closes_every_observable_prefix_boundary() {
    let cases: &[(&str, &[u8], KeyKind)] = &[
        ("one byte short", b"t12345678_", KeyKind::Unknown),
        ("minimal row prefix", b"t12345678_r", KeyKind::Row),
        ("minimal index prefix", b"t12345678_i", KeyKind::Index),
        (
            "opaque table id bytes",
            &[b't', 0, 0xff, b'_', b'i', b'r', 0, 1, 2, b'_', b'r'],
            KeyKind::Row,
        ),
        ("wrong table prefix", b"x12345678_r", KeyKind::Unknown),
        ("wrong separator", b"t12345678-r", KeyKind::Unknown),
        ("unknown kind", b"t12345678_x", KeyKind::Unknown),
        ("row suffix is opaque", b"t12345678_ranything", KeyKind::Row),
        (
            "index suffix is opaque",
            b"t12345678_ianything",
            KeyKind::Index,
        ),
        (
            "kind marker at the wrong offset",
            b"tt12345678_r",
            KeyKind::Unknown,
        ),
    ];

    for (name, key, expected) in cases {
        assert_eq!(get_key_kind(key), *expected, "boundary {name}");
    }
}

#[test]
fn decode_table_id_matches_tablecodec_prefix_and_zero_fallback() {
    let mut key = vec![b't'];
    encode_int(&mut key, 55);
    key.extend_from_slice(b"_r");
    assert_eq!(decode_table_id(&key), 55);

    let mut negative = vec![b't'];
    encode_int(&mut negative, -66);
    assert_eq!(decode_table_id(&negative), -66);

    // Go's DecodeTableID returns zero for nil, non-table, short, malformed,
    // and API-V2-prefixed keys until an API-V2 codec owner is ported.
    for key in [b"".as_slice(), b"x12345678_r", b"t", b"t\0\x01"] {
        assert_eq!(decode_table_id(key), 0, "key {key:?}");
    }
}

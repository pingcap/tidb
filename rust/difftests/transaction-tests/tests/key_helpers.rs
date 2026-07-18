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

//! Direct source-shaped checks for the byte-key helpers in `pkg/kv/key.go`.
//!
//! The original `TestPartialNext` and `TestIsPoint` anchors are owned by the
//! key foundation fragment. These tests cover the remaining allocation and
//! byte-semantics helpers without claiming those existing anchors again.

use std::cmp::Ordering;

use tidb_txnkv::{Key, KeyRange};

#[test]
fn key_helpers_preserve_bytes_and_order() {
    let original = Key::from_bytes([0x00, 0x0f, 0x10, 0xff]);
    let mut clone_bytes = original.clone().into_bytes();
    clone_bytes[0] = 0x01;
    let clone = Key::from_bytes(clone_bytes);

    assert_eq!(original.as_bytes(), &[0x00, 0x0f, 0x10, 0xff]);
    assert_eq!(clone.as_bytes(), &[0x01, 0x0f, 0x10, 0xff]);
    assert_eq!(original.to_string(), "000f10ff");
    assert_eq!(original.compare(&clone), Ordering::Less);
    assert_eq!(
        original.compare(&Key::from_bytes([0x00, 0x0f, 0x11])),
        Ordering::Less
    );
    assert_eq!(
        Key::from_bytes([0x01]).compare(&original),
        Ordering::Greater
    );
}

#[test]
fn key_prefix_and_next_handle_empty_and_overflow() {
    let key = Key::from_bytes(b"rowkey1");
    assert_eq!(key.next().as_bytes(), b"rowkey1\0");
    assert_eq!(key.prefix_next().as_bytes(), b"rowkey2");
    assert_eq!(key.as_bytes(), b"rowkey1");

    assert_eq!(Key::from_bytes(Vec::new()).next().as_bytes(), &[0]);
    assert_eq!(Key::from_bytes(Vec::new()).prefix_next().as_bytes(), &[0]);
    assert_eq!(
        Key::from_bytes([0xff, 0xff]).prefix_next().as_bytes(),
        &[0xff, 0xff, 0]
    );
    assert_eq!(
        Key::from_bytes([0x7b, 0x7b, 0xff, 0xff])
            .prefix_next()
            .as_bytes(),
        &[0x7b, 0x7c, 0, 0]
    );
}

#[test]
fn key_has_prefix_matches_source_bytes_has_prefix() {
    let key = Key::from_bytes(b"rowkey1_column1");
    assert!(key.has_prefix(&Key::from_bytes(b"rowkey1")));
    assert!(key.has_prefix(&key));
    assert!(key.has_prefix(&Key::from_bytes(Vec::new())));
    assert!(!key.has_prefix(&Key::from_bytes(b"rowkey2")));
    assert!(!Key::from_bytes(b"rowkey").has_prefix(&key));
}

#[test]
fn key_range_is_point_preserves_half_open_boundaries() {
    let cases = [
        (b"rowkey1".as_slice(), b"rowkey2".as_slice(), true),
        (b"rowkey1".as_slice(), b"rowkey3".as_slice(), false),
        (&[0xff, 0xff][..], &[0, 0][..], false),
        (&[0x7b, 0x7b, 0xff, 0xff][..], &[0x7b, 0x7c, 0, 0][..], true),
        (
            &[0x7b, 0x7b, 0xff, 0xff][..],
            &[0x7b, 0x7c, 0, 1][..],
            false,
        ),
    ];

    for (start, end, expected) in cases {
        assert_eq!(
            KeyRange::new(Key::from_bytes(start), Key::from_bytes(end)).is_point(),
            expected,
            "start={start:?} end={end:?}"
        );
    }
}

#[test]
fn key_range_definition_is_safe_and_explicit() {
    let range = KeyRange::new(Key::from_bytes(b"start"), Key::from_bytes(b"end"));
    assert_eq!(range.start_key.as_bytes(), b"start");
    assert_eq!(range.end_key.as_bytes(), b"end");
}

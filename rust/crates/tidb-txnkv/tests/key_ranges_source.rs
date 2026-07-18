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
// See the License for the specific language governing permissions and
// limitations under the License.

//! Direct source tests for `pkg/store/copr/key_ranges_test.go`.

use tidb_txnkv::{Key, KeyRange, KeyRanges};

fn key(value: &str) -> Key {
    Key::from_bytes(value.as_bytes())
}

fn key_range(start: &str, end: &str) -> KeyRange {
    KeyRange::new(key(start), key(end))
}

fn build_ranges(bounds: &[&str]) -> KeyRanges {
    assert_eq!(bounds.len() % 2, 0);
    KeyRanges::new(
        bounds
            .chunks_exact(2)
            .map(|bound| key_range(bound[0], bound[1]))
            .collect(),
    )
}

fn assert_equal(ranges: &KeyRanges, expected: &[KeyRange], check_slices: bool) {
    assert_eq!(ranges.len(), expected.len());
    assert_eq!(ranges.is_empty(), expected.is_empty());
    for (index, expected_range) in expected.iter().enumerate() {
        assert_eq!(ranges.ref_at(index), expected_range);
        assert_eq!(ranges.at(index), *expected_range);
    }
    assert_eq!(ranges.to_ranges(), expected);

    if check_slices {
        for from in 0..=ranges.len() {
            for to in from..=ranges.len() {
                assert_equal(&ranges.slice(from, to), &expected[from..to], false);
            }
        }
    }
}

fn split_half(ranges: &KeyRanges, split_key: &str, check_left: bool) -> Vec<KeyRange> {
    let (left, right) = ranges.split(&key(split_key));
    if check_left {
        left.to_ranges()
    } else {
        right.to_ranges()
    }
}

#[test]
fn test_cop_ranges() {
    // These four values reproduce the source's mid-only, first+mid,
    // mid+last, and first+mid+last representations through the public split
    // operation.  Every possible slice of each representation is checked.
    let expected = vec![
        key_range("a", "b"),
        key_range("c", "d"),
        key_range("e", "f"),
    ];

    let mid = build_ranges(&["a", "b", "c", "d", "e", "f"]);
    assert_equal(&mid, &expected, true);

    let (_, first_mid) = build_ranges(&["0", "b", "c", "d", "e", "f"]).split(&key("a"));
    assert_equal(&first_mid, &expected, true);

    let (mid_last, _) = build_ranges(&["a", "b", "c", "d", "e", "z"]).split(&key("f"));
    assert_equal(&mid_last, &expected, true);

    let (_, first_mid_open) = build_ranges(&["0", "b", "c", "d", "e", "z"]).split(&key("a"));
    let (first_mid_last, _) = first_mid_open.split(&key("f"));
    assert_equal(&first_mid_last, &expected, true);

    // Equality is over the logical sequence, independent of whether a range
    // is held in first/middle/last storage.
    assert_eq!(mid, first_mid);
    assert_eq!(mid, mid_last);
    assert_eq!(mid, first_mid_last);
}

#[test]
fn test_cop_range_split() {
    // input range: [c-d) [e-g) [l-o)
    let ranges = build_ranges(&["c", "d", "e", "g", "l", "o"]);
    assert_eq!(
        split_half(&ranges, "c", false),
        build_ranges(&["c", "d", "e", "g", "l", "o"]).to_ranges()
    );
    assert_eq!(
        split_half(&ranges, "d", false),
        build_ranges(&["e", "g", "l", "o"]).to_ranges()
    );
    assert_eq!(
        split_half(&ranges, "f", false),
        build_ranges(&["f", "g", "l", "o"]).to_ranges()
    );

    // input range: [a-b) [c-d) [e-g) [l-o), with the first range in `first`.
    let (_, ranges) = build_ranges(&["0", "b", "c", "d", "e", "g", "l", "o"]).split(&key("a"));
    assert_eq!(
        split_half(&ranges, "a", false),
        build_ranges(&["a", "b", "c", "d", "e", "g", "l", "o"]).to_ranges()
    );
    assert_eq!(
        split_half(&ranges, "c", false),
        build_ranges(&["c", "d", "e", "g", "l", "o"]).to_ranges()
    );
    assert_eq!(
        split_half(&ranges, "m", false),
        build_ranges(&["m", "o"]).to_ranges()
    );

    // input range: [a-b) [c-d) [e-g) [l-o) [q-t), with both boundaries.
    let (_, ranges_open) =
        build_ranges(&["0", "b", "c", "d", "e", "g", "l", "o", "q", "z"]).split(&key("a"));
    let (ranges, _) = ranges_open.split(&key("t"));
    assert_eq!(
        split_half(&ranges, "f", false),
        build_ranges(&["f", "g", "l", "o", "q", "t"]).to_ranges()
    );
    assert_eq!(
        split_half(&ranges, "h", false),
        build_ranges(&["l", "o", "q", "t"]).to_ranges()
    );
    assert_eq!(
        split_half(&ranges, "r", false),
        build_ranges(&["r", "t"]).to_ranges()
    );

    // The same source representations, checking the left half.
    let ranges = build_ranges(&["c", "d", "e", "g", "l", "o"]);
    assert_eq!(
        split_half(&ranges, "m", true),
        build_ranges(&["c", "d", "e", "g", "l", "m"]).to_ranges()
    );
    assert_eq!(
        split_half(&ranges, "g", true),
        build_ranges(&["c", "d", "e", "g"]).to_ranges()
    );
    assert_eq!(
        split_half(&ranges, "g", true),
        build_ranges(&["c", "d", "e", "g"]).to_ranges()
    );

    let (_, ranges) = build_ranges(&["0", "b", "c", "d", "e", "g", "l", "o"]).split(&key("a"));
    assert_eq!(
        split_half(&ranges, "d", true),
        build_ranges(&["a", "b", "c", "d"]).to_ranges()
    );
    assert_eq!(
        split_half(&ranges, "d", true),
        build_ranges(&["a", "b", "c", "d"]).to_ranges()
    );
    assert_eq!(
        split_half(&ranges, "o", true),
        build_ranges(&["a", "b", "c", "d", "e", "g", "l", "o"]).to_ranges()
    );

    let (_, ranges_open) =
        build_ranges(&["0", "b", "c", "d", "e", "g", "l", "o", "q", "z"]).split(&key("a"));
    let (ranges, _) = ranges_open.split(&key("t"));
    assert_eq!(
        split_half(&ranges, "o", true),
        build_ranges(&["a", "b", "c", "d", "e", "g", "l", "o"]).to_ranges()
    );
    assert_eq!(
        split_half(&ranges, "p", true),
        build_ranges(&["a", "b", "c", "d", "e", "g", "l", "o"]).to_ranges()
    );
    assert_eq!(
        split_half(&ranges, "t", true),
        build_ranges(&["a", "b", "c", "d", "e", "g", "l", "o", "q", "t"]).to_ranges()
    );
}

#[test]
fn unanchored_source_methods_preserve_reset_string_and_safe_pb_conversion() {
    let mut ranges = build_ranges(&["a\n", "b\t", "c", "d"]);
    assert_eq!(ranges.to_string(), "[\"a\\n\", \"b\\t\"][\"c\", \"d\"]");

    let protobuf = ranges.to_pb_ranges();
    assert_eq!(protobuf.len(), 2);
    assert_eq!(protobuf[0].start, b"a\n");
    assert_eq!(protobuf[0].end, b"b\t");
    assert_eq!(protobuf[1].start, b"c");
    assert_eq!(protobuf[1].end, b"d");

    let binary_and_utf8 = KeyRanges::new(vec![KeyRange::new(
        Key::from_bytes([0xff, b'\"', b'\\', 0x00, 0x1b, 0x7f].as_slice()),
        Key::from_bytes("世界".as_bytes()),
    )]);
    assert_eq!(
        binary_and_utf8.to_string(),
        "[\"\\xff\\\"\\\\\\x00\\x1b\\x7f\", \"世界\"]"
    );

    // Go's strconv.IsPrint excludes format and unassigned code points even
    // when the host language does not classify them as control characters.
    let unicode_categories = KeyRanges::new(vec![KeyRange::new(
        Key::from_bytes("\u{00ad}\u{200b}\u{0378}".as_bytes()),
        Key::from_bytes("世界".as_bytes()),
    )]);
    assert_eq!(
        unicode_categories.to_string(),
        "[\"\\u00ad\\u200b\\u0378\", \"世界\"]"
    );

    // Start from first+middle+last storage and prove Reset clears both
    // detached boundaries rather than preserving stale ranges.
    let (_, with_first) = build_ranges(&["0", "b", "c", "d", "e", "z"]).split(&key("a"));
    let (mut with_all_boundaries, _) = with_first.split(&key("f"));
    assert_eq!(with_all_boundaries.len(), 3);
    with_all_boundaries.reset(vec![key_range("x", "y"), key_range("y", "z")]);
    assert_equal(
        &with_all_boundaries,
        &[key_range("x", "y"), key_range("y", "z")],
        true,
    );

    ranges.reset(vec![key_range("x", "z")]);
    assert_equal(&ranges, &[key_range("x", "z")], true);
}

#[test]
fn source_adaptations_have_explicit_safe_policies() {
    let ranges = build_ranges(&["a", "b"]);
    assert!(ranges.get(ranges.len()).is_none());
    assert!(std::panic::catch_unwind(|| ranges.ref_at(ranges.len())).is_err());

    // An empty EndKey is unbounded in the source's sort predicate. Splitting
    // inside it keeps the empty end on the right boundary.
    let ranges = build_ranges(&["a", "b", "c", ""]);
    let (left, right) = ranges.split(&key("z"));
    assert_eq!(left, build_ranges(&["a", "b", "c", "z"]));
    assert_eq!(right, build_ranges(&["z", ""]));
}

#[test]
fn test_key_range_definition_safe_conversion_subset() {
    let ranges = build_ranges(&["s1", "e1", "s2", "e2"]);
    let protobuf = ranges.to_pb_ranges();

    assert_eq!(protobuf.len(), 2);
    assert_eq!(
        (&protobuf[0].start[..], &protobuf[0].end[..]),
        (&b"s1"[..], &b"e1"[..])
    );
    assert_eq!(
        (&protobuf[1].start[..], &protobuf[1].end[..]),
        (&b"s2"[..], &b"e2"[..])
    );
}

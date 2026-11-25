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

//! Go `strings.EqualFold` and `strings.ToLower` equality, the two identifier
//! comparisons TiDB makes. The expected values are what Go answers.

use std::borrow::Cow;

use tidb_mysql::{equal_fold, lowercase_equal, simple_fold, to_lowercase_cow, to_uppercase_cow};

/// The string a Go string holding `bytes` is carried in: valid UTF-8 as is,
/// each other byte `b` as the raw-byte escape `U+10FF00 + b`.
fn go_string(bytes: &[u8]) -> String {
    let mut value = String::new();
    for chunk in bytes.utf8_chunks() {
        value.push_str(chunk.valid());
        for &byte in chunk.invalid() {
            value.push(char::from_u32(0x10FF00 + u32::from(byte)).unwrap());
        }
    }
    value
}

#[test]
fn equal_fold_and_lowercase_equal_match_go() {
    // (left, right, strings.EqualFold, ToLower(left) == ToLower(right))
    let cases: &[(&[u8], &[u8], bool, bool)] = &[
        (b"abc", b"ABC", true, true),
        (b"abc", b"abd", false, false),
        (b"ab", b"abc", false, false),
        ("k".as_bytes(), "\u{212a}".as_bytes(), true, true),
        ("K".as_bytes(), "\u{212a}".as_bytes(), true, true),
        ("s".as_bytes(), "\u{17f}".as_bytes(), true, false),
        ("S".as_bytes(), "\u{17f}".as_bytes(), true, false),
        ("\u{130}".as_bytes(), b"i", false, true),
        ("\u{131}".as_bytes(), b"I", false, false),
        ("\u{b5}".as_bytes(), "\u{39c}".as_bytes(), true, false),
        ("\u{b5}".as_bytes(), "\u{3bc}".as_bytes(), true, false),
        ("\u{3c2}".as_bytes(), "\u{3a3}".as_bytes(), true, false),
        ("\u{3c2}".as_bytes(), "\u{3c3}".as_bytes(), true, false),
        ("\u{432}".as_bytes(), "\u{1c80}".as_bytes(), true, false),
        ("\u{412}".as_bytes(), "\u{1c80}".as_bytes(), true, false),
        ("\u{e9}".as_bytes(), "\u{c9}".as_bytes(), true, true),
        ("\u{df}".as_bytes(), "\u{1e9e}".as_bytes(), true, true),
        ("\u{1c4}".as_bytes(), "\u{1c6}".as_bytes(), true, true),
        ("\u{212b}".as_bytes(), "\u{e5}".as_bytes(), true, true),
        ("\u{212b}".as_bytes(), "\u{c5}".as_bytes(), true, true),
        ("PR\u{130}MARY".as_bytes(), b"primary", false, true),
        ("\u{212a}ey".as_bytes(), b"KEY", true, true),
        (b"d\xe9", b"D\xe9", true, true),
        (b"d\xe9", b"d\xe8", true, true),
        (b"d\xe9", "d\u{fffd}".as_bytes(), true, true),
        (b"\xe2\x82", "\u{fffd}\u{fffd}".as_bytes(), true, true),
        (b"\xe2\x82", "\u{fffd}".as_bytes(), false, false),
    ];
    for &(left, right, fold, lower) in cases {
        let (left, right) = (go_string(left), go_string(right));
        assert_eq!(
            equal_fold(&left, &right),
            fold,
            "EqualFold({left:?}, {right:?})"
        );
        assert_eq!(
            equal_fold(&right, &left),
            fold,
            "EqualFold({right:?}, {left:?})"
        );
        assert_eq!(
            lowercase_equal(&left, &right),
            lower,
            "ToLower({left:?}) == ToLower({right:?})"
        );
    }
}

#[test]
fn identifier_comparisons_decode_escaped_utf8_before_casing() {
    // The scanner can also carry each non-ASCII byte as an escape, even
    // when the bytes form valid UTF-8. C3 89 is É and E2 82 is truncated.
    let escaped = "\u{10ffc3}\u{10ff89}\u{10ffe2}\u{10ff82}";
    assert!(equal_fold(escaped, "é��"));
    assert!(lowercase_equal(escaped, "é��"));
    assert!(!equal_fold(escaped, "é�"));
    assert!(!lowercase_equal(escaped, "é�"));
}

#[test]
fn simple_case_borrows_unchanged_text_and_transforms_raw_escape_runs() {
    assert!(matches!(to_uppercase_cow("SELECT"), Cow::Borrowed("SELECT")));
    assert!(matches!(
        to_lowercase_cow("already_lower_σ中"),
        Cow::Borrowed("already_lower_σ中")
    ));
    assert_eq!(to_lowercase_cow("prefix_İΣ"), "prefix_iσ");
    assert_eq!(
        to_uppercase_cow("PREFIX_\u{10ffc3}\u{10ff89}\u{10ffe2}\u{10ff82}"),
        "PREFIX_\u{10ffc3}\u{10ff89}��"
    );
}

#[test]
fn simple_fold_walks_go_orbits() {
    // Go's `unicode.SimpleFold` examples.
    assert_eq!(simple_fold('A'), 'a');
    assert_eq!(simple_fold('a'), 'A');
    assert_eq!(simple_fold('K'), 'k');
    assert_eq!(simple_fold('k'), '\u{212a}');
    assert_eq!(simple_fold('\u{212a}'), 'K');
    assert_eq!(simple_fold('1'), '1');
    assert_eq!(simple_fold('\u{130}'), '\u{130}');
    assert_eq!(simple_fold('\u{390}'), '\u{1fd3}');
    assert_eq!(simple_fold('\u{1fd3}'), '\u{390}');
    assert!(equal_fold("\u{390}", "\u{1fd3}"));
}

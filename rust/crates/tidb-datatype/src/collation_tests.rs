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

use std::cmp::Ordering;

use crate::Collation;

fn signed(ordering: Ordering) -> i8 {
    match ordering {
        Ordering::Less => -1,
        Ordering::Equal => 0,
        Ordering::Greater => 1,
    }
}

fn bytes(hex: &str) -> Vec<u8> {
    assert_eq!(hex.len() % 2, 0);
    hex.as_bytes()
        .chunks_exact(2)
        .map(|pair| {
            let text = std::str::from_utf8(pair).expect("ASCII hex pair");
            u8::from_str_radix(text, 16).expect("valid hex pair")
        })
        .collect()
}

/// All twenty rows and all four relevant columns from
/// `pkg/util/collate/collate_test.go::TestUTF8CollatorCompare`.
#[test]
fn compare_executes_all_original_binary_bin_general_and_unicode_columns() {
    let collations = [
        Collation::Binary,
        Collation::Utf8Mb4Bin,
        Collation::Utf8Mb4GeneralCi,
        Collation::Utf8Mb4UnicodeCi,
    ];
    let rows = [
        ("a", "b", [-1, -1, -1, -1]),
        ("a", "A", [1, 1, 0, 0]),
        ("À", "A", [1, 1, 0, 0]),
        ("abc", "abc", [0, 0, 0, 0]),
        ("abc", "ab", [1, 1, 1, 1]),
        ("😜", "😃", [1, 1, 0, 0]),
        ("a", "a ", [-1, 0, 0, 0]),
        ("a ", "a  ", [-1, 0, 0, 0]),
        ("a\t", "a", [1, 1, 1, 1]),
        ("ß", "s", [1, 1, 0, 1]),
        ("ß", "ss", [1, 1, -1, 0]),
        ("啊", "吧", [1, 1, 1, 1]),
        ("中文", "汉字", [-1, -1, -1, -1]),
        ("æ", "ae", [1, 1, 1, 1]),
        ("Å", "A", [1, 1, 1, 0]),
        ("Å", "A", [1, 1, 0, 0]),
        ("\u{1730F}", "啊", [1, 1, 1, 1]),
        ("가", "㉡", [1, 1, 1, 1]),
        ("갟", "감1", [1, 1, 1, 1]),
        ("\u{FFFFE}", "\u{FFFFF}", [-1, -1, 0, 0]),
    ];
    for (left, right, expected) in rows {
        for (index, collation) in collations.into_iter().enumerate() {
            let actual = signed(collation.compare(left.as_bytes(), right.as_bytes()));
            assert_eq!(
                actual, expected[index],
                "{left:?} vs {right:?} using {collation}"
            );
            assert_eq!(
                actual,
                signed(
                    collation
                        .key(left.as_bytes())
                        .cmp(&collation.key(right.as_bytes())),
                )
            );
        }
    }
}

/// All eight rows and both Key/ImmutableKey-equivalent assertions for the four
/// relevant columns in `TestUTF8CollatorKey`. Rust owns immutable `Vec<u8>`
/// keys, so one result covers both source methods without an aliasing seam.
#[test]
fn key_executes_all_original_binary_bin_general_and_unicode_columns() {
    let rows: &[(&str, [&str; 4])] = &[
        ("a", ["61", "61", "0041", "0E33"]),
        ("A", ["41", "41", "0041", "0E33"]),
        (
            "Foo © bar 𝌆 baz ☃ qux",
            [
                "466f6f20c2a92062617220f09d8c862062617a20e2988320717578",
                "466f6f20c2a92062617220f09d8c862062617a20e2988320717578",
                "0046004F004F002000A900200042004100520020FFFD002000420041005A002026030020005100550058",
                "0EB90F820F82020902C502090E4A0E330FC00209FFFD02090E4A0E33106A020906FF02090FB4101F105A",
            ],
        ),
        ("a ", ["6120", "61", "0041", "0E33"]),
        (
            "ﷻ",
            [
                "EFB7BB",
                "EFB7BB",
                "FDFB",
                "135E13AB0209135E13AB135013AB13B7",
            ],
        ),
        (
            "中文",
            [
                "E4B8ADE69687",
                "E4B8ADE69687",
                "4E2D6587",
                "FB40CE2DFB40E587",
            ],
        ),
        (
            "갟감1",
            [
                "EAB09FEAB09031",
                "EAB09FEAB09031",
                "AC1FAC100031",
                "FBC1AC1FFBC1AC100E2A",
            ],
        ),
        (
            "\u{FFFFE}\u{FFFFF}",
            [
                "F3BFBFBEF3BFBFBF",
                "F3BFBFBEF3BFBFBF",
                "FFFDFFFD",
                "FFFDFFFD",
            ],
        ),
    ];
    let collations = [
        Collation::Binary,
        Collation::Utf8Mb4Bin,
        Collation::Utf8Mb4GeneralCi,
        Collation::Utf8Mb4UnicodeCi,
    ];
    for &(value, expected) in rows {
        for (index, collation) in collations.into_iter().enumerate() {
            let first = collation.key(value.as_bytes());
            let second = collation.key(value.as_bytes());
            assert_eq!(
                first,
                bytes(expected[index]),
                "key {value:?} using {collation}"
            );
            assert_eq!(second, first, "immutable key {value:?} using {collation}");
        }
    }
}

/// Relevant name/type rows from `TestGetCollator`; the remaining test rows
/// cover intentionally out-of-scope 0900, GBK, pinyin, ID rewriting, and the
/// legacy new-collation toggle.
#[test]
fn registry_maps_every_translated_get_collator_name_to_one_authority() {
    let rows = [
        ("binary", Collation::Binary),
        ("utf8mb4_bin", Collation::Utf8Mb4Bin),
        ("utf8_bin", Collation::Utf8Bin),
        ("utf8mb4_general_ci", Collation::Utf8Mb4GeneralCi),
        ("utf8_general_ci", Collation::Utf8GeneralCi),
        ("utf8mb4_unicode_ci", Collation::Utf8Mb4UnicodeCi),
        ("utf8_unicode_ci", Collation::Utf8UnicodeCi),
    ];
    for (name, expected) in rows {
        assert_eq!(Collation::from_name(name), Some(expected));
    }
}

/// Every general-CI and UCA-4.0 assertion attributable to
/// `TestCampareInvalidUTF8Rune`. Rust byte slices preserve the Go-string input
/// state: compare returns equality at the first invalid sequence and key keeps
/// the valid prefix.
#[test]
fn invalid_utf8_executes_all_original_general_and_unicode_assertions() {
    for collation in [Collation::Utf8Mb4GeneralCi, Collation::Utf8Mb4UnicodeCi] {
        assert_eq!(collation.compare(&[0xFF], &[0xFF]), Ordering::Equal);
        assert_eq!(collation.compare(&[0xFF], &[0xFE]), Ordering::Equal);
        assert_eq!(collation.compare(&[0xFF], &[0xFF, 0x3E]), Ordering::Equal);
        assert_eq!(
            collation.compare(&[0x3E, 0xFF], &[0x3E, 0xFF, 0x3E]),
            Ordering::Equal
        );

        let invalid_at_start = collation.key(&[0xFF]);
        let invalid_after_prefix = collation.key(&[0x3E, 0xFF]);
        let suffix_after_invalid = collation.key(&[0x3E, 0xFF, 0x3E]);
        assert!(invalid_at_start.is_empty());
        assert!(!invalid_after_prefix.is_empty());
        assert_eq!(suffix_after_invalid, invalid_after_prefix);
    }
}

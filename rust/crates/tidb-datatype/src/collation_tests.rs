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

use crate::{
    binary_collation_name, binary_collator, collation_id_to_name, collation_name_to_id,
    collation_to_proto, compatible_collate, get_binary_collator_slice, get_charset_info,
    get_collator, get_collator_by_id, get_collator_with_mode, get_supported_collation_by_name,
    is_bin_collation, is_ci_collation, is_default_collation_for_utf8mb4, is_pad_space_collation,
    new_collation_enabled, proto_to_collation, restore_collation_id_if_needed,
    rewrite_new_collation_id_if_needed, set_new_collation_enabled,
    substitute_missing_collation_to_default, supported_collations, Collation, Collator,
};

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

/// All twenty rows and all eight columns from
/// `pkg/util/collate/collate_test.go::TestUTF8CollatorCompare`.
#[test]
fn compare_executes_all_original_columns() {
    let collations = [
        Collation::Binary,
        Collation::Utf8Mb4Bin,
        Collation::Utf8Mb4GeneralCi,
        Collation::Utf8Mb4UnicodeCi,
        Collation::Utf8Mb40900AiCi,
        Collation::Utf8Mb40900Bin,
        Collation::GbkBin,
        Collation::GbkChineseCi,
    ];
    let rows = [
        ("a", "b", [-1, -1, -1, -1, -1, -1, -1, -1]),
        ("a", "A", [1, 1, 0, 0, 0, 1, 1, 0]),
        ("À", "A", [1, 1, 0, 0, 0, 1, -1, -1]),
        ("abc", "abc", [0, 0, 0, 0, 0, 0, 0, 0]),
        ("abc", "ab", [1, 1, 1, 1, 1, 1, 1, 1]),
        ("😜", "😃", [1, 1, 0, 0, 1, 1, 0, 0]),
        ("a", "a ", [-1, 0, 0, 0, -1, -1, 0, 0]),
        ("a ", "a  ", [-1, 0, 0, 0, -1, -1, 0, 0]),
        ("a\t", "a", [1, 1, 1, 1, 1, 1, 1, 1]),
        ("ß", "s", [1, 1, 0, 1, 1, 1, -1, -1]),
        ("ß", "ss", [1, 1, -1, 0, 0, 1, -1, -1]),
        ("啊", "吧", [1, 1, 1, 1, 1, 1, -1, -1]),
        ("中文", "汉字", [-1, -1, -1, -1, -1, -1, 1, 1]),
        ("æ", "ae", [1, 1, 1, 1, 0, 1, -1, -1]),
        ("Å", "A", [1, 1, 1, 0, 0, 1, 1, 1]),
        ("Å", "A", [1, 1, 0, 0, 0, 1, -1, -1]),
        ("\u{1730F}", "啊", [1, 1, 1, 1, -1, 1, -1, -1]),
        ("가", "㉡", [1, 1, 1, 1, -1, 1, 0, 0]),
        ("갟", "감1", [1, 1, 1, 1, 1, 1, -1, -1]),
        ("\u{FFFFE}", "\u{FFFFF}", [-1, -1, 0, 0, -1, -1, 0, 0]),
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

/// All eight rows and both Key/ImmutableKey-equivalent assertions for all
/// columns in `TestUTF8CollatorKey`. Rust owns immutable `Vec<u8>`
/// keys, so one result covers both source methods without an aliasing seam.
#[test]
fn key_executes_all_original_columns() {
    let rows: &[(&str, [&str; 8])] = &[
        ("a", ["61", "61", "0041", "0E33", "1C47", "61", "61", "41"]),
        ("A", ["41", "41", "0041", "0E33", "1C47", "41", "41", "41"]),
        (
            "Foo © bar 𝌆 baz ☃ qux",
            [
                "466f6f20c2a92062617220f09d8c862062617a20e2988320717578",
                "466f6f20c2a92062617220f09d8c862062617a20e2988320717578",
                "0046004F004F002000A900200042004100520020FFFD002000420041005A002026030020005100550058",
                "0EB90F820F82020902C502090E4A0E330FC00209FFFD02090E4A0E33106A020906FF02090FB4101F105A",
                "1CE51DDD1DDD0209058402091C601C471E3302090EF002091C601C471F210209091B02091E211EB51EFF",
                "466F6F20C2A92062617220F09D8C862062617A20E2988320717578",
                "466F6F203F20626172203F2062617A203F20717578",
                "464F4F203F20424152203F2042415A203F20515558",
            ],
        ),
        ("a ", ["6120", "61", "0041", "0E33", "1C470209", "6120", "61", "41"]),
        (
            "ﷻ",
            [
                "EFB7BB",
                "EFB7BB",
                "FDFB",
                "135E13AB0209135E13AB135013AB13B7",
                "2325239C02092325239C230B239C23B1",
                "EFB7BB",
                "3F",
                "3F",
            ],
        ),
        (
            "中文",
            [
                "E4B8ADE69687",
                "E4B8ADE69687",
                "4E2D6587",
                "FB40CE2DFB40E587",
                "FB40CE2DFB40E587",
                "E4B8ADE69687",
                "D6D0CEC4",
                "D321C1AD",
            ],
        ),
        (
            "갟감1",
            [
                "EAB09FEAB09031",
                "EAB09FEAB09031",
                "AC1FAC100031",
                "FBC1AC1FFBC1AC100E2A",
                "3BF53C743CD33BF53C733CE01C3E",
                "EAB09FEAB09031",
                "3F3F31",
                "3F3F31",
            ],
        ),
        (
            "\u{FFFFE}\u{FFFFF}",
            [
                "F3BFBFBEF3BFBFBF",
                "F3BFBFBEF3BFBFBF",
                "FFFDFFFD",
                "FFFDFFFD",
                "FBDFFFFEFBDFFFFF",
                "F3BFBFBEF3BFBFBF",
                "3F3F",
                "3F3F",
            ],
        ),
    ];
    let collations = [
        Collation::Binary,
        Collation::Utf8Mb4Bin,
        Collation::Utf8Mb4GeneralCi,
        Collation::Utf8Mb4UnicodeCi,
        Collation::Utf8Mb40900AiCi,
        Collation::Utf8Mb40900Bin,
        Collation::GbkBin,
        Collation::GbkChineseCi,
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

#[test]
fn registry_and_mode_execute_all_original_get_collator_rows() {
    let _guard = crate::charset::REGISTRY_TEST_LOCK
        .lock()
        .expect("charset test lock poisoned");
    set_new_collation_enabled(true);
    let rows = [
        ("binary", Collation::Binary),
        ("utf8mb4_bin", Collation::Utf8Mb4Bin),
        ("utf8_bin", Collation::Utf8Bin),
        ("utf8mb4_general_ci", Collation::Utf8Mb4GeneralCi),
        ("utf8_general_ci", Collation::Utf8GeneralCi),
        ("utf8mb4_unicode_ci", Collation::Utf8Mb4UnicodeCi),
        ("utf8_unicode_ci", Collation::Utf8UnicodeCi),
        (
            "utf8mb4_zh_pinyin_tidb_as_cs",
            Collation::Utf8Mb4ZhPinyinTiDbAsCs,
        ),
        ("utf8mb4_0900_ai_ci", Collation::Utf8Mb40900AiCi),
        ("utf8mb4_0900_bin", Collation::Utf8Mb40900Bin),
        ("gbk_bin", Collation::GbkBin),
    ];
    for (name, expected) in rows {
        assert_eq!(Collation::from_name(name), Some(expected));
        assert_eq!(expected.id(), collation_name_to_id(name));
        assert_eq!(get_collator(name), Collator::New(expected));
    }
    for collation in [
        Collation::Binary,
        Collation::AsciiBin,
        Collation::Latin1Bin,
        Collation::Utf8Bin,
        Collation::Utf8GeneralCi,
        Collation::Utf8UnicodeCi,
        Collation::Utf8Mb4Bin,
        Collation::Utf8Mb4GeneralCi,
        Collation::Utf8Mb4UnicodeCi,
        Collation::Utf8Mb40900AiCi,
        Collation::Utf8Mb40900Bin,
        Collation::Utf8Mb4ZhPinyinTiDbAsCs,
        Collation::GbkBin,
        Collation::GbkChineseCi,
        Collation::Gb18030Bin,
        Collation::Gb18030ChineseCi,
    ] {
        assert_eq!(collation_id_to_name(collation.id()), collation.name());
    }
    assert_eq!(
        get_collator("default_test"),
        Collator::New(Collation::Utf8Mb4Bin)
    );
    for (id, expected) in [
        (63, Collation::Binary),
        (46, Collation::Utf8Mb4Bin),
        (83, Collation::Utf8Bin),
        (45, Collation::Utf8Mb4GeneralCi),
        (33, Collation::Utf8GeneralCi),
        (224, Collation::Utf8Mb4UnicodeCi),
        (192, Collation::Utf8UnicodeCi),
        (255, Collation::Utf8Mb40900AiCi),
        (2048, Collation::Utf8Mb4ZhPinyinTiDbAsCs),
        (87, Collation::GbkBin),
    ] {
        assert_eq!(get_collator_by_id(id), Collator::New(expected));
    }
    assert_eq!(
        get_collator_by_id(9999),
        Collator::New(Collation::Utf8Mb4Bin)
    );

    set_new_collation_enabled(false);
    for name in [
        "binary",
        "utf8mb4_bin",
        "utf8_bin",
        "utf8mb4_general_ci",
        "utf8_general_ci",
        "utf8mb4_unicode_ci",
        "utf8_unicode_ci",
        "utf8mb4_zh_pinyin_tidb_as_cs",
        "utf8mb4_0900_ai_ci",
        "default_test",
    ] {
        assert_eq!(get_collator(name), Collator::DerivedBinary);
    }
    for id in [63, 46, 83, 45, 33, 224, 255, 309, 192, 2048, 9999] {
        assert_eq!(get_collator_by_id(id), Collator::DerivedBinary);
    }
    set_new_collation_enabled(false);
}

/// Every general-CI and UCA-4.0 assertion attributable to
/// `TestCampareInvalidUTF8Rune`. Rust byte slices preserve the Go-string input
/// state: compare returns equality at the first invalid sequence and key keeps
/// the valid prefix.
#[test]
fn invalid_utf8_executes_all_original_collator_assertions() {
    for (index, collation) in [
        Collation::Utf8Mb4GeneralCi,
        Collation::Utf8Mb40900AiCi,
        Collation::Utf8Mb4UnicodeCi,
        Collation::GbkChineseCi,
        Collation::Gb18030Bin,
        Collation::GbkBin,
    ]
    .into_iter()
    .enumerate()
    {
        assert_eq!(collation.compare(&[0xFF], &[0xFF]), Ordering::Equal);
        assert_eq!(collation.compare(&[0xFF], &[0xFE]), Ordering::Equal);
        if index < 4 {
            assert_eq!(collation.compare(&[0xFF], &[0xFF, 0x3E]), Ordering::Equal);
            assert_eq!(
                collation.compare(&[0x3E, 0xFF], &[0x3E, 0xFF, 0x3E]),
                Ordering::Equal
            );
        }

        let invalid_at_start = collation.key(&[0xFF]);
        let invalid_after_prefix = collation.key(&[0x3E, 0xFF]);
        let suffix_after_invalid = collation.key(&[0x3E, 0xFF, 0x3E]);
        if index < 4 {
            assert!(invalid_at_start.is_empty());
            assert!(!invalid_after_prefix.is_empty());
            assert_eq!(suffix_after_invalid, invalid_after_prefix);
        } else {
            assert_eq!(invalid_at_start, b"?");
            assert!(!invalid_after_prefix.is_empty());
            assert!(!suffix_after_invalid.is_empty());
        }
    }
}

#[test]
fn mode_id_and_helper_functions_follow_source() {
    let _guard = crate::charset::REGISTRY_TEST_LOCK
        .lock()
        .expect("charset test lock poisoned");
    set_new_collation_enabled(true);
    assert!(new_collation_enabled());
    assert_eq!(
        get_charset_info("gbk").unwrap().default_collation,
        "gbk_chinese_ci"
    );
    assert_eq!(rewrite_new_collation_id_if_needed(5), -5);
    assert_eq!(rewrite_new_collation_id_if_needed(-5), -5);
    assert_eq!(rewrite_new_collation_id_if_needed(i32::MIN), i32::MIN);
    assert_eq!(restore_collation_id_if_needed(-5), 5);
    assert_eq!(restore_collation_id_if_needed(5), 5);
    assert_eq!(restore_collation_id_if_needed(i32::MIN), i32::MIN);
    assert_eq!(collation_to_proto("utf8mb4_bin"), -46);
    assert_eq!(proto_to_collation(-46), "utf8mb4_bin");

    set_new_collation_enabled(false);
    assert_eq!(
        get_charset_info("gbk").unwrap().default_collation,
        "gbk_bin"
    );
    assert_eq!(rewrite_new_collation_id_if_needed(5), 5);
    assert_eq!(rewrite_new_collation_id_if_needed(-5), -5);
    assert_eq!(restore_collation_id_if_needed(5), 5);
    assert_eq!(restore_collation_id_if_needed(-5), -5);
    assert_eq!(
        get_collator_with_mode(false, "binary"),
        Collator::DerivedBinary
    );
    set_new_collation_enabled(true);

    assert!(compatible_collate("utf8_general_ci", "utf8mb4_general_ci"));
    assert!(compatible_collate("latin1_bin", "utf8mb4_bin"));
    assert!(compatible_collate("utf8_unicode_ci", "utf8mb4_unicode_ci"));
    assert!(!compatible_collate("utf8_general_ci", "utf8_unicode_ci"));
    assert_eq!(collation_name_to_id("utf8mb4_0900_ai_ci"), 255);
    assert_eq!(collation_name_to_id("missing"), 46);
    assert_eq!(collation_id_to_name(255), "utf8mb4_0900_ai_ci");
    assert_eq!(collation_id_to_name(9999), "utf8mb4_bin");
    assert_eq!(
        substitute_missing_collation_to_default("missing"),
        "utf8mb4_bin"
    );
    assert_eq!(
        get_supported_collation_by_name("utf8mb4_0900_as_cs")
            .unwrap_err()
            .to_string(),
        "[ddl:1273]Unsupported collation when new collation is enabled: 'utf8mb4_0900_as_cs'"
    );
    assert!(supported_collations()
        .windows(2)
        .all(|rows| rows[0].name <= rows[1].name));

    assert!(is_default_collation_for_utf8mb4("utf8mb4_0900_ai_ci"));
    assert!(is_ci_collation("gb18030_chinese_ci"));
    assert_eq!(binary_collation_name("utf8mb4_unicode_ci"), "utf8mb4_bin");
    assert_eq!(binary_collation_name("binary"), "binary");
    assert_eq!(
        binary_collator("utf8mb4_unicode_ci"),
        Collator::New(Collation::Utf8Mb4Bin)
    );
    assert_eq!(
        get_binary_collator_slice(3),
        vec![Collator::DerivedBinary; 3]
    );
    assert!(is_bin_collation("utf8mb4_0900_bin"));
    assert!(!is_bin_collation("gbk_bin"));
    assert!(!is_pad_space_collation("utf8mb4_0900_ai_ci"));
    assert!(is_pad_space_collation("gbk_chinese_ci"));
    set_new_collation_enabled(false);
}

#[test]
fn wildcard_patterns_follow_each_source_collator_family() {
    let _guard = crate::charset::REGISTRY_TEST_LOCK
        .lock()
        .expect("charset test lock poisoned");
    set_new_collation_enabled(true);
    for (collator, pattern, matching, rejected) in [
        (Collator::New(Collation::Binary), "a_中%", "ab中文", "a中文"),
        (Collator::DerivedBinary, "a_中%", "ab中文", "a中文"),
        (
            Collator::New(Collation::Utf8Mb4GeneralCi),
            "À%",
            "abc",
            "xbc",
        ),
        (Collator::New(Collation::Utf8Mb4UnicodeCi), "ß", "ß", "ss"),
        (Collator::New(Collation::Utf8Mb40900AiCi), "æ", "æ", "x"),
        (Collator::New(Collation::GbkChineseCi), "a%", "ABC", "xbc"),
        (
            Collator::New(Collation::Gb18030ChineseCi),
            "a%",
            "ABC",
            "xbc",
        ),
    ] {
        let compiled = collator.pattern(pattern, b'\\');
        assert!(
            compiled.is_match(matching.as_bytes()),
            "{collator:?} {pattern}"
        );
        assert!(
            !compiled.is_match(rejected.as_bytes()),
            "{collator:?} {pattern}"
        );
    }
    let escaped = get_collator("binary").pattern(r"a\%b", b'\\');
    assert!(escaped.is_match(b"a%b"));
    assert!(!escaped.is_match(b"axxb"));
    let reordered = get_collator("binary").pattern("%_", b'\\');
    assert!(reordered.is_match(b"x"));
    assert!(reordered.is_match(b"xyz"));
    set_new_collation_enabled(false);
}

#[test]
#[should_panic(expected = "utf8mb4_zh_pinyin_tidb_as_cs is not implemented")]
fn pinyin_stub_preserves_source_panic() {
    let _ = Collation::Utf8Mb4ZhPinyinTiDbAsCs.key(b"value");
}

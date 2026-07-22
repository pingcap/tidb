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

//! Complete original-test translation for `pkg/parser/charset`.

use tidb_datatype::{
    add_charset, add_collation, count_valid_bytes, find_encoding, get_charset_info,
    get_collation_by_name, get_default_collation, get_default_collation_legacy,
    get_supported_charsets, get_supported_collations, remove_charset, valid_charset_and_collation,
    CharsetInfo, CollationInfo, Encoding, TransformOp, PAD_NONE,
};

static REGISTRY_TEST_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

#[test]
fn test_valid_charset() {
    let _guard = REGISTRY_TEST_LOCK.lock().unwrap();
    for (charset, collation, expected) in [
        ("utf8", "utf8_general_ci", true),
        ("", "utf8_general_ci", true),
        ("utf8mb4", "utf8mb4_bin", true),
        ("latin1", "latin1_bin", true),
        ("utf8", "utf8_invalid_ci", false),
        ("utf16", "utf16_bin", false),
        ("gb2312", "gb2312_chinese_ci", false),
        ("UTF8", "UTF8_BIN", true),
        ("UTF8", "utf8_bin", true),
        ("UTF8MB4", "utf8mb4_bin", true),
        ("UTF8MB4", "UTF8MB4_bin", true),
        ("UTF8MB4", "UTF8MB4_general_ci", true),
        ("Utf8", "uTf8_bIN", true),
        ("utf8mb3", "", true),
        ("utf8mb3", "utf8mb3_bin", true),
        ("utf8mb3", "utf8mb3_general_ci", true),
        ("utf8mb3", "utf8mb3_unicode_ci", true),
    ] {
        assert_eq!(valid_charset_and_collation(charset, collation), expected);
    }
}

#[test]
fn test_get_default_collation() {
    let _guard = REGISTRY_TEST_LOCK.lock().unwrap();
    for (charset, expected) in [
        ("utf8", Some("utf8_bin")),
        ("UTF8", Some("utf8_bin")),
        ("utf8mb4", Some("utf8mb4_bin")),
        ("ascii", Some("ascii_bin")),
        ("binary", Some("binary")),
        ("latin1", Some("latin1_bin")),
        ("invalid_cs", None),
        ("", None),
    ] {
        assert_eq!(get_default_collation(charset).ok().as_deref(), expected);
    }
    let supported = get_supported_charsets();
    let defaults: Vec<_> = get_supported_collations()
        .into_iter()
        .filter(|row| row.is_default)
        .collect();
    assert_eq!(supported.len(), defaults.len());
    for charset in supported {
        assert!(defaults.iter().any(|row| {
            row.charset_name == charset.name && row.name == charset.default_collation
        }));
    }
}

#[test]
fn test_get_charset_desc() {
    let _guard = REGISTRY_TEST_LOCK.lock().unwrap();
    for (charset, expected) in [
        ("utf8", Some("utf8")),
        ("UTF8", Some("utf8")),
        ("utf8mb4", Some("utf8mb4")),
        ("ascii", Some("ascii")),
        ("binary", Some("binary")),
        ("latin1", Some("latin1")),
        ("invalid_cs", None),
        ("", None),
    ] {
        assert_eq!(
            get_charset_info(charset).ok().map(|info| info.name),
            expected.map(str::to_owned)
        );
    }
}

#[test]
fn test_get_collation_by_name() {
    let _guard = REGISTRY_TEST_LOCK.lock().unwrap();
    assert_eq!(get_collation_by_name("UTF8MB4_BIN").unwrap().id, 46);
    assert_eq!(get_collation_by_name("utf8mb3_unicode_ci").unwrap().id, 192);
    assert_eq!(
        get_collation_by_name("non_exist").unwrap_err().to_string(),
        "[ddl:1273]Unknown collation: 'non_exist'"
    );
}

#[test]
fn test_valid_custom_charset() {
    let _guard = REGISTRY_TEST_LOCK.lock().unwrap();
    add_charset(CharsetInfo {
        name: "custom".to_owned(),
        default_collation: "custom_collation".to_owned(),
        collations: Default::default(),
        description: "Custom".to_owned(),
        maxlen: 4,
    });
    add_collation(CollationInfo {
        id: 99_999,
        charset_name: "custom".to_owned(),
        name: "custom_collation".to_owned(),
        is_default: true,
        sortlen: 8,
        pad_attribute: PAD_NONE.to_owned(),
    });
    assert!(valid_charset_and_collation("custom", "custom_collation"));
    assert!(!valid_charset_and_collation("utf8", "utf8_invalid_ci"));
    remove_charset("custom");
}

#[test]
fn test_utf8mb3() {
    let _guard = REGISTRY_TEST_LOCK.lock().unwrap();
    assert_eq!(get_default_collation_legacy("utf8mb3").unwrap(), "utf8_bin");
    assert_eq!(get_charset_info("utf8mb3").unwrap().name, "utf8");
    for (alias, canonical) in [
        ("utf8mb3_bin", "utf8_bin"),
        ("utf8mb3_general_ci", "utf8_general_ci"),
        ("utf8mb3_unicode_ci", "utf8_unicode_ci"),
    ] {
        assert_eq!(get_collation_by_name(alias).unwrap().name, canonical);
    }
}

#[test]
fn benchmark_get_charset_desc_obligation_executes_one_lookup() {
    let _guard = REGISTRY_TEST_LOCK.lock().unwrap();
    assert_eq!(get_charset_info("utf8mb4").unwrap().name, "utf8mb4");
}

fn joined(parts: &[&[u8]]) -> Vec<u8> {
    parts.concat()
}

fn assert_transform(
    encoding: Encoding,
    source: &[u8],
    operation: TransformOp,
    expected: &[u8],
    valid: bool,
) {
    let result = encoding.transform(source, operation);
    assert_eq!(result.bytes(), expected, "source={source:?}");
    assert_eq!(result.error().is_none(), valid, "source={source:?}");
}

#[test]
fn test_encoding() {
    let gbk = find_encoding("gbk");
    assert_eq!(gbk.name(), "gbk");
    let text = "一二三四".as_bytes();
    let encoded = gbk.transform(text, TransformOp::ENCODE);
    assert!(encoded.error().is_none());
    assert_transform(gbk, encoded.bytes(), TransformOp::DECODE, text, true);

    let decode_cases: Vec<(Vec<u8>, Vec<u8>, bool)> = vec![
        (
            "一二三".as_bytes().to_vec(),
            "涓?簩涓?".as_bytes().to_vec(),
            false,
        ),
        (
            "一二三123".as_bytes().to_vec(),
            "涓?簩涓?23".as_bytes().to_vec(),
            false,
        ),
        (
            "测试".as_bytes().to_vec(),
            "娴嬭瘯".as_bytes().to_vec(),
            true,
        ),
        (
            "案1案2".as_bytes().to_vec(),
            "妗?妗?".as_bytes().to_vec(),
            false,
        ),
        (
            "焊䏷菡釬".as_bytes().to_vec(),
            "鐒婁彿鑿￠嚞".as_bytes().to_vec(),
            true,
        ),
        (
            "鞍杏以伊位依".as_bytes().to_vec(),
            "闉嶆潖浠ヤ紛浣嶄緷".as_bytes().to_vec(),
            true,
        ),
        (
            "移維緯胃萎衣謂違".as_bytes().to_vec(),
            "绉荤董绶?儍钀庤。璎傞仌".as_bytes().to_vec(),
            false,
        ),
        (
            "仆仂仗仞仭仟价伉佚估".as_bytes().to_vec(),
            "浠嗕粋浠椾粸浠?粺浠蜂級浣氫及".as_bytes().to_vec(),
            false,
        ),
        (
            "佝佗佇佶侈侏侘佻佩佰侑佯".as_bytes().to_vec(),
            "浣濅綏浣囦蕉渚堜緩渚樹交浣╀桨渚戜蒋".as_bytes().to_vec(),
            true,
        ),
        (b"\x80".to_vec(), b"?".to_vec(), false),
        (b"\x80a".to_vec(), b"?".to_vec(), false),
        (b"\x80aa".to_vec(), b"?a".to_vec(), false),
        (b"aa\x80ab".to_vec(), b"aa?b".to_vec(), false),
        (
            joined(&["a你好".as_bytes(), b"\x80", "a测试".as_bytes()]),
            "a浣犲ソ?娴嬭瘯".as_bytes().to_vec(),
            false,
        ),
        (b"aa\x80".to_vec(), b"aa?".to_vec(), false),
    ];
    for (source, expected, valid) in decode_cases {
        assert_transform(gbk, &source, TransformOp::DECODE_REPLACE, &expected, valid);
    }
    for (source, expected, valid) in [
        ("一二三", b"\xD2\xBB\xB6\xFE\xC8\xFD".as_slice(), true),
        ("🀁", b"?", false),
        ("valid_string_🀁", b"valid_string_?", false),
        ("€", b"?", false),
        ("€a", b"?a", false),
        ("a€aa", b"a?aa", false),
        ("aaa€", b"aaa?", false),
    ] {
        assert_transform(
            gbk,
            source.as_bytes(),
            TransformOp::ENCODE_REPLACE,
            expected,
            valid,
        );
    }
}

#[test]
fn test_encoding_validate() {
    let invalid = b"\xFF\xFE\xFD";
    let rows: Vec<(&str, Vec<u8>, Vec<u8>, bool)> = vec![
        ("ascii", vec![], vec![], true),
        ("ascii", b"qwerty".to_vec(), b"qwerty".to_vec(), true),
        (
            "ascii",
            "qwÊrty".as_bytes().to_vec(),
            b"qw?rty".to_vec(),
            false,
        ),
        ("ascii", "中文".as_bytes().to_vec(), b"??".to_vec(), false),
        (
            "utf8mb4",
            "😂".as_bytes().to_vec(),
            "😂".as_bytes().to_vec(),
            true,
        ),
        ("utf8mb4", invalid.to_vec(), b"???".to_vec(), false),
        (
            "utf8",
            "valid_str😂".as_bytes().to_vec(),
            b"valid_str?".to_vec(),
            false,
        ),
        ("utf8", invalid.to_vec(), b"???".to_vec(), false),
        (
            "gbk",
            "中文".as_bytes().to_vec(),
            "中文".as_bytes().to_vec(),
            true,
        ),
        (
            "gbk",
            "中文À中文".as_bytes().to_vec(),
            "中文?中文".as_bytes().to_vec(),
            false,
        ),
        (
            "gb18030",
            "中文À中文".as_bytes().to_vec(),
            "中文À中文".as_bytes().to_vec(),
            true,
        ),
        (
            "gb18030",
            "😂".as_bytes().to_vec(),
            "😂".as_bytes().to_vec(),
            true,
        ),
    ];
    for (charset, source, expected, valid) in rows {
        let encoding = if charset == "utf8" {
            Encoding::Utf8Mb3Strict
        } else {
            find_encoding(charset)
        };
        assert_eq!(encoding.is_valid(&source), valid, "{charset}");
        assert_eq!(
            encoding
                .transform(&source, TransformOp::REPLACE_NO_ERR)
                .bytes(),
            expected,
            "{charset}"
        );
        let _ = count_valid_bytes(encoding, &source);
    }
}

#[test]
fn test_encoding_gb18030() {
    let encoding = find_encoding("gb18030");
    assert_eq!(encoding.name(), "gb18030");
    let text = "一二三四".as_bytes();
    let encoded = encoding.transform(text, TransformOp::ENCODE);
    assert!(encoded.error().is_none());
    assert_transform(encoding, encoded.bytes(), TransformOp::DECODE, text, true);

    for (source, expected, valid) in [
        ("一二三".as_bytes(), "涓?浜屼笁".as_bytes(), false),
        ("测试".as_bytes(), "娴嬭瘯".as_bytes(), true),
        (
            "移維緯胃萎衣謂違".as_bytes(),
            "绉荤董绶\u{e21d}儍钀庤。璎傞仌".as_bytes(),
            true,
        ),
        (
            "仆仂仗仞仭仟价伉佚估".as_bytes(),
            "浠嗕粋浠椾粸浠\u{e15d}粺浠蜂級浣氫及".as_bytes(),
            true,
        ),
    ] {
        assert_transform(
            encoding,
            source,
            TransformOp::DECODE_REPLACE,
            expected,
            valid,
        );
    }
    for (source, expected, valid) in [
        (b"\x80".as_slice(), b"?".as_slice(), false),
        (b"\x80a", b"?a", false),
        (b"aa\x80ab", b"aa?ab", false),
        (
            b"\xB0\xB2\x84\x31\xA4\x37\x30\x84\x31\xA4\x37\x32",
            "安�0�2".as_bytes(),
            true,
        ),
        (
            b"\x80\x84\x31\xA4\x37\x80\x84\x31\xA4\x37",
            "?�?�".as_bytes(),
            false,
        ),
        (b"\x84\x31\xA4\x37\x81", "�?".as_bytes(), false),
    ] {
        assert_transform(
            encoding,
            source,
            TransformOp::DECODE_REPLACE,
            expected,
            valid,
        );
    }
    for (source, expected) in [
        ("一二三", b"\xD2\xBB\xB6\xFE\xC8\xFD".as_slice()),
        ("🀁", b"\x94\x38\xE1\x31"),
        ("€", b"\xA2\xE3"),
        ("€a", b"\xA2\xE3a"),
        ("a€aa", b"a\xA2\xE3aa"),
        ("aaa€", b"aaa\xA2\xE3"),
        ("ḿ", b"\xA8\xBC"),
    ] {
        assert_transform(
            encoding,
            source.as_bytes(),
            TransformOp::ENCODE_REPLACE,
            expected,
            true,
        );
    }
}

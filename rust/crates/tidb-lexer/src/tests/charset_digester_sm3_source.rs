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

//! Go-derived tests for the charset/collation lookup boundaries this crate
//! owns (`pkg/parser/charset`), against [`crate::canonical_charset`] /
//! [`crate::canonical_collation`] / [`crate::canonical_legacy_charset`].

// ---------------------------------------------------------------------------
// pkg/parser/charset/charset_test.go ports
// ---------------------------------------------------------------------------

/// Mirrors Go `Charset.Collations` membership for the five charsets in
/// `CharacterSetInfos` (`pkg/parser/charset/charset.go`). Every collation of
/// a supported charset is named `<charset>_...`, except the `binary`
/// charset whose single collation is itself named `binary`.
fn collation_in_charset(collation: &str, charset: &str) -> bool {
    if charset == "binary" {
        collation == "binary"
    } else {
        collation.starts_with(&format!("{charset}_"))
    }
}

/// Mirrors Go `ValidCharsetAndCollation` over this crate's catalogs:
/// empty/utf8mb3 collapse to utf8, unknown-or-unsupported charsets fail,
/// an empty collation inherits the default, otherwise the (lowercased,
/// utf8mb3-aliased) collation must belong to the charset.
fn valid_charset_and_collation(cs: &str, co: &str) -> bool {
    // Go collapses "" and "utf8mb3" to utf8 before the info lookup.
    let cs = if cs.is_empty() { "utf8" } else { cs };
    let canon = crate::canonical_legacy_charset(cs);
    let Some(charset) = canon else {
        return false;
    };
    if co.is_empty() {
        return true;
    }
    match crate::canonical_collation(co) {
        Some(collation) => collation_in_charset(collation, charset),
        None => false,
    }
}

/// Default collations of the five charsets in Go's `CharacterSetInfos`.
fn default_collation(charset: &str) -> Option<&'static str> {
    match charset {
        "utf8" => Some("utf8_bin"),
        "utf8mb4" => Some("utf8mb4_bin"),
        "ascii" => Some("ascii_bin"),
        "latin1" => Some("latin1_bin"),
        "binary" => Some("binary"),
        _ => None,
    }
}

/// Go: pkg/parser/charset/charset_test.go::TestValidCharset
#[test]
fn test_valid_charset() {
    let tests = [
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
    ];
    for (cs, co, succ) in tests {
        assert_eq!(
            valid_charset_and_collation(cs, co),
            succ,
            "charset={cs:?} collation={co:?}"
        );
    }
}

/// Go: pkg/parser/charset/charset_test.go::TestGetDefaultCollation
#[test]
fn test_get_default_collation() {
    let tests = [
        ("utf8", "utf8_bin", true),
        ("UTF8", "utf8_bin", true),
        ("utf8mb4", "utf8mb4_bin", true),
        ("ascii", "ascii_bin", true),
        ("binary", "binary", true),
        ("latin1", "latin1_bin", true),
        ("invalid_cs", "", false),
        ("", "utf8_bin", false),
    ];
    for (cs, expect, succ) in tests {
        let got = crate::canonical_legacy_charset(cs).and_then(default_collation);
        if !succ {
            assert!(got.is_none(), "charset={cs:?}");
        } else {
            assert_eq!(got, Some(expect), "charset={cs:?}");
        }
    }

    // Consistency between the collation table and the charset desc table:
    // every supported charset's default collation must exist in the
    // collation catalog, and the number of resolved defaults equals the
    // number of supported charsets (5).
    let supported = ["utf8", "utf8mb4", "ascii", "latin1", "binary"];
    let mut charset_num = 0;
    for charset in supported {
        let desc = default_collation(charset).expect("supported charset");
        assert!(
            crate::collation::COLLATION_NAMES
                .binary_search(&desc)
                .is_ok(),
            "default collation {desc} missing from catalog"
        );
        charset_num += 1;
    }
    assert_eq!(supported.len(), charset_num);
}

/// Go: pkg/parser/charset/charset_test.go::TestGetCharsetDesc
#[test]
fn test_get_charset_desc() {
    let tests = [
        ("utf8", "utf8", true),
        ("UTF8", "utf8", true),
        ("utf8mb4", "utf8mb4", true),
        ("ascii", "ascii", true),
        ("binary", "binary", true),
        ("latin1", "latin1", true),
        ("invalid_cs", "", false),
        ("", "utf8_bin", false),
    ];
    for (cs, result, succ) in tests {
        match crate::canonical_charset(cs) {
            None => assert!(!succ, "charset={cs:?}"),
            Some(name) => {
                assert!(succ, "charset={cs:?}");
                assert_eq!(result, name, "charset={cs:?}");
            }
        }
    }
}

/// Go: pkg/parser/charset/charset_test.go::TestGetCollationByName
///
/// Go asserts each entry of its `collations` table resolves to itself and
/// that an unknown name fails with `[ddl:1273]Unknown collation: 'non_exist'`.
/// This crate stores names rather than `*Collation` records, so identity of
/// resolution is pinned through the canonicalizer; the error text is a Go
/// error-string boundary not carried here.
#[test]
fn test_get_collation_by_name() {
    for name in crate::collation::COLLATION_NAMES {
        assert_eq!(
            crate::canonical_collation(name),
            Some(*name),
            "collation={name}"
        );
    }
    assert_eq!(crate::canonical_collation("non_exist"), None);
}

/// Go: pkg/parser/charset/charset_test.go::TestUTF8MB3
#[test]
fn test_utf8_mb3() {
    // GetDefaultCollationLegacy("utf8mb3") == "utf8_bin".
    assert_eq!(
        crate::canonical_legacy_charset("utf8mb3").and_then(default_collation),
        Some("utf8_bin")
    );
    // GetCharsetInfo("utf8mb3").Name == "utf8".
    assert_eq!(crate::canonical_charset("utf8mb3"), Some("utf8"));

    let tests = [
        ("utf8mb3_bin", "utf8_bin"),
        ("utf8mb3_general_ci", "utf8_general_ci"),
        ("utf8mb3_unicode_ci", "utf8_unicode_ci"),
    ];
    for (name, alias) in tests {
        assert_eq!(crate::canonical_collation(name), Some(alias), "name={name}");
    }
}

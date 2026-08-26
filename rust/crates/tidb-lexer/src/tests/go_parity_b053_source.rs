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

//! Batch b053 port of `pkg/parser` part-3 unit tests (Go tests sorted by
//! file path + line number, items 121-180 on origin/master).
//!
//! The only behaviors this crate owns end-to-end are the charset/collation
//! lookup boundaries (`pkg/parser/charset`), which are genuinely ported
//! below against [`crate::canonical_charset`] /
//! [`crate::canonical_collation`] / [`crate::canonical_legacy_charset`].
//! Every other Go test in the range exercises a subsystem owned by another
//! crate or not yet transcreated (ast, auth, encoding tables, digester,
//! duration, format, hint parser, lateral parsing); those carry explicit
//! `go-parity-gap` ignores rather than approximations.

use super::*;

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
            crate::collation::COLLATION_NAMES.binary_search(&desc).is_ok(),
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

// ---------------------------------------------------------------------------
// go-parity gaps: Go tests in the b053 range whose behavior lives outside
// this crate's transcreated surface.
// ---------------------------------------------------------------------------

// Go: pkg/parser/ast/sem_test.go::TestAdminCommand
#[test]
#[ignore = "go-parity-gap: ast SEM admin-command classification lives in tidb-parser/tidb-ast, not tidb-lexer"]
fn test_admin_command() {}

// Go: pkg/parser/ast/sem_test.go::TestBRIECommand
#[test]
#[ignore = "go-parity-gap: ast SEM BRIE-command classification lives in tidb-parser/tidb-ast, not tidb-lexer"]
fn test_brie_command() {}

// Go: pkg/parser/ast/stats_test.go::TestRefreshStatsStmt
#[test]
#[ignore = "go-parity-gap: RefreshStatsStmt AST node lives in tidb-ast, not tidb-lexer"]
fn test_refresh_stats_stmt() {}

// Go: pkg/parser/ast/stats_test.go::TestFlushStatsDeltaScoped
#[test]
#[ignore = "go-parity-gap: FlushStatsDelta AST node lives in tidb-ast, not tidb-lexer"]
fn test_flush_stats_delta_scoped() {}

// Go: pkg/parser/ast/stats_test.go::TestRefreshStatsStmtDedup
#[test]
#[ignore = "go-parity-gap: RefreshStatsStmt AST dedup lives in tidb-ast, not tidb-lexer"]
fn test_refresh_stats_stmt_dedup() {}

// Go: pkg/parser/ast/util_test.go::TestCacheable
#[test]
#[ignore = "go-parity-gap: AST cacheable/union classification lives in tidb-ast, not tidb-lexer"]
fn test_cacheable() {}

// Go: pkg/parser/ast/util_test.go::TestUnionReadOnly
#[test]
#[ignore = "go-parity-gap: AST union read-only walk lives in tidb-ast, not tidb-lexer"]
fn test_union_read_only() {}

// Go: pkg/parser/auth/caching_sha2_test.go::TestCheckShaPasswordGood
#[test]
#[ignore = "go-parity-gap: caching_sha2 password verification is unported (tidb-parser/auth boundary, crypto deps)"]
fn test_check_sha_password_good() {}

// Go: pkg/parser/auth/caching_sha2_test.go::TestCheckShaPasswordBad
#[test]
#[ignore = "go-parity-gap: caching_sha2 password verification is unported"]
fn test_check_sha_password_bad() {}

// Go: pkg/parser/auth/caching_sha2_test.go::TestCheckShaPasswordShort
#[test]
#[ignore = "go-parity-gap: caching_sha2 password verification is unported"]
fn test_check_sha_password_short() {}

// Go: pkg/parser/auth/caching_sha2_test.go::TestCheckShaPasswordDigestTypeIncompatible
#[test]
#[ignore = "go-parity-gap: caching_sha2 password verification is unported"]
fn test_check_sha_password_digest_type_incompatible() {}

// Go: pkg/parser/auth/caching_sha2_test.go::TestCheckShaPasswordIterationsInvalid
#[test]
#[ignore = "go-parity-gap: caching_sha2 password verification is unported"]
fn test_check_sha_password_iterations_invalid() {}

// Go: pkg/parser/auth/caching_sha2_test.go::TestNewSha2Password
#[test]
#[ignore = "go-parity-gap: caching_sha2 scramble generation is unported"]
fn test_new_sha2_password() {}

// Go: pkg/parser/auth/mysql_native_password_test.go::TestEncodePassword
#[test]
#[ignore = "go-parity-gap: mysql_native_password helpers are unported (SHA1 dependency)"]
fn test_encode_password() {}

// Go: pkg/parser/auth/mysql_native_password_test.go::TestDecodePassword
#[test]
#[ignore = "go-parity-gap: mysql_native_password helpers are unported (SHA1 dependency)"]
fn test_decode_password() {}

// Go: pkg/parser/auth/mysql_native_password_test.go::TestCheckScramble
#[test]
#[ignore = "go-parity-gap: mysql_native_password scramble check is unported (SHA1 dependency)"]
fn test_check_scramble() {}

// Go: pkg/parser/auth/tidb_sm3_test.go::TestSM3
#[test]
#[ignore = "go-parity-gap: SM3 hash primitive is unported (no SM3 crate in workspace)"]
fn test_sm3() {}

// Go: pkg/parser/auth/tidb_sm3_test.go::TestCheckSM3PasswordGood
#[test]
#[ignore = "go-parity-gap: SM3 password verification is unported (no SM3 crate in workspace)"]
fn test_check_sm3_password_good() {}

// Go: pkg/parser/auth/tidb_sm3_test.go::TestCheckSM3PasswordBad
#[test]
#[ignore = "go-parity-gap: SM3 password verification is unported (no SM3 crate in workspace)"]
fn test_check_sm3_password_bad() {}

// Go: pkg/parser/auth/tidb_sm3_test.go::TestCheckSM3PasswordShort
#[test]
#[ignore = "go-parity-gap: SM3 password verification is unported (no SM3 crate in workspace)"]
fn test_check_sm3_password_short() {}

// Go: pkg/parser/auth/tidb_sm3_test.go::TestCheckSM3PasswordDigestTypeIncompatible
#[test]
#[ignore = "go-parity-gap: SM3 password verification is unported (no SM3 crate in workspace)"]
fn test_check_sm3_password_digest_type_incompatible() {}

// Go: pkg/parser/auth/tidb_sm3_test.go::TestCheckSM3PasswordIterationsInvalid
#[test]
#[ignore = "go-parity-gap: SM3 password verification is unported (no SM3 crate in workspace)"]
fn test_check_sm3_password_iterations_invalid() {}

// Go: pkg/parser/auth/tidb_sm3_test.go::TestNewSM3Password
#[test]
#[ignore = "go-parity-gap: SM3 password generation is unported (no SM3 crate in workspace)"]
fn test_new_sm3_password() {}

// Go: pkg/parser/charset/charset_test.go::TestValidCustomCharset
#[test]
#[ignore = "go-parity-gap: runtime AddCharset/AddCollation registry mutation is not part of the static lexer catalogs"]
fn test_valid_custom_charset() {}

// Go: pkg/parser/charset/encoding_test.go::TestEncoding
#[test]
#[ignore = "go-parity-gap: GBK/GB18030/multibyte Encoding transform tables are unported in tidb-lexer"]
fn test_encoding() {}

// Go: pkg/parser/charset/encoding_test.go::TestEncodingValidate
#[test]
#[ignore = "go-parity-gap: Encoding validation tables (GBK/GB18030/GBoffset) are unported in tidb-lexer"]
fn test_encoding_validate() {}

// Go: pkg/parser/charset/encoding_test.go::TestEncodingGB18030
#[test]
#[ignore = "go-parity-gap: GB18030 4-byte decoding tables are unported in tidb-lexer"]
fn test_encoding_gb18030() {}

// Go: pkg/parser/digester_test.go::TestNormalize
#[test]
#[ignore = "go-parity-gap: SQL digester normalization lives in the digester surface, not yet transcreated into tidb-lexer"]
fn test_normalize() {}

// Go: pkg/parser/digester_test.go::TestNormalizeRedact
#[test]
#[ignore = "go-parity-gap: digester redaction mode is unported"]
fn test_normalize_redact() {}

// Go: pkg/parser/digester_test.go::TestNormalizeKeepHint
#[test]
#[ignore = "go-parity-gap: digester keep-hint mode is unported"]
fn test_normalize_keep_hint() {}

// Go: pkg/parser/digester_test.go::TestNormalizeDigest
#[test]
#[ignore = "go-parity-gap: digest computation over normalized SQL is unported"]
fn test_normalize_digest() {}

// Go: pkg/parser/digester_test.go::TestDigestHashEqForSimpleSQL
#[test]
#[ignore = "go-parity-gap: digest hash equality semantics are unported"]
fn test_digest_hash_eq_for_simple_sql() {}

// Go: pkg/parser/digester_test.go::TestDigestHashNotEqForSimpleSQL
#[test]
#[ignore = "go-parity-gap: digest hash inequality semantics are unported"]
fn test_digest_hash_not_eq_for_simple_sql() {}

// Go: pkg/parser/digester_test.go::TestGenDigest
#[test]
#[ignore = "go-parity-gap: GenDigest helper is unported"]
fn test_gen_digest() {}

// Go: pkg/parser/duration/duration_test.go::TestParseDuration
#[test]
#[ignore = "go-parity-gap: pkg/parser/duration MySQL time parsing is a separate package not owned by tidb-lexer"]
fn test_parse_duration() {}

// Go: pkg/parser/format/format_test.go::TestFormat
#[test]
#[ignore = "go-parity-gap: pkg/parser/format restore-context writer is not owned by tidb-lexer"]
fn test_format() {}

// Go: pkg/parser/format/format_test.go::TestRestoreCtx
#[test]
#[ignore = "go-parity-gap: RestoreCtx flag handling is not owned by tidb-lexer"]
fn test_restore_ctx() {}

// Go: pkg/parser/format/format_test.go::TestRestoreSpecialComment
#[test]
#[ignore = "go-parity-gap: special-comment restore formatting is not owned by tidb-lexer"]
fn test_restore_special_comment() {}

// Go: pkg/parser/hintparser_test.go::TestParseHint
#[test]
#[ignore = "go-parity-gap: optimizer-hint grammar (hintparser.y) lives in tidb-parser, not tidb-lexer"]
fn test_parse_hint() {}

// Go: pkg/parser/hintparser_test.go::TestMaxOptimizerHintDepth
#[test]
#[ignore = "go-parity-gap: optimizer-hint grammar depth limit lives in tidb-parser, not tidb-lexer"]
fn test_max_optimizer_hint_depth() {}

// Go: pkg/parser/lateral_test.go::TestLateralParsing
#[test]
#[ignore = "go-parity-gap: LATERAL parse acceptance requires the full yacc parser (tidb-parser), not the lexer"]
fn test_lateral_parsing() {}

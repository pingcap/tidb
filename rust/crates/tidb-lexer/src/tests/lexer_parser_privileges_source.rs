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

//! Executable scanner-owned behavior derived from pinned Go
//! `pkg/parser/lexer_test.go` and `pkg/parser/parser_test.go` cases. The tests
//! are re-derived from `Scanner.scan` / `Scanner.Lex` /
//! `isTokenIdentifier`.
//! mysql/opcode/yacc-parser surfaces live with their owning crates and are
//! deliberately not represented here.

use super::*;

fn first(sql: &str) -> Token {
    Lexer::new(sql).next_token()
}

fn first_with_mode(sql: &str, mode: SqlMode) -> Token {
    Lexer::new(sql).with_sql_mode(mode).next_token()
}

fn tokens(sql: &str) -> Vec<Token> {
    Lexer::new(sql)
        .tokenize()
        .into_iter()
        .filter(|token| token.kind != TokenKind::Eof)
        .collect()
}

fn kinds(sql: &str) -> Vec<TokenKind> {
    tokens(sql).into_iter().map(|token| token.kind).collect()
}

fn decoded_string(sql: &str) -> Vec<u8> {
    decoded_string_with_mode(sql, SqlMode::default())
}

fn decoded_string_with_mode(sql: &str, mode: SqlMode) -> Vec<u8> {
    match Lexer::new(sql).with_sql_mode(mode).next_literal() {
        LiteralValue::String(bytes) => bytes,
        other => panic!("expected string literal for {sql:?}, got {other:?}"),
    }
}

fn assert_token(sql: &str, kind: TokenKind, text: &str) {
    let token = first(sql);
    assert_eq!(token.kind, kind, "input={sql:?}");
    assert_eq!(token.text, text, "input={sql:?}");
}

fn is_general_keyword(word: &str) -> bool {
    crate::keywords::GENERAL_KEYWORDS
        .binary_search(&word)
        .is_ok()
}

fn is_builtin_keyword(word: &str) -> bool {
    crate::keywords::BUILTIN_FUNC_KEYWORDS
        .binary_search(&word)
        .is_ok()
}

fn is_window_keyword(word: &str) -> bool {
    crate::keywords::WINDOW_FUNC_KEYWORDS
        .binary_search(&word)
        .is_ok()
}

fn is_scanner_keyword(word: &str) -> bool {
    is_general_keyword(word) || is_window_keyword(word) || is_builtin_keyword(word)
}

// ---------------------------------------------------------------------------
// pkg/parser/lexer_test.go
// ---------------------------------------------------------------------------

/// Go: `pkg/parser/lexer_test.go::TestScanString`.
///
/// Pins `Scanner.scanString` / `handleEscape` (`lexer.go`): the first token
/// is a string literal and `LexLiteral` / `next_literal` yields the unescaped
/// inner bytes (`util.UnescapeChar`). Two Go vectors (`'\a\x90'` and friends
/// with isolated non-UTF-8 bytes) cannot be represented as Rust `&str` and
/// are omitted rather than lossily recoded.
#[test]
fn test_scan_string() {
    let cases: &[(&str, &[u8])] = &[
        ("' \\n\\tTest String'", b" \n\tTest String"),
        ("'\\x\\B'", b"xB"),
        ("'\\0\\'\\\"\\b\\n\\r\\t\\\\'", b"\0'\"\x08\n\r\t\\"),
        ("'\\Z'", b"\x1a"),
        ("'\\%\\_'", b"\\%\\_"),
        ("'hello'", b"hello"),
        ("'\"hello\"'", b"\"hello\""),
        ("'\"\"hello\"\"'", b"\"\"hello\"\""),
        ("'hel''lo'", b"hel'lo"),
        ("'\\'hello'", b"'hello"),
        ("\"hello\"", b"hello"),
        ("\"'hello'\"", b"'hello'"),
        ("\"''hello''\"", b"''hello''"),
        ("\"hel\"\"lo\"", b"hel\"lo"),
        ("\"\\\"hello\"", b"\"hello"),
        ("'disappearing\\ backslash'", b"disappearing backslash"),
        (
            "'한국의中文UTF8およびテキストトラック'",
            "한국의中文UTF8およびテキストトラック".as_bytes(),
        ),
        ("'\\a\x18èàø»\x05'", "a\x18èàø»\x05".as_bytes()),
    ];
    for (input, expected) in cases {
        let token = first(input);
        assert_eq!(token.kind, TokenKind::Str, "input={input:?}");
        assert_eq!(token.offset, 0, "input={input:?}");
        assert_eq!(decoded_string(input), *expected, "input={input:?}");
    }
}

/// Go: `pkg/parser/lexer_test.go::TestScanStringWithNoBackslashEscapesMode`.
///
/// Same `scanString` path with `ModeNoBackslashEscapes`: backslash is an
/// ordinary character, doubled quotes still collapse, and a lone `\'`
/// terminates the string at the quote.
#[test]
fn test_scan_string_with_no_backslash_escapes_mode() {
    let mode = SqlMode {
        no_backslash_escapes: true,
        ..SqlMode::default()
    };
    let cases: &[(&str, &[u8])] = &[
        ("' \\n\\tTest String'", br" \n\tTest String"),
        ("'\\x\\B'", br"\x\B"),
        ("'\\0\\\\''\"\\b\\n\\r\\t\\'", br#"\0\\'"\b\n\r\t\"#),
        ("'\\Z'", br"\Z"),
        ("'\\%\\_'", br"\%\_"),
        ("'hello'", b"hello"),
        ("'\"hello\"'", b"\"hello\""),
        ("'\"\"hello\"\"'", b"\"\"hello\"\""),
        ("'hel''lo'", b"hel'lo"),
        ("'\\'hello'", b"\\"),
        ("\"hello\"", b"hello"),
        ("\"'hello'\"", b"'hello'"),
        ("\"''hello''\"", b"''hello''"),
        ("\"hel\"\"lo\"", b"hel\"lo"),
        ("\"\\\"hello\"", b"\\"),
        (
            "'한국의中文UTF8およびテキストトラック'",
            "한국의中文UTF8およびテキストトラック".as_bytes(),
        ),
    ];
    for (input, expected) in cases {
        let token = first_with_mode(input, mode);
        assert_eq!(token.kind, TokenKind::Str, "input={input:?}");
        assert_eq!(token.offset, 0, "input={input:?}");
        assert_eq!(
            decoded_string_with_mode(input, mode),
            *expected,
            "input={input:?}"
        );
    }
}

/// Go: `pkg/parser/lexer_test.go::TestIdentifier`.
///
/// `Lex` + `isTokenIdentifier`: whitespace-skipped idents, digit-led
/// identifiers, invalid hex/bit/float fall-backs, and NUL-terminated
/// identifier bodies.
#[test]
fn test_identifier() {
    for (input, expected) in [
        ("哈哈", "哈哈"),
        ("`numeric`", "numeric"),
        ("\r\n \r \n \tthere\t \n", "there"),
        ("5number", "5number"),
        ("1_x", "1_x"),
        ("0_x", "0_x"),
        ("�xxx", "�xxx"),
        ("9e", "9e"),
        ("0b", "0b"),
        ("0b123", "0b123"),
        ("0b1ab", "0b1ab"),
        ("0B01", "0B01"),
        ("0x", "0x"),
        ("0x7fz3", "0x7fz3"),
        ("023a4", "023a4"),
        ("9eTSs", "9eTSs"),
        ("t1\0xxx", "t1"),
    ] {
        assert_token(input, TokenKind::Ident, expected);
    }
}

/// Go: `pkg/parser/lexer_test.go::TestSpecialComment`.
///
/// `/*!40101 ... */` drops the version marker and lexes the body as live
/// SQL, preserving the original source offsets.
#[test]
fn test_special_comment() {
    let mut lexer = Lexer::new("/*!40101 select\n5*/");
    let first = lexer.next_token();
    assert_eq!(first.kind, TokenKind::Keyword);
    assert_eq!(first.text, "select");
    assert_eq!((first.offset, first.end_offset), (9, 15));
    let second = lexer.next_token();
    assert_eq!(second.kind, TokenKind::IntLit);
    assert_eq!(second.text, "5");
    assert_eq!((second.offset, second.end_offset), (16, 17));
}

/// Go: `pkg/parser/lexer_test.go::TestFeatureIDsComment`.
///
/// `/*T![auto_rand] ... */` is executable; an unknown feature id is
/// dropped as a plain comment. `scan()` (not `Lex`) is used for the first
/// token so `auto_random` stays an identifier, matching Go.
#[test]
fn test_feature_ids_comment() {
    let mut lexer = Lexer::new("/*T![auto_rand] auto_random(5) */");
    let (kind, start, end) = lexer.scan();
    assert_eq!(kind, TokenKind::Ident);
    assert_eq!(&lexer.r.src()[start..end], "auto_random");
    assert_eq!((start, end), (16, 27));
    let first = lexer.next_token();
    assert_eq!(first.kind, TokenKind::Op);
    assert_eq!(first.text, "(");
    assert_eq!((first.offset, first.end_offset), (27, 28));
    let second = lexer.next_token();
    assert_eq!(second.kind, TokenKind::IntLit);
    assert_eq!(second.text, "5");
    assert_eq!((second.offset, second.end_offset), (28, 29));
    assert_eq!(lexer.next_token().text, ")");

    assert_eq!(
        kinds("/*T![unsupported_feature] unsupported(123) */"),
        Vec::<TokenKind>::new()
    );
}

/// Go: `pkg/parser/lexer_test.go::TestOptimizerHint`.
///
/// A `/*+ ... */` immediately after `SELECT` is retained as `hintComment`
/// with the original source span.
#[test]
fn test_optimizer_hint() {
    let tokens = tokens("SELECT /*+ BKA(t1) */ 0;");
    assert_eq!(
        tokens
            .iter()
            .map(|token| (token.kind, token.text.as_str(), token.offset))
            .collect::<Vec<_>>(),
        vec![
            (TokenKind::Keyword, "SELECT", 0),
            (TokenKind::HintComment, "/*+ BKA(t1) */", 7),
            (TokenKind::IntLit, "0", 22),
            (TokenKind::Op, ";", 23),
        ]
    );
}

/// Go: `pkg/parser/lexer_test.go::TestOptimizerHintAfterCertainKeywordOnly`.
///
/// Hints are recognized only after SELECT/INSERT/REPLACE/UPDATE/DELETE/
/// CREATE (and after a dropped comment still sitting on that keyword).
/// Quoted `SELECT`, a leading hint, and a hint after `*` are dropped.
#[test]
fn test_optimizer_hint_after_certain_keyword_only() {
    let tests = [
        (
            "SELECT /*+ hint */ *",
            vec![TokenKind::Keyword, TokenKind::HintComment, TokenKind::Op],
        ),
        (
            "UPDATE /*+ hint */",
            vec![TokenKind::Keyword, TokenKind::HintComment],
        ),
        (
            "INSERT /*+ hint */",
            vec![TokenKind::Keyword, TokenKind::HintComment],
        ),
        (
            "REPLACE /*+ hint */",
            vec![TokenKind::Keyword, TokenKind::HintComment],
        ),
        (
            "DELETE /*+ hint */",
            vec![TokenKind::Keyword, TokenKind::HintComment],
        ),
        (
            "CREATE /*+ hint */",
            vec![TokenKind::Keyword, TokenKind::HintComment],
        ),
        (
            "/*+ hint */ SELECT *",
            vec![TokenKind::Keyword, TokenKind::Op],
        ),
        (
            "SELECT /* comment */ /*+ hint */ *",
            vec![TokenKind::Keyword, TokenKind::HintComment, TokenKind::Op],
        ),
        (
            "SELECT * /*+ hint */",
            vec![TokenKind::Keyword, TokenKind::Op],
        ),
        (
            "SELECT /*T![auto_rand] * */ /*+ hint */",
            vec![TokenKind::Keyword, TokenKind::Op],
        ),
        (
            "SELECT /*T![unsupported] * */ /*+ hint */",
            vec![TokenKind::Keyword, TokenKind::HintComment],
        ),
        (
            "SELECT /*+ hint1 */ /*+ hint2 */ *",
            vec![TokenKind::Keyword, TokenKind::HintComment, TokenKind::Op],
        ),
        (
            "SELECT * FROM /*+ hint */",
            vec![TokenKind::Keyword, TokenKind::Op, TokenKind::Keyword],
        ),
        ("`SELECT` /*+ hint */", vec![TokenKind::Ident]),
        ("'SELECT' /*+ hint */", vec![TokenKind::Str]),
    ];
    for (input, expected) in tests {
        assert_eq!(kinds(input), expected, "input={input:?}");
    }
}

/// Go: `pkg/parser/lexer_test.go::TestInt`.
///
/// `Lex` + `toInt`: leading-zero integer spellings convert as `uint64`.
#[test]
fn test_int() {
    for (input, expected) in [
        ("01000001783", 1_000_001_783_u64),
        ("00001783", 1_783),
        ("0", 0),
        ("0000", 0),
        ("01", 1),
        ("10", 10),
    ] {
        let token = first(input);
        assert_eq!(token.kind, TokenKind::IntLit, "input={input:?}");
        match Lexer::new(input).next_literal() {
            LiteralValue::Int(value) => {
                assert_eq!(value as u64, expected, "input={input:?}")
            }
            LiteralValue::UInt(value) => assert_eq!(value, expected, "input={input:?}"),
            other => panic!("expected integer for {input:?}, got {other:?}"),
        }
    }
}

/// Go: `pkg/parser/lexer_test.go::TestSQLModeANSIQuotes`.
///
/// Double-quoted values become identifiers (delimiters stripped, doubled
/// quotes collapsed). Single-quoted values stay strings; `Lex` / `next_literal`
/// unescapes doubled quotes to a single quote.
#[test]
fn test_sql_mode_ansi_quotes() {
    let mode = SqlMode {
        ansi_quotes: true,
        ..SqlMode::default()
    };
    for (input, kind, text) in [
        (r#""identifier""#, TokenKind::Ident, "identifier"),
        ("`identifier`", TokenKind::Ident, "identifier"),
        ("\"identifier\"\"and\"", TokenKind::Ident, "identifier\"and"),
        ("'string''string'", TokenKind::Str, "string'string"),
        (r#""identifier"'and'"#, TokenKind::Ident, "identifier"),
        (r#"'string'"identifier"#, TokenKind::Str, "string"),
    ] {
        let token = first_with_mode(input, mode);
        assert_eq!(token.kind, kind, "input={input:?}");
        if kind == TokenKind::Ident {
            assert_eq!(token.text, text, "input={input:?}");
        } else {
            assert_eq!(
                decoded_string_with_mode(input, mode),
                text.as_bytes(),
                "input={input:?}"
            );
        }
    }
    let mut lexer = Lexer::new("'string' 'string'").with_sql_mode(mode);
    assert_eq!(
        lexer.next_literal(),
        LiteralValue::String(b"string".to_vec())
    );
    assert_eq!(
        lexer.next_literal(),
        LiteralValue::String(b"string".to_vec())
    );
}

/// Go: `pkg/parser/lexer_test.go::TestIllegal`.
///
/// Unterminated quotes, NUL, and truncated user/system variables are
/// `invalid` tokens. The Go table's last row is `@@global.\``; `@@session.'`
/// is the same truncated-quoted-body path.
#[test]
fn test_illegal() {
    for input in [
        "'",
        "'fu",
        "'\\n",
        "'\\",
        "\0",
        "`",
        "\"",
        "@`",
        "@'",
        "@\"",
        "@@`",
        "@@global.`",
    ] {
        assert_eq!(first(input).kind, TokenKind::Invalid, "input={input:?}");
    }
}

/// Go: `pkg/parser/lexer_test.go::TestVersionDigits`.
///
/// `scanVersionDigits(min, max)` consumes up to `max` digits and rewinds
/// when fewer than `min` are present. The next `readByte` is the first
/// unconsumed byte (0 at EOF).
#[test]
fn test_version_digits() {
    for (input, min, max, next) in [
        ("12345", 5, 5, 0),
        ("12345xyz", 5, 5, b'x'),
        ("1234xyz", 5, 5, b'1'),
        ("123456", 5, 5, b'6'),
        ("1234", 5, 5, b'1'),
        ("", 5, 5, 0),
        ("1234567xyz", 5, 6, b'7'),
        ("12345xyz", 5, 6, b'x'),
        ("12345", 5, 6, 0),
        ("1234xyz", 5, 6, b'1'),
    ] {
        let mut lexer = Lexer::new("");
        lexer.r = Reader::new(input);
        let _ = lexer.scan_version_digits(min, max);
        assert_eq!(lexer.r.read_byte(), next, "input={input:?}");
    }
}

/// Go: `pkg/parser/lexer_test.go::TestFeatureIDs`.
///
/// `scanFeatureIDs` parses `[id,id,...]` or rewinds to `[` on a malformed
/// list (whitespace, missing close, empty, trailing comma).
#[test]
fn test_feature_ids() {
    for (input, expected, next) in [
        ("[feature]", Some(vec!["feature"]), 0),
        ("[feature] xx", Some(vec!["feature"]), b' '),
        ("[feature1,feature2]", Some(vec!["feature1", "feature2"]), 0),
        (
            "[feature1,feature2,feature3]",
            Some(vec!["feature1", "feature2", "feature3"]),
            0,
        ),
        ("[id_en_ti_fier]", Some(vec!["id_en_ti_fier"]), 0),
        ("[invalid,    whitespace]", None, b'['),
        ("[unclosed_brac", None, b'['),
        ("unclosed_brac]", None, b'u'),
        ("[invalid_comma,]", None, b'['),
        ("[,]", None, b'['),
        ("[]", None, b'['),
    ] {
        let mut lexer = Lexer::new("");
        lexer.r = Reader::new(input);
        let actual = lexer.scan_feature_ids();
        let actual_refs = actual
            .as_ref()
            .map(|ids| ids.iter().map(String::as_str).collect::<Vec<_>>());
        assert_eq!(
            actual_refs.as_deref(),
            expected.as_deref(),
            "input={input:?}"
        );
        assert_eq!(lexer.r.read_byte(), next, "input={input:?}");
    }
}

// ---------------------------------------------------------------------------
// pkg/parser/main_test.go
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// pkg/parser/mysql/*
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// pkg/parser/opcode/opcode_test.go
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// pkg/parser/parser_test.go — lexer-owned slices
// ---------------------------------------------------------------------------

/// Go: `pkg/parser/parser_test.go::TestSimple` (reserved / unreserved lists).
///
/// The Go test's parse-success assertions need yacc. The lexical
/// preconditions it depends on — every reserved spelling is a scanner
/// token (`tokenMap` / `windowFuncTokenMap`), a following `.` keeps the
/// word an identifier (`isTokenIdentifier`), and the unreserved list is
/// not classified reserved — are owned here. CamelCase Go names map to
/// the underscored tokenMap spellings (`CUME_DIST`, `FAILED_LOGIN_ATTEMPTS`).
#[test]
fn test_simple() {
    let reserved_kws = [
        "add",
        "all",
        "alter",
        "analyze",
        "and",
        "as",
        "asc",
        "between",
        "bigint",
        "binary",
        "blob",
        "both",
        "by",
        "call",
        "cascade",
        "case",
        "change",
        "character",
        "check",
        "collate",
        "column",
        "constraint",
        "convert",
        "create",
        "cross",
        "current_date",
        "current_time",
        "current_timestamp",
        "current_user",
        "database",
        "databases",
        "day_hour",
        "day_microsecond",
        "day_minute",
        "day_second",
        "decimal",
        "default",
        "delete",
        "desc",
        "describe",
        "distinct",
        "distinctRow",
        "div",
        "double",
        "drop",
        "dual",
        "else",
        "enclosed",
        "escaped",
        "exists",
        "explain",
        "false",
        "float",
        "fetch",
        "for",
        "force",
        "foreign",
        "from",
        "fulltext",
        "grant",
        "group",
        "having",
        "hour_microsecond",
        "hour_minute",
        "hour_second",
        "if",
        "ignore",
        "in",
        "index",
        "infile",
        "inner",
        "insert",
        "int",
        "into",
        "integer",
        "interval",
        "is",
        "join",
        "key",
        "keys",
        "kill",
        "leading",
        "left",
        "like",
        "ilike",
        "limit",
        "lines",
        "load",
        "localtime",
        "localtimestamp",
        "lock",
        "longblob",
        "longtext",
        "mediumblob",
        "maxvalue",
        "mediumint",
        "mediumtext",
        "minute_microsecond",
        "minute_second",
        "mod",
        "not",
        "no_write_to_binlog",
        "null",
        "numeric",
        "on",
        "option",
        "optionally",
        "or",
        "order",
        "outer",
        "partition",
        "precision",
        "primary",
        "procedure",
        "range",
        "read",
        "real",
        "recursive",
        "references",
        "regexp",
        "rename",
        "repeat",
        "replace",
        "revoke",
        "restrict",
        "right",
        "rlike",
        "schema",
        "schemas",
        "second_microsecond",
        "select",
        "set",
        "show",
        "smallint",
        "starting",
        "table",
        "terminated",
        "then",
        "tinyblob",
        "tinyint",
        "tinytext",
        "to",
        "trailing",
        "true",
        "union",
        "unique",
        "unlock",
        "unsigned",
        "update",
        "use",
        "using",
        "utc_date",
        "values",
        "varbinary",
        "varchar",
        "when",
        "where",
        "write",
        "xor",
        "year_month",
        "zerofill",
        "generated",
        "virtual",
        "stored",
        "usage",
        "delayed",
        "high_priority",
        "low_priority",
        "CUME_DIST",
        "DENSE_RANK",
        "FIRST_VALUE",
        "lag",
        "LAST_VALUE",
        "lead",
        "NTH_VALUE",
        "ntile",
        "over",
        "PERCENT_RANK",
        "rank",
        "row",
        "rows",
        "ROW_NUMBER",
        "window",
        "linear",
        "match",
        "until",
        "placement",
        "tablesample",
        "FAILED_LOGIN_ATTEMPTS",
        "PASSWORD_LOCK_TIME",
    ];
    for kw in reserved_kws {
        let upper = kw.to_ascii_uppercase();
        assert!(
            is_scanner_keyword(&upper),
            "Go TestSimple requires {kw:?} to be a scanner token"
        );
        let qualified = format!("db.{kw}");
        let scanned = tokens(&qualified);
        assert!(
            scanned.len() >= 3,
            "expected ident . ident for {qualified:?}, got {scanned:?}"
        );
        assert_eq!(scanned[0].kind, TokenKind::Ident, "input={qualified:?}");
        assert_eq!(scanned[1].text, ".", "input={qualified:?}");
        assert_eq!(
            scanned[2].kind,
            TokenKind::Ident,
            "isTokenIdentifier must keep {kw:?} as ident after '.'"
        );
    }

    let unreserved_kws = [
        "add_columnar_replica_on_demand",
        "auto_increment",
        "after",
        "begin",
        "bit",
        "bool",
        "boolean",
        "charset",
        "columns",
        "commit",
        "date",
        "datediff",
        "datetime",
        "deallocate",
        "do",
        "from_days",
        "end",
        "engine",
        "engines",
        "execute",
        "extended",
        "first",
        "file",
        "full",
        "local",
        "names",
        "offset",
        "password",
        "prepare",
        "quick",
        "rollback",
        "savepoint",
        "session",
        "signed",
        "start",
        "global",
        "operate",
        "tables",
        "tablespace",
        "target",
        "text",
        "time",
        "timestamp",
        "tidb",
        "transaction",
        "truncate",
        "unknown",
        "value",
        "warnings",
        "year",
        "now",
        "substr",
        "subpartition",
        "subpartitions",
        "substring",
        "mode",
        "any",
        "some",
        "user",
        "identified",
        "collation",
        "comment",
        "avg_row_length",
        "checksum",
        "compression",
        "connection",
        "key_block_size",
        "max_rows",
        "min_rows",
        "national",
        "quarter",
        "escape",
        "grants",
        "status",
        "fields",
        "triggers",
        "language",
        "delay_key_write",
        "isolation",
        "partitions",
        "repeatable",
        "committed",
        "uncommitted",
        "only",
        "serializable",
        "level",
        "curtime",
        "variables",
        "dayname",
        "version",
        "btree",
        "hash",
        "row_format",
        "dynamic",
        "fixed",
        "compressed",
        "compact",
        "redundant",
        "sql_no_cache",
        "sql_cache",
        "action",
        "round",
        "enable",
        "disable",
        "reverse",
        "space",
        "privileges",
        "get_lock",
        "release_lock",
        "sleep",
        "no",
        "greatest",
        "least",
        "binlog",
        "hex",
        "unhex",
        "function",
        "indexes",
        "from_unixtime",
        "processlist",
        "events",
        "less",
        "than",
        "timediff",
        "ln",
        "log",
        "log2",
        "log10",
        "timestampdiff",
        "pi",
        "proxy",
        "quote",
        "none",
        "super",
        "shared",
        "exclusive",
        "always",
        "stats",
        "stats_meta",
        "stats_histogram",
        "stats_buckets",
        "stats_healthy",
        "tidb_version",
        "replication",
        "slave",
        "client",
        "max_connections_per_hour",
        "max_queries_per_hour",
        "max_updates_per_hour",
        "max_user_connections",
        "event",
        "reload",
        "routine",
        "temporary",
        "following",
        "preceding",
        "unbounded",
        "respect",
        "nulls",
        "current",
        "last",
        "against",
        "expansion",
        "chain",
        "error",
        "general",
        "nvarchar",
        "pack_keys",
        "p",
        "shard_row_id_bits",
        "pre_split_regions",
        "constraints",
        "role",
        "replicas",
        "policy",
        "s3",
        "strict",
        "running",
        "stop",
        "preserve",
        "placement",
        "attributes",
        "attribute",
        "resource",
        "burstable",
        "calibrate",
        "masking",
        "rollup",
    ];
    for kw in unreserved_kws {
        assert!(
            !is_reserved(kw),
            "Go TestSimple uses {kw:?} in identifier position; it must not be reserved"
        );
    }
}

/// Go: `pkg/parser/parser_test.go::TestSpecialComments`.
///
/// Statement restore is parser-owned. The lexer-visible pieces are:
/// executable comments still honor `NO_BACKSLASH_ESCAPES`, and a `/*+`
/// after SELECT is retained even when the hint body is non-ASCII.
#[test]
fn test_special_comments() {
    let sql = r#"SELECT /*! '\' */;"#;
    let default_kinds = kinds(sql);
    assert_eq!(default_kinds.first(), Some(&TokenKind::Keyword));
    assert!(
        default_kinds.iter().any(|kind| *kind == TokenKind::Invalid),
        "default SQL mode must leave `'\\'` inside /*! */ unterminated, got {default_kinds:?}"
    );

    let mode = SqlMode {
        no_backslash_escapes: true,
        ..SqlMode::default()
    };
    let escaped = Lexer::new(sql)
        .with_sql_mode(mode)
        .tokenize()
        .into_iter()
        .filter(|token| token.kind != TokenKind::Eof)
        .collect::<Vec<_>>();
    assert_eq!(escaped[0].kind, TokenKind::Keyword);
    assert_eq!(escaped[1].kind, TokenKind::Str);
    assert!(
        !escaped.iter().any(|token| token.kind == TokenKind::Invalid),
        "NO_BACKSLASH_ESCAPES must close `'\\'` inside /*! */, got {escaped:?}"
    );

    let hint = tokens("SELECT /*+ 😅 */ SLEEP(1);");
    assert_eq!(hint[0].kind, TokenKind::Keyword);
    assert_eq!(hint[1].kind, TokenKind::HintComment);
}

/// Go: `pkg/parser/parser_test.go::TestBuiltinFuncAsIdentifier`.
///
/// Pins `isTokenIdentifier`'s `btFuncTokenMap` adjacency rule: a builtin
/// function name is a keyword only when `(` follows (skipping spaces under
/// `IGNORE_SPACE`). Names that also live in `tokenMap` stay keywords even
/// with a space; names in neither map stay identifiers even with `(`.
#[test]
fn test_builtin_func_as_identifier() {
    let whitespace_funcs = [
        "BIT_AND",
        "BIT_OR",
        "BIT_XOR",
        "CAST",
        "COUNT",
        "CURDATE",
        "CURTIME",
        "DATE_ADD",
        "DATE_SUB",
        "EXTRACT",
        "GROUP_CONCAT",
        "MAX",
        "MID",
        "MIN",
        "NOW",
        "POSITION",
        "STDDEV_POP",
        "STDDEV_SAMP",
        "SUBSTR",
        "SUBSTRING",
        "SUM",
        "SYSDATE",
        "TRIM",
        "VAR_POP",
        "VAR_SAMP",
    ];
    let ignore_space = SqlMode {
        ignore_space: true,
        ..SqlMode::default()
    };
    for name in whitespace_funcs {
        assert!(
            is_builtin_keyword(name),
            "{name} must be in btFuncTokenMap / BUILTIN_FUNC_KEYWORDS"
        );
        assert_eq!(
            first(&format!("{name}(")).kind,
            TokenKind::Keyword,
            "{name}( must be the builtin keyword"
        );
        let spaced = first(&format!("{name} ("));
        if is_general_keyword(name) {
            assert_eq!(
                spaced.kind,
                TokenKind::Keyword,
                "{name} is also in tokenMap so a following space still yields a keyword"
            );
        } else {
            assert_eq!(
                spaced.kind,
                TokenKind::Ident,
                "{name} is builtin-only so `name (` is an identifier without IGNORE_SPACE"
            );
        }
        assert_eq!(
            first_with_mode(&format!("{name} ("), ignore_space).kind,
            TokenKind::Keyword,
            "IGNORE_SPACE must still treat {name} ( as the builtin keyword"
        );
    }

    for name in ["ADDDATE", "SUBDATE"] {
        assert!(is_general_keyword(name), "{name} is in tokenMap");
        assert!(!is_builtin_keyword(name), "{name} is not in btFuncTokenMap");
        assert_eq!(first(&format!("{name}(")).kind, TokenKind::Keyword);
        assert_eq!(first(&format!("{name} (")).kind, TokenKind::Keyword);
    }
    for name in ["SESSION_USER", "SYSTEM_USER"] {
        assert!(
            !is_scanner_keyword(name),
            "{name} is not in tokenMap/btFuncTokenMap"
        );
        assert_eq!(first(&format!("{name}(")).kind, TokenKind::Ident);
        assert_eq!(first(&format!("{name} (")).kind, TokenKind::Ident);
        assert_eq!(
            first_with_mode(&format!("{name} ("), ignore_space).kind,
            TokenKind::Ident
        );
    }
}

/// Go: `pkg/parser/parser_test.go::TestHintError`.
///
/// Hint-body acceptance is the hint parser. The scanner-owned pieces are
/// the `hintedTokens` position check, the `FOR UPDATE /*+` warning, and
/// the `BINDING FOR SELECT /*+` exception (`startWithSlash` in lexer.go).
#[test]
fn test_hint_error() {
    let (_, warnings) =
        Lexer::new("select c1, c2 from /*+ tidb_unknow(T1,t2) */ t1, t2 where t1.c1 = t2.c1")
            .tokenize_with_warnings();
    assert!(
        warnings.iter().any(|warning| warning.contains("8066")),
        "hint after FROM must warn 8066, got {warnings:?}"
    );
    assert!(
        !kinds("select c1, c2 from /*+ tidb_unknow(T1,t2) */ t1")
            .iter()
            .any(|kind| *kind == TokenKind::HintComment),
        "hint after FROM must be dropped"
    );

    let (tokens, warnings) = Lexer::new("SELECT id FROM tbl WHERE id = 0 FOR UPDATE /*+ xyz */")
        .tokenize_with_warnings();
    assert!(
        warnings
            .iter()
            .any(|warning| warning.contains("near '/*+'")),
        "FOR UPDATE /*+ must warn near /*+, got {warnings:?}"
    );
    assert!(
        !tokens
            .iter()
            .any(|token| token.kind == TokenKind::HintComment),
        "FOR UPDATE /*+ must not retain a hint token"
    );

    let (tokens, warnings) = Lexer::new(
        "create global binding for select /*+ max_execution_time(1) */ 1 using select /*+ max_execution_time(1) */ 1;",
    )
    .tokenize_with_warnings();
    assert!(
        !warnings
            .iter()
            .any(|warning| warning.contains("near '/*+'")),
        "BINDING FOR SELECT /*+ must not warn near /*+, got {warnings:?}"
    );
    assert!(
        tokens
            .iter()
            .any(|token| token.kind == TokenKind::HintComment),
        "BINDING FOR SELECT /*+ must retain the hint comment"
    );
}

/// Go: `pkg/parser/parser_test.go::TestParserErrMsg`.
///
/// Exact `near '/*'` wrapping is parser-owned. The scanner still reports
/// an unterminated `/*` as `invalid`, and a closed comment is dropped.
#[test]
fn test_parser_err_msg() {
    assert!(
        kinds("select 1/*")
            .iter()
            .any(|kind| *kind == TokenKind::Invalid),
        "unterminated block comment must be invalid"
    );
    assert_eq!(
        kinds("select 1/* comment */"),
        vec![TokenKind::Keyword, TokenKind::IntLit]
    );
    assert!(
        kinds("delete from t where a = 7 or 1=1/*' and b = 'p'")
            .iter()
            .any(|kind| *kind == TokenKind::Invalid),
        "unclosed comment in DELETE must be invalid"
    );
}

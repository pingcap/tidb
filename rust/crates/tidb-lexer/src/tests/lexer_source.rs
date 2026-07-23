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

//! Source-owned port of `pkg/parser/lexer_test.go`.
//!
//! The Go suite exposes both scanner internals and the public `Lex` contract.
//! This module keeps the same test boundaries where the Rust scanner has an
//! equivalent (including its byte offsets and SQL-mode switches), and records
//! the value-conversion-only portions as PARTIAL in the coverage evidence.

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

fn assert_token(sql: &str, kind: TokenKind, text: &str) {
    let token = first(sql);
    assert_eq!(token.kind, kind, "input={sql:?}");
    assert_eq!(token.text, text, "input={sql:?}");
}

#[test]
fn test_token_id() {
    // The Go test iterates every generated tokenMap entry.  Rust carries the
    // same generated sets as keyword tables rather than integer token IDs.
    for word in crate::keywords::GENERAL_KEYWORDS
        .iter()
        .chain(crate::keywords::WINDOW_FUNC_KEYWORDS.iter())
    {
        let token = first(word);
        assert_eq!(token.kind, TokenKind::Keyword, "word={word}");
        assert_eq!(token.text, *word, "word={word}");
    }
    for word in crate::keywords::BUILTIN_FUNC_KEYWORDS {
        let input = format!("{word}(");
        let token = first(&input);
        assert_eq!(token.kind, TokenKind::Keyword, "word={word}");
        assert_eq!(token.text, *word, "word={word}");
    }
}

#[test]
fn test_single_char() {
    for ch in ['|', '&', '-', '+', '*', '/', '%', '^', '~', '(', ',', ')'] {
        let text = ch.to_string();
        assert_token(&text, TokenKind::Op, &text);
    }
}

#[test]
fn test_single_char_other() {
    for (input, kind, text) in [
        ("AT", TokenKind::Ident, "AT"),
        ("?", TokenKind::Op, "?"),
        ("PLACEHOLDER", TokenKind::Ident, "PLACEHOLDER"),
        ("=", TokenKind::Op, "="),
        (".", TokenKind::Op, "."),
    ] {
        assert_token(input, kind, text);
    }
}

#[test]
fn test_at_leading_identifier() {
    for input in [
        "@",
        "@''",
        "@1",
        "@.1_",
        "@-1.",
        "@~",
        "@$",
        "@a_3cbbc",
        "@`a_3cbbc`",
        "@-3cbbc",
        "@!3cbbc",
        "@@global.test",
        "@@session.test",
        "@@local.test",
        "@@test",
        "@@global.`test`",
        "@@session.`test`",
        "@@local.`test`",
        "@@`test`",
    ] {
        assert_eq!(first(input).kind, TokenKind::UserVar, "input={input:?}");
    }
    assert_token("@@global.test", TokenKind::UserVar, "@@global.test");
    assert_token("@@global.`test`", TokenKind::UserVar, "@@global.`test`");
}

#[test]
fn test_underscore_cs() {
    let mut lexer = Lexer::new("_utf8\"string\"");
    assert_eq!(lexer.next_token().kind, TokenKind::CharsetIntroducer);
    assert_eq!(lexer.next_token().kind, TokenKind::Str);

    let mut lexer = Lexer::new("N'string'");
    assert_eq!(lexer.next_token().kind, TokenKind::CharsetIntroducer);
    assert_eq!(lexer.next_token().kind, TokenKind::Str);
}

#[test]
fn legacy_charset_introducer_registry_matches_go() {
    for (input, expected) in [
        ("utf8", Some("utf8")),
        ("utf8mb3", Some("utf8")),
        ("utf8mb4", Some("utf8mb4")),
        ("ascii", Some("ascii")),
        ("latin1", Some("latin1")),
        ("binary", Some("binary")),
        ("gbk", None),
        ("ujis", None),
        ("gb18030", None),
    ] {
        assert_eq!(canonical_legacy_charset(input), expected, "input={input}");
    }
}

#[test]
fn test_literal() {
    let cases = [
        ("'''a'''", TokenKind::Str),
        ("''a''", TokenKind::Str),
        ("\"\"a\"\"", TokenKind::Str),
        (r#"\'a\'"#, TokenKind::Op),
        (r#"\"a\""#, TokenKind::Op),
        ("0.2314", TokenKind::DecLit),
        (
            "1234567890123456789012345678901234567890",
            TokenKind::DecLit,
        ),
        ("132.313", TokenKind::DecLit),
        ("132.3e231", TokenKind::FloatLit),
        ("132.3e-231", TokenKind::FloatLit),
        ("001e-12", TokenKind::FloatLit),
        ("23416", TokenKind::IntLit),
        ("123test", TokenKind::Ident),
        ("123�xxx", TokenKind::Ident),
        ("0", TokenKind::IntLit),
        ("0x3c26", TokenKind::HexLit),
        ("x'13181C76734725455A'", TokenKind::HexLit),
        ("0b01", TokenKind::BitLit),
        ("t1\0", TokenKind::Ident),
        ("N'some text'", TokenKind::CharsetIntroducer),
        ("n'some text'", TokenKind::CharsetIntroducer),
        (r#"\N"#, TokenKind::Keyword),
        (".*", TokenKind::Op),
        (".1_t_1_x", TokenKind::DecLit),
        ("9e9e", TokenKind::FloatLit),
        (".1e", TokenKind::Invalid),
        (".1e23", TokenKind::FloatLit),
        (".123", TokenKind::DecLit),
        (".1*23", TokenKind::DecLit),
        (".1,23", TokenKind::DecLit),
        (".1 23", TokenKind::DecLit),
        (".1$23", TokenKind::DecLit),
        (".1a23", TokenKind::DecLit),
        (".1e23$23", TokenKind::FloatLit),
        (".1e23a23", TokenKind::FloatLit),
        (".1C23", TokenKind::DecLit),
        (".1\u{81}", TokenKind::DecLit),
        (".1Ｔ", TokenKind::DecLit),
        ("b''", TokenKind::BitLit),
        ("b'0101'", TokenKind::BitLit),
        ("0b0101", TokenKind::BitLit),
    ];
    for (input, expected) in cases {
        assert_eq!(first(input).kind, expected, "input={input:?}");
    }
}

#[test]
fn test_literal_value() {
    use LiteralValue::{Bytes, Decimal, Float, Int, String as Text};

    let cases = [
        ("'''a'''", Text(b"'a'".to_vec())),
        ("''a''", Text(Vec::new())),
        ("\"\"a\"\"", Text(Vec::new())),
        (r#"\'a\'"#, Text(b"\\".to_vec())),
        (r#"\"a\""#, Text(b"\\".to_vec())),
        ("0.2314", Decimal("0.2314".to_string())),
        (
            "1234567890123456789012345678901234567890",
            Decimal("1234567890123456789012345678901234567890".to_string()),
        ),
        ("132.313", Decimal("132.313".to_string())),
        ("132.3e231", Float(1.323e233)),
        ("132.3e-231", Float(1.323e-229)),
        ("001e-12", Float(1e-12)),
        ("23416", Int(23_416)),
        ("123test", Text(b"123test".to_vec())),
        ("123�xxx", Text("123�xxx".as_bytes().to_vec())),
        ("0", Int(0)),
        ("0x3c26", Bytes(vec![60, 38])),
        (
            "x'13181C76734725455A'",
            Bytes(vec![19, 24, 28, 118, 115, 71, 37, 69, 90]),
        ),
        ("0b01", Bytes(vec![1])),
        ("t1\0", Text(b"t1".to_vec())),
        ("N'some text'", Text(b"utf8".to_vec())),
        ("n'some text'", Text(b"utf8".to_vec())),
        (r#"\N"#, Text(br"\N".to_vec())),
        (".*", Text(b".".to_vec())),
        (".1_t_1_x", Decimal("0.1".to_string())),
        ("9e9e", Float(9_000_000_000.0)),
        (".1e", Text(Vec::new())),
        (".1e23", Float(1e22)),
        (".123", Decimal("0.123".to_string())),
        (".1*23", Decimal("0.1".to_string())),
        (".1,23", Decimal("0.1".to_string())),
        (".1 23", Decimal("0.1".to_string())),
        (".1$23", Decimal("0.1".to_string())),
        (".1a23", Decimal("0.1".to_string())),
        (".1e23$23", Float(1e22)),
        (".1e23a23", Float(1e22)),
        (".1C23", Decimal("0.1".to_string())),
        (".1\u{81}", Decimal("0.1".to_string())),
        (".1Ｔ", Decimal("0.1".to_string())),
        ("b''", Bytes(Vec::new())),
        ("b'0101'", Bytes(vec![5])),
        ("0b0101", Bytes(vec![5])),
    ];
    for (input, expected) in cases {
        assert_eq!(
            Lexer::new(input).next_literal(),
            expected,
            "input={input:?}"
        );
    }
}

#[test]
fn test_comment() {
    for (input, expected) in [
        ("-- select --\n1", vec![TokenKind::IntLit]),
        (
            "/*!40101 SET character_set_client = utf8 */;",
            vec![
                TokenKind::Keyword,
                TokenKind::Ident,
                TokenKind::Op,
                TokenKind::Ident,
                TokenKind::Op,
            ],
        ),
        (
            "/* SET character_set_client = utf8 */;",
            vec![TokenKind::Op],
        ),
        ("/* some comments */ SELECT ", vec![TokenKind::Keyword]),
        (
            "-- comment continues to the end of line\nSELECT",
            vec![TokenKind::Keyword],
        ),
        (
            "# comment continues to the end of line\nSELECT",
            vec![TokenKind::Keyword],
        ),
        ("#comment\n123", vec![TokenKind::IntLit]),
        ("--5", vec![TokenKind::Op, TokenKind::Op, TokenKind::IntLit]),
        ("--\nSELECT", vec![TokenKind::Keyword]),
        ("--\tSELECT", vec![]),
        ("--\r\nSELECT", vec![TokenKind::Keyword]),
        ("--", vec![]),
        ("/*T![unsupported] '*/0 -- ' */", vec![TokenKind::IntLit]),
        ("/*T![auto_rand] '*/0 -- ' */", vec![TokenKind::Str]),
    ] {
        assert_eq!(kinds(input), expected, "input={input:?}");
    }
}

#[test]
fn test_scan_quoted_ident() {
    assert_token("`fk`", TokenKind::Ident, "fk");
    assert_token("`a``b`", TokenKind::Ident, "a`b");
    assert_eq!(first("`fk`").offset, 0);
    assert_eq!(first("`fk`").end_offset, 4);
}

#[test]
fn test_scan_string() {
    let cases = [
        ("' \\n\\tTest String'", "' \\n\\tTest String'"),
        ("'\\x\\B'", "'\\x\\B'"),
        ("'\\Z'", "'\\Z'"),
        ("'\\%\\_'", "'\\%\\_'"),
        ("'hello'", "'hello'"),
        ("'\"hello\"'", "'\"hello\"'"),
        ("'hel''lo'", "'hel''lo'"),
        ("'\\'hello'", "'\\'hello'"),
        ("\"hello\"", "\"hello\""),
        ("\"'hello'\"", "\"'hello'\""),
        ("\"hel\"\"lo\"", "\"hel\"\"lo\""),
    ];
    for (input, raw) in cases {
        assert_token(input, TokenKind::Str, raw);
    }
}

#[test]
fn test_scan_string_with_no_backslash_escapes_mode() {
    let mode = SqlMode {
        no_backslash_escapes: true,
        ..SqlMode::default()
    };
    for input in [
        "' \\n\\tTest String'",
        "'\\x\\B'",
        "'\\Z'",
        "'\\%\\_'",
        "'hello'",
        "'hel''lo'",
        "\"hello\"",
    ] {
        assert_eq!(
            first_with_mode(input, mode).kind,
            TokenKind::Str,
            "input={input:?}"
        );
    }
}

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
    assert_eq!(
        kinds("/*T! SHARD_ROW_ID_BITS = 4 PRE_SPLIT_REGIONS = 2 */"),
        vec![
            TokenKind::Keyword,
            TokenKind::Op,
            TokenKind::IntLit,
            TokenKind::Keyword,
            TokenKind::Op,
            TokenKind::IntLit,
        ]
    );
}

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
        assert_eq!(
            token.text.parse::<u64>().unwrap(),
            expected,
            "input={input:?}"
        );
    }
}

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
        ("'string''string'", TokenKind::Str, "'string''string'"),
        (r#""identifier"'and'"#, TokenKind::Ident, "identifier"),
        (r#"'string'"identifier"#, TokenKind::Str, "'string'"),
    ] {
        assert_token_mode(input, mode, kind, text);
    }
    let mut lexer = Lexer::new("'string' 'string'").with_sql_mode(mode);
    assert_eq!(lexer.next_token().kind, TokenKind::Str);
    assert_eq!(lexer.next_token().kind, TokenKind::Str);
}

fn assert_token_mode(sql: &str, mode: SqlMode, kind: TokenKind, text: &str) {
    let token = first_with_mode(sql, mode);
    assert_eq!(token.kind, kind, "input={sql:?}");
    assert_eq!(token.text, text, "input={sql:?}");
}

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
        "@@session.'",
    ] {
        assert_eq!(first(input).kind, TokenKind::Invalid, "input={input:?}");
    }
}

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

#[test]
fn test_long_tokens_do_not_panic_or_truncate() {
    let ident = "a".repeat(100_000);
    let token = first(&ident);
    assert_eq!(token.kind, TokenKind::Ident);
    assert_eq!(token.text.len(), ident.len());
    assert_eq!(token.end_offset, ident.len());

    let string = format!("'{}'", "x".repeat(100_000));
    let token = first(&string);
    assert_eq!(token.kind, TokenKind::Str);
    assert_eq!(token.text.len(), string.len());
    assert_eq!(token.end_offset, string.len());
}

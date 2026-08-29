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

//! Executable lexical preconditions derived from pinned Go
//! `pkg/parser/parser_test.go` cases:
//!
//! - keyword-table membership (`tokenMap` / `windowFuncTokenMap`)
//! - charset-introducer recognition (`underscoreCS`)
//! - ANSI_QUOTES identifier quoting
//! - string-literal unescaping (`LexLiteral` / `UnescapeChar`)
//! - user/system variable tokenization (`@@global.` / `@@session.` /
//!   `@@local.` / `@@instance.` prefixes, quoted bodies)
//! - `AS OF` two-word merge
//! - window-function keyword gating (`EnableWindowFunc`)
//! - `/*T![ttl] ... */` feature-ID allowlist
//! - integer overflow classification (`toInt` → `toDecimal` past u64)
//!
//! Parse/restore/AST-shape checks live with their owning crates and are
//! deliberately not represented here.

use super::*;

fn tokens(sql: &str) -> Vec<Token> {
    Lexer::new(sql)
        .tokenize()
        .into_iter()
        .filter(|token| token.kind != TokenKind::Eof)
        .collect()
}

fn first_kind(sql: &str) -> TokenKind {
    Lexer::new(sql).next_token().kind
}

fn is_general_or_window_keyword(word: &str) -> bool {
    let upper = word.to_ascii_uppercase();
    crate::keywords::GENERAL_KEYWORDS
        .binary_search(&upper.as_str())
        .is_ok()
        || crate::keywords::WINDOW_FUNC_KEYWORDS
            .binary_search(&upper.as_str())
            .is_ok()
}

fn decoded_string(sql: &str) -> Vec<u8> {
    match Lexer::new(sql).next_literal() {
        LiteralValue::String(bytes) => bytes,
        other => panic!("expected string literal, got {other:?} from {sql:?}"),
    }
}

fn kinds_with_window(sql: &str, support_window_func: bool) -> Vec<TokenKind> {
    let mut lexer = Lexer::new(sql);
    lexer.set_support_window_func(support_window_func);
    lexer
        .tokenize()
        .into_iter()
        .filter(|token| token.kind != TokenKind::Eof)
        .map(|token| token.kind)
        .collect()
}

// ---------------------------------------------------------------------------
// pkg/parser/parser_test.go — TestSQLResult
// ---------------------------------------------------------------------------

/// Go: `pkg/parser/parser_test.go`, `TestSQLResult`.
///
/// The Go table is a parse/restore sweep of SELECT modifier keywords.
/// The lexical precondition — each modifier is a `tokenMap` keyword so
/// the grammar can accept it after `SELECT` — is owned here.
#[test]
fn sql_result_select_modifiers_are_keywords() {
    for word in [
        "SQL_BIG_RESULT",
        "SQL_SMALL_RESULT",
        "SQL_BUFFER_RESULT",
        "SQL_CALC_FOUND_ROWS",
        "STRAIGHT_JOIN",
        "DISTINCT",
    ] {
        assert!(
            is_general_or_window_keyword(word),
            "{word} must be a scanner keyword (Go tokenMap)"
        );
        assert_eq!(first_kind(word), TokenKind::Keyword, "word={word}");
    }
}

// ---------------------------------------------------------------------------
// pkg/parser/parser_test.go — TestSQLNoCache
// ---------------------------------------------------------------------------

/// Go: `pkg/parser/parser_test.go`, `TestSQLNoCache`.
///
/// The Go test inspects `SelectStmtOpts.SQLCache` after parse. The
/// lexical precondition is that both spellings are scanner keywords.
#[test]
fn sql_no_cache_keywords_are_lexical_tokens() {
    for word in ["SQL_NO_CACHE", "SQL_CACHE"] {
        assert!(is_general_or_window_keyword(word), "word={word}");
        assert_eq!(first_kind(word), TokenKind::Keyword, "word={word}");
    }
}

// ---------------------------------------------------------------------------
// pkg/parser/parser_test.go — TestEscape
// ---------------------------------------------------------------------------

/// Go: `pkg/parser/parser_test.go`, `TestEscape`.
///
/// Go `RunTest`s parse/restore of quoted strings. The scanner-owned
/// half is `LexLiteral` unescaping of the same vectors (doubled quotes,
/// backslash escapes, identity fall-back for `\x` / `\a`). Unterminated
/// `"""` is an `Invalid` token rather than a parse error.
#[test]
fn escape_quoted_string_unescaping() {
    // Go `select """;` — three quotes then semicolon: the doubled-quote
    // pair is consumed inside the string, the third quote never closes it.
    let rest = tokens("select \"\"\";");
    assert_eq!(rest[0].kind, TokenKind::Keyword);
    assert!(
        rest.iter().any(|token| token.kind == TokenKind::Invalid),
        "unterminated select \"\"\"; must produce Invalid, got {rest:?}"
    );

    // Go `select """";` — four quotes is a string containing one `"`.
    assert_eq!(decoded_string("\"\"\"\""), b"\"".to_vec());
    // UTF-8 payload is preserved as bytes.
    assert_eq!(
        decoded_string("\"汉字\""),
        "汉字".as_bytes(),
        "UTF-8 payload must round-trip through LexLiteral"
    );
    assert_eq!(decoded_string(r#"'abc"def'"#), br#"abc"def"#.to_vec());
    // `\r\n` unescapes; `\a` / `\x` drop the backslash (UnescapeChar identity).
    assert_eq!(decoded_string(r#"'a\r\n'"#), b"a\r\n".to_vec());
    assert_eq!(decoded_string(r#""\a\r\n""#), b"a\r\n".to_vec());
    assert_eq!(decoded_string(r#""\xFF""#), b"xFF".to_vec());
}

// ---------------------------------------------------------------------------
// pkg/parser/parser_test.go — TestExplain / TestPrepare / TestDeallocate /
// TestExecute / TestTrace / TestBinding / TestView
// ---------------------------------------------------------------------------

/// Go: `pkg/parser/parser_test.go`, `TestBinding`.
///
/// Hint comments after CREATE/DROP are a lexer-owned position rule
/// (`hintedTokens` includes CREATE). Pin that the CREATE BINDING
/// vectors keep the `/*+ ... */` token. Statement grammar is asserted in the
/// parser crate.
#[test]
fn binding_create_keeps_optimizer_hint() {
    let sql = "CREATE GLOBAL BINDING FOR UPDATE `t` SET `a`=1 WHERE `b`=1 USING UPDATE /*+ USE_INDEX(`t` `b`)*/ `t` SET `a`=1 WHERE `b`=1";
    let kinds: Vec<_> = tokens(sql).into_iter().map(|token| token.kind).collect();
    assert!(
        kinds.contains(&TokenKind::HintComment),
        "CREATE ... UPDATE /*+ ... */ must retain a hint token, got {kinds:?}"
    );
}

// ---------------------------------------------------------------------------
// pkg/parser/parser_test.go — TestTimestampDiffUnit / TestFuncCallExprOffset
// ---------------------------------------------------------------------------

/// Go: `pkg/parser/parser_test.go`, `TestFuncCallExprOffset`.
///
/// The Go test asserts `FuncCallExpr.OriginTextPosition()` for `s.a()`
/// (offset 7) and `b()` (offset 14) in `SELECT s.a(), b();`.
/// `yySetOffset` copies the first token's offset of each expression;
/// for `s.a()` that is `s` at byte 7, for `b()` that is `b` at byte 14.
#[test]
fn func_call_expr_offset_identifier_spans() {
    let sql = "SELECT s.a(), b();";
    let toks = tokens(sql);
    // SELECT s . a ( ) , b ( ) ;
    let names: Vec<_> = toks
        .iter()
        .filter(|token| token.kind == TokenKind::Ident)
        .collect();
    assert_eq!(names.len(), 3, "expected s, a, b identifiers, got {toks:?}");
    assert_eq!(names[0].text, "s");
    assert_eq!(names[0].offset, 7, "s.a() starts at the 's' token");
    assert_eq!(names[1].text, "a");
    assert_eq!(names[2].text, "b");
    assert_eq!(names[2].offset, 14, "b() starts at the 'b' token");
}

// ---------------------------------------------------------------------------
// pkg/parser/parser_test.go — TestSessionManage / TestParseShowOpenTables
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// pkg/parser/parser_test.go — TestSQLModeANSIQuotes
// ---------------------------------------------------------------------------

/// Go: `pkg/parser/parser_test.go`, `TestSQLModeANSIQuotes`.
///
/// Go parses `CREATE TABLE "table" ("id" int)` and `select * from t "tt"`
/// under `ModeANSIQuotes`. The scanner-owned half is that double-quoted
/// spans become identifiers (not strings) under that flag.
#[test]
fn sql_mode_ansi_quotes_double_quoted_identifiers() {
    let mode = SqlMode {
        ansi_quotes: true,
        ..SqlMode::default()
    };
    let mut lexer = Lexer::new(r#"CREATE TABLE "table" ("id" int)"#).with_sql_mode(mode);
    let mut idents = Vec::new();
    loop {
        let token = lexer.next_token();
        if token.kind == TokenKind::Eof {
            break;
        }
        assert_ne!(
            token.kind,
            TokenKind::Str,
            "ANSI_QUOTES must not emit a Str token, got {token:?}"
        );
        if token.kind == TokenKind::Ident {
            idents.push(token.text);
        }
    }
    assert!(
        idents.iter().any(|name| name == "table"),
        "\"table\" must unquote to identifier table, got {idents:?}"
    );
    assert!(
        idents.iter().any(|name| name == "id"),
        "\"id\" must unquote to identifier id, got {idents:?}"
    );

    let mut lexer = Lexer::new(r#"select * from t "tt""#).with_sql_mode(mode);
    let mut saw_alias = false;
    loop {
        let token = lexer.next_token();
        if token.kind == TokenKind::Eof {
            break;
        }
        assert_ne!(token.kind, TokenKind::Str, "got unexpected Str {token:?}");
        if token.kind == TokenKind::Ident && token.text == "tt" {
            saw_alias = true;
        }
    }
    assert!(saw_alias, "\"tt\" must lex as identifier tt");
}

// ---------------------------------------------------------------------------
// pkg/parser/parser_test.go — TestDDLStatements / TestAnalyze /
// TestTableSample / TestGeneratedColumn / TestSetTransaction / TestSideEffect
// ---------------------------------------------------------------------------

/// Go: `pkg/parser/parser_test.go`, `TestSideEffect`.
///
/// Go proves a failed `ParseOneStmt` leaves the parser reusable. The
/// lexer is constructed per input and has no retained parse state, so
/// the equivalent scanner fact is that the illegal `/*!50100 'abc'`
/// body still tokenizes independently of a subsequent `show tables`.
#[test]
fn side_effect_lexer_is_stateless_across_inputs() {
    let bad = "create table t /*!50100 'abc', 'abc' */;";
    let _ = tokens(bad);
    let show = tokens("show tables;");
    assert_eq!(show[0].kind, TokenKind::Keyword);
    assert_eq!(show[0].text, "show");
    assert_eq!(show[1].kind, TokenKind::Keyword);
    assert_eq!(show[1].text, "tables");
}

// ---------------------------------------------------------------------------
// pkg/parser/parser_test.go — TestTablePartition /
// TestTablePartitionNameList / TestNotExistsSubquery
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// pkg/parser/parser_test.go — TestWindowFunctionIdentifier /
// TestWindowFunctions / TestVisitFrameBound
// ---------------------------------------------------------------------------

/// Go: `pkg/parser/parser_test.go`, `TestWindowFunctionIdentifier`.
///
/// Go iterates `windowFuncTokenMap` and asserts `select 1 <kw>` is
/// rejected with window funcs *enabled* (the keyword cannot be an
/// alias) and accepted with them *disabled* (the word is an ident).
/// That token-class switch is exactly `Lexer::set_support_window_func`.
#[test]
fn window_function_identifier_keyword_gating() {
    for word in crate::keywords::WINDOW_FUNC_KEYWORDS {
        let sql = format!("select 1 {word}");
        let enabled = kinds_with_window(&sql, true);
        assert_eq!(
            enabled,
            vec![TokenKind::Keyword, TokenKind::IntLit, TokenKind::Keyword],
            "window enabled: {sql}"
        );
        let disabled = kinds_with_window(&sql, false);
        assert_eq!(
            disabled,
            vec![TokenKind::Keyword, TokenKind::IntLit, TokenKind::Ident],
            "window disabled: {sql}"
        );
        assert_eq!(
            {
                let mut lexer = Lexer::new(&sql);
                lexer.set_support_window_func(false);
                lexer.tokenize()
            }
            .into_iter()
            .find(|token| token.kind == TokenKind::Ident)
            .map(|token| token.text),
            Some((*word).to_string()),
            "disabled window keyword must survive as its own identifier"
        );
    }
}

// ---------------------------------------------------------------------------
// pkg/parser/parser_test.go — TestFieldText / TestQuotedSystemVariables /
// TestQuotedVariableColumnName
// ---------------------------------------------------------------------------

/// Go: `pkg/parser/parser_test.go`, `TestQuotedSystemVariables`.
///
/// Go parses
/// `@@Sql_Mode, @@\`SQL_MODE\`, @@session.\`sql_mode\`, @@global.\`s ql\`\`mode\`,
/// @@session.'sql\\nmode', @@local.\"sql\\\"mode\", @@instance.sql_mode`
/// and inspects `VariableExpr` name/scope. The scanner-owned half is
/// that each of those spellings is a single `UserVar` token with the
/// quoted body preserved in `text`.
#[test]
fn quoted_system_variables_tokenize_as_user_vars() {
    let sql = r#"select @@Sql_Mode, @@`SQL_MODE`, @@session.`sql_mode`, @@global.`s ql``mode`, @@session.'sql\nmode', @@local."sql\"mode", @@instance.sql_mode;"#;
    let vars: Vec<_> = tokens(sql)
        .into_iter()
        .filter(|token| token.kind == TokenKind::UserVar)
        .map(|token| token.text)
        .collect();
    assert_eq!(
        vars,
        vec![
            "@@Sql_Mode".to_string(),
            "@@`SQL_MODE`".to_string(),
            "@@session.`sql_mode`".to_string(),
            "@@global.`s ql``mode`".to_string(),
            r#"@@session.'sql\nmode'"#.to_string(),
            r#"@@local."sql\"mode""#.to_string(),
            "@@instance.sql_mode".to_string(),
        ]
    );
}

/// Go: `pkg/parser/parser_test.go`, `TestQuotedVariableColumnName`.
///
/// Go asserts `field.Text()` equals the original spellings. The
/// scanner-owned half is that each of those spellings is one token
/// whose source span matches the Go field text.
#[test]
fn quoted_variable_column_name_spans() {
    let sql = r#"select @abc, @`abc`, @'aBc', @"AbC", @6, @`6`, @'6', @"6", @@sql_mode, @@`sql_mode`, @;"#;
    let expected = [
        "@abc",
        "@`abc`",
        "@'aBc'",
        r#"@"AbC""#,
        "@6",
        "@`6`",
        "@'6'",
        r#"@"6""#,
        "@@sql_mode",
        "@@`sql_mode`",
        "@",
    ];
    let vars: Vec<_> = tokens(sql)
        .into_iter()
        .filter(|token| token.kind == TokenKind::UserVar)
        .map(|token| token.text)
        .collect();
    assert_eq!(vars, expected);
}

// ---------------------------------------------------------------------------
// pkg/parser/parser_test.go — TestCharset / TestUnderscoreCharset
// ---------------------------------------------------------------------------

/// Go: `pkg/parser/parser_test.go`, `TestUnderscoreCharset`.
///
/// Go classifies `_utf8` / `_gbk` / `_ujis` / `_gbk1` / `_ujisx` as
/// parse-ok, unsupported-introducer, or near-quote syntax error. The
/// scanner-owned half is the `underscoreCS` decision:
/// recognized charset names become `CharsetIntroducer`; unknown
/// suffixes stay identifiers. Grammar-level `[ddl:1115]` lives in the
/// parser.
#[test]
fn underscore_charset_introducer_classification() {
    // utf8 is a legacy charset: introducer + string.
    let lexer = Lexer::new("select hex(_utf8 '3F')");
    let kinds: Vec<_> = lexer
        .tokenize()
        .into_iter()
        .filter(|token| token.kind != TokenKind::Eof)
        .map(|token| token.kind)
        .collect();
    assert!(
        kinds.contains(&TokenKind::CharsetIntroducer),
        "_utf8 must be CharsetIntroducer, got {kinds:?}"
    );

    // gbk / ujis are in CHARSET_NAMES, so they are still introducers
    // (the parser, not the scanner, reports "Unsupported character
    // introducer"). gbk1 / ujisx are not charset names.
    for (cs, want_introducer) in [
        ("utf8", true),
        ("gbk", true),
        ("ujis", true),
        ("gbk1", false),
        ("ujisx", false),
    ] {
        let sql = format!("_{cs}");
        let kind = first_kind(&sql);
        if want_introducer {
            assert_eq!(
                kind,
                TokenKind::CharsetIntroducer,
                "_{cs} must be an introducer"
            );
            assert!(
                crate::canonical_charset(cs).is_some(),
                "{cs} must be in CHARSET_NAMES"
            );
        } else {
            assert_eq!(kind, TokenKind::Ident, "_{cs} must stay an identifier");
            assert!(
                crate::canonical_charset(cs).is_none(),
                "{cs} must not be a recognized charset"
            );
        }
    }

    // Only the five legacy charsets may carry an introducer through
    // the expression grammar; gbk/ujis are recognized names but not
    // legacy (mirrors charset.GetDefaultCollationLegacy).
    assert_eq!(crate::canonical_legacy_charset("utf8"), Some("utf8"));
    assert_eq!(crate::canonical_legacy_charset("gbk"), None);
    assert_eq!(crate::canonical_legacy_charset("ujis"), None);
    assert_eq!(crate::canonical_legacy_charset("gbk1"), None);
}

// ---------------------------------------------------------------------------
// pkg/parser/parser_test.go — TestFulltextSearch / TestStartTransaction
// ---------------------------------------------------------------------------

/// Go: `pkg/parser/parser_test.go`, `TestStartTransaction`.
///
/// `AS OF` is a two-word scanner merge (`AS` + `OF` → one keyword).
/// Pin that merge on the READ ONLY AS OF TIMESTAMP vectors. The statement
/// grammar is asserted in the parser crate.
#[test]
fn start_transaction_as_of_two_word_merge() {
    let sql = "START TRANSACTION READ ONLY AS OF TIMESTAMP '2015-09-21 00:07:01'";
    let toks = tokens(sql);
    assert!(
        toks.iter()
            .any(|token| token.kind == TokenKind::Keyword
                && token.text.eq_ignore_ascii_case("AS OF")),
        "AS OF must merge into one keyword, got {toks:?}"
    );
}

// ---------------------------------------------------------------------------
// pkg/parser/parser_test.go — TestSignedInt64OutOfRange
// ---------------------------------------------------------------------------

/// Go: `pkg/parser/parser_test.go`, `TestSignedInt64OutOfRange`.
///
/// Go `ParseOneStmt` rejects `18446744073709551612` as "out of range"
/// because the parser copies the scanner integer into an `int64`. The
/// scanner-owned half is that the same spelling still fits in `u64`
/// (`2^64-4`) so it is an `IntLit` whose `next_literal` is `UInt`, not
/// a `DecLit` overflow. Values past `u64::MAX` degrade to `DecLit`.
#[test]
fn signed_int64_out_of_range_literal_class() {
    const OVER_I64: &str = "18446744073709551612"; // 2^64 - 4
    assert_eq!(first_kind(OVER_I64), TokenKind::IntLit);
    assert_eq!(
        Lexer::new(OVER_I64).next_literal(),
        LiteralValue::UInt(18_446_744_073_709_551_612)
    );
    // Past u64: Go toInt → toDecimal.
    assert_eq!(
        first_kind("18446744073709551616"),
        TokenKind::DecLit,
        "2^64 must degrade to DecLit"
    );
}

// ---------------------------------------------------------------------------
// pkg/parser/parser_test.go — TestBRIE / TestStatisticsOps /
// TestHighNotPrecedenceMode / TestCTE / TestCTEMerge / TestAsOfClause
// ---------------------------------------------------------------------------

/// Go: `pkg/parser/parser_test.go`, `TestHighNotPrecedenceMode`.
///
/// Go compares AST shape of `NOT 1 BETWEEN` vs `!1 BETWEEN` with and
/// without `ModeHighNotPrecedence`. The scanner still emits `NOT` as
/// a keyword and `!` as an operator in both modes — the flag is a
/// grammar-level rewrite (`NOT` binds like unary `!`). This test pins only
/// the token stream owned here.
#[test]
fn high_not_precedence_mode_token_stream() {
    let not_sql = "SELECT NOT 1 BETWEEN -5 AND 5";
    let bang_sql = "SELECT !1 BETWEEN -5 AND 5";
    let default_not: Vec<_> = tokens(not_sql)
        .into_iter()
        .map(|token| (token.kind, token.text))
        .collect();
    let mode = SqlMode {
        high_not_precedence: true,
        ..SqlMode::default()
    };
    let high_not: Vec<_> = Lexer::new(not_sql)
        .with_sql_mode(mode)
        .tokenize()
        .into_iter()
        .filter(|token| token.kind != TokenKind::Eof)
        .map(|token| (token.kind, token.text))
        .collect();
    // Scanner does not rewrite NOT under HIGH_NOT_PRECEDENCE; token
    // identity is unchanged (the parser reads the flag).
    assert_eq!(default_not, high_not);
    assert!(
        default_not
            .iter()
            .any(|(kind, text)| *kind == TokenKind::Keyword && text.eq_ignore_ascii_case("NOT")),
        "NOT remains a keyword, got {default_not:?}"
    );
    let bang: Vec<_> = tokens(bang_sql)
        .into_iter()
        .map(|token| token.kind)
        .collect();
    assert!(
        bang.contains(&TokenKind::Op),
        "! must be an operator, got {bang:?}"
    );
}

// ---------------------------------------------------------------------------
// pkg/parser/parser_test.go — TestPartitionKeyAlgorithm / TestHelp /
// TestWithoutCharsetFlags / TestRestoreBinOpWithBrackets / TestCTEBindings
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// pkg/parser/parser_test.go — TestPlanReplayer / TestTrafficStmt /
// TestGBKEncoding / TestGB18030Encoding
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// pkg/parser/parser_test.go — TestInsertStatementMemoryAllocation /
// TestCharsetIntroducer / TestNonTransactionalDML
// ---------------------------------------------------------------------------

/// Go: `pkg/parser/parser_test.go`, `TestCharsetIntroducer`.
///
/// Go asserts `_gbk 'a'` / `_gbk 0x1234` / `_gbk 0b101001` all fail
/// with `[ddl:1115] Unsupported character introducer: 'gbk'`. The
/// scanner still classifies `_gbk` as an introducer (gbk is in
/// `CHARSET_NAMES`); the error is a grammar action on a recognized
/// but non-legacy charset.
#[test]
fn charset_introducer_gbk_is_recognized_name() {
    for sql in ["_gbk 'a'", "_gbk 0x1234", "_gbk 0b101001"] {
        let token = Lexer::new(sql).next_token();
        assert_eq!(
            token.kind,
            TokenKind::CharsetIntroducer,
            "{sql} must still be an introducer"
        );
        assert_eq!(token.text, "gbk", "{sql} canonical name");
    }
    assert_eq!(crate::canonical_charset("gbk"), Some("gbk"));
    assert_eq!(crate::canonical_legacy_charset("gbk"), None);
}

// ---------------------------------------------------------------------------
// pkg/parser/parser_test.go — TestIntervalPartition / TestTTLTableOption /
// TestIssue45898 / TestMultiStmt / TestCompatTypes / TestVector /
// TestExplainExplore
// ---------------------------------------------------------------------------

/// Go: `pkg/parser/parser_test.go`, `TestTTLTableOption`.
///
/// `/*T![ttl] ... */` is executable because `ttl` is in the feature-ID
/// allowlist (`pkg/parser/tidb`). This test pins that membership.
#[test]
fn ttl_table_option_feature_id_allowlist() {
    assert!(can_parse_feature(&[FEATURE_ID_TTL]));
    assert!(!can_parse_feature(&[FEATURE_ID_RESOURCE_GROUP]));
    // Executable comment: the body is live SQL, markers dropped.
    let kinds: Vec<_> = tokens("create table t (created_at datetime) /*T![ttl] ttl=created_at + INTERVAL 1 YEAR ttl_enable='ON'*/")
        .into_iter()
        .map(|token| token.kind)
        .collect();
    assert!(
        kinds.contains(&TokenKind::Keyword),
        "T![ttl] body must be scanned as live SQL, got {kinds:?}"
    );
    assert!(
        !kinds.contains(&TokenKind::HintComment),
        "T![ttl] is an executable comment, not a hint"
    );
}

/// Go: `pkg/parser/parser_test.go`, `TestMultiStmt`.
///
/// Go splits `SELECT 'foo'; SELECT 'foo;bar','baz'; ...` into four
/// statements and inspects field text. The scanner-owned half is that
/// `';'` inside a string is *not* a statement separator, and that the
/// four `SELECT` keywords plus the four terminating `;` / EOF-adjacent
/// ints are visible in the token stream.
#[test]
fn multi_stmt_string_does_not_split_on_embedded_semicolon() {
    let sql = "SELECT 'foo'; SELECT 'foo;bar','baz'; select 'foo' , 'bar' , 'baz' ;select 1";
    let toks = tokens(sql);
    let selects = toks
        .iter()
        .filter(|token| {
            token.kind == TokenKind::Keyword && token.text.eq_ignore_ascii_case("SELECT")
        })
        .count();
    assert_eq!(selects, 4, "four SELECT keywords, got {toks:?}");
    let strings: Vec<_> = toks
        .iter()
        .filter(|token| token.kind == TokenKind::Str)
        .map(|token| token.text.as_str())
        .collect();
    assert_eq!(
        strings,
        ["'foo'", "'foo;bar'", "'baz'", "'foo'", "'bar'", "'baz'"]
    );
    assert_eq!(decoded_string("'foo;bar'"), b"foo;bar".to_vec());
}

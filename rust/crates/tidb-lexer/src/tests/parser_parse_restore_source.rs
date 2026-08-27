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

//! Batch b055 port of `pkg/parser` part-5 unit tests (Go tests sorted by
//! file path + line number on `origin/master`, items 241–300).
//!
//! The range is entirely `pkg/parser/parser_test.go` (60 tests). Those
//! tests drive `parser.Parse` / `ParseOneStmt` plus AST Restore / field
//! inspection. The yacc parser and AST live in `tidb-parser` / `tidb-ast`,
//! which depend on this crate, so this crate cannot depend back on them.
//!
//! Surfaces this crate *does* own, and that those Go tests exercise as
//! lexical preconditions, are pinned below rather than approximated:
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
//! Every remaining Go assertion is a parse/restore/AST-shape check and is
//! recorded as an explicit `go-parity-gap` ignore.

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

/// Go: `pkg/parser/parser_test.go`, `TestSQLResult` statement bodies.
#[test]
#[ignore = "go-parity-gap: SELECT modifier parse/restore requires the yacc parser and AST, not owned by tidb-lexer"]
fn sql_result() {}

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

/// Go: `pkg/parser/parser_test.go`, `TestSQLNoCache` statement bodies.
#[test]
#[ignore = "go-parity-gap: SelectStmtOpts.SQLCache is set by the yacc parser, not owned by tidb-lexer"]
fn sql_no_cache() {}

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

/// Go: `pkg/parser/parser_test.go`, `TestEscape` parse/restore bodies.
#[test]
#[ignore = "go-parity-gap: charset-prefixed string restore (_UTF8MB4'...') requires the yacc parser and AST"]
fn escape() {}

// ---------------------------------------------------------------------------
// pkg/parser/parser_test.go — TestExplain / TestPrepare / TestDeallocate /
// TestExecute / TestTrace / TestBinding / TestView
// ---------------------------------------------------------------------------

/// Go: `pkg/parser/parser_test.go`, `TestExplain`.
#[test]
#[ignore = "go-parity-gap: EXPLAIN/DESC parse and Restore live in tidb-parser/tidb-ast, not tidb-lexer"]
fn explain() {}

/// Go: `pkg/parser/parser_test.go`, `TestPrepare`.
#[test]
#[ignore = "go-parity-gap: PREPARE statement parse/restore requires the yacc parser, not owned by tidb-lexer"]
fn prepare() {}

/// Go: `pkg/parser/parser_test.go`, `TestDeallocate`.
#[test]
#[ignore = "go-parity-gap: DEALLOCATE PREPARE parse/restore requires the yacc parser, not owned by tidb-lexer"]
fn deallocate() {}

/// Go: `pkg/parser/parser_test.go`, `TestExecute`.
#[test]
#[ignore = "go-parity-gap: EXECUTE statement parse/restore requires the yacc parser, not owned by tidb-lexer"]
fn execute() {}

/// Go: `pkg/parser/parser_test.go`, `TestTrace`.
#[test]
#[ignore = "go-parity-gap: TRACE statement parse/restore requires the yacc parser, not owned by tidb-lexer"]
fn trace() {}

/// Go: `pkg/parser/parser_test.go`, `TestBinding`.
///
/// Hint comments after CREATE/DROP are a lexer-owned position rule
/// (`hintedTokens` includes CREATE). Pin that the CREATE BINDING
/// vectors keep the `/*+ ... */` token; the statement grammar itself
/// is a parser gap.
#[test]
fn binding_create_keeps_optimizer_hint() {
    let sql = "CREATE GLOBAL BINDING FOR UPDATE `t` SET `a`=1 WHERE `b`=1 USING UPDATE /*+ USE_INDEX(`t` `b`)*/ `t` SET `a`=1 WHERE `b`=1";
    let kinds: Vec<_> = tokens(sql).into_iter().map(|token| token.kind).collect();
    assert!(
        kinds.contains(&TokenKind::HintComment),
        "CREATE ... UPDATE /*+ ... */ must retain a hint token, got {kinds:?}"
    );
}

/// Go: `pkg/parser/parser_test.go`, `TestBinding` statement bodies.
#[test]
#[ignore = "go-parity-gap: CREATE/DROP/SET BINDING parse/restore requires the yacc parser, not owned by tidb-lexer"]
fn binding() {}

/// Go: `pkg/parser/parser_test.go`, `TestView`.
#[test]
#[ignore = "go-parity-gap: CREATE VIEW parse/restore and CreateViewStmt.Select.Text require the yacc parser and AST"]
fn view() {}

// ---------------------------------------------------------------------------
// pkg/parser/parser_test.go — TestTimestampDiffUnit / TestFuncCallExprOffset
// ---------------------------------------------------------------------------

/// Go: `pkg/parser/parser_test.go`, `TestTimestampDiffUnit`.
#[test]
#[ignore = "go-parity-gap: TIMESTAMPDIFF unit AST typing and illegal-unit rejection require the yacc parser"]
fn timestamp_diff_unit() {}

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

/// Go: `pkg/parser/parser_test.go`, `TestFuncCallExprOffset` AST field.
#[test]
#[ignore = "go-parity-gap: FuncCallExpr.OriginTextPosition is an AST field populated by the yacc parser"]
fn func_call_expr_offset() {}

// ---------------------------------------------------------------------------
// pkg/parser/parser_test.go — TestSessionManage / TestParseShowOpenTables
// ---------------------------------------------------------------------------

/// Go: `pkg/parser/parser_test.go`, `TestSessionManage`.
#[test]
#[ignore = "go-parity-gap: KILL/SHOW PROCESSLIST/SHUTDOWN/RESTART parse/restore require the yacc parser"]
fn session_manage() {}

/// Go: `pkg/parser/parser_test.go`, `TestParseShowOpenTables`.
#[test]
#[ignore = "go-parity-gap: SHOW OPEN TABLES parse/restore requires the yacc parser, not owned by tidb-lexer"]
fn parse_show_open_tables() {}

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

/// Go: `pkg/parser/parser_test.go`, `TestSQLModeANSIQuotes` parse acceptance.
#[test]
#[ignore = "go-parity-gap: ANSI_QUOTES CREATE TABLE / SELECT alias parse acceptance requires the yacc parser"]
fn sql_mode_ansi_quotes() {}

// ---------------------------------------------------------------------------
// pkg/parser/parser_test.go — TestDDLStatements / TestAnalyze /
// TestTableSample / TestGeneratedColumn / TestSetTransaction / TestSideEffect
// ---------------------------------------------------------------------------

/// Go: `pkg/parser/parser_test.go`, `TestDDLStatements`.
#[test]
#[ignore = "go-parity-gap: CREATE TABLE charset/collate/flag assignment and grammar errors require the yacc parser"]
fn ddl_statements() {}

/// Go: `pkg/parser/parser_test.go`, `TestAnalyze`.
#[test]
#[ignore = "go-parity-gap: ANALYZE TABLE parse/restore requires the yacc parser, not owned by tidb-lexer"]
fn analyze() {}

/// Go: `pkg/parser/parser_test.go`, `TestTableSample`.
#[test]
#[ignore = "go-parity-gap: TABLESAMPLE parse/restore requires the yacc parser, not owned by tidb-lexer"]
fn table_sample() {}

/// Go: `pkg/parser/parser_test.go`, `TestGeneratedColumn`.
#[test]
#[ignore = "go-parity-gap: generated-column option AST text and ddl:1221 errors require the yacc parser"]
fn generated_column() {}

/// Go: `pkg/parser/parser_test.go`, `TestSetTransaction`.
#[test]
#[ignore = "go-parity-gap: SET TRANSACTION rewrite to tx_isolation is a parser/AST action, not owned by tidb-lexer"]
fn set_transaction() {}

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

/// Go: `pkg/parser/parser_test.go`, `TestSideEffect` parser reuse.
#[test]
#[ignore = "go-parity-gap: parser reuse after a failed ParseOneStmt is yyParser state, not owned by tidb-lexer"]
fn side_effect() {}

// ---------------------------------------------------------------------------
// pkg/parser/parser_test.go — TestTablePartition /
// TestTablePartitionNameList / TestNotExistsSubquery
// ---------------------------------------------------------------------------

/// Go: `pkg/parser/parser_test.go`, `TestTablePartition`.
#[test]
#[ignore = "go-parity-gap: partition DDL parse/restore and Partition.Definitions comments require the yacc parser and AST"]
fn table_partition() {}

/// Go: `pkg/parser/parser_test.go`, `TestTablePartitionNameList`.
#[test]
#[ignore = "go-parity-gap: TableName.PartitionNames is populated by the yacc parser, not owned by tidb-lexer"]
fn table_partition_name_list() {}

/// Go: `pkg/parser/parser_test.go`, `TestNotExistsSubquery`.
#[test]
#[ignore = "go-parity-gap: ExistsSubqueryExpr.Not is an AST field set by the yacc parser"]
fn not_exists_subquery() {}

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

/// Go: `pkg/parser/parser_test.go`, `TestWindowFunctionIdentifier` parse.
#[test]
#[ignore = "go-parity-gap: SELECT alias acceptance of window keywords requires the yacc parser"]
fn window_function_identifier() {}

/// Go: `pkg/parser/parser_test.go`, `TestWindowFunctions`.
#[test]
#[ignore = "go-parity-gap: window-function parse/restore requires the yacc parser with EnableWindowFunc, not owned by tidb-lexer"]
fn window_functions() {}

/// Go: `pkg/parser/parser_test.go`, `TestVisitFrameBound`.
#[test]
#[ignore = "go-parity-gap: FrameBound visitor inspection requires parsed window AST nodes"]
fn visit_frame_bound() {}

// ---------------------------------------------------------------------------
// pkg/parser/parser_test.go — TestFieldText / TestQuotedSystemVariables /
// TestQuotedVariableColumnName
// ---------------------------------------------------------------------------

/// Go: `pkg/parser/parser_test.go`, `TestFieldText`.
#[test]
#[ignore = "go-parity-gap: SelectField.Text / TraceStmt.Text are AST origin-text fields, not owned by tidb-lexer"]
fn field_text() {}

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

/// Go: `pkg/parser/parser_test.go`, `TestQuotedSystemVariables` AST fields.
#[test]
#[ignore = "go-parity-gap: VariableExpr Name/IsGlobal/IsInstance/ExplicitScope are AST fields set by the parser"]
fn quoted_system_variables() {}

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

/// Go: `pkg/parser/parser_test.go`, `TestQuotedVariableColumnName` field.Text.
#[test]
#[ignore = "go-parity-gap: SelectField.Text for variable columns is an AST origin-text field"]
fn quoted_variable_column_name() {}

// ---------------------------------------------------------------------------
// pkg/parser/parser_test.go — TestCharset / TestUnderscoreCharset
// ---------------------------------------------------------------------------

/// Go: `pkg/parser/parser_test.go`, `TestCharset`.
#[test]
#[ignore = "go-parity-gap: ALTER DATABASE/SCHEMA CHAR SET parse requires the yacc parser, not owned by tidb-lexer"]
fn charset() {}

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

/// Go: `pkg/parser/parser_test.go`, `TestUnderscoreCharset` parse errors.
#[test]
#[ignore = "go-parity-gap: unsupported-introducer [ddl:1115] and near-quote syntax errors are parser actions"]
fn underscore_charset() {}

// ---------------------------------------------------------------------------
// pkg/parser/parser_test.go — TestFulltextSearch / TestStartTransaction
// ---------------------------------------------------------------------------

/// Go: `pkg/parser/parser_test.go`, `TestFulltextSearch`.
#[test]
#[ignore = "go-parity-gap: MATCH AGAINST parse/Format requires the yacc parser and AST"]
fn fulltext_search() {}

/// Go: `pkg/parser/parser_test.go`, `TestStartTransaction`.
///
/// `AS OF` is a two-word scanner merge (`AS` + `OF` → one keyword).
/// Pin that merge on the READ ONLY AS OF TIMESTAMP vectors; the
/// START TRANSACTION grammar itself is a parser gap.
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

/// Go: `pkg/parser/parser_test.go`, `TestStartTransaction` parse/restore.
#[test]
#[ignore = "go-parity-gap: START TRANSACTION parse/restore requires the yacc parser, not owned by tidb-lexer"]
fn start_transaction() {}

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

/// Go: `pkg/parser/parser_test.go`, `TestSignedInt64OutOfRange` parse errors.
#[test]
#[ignore = "go-parity-gap: int64 range rejection in RECOVER/ADMIN/CREATE USER is a parser action on the scanned integer"]
fn signed_int64_out_of_range() {}

// ---------------------------------------------------------------------------
// pkg/parser/parser_test.go — TestBRIE / TestStatisticsOps /
// TestHighNotPrecedenceMode / TestCTE / TestCTEMerge / TestAsOfClause
// ---------------------------------------------------------------------------

/// Go: `pkg/parser/parser_test.go`, `TestBRIE`.
#[test]
#[ignore = "go-parity-gap: BACKUP/RESTORE/BRIE parse/restore requires the yacc parser, not owned by tidb-lexer"]
fn brie() {}

/// Go: `pkg/parser/parser_test.go`, `TestStatisticsOps`.
#[test]
#[ignore = "go-parity-gap: CREATE/DROP STATISTICS parse and CreateStatisticsStmt fields require the yacc parser"]
fn statistics_ops() {}

/// Go: `pkg/parser/parser_test.go`, `TestHighNotPrecedenceMode`.
///
/// Go compares AST shape of `NOT 1 BETWEEN` vs `!1 BETWEEN` with and
/// without `ModeHighNotPrecedence`. The scanner still emits `NOT` as
/// a keyword and `!` as an operator in both modes — the flag is a
/// grammar-level rewrite (`NOT` binds like unary `!`). Pin the token
/// stream; the AST rewrite is a parser gap.
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

/// Go: `pkg/parser/parser_test.go`, `TestHighNotPrecedenceMode` AST rewrite.
#[test]
#[ignore = "go-parity-gap: HIGH_NOT_PRECEDENCE rewrites NOT-vs-BETWEEN AST shape in the yacc parser"]
fn high_not_precedence_mode() {}

/// Go: `pkg/parser/parser_test.go`, `TestCTE`.
#[test]
#[ignore = "go-parity-gap: WITH/CTE parse/restore requires the yacc parser, not owned by tidb-lexer"]
fn cte() {}

/// Go: `pkg/parser/parser_test.go`, `TestCTEMerge`.
#[test]
#[ignore = "go-parity-gap: CTE merge parse/restore requires the yacc parser, not owned by tidb-lexer"]
fn cte_merge() {}

/// Go: `pkg/parser/parser_test.go`, `TestAsOfClause`.
///
/// `AS OF` merge is the same scanner rule pinned under
/// `start_transaction_as_of_two_word_merge`; the clause grammar and
/// SET TRANSACTION rewrite are parser-owned.
#[test]
#[ignore = "go-parity-gap: AS OF TIMESTAMP clause parse/restore and SET TRANSACTION rewrite require the yacc parser"]
fn as_of_clause() {}

// ---------------------------------------------------------------------------
// pkg/parser/parser_test.go — TestPartitionKeyAlgorithm / TestHelp /
// TestWithoutCharsetFlags / TestRestoreBinOpWithBrackets / TestCTEBindings
// ---------------------------------------------------------------------------

/// Go: `pkg/parser/parser_test.go`, `TestPartitionKeyAlgorithm`.
#[test]
#[ignore = "go-parity-gap: PARTITION BY LINEAR KEY ALGORITHM parse requires the yacc parser"]
fn partition_key_algorithm() {}

/// Go: `pkg/parser/parser_test.go`, `TestHelp`.
#[test]
#[ignore = "go-parity-gap: HELP statement parse/restore requires the yacc parser, not owned by tidb-lexer"]
fn help() {}

/// Go: `pkg/parser/parser_test.go`, `TestWithoutCharsetFlags`.
#[test]
#[ignore = "go-parity-gap: RestoreStringWithoutCharset / RestoreStringWithoutDefaultCharset are AST Restore flags"]
fn without_charset_flags() {}

/// Go: `pkg/parser/parser_test.go`, `TestRestoreBinOpWithBrackets`.
#[test]
#[ignore = "go-parity-gap: RestoreBracketAroundBinaryOperation is an AST Restore flag, not owned by tidb-lexer"]
fn restore_bin_op_with_brackets() {}

/// Go: `pkg/parser/parser_test.go`, `TestCTEBindings`.
#[test]
#[ignore = "go-parity-gap: CTE binding Restore with DefaultDB requires the yacc parser and AST"]
fn cte_bindings() {}

// ---------------------------------------------------------------------------
// pkg/parser/parser_test.go — TestPlanReplayer / TestTrafficStmt /
// TestGBKEncoding / TestGB18030Encoding
// ---------------------------------------------------------------------------

/// Go: `pkg/parser/parser_test.go`, `TestPlanReplayer`.
#[test]
#[ignore = "go-parity-gap: PLAN REPLAYER parse and PlanReplayerStmt fields require the yacc parser and AST"]
fn plan_replayer() {}

/// Go: `pkg/parser/parser_test.go`, `TestTrafficStmt`.
#[test]
#[ignore = "go-parity-gap: TRAFFIC CAPTURE/REPLAY parse/restore requires the yacc parser, not owned by tidb-lexer"]
fn traffic_stmt() {}

/// Go: `pkg/parser/parser_test.go`, `TestGBKEncoding`.
#[test]
#[ignore = "go-parity-gap: CharsetClient(gbk) transcoding and GBK parser options are unported in tidb-lexer"]
fn gbk_encoding() {}

/// Go: `pkg/parser/parser_test.go`, `TestGB18030Encoding`.
#[test]
#[ignore = "go-parity-gap: CharsetClient(gb18030) transcoding is unported in tidb-lexer"]
fn gb18030_encoding() {}

// ---------------------------------------------------------------------------
// pkg/parser/parser_test.go — TestInsertStatementMemoryAllocation /
// TestCharsetIntroducer / TestNonTransactionalDML
// ---------------------------------------------------------------------------

/// Go: `pkg/parser/parser_test.go`, `TestInsertStatementMemoryAllocation`.
#[test]
#[ignore = "go-parity-gap: ParseOneStmt allocation bound is a parser memory-layout check, not a lexer contract"]
fn insert_statement_memory_allocation() {}

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

/// Go: `pkg/parser/parser_test.go`, `TestCharsetIntroducer` parse error.
#[test]
#[ignore = "go-parity-gap: [ddl:1115] Unsupported character introducer is a parser error on a recognized non-legacy charset"]
fn charset_introducer() {}

/// Go: `pkg/parser/parser_test.go`, `TestNonTransactionalDML`.
#[test]
#[ignore = "go-parity-gap: BATCH ON/LIMIT non-transactional DML parse/restore requires the yacc parser"]
fn non_transactional_dml() {}

// ---------------------------------------------------------------------------
// pkg/parser/parser_test.go — TestIntervalPartition / TestTTLTableOption /
// TestIssue45898 / TestMultiStmt / TestCompatTypes / TestVector /
// TestExplainExplore
// ---------------------------------------------------------------------------

/// Go: `pkg/parser/parser_test.go`, `TestIntervalPartition`.
#[test]
#[ignore = "go-parity-gap: INTERVAL partition DDL parse/restore requires the yacc parser, not owned by tidb-lexer"]
fn interval_partition() {}

/// Go: `pkg/parser/parser_test.go`, `TestTTLTableOption`.
///
/// `/*T![ttl] ... */` is executable because `ttl` is in the feature-ID
/// allowlist (`pkg/parser/tidb`). Pin that membership; the CREATE/ALTER
/// TABLE option grammar is a parser gap.
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

/// Go: `pkg/parser/parser_test.go`, `TestTTLTableOption` statement bodies.
#[test]
#[ignore = "go-parity-gap: TTL table-option parse/restore and TTL_ENABLE validation require the yacc parser"]
fn ttl_table_option() {}

/// Go: `pkg/parser/parser_test.go`, `TestIssue45898`.
#[test]
#[ignore = "go-parity-gap: parser reuse after a truncated `a.` input is yyParser state, not owned by tidb-lexer"]
fn issue45898() {}

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

/// Go: `pkg/parser/parser_test.go`, `TestMultiStmt` statement split.
#[test]
#[ignore = "go-parity-gap: multi-statement Parse and SelectField.Text require the yacc parser"]
fn multi_stmt() {}

/// Go: `pkg/parser/parser_test.go`, `TestCompatTypes`.
#[test]
#[ignore = "go-parity-gap: vendor type aliases (BOOL→TINYINT(1), etc.) are parser/AST Restore rewrites"]
fn compat_types() {}

/// Go: `pkg/parser/parser_test.go`, `TestVector`.
#[test]
#[ignore = "go-parity-gap: VECTOR / VECTOR<FLOAT> type parse/restore requires the yacc parser"]
fn vector() {}

/// Go: `pkg/parser/parser_test.go`, `TestExplainExplore`.
#[test]
#[ignore = "go-parity-gap: EXPLAIN EXPLORE parse and ExplainStmt.Explore/ReplayerFile fields require the yacc parser"]
fn explain_explore() {}

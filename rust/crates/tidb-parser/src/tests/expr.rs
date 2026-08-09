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

//! Expression grammar tests: operators, predicates, literals,
//! builtin-function call shapes, `CAST`/`CONVERT`, and the
//! special keyword-led function grammars.

use super::*;

#[test]
fn operator_precedence() {
    // * binds tighter than +
    assert_eq!(r("select 1+2*3"), "SELECT 1+2*3");
    // parentheses are preserved
    assert_eq!(r("select (1+2)*3"), "SELECT (1+2)*3");
    // AND binds tighter than OR; both looser than comparison
    assert_eq!(
        r("select a from t where a=1 or b=2 and c=3"),
        "SELECT `a` FROM `t` WHERE `a`=1 OR `b`=2 AND `c`=3"
    );
    // XOR sits between OR and AND
    assert_eq!(
        r("select a from t where a and b xor c or d"),
        "SELECT `a` FROM `t` WHERE `a` AND `b` XOR `c` OR `d`"
    );
}

/// Go `pkg/parser/ast/format_test.go`'s decimal restore rows: numeric
/// decimal values lose leading integer zeros while retaining the written
/// fractional scale. This is distinct from integer normalization and from
/// hex/bit literals, whose leading zeros have separate source contracts.
#[test]
fn decimal_literal_restore_preserves_fractional_scale() {
    assert_eq!(r("select 000003.000000"), "SELECT 3.000000");
    assert_eq!(r("select 00.0001000"), "SELECT 0.0001000");
    assert_eq!(r("select .50"), "SELECT 0.50");
}

/// `HandParser.parseCharFuncCall` validates `USING` through
/// `charset.GetCharsetInfo` and restores the canonical charset name. Keep the
/// invalid integration row on this source-owned expression leaf rather than
/// allowing the generic name parser to accept arbitrary payloads.
#[test]
fn char_using_charset_matches_go_registry() {
    assert_eq!(
        r("select char(97, null, 100, 256, 89 using utf8mb3)"),
        "SELECT CHAR_FUNC(97, NULL, 100, 256, 89, 'utf8')"
    );
    assert!(parse("select char(97, null, 100, 256, 89 using tidb)").is_err());
}

#[test]
fn unary_and_not() {
    assert_eq!(r("select -a, +b, ~c, !d"), "SELECT -`a`,+`b`,~`c`,!`d`");
    assert_eq!(
        r("select a from t where not b = 1"),
        "SELECT `a` FROM `t` WHERE NOT `b`=1"
    );
}

/// `expr COLLATE collation_name` — a general postfix suffix, distinct from
/// [`tidb_ast::ColumnOption::Collate`]'s own column-definition-level
/// syntax. Precedence confirmed by reading real TiDB's own hand-written
/// parser directly (`pkg/parser/prec.go`'s `precCollate`, `expr_parser.go`'s
/// `case collate:` arm), not guessed from restore text alone (ambiguous
/// for this grammar shape, since COLLATE's own right-hand side is always a
/// bare name, never a sub-expression that restore's own parenthesization
/// could disambiguate).
#[test]
fn collate_expr() {
    assert_eq!(
        r("select a collate utf8mb4_bin from t"),
        "SELECT `a` COLLATE utf8mb4_bin FROM `t`"
    );
    // Canonically lowercased on restore — the OPPOSITE case convention
    // from a charset name's own uppercasing.
    assert_eq!(
        r("select a collate UTF8MB4_BIN from t"),
        "SELECT `a` COLLATE utf8mb4_bin FROM `t`"
    );
    // Binds TIGHTER than unary -/NOT — MySQL's own documented example:
    // `-1 COLLATE x` == `-(1 COLLATE x)`.
    assert_eq!(
        r("select -a collate utf8mb4_bin from t"),
        "SELECT -`a` COLLATE utf8mb4_bin FROM `t`"
    );
    assert_eq!(
        r("select not a collate utf8mb4_bin from t"),
        "SELECT NOT `a` COLLATE utf8mb4_bin FROM `t`"
    );
    // Grabs only its immediate left operand from a binary expression,
    // regardless of which side it's written on.
    assert_eq!(
        r("select a + b collate utf8mb4_bin from t"),
        "SELECT `a`+`b` COLLATE utf8mb4_bin FROM `t`"
    );
    assert_eq!(
        r("select a collate utf8mb4_bin + b from t"),
        "SELECT `a` COLLATE utf8mb4_bin+`b` FROM `t`"
    );
    // Chains left-to-right with itself.
    assert_eq!(
        r("select a collate utf8mb4_bin collate utf8mb4_general_ci from t"),
        "SELECT `a` COLLATE utf8mb4_bin COLLATE utf8mb4_general_ci FROM `t`"
    );
    assert_eq!(
        r("select a collate utf8mb4_bin and b from t"),
        "SELECT `a` COLLATE utf8mb4_bin AND `b` FROM `t`"
    );
    // The collation name accepts a QUOTED STRING too, not just a bare
    // identifier (`parseCollateExpr`'s own `p.next()` takes literally
    // any next token) — restores identically either way, lowercased,
    // unquoted.
    assert_eq!(
        r("select a collate 'binary' from t"),
        "SELECT `a` COLLATE binary FROM `t`"
    );
    assert_eq!(
        r("select a collate 'UTF8MB4_BIN' from t"),
        "SELECT `a` COLLATE utf8mb4_bin FROM `t`"
    );
    // Go's CollationName production validates expression suffixes too.
    assert!(parse("select 1 collate some_unknown_collation").is_err());
}

#[test]
fn predicates() {
    assert_eq!(
        r("select a from t where a in (1,2,3)"),
        "SELECT `a` FROM `t` WHERE `a` IN (1,2,3)"
    );
    assert_eq!(
        r("select a from t where a not between 1 and 10"),
        "SELECT `a` FROM `t` WHERE `a` NOT BETWEEN 1 AND 10"
    );
    assert_eq!(
        r("select a from t where n like 'x%'"),
        "SELECT `a` FROM `t` WHERE `n` LIKE _UTF8MB4'x%'"
    );
    assert_eq!(
        r("select a from t where a is not null"),
        "SELECT `a` FROM `t` WHERE `a` IS NOT NULL"
    );
    // `REGEXP`/`RLIKE` — same precedence level as LIKE/IN/BETWEEN; `RLIKE`
    // normalizes to `REGEXP` on restore (both real MySQL synonyms).
    assert_eq!(
        r("select a from t where a regexp '.*'"),
        "SELECT `a` FROM `t` WHERE `a` REGEXP _UTF8MB4'.*'"
    );
    assert_eq!(
        r("select a from t where a not regexp 'x'"),
        "SELECT `a` FROM `t` WHERE `a` NOT REGEXP _UTF8MB4'x'"
    );
    assert_eq!(
        r("select 'a' rlike 'x'"),
        "SELECT _UTF8MB4'a' REGEXP _UTF8MB4'x'"
    );
    // The pattern is a bit_expr (tighter than the predicate itself) —
    // same precedence LIKE's own pattern uses.
    assert_eq!(
        r("select a regexp 'x' | 'y' from t"),
        "SELECT `a` REGEXP _UTF8MB4'x'|_UTF8MB4'y' FROM `t`"
    );
}

#[test]
fn like_escape_clause() {
    // A non-default escape character is preserved on restore.
    assert_eq!(
        r("select 'a' like '+a' escape '+'"),
        "SELECT _UTF8MB4'a' LIKE _UTF8MB4'+a' ESCAPE '+'"
    );
    // `ESCAPE '\'` is the real MySQL/TiDB default, so real TiDB elides the
    // clause entirely on restore (confirmed via godump).
    assert_eq!(
        r("select 'a' like 'x' escape '\\\\'"),
        "SELECT _UTF8MB4'a' LIKE _UTF8MB4'x'"
    );
    // `ESCAPE ''` disables escape processing entirely — a distinct case
    // from the default, so it's NOT elided.
    assert_eq!(
        r("select 'a' like 'x' escape ''"),
        "SELECT _UTF8MB4'a' LIKE _UTF8MB4'x' ESCAPE ''"
    );
    // Composes with NOT LIKE.
    assert_eq!(
        r("select 'a' not like 'x%' escape '+'"),
        "SELECT _UTF8MB4'a' NOT LIKE _UTF8MB4'x%' ESCAPE '+'"
    );
    // A plain LIKE with no ESCAPE clause is unaffected.
    assert_eq!(
        r("select a from t where n like 'x%'"),
        "SELECT `a` FROM `t` WHERE `n` LIKE _UTF8MB4'x%'"
    );
}

/// `pkg/parser/parser_test.go::TestLikeEscape`.
#[test]
fn test_like_escape() {
    for (sql, expected) in [
        (
            r#"select "abc_" like "abc\\_" escape ''"#,
            Some(r#"SELECT _UTF8MB4'abc_' LIKE _UTF8MB4'abc\\_' ESCAPE ''"#),
        ),
        (
            r#"select "abc_" like "abc\\_" escape '\\'"#,
            Some(r#"SELECT _UTF8MB4'abc_' LIKE _UTF8MB4'abc\\_'"#),
        ),
        (r#"select "abc_" like "abc\\_" escape '||'"#, None),
        (
            r#"select "abc" like "escape" escape '+'"#,
            Some(r#"SELECT _UTF8MB4'abc' LIKE _UTF8MB4'escape' ESCAPE '+'"#),
        ),
        (
            r#"select '''_' like '''_' escape ''''"#,
            Some(r#"SELECT _UTF8MB4'''_' LIKE _UTF8MB4'''_' ESCAPE ''''"#),
        ),
    ] {
        match expected {
            Some(expected) => assert_eq!(r(sql), expected, "{sql}"),
            None => assert!(parse(sql).is_err(), "{sql}"),
        }
    }
}

/// `pkg/parser/parser_test.go::TestEscape`.
#[test]
fn test_escape() {
    for (sql, expected) in [
        (r#"select """;"#, None),
        (r#"select """";"#, Some(r#"SELECT _UTF8MB4'"'"#)),
        (r#"select "汉字";"#, Some("SELECT _UTF8MB4'汉字'")),
        (r#"select 'abc"def';"#, Some(r#"SELECT _UTF8MB4'abc"def'"#)),
        (r#"select 'a\r\n';"#, Some("SELECT _UTF8MB4'a\r\n'")),
        (r#"select "\a\r\n""#, Some("SELECT _UTF8MB4'a\r\n'")),
        (r#"select "\xFF""#, Some("SELECT _UTF8MB4'xFF'")),
    ] {
        match expected {
            Some(expected) => assert_eq!(r(sql), expected, "{sql:?}"),
            None => assert!(parse(sql).is_err(), "{sql:?}"),
        }
    }
}

#[test]
fn functions_and_literals() {
    assert_eq!(r("select f(a, b+1)"), "SELECT F(`a`, `b`+1)");
    assert_eq!(r("select null, true, false"), "SELECT NULL,TRUE,FALSE");
    assert_eq!(r("select 'it''s'"), "SELECT _UTF8MB4'it''s'");
    // Leading zeros drop from integer literals.
    assert_eq!(r("select 007"), "SELECT 7");
}

#[test]
fn variables() {
    assert_eq!(
        r("select @v, @@global.x, @@session.y, @@z"),
        "SELECT @`v`,@@GLOBAL.`x`,@@SESSION.`y`,@@`z`"
    );
}

/// Exact AST cases from Go `pkg/parser/parser_test.go`'s
/// `TestQuotedSystemVariables` and `TestDottedSystemVariableInExpr`.
#[test]
fn quoted_and_dotted_system_variables_match_go() {
    let Stmt::Query(query) = parse(
        "select @@Sql_Mode, @@`SQL_MODE`, @@session.`sql_mode`, @@global.`s ql``mode`, \
         @@session.'sql\\nmode', @@local.\"sql\\\"mode\", @@instance.sql_mode",
    )
    .unwrap() else {
        panic!("expected query")
    };
    let tidb_ast::QueryStmt::Select(select) = query.into_inner() else {
        panic!("expected select")
    };
    let expected = [
        (None, "sql_mode"),
        (None, "sql_mode"),
        (Some(tidb_ast::SysVarScope::Session), "sql_mode"),
        (Some(tidb_ast::SysVarScope::Global), "s ql`mode"),
        (Some(tidb_ast::SysVarScope::Session), "sql\nmode"),
        (Some(tidb_ast::SysVarScope::Session), "sql\"mode"),
        (Some(tidb_ast::SysVarScope::Instance), "sql_mode"),
    ];
    assert_eq!(select.fields.len(), expected.len());
    for (field, expected) in select.fields.iter().zip(expected) {
        let SelectField::Expr {
            expr: Expr::SysVar { scope, name },
            ..
        } = field
        else {
            panic!("expected system variable")
        };
        assert_eq!((*scope, name.as_str()), expected);
    }

    let Stmt::Query(query) = parse("select @@validate_password.length").unwrap() else {
        panic!("expected query")
    };
    let tidb_ast::QueryStmt::Select(select) = query.into_inner() else {
        panic!("expected select")
    };
    let SelectField::Expr {
        expr: Expr::SysVar { scope, name },
        ..
    } = &select.fields[0]
    else {
        panic!("expected system variable")
    };
    assert_eq!(*scope, None);
    assert_eq!(name, "validate_password.length");
}

/// Exact source-text cases from Go `pkg/parser/parser_test.go`'s
/// `TestQuotedVariableColumnName` (pingcap/parser#95).
#[test]
fn quoted_variable_fields_keep_their_original_text() {
    let sql = "select @abc, @`abc`, @'aBc', @\"AbC\", @6, @`6`, @'6', @\"6\", \
               @@sql_mode, @@`sql_mode`, @";
    let Stmt::Query(query) = parse(sql).unwrap() else {
        panic!("expected query")
    };
    let tidb_ast::QueryStmt::Select(select) = query.into_inner() else {
        panic!("expected select")
    };
    let expected = [
        "@abc",
        "@`abc`",
        "@'aBc'",
        "@\"AbC\"",
        "@6",
        "@`6`",
        "@'6'",
        "@\"6\"",
        "@@sql_mode",
        "@@`sql_mode`",
        "@",
    ];
    assert_eq!(select.fields.len(), expected.len());
    for (index, expected) in expected.iter().enumerate() {
        assert_eq!(
            select.fields.text(index),
            Some(expected.as_bytes()),
            "field {index}"
        );
    }
}

/// `@name := value` — an inline user-variable assignment expression,
/// usable anywhere an ordinary expression can appear. See
/// `tidb_ast::Expr::Assign`'s own doc for the real MySQL/TiDB quirk this
/// covers: `:=` following ANY variable atom, even `@@sysvar`/
/// `@@scope.sysvar`, always targets a plain user variable by its own
/// bare name.
#[test]
fn user_var_assign() {
    assert_eq!(r("select @i := 1"), "SELECT @`i`:=1");
    // `value` is parsed at the LOWEST precedence, so a further nested
    // `:=` chains right-associatively.
    assert_eq!(r("select @i := @j := 3"), "SELECT @`i`:=@`j`:=3");
    assert_eq!(
        r("select @tmp := c-2 from t where c=3"),
        "SELECT @`tmp`:=`c`-2 FROM `t` WHERE `c`=3"
    );
    assert_eq!(r("select @tmp1 := 11, @tmp2"), "SELECT @`tmp1`:=11,@`tmp2`");
    // Usable inside a parenthesized subquery, a `LIMIT`-clamped derived
    // table, and a `CASE` expression, not just a plain select field.
    assert_eq!(
        r("select @v := (select 1 from t1 t2 left join t1 on t1.a group by t1.a) from t1"),
        "SELECT @`v`:=(SELECT 1 FROM `t1` AS `t2` LEFT JOIN `t1` ON `t1`.`a` GROUP BY `t1`.`a`) FROM `t1`"
    );
    // A genuine, if obscure, MySQL/TiDB quirk: `:=` following a system-
    // variable atom (with or without an explicit scope) still targets a
    // PLAIN user variable, discarding the `@@` marker and scope
    // entirely — confirmed via `godump restore`.
    assert_eq!(r("select @@autocommit := 1"), "SELECT @`autocommit`:=1");
    assert_eq!(
        r("select @@session.autocommit := 1"),
        "SELECT @`autocommit`:=1"
    );
    assert_eq!(
        r("select @@global.autocommit := 1"),
        "SELECT @`autocommit`:=1"
    );
}

/// A bare or `AS`-explicit alias accepts not just a plain identifier but
/// also MOST keywords — see `is_alias_excluded_keyword`'s own doc for
/// the exact, curated exclusion list this mirrors (real TiDB's own
/// `pkg/parser/select_clauses_parser.go`'s `CanBeImplicitAlias`). Found
/// while implementing `:=` (`user_var_assign`'s own
/// `SELECT @sum := IF(...) sum` needed this to close).
#[test]
fn keyword_implicit_alias() {
    assert_eq!(r("select 1 sum"), "SELECT 1 AS `sum`");
    assert_eq!(r("select 1 as sum"), "SELECT 1 AS `sum`");
    assert_eq!(r("select 1 json"), "SELECT 1 AS `json`");
    assert_eq!(r("select 1 binary"), "SELECT 1 AS `binary`");
    assert_eq!(r("select 1 timestamp"), "SELECT 1 AS `timestamp`");
    // Applies to a table-ref alias too, not just a select-field alias.
    assert_eq!(
        r("select a from t1 t1sum"),
        "SELECT `a` FROM `t1` AS `t1sum`"
    );
    // `WINDOW` is excluded EVEN with an explicit `AS` (real TiDB itself
    // rejects both forms — confirmed via `godump restore`), since a
    // bare `WINDOW` right after the field list could otherwise be
    // confused with the query's own `WINDOW` clause introducer.
    assert!(parse("select 1 window").is_err());
    assert!(parse("select 1 as window").is_err());
    // The curated clause/DDL/window-function exclusion list still
    // applies — these are genuine `ParseError`s, not silently accepted.
    assert!(parse("select 1 rank").is_err());
    assert!(parse("select 1 partition").is_err());
    assert!(parse("select 1 default").is_err());
}

/// Exact keyword-map sweep from Go `pkg/parser/parser_test.go`'s
/// `TestWindowFunctionIdentifier`.
#[test]
fn window_function_keywords_follow_the_parser_switch() {
    for keyword in [
        "CUME_DIST",
        "DENSE_RANK",
        "FIRST_VALUE",
        "GROUPS",
        "LAG",
        "LAST_VALUE",
        "LEAD",
        "NTH_VALUE",
        "NTILE",
        "OVER",
        "PERCENT_RANK",
        "RANK",
        "ROW_NUMBER",
        "WINDOW",
    ] {
        let sql = format!("select 1 {keyword}");
        assert!(
            parse_with_window_functions(&sql, true).is_err(),
            "{keyword} must remain a keyword when window functions are enabled"
        );
        assert_eq!(
            parse_with_window_functions(&sql, false).unwrap().restore(),
            format!("SELECT 1 AS `{keyword}`"),
            "{keyword} must become an identifier when window functions are disabled"
        );
    }
}

#[test]
fn number_literals() {
    assert_eq!(
        r("select 1e3, 1.5e3, 2.5e-3"),
        "SELECT 1e+03,1.5e+03,2.5e-03"
    );
    assert_eq!(
        r("select 0xff, 0xf, 0xABCDEF"),
        "SELECT x'ff',x'0f',x'abcdef'"
    );
    assert_eq!(r("select 0b101, 0b0101"), "SELECT b'101',b'101'");
    let one = tidb_ast::BitLiteralValue::from_digits("1");
    assert_eq!(one, tidb_ast::BitLiteralValue::from_digits("00000001"));
    let wide_one = tidb_ast::BitLiteralValue::from_digits("000000001");
    assert_ne!(one, wide_one, "byte width is part of the Go AST value");
    assert_eq!(wide_one.as_bytes(), &[0x00, 0x01]);
    // A genuinely EMPTY quoted bit literal (`b''`/`B''`) restores as
    // `b''`, NOT `b'0'` — a different value from `b'0'` in real TiDB
    // (confirmed via `goeval`: `LENGTH(b'')` is 0, `LENGTH(b'0')` is 1,
    // even though both evaluate to 0 under arithmetic). The bare `0b`
    // form can never reach this empty (the lexer only emits a `BitLit`
    // token for `0b` when at least one binary digit follows).
    assert_eq!(
        r("select b'', B'', b'0', b'00'"),
        "SELECT b'',b'',b'0',b'0'"
    );
    assert_eq!(r("select .5, 1.50"), "SELECT 0.5,1.50");
    // A float literal that would overflow to infinity is rejected at
    // PARSE time, matching real TiDB (confirmed via `godump restore`:
    // the boundary is exactly `f64::MAX` — `1.7976931348623157e308`
    // parses, `1.8e308` doesn't).
    assert!(parse("select 1.7976931348623157e308").is_ok());
    assert!(parse("select 1.8e308").is_err());
    assert!(parse("select 1e400").is_err());
}

/// Go's `ast.NewHexLiteral` keeps quoted and numeric hexadecimal syntax
/// distinct after lexing: x'..'/X'..' requires byte pairs, lowercase 0x..
/// pads an odd digit count, and uppercase numeric 0X.. is invalid. The same
/// constructor owns bare and charset-introduced literals.
#[test]
fn quoted_hex_requires_byte_pairs_while_numeric_hex_pads() {
    assert_eq!(r("select 0xF, _utf8 0xF"), "SELECT x'0f',_UTF8 x'0f'");
    assert_eq!(r("select x'0F', X'0F'"), "SELECT x'0f',x'0f'");

    for sql in [
        "select x'F'",
        "select X'F'",
        "select _utf8 x'F'",
        "select _utf8 X'F'",
        "select 0XF",
        "select _utf8 0XF",
    ] {
        assert!(parse(sql).is_err(), "Go rejects {sql}");
    }
}

/// A decimal/integer literal whose digit count exceeds real MySQL/TiDB's
/// internal `MyDecimal` storage capacity (9 "words" of 9 digits each, 81
/// total, split between the integer and fraction parts, with the integer
/// part alone capped at 9 words) is silently clamped or replaced at LEX
/// time — read directly from `pkg/types/mydecimal.go`'s `FromString`/
/// `fixWordCntError` and `pkg/parser/lexer_helpers.go`'s `toDecimal`, not
/// guessed from restore text alone. See
/// `tidb_ast::util::clamp_decimal_magnitude`'s own doc for the full rule.
#[test]
fn decimal_magnitude_clamping() {
    // Integer part alone over 81 digits: the WHOLE literal is replaced with
    // the fixed constant `mysql.DefaultDecimal` (65 nines), regardless of
    // digit values.
    assert_eq!(
        r(&format!("select {}", "8".repeat(88))),
        format!("SELECT {}", "9".repeat(65))
    );
    // Exactly 81 int digits fits in the 9-word budget untouched.
    assert_eq!(
        r(&format!("select {}", "1".repeat(81))),
        format!("SELECT {}", "1".repeat(81))
    );
    // 82 int digits overflows.
    assert_eq!(
        r(&format!("select {}", "1".repeat(82))),
        format!("SELECT {}", "9".repeat(65))
    );
    // Fraction-only: 1 int digit (the leading `0`) leaves an 8-word (72
    // digit) fraction budget; excess fraction digits are TRUNCATED (no
    // rounding), not replaced.
    assert_eq!(
        r(&format!("select 0.{}", "0".repeat(89))),
        format!("SELECT 0.{}", "0".repeat(72))
    );
    assert_eq!(
        r("select 0.0000000000000000000000000000000000000000000000000000000000000000000000012"),
        "SELECT 0.000000000000000000000000000000000000000000000000000000000000000000000001"
    );
    // Exactly 72 fraction digits fits untouched.
    assert_eq!(
        r(&format!("select 0.{}", "1".repeat(72))),
        format!("SELECT 0.{}", "1".repeat(72))
    );
    // An 81-digit integer part leaves ZERO fraction-word budget — any
    // fraction (and its `.`) is dropped entirely, not just truncated to 0
    // digits with a dangling dot.
    assert_eq!(
        r(&format!("select {}.5", "1".repeat(81))),
        format!("SELECT {}", "1".repeat(81))
    );
}

#[test]
fn aggregates() {
    assert_eq!(
        r("select count(*), count(a), count(distinct a) from t"),
        "SELECT COUNT(1),COUNT(`a`),COUNT(DISTINCT `a`) FROM `t`"
    );
    assert_eq!(
        r("select sum(a), avg(b), max(c), min(d) from t"),
        "SELECT SUM(`a`),AVG(`b`),MAX(`c`),MIN(`d`) FROM `t`"
    );
    // STD/VARIANCE synonyms fold to canonical names.
    assert_eq!(
        r("select std(a), variance(b) from t"),
        "SELECT STDDEV_POP(`a`),VAR_POP(`b`) FROM `t`"
    );
    assert_eq!(
        r("select count(*) + 1 from t"),
        "SELECT COUNT(1)+1 FROM `t`"
    );
}

/// `COUNT(DISTINCT a, b, ...)` — the ONE multi-argument shape real
/// MySQL/TiDB grammar allows among the aggregates this crate models (read
/// directly from `pkg/parser/expr_func_parser.go`'s
/// `parseAggregateFuncCall`, not guessed): `COUNT` requires `DISTINCT` for
/// more than one argument, every other aggregate here (`SUM`/`AVG`/`MAX`/
/// `MIN`/...) rejects a comma unconditionally, matching plain `COUNT(a,
/// b)` without `DISTINCT` too — same generic syntax error either way,
/// confirmed via `godump restore` returning `!ERR` for both.
#[test]
fn count_distinct_multi_arg() {
    assert_eq!(
        r("select count(distinct a, b) from t"),
        "SELECT COUNT(DISTINCT `a`, `b`) FROM `t`"
    );
    assert_eq!(
        r("select count(distinct a, b, c) from t"),
        "SELECT COUNT(DISTINCT `a`, `b`, `c`) FROM `t`"
    );
    assert_eq!(
        r("select count(distinct c), count(distinct a, b) from t"),
        "SELECT COUNT(DISTINCT `c`),COUNT(DISTINCT `a`, `b`) FROM `t`"
    );
    assert!(parse("select count(a, b) from t").is_err());
    assert!(parse("select sum(distinct a, b) from t").is_err());
    assert!(parse("select max(distinct a, b) from t").is_err());
    assert!(parse("select count(distinct *) from t").is_err());
}

/// `ROW(expr, expr, ...)` — an explicit or implicit (bare `(expr, expr,
/// ...)`, 2+ elements) row/tuple constructor, both restoring identically
/// as `ROW(...)` (commas with NO trailing space, unlike every other
/// comma-separated list in this crate — a real MySQL restore quirk, read
/// from `pkg/parser/ast/expressions.go`'s `RowExpr.Restore`, not a bug).
/// See `tidb_ast::Expr::Row`'s own doc.
#[test]
fn row_constructor() {
    assert_eq!(r("select row(1,2,3) from t"), "SELECT ROW(1,2,3) FROM `t`");
    // Bare `(1,2,3)` builds the SAME node as `ROW(1,2,3)`.
    assert_eq!(r("select (1,2,3) from t"), "SELECT ROW(1,2,3) FROM `t`");
    // A single element stays a plain parenthesized expression, NOT a Row.
    assert_eq!(r("select (1) from t"), "SELECT (1) FROM `t`");
    // Nesting: the outer parens wrap a single Row expr.
    assert_eq!(r("select ((1,2)) from t"), "SELECT (ROW(1,2)) FROM `t`");
    // Row-wise comparison and tuple membership.
    assert_eq!(
        r("select row(1,2,3) > (3,2,1) from t"),
        "SELECT ROW(1,2,3)>ROW(3,2,1) FROM `t`"
    );
    assert_eq!(
        r("select row(1,2) in (row(1,2),(3,4)) from t"),
        "SELECT ROW(1,2) IN (ROW(1,2),ROW(3,4)) FROM `t`"
    );
    // `ROW(1)`/`ROW()` both require at least 2 elements.
    assert!(parse("select row(1) from t").is_err());
    assert!(parse("select row() from t").is_err());
    // INSERT's own VALUES list is a separate grammar production, unaffected.
    assert_eq!(
        r("insert into t values (1,2,3)"),
        "INSERT INTO `t` VALUES (1,2,3)"
    );
}

/// `INTERVAL(N, N1, N2, ...)` (immediately followed by `(`) is a totally
/// unrelated generic scalar function (an index-lookup among a sorted
/// numeric list) — NOT `INTERVAL value unit`'s own date-arithmetic
/// prefix-expression grammar (see `parse_prefix`'s own `"INTERVAL"` arm
/// doc). A REGRESSION TEST: adding [`Expr::Row`]'s bare-paren-comma-list
/// grammar initially broke this — `parse_interval`'s own `value =
/// self.parse_expr(prec::NONE)` started silently absorbing
/// `INTERVAL(...)`'s ENTIRE argument list (including the closing paren)
/// into a single `Row` value, then misinterpreting the outer `SELECT`'s
/// own `FROM` keyword as the interval's time unit — a genuine parse
/// derailment, caught by the coverage harness as a restore mismatch
/// before this fix, never shipped.
#[test]
fn interval_function_vs_date_arithmetic() {
    assert_eq!(
        r("select interval(23, 1, 15, 17, 30, 44, 200) from t"),
        "SELECT INTERVAL(23, 1, 15, 17, 30, 44, 200) FROM `t`"
    );
    assert_eq!(
        r("select interval(1, 0, 1, 2) from t"),
        "SELECT INTERVAL(1, 0, 1, 2) FROM `t`"
    );
    // The date-arithmetic form (`INTERVAL` NOT immediately followed by
    // `(`) is unaffected, on either side of `+`.
    assert_eq!(
        r("select date_add('2020-01-01', interval 1 day) from t"),
        "SELECT DATE_ADD(_UTF8MB4'2020-01-01', INTERVAL 1 DAY) FROM `t`"
    );
    assert_eq!(
        r("select interval 1 day + '2020-01-01' from t"),
        "SELECT DATE_ADD(_UTF8MB4'2020-01-01', INTERVAL 1 DAY) FROM `t`"
    );
}

/// `COLLATION`/`WEIGHT_STRING`/`APPROX_COUNT_DISTINCT` are lexer keywords
/// (matching real TiDB) that otherwise hit `parse_prefix`'s generic
/// "unsupported keyword in expression" catch-all — found via a fresh
/// stratified sample of the coverage-measurement's own unhandled bucket.
/// `COLLATION`/`APPROX_COUNT_DISTINCT` needed no new parsing logic at
/// all, just adding each name to the existing `is_scalar_kw_func`/
/// `agg_canonical` keyword-to-function-shape dispatch tables the generic
/// `parse_named_func`/`parse_aggregate` already handle every other name
/// through. `WEIGHT_STRING` later gained its OWN dedicated grammar (see
/// `Parser::parse_weight_string`'s own doc) for its `AS {CHAR|BINARY}(N)`
/// clause, which the generic comma-arg shape can't produce.
#[test]
fn misc_keyword_functions() {
    assert_eq!(
        r("select collation(a) from t"),
        "SELECT COLLATION(`a`) FROM `t`"
    );
    assert_eq!(
        r("select weight_string(a) from t"),
        "SELECT WEIGHT_STRING(`a`) FROM `t`"
    );
    // `APPROX_COUNT_DISTINCT` restores with a `DISTINCT` modifier like
    // every other aggregate here (confirmed via `godump restore`),
    // unlike `COLLATION`/`WEIGHT_STRING`'s own plain scalar-call shape.
    assert_eq!(
        r("select approx_count_distinct(a) from t"),
        "SELECT APPROX_COUNT_DISTINCT(`a`) FROM `t`"
    );
    assert_eq!(
        r("select approx_count_distinct(distinct a) from t"),
        "SELECT APPROX_COUNT_DISTINCT(DISTINCT `a`) FROM `t`"
    );
    // `WEIGHT_STRING(str AS {CHAR|BINARY}(N))` — the extended form — now
    // has its own dedicated grammar; see `weight_string`'s own test below
    // for the full coverage (this assertion used to expect a
    // `ParseError` here, a deliberate, narrower scope boundary from when
    // `WEIGHT_STRING` only had the generic comma-arg shape — now stale,
    // updated in the same pass that implemented the extended form).
    assert_eq!(
        r("select weight_string(a as char(5)) from t"),
        "SELECT WEIGHT_STRING(`a` AS CHAR(5)) FROM `t`"
    );
}

/// `DEFAULT(col)` — a column's own `DEFAULT` value, modelled as a plain
/// `Expr::Func` call over a single `Expr::Column` argument (see
/// `Parser::parse_default_expr`'s own doc for why no dedicated AST node
/// is needed despite real TiDB using one internally). `col` is a DOTTED
/// COLUMN-NAME PATH specifically, not an arbitrary expression. Every
/// assertion here was cross-checked against real TiDB via `godump
/// restore` (not assumed).
#[test]
fn default_expr() {
    assert_eq!(
        r("select default(x) from t1"),
        "SELECT DEFAULT(`x`) FROM `t1`"
    );
    assert_eq!(
        r("select default(t1.a) from t1"),
        "SELECT DEFAULT(`t1`.`a`) FROM `t1`"
    );
    assert_eq!(
        r("select a from t8 order by default(b) * a"),
        "SELECT `a` FROM `t8` ORDER BY DEFAULT(`b`)*`a`"
    );
    // The argument must be a column path, not an arbitrary expression —
    // a genuine `ParseError` otherwise, confirmed via `godump restore`.
    assert!(parse("select default(1+1) from t1").is_err());
    assert!(parse("select default() from t1").is_err());
    assert!(parse("select default(a,b) from t1").is_err());
    // A bare `DEFAULT` (no parens) is ALSO a genuine `ParseError` in
    // general expression context — real TiDB only allows it in
    // `VALUES`/`SET`-assignment positions.
    assert!(parse("select default from t1").is_err());
}

/// `CHAR(expr, ...)` / `CHAR(expr, ... USING charset)` — see
/// `tidb_ast::Expr::RawString`'s own doc for why the trailing
/// charset-name argument needs a dedicated no-introducer string variant
/// rather than reusing `Expr::String` directly.
#[test]
fn char_func() {
    assert_eq!(
        r("select char(97, 100, 256, 89)"),
        "SELECT CHAR_FUNC(97, 100, 256, 89, NULL)"
    );
    assert_eq!(
        r("select char(97, null, 100 using ascii)"),
        "SELECT CHAR_FUNC(97, NULL, 100, 'ascii')"
    );
    assert_eq!(
        r("select char(0x1234 using gb18030)"),
        "SELECT CHAR_FUNC(x'1234', 'gb18030')"
    );
    assert_eq!(
        r("select char(a using gbk), char(a) from t"),
        "SELECT CHAR_FUNC(`a`, 'gbk'),CHAR_FUNC(`a`, NULL) FROM `t`"
    );
    // Usable as a boolean predicate, not just a projected expression.
    assert_eq!(
        r("select c0 from t0 where char(204355900)"),
        "SELECT `c0` FROM `t0` WHERE CHAR_FUNC(204355900, NULL)"
    );
    // `CHAR()` with ZERO arguments is a real, EARLIER short-circuit in
    // real TiDB's own `parseScalarFuncCall` — the name stays `CHAR`
    // (not renamed to `CHAR_FUNC`) and no `NULL` sentinel is appended.
    assert_eq!(r("select char()"), "SELECT CHAR()");
    // The desugared name itself, typed directly by a user, is an
    // ordinary function call with no extra sentinel appended.
    assert_eq!(r("select char_func(97, 100)"), "SELECT CHAR_FUNC(97, 100)");
}

/// A RESERVED keyword immediately followed by `(` is a function call —
/// found via reading `pkg/parser/expr_prefix_parser.go`'s
/// `parsePrefixKeywordExpr` own final fallback directly: real TiDB's own
/// rule is far more general than a per-keyword allowlist (`REPEAT`/
/// `REPLACE` are real MySQL string functions that happen to share a name
/// with a reserved keyword, and plainly parse in real TiDB despite never
/// having been individually recognized as function names here before).
/// `INSERT`/`MOD` need their own dispatch arms ahead of the general
/// fallback since their desugared shape differs from a plain
/// `Expr::Func` call (see those arms' own doc comments).
#[test]
fn reserved_keyword_func_call() {
    assert_eq!(
        r("select repeat('ab', 3)"),
        "SELECT REPEAT(_UTF8MB4'ab', 3)"
    );
    assert_eq!(
        r("select replace('abc', 'a', 'x')"),
        "SELECT REPLACE(_UTF8MB4'abc', _UTF8MB4'a', _UTF8MB4'x')"
    );
    // `INSERT(...)` desugars to `INSERT_FUNC`, real TiDB's own renamed
    // AST function name (confirmed via `godump restore`) — distinct from
    // `REPEAT`/`REPLACE`, which keep their own name unchanged.
    assert_eq!(
        r("select insert('abc', 1, 2, 'X')"),
        "SELECT INSERT_FUNC(_UTF8MB4'abc', 1, 2, _UTF8MB4'X')"
    );
    // `MOD(a, b)` desugars to the `%` binary operator, not a function
    // call at all (confirmed via `godump restore`) — the ONE exception
    // the general reserved-keyword fallback must NOT catch as a plain
    // `Expr::Func`.
    assert_eq!(r("select mod(10, 3)"), "SELECT 10%3");
    assert_eq!(
        r("select a from t where mod(a, 5) < 2"),
        "SELECT `a` FROM `t` WHERE `a`%5<2"
    );
    assert!(parse("select mod(1, 2, 3)").is_err());
    assert!(parse("select mod(1)").is_err());
    // The small set of clause-introducing keywords must NEVER be
    // consumed as a function name, even followed by `(` — a genuine
    // `ParseError` here too, matching real TiDB (confirmed via `godump
    // restore`: `SELECT LOCK()`/`SELECT FROM()` are both `!ERR`).
    assert!(parse("select lock()").is_err());
    assert!(parse("select from()").is_err());
}

#[test]
fn group_concat() {
    // SEPARATOR always restores explicitly, even when not written.
    assert_eq!(
        r("select group_concat(a) from t"),
        "SELECT GROUP_CONCAT(`a` SEPARATOR ',') FROM `t`"
    );
    assert_eq!(
        r("select group_concat(distinct a) from t"),
        "SELECT GROUP_CONCAT(DISTINCT `a` SEPARATOR ',') FROM `t`"
    );
    assert_eq!(
        r("select group_concat(a separator '-') from t"),
        "SELECT GROUP_CONCAT(`a` SEPARATOR '-') FROM `t`"
    );
    // Multiple arguments, comma-space separated (like a plain function
    // call), distinct from the SEPARATOR-between-rows clause.
    assert_eq!(
        r("select group_concat(a, b) from t"),
        "SELECT GROUP_CONCAT(`a`, `b` SEPARATOR ',') FROM `t`"
    );
    let stmt = parse("select group_concat(a, b separator '-') from t").unwrap();
    let Stmt::Query(query) = stmt else {
        panic!("expected Query envelope")
    };
    let tidb_ast::QueryStmt::Select(s) = query.into_inner() else {
        panic!("expected SELECT query")
    };
    let SelectField::Expr { expr, .. } = &s.fields[0] else {
        panic!("expected expr field")
    };
    assert_eq!(
        expr,
        &Expr::GroupConcat {
            distinct: false,
            args: vec![
                Expr::Column(vec!["a".to_string()]),
                Expr::Column(vec!["b".to_string()])
            ],
            order_by: vec![],
            separator: "-".to_string().into(),
        }
    );
}

/// Exact metadata cases from Go
/// `pkg/parser/parser_test.go:TestGroupConcatSeparatorCharsetCollation`.
#[test]
fn group_concat_separator_inherits_connection_charset_and_collation() {
    for (sql, charset, collation, expected_separator) in [
        ("select group_concat('x')", "latin1", "latin1_bin", ","),
        (
            "select group_concat('x' separator ';')",
            "latin1",
            "latin1_bin",
            ";",
        ),
        (
            "select group_concat('x')",
            tidb_mysql::DefaultCharset,
            tidb_mysql::DefaultCollationName,
            ",",
        ),
    ] {
        let Stmt::Query(query) = parse_with_connection(sql, charset, collation).unwrap() else {
            panic!("expected query")
        };
        let tidb_ast::QueryStmt::Select(select) = query.into_inner() else {
            panic!("expected select")
        };
        let SelectField::Expr {
            expr: Expr::GroupConcat { separator, .. },
            ..
        } = &select.fields[0]
        else {
            panic!("expected GROUP_CONCAT")
        };
        assert_eq!(separator.value, expected_separator);
        assert_eq!(separator.charset, charset);
        assert_eq!(separator.collation, collation);
    }
}

/// `PIPES_AS_CONCAT` turns `||` into a `CONCAT()` CALL at `precConcat`
/// (`pkg/parser/lexer.go:248` keeps Go's `pipes` token, and
/// `pkg/parser/expr_parser.go:216` compiles it), rather than the `OR` the
/// same spelling means by default.
///
/// Every expectation is a verbatim `Restore` capture from `pkg/parser` with
/// `SetSQLMode(mysql.ModePipesAsConcat)`. The precedence rows are the point:
/// `precConcat = 14` sits ABOVE unary and every arithmetic level, and BELOW
/// `COLLATE` alone.
#[test]
fn pipes_as_concat_sql_mode_matches_go() {
    fn concat(sql: &str) -> String {
        parse_with_sql_mode(
            sql,
            SqlMode {
                pipes_as_concat: true,
                ..SqlMode::default()
            },
        )
        .unwrap()
        .restore()
    }

    assert_eq!(
        concat("SELECT 'a' || 'b'"),
        "SELECT CONCAT(_UTF8MB4'a', _UTF8MB4'b')"
    );
    // Left-associative, like every other infix level here.
    assert_eq!(
        concat("SELECT 1 || 2 || 3"),
        "SELECT CONCAT(CONCAT(1, 2), 3)"
    );
    // Binds tighter than `+`/`-` ...
    assert_eq!(concat("SELECT 1 + 2 || 3 + 4"), "SELECT 1+CONCAT(2, 3)+4");
    // ... and tighter than unary minus ...
    assert_eq!(concat("SELECT -1 || 2"), "SELECT -CONCAT(1, 2)");
    // ... and tighter than comparison ...
    assert_eq!(concat("SELECT 1 || 2 = 12"), "SELECT CONCAT(1, 2)=12");
    // ... but looser than COLLATE, the one level above it.
    assert_eq!(
        concat("SELECT a || b COLLATE utf8mb4_bin"),
        "SELECT CONCAT(`a`, `b` COLLATE utf8mb4_bin)"
    );
    // Unset, the same spelling is still boolean OR.
    assert_eq!(r("select 'a' || 'b'"), "SELECT _UTF8MB4'a' OR _UTF8MB4'b'");
}

/// Exact AST/restore contract from Go
/// `pkg/parser/parser_test.go:TestHighNotPrecedenceMode`.
#[test]
fn high_not_precedence_sql_mode_matches_go() {
    assert_eq!(
        parse("SELECT NOT 1 BETWEEN -5 AND 5").unwrap().restore(),
        "SELECT NOT 1 BETWEEN -5 AND 5"
    );
    assert_eq!(
        parse("SELECT !1 BETWEEN -5 AND 5").unwrap().restore(),
        "SELECT !1 BETWEEN -5 AND 5"
    );
    assert_eq!(
        parse_with_sql_mode(
            "SELECT NOT 1 BETWEEN -5 AND 5",
            SqlMode {
                high_not_precedence: true,
                ..SqlMode::default()
            },
        )
        .unwrap()
        .restore(),
        "SELECT !1 BETWEEN -5 AND 5"
    );
}

#[test]
fn case_when() {
    // Simple form (a compare value) and searched form (no compare
    // value, each WHEN is a standalone condition) share one AST node.
    assert_eq!(
        r("select case 1 when 1 then 'a' when 2 then 'b' else 'c' end"),
        "SELECT CASE 1 WHEN 1 THEN _UTF8MB4'a' WHEN 2 THEN _UTF8MB4'b' ELSE _UTF8MB4'c' END"
    );
    assert_eq!(
        r("select case when 1=1 then 10 else 20 end"),
        "SELECT CASE WHEN 1=1 THEN 10 ELSE 20 END"
    );
    // ELSE is optional.
    assert_eq!(
        r("select case 1 when 2 then 'x' end"),
        "SELECT CASE 1 WHEN 2 THEN _UTF8MB4'x' END"
    );
    // Nests, and composes with ordinary binary operators.
    assert_eq!(
            r("select case when 1=1 then case when 2=2 then 'nested' else 'no' end else 'outer' end"),
            "SELECT CASE WHEN 1=1 THEN CASE WHEN 2=2 THEN _UTF8MB4'nested' ELSE _UTF8MB4'no' END ELSE _UTF8MB4'outer' END"
        );
    assert_eq!(
        r("select 1 + case when 1=1 then 10 else 20 end"),
        "SELECT 1+CASE WHEN 1=1 THEN 10 ELSE 20 END"
    );
    // At least one WHEN clause is required -- confirmed via `godump
    // restore`: `CASE END`/`CASE 1 END` are genuine parse errors in
    // real MySQL, not assumed.
    assert!(parse("select case end").is_err());
    assert!(parse("select case 1 end").is_err());
}

/// `MATCH(col, ...) AGAINST(expr [search_modifier])` — full-text search.
/// Grammar/restore read directly from real TiDB's own hand-written parser
/// (`pkg/parser/expr_parser.go`'s `parseMatchAgainstExpr`) and AST restore
/// (`pkg/parser/ast/expressions.go`'s `MatchAgainst.Restore`), confirmed via
/// `godump restore`: the 4 written modifier spellings collapse to 3
/// distinct restore outputs (an explicit `IN NATURAL LANGUAGE MODE`
/// restores identically to no modifier at all).
#[test]
fn match_against() {
    assert_eq!(
        r("select match(a) against('x') from t"),
        "SELECT MATCH (`a`) AGAINST (_UTF8MB4'x') FROM `t`"
    );
    assert_eq!(
        r("select match(a,b) against('x') from t"),
        "SELECT MATCH (`a`,`b`) AGAINST (_UTF8MB4'x') FROM `t`"
    );
    assert_eq!(
        r("select match(a) against('x' in boolean mode) from t"),
        "SELECT MATCH (`a`) AGAINST (_UTF8MB4'x' IN BOOLEAN MODE) FROM `t`"
    );
    // `IN NATURAL LANGUAGE MODE` (the implicit default) restores identically
    // to no modifier at all.
    assert_eq!(
        r("select match(a) against('x' in natural language mode) from t"),
        "SELECT MATCH (`a`) AGAINST (_UTF8MB4'x') FROM `t`"
    );
    // `WITH QUERY EXPANSION`, with or without a preceding `IN NATURAL
    // LANGUAGE MODE`, restores identically.
    assert_eq!(
        r("select match(a) against('x' in natural language mode with query expansion) from t"),
        "SELECT MATCH (`a`) AGAINST (_UTF8MB4'x' WITH QUERY EXPANSION) FROM `t`"
    );
    assert_eq!(
        r("select match(a) against('x' with query expansion) from t"),
        "SELECT MATCH (`a`) AGAINST (_UTF8MB4'x' WITH QUERY EXPANSION) FROM `t`"
    );
    // Usable as a predicate (WHERE/HAVING) and as a scalar expression
    // (SELECT list/ORDER BY), and a qualified column name in the list.
    assert_eq!(
        r("select id from t where match(id) against('x')"),
        "SELECT `id` FROM `t` WHERE MATCH (`id`) AGAINST (_UTF8MB4'x')"
    );
    assert_eq!(
        r("select match(t.a) against('x') score from t order by score desc"),
        "SELECT MATCH (`t`.`a`) AGAINST (_UTF8MB4'x') AS `score` FROM `t` ORDER BY `score` DESC"
    );
    // `IN BOOLEAN MODE WITH QUERY EXPANSION` is a genuine `ParseError` in
    // real TiDB (confirmed by reading `parseMatchAgainstExpr`).
    assert!(
        parse("select match(a) against('x' in boolean mode with query expansion) from t").is_err()
    );
}

/// `col -> path` / `col ->> path` — JSON extraction, DESUGARS at parse
/// time to `JSON_EXTRACT(col, path)` / `JSON_UNQUOTE(JSON_EXTRACT(col,
/// path))` (real TiDB's own grammar has no dedicated AST node for this
/// operator — `pkg/parser/expr_parser.go`'s `case jss, juss:` builds a
/// plain `FuncCallExpr`), and `expr MEMBER OF (array)` — see
/// `tidb_ast::Expr::MemberOf`'s own doc. Every assertion here was
/// cross-checked against real TiDB via `godump restore` (not assumed).
#[test]
fn json_extract_and_member_of() {
    assert_eq!(
        r("select a->'$.a' from t"),
        "SELECT JSON_EXTRACT(`a`, _UTF8MB4'$.a') FROM `t`"
    );
    assert_eq!(
        r("select a->>'$.a' from t"),
        "SELECT JSON_UNQUOTE(JSON_EXTRACT(`a`, _UTF8MB4'$.a')) FROM `t`"
    );
    assert_eq!(
        r("select a->'$.a[2].aa' as x, a->>'$.b' as y from test_json"),
        "SELECT JSON_EXTRACT(`a`, _UTF8MB4'$.a[2].aa') AS `x`,JSON_UNQUOTE(JSON_EXTRACT(`a`, _UTF8MB4'$.b')) AS `y` FROM `test_json`"
    );
    // Composes normally with surrounding operators/predicates.
    assert_eq!(
        r("select not (a->'$.a' = 1)"),
        "SELECT NOT (JSON_EXTRACT(`a`, _UTF8MB4'$.a')=1)"
    );
    assert_eq!(
        r("select a->'$.a' + 1 from t"),
        "SELECT JSON_EXTRACT(`a`, _UTF8MB4'$.a')+1 FROM `t`"
    );
    // `->`/`->>` ONLY apply when the left operand is a BARE COLUMN
    // reference — a genuine `ParseError` otherwise, confirmed via
    // `godump restore`.
    assert!(parse("select (a+b)->'$.x' from t").is_err());
    assert!(parse("select 1->'$.x'").is_err());
    assert!(parse("select 'abc'->'$.x'").is_err());
    // The right side must be a plain string-literal token.
    assert!(parse("select a->(1+1) from t").is_err());

    assert_eq!(
        r("select 1 member of ('[1,2,3]')"),
        "SELECT 1 MEMBER OF (_UTF8MB4'[1,2,3]')"
    );
    assert_eq!(
        r("select '[4,5]' member of ('[[3,4],[4,5]]')"),
        "SELECT _UTF8MB4'[4,5]' MEMBER OF (_UTF8MB4'[[3,4],[4,5]]')"
    );
    // Composes with `->` inside the array argument.
    assert_eq!(
        r("select 1 member of (j0->'$.path0')"),
        "SELECT 1 MEMBER OF (JSON_EXTRACT(`j0`, _UTF8MB4'$.path0'))"
    );
    // Go accepts this zero-argument native-function shape but its own
    // `FuncCallExpr.Restore` deliberately returns an error for MEMBER OF,
    // which remains the single pinned restore-failure row in the integration
    // oracle. Rust keeps the parse boundary explicit instead of treating it
    // as an untracked parser gap.
    assert_eq!(r("select json_memberof()"), "SELECT JSON_MEMBEROF()");
}

/// Real MySQL/TiDB distinguishes RESERVED keywords (never usable as a
/// bare, unquoted identifier) from a much larger set of NON-RESERVED
/// keywords (usable as a table/column/alias name whenever the grammar
/// isn't otherwise ambiguous there) — see `tidb_lexer::is_reserved`'s own
/// doc for how the reserved list was derived. Every assertion here was
/// cross-checked against real TiDB via `godump restore` (not assumed).
#[test]
fn nonreserved_keyword_as_identifier() {
    assert_eq!(r("select uuid from t"), "SELECT `uuid` FROM `t`");
    assert_eq!(r("select uuid() from t"), "SELECT UUID() FROM `t`");
    assert_eq!(
        r("select * from t where uuid = 1"),
        "SELECT * FROM `t` WHERE `uuid`=1"
    );
    // A qualified path's LATER segment may also be a non-reserved keyword.
    assert_eq!(r("select t.uuid from t"), "SELECT `t`.`uuid` FROM `t`");
    assert_eq!(
        r("select status, value, type from t"),
        "SELECT `status`,`value`,`type` FROM `t`"
    );
    assert_eq!(
        r("select a.status from t as a"),
        "SELECT `a`.`status` FROM `t` AS `a`"
    );
    // A genuinely RESERVED keyword still can never be a bare identifier.
    assert!(parse("select select from t").is_err());
}

/// `pkg/parser/parser_test.go::TestUUIDKeywordCompatibility`.
#[test]
fn test_uuid_keyword_compatibility() {
    for (sql, expected) in [
        ("SELECT uuid FROM t", "SELECT `uuid` FROM `t`"),
        (
            "SELECT uuid.uuid FROM uuid",
            "SELECT `uuid`.`uuid` FROM `uuid`",
        ),
        ("SELECT 1 AS uuid", "SELECT 1 AS `uuid`"),
        ("SELECT * FROM t AS uuid", "SELECT * FROM `t` AS `uuid`"),
        (
            "ALTER TABLE t ADD COLUMN uuid INT",
            "ALTER TABLE `t` ADD COLUMN `uuid` INT",
        ),
        (
            "CREATE TABLE t (uuid INT, KEY uuid (uuid))",
            "CREATE TABLE `t` (`uuid` INT,INDEX `uuid`(`uuid`))",
        ),
    ] {
        assert_eq!(r(sql), expected, "source SQL: {sql}");
    }
}

/// Pins the two-Go-list split behind reserved-keyword identifier
/// admission — see `tidb_lexer::reserved`'s own doc and
/// `rust/docs/parser-lexer-divergence.md` finding #4/#5. Go's
/// `expr_prefix_parser.go` gates a bare expression-position identifier
/// (including EVERY segment of a qualified `a.b.c` path) on the small
/// 13-word `isReservedClauseKeyword` ONLY — a fully RESERVED keyword like
/// `DATABASE`/`ROWS`/`DEC` is still a valid bare column reference there.
/// `select_parser.go`/`join_parser.go`'s explicit `AS name` alias forms
/// gate on the much larger `IsReserved` (236 spellings once
/// `pkg/parser/misc.go`'s `tokenMap` many-to-one collisions are fanned
/// out) instead. Every assertion here was cross-checked against real
/// TiDB via a direct `pkg/parser.New().ParseSQL` probe on this branch
/// (not assumed), and each Go-only spelling collision
/// (`DATABASE`/`SCHEMA`, `DATABASES`/`SCHEMAS`, `DISTINCT`/
/// `DISTINCTROW`, `DECIMAL`/`DEC`) is exercised by its NEWLY-reserved
/// spelling specifically, since that's the one a naive tokenMap
/// inversion drops.
#[test]
fn reserved_keyword_two_list_split() {
    // Bare expression-position identifier: only the 13 clause keywords
    // are refused, so DATABASE/DISTINCT/DEC (all fully RESERVED) are
    // still admitted here — this is the exact query that broke when
    // DATABASE was first added to the single (then-unsplit) reserved
    // list: `SHOW DATABASES WHERE Database LIKE ...` is real,
    // TiDB-documented SQL.
    assert_eq!(
        r("show databases where Database like 'test_%'"),
        "SHOW DATABASES WHERE `Database` LIKE _UTF8MB4'test_%'"
    );
    assert_eq!(r("select database from t"), "SELECT `database` FROM `t`");
    assert_eq!(r("select dec from t"), "SELECT `dec` FROM `t`");
    // Every segment of a qualified path, not just the first.
    assert_eq!(
        r("select database.table.column"),
        "SELECT `database`.`table`.`column`"
    );

    // Explicit `AS name` alias: the FULL 236-entry `IsReserved` gate
    // applies, so all four newly-added spellings are refused here even
    // though they're fine as bare column references above.
    assert!(parse("select 1 as database").is_err());
    assert!(parse("select 1 as distinct").is_err());
    assert!(parse("select 1 as dec").is_err());
    assert!(parse("select database from x as database").is_err());
    // Non-reserved keywords stay valid explicit aliases (mutation-probe
    // guard: a fix that over-widens the `AS name` refusal to ALL
    // keywords, not just reserved ones, fails here).
    assert_eq!(r("select 1 as sum"), "SELECT 1 AS `sum`");
    assert_eq!(r("select 1 as json"), "SELECT 1 AS `json`");

    // CTE name: same `IsReserved` gate as the explicit alias form.
    assert!(parse("with database as (select 1) select * from database").is_err());
    assert!(parse("with distinct as (select 1) select * from distinct").is_err());

    // Backtick-quoting always works, both gates, matching Go exactly.
    assert_eq!(r("select `database` from t"), "SELECT `database` FROM `t`");
    assert_eq!(r("select `distinct` from t"), "SELECT `distinct` FROM `t`");
}

/// `BINARY expr` — a bare prefix operator, the SAME operation as
/// `CAST(expr AS BINARY)` under a third concrete syntax — see
/// `tidb_ast::CastStyle::BinaryOperator`'s own doc. Every assertion here
/// was cross-checked against real TiDB via `godump restore` (not
/// assumed).
#[test]
fn binary_operator() {
    assert_eq!(r("select binary a from t"), "SELECT BINARY `a` FROM `t`");
    // Binds at the SAME tight precedence as unary `-`/`~`/`!`.
    assert_eq!(r("select binary -a from t"), "SELECT BINARY -`a` FROM `t`");
    assert_eq!(
        r("select binary a = binary b from t"),
        "SELECT BINARY `a`=BINARY `b` FROM `t`"
    );
    assert_eq!(
        r("select 'a' regexp binary 'A'"),
        "SELECT _UTF8MB4'a' REGEXP BINARY _UTF8MB4'A'"
    );
    // `CAST(expr AS BINARY)` — the SAME `Expr::Cast` node, a different
    // concrete syntax — keeps its own distinct restore form, unaffected.
    assert_eq!(
        r("select cast(a as binary) from t"),
        "SELECT CAST(`a` AS BINARY) FROM `t`"
    );
}

/// `TRIM([{BOTH|LEADING|TRAILING} [remstr] FROM] str)` / `TRIM(str)` /
/// `TRIM(remstr FROM str)` — see `tidb_ast::Expr::Trim`'s own doc for the
/// full grammar and restore quirks. Every assertion here was cross-checked
/// against real TiDB via `godump restore` (not assumed).
#[test]
fn trim_func() {
    // Simple form: no FROM at all.
    assert_eq!(
        r("select trim('  bar  ')"),
        "SELECT TRIM(_UTF8MB4'  bar  ')"
    );
    // `remstr FROM str`, no direction keyword.
    assert_eq!(
        r("select trim('x' from 'xxxbarxxx')"),
        "SELECT TRIM(_UTF8MB4'x' FROM _UTF8MB4'xxxbarxxx')"
    );
    // `direction remstr FROM str`, all three directions.
    assert_eq!(
        r("select trim(leading 'x' from 'xxxbarxxx')"),
        "SELECT TRIM(LEADING _UTF8MB4'x' FROM _UTF8MB4'xxxbarxxx')"
    );
    assert_eq!(
        r("select trim(trailing 'xyz' from 'barxxyz')"),
        "SELECT TRIM(TRAILING _UTF8MB4'xyz' FROM _UTF8MB4'barxxyz')"
    );
    assert_eq!(
        r("select trim(both 'x' from 'xxxbarxxx')"),
        "SELECT TRIM(BOTH _UTF8MB4'x' FROM _UTF8MB4'xxxbarxxx')"
    );
    // `direction FROM str`, no remstr expression given: the parser
    // DEFAULTS remstr to a single-space literal (confirmed via
    // `godump restore` — this is a genuine TiDB/MySQL AST quirk, not an
    // omission).
    assert_eq!(
        r("select trim(leading from '   bar')"),
        "SELECT TRIM(LEADING _UTF8MB4' ' FROM _UTF8MB4'   bar')"
    );
    // An explicit `NULL` remstr, by contrast, is OMITTED from restore
    // (checked by value, not by whether the source wrote anything) while
    // the `FROM` keyword itself is preserved.
    assert_eq!(
        r("select trim(null from 'bar')"),
        "SELECT TRIM(FROM _UTF8MB4'bar')"
    );
    assert_eq!(
        r("select trim(leading null from 'bar')"),
        "SELECT TRIM(LEADING FROM _UTF8MB4'bar')"
    );
    assert_eq!(r("select trim(null)"), "SELECT TRIM(NULL)");
    // Composes with COLLATE on both operands.
    assert_eq!(
        r("select trim(both 'abc' collate utf8mb4_bin from 'c' collate utf8mb4_general_ci)"),
        "SELECT TRIM(BOTH _UTF8MB4'abc' COLLATE utf8mb4_bin FROM _UTF8MB4'c' COLLATE utf8mb4_general_ci)"
    );
    // A comma-separated argument list — MySQL/TiDB's OTHER `TRIM(str,
    // remstr)` shape — is NOT supported; real TiDB rejects it too (its
    // grammar only knows the `FROM`-based forms).
    assert!(parse("select trim('a', 'b')").is_err());
}

/// `{d 'literal'}` / `{t 'literal'}` / `{ts 'literal'}` — an ODBC
/// escape-sequence literal, restoring byte-identically to the plain
/// `DATE`/`TIME`/`TIMESTAMP 'literal'` keyword form (see
/// `tidb_ast::CastStyle::DateLiteral`'s own doc). Every assertion here
/// was cross-checked against real TiDB via `godump restore`.
#[test]
fn odbc_escape_literal() {
    assert_eq!(
        r("select { d '2024-01-01 01:12:31' }"),
        "SELECT DATE '2024-01-01 01:12:31'"
    );
    assert_eq!(r("select { d '2024-01-01' }"), "SELECT DATE '2024-01-01'");
    assert_eq!(r("select { t '14:00:00' }"), "SELECT TIME '14:00:00'");
    assert_eq!(
        r("select { ts '2024-01-01 14:00:00+00:00' }"),
        "SELECT TIMESTAMP '2024-01-01 14:00:00+00:00'"
    );
    // The type identifier is matched by TEXT, case-insensitively, not by
    // token kind.
    assert_eq!(r("select {D '2024-01-01'}"), "SELECT DATE '2024-01-01'");
    // Any OTHER type identifier (`fn`, or a full word like `date`/`time`/
    // `timestamp`) is a pass-through: the braces (and the identifier
    // itself) are discarded, leaving only the inner expression — real
    // TiDB's own `default:` arm.
    assert_eq!(
        r("select {fn concat('a','b')}"),
        "SELECT CONCAT(_UTF8MB4'a', _UTF8MB4'b')"
    );
    assert_eq!(
        r("select {date '2024-01-01'}"),
        "SELECT _UTF8MB4'2024-01-01'"
    );
    assert_eq!(r("select {d 1+1}"), "SELECT DATE 1+1");
    assert_eq!(r("select {t a}"), "SELECT TIME `a`");
    assert_eq!(r("select {ts abs(-1)}"), "SELECT TIMESTAMP ABS(-1)");
}

/// Adjacent bare string-literal tokens concatenate into ONE value at parse
/// time, no operator between them (`'a' 'b'` -> `'ab'`) — see
/// `tidb_parser::Parser::parse_prefix`'s own `TokenKind::Str` arm doc.
/// Every assertion here was cross-checked against real TiDB via `godump
/// restore`.
#[test]
fn adjacent_string_concat() {
    assert_eq!(r(r#"select "ss" "a" "b""#), "SELECT _UTF8MB4'ssab'");
    assert_eq!(
        r("select 'string' 'string'"),
        "SELECT _UTF8MB4'stringstring'"
    );
    // Mixed quote styles concatenate the same way, and each literal
    // contributes its OWN characters verbatim — a literal space token
    // contributes a space, not a separator.
    assert_eq!(
        r(r#"select "ss" "a" ' ' "b" ' ' "d""#),
        "SELECT _UTF8MB4'ssa b d'"
    );
    // Applies at every position a bare literal can appear: multiple
    // select-list items, and composes normally with a following clause.
    assert_eq!(
        r("select 'a' 'b', 'c' 'd' from t"),
        "SELECT _UTF8MB4'ab',_UTF8MB4'cd' FROM `t`"
    );
}

/// `POSITION(substr IN str)` — see `tidb_ast::Expr::Position`'s own doc.
/// Every assertion here was cross-checked against real TiDB via `godump
/// restore`.
#[test]
fn position_func() {
    assert_eq!(
        r("select position('a' in 'AA')"),
        "SELECT POSITION(_UTF8MB4'a' IN _UTF8MB4'AA')"
    );
    assert_eq!(
        r("select hex(position(a in 0xe4b880)) from t"),
        "SELECT HEX(POSITION(`a` IN x'e4b880')) FROM `t`"
    );
    // `POSITION` alone (no parens) is a bare, non-reserved-keyword column
    // reference, not a function call.
    assert_eq!(r("select position from t"), "SELECT `position` FROM `t`");
}

/// `SUBSTR`/`SUBSTRING(str, pos[, len])` / `SUBSTR`/`SUBSTRING(str FROM
/// pos [FOR len])` — see `Parser::parse_substring_func`'s own doc. Every
/// assertion here was cross-checked against real TiDB via `godump
/// restore`.
#[test]
fn substring_from_for() {
    assert_eq!(
        r("select substr('foobarbar' from 4), substr('Sakila' from -4 for 2)"),
        "SELECT SUBSTR(_UTF8MB4'foobarbar', 4),SUBSTR(_UTF8MB4'Sakila', -4, 2)"
    );
    // The plain comma form still works too, for both keyword spellings.
    assert_eq!(
        r("select substr('foo', 1, 2)"),
        "SELECT SUBSTR(_UTF8MB4'foo', 1, 2)"
    );
    assert_eq!(
        r("select substring('foo' from 1 for 2)"),
        "SELECT SUBSTRING(_UTF8MB4'foo', 1, 2)"
    );
    // Bare identifier usage (no parens) is unaffected.
    assert_eq!(r("select substr from t"), "SELECT `substr` FROM `t`");
    assert_eq!(r("select substring from t"), "SELECT `substring` FROM `t`");
    // Mixing separators (`FROM ... ,` or `, ... FOR`) is a genuine
    // `ParseError` in real TiDB (confirmed via `godump restore`) — its
    // own `parseSubstringFunc` picks the length separator from whichever
    // one was used for `pos`, never re-detecting it.
    assert!(parse("select substr('foo' from 1, 2)").is_err());
    assert!(parse("select substr('foo', 1 for 2)").is_err());
}

/// `TRIM`/`CHAR`/`JSON_SUM_CRC32` are builtin-function-name keywords with
/// a bare, no-parens identifier meaning TOO (like `POSITION`/`SUBSTR`/
/// `SUBSTRING`) — regression test for a genuine, pre-existing bug: their
/// own dispatch arms called `parse_trim`/`parse_char_func`/
/// `parse_json_sum_crc32` unconditionally, missing the immediately-
/// following-`(` guard, so a bare column reference incorrectly failed to
/// parse. Every assertion here was cross-checked against real TiDB via
/// `godump restore`.
#[test]
fn bare_keyword_identifier() {
    assert_eq!(
        r("select char, trim, json_sum_crc32 from t"),
        "SELECT `char`,`trim`,`json_sum_crc32` FROM `t`"
    );
    // The parenthesized function-call forms are unaffected.
    assert_eq!(
        r("select char(65,66,67)"),
        "SELECT CHAR_FUNC(65, 66, 67, NULL)"
    );
    assert_eq!(
        r("select trim('  bar  ')"),
        "SELECT TRIM(_UTF8MB4'  bar  ')"
    );
    assert_eq!(
        r("select json_sum_crc32(1 as signed array)"),
        "SELECT JSON_SUM_CRC32(1 AS SIGNED ARRAY)"
    );
}

/// `EXTRACT`/`CAST`/`CONVERT`/`TIMESTAMPADD`/`TIMESTAMPDIFF`/
/// `GET_FORMAT`/`ADDDATE`/`SUBDATE` — the SAME missing-bare-identifier-
/// guard bug class as `bare_keyword_identifier` above, found by auditing
/// the rest of real TiDB's own builtin-function-name keywords. Every
/// assertion here was cross-checked against real TiDB via `godump
/// restore`.
#[test]
fn bare_keyword_identifier_extract_cast() {
    assert_eq!(
        r("select cast, extract, convert, timestampadd, timestampdiff, get_format, adddate, subdate from t"),
        "SELECT `cast`,`extract`,`convert`,`timestampadd`,`timestampdiff`,`get_format`,`adddate`,`subdate` FROM `t`"
    );
    // The parenthesized function-call forms are unaffected.
    assert_eq!(r("select cast(1 as signed)"), "SELECT CAST(1 AS SIGNED)");
    assert_eq!(
        r("select extract(year from '2020-01-01')"),
        "SELECT EXTRACT(YEAR FROM _UTF8MB4'2020-01-01')"
    );
    assert_eq!(
        r("select convert('123', signed)"),
        "SELECT CONVERT(_UTF8MB4'123', SIGNED)"
    );
    assert_eq!(
        r("select timestampadd(day, 1, '2020-01-01')"),
        "SELECT TIMESTAMPADD(DAY, 1, _UTF8MB4'2020-01-01')"
    );
    assert_eq!(
        r("select timestampdiff(day, '2020-01-01', '2020-01-02')"),
        "SELECT TIMESTAMPDIFF(DAY, _UTF8MB4'2020-01-01', _UTF8MB4'2020-01-02')"
    );
    assert_eq!(
        r("select get_format(date, 'usa')"),
        "SELECT GET_FORMAT(DATE, _UTF8MB4'usa')"
    );
    assert_eq!(
        r("select adddate('2020-01-01', interval 1 day)"),
        "SELECT ADDDATE(_UTF8MB4'2020-01-01', INTERVAL 1 DAY)"
    );
    assert_eq!(
        r("select subdate('2020-01-01', interval 1 day)"),
        "SELECT SUBDATE(_UTF8MB4'2020-01-01', INTERVAL 1 DAY)"
    );
    // `CASE`/`BINARY`/`MATCH`/`INTERVAL` are genuinely reserved,
    // expression-introducing keywords with NO bare-identifier fallback in
    // real TiDB either (confirmed via `godump restore`) — correctly
    // still `ParseError`s, unaffected by this fix.
    assert!(parse("select case from t").is_err());
    assert!(parse("select binary from t").is_err());
    assert!(parse("select match from t").is_err());
    assert!(parse("select interval from t").is_err());
}

/// `CONVERT(expr USING charset)` / `CHAR(... USING charset)` accept a
/// QUOTED STRING LITERAL charset name too, not just a bare identifier/
/// keyword — see `Parser::parse_using_charset_name`'s own doc. Every
/// assertion here was cross-checked against real TiDB via `godump
/// restore`.
#[test]
fn using_charset_string_literal() {
    assert_eq!(
        r(r#"select convert("123" using "binary")"#),
        "SELECT CONVERT(_UTF8MB4'123' USING 'binary')"
    );
    // Both forms restore identically.
    assert_eq!(
        r("select convert('123' using binary)"),
        "SELECT CONVERT(_UTF8MB4'123' USING 'binary')"
    );
    assert_eq!(
        r(r#"select char(65 using "gbk")"#),
        "SELECT CHAR_FUNC(65, 'gbk')"
    );
    assert_eq!(
        r("select char(65 using gbk)"),
        "SELECT CHAR_FUNC(65, 'gbk')"
    );
    // Go's `parseConvertFunc` rejects a name that is not in
    // `charset.GetCharsetInfo`; the generic token reader must not turn this
    // into an accepted conversion.
    assert!(parse(r#"select convert("123" using "866")"#).is_err());
}

/// `_charset'x'` / `N'x'` / `n'x'` — a character-set-introduced string
/// literal — see `tidb_ast::Expr::CharsetString`'s own doc for the exact
/// scope (an explicit `_utf8mb4'x'` reuses plain `Expr::String` instead,
/// since it restores identically). Every assertion here was cross-checked
/// against real TiDB via `godump restore`.
#[test]
fn charset_string_literal() {
    // The default charset's own explicit introducer reuses Expr::String.
    assert_eq!(r("select _utf8mb4'12345'"), "SELECT _UTF8MB4'12345'");
    // Non-default charsets get the dedicated CharsetString shape.
    assert_eq!(r("select _latin1'a'"), "SELECT _LATIN1'a'");
    assert_eq!(r("select _ascii'你'"), "SELECT _ASCII'你'");
    assert_eq!(r("select _binary'a'"), "SELECT _BINARY'a'");
    // `N'x'`/`n'x'` map to charset UTF8, NOT UTF8MB4 — a real, easy-to-miss
    // distinction.
    assert_eq!(r("select N'a'"), "SELECT _UTF8'a'");
    assert_eq!(r("select n'a'"), "SELECT _UTF8'a'");
    // Double-quoted body, and a space between the introducer and the
    // literal, both compose normally.
    assert_eq!(r(r#"select _utf8"string""#), "SELECT _UTF8'string'");
    assert_eq!(r("select _latin1 'a'"), "SELECT _LATIN1'a'");
    // `BINARY 'x'` (bare keyword, no underscore) stays the UNARY
    // cast-to-binary operator, genuinely distinct from `_binary'x'` —
    // confirmed via `godump restore` that these restore differently
    // (`BINARY _UTF8MB4'x'` vs `_BINARY'x'`), the disambiguation this
    // feature's own `TokenKind::CharsetIntroducer` split exists for.
    assert_eq!(r("select binary 'x'"), "SELECT BINARY _UTF8MB4'x'");
    // Composes with COLLATE normally.
    assert_eq!(
        r("select _latin1'a' collate latin1_bin"),
        "SELECT _LATIN1'a' COLLATE latin1_bin"
    );
    // Does NOT concatenate with an adjacent bare string literal (unlike
    // plain, un-introduced string tokens) — the trailing literal is an
    // implicit alias, as TiDB's shared `CanBeImplicitAlias` rule permits.
    assert_eq!(r("select _latin1'a' 'b'"), "SELECT _LATIN1'a' AS `b`");
    // A charset introducer with nothing following is a genuine
    // `ParseError`, matching real TiDB (the lexer recognizes it
    // unconditionally; only the grammar requires the following literal).
    assert!(parse("select _latin1 from t").is_err());
}

/// `pkg/parser/expr_parser.go::parseCharsetIntroducer` validates the lexer
/// token through `charset.GetDefaultCollationLegacy`; registered-but-legacy
/// names such as GBK and UJIS remain genuine unsupported-introducer errors.
#[test]
fn unsupported_charset_introducer_rows_reject_like_go() {
    for sql in ["select _gbk 'a'", "select _ujis 'a'"] {
        assert!(parse(sql).is_err(), "unexpectedly accepted {sql}");
    }
}

/// `pkg/parser/parser_test.go::TestUnderscoreCharset`.
#[test]
fn test_underscore_charset() {
    assert!(parse("select hex(_utf8 '3F')").is_ok());

    for charset in ["gbk", "ujis"] {
        let sql = format!("select hex(_{charset} '3F')");
        let error = parse(&sql).expect_err("registered legacy introducer is unsupported");
        assert_eq!(
            error.compatibility_message(&sql),
            format!("[parser:1115]Unsupported character introducer: '{charset}'"),
            "source SQL: {sql}"
        );
    }

    for (charset, expected) in [
        ("gbk1", "line 1 column 21 near \"'3F')\" "),
        ("ujisx", "line 1 column 22 near \"'3F')\" "),
    ] {
        let sql = format!("select hex(_{charset} '3F')");
        let error = parse(&sql).expect_err("unknown introducer must fail as grammar");
        assert_eq!(
            error.compatibility_message(&sql),
            expected,
            "source SQL: {sql}"
        );
    }
}

/// `WEIGHT_STRING(expr [AS {CHAR|CHARACTER|BINARY}(len)])` — see
/// `tidb_ast::Expr::WeightString`'s own doc. Every assertion here was
/// cross-checked against real TiDB via `godump restore`.
#[test]
fn weight_string() {
    // The plain, no-`AS` form.
    assert_eq!(
        r("select weight_string('ab')"),
        "SELECT WEIGHT_STRING(_UTF8MB4'ab')"
    );
    // `BINARY`/`CHAR`, and `CHARACTER` as a real synonym for `CHAR`.
    assert_eq!(
        r("select weight_string(a as binary(1)) from t"),
        "SELECT WEIGHT_STRING(`a` AS BINARY(1)) FROM `t`"
    );
    assert_eq!(
        r("select weight_string(a as char(3)) from t"),
        "SELECT WEIGHT_STRING(`a` AS CHAR(3)) FROM `t`"
    );
    assert_eq!(
        r("select weight_string(a as character(3)) from t"),
        "SELECT WEIGHT_STRING(`a` AS CHAR(3)) FROM `t`"
    );
    // Composes with a nested `CAST` argument.
    assert_eq!(
        r("select weight_string(cast(20190821 as date) as binary(5))"),
        "SELECT WEIGHT_STRING(CAST(20190821 AS DATE) AS BINARY(5))"
    );
    // `len` has no upper bound enforced at parse time — a huge value
    // restores back verbatim (confirmed via `godump restore`).
    assert_eq!(
        r("select weight_string('ab' as binary(1000000000000000000))"),
        "SELECT WEIGHT_STRING(_UTF8MB4'ab' AS BINARY(1000000000000000000))"
    );
    // Bare identifier usage (no parens) is unaffected.
    assert_eq!(
        r("select weight_string from t"),
        "SELECT `weight_string` FROM `t`"
    );
}

#[test]
fn backslash_n_null() {
    // `\N` (case-sensitive) is MySQL/TiDB shorthand for the NULL literal,
    // registered as an exact-string lexer token (`initTokenString("\N", null)`
    // in real TiDB's `pkg/parser/misc.go`) rather than parsed as an operator.
    // It desugars entirely to the pre-existing `Expr::Null`, so it restores
    // as plain `NULL` (cross-checked against real TiDB via `godump restore`).
    assert_eq!(r("select \\N"), "SELECT NULL");
    assert_eq!(r("select \\N from test"), "SELECT NULL FROM `test`");
    assert_eq!(r("select (\\N) from test"), "SELECT (NULL) FROM `test`");
    assert_eq!(r("select \\N, \\N"), "SELECT NULL,NULL");
    // Lowercase `\n` is NOT the shorthand: the backslash alone is an
    // unrecognized operator, so this is a genuine parse error.
    assert!(parse("select \\n").is_err());
}

/// A schema-qualified GENERIC function call, `schema.func(args...)` —
/// cross-checked against real TiDB via `godump restore`. See
/// `tidb_ast::Expr::GenericFuncCall`'s own doc for the restore
/// asymmetry (back-quoted, case-preserved, unlike a builtin call's own
/// canonical-uppercase restore).
#[test]
fn generic_qualified_func_call() {
    // The real-corpus cluster (case-preserving both sides).
    assert_eq!(r("SELECT T.upper(1)"), "SELECT `T`.`upper`(1)");
    assert_eq!(r("SELECT t.upper(1)"), "SELECT `t`.`upper`(1)");
    // Multiple args, and no args at all.
    assert_eq!(
        r("select t.foo(1, 2, 3) from t"),
        "SELECT `t`.`foo`(1, 2, 3) FROM `t`"
    );
    assert_eq!(r("select t.foo() from t"), "SELECT `t`.`foo`() FROM `t`");
    // Composes with an outer aggregate AND a `GROUP BY` referencing the
    // SAME expression — confirms the 9-traversal-function sweep
    // (`expr_has_aggregate` in particular) recurses into this node's
    // own `args`, not just skips over it.
    assert_eq!(
        r("select count(t.foo(1)) from t group by t.foo(1)"),
        "SELECT COUNT(`t`.`foo`(1)) FROM `t` GROUP BY `t`.`foo`(1)"
    );
    // A plain qualified COLUMN reference (no parens) is unaffected —
    // the 4-token lookahead requires the trailing `(` to fire at all.
    assert_eq!(r("select t.col from t"), "SELECT `t`.`col` FROM `t`");
    assert_eq!(
        r("select db.t.col from t"),
        "SELECT `db`.`t`.`col` FROM `t`"
    );
    // A plain, unqualified builtin call is ALSO unaffected — restores
    // uppercase, unquoted, the existing `Expr::Func` convention.
    assert_eq!(r("select upper(1)"), "SELECT UPPER(1)");
}

/// `CURRENT_ROLE` — a nullary function with a bare no-parens grammar
/// form, joining the existing `CURRENT_TIMESTAMP`/`CURRENT_DATE`/
/// `UTC_DATE`/... family (see that dispatch arm's own doc).
#[test]
fn current_role() {
    assert_eq!(r("select current_role"), "SELECT CURRENT_ROLE()");
    assert_eq!(r("select current_role()"), "SELECT CURRENT_ROLE()");
}

/// `NEXTVAL`/`LASTVAL`/`SETVAL(seq, value)` already parse and restore
/// correctly through the ordinary function-call path — no dedicated AST
/// shape needed at all, unlike real TiDB's own `TableNameExpr`-based
/// sequence-name argument (`pkg/parser/expr_func_parser.go`'s
/// `parseSequenceTableArg`): a plain (possibly schema-qualified)
/// `Expr::Column` restores identically, since both this crate's own
/// `restore_path` and real TiDB's own `TableNameExpr.Restore`
/// unconditionally back-quote every path segment (confirmed via `godump
/// restore`). `NEXT VALUE FOR seq_name` is SQL-standard sugar for
/// `NEXTVAL(seq_name)` — real TiDB desugars it at PARSE time too
/// (confirmed via `godump restore`: it restores byte-identical to
/// writing `NEXTVAL(seq_name)` directly) — implemented the same way
/// here. `NEXT`/`VALUE` are both non-reserved, so a bare `next`/`value`
/// used as an ordinary column name elsewhere is unaffected (confirmed:
/// the 3-token guard only fires for the EXACT `NEXT VALUE FOR` sequence).
#[test]
fn sequence_functions() {
    assert_eq!(r("select nextval(seq1)"), "SELECT NEXTVAL(`seq1`)");
    assert_eq!(
        r("select nextval(db1.seq1)"),
        "SELECT NEXTVAL(`db1`.`seq1`)"
    );
    assert_eq!(r("select lastval(seq1)"), "SELECT LASTVAL(`seq1`)");
    assert_eq!(r("select setval(seq1, 100)"), "SELECT SETVAL(`seq1`, 100)");
    // SQL-standard sugar, desugared to the SAME canonical shape.
    assert_eq!(r("select next value for seq1"), "SELECT NEXTVAL(`seq1`)");
    assert_eq!(
        r("select next value for db1.seq1"),
        "SELECT NEXTVAL(`db1`.`seq1`)"
    );
    // `NEXT`/`VALUE` stay ordinary, non-reserved identifiers everywhere
    // else.
    assert_eq!(
        r("select next, value from t"),
        "SELECT `next`,`value` FROM `t`"
    );
    assert_eq!(r("select next from t"), "SELECT `next` FROM `t`");
    assert_eq!(r("select value from t"), "SELECT `value` FROM `t`");
}

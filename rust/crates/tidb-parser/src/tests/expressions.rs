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

//! Transcreation of `pkg/parser/ast/expressions_test.go`.

use super::*;

#[test]
fn test_expresions_visitor_cover() {
    for expression in [
        "1 BETWEEN 0 AND 2",
        "1+2",
        "CASE 1 WHEN 2 THEN 3 WHEN 4 THEN 5 ELSE 6 END",
        "a",
        "1 = ANY (SELECT 1)",
        "DEFAULT(a)",
        "EXISTS (SELECT 1)",
        "1 IS NULL",
        "1 IS TRUE",
        "?",
        "(1)",
        "1 IN (2, 3, 4)",
        "'a' LIKE 'b'",
        "'a' REGEXP 'b'",
        "POSITION('a' IN 'b')",
        "ROW(1, 2)",
        "-1",
        "1",
        "VALUES(a)",
        "@a",
    ] {
        assert_full_visitor_traversal(&format!("SELECT {expression}"));
    }
}

/// Mirrors Go's `TestUnaryOperationExprRestore` (expressions_test.go:103).
/// The helper extracts the expression node from `SELECT`, while `r` below
/// checks the same canonical expression text in its statement envelope.
#[test]
fn test_unary_operation_expr_restore() {
    for (source, expected) in [
        ("++1", "++1"),
        ("--1", "--1"),
        ("-+1", "-+1"),
        ("-1", "-1"),
        ("not true", "NOT TRUE"),
        ("~3", "~3"),
        ("!true", "!TRUE"),
    ] {
        assert_eq!(
            r(&format!("select {source}")),
            format!("SELECT {expected}"),
            "source SQL: {source}"
        );
    }
}

/// Mirrors Go's `TestColumnNameExprRestore` (expressions_test.go:119).
/// Each path component is quoted independently, and an embedded backtick is
/// doubled exactly as `ast.ColumnName.Restore` does.
#[test]
fn test_column_name_expr_restore() {
    for (source, expected) in [
        ("abc", "`abc`"),
        ("`abc`", "`abc`"),
        ("`ab``c`", "`ab``c`"),
        ("sabc.tABC", "`sabc`.`tABC`"),
        ("dabc.sabc.tabc", "`dabc`.`sabc`.`tabc`"),
        ("dabc.`sabc`.tabc", "`dabc`.`sabc`.`tabc`"),
        ("`dABC`.`sabc`.tabc", "`dABC`.`sabc`.`tabc`"),
    ] {
        assert_eq!(
            r(&format!("select {source}")),
            format!("SELECT {expected}"),
            "source SQL: {source}"
        );
    }
}

/// Mirrors Go's `TestIsNullExprRestore` (expressions_test.go:135).
#[test]
fn test_is_null_expr_restore() {
    for (source, expected) in [
        ("a is null", "`a` IS NULL"),
        ("a is not null", "`a` IS NOT NULL"),
    ] {
        assert_eq!(
            r(&format!("select {source}")),
            format!("SELECT {expected}"),
            "source SQL: {source}"
        );
    }
}

/// Keep the parse-error boundary beside the restore vectors. These are the
/// incomplete operator/predicate forms rejected by Go's expression parser;
/// accepting them would turn a malformed source row into a different AST.
#[test]
fn expression_restore_source_error_vectors() {
    for source in [
        "select +",
        "select not",
        "select ~",
        "select !",
        "select a is",
        "select a is not",
        "select a is maybe",
        "select .a",
        "select `a",
        "select abc.def.ghi.jkl",
    ] {
        assert!(parse(source).is_err(), "Go rejects source SQL: {source}");
    }
}

fn assert_select_expr(source: &str, expected: &str) {
    assert_eq!(
        r(&format!("select {source}")),
        format!("SELECT {expected}"),
        "source SQL: {source}"
    );
}

#[test]
fn test_is_truth_restore() {
    for (source, expected) in [
        ("a is true", "`a` IS TRUE"),
        ("a is not true", "`a` IS NOT TRUE"),
        ("a is FALSE", "`a` IS FALSE"),
        ("a is not false", "`a` IS NOT FALSE"),
    ] {
        assert_select_expr(source, expected);
    }
}

#[test]
fn test_between_expr_restore() {
    for (source, expected) in [
        ("b between 1 and 2", "`b` BETWEEN 1 AND 2"),
        ("b not between 1 and 2", "`b` NOT BETWEEN 1 AND 2"),
        ("b between a and b", "`b` BETWEEN `a` AND `b`"),
        (
            "b between '' and 'b'",
            "`b` BETWEEN _UTF8MB4'' AND _UTF8MB4'b'",
        ),
        (
            "b between '2018-11-01' and '2018-11-02'",
            "`b` BETWEEN _UTF8MB4'2018-11-01' AND _UTF8MB4'2018-11-02'",
        ),
    ] {
        assert_select_expr(source, expected);
    }
}

#[test]
fn test_case_expr() {
    for (source, expected) in [
        ("case when 1 then 2 end", "CASE WHEN 1 THEN 2 END"),
        (
            "case when 1 then 'a' when 2 then 'b' end",
            "CASE WHEN 1 THEN _UTF8MB4'a' WHEN 2 THEN _UTF8MB4'b' END",
        ),
        (
            "case when 1 then 'a' when 2 then 'b' else 'c' end",
            "CASE WHEN 1 THEN _UTF8MB4'a' WHEN 2 THEN _UTF8MB4'b' ELSE _UTF8MB4'c' END",
        ),
        (
            "case when 'a'!=1 then true else false end",
            "CASE WHEN _UTF8MB4'a'!=1 THEN TRUE ELSE FALSE END",
        ),
        (
            "case a when 'a' then true else false end",
            "CASE `a` WHEN _UTF8MB4'a' THEN TRUE ELSE FALSE END",
        ),
    ] {
        assert_select_expr(source, expected);
    }
}

#[test]
fn test_binary_operation_expr() {
    for (source, expected) in [
        ("'a'!=1", "_UTF8MB4'a'!=1"),
        ("a!=1", "`a`!=1"),
        ("3<5", "3<5"),
        ("10>5", "10>5"),
        ("3+5", "3+5"),
        ("3-5", "3-5"),
        ("a<>5", "`a`!=5"),
        ("a=1", "`a`=1"),
        ("a mod 2", "`a`%2"),
        ("a div 2", "`a` DIV 2"),
        ("true and true", "TRUE AND TRUE"),
        ("false or false", "FALSE OR FALSE"),
        ("true xor false", "TRUE XOR FALSE"),
        ("3 & 4", "3&4"),
        ("5 | 6", "5|6"),
        ("7 ^ 8", "7^8"),
        ("9 << 10", "9<<10"),
        ("11 >> 12", "11>>12"),
    ] {
        assert_select_expr(source, expected);
    }
}

#[test]
fn test_binary_operation_expr_with_flags() {
    for (source, expected) in [
        ("'a'!=1", "SELECT _UTF8MB4'a' != 1"),
        ("a!=1", "SELECT `a` != 1"),
        ("3<5", "SELECT 3 < 5"),
        ("10>5", "SELECT 10 > 5"),
        ("3+5", "SELECT 3 + 5"),
        ("3-5", "SELECT 3 - 5"),
        ("a<>5", "SELECT `a` != 5"),
        ("a=1", "SELECT `a` = 1"),
    ] {
        let statement = parse(&format!("select {source}")).expect("source expression parses");
        let Stmt::Query(query) = statement else {
            panic!("expected query");
        };
        let tidb_ast::QueryStmt::Select(select) = query.into_inner() else {
            panic!("expected select");
        };
        let SelectField::Expr { expr, .. } = &select.fields[0] else {
            panic!("expected expression field");
        };
        assert_eq!(
            expr.restore_with_flags(
                tidb_ast::RestoreFlags::default()
                    | tidb_ast::RestoreFlags::SPACES_AROUND_BINARY_OPERATION,
            ),
            expected
                .strip_prefix("SELECT ")
                .expect("expected query prefix"),
            "source SQL: {source}"
        );
    }
}

#[test]
fn test_parentheses_expr() {
    for (source, expected) in [("(1+2)*3", "(1+2)*3"), ("1+2*3", "1+2*3")] {
        assert_select_expr(source, expected);
    }
}

#[test]
fn test_when_clause() {
    for (source, expected) in [
        ("when 1 then 2", "CASE WHEN 1 THEN 2 END"),
        ("when 1 then 'a'", "CASE WHEN 1 THEN _UTF8MB4'a' END"),
        (
            "when 'a'!=1 then true",
            "CASE WHEN _UTF8MB4'a'!=1 THEN TRUE END",
        ),
    ] {
        assert_select_expr(&format!("case {source} end"), expected);
    }
}

#[test]
fn test_default_expr() {
    for (source, expected) in [("default", "DEFAULT"), ("default(i)", "DEFAULT(`i`)")] {
        assert_eq!(
            r(&format!("insert into t values({source})")),
            format!("INSERT INTO `t` VALUES ({expected})"),
            "source SQL: {source}"
        );
    }
}

#[test]
fn test_pattern_in_expr_restore() {
    for (source, expected) in [
        ("'a' in ('b')", "_UTF8MB4'a' IN (_UTF8MB4'b')"),
        ("2 in (0,3,7)", "2 IN (0,3,7)"),
        ("2 not in (0,3,7)", "2 NOT IN (0,3,7)"),
        ("2 in (select 2)", "2 IN (SELECT 2)"),
        ("2 not in (select 2)", "2 NOT IN (SELECT 2)"),
    ] {
        assert_select_expr(source, expected);
    }
}

#[test]
fn test_pattern_like_expr_restore() {
    for (source, expected) in [
        ("a like 't1'", "`a` LIKE _UTF8MB4't1'"),
        ("a like 't1%'", "`a` LIKE _UTF8MB4't1%'"),
        ("a like '%t1%'", "`a` LIKE _UTF8MB4'%t1%'"),
        ("a like '%t1_|'", "`a` LIKE _UTF8MB4'%t1_|'"),
        ("a not like 't1'", "`a` NOT LIKE _UTF8MB4't1'"),
        ("a not like 't1%'", "`a` NOT LIKE _UTF8MB4't1%'"),
        ("a not like '%D%v%'", "`a` NOT LIKE _UTF8MB4'%D%v%'"),
        ("a not like '%t1_|'", "`a` NOT LIKE _UTF8MB4'%t1_|'"),
    ] {
        assert_select_expr(source, expected);
    }
}

#[test]
fn test_pattern_regexp_expr_restore() {
    for (source, expected) in [
        ("a regexp 't1'", "`a` REGEXP _UTF8MB4't1'"),
        (
            "a regexp '^[abc][0-9]{11}|ok$'",
            "`a` REGEXP _UTF8MB4'^[abc][0-9]{11}|ok$'",
        ),
        ("a rlike 't1'", "`a` REGEXP _UTF8MB4't1'"),
        (
            "a rlike '^[abc][0-9]{11}|ok$'",
            "`a` REGEXP _UTF8MB4'^[abc][0-9]{11}|ok$'",
        ),
        ("a not regexp 't1'", "`a` NOT REGEXP _UTF8MB4't1'"),
        (
            "a not regexp '^[abc][0-9]{11}|ok$'",
            "`a` NOT REGEXP _UTF8MB4'^[abc][0-9]{11}|ok$'",
        ),
        ("a not rlike 't1'", "`a` NOT REGEXP _UTF8MB4't1'"),
        (
            "a not rlike '^[abc][0-9]{11}|ok$'",
            "`a` NOT REGEXP _UTF8MB4'^[abc][0-9]{11}|ok$'",
        ),
    ] {
        assert_select_expr(source, expected);
    }
}

#[test]
fn test_values_expr() {
    for (source, expected) in [
        ("values(a)", "VALUES(`a`)"),
        ("values(a)+values(b)", "VALUES(`a`)+VALUES(`b`)"),
    ] {
        assert_eq!(
            r(&format!(
                "insert into t values (1,2,3) on duplicate key update c={source}"
            )),
            format!("INSERT INTO `t` VALUES (1,2,3) ON DUPLICATE KEY UPDATE `c`={expected}"),
            "source SQL: {source}"
        );
    }
}

#[test]
fn test_row_expr_restore() {
    for (source, expected) in [
        ("(1,2)", "ROW(1,2)"),
        ("(col1,col2)", "ROW(`col1`,`col2`)"),
        ("row(1,2)", "ROW(1,2)"),
        ("row(col1,col2)", "ROW(`col1`,`col2`)"),
    ] {
        assert_eq!(
            r(&format!("select 1 from t1 where {source} = row(1,2)")),
            format!("SELECT 1 FROM `t1` WHERE {expected}=ROW(1,2)"),
            "source SQL: {source}"
        );
    }
}

#[test]
fn test_max_value_expr_restore() {
    assert_eq!(
        r("alter table posts add partition (partition p1 values less than maxvalue)"),
        "ALTER TABLE `posts` ADD PARTITION (PARTITION `p1` VALUES LESS THAN (MAXVALUE))"
    );
}

#[test]
fn test_position_expr_restore() {
    assert_eq!(
        r("select * from t order by 1"),
        "SELECT * FROM `t` ORDER BY 1"
    );
}

#[test]
fn test_exists_subquery_expr_restore() {
    for (source, expected) in [
        ("EXISTS (SELECT 2)", "EXISTS (SELECT 2)"),
        ("NOT EXISTS (SELECT 2)", "NOT EXISTS (SELECT 2)"),
        ("NOT NOT EXISTS (SELECT 2)", "EXISTS (SELECT 2)"),
        ("NOT NOT NOT EXISTS (SELECT 2)", "NOT EXISTS (SELECT 2)"),
    ] {
        assert_eq!(
            r(&format!("select 1 from t1 where {source}")),
            format!("SELECT 1 FROM `t1` WHERE {expected}"),
            "source SQL: {source}"
        );
    }
}

#[test]
fn test_variable_expr() {
    for (source, expected) in [
        ("@a>1", "@`a`>1"),
        ("@`aB`+1", "@`aB`+1"),
        ("@'a':=1", "@`a`:=1"),
        ("@`a``b`=4", "@`a``b`=4"),
        (r#"@"aBC">1"#, "@`aBC`>1"),
        ("@`a`+1", "@`a`+1"),
        ("@``", "@``"),
        ("@", "@``"),
        ("@@``", "@@``"),
        ("@@var", "@@`var`"),
        ("@@global.b='foo'", "@@GLOBAL.`b`=_UTF8MB4'foo'"),
        ("@@session.'C'", "@@SESSION.`c`"),
        (r#"@@local."aBc""#, "@@SESSION.`abc`"),
    ] {
        assert_select_expr(source, expected);
    }
}

#[test]
fn test_match_against_expr() {
    for (source, expected) in [
        (
            "MATCH(content, title) AGAINST ('search for')",
            "MATCH (`content`,`title`) AGAINST (_UTF8MB4'search for')",
        ),
        (
            "MATCH(content) AGAINST ('search for' IN BOOLEAN MODE)",
            "MATCH (`content`) AGAINST (_UTF8MB4'search for' IN BOOLEAN MODE)",
        ),
        (
            "MATCH(content, title) AGAINST ('search for' WITH QUERY EXPANSION)",
            "MATCH (`content`,`title`) AGAINST (_UTF8MB4'search for' WITH QUERY EXPANSION)",
        ),
        (
            "MATCH(content) AGAINST ('search for' IN NATURAL LANGUAGE MODE WITH QUERY EXPANSION)",
            "MATCH (`content`) AGAINST (_UTF8MB4'search for' WITH QUERY EXPANSION)",
        ),
        (
            "MATCH(content) AGAINST ('search') AND id = 1",
            "MATCH (`content`) AGAINST (_UTF8MB4'search') AND `id`=1",
        ),
        (
            "MATCH(content) AGAINST ('search') OR id = 1",
            "MATCH (`content`) AGAINST (_UTF8MB4'search') OR `id`=1",
        ),
        (
            "MATCH(content) AGAINST (X'40404040' | X'01020304') OR id = 1",
            "MATCH (`content`) AGAINST (x'40404040'|x'01020304') OR `id`=1",
        ),
    ] {
        assert_eq!(
            r(&format!("select * from t where {source}")),
            format!("SELECT * FROM `t` WHERE {expected}"),
            "source SQL: {source}"
        );
    }
}

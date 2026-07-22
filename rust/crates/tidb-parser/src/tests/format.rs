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

//! Transcreation of `pkg/parser/ast/format_test.go`.

use super::*;

fn format_select_expr(source: &str) -> String {
    let statement = parse(&format!("SELECT {source}"))
        .unwrap_or_else(|error| panic!("failed to parse {source:?}: {error:?}"));
    let tidb_ast::Stmt::Query(query) = statement else {
        panic!("SELECT must parse as a query")
    };
    let tidb_ast::QueryStmt::Select(select) = query.as_ref() else {
        panic!("single SELECT must not parse as a set operation")
    };
    let tidb_ast::SelectField::Expr { expr, .. } = &select.fields[0] else {
        panic!("test input must produce an expression field")
    };
    expr.format()
}

/// Direct transcreation of `pkg/parser/ast/format_test.go::TestAstFormat`.
#[test]
fn test_ast_format() {
    const DATE_LITERAL: &str = "'tidb`.(dateliteral";
    const TIME_LITERAL: &str = "'tidb`.(timeliteral";
    const TIMESTAMP_LITERAL: &str = "'tidb`.(timestampliteral";

    let cases = [
        ("null", "NULL".to_string()),
        ("true", "TRUE".to_string()),
        ("350", "350".to_string()),
        ("001e-12", "1e-12".to_string()),
        ("345.678", "345.678".to_string()),
        ("00.0001000", "0.0001000".to_string()),
        ("null", "NULL".to_string()),
        ("\"Hello, world\"", "\"Hello, world\"".to_string()),
        ("'Hello, world'", "\"Hello, world\"".to_string()),
        ("'Hello, \"world\"'", "\"Hello, \\\"world\\\"\"".to_string()),
        ("_utf8'你好'", "\"你好\"".to_string()),
        ("x'bcde'", "x'bcde'".to_string()),
        ("x''", "x''".to_string()),
        ("x'0035'", "x'0035'".to_string()),
        ("b'00111111'", "b'111111'".to_string()),
        (
            "time'10:10:10.123'",
            format!("{TIME_LITERAL}(\"10:10:10.123\")"),
        ),
        (
            "timestamp'1999-01-01 10:0:0.123'",
            format!("{TIMESTAMP_LITERAL}(\"1999-01-01 10:0:0.123\")"),
        ),
        (
            "date '1700-01-01'",
            format!("{DATE_LITERAL}(\"1700-01-01\")"),
        ),
        ("f between 30 and 50", "`f` BETWEEN 30 AND 50".to_string()),
        (
            "f not between 30 and 50",
            "`f` NOT BETWEEN 30 AND 50".to_string(),
        ),
        ("345 + \"  hello  \"", "345 + \"  hello  \"".to_string()),
        (
            "\"hello world\" >= 'hello world'",
            "\"hello world\" >= \"hello world\"".to_string(),
        ),
        (
            "case 3 when 1 then false else true end",
            "CASE 3 WHEN 1 THEN FALSE ELSE TRUE END".to_string(),
        ),
        (
            "database.table.column",
            "`database`.`table`.`column`".to_string(),
        ),
        ("3 is null", "3 IS NULL".to_string()),
        ("3 is not null", "3 IS NOT NULL".to_string()),
        ("3 is true", "3 IS TRUE".to_string()),
        ("3 is not true", "3 IS NOT TRUE".to_string()),
        ("3 is false", "3 IS FALSE".to_string()),
        ("(x is false)", "(`x` IS FALSE)".to_string()),
        ("3 in (a,b,\"h\",6)", "3 IN (`a`,`b`,\"h\",6)".to_string()),
        (
            "3 not in (a,b,\"h\",6)",
            "3 NOT IN (`a`,`b`,\"h\",6)".to_string(),
        ),
        ("\"abc\" like '%b%'", "\"abc\" LIKE \"%b%\"".to_string()),
        (
            "\"abc\" not like '%b%'",
            "\"abc\" NOT LIKE \"%b%\"".to_string(),
        ),
        (
            "\"abc\" like '%b%' escape '_'",
            "\"abc\" LIKE \"%b%\" ESCAPE '_'".to_string(),
        ),
        (
            "\"abc\" regexp '.*bc?'",
            "\"abc\" REGEXP \".*bc?\"".to_string(),
        ),
        (
            "\"abc\" not regexp '.*bc?'",
            "\"abc\" NOT REGEXP \".*bc?\"".to_string(),
        ),
        ("- 4", "-4".to_string()),
        ("-(-4)", "-(-4)".to_string()),
        ("a%b", "`a` % `b`".to_string()),
        ("a%b+6", "`a` % `b` + 6".to_string()),
        ("a%(b+6)", "`a` % (`b` + 6)".to_string()),
        (
            "json_extract(a,'$.b','$.\"c d\"')",
            "json_extract(`a`, \"$.b\", \"$.\\\"c d\\\"\")".to_string(),
        ),
        ("length(a)", "length(`a`)".to_string()),
        ("a -> '$.a'", "json_extract(`a`, \"$.a\")".to_string()),
        (
            "a.b ->> '$.a'",
            "json_unquote(json_extract(`a`.`b`, \"$.a\"))".to_string(),
        ),
        (
            "DATE_ADD('1970-01-01', interval 3 second)",
            "date_add(\"1970-01-01\", INTERVAL 3 SECOND)".to_string(),
        ),
        (
            "TIMESTAMPDIFF(month, '2001-01-01', '2001-02-02 12:03:05.123')",
            "timestampdiff(MONTH, \"2001-01-01\", \"2001-02-02 12:03:05.123\")".to_string(),
        ),
        ("cast(a as signed)", "CAST(`a` AS SIGNED)".to_string()),
        (
            "cast(a as unsigned integer)",
            "CAST(`a` AS UNSIGNED)".to_string(),
        ),
        (
            "cast(a as char(3) binary)",
            "CAST(`a` AS BINARY(3))".to_string(),
        ),
        ("cast(a as decimal)", "CAST(`a` AS DECIMAL(10))".to_string()),
        (
            "cast(a as decimal(3))",
            "CAST(`a` AS DECIMAL(3))".to_string(),
        ),
        (
            "cast(a as decimal(3,3))",
            "CAST(`a` AS DECIMAL(3, 3))".to_string(),
        ),
        (
            "((case when (c0 = 0) then 0 when (c0 > 0) then (c1 / c0) end))",
            "((CASE WHEN (`c0` = 0) THEN 0 WHEN (`c0` > 0) THEN (`c1` / `c0`) END))".to_string(),
        ),
        ("convert(a, signed)", "CONVERT(`a`, SIGNED)".to_string()),
        ("binary \"hello\"", "BINARY \"hello\"".to_string()),
    ];

    for (input, expected) in cases {
        assert_eq!(format_select_expr(input), expected, "{input}");
    }
}

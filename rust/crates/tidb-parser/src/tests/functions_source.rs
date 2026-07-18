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

//! Source-shaped coverage for `pkg/parser/ast/functions_test.go`.
//!
//! The Go tests exercise AST restoration, including canonical spellings for
//! function names, charset introducers, `COUNT(*)`, aggregate aliases, and
//! `CONVERT(... USING ...)` charset values. Keep these vectors in a dedicated
//! ownership slice so parser work can proceed without growing `expr.rs`.

use super::*;

#[test]
fn func_call_restore_source_vectors() {
    // Direct rows from `pkg/parser/ast/functions_test.go:TestFuncCallExprRestore`.
    for (sql, expected) in [
        ("JSON_ARRAYAGG(attribute)", "JSON_ARRAYAGG(`attribute`)"),
        (
            "JSON_OBJECTAGG(attribute, value)",
            "JSON_OBJECTAGG(`attribute`, `value`)",
        ),
        ("ABS(-1024)", "ABS(-1024)"),
        ("ACOS(3.14)", "ACOS(3.14)"),
        ("CONV('a',16,2)", "CONV(_UTF8MB4'a', 16, 2)"),
        ("COS(PI())", "COS(PI())"),
        ("RAND()", "RAND()"),
        (
            "ADDDATE('2000-01-01', 1)",
            "ADDDATE(_UTF8MB4'2000-01-01', INTERVAL 1 DAY)",
        ),
        (
            "DATE_ADD('2000-01-01', INTERVAL 1 DAY)",
            "DATE_ADD(_UTF8MB4'2000-01-01', INTERVAL 1 DAY)",
        ),
        (
            "DATE_ADD('2000-01-01', INTERVAL '1 1:12:23.100000' DAY_MICROSECOND)",
            "DATE_ADD(_UTF8MB4'2000-01-01', INTERVAL _UTF8MB4'1 1:12:23.100000' DAY_MICROSECOND)",
        ),
        (
            "EXTRACT(DAY FROM '2000-01-01')",
            "EXTRACT(DAY FROM _UTF8MB4'2000-01-01')",
        ),
        (
            "extract(day from '1999-01-01')",
            "EXTRACT(DAY FROM _UTF8MB4'1999-01-01')",
        ),
        (
            "GET_FORMAT(DATE, 'EUR')",
            "GET_FORMAT(DATE, _UTF8MB4'EUR')",
        ),
        (
            "POSITION('a' IN 'abc')",
            "POSITION(_UTF8MB4'a' IN _UTF8MB4'abc')",
        ),
        ("TRIM('  bar   ')", "TRIM(_UTF8MB4'  bar   ')"),
        (
            "TRIM('a' FROM '  bar   ')",
            "TRIM(_UTF8MB4'a' FROM _UTF8MB4'  bar   ')",
        ),
        (
            "TRIM(LEADING FROM '  bar   ')",
            "TRIM(LEADING _UTF8MB4' ' FROM _UTF8MB4'  bar   ')",
        ),
        (
            "TRIM(BOTH FROM '  bar   ')",
            "TRIM(BOTH _UTF8MB4' ' FROM _UTF8MB4'  bar   ')",
        ),
        (
            "TRIM(TRAILING FROM '  bar   ')",
            "TRIM(TRAILING _UTF8MB4' ' FROM _UTF8MB4'  bar   ')",
        ),
        (
            "TRIM(LEADING 'x' FROM 'xxxyxxx')",
            "TRIM(LEADING _UTF8MB4'x' FROM _UTF8MB4'xxxyxxx')",
        ),
        (
            "TRIM(BOTH 'x' FROM 'xxxyxxx')",
            "TRIM(BOTH _UTF8MB4'x' FROM _UTF8MB4'xxxyxxx')",
        ),
        (
            "TRIM(TRAILING 'x' FROM 'xxxyxxx')",
            "TRIM(TRAILING _UTF8MB4'x' FROM _UTF8MB4'xxxyxxx')",
        ),
        (
            "TRIM(BOTH col1 FROM col2)",
            "TRIM(BOTH `col1` FROM `col2`)",
        ),
        (
            "DATE_ADD('2008-01-02', INTERVAL INTERVAL(1, 0, 1) DAY)",
            "DATE_ADD(_UTF8MB4'2008-01-02', INTERVAL INTERVAL(1, 0, 1) DAY)",
        ),
        (
            "BENCHMARK(1000000, AES_ENCRYPT('text', UNHEX('F3229A0B371ED2D9441B830D21A390C3')))",
            "BENCHMARK(1000000, AES_ENCRYPT(_UTF8MB4'text', UNHEX(_UTF8MB4'F3229A0B371ED2D9441B830D21A390C3')))",
        ),
        (
            "SUBSTRING('Quadratically', 5)",
            "SUBSTRING(_UTF8MB4'Quadratically', 5)",
        ),
        (
            "SUBSTRING('Quadratically' FROM 5)",
            "SUBSTRING(_UTF8MB4'Quadratically', 5)",
        ),
        (
            "SUBSTRING('Quadratically', 5, 6)",
            "SUBSTRING(_UTF8MB4'Quadratically', 5, 6)",
        ),
        (
            "SUBSTRING('Quadratically' FROM 5 FOR 6)",
            "SUBSTRING(_UTF8MB4'Quadratically', 5, 6)",
        ),
        ("JSON_TYPE('[123]')", "JSON_TYPE(_UTF8MB4'[123]')"),
        ("bit_and(all c1)", "BIT_AND(`c1`)"),
        ("nextval(seq)", "NEXTVAL(`seq`)"),
        ("nextval(test.seq)", "NEXTVAL(`test`.`seq`)"),
        ("lastval(seq)", "LASTVAL(`seq`)"),
        ("lastval(test.seq)", "LASTVAL(`test`.`seq`)"),
        ("setval(seq, 100)", "SETVAL(`seq`, 100)"),
        ("setval(test.seq, 100)", "SETVAL(`test`.`seq`, 100)"),
        ("next value for seq", "NEXTVAL(`seq`)"),
        ("next value for test.seq", "NEXTVAL(`test`.`seq`)"),
        ("next value for sequence", "NEXTVAL(`sequence`)"),
        ("NeXt vAluE for seQuEncE2", "NEXTVAL(`seQuEncE2`)"),
        ("NeXt vAluE for test.seQuEncE2", "NEXTVAL(`test`.`seQuEncE2`)"),
        ("weight_string(a)", "WEIGHT_STRING(`a`)"),
        ("Weight_stRing(test.a)", "WEIGHT_STRING(`test`.`a`)"),
        ("weight_string('a')", "WEIGHT_STRING(_UTF8MB4'a')"),
        (
            "weight_string('a' collate utf8_general_ci collate utf8mb4_general_ci)",
            "WEIGHT_STRING(_UTF8MB4'a' COLLATE utf8_general_ci COLLATE utf8mb4_general_ci)",
        ),
        (
            "weight_string(_utf8 'a' collate utf8_general_ci)",
            "WEIGHT_STRING(_UTF8'a' COLLATE utf8_general_ci)",
        ),
        ("weight_string(_utf8 'a')", "WEIGHT_STRING(_UTF8'a')"),
        (
            "weight_string(a as char(5))",
            "WEIGHT_STRING(`a` AS CHAR(5))",
        ),
        (
            "weight_string(a as character(5))",
            "WEIGHT_STRING(`a` AS CHAR(5))",
        ),
        (
            "weight_string(a as binary(5))",
            "WEIGHT_STRING(`a` AS BINARY(5))",
        ),
        (
            "hex(weight_string('abc' as binary(5)))",
            "HEX(WEIGHT_STRING(_UTF8MB4'abc' AS BINARY(5)))",
        ),
        ("soundex(attr)", "SOUNDEX(`attr`)"),
        ("soundex('string')", "SOUNDEX(_UTF8MB4'string')"),
    ] {
        assert_eq!(
            r(&format!("select {sql}")),
            format!("SELECT {expected}"),
            "source SQL: {sql}"
        );
    }
}

#[test]
fn func_cast_restore_source_vectors() {
    // Direct rows from `pkg/parser/ast/functions_test.go:TestFuncCastExprRestore`.
    for (sql, expected) in [
        (
            "CONVERT('Müller' USING UtF8)",
            "CONVERT(_UTF8MB4'Müller' USING 'utf8')",
        ),
        (
            "CONVERT('Müller' USING UtF8Mb4)",
            "CONVERT(_UTF8MB4'Müller' USING 'utf8mb4')",
        ),
        (
            "CONVERT('Müller', CHAR(32) CHARACTER SET UtF8)",
            "CONVERT(_UTF8MB4'Müller', CHAR(32) CHARSET UTF8)",
        ),
        (
            "CAST('test' AS CHAR CHARACTER SET UtF8)",
            "CAST(_UTF8MB4'test' AS CHAR CHARSET UTF8)",
        ),
        ("BINARY 'New York'", "BINARY _UTF8MB4'New York'"),
    ] {
        assert_eq!(
            r(&format!("select {sql}")),
            format!("SELECT {expected}"),
            "source SQL: {sql}"
        );
    }
}

#[test]
fn aggregate_restore_source_vectors() {
    // Direct rows from `pkg/parser/ast/functions_test.go:TestAggregateFuncExprRestore`.
    for (sql, expected) in [
        ("AVG(test_score)", "AVG(`test_score`)"),
        ("AVG(distinct test_score)", "AVG(DISTINCT `test_score`)"),
        ("BIT_AND(test_score)", "BIT_AND(`test_score`)"),
        ("BIT_OR(test_score)", "BIT_OR(`test_score`)"),
        ("BIT_XOR(test_score)", "BIT_XOR(`test_score`)"),
        ("COUNT(test_score)", "COUNT(`test_score`)"),
        ("COUNT(*)", "COUNT(1)"),
        (
            "COUNT(DISTINCT scores, results)",
            "COUNT(DISTINCT `scores`, `results`)",
        ),
        ("MIN(test_score)", "MIN(`test_score`)"),
        ("MIN(DISTINCT test_score)", "MIN(DISTINCT `test_score`)"),
        ("MAX(test_score)", "MAX(`test_score`)"),
        ("MAX(DISTINCT test_score)", "MAX(DISTINCT `test_score`)"),
        ("STD(test_score)", "STDDEV_POP(`test_score`)"),
        ("STDDEV(test_score)", "STDDEV_POP(`test_score`)"),
        ("STDDEV_POP(test_score)", "STDDEV_POP(`test_score`)"),
        ("STDDEV_SAMP(test_score)", "STDDEV_SAMP(`test_score`)"),
        ("SUM(test_score)", "SUM(`test_score`)"),
        ("SUM(DISTINCT test_score)", "SUM(DISTINCT `test_score`)"),
        ("VAR_POP(test_score)", "VAR_POP(`test_score`)"),
        ("VAR_SAMP(test_score)", "VAR_SAMP(`test_score`)"),
        ("VARIANCE(test_score)", "VAR_POP(`test_score`)"),
        (
            "JSON_OBJECTAGG(test_score, results)",
            "JSON_OBJECTAGG(`test_score`, `results`)",
        ),
        ("GROUP_CONCAT(a)", "GROUP_CONCAT(`a` SEPARATOR ',')"),
        (
            "GROUP_CONCAT(a separator '--')",
            "GROUP_CONCAT(`a` SEPARATOR '--')",
        ),
        (
            "GROUP_CONCAT(a order by b desc, c)",
            "GROUP_CONCAT(`a` ORDER BY `b` DESC,`c` SEPARATOR ',')",
        ),
        (
            "GROUP_CONCAT(a order by b desc, c separator '--')",
            "GROUP_CONCAT(`a` ORDER BY `b` DESC,`c` SEPARATOR '--')",
        ),
    ] {
        assert_eq!(
            r(&format!("select {sql}")),
            format!("SELECT {expected}"),
            "source SQL: {sql}"
        );
    }
}

#[test]
fn convert_charset_source_vectors() {
    // Direct rows from `pkg/parser/ast/functions_test.go:TestConvert`. The
    // Rust parser exposes a structured ParseError rather than Go's numeric
    // `[parser:1115]` wrapper, so invalid charset rows assert rejection while
    // valid rows assert the canonical charset value in restored SQL.
    for (sql, expected, charset) in [
        (
            r#"SELECT CONVERT("abc" USING "latin1")"#,
            "SELECT CONVERT(_UTF8MB4'abc' USING 'latin1')",
            "latin1",
        ),
        (
            r#"SELECT CONVERT("abc" USING laTiN1)"#,
            "SELECT CONVERT(_UTF8MB4'abc' USING 'latin1')",
            "latin1",
        ),
        (
            r#"SELECT CONVERT("abc" USING "binary")"#,
            "SELECT CONVERT(_UTF8MB4'abc' USING 'binary')",
            "binary",
        ),
        (
            r#"SELECT CONVERT("abc" USING biNaRy)"#,
            "SELECT CONVERT(_UTF8MB4'abc' USING 'binary')",
            "binary",
        ),
    ] {
        assert_eq!(r(sql), expected, "source SQL: {sql}");
        let tidb_ast::Stmt::Query(query) = parse(sql).expect("valid CONVERT source row") else {
            panic!("expected query statement for {sql}");
        };
        let tidb_ast::QueryStmt::Select(select) = *query else {
            panic!("expected SELECT statement for {sql}");
        };
        let tidb_ast::SelectField::Expr { expr, .. } = &select.fields[0] else {
            panic!("expected expression field for {sql}");
        };
        let tidb_ast::Expr::ConvertUsing {
            charset: actual, ..
        } = expr
        else {
            panic!("expected CONVERT USING AST node for {sql}");
        };
        assert_eq!(actual, charset, "source SQL: {sql}");
    }

    for sql in [
        "SELECT CONVERT(a USING a)",
        r#"SELECT CONVERT("abc" USING CONCAT("utf", "8"))"#,
    ] {
        assert!(parse(sql).is_err(), "Go rejects unknown charset: {sql}");
    }
}

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

//! Transcreation of `pkg/parser/ast/flag_test.go`.

use super::*;
use tidb_ast::{
    QueryStmt, Stmt, FLAG_CONSTANT, FLAG_HAS_AGGREGATE_FUNC, FLAG_HAS_DEFAULT, FLAG_HAS_FUNC,
    FLAG_HAS_PARAM_MARKER, FLAG_HAS_REFERENCE, FLAG_HAS_SUBQUERY, FLAG_HAS_VARIABLE,
};

fn first_expression(sql: &str) -> Expr {
    let Stmt::Query(query) = parse(&format!("SELECT {sql}")).expect("parse") else {
        panic!("expected query");
    };
    let QueryStmt::Select(select) = query.into_inner() else {
        panic!("expected select");
    };
    let SelectField::Expr { expr, .. } = select.fields.into_iter().next().expect("field") else {
        panic!("expected expression field");
    };
    expr
}

#[test]
fn test_has_agg_flag() {
    let expression = Expr::Between {
        expr: Box::new(Expr::Int("1".to_owned())),
        low: Box::new(Expr::Int("0".to_owned())),
        high: Box::new(Expr::Aggregate {
            name: "COUNT".to_owned(),
            distinct: false,
            args: vec![Expr::Int("1".to_owned())],
        }),
        not: false,
    };
    assert!(expression.has_aggregate_flag());
    assert!(!Expr::UserVar("x".to_owned()).has_aggregate_flag());
}

#[test]
fn test_flag() {
    let cases = [
        ("1 BETWEEN 0 AND 2", FLAG_CONSTANT),
        ("CASE 1 WHEN 1 THEN 1 ELSE 0 END", FLAG_CONSTANT),
        (
            "CASE 1 WHEN a > 1 THEN 1 ELSE 0 END",
            FLAG_CONSTANT | FLAG_HAS_REFERENCE,
        ),
        ("1 = ANY (SELECT 1) OR EXISTS (SELECT 1)", FLAG_HAS_SUBQUERY),
        (
            "1 IN (1) OR 1 IS TRUE OR NULL IS NULL OR 'abc' LIKE 'abc' OR 'abc' RLIKE 'abc'",
            FLAG_CONSTANT,
        ),
        ("ROW(1, 1) = ROW(1, 1)", FLAG_CONSTANT),
        ("(1 + a) > ?", FLAG_HAS_REFERENCE | FLAG_HAS_PARAM_MARKER),
        ("TRIM('abc ')", FLAG_HAS_FUNC),
        (
            "NOW() + EXTRACT(YEAR FROM '2009-07-02') + CAST(1 AS UNSIGNED)",
            FLAG_HAS_FUNC,
        ),
        ("SUBSTRING('abc', 1)", FLAG_HAS_FUNC),
        ("SUM(a)", FLAG_HAS_AGGREGATE_FUNC | FLAG_HAS_REFERENCE),
        ("(SELECT 1)", FLAG_HAS_SUBQUERY),
        ("@auto_commit", FLAG_HAS_VARIABLE),
        ("DEFAULT(a)", FLAG_HAS_DEFAULT),
        ("a IS NULL", FLAG_HAS_REFERENCE),
        ("1 IS TRUE", FLAG_CONSTANT),
        (
            "a IN (1, COUNT(*), 3)",
            FLAG_CONSTANT | FLAG_HAS_REFERENCE | FLAG_HAS_AGGREGATE_FUNC,
        ),
        ("'Michael!' REGEXP '.*'", FLAG_CONSTANT),
        ("a REGEXP '.*'", FLAG_HAS_REFERENCE),
        ("-a", FLAG_HAS_REFERENCE),
    ];
    for (source, expected) in cases {
        assert_eq!(first_expression(source).flags(), expected, "{source}");
    }
}

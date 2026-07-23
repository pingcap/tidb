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

//! Direct transcreation of `pkg/parser/parser_integration_test.go`.

use tidb_ast::{AdminStmt, DmlStmt, QueryStmt, Stmt};
use tidb_parser::parse;

fn assert_select(sql: &str) {
    assert!(
        matches!(
            parse(sql),
            Ok(Stmt::Query(query)) if matches!(&*query, QueryStmt::Select(_))
        ),
        "{sql}"
    );
}

#[test]
fn test_hand_parser_simple_select() {
    for sql in [
        "SELECT 1",
        "SELECT 1, 2, 3",
        "SELECT a FROM t",
        "SELECT a, b FROM t WHERE a = 1",
        "SELECT * FROM t",
        "SELECT t.* FROM t",
        "SELECT a FROM t WHERE a > 1 AND b < 2",
        "SELECT a FROM t WHERE a = 1 OR b = 2",
        "SELECT a FROM t ORDER BY a",
        "SELECT a FROM t ORDER BY a DESC",
        "SELECT a FROM t LIMIT 10",
        "SELECT a FROM t LIMIT 10, 20",
        "SELECT a FROM t LIMIT 10 OFFSET 5",
        "SELECT a, b FROM t GROUP BY a",
        "SELECT a, COUNT(*) FROM t GROUP BY a HAVING COUNT(*) > 1",
        "SELECT DISTINCT a FROM t",
    ] {
        assert_select(sql);
    }
}

#[test]
fn test_hand_parser_joins() {
    for sql in [
        "SELECT a FROM t1 JOIN t2 ON t1.id = t2.id",
        "SELECT a FROM t1 LEFT JOIN t2 ON t1.id = t2.id",
        "SELECT a FROM t1 RIGHT JOIN t2 ON t1.id = t2.id",
        "SELECT a FROM t1 INNER JOIN t2 ON t1.id = t2.id",
        "SELECT a FROM t1 CROSS JOIN t2",
        "SELECT a FROM t1, t2 WHERE t1.id = t2.id",
    ] {
        assert_select(sql);
    }
}

#[test]
fn test_hand_parser_insert() {
    for sql in [
        "INSERT INTO t VALUES (1, 2, 3)",
        "INSERT INTO t (a, b, c) VALUES (1, 2, 3)",
        "INSERT INTO t (a, b) VALUES (1, 2), (3, 4)",
        "INSERT INTO t SET a = 1, b = 2",
        "REPLACE INTO t VALUES (1, 2, 3)",
    ] {
        assert!(
            matches!(parse(sql), Ok(Stmt::Dml(dml)) if matches!(&*dml, DmlStmt::Insert(_))),
            "{sql}"
        );
    }
    let Stmt::Dml(dml) = parse("INSERT INTO t SET a = 1, b = 2, c = 3").unwrap() else {
        unreachable!()
    };
    let DmlStmt::Insert(insert) = dml.into_inner() else {
        unreachable!()
    };
    assert_eq!(insert.set_columns.len(), 3);
    assert_eq!(insert.rows.len(), 1);
    assert_eq!(insert.rows[0].len(), 3);
    assert_select("SELECT CONCAT(a, b) FROM t");
}

#[test]
fn test_hand_parser_update() {
    for sql in [
        "UPDATE t SET a = 1",
        "UPDATE t SET a = 1, b = 2 WHERE c = 3",
        "UPDATE t SET a = a + 1 ORDER BY b LIMIT 10",
    ] {
        assert!(
            matches!(parse(sql), Ok(Stmt::Dml(dml)) if matches!(&*dml, DmlStmt::Update(_))),
            "{sql}"
        );
    }
}

#[test]
fn test_hand_parser_delete() {
    for sql in [
        "DELETE FROM t WHERE a = 1",
        "DELETE FROM t ORDER BY a LIMIT 10",
        "DELETE FROM t",
    ] {
        assert!(
            matches!(parse(sql), Ok(Stmt::Dml(dml)) if matches!(&*dml, DmlStmt::Delete(_))),
            "{sql}"
        );
    }
}

#[test]
fn test_hand_parser_expressions() {
    for sql in [
        "SELECT 1 + 2",
        "SELECT a * b + c",
        "SELECT a AND b OR c",
        "SELECT NOT a",
        "SELECT -a",
        "SELECT a IS NULL",
        "SELECT a IS NOT NULL",
        "SELECT a IN (1, 2, 3)",
        "SELECT a NOT IN (1, 2, 3)",
        "SELECT a BETWEEN 1 AND 10",
        "SELECT a NOT BETWEEN 1 AND 10",
        "SELECT a LIKE 'foo%'",
        "SELECT CASE WHEN a = 1 THEN 'one' ELSE 'other' END",
        "SELECT CASE a WHEN 1 THEN 'one' WHEN 2 THEN 'two' END",
        "SELECT a > b",
        "SELECT a >= b",
        "SELECT a < b",
        "SELECT a <= b",
        "SELECT a != b",
        "SELECT a <> b",
        "SELECT a <=> b",
        "SELECT a = 1 AND (b = 2 OR c = 3)",
        "SELECT ?",
    ] {
        assert_select(sql);
    }
}

#[test]
fn test_hand_parser_subquery() {
    for sql in [
        "SELECT a FROM t WHERE a IN (SELECT b FROM t2)",
        "SELECT EXISTS (SELECT 1 FROM t)",
        "SELECT (SELECT 1)",
    ] {
        assert_select(sql);
    }
}

#[test]
fn test_hand_parser_show() {
    assert!(
        matches!(parse("SHOW BUILTINS"), Ok(Stmt::Admin(admin)) if matches!(&*admin, AdminStmt::ShowBuiltins))
    );
}

#[test]
fn test_hand_parser_show_create_user() {
    for (sql, expected) in [
        (
            "show create user 'root'@'localhost'",
            Some("SHOW CREATE USER `root`@`localhost`"),
        ),
        ("show create user if not exists", None),
        (
            "show create user current_user",
            Some("SHOW CREATE USER CURRENT_USER"),
        ),
    ] {
        match expected {
            Some(expected) => assert_eq!(parse(sql).unwrap().restore(), expected, "{sql}"),
            None => assert!(parse(sql).is_err(), "{sql}"),
        }
    }
}

#[test]
fn test_hint_integration() {
    for sql in [
        "SELECT /*+ HASH_JOIN(t1) */ * FROM t1",
        "SELECT /*+ USE_INDEX(t, idx) */ a FROM t WHERE a > 1",
        "INSERT /*+ SET_VAR(foreign_key_checks=OFF) */ INTO t VALUES (1)",
        "UPDATE /*+ USE_INDEX(t, idx) */ t SET a = 1",
        "DELETE /*+ USE_INDEX(t, idx) */ FROM t WHERE a = 1",
    ] {
        let restored = parse(sql)
            .unwrap_or_else(|error| panic!("{sql}: {error:?}"))
            .restore();
        assert!(!restored.is_empty(), "{sql}");
        assert_eq!(
            parse(&restored).unwrap().restore(),
            restored,
            "source SQL: {sql}"
        );
    }
}

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

//! Direct transcreation of the compact root `pkg/parser/test_*_test.go` files.

use tidb_ast::{DmlStmt, Join, JoinNode, JoinType, QueryStmt, Stmt};
use tidb_parser::parse;

fn select(sql: &str) -> Box<tidb_ast::SelectStmt> {
    match parse(sql).unwrap_or_else(|error| panic!("{sql}: {error:?}")) {
        Stmt::Query(query) => match query.into_inner() {
            QueryStmt::Select(select) => select,
            other => panic!("{sql}: expected SELECT, got {other:?}"),
        },
        other => panic!("{sql}: expected query, got {other:?}"),
    }
}

#[test]
fn test_default_expr_syntax() {
    let statement =
        parse("INSERT INTO t1 (a) SELECT b FROM t2 ON DUPLICATE KEY UPDATE a=DEFAULT(b);").unwrap();
    let Stmt::Dml(dml) = statement else {
        panic!("expected DML statement");
    };
    let DmlStmt::Insert(insert) = dml.into_inner() else {
        panic!("expected INSERT statement");
    };
    assert_eq!(insert.on_duplicate.len(), 1);
}

#[test]
fn test_fk_subquery_syntax() {
    let statement = select(
        "select 1 from `child` where `a` is not null and (`a`) not in (select `a` from `parent` ) limit 1",
    );
    assert!(statement.limit.is_some());
}

#[test]
fn test_join_edge_cases() {
    let rows = [
        (
            "INSERT INTO t1 SELECT 1, a FROM t2 NATURAL JOIN t3 ON DUPLICATE KEY UPDATE j= a",
            true,
        ),
        (
            "INSERT INTO t1 SELECT 1, a FROM t2 STRAIGHT_JOIN t3 ON DUPLICATE KEY UPDATE j= a",
            false,
        ),
        (
            "SELECT * FROM t1 LEFT JOIN t2 LEFT JOIN t3 ON t2.a = t3.a ON t1.a = t2.a",
            true,
        ),
        (
            "SELECT * FROM t1 LEFT JOIN t2 NATURAL JOIN t3 ON t1.a = t2.a",
            true,
        ),
        ("SELECT * FROM t1 JOIN t2 JOIN t3", true),
        ("SELECT * FROM t1 LEFT JOIN t2", false),
        (
            "SELECT * FROM t1 LEFT JOIN t2 RIGHT JOIN t3 ON t2.a = t3.a ON t1.a = t2.a",
            true,
        ),
    ];
    for (sql, valid) in rows {
        assert_eq!(parse(sql).is_ok(), valid, "{sql}");
    }
}

#[test]
fn test_natural_join_in_rhs() {
    let statement = select("SELECT * FROM t1 LEFT JOIN t2 NATURAL JOIN t3 ON t1.a = t2.a");
    let join = statement.from.expect("FROM join");
    assert_eq!(join.tp, JoinType::Left);
    assert!(join.on.is_some());
    let Some(JoinNode::Join(rhs)) = join.right else {
        panic!("expected joined RHS: {join:?}");
    };
    assert!(rhs.natural);
    assert!(rhs.on.is_none());
}

#[test]
fn test_yacc_offset_parity() {
    let rows = [
        (
            "select (select * from t3 where id not null) from t1, t2",
            (1, 42),
        ),
        (
            "create table t_34455 (\na int not null,\nforeign key (a) references t3 (a) match full match partial)",
            (3, 51),
        ),
        ("select cast('{a:1}' as text)", (1, 27)),
    ];
    for (sql, expected_line_column) in rows {
        let error = parse(sql).expect_err(sql);
        let prefix = &sql[..error.offset];
        let line = prefix.bytes().filter(|byte| *byte == b'\n').count() + 1;
        let column = prefix
            .rfind('\n')
            .map_or(error.offset, |newline| error.offset - newline);
        assert_eq!((line, column), expected_line_column, "{sql}: {error:?}");
    }
}

#[test]
fn test_check_dev2_bugs() {
    let rows = [
        (
            "create table t (a int) ENGINE_ATTRIBUTE = '{\"key\": \"value\"}'",
            true,
        ),
        ("create table t (col_23 tinyint default 71 not null)", true),
        (
            "create table t (col timestamp default '1971-06-09' not null, col1 int default 1, unique key(col1))",
            true,
        ),
        ("ANALYZE TABLE t0 INDEX PRIMARY", true),
        (
            "prepare stmt from '(select * from t1 union all select * from t1) intersect select * from t2'",
            true,
        ),
        (
            "set transaction read only as of timestamp now(6) - interval 0.1 second",
            true,
        ),
        ("CREATE TABLE `t` (`a` int) DEFAULT CHARSET=abcdefg", false),
        ("create table t (col_30 decimal default 0)", true),
        (
            "SELECT * FROM t1 STRAIGHT_JOIN t2 ON t1.a = t2.a",
            true,
        ),
        (
            "alter table t1 add partition\n(partition p1 values in (maxvalue, maxvalue))",
            false,
        ),
        (
            "DELETE FROM t1 alias USING t1, t2 alias WHERE t1.a = alias.a",
            false,
        ),
    ];
    for (sql, valid) in rows {
        assert_eq!(parse(sql).is_ok(), valid, "{sql}");
    }
}

fn find_lateral(join: &Join) -> Option<&[String]> {
    fn in_node(node: &JoinNode) -> Option<&[String]> {
        match node {
            JoinNode::Derived {
                lateral: true,
                column_names,
                ..
            } => Some(column_names),
            JoinNode::Join(join) => find_lateral(join),
            _ => None,
        }
    }
    in_node(&join.left).or_else(|| join.right.as_ref().and_then(in_node))
}

#[test]
fn test_lateral_parsing() {
    let rows: &[(&str, bool, &[&str])] = &[
        ("SELECT * FROM t1, LATERAL (SELECT t1.a) AS dt", true, &[]),
        ("SELECT * FROM t1 LEFT JOIN LATERAL (SELECT t1.b) AS dt ON true", true, &[]),
        ("SELECT * FROM t1 CROSS JOIN LATERAL (SELECT t1.c) AS dt", true, &[]),
        ("SELECT * FROM t1 RIGHT JOIN LATERAL (SELECT t1.d) AS dt ON true", true, &[]),
        ("SELECT * FROM t1 JOIN LATERAL (SELECT t1.e) AS dt ON true", true, &[]),
        ("SELECT * FROM t1, LATERAL (SELECT t1.a, COUNT(*) FROM t2 WHERE t2.x = t1.x GROUP BY t1.a) AS dt", true, &[]),
        ("SELECT * FROM t1, LATERAL (SELECT * FROM (SELECT t1.a) AS inner_dt) AS dt", true, &[]),
        ("SELECT * FROM t1, LATERAL (SELECT t1.a) AS dt1, LATERAL (SELECT t1.b) AS dt2", true, &[]),
        ("SELECT * FROM t1, (SELECT a FROM t2) AS dt", false, &[]),
        ("SELECT * FROM t1, LATERAL (SELECT * FROM t2 WHERE t2.x = t1.x) AS dt WHERE dt.y > 10", true, &[]),
        ("SELECT * FROM t1, LATERAL (SELECT t1.a, t1.b) AS dt(c1, c2)", true, &["c1", "c2"]),
        ("SELECT * FROM t1, LATERAL (SELECT t1.a) dt(col1)", true, &["col1"]),
        ("SELECT * FROM t1 LEFT JOIN LATERAL (SELECT t1.a, t1.b, t1.c) AS dt(x, y, z) ON true", true, &["x", "y", "z"]),
    ];
    for (sql, lateral, columns) in rows {
        let statement = select(sql);
        let restored =
            Stmt::Query(tidb_ast::NodeBox::new(QueryStmt::Select(statement.clone()))).restore();
        let reparsed = select(&restored);
        for parsed in [&statement, &reparsed] {
            let found = parsed.from.as_ref().and_then(find_lateral);
            assert_eq!(found.is_some(), *lateral, "{sql}");
            if *lateral {
                assert_eq!(found.unwrap(), *columns, "{sql}");
            }
        }
    }
    assert!(parse("SELECT * FROM t1, LATERAL (SELECT t1.a)").is_err());
}

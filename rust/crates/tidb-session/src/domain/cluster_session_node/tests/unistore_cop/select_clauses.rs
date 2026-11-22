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

//! The clauses a SELECT carries -- the expression list, WHERE, ORDER BY,
//! LIMIT, HAVING -- as the executor's in-memory tier used to check them, over
//! the cop-backed TestKit stack (X11).

use super::*;

fn session(stack: &UnistoreClusterStack, connection: u64) -> Session {
    let mut session = stack.factory.open_session(session_context(connection)).expect("session opens");
    rows(&mut session, "use test");
    session
}

fn q(tk: &mut Session, sql: &str) -> Vec<Vec<String>> {
    displayed(rows(tk, sql))
}

fn error(tk: &mut Session, sql: &str) -> tidb_store_driver_error::sql_query_error::SqlQueryError {
    tk.node_conn().execute(sql).map(|_| ()).expect_err(sql)
}

#[test]
fn select_constant_arithmetic() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = session(&stack, 500);
    assert_eq!(q(&mut tk, "SELECT 1 + 1"), [["2"]]);
    assert_eq!(q(&mut tk, "SELECT 1 + 1, 2 * 3"), [["2", "6"]]);
    assert_eq!(q(&mut tk, "SELECT 2 * 3 - 1"), [["5"]]);
}

#[test]
fn select_with_where() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = session(&stack, 501);
    assert_eq!(q(&mut tk, "SELECT 42 WHERE 1 = 1"), [["42"]]);
    assert!(q(&mut tk, "SELECT 42 WHERE 1 = 0").is_empty());
}

/// An explicit case-insensitive collation decides `=`, `IN` (in WHERE and in
/// HAVING) and a wildcard-free `LIKE` over a `utf8mb4_bin` column, on a
/// range-partitioned table and on a plain one.
#[test]
fn an_explicit_case_insensitive_collation_controls_comparison() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = session(&stack, 502);
    rows(
        &mut tk,
        "CREATE TABLE collated (a VARCHAR(10) COLLATE utf8mb4_bin) PARTITION BY RANGE COLUMNS(a) (PARTITION p_upper VALUES LESS THAN ('a'), PARTITION p_lower VALUES LESS THAN (MAXVALUE))",
    );
    rows(&mut tk, "INSERT INTO collated VALUES ('AA'), ('aa'), ('AAA'), ('aaa')");
    rows(&mut tk, "CREATE TABLE plain (a VARCHAR(10) COLLATE utf8mb4_bin)");
    rows(&mut tk, "INSERT INTO plain VALUES ('AA'), ('aa'), ('AAA'), ('aaa')");
    let values = |tk: &mut Session, sql: &str| {
        let mut values: Vec<String> = q(tk, sql).into_iter().map(|row| row[0].clone()).collect();
        values.sort();
        values
    };
    assert_eq!(values(&mut tk, "SELECT a FROM collated WHERE a = 'AA' COLLATE utf8mb4_general_ci"), ["AA", "aa"]);
    assert_eq!(
        values(&mut tk, "SELECT a FROM collated WHERE a IN ('AAA' COLLATE utf8mb4_general_ci, 'aa')"),
        ["AA", "AAA", "aa", "aaa"]
    );
    assert_eq!(
        values(&mut tk, "SELECT a FROM collated HAVING a IN ('AAA' COLLATE utf8mb4_general_ci, 'aa')"),
        ["AA", "AAA", "aa", "aaa"]
    );
    for table in ["collated", "plain"] {
        assert_eq!(
            values(&mut tk, &format!("SELECT a FROM {table} WHERE a LIKE 'aa' COLLATE utf8mb4_general_ci")),
            ["AA", "aa"],
            "{table}"
        );
    }
}

/// Row-valued `IN` lowers each tuple equality column by column with SQL's
/// three-valued NULL behavior.
#[test]
fn row_in_matches_go_tuple_and_null_semantics() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = session(&stack, 503);
    rows(&mut tk, "CREATE TABLE stock (id INT PRIMARY KEY, w_id INT, i_id INT)");
    rows(
        &mut tk,
        "INSERT INTO stock VALUES (1, 1, 1), (2, 2, 2), (3, 1, 3), (4, 1, NULL), (5, 2, NULL), (6, NULL, 2), (7, NULL, NULL), (8, 3, NULL)",
    );
    assert_eq!(
        q(&mut tk, "SELECT id FROM stock WHERE (w_id, i_id) IN ((1, 1), (2, 2)) ORDER BY id FOR UPDATE"),
        [["1"], ["2"]]
    );
    assert_eq!(q(&mut tk, "SELECT id FROM stock WHERE (w_id, i_id) NOT IN ((1, 1), (2, 2)) ORDER BY id"), [["3"], ["8"]]);
}

#[test]
fn row_in_rejects_a_different_column_count_like_go() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = session(&stack, 504);
    let error = error(&mut tk, "SELECT (1, 2) IN ((1, 2, 3))");
    assert_eq!((error.code, error.message.as_str()), (1241, "Operand should contain 2 column(s)"));
}

#[test]
fn limit_and_order_by_wire_up() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = session(&stack, 505);
    assert_eq!(q(&mut tk, "SELECT 42 LIMIT 1"), [["42"]]);
    assert!(q(&mut tk, "SELECT 42 LIMIT 0").is_empty());
    assert!(q(&mut tk, "SELECT 42 LIMIT 1, 1").is_empty());
    assert_eq!(q(&mut tk, "SELECT 42 ORDER BY 1 DESC"), [["42"]]);
}

/// ORDER BY a column that is not projected, with and without LIMIT.
#[test]
fn select_from_table_order_limit() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = session(&stack, 506);
    rows(&mut tk, "CREATE TABLE t (a BIGINT, b BIGINT)");
    rows(&mut tk, "INSERT INTO t VALUES (1, 30), (2, 20), (3, 10)");
    assert_eq!(q(&mut tk, "SELECT a FROM t ORDER BY b"), [["3"], ["2"], ["1"]]);
    assert_eq!(q(&mut tk, "SELECT a FROM t ORDER BY b DESC LIMIT 2"), [["1"], ["2"]]);
}

/// HAVING on the non-aggregate path filters and resolves names against the
/// select list only (Go `resolveHavingAndOrderBy`): an alias shadows a column,
/// and a source column the list does not project is 1054.
#[test]
fn plain_having_filters_and_sees_only_the_select_list() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = session(&stack, 507);
    rows(&mut tk, "CREATE TABLE ht (a INT, b INT)");
    rows(&mut tk, "INSERT INTO ht VALUES (1, 10), (2, 20)");
    assert_eq!(q(&mut tk, "SELECT a, b FROM ht HAVING b > 15"), [["2", "20"]]);
    assert_eq!(q(&mut tk, "SELECT a FROM ht HAVING a > 1"), [["2"]]);
    assert_eq!(q(&mut tk, "SELECT * FROM ht HAVING b > 15"), [["2", "20"]]);
    assert_eq!(q(&mut tk, "SELECT a FROM ht HAVING 1"), [["1"], ["2"]]);
    assert!(q(&mut tk, "SELECT a FROM ht HAVING 0").is_empty());
    assert!(q(&mut tk, "SELECT a FROM ht HAVING NULL").is_empty());
    assert_eq!(q(&mut tk, "SELECT a, b FROM ht HAVING b > 15 LIMIT 1"), [["2", "20"]]);
    assert_eq!(q(&mut tk, "SELECT a, b FROM ht HAVING a > 0 ORDER BY b DESC"), [["2", "20"], ["1", "10"]]);
    assert_eq!(q(&mut tk, "SELECT b AS a FROM ht HAVING a > 15"), [["20"]]);
    assert_eq!(q(&mut tk, "SELECT a+1 AS a FROM ht HAVING a > 2"), [["3"]]);
    assert_eq!(q(&mut tk, "SELECT a AS z, b FROM ht HAVING z > 1"), [["2", "20"]]);
    assert_eq!(q(&mut tk, "SELECT 1 AS one FROM ht HAVING one = 1"), [["1"], ["1"]]);
    assert_eq!(q(&mut tk, "SELECT a FROM ht t1 HAVING t1.a > 1"), [["2"]]);
    for (sql, name) in [
        ("SELECT a FROM ht HAVING b > 0", "b"),
        ("SELECT a FROM ht HAVING ht.b > 0", "ht.b"),
        ("SELECT a FROM ht HAVING b IS NULL", "b"),
        ("SELECT a FROM ht HAVING b > 0 AND a > 0", "b"),
        ("SELECT b AS a FROM ht HAVING ht.a > 1", "ht.a"),
        ("SELECT a FROM ht t1 HAVING ht.a > 1", "ht.a"),
    ] {
        let error = error(&mut tk, sql);
        assert_eq!(error.code, 1054, "{sql}");
        assert_eq!(error.message, format!("Unknown column '{name}' in 'having clause'"), "{sql}");
    }
}

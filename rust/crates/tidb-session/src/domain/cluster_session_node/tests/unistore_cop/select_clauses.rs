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

use super::sql_kit::exec_all;
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

fn having_tables(tk: &mut Session) {
    exec_all(tk, &[
        "CREATE TABLE ht (a INT, b INT)",
        "CREATE TABLE hs (x INT, y INT)",
        "INSERT INTO ht VALUES (1, 10), (2, 20)",
        "INSERT INTO hs VALUES (10, 5)",
    ]);
}

/// An empty correlated scalar subquery in HAVING is NULL and drops its row;
/// HAVING resolves select-list names and refuses columns the select list
/// does not carry (Go's 1054 in 'having clause').
#[test]
fn an_empty_correlated_having_subquery_is_null_and_drops_its_row() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = session(&stack, 630);
    having_tables(&mut tk);
    let first = vec![vec!["1".to_owned(), "10".to_owned()]];
    let second = vec![vec!["2".to_owned(), "20".to_owned()]];
    let none = Vec::<Vec<String>>::new();
    for (sql, want) in [
        ("SELECT a, b FROM ht HAVING (SELECT y FROM hs WHERE hs.x = ht.b) > 0", &first),
        ("SELECT a, b FROM ht HAVING (SELECT y FROM hs WHERE hs.x = ht.b) = 5", &first),
        ("SELECT a, b FROM ht HAVING (SELECT y FROM hs WHERE hs.x = ht.b) <> 5", &none),
        ("SELECT a, b FROM ht HAVING (SELECT y FROM hs WHERE hs.x = ht.b) > 100", &none),
        ("SELECT a, b FROM ht HAVING (SELECT y FROM hs WHERE hs.x = ht.b) IS NULL", &second),
        ("SELECT a, b FROM ht HAVING (SELECT y FROM hs WHERE hs.x = ht.b) IS NOT NULL", &first),
        ("SELECT a, b FROM ht HAVING (SELECT count(*) FROM hs WHERE hs.x = ht.b) > 0", &first),
        ("SELECT a, b FROM ht HAVING EXISTS (SELECT 1 FROM hs WHERE hs.x = ht.b)", &first),
        ("SELECT a, b FROM ht HAVING NOT EXISTS (SELECT 1 FROM hs WHERE hs.x = ht.b)", &second),
        ("SELECT a, b FROM ht HAVING ht.b IN (SELECT x FROM hs)", &first),
        ("SELECT a, b FROM ht HAVING (SELECT y FROM hs WHERE hs.x = b) > 0", &first),
        ("SELECT a, b FROM ht HAVING (SELECT y FROM hs WHERE hs.x = ht.a) > 0", &none),
    ] {
        assert_eq!(&displayed(rows(&mut tk, sql)), want, "{sql}");
    }
    assert_eq!(displayed(rows(&mut tk, "SELECT a FROM ht HAVING (SELECT count(*) FROM hs) > 0")), [["1"], ["2"]]);
    assert_eq!(displayed(rows(&mut tk, "SELECT b AS bb FROM ht HAVING (SELECT y FROM hs WHERE hs.x = bb) > 0")), [["10"]]);
    for (sql, name) in [
        ("SELECT a FROM ht HAVING (SELECT y FROM hs WHERE hs.x = ht.b) > 0", "ht.b"),
        ("SELECT a FROM ht HAVING (SELECT y FROM hs WHERE hs.x = b) > 0", "b"),
        ("SELECT a FROM ht HAVING EXISTS (SELECT 1 FROM hs WHERE hs.x = ht.b)", "ht.b"),
        ("SELECT b AS bb FROM ht HAVING (SELECT y FROM hs WHERE hs.x = ht.b) > 0", "ht.b"),
    ] {
        let refused = error(&mut tk, sql);
        assert_eq!((refused.code, refused.message), (1054, format!("Unknown column '{name}' in 'having clause'")), "{sql}");
    }
}

/// The HAVING subquery's value is not an extra result column.
#[test]
fn a_having_scalar_subquery_does_not_leak_its_value_as_a_result_column() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = session(&stack, 631);
    having_tables(&mut tk);
    assert_eq!(displayed(rows(&mut tk, "SELECT a, b FROM ht HAVING (SELECT y FROM hs WHERE hs.x = ht.b) > 0")), [["1", "10"]]);
}

/// Go `TableSampleExecutor` over REGIONS: the first row of each region
/// (each partition is its own), filters after sampling, and 8128 for every
/// other method.
#[test]
fn table_sample_regions_uses_the_ordinary_physical_executor_path() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = session(&stack, 632);
    exec_all(&mut tk, &[
        "CREATE TABLE smp (a BIGINT PRIMARY KEY)",
        "INSERT INTO smp VALUES (1), (2), (3)",
        "CREATE TABLE empty_sample (a BIGINT)",
        "CREATE TABLE partition_sample (a BIGINT) PARTITION BY RANGE (a) (PARTITION p0 VALUES LESS THAN (10), PARTITION p1 VALUES LESS THAN (20))",
        "INSERT INTO partition_sample VALUES (2), (1), (12), (11)",
    ]);
    assert_eq!(displayed(rows(&mut tk, "SELECT a FROM smp TABLESAMPLE REGIONS()")), [["1"]]);
    assert!(rows(&mut tk, "SELECT a FROM smp TABLESAMPLE REGIONS() WHERE a = 2").is_empty());
    assert_eq!(displayed(rows(&mut tk, "SELECT count(*) FROM smp TABLESAMPLE REGIONS()")), [["1"]]);
    assert!(rows(&mut tk, "SELECT a FROM empty_sample TABLESAMPLE REGIONS()").is_empty());
    assert_eq!(displayed(rows(&mut tk, "SELECT a FROM partition_sample TABLESAMPLE REGIONS() ORDER BY a")), [["2"], ["12"]]);
    assert_eq!(displayed(rows(&mut tk, "SELECT a FROM partition_sample PARTITION (p1) TABLESAMPLE REGIONS()")), [["12"]]);
    for sql in ["SELECT a FROM smp TABLESAMPLE BERNOULLI (10 PERCENT)", "SELECT a FROM smp TABLESAMPLE SYSTEM (2 ROWS) REPEATABLE(7)"] {
        let refused = error(&mut tk, sql);
        assert_eq!((refused.code, refused.message.as_str()), (8128, "Invalid TABLESAMPLE: Only supports REGIONS sampling method"), "{sql}");
    }
    // Go `TestValidator`'s REGIONS rows (8b89a749233): REGIONS takes no
    // sample size and no REPEATABLE seed, as a statement or when prepared.
    for sql in [
        "SELECT a FROM smp TABLESAMPLE REGIONS(0 ROWS)",
        "SELECT a FROM smp TABLESAMPLE REGIONS(0 PERCENT)",
        "SELECT a FROM smp TABLESAMPLE REGIONS(100 PERCENT)",
        "SELECT a FROM smp TABLESAMPLE REGIONS(1)",
        "SELECT a FROM smp TABLESAMPLE REGIONS() REPEATABLE (0)",
        "SELECT a FROM smp TABLESAMPLE REGIONS(1 ROWS) REPEATABLE (1)",
        "PREPARE sample_stmt FROM 'SELECT a FROM smp TABLESAMPLE REGIONS(? ROWS)'",
        "PREPARE sample_stmt FROM 'SELECT a FROM smp TABLESAMPLE REGIONS() REPEATABLE (?)'",
    ] {
        let refused = error(&mut tk, sql);
        assert_eq!(
            (refused.code, refused.message.as_str()),
            (8128, "Invalid TABLESAMPLE: REGIONS sampling does not support sample size or REPEATABLE"),
            "{sql}"
        );
    }
    assert_eq!(displayed(rows(&mut tk, "SELECT a FROM smp")), [["1"], ["2"], ["3"]]);
}

/// Go's 1054 names the clause the unknown column appeared in.
#[test]
fn an_unknown_column_names_its_clause() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = session(&stack, 633);
    rows(&mut tk, "CREATE TABLE uc (a INT)");
    for (sql, column, clause) in [
        ("SELECT no_col FROM uc", "no_col", "field list"),
        ("SELECT a FROM uc WHERE nc = 1", "nc", "where clause"),
        ("SELECT uc.nc FROM uc", "uc.nc", "field list"),
        ("SELECT count(*) FROM uc GROUP BY no_col", "no_col", "group statement"),
        ("SELECT a FROM uc GROUP BY a, no_col", "no_col", "group statement"),
        ("SELECT a FROM uc GROUP BY t11.c1, a", "t11.c1", "group statement"),
        ("SELECT count(*) FROM uc WHERE nc = 1", "nc", "where clause"),
    ] {
        let refused = error(&mut tk, sql);
        assert_eq!((refused.code, refused.message), (1054, format!("Unknown column '{column}' in '{clause}'")), "{sql}");
    }
}

/// A derived table over an aggregate answers the aggregate's output type.
#[test]
fn a_derived_aggregate_answers_its_output() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = session(&stack, 634);
    exec_all(&mut tk, &[
        "CREATE TABLE dt (id BIGINT PRIMARY KEY, g BIGINT, v BIGINT)",
        "INSERT INTO dt VALUES (1,1,10),(2,1,20),(3,2,30),(4,2,40),(5,2,50)",
    ]);
    let sums = rows(&mut tk, "SELECT * FROM (SELECT g, sum(v) AS t FROM dt GROUP BY g) s ORDER BY g");
    assert!(sums.iter().all(|row| matches!(row[1], Datum::Decimal(_))), "{sums:?}");
    assert_eq!(displayed(sums), [["1", "30"], ["2", "120"]]);
    assert_eq!(rows(&mut tk, "SELECT * FROM (SELECT g, count(*) AS t FROM dt GROUP BY g) s ORDER BY g"), vec![vec![Datum::Int(1), Datum::Int(2)], vec![Datum::Int(2), Datum::Int(3)]]);
}

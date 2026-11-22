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

//! `UNION` / `INTERSECT` / `EXCEPT` and common table expressions the
//! executor's in-memory tier used to check, over the cop-backed TestKit stack
//! (X11).

use super::sql_kit::{exec_all, explain_brief};
use super::*;

fn session(stack: &UnistoreClusterStack, connection: u64) -> Session {
    let mut session = stack.factory.open_session(session_context(connection)).expect("session opens");
    rows(&mut session, "use test");
    session
}

fn ints(tk: &mut Session, sql: &str) -> Vec<i64> {
    displayed(rows(tk, sql)).into_iter().map(|row| row[0].parse().unwrap()).collect()
}

fn sorted(tk: &mut Session, sql: &str) -> Vec<i64> {
    let mut values = ints(tk, sql);
    values.sort_unstable();
    values
}

fn explain(tk: &mut Session, sql: &str) -> Vec<Vec<String>> {
    displayed(rows(tk, &format!("explain format = 'brief' {sql}")))
}

/// Non-recursive CTEs resolve like tables, a column list renames, a later CTE
/// reads an earlier one, a CTE shadows a table, WITH RECURSIVE runs its
/// fixpoint, and a mismatched column list is an error.
#[test]
fn common_table_expressions() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = session(&stack, 492);
    rows(&mut tk, "CREATE TABLE c1 (a BIGINT, b BIGINT)");
    rows(&mut tk, "INSERT INTO c1 VALUES (1, 10), (2, 20), (3, 30)");
    assert_eq!(sorted(&mut tk, "WITH c AS (SELECT a FROM c1 WHERE a > 1) SELECT a FROM c"), [2, 3]);
    assert_eq!(displayed(rows(&mut tk, "WITH c AS (SELECT a, b FROM c1) SELECT SUM(b) FROM c WHERE a >= 2")), [["50"]]);
    assert_eq!(ints(&mut tk, "WITH c (x) AS (SELECT a FROM c1 WHERE a = 3) SELECT x FROM c"), [3]);
    assert_eq!(
        ints(&mut tk, "WITH c AS (SELECT a FROM c1 WHERE a > 1), d AS (SELECT a FROM c WHERE a > 2) SELECT a FROM d"),
        [3]
    );
    assert_eq!(ints(&mut tk, "WITH c AS (SELECT a FROM c1 WHERE a = 2) SELECT c1.b FROM c JOIN c1 ON c.a = c1.a"), [20]);
    assert_eq!(ints(&mut tk, "WITH c1 AS (SELECT 9 AS a) SELECT a FROM c1"), [9]);
    assert_eq!(
        ints(&mut tk, "WITH RECURSIVE c (n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM c WHERE n < 3) SELECT n FROM c"),
        [1, 2, 3]
    );
    assert!(tk.node_conn().execute("WITH c (x, y) AS (SELECT a FROM c1) SELECT x FROM c").is_err());
}

/// UNION dedups and UNION ALL keeps duplicates, EXCEPT and INTERSECT are
/// DISTINCT, their ALL forms are refused as Go refuses them, ORDER BY and
/// LIMIT apply to the folded result, terms fold left to right, a CTE prefix
/// belongs to the statement, and a term of another width is 1222.
#[test]
fn set_operations() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = session(&stack, 493);
    rows(&mut tk, "CREATE TABLE u1 (a BIGINT)");
    rows(&mut tk, "CREATE TABLE u2 (a BIGINT)");
    rows(&mut tk, "INSERT INTO u1 VALUES (1), (2), (2), (3)");
    rows(&mut tk, "INSERT INTO u2 VALUES (2), (3), (4)");
    assert_eq!(sorted(&mut tk, "SELECT a FROM u1 UNION SELECT a FROM u2"), [1, 2, 3, 4]);
    assert_eq!(sorted(&mut tk, "SELECT a FROM u1 UNION ALL SELECT a FROM u2"), [1, 2, 2, 2, 3, 3, 4]);
    assert_eq!(ints(&mut tk, "SELECT a FROM u1 EXCEPT SELECT a FROM u2"), [1]);
    assert_eq!(sorted(&mut tk, "SELECT a FROM u1 INTERSECT SELECT a FROM u2"), [2, 3]);
    for (sql, message) in [
        ("SELECT a FROM u1 INTERSECT ALL SELECT a FROM u2", "TiDB do not support intersect all"),
        ("SELECT a FROM u1 EXCEPT ALL SELECT a FROM u2", "TiDB do not support except all"),
    ] {
        let error = tk.node_conn().execute(sql).map(|_| ()).expect_err(sql);
        assert!(error.message.contains(message), "{sql}: {}", error.message);
    }
    assert_eq!(ints(&mut tk, "SELECT a FROM u1 UNION SELECT a FROM u2 ORDER BY a DESC"), [4, 3, 2, 1]);
    assert_eq!(ints(&mut tk, "SELECT a FROM u1 UNION SELECT a FROM u2 ORDER BY a LIMIT 2"), [1, 2]);
    assert_eq!(sorted(&mut tk, "SELECT a FROM u1 UNION SELECT a FROM u2 UNION SELECT 9"), [1, 2, 3, 4, 9]);
    assert_eq!(
        sorted(&mut tk, "WITH c AS (SELECT a FROM u1 WHERE a = 3) SELECT a FROM c UNION SELECT a FROM u2"),
        [2, 3, 4]
    );
    let error = tk.node_conn().execute("SELECT a FROM u1 UNION SELECT a, a FROM u2").map(|_| ()).unwrap_err();
    assert_eq!(error.code, 1222, "{}", error.message);
}

/// Go `buildIntersect` / `buildExcept`: the first input is deduplicated and
/// every later input is the build side of a left-deep (anti) semi join with
/// NULL-safe equality; EXPLAIN ANALYZE counts each fold's output.
#[test]
fn intersect_and_except_explain_as_go_semi_join_chains() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = session(&stack, 494);
    for table in ["sx1", "sx2", "sx3"] {
        rows(&mut tk, &format!("CREATE TABLE {table} (a BIGINT)"));
    }
    rows(&mut tk, "INSERT INTO sx1 VALUES (1),(2),(2),(3)");
    rows(&mut tk, "INSERT INTO sx2 VALUES (2),(3),(4)");
    rows(&mut tk, "INSERT INTO sx3 VALUES (3),(4),(5)");
    let intersect = "SELECT a FROM sx1 INTERSECT SELECT a FROM sx2 INTERSECT SELECT a FROM sx3";
    let plan = explain(&mut tk, intersect);
    let joins: Vec<_> = plan.iter().filter(|row| row[0].contains("HashJoin")).collect();
    assert_eq!(joins.len(), 2, "{plan:#?}");
    assert!(joins[0][4].starts_with("semi join, left side:HashJoin"), "{plan:#?}");
    assert!(joins[1][4].starts_with("semi join, left side:HashAgg"), "{plan:#?}");
    assert!(joins.iter().all(|row| row[4].contains("equal:[nulleq(")), "{plan:#?}");
    let analyzed = displayed(rows(&mut tk, &format!("explain analyze format = 'brief' {intersect}")));
    let act_rows: Vec<&str> = analyzed.iter().filter(|row| row[0].contains("HashJoin")).map(|row| row[2].as_str()).collect();
    assert_eq!(act_rows, ["1", "2"], "{analyzed:#?}");
    let plan = explain(&mut tk, "SELECT a FROM sx1 EXCEPT SELECT a FROM sx2 EXCEPT SELECT a FROM sx3");
    let joins: Vec<_> = plan.iter().filter(|row| row[0].contains("HashJoin")).collect();
    assert_eq!(joins.len(), 2, "{plan:#?}");
    assert!(joins.iter().all(|row| row[4].starts_with("anti semi join, left side:")), "{plan:#?}");
}

/// Go's outer-join eliminator gets the duplicate-agnostic columns of a
/// UNION DISTINCT's HashAgg, so an unread non-unique inner side disappears
/// there; UNION ALL keeps it.
#[test]
fn union_distinct_propagates_duplicate_agnostic_outer_join_elimination() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = session(&stack, 495);
    rows(&mut tk, "CREATE TABLE ul (id BIGINT PRIMARY KEY)");
    rows(&mut tk, "CREATE TABLE ur (k BIGINT)");
    rows(&mut tk, "INSERT INTO ul VALUES (1),(2)");
    rows(&mut tk, "INSERT INTO ur VALUES (1),(1)");
    let mentions_inner = |tk: &mut Session, sql: &str| {
        displayed(rows(tk, &format!("explain format = 'row' {sql}"))).iter().flatten().any(|cell| cell.contains("table:ur"))
    };
    assert!(!mentions_inner(&mut tk, "SELECT id FROM ul UNION SELECT ul.id FROM ul LEFT JOIN ur ON ul.id = ur.k"));
    assert!(mentions_inner(&mut tk, "SELECT id FROM ul UNION ALL SELECT ul.id FROM ul LEFT JOIN ur ON ul.id = ur.k"));
}

/// A UNION DISTINCT fixpoint deduplicates against one accumulating hash
/// table (Go `cteProducer.hashTbl`): this 100000-round recursion finishes.
#[test]
fn a_distinct_fixpoint_dedups_incrementally() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = session(&stack, 496);
    rows(&mut tk, "set @@cte_max_recursion_depth = 200000");
    assert_eq!(
        displayed(rows(
            &mut tk,
            "WITH RECURSIVE cte(a,b) AS (SELECT 1,1 UNION SELECT a+1,b+1 FROM cte WHERE cte.a < 100000) SELECT count(*), max(a), min(a) FROM cte",
        )),
        [["100000", "100000", "1"]]
    );
}

/// A recursive CTE's schema is the seed's, and every recursive block is cast
/// into it (Go `buildProjection4CTEUnion`).
#[test]
fn a_recursive_block_is_cast_into_the_seed_schema() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = session(&stack, 497);
    assert_eq!(
        displayed(rows(
            &mut tk,
            "WITH RECURSIVE t (a,b,c,d) AS ( SELECT 1, 2, 3, 4 UNION SELECT a + 1, b + 1, c + 1, concat(d, 1) FROM t WHERE a < 5 ) SELECT * FROM t",
        )),
        [
            ["1", "2", "3", "4"],
            ["2", "3", "4", "41"],
            ["3", "4", "5", "411"],
            ["4", "5", "6", "4111"],
            ["5", "6", "7", "41111"],
        ]
    );
    assert_eq!(
        displayed(rows(
            &mut tk,
            "WITH RECURSIVE t (a,b) AS ( SELECT 1, cast(2 AS CHAR(20)) UNION ALL SELECT a + 1, concat(b, 'x') FROM t WHERE a < 3 ) SELECT * FROM t",
        ))
        .into_iter()
        .map(|row| row[1].clone())
        .collect::<Vec<_>>(),
        ["2", "2x", "2xx"]
    );
    assert_eq!(
        displayed(rows(
            &mut tk,
            "WITH RECURSIVE t (a,b) AS ( SELECT 1, 2 UNION ALL SELECT a + 1, 'zz' FROM t WHERE a < 3 ) SELECT * FROM t",
        )),
        [["1", "2"], ["2", "0"], ["3", "0"]]
    );
}

/// A single-use CTE is inlined and keeps the base table's analyzed
/// statistics; a CTE used twice still answers its rows.
#[test]
fn single_use_cte_explain_keeps_base_statistics_and_multiple_uses_materialize() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = session(&stack, 622);
    exec_all(&mut tk, &[
        "CREATE TABLE warehouse (w_id BIGINT, w_name VARCHAR(16))",
        "INSERT INTO warehouse VALUES (1, 'one'), (2, 'two'), (3, 'three')",
        "ANALYZE TABLE warehouse",
    ]);
    let plan = explain_brief(&mut tk, "WITH w AS (SELECT w_id, w_name FROM warehouse) SELECT w_id FROM w");
    let scan = plan.iter().find(|row| row[0].contains("TableFullScan")).expect("a table scan");
    assert_eq!((scan[1].as_str(), scan[3].as_str()), ("3.00", "table:warehouse"), "{plan:?}");
    assert!(!scan[4].contains("stats:pseudo"), "{plan:?}");
    assert_eq!(
        displayed(rows(&mut tk, "WITH w AS (SELECT w_id FROM warehouse) SELECT x.w_id FROM w AS x JOIN w AS y ON x.w_id = y.w_id ORDER BY x.w_id")),
        [["1"], ["2"], ["3"]]
    );
}

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

//! Aggregate behavior the executor's in-memory tier used to check, over the
//! cop-backed TestKit stack (X11).

use super::*;

fn session(stack: &UnistoreClusterStack, connection: u64) -> Session {
    let mut session = stack.factory.open_session(session_context(connection)).expect("session opens");
    rows(&mut session, "use test");
    session
}

/// SUM and AVG over a FLOAT column compute in the real domain.
#[test]
fn float_sum_and_avg_use_the_real_domain() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = session(&stack, 470);
    rows(&mut tk, "CREATE TABLE f (v FLOAT)");
    rows(&mut tk, "INSERT INTO f VALUES (1.25), (2.5), (NULL)");
    assert_eq!(displayed(rows(&mut tk, "SELECT SUM(v), AVG(v) FROM f")), [["3.75", "1.875"]]);
}

/// MAX and MIN over a clustered key column that the secondary index does not
/// rank answer the true extremes (an unproven bounded reverse index read
/// answered 7).
#[test]
fn max_min_over_a_clustered_column_the_index_does_not_rank_answers_the_true_extreme() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = session(&stack, 471);
    rows(
        &mut tk,
        "CREATE TABLE ox (a VARCHAR(3) NOT NULL, b VARCHAR(8) NOT NULL, c INT NOT NULL, d VARCHAR(2), PRIMARY KEY(a,b,c) CLUSTERED, KEY k4(a,b,d))",
    );
    rows(&mut tk, "INSERT INTO ox VALUES ('a01','20220101',3,'x'),('a02','20220102',42,'y'),('a03','20220103',7,'z')");
    assert_eq!(displayed(rows(&mut tk, "SELECT MAX(c), MIN(c) FROM ox")), [["42", "3"]]);
}

/// A MAX over a derived SUM compares the materialized coprocessor TopN keys.
#[test]
fn max_over_a_derived_sum_materializes_coprocessor_topn_keys() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = session(&stack, 472);
    rows(&mut tk, "CREATE TABLE topn_cast (id BIGINT PRIMARY KEY, v BIGINT)");
    rows(&mut tk, "INSERT INTO topn_cast VALUES (1,7),(2,42),(3,9),(4,NULL)");
    assert_eq!(
        displayed(rows(
            &mut tk,
            "SELECT id FROM (SELECT id,SUM(v) AS total FROM topn_cast GROUP BY id) d WHERE total=(SELECT MAX(total) FROM (SELECT id,SUM(v) AS total FROM topn_cast GROUP BY id) x)",
        )),
        [["2"]]
    );
}

/// A grouped SUM over an indexed column answers one row per group.
#[test]
fn grouped_aggregation_enumerates_families_over_one_planned_child() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = session(&stack, 473);
    rows(&mut tk, "CREATE TABLE shared_child (id INT PRIMARY KEY, k INT, INDEX idx_k(k))");
    rows(&mut tk, "INSERT INTO shared_child VALUES (1, 10), (2, 10), (3, 20)");
    let mut answer = displayed(rows(&mut tk, "SELECT k, SUM(id) FROM shared_child GROUP BY k"));
    answer.sort();
    assert_eq!(answer, [["10", "3"], ["20", "3"]]);
}

/// BIT_AND / BIT_OR / BIT_XOR fold in the unsigned domain: an all-NULL input
/// is `18446744073709551615 0 0`, and `-1` inputs read as all bits set.
#[test]
fn bit_aggregates_are_unsigned() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = session(&stack, 474);
    let query = "SELECT BIT_AND(v), BIT_OR(v), BIT_XOR(v) FROM b";
    for (values, expected) in [
        ("(NULL)", ["18446744073709551615", "0", "0"]),
        ("(-1),(-1)", ["18446744073709551615", "18446744073709551615", "0"]),
        ("(3),(5),(NULL)", ["1", "7", "6"]),
    ] {
        rows(&mut tk, "DROP TABLE IF EXISTS b");
        rows(&mut tk, "CREATE TABLE b (v BIGINT)");
        rows(&mut tk, &format!("INSERT INTO b VALUES {values}"));
        assert_eq!(displayed(rows(&mut tk, query)), [expected], "{values}");
    }
}

/// Aggregates read the collation of what they aggregate: MIN/MAX, DISTINCT
/// over a column and over computed strings, and GROUP_CONCAT's own ORDER BY
/// and DISTINCT, on a `utf8mb4_general_ci` column.
#[test]
fn aggregates_read_the_arguments_collation() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = session(&stack, 475);
    let with = |tk: &mut Session, values: &str| {
        rows(tk, "DROP TABLE IF EXISTS g");
        rows(tk, "CREATE TABLE g (s VARCHAR(10) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci)");
        rows(tk, &format!("INSERT INTO g VALUES {values}"));
    };
    with(&mut tk, "('a'), ('B'), ('A')");
    assert_eq!(displayed(rows(&mut tk, "SELECT MAX(s), MIN(s) FROM g")), [["B", "a"]]);
    assert_eq!(
        displayed(rows(&mut tk, "SELECT COUNT(DISTINCT s), COUNT(DISTINCT CONCAT(s, '')), COUNT(DISTINCT UPPER(s)) FROM g")),
        [["2", "2", "2"]]
    );
    with(&mut tk, "('B'), ('a')");
    assert_eq!(displayed(rows(&mut tk, "SELECT GROUP_CONCAT(s ORDER BY s) FROM g")), [["a,B"]]);
    assert_eq!(displayed(rows(&mut tk, "SELECT GROUP_CONCAT(s ORDER BY s DESC) FROM g")), [["B,a"]]);
    with(&mut tk, "('b'), ('A'), ('a'), ('B')");
    // Go keeps each collation key's first value and joins its map in
    // iteration order: compare as a set.
    let joined = displayed(rows(&mut tk, "SELECT GROUP_CONCAT(DISTINCT s) FROM g")).remove(0).remove(0);
    let mut values: Vec<&str> = joined.split(',').collect();
    values.sort_unstable();
    assert_eq!(values, ["A", "b"]);
}

/// Global and grouped aggregates over `t (a, b)` = (1,30),(2,20),(3,10):
/// SUM and AVG over integers are DECIMAL, empty inputs give one global row
/// and no groups, DISTINCT folds per group, and empty MIN/MAX/AVG are NULL.
#[test]
fn aggregate_selects() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = session(&stack, 476);
    rows(&mut tk, "CREATE TABLE t (a BIGINT, b BIGINT)");
    rows(&mut tk, "INSERT INTO t VALUES (1, 30), (2, 20), (3, 10)");
    let q = |tk: &mut Session, sql: &str| displayed(rows(tk, sql));
    assert_eq!(q(&mut tk, "SELECT COUNT(*), SUM(a) FROM t"), [["3", "6"]]);
    let mut groups = q(&mut tk, "SELECT a, COUNT(*) FROM t WHERE b >= 20 GROUP BY a");
    groups.sort();
    assert_eq!(groups, [["1", "1"], ["2", "1"]]);
    assert_eq!(q(&mut tk, "SELECT COUNT(a) FROM t WHERE a > 100"), [["0"]]);
    assert!(rows(&mut tk, "SELECT a, COUNT(*) FROM t WHERE a > 100 GROUP BY a").is_empty());
    assert_eq!(q(&mut tk, "SELECT MIN(a), MAX(b) FROM t"), [["1", "30"]]);
    assert_eq!(q(&mut tk, "SELECT AVG(a) FROM t"), [["2.0000"]]);
    assert_eq!(q(&mut tk, "SELECT COUNT(DISTINCT a), COUNT(DISTINCT 1) FROM t"), [["3", "1"]]);
    assert_eq!(q(&mut tk, "SELECT MIN(a), MAX(a), AVG(a) FROM t WHERE a > 100"), [["NULL", "NULL", "NULL"]]);
}

/// SELECT DISTINCT is an aggregation over every projected column. Go's
/// serial HashAgg (both hash-agg concurrencies 1) emits groups in first-seen
/// order, which the order-sensitive checks pin.
#[test]
fn select_distinct() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = session(&stack, 477);
    rows(&mut tk, "SET tidb_hashagg_partial_concurrency = 1");
    rows(&mut tk, "SET tidb_hashagg_final_concurrency = 1");
    rows(&mut tk, "CREATE TABLE d2 (a BIGINT, b BIGINT)");
    rows(&mut tk, "INSERT INTO d2 VALUES (1, 1), (1, 2), (1, 1), (2, 2)");
    let q = |tk: &mut Session, sql: &str| displayed(rows(tk, sql));
    assert_eq!(q(&mut tk, "SELECT DISTINCT a FROM d2"), [["1"], ["2"]]);
    assert_eq!(q(&mut tk, "SELECT DISTINCT a, b FROM d2"), [["1", "1"], ["1", "2"], ["2", "2"]]);
    assert_eq!(rows(&mut tk, "SELECT a FROM d2").len(), 4);
    assert_eq!(q(&mut tk, "SELECT DISTINCT a + b FROM d2"), [["2"], ["3"], ["4"]]);
    assert_eq!(q(&mut tk, "SELECT DISTINCT a FROM d2 ORDER BY a DESC"), [["2"], ["1"]]);
    assert_eq!(q(&mut tk, "SELECT DISTINCT a FROM d2 LIMIT 1"), [["1"]]);
    assert_eq!(q(&mut tk, "SELECT DISTINCT a FROM d2 WHERE b = 2"), [["1"], ["2"]]);
    rows(&mut tk, "CREATE TABLE g3 (k BIGINT, v BIGINT)");
    rows(&mut tk, "INSERT INTO g3 VALUES (1, 5), (2, 5), (3, 9)");
    assert_eq!(q(&mut tk, "SELECT DISTINCT SUM(v) FROM g3 GROUP BY k"), [["5"], ["9"]]);
}

/// EXPLAIN brief rows with the plan ids dropped from the operator names.
fn explain_brief(tk: &mut Session, sql: &str) -> Vec<Vec<String>> {
    displayed(rows(tk, &format!("explain format = 'brief' {sql}")))
        .into_iter()
        .map(|mut row| {
            if let Some((name, id)) = row[0].rsplit_once('_') {
                if !id.is_empty() && id.bytes().all(|b| b.is_ascii_digit()) {
                    row[0] = name.to_owned();
                }
            }
            row
        })
        .collect()
}

/// HAVING filters aggregate rows and ORDER BY sorts them; an aggregate or a
/// grouped column used only there is a hidden column trimmed from the
/// result. Go's serial HashAgg (concurrency 1) pins first-seen order.
#[test]
fn aggregate_having_and_order_by() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = session(&stack, 478);
    rows(&mut tk, "SET tidb_hashagg_partial_concurrency = 1");
    rows(&mut tk, "SET tidb_hashagg_final_concurrency = 1");
    rows(&mut tk, "CREATE TABLE g (a BIGINT, b BIGINT)");
    rows(&mut tk, "INSERT INTO g VALUES (1, 10), (1, 20), (2, 5), (3, 7), (3, 8)");
    let q = |tk: &mut Session, sql: &str| displayed(rows(tk, sql));
    assert_eq!(q(&mut tk, "SELECT a, COUNT(*) FROM g GROUP BY a HAVING COUNT(*) > 1"), [["1", "2"], ["3", "2"]]);
    assert_eq!(q(&mut tk, "SELECT a FROM g GROUP BY a HAVING SUM(b) > 15"), [["1"]]);
    assert_eq!(q(&mut tk, "SELECT a FROM g GROUP BY a ORDER BY SUM(b) DESC"), [["1"], ["3"], ["2"]]);
    assert_eq!(
        q(&mut tk, "SELECT a, SUM(b) FROM g GROUP BY a HAVING COUNT(*) > 1 ORDER BY SUM(b) LIMIT 1"),
        [["3", "15"]]
    );
    assert_eq!(
        q(&mut tk, "SELECT a, SUM(b) AS total FROM g GROUP BY a ORDER BY total"),
        [["2", "5"], ["3", "15"], ["1", "30"]]
    );
    assert_eq!(q(&mut tk, "SELECT COUNT(*) FROM g GROUP BY a HAVING a > 1"), [["1"], ["2"]]);
    assert!(rows(&mut tk, "SELECT COUNT(*) FROM g HAVING COUNT(*) > 100").is_empty());
}

/// A global AVG over a range splits into a cop partial StreamAgg of COUNT
/// and SUM and a root final StreamAgg.
#[test]
fn global_avg_uses_count_sum_partial_and_final_stream_agg() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = session(&stack, 479);
    rows(&mut tk, "CREATE TABLE avg_revenue (id INT PRIMARY KEY, price DECIMAL(10,2), k INT)");
    rows(&mut tk, "INSERT INTO avg_revenue VALUES (10,100.00,1),(20,200.00,2),(30,300.00,3)");
    let sql = "SELECT AVG(price) FROM avg_revenue WHERE k >= 1 AND k <= 3";
    assert_eq!(displayed(rows(&mut tk, sql)), [["200.000000"]]);
    let plan = explain_brief(&mut tk, sql);
    assert_eq!(
        plan.iter().map(|row| row[0].as_str()).collect::<Vec<_>>(),
        ["StreamAgg", "└─TableReader", "  └─StreamAgg", "    └─Selection", "      └─TableFullScan"]
    );
    assert!(plan[0][4].contains("funcs:avg(Column#"), "{plan:?}");
    assert!(plan[0][4].matches("Column#").count() >= 3, "{plan:?}");
    assert!(plan[2][4].contains("funcs:count(test.avg_revenue.price)"), "{plan:?}");
    assert!(plan[2][4].contains("funcs:sum(test.avg_revenue.price)"), "{plan:?}");
}

fn operators(plan: &[Vec<String>]) -> Vec<&str> {
    plan.iter().map(|row| row[0].as_str()).collect()
}

fn tasks(plan: &[Vec<String>]) -> Vec<&str> {
    plan.iter().map(|row| row[2].as_str()).collect()
}

/// A global SUM of an expression over a range is a cop partial and a root
/// final StreamAgg.
#[test]
fn global_sum_expression_uses_partial_and_final_stream_agg() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = session(&stack, 480);
    rows(&mut tk, "CREATE TABLE revenue (id INT PRIMARY KEY, price DECIMAL(10,2), discount DECIMAL(4,2), k INT)");
    rows(&mut tk, "INSERT INTO revenue VALUES (1,100.00,0.05,1),(2,200.00,0.10,2),(3,300.00,0.20,4)");
    let sql = "SELECT SUM(price * discount) FROM revenue WHERE k >= 1 AND k <= 3";
    assert_eq!(displayed(rows(&mut tk, sql)), [["25.0000"]]);
    let plan = explain_brief(&mut tk, sql);
    assert_eq!(
        operators(&plan),
        ["StreamAgg", "└─TableReader", "  └─StreamAgg", "    └─Selection", "      └─TableFullScan"]
    );
    assert_eq!(tasks(&plan), ["root", "root", "cop[tikv]", "cop[tikv]", "cop[tikv]"]);
    assert!(plan[0][4].starts_with("funcs:sum(Column#"), "{plan:?}");
    assert!(plan[2][4].contains("sum(mul(test.revenue.price, test.revenue.discount))"), "{plan:?}");
}

/// A single integer SUM over a join uses Go's serial root StreamAgg with
/// SUM's decimal input projection when both hash-agg concurrencies are 1.
#[test]
fn joined_integer_sum_uses_root_stream_agg() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = session(&stack, 481);
    rows(&mut tk, "SET tidb_hashagg_partial_concurrency = 1");
    rows(&mut tk, "SET tidb_hashagg_final_concurrency = 1");
    rows(&mut tk, "CREATE TABLE sum_orders (id INT PRIMARY KEY, customer_id INT, quantity INT)");
    rows(&mut tk, "CREATE TABLE sum_customers (id INT PRIMARY KEY)");
    rows(&mut tk, "INSERT INTO sum_orders VALUES (1,10,7),(2,20,11),(3,30,13)");
    rows(&mut tk, "INSERT INTO sum_customers VALUES (10),(20),(30)");
    let sql = "SELECT SUM(o.quantity) FROM sum_orders o JOIN sum_customers c ON o.customer_id = c.id";
    assert_eq!(displayed(rows(&mut tk, sql)), [["31"]]);
    let plan = explain_brief(&mut tk, sql);
    assert_eq!(plan[0][0], "StreamAgg", "{plan:?}");
    assert!(
        plan.iter().any(|row| row[0].trim_start_matches(&[' ', '│', '├', '└', '─'][..]) == "Projection"),
        "{plan:?}"
    );
}

/// A global integer SUM over a handle range: partial StreamAgg in the cop
/// task, final StreamAgg at the root, the answer a DECIMAL.
#[test]
fn global_integer_sum_uses_gos_stream_agg_and_cast_projection() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = session(&stack, 482);
    rows(&mut tk, "CREATE TABLE sum_range (id INT PRIMARY KEY, k INT NOT NULL)");
    rows(&mut tk, "INSERT INTO sum_range VALUES (1, 10), (2, 20)");
    let sql = "SELECT SUM(k) FROM sum_range WHERE id BETWEEN 1 AND 100";
    let plan = explain_brief(&mut tk, sql);
    assert_eq!(operators(&plan), ["StreamAgg", "└─TableReader", "  └─StreamAgg", "    └─TableRangeScan"]);
    assert_eq!(tasks(&plan), ["root", "root", "cop[tikv]", "cop[tikv]"]);
    assert!(plan[0][4].starts_with("funcs:sum(Column#"), "{plan:?}");
    assert!(plan[2][4].contains("funcs:sum(test.sum_range.k)->Column#"), "{plan:?}");
    assert_eq!(displayed(rows(&mut tk, sql)), [["30"]]);
}

/// A global COUNT over a primary-key equality is a StreamAgg over Point_Get
/// with no Selection left.
#[test]
fn global_count_over_point_get_uses_gos_stream_agg_without_selection() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = session(&stack, 483);
    rows(
        &mut tk,
        "CREATE TABLE point_count (id INT NOT NULL AUTO_INCREMENT, k INT NOT NULL DEFAULT 0, c CHAR(120) NOT NULL DEFAULT '', pad CHAR(60) NOT NULL DEFAULT '', PRIMARY KEY (id) CLUSTERED, KEY k_1 (k))",
    );
    rows(&mut tk, "INSERT INTO point_count VALUES (1, 10, 'one', 'pad'), (2, 20, 'two', 'pad')");
    let sql = "SELECT COUNT(*) FROM point_count WHERE id = 1";
    assert_eq!(operators(&explain_brief(&mut tk, sql)), ["StreamAgg", "└─Point_Get"]);
    assert_eq!(displayed(rows(&mut tk, sql)), [["1"]]);
}

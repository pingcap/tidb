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

use super::sql_kit::{exec_all, explain_brief, q};
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

/// Go `buildProjectionField`: a computed field's column has no `OrigName`,
/// so its alias never reaches the operator text.
#[test]
fn a_computed_projection_column_explains_as_column_not_its_alias() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = session(&stack, 484);
    rows(&mut tk, "CREATE TABLE alias_agg (g BIGINT, v BIGINT)");
    let text: Vec<String> = explain_brief(&mut tk, "SELECT v + 0 AS revenue FROM alias_agg")
        .into_iter()
        .map(|row| row.join("\t"))
        .collect();
    assert!(text.iter().any(|line| line.contains("Projection") && line.contains("->Column#")), "{text:#?}");
    assert!(!text.iter().any(|line| line.contains("->revenue")), "{text:#?}");
}

/// A constant DATE_ADD under an AND folds to a literal before push-down.
#[test]
fn a_constant_date_add_in_a_predicate_folds_before_push_down() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = session(&stack, 485);
    rows(&mut tk, "CREATE TABLE t (d DATE NOT NULL)");
    let info: Vec<String> = explain_brief(
        &mut tk,
        "SELECT * FROM t WHERE d >= '1996-01-01' AND d < DATE_ADD('1996-12-01', INTERVAL 1 MONTH)",
    )
    .into_iter()
    .map(|row| row[4].clone())
    .collect();
    assert!(info.iter().any(|line| line.contains("lt(test.t.d, 1997-01-01 00:00:00.000000)")), "{info:?}");
    assert!(!info.iter().any(|line| line.contains("date_add")), "{info:?}");
}

/// A computed DISTINCT argument rides through the cop partial aggregation's
/// group key, beside a real aggregate and beside a GROUP BY column.
#[test]
fn a_computed_distinct_argument_round_trips_through_the_cop_partial_aggregation() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = session(&stack, 486);
    rows(&mut tk, "CREATE TABLE g (s VARCHAR(10) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci)");
    rows(&mut tk, "INSERT INTO g VALUES ('a'), ('B'), ('A')");
    assert_eq!(displayed(rows(&mut tk, "SELECT COUNT(DISTINCT CONCAT(s, '')), COUNT(*) FROM g")), [["2", "3"]]);
    let answer = displayed(rows(&mut tk, "SELECT s, COUNT(DISTINCT CONCAT(s, '')) FROM g GROUP BY s ORDER BY s"));
    assert_eq!(answer.len(), 2);
    assert_eq!(answer[0][1], "1");
    assert_eq!(answer[1][0], "B");
    assert_eq!(answer[1][1], "1");
}

/// A grouped aggregate answers its group key in the SELECT list's position.
#[test]
fn a_delivered_grouped_aggregate_matches_the_planned_statement() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = session(&stack, 487);
    rows(&mut tk, "CREATE TABLE cost_receipt (id INT PRIMARY KEY, k INT, c VARCHAR(20))");
    rows(&mut tk, "INSERT INTO cost_receipt VALUES (1, 10, 'a'), (2, 10, 'b'), (3, 20, 'a')");
    let mut answer = displayed(rows(&mut tk, "SELECT k, SUM(id) FROM cost_receipt WHERE id BETWEEN 1 AND 100 GROUP BY k"));
    answer.sort();
    assert_eq!(answer, [["10", "3"], ["20", "3"]]);
}

/// A grouped COUNT over a key prefix range: the cop partial StreamAgg counts,
/// the root final StreamAgg carries the group key as `firstrow`, and a
/// Projection restores the SELECT list.
#[test]
fn grouped_partial_count_carries_the_group_key() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = session(&stack, 488);
    rows(
        &mut tk,
        "CREATE TABLE order_line (ol_o_id INT NOT NULL, ol_d_id INT NOT NULL, ol_w_id INT NOT NULL, ol_number INT NOT NULL, PRIMARY KEY (ol_w_id,ol_d_id,ol_o_id,ol_number))",
    );
    rows(&mut tk, "INSERT INTO order_line VALUES (1,1,1,1),(1,1,1,2),(2,2,1,1),(1,1,2,1)");
    let sql = "SELECT ol_d_id, COUNT(*) cn FROM order_line WHERE ol_w_id=1 GROUP BY ol_d_id";
    assert_eq!(displayed(rows(&mut tk, sql)), [["1", "2"], ["2", "1"]]);
    let plan = explain_brief(&mut tk, sql);
    assert_eq!(
        operators(&plan),
        ["Projection", "└─StreamAgg", "  └─TableReader", "    └─StreamAgg", "      └─TableRangeScan"]
    );
    assert!(plan[0][4].starts_with("test.order_line.ol_d_id, Column#"), "{plan:?}");
    assert!(plan[1][4].contains("funcs:count(Column#"), "{plan:?}");
    assert!(plan[1][4].contains("funcs:firstrow(test.order_line.ol_d_id)"), "{plan:?}");
    assert!(!plan[3][4].contains("firstrow"), "{plan:?}");
}

/// Go `buildSelect`: a derived table's ORDER BY is built only with a LIMIT,
/// with `tidb_remove_orderby_in_subquery` off, or at the top level.
#[test]
fn derived_table_order_by_is_removed_unless_top_level_limit_or_disabled() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = session(&stack, 489);
    rows(&mut tk, "CREATE TABLE s (a INT NOT NULL, b INT NOT NULL, PRIMARY KEY (a))");
    rows(&mut tk, "CREATE TABLE t (a INT NOT NULL, b INT NOT NULL, PRIMARY KEY (a))");
    let ops = |tk: &mut Session, sql: &str| -> Vec<String> {
        explain_brief(tk, sql).into_iter().map(|row| row[0].clone()).collect()
    };
    let derived = "SELECT COUNT(*) FROM (SELECT s.a, SUM(s.b) sm FROM s GROUP BY s.a ORDER BY s.a + 0) d, t WHERE t.a = d.a";
    let plan = ops(&mut tk, derived);
    assert!(!plan.iter().any(|op| op.contains("Sort")), "{plan:?}");
    let with_limit = "SELECT COUNT(*) FROM (SELECT s.a, SUM(s.b) sm FROM s GROUP BY s.a ORDER BY s.a + 0 LIMIT 3) d, t WHERE t.a = d.a";
    let plan = ops(&mut tk, with_limit);
    assert!(plan.iter().any(|op| op.contains("Sort") || op.contains("TopN")), "{plan:?}");
    rows(&mut tk, "SET tidb_remove_orderby_in_subquery = OFF");
    let plan = ops(&mut tk, derived);
    assert!(plan.iter().any(|op| op.contains("Sort")), "{plan:?}");
    rows(&mut tk, "SET tidb_remove_orderby_in_subquery = DEFAULT");
    let plan = ops(&mut tk, "SELECT s.a, SUM(s.b) sm FROM s GROUP BY s.a ORDER BY SUM(s.b)");
    assert!(plan.iter().any(|op| op.contains("Sort")), "{plan:?}");
}

/// A DISTINCT scalar subquery with a residual predicate explains as a root
/// HashAgg over a Selection, and answers.
#[test]
fn explain_distinct_scalar_subquery_with_filter() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = session(&stack, 490);
    rows(&mut tk, "CREATE TABLE date_dim (d_month_seq BIGINT, d_year BIGINT, d_moy BIGINT)");
    rows(&mut tk, "INSERT INTO date_dim VALUES (1201, 2000, 2), (1201, 2000, 2)");
    let sql = "SELECT (SELECT DISTINCT d_month_seq FROM date_dim WHERE d_year = 2000 AND d_moy = 2)";
    let plan = explain_brief(&mut tk, sql);
    assert!(plan.iter().any(|row| row[0].contains("HashAgg")), "{plan:?}");
    assert!(plan.iter().any(|row| row[0].contains("Selection")), "{plan:?}");
    assert_eq!(displayed(rows(&mut tk, sql)), [["1201"]]);
}

/// The operator names with the tree drawing stripped.
fn bare_operators(plan: &[Vec<String>]) -> Vec<String> {
    plan.iter().map(|row| row[0].trim_start_matches(&[' ', '│', '├', '└', '─'][..]).to_owned()).collect()
}

/// STREAM_AGG / HASH_AGG hints choose the aggregate; a stream aggregate over
/// an unordered child keeps its enforced Sort, a hash aggregate sorts above.
#[test]
fn aggregation_hints_are_lowered_from_the_shared_physical_plan() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = session(&stack, 640);
    exec_all(&mut tk, &[
        "CREATE TABLE hinted_agg (g BIGINT, v BIGINT)",
        "INSERT INTO hinted_agg VALUES (2,20),(1,10),(2,21),(3,30)",
        "CREATE TABLE hinted_order (g BIGINT, v BIGINT, INDEX g_idx(g))",
        "INSERT INTO hinted_order VALUES (2,20),(1,10),(2,21),(3,30)",
    ]);
    let stream_sql = "SELECT /*+ STREAM_AGG() */ g, COUNT(v) FROM hinted_agg GROUP BY g";
    assert_eq!(q(&mut tk, stream_sql), [["1", "1"], ["2", "2"], ["3", "1"]]);
    for (sql, root, child) in [
        (stream_sql, "StreamAgg", Some("Sort")),
        ("SELECT /*+ HASH_AGG() */ g, COUNT(v) FROM hinted_agg GROUP BY g", "HashAgg", None),
    ] {
        let plan = explain_brief(&mut tk, sql);
        let names = operators(&plan);
        let at = names.iter().position(|name| name.contains(root)).unwrap_or_else(|| panic!("{names:?}"));
        if let Some(child) = child {
            assert!(names.get(at + 1).is_some_and(|name| name.contains(child)), "{names:?}");
        }
    }
    for (hint, aggregate, direction, expected) in [
        ("HASH_AGG", "HashAgg", "ASC", [1, 2, 3]),
        ("HASH_AGG", "HashAgg", "DESC", [3, 2, 1]),
        ("STREAM_AGG", "StreamAgg", "ASC", [1, 2, 3]),
        ("STREAM_AGG", "StreamAgg", "DESC", [3, 2, 1]),
    ] {
        let sql = format!("SELECT /*+ {hint}() */ g, COUNT(*) FROM hinted_order GROUP BY g ORDER BY g {direction}");
        let want = expected.iter().map(|g| vec![g.to_string(), (if *g == 2 { 2 } else { 1 }).to_string()]).collect::<Vec<_>>();
        assert_eq!(q(&mut tk, &sql), want, "{sql}");
        let plan = explain_brief(&mut tk, &sql);
        let names = operators(&plan);
        let at = names.iter().position(|name| name.contains(aggregate)).unwrap_or_else(|| panic!("{sql}: {names:?}"));
        if hint == "HASH_AGG" {
            assert!(names[..at].iter().any(|name| name.contains("Sort")), "{names:?}");
            assert!(!names.iter().any(|name| name.contains("StreamAgg")), "{names:?}");
        }
    }
}

/// A STREAM_AGG DISTINCT keeps the Sort its child needs.
#[test]
fn distinct_aggregation_family_is_lowered_from_the_shared_physical_plan() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = session(&stack, 641);
    exec_all(&mut tk, &["CREATE TABLE hinted_distinct (g BIGINT)", "INSERT INTO hinted_distinct VALUES (2),(1),(2),(3)"]);
    let sql = "SELECT /*+ STREAM_AGG() */ DISTINCT g FROM hinted_distinct";
    assert_eq!(q(&mut tk, sql), [["1"], ["2"], ["3"]]);
    let plan = explain_brief(&mut tk, sql);
        let names = operators(&plan);
    let at = names.iter().position(|name| name.contains("StreamAgg")).unwrap_or_else(|| panic!("{names:?}"));
    assert!(names.get(at + 1).is_some_and(|name| name.contains("Sort")), "{names:?}");
}

/// An aggregate over a handle range keeps the TableRangeScan's range.
#[test]
fn a_shared_aggregate_access_receipt_keeps_the_table_range() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = session(&stack, 642);
    rows(&mut tk, "CREATE TABLE sbtest1 (id BIGINT PRIMARY KEY, k BIGINT, INDEX k_1(k))");
    let plan = explain_brief(&mut tk, "SELECT SUM(k) FROM sbtest1 WHERE id BETWEEN 100 AND 199");
    assert!(plan.iter().any(|row| row[0].contains("TableRangeScan") && row[4].contains("range:[100,199]")), "{plan:#?}");
}

/// Go's `AggregationPushDownSolver` substitutes the child Projection before
/// `InjectProjBelowAgg`; derived group outputs are restored below the sort.
#[test]
fn aggregate_push_down_substitutes_child_projection_before_injection() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = session(&stack, 643);
    exec_all(&mut tk, &[
        "CREATE TABLE orders (o_orderkey INT PRIMARY KEY, o_orderdate DATE)",
        "CREATE TABLE lineitem (l_orderkey INT, l_extendedprice DECIMAL(15,2), l_discount DECIMAL(15,2), nation VARCHAR(25), cust_nation VARCHAR(25))",
    ]);
    let sql = "SELECT o_year, SUM(CASE WHEN nation = 'INDIA' THEN volume ELSE 0 END) / SUM(volume) AS share \
        FROM (SELECT EXTRACT(YEAR FROM o_orderdate) AS o_year, l_extendedprice * (1 - l_discount) AS volume, nation \
              FROM orders, lineitem WHERE o_orderkey = l_orderkey) all_nations GROUP BY o_year ORDER BY o_year";
    let plan = explain_brief(&mut tk, sql);
    let info = |row: usize| plan[row][4].clone();
    assert_eq!(&bare_operators(&plan)[..6], ["Sort", "Projection", "HashAgg", "Projection", "Projection", "HashJoin"], "{plan:#?}");
    assert!(info(0).starts_with("Column#"), "{plan:#?}");
    assert!(info(1).starts_with("Column#") && info(1).contains("div(Column#"), "{plan:#?}");
    assert_eq!(info(2).matches("funcs:firstrow(").count(), 1, "{plan:#?}");
    assert!(
        info(3).contains("test.lineitem.nation")
            && info(3).contains("test.lineitem.l_extendedprice")
            && info(3).contains("extract(YEAR, test.orders.o_orderdate)")
            && info(3).matches("test.orders.o_orderdate").count() == 1,
        "{plan:#?}"
    );
    assert!(
        info(4).contains("test.lineitem.l_extendedprice")
            && info(4).contains("test.lineitem.l_discount")
            && info(4).contains("test.orders.o_orderdate")
            && info(4).contains("test.lineitem.nation"),
        "{plan:#?}"
    );
    for (sql, direct_groups) in [
        (
            "SELECT supp_nation, cust_nation, l_year, SUM(volume) AS revenue \
             FROM (SELECT nation AS supp_nation, cust_nation, EXTRACT(YEAR FROM o_orderdate) AS l_year, l_extendedprice * (1 - l_discount) AS volume \
                   FROM orders, lineitem WHERE o_orderkey = l_orderkey) shipping \
             GROUP BY supp_nation, cust_nation, l_year ORDER BY supp_nation, cust_nation, l_year",
            &["test.lineitem.nation", "test.lineitem.cust_nation"][..],
        ),
        (
            "SELECT nation, o_year, SUM(amount) AS sum_profit \
             FROM (SELECT nation, EXTRACT(YEAR FROM o_orderdate) AS o_year, l_extendedprice * (1 - l_discount) AS amount \
                   FROM orders, lineitem WHERE o_orderkey = l_orderkey) profit \
             GROUP BY nation, o_year ORDER BY nation, o_year DESC",
            &["test.lineitem.nation"][..],
        ),
    ] {
        assert!(rows(&mut tk, sql).is_empty());
        let plan = explain_brief(&mut tk, sql);
        let info = |row: usize| plan[row][4].clone();
        assert_eq!(&bare_operators(&plan)[..4], ["Sort", "Projection", "HashAgg", "Projection"], "{plan:#?}");
        assert_eq!(info(2).matches("funcs:firstrow(").count(), direct_groups.len() + 1, "{plan:#?}");
        for group in direct_groups {
            assert!(info(0).contains(group) && info(1).contains(group), "{plan:#?}");
            assert!(info(2).contains(&format!(")->{group}")), "{plan:#?}");
        }
        assert!(info(0).contains("Column#") && info(1).contains("Column#") && info(2).contains(")->Column#"), "{plan:#?}");
    }
}

/// Go keeps the visible projection below Sort and `restoreSchemaIfChanged`
/// keeps the reordered join's column identities below `InjectProjBelowAgg`.
#[test]
fn grouped_order_by_projects_visible_fields_below_sort() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = session(&stack, 644);
    exec_all(&mut tk, &[
        "CREATE TABLE orders (o_orderkey INT PRIMARY KEY, o_orderpriority VARCHAR(15))",
        "CREATE TABLE lineitem (l_orderkey INT, l_shipmode VARCHAR(10), l_commitdate DATE, l_receiptdate DATE, l_shipdate DATE)",
    ]);
    let sql = "SELECT l_shipmode, \
        SUM(CASE WHEN o_orderpriority = '1-URGENT' OR o_orderpriority = '2-HIGH' THEN 1 ELSE 0 END) AS high_line_count, \
        SUM(CASE WHEN o_orderpriority <> '1-URGENT' AND o_orderpriority <> '2-HIGH' THEN 1 ELSE 0 END) AS low_line_count \
        FROM orders, lineitem WHERE o_orderkey = l_orderkey AND l_shipmode IN ('RAIL', 'FOB') \
        AND l_commitdate < l_receiptdate AND l_shipdate < l_commitdate AND l_receiptdate >= '1997-01-01' \
        AND l_receiptdate < DATE_ADD('1997-01-01', INTERVAL 1 YEAR) GROUP BY l_shipmode ORDER BY l_shipmode";
    let plan = explain_brief(&mut tk, sql);
    let info = |row: usize| plan[row][4].clone();
    assert_eq!(&bare_operators(&plan)[..5], ["Sort", "Projection", "HashAgg", "Projection", "Projection"], "{plan:#?}");
    assert_eq!(info(4), "test.orders.o_orderpriority, test.lineitem.l_shipmode", "{plan:#?}");
    assert!(info(3).contains("test.orders.o_orderpriority") && info(3).contains("test.lineitem.l_shipmode"), "{plan:#?}");
    let hash_agg = info(2);
    let group = hash_agg.strip_prefix("group by:").and_then(|rest| rest.split(',').next()).expect("a group").trim().to_owned();
    assert!(hash_agg.contains(&format!("funcs:firstrow({group})->test.lineitem.l_shipmode")), "{plan:#?}");
}

/// The reordered region-dimension join clamps d_name's NDV before the fact
/// join; the Sort reads the aggregate's generated column; HashJoin prints its
/// equality aligned with the logical children. Statistics are a real ANALYZE
/// of rows with the TPC-H-shaped NDVs (1000 facts, 25 dims, 5 regions).
#[test]
fn grouped_rows_follow_the_reordered_join_tree() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = session(&stack, 645);
    exec_all(&mut tk, &[
        "CREATE TABLE fact (f_id INT PRIMARY KEY, f_dim_id INT, f_value INT)",
        "CREATE TABLE dim (d_id INT PRIMARY KEY, d_region_id INT, d_name VARCHAR(20))",
        "CREATE TABLE region (r_id INT PRIMARY KEY, r_name VARCHAR(20))",
    ]);
    let facts = (1..=1000).map(|id| format!("({id},{},{id})", id % 25 + 1)).collect::<Vec<_>>().join(",");
    rows(&mut tk, &format!("INSERT INTO fact VALUES {facts}"));
    let dims = (1..=25).map(|id| format!("({id},{},'d{id}')", id % 5 + 1)).collect::<Vec<_>>().join(",");
    rows(&mut tk, &format!("INSERT INTO dim VALUES {dims}"));
    exec_all(&mut tk, &[
        "INSERT INTO region VALUES (1,'MIDDLE'),(2,'R2'),(3,'R3'),(4,'R4'),(5,'R5')",
        "ANALYZE TABLE fact, dim, region",
    ]);
    let plan = explain_brief(
        &mut tk,
        "SELECT d_name, SUM(f_value) AS revenue FROM fact, dim, region \
         WHERE f_dim_id = d_id AND d_region_id = r_id AND r_name = 'MIDDLE' GROUP BY d_name ORDER BY revenue DESC",
    );
    let names = bare_operators(&plan);
    let agg = &plan[names.iter().position(|name| name == "HashAgg").expect("a HashAgg")];
    assert_eq!(agg[1], "5.00", "{plan:#?}");
    let sum_out = agg[4].split("funcs:sum(").nth(1).and_then(|rest| rest.split(")->").nth(1)).and_then(|rest| rest.split(',').next()).expect("a sum");
    assert_eq!(plan[0][4], format!("{sum_out}:desc"), "{plan:#?}");
    assert!(plan.iter().any(|row| row[4].contains("eq(test.dim.d_id, test.fact.f_dim_id)")), "{plan:#?}");
}

/// A filter over a derived aggregate's output refreshes the source
/// statistics: 8 rows under pseudo statistics.
#[test]
fn derived_aggregate_null_filter_refreshes_source_statistics() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = session(&stack, 646);
    exec_all(&mut tk, &[
        "CREATE TABLE district (d_id INT NOT NULL, d_w_id INT NOT NULL, d_ytd DECIMAL(12,2), PRIMARY KEY(d_w_id,d_id) CLUSTERED)",
        "INSERT INTO district VALUES (1,1,10),(2,1,NULL),(1,2,20)",
    ]);
    let sql = "SELECT d_id FROM (SELECT d_id,d_w_id,SUM(d_ytd) s FROM district GROUP BY d_id,d_w_id) d WHERE d_w_id=1 AND s IS NOT NULL";
    assert_eq!(q(&mut tk, sql), [["1"]]);
    let plan = explain_brief(&mut tk, sql);
    let filter = plan.iter().find(|row| row[4].contains("not(isnull(cast(")).unwrap_or_else(|| panic!("{plan:#?}"));
    assert_eq!(filter[1], "8.00", "{plan:#?}");
}

/// A narrow covering index range keeps order for the grouped columns and
/// never prices a table read on top.
#[test]
fn a_narrow_covering_index_range_prices_the_common_handle_once() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = session(&stack, 647);
    rows(&mut tk, "CREATE TABLE orders (o_id INT NOT NULL, o_d_id INT NOT NULL, o_w_id INT NOT NULL, o_c_id INT, o_entry_d DATETIME, o_carrier_id INT, o_ol_cnt INT, o_all_local INT, PRIMARY KEY (o_w_id,o_d_id,o_id) CLUSTERED, KEY idx_order (o_w_id,o_d_id,o_c_id,o_id))");
    let plan = explain_brief(&mut tk, "SELECT o_w_id, o_d_id, count(*) FROM orders WHERE o_w_id = 1 GROUP BY o_w_id, o_d_id");
    assert!(
        plan.iter().any(|row| row[0].contains("IndexRangeScan") && row[3].contains("idx_order") && row[4].contains("range:[1,1]") && row[4].contains("keep order:true")),
        "{plan:#?}"
    );
    assert!(plan.iter().all(|row| !row[0].contains("TableRangeScan")), "{plan:#?}");
}

/// Go pushes a global COUNT over index ranges as StreamAgg through an
/// IndexReader; full COUNT(*) and SUM pick HashAgg over an IndexFullScan.
#[test]
fn global_count_over_index_ranges_uses_gos_stream_agg_and_index_reader() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = session(&stack, 648);
    exec_all(&mut tk, &[
        "CREATE TABLE count_ranges (id INT PRIMARY KEY, k INT NOT NULL, KEY k_1(k))",
        "INSERT INTO count_ranges VALUES (1, 1), (2, 15), (3, 20)",
    ]);
    let sql = "SELECT COUNT(k) FROM count_ranges WHERE (k BETWEEN 1 AND 6) OR (k BETWEEN 2 AND 7) OR \
        (k BETWEEN 3 AND 8) OR (k BETWEEN 4 AND 9) OR (k BETWEEN 5 AND 10) OR (k BETWEEN 6 AND 11) OR \
        (k BETWEEN 7 AND 12) OR (k BETWEEN 8 AND 13) OR (k BETWEEN 9 AND 14) OR (k BETWEEN 10 AND 15)";
    let plan = explain_brief(&mut tk, sql);
    assert_eq!(operators(&plan), ["StreamAgg", "└─IndexReader", "  └─StreamAgg", "    └─IndexRangeScan"]);
    assert_eq!(plan.iter().map(|row| row[2].as_str()).collect::<Vec<_>>(), ["root", "root", "cop[tikv]", "cop[tikv]"]);
    assert!(plan[0][4].starts_with("funcs:count(Column#"), "{plan:#?}");
    assert_eq!(plan[1][4], "index:StreamAgg");
    assert!(plan[2][4].starts_with("funcs:count(test.count_ranges.k)->Column#"), "{plan:#?}");
    assert!(plan[3][3].contains("index:k_1(k)"), "{plan:#?}");
    assert_eq!(q(&mut tk, sql), [["2"]]);
    for (sql, want) in [("SELECT COUNT(*) FROM count_ranges", "3"), ("SELECT SUM(k) FROM count_ranges", "36")] {
        assert_eq!(operators(&explain_brief(&mut tk, sql)), ["HashAgg", "└─IndexReader", "  └─HashAgg", "    └─IndexFullScan"], "{sql}");
        assert_eq!(q(&mut tk, sql), [[want]], "{sql}");
    }
}

#[test]
fn explain_uses_the_common_physical_plan_without_legacy_ast_execution() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = session(&stack, 649);
    rows(&mut tk, "CREATE TABLE explain_common_plan (id INT PRIMARY KEY, k INT)");
    assert!(!explain_brief(&mut tk, "SELECT k, SUM(id) FROM explain_common_plan WHERE id > 0 GROUP BY k").is_empty());
}

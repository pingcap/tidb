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


//! Subqueries through Go's planner: uncorrelated scalars evaluated and
//! labelled, Go's statement-wide plan-column ids, decorrelation of correlated
//! AVG / SUM / EXISTS into joins and grouped aggregates, the NOT EXISTS anti
//! semi join beside an evaluated scalar, and correlated semantics, over the
//! cop-backed TestKit stack (X11). The estimate magnitudes the old tier pinned
//! under synthetic TPC-H statistics belong to Go's `casetest/tpch` suite.

use super::sql_kit::*;
use super::*;

fn bare(name: &str) -> String {
    name.trim_start_matches(&[' ', '│', '├', '└', '─'][..]).to_owned()
}

/// A nested uncorrelated subquery scans its tables with their own
/// statistics: the same estimate as the standalone query.
#[test]
fn an_uncorrelated_join_subquery_with_a_filter_keeps_its_table_statistics() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = session(&stack, 770);
    exec_all(&mut tk, &[
        "CREATE TABLE supplier (s_suppkey BIGINT PRIMARY KEY CLUSTERED, s_nationkey BIGINT NOT NULL, s_acctbal DECIMAL(15,2) NOT NULL)",
        "CREATE TABLE partsupp (ps_partkey BIGINT NOT NULL, ps_suppkey BIGINT NOT NULL, ps_supplycost DECIMAL(15,2) NOT NULL, PRIMARY KEY (ps_partkey, ps_suppkey) CLUSTERED)",
        "INSERT INTO supplier VALUES (1, 3, 100.00)",
        "INSERT INTO partsupp VALUES (1, 1, 10.00)",
    ]);
    let inner = "SELECT SUM(ps_supplycost) * 0.0001 FROM partsupp, supplier WHERE ps_suppkey = s_suppkey AND s_nationkey = 3";
    let full_scan = |plan: &[Vec<String>], table: &str| {
        plan.iter().find(|row| row[0].contains("TableFullScan") && row[3] == table).map(|row| row[1].clone()).unwrap_or_else(|| panic!("no full scan of {table}: {plan:#?}"))
    };
    let standalone = explain_brief(&mut tk, inner);
    let nested = explain_brief(&mut tk, &format!("SELECT ps_partkey FROM partsupp WHERE ps_supplycost > ({inner})"));
    let at = nested.iter().position(|row| row[0].starts_with("ScalarSubQuery")).unwrap_or_else(|| panic!("{nested:#?}"));
    assert_eq!(full_scan(&nested[at..], "table:partsupp"), full_scan(&standalone, "table:partsupp"), "{nested:#?}");
}

/// TPC-H q22: Go's `InjectProjBelowAgg` evaluates the scalar group item
/// between the outer HashAgg and the anti semi join, the scalar child takes
/// Go's plan-column id, the evaluated predicate reaches the DataSource below
/// the anti join, and the scalar AVG splits into TiKV COUNT/SUM.
#[test]
fn evaluated_scalar_predicate_is_pushed_below_a_sibling_anti_semi_join() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = session(&stack, 771);
    exec_all(&mut tk, &[
        "CREATE TABLE customer (c_custkey BIGINT PRIMARY KEY CLUSTERED, c_phone VARCHAR(16), c_acctbal DECIMAL(15,2))",
        "CREATE TABLE orders (o_custkey BIGINT)",
        "INSERT INTO customer VALUES (1, '20-1', 100.00), (2, '40-2', 10.00)",
        "INSERT INTO orders VALUES (2)",
    ]);
    let plan = explain_brief(
        &mut tk,
        "SELECT cntrycode, COUNT(*), SUM(c_acctbal) FROM ( \
           SELECT SUBSTRING(c_phone, 1, 2) AS cntrycode, c_acctbal FROM customer \
           WHERE SUBSTRING(c_phone, 1, 2) IN ('20', '40') \
           AND c_acctbal > (SELECT AVG(c_acctbal) FROM customer WHERE c_acctbal > 0.00 AND SUBSTRING(c_phone, 1, 2) IN ('20', '40')) \
           AND NOT EXISTS (SELECT 1 FROM orders WHERE o_custkey = c_custkey) \
         ) AS custsale GROUP BY cntrycode ORDER BY cntrycode",
    );
    let anti = plan.iter().position(|row| row[0].contains("Join") && row[4].starts_with("anti semi join")).unwrap_or_else(|| panic!("{plan:#?}"));
    let outer_agg = plan[..anti].iter().rposition(|row| row[0].contains("HashAgg")).unwrap_or_else(|| panic!("{plan:#?}"));
    assert!(
        plan[outer_agg + 1..anti].iter().any(|row| row[0].contains("Projection") && row[4].contains("test.customer.c_acctbal") && row[4].contains("substring(test.customer.c_phone")),
        "{plan:#?}"
    );
    let scalar = plan.iter().position(|row| row[0].contains("ScalarSubQuery")).unwrap_or_else(|| panic!("{plan:#?}"));
    assert_eq!(plan[scalar][4], "Output: ScalarQueryCol#14", "{plan:#?}");
    let evaluated = |row: &Vec<String>| row[0].contains("Selection") && row[4].contains("ScalarQueryCol#") && row[4].contains("substring(test.customer.c_phone");
    assert!(plan[anti + 1..scalar].iter().any(evaluated), "{plan:#?}");
    assert!(plan[..anti].iter().all(|row| !evaluated(row)), "{plan:#?}");
    assert!(
        plan[scalar + 1..].iter().any(|row| row[0].contains("HashAgg") && row[2].contains("cop") && row[4].contains("funcs:count(test.customer.c_acctbal)") && row[4].contains("funcs:sum(test.customer.c_acctbal)")),
        "{plan:#?}"
    );
}

/// Plain EXPLAIN evaluates an uncorrelated scalar subquery and shows the
/// constant; HAVING resolves it through the aggregate output; q11 keeps Go's
/// column identities.
#[test]
fn plain_explain_evaluates_and_labels_an_uncorrelated_scalar_subquery() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = session(&stack, 772);
    exec_all(&mut tk, &["CREATE TABLE balances (v DECIMAL(10,2))", "INSERT INTO balances VALUES (1.00), (3.00)"]);
    let plan = explain_brief(&mut tk, "SELECT v FROM balances WHERE v > (SELECT AVG(v) FROM balances)");
    assert!(plan.iter().any(|row| row[0].contains("ScalarSubQuery")), "{plan:#?}");
    assert!(plan.iter().any(|row| row[0].contains("Selection") && row[4].contains("ScalarQueryCol#") && row[4].contains('(')), "{plan:#?}");
    let grouped = explain_brief(&mut tk, "SELECT v, SUM(v) FROM balances GROUP BY v HAVING SUM(v) > (SELECT AVG(v) FROM balances)");
    assert!(grouped.iter().any(|row| row[0].contains("Selection") && row[4].contains("ScalarQueryCol#") && row[4].contains("(2.000000)")), "{grouped:#?}");
    exec_all(&mut tk, &[
        "CREATE TABLE inventory (k INT, price DECIMAL(10,2), qty INT)",
        "CREATE TABLE inventory_key (k INT)",
        "INSERT INTO inventory VALUES (1, 2.00, 3), (2, 4.00, 5)",
        "INSERT INTO inventory_key VALUES (1), (2)",
    ]);
    let q11 = explain_brief(
        &mut tk,
        "SELECT inventory.k, SUM(inventory.price * inventory.qty) AS value FROM inventory, inventory_key WHERE inventory.k = inventory_key.k \
         GROUP BY inventory.k HAVING SUM(inventory.price * inventory.qty) > (SELECT SUM(price * qty) * 0.1 FROM inventory) ORDER BY value DESC",
    );
    let names = q11.iter().map(|row| bare(&row[0])).collect::<Vec<_>>();
    assert_eq!(&names[..5], ["Projection", "Sort", "Selection", "HashAgg", "Projection"], "{q11:#?}");
    assert!(q11[0][4].contains("Column#14->Column#27"), "{q11:#?}");
    assert!(q11.iter().any(|row| row[0].contains("ScalarSubQuery") && row[4] == "Output: ScalarQueryCol#25"), "{q11:#?}");
    assert_eq!(q11[3][4].matches("funcs:sum(").count(), 1, "{q11:#?}");
    assert!(q11[4][4].contains("cast(test.inventory.qty, decimal(10,0) BINARY)"), "{q11:#?}");
}

/// Views and correlated scalars consume Go's plan-column ids; q15 over a
/// grouped view keeps the view-output filter above its aggregate and Go's
/// max/min elimination; a non-unique IN groups its inner side and a unique
/// one does not. Statistics are a real ANALYZE of 10000 outer rows and 10000
/// inner rows over 500 keys.
#[test]
fn explaining_a_correlated_scalar_type_reads_no_storage() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = session(&stack, 773);
    exec_all(&mut tk, &[
        "CREATE TABLE outer_t (k BIGINT PRIMARY KEY CLUSTERED)",
        "CREATE TABLE inner_t (k BIGINT NOT NULL, v DECIMAL(6,2))",
        "CREATE TABLE inner_u (k BIGINT PRIMARY KEY)",
    ]);
    let outer = (1..=10_000).map(|k| format!("({k})")).collect::<Vec<_>>().join(",");
    rows(&mut tk, &format!("INSERT INTO outer_t VALUES {outer}"));
    let inner = (1..=10_000).map(|i| format!("({}, {}.{:02})", i % 500 + 1, i / 100, i % 100)).collect::<Vec<_>>().join(",");
    rows(&mut tk, &format!("INSERT INTO inner_t VALUES {inner}"));
    exec_all(&mut tk, &[
        "ANALYZE TABLE outer_t, inner_t",
        "CREATE VIEW inner_v AS SELECT v FROM inner_t",
        "CREATE VIEW revenue_v AS SELECT k AS supplier_no, SUM(v) AS total_revenue FROM inner_t GROUP BY k",
    ]);
    explain_brief(&mut tk, "SELECT (SELECT SUM(v) FROM inner_t WHERE inner_t.k=outer_t.k) FROM outer_t");
    explain_brief(&mut tk, "SELECT * FROM inner_v");
    for (sql, expected) in [
        ("SELECT k FROM outer_t WHERE k=(SELECT MAX(v) FROM inner_v)", "Output: ScalarQueryCol#8"),
        ("SELECT k FROM outer_t WHERE k=(SELECT MAX(total_revenue) FROM revenue_v)", "Output: ScalarQueryCol#10"),
    ] {
        let plan = explain_brief(&mut tk, sql);
        assert!(plan.iter().any(|row| row[0].contains("ScalarSubQuery") && row[4] == expected), "{sql}: {plan:#?}");
    }
    let q15 = explain_brief(
        &mut tk,
        "SELECT outer_t.k, total_revenue FROM outer_t, revenue_v WHERE outer_t.k=supplier_no AND total_revenue=(SELECT MAX(total_revenue) FROM revenue_v) ORDER BY outer_t.k",
    );
    let names = q15.iter().map(|row| bare(&row[0])).collect::<Vec<_>>();
    assert_eq!(names[0], "Sort", "{q15:#?}");
    assert!(names[1].starts_with("Index") && names[1].ends_with("Join"), "{q15:#?}");
    assert_eq!(&names[2..4], ["Selection(Build)", "HashAgg"], "{q15:#?}");
    assert_eq!(q15[1][1], "400.00", "{q15:#?}");
    assert!(q15[2][4].contains("ScalarQueryCol#"), "{q15:#?}");
    assert!(q15[1][4].contains("test.inner_t.k") && !q15[1][4].contains("test.revenue_v.supplier_no"), "{q15:#?}");
    assert!(q15[2][4].contains("eq(Column#") && !q15[2][4].contains("test.revenue_v") && !q15[2][4].contains("not(isnull("), "{q15:#?}");
    let scalar = names.iter().position(|name| name == "ScalarSubQuery").unwrap_or_else(|| panic!("{q15:#?}"));
    assert_eq!(q15[scalar][4], "Output: ScalarQueryCol#15", "{q15:#?}");
    let dynamic = names[..scalar].iter().position(|name| name == "TableRangeScan").unwrap_or_else(|| panic!("{q15:#?}"));
    assert!(q15[dynamic][4].contains("test.inner_t.k"), "{q15:#?}");
    assert_eq!(&names[scalar..scalar + 6], ["ScalarSubQuery", "MaxOneRow", "StreamAgg", "TopN", "Selection", "HashAgg"], "{q15:#?}");
    assert_eq!(q15[scalar + 4][1], "400.00", "{q15:#?}");
    assert!(q15[scalar + 4][4].contains("not(isnull("), "{q15:#?}");
    assert!(!q15[scalar + 5][4].contains("firstrow("), "{q15:#?}");
    let non_unique = explain_brief(&mut tk, "SELECT k FROM outer_t WHERE k IN (SELECT k FROM inner_t)").iter().map(|row| bare(&row[0])).collect::<Vec<_>>();
    assert!(non_unique.iter().any(|name| name.starts_with("IndexJoin")), "{non_unique:#?}");
    assert!(non_unique.iter().any(|name| name.contains("HashAgg")), "{non_unique:#?}");
    let unique = explain_brief(&mut tk, "SELECT k FROM outer_t WHERE k IN (SELECT k FROM inner_u)").iter().map(|row| bare(&row[0])).collect::<Vec<_>>();
    assert!(unique.iter().any(|name| name.starts_with("MergeJoin")), "{unique:#?}");
    assert!(unique.iter().all(|name| !name.contains("HashAgg")), "{unique:#?}");
}

/// TPC-H q17: the correlated AVG becomes a grouped inner aggregation joined
/// on the correlation key.
#[test]
fn correlated_avg_predicate_decorrelates_to_grouped_join() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = session(&stack, 774);
    exec_all(&mut tk, &[
        "CREATE TABLE part (p_partkey BIGINT PRIMARY KEY, p_brand VARCHAR(16), p_container VARCHAR(16))",
        "CREATE TABLE lineitem (l_partkey BIGINT, l_quantity DECIMAL(15,2), l_extendedprice DECIMAL(15,2))",
    ]);
    let plan = explain_brief(
        &mut tk,
        "SELECT SUM(l_extendedprice) / 7.0 FROM lineitem, part WHERE p_partkey = l_partkey AND p_brand = 'Brand#44' \
         AND p_container = 'WRAP PKG' AND l_quantity < (SELECT 0.2 * AVG(l_quantity) FROM lineitem WHERE l_partkey = p_partkey)",
    );
    assert!(plan.iter().any(|row| row[0].contains("HashAgg") && row[4].contains("group by:test.lineitem.l_partkey") && row[4].contains("avg(")), "{plan:#?}");
    assert!(plan.iter().any(|row| row[0].contains("HashJoin") && row[4].contains("test.part.p_partkey") && row[4].contains("test.lineitem.l_partkey")), "{plan:#?}");
    assert!(plan.iter().all(|row| !row[4].contains("SELECT 0.2*AVG")), "{plan:#?}");
}

/// TPC-H q20: the correlated SUM is pulled above a left outer join keyed by
/// partsupp's unique key, grouped by that key, and compared above it; nested
/// under an IN it keeps Go's identities and no internal alias leaks.
#[test]
fn correlated_sum_predicate_pulls_above_unique_outer_join() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = session(&stack, 775);
    exec_all(&mut tk, &[
        "CREATE TABLE part (p_partkey BIGINT PRIMARY KEY, p_name VARCHAR(32))",
        "CREATE TABLE partsupp (ps_partkey BIGINT NOT NULL, ps_suppkey BIGINT NOT NULL, ps_availqty BIGINT, PRIMARY KEY(ps_partkey, ps_suppkey) CLUSTERED)",
        "CREATE TABLE lineitem (l_partkey BIGINT, l_suppkey BIGINT, l_quantity DECIMAL(15,2), l_shipdate DATE)",
        "CREATE TABLE supplier (s_suppkey BIGINT PRIMARY KEY, s_name VARCHAR(32), s_address VARCHAR(64), s_nationkey BIGINT)",
        "CREATE TABLE nation (n_nationkey BIGINT PRIMARY KEY, n_name VARCHAR(32))",
        "INSERT INTO partsupp VALUES (1, 1, 10)",
        "INSERT INTO lineitem VALUES (1, 1, 1.00, '1992-01-01'), (1, 1, 2.00, '1993-06-01'), (2, 2, 3.00, '1994-06-01')",
        "INSERT INTO supplier VALUES (1, 'Supplier#1', 'Address#1', 1)",
        "INSERT INTO nation VALUES (1, 'ALGERIA')",
    ]);
    let parts = (1..=128).map(|key| format!("({key}, '{}')", if key == 1 { "green alpha".to_owned() } else { format!("red {key:03}") })).collect::<Vec<_>>().join(", ");
    rows(&mut tk, &format!("INSERT INTO part VALUES {parts}"));
    let pulled = |row: &Vec<String>| row[0].contains("Join") && row[4].contains("left outer join") && row[4].contains("partsupp.ps_partkey") && row[4].contains("lineitem.l_partkey");
    let correlated = "SELECT ps_suppkey FROM partsupp WHERE ps_partkey IN (SELECT p_partkey FROM part WHERE p_name LIKE 'green%') \
         AND ps_availqty > (SELECT 0.5 * SUM(l_quantity) FROM lineitem WHERE l_partkey = ps_partkey AND l_suppkey = ps_suppkey \
         AND l_shipdate >= '1993-01-01' AND l_shipdate < DATE_ADD('1993-01-01', INTERVAL '1' YEAR))";
    let plan = explain_brief(&mut tk, correlated);
    assert!(plan.iter().any(pulled), "{plan:#?}");
    assert!(plan.iter().any(|row| row[0].contains("HashAgg") && row[4].contains("group by:test.partsupp.ps_partkey") && row[4].contains("test.partsupp.ps_suppkey") && row[4].contains("sum(")), "{plan:#?}");
    assert!(plan.iter().any(|row| row[0].contains("Selection") && row[4].contains("gt(") && row[4].contains("mul(0.5")), "{plan:#?}");
    let nested = explain_brief(&mut tk, &format!("SELECT s_name, s_address FROM supplier, nation WHERE s_nationkey = n_nationkey AND s_suppkey IN ({correlated}) AND n_name = 'ALGERIA' ORDER BY s_name"));
    assert!(nested.iter().any(pulled), "{nested:#?}");
    assert!(!nested[0][0].contains("Projection"), "{nested:#?}");
    let membership = nested.iter().find(|row| row[0].contains("Join") && row[4].contains("supplier.s_suppkey")).unwrap_or_else(|| panic!("{nested:#?}"));
    assert!(membership[4].contains("partsupp.ps_suppkey"), "{nested:#?}");
    assert_ne!(membership[1], "N/A", "{nested:#?}");
    assert!(nested.iter().any(|row| row[0].contains("Join") && row[4].contains("equal:[eq(test.nation.n_nationkey, test.supplier.s_nationkey)]")), "{nested:#?}");
    let selection = nested.iter().position(|row| row[0].contains("Selection") && row[4].contains("mul(0.5")).unwrap_or_else(|| panic!("{nested:#?}"));
    let selection_rows = nested[selection][1].parse::<f64>().unwrap();
    let aggregate_rows = nested[selection + 1][1].parse::<f64>().unwrap();
    assert!((selection_rows - aggregate_rows * 0.8).abs() < 0.02, "{nested:#?}");
    assert!(nested[selection][4].contains("cast(test.partsupp.ps_availqty, decimal(20,0) BINARY)") && nested[selection][4].contains("mul(0.5, Column#"), "{nested:#?}");
    assert!(nested.iter().all(|row| !row[4].contains("__decorrelated_")), "{nested:#?}");
}

#[test]
fn correlated_subqueries() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = session(&stack, 776);
    exec_all(&mut tk, &[
        "CREATE TABLE o (id BIGINT, v BIGINT)",
        "CREATE TABLE i (id BIGINT, w BIGINT)",
        "INSERT INTO o VALUES (1, 10), (2, 20), (3, 30)",
        "INSERT INTO i VALUES (1, 10), (2, 5), (2, 25), (4, 40)",
    ]);
    for (sql, want) in [
        ("SELECT id FROM o WHERE v = (SELECT MAX(w) FROM i WHERE i.id = o.id)", &["1"][..]),
        ("SELECT id FROM o WHERE v < (SELECT MAX(w) FROM i WHERE i.id = o.id)", &["2"][..]),
        ("SELECT id FROM o WHERE (SELECT MAX(w) FROM i WHERE i.id = o.id) IS NULL", &["3"][..]),
        ("SELECT id FROM o WHERE EXISTS (SELECT 1 FROM i WHERE i.id = o.id) ORDER BY id", &["1", "2"][..]),
        ("SELECT id FROM o WHERE NOT EXISTS (SELECT 1 FROM i WHERE i.id = o.id)", &["3"][..]),
        ("SELECT id FROM o WHERE EXISTS (SELECT 1 FROM i WHERE i.id = o.id) AND NOT EXISTS (SELECT 1 FROM i WHERE i.id = o.id AND i.w > 20) ORDER BY id", &["1"][..]),
        ("SELECT id FROM o WHERE EXISTS (SELECT 1 FROM i WHERE i.w = v)", &["1"][..]),
        ("SELECT id FROM o WHERE v IN (SELECT w FROM i WHERE i.id = o.id)", &["1"][..]),
        ("SELECT id FROM o WHERE v NOT IN (SELECT w FROM i WHERE i.id = o.id) ORDER BY id", &["2", "3"][..]),
        ("SELECT id FROM o WHERE v > ANY (SELECT w FROM i WHERE i.id = o.id)", &["2"][..]),
        ("SELECT id FROM o WHERE v > ALL (SELECT w FROM i WHERE i.id = o.id)", &["3"][..]),
    ] {
        assert_eq!(q(&mut tk, sql).into_iter().map(|row| row[0].clone()).collect::<Vec<_>>(), want, "{sql}");
    }
    assert_eq!(error(&mut tk, "SELECT id FROM o WHERE v = (SELECT w FROM i WHERE i.id = o.id)").code, 1242);
}

#[test]
fn correlated_exists_apply_answers_once_per_outer_row() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = session(&stack, 777);
    exec_all(&mut tk, &[
        "CREATE TABLE t (g BIGINT, v BIGINT)",
        "CREATE TABLE s (k BIGINT, x BIGINT)",
        "INSERT INTO t VALUES (1, 10), (1, 20), (2, 5), (3, 100), (NULL, 7)",
        "INSERT INTO s VALUES (1, 1), (1, 2), (2, 3)",
    ]);
    assert_eq!(
        q(&mut tk, "SELECT g, EXISTS(SELECT 1 FROM s WHERE s.k = t.g) FROM t ORDER BY g"),
        [["NULL", "0"], ["1", "1"], ["1", "1"], ["2", "1"], ["3", "0"]]
    );
}

/// A HAVING subquery may correlate only to the aggregation's output.
#[test]
fn a_having_subquery_may_only_correlate_to_the_aggregations_output() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = session(&stack, 778);
    exec_all(&mut tk, &[
        "CREATE TABLE ht (a INT, b INT)",
        "CREATE TABLE hs (x INT, y INT)",
        "INSERT INTO ht VALUES (1, 1), (2, 2)",
        "INSERT INTO hs VALUES (1, 5)",
    ]);
    for sql in [
        "SELECT a FROM ht GROUP BY a HAVING (SELECT y FROM hs WHERE hs.x = ht.b) > 0",
        "SELECT a FROM ht GROUP BY a HAVING EXISTS (SELECT 1 FROM hs WHERE hs.x = ht.b)",
        "SELECT a FROM ht GROUP BY a HAVING a IN (SELECT x FROM hs WHERE hs.y = ht.b)",
        "SELECT max(b) FROM ht HAVING (SELECT y FROM hs WHERE hs.x = ht.b) > 0",
    ] {
        let refused = error(&mut tk, sql);
        assert_eq!((refused.code, refused.message.as_str()), (1054, "Unknown column 'ht.b' in 'having clause'"), "{sql}");
    }
    for sql in [
        "SELECT a FROM ht GROUP BY a HAVING (SELECT y FROM hs WHERE hs.x = ht.a) > 0",
        "SELECT a FROM ht GROUP BY a HAVING (SELECT count(*) FROM hs) > 0",
        "SELECT a, b FROM ht HAVING (SELECT y FROM hs WHERE hs.x = ht.b) > 0",
    ] {
        rows(&mut tk, sql);
    }
}

#[test]
fn an_aggregate_over_a_correlated_exists_decorrelates() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = session(&stack, 779);
    exec_all(&mut tk, &["CREATE TABLE w (g VARCHAR(5), v INT)", "INSERT INTO w VALUES ('a',1),('a',2),('b',3),('b',4),('c',NULL)"]);
    assert_eq!(q(&mut tk, "SELECT count(*) FROM w a WHERE EXISTS (SELECT 1 FROM w b WHERE b.v = a.v)"), [["4"]]);
    assert_eq!(q(&mut tk, "SELECT count(*) FROM w a WHERE NOT EXISTS (SELECT 1 FROM w b WHERE b.v = a.v)"), [["1"]]);
    assert_eq!(q(&mut tk, "SELECT g, sum(v) FROM w a WHERE EXISTS (SELECT 1 FROM w b WHERE b.v = a.v) GROUP BY g ORDER BY g"), [["a", "3"], ["b", "7"]]);
}

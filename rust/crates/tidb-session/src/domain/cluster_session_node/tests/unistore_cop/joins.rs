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


//! Joins through Go's planner and executors: inner/outer/natural/USING
//! semantics, the anti semi join an outer join with IS NULL becomes, join
//! filter selectivity and constant propagation, covering and point leaves,
//! index-join probe estimates and ranges, merge-join orders and hints, and
//! outer-join elimination, over the cop-backed TestKit stack (X11).

use super::sql_kit::*;
use super::*;

fn sorted(tk: &mut Session, sql: &str) -> Vec<String> {
    let mut out = q(tk, sql).into_iter().map(|row| row.join(" ")).collect::<Vec<_>>();
    out.sort();
    out
}

#[test]
fn joins() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = session(&stack, 750);
    exec_all(&mut tk, &[
        "CREATE TABLE l (id BIGINT, v BIGINT)",
        "CREATE TABLE r (id BIGINT, w BIGINT)",
        "INSERT INTO l VALUES (1, 10), (2, 20), (3, 30)",
        "INSERT INTO r VALUES (1, 100), (3, 300), (3, 301)",
    ]);
    assert_eq!(q(&mut tk, "SELECT l.id, l.v, r.w FROM l JOIN r ON l.id = r.id ORDER BY l.id, r.w"), [["1", "10", "100"], ["3", "30", "300"], ["3", "30", "301"]]);
    assert_eq!(q(&mut tk, "SELECT l.id, r.w FROM l LEFT JOIN r ON l.id = r.id ORDER BY l.id, r.w"), [["1", "100"], ["2", "NULL"], ["3", "300"], ["3", "301"]]);
    assert_eq!(q(&mut tk, "SELECT l.id FROM l LEFT JOIN r ON l.id = r.id WHERE r.id IS NULL"), [["2"]]);
    assert_eq!(q(&mut tk, "SELECT l.id, r.w FROM l LEFT JOIN r ON l.id = r.id AND r.w > 200 ORDER BY l.id, r.w"), [["1", "NULL"], ["2", "NULL"], ["3", "300"], ["3", "301"]]);
    assert_eq!(q(&mut tk, "SELECT l.v, r.id FROM l RIGHT JOIN r ON l.id = r.id AND l.v > 100 ORDER BY r.id"), [["NULL", "1"], ["NULL", "3"], ["NULL", "3"]]);
    assert_eq!(q(&mut tk, "SELECT l.id FROM l, r").len(), 9);
    assert_eq!(q(&mut tk, "SELECT * FROM l JOIN r ON l.id = r.id")[0].len(), 4);
    assert_eq!(q(&mut tk, "SELECT r.* FROM l JOIN r ON l.id = r.id")[0].len(), 2);
    assert_eq!(error(&mut tk, "SELECT id FROM l JOIN r ON l.id = r.id").code, 1052);
    assert_eq!(q(&mut tk, "SELECT v, w FROM l JOIN r ON l.id = r.id").len(), 3);
    assert_eq!(q(&mut tk, "SELECT a.id FROM l AS a JOIN r AS b ON a.id = b.id").len(), 3);
    exec_all(&mut tk, &["CREATE TABLE m (id BIGINT)", "INSERT INTO m VALUES (3)"]);
    assert_eq!(q(&mut tk, "SELECT COUNT(*) FROM l JOIN r ON l.id = r.id JOIN m ON m.id = r.id"), [["2"]]);
    for sql in ["SELECT * FROM l NATURAL JOIN r", "SELECT * FROM l JOIN r USING (id)"] {
        assert_eq!(q(&mut tk, sql)[0].len(), 3, "{sql}");
    }
    assert_eq!(q(&mut tk, "SELECT id FROM l JOIN r USING (id)").len(), 3);
}

/// Go converts `LEFT JOIN ... WHERE inner.notnull IS NULL` into an anti semi
/// join; a NullEQ key keeps the outer join.
#[test]
fn outer_join_inner_key_is_null_becomes_anti_semi_join() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = session(&stack, 751);
    exec_all(&mut tk, &[
        "CREATE TABLE anti_l (id BIGINT, v BIGINT)",
        "CREATE TABLE anti_r (id BIGINT, w BIGINT NOT NULL)",
        "CREATE TABLE anti_d (id BIGINT)",
        "INSERT INTO anti_l VALUES (1,10),(2,20),(3,30)",
        "INSERT INTO anti_r VALUES (1,100),(3,300)",
        "INSERT INTO anti_d VALUES (1),(2),(3)",
    ]);
    let unnegated = |plan: &[String]| plan.iter().any(|line| line.contains("isnull(test.anti_r.id)") && !line.contains("not(isnull("));
    for (sql, want) in [
        ("SELECT anti_l.id FROM anti_l LEFT JOIN anti_r ON anti_l.id=anti_r.id WHERE anti_r.id IS NULL", vec![vec!["2".to_owned()]]),
        ("SELECT anti_l.id FROM (anti_l LEFT JOIN anti_r ON anti_l.id=anti_r.id) JOIN anti_d ON anti_d.id=anti_l.id WHERE anti_r.id IS NULL", vec![vec!["2".to_owned()]]),
        ("SELECT anti_l.id FROM anti_l LEFT JOIN anti_r ON anti_l.id=anti_r.id WHERE anti_r.w IS NULL", vec![vec!["2".to_owned()]]),
        ("SELECT anti_l.id FROM anti_r RIGHT JOIN anti_l ON anti_r.id=anti_l.id WHERE anti_r.id IS NULL", vec![vec!["2".to_owned()]]),
        ("SELECT anti_l.id, anti_r.w FROM anti_l LEFT JOIN anti_r ON anti_l.id=anti_r.id WHERE anti_r.id IS NULL", vec![vec!["2".to_owned(), "NULL".to_owned()]]),
    ] {
        assert_eq!(q(&mut tk, sql), want, "{sql}");
        let plan = plan_lines(&mut tk, sql);
        assert!(plan.iter().any(|line| line.contains("anti semi join")), "{sql}: {plan:#?}");
        assert!(!unnegated(&plan), "{sql}: {plan:#?}");
    }
    let plan = plan_lines(&mut tk, "SELECT anti_l.id FROM anti_l LEFT JOIN anti_r ON anti_l.id <=> anti_r.id WHERE anti_r.id IS NULL");
    assert!(plan.iter().any(|line| line.contains("left outer join")), "{plan:#?}");
}

fn reader_estimate(plan: &[Vec<String>], table: &str) -> String {
    let scan = plan.iter().position(|row| row[3] == format!("table:{table}")).unwrap_or_else(|| panic!("no scan for {table}: {plan:#?}"));
    let reader = (0..scan).rev().find(|&i| plan[i][0].contains("TableReader")).unwrap_or_else(|| panic!("no reader above {table}: {plan:#?}"));
    plan[reader][1].clone()
}

fn ab_tables(tk: &mut Session) {
    exec_all(tk, &["CREATE TABLE a (x INT NOT NULL, y INT)", "CREATE TABLE b (x INT NOT NULL)"]);
    let a_rows = (1..=100).map(|v| format!("({v}, {v})")).collect::<Vec<_>>().join(",");
    rows(tk, &format!("INSERT INTO a VALUES {a_rows}"));
    let b_rows = (1..=100).map(|v| format!("({v})")).collect::<Vec<_>>().join(",");
    rows(tk, &format!("INSERT INTO b VALUES {b_rows}"));
    rows(tk, "ANALYZE TABLE a, b");
}

/// A single-table filter scales only its own side's estimate.
#[test]
fn a_join_filter_is_charged_only_to_the_filtered_side() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = session(&stack, 752);
    ab_tables(&mut tk);
    let plan = explain_brief(&mut tk, "SELECT * FROM a, b WHERE a.x = b.x AND a.y > 50");
    assert_eq!(reader_estimate(&plan, "a"), "51.00", "{plan:#?}");
    assert_eq!(reader_estimate(&plan, "b"), "100.00", "{plan:#?}");
}

/// The join equality's constant propagates to the other side.
#[test]
fn a_join_equality_propagates_its_constant_to_the_other_side() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = session(&stack, 753);
    ab_tables(&mut tk);
    let plan = explain_brief(&mut tk, "SELECT * FROM a, b WHERE a.x = b.x AND b.x = 7 AND a.y > 50");
    let selection = plan.iter().find(|row| row[0].contains("Selection") && row[4].contains("test.a.y")).unwrap_or_else(|| panic!("{plan:#?}"));
    assert!(selection[4].contains("eq(test.a.x, 7)"), "{plan:#?}");
    assert_eq!(reader_estimate(&plan, "a"), "1.00", "{plan:#?}");
}

#[test]
fn a_join_leaf_reads_a_covering_index_without_moving_a_row() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = session(&stack, 754);
    exec_all(&mut tk, &[
        "CREATE TABLE jl (a INT PRIMARY KEY, b INT, c VARCHAR(32), KEY(b))",
        "CREATE TABLE jr (a INT PRIMARY KEY, b INT, c VARCHAR(32), KEY(b))",
        "INSERT INTO jl VALUES (1, 10, 'x'), (2, 20, 'y'), (3, 30, 'z')",
        "INSERT INTO jr VALUES (7, 10, 'p'), (8, 30, 'q')",
    ]);
    let covering = "SELECT jl.a FROM jl LEFT JOIN jr ON jl.b = jr.b";
    let plan = plan_lines(&mut tk, covering);
    assert!(plan.iter().any(|line| line.contains("IndexFullScan") && line.contains("table:jl")), "{plan:?}");
    let plan = plan_lines(&mut tk, "SELECT * FROM jl LEFT JOIN jr ON jl.b = jr.b");
    assert!(plan.iter().any(|line| line.contains("TableFullScan") && line.contains("table:jl")), "{plan:?}");
    assert_eq!(sorted(&mut tk, covering), ["1", "2", "3"]);
}

#[test]
fn a_join_leaf_uses_a_single_table_constant_for_access() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = session(&stack, 755);
    exec_all(&mut tk, &[
        "CREATE TABLE lp (id INT PRIMARY KEY, value INT)",
        "CREATE TABLE rp (id INT PRIMARY KEY, value INT)",
        "INSERT INTO lp VALUES (1, 10), (2, 20), (3, 30)",
        "INSERT INTO rp VALUES (7, 20), (8, 20), (9, 30)",
    ]);
    let sql = "SELECT lp.id, rp.id FROM lp JOIN rp ON lp.value = rp.value WHERE lp.id = 2";
    let plan = plan_lines(&mut tk, sql);
    assert!(plan.iter().any(|line| line.contains("Point_Get") && line.contains("table:lp")), "{plan:?}");
    assert_eq!(sorted(&mut tk, sql), ["2 7", "2 8"]);
}

/// Under pseudo statistics the index join's inner scan shows its clamped row
/// times the 1.25 outer probes, and the probe Selection its residual
/// selectivity.
#[test]
fn an_index_join_probe_displays_the_outer_probe_count() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = session(&stack, 756);
    exec_all(&mut tk, &[
        "CREATE TABLE order_line (ol_o_id INT NOT NULL, ol_d_id INT NOT NULL, ol_w_id INT NOT NULL, ol_number INT NOT NULL, ol_i_id INT NOT NULL, PRIMARY KEY (ol_w_id, ol_d_id, ol_o_id, ol_number) CLUSTERED)",
        "CREATE TABLE stock (s_i_id INT NOT NULL, s_w_id INT NOT NULL, s_quantity INT, PRIMARY KEY (s_w_id, s_i_id) CLUSTERED)",
        "INSERT INTO order_line VALUES (3627, 7, 1, 1, 100), (3630, 7, 1, 1, 101), (3647, 7, 1, 1, 102), (3630, 8, 1, 1, 101)",
        "INSERT INTO stock VALUES (100, 1, 10), (101, 1, 20), (102, 1, 5), (100, 2, 5)",
    ]);
    let plan = explain_brief(
        &mut tk,
        "SELECT /*+ TIDB_INLJ(`order_line`, `stock`)*/ COUNT(DISTINCT (`s_i_id`)) AS `stock_count` FROM (`order_line`) JOIN `stock` \
         WHERE `ol_w_id`=1 AND `ol_d_id`=7 AND `ol_o_id`<3647 AND `ol_o_id`>=3647-20 AND `s_w_id`=1 AND `s_i_id`=`ol_i_id` AND `s_quantity`<18",
    );
    let scan = plan.iter().find(|row| row[0].contains("TableRangeScan") && row[3] == "table:stock").unwrap_or_else(|| panic!("{plan:#?}"));
    assert_eq!(scan[1], "1.25", "{plan:#?}");
    let reader = plan.iter().find(|row| row[0].contains("TableReader") && row[0].contains("(Probe)")).unwrap_or_else(|| panic!("{plan:#?}"));
    assert_eq!(reader[1], "0.42", "{plan:#?}");
}

#[test]
fn a_complete_common_handle_equality_is_a_point_get() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = session(&stack, 757);
    exec_all(&mut tk, &[
        "CREATE TABLE customer (c_id INT NOT NULL, c_d_id INT NOT NULL, c_w_id INT NOT NULL, c_last VARCHAR(16), PRIMARY KEY (c_w_id, c_d_id, c_id))",
        "CREATE TABLE warehouse (w_id INT NOT NULL, PRIMARY KEY (w_id))",
        "INSERT INTO customer VALUES (629, 6, 1, 'Smith')",
        "INSERT INTO warehouse VALUES (1)",
    ]);
    let plan = plan_lines(&mut tk, "SELECT c_last FROM customer, warehouse WHERE w_id = 1 AND c_w_id = w_id AND c_d_id = 6 AND c_id = 629");
    assert!(plan.iter().any(|line| line.contains("Point_Get") && line.contains("table:customer")), "{plan:?}");
    assert!(!plan.iter().any(|line| line.contains("TableRangeScan")), "{plan:?}");
}

#[test]
fn a_parent_does_not_consume_an_order_its_child_does_not_deliver() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = session(&stack, 758);
    exec_all(&mut tk, &[
        "CREATE TABLE bot (a BIGINT PRIMARY KEY, k BIGINT)",
        "CREATE TABLE mid (a BIGINT PRIMARY KEY, k BIGINT)",
        "CREATE TABLE top (a BIGINT PRIMARY KEY)",
        "INSERT INTO bot VALUES (1, 7), (2, 7), (3, 8)",
        "INSERT INTO mid VALUES (10, 8), (20, 7), (30, 7)",
        "INSERT INTO top VALUES (10), (20), (30)",
    ]);
    assert_eq!(
        sorted(&mut tk, "SELECT bot.a, mid.a, top.a FROM bot JOIN mid ON bot.k = mid.k JOIN top ON mid.a = top.a"),
        ["1 20 20", "1 30 30", "2 20 20", "2 30 30", "3 10 10"]
    );
}

#[test]
fn a_leaf_delivers_only_the_order_its_scan_walks_in() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = session(&stack, 759);
    exec_all(&mut tk, &[
        "CREATE TABLE il (h BIGINT PRIMARY KEY, s BIGINT NOT NULL, KEY ks (s))",
        "CREATE TABLE ir (h BIGINT PRIMARY KEY, s BIGINT NOT NULL, KEY ks (s))",
        "INSERT INTO il VALUES (1, 30), (2, 10), (3, 20)",
        "INSERT INTO ir VALUES (1, 20), (2, 30), (3, 10)",
    ]);
    assert_eq!(sorted(&mut tk, "SELECT il.h, ir.h FROM il JOIN ir ON il.s = ir.s"), ["1 2", "2 3", "3 1"]);
}

#[test]
fn a_leaf_asked_for_an_index_order_walks_the_index_and_says_so() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = session(&stack, 760);
    exec_all(&mut tk, &["CREATE TABLE oi (a INT NOT NULL, KEY idx (a))", "INSERT INTO oi VALUES (1), (2), (2), (3)"]);
    let sql = "SELECT t1.a, t2.a FROM oi t1 JOIN oi t2 ON t1.a = t2.a";
    let plan = plan_lines(&mut tk, sql);
    assert!(plan.iter().any(|line| line.contains("MergeJoin")), "{plan:?}");
    for side in ["table:t1", "table:t2"] {
        assert!(plan.iter().any(|line| line.contains("IndexFullScan") && line.contains(side) && line.contains("keep order:true")), "{side}: {plan:?}");
    }
    assert_eq!(sorted(&mut tk, sql), ["1 1", "2 2", "2 2", "2 2", "2 2", "3 3"]);
}

#[test]
fn a_forced_merge_lowers_the_planner_selected_sort_enforcers() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = session(&stack, 761);
    exec_all(&mut tk, &[
        "CREATE TABLE ncl (k BIGINT NOT NULL, o BIGINT NOT NULL, payload BIGINT, PRIMARY KEY (k, o) NONCLUSTERED)",
        "CREATE TABLE ncr (k BIGINT NOT NULL, o BIGINT NOT NULL, payload BIGINT, PRIMARY KEY (k, o) NONCLUSTERED)",
        "INSERT INTO ncl VALUES (1, 1, 10), (2, 1, 20)",
        "INSERT INTO ncr VALUES (1, 1, 100), (2, 1, 200)",
    ]);
    let sql = "SELECT /*+ TIDB_SMJ(l, r) */ l.payload, r.payload FROM ncl l JOIN ncr r ON l.k = r.k AND l.o = r.o";
    let plan = plan_lines(&mut tk, sql);
    assert!(plan.iter().any(|line| line.contains("MergeJoin")), "{plan:#?}");
    for (side, keys) in [("table:l", "test.ncl.k, test.ncl.o"), ("table:r", "test.ncr.k, test.ncr.o")] {
        assert!(plan.iter().any(|line| line.contains("Sort") && line.contains(keys)), "{side}: {plan:#?}");
        assert!(plan.iter().any(|line| line.contains("TableFullScan") && line.contains(side) && line.contains("keep order:false")), "{side}: {plan:#?}");
    }
    assert_eq!(sorted(&mut tk, sql), ["10 100", "20 200"]);
}

/// A join hint decides the join family before any cost is compared.
#[test]
fn a_join_hint_decides_the_family_before_any_cost_is_compared() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = session(&stack, 762);
    exec_all(&mut tk, &["CREATE TABLE hj (a INT NOT NULL, KEY idx (a))", "INSERT INTO hj VALUES (1), (2), (3)"]);
    let body = "* FROM hj t1 JOIN hj t2 ON t1.a = t2.a";
    assert!(plan_lines(&mut tk, &format!("SELECT /*+ TIDB_SMJ(t1, t2) */ {body}")).iter().any(|line| line.contains("MergeJoin")));
    for hint in ["TIDB_HJ(t1, t2)", "TIDB_INLJ(t2)"] {
        let plan = plan_lines(&mut tk, &format!("SELECT /*+ {hint} */ {body}"));
        assert!(!plan.iter().any(|line| line.contains("MergeJoin")), "{hint}: {plan:?}");
    }
    for hint in ["TIDB_SMJ(t1, t2)", "TIDB_HJ(t1, t2)", "TIDB_INLJ(t2)"] {
        assert_eq!(sorted(&mut tk, &format!("SELECT /*+ {hint} */ t1.a, t2.a FROM hj t1 JOIN hj t2 ON t1.a = t2.a")), ["1 1", "2 2", "3 3"], "{hint}");
    }
}

/// An outer comparison on the next key column narrows every probe range:
/// Go prints `eq(inner, outer)` pairs and both outer-derived bounds.
#[test]
fn an_outer_comparison_on_the_next_key_column_narrows_every_probe_range() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = session(&stack, 763);
    exec_all(&mut tk, &[
        "CREATE TABLE district (d_id INT NOT NULL, d_w_id INT NOT NULL, d_next_o_id INT, PRIMARY KEY (d_w_id,d_id))",
        "CREATE TABLE order_line (ol_o_id INT NOT NULL, ol_d_id INT NOT NULL, ol_w_id INT NOT NULL, ol_number INT NOT NULL, PRIMARY KEY (ol_w_id,ol_d_id,ol_o_id,ol_number))",
        "INSERT INTO district VALUES (1,1,30),(2,1,NULL)",
        "INSERT INTO order_line VALUES (9,1,1,1),(10,1,1,1),(11,1,1,1),(29,1,1,1),(30,1,1,1),(31,1,1,1),(40,1,1,1),(10,2,1,1)",
    ]);
    let sql = "SELECT /*+ TIDB_INLJ(order_line) */ ol_o_id FROM district JOIN order_line ON ol_w_id = d_w_id AND ol_d_id = d_id \
               AND ol_o_id >= d_next_o_id - 20 AND ol_o_id < d_next_o_id";
    let mut got = q(&mut tk, sql).into_iter().map(|row| row[0].parse::<i64>().unwrap()).collect::<Vec<_>>();
    got.sort_unstable();
    assert_eq!(got, [10, 11, 29]);
    let plan = plan_lines(&mut tk, sql);
    assert!(plan.iter().any(|line| line.contains("IndexJoin")), "{plan:#?}");
    assert!(
        plan.iter().any(|line| line.contains("TableRangeScan")
            && line.contains("table:order_line")
            && line.contains("eq(test.order_line.ol_w_id, test.district.d_w_id)")
            && line.contains("eq(test.order_line.ol_d_id, test.district.d_id)")
            && line.contains("ge(test.order_line.ol_o_id, minus(test.district.d_next_o_id, 20))")
            && line.contains("lt(test.order_line.ol_o_id, test.district.d_next_o_id)")),
        "{plan:#?}"
    );
}

/// Go's outer-join elimination runs to a fixpoint over unread PK-joined
/// inner sides.
#[test]
fn nested_unread_left_join_chain_eliminates_to_fixpoint() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = session(&stack, 764);
    exec_all(&mut tk, &[
        "CREATE TABLE jc_a (pk VARCHAR(32) PRIMARY KEY, v BIGINT)",
        "CREATE TABLE jc_b (pk VARCHAR(32) PRIMARY KEY)",
        "CREATE TABLE jc_c (pk VARCHAR(32) PRIMARY KEY)",
    ]);
    let mentions = |tk: &mut Session, sql: &str, table: &str| plan_lines(tk, sql).iter().any(|line| line.contains(table));
    let sql = "SELECT jc_a.v FROM (jc_a LEFT JOIN jc_b ON jc_a.pk = jc_b.pk) LEFT JOIN jc_c ON jc_b.pk = jc_c.pk WHERE jc_a.v < 10 LIMIT 200";
    assert!(!mentions(&mut tk, sql, "jc_b"));
    assert!(!mentions(&mut tk, sql, "jc_c"));
    assert!(mentions(&mut tk, "SELECT jc_a.v, jc_b.pk FROM (jc_a LEFT JOIN jc_b ON jc_a.pk = jc_b.pk) LEFT JOIN jc_c ON jc_b.pk = jc_c.pk WHERE jc_a.v < 10 LIMIT 200", "jc_b"));
    assert!(!mentions(&mut tk, "SELECT jc_a.v FROM jc_a LEFT JOIN jc_b ON (jc_a.pk = jc_b.pk) WHERE jc_a.v < 10 LIMIT 200", "jc_b"));
}

/// A join leaf's LIMIT counts rows after its residual selection.
#[test]
fn a_join_leaf_limit_counts_after_its_residual_selection() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = session(&stack, 765);
    exec_all(&mut tk, &[
        "CREATE TABLE jr_core (pk BIGINT PRIMARY KEY CLUSTERED, mi VARCHAR(8), m BIGINT, country VARCHAR(4), KEY mi_m (mi, m))",
        "CREATE TABLE jr_ts (pk BIGINT PRIMARY KEY CLUSTERED, w BIGINT)",
    ]);
    let core = (1..=60).map(|pk| format!("({pk}, 'x', 7, '{}')", if pk % 2 == 0 { "CN" } else { "US" })).collect::<Vec<_>>().join(", ");
    rows(&mut tk, &format!("INSERT INTO jr_core (pk, mi, m, country) VALUES {core}"));
    let ts = (1..=60).map(|pk| format!("({pk}, {})", pk * 10)).collect::<Vec<_>>().join(", ");
    rows(&mut tk, &format!("INSERT INTO jr_ts VALUES {ts}"));
    let pks = |tk: &mut Session, sql: &str| {
        let mut out = q(tk, sql).into_iter().map(|row| row[0].parse::<i64>().unwrap()).collect::<Vec<_>>();
        out.sort_unstable();
        out
    };
    let base = "SELECT c.pk, t.w FROM jr_core c FORCE INDEX (mi_m) LEFT JOIN jr_ts t ON (c.pk = t.pk) WHERE c.m = 7 AND c.mi = 'x' AND c.country = 'CN' LIMIT";
    assert_eq!(pks(&mut tk, &format!("{base} 50")), (1..=30).map(|i| i * 2).collect::<Vec<_>>());
    assert_eq!(pks(&mut tk, &format!("{base} 20")), (1..=20).map(|i| i * 2).collect::<Vec<_>>());
    assert_eq!(pks(&mut tk, "SELECT c.pk FROM jr_core c FORCE INDEX (mi_m) LEFT JOIN jr_ts t ON (c.pk = t.pk) WHERE c.m = 7 AND c.mi = 'x' LIMIT 20"), (1..=20).collect::<Vec<_>>());
}

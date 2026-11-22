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


//! Index and handle ranges (Go `pkg/util/ranger`, `pkg/planner` access
//! paths): the ranges Go builds and prints, multi-column and multi-range
//! scans agreeing with a full scan, the handle dimension a non-unique index
//! carries, LIKE over collations, ordered handle walks and limits, over the
//! cop-backed TestKit stack (X11).

use super::sql_kit::*;
use super::*;

fn ids(tk: &mut Session, sql: &str) -> Vec<i64> {
    let mut ids = rows(tk, sql)
        .into_iter()
        .map(|row| match row[0] {
            Datum::Int(v) => v,
            ref other => panic!("expected an int, got {other:?}"),
        })
        .collect::<Vec<_>>();
    ids.sort_unstable();
    ids
}

fn sorted(tk: &mut Session, sql: &str) -> Vec<String> {
    let mut out = q(tk, sql).into_iter().map(|row| row.join(" ")).collect::<Vec<_>>();
    out.sort();
    out
}

fn bare(name: &str) -> String {
    name.trim_start_matches(|ch| matches!(ch, ' ' | '└' | '├' | '│' | '─')).to_owned()
}

/// An ordered LIMIT over a clustered prefix keeps order on the table range;
/// over a non-covering index the limit embeds in the IndexLookUp and its
/// offset is consumed by the handle stream.
#[test]
fn ordered_limit_adjusts_the_common_handle_scan_estimate() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = session(&stack, 730);
    exec_all(&mut tk, &[
        "CREATE TABLE new_order (no_o_id INT NOT NULL, no_d_id INT NOT NULL, no_w_id INT NOT NULL, PRIMARY KEY (no_w_id, no_d_id, no_o_id) CLUSTERED)",
        "INSERT INTO new_order VALUES (1,1,1),(2,1,1),(1,2,1),(1,1,2)",
    ]);
    let plan = explain_brief(&mut tk, "SELECT no_o_id FROM new_order WHERE no_w_id=1 AND no_d_id=1 ORDER BY no_o_id LIMIT 1 FOR UPDATE");
    let scan = plan.iter().find(|row| row[0].contains("TableRangeScan")).unwrap_or_else(|| panic!("{plan:#?}"));
    assert!(scan[4].contains("range:[1 1,1 1]") && scan[4].contains("keep order:true"), "{plan:#?}");

    exec_all(&mut tk, &[
        "CREATE TABLE orders (o_id INT NOT NULL, o_d_id INT NOT NULL, o_w_id INT NOT NULL, o_c_id INT, o_entry_d DATETIME, o_carrier_id INT, o_ol_cnt INT, o_all_local INT, PRIMARY KEY (o_w_id, o_d_id, o_id) CLUSTERED, KEY idx_order (o_w_id, o_d_id, o_c_id, o_id))",
        "INSERT INTO orders VALUES (1,1,1,1,NULL,NULL,1,1), (2,1,1,1,NULL,2,1,1), (3,1,1,2,NULL,3,1,1), (1,2,1,1,NULL,4,1,1)",
    ]);
    // Go's plan under pseudo statistics (captured with gorun): a cop TopN
    // over the clustered prefix range, and for an offset a cop Limit that
    // keeps the reverse order. The IndexLookUp with an embedded limit Go picks
    // under TPC-C statistics belongs to its statistics-loaded casetests.
    let plan = explain_brief(&mut tk, "SELECT o_id, o_carrier_id, o_entry_d FROM orders WHERE o_w_id=1 AND o_d_id=1 AND o_c_id=1 ORDER BY o_id DESC LIMIT 1");
    assert_eq!(plan.iter().map(|row| bare(&row[0])).collect::<Vec<_>>(), ["Projection", "TopN", "TableReader", "TopN", "Selection", "TableRangeScan"], "{plan:#?}");
    assert_eq!(plan[3][4], "test.orders.o_id:desc, offset:0, count:1", "{plan:#?}");
    let offset_sql = "SELECT o_id, o_carrier_id, o_entry_d FROM orders WHERE o_w_id=1 AND o_d_id=1 AND o_c_id=1 ORDER BY o_id DESC LIMIT 1,1";
    assert_eq!(q(&mut tk, offset_sql), [["1", "NULL", "NULL"]]);
    let plan = explain_brief(&mut tk, offset_sql);
    assert_eq!(plan.iter().map(|row| bare(&row[0])).collect::<Vec<_>>(), ["Projection", "Limit", "TableReader", "Limit", "Selection", "TableRangeScan"], "{plan:#?}");
    assert_eq!((plan[1][4].as_str(), plan[3][4].as_str()), ("offset:1, count:1", "offset:0, count:2"), "{plan:#?}");
    assert!(plan[5][4].contains("keep order:true, desc"), "{plan:#?}");
}

#[test]
fn non_covering_random_points_use_index_lookup_without_identity_projection() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = session(&stack, 731);
    exec_all(&mut tk, &[
        "CREATE TABLE lookup_shape (id INT PRIMARY KEY, k INT NOT NULL, c CHAR(4), pad CHAR(4), KEY k_1(k))",
        "INSERT INTO lookup_shape VALUES (1, 1, 'a', 'x'), (2, 2, 'b', 'y')",
    ]);
    let sql = "SELECT id, k, c, pad FROM lookup_shape WHERE k IN (1, 2)";
    let plan = explain_brief(&mut tk, sql);
    assert_eq!(plan.iter().map(|row| row[0].as_str()).collect::<Vec<_>>(), ["IndexLookUp", "├─IndexRangeScan(Build)", "└─TableRowIDScan(Probe)"]);
    assert_eq!(plan.iter().map(|row| row[2].as_str()).collect::<Vec<_>>(), ["root", "cop[tikv]", "cop[tikv]"]);
    assert_eq!(q(&mut tk, sql).len(), 2);
}

#[test]
fn multi_column_and_multi_range_scans_read_the_same_rows_as_a_full_scan() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = session(&stack, 732);
    exec_all(&mut tk, &[
        "CREATE TABLE m (id BIGINT PRIMARY KEY, a BIGINT, b BIGINT, KEY ab (a, b))",
        "INSERT INTO m VALUES (1, 1, 1), (2, 1, 5), (3, 1, 9), (4, 2, 5), (5, 3, 5), (6, NULL, 1), (7, 2, NULL)",
    ]);
    for (sql, want) in [
        ("SELECT id FROM m WHERE a = 1 AND b > 1", &[2, 3][..]),
        ("SELECT id FROM m WHERE a = 1 AND b BETWEEN 1 AND 5", &[1, 2][..]),
        ("SELECT id FROM m WHERE a IN (1, 3) AND b = 5", &[2, 5][..]),
        ("SELECT id FROM m WHERE (a = 1 AND b = 5) OR (a = 3 AND b = 5)", &[2, 5][..]),
        ("SELECT id FROM m WHERE a IS NULL", &[6][..]),
        ("SELECT id FROM m WHERE a = 2 AND b IS NULL", &[7][..]),
        ("SELECT id FROM m WHERE a = 1 AND id > 1", &[2, 3][..]),
    ] {
        assert_eq!(ids(&mut tk, sql), want, "{sql}");
    }
}

#[test]
fn index_range_scans() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = session(&stack, 733);
    exec_all(&mut tk, &[
        "CREATE TABLE r (id BIGINT PRIMARY KEY, score BIGINT, KEY score_idx (score))",
        "INSERT INTO r VALUES (1, 10), (2, 20), (3, 30), (4, 20), (5, NULL)",
    ]);
    for (sql, want) in [
        ("SELECT id FROM r WHERE score > 15", &[2, 3, 4][..]),
        ("SELECT id FROM r WHERE score >= 20", &[2, 3, 4][..]),
        ("SELECT id FROM r WHERE score < 30", &[1, 2, 4][..]),
        ("SELECT id FROM r WHERE score <= 10", &[1][..]),
        ("SELECT id FROM r WHERE score = 20", &[2, 4][..]),
        ("SELECT id FROM r WHERE 15 < score", &[2, 3, 4][..]),
        ("SELECT id FROM r WHERE score > 10 AND score < 30", &[2, 4][..]),
        ("SELECT id FROM r WHERE score >= 20 AND score <= 20", &[2, 4][..]),
        ("SELECT id FROM r WHERE score > -100", &[1, 2, 3, 4][..]),
        ("SELECT id FROM r WHERE score IS NULL", &[5][..]),
        ("SELECT id FROM r WHERE score > 15 AND id = 3", &[3][..]),
    ] {
        assert_eq!(ids(&mut tk, sql), want, "{sql}");
    }
    rows(&mut tk, "UPDATE r SET score = 99 WHERE id = 1");
    assert_eq!(ids(&mut tk, "SELECT id FROM r WHERE score > 50"), [1]);
    rows(&mut tk, "DELETE FROM r WHERE id = 1");
    assert!(ids(&mut tk, "SELECT id FROM r WHERE score > 50").is_empty());
}

#[test]
fn index_range_scan_over_a_unique_index() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = session(&stack, 734);
    exec_all(&mut tk, &["CREATE TABLE u2 (id BIGINT PRIMARY KEY, code BIGINT UNIQUE)", "INSERT INTO u2 VALUES (1, 100), (2, 200), (3, 300), (4, NULL)"]);
    assert_eq!(ids(&mut tk, "SELECT id FROM u2 WHERE code >= 200"), [2, 3]);
    assert_eq!(ids(&mut tk, "SELECT id FROM u2 WHERE code IS NULL"), [4]);
}

/// The ranges Go builds, as EXPLAIN prints them.
#[test]
fn index_ranges_are_built_the_way_go_builds_them() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = session(&stack, 735);
    rows(&mut tk, "CREATE TABLE q (id BIGINT PRIMARY KEY, score BIGINT, note VARCHAR(8), KEY s (score))");
    let range_of = |tk: &mut Session, sql: &str| {
        let plan = explain_brief(tk, sql);
        plan.iter().find(|row| row[0].contains("IndexRangeScan")).map(|row| row[4].split(", keep order").next().unwrap().to_owned())
    };
    assert_eq!(range_of(&mut tk, "SELECT id FROM q WHERE score > 5").as_deref(), Some("range:(5,+inf]"));
    assert_eq!(range_of(&mut tk, "SELECT id FROM q WHERE score < 5").as_deref(), Some("range:[-inf,5)"));
    assert_eq!(range_of(&mut tk, "SELECT id FROM q WHERE score > 5 AND score <= 9").as_deref(), Some("range:(5,9]"));
    assert_eq!(range_of(&mut tk, "SELECT id FROM q WHERE score > 1 OR score < 0").as_deref(), Some("range:[-inf,0), (1,+inf]"));
    assert_eq!(plan_lines(&mut tk, "SELECT id FROM q WHERE score > NULL"), ["TableDual\t0.00\troot\t\trows:0"]);
    assert!(plan_lines(&mut tk, "SELECT id FROM q WHERE note = 'x'").iter().all(|line| !line.contains("index:")));
    assert!(plan_lines(&mut tk, "SELECT id FROM q").iter().any(|line| line.contains("IndexFullScan") && line.contains("index:s(score)")));
}

#[test]
fn a_like_over_a_case_insensitive_index_returns_gos_rows() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = session(&stack, 736);
    exec_all(&mut tk, &[
        "CREATE TABLE ci (a VARCHAR(50) COLLATE utf8mb4_general_ci, KEY idx (a))",
        "INSERT INTO ci VALUES ('ab`q'), ('abAq'), ('abzz'), ('abcq'), ('abéxx')",
    ]);
    for (pattern, want) in [("abé%", "abéxx"), ("ab`%", "ab`q"), ("abA%", "abAq"), ("aba%", "abAq"), ("abz%", "abzz")] {
        assert_eq!(sorted(&mut tk, &format!("SELECT a FROM ci WHERE a LIKE '{pattern}'")), [want], "{pattern}");
    }
}

#[test]
fn a_like_with_trailing_spaces_starts_at_the_trimmed_key() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = session(&stack, 737);
    exec_all(&mut tk, &["CREATE TABLE bn (a VARCHAR(50) COLLATE utf8mb4_bin, KEY idx (a))", "INSERT INTO bn VALUES ('abc  '), ('abc  x'), ('abc'), ('abd')"]);
    assert_eq!(sorted(&mut tk, "SELECT a FROM bn WHERE a LIKE 'abc  %'"), ["abc  ", "abc  x"]);
}

/// A non-unique index ranges on the clustered handle behind its columns.
#[test]
fn a_non_unique_index_ranges_on_the_clustered_handle_too() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = session(&stack, 738);
    rows(&mut tk, "CREATE TABLE t1 (c1 INT PRIMARY KEY, c2 INT, c3 INT, INDEX c2 (c2))");
    let plan = plan_lines(&mut tk, "SELECT * FROM t1 WHERE c1 > 1 AND c2 = 1 AND c3 < 1");
    assert!(plan.iter().any(|line| line.contains("index:c2(c2)") && line.contains("range:(1 1,1 +inf]")), "{plan:?}");
    let plan = plan_lines(&mut tk, "SELECT * FROM t1 WHERE c2 = 1");
    assert!(plan.iter().any(|line| line.contains("index:c2(c2)") && line.contains("range:[1,1]")), "{plan:?}");
}

#[test]
fn a_handle_range_reads_the_rows_a_full_scan_reads() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = session(&stack, 739);
    exec_all(&mut tk, &[
        "CREATE TABLE h (c1 BIGINT PRIMARY KEY, c2 BIGINT, c3 BIGINT, INDEX c2 (c2))",
        "INSERT INTO h VALUES (-9, 1, 0), (-1, 1, 0), (0, 1, 0), (1, 1, 0), (2, 1, 0), (3, 1, 0), (4, 2, 0), (5, NULL, 0), (6, -1, 0), (7, 0, 0)",
    ]);
    for predicate in [
        "c2 = 1 AND c1 > 1", "c2 = 1 AND c1 >= 1", "c2 = 1 AND c1 < 0", "c2 = 1 AND c1 <= -1",
        "c2 = 1 AND c1 BETWEEN -1 AND 2", "c2 = 1 AND c1 > -100", "c2 = 1 AND c1 <> 0",
        "c2 IN (1, 2) AND c1 > 0", "c2 = -1 AND c1 > 0", "c2 = 0 AND c1 > 0", "c2 IS NULL AND c1 > 0",
        "c2 = 1 AND c1 > 3", "c2 = 1 AND c1 > 100",
    ] {
        assert_eq!(
            ids(&mut tk, &format!("SELECT c1 FROM h USE INDEX (c2) WHERE {predicate}")),
            ids(&mut tk, &format!("SELECT c1 FROM h IGNORE INDEX (c2) WHERE {predicate}")),
            "{predicate}"
        );
    }
    assert_eq!(ids(&mut tk, "SELECT c1 FROM h USE INDEX (c2) WHERE c2 = 1 AND c1 > 1"), [2, 3]);
    assert_eq!(ids(&mut tk, "SELECT c1 FROM h USE INDEX (c2) WHERE c2 = 1 AND c1 <= -1"), [-9, -1]);
    assert_eq!(ids(&mut tk, "SELECT c1 FROM h USE INDEX (c2) WHERE c2 = 1 AND c1 BETWEEN -1 AND 2"), [-1, 0, 1, 2]);
    assert_eq!(ids(&mut tk, "SELECT c1 FROM h USE INDEX (c2) WHERE c2 IS NULL AND c1 > 0"), [5]);
}

/// A unique index carries no handle dimension: Go answers with a Point_Get
/// and a root Selection on the handle.
#[test]
fn a_unique_index_gets_no_handle_dimension() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = session(&stack, 740);
    exec_all(&mut tk, &["CREATE TABLE u (c1 BIGINT PRIMARY KEY, c2 BIGINT, UNIQUE KEY uc2 (c2))", "INSERT INTO u VALUES (1, 10), (2, 20), (3, 30)"]);
    let plan = plan_lines(&mut tk, "SELECT * FROM u USE INDEX (uc2) WHERE c2 = 20 AND c1 > 1");
    assert!(plan.iter().all(|line| !line.contains("range:(20 1")), "{plan:?}");
    assert_eq!(ids(&mut tk, "SELECT c1 FROM u USE INDEX (uc2) WHERE c2 = 20 AND c1 > 1"), [2]);
}

#[test]
fn an_unsigned_clustered_handle_gets_no_handle_dimension() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = session(&stack, 741);
    exec_all(&mut tk, &[
        "CREATE TABLE hu (c1 BIGINT UNSIGNED PRIMARY KEY, c2 INT, INDEX c2 (c2))",
        "INSERT INTO hu VALUES (1, 1), (9223372036854775807, 1), (9223372036854775808, 1), (18446744073709551615, 1)",
    ]);
    assert_eq!(
        sorted(&mut tk, "SELECT c1 FROM hu USE INDEX (c2) WHERE c2 = 1 AND c1 > 1"),
        ["18446744073709551615", "9223372036854775807", "9223372036854775808"]
    );
}

#[test]
fn a_handle_that_is_already_a_key_part_is_not_appended_again() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = session(&stack, 742);
    exec_all(&mut tk, &["CREATE TABLE hd (c1 BIGINT PRIMARY KEY, c2 INT, INDEX c2c1 (c2, c1))", "INSERT INTO hd VALUES (1, 1), (2, 1), (3, 1)"]);
    let plan = plan_lines(&mut tk, "SELECT * FROM hd USE INDEX (c2c1) WHERE c2 = 1 AND c1 > 1");
    assert!(plan.iter().any(|line| line.contains("index:c2c1(c2, c1)") && line.contains("range:(1 1,1 +inf]")), "{plan:?}");
    assert_eq!(ids(&mut tk, "SELECT c1 FROM hd USE INDEX (c2c1) WHERE c2 = 1 AND c1 > 1"), [2, 3]);
}

#[test]
fn a_prefix_key_part_takes_the_handle_dimension_behind_it() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = session(&stack, 743);
    exec_all(&mut tk, &[
        "CREATE TABLE hp (c1 BIGINT PRIMARY KEY, s VARCHAR(20), INDEX sp (s(3)))",
        "INSERT INTO hp VALUES (1, 'abcdef'), (2, 'abcdef'), (3, 'abcdef'), (4, 'abcxyz'), (5, 'zzz')",
    ]);
    assert_eq!(ids(&mut tk, "SELECT c1 FROM hp USE INDEX (sp) WHERE s = 'abcdef' AND c1 > 1"), [2, 3]);
    for predicate in ["s = 'abcdef' AND c1 > 1", "s = 'abcdef' AND c1 < 3", "s = 'abcxyz' AND c1 > 1", "s = 'zzz' AND c1 >= 5"] {
        assert_eq!(
            ids(&mut tk, &format!("SELECT c1 FROM hp USE INDEX (sp) WHERE {predicate}")),
            ids(&mut tk, &format!("SELECT c1 FROM hp IGNORE INDEX (sp) WHERE {predicate}")),
            "{predicate}"
        );
    }
}

#[test]
fn a_handle_that_leads_its_index_is_not_appended_behind_it() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = session(&stack, 744);
    exec_all(&mut tk, &["CREATE TABLE hr (c1 BIGINT PRIMARY KEY, c2 INT, INDEX c1c2 (c1, c2))", "INSERT INTO hr VALUES (1, 2), (2, 2), (3, 2)"]);
    assert_eq!(ids(&mut tk, "SELECT c1 FROM hr USE INDEX (c1c2) WHERE c1 = 1 AND c2 = 2"), [1]);
}

/// DESC over the handle walks the range backwards under a Limit, no TopN.
#[test]
fn a_descending_handle_limit_answers_the_largest_ids() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = session(&stack, 745);
    exec_all(&mut tk, &["CREATE TABLE walk (id BIGINT PRIMARY KEY, v INT)", "INSERT INTO walk VALUES (1,10),(2,20),(3,30),(5,50),(100,1)"]);
    let sql = "SELECT id FROM walk WHERE id > 1 ORDER BY id DESC LIMIT 2";
    assert_eq!(q(&mut tk, sql), [["100"], ["5"]]);
    let plan = explain_brief(&mut tk, sql);
    let names = plan.iter().map(|row| bare(&row[0])).collect::<Vec<_>>();
    assert!(names.iter().all(|name| !name.starts_with("TopN")), "{names:?}");
    assert!(names.iter().any(|name| name.starts_with("Limit")), "{names:?}");
    let scan = plan.iter().find(|row| row[0].contains("TableRangeScan") || row[0].contains("TableFullScan")).expect("a table scan");
    assert!(scan[4].contains("keep order:true, desc"), "{plan:#?}");
}

#[test]
fn an_ascending_handle_order_limit_pushes_a_limit_not_a_topn() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = session(&stack, 746);
    exec_all(&mut tk, &["CREATE TABLE walk (id BIGINT PRIMARY KEY, v INT)", "INSERT INTO walk VALUES (1,10),(2,20),(3,30),(5,50),(100,1)"]);
    let sql = "SELECT id FROM walk ORDER BY id LIMIT 2";
    assert_eq!(q(&mut tk, sql), [["1"], ["2"]]);
    let names = explain_brief(&mut tk, sql).iter().map(|row| bare(&row[0])).collect::<Vec<_>>();
    assert!(names.iter().all(|name| !name.starts_with("TopN")), "{names:?}");
    assert!(names.iter().any(|name| name.starts_with("Limit")), "{names:?}");
}

#[test]
fn partial_index_topn_eliminates_identity_projection() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = session(&stack, 747);
    exec_all(&mut tk, &[
        "CREATE TABLE flex_plan (a INT, b INT, c INT, d INT, KEY idx_ab (a, b DESC, d))",
        "INSERT INTO flex_plan VALUES (1,10,3,1),(1,10,2,2),(1,9,1,3),(2,20,1,4)",
    ]);
    let names = explain_brief(&mut tk, "SELECT * FROM flex_plan WHERE a=1 AND b>0 ORDER BY b DESC, c ASC LIMIT 2").iter().map(|row| row[0].clone()).collect::<Vec<_>>();
    assert!(names.first().is_some_and(|name| name.starts_with("TopN")), "{names:?}");
    assert!(names.iter().all(|name| !name.starts_with("Projection")), "{names:?}");
}

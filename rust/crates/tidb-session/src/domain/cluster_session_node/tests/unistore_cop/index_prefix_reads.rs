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


//! Reads through a prefix index (Go `pkg/planner` prefix-column handling,
//! `tidb_opt_partial_ordered_index_for_topn`,
//! `tidb_opt_prefix_index_single_scan`): the whole value comes back, the
//! residual predicate tells shared prefixes apart, a unique prefix rejects a
//! shared prefix and answers no point get, a prefix key part never satisfies
//! ORDER BY unless the partial-order TopN applies, over the cop-backed TestKit
//! stack (X11).

use super::sql_kit::*;
use super::*;

fn prefix_table(tk: &mut Session) {
    exec_all(tk, &["CREATE TABLE t (a VARCHAR(20), b INT, KEY idx (a(3)))", "INSERT INTO t VALUES ('abcdef', 1), ('abcxyz', 2), ('zzz', 3)"]);
}

fn sorted(tk: &mut Session, sql: &str) -> Vec<String> {
    let mut out = q(tk, sql).into_iter().map(|row| row.join(" ")).collect::<Vec<_>>();
    out.sort();
    out
}

#[test]
fn selecting_the_indexed_column_returns_the_whole_value_not_the_prefix() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = session(&stack, 710);
    prefix_table(&mut tk);
    assert_eq!(sorted(&mut tk, "SELECT a FROM t WHERE a = 'abcdef'"), ["abcdef"]);
}

#[test]
fn two_rows_sharing_a_prefix_are_told_apart_by_the_residual_predicate() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = session(&stack, 711);
    prefix_table(&mut tk);
    assert_eq!(sorted(&mut tk, "SELECT a FROM t WHERE a = 'abcdef'"), ["abcdef"]);
    assert_eq!(sorted(&mut tk, "SELECT a FROM t WHERE a = 'abcxyz'"), ["abcxyz"]);
}

#[test]
fn a_query_for_the_prefix_itself_matches_nothing() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = session(&stack, 712);
    prefix_table(&mut tk);
    assert!(sorted(&mut tk, "SELECT a FROM t WHERE a = 'abc'").is_empty());
}

#[test]
fn inequalities_over_a_cut_endpoint_still_read_every_qualifying_row() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = session(&stack, 713);
    prefix_table(&mut tk);
    assert_eq!(sorted(&mut tk, "SELECT a FROM t WHERE a > 'abc'"), ["abcdef", "abcxyz", "zzz"]);
    assert_eq!(sorted(&mut tk, "SELECT a FROM t WHERE a >= 'abcdef'"), ["abcdef", "abcxyz", "zzz"]);
    assert!(sorted(&mut tk, "SELECT a FROM t WHERE a < 'abcdef'").is_empty());
    assert_eq!(sorted(&mut tk, "SELECT a FROM t WHERE a IN ('abcdef', 'zzz')"), ["abcdef", "zzz"]);
}

#[test]
fn a_unique_prefix_index_rejects_a_row_that_only_shares_the_prefix() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = session(&stack, 714);
    exec_all(&mut tk, &["CREATE TABLE u (a VARCHAR(20), UNIQUE KEY uidx (a(3)))", "INSERT INTO u VALUES ('abcdef')"]);
    let refused = error(&mut tk, "INSERT INTO u VALUES ('abcxyz')");
    assert!(refused.code == 1062 && refused.message.starts_with("Duplicate entry 'abc' "), "{}", refused.message);
    rows(&mut tk, "INSERT INTO u VALUES ('abdefg')");
    assert_eq!(sorted(&mut tk, "SELECT a FROM u"), ["abcdef", "abdefg"]);
}

#[test]
fn a_unique_prefix_index_does_not_answer_a_point_get() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = session(&stack, 715);
    exec_all(&mut tk, &["CREATE TABLE u (a VARCHAR(20), b INT, UNIQUE KEY uidx (a(3)))", "INSERT INTO u VALUES ('abcdef', 1), ('zzz', 2)"]);
    assert!(sorted(&mut tk, "SELECT a FROM u WHERE a = 'abcxyz'").is_empty());
    assert_eq!(sorted(&mut tk, "SELECT a FROM u WHERE a = 'abcdef'"), ["abcdef"]);
    assert_eq!(sorted(&mut tk, "SELECT a FROM u WHERE a IN ('abcxyz', 'zzz')"), ["zzz"]);
}

#[test]
fn an_order_by_is_not_satisfied_by_a_prefix_key_part() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = session(&stack, 716);
    exec_all(&mut tk, &["CREATE TABLE o (a VARCHAR(20), KEY idx (a(3)))", "INSERT INTO o VALUES ('abczz'), ('abcaa'), ('aaa')"]);
    assert_eq!(q(&mut tk, "SELECT a FROM o WHERE a > 'a' ORDER BY a LIMIT 2"), [["aaa"], ["abcaa"]]);
}

#[test]
fn a_leading_whole_key_part_still_orders_by_itself() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = session(&stack, 717);
    exec_all(&mut tk, &["CREATE TABLE c (a INT, b VARCHAR(20), KEY idx (a, b(3)))", "INSERT INTO c VALUES (3, 'zzz'), (1, 'abczz'), (2, 'abcaa')"]);
    assert_eq!(q(&mut tk, "SELECT a FROM c WHERE a > 0 ORDER BY a LIMIT 2"), [["1"], ["2"]]);
    assert_eq!(q(&mut tk, "SELECT b FROM c WHERE a > 0 ORDER BY a, b LIMIT 2"), [["abczz"], ["abcaa"]]);
}

/// Go's partial-order TopN over a prefix index keeps every row sharing the
/// boundary prefix; NO_ORDER_INDEX or DISABLE fall back to a full sort.
#[test]
fn partial_order_topn_keeps_competing_rows_with_the_same_prefix() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = session(&stack, 718);
    exec_all(&mut tk, &["CREATE TABLE p (a VARCHAR(20), KEY idx(a(3)))", "INSERT INTO p VALUES ('abczz'), ('abcaa'), ('abcmm'), ('aaa'), ('zzz')"]);
    for (order, prefix_expected, sorted_expected) in [
        ("ASC", ["abcaa", "abczz"], ["abcaa", "abcmm"]),
        ("DESC", ["abcmm", "abcaa"], ["abczz", "abcmm"]),
    ] {
        for (hint, clause, enabled, prefix) in [
            ("/*+ ORDER_INDEX(p, idx) */", "", true, true),
            ("", "USE INDEX(idx)", true, true),
            ("", "FORCE INDEX(idx)", true, true),
            ("/*+ USE_INDEX(p, idx) */", "", true, true),
            ("/*+ NO_ORDER_INDEX(p, idx) */", "USE INDEX(idx)", true, false),
            ("", "USE INDEX(idx)", false, false),
        ] {
            rows(&mut tk, &format!("SET tidb_opt_partial_ordered_index_for_topn = '{}'", if enabled { "COST" } else { "DISABLE" }));
            let sql = format!("SELECT {hint} a FROM p {clause} ORDER BY a {order} LIMIT 1, 2");
            let got = q(&mut tk, &sql).into_iter().map(|row| row[0].clone()).collect::<Vec<_>>();
            assert_eq!(got, if prefix { prefix_expected } else { sorted_expected }, "{sql}");
            let text = plan_lines(&mut tk, &sql).join(" ");
            assert_eq!(text.contains("prefix_col:"), prefix, "{sql}: {text}");
        }
    }
}

/// The partial-order prefix limit over NULLs, case-insensitive and multibyte
/// collations, composite keys, and filters applied after the index.
#[test]
fn partial_order_prefix_limit_handles_null_collation_composite_keys_and_filters() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = session(&stack, 719);
    rows(&mut tk, "SET tidb_opt_partial_ordered_index_for_topn = 'COST'");
    for (table, ddl, insert, sql, expected) in [
        ("pn", "a VARCHAR(20), KEY idx(a(2))", "(NULL), (NULL), ('zz')", "SELECT /*+ ORDER_INDEX(pn, idx) */ a FROM pn ORDER BY a LIMIT 1, 1", "NULL"),
        ("pc", "a VARCHAR(20) COLLATE utf8mb4_general_ci, KEY idx(a(2))", "('abz'), ('ABa'), ('zz')", "SELECT /*+ ORDER_INDEX(pc, idx) */ a FROM pc ORDER BY a LIMIT 1", "abz"),
        ("pu", "a VARCHAR(20) COLLATE utf8mb4_bin, KEY idx(a(2))", "('猫咪z'), ('猫咪a'), ('狗狗b')", "SELECT /*+ ORDER_INDEX(pu, idx) */ a FROM pu ORDER BY a LIMIT 1, 1", "猫咪z"),
        ("pm", "id INT, a VARCHAR(20), KEY idx(id, a(2))", "(1, 'abz'), (1, 'aba'), (2, 'zz')", "SELECT /*+ ORDER_INDEX(pm, idx) */ a FROM pm ORDER BY id, a LIMIT 1", "abz"),
        ("pi", "id INT PRIMARY KEY, a VARCHAR(20), KEY idx(a(2))", "(1, 'abz'), (2, 'abb'), (3, 'aba'), (4, 'zz')", "SELECT /*+ ORDER_INDEX(pi, idx) */ a FROM pi WHERE id > 1 ORDER BY a LIMIT 1", "abb"),
        ("pg", "k VARCHAR(20) COLLATE utf8mb4_general_ci PRIMARY KEY CLUSTERED, a VARCHAR(20), KEY idx(a(2))", "('KeyA', 'abz'), ('KeyB', 'abb'), ('KeyC', 'aba'), ('KeyD', 'zz')", "SELECT /*+ ORDER_INDEX(pg, idx) */ a FROM pg WHERE k > 'KeyA' ORDER BY a LIMIT 1", "abb"),
        ("pd", "a VARCHAR(20) PRIMARY KEY CLUSTERED, b VARCHAR(20), KEY idx(a(2))", "('abz', '1'), ('abb', '2'), ('aba', '3'), ('zz', '4')", "SELECT b FROM pd USE INDEX(idx) WHERE a = 'aba' LIMIT 1", "3"),
        ("pt", "a VARCHAR(20), b INT, KEY idx(a(2))", "('abz', 0), ('abb', 1), ('aba', 1), ('zz', 1)", "SELECT /*+ ORDER_INDEX(pt, idx) */ a FROM pt WHERE b > 0 ORDER BY a LIMIT 1", "aba"),
    ] {
        exec_all(&mut tk, &[&format!("CREATE TABLE {table} ({ddl})"), &format!("INSERT INTO {table} VALUES {insert}")]);
        assert_eq!(q(&mut tk, sql), [[expected]], "{sql}");
    }
    assert_eq!(q(&mut tk, "SELECT a FROM pi USE INDEX(idx) WHERE id > 1 LIMIT 1"), [["abb"]]);
    assert_eq!(q(&mut tk, "SELECT a FROM pi USE INDEX(idx) WHERE id > 1 LIMIT 1, 1"), [["aba"]]);
}

/// With `tidb_opt_prefix_index_single_scan` a NULL-only condition over a
/// prefix column is answered by a covering IndexReader; one that needs the
/// value reads the rows.
#[test]
fn prefix_null_conditions_use_a_covering_reader_without_exposing_prefix_values() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = session(&stack, 720);
    exec_all(&mut tk, &["CREATE TABLE pn (a VARCHAR(20), b INT, KEY idx(a(2), b))", "INSERT INTO pn VALUES (NULL, 1), ('abz', 2), ('aba', 3), ('zz', 4)"]);
    for enabled in [true, false] {
        rows(&mut tk, &format!("SET tidb_opt_prefix_index_single_scan = {}", if enabled { "ON" } else { "OFF" }));
        for (projection, condition, expected, null_only) in [
            ("b", "a IS NULL", &["1"][..], true),
            ("b", "a IS NOT NULL", &["2", "3", "4"][..], true),
            ("b", "(a IS NULL) = 0", &["2", "3", "4"][..], true),
            ("b", "a IS NULL OR b = 3", &["1", "3"][..], true),
            ("b", "a IS NULL AND b > 0", &["1"][..], true),
            ("b", "CHAR_LENGTH(a) IS NULL", &["1"][..], false),
            ("b", "a IS NULL OR a = 'aba'", &["1", "3"][..], false),
            ("a", "a IS NOT NULL", &["aba", "abz", "zz"][..], false),
        ] {
            let sql = format!("SELECT {projection} FROM pn USE INDEX(idx) WHERE {condition}");
            assert_eq!(sorted(&mut tk, &sql), expected, "{sql}, enabled={enabled}");
            let text = plan_lines(&mut tk, &sql).join(" ");
            assert_eq!(text.contains("IndexReader"), enabled && null_only, "{sql}, enabled={enabled}: {text}");
        }
    }
}

/// An index join's covering inner reader matches Go master with prefix
/// filters and outer NULLs, committed and with dirty transaction rows.
#[test]
fn index_join_covering_rows_match_master_with_prefix_filters_and_outer_nulls() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = session(&stack, 721);
    exec_all(&mut tk, &[
        "CREATE TABLE o (k INT)",
        "CREATE TABLE i (k INT, a VARCHAR(20), b INT, KEY idx(k, a(2), b))",
        "INSERT INTO o VALUES(1),(3),(NULL),(3),(9)",
        "INSERT INTO i VALUES(1,NULL,10),(1,'abz',11),(3,NULL,30),(3,'aba',31),(3,'abb',32)",
    ]);
    let cases = [
        ("SELECT /*+ INL_JOIN(i) */ o.k,i.b FROM o LEFT JOIN i USE INDEX(idx) ON o.k=i.k WHERE i.a IS NULL", &["1 10", "3 30", "3 30", "9 NULL", "NULL NULL"][..], false),
        ("SELECT /*+ INL_JOIN(i) */ o.k,i.b FROM o JOIN i USE INDEX(idx) ON o.k=i.k WHERE i.a IS NOT NULL", &["1 11", "3 31", "3 31", "3 32", "3 32"][..], false),
        ("SELECT /*+ INL_JOIN(i) */ o.k,i.a FROM o JOIN i USE INDEX(idx) ON o.k=i.k WHERE i.a IS NOT NULL", &["1 abz", "3 aba", "3 aba", "3 abb", "3 abb"][..], false),
        ("SELECT /*+ INL_JOIN(i) */ o.k,i.b FROM o JOIN i USE INDEX(idx) ON o.k=i.k WHERE i.b>10", &["1 11", "3 30", "3 30", "3 31", "3 31", "3 32", "3 32"][..], true),
    ];
    for dirty in [false, true] {
        if dirty {
            exec_all(&mut tk, &["BEGIN", "INSERT INTO i VALUES (200, 'q', 1)"]);
        }
        for (sql, expected, covering) in cases {
            assert_eq!(sorted(&mut tk, sql), expected, "{sql} dirty={dirty}");
            let text = plan_lines(&mut tk, sql).join(" ");
            assert_eq!(text.contains("IndexReader"), covering, "{sql}: {text}");
        }
    }
    rows(&mut tk, "ROLLBACK");
}

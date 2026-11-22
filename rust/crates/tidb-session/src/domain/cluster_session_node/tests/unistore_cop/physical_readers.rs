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


//! Physical readers through Go's planner, as the executor's in-memory tier
//! used to check them (X11): index-join compare filters and prefix lookup keys,
//! a prepared index join rebinding its parameter, a fresh and a cached
//! execution answering alike, a dirty table's union scan, and the taobench
//! edge reads over a non-clustered composite primary key.

use super::sql_kit::*;
use super::*;

#[test]
fn index_join_compare_filters_intersect_on_go_one_column_range() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = session(&stack, 870);
    exec_all(&mut tk, &[
        "CREATE TABLE ij_outer (a BIGINT, lo1 BIGINT, lo2 BIGINT)",
        "CREATE TABLE ij_inner (a BIGINT, b BIGINT, KEY ab(a,b))",
        "INSERT INTO ij_outer VALUES (1,10,12)",
        "INSERT INTO ij_inner VALUES (1,9),(1,10),(1,11),(1,12),(1,13),(2,99)",
    ]);
    let sql = "SELECT /*+ TIDB_INLJ(i) */ i.b FROM ij_outer o JOIN ij_inner i ON i.a=o.a AND i.b>o.lo1 AND i.b>=o.lo2 ORDER BY i.b";
    assert!(plan_lines(&mut tk, sql).iter().any(|line| line.contains("IndexJoin")));
    assert_eq!(q(&mut tk, sql), [["12"], ["13"]]);
}

#[test]
fn index_join_cuts_equality_probe_to_go_prefix_index_key() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = session(&stack, 871);
    exec_all(&mut tk, &[
        "CREATE TABLE pij_outer (s VARCHAR(20))",
        "CREATE TABLE pij_inner (s VARCHAR(20), KEY s3(s(3)))",
        "INSERT INTO pij_outer VALUES ('abcdef')",
        "INSERT INTO pij_inner VALUES ('abcdef'),('abcxyz'),('zzz')",
    ]);
    assert_eq!(q(&mut tk, "SELECT /*+ TIDB_INLJ(i) */ i.s FROM pij_outer o JOIN pij_inner i ON i.s=o.s"), [["abcdef"]]);
}

/// Go cuts `dLookUpKey` before `sortAndDedupLookUpContents`, so one prefix
/// range is read once and each row still meets its own outer row.
#[test]
fn index_join_deduplicates_go_prefix_lookup_keys_before_reading() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = session(&stack, 872);
    exec_all(&mut tk, &[
        "CREATE TABLE pdij_outer (s VARCHAR(20))",
        "CREATE TABLE pdij_inner (s VARCHAR(20), KEY s3(s(3)))",
        "INSERT INTO pdij_outer VALUES ('abcdef'),('abcxyz')",
        "INSERT INTO pdij_inner VALUES ('abcdef'),('abcxyz')",
    ]);
    assert_eq!(q(&mut tk, "SELECT /*+ TIDB_INLJ(i) */ i.s FROM pdij_outer o JOIN pdij_inner i ON i.s=o.s ORDER BY i.s"), [["abcdef"], ["abcxyz"]]);
}

#[test]
fn cached_index_join_compare_filter_rebinds_its_parameter() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = session(&stack, 873);
    exec_all(&mut tk, &[
        "CREATE TABLE cij_outer (a BIGINT, lo BIGINT)",
        "CREATE TABLE cij_inner (a BIGINT, b BIGINT, KEY ab(a,b))",
        "INSERT INTO cij_outer VALUES (1,10)",
        "INSERT INTO cij_inner VALUES (1,10),(1,11),(1,12),(1,13)",
        "PREPARE st FROM 'SELECT /*+ TIDB_INLJ(i) */ i.b FROM cij_outer o JOIN cij_inner i ON i.a=o.a AND i.b>o.lo+? ORDER BY i.b'",
        "SET @p = 0",
    ]);
    assert_eq!(q(&mut tk, "EXECUTE st USING @p"), [["11"], ["12"], ["13"]]);
    rows(&mut tk, "SET @p = 2");
    assert_eq!(q(&mut tk, "EXECUTE st USING @p"), [["13"]]);
    assert_eq!(q(&mut tk, "SELECT @@last_plan_from_cache"), [["1"]]);
}

#[test]
fn fresh_and_cached_physical_execution_returns_the_same_rows_and_metadata() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = session(&stack, 874);
    exec_all(&mut tk, &[
        "CREATE TABLE shared_physical_builder (id BIGINT PRIMARY KEY, value BIGINT NOT NULL)",
        "INSERT INTO shared_physical_builder VALUES (1,10),(2,20),(3,30)",
        "PREPARE st FROM 'SELECT value FROM shared_physical_builder WHERE id BETWEEN ? AND ? ORDER BY id'",
        "SET @a = 1, @b = 2",
    ]);
    let ordinary_sql = "SELECT value FROM shared_physical_builder WHERE id BETWEEN 1 AND 2 ORDER BY id";
    assert_eq!(q(&mut tk, "SELECT COUNT(*) FROM shared_physical_builder"), [["3"]]);
    let ordinary = (field_names(&mut tk, ordinary_sql), q(&mut tk, ordinary_sql));
    for _ in 0..2 {
        assert_eq!((field_names(&mut tk, "EXECUTE st USING @a, @b"), q(&mut tk, "EXECUTE st USING @a, @b")), ordinary);
    }
}

/// A dirty table's reads merge the transaction's rows through a union scan,
/// keeping index order and conditions.
#[test]
fn dirty_table_union_scan_plan_executes_conditions_and_index_order() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = session(&stack, 875);
    exec_all(&mut tk, &[
        "CREATE TABLE union_scan_build (id BIGINT PRIMARY KEY, value BIGINT, INDEX value_idx(value))",
        "BEGIN",
        "INSERT INTO union_scan_build VALUES (3,30),(1,10),(2,20)",
    ]);
    let sql = "SELECT id, value FROM union_scan_build USE INDEX(value_idx) WHERE value >= 20 ORDER BY value";
    assert!(plan_lines(&mut tk, sql).iter().any(|line| line.contains("UnionScan")));
    assert_eq!(q(&mut tk, sql), [["2", "20"], ["3", "30"]]);
    assert_eq!(q(&mut tk, "SELECT value FROM union_scan_build WHERE id = 2"), [["20"]]);
    rows(&mut tk, "ROLLBACK");
}

fn edges(tk: &mut Session) {
    exec_all(tk, &[
        "CREATE TABLE t (id1 BIGINT NOT NULL, id2 BIGINT NOT NULL, type VARCHAR(63) NOT NULL COLLATE utf8mb4_bin, ts BIGINT NOT NULL, value VARCHAR(150), PRIMARY KEY (id1, id2, type) NONCLUSTERED)",
        "INSERT INTO t VALUES (848250056732, 1947761684552, '3', 1660627540589311589, 'hello')",
    ]);
}

/// The string '3' coerces to the integer 3 under MySQL comparison rules.
#[test]
fn edge_point_read_with_int_type_literal_returns_the_row() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = session(&stack, 876);
    edges(&mut tk);
    assert_eq!(q(&mut tk, "SELECT ts FROM t WHERE id1 = 848250056732 AND id2 = 1947761684552 AND type = 3"), [["1660627540589311589"]]);
    assert_eq!(q(&mut tk, "SELECT ts FROM t WHERE id1 = 848250056732 AND id2 = 1947761684552"), [["1660627540589311589"]]);
}

#[test]
fn bare_count_star_over_covering_primary_answers() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = session(&stack, 877);
    edges(&mut tk);
    assert_eq!(q(&mut tk, "SELECT COUNT(*) FROM t USE INDEX (PRIMARY) LIMIT 1"), [["1"]]);
    assert_eq!(q(&mut tk, "SELECT COUNT(*) FROM t USE INDEX (PRIMARY)"), [["1"]]);
    assert_eq!(q(&mut tk, "SELECT 1 FROM t LIMIT 1"), [["1"]]);
}

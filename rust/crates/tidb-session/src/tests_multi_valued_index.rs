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

//! Multi-valued indexes (`CAST(... AS ... ARRAY)` key parts): Go's DDL
//! admission, `castJSONAsArrayFunctionSig`, and the write path's
//! `index.getIndexedValue` expansion. Every expected answer is a Go oracle
//! run on this branch, recorded in `rust/docs/multi-valued-index-execplan.md`.

use crate::tests_support::row_text;
use crate::Session;

fn error_text(session: &mut Session, sql: &str) -> String {
    session
        .run(sql)
        .expect_err("the statement must fail")
        .to_mysql_error()
        .message
}

/// The stored entry count of one index of `test.<table>`.
fn index_entry_count(session: &mut Session, table: &str, index: &str) -> usize {
    session
        .with_catalog_mut(|catalog| {
            let Some(tidb_executor::TableEntry::Kv(stored)) = catalog.table_mut_in("test", table)
            else {
                panic!("{table} is not stored as bytes");
            };
            let stored = std::sync::Arc::make_mut(stored);
            let index = stored
                .index_list_for_check()
                .into_iter()
                .find(|candidate| candidate.name.eq_ignore_ascii_case(index))
                .expect("the index exists");
            Ok(stored
                .index_entries_for_check(index.id)
                .expect("entries readable")
                .len())
        })
        .unwrap()
}

/// `[1,2,2]` files two keys, NULL one, `[]` none and the scalar `7` one (Go
/// casts it to `[7]`): Go `getIndexedValue`'s four cases. Before the port
/// the DDL itself was refused, and every later statement on the table failed
/// with "doesn't exist".
#[test]
fn a_multi_valued_index_files_one_key_per_distinct_element() {
    let mut session = Session::new();
    session
        .run("create table t(a int, j json, index kj((cast(j as signed array))))")
        .unwrap();
    assert_eq!(
        row_text(session.run("show create table t"))[0][1],
        "CREATE TABLE `t` (\n  `a` int DEFAULT NULL,\n  `j` json DEFAULT NULL,\n  \
         KEY `kj` ((cast(`j` as signed array)))\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 \
         COLLATE=utf8mb4_bin"
    );
    session
        .run("insert into t values (1, '[1,2,2]'), (2, null), (3, '[]'), (4, '[3]'), (5, '7')")
        .unwrap();
    assert_eq!(index_entry_count(&mut session, "t", "kj"), 2 + 1 + 0 + 1 + 1);
    assert_eq!(
        row_text(session.run("select a from t where 2 member of (j)")),
        vec![vec!["1"]]
    );
    assert_eq!(
        row_text(session.run("select a from t where json_overlaps(j, '[1,3]') order by a")),
        vec![vec!["1"], vec!["4"]]
    );
    session.run("admin check table t").unwrap();

    // An UPDATE removes every old key and files every new one; a DELETE
    // removes them all.
    session.run("update t set j = '[1,9,1]' where a = 1").unwrap();
    assert_eq!(index_entry_count(&mut session, "t", "kj"), 2 + 1 + 0 + 1 + 1);
    session.run("delete from t where a = 4").unwrap();
    assert_eq!(index_entry_count(&mut session, "t", "kj"), 2 + 1 + 0 + 1);
    session.run("admin check table t").unwrap();

    // ADD INDEX backfills the same expansion.
    session
        .run("alter table t add index k2((cast(j as unsigned array)))")
        .unwrap();
    assert_eq!(index_entry_count(&mut session, "t", "k2"), 2 + 1 + 0 + 1);
    session.run("admin check table t").unwrap();
}

/// A unique multi-valued index rejects a shared ELEMENT, and Go's message
/// carries the whole array (`addIndices` formats the row's values before
/// expansion).
#[test]
fn a_unique_multi_valued_index_rejects_a_shared_element() {
    let mut session = Session::new();
    session
        .run("create table t4(j json, unique index k((cast(j->'$.a' as char(10) array))))")
        .unwrap();
    assert_eq!(
        error_text(
            &mut session,
            r#"insert into t4 values ('{"a":["x","y"]}'), ('{"a":["y"]}')"#
        ),
        r#"Duplicate entry '["y"]' for key 't4.k'"#
    );
    session
        .run(r#"insert into t4 values ('{"a":["x","y"]}'), ('{"a":["z"]}')"#)
        .unwrap();
    assert_eq!(index_entry_count(&mut session, "t4", "k"), 3);
}

/// Go `completeError`: a failed element conversion during INSERT names the
/// index (3903, 3907); the object refusal and the cast outside an index are
/// Go's 1235 texts.
#[test]
fn an_unconvertible_element_names_its_index() {
    let mut session = Session::new();
    session
        .run("create table t(a int, j json, index kj((cast(j as signed array))))")
        .unwrap();
    assert_eq!(
        error_text(&mut session, r#"insert into t values (6, '["x"]')"#),
        "Invalid JSON value for CAST for expression index 'kj'"
    );
    assert_eq!(
        error_text(&mut session, r#"insert into t values (6, '{"a":1}')"#),
        "This version of TiDB doesn't yet support 'CAST-ing JSON OBJECT type to array'"
    );
    assert_eq!(
        error_text(&mut session, "select cast('[1,2]' as signed array)"),
        "This version of TiDB doesn't yet support 'Use of CAST( .. AS .. ARRAY) outside of \
         functional index in CREATE(non-SELECT)/ALTER TABLE or in general expressions'"
    );
    session
        .run("create table t4(j json, index k((cast(j as char(10) array))))")
        .unwrap();
    assert_eq!(
        error_text(&mut session, r#"insert into t4 values ('["abcdefghijklmn"]')"#),
        "Data too long for expression index 'k'"
    );
}

/// Go `buildIndexColumns`, `CheckPKOnGeneratedColumn` and
/// `castAsArrayFunctionClass` refusals.
#[test]
fn multi_valued_key_parts_follow_go_admission_rules() {
    let mut session = Session::new();
    assert_eq!(
        error_text(
            &mut session,
            "create table t2(j json, index k((cast(j as signed array)), \
             (cast(j as unsigned array))))"
        ),
        "This version of TiDB doesn't yet support 'more than one multi-valued key part per index'"
    );
    assert_eq!(
        error_text(
            &mut session,
            "create table t3(j json, primary key((cast(j as signed array))))"
        ),
        "The primary key cannot be an expression index"
    );
    assert_eq!(
        error_text(
            &mut session,
            "create table t6(j json, index k((cast(j as json array))))"
        ),
        "This version of TiDB doesn't yet support 'CAST-ing data to array of json BINARY'"
    );
    assert_eq!(
        error_text(
            &mut session,
            "create table t7(j json, k json as (cast(j as signed array)))"
        ),
        "This version of TiDB doesn't yet support 'Use of CAST( .. AS .. ARRAY) outside of \
         functional index in CREATE(non-SELECT)/ALTER TABLE or in general expressions'"
    );
}

/// The operator-and-access-object column of each `EXPLAIN FORMAT='brief'`
/// row, with estimates: what `globalindex/multi_valued_index.result` records.
fn brief_plan(session: &mut Session, sql: &str) -> Vec<String> {
    row_text(session.run(sql))
        .into_iter()
        .map(|row| format!("{} {} {}", row[0], row[1], row[3]))
        .collect()
}

/// Go `generateANDIndexMerge4MVIndex` and `generateANDIndexMerge4ComposedIndex`:
/// `member of` reads one MV partial (a union), `json_overlaps` a union of one
/// partial per value with the filter kept above it, and a hinted `id > 0`
/// joins an intersection. Before the port every one read the whole table.
/// Plans and estimates are the recorded TiDB output.
#[test]
fn multi_valued_predicates_plan_index_merges_as_go_does() {
    let mut session = Session::new();
    session
        .run(
            "CREATE TABLE customers (id bigint(20), name char(10) DEFAULT NULL, \
             custinfo json DEFAULT NULL, KEY idx(id), UNIQUE KEY zips \
             ((cast(json_extract(custinfo, _utf8'$.zipcode') as unsigned array))) GLOBAL) \
             PARTITION BY HASH (id) PARTITIONS 5",
        )
        .unwrap();
    for row in [
        r#"(1, 'pingcap', '{"zipcode": [1,2]}')"#,
        r#"(2, 'pingcap', '{"zipcode": [3,3,4]}')"#,
        r#"(3, 'pingcap', '{"zipcode": [5,6]}')"#,
    ] {
        session.run(&format!("INSERT INTO customers VALUES {row}")).unwrap();
    }
    let zips = "table:customers, index:zips(cast(json_extract(`custinfo`, _utf8'$.zipcode') as unsigned array))";
    assert_eq!(
        brief_plan(
            &mut session,
            "explain format='brief' select * from customers where (1 member of (custinfo->'$.zipcode'))"
        ),
        vec![
            "IndexMerge 1.00 partition:all".to_owned(),
            format!("├─IndexRangeScan(Build) 1.00 {zips}"),
            "└─TableRowIDScan(Probe) 1.00 table:customers".to_owned(),
        ]
    );
    assert_eq!(
        row_text(session.run("select id from customers where (1 member of (custinfo->'$.zipcode'))")),
        vec![vec!["1"]]
    );
    assert_eq!(
        brief_plan(
            &mut session,
            "explain format='brief' select * from customers \
             where json_overlaps(\"[1, 6, 10]\", custinfo->'$.zipcode') and id > 1"
        ),
        vec![
            "Selection 2.40 ".to_owned(),
            "└─IndexMerge 1.00 partition:all".to_owned(),
            format!("  ├─IndexRangeScan(Build) 1.00 {zips}"),
            format!("  ├─IndexRangeScan(Build) 1.00 {zips}"),
            format!("  ├─IndexRangeScan(Build) 1.00 {zips}"),
            "  └─Selection(Probe) 1.00 ".to_owned(),
            "    └─TableRowIDScan 3.00 table:customers".to_owned(),
        ]
    );
    assert_eq!(
        row_text(session.run(
            "select id from customers where json_overlaps(\"[1, 3, 7, 10]\", custinfo->'$.zipcode') order by id"
        )),
        vec![vec!["1"], vec!["2"]]
    );
    assert_eq!(
        brief_plan(
            &mut session,
            "explain format='brief' select /*+ USE_INDEX_MERGE(customers, idx, zips) */ * \
             from customers where (1 member of (custinfo->'$.zipcode')) and id > 0"
        ),
        vec![
            "IndexMerge 0.33 partition:all".to_owned(),
            "├─IndexRangeScan(Build) 3333.33 table:customers, index:idx(id)".to_owned(),
            format!("├─IndexRangeScan(Build) 1.00 {zips}"),
            "└─TableRowIDScan(Probe) 0.33 table:customers".to_owned(),
        ]
    );
}

/// `json_contains` intersects one partial per value (Go
/// `multiValuesANDOnMVColTp`); the string argument is parsed as a JSON
/// document because a string-sourced JSON cast is ParseToJSON
/// (`castAsJSONFunctionClass`).
#[test]
fn json_contains_intersects_one_partial_per_value() {
    let mut session = Session::new();
    session
        .run("create table t(a int, j json, index kj((cast(j as signed array))))")
        .unwrap();
    session
        .run("insert into t values (1,'[1,2,2]'),(2,null),(3,'[]'),(4,'[3]'),(5,'7')")
        .unwrap();
    let plan = brief_plan(
        &mut session,
        "explain format='brief' select /*+ use_index_merge(t, kj) */ a from t \
         where json_contains(j, '[1,2]')",
    );
    assert!(plan[1].starts_with("└─IndexMerge"), "{plan:?}");
    assert_eq!(
        row_text(session.run(
            "explain format='brief' select /*+ use_index_merge(t, kj) */ a from t \
             where json_contains(j, '[1,2]')"
        ))[1][4],
        "type: intersection"
    );
    assert_eq!(plan.len(), 5, "{plan:?}");
    assert_eq!(
        row_text(session.run(
            "select /*+ use_index_merge(t, kj) */ a from t where json_contains(j, '[1,2]')"
        )),
        vec![vec!["1"]]
    );
}

/// An ARRAY result keeps its element's charset: `CHAR(2) ARRAY` counts
/// characters (Go keeps the derived collation out of `tp`), and a
/// `JSON_ARRAY` of temporal values holds JSON DATE/TIME values a DATE/TIME
/// array accepts. All four are accepted by TiDB
/// (`expression/multi_valued_index.result`).
#[test]
fn array_elements_keep_their_type() {
    let mut session = Session::new();
    session
        .run("create table t(a json, index idx((cast(a as char(2) array))))")
        .unwrap();
    session.run(r#"insert into t values ('["汉字"]')"#).unwrap();
    for (element, value) in [
        ("date", r#"cast("2022-02-02" as date)"#),
        ("time", r#"cast("11:00:00" as time)"#),
        ("datetime", r#"cast("2022-02-02 11:00:00" as datetime)"#),
    ] {
        session.run("drop table t").unwrap();
        session
            .run(&format!(
                "create table t(a json, index idx((cast(a as {element} array))))"
            ))
            .unwrap();
        session
            .run(&format!("insert into t values (json_array({value}))"))
            .unwrap();
        session.run("admin check table t").unwrap();
    }
}

/// The access objects and ranges of each `EXPLAIN FORMAT='plan_tree'` row:
/// what `planner/core/indexmerge_path.result` records.
fn plan_tree(session: &mut Session, sql: &str) -> Vec<String> {
    row_text(session.run(&format!("explain format='plan_tree' {sql}")))
        .into_iter()
        .map(|row| format!("{} {} {}", row[0].trim(), row[2], row[3]))
        .collect()
}

/// Go `generateORIndexMerge` over an MV index (`initUnfinishedPathsFromExpr`
/// case 2/3, `buildIntoAccessPath`'s MV arm): each OR branch is an
/// alternative of one partial per value, prefix columns from the branch join
/// the range, and an unpushable OR (`json_overlaps`) still builds the merge
/// because Go reads AllConds. `json_contains` alternatives intersect and so
/// cannot serve a union. Recorded TiDB plans.
#[test]
fn or_lists_over_a_multi_valued_index_unite_value_partials() {
    let mut session = Session::new();
    session
        .run(
            "create table t(a int, b int, c int, j json, \
             index idx1((cast(j as signed array))), \
             index idx2(a, b, (cast(j as signed array)), c))",
        )
        .unwrap();
    session
        .run("insert into t values (1, 2, 3, '[1,2,3]'), (11, 12, 13, '[13]'), (20, 21, 22, '[5]')")
        .unwrap();
    let idx1 = "table:t, index:idx1(cast(`j` as signed array))";
    assert_eq!(
        plan_tree(
            &mut session,
            "select /*+ use_index_merge(t, idx1) */ * from t where (1 member of (j)) or (2 member of (j))"
        ),
        vec![
            "IndexMerge  type: union".to_owned(),
            format!("├─IndexRangeScan(Build) {idx1} range:[1,1], keep order:false, stats:pseudo"),
            format!("├─IndexRangeScan(Build) {idx1} range:[2,2], keep order:false, stats:pseudo"),
            "└─TableRowIDScan(Probe) table:t keep order:false, stats:pseudo".to_owned(),
        ]
    );
    assert_eq!(
        row_text(session.run(
            "select /*+ use_index_merge(t, idx1) */ a from t where (1 member of (j)) or (5 member of (j)) order by a"
        )),
        vec![vec!["1"], vec!["20"]]
    );
    let overlaps = plan_tree(
        &mut session,
        "select /*+ use_index_merge(t, idx1) */ * from t \
         where (json_overlaps(j, '[1, 2]')) or (json_overlaps(j, '[3, 4]'))",
    );
    assert!(overlaps[0].starts_with("Selection"), "{overlaps:?}");
    assert_eq!(overlaps[1], "└─IndexMerge  type: union", "{overlaps:?}");
    assert_eq!(overlaps.len(), 7, "{overlaps:?}");
    let contains = plan_tree(
        &mut session,
        "select /*+ use_index_merge(t, idx1) */ * from t \
         where (json_contains(j, '[1, 2]')) or (json_contains(j, '[3, 4]'))",
    );
    assert!(contains[0].starts_with("TableReader"), "{contains:?}");
    let idx2 = "table:t, index:idx2(a, b, cast(`j` as signed array), c)";
    assert_eq!(
        plan_tree(
            &mut session,
            "select /*+ use_index_merge(t, idx2) */ * from t \
             where (a=1 and b=2 and (3 member of (j))) or (a=11 and b=12 and (13 member of (j)))"
        ),
        vec![
            "IndexMerge  type: union".to_owned(),
            format!("├─IndexRangeScan(Build) {idx2} range:[1 2 3,1 2 3], keep order:false, stats:pseudo"),
            format!("├─IndexRangeScan(Build) {idx2} range:[11 12 13,11 12 13], keep order:false, stats:pseudo"),
            "└─TableRowIDScan(Probe) table:t keep order:false, stats:pseudo".to_owned(),
        ]
    );
    assert_eq!(
        row_text(session.run(
            "select /*+ use_index_merge(t, idx2) */ a from t \
             where (a=1 and b=2 and (3 member of (j))) or (a=11 and b=12 and (13 member of (j))) order by a"
        )),
        vec![vec!["1"], vec!["11"]]
    );
}

/// Go `convertToIndexScan`: an MV index is never an ordinary index read,
/// even through its non-array prefix -- it would return a row once per array
/// element and miss every empty array. A forced MV index falls back to the
/// table path (`getPossibleAccessPaths` keeps it when every path is
/// undetermined). TiDB records TableFullScan for each.
#[test]
fn a_multi_valued_index_is_never_read_as_an_ordinary_index() {
    let mut session = Session::new();
    session
        .run("create table t(a int, b int, c int, j json, index idx(a, b, (cast(j as signed array)), c))")
        .unwrap();
    session
        .run("insert into t values (1, 2, 3, '[1,2,3]'), (1, 5, 6, '[]')")
        .unwrap();
    for sql in [
        "select /*+ use_index_merge(t, idx) */ * from t where a=1 and b=2",
        "select * from t use index(idx) where a=1",
        "select * from t force index(idx) where a=1",
    ] {
        let plan = plan_tree(&mut session, sql);
        assert!(
            plan.iter().any(|row| row.contains("TableFullScan")),
            "{sql}: {plan:?}"
        );
    }
    // Both rows, once each: an index read would duplicate the first row and
    // lose the second (its array is empty).
    assert_eq!(
        row_text(session.run("select a, b from t force index(idx) where a = 1 order by b")),
        vec![vec!["1", "2"], vec!["1", "5"]]
    );
}

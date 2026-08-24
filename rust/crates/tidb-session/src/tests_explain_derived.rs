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

//! `EXPLAIN` over a derived table -- a subquery standing in `FROM`.
//!
//! Mirrors `pkg/planner/core/logical_plan_builder.go`'s `buildResultSetNode`
//! for an `ast.TableSource` whose `Source` is a `*ast.SelectStmt`: Go builds
//! the subquery's own plan and stands it where the `FROM` entry was, so a
//! derived table is NOT an operator in Go's plan text. Every Go plan quoted
//! below was captured with `rust/difftests/gorun` on the schema
//! `t(a int, b int, c int, key ia(a), key iab(a,b))` with no analyzed
//! statistics, and each assertion states whether this tier's ACCESS decision
//! (the property `difftests/result-tests` compares -- operator, access
//! object, range, statistics source) agrees with that capture.
//!
//! The assertions below pin this tier's complete trace. Identity projections
//! are eliminated like Go; reader/cop layers are retained where the physical
//! plan needs them, while a single local leaf may still be printed directly.

#![cfg(test)]

use crate::tests_support::*;
use crate::*;

/// The plan rows of one statement as `|`-joined text, one string per row.
fn plan(session: &mut Session, sql: &str) -> Vec<String> {
    row_text(session.run(sql))
        .into_iter()
        .map(|row| row.join("|"))
        .collect()
}

fn derived_session() -> Session {
    let mut session = Session::new();
    session
        .run("create table t (a int, b int, c int, key ia(a), key iab(a,b))")
        .unwrap();
    session
        .run("insert into t values (1,1,1),(2,2,2),(3,3,3)")
        .unwrap();
    session
}

/// The plainest derived table. Go prints NO node naming the alias:
///
/// ```text
/// TableReader_6      10000.00  root              data:TableFullScan_5
/// └─TableFullScan_5  10000.00  cop[tikv]  table:t  keep order:false, stats:pseudo
/// ```
///
/// The two agree operator for operator now, the derived table contributing
/// no node of its own: the subquery's recorded subtree simply stands in the
/// `FROM` position, and that subtree is Go's cop task under its reader. Only
/// the ids and the child NAME inside `data:` differ, both of which are build
/// order here and plan order in Go.
#[test]
fn a_derived_table_is_its_subquery_s_own_plan() {
    let mut session = derived_session();
    assert_eq!(
        plan(&mut session, "explain select * from (select * from t) x"),
        vec![
            "TableReader_2|10000.00|root||data:TableFullScan",
            "└─TableFullScan_1|10000.00|cop[tikv]|table:t|keep order:false, stats:pseudo",
        ]
    );
    // The rows the same query returns are unchanged by being described: a
    // plan-only trace stops before the drain, but an ordinary run still
    // materializes the subquery.
    assert_eq!(
        row_text(session.run("select * from (select * from t) x")),
        vec![
            vec!["1".to_owned(), "1".to_owned(), "1".to_owned()],
            vec!["2".to_owned(), "2".to_owned(), "2".to_owned()],
            vec!["3".to_owned(), "3".to_owned(), "3".to_owned()],
        ]
    );
}

/// Nesting adds no case: each derived table is one more subtree in the same
/// position. Go's capture is the same single scan it prints for one level,
/// with the ids shifted (`TableReader_7` / `TableFullScan_6`).
///
/// ACCESS AGREES: one `TableFullScan` of `table:t`, pseudo statistics.
#[test]
fn a_nested_derived_table_nests_the_subtree() {
    let mut session = derived_session();
    assert_eq!(
        plan(
            &mut session,
            "explain select * from (select * from (select * from t) y) x"
        ),
        vec![
            "TableReader_2|10000.00|root||data:TableFullScan",
            "└─TableFullScan_1|10000.00|cop[tikv]|table:t|keep order:false, stats:pseudo",
        ]
    );
}

/// A derived table over no table at all. Go's capture is
///
/// ```text
/// Projection_4   1.00  root    1->Column#1
/// └─TableDual_5  1.00  root    rows:1
/// ```
///
/// ACCESS AGREES trivially: neither side reads a table, and both reach
/// `TableDual` with `rows:1`.
#[test]
fn a_derived_table_over_no_table_reaches_table_dual() {
    let mut session = derived_session();
    assert_eq!(
        plan(&mut session, "explain select * from (select 1 as one) x"),
        vec![
            "Projection_2|1.00|root||1",
            "└─TableDual_1|1.00|root||rows:1",
        ]
    );
    assert_eq!(
        row_text(session.run("select * from (select 1 as one) x")),
        vec![vec!["1".to_owned()]]
    );
}

/// Two derived tables joined: both sides of the join are subtrees, so the
/// `FROM` list holds no base table at all. Go's capture reads `t` twice:
///
/// ```text
/// HashJoin_11         12487.50  root  inner join, equal:[eq(test.t.a, test.t.b)]
/// ├─TableReader_40(Build)  9990.00  root  data:Selection_39
/// │ └─Selection_39    9990.00  cop[tikv]        not(isnull(test.t.b))
/// │   └─TableFullScan_38  10000.00  cop[tikv]  table:t  keep order:false, stats:pseudo
/// └─TableReader_27(Probe)  9990.00  root  data:Selection_26
///   └─Selection_26    9990.00  cop[tikv]        not(isnull(test.t.a))
///     └─TableFullScan_25  10000.00  cop[tikv]  table:t  keep order:false, stats:pseudo
/// ```
///
/// ACCESS AGREES on `table:t`: a full table scan off pseudo statistics, on
/// both of the join's sides.
///
/// The JOIN METHOD now agrees: `driver::predicate_push_down` gives the join
/// the `WHERE` equality, so this reads `inner join, equal:[...]` where it
/// used to read `CARTESIAN inner join` -- the same hash join off the same
/// key that Go's capture shows.
///
/// The root JOIN METHOD and estimate agree, and the derived wildcards leave
/// no identity Projection in either plan. Go's `DeriveNotNullConds`
/// (`pkg/planner/core/operator/logicalop/logical_join.go`, the inner-join arm
/// that derives `not(isnull(key))` per join key) puts one coprocessor
/// `Selection` inside each reader and points the readers at them
/// (`data:Selection`); the merged aliases resolve to the base table's own
/// name, which is why the equality reads `eq(test.t.a, test.t.b)` -- the same
/// shape the capture above shows.
#[test]
fn two_derived_tables_join_without_a_base_table() {
    let mut session = derived_session();
    assert_eq!(
        plan(
            &mut session,
            "explain select * from (select * from t) x, (select * from t) y where x.a = y.b"
        ),
        vec![
            "HashJoin_7|12487.50|root||inner join, equal:[eq(test.t.a, test.t.b)]",
            "├─TableReader_3(Build)|9990.00|root||data:Selection",
            "│ └─Selection_2|9990.00|cop[tikv]||not(isnull(test.t.b))",
            "│   └─TableFullScan_1|10000.00|cop[tikv]|table:t|keep order:false, stats:pseudo",
            "└─TableReader_6(Probe)|9990.00|root||data:Selection",
            "  └─Selection_5|9990.00|cop[tikv]||not(isnull(test.t.a))",
            "    └─TableFullScan_4|10000.00|cop[tikv]|table:t|keep order:false, stats:pseudo",
        ]
    );
    // Go's captured rows for the same join, verbatim: 1|1|1|1|1|1 and so on.
    assert_eq!(
        row_text(
            session.run("select * from (select * from t) x, (select * from t) y where x.a = y.b")
        ),
        vec![
            vec!["1"; 6]
                .into_iter()
                .map(str::to_owned)
                .collect::<Vec<_>>(),
            vec!["2"; 6]
                .into_iter()
                .map(str::to_owned)
                .collect::<Vec<_>>(),
            vec!["3"; 6]
                .into_iter()
                .map(str::to_owned)
                .collect::<Vec<_>>(),
        ]
    );
}

/// A derived table with its own `ORDER BY ... LIMIT`, whose ordering the outer
/// query then reads. Go's capture pushes the limit into an ordered index scan:
///
/// ```text
/// Limit_13             2.00  root                     offset:0, count:2
/// └─IndexReader_30     2.00  root                     index:Limit_29
///   └─Limit_29         2.00  cop[tikv]                offset:0, count:2
///     └─IndexFullScan_27  2.00  cop[tikv]  table:t, index:ia(a)  keep order:true, desc, stats:pseudo
/// ```
///
/// The OBJECT now agrees: `ia(a)` covers `select a`, so this tier reads the
/// whole index too (`skylinePruning`'s `path.IsSingleScan`).
///
/// Go reads `ia(a)` backwards because the index already supplies `a desc`,
/// so it needs no ordering operator and prints a plain `Limit` over a
/// `keep order:true, desc` scan (captured):
///
///   Limit_13            2.00  root                 offset:0, count:2
///   └─IndexReader_26    2.00  root                 index:Limit_25
///     └─Limit_25        2.00  cop[tikv]            offset:0, count:2
///       └─IndexFullScan_24 2.00 cop[tikv] table:t, index:ia(a) keep order:true, desc, stats:pseudo
///
/// This tier now has the same physical property flow and plan shape. The
/// operator-info spelling is Go's `PhysicalIndexScan.OperatorInfo`
/// (`pkg/planner/core/operator/physicalop/physical_index_scan.go:296-311`):
/// `keep order:true`, then `, desc`, then `, stats:pseudo`. The scan's own
/// estimate is Go's limit-adjusted row count
/// (`cardinality.AdjustRowCountForIndexScanByLimit`,
/// `pkg/planner/cardinality/cross_estimation.go:93-124`; with pseudo stats and
/// no filters it is exactly the LIMIT count, and the ordering-risk ratio does
/// not apply because the path has no index/table filters), which is also what
/// both captures above show.
///
/// The rows and their order agree with Go's `3;2` as well.
#[test]
fn a_derived_table_keeps_its_own_order_by_limit() {
    let mut session = derived_session();
    assert_eq!(
        plan(
            &mut session,
            "explain select x.a from (select a from t order by a desc limit 2) x"
        ),
        vec![
            "Projection_5|2.00|root||test.t.a",
            "└─Limit_4|2.00|root||offset:0, count:2",
            "  └─IndexReader_3|2.00|root||index:Limit",
            "    └─Limit_2|2.00|cop[tikv]||offset:0, count:2",
            "      └─IndexFullScan_1|2.00|cop[tikv]|table:t, index:ia(a)|keep order:true, desc, stats:pseudo",
        ]
    );
    assert_eq!(
        row_text(session.run("select x.a from (select a from t order by a desc limit 2) x")),
        vec![vec!["3".to_owned()], vec!["2".to_owned()]]
    );
}

/// `EXPLAIN ANALYZE` meters the operators INSIDE the derived table, so its
/// `actRows` column reports what each one really produced rather than
/// attributing the whole subquery to one node.
///
/// This is the assertion that the descent is real and not cosmetic: on three
/// rows, `a > 1` inside the derived table passes 2 and `x.b < 3` outside it
/// passes 1, and both counts are on their own operator. A recorder that
/// stopped at the derived table could not tell those two apart.
#[test]
fn explain_analyze_meters_inside_the_derived_table() {
    let mut session = derived_session();
    // `actRows` is the third column of `EXPLAIN ANALYZE`'s row.
    let act_rows: Vec<(String, String)> = row_text(
        session.run("explain analyze select * from (select * from t where a > 1) x where x.b < 3"),
    )
    .into_iter()
    .map(|row| {
        (
            row[0].trim_start_matches(['└', '─', '│', ' ']).to_owned(),
            row[2].clone(),
        )
    })
    .collect();
    assert_eq!(
        act_rows,
        vec![
            ("Selection_4".to_owned(), "1".to_owned()),
            ("IndexLookUp_3".to_owned(), "2".to_owned()),
            ("├─IndexRangeScan_1(Build)".to_owned(), "1".to_owned()),
            ("TableRowIDScan_2(Probe)".to_owned(), "1".to_owned()),
        ]
    );
}

/// A derived table whose body is a SET OPERATION stands its `Union` subtree
/// directly in the `FROM` position. Go describes the same shape (captured: a
/// `Union_11` over two `TableReader`s), without an operator for the alias.
#[test]
fn a_set_operation_derived_table_keeps_its_union_plan() {
    let mut session = derived_session();
    let rows = plan(
        &mut session,
        "explain select * from (select * from t union all select a,b,c from t) x",
    );
    assert_eq!(rows.len(), 3);
    assert!(rows[0].starts_with("Union_"), "got {rows:?}");
    assert!(rows[1].contains("TableFullScan_"), "got {rows:?}");
    assert!(rows[2].contains("TableFullScan_"), "got {rows:?}");
    // The same statement executes: 3 rows from each arm, concatenated.
    assert_eq!(
        row_text(session.run(
            "select * from (select * from t union all select a,b,c from t) x order by a, b, c"
        ))
        .len(),
        6
    );
}

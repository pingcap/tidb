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
//! What this tier prints ABOVE the leaves diverges from Go by construction
//! (no `cop[tikv]` task, no `TableReader` wrapper, an always-present
//! `Projection` -- see `tidb_executor::explain`'s module doc), so the
//! assertions below are on this tier's own rows and the comparison to Go is
//! made on the access leaf.

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
/// ACCESS AGREES: `table:t` is read by a full table scan off pseudo
/// statistics on both sides. The extra `Projection` is this tier's, and the
/// derived table itself contributes no operator here either -- the subquery's
/// recorded subtree simply stands in the `FROM` position.
#[test]
fn a_derived_table_is_its_subquery_s_own_plan() {
    let mut session = derived_session();
    assert_eq!(
        plan(&mut session, "explain select * from (select * from t) x"),
        vec![
            "Projection_3|10000.00|root||*",
            "└─Projection_2|10000.00|root||*",
            "  └─TableFullScan_1|10000.00|root|table:t|keep order:false, stats:pseudo",
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
            "Projection_4|10000.00|root||*",
            "└─Projection_3|10000.00|root||*",
            "  └─Projection_2|10000.00|root||*",
            "    └─TableFullScan_1|10000.00|root|table:t|keep order:false, stats:pseudo",
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
            "Projection_3|1.00|root||*",
            "└─Projection_2|1.00|root||1",
            "  └─TableDual_1|1.00|root||rows:1",
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
/// DIVERGENCE, above the leaves and pre-existing: Go turns the `WHERE` into
/// the join's equal condition and derives `not(isnull(...))`, while this tier
/// keeps a `CARTESIAN inner join` with the predicate as a `Selection` above
/// it. Both read the same rows -- asserted here, not inferred.
#[test]
fn two_derived_tables_join_without_a_base_table() {
    let mut session = derived_session();
    assert_eq!(
        plan(
            &mut session,
            "explain select * from (select * from t) x, (select * from t) y where x.a = y.b"
        ),
        vec![
            "Projection_7|N/A|root||*",
            "└─Selection_6|N/A|root||eq(test.x.a, test.y.b)",
            "  └─HashJoin_5|N/A|root||CARTESIAN inner join",
            "    ├─Projection_2(Build)|10000.00|root||*",
            "    │ └─TableFullScan_1|10000.00|root|table:t|keep order:false, stats:pseudo",
            "    └─Projection_4(Probe)|10000.00|root||*",
            "      └─TableFullScan_3|10000.00|root|table:t|keep order:false, stats:pseudo",
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
/// DIVERGENCE, narrowed to the ORDER: Go reads `ia(a)` backwards because the
/// index already supplies `a desc`, so its scan stops after 2 rows, while
/// this tier scans the index forwards and sorts. That is a `keep order` /
/// pushed-limit gap, not an enumeration one, and it is NOT anything about
/// derived tables. The rows, and their order, agree with Go's `3;2`.
#[test]
fn a_derived_table_keeps_its_own_order_by_limit() {
    let mut session = derived_session();
    assert_eq!(
        plan(
            &mut session,
            "explain select x.a from (select a from t order by a desc limit 2) x"
        ),
        vec![
            "Projection_5|2.00|root||test.x.a",
            "└─Limit_4|2.00|root||offset:0, count:2",
            "  └─Projection_3|10000.00|root||test.t.a",
            "    └─Sort_2|10000.00|root||test.t.a:desc",
            "      └─IndexFullScan_1|10000.00|root|table:t, index:ia(a)|keep order:false, stats:pseudo",
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
            ("Projection_5".to_owned(), "1".to_owned()),
            ("Selection_4".to_owned(), "1".to_owned()),
            ("Projection_3".to_owned(), "2".to_owned()),
            ("Selection_2".to_owned(), "2".to_owned()),
            ("TableFullScan_1".to_owned(), "3".to_owned()),
        ]
    );
}

/// A derived table whose body is a SET OPERATION is the one shape still not
/// described: `run_set_opr_stmt` concatenates its arms without recording an
/// operator, so there is no subtree to stand in the `FROM` position. The
/// refusal names the arm, and the statement still RUNS -- only its
/// description is refused.
///
/// Go describes it (captured: a `Union_11` over two `TableReader`s with
/// `gt(test.t.a, 0)` pushed into both arms), so this is a refusal to describe
/// and not a claim about Go.
#[test]
fn a_set_operation_derived_table_is_refused_only_as_a_description() {
    let mut session = derived_session();
    let refused =
        session.run("explain select * from (select * from t union all select a,b,c from t) x");
    assert!(
        matches!(
            refused,
            Err(DriverError::Unsupported(
                "a set-operation derived table's plan is not recorded yet"
            ))
        ),
        "got {refused:?}"
    );
    // The same statement executes: 3 rows from each arm, concatenated.
    assert_eq!(
        row_text(session.run(
            "select * from (select * from t union all select a,b,c from t) x order by a, b, c"
        ))
        .len(),
        6
    );
}

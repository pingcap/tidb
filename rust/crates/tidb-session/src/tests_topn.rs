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

//! `ORDER BY` + `LIMIT` fused into one `TopN`, end to end through a session.
//!
//! Go's `topn_push_down` rule (`pkg/planner/core/rule_topn_push_down.go`)
//! turns a `LogicalLimit` into a by-item-less `LogicalTopN`, pushes it down
//! through the projection, and lets the `LogicalSort` hand it its by-items.
//! Every expected value below was captured from real TiDB with
//! `rust/difftests/gorun` on the schema and rows this file creates.

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

fn flat(rows: Vec<Vec<String>>) -> Vec<String> {
    rows.into_iter().map(|row| row.join(",")).collect()
}

fn topn_session() -> Session {
    let mut session = Session::new();
    session.run("create table t (a int, b int)").unwrap();
    session
        .run("insert into t values (3,1),(3,2),(2,3),(1,4),(2,5),(1,6)")
        .unwrap();
    session
}

/// The rows the window selects, for the whole `offset`/`count`/direction
/// grid. Captured from Go on these six rows.
#[test]
fn the_fused_topn_returns_gos_rows() {
    let mut session = topn_session();
    // gorun: `select a from t order by a limit 2` -> 1;1
    assert_eq!(
        flat(row_text(session.run("select a from t order by a limit 2"))),
        vec!["1".to_owned(), "1".to_owned()]
    );
    // gorun: `select a from t order by a limit 2,3` -> 2;2;3
    assert_eq!(
        flat(row_text(
            session.run("select a from t order by a limit 2,3")
        )),
        vec!["2".to_owned(), "2".to_owned(), "3".to_owned()]
    );
    // gorun: `select a from t order by a desc limit 2` -> 3;3
    assert_eq!(
        flat(row_text(
            session.run("select a from t order by a desc limit 2")
        )),
        vec!["3".to_owned(), "3".to_owned()]
    );
    // A second by-item makes every row distinct, so the whole ordered
    // window is pinned and not just its key column.
    // gorun: `select a, b from t order by a, b desc limit 3` -> 1 6;1 4;2 5
    assert_eq!(
        flat(row_text(
            session.run("select a, b from t order by a, b desc limit 3")
        )),
        vec!["1,6".to_owned(), "1,4".to_owned(), "2,5".to_owned()]
    );
    // An offset past the end selects nothing.
    assert!(row_text(session.run("select a from t order by a limit 100,3")).is_empty());
}

/// The plan the fusion produces. Real TiDB prints a ROOT `TopN` with this
/// info text (captured: `TopN_8|2.00|root||test.t.a, offset:0, count:2`);
/// this tier's standing divergences add the always-present `Projection` and
/// drop the cop task (see `tidb_executor::explain`'s module doc).
#[test]
fn the_fused_topn_prints_gos_operator_info() {
    let mut session = topn_session();
    assert_eq!(
        plan(&mut session, "explain select a from t order by a limit 2"),
        vec![
            "Projection_3|2.00|root||test.t.a",
            "└─TopN_2|2.00|root||test.t.a, offset:0, count:2",
            "  └─TableFullScan_1|10000.00|root|table:t|keep order:false, stats:pseudo",
        ]
    );
    // `limit 1,2`: Go's estRows is the COUNT, not `offset + count`
    // (`property.DeriveLimitStats(child, Count)`); captured as
    // `TopN_8|2.00|root||test.t.b, offset:1, count:2`.
    assert_eq!(
        plan(&mut session, "explain select a from t order by b limit 1,2"),
        vec![
            "Projection_3|2.00|root||test.t.a",
            "└─TopN_2|2.00|root||test.t.b, offset:1, count:2",
            "  └─TableFullScan_1|10000.00|root|table:t|keep order:false, stats:pseudo",
        ]
    );
    // A descending by-item prints Go's `:desc` suffix, as the `Sort` did.
    assert_eq!(
        plan(
            &mut session,
            "explain select a from t order by b desc limit 3"
        ),
        vec![
            "Projection_3|3.00|root||test.t.a",
            "└─TopN_2|3.00|root||test.t.b:desc, offset:0, count:3",
            "  └─TableFullScan_1|10000.00|root|table:t|keep order:false, stats:pseudo",
        ]
    );
}

/// `SELECT DISTINCT` must NOT fuse. This tier's dedup sits BETWEEN the sort
/// and the limit, so a bounded sort would discard rows before they were
/// deduplicated -- `select distinct a from t order by a limit 2` would answer
/// `1` instead of Go's `1;2`, because the two smallest raw rows are both `1`.
///
/// Go reaches the right answer by a different route: its `TopN` lands ABOVE
/// the aggregation (captured: `TopN_9|2.00|root||test.t.a, offset:0, count:2`
/// over `HashAgg_18`), a position this tier's build order cannot express, so
/// it keeps the `Sort` and the `Limit` instead.
#[test]
fn select_distinct_does_not_fuse_and_keeps_gos_rows() {
    let mut session = topn_session();
    // gorun: `select distinct a from t order by a limit 2` -> 1;2
    assert_eq!(
        flat(row_text(
            session.run("select distinct a from t order by a limit 2")
        )),
        vec!["1".to_owned(), "2".to_owned()]
    );
    assert_eq!(
        plan(
            &mut session,
            "explain select distinct a from t order by a limit 2"
        ),
        vec![
            "Limit_5|2.00|root||offset:0, count:2",
            "└─HashAgg_4|8000.00|root||group by:test.t.a, funcs:firstrow",
            "  └─Projection_3|10000.00|root||test.t.a",
            "    └─Sort_2|10000.00|root||test.t.a",
            "      └─TableFullScan_1|10000.00|root|table:t|keep order:false, stats:pseudo",
        ]
    );
}

/// `ORDER BY` with no `LIMIT` above it has nothing to fuse with, so the plain
/// `Sort` stays -- and still returns every row.
#[test]
fn an_order_by_without_a_limit_still_builds_a_sort() {
    let mut session = topn_session();
    assert_eq!(
        plan(&mut session, "explain select a from t order by a"),
        vec![
            "Projection_3|10000.00|root||test.t.a",
            "└─Sort_2|10000.00|root||test.t.a",
            "  └─TableFullScan_1|10000.00|root|table:t|keep order:false, stats:pseudo",
        ]
    );
    assert_eq!(
        flat(row_text(session.run("select a from t order by a"))).len(),
        6
    );
}

/// A `GROUP BY` pipeline fuses above the aggregate, which is where Go's rule
/// also stops: `LogicalAggregation` inherits `BaseLogicalPlan.PushDownTopN`,
/// which attaches the `TopN` on top rather than pushing it through. Captured
/// shape: `Projection_7|2.00|root|| ...` over `TopN_10|2.00|root||test.t.a,
/// offset:0, count:2` over the two-phase `HashAgg`.
#[test]
fn a_group_by_pipeline_fuses_above_the_aggregate() {
    let mut session = topn_session();
    assert_eq!(
        plan(
            &mut session,
            "explain select a, count(*) from t group by a order by a limit 2"
        ),
        vec![
            "TopN_3|2.00|root||test.t.a, offset:0, count:2",
            "└─HashAgg_2|8000.00|root||group by:test.t.a, funcs:test.t.a, count(1)",
            "  └─TableFullScan_1|10000.00|root|table:t|keep order:false, stats:pseudo",
        ]
    );
    // gorun: `select a, count(*) from t group by a order by a limit 2`
    //   -> 1 2;2 2
    assert_eq!(
        flat(row_text(session.run(
            "select a, count(*) from t group by a order by a limit 2"
        ))),
        vec!["1,2".to_owned(), "2,2".to_owned()]
    );
}

/// The aggregate pipeline's own DISTINCT guard exists for the plain path's
/// reason -- stage 11's dedup runs ABOVE stage 9's order/limit, so fusing
/// would discard rows before they were deduplicated.
///
/// Without a `LIMIT` the pipeline already agrees with Go.
#[test]
fn a_distinct_over_a_group_by_orders_like_go() {
    let mut session = topn_session();
    // gorun: `select distinct a from t group by a, b order by a` -> 1;2;3
    assert_eq!(
        flat(row_text(
            session.run("select distinct a from t group by a, b order by a")
        )),
        vec!["1".to_owned(), "2".to_owned(), "3".to_owned()]
    );
}

/// KNOWN BUG, pre-dating the `TopN` work and NOT introduced by it: the
/// aggregate pipeline applies `SELECT DISTINCT` (stage 11) ABOVE the `LIMIT`
/// (stage 9), while Go's `buildSelect` builds `Projection -> Distinct ->
/// Sort -> Limit`, with the dedup BELOW the limit. So the limit truncates
/// before the dedup and this answers `1` where Go answers `1;2`.
///
/// Fixing it means running stage 10's projection and the dedup before stage
/// 9 and resolving the by-items against the PROJECTED schema (which
/// `checkOrderByInDistinct` already guarantees they are in) -- a
/// restructuring of the aggregate pipeline's tail with its own test surface,
/// not a side effect of the ORDER BY + LIMIT fusion. Ignored rather than
/// pinned, so the wrong answer is never recorded as expected.
///
/// It also means the aggregate path's own DISTINCT guard cannot be probed by
/// a mutation today: removing it produces the same wrong answer this bug
/// already produces.
#[test]
#[ignore = "pre-existing: the aggregate pipeline dedups above the LIMIT, not below it"]
fn a_distinct_over_a_group_by_limits_after_the_dedup_like_go() {
    let mut session = topn_session();
    // gorun: `select distinct a from t group by a, b order by a limit 2` -> 1;2
    assert_eq!(
        flat(row_text(session.run(
            "select distinct a from t group by a, b order by a limit 2"
        ))),
        vec!["1".to_owned(), "2".to_owned()]
    );
}

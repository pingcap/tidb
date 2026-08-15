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
/// this tier's standing divergence adds the always-present `Projection`; the
/// root reader and cop task otherwise retain Go's physical split.
#[test]
fn the_fused_topn_prints_gos_operator_info() {
    let mut session = topn_session();
    assert_eq!(
        plan(&mut session, "explain select a from t order by a limit 2"),
        vec![
            "Projection_5|2.00|root||test.t.a",
            "└─TopN_4|2.00|root||test.t.a, offset:0, count:2",
            "  └─TableReader_3|2.00|root||data:TopN",
            "    └─TopN_2|2.00|cop[tikv]||test.t.a, offset:0, count:2",
            "      └─TableFullScan_1|10000.00|cop[tikv]|table:t|keep order:false, stats:pseudo",
        ]
    );
    // `limit 1,2`: Go's estRows is the COUNT, not `offset + count`
    // (`property.DeriveLimitStats(child, Count)`); captured as
    // `TopN_8|2.00|root||test.t.b, offset:1, count:2`.
    assert_eq!(
        plan(&mut session, "explain select a from t order by b limit 1,2"),
        vec![
            "Projection_5|2.00|root||test.t.a",
            "└─TopN_4|2.00|root||test.t.b, offset:1, count:2",
            "  └─TableReader_3|3.00|root||data:TopN",
            "    └─TopN_2|3.00|cop[tikv]||test.t.b, offset:0, count:3",
            "      └─TableFullScan_1|10000.00|cop[tikv]|table:t|keep order:false, stats:pseudo",
        ]
    );
    // A descending by-item prints Go's `:desc` suffix, as the `Sort` did.
    assert_eq!(
        plan(
            &mut session,
            "explain select a from t order by b desc limit 3"
        ),
        vec![
            "Projection_5|3.00|root||test.t.a",
            "└─TopN_4|3.00|root||test.t.b:desc, offset:0, count:3",
            "  └─TableReader_3|3.00|root||data:TopN",
            "    └─TopN_2|3.00|cop[tikv]||test.t.b:desc, offset:0, count:3",
            "      └─TableFullScan_1|10000.00|cop[tikv]|table:t|keep order:false, stats:pseudo",
        ]
    );
}

/// `SELECT DISTINCT` fuses only after its deduplicating aggregation exists.
/// A TopN below that HashAgg would discard duplicate rows before they were
/// deduplicated and could answer only `1`; Go instead puts TopN above HashAgg
/// and returns `1;2`.
#[test]
fn select_distinct_fuses_above_aggregation_and_keeps_gos_rows() {
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
            "TopN_5|2.00|root||test.t.a, offset:0, count:2",
            "└─HashAgg_4|8000.00|root||group by:test.t.a, funcs:firstrow(test.t.a)->test.t.a",
            "  └─TableReader_3|8000.00|root||data:HashAgg",
            "    └─HashAgg_2|8000.00|cop[tikv]||group by:test.t.a,",
            "      └─TableFullScan_1|10000.00|cop[tikv]|table:t|keep order:false, stats:pseudo",
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

/// Go source of truth: `TestIssue54206` in
/// `pkg/executor/sortexec/topn_spill_test.go`.
///
/// Disabling temporary storage must not make the TopN over a projected LEFT
/// JOIN result depend on a spill path. The false join predicate produces one
/// null-extended row, whose projected value is still available to the alias
/// used by `ORDER BY`.
#[test]
fn test_issue_54206() {
    let mut session = Session::new();
    session
        .run("SET @@global.tidb_enable_tmp_storage_on_oom = 0")
        .unwrap();
    session.run("CREATE TABLE t1(a BIGINT, b BIGINT)").unwrap();
    session.run("CREATE TABLE t2(a BIGINT, b BIGINT)").unwrap();
    session.run("INSERT INTO t1 VALUES(1, 1)").unwrap();

    assert_eq!(
        flat(row_text(session.run(
            "SELECT t1.a + t1.b AS result \
             FROM t1 LEFT JOIN t2 ON 1 = 0 ORDER BY result LIMIT 1"
        ))),
        vec!["2".to_owned()]
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
            "└─HashAgg_2|8000.00|root||group by:test.t.a, funcs:test.t.a, count(1)->Column#0",
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

/// Go's `buildSelect` builds `Projection -> Distinct -> Sort -> Limit`
/// (`logical_plan_builder.go:4528-4602`), with the dedup BELOW the limit.
///
/// The aggregate pipeline used to run its `SELECT DISTINCT` (stage 11) ABOVE
/// the `LIMIT` (stage 9), so the limit truncated rows the dedup would have
/// collapsed and this answered `1` where Go answers `1;2`. With DISTINCT the
/// projection and the dedup now run first, and the by-items re-resolve
/// against the projected output -- which `checkOrderByInDistinct` already
/// guarantees they are in.
#[test]
fn a_distinct_over_a_group_by_limits_after_the_dedup_like_go() {
    let mut session = topn_session();
    // gorun: `select distinct a from t group by a, b order by a limit 2` -> 1;2
    for (sql, want) in [
        // gorun, on these six rows:
        (
            "select distinct a from t group by a, b order by a limit 2",
            "1;2",
        ),
        (
            "select distinct a from t group by a, b order by a desc limit 2",
            "3;2",
        ),
        (
            "select distinct a from t group by a, b order by a limit 1, 2",
            "2;3",
        ),
        (
            "select distinct a, count(*) from t group by a, b order by a limit 3",
            "1,1;2,1;3,1",
        ),
        // A computed select field has no aggregation-output column to
        // deduplicate on until the final projection evaluates it, so this
        // shape keeps the OLD stage order -- and still agrees with Go here,
        // because every `a+0` group is distinct already.
        (
            "select distinct a+0 from t group by a, b order by a+0 limit 2",
            "1;2",
        ),
        // A HAVING aggregate leaves a carrier column in the aggregation's
        // output that the select list never reports. The dedup key is the
        // SELECT LIST's columns, not the whole row -- grouping by (a, sum(b))
        // would collapse nothing and the limit would answer `1`.
        (
            "select distinct a from t group by a, b having sum(b) > 0 order by a limit 2",
            "1;2",
        ),
    ] {
        assert_eq!(
            flat(row_text(session.run(sql))).join(";"),
            want.to_owned(),
            "{sql}"
        );
    }
}

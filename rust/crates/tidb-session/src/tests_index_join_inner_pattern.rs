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

//! Which operators an index join may re-seed a leaf THROUGH.

use crate::tests_support::row_text;
use crate::Session;

fn plan(session: &mut Session, sql: &str) -> String {
    row_text(session.run(sql))
        .into_iter()
        .map(|row| row.join(" "))
        .collect::<Vec<_>>()
        .join("\n")
}

fn fixture() -> Session {
    let mut session = Session::new();
    for table in ["t1", "t2", "t3"] {
        session
            .run(&format!(
                "CREATE TABLE {table} (a int, b int, c varchar(32), PRIMARY KEY (a), KEY (b))"
            ))
            .unwrap();
    }
    session
        .run("INSERT INTO t1 VALUES (1,10,'a1'),(2,20,'a2')")
        .unwrap();
    session
        .run("INSERT INTO t2 VALUES (1,100,'b1'),(2,200,'b2')")
        .unwrap();
    session
        .run("INSERT INTO t3 VALUES (1,1000,'c1'),(2,2000,'c2')")
        .unwrap();
    session
}

/// Go `admitIndexJoinInnerChildPattern` names the operators that may sit
/// between an index join and the `DataSource` it re-seeds, and refuses every
/// other one -- "index join inner side couldn't allow join, sort, limit,
/// because they are Optimization Fence".
///
/// `WITH ROLLUP` builds an `Expand`, which that switch does not name at all.
/// Walking to any table that merely CARRIES the join key by name reached `t2`
/// straight through the rollup, and the probe then re-seeded a leaf whose
/// rows the `Expand` above it had already re-shaped.
#[test]
fn a_rollup_is_a_fence_an_index_join_probe_may_not_cross() {
    let mut session = fixture();
    let rollup = plan(
        &mut session,
        "EXPLAIN SELECT t1.a, dt.key_a, dt.sum_b FROM t1 JOIN (\
         SELECT t2.a AS key_a, sum(t3.b) AS sum_b FROM t2 JOIN t3 ON t2.a = t3.a \
         GROUP BY t2.a WITH ROLLUP) dt ON t1.a = dt.key_a",
    );
    assert!(
        !rollup.contains("decided by"),
        "no leaf under the rollup may be re-seeded by the probe:\n{rollup}"
    );
    // TiDB reads both inner tables whole, under an ordinary hash join.
    assert!(
        rollup.contains("table:t2") && rollup.contains("table:t3"),
        "both inner tables are still read:\n{rollup}"
    );

    // The SAME query without `WITH ROLLUP` keeps every operator in Go's
    // admitted set, so nothing here is a blanket refusal of grouped inners.
    let grouped = plan(
        &mut session,
        "EXPLAIN SELECT t1.a, dt.key_a, dt.sum_b FROM t1 JOIN (\
         SELECT t2.a AS key_a, sum(t3.b) AS sum_b FROM t2 JOIN t3 ON t2.a = t3.a \
         GROUP BY t2.a) dt ON t1.a = dt.key_a",
    );
    assert!(
        grouped.contains("table:t2") && grouped.contains("table:t3"),
        "the plain grouped form still plans:\n{grouped}"
    );

    // Both forms answer the same rows either way, which is what the fence
    // exists to keep true.
    assert_eq!(
        row_text(session.run(
            "SELECT t1.a, dt.key_a, dt.sum_b FROM t1 JOIN (\
             SELECT t2.a AS key_a, sum(t3.b) AS sum_b FROM t2 JOIN t3 ON t2.a = t3.a \
             GROUP BY t2.a WITH ROLLUP) dt ON t1.a = dt.key_a ORDER BY t1.a"
        )),
        vec![vec!["1", "1", "1000"], vec!["2", "2", "2000"]]
    );
}

/// Go `checkIndexJoinInnerTaskWithAgg`: an inner join key that comes from the
/// re-seeded `DataSource` must also be a GROUP BY key, "otherwise the
/// aggregation group might be split into multiple groups by the join keys,
/// which generate incorrect result".
///
/// `ONLY_FULL_GROUP_BY` normally makes this unreachable -- with it on, an
/// aggregation's outputs are its group keys and its aggregates, so a key that
/// comes from the leaf IS a group key. Off, a derived table may output a bare
/// column the grouping never named, and re-seeding the leaf by it would hand
/// each group only the rows one probe key selected.
///
/// This is a PIN, not a demonstration: it holds before the admission walk
/// landed as well, because something else already declined this shape. It is
/// here because the rule it names is a correctness rule and nothing else
/// states it.
#[test]
fn a_probe_key_outside_the_group_keys_is_refused() {
    let mut session = fixture();
    session.run("SET @@sql_mode = ''").unwrap();
    let plan_text = plan(
        &mut session,
        "EXPLAIN SELECT t1.a FROM t1 JOIN (\
         SELECT t2.a AS key_a, t3.b AS bb FROM t2 JOIN t3 ON t2.a = t3.a \
         GROUP BY t2.a) dt ON t1.b = dt.bb",
    );
    assert!(
        !plan_text.contains("decided by"),
        "`bb` is not a group key, so no leaf is re-seeded by it:\n{plan_text}"
    );
}

/// The IN-subquery dedup an index join builds from is a StreamAgg on both
/// sides of its reader once the subquery's key has an ordered index.
///
/// Go's `buildDistinct` (`pkg/planner/core/logical_plan_builder.go:1966`)
/// makes that dedup a LogicalAggregation, `getStreamAggs`
/// (`pkg/planner/core/operator/physicalop/physical_stream_agg.go:89`)
/// enumerates it beside the hash candidate, and the index prefix
/// `PreparePossibleProperties` offers makes the stream one admissible. The
/// index join asks its OUTER child for no order, which does not take that
/// costed candidate away -- the ordered scan is what it was costed WITH.
///
/// Recorded by TiDB in `tests/integrationtest/r/index_join.result:47-56`.
#[test]
fn an_index_joins_dedup_build_side_keeps_its_ordered_index_stream_agg() {
    let mut session = Session::new();
    session
        .run("SET @@tidb_opt_insubq_to_join_and_agg=1")
        .unwrap();
    session
        .run("CREATE TABLE t1(a int not null, b int not null, key a(a))")
        .unwrap();
    session
        .run("CREATE TABLE t2(a int not null, b int not null, key a(a))")
        .unwrap();
    let plan_text = plan(
        &mut session,
        "EXPLAIN SELECT /*+ TIDB_INLJ(t1) */ * FROM t1 WHERE t1.a IN (SELECT t2.a FROM t2)",
    );
    // Refreshed against the LIVE oracle (SELECT tidb_version() =>
    // fdfadb96b2cfdc5a7c26b8eb7b2a3da5f3038d85): Go builds the hinted
    // IndexJoin with the dedup StreamAgg chain as the OUTER build side and
    // t1 as the INNER probe through its `a(a)` index -- `IndexJoin ->
    // IndexLookUp(Probe) -> [IndexRangeScan decided-by | TableRowIDScan]`.
    // The hint reaches the rewritten join through Go's
    // `SetPreferredJoinTypeAndOrder(b.TableHints())` stamp on the subquery
    // apply (logical_plan_builder.go:5760).
    for expected in [
        "IndexJoin_11 10000.00 root  inner join, inner:IndexLookUp_26, outer key:test.t2.a, inner key:test.t1.a, equal cond:eq(test.t2.a, test.t1.a)",
        "StreamAgg_40(Build) 8000.00 root  group by:test.t2.a, funcs:firstrow(test.t2.a)->test.t2.a",
        "IndexReader_41 8000.00 root  index:StreamAgg_30",
        "StreamAgg_30 8000.00 cop[tikv]  group by:test.t2.a, ",
        "IndexFullScan_19 10000.00 cop[tikv] table:t2, index:a(a) keep order:true, stats:pseudo",
        "IndexLookUp_26(Probe) 10000.00 root",
        "IndexRangeScan_24(Build) 10000.00 cop[tikv] table:t1, index:a(a) range: decided by [eq(test.t1.a, test.t2.a)], keep order:false, stats:pseudo",
        "TableRowIDScan_25(Probe) 10000.00 cop[tikv] table:t1 keep order:false, stats:pseudo",
    ] {
        assert!(
            plan_text.contains(expected),
            "missing `{expected}` in:\n{plan_text}"
        );
    }
}

/// Task 65: a common (composite/CLUSTERED) handle table probed by an index
/// join on a PREFIX of its primary key (here just `a` of `PRIMARY KEY(a,b)`)
/// used to mislabel its `TableReader`'s own `data:` field as
/// `TableFullScan` even though the scan beneath it correctly printed
/// `TableRangeScan` with `range: decided by [...]`.
///
/// Root cause: Go's `PhysicalTableScan.IsFullScan` short-circuits to "not a
/// full scan" via `haveCorCol()` (an index-join inner probe's access
/// condition is built against the outer row, i.e. a correlated column)
/// BEFORE it ever inspects the ranges. The Rust `scan_kind` decision
/// (`find_best_task/dispatch.rs`) inspected only the plan-time placeholder
/// ranges: an int-handle placeholder (`full_int_range`) happened to fail
/// `is_full_range`'s boundary check and so was already labelled correctly by
/// coincidence, but a common-handle placeholder (`full_range`) satisfied it,
/// mislabelling the `TableReader` as `TableFullScan` while its own child
/// scan (computed by a different, correct code path) still said
/// `TableRangeScan`.
///
/// Refreshed against the LIVE oracle (`SELECT tidb_version()` =>
/// `844818561a477dc6de4bf521d500ffbd56340b1e`): Go's `TableReader(Probe)`
/// prints `data:TableRangeScan_25` for this exact schema and query.
#[test]
fn a_common_handle_probes_prefix_key_reader_labels_table_range_scan() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE ps (a INT NOT NULL, b INT NOT NULL, v INT, PRIMARY KEY(a,b) CLUSTERED)")
        .unwrap();
    session.run("CREATE TABLE o (a INT NOT NULL)").unwrap();
    let plan_text = plan(
        &mut session,
        "EXPLAIN SELECT /*+ INL_JOIN(ps) */ * FROM o JOIN ps ON o.a = ps.a",
    );
    let probe_reader_line = plan_text
        .lines()
        .find(|line| line.contains("TableReader") && line.contains("(Probe)"))
        .unwrap_or_else(|| panic!("expected a probe-side TableReader:\n{plan_text}"));
    assert!(
        probe_reader_line.contains("data:TableRangeScan"),
        "the probe-side TableReader must label itself TableRangeScan, matching \
         its own child scan and Go's live oracle, not TableFullScan:\n{plan_text}"
    );
    assert!(
        plan_text.contains("range: decided by"),
        "the child TableRangeScan already correctly names its runtime range:\n{plan_text}"
    );
}

/// Go `admitIndexJoinInnerChildPattern` (`exhaust_physical_plans.go:643`)
/// admits a `LogicalSelection` between an index join and the `DataSource` it
/// re-seeds ONLY when `tidb_enable_inl_join_inner_multi_pattern` (default ON)
/// holds. `t2.c REGEXP 'x'` is not TiKV-pushable, so it survives as a real
/// `LogicalSelection` above `t2` rather than folding into the scan's own
/// pushed-down conditions -- exactly the shape the gate has to see.
///
/// Before `admits_index_join_inner_child_pattern` was wired into
/// `find_best_task`'s generic dispatch tail (`find_best_task/dispatch.rs`),
/// Rust had no equivalent of this up-front gate at all: `SET SESSION
/// tidb_enable_inl_join_inner_multi_pattern = OFF` was silently ignored and
/// the walk-through-Selection IndexJoin below was built either way.
#[test]
fn a_selection_probe_walks_through_only_with_multi_pattern_on() {
    let mut session = fixture();
    let sql = "EXPLAIN SELECT /*+ INL_JOIN(t2) */ * FROM t1 JOIN t2 ON t1.a = t2.a \
               WHERE t2.c REGEXP 'x'";

    let on = plan(&mut session, sql);
    assert!(
        on.contains("IndexJoin") && on.contains("inner:Selection"),
        "multi_pattern defaults ON: the probe must walk through the residual \
         Selection above t2:\n{on}"
    );
    assert!(
        on.contains("range: decided by"),
        "the walked-through Selection's child scan must still be the \
         runtime-ranged probe:\n{on}"
    );

    session
        .run("SET SESSION tidb_enable_inl_join_inner_multi_pattern = OFF")
        .unwrap();
    let off = plan(&mut session, sql);
    assert!(
        !off.contains("IndexJoin"),
        "multi_pattern OFF: Go refuses Selection as an index-join inner \
         pattern, so no IndexJoin -- got:\n{off}"
    );
    assert!(
        !off.contains("range: decided by"),
        "with the walk-through refused, t2 must plan as an ordinary scan, \
         not a runtime-ranged probe:\n{off}"
    );
}

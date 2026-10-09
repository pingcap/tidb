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
    // Keep Go's operator/ordering contract; old plan IDs and pseudo-NDV
    // estimates belonged to a different source revision.
    assert!(plan_text.contains("inner join, inner:IndexLookUp_"));
    assert_eq!(
        plan_text
            .lines()
            .filter(|line| line.contains("StreamAgg_") && line.contains("group by:test.t2.a"))
            .count(),
        2
    );
    for expected in [
        "outer key:test.t2.a, inner key:test.t1.a",
        "group by:test.t2.a, funcs:firstrow(test.t2.a)->test.t2.a",
        "cop[tikv] table:t2, index:a(a) keep order:true",
        "table:t1, index:a(a) range: decided by [eq(test.t1.a, test.t2.a)]",
        "cop[tikv] table:t1 keep order:false",
    ] {
        assert!(
            plan_text.contains(expected),
            "missing `{expected}` in:\n{plan_text}"
        );
    }
    session
        .run("INSERT INTO t1 VALUES (1,10),(2,20),(3,30)")
        .unwrap();
    session
        .run("INSERT INTO t2 VALUES (1,100),(1,101),(2,200)")
        .unwrap();
    assert_eq!(row_text(session.run(
        "SELECT /*+ TIDB_INLJ(t1) */ * FROM t1 WHERE t1.a IN (SELECT t2.a FROM t2) ORDER BY t1.a")),
        vec![vec!["1", "10"], vec!["2", "20"]]);
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

/// Go scalarExprSupportedByTiKV admits REGEXP for nonbinary collations.
/// Its cop Selection does not require a LogicalSelection inner pattern.
#[test]
fn a_pushdown_filter_keeps_the_index_probe_without_multi_pattern() {
    let mut session = fixture();
    let sql = "SELECT /*+ INL_JOIN(t2) */ t1.a FROM t1 JOIN t2 ON t1.a=t2.a \
        WHERE t2.c REGEXP '1$'";
    for enabled in ["ON", "OFF"] {
        session
            .run(&format!(
                "SET tidb_enable_inl_join_inner_multi_pattern={enabled}"
            ))
            .unwrap();
        let text = plan(&mut session, &format!("EXPLAIN {sql}"));
        assert!(
            text.contains("IndexJoin") && text.contains("range: decided by"),
            "{text}"
        );
        assert!(
            text.lines()
                .any(|line| line.contains("cop[tikv]") && line.contains("regexp(")),
            "{text}"
        );
        assert_eq!(row_text(session.run(sql)), vec![vec!["1"]]);
    }
}

#[test]
fn probe_explain_distinguishes_range_access_from_residual_filters() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE probe_outer(a INT NOT NULL,b INT NOT NULL)")
        .unwrap();
    session.run("CREATE TABLE probe_common(a INT NOT NULL,b INT NOT NULL,v INT,PRIMARY KEY(a,b) CLUSTERED)").unwrap();
    session
        .run("CREATE TABLE probe_index(a INT NOT NULL,b INT NOT NULL,v INT,INDEX ab(a,b))")
        .unwrap();
    let mut failures = Vec::new();
    for table in ["probe_common", "probe_index"] {
        for hint in ["INL_JOIN", "INL_HASH_JOIN"] {
            for (condition, access) in [
                ("", ""),
                (" AND p.b>o.b", "gt(test.{table}.b, test.probe_outer.b)"),
                (" AND p.b!=o.b", ""),
                (" AND p.v>o.b", ""),
                (" AND p.b>3", "gt(test.{table}.b, 3)"),
                (" AND p.b=o.b", "eq(test.{table}.b, test.probe_outer.b)"),
                (" AND o.b<p.b", "lt(test.probe_outer.b, test.{table}.b)"),
                (" AND p.b>=o.b AND p.b<o.b+10", "ge(test.{table}.b, test.probe_outer.b) lt(test.{table}.b, plus(test.probe_outer.b, 10))"),
                (" AND p.b>o.b AND p.b>3", "gt(test.{table}.b, test.probe_outer.b)"),
                (" AND p.b!=3", "ne(test.{table}.b, 3)"),
                (" AND p.b>3 AND p.v>4", "gt(test.{table}.b, 3)"),
                (" AND p.a>o.b", ""),
            ] {
                let sql = format!("EXPLAIN SELECT /*+ {hint}(p) */ * FROM probe_outer o JOIN {table} p ON p.a=o.a{condition}");
                let result = plan(&mut session, &sql);
                let actual = result.lines().find_map(|line| line.split_once("range: decided by ").map(|(_,value)| value.split_once(", keep order:").unwrap().0));
                let access = access.replace("{table}",table);
                let expected = format!("[eq(test.{table}.a, test.probe_outer.a){}{}]", if access.is_empty(){""}else{" "}, access);
                if actual != Some(expected.as_str()) { failures.push(format!("{sql}: expected {expected}; got {actual:?}\n{result}")); }
            }
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

#[test]
fn index_probe_key_order_and_fixed_prefix_match_go_plans_and_rows() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE key_outer(a INT NOT NULL,b INT NOT NULL,c INT NOT NULL)")
        .unwrap();
    session
        .run("INSERT INTO key_outer VALUES(1,7,9),(2,7,9),(1,8,10)")
        .unwrap();
    session.run("CREATE TABLE key_common(a INT NOT NULL,b INT NOT NULL,c INT NOT NULL,v INT,PRIMARY KEY(a,b,c) CLUSTERED)").unwrap();
    session.run("CREATE TABLE key_index(a INT NOT NULL,b INT NOT NULL,c INT NOT NULL,v INT,INDEX abc(a,b,c))").unwrap();
    let cases = [
        (
            "p.c=o.c and p.b=o.b and p.a=o.a",
            "[eq(test.{table}.a, test.key_outer.a) eq(test.{table}.b, test.key_outer.b) eq(test.{table}.c, test.key_outer.c)]",
            vec![vec!["1", "1", "7", "9"], vec!["2", "2", "7", "9"], vec!["3", "1", "8", "10"]],
        ),
        (
            "p.b=o.b and p.a=o.a and p.c=o.c",
            "[eq(test.{table}.a, test.key_outer.a) eq(test.{table}.b, test.key_outer.b) eq(test.{table}.c, test.key_outer.c)]",
            vec![vec!["1", "1", "7", "9"], vec!["2", "2", "7", "9"], vec!["3", "1", "8", "10"]],
        ),
        (
            "p.c=o.c and p.a=o.a and p.b=7",
            "[eq(test.{table}.a, test.key_outer.a) eq(test.{table}.c, test.key_outer.c) eq(test.{table}.b, 7)]",
            vec![vec!["1", "1", "7", "9"], vec!["2", "2", "7", "9"]],
        ),
        (
            "p.c=o.c and p.b=7 and p.a in (1,2)",
            "[eq(test.{table}.c, test.key_outer.c) in(test.{table}.a, 1, 2) eq(test.{table}.b, 7)]",
            vec![vec!["1", "1", "7", "9"], vec!["1", "2", "7", "9"], vec!["2", "1", "7", "9"], vec!["2", "2", "7", "9"]],
        ),
        (
            "p.c=o.c and p.a=o.a and p.b in (7,8)",
            "[eq(test.{table}.a, test.key_outer.a) eq(test.{table}.c, test.key_outer.c) in(test.{table}.b, 7, 8)]",
            vec![vec!["1", "1", "7", "9"], vec!["2", "2", "7", "9"], vec!["3", "1", "8", "10"]],
        ),
        (
            "p.c=o.c and p.a=o.a",
            "[eq(test.{table}.a, test.key_outer.a)]",
            vec![vec!["1", "1", "7", "9"], vec!["2", "2", "7", "9"], vec!["3", "1", "8", "10"]],
        ),
        (
            "p.b=o.b and p.a=1 and p.c>o.c",
            "[eq(test.{table}.b, test.key_outer.b) eq(test.{table}.a, 1) gt(test.{table}.c, test.key_outer.c)]",
            vec![],
        ),
    ];
    let mut failures = Vec::new();
    for table in ["key_common", "key_index"] {
        session
            .run(&format!(
                "INSERT INTO {table} VALUES(1,7,9,1),(2,7,9,2),(1,8,10,3),(3,7,9,4)"
            ))
            .unwrap();
        for hint in ["INL_JOIN", "INL_HASH_JOIN"] {
            for (condition, expected_range, expected_rows) in &cases {
                let sql = format!("SELECT /*+ {hint}(p) */ p.v,o.a,o.b,o.c FROM key_outer o JOIN {table} p ON {condition}");
                let explanation = plan(&mut session, &format!("EXPLAIN {sql}"));
                let actual = explanation.lines().find_map(|line| {
                    line.split_once("range: decided by ")
                        .map(|(_, value)| value.split_once(", keep order:").unwrap().0)
                });
                let expected_range = expected_range.replace("{table}", table);
                if actual != Some(expected_range.as_str()) {
                    failures.push(format!(
                        "{sql}: expected range {expected_range}, got {actual:?}"
                    ));
                }
                match session.run(&format!("{sql} ORDER BY p.v,o.a,o.b,o.c")) {
                    Ok(result) => {
                        let actual = row_text(Ok(result));
                        if &actual != expected_rows {
                            failures.push(format!(
                                "{sql}: expected rows {expected_rows:?}, got {actual:?}"
                            ));
                        }
                    }
                    Err(error) => failures.push(format!("{sql}: {error:?}")),
                }
            }
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

#[test]
fn prepared_index_probe_fixed_prefix_changes_match_go() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE pc_outer(a INT NOT NULL,b INT NOT NULL,c INT NOT NULL)")
        .unwrap();
    session
        .run("INSERT INTO pc_outer VALUES(1,7,9),(2,7,9),(1,8,10)")
        .unwrap();
    session.run("CREATE TABLE pc_inner(a INT NOT NULL,b INT NOT NULL,c INT NOT NULL,v INT,PRIMARY KEY(a,b,c) CLUSTERED)").unwrap();
    session
        .run("INSERT INTO pc_inner VALUES(1,7,9,1),(2,7,9,2),(1,8,10,3),(3,7,9,4)")
        .unwrap();
    for (condition, runs) in [
        (
            "p.c=o.c and p.a=o.a and p.b=?",
            vec![
                (
                    7,
                    8,
                    "0",
                    vec![vec!["1", "1", "7", "9"], vec!["2", "2", "7", "9"]],
                ),
                (8, 9, "1", vec![vec!["3", "1", "8", "10"]]),
                (
                    7,
                    8,
                    "1",
                    vec![vec!["1", "1", "7", "9"], vec!["2", "2", "7", "9"]],
                ),
            ],
        ),
        (
            "p.c=o.c and p.a=o.a and p.b in (?,?)",
            vec![
                (
                    7,
                    8,
                    "0",
                    vec![
                        vec!["1", "1", "7", "9"],
                        vec!["2", "2", "7", "9"],
                        vec!["3", "1", "8", "10"],
                    ],
                ),
                (8, 9, "1", vec![vec!["3", "1", "8", "10"]]),
                (
                    7,
                    8,
                    "1",
                    vec![
                        vec!["1", "1", "7", "9"],
                        vec!["2", "2", "7", "9"],
                        vec!["3", "1", "8", "10"],
                    ],
                ),
            ],
        ),
        (
            "p.c=o.c and p.b=7 and p.a in (?,?)",
            vec![
                (
                    1,
                    2,
                    "0",
                    vec![
                        vec!["1", "1", "7", "9"],
                        vec!["1", "2", "7", "9"],
                        vec!["2", "1", "7", "9"],
                        vec!["2", "2", "7", "9"],
                    ],
                ),
                (
                    2,
                    3,
                    "1",
                    vec![
                        vec!["2", "1", "7", "9"],
                        vec!["2", "2", "7", "9"],
                        vec!["4", "1", "7", "9"],
                        vec!["4", "2", "7", "9"],
                    ],
                ),
                (
                    1,
                    1,
                    "0",
                    vec![vec!["1", "1", "7", "9"], vec!["1", "2", "7", "9"]],
                ),
                (
                    1,
                    2,
                    "0",
                    vec![
                        vec!["1", "1", "7", "9"],
                        vec!["1", "2", "7", "9"],
                        vec!["2", "1", "7", "9"],
                        vec!["2", "2", "7", "9"],
                    ],
                ),
            ],
        ),
    ] {
        session.run(&format!("PREPARE stmt FROM 'SELECT /*+ INL_JOIN(p) */ p.v,o.a,o.b,o.c FROM pc_outer o JOIN pc_inner p ON {condition} ORDER BY p.v,o.a,o.b,o.c'")).unwrap();
        for (x, y, hit, expected) in runs {
            session.run(&format!("SET @x={x},@y={y}")).unwrap();
            let args = if condition.contains("in") {
                "@x,@y"
            } else {
                "@x"
            };
            assert_eq!(
                row_text(session.run(&format!("EXECUTE stmt USING {args}"))),
                expected,
                "{condition}/{x}/{y}"
            );
            assert_eq!(
                row_text(session.run("SELECT @@last_plan_from_cache")),
                [[hit]],
                "{condition}/{x}/{y}"
            );
        }
        session.run("DEALLOCATE PREPARE stmt").unwrap();
    }
}

// Go tests/integrationtest/t/planner/core/indexjoin.test, issue #71737.
#[test]
fn index_probe_group_expression_preserves_whole_groups() {
    let mut session = Session::new();
    for sql in [
        "CREATE TABLE o(c1 INT,c2 INT)",
        "CREATE TABLE i(c1 INT,c2 INT,KEY(c2))",
        "INSERT INTO o VALUES (1,2)",
        "INSERT INTO i VALUES (1,2),(1,4)",
        "SET sql_mode=''",
        "SET tidb_index_join_batch_size=1",
    ] {
        session.run(sql).unwrap();
    }
    session
        .run(&format!(
            "INSERT INTO o VALUES {}",
            vec!["(0,1001)"; 200].join(",")
        ))
        .unwrap();
    session.run("INSERT INTO o VALUES (2,4)").unwrap();
    for hint in ["INL_JOIN", "INL_HASH_JOIN", "HASH_JOIN"] {
        let sql = format!(
            "SELECT /*+ {hint}(d) */ d.cnt FROM o JOIN \
            (SELECT c2,COUNT(*) cnt FROM i GROUP BY c2%2) d ON o.c2=d.c2 ORDER BY d.cnt"
        );
        assert_eq!(row_text(session.run(&sql)), vec![vec!["2"]], "{hint}");
    }
}

/// Go `TestIssue33231` (`partition_pruner.test`). Two outer rows with the
/// same key and different `c_str` rebuild OVERLAPPING inner ranges from the
/// `t1.c_str <= t2.c_str` comparison -- `[7, >= 'affectionate']` and
/// `[7, >= 'epic']`. Go's `buildKvRangesForIndexJoin` unions a task's ranges
/// (`ranger.UnionRanges`), so the inner row is read once; reading it once per
/// range joined each outer row with it twice. Both the common-handle table
/// path and a secondary-index path build these ranges.
#[test]
fn overlapping_compare_ranges_read_each_inner_row_once() {
    for (clustered, inner) in [("CLUSTERED", "PRIMARY"), ("NONCLUSTERED", "k")] {
        let mut session = Session::new();
        session
            .run("set @@session.tidb_partition_prune_mode = 'dynamic'")
            .unwrap();
        session
            .run(&format!(
                "create table t1 (c_int int, c_str varchar(40), primary key (c_int, c_str) \
                 {clustered}, key k (c_int, c_str)) partition by hash (c_int) partitions 4"
            ))
            .unwrap();
        session.run("create table t2 like t1").unwrap();
        session
            .run("insert into t1 values (6, 'beautiful curran'), (7, 'epic kalam'), (7, 'affectionate curie')")
            .unwrap();
        session
            .run("insert into t2 values (6, 'vigorous rhodes'), (7, 'sweet aryabhata')")
            .unwrap();
        let sql = format!(
            "select /*+ INL_JOIN(t2) use_index(t2, {inner}) */ * from t1, t2 \
             where t1.c_int = t2.c_int and t1.c_str <= t2.c_str and t2.c_int in (6, 7, 6) \
             order by t1.c_int, t1.c_str"
        );
        let explain = plan(&mut session, &format!("explain {sql}"));
        assert!(
            explain.contains("IndexJoin") && explain.contains("le(test.t1.c_str, test.t2.c_str)"),
            "{clustered}: the comparison must decide the inner ranges:\n{explain}"
        );
        assert_eq!(
            row_text(session.run(&sql)),
            vec![
                vec!["6", "beautiful curran", "6", "vigorous rhodes"],
                vec!["7", "affectionate curie", "7", "sweet aryabhata"],
                vec!["7", "epic kalam", "7", "sweet aryabhata"],
            ],
            "{clustered}:\n{explain}"
        );
    }
}

/// Go `TestIndexJoinEnumSetIssue19233` (`index_lookup_join.test`): a VARCHAR
/// outer key probing a unique ENUM or SET index is converted into the inner
/// column's type, and the batch's sort/dedup key must encode that ENUM/SET
/// datum by its member name as Go's `GetBytes` does; the string-only encoder
/// failed the statement with "a join key column has no comparable encoding".
#[test]
fn a_string_key_probing_an_enum_or_set_index_joins_by_member_name() {
    let mut session = Session::new();
    for sql in [
        "CREATE TABLE p1 (type enum('HOST_PORT') NOT NULL, UNIQUE KEY (type))",
        "CREATE TABLE p2 (type set('HOST_PORT') NOT NULL, UNIQUE KEY (type))",
        "CREATE TABLE i (objectType varchar(64) NOT NULL)",
        "insert into i values ('SWITCH'), ('HOST_PORT'), ('HOST_PORT')",
        "insert into p1 values ('HOST_PORT')",
        "insert into p2 values ('HOST_PORT')",
    ] {
        session.run(sql).unwrap();
    }
    for hint in ["INL_JOIN", "INL_HASH_JOIN"] {
        for inner in ["p1", "p2"] {
            let sql = format!(
                "select /*+ {hint}({inner}) */ * from i, {inner} where i.objectType = {inner}.type"
            );
            let explain = plan(&mut session, &format!("explain format='brief' {sql}"));
            assert!(explain.contains("Index"), "{sql}:\n{explain}");
            assert_eq!(
                row_text(session.run(&sql)),
                vec![vec!["HOST_PORT", "HOST_PORT"], vec!["HOST_PORT", "HOST_PORT"]],
                "{sql}"
            );
        }
    }
}

/// Go `HashChunkRow` hashes DATE/DATETIME/TIMESTAMP keys by their packed
/// value and TIME keys by nanoseconds. The key classes refused them, so an
/// index join on a TIMESTAMP key built its probe over an empty key list and
/// panicked indexing it.
#[test]
fn an_index_join_probes_temporal_keys() {
    let mut session = Session::new();
    for sql in [
        "create table o (ts timestamp, dt datetime(3), d date, tm time)",
        "create table i (ts timestamp, dt datetime, d date, tm time(2), v int, \
         key (ts), key (dt), key (d), key (tm))",
        "insert into o values ('2025-03-27 00:56:13', '2025-03-27 00:56:13.000', \
         '2025-03-27', '10:00:00')",
        "insert into i values ('2025-03-27 00:56:13', '2025-03-27 00:56:13', \
         '2025-03-27', '10:00:00.00', 1), ('2024-01-01 00:00:00', '2024-01-01 00:00:00', \
         '2024-01-01', '11:00:00', 2)",
    ] {
        session.run(sql).unwrap();
    }
    for column in ["ts", "dt", "d", "tm"] {
        let sql = format!("select /*+ inl_join(i) */ i.v from o join i on o.{column} = i.{column}");
        let explain = plan(&mut session, &format!("explain format='brief' {sql}"));
        assert!(explain.contains("IndexJoin"), "{sql}:\n{explain}");
        assert_eq!(row_text(session.run(&sql)), vec![vec!["1"]], "{sql}");
    }
}

/// Go `AddRecord` truncates a prefix column of a clustered primary key
/// before encoding the handle (`TruncateIndexValues`), and `getIndexValues`
/// refuses a point get on such a key. The full value was encoded, so an
/// index-join lookup by the truncated handle found nothing.
#[test]
fn a_prefix_clustered_primary_key_stores_and_probes_the_truncated_handle() {
    let mut session = Session::new();
    for sql in [
        "create table u (a int, b char(10), c varchar(255), primary key (c(5)) clustered)",
        "insert into u values (20301, 'Charlie', 'aaaaaaa')",
    ] {
        session.run(sql).unwrap();
    }
    let joined = "select /*+ inl_join(t2) */ t1.a, t2.c from u t1 join u t2 on t1.c = t2.c";
    assert!(plan(&mut session, &format!("explain format='brief' {joined}")).contains("IndexJoin"));
    assert_eq!(row_text(session.run(joined)), vec![vec!["20301", "aaaaaaa"]]);
    let point = "select a from u where c = 'aaaaaaa'";
    let explain = plan(&mut session, &format!("explain format='brief' {point}"));
    assert!(!explain.contains("Point_Get"), "{explain}");
    assert!(explain.contains("[\"aaaaa\",\"aaaaa\"]"), "{explain}");
    assert_eq!(row_text(session.run(point)), vec![vec!["20301"]]);
    assert!(row_text(session.run("select a from u where c = 'aaaaabb'")).is_empty());
    // Go reports the values the handle holds.
    let duplicate = session
        .run("insert into u values (1, 'x', 'aaaaabb')")
        .unwrap_err()
        .to_string();
    assert!(duplicate.contains("Duplicate entry 'aaaaa' for key 'u.PRIMARY'"), "{duplicate}");
}

/// Go appends the integer handle to a non-unique index's columns, so a join
/// comparison on the handle becomes the probe's per-row range on that slot,
/// built with the target column's own type (`TargetCol.RetType`). The probe
/// read the type from the index's declared columns, found none for the
/// handle slot and answered every probe with an empty range.
#[test]
fn an_index_join_compares_the_appended_handle_slot() {
    let mut session = Session::new();
    for sql in [
        "create table k (a int, pk int primary key, index(a))",
        "create table t (a int, pk int primary key, index(a))",
        "insert into k values (0,8),(0,23),(1,21),(1,33),(1,52),(2,17),(2,34),(2,39),(2,40),\
         (2,66),(2,67),(3,9),(3,25),(3,41),(3,48),(4,4),(4,11),(4,15),(4,26),(4,27),(4,31),\
         (4,35),(4,45),(4,47),(4,49)",
        "insert into t values (3,4),(3,5),(3,27),(3,29),(3,57),(3,58),(3,79),(3,84),(3,92),(3,95)",
    ] {
        session.run(sql).unwrap();
    }
    for hint in ["inl_join", "inl_hash_join"] {
        let sql = format!(
            "select /*+ {hint}(t) */ count(*) from k left join t on k.a = t.a and k.pk > t.pk"
        );
        let explain = plan(&mut session, &format!("explain format='brief' {sql}"));
        assert!(explain.contains("gt(test.k.pk, test.t.pk)]"), "{sql}:\n{explain}");
        assert_eq!(row_text(session.run(&sql)), vec![vec!["33"]], "{sql}");
    }
}

/// Go `ExtractTableAlias` gives an alias whose names carry no database, a
/// derived table's, the current database -- the one the hint's own table
/// defaulted to -- so `INL_JOIN(tmp)` matches the aggregated subquery and
/// drives an index join through it instead of warning that no table matches.
#[test]
fn an_index_join_hint_matches_a_derived_table_alias() {
    let mut session = Session::new();
    for sql in [
        "create table t (a int, b int, index idx(a, b))",
        "create table t1 (a int, b int, index idx(a, b))",
        "insert into t values (1, 1), (1, 2), (2, 3)",
        "insert into t1 values (1, 10), (3, 30)",
    ] {
        session.run(sql).unwrap();
    }
    let sql = "select /*+ INL_JOIN(tmp) */ * from (select a, count(b) from t group by a) tmp, t1 \
               where tmp.a = t1.a";
    let explain = plan(&mut session, &format!("explain format='brief' {sql}"));
    assert!(explain.contains("IndexJoin"), "{explain}");
    assert!(explain.contains("range: decided by [eq(test.t.a, test.t1.a)]"), "{explain}");
    assert_eq!(row_text(session.run("show warnings")), Vec::<Vec<String>>::new());
    assert_eq!(row_text(session.run(sql)), vec![vec!["1", "2", "1", "10"]]);
}

/// Go `constructDS2TableScanTask` / `constructDS2IndexScanTask`: "If the inner
/// task need to keep order, the partition table reader can't satisfy it."
/// A stream aggregate over a partitioned inner table therefore cannot take
/// the index-join probe, and the hinted join falls back to a hash join.
#[test]
fn a_partitioned_inner_table_cannot_keep_order_for_an_index_join() {
    let mut session = Session::new();
    for sql in [
        "create table p (a int, b int, c int, key ia(a)) partition by hash(c) partitions 2",
        "create table q (a int, b int, c int, key ia(a))",
        "create table o (a int, c int)",
        "insert into p values (1, 1, 1), (1, 3, 2), (11, 2, 3)",
        "insert into q values (1, 1, 1), (1, 3, 2), (11, 2, 3)",
        "insert into o values (1, 5), (11, 6)",
    ] {
        session.run(sql).unwrap();
    }
    let query = |table: &str| {
        format!(
            "select /*+ inl_join(s) */ o.c, s.m from o join (select /*+ stream_agg() */ a, \
             max(b) m from {table} group by a) s on o.a = s.a order by o.c"
        )
    };
    let explain = plan(&mut session, &format!("explain format='brief' {}", query("p")));
    assert!(explain.contains("HashJoin") && !explain.contains("IndexJoin"), "{explain}");
    assert_eq!(
        row_text(session.run("show warnings")),
        vec![vec![
            "Warning",
            "1815",
            "Optimizer Hint /*+ INL_JOIN(s) */ or /*+ TIDB_INLJ(s) */ is inapplicable"
        ]]
    );
    assert_eq!(row_text(session.run(&query("p"))), vec![vec!["5", "3"], vec!["6", "2"]]);
    let explain = plan(&mut session, &format!("explain format='brief' {}", query("q")));
    assert!(explain.contains("IndexJoin"), "{explain}");
}

/// Go `ExtractTableAlias` gives an alias its own plan's query block -- the
/// subquery's for `t2` below -- and its parent's only for a named derived
/// table (`PlannerSelectBlockAsName`). Every alias took the join's block, so
/// `TIDB_INLJ(t2@sel_2)` never matched the semi join's inner side.
#[test]
fn a_query_block_qualified_join_hint_matches_the_subquery_table() {
    let mut session = Session::new();
    for sql in [
        "set @@tidb_opt_insubq_to_join_and_agg = 0",
        "create table t1 (a int not null, b int not null, key a(a))",
        "create table t2 (a int not null, b int not null, key a(a))",
    ] {
        session.run(sql).unwrap();
    }
    let explain = plan(
        &mut session,
        "explain format='brief' select /*+ TIDB_INLJ(t2@sel_2) */ * from t1 where t1.a in (select t2.a from t2)",
    );
    assert!(
        explain.contains("IndexJoin") && explain.contains("decided by [eq(test.t2.a, test.t1.a)]"),
        "{explain}"
    );
    assert_eq!(row_text(session.run("show warnings")), Vec::<Vec<String>>::new());
    // A named derived table belongs to the block around it.
    let explain = plan(
        &mut session,
        "explain format='brief' select /*+ INL_JOIN(dt) */ * from t1 join (select a from t2) dt on t1.a = dt.a",
    );
    assert!(explain.contains("IndexJoin"), "{explain}");
    assert_eq!(row_text(session.run("show warnings")), Vec::<Vec<String>>::new());
}

/// `r/planner/core/casetest/physicalplantest/physical_plan.result`'s
/// decorrelated scalar `sum`: an IndexHashJoin whose inner StreamAgg reads a
/// heap table through an IndexLookUp. That reader keeps `_tidb_rowid` in its
/// schema (the Projection above it prunes the column), so the lookup must
/// report each row's handle; it refused the column and the statement failed.
#[test]
fn an_inner_index_lookup_over_a_heap_table_reports_the_row_handle() {
    let mut session = Session::new();
    for sql in [
        "create table ta(id int, code int, name varchar(20), index idx_ta_id(id), index idx_ta_name(name), index idx_ta_code(code))",
        "create table tb(id int, code int, name varchar(20), index idx_tb_id(id), index idx_tb_name(name))",
        "insert into ta values (1, 10, 'chad9991'), (2, 20, 'chad9992'), (3, 30, 'x')",
        "insert into tb values (1, 5, 'a'), (1, 7, 'b'), (2, 9, 'c')",
    ] {
        session.run(sql).unwrap();
    }
    let sql = "SELECT ta.NAME, (SELECT sum(tb.CODE) FROM tb WHERE ta.id = tb.id) tb_sum_code \
        FROM ta WHERE ta.NAME LIKE 'chad999%'";
    let explain = plan(&mut session, &format!("explain format='brief' {sql}"));
    assert!(
        explain.contains("IndexHashJoin") && explain.contains("inner:StreamAgg"),
        "{explain}"
    );
    let mut rows = row_text(session.run(sql));
    rows.sort();
    assert_eq!(rows, vec![vec!["chad9991", "12"], vec!["chad9992", "9"]]);
}

/// Go `getTableScanPenalty` charges no full-range penalty to a scan with
/// `RangeInfo`, which `constructDS2TableScanTask` sets on every index-join
/// probe. The integer-handle probe carries the full integer range as a
/// placeholder (rebuilt per outer row), so it was priced as a risky full scan
/// of 1000 extra rows and lost to a secondary index over the same column.
/// Go (oracle, verbose): the probe TableRangeScan costs 162.80.
#[test]
fn an_integer_handle_probe_scan_pays_no_full_scan_penalty() {
    let mut session = Session::new();
    session
        .run("create table t (a int primary key, b int, index idx(a))")
        .unwrap();
    let explain = plan(
        &mut session,
        "explain format='brief' select /*+ TIDB_INLJ(t2) */ * from t t1, t t2 where t1.a = t2.a",
    );
    assert!(
        explain.contains("inner:TableReader") && explain.contains("TableRangeScan"),
        "{explain}"
    );
    assert!(!explain.contains("idx(a)"), "{explain}");
}

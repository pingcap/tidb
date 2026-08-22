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

//! Go `pkg/planner/core/rule/rule_partition_processor.go`, the rule that
//! decides WHICH partitions a plan reads and what the plan looks like once it
//! has decided.
//!
//! Every case here is one recorded TiDB answer from
//! `tests/integrationtest/r/**` that this tier used to get wrong, and each
//! names the Go function whose behaviour it pins.

#![cfg(test)]

use crate::tests_support::row_text;
use crate::Session;

/// Every plan line, joined by tabs, so a test can assert over the whole
/// printed tree rather than one column of it.
fn plan_lines(session: &mut Session, sql: &str) -> Vec<String> {
    row_text(session.run(sql))
        .into_iter()
        .map(|row| row.join("\t"))
        .collect()
}

/// The partitions an `EXPLAIN` names, in plan order.
///
/// Read per CELL rather than per line: the access object is one column
/// (`table:t2, partition:p1`), so splitting a tab-joined line on `", "`
/// would carry the next column into the name.
fn partitions(session: &mut Session, sql: &str) -> Vec<String> {
    row_text(session.run(sql))
        .into_iter()
        .filter_map(|row| {
            row.iter()
                .find_map(|cell| cell.split(", ").find_map(|part| part.strip_prefix("partition:")))
                .map(str::to_owned)
        })
        .collect()
}

/// Go `PartitionProcessor.prune` opens with `applyPredicateSimplification`,
/// whose first act is `expression.PushDownNot`, and the rule's own comment
/// says why: "When we build range from ds.AllConds, the condition like 'not
/// (a != 1)' would not be handled so we need to convert it to 'a = 1', which
/// can be handled when building range."
///
/// `not (a < 5)` therefore prunes exactly as `a >= 5` does. Without the
/// rewrite the ranger reads no range from the `NOT` at all, which this tier
/// takes as "prune nothing" -- and the `values less than (0)` partition,
/// which provably holds no row with `a >= 5`, stayed in the plan.
///
/// Recorded by TiDB in
/// `tests/integrationtest/r/planner/core/partition_pruner.result`:
///
/// ```text
/// set @@tidb_partition_prune_mode='static';
/// explain format='plan_tree' select * from t2 where not (a < 5);
/// PartitionUnion
/// ├─TableReader ... TableFullScan  table:t2, partition:p1
/// └─TableReader ... TableFullScan  table:t2, partition:p2
/// ```
///
/// MUTATION: drop the `push_down_not` call in `pruned_partition_ids` and the
/// plan names p0, p1 and p2.
#[test]
fn a_negated_comparison_prunes_like_its_opposite() {
    let mut session = Session::new();
    session
        .run("SET @@tidb_partition_prune_mode = 'static'")
        .unwrap();
    session
        .run(
            "CREATE TABLE t2 (a INT) PARTITION BY RANGE (a) (\
             PARTITION p0 VALUES LESS THAN (0), \
             PARTITION p1 VALUES LESS THAN (10), \
             PARTITION p2 VALUES LESS THAN (20))",
        )
        .unwrap();

    let negated = partitions(
        &mut session,
        "EXPLAIN FORMAT='brief' SELECT * FROM t2 WHERE NOT (a < 5)",
    );
    assert_eq!(negated, vec!["p1", "p2"], "not (a < 5) is a >= 5");
    // The control: the rewrite's own target, which was already right.
    assert_eq!(
        partitions(
            &mut session,
            "EXPLAIN FORMAT='brief' SELECT * FROM t2 WHERE a >= 5"
        ),
        negated,
        "the rewrite must reach the same partitions as the rewritten form"
    );

    // De Morgan, the other half of Go's `pushNotAcrossExpr`: `not (a < 0 or
    // a >= 10)` is `a >= 0 and a < 10`, which is p1 alone. Asserted against
    // the rewritten form as well, so the case pins the REWRITE rather than
    // the ranger's absolute answer for that predicate.
    let de_morgan = partitions(
        &mut session,
        "EXPLAIN FORMAT='brief' SELECT * FROM t2 WHERE NOT (a < 0 OR a >= 10)",
    );
    assert_eq!(de_morgan, vec!["p1"], "NOT over OR is De Morgan");
    assert_eq!(
        partitions(
            &mut session,
            "EXPLAIN FORMAT='brief' SELECT * FROM t2 WHERE a >= 0 AND a < 10"
        ),
        de_morgan,
    );

    // And the rows never depended on any of this: the `WHERE` is still
    // evaluated above the scan, so pruning may only ever change WHICH
    // partitions were opened.
    session
        .run("INSERT INTO t2 VALUES (-1), (3), (7), (15)")
        .unwrap();
    assert_eq!(
        row_text(session.run("SELECT a FROM t2 WHERE NOT (a < 5) ORDER BY a")),
        vec![vec!["7".to_owned()], vec!["15".to_owned()]]
    );
}

/// Go `makeUnionAllChildren`'s `len(children) == 0` branch: a static-mode
/// read whose pruning left NO partition becomes
/// `LogicalTableDual{RowCount: 0}`, not a scan over an empty partition set.
///
/// Recorded by TiDB in
/// `tests/integrationtest/r/planner/core/casetest/partition/integration_partition.result`
/// for a `LIST (b)` table whose only values are `0..5`:
///
/// ```text
/// explain format='plan_tree' select a from tlist use index () where b > 10 order by b limit 10;
/// TableDual  root    rows:0
/// ```
///
/// MUTATION: restore the `partition_union` call for the empty set and the
/// plan prints `TableRangeScan ... range:(10,+inf]` over a table no partition
/// of which can hold such a row.
#[test]
fn pruning_every_partition_away_is_a_table_dual() {
    let mut session = Session::new();
    session
        .run("SET @@tidb_partition_prune_mode = 'static'")
        .unwrap();
    session
        .run(
            "CREATE TABLE tlist (a INT, b INT, c INT, INDEX ia(a), PRIMARY KEY (b) CLUSTERED) \
             PARTITION BY LIST (b) (\
             PARTITION p0 VALUES IN (0, 1, 2), \
             PARTITION p1 VALUES IN (3, 4, 5))",
        )
        .unwrap();
    session.run("INSERT INTO tlist VALUES (1, 1, 1), (2, 4, 2)").unwrap();

    let plan = plan_lines(
        &mut session,
        "EXPLAIN FORMAT='brief' SELECT a FROM tlist USE INDEX () WHERE b > 10 ORDER BY b LIMIT 10",
    );
    assert!(
        plan.iter().any(|line| line.contains("TableDual")),
        "an all-pruned read is a dual: {plan:?}"
    );
    assert!(
        !plan.iter().any(|line| line.contains("Scan")),
        "no partition survives, so nothing is scanned: {plan:?}"
    );

    // The control, in the SAME statement shape: a predicate some partition
    // CAN satisfy still scans it, and only it.
    assert_eq!(
        partitions(
            &mut session,
            "EXPLAIN FORMAT='brief' SELECT a FROM tlist USE INDEX () WHERE b > 3 \
             ORDER BY b LIMIT 10"
        ),
        vec!["p1"],
    );
    assert_eq!(
        row_text(session.run("SELECT a FROM tlist WHERE b > 10")),
        Vec::<Vec<String>>::new()
    );
}

/// Go's point-get conversion for a heap table's `_tidb_rowid`
/// (`pkg/planner/core/find_best_task.go`):
///
/// ```go
/// // Partition table can't use `_tidb_rowid` to generate PointGet Plan
/// // unless one partition is explicitly specified.
/// if canConvertPointGet && path.IsIntHandlePath &&
///     !ds.Table.Meta().PKIsHandle && len(ds.PartitionNames) != 1 {
///     canConvertPointGet = false
/// }
/// ```
///
/// The test is on the WRITTEN `PARTITION (...)` list's length. Recorded by
/// TiDB in `tests/integrationtest/r/planner/core/integration_partition.result`
/// over `create table t(id int) PARTITION BY HASH(id) partitions 5`: the bare
/// form and `partition(p0,p1)` are `TableRangeScan`, `partition(p0)` and
/// `partition(p1)` are `Point_Get table:t, partition:pN  handle:1`.
///
/// MUTATION: drop the `single_named_partition` disjunct in `try_point_get`
/// and both single-partition forms fall back to a range scan.
#[test]
fn a_row_id_point_get_needs_exactly_one_named_partition() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE t (id INT) PARTITION BY HASH(id) PARTITIONS 5")
        .unwrap();
    session.run("INSERT INTO t VALUES (0), (0)").unwrap();

    for (from, expected_point) in [
        ("t", None),
        // Go `PointGetPlan.AccessObject` names the ONE partition the plan
        // resolved, which for an explicit list of one is that partition.
        ("t PARTITION(p0)", Some("partition:p0")),
        ("t PARTITION(p1)", Some("partition:p1")),
        ("t PARTITION(p0,p1)", None),
    ] {
        let plan = plan_lines(
            &mut session,
            &format!("EXPLAIN FORMAT='brief' SELECT *,_tidb_rowid FROM {from} WHERE _tidb_rowid=1"),
        );
        match expected_point {
            Some(named) => assert!(
                plan.iter()
                    .any(|line| line.contains("Point_Get") && line.contains(named)),
                "FROM {from} planned {plan:?}"
            ),
            None => assert!(
                !plan.iter().any(|line| line.contains("Point_Get")),
                "FROM {from} planned {plan:?}"
            ),
        }
    }

    // The rows are the same either way -- `_tidb_rowid` 1 lives in whichever
    // partition the first row routed to, and only that one answers.
    assert_eq!(
        row_text(session.run("SELECT id,_tidb_rowid FROM t PARTITION(p0) WHERE _tidb_rowid=1")),
        vec![vec!["0".to_owned(), "1".to_owned()]]
    );
    assert_eq!(
        row_text(session.run("SELECT id,_tidb_rowid FROM t PARTITION(p1) WHERE _tidb_rowid=1")),
        Vec::<Vec<String>>::new()
    );
}

/// Go's `PartitionProcessor.rewriteDataSource` walks the WHOLE logical plan
/// and divides EVERY `DataSource` it finds, so a partitioned table read as a
/// JOIN leaf is fanned out under a `PartitionUnion` exactly as a single-table
/// `SELECT`'s source is.
///
/// Recorded by TiDB in
/// `tests/integrationtest/r/planner/core/casetest/partition/partition_pruner.result`
/// (issue 42135), `@@tidb_partition_prune_mode='static'`:
///
/// ```text
/// └─PartitionUnion(Probe)
///   ├─TableReader ... TableFullScan  table:tx2, partition:p1
///   └─TableReader ... TableFullScan  table:tx2, partition:p2
/// ```
///
/// MUTATION: drop the leaf fan-out in `build_from` and the plan prints one
/// partition-less `TableFullScan table:tx2`.
#[test]
fn a_partitioned_join_leaf_fans_out_under_static_pruning() {
    let mut session = Session::new();
    session
        .run("SET @@tidb_partition_prune_mode = 'static'")
        .unwrap();
    session
        .run("CREATE TABLE tx1 (ID VARCHAR(13), a VARCHAR(13), ltype INT(5) NOT NULL)")
        .unwrap();
    session
        .run(
            "CREATE TABLE tx2 (ID VARCHAR(13), rid VARCHAR(12), ltype INT(5) NOT NULL) \
             PARTITION BY LIST (ltype) (\
             PARTITION p1 VALUES IN (501), PARTITION p2 VALUES IN (502))",
        )
        .unwrap();
    session.run("INSERT INTO tx1 VALUES (1,1,501)").unwrap();
    session.run("INSERT INTO tx2 VALUES (1,1,501)").unwrap();

    let plan = plan_lines(
        &mut session,
        "EXPLAIN FORMAT='brief' SELECT * FROM tx1 INNER JOIN tx2 \
         ON tx1.ID=tx2.ID AND tx1.ltype=tx2.ltype WHERE tx2.rid='1'",
    );
    assert!(
        plan.iter().any(|line| line.contains("PartitionUnion")),
        "the partitioned leaf fans out: {plan:?}"
    );
    let named = partitions(
        &mut session,
        "EXPLAIN FORMAT='brief' SELECT * FROM tx1 INNER JOIN tx2 \
         ON tx1.ID=tx2.ID AND tx1.ltype=tx2.ltype WHERE tx2.rid='1'",
    );
    assert_eq!(named, vec!["p1", "p2"], "both partitions are named: {plan:?}");

    // The rows are unaffected: the leaf reads the same partitions either way.
    assert_eq!(
        row_text(session.run(
            "SELECT tx1.ID FROM tx1 INNER JOIN tx2 \
             ON tx1.ID=tx2.ID AND tx1.ltype=tx2.ltype WHERE tx2.rid='1'"
        )),
        vec![vec!["1".to_owned()]]
    );
}

/// Go's static mode gives every surviving partition its own `DataSource`
/// (`makeUnionAllChildren`), so a `Batch_Point_Get` over several partitions
/// is built once PER partition and each names the one it reads.
///
/// Recorded by TiDB in
/// `tests/integrationtest/r/planner/core/casetest/partition/partition_pruner.result`
/// (issue 59827) over `partition by key(b) partitions 3`:
///
/// ```text
/// explain format = 'brief' select * from t where b in (1,2);
/// PartitionUnion       3.00  root
/// ├─Batch_Point_Get    2.00  root  table:t, partition:p1, index:PRIMARY(b)
/// └─Batch_Point_Get    1.00  root  table:t, partition:p2, index:PRIMARY(b)
/// ```
///
/// MUTATION: blank the access object and delegate to `partition_union` again
/// -- which fans out SCANS only -- and one partition-less `Batch_Point_Get`
/// is printed.
#[test]
fn a_static_mode_batch_point_get_names_every_partition_it_reads() {
    let mut session = Session::new();
    session
        .run("SET @@tidb_partition_prune_mode = 'static'")
        .unwrap();
    session
        .run(
            "CREATE TABLE t (a VARCHAR(255), b INT PRIMARY KEY NONCLUSTERED, KEY (a)) \
             PARTITION BY KEY(b) PARTITIONS 3",
        )
        .unwrap();
    session
        .run("INSERT INTO t VALUES ('Ab',1),('abc',2),('BC',3),('AC',4),('BA',5),('cda',6)")
        .unwrap();

    let plan = plan_lines(
        &mut session,
        "EXPLAIN FORMAT='brief' SELECT * FROM t WHERE b IN (1,2)",
    );
    let batch: Vec<&String> = plan
        .iter()
        .filter(|line| line.contains("Batch_Point_Get"))
        .collect();
    assert_eq!(batch.len(), 2, "one per partition: {plan:?}");
    assert!(
        batch.iter().all(|line| line.contains("partition:")),
        "each names its own partition: {plan:?}"
    );

    let mut rows = row_text(session.run("SELECT a, b FROM t WHERE b IN (1,2)"));
    rows.sort();
    assert_eq!(
        rows,
        vec![
            vec!["Ab".to_owned(), "1".to_owned()],
            vec!["abc".to_owned(), "2".to_owned()],
        ]
    );
}

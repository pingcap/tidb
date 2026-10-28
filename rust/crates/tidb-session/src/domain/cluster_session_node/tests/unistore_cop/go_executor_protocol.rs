// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Go's executor protocols as `TRACE` shows them, captured from Go TiDB on
//! the same statements: when a subquery runs, how often an operator asks its
//! child for rows, and which executor opens where.

use super::*;

/// Go `getInsertColExpr` evaluates an uncorrelated scalar subquery in a
/// VALUES list while it plans the INSERT (`handleScalarSubquery`'s
/// `EvalSubqueryFirstRow`): its executors run under `executor.EvalSubQuery`
/// beside `planner.Optimize`, over a `MaxOneRow`, before `session.runStmt`.
/// IN and ANY build a semi join over the dual instead, which Go refuses
/// (issue 30626); an uncorrelated EXISTS is evaluated like a scalar subquery.
#[test]
fn insert_values_subqueries_run_while_the_insert_is_planned() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(157)).unwrap();
    for sql in [
        "CREATE TABLE test.sq (id INT PRIMARY KEY, v INT, k INT)",
        "INSERT INTO test.sq VALUES (1, 1, 1), (2, 101, 2), (3, 3, 3)",
    ] {
        rows(&mut session, sql);
    }
    let tree = tidb_trace_tree(
        &mut session,
        "INSERT INTO test.sq VALUES (10, (SELECT MAX(v) FROM test.sq WHERE id < 5), 1)",
    );
    assert_eq!(
        tree[..12],
        [
            "trace",
            "  └─session.ExecuteStmt",
            "    ├─executor.Compile",
            "    │ ├─planner.Preprocess",
            "    │ ├─planner.Optimize",
            "    │ └─executor.EvalSubQuery",
            "    │   ├─TableReaderExecutor.Open",
            "    │   │ └─distsql.Select",
            "    │   └─*executor.MaxOneRowExec.Next",
            "    │     ├─*aggregate.StreamAggExec.Next",
            "    │     │ ├─*sortexec.TopNExec.Next",
            "    │     │ │ ├─*executor.TableReaderExecutor.Next",
        ],
        "{tree:#?}"
    );
    assert_eq!(
        tree[12..20],
        [
            "    │     │ │ └─*executor.TableReaderExecutor.Next",
            "    │     │ └─*sortexec.TopNExec.Next",
            "    │     └─*aggregate.StreamAggExec.Next",
            "    └─session.runStmt",
            "      ├─executor.handleNoDelayExecutor",
            "      │ └─*executor.InsertExec.Next",
            "      │   └─table.AddRecord",
            "      └─session.CommitTxn",
        ],
        "{tree:#?}"
    );
    assert_eq!(
        displayed(rows(&mut session, "SELECT v FROM test.sq WHERE id = 10")),
        [["101"]]
    );
    // Go evaluates an uncorrelated EXISTS while it plans, as it does a
    // scalar subquery.
    for sql in [
        "INSERT INTO test.sq VALUES (12, EXISTS (SELECT id FROM test.sq), 1)",
        "INSERT INTO test.sq VALUES (14, EXISTS (SELECT id FROM test.sq WHERE id < 3), 1)",
    ] {
        session.node_conn().execute_write(sql).unwrap();
    }
    assert_eq!(
        displayed(rows(
            &mut session,
            "SELECT v FROM test.sq WHERE id IN (12, 14)"
        )),
        [["1"], ["1"]]
    );
    for sql in [
        "INSERT INTO test.sq VALUES (11, 1 IN (SELECT id FROM test.sq), 1)",
        "INSERT INTO test.sq VALUES (13, 2 > ANY (SELECT id FROM test.sq), 1)",
    ] {
        let error = session.node_conn().execute_write(sql).expect_err(sql);
        assert_eq!(
            (error.code, error.message.as_str()),
            (
                1105,
                "Insert's SET operation or VALUES_LIST doesn't support complex subqueries now"
            ),
            "{sql}"
        );
    }
}

/// Go's `SelectionExec.Next` and `NestedLoopApplyExec.Next` keep no
/// end-of-input state: a call after the child ran dry asks it again. Per
/// outer row Go's Apply therefore calls its inner Selection twice, the second
/// call asking `MaxOneRowExec` once more, and its last `Next` asks the outer
/// reader again.
#[test]
fn apply_and_selection_ask_an_exhausted_child_again() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(158)).unwrap();
    for sql in [
        "CREATE TABLE test.ap (id INT PRIMARY KEY, v INT, k INT)",
        "INSERT INTO test.ap VALUES (0, 0, 0), (1, 101, 1), (2, 2, 2), (3, 3, 3)",
    ] {
        rows(&mut session, sql);
    }
    let tree = tidb_trace_tree(
        &mut session,
        "SELECT id FROM test.ap t1 WHERE t1.id < 3 AND t1.v >= \
         (SELECT /*+ NO_DECORRELATE() */ MAX(v) FROM test.ap t2 WHERE t2.id = t1.id)",
    );
    let inner = [
        "  │   ├─*executor.SelectionExec.Next",
        "  │   │ ├─*executor.MaxOneRowExec.Next",
        "  │   │ │ ├─*aggregate.StreamAggExec.Next",
        "  │   │ │ │ ├─*sortexec.TopNExec.Next",
        "  │   │ │ │ │ ├─*executor.TableReaderExecutor.Next",
        "  │   │ │ │ │ └─*executor.TableReaderExecutor.Next",
        "  │   │ │ │ └─*sortexec.TopNExec.Next",
        "  │   │ │ └─*aggregate.StreamAggExec.Next",
        "  │   │ └─*executor.MaxOneRowExec.Next",
        "  │   ├─*executor.SelectionExec.Next",
        "  │   │ └─*executor.MaxOneRowExec.Next",
    ];
    let per_row = tree
        .windows(inner.len())
        .filter(|window| *window == inner)
        .count();
    assert_eq!(per_row, 3, "{tree:#?}");
    assert_eq!(
        tree[tree.len() - 4..],
        [
            "  │   └─*executor.TableReaderExecutor.Next",
            "  └─*executor.ProjectionExec.Next",
            "    └─*join.NestedLoopApplyExec.Next",
            "      └─*executor.TableReaderExecutor.Next",
        ],
        "{tree:#?}"
    );
}

/// Go's `UnionScanExec` over a table the open transaction wrote: the reader
/// below reads the snapshot through the coprocessor -- an index reader sends
/// its request while the union scan opens -- and the buffered rows come from
/// the mem reader `buildMem*Reader` builds; `getSnapshotRow` keeps asking the
/// drained reader each time a row is merged.
#[test]
fn union_scan_merges_the_snapshot_reader_with_the_membuffer() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(159)).unwrap();
    for sql in [
        "CREATE TABLE test.us (id INT PRIMARY KEY, v INT, k INT, KEY ik (k))",
        "INSERT INTO test.us VALUES (1, 1, 1), (2, 2, 2), (3, 3, 3)",
    ] {
        rows(&mut session, sql);
    }
    session.node_conn().control_transaction("BEGIN").unwrap();
    for sql in [
        "INSERT INTO test.us VALUES (7, 7, 1)",
        "UPDATE test.us SET v = 20 WHERE id = 2",
        "DELETE FROM test.us WHERE id = 3",
    ] {
        session.node_conn().execute_write(sql).unwrap();
    }
    let tree = tidb_trace_tree(
        &mut session,
        "SELECT * FROM test.us IGNORE INDEX (ik) WHERE v > 0",
    );
    assert_eq!(
        tree[5..],
        [
            "  │ └─session.runStmt",
            "  │   └─UnionScanExec.Open",
            "  │     ├─TableReaderExecutor.Open",
            "  │     │ └─distsql.Select",
            "  │     └─buildMemTableReader",
            // One snapshot chunk survives as row 1; each of the two buffered
            // rows (2 and 7) and the end of input then asks the reader again.
            "  ├─*executor.UnionScanExec.Next",
            "  │ ├─*executor.TableReaderExecutor.Next",
            "  │ ├─*executor.TableReaderExecutor.Next",
            "  │ ├─*executor.TableReaderExecutor.Next",
            "  │ └─*executor.TableReaderExecutor.Next",
            "  └─*executor.UnionScanExec.Next",
            "    └─*executor.TableReaderExecutor.Next",
        ],
        "{tree:#?}"
    );
    let tree = tidb_trace_tree(
        &mut session,
        "SELECT k FROM test.us USE INDEX (ik) WHERE k < 3",
    );
    assert_eq!(
        tree[5..9],
        [
            "  │ └─session.runStmt",
            "  │   └─UnionScanExec.Open",
            "  │     ├─distsql.Select",
            "  │     └─buildMemIndexReader",
        ],
        "{tree:#?}"
    );
    // Through the index too, the updated row is the buffered one (its index
    // entry is written again, untouched), the deleted row is gone and the
    // inserted row is merged in.
    assert_eq!(
        displayed(rows(
            &mut session,
            "SELECT * FROM test.us USE INDEX (ik) WHERE k < 5 ORDER BY id",
        )),
        [["1", "1", "1"], ["2", "20", "2"], ["7", "7", "1"]]
    );
    assert_eq!(
        displayed(rows(
            &mut session,
            "SELECT id FROM test.us USE INDEX (ik) WHERE k < 5 ORDER BY id",
        )),
        [["1"], ["2"], ["7"]]
    );
    session.node_conn().control_transaction("ROLLBACK").unwrap();
}

/// Go plans an index merge over a table the open transaction wrote under a
/// `UnionScanExec`: the merge reader reads the snapshot through the
/// coprocessor, the mem reader walks the buffered rows, and a global index's
/// handles keep their partition id on the way to the final table lookup.
/// Captured on the Go oracle (`globalindex/mem_index_merge_non_unique`).
#[test]
fn index_merge_over_a_global_index_sees_the_snapshot_and_the_membuffer() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(160)).unwrap();
    for sql in [
        "CREATE TABLE test.tpk2 (a INT, b INT, c INT NOT NULL, d INT NOT NULL AUTO_INCREMENT, \
         KEY idx_bc (b, c), KEY idx_a (a) GLOBAL, UNIQUE KEY uidx_ac (a, c), KEY idx_c (c), \
         KEY idx_ac (a, c)) \
         PARTITION BY HASH (c) PARTITIONS 5",
        "INSERT INTO test.tpk2 VALUES (1, 2, 1, 1), (3, 6, 3, 3)",
    ] {
        rows(&mut session, sql);
    }
    session.node_conn().control_transaction("BEGIN").unwrap();
    session
        .node_conn()
        .execute_write("INSERT INTO test.tpk2 VALUES (2, 4, 2, 2)")
        .unwrap();
    for (sql, expected) in [
        (
            "SELECT /*+ use_index_merge(tpk2, idx_a, idx_bc) */ * FROM test.tpk2 \
             WHERE a = 1 OR b = 4 ORDER BY d",
            vec![["1", "2", "1", "1"], ["2", "4", "2", "2"]],
        ),
        (
            "SELECT /*+ use_index_merge(tpk2, idx_a, idx_bc) */ * FROM test.tpk2 \
             WHERE a = 2 OR b = 4 ORDER BY d",
            vec![["2", "4", "2", "2"]],
        ),
        (
            "SELECT /*+ use_index_merge(tpk2, idx_a, idx_c) */ * FROM test.tpk2 \
             WHERE a > 1 AND c > 1 ORDER BY d",
            vec![["2", "4", "2", "2"], ["3", "6", "3", "3"]],
        ),
        (
            "SELECT /*+ use_index_merge(tpk2, idx_ac, idx_bc) */ * FROM test.tpk2 \
             WHERE a = 1 OR b = 4 ORDER BY c LIMIT 1",
            vec![["1", "2", "1", "1"]],
        ),
        (
            "SELECT /*+ use_index_merge(tpk2, idx_ac, idx_bc) */ * FROM test.tpk2 \
             WHERE a = 1 OR b = 4 ORDER BY c DESC LIMIT 1",
            vec![["2", "4", "2", "2"]],
        ),
    ] {
        assert_eq!(displayed(rows(&mut session, sql)), expected, "{sql}");
    }
    session.node_conn().control_transaction("ROLLBACK").unwrap();
}

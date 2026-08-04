#![cfg(test)]

use crate::tests_support::*;
use crate::*;

/// `EXPLAIN <select>` reports the plan this tier would run, in Go's five
/// columns, without executing anything.
///
/// Every row here was compared against a `testkit.CreateMockStore`
/// capture of real TiDB's `EXPLAIN` on the same schema with no analyzed
/// statistics. Where a row differs, the divergence is named in the
/// assertion's own comment and in `tidb_executor::explain`'s module doc.
#[test]
fn explain_select() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE t (a BIGINT PRIMARY KEY, b VARCHAR(64), c INT, INDEX ub(b))")
        .unwrap();

    // The Point_Get row is byte-identical to the TiDB capture:
    //   Point_Get_1 | 1.00 | root | table:t | handle:1
    // DIVERGENCE (explain module doc, items 3 and 7): TiDB's fast plan
    // REPLACES the whole pipeline, so it prints that one row. This tier's
    // point get only narrows the source -- `run_select_stmt` keeps the
    // WHERE as a Selection above it (deliberately: an extra conjunct the
    // handle did not pin still has to filter) and always builds a
    // Projection. Both re-check rows the handle lookup already returned,
    // so neither reduces the 1.00.
    assert_eq!(
        row_text(session.run("EXPLAIN SELECT * FROM t WHERE a = 1")),
        vec![
            vec![
                "Projection_3".to_owned(),
                "1.00".to_owned(),
                "root".to_owned(),
                String::new(),
                "*".to_owned(),
            ],
            vec![
                "└─Selection_2".to_owned(),
                "1.00".to_owned(),
                "root".to_owned(),
                String::new(),
                "eq(test.t.a, 1)".to_owned(),
            ],
            vec![
                "  └─Point_Get_1".to_owned(),
                "1.00".to_owned(),
                "root".to_owned(),
                "table:t".to_owned(),
                "handle:1".to_owned(),
            ],
        ]
    );

    // Same shape, same reason. The Batch_Point_Get row itself matches the
    // capture byte for byte:
    //   Batch_Point_Get_1 | 3.00 | root | table:t |
    //     handle:[1 2 3], keep order:false, desc:false
    assert_eq!(
        row_text(session.run("EXPLAIN SELECT * FROM t WHERE a IN (1,2,3)"))[2],
        vec![
            "  └─Batch_Point_Get_1".to_owned(),
            "3.00".to_owned(),
            "root".to_owned(),
            "table:t".to_owned(),
            "handle:[1 2 3], keep order:false, desc:false".to_owned(),
        ]
    );

    // DIVERGENCE (explain module doc, items 1/3/5): TiDB prints
    //   TableReader_5 | 10000.00 | root | | data:TableFullScan_4
    //   └─TableFullScan_4 | 10000.00 | cop[tikv] | table:t | keep order:false, stats:pseudo
    // This tier has no coprocessor, so there is no TableReader and no
    // cop task; and the driver always builds a projection, which Go
    // elides here. The scan row's estRows/access/info match exactly.
    assert_eq!(
        row_text(session.run("EXPLAIN SELECT * FROM t")),
        vec![
            vec![
                "Projection_2".to_owned(),
                "10000.00".to_owned(),
                "root".to_owned(),
                String::new(),
                "*".to_owned(),
            ],
            vec![
                "└─TableFullScan_1".to_owned(),
                "10000.00".to_owned(),
                "root".to_owned(),
                "table:t".to_owned(),
                "keep order:false, stats:pseudo".to_owned(),
            ],
        ]
    );

    // A filter on an INDEXED column that real TiDB nevertheless answers with
    // a FULL SCAN. Captured from a live v8.5.6 playground on this exact
    // schema, with no analyzed statistics:
    //
    //   TableReader_7        3333.33  root                 data:Selection_6
    //   └─Selection_6        3333.33  cop[tikv]            gt(ac.t.b, "x")
    //     └─TableFullScan_5  10000.00 cop[tikv]  table:t   keep order:false, stats:pseudo
    //
    // `SELECT *` needs `c`, which `ub(b)` does not store, so the index path
    // is an `IndexLookUp` and pays Go's double-read request cost
    // (`indexRows / IndexLookupSize * 32 * tidb_request_factor`, 6e6 per
    // task) -- which a 3333-row range cannot repay. This assertion is the
    // receipt for the cost-based choice: the earlier "first index whose
    // leading column is constrained" rule printed `IndexRangeScan` here and
    // did NOT match Go.
    assert_eq!(
        row_text(session.run("EXPLAIN SELECT * FROM t WHERE b > 'x'")),
        vec![
            vec![
                "Projection_3".to_owned(),
                "3333.33".to_owned(),
                "root".to_owned(),
                String::new(),
                "*".to_owned(),
            ],
            vec![
                "└─Selection_2".to_owned(),
                "3333.33".to_owned(),
                "root".to_owned(),
                String::new(),
                // Go's own function-call rendering, captured:
                // gt(test.t.b, "x").
                "gt(test.t.b, \"x\")".to_owned(),
            ],
            vec![
                "  └─TableFullScan_1".to_owned(),
                "10000.00".to_owned(),
                "root".to_owned(),
                "table:t".to_owned(),
                "keep order:false, stats:pseudo".to_owned(),
            ],
        ]
    );

    // ORDER BY + LIMIT: the fused TopN, Go's `topn_push_down` rule. Real
    // TiDB prints (captured with `gorun`, on a `c` with no index)
    //
    //   TopN_7             10.00     root                test.t.b, offset:0, count:10
    //   └─TableReader_17   10.00     root                data:TopN_16
    //     └─TopN_16        10.00     cop[tikv]           test.t.b, offset:0, count:10
    //       └─TableFullScan_15 10000.00 cop[tikv] table:t keep order:false, stats:pseudo
    //
    // so the ROOT TopN, its 10.00, and its `offset:0, count:10` are Go's
    // exactly. The remaining differences are the two standing ones: this
    // tier has no cop task (item 1), and always builds a Projection (item 3),
    // which Go folds into the TopN as an inline projection.
    assert_eq!(
        row_text(session.run("EXPLAIN SELECT * FROM t ORDER BY c LIMIT 10")),
        vec![
            vec![
                "Projection_3".to_owned(),
                "10.00".to_owned(),
                "root".to_owned(),
                String::new(),
                "*".to_owned(),
            ],
            vec![
                "└─TopN_2".to_owned(),
                "10.00".to_owned(),
                "root".to_owned(),
                String::new(),
                "test.t.c, offset:0, count:10".to_owned(),
            ],
            vec![
                "  └─TableFullScan_1".to_owned(),
                "10000.00".to_owned(),
                "root".to_owned(),
                "table:t".to_owned(),
                "keep order:false, stats:pseudo".to_owned(),
            ],
        ]
    );

    // ORDER BY with no LIMIT above it still builds a plain Sort: there is
    // nothing for the rule to fuse.
    assert_eq!(
        row_text(session.run("EXPLAIN SELECT * FROM t ORDER BY c"))
            .into_iter()
            .map(|row| row[0].clone())
            .collect::<Vec<_>>(),
        vec![
            "Projection_3".to_owned(),
            "└─Sort_2".to_owned(),
            "  └─TableFullScan_1".to_owned(),
        ]
    );

    // GROUP BY. The 8000.00 is Go's own stats-less distinctFactor result,
    // captured. DIVERGENCE (item 4): TiDB splits this into a cop-side
    // HashAgg_5 (`funcs:count(1)->Column#6`) and a root HashAgg_9 under a
    // Projection_4; this tier has one aggregate and no Column#N slots.
    assert_eq!(
        row_text(session.run("EXPLAIN SELECT c, COUNT(*) FROM t GROUP BY c")),
        vec![
            vec![
                "HashAgg_2".to_owned(),
                "8000.00".to_owned(),
                "root".to_owned(),
                String::new(),
                // The parser normalizes COUNT(*) to COUNT(1), so this
                // half is byte-identical to the cop-side funcs: text.
                "group by:test.t.c, funcs:test.t.c, count(1)".to_owned(),
            ],
            vec![
                "└─TableFullScan_1".to_owned(),
                "10000.00".to_owned(),
                "root".to_owned(),
                "table:t".to_owned(),
                "keep order:false, stats:pseudo".to_owned(),
            ],
        ]
    );
}

/// `EXPLAIN ANALYZE <select>` really executes the query, and reports the
/// REAL number of rows each operator produced -- not an estimate.
///
/// Captured against `testkit.CreateMockStore`: real TiDB's
/// `actRows` column for `explain analyze select * from t where v > 2`
/// (table rows `(1,1),(2,2),(3,3),(4,10)`) is `4` for the
/// `TableFullScan` (it reads every row), `2` for the `Selection` (only
/// `v=3` and `v=10` pass `v > 2`), and `2` again for the `TableReader`
/// root (a pass-through). This tier has no `TableReader` (`explain`
/// module doc, divergence 1) and always builds a `Projection`
/// (divergence 3), so the real shape here is `Projection` over
/// `Selection` over `TableFullScan` -- the `Projection`'s `actRows` is
/// the same real `2`, matching the real row set, not a guess.
#[test]
fn explain_analyze_select() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .unwrap();
    session
        .run("INSERT INTO t VALUES (1,1),(2,2),(3,3),(4,10)")
        .unwrap();

    let rows = row_text(session.run("EXPLAIN ANALYZE SELECT * FROM t WHERE v > 2"));
    // Columns: id, estRows, actRows, task, access object, execution
    // info, operator info, memory, disk.
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0][0], "Projection_3");
    assert_eq!(rows[0][2], "2"); // actRows: real, not the 3333.33 estimate.
    assert_eq!(rows[1][0], "└─Selection_2");
    assert_eq!(rows[1][2], "2");
    assert_eq!(rows[2][0], "  └─TableFullScan_1");
    assert_eq!(rows[2][2], "4");
    // Every operator here runs in-process (divergence 1), and this tier
    // collects no runtime timing/memory/disk counters at all.
    for row in &rows {
        assert_eq!(row[3], "root");
        assert_eq!(row[5], "N/A"); // execution info
        assert_eq!(row[7], "N/A"); // memory
        assert_eq!(row[8], "N/A"); // disk
    }
}

/// `EXPLAIN ANALYZE <insert>` really inserts -- captured: real TiDB's
/// `EXPLAIN ANALYZE INSERT` leaves the row in the table afterward, the
/// inverse of `EXPLAIN INSERT`, which inserts nothing (see the
/// `explain_insert_never_executes` test below). The `Insert_1` row's
/// `actRows` is `0` (captured), since the insert executor's own
/// row-producing interface yields no rows -- the write is a side
/// effect, not this operator's output.
#[test]
fn explain_analyze_insert_executes() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .unwrap();

    let rows = row_text(session.run("EXPLAIN ANALYZE INSERT INTO t VALUES (1, 5)"));
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0], "Insert_1");
    assert_eq!(rows[0][2], "0");

    // The inverse of the EXPLAIN test: the row is really there now.
    assert_eq!(
        row_text(session.run("SELECT * FROM t")),
        vec![vec!["1".to_owned(), "5".to_owned()]]
    );
}

/// `EXPLAIN ANALYZE <update>` really updates -- captured against
/// `testkit.CreateMockStore`: `explain analyze update t set b = 111
/// where c = 200` on a 4-row table leaves `Update_3`'s own `actRows` at
/// `0` (a write is a side effect, same as `Insert_1`), with a
/// `Selection` (`actRows` `1`, the real number of `WHERE`-matching
/// rows) over a `TableFullScan` (`actRows` `4`, the real pre-write row
/// count) beneath it. The scan is the right read for THIS `WHERE`: `c` is an
/// ordinary column that neither pins a key nor bounds the handle, so both
/// engines read the table. The key and handle shapes are
/// `explain_update_and_delete_plan_without_writing` and
/// `tidb_session::tests_sysbench_access`.
#[test]
fn explain_analyze_update_executes() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE t (a INT PRIMARY KEY, b INT, c INT)")
        .unwrap();
    session
        .run("INSERT INTO t VALUES (1,10,100),(2,20,200),(3,30,300),(4,40,400)")
        .unwrap();

    let rows = row_text(session.run("EXPLAIN ANALYZE UPDATE t SET b = 111 WHERE c = 200"));
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0][0], "Update_3");
    assert_eq!(rows[0][2], "0");
    assert_eq!(rows[1][0], "└─Selection_2");
    assert_eq!(rows[1][2], "1");
    assert_eq!(rows[2][0], "  └─TableFullScan_1");
    assert_eq!(rows[2][2], "4");

    // The inverse of the plain-EXPLAIN test: the table really changed.
    assert_eq!(
        row_text(session.run("SELECT b FROM t WHERE a = 2")),
        vec![vec!["111".to_owned()]]
    );
}

/// `EXPLAIN ANALYZE <delete>` really deletes -- same real read-then-write
/// shape as [`explain_analyze_update_executes`], over `Delete_N`.
///
/// Its `WHERE` pins the PRIMARY KEY, so unlike the `UPDATE` above (whose
/// `WHERE c = 200` names no handle and still reads all four rows) the read is
/// a `Point_Get` that reads the ONE record under handle 2. The corpus for
/// that narrowing is `tidb_session::tests_sysbench_access`; what this test
/// adds is that the row it read is still the row it deleted.
#[test]
fn explain_analyze_delete_executes() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE t (a INT PRIMARY KEY, b INT)")
        .unwrap();
    session
        .run("INSERT INTO t VALUES (1,10),(2,20),(3,30)")
        .unwrap();

    let rows = row_text(session.run("EXPLAIN ANALYZE DELETE FROM t WHERE a = 2"));
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0][0], "Delete_3");
    assert_eq!(rows[0][2], "0");
    assert_eq!(rows[1][0], "└─Selection_2");
    assert_eq!(rows[1][2], "1");
    assert_eq!(rows[2][0], "  └─Point_Get_1");
    // One record read, not the three a full scan would have read.
    assert_eq!(rows[2][2], "1");

    assert_eq!(
        row_text(session.run("SELECT a FROM t ORDER BY a")),
        vec![vec!["1".to_owned()], vec!["3".to_owned()]]
    );
}

/// `EXPLAIN ANALYZE` of a `Point_Get`/`Batch_Point_Get`/`IndexRangeScan`
/// access path: real `actRows`, not `N/A` (divergence 7: the point get
/// keeps its `Selection`/`Projection` above it here, so the access-path
/// row is the LAST one, at the bottom of the tree). `Point_Get_1`'s
/// `actRows` is `1` for a hit and `0` for a miss, `Batch_Point_Get_1`'s
/// is the number of handles actually found, and `IndexRangeScan`'s is
/// the real number of rows the range covers -- all confirmed by
/// capture against `testkit.CreateMockStore`.
#[test]
fn explain_analyze_fast_paths_real_act_rows() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE pg (a INT PRIMARY KEY, b INT, KEY idx_b(b))")
        .unwrap();
    session
        .run("INSERT INTO pg VALUES (1,10),(2,20),(3,30),(4,40)")
        .unwrap();

    let rows = row_text(session.run("EXPLAIN ANALYZE SELECT * FROM pg WHERE a = 2"));
    assert_eq!(rows[2][0], "  └─Point_Get_1");
    assert_eq!(rows[2][2], "1");

    let rows = row_text(session.run("EXPLAIN ANALYZE SELECT * FROM pg WHERE a = 999"));
    assert_eq!(rows[2][0], "  └─Point_Get_1");
    assert_eq!(rows[2][2], "0");

    let rows = row_text(session.run("EXPLAIN ANALYZE SELECT * FROM pg WHERE a IN (1,2,3)"));
    assert_eq!(rows[2][0], "  └─Batch_Point_Get_1");
    assert_eq!(rows[2][2], "3");

    let rows = row_text(session.run("EXPLAIN ANALYZE SELECT * FROM pg WHERE b > 15 AND b < 35"));
    assert_eq!(rows[2][0], "  └─IndexRangeScan_1");
    assert_eq!(rows[2][2], "2");
}

/// `EXPLAIN ANALYZE` of a grouped aggregate/`DISTINCT`: real `actRows`
/// -- captured: a `GROUP BY` on `(1,1),(1,2),(2,3),(2,4),(3,5)` groups
/// into 3 real groups, and `SELECT DISTINCT a` over the same rows
/// dedups to the same 3 real distinct values.
#[test]
fn explain_analyze_grouped_agg_and_distinct_real_act_rows() {
    let mut session = Session::new();
    session.run("CREATE TABLE g (a INT, b INT)").unwrap();
    session
        .run("INSERT INTO g VALUES (1,1),(1,2),(2,3),(2,4),(3,5)")
        .unwrap();

    let rows = row_text(session.run("EXPLAIN ANALYZE SELECT a, COUNT(*) FROM g GROUP BY a"));
    assert_eq!(rows[0][0], "HashAgg_2");
    assert_eq!(rows[0][2], "3");

    let rows = row_text(session.run("EXPLAIN ANALYZE SELECT DISTINCT a FROM g"));
    assert_eq!(rows[0][2], "3");
}

/// `EXPLAIN ANALYZE INSERT ... SELECT`'s source gets the SAME real
/// `actRows` a plain `EXPLAIN ANALYZE SELECT` of that query would --
/// captured: `insert into dst select * from src where a > 1` on
/// `src = (1),(2),(3)` reports `2` for the `Projection`/`Selection`
/// (the `WHERE`-matching rows) over the real `3`-row `TableFullScan`,
/// computed before the insert writes anything.
#[test]
fn explain_analyze_insert_select_source_real_act_rows() {
    let mut session = Session::new();
    session.run("CREATE TABLE src (a INT)").unwrap();
    session.run("CREATE TABLE dst (a INT)").unwrap();
    session.run("INSERT INTO src VALUES (1),(2),(3)").unwrap();

    let rows =
        row_text(session.run("EXPLAIN ANALYZE INSERT INTO dst SELECT * FROM src WHERE a > 1"));
    assert_eq!(rows.len(), 4);
    assert_eq!(rows[0][0], "Insert_4");
    assert_eq!(rows[0][2], "0");
    assert_eq!(rows[1][0], "└─Projection_3");
    assert_eq!(rows[1][2], "2");
    assert_eq!(rows[2][0], "  └─Selection_2");
    assert_eq!(rows[2][2], "2");
    assert_eq!(rows[3][0], "    └─TableFullScan_1");
    assert_eq!(rows[3][2], "3");

    assert_eq!(
        row_text(session.run("SELECT a FROM dst ORDER BY a")),
        vec![vec!["2".to_owned()], vec!["3".to_owned()]]
    );
}

/// `EXPLAIN` of a write: it must never run the statement. Captured
/// against real TiDB: `EXPLAIN INSERT INTO t VALUES (1)` answers
/// `Insert_1 | N/A | root | | N/A` and inserts nothing.
#[test]
fn explain_insert_plans_without_writing() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE t (a BIGINT PRIMARY KEY)")
        .unwrap();

    assert_eq!(
        row_text(session.run("EXPLAIN INSERT INTO t VALUES (1)")),
        vec![vec![
            "Insert_1".to_owned(),
            "N/A".to_owned(),
            "root".to_owned(),
            String::new(),
            "N/A".to_owned(),
        ]]
    );
    // The plan really did not write the row.
    assert_eq!(
        row_text(session.run("SELECT COUNT(*) FROM t")),
        vec![vec!["0".to_owned()]]
    );
}

/// `EXPLAIN UPDATE`/`EXPLAIN DELETE`: the write's plan is `Update_N`/
/// `Delete_N` over the same read the write drivers actually build to
/// find the target rows. A primary-key equality pins a whole key, so
/// that read is a `Point_Get` -- from `try_point_get`, the same function
/// the read side reaches through `TryFastPlan`, as Go's
/// `tryUpdatePointPlan` does.
///
/// Divergence 7 (`explain` module doc) is what is left of the gap: Go's
/// point plan REPLACES the pipeline, where this tier keeps the `WHERE`
/// above the fetch and so still prints the `Selection`. Both read the
/// one record, by key.
#[test]
fn explain_update_and_delete_plan_without_writing() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE t (a BIGINT PRIMARY KEY, b BIGINT)")
        .unwrap();
    session.run("INSERT INTO t VALUES (1, 1)").unwrap();

    assert_eq!(
        row_text(session.run("EXPLAIN UPDATE t SET b = 100 WHERE a = 1")),
        vec![
            vec![
                "Update_3".to_owned(),
                "N/A".to_owned(),
                "root".to_owned(),
                String::new(),
                "N/A".to_owned(),
            ],
            vec![
                "└─Selection_2".to_owned(),
                // The access path already priced this condition, so the
                // Selection does not reduce the estimate a second time --
                // the same rule the read side's point get follows.
                "1.00".to_owned(),
                "root".to_owned(),
                String::new(),
                "eq(test.t.a, 1)".to_owned(),
            ],
            vec![
                "  └─Point_Get_1".to_owned(),
                "1.00".to_owned(),
                "root".to_owned(),
                "table:t".to_owned(),
                "handle:1".to_owned(),
            ],
        ]
    );
    assert_eq!(
        row_text(session.run("EXPLAIN DELETE FROM t WHERE a = 1")),
        vec![
            vec![
                "Delete_3".to_owned(),
                "N/A".to_owned(),
                "root".to_owned(),
                String::new(),
                "N/A".to_owned(),
            ],
            vec![
                "└─Selection_2".to_owned(),
                "1.00".to_owned(),
                "root".to_owned(),
                String::new(),
                "eq(test.t.a, 1)".to_owned(),
            ],
            vec![
                "  └─Point_Get_1".to_owned(),
                "1.00".to_owned(),
                "root".to_owned(),
                "table:t".to_owned(),
                "handle:1".to_owned(),
            ],
        ]
    );
    // Neither plan wrote or removed the row.
    assert_eq!(
        row_text(session.run("SELECT * FROM t")),
        vec![vec!["1".to_owned(), "1".to_owned()]]
    );
}

/// `EXPLAIN FORMAT = 'brief'` prints the identical tree with every
/// operator's `_N` build-order suffix stripped (captured: `explain
/// format = 'brief' select * from t` strips the `Point_Get_1`/
/// `Selection_2`/`Projection_3` ids down to `Point_Get`/`Selection`/
/// `Projection`; `'row'`, the default, keeps them).
#[test]
fn explain_brief_format_strips_operator_ids() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE t (a BIGINT PRIMARY KEY)")
        .unwrap();
    session.run("INSERT INTO t VALUES (1)").unwrap();

    assert_eq!(
        row_text(session.run("EXPLAIN FORMAT = 'brief' SELECT * FROM t WHERE a = 1")),
        vec![
            vec![
                "Projection".to_owned(),
                "1.00".to_owned(),
                "root".to_owned(),
                String::new(),
                "*".to_owned(),
            ],
            vec![
                "└─Selection".to_owned(),
                "1.00".to_owned(),
                "root".to_owned(),
                String::new(),
                "eq(test.t.a, 1)".to_owned(),
            ],
            vec![
                "  └─Point_Get".to_owned(),
                "1.00".to_owned(),
                "root".to_owned(),
                "table:t".to_owned(),
                "handle:1".to_owned(),
            ],
        ]
    );
    assert_eq!(
        row_text(session.run("EXPLAIN FORMAT = 'row' SELECT * FROM t WHERE a = 1"))[2],
        vec![
            "  └─Point_Get_1".to_owned(),
            "1.00".to_owned(),
            "root".to_owned(),
            "table:t".to_owned(),
            "handle:1".to_owned(),
        ]
    );
}

/// EXPLAIN still refuses the forms this tier cannot plan honestly:
/// ANALYZE (Go executes the statement to gather runtime counters this
/// tier does not collect, captured) and any format name Go itself does
/// not recognize.
#[test]
fn explain_refuses_what_it_cannot_plan() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE t (a BIGINT PRIMARY KEY)")
        .unwrap();

    // `EXPLAIN ANALYZE` of a `SELECT`/`INSERT`/`UPDATE`/`DELETE` really
    // runs (see `explain_analyze_select`/`explain_analyze_insert_executes`/
    // `explain_analyze_update_executes`/`explain_analyze_delete_executes`);
    // only a set-operation query is refused.
    assert!(matches!(
        session.run("EXPLAIN ANALYZE (SELECT a FROM t) UNION (SELECT a FROM t)"),
        Err(DriverError::Unsupported(reason)) if reason == "EXPLAIN ANALYZE of a set operation is not supported yet"
    ));
    assert!(matches!(
        session.run("EXPLAIN FORMAT = 'bogus' SELECT * FROM t"),
        Err(DriverError::Unsupported(reason)) if reason == "unknown EXPLAIN format name"
    ));
}

/// Predicate push-down does not change the plan EXPLAIN prints.
///
/// Captured from Go (`pkg/executor/zz_dump_pushdown_test.go`, mock store,
/// `explain format='brief'`): for `select a, b from t where a > 5`, for the
/// split `where a > 5 and b + 1 < 10`, and for the wholly unpushable
/// `where a > 5 or b < 10`, Go prints the SAME three-row shape --
/// `TableReader` over ONE `Selection` over `TableFullScan(10000.00)`. Go's
/// coprocessor accepts every one of those predicates, so its own split never
/// surfaces as a second, root-side `Selection`; only the estimate moves
/// (`3333.33` for the single `>`, `2666.67` for the split).
///
/// This tier prints no `TableReader` and no `cop[tikv]` task (explain module
/// doc, divergence 1) and always builds a `Projection` (divergence 3), so the
/// same plan reads as `Projection` over `Selection` over `TableFullScan` --
/// whichever half of the predicate the scan itself took over.
#[test]
fn pushing_a_predicate_into_the_scan_keeps_the_captured_plan_shape() {
    let mut session = Session::new();
    session.run("CREATE TABLE t (a BIGINT, b BIGINT)").unwrap();

    for (sql, printed) in [
        ("SELECT a, b FROM t WHERE a > 5", "gt(test.t.a, 5)"),
        (
            "SELECT a, b FROM t WHERE a > 5 AND b + 1 < 10",
            "and(gt(test.t.a, 5), lt(plus(test.t.b, 1), 10))",
        ),
        (
            "SELECT a, b FROM t WHERE a > 5 OR b < 10",
            "or(gt(test.t.a, 5), lt(test.t.b, 10))",
        ),
    ] {
        let rows = row_text(session.run(&format!("EXPLAIN {sql}")));
        assert_eq!(rows.len(), 3, "{sql}");
        assert_eq!(rows[0][0], "Projection_3", "{sql}");
        assert_eq!(rows[1][0], "\u{2514}\u{2500}Selection_2", "{sql}");
        assert_eq!(rows[1][4], printed, "{sql}");
        assert_eq!(rows[2][0], "  \u{2514}\u{2500}TableFullScan_1", "{sql}");
        assert_eq!(rows[2][1], "10000.00", "{sql}");
        // No second Selection, and no task column ever leaves `root`.
        for row in &rows {
            assert_eq!(row[2], "root", "{sql}");
        }
    }

    // The single `>` keeps Go's captured 3333.33 estimate, which the split
    // must not disturb.
    let rows = row_text(session.run("EXPLAIN SELECT a, b FROM t WHERE a > 5"));
    assert_eq!(rows[1][1], "3333.33");
}

/// `EXPLAIN ANALYZE` over a scan that took the whole `WHERE`: the
/// `TableFullScan` still reports the rows it READ, not the rows the pushed
/// predicate let through.
///
/// This is the counter that would silently break if a filtering scan reported
/// its output as its scanned count: Go's capture for
/// `explain analyze select * from t where v > 2` over `(1,1),(2,2),(3,3),
/// (4,10)` is `4` for `TableFullScan` and `2` for `Selection`.
#[test]
fn a_filtering_scan_still_reports_the_rows_it_read() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE t (id BIGINT PRIMARY KEY, v BIGINT)")
        .unwrap();
    session
        .run("INSERT INTO t VALUES (1,1),(2,2),(3,3),(4,10)")
        .unwrap();
    let rows = row_text(session.run("EXPLAIN ANALYZE SELECT * FROM t WHERE v > 2"));
    assert_eq!(rows[1][0], "\u{2514}\u{2500}Selection_2");
    assert_eq!(rows[1][2], "2", "rows that passed the predicate");
    assert_eq!(rows[2][0], "  \u{2514}\u{2500}TableFullScan_1");
    assert_eq!(rows[2][2], "4", "rows the scan read, before filtering");
}

/// `EXPLAIN` of a hash join, against a `pkg/executor` mock-store capture on
/// the same statistics-free schema (`TestZZDumpHashJoin`).
///
/// Every assertion below is the join row's own `operator info` cell plus the
/// `(Build)`/`(Probe)` labels, which are byte-identical to that capture. The
/// rows AROUND the join still diverge in the ways `tidb_executor::explain`'s
/// module doc already names -- this tier reads a table directly instead of
/// through a `TableReader`/`Selection` pair, does not push the implicit
/// `not(isnull(key))` down, prints `N/A` where an equi-join's cardinality
/// would need statistics, and always builds a `Projection`. What this test
/// pins is the join operator itself.
#[test]
fn explain_hash_join_operator_info_matches_go() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE hj1 (a INT, b INT, s VARCHAR(20))")
        .unwrap();
    session
        .run("CREATE TABLE hj2 (a INT, b INT, s VARCHAR(20))")
        .unwrap();

    // The join row, and which child carries which label.
    let join_shape = |session: &mut Session, sql: &str| -> Vec<String> {
        let rows = row_text(session.run(sql));
        let join = rows
            .iter()
            .find(|row| row[0].ends_with("HashJoin"))
            .expect("the plan has a HashJoin row");
        let mut out = vec![join[4].clone()];
        out.extend(
            rows.iter()
                .filter(|row| row[0].contains("(Build)") || row[0].contains("(Probe)"))
                .map(|row| format!("{} {}", label_of(&row[0]), row[3])),
        );
        out
    };

    // Captured: `inner join, equal:[eq(test.hj1.a, test.hj2.a)]`, with hj2
    // as `(Build)`. Go's stats-less enumeration reaches the RIGHT child as
    // the build side first for an inner join, and this tier has no
    // statistics to re-pick with.
    assert_eq!(
        join_shape(
            &mut session,
            "EXPLAIN FORMAT='brief' SELECT * FROM hj1 JOIN hj2 ON hj1.a = hj2.a"
        ),
        vec![
            "inner join, equal:[eq(test.hj1.a, test.hj2.a)]",
            "(Build) table:hj2",
            "(Probe) table:hj1",
        ]
    );

    // An outer join names its LEFT child (`explainJoinLeftSide`, omitted for
    // an inner join) and builds on the NON-preserved side, so the preserved
    // side is the one being streamed.
    assert_eq!(
        join_shape(
            &mut session,
            "EXPLAIN FORMAT='brief' SELECT * FROM hj1 LEFT JOIN hj2 ON hj1.a = hj2.a"
        ),
        vec![
            "left outer join, left side:TableFullScan, equal:[eq(test.hj1.a, test.hj2.a)]",
            "(Build) table:hj2",
            "(Probe) table:hj1",
        ]
    );
    assert_eq!(
        join_shape(
            &mut session,
            "EXPLAIN FORMAT='brief' SELECT * FROM hj1 RIGHT JOIN hj2 ON hj1.a = hj2.a"
        ),
        vec![
            "right outer join, left side:TableFullScan, equal:[eq(test.hj1.a, test.hj2.a)]",
            "(Build) table:hj1",
            "(Probe) table:hj2",
        ]
    );

    // Multiple keys stay in `ON` order and are SPACE separated inside
    // `equal:[...]` -- Go writes no comma between them.
    assert_eq!(
        join_shape(
            &mut session,
            "EXPLAIN FORMAT='brief' SELECT * FROM hj1 JOIN hj2 \
             ON hj1.a = hj2.a AND hj1.b = hj2.b"
        )[0],
        "inner join, equal:[eq(test.hj1.a, test.hj2.a) eq(test.hj1.b, test.hj2.b)]"
    );

    // A non-equi conjunct alongside an equal one is the residue the hash
    // table cannot index; it prints as `other cond:` and is still evaluated
    // per candidate pair.
    assert_eq!(
        join_shape(
            &mut session,
            "EXPLAIN FORMAT='brief' SELECT * FROM hj1 JOIN hj2 \
             ON hj1.a = hj2.a AND hj1.b > hj2.b"
        )[0],
        "inner join, equal:[eq(test.hj1.a, test.hj2.a)], other cond:gt(test.hj1.b, test.hj2.b)"
    );

    // No equal condition at all: `CARTESIAN`, and the executor falls back to
    // the nested loop.
    assert_eq!(
        join_shape(
            &mut session,
            "EXPLAIN FORMAT='brief' SELECT * FROM hj1 JOIN hj2 ON hj1.a > hj2.a"
        )[0],
        "CARTESIAN inner join, other cond:gt(test.hj1.a, test.hj2.a)"
    );
    assert_eq!(
        join_shape(
            &mut session,
            "EXPLAIN FORMAT='brief' SELECT * FROM hj1 JOIN hj2"
        )[0],
        "CARTESIAN inner join"
    );

    // A string key hashes too (under the comparison collation's sort key).
    assert_eq!(
        join_shape(
            &mut session,
            "EXPLAIN FORMAT='brief' SELECT * FROM hj1 LEFT JOIN hj2 ON hj1.s = hj2.s"
        )[0],
        "left outer join, left side:TableFullScan, equal:[eq(test.hj1.s, test.hj2.s)]"
    );
}

fn label_of(drawn_name: &str) -> &'static str {
    if drawn_name.contains("(Build)") {
        "(Build)"
    } else {
        "(Probe)"
    }
}

/// The `estRows` a `Selection` over an UNANALYZED table prints, against the
/// numbers real TiDB prints for the same statements.
///
/// `tidb_planner`'s `selectivity_pseudo_source` pins the same 16 shapes at the
/// arithmetic's own entry points, from a repo-root `gorun` capture on
/// `create table t(a int, b int, c varchar(32), d int unique, e int, f int)`
/// with no `ANALYZE`. This is the LIVE half: the same numbers reached by
/// running `EXPLAIN` through the session, so the wiring cannot rot while the
/// leaves keep passing.
///
/// This is the ONLY guard on these numbers. `difftests`' integration replay
/// compares a plan through `access_property`, which keeps the operator, the
/// access object, the range and `stats:pseudo` and drops `estRows` on the
/// floor -- so its divergence ratchet cannot see an estimate move in either
/// direction, and did not move when these fifteen numbers were corrected.
///
/// The one number that does not match is named at the bottom.
#[test]
fn explain_est_rows_on_an_unanalyzed_table() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE t(a int, b int, c varchar(32), d int unique, e int, f int)")
        .unwrap();

    /// The `estRows` of the `Selection` the driver builds over the scan.
    fn est_rows(session: &mut Session, where_clause: &str) -> String {
        let rows = row_text(session.run(&format!("EXPLAIN SELECT * FROM t WHERE {where_clause}")));
        let selection = rows
            .iter()
            .find(|row| row[0].contains("Selection"))
            .unwrap_or_else(|| panic!("no Selection row for {where_clause}"));
        selection[1].clone()
    }

    // Every right-hand string below is TiDB's own printed estRows.
    let pinned = [
        // One equality is the control: a single node, no combination at all.
        ("a = 1", "10.00"),
        // The product, then the one-row floor: 0.001^2 and 0.001^3 both land
        // under 1/10000, which is why TiDB prints 1.00 for both. A MINIMUM
        // over the per-operator rates -- what this path used before the
        // wiring -- prints 10.00 here for every one of the three.
        ("a = 1 and b = 2", "1.00"),
        ("a = 1 and b = 2 and e = 3", "1.00"),
        ("a in (1,2,3)", "30.00"),
        ("a in (1,2,3) and b in (4,5)", "1.00"),
        ("a > 1", "3333.33"),
        // The product loop is the only thing separating 3333.33 from 1111.11.
        ("a > 1 and b > 2", "1111.11"),
        ("a between 1 and 5", "250.00"),
        ("a between 1 and 5 and b between 2 and 9", "6.25"),
        // A PREFIX `LIKE` builds a bounded range, so it is an ordinary
        // between-rate node rather than a leftover default.
        ("c like 'a%'", "250.00"),
        ("a = 1 and c like 'a%'", "1.00"),
        // A non-prefix `LIKE` builds no range and takes
        // `GetStrMatchDefaultSelectivity()` = 0.1, not the general 0.8 factor
        // (which would print 8000.00 here).
        ("c like '%a%'", "1000.00"),
        ("c not like '%a%'", "9000.00"),
        // The leftover block charges its minimum ONCE for the whole remaining
        // mask, not once per condition: two of them still print 1000.00.
        ("c like '%a%' and c like '%b%'", "1000.00"),
        // No range for either column: one leftover `Other` at 0.8.
        ("a + b > 1", "8000.00"),
    ];
    for (where_clause, expected) in pinned {
        assert_eq!(
            est_rows(&mut session, where_clause),
            expected,
            "{where_clause}"
        );
    }

    // Past 63 conditions `Selectivity` abandons the nodes and calls
    // `pseudoSelectivity` (`selectivity.go:69-73`), whose answer is a MINIMUM
    // over the per-operator rates.
    let not_equal: Vec<String> = (0..64).map(|value| format!("a != {value}")).collect();
    let not_equal = not_equal.join(" and ");
    // `ne` matches neither switch arm, so `minFactor` never leaves 0.8.
    assert_eq!(est_rows(&mut session, &not_equal), "8000.00", "64 x a != k");
    // One ordering predicate drops it to 1/pseudoLessRate. This is the
    // assertion that PROVES the >63 arm is the one running: the node path
    // would charge 1/3 for `b > 3` and 0.8 for the leftover 64 and print
    // 2666.67.
    assert_eq!(
        est_rows(&mut session, &format!("{not_equal} and b > 3")),
        "3333.33",
        "64 x a != k and b > 3"
    );
    // An equality on the UNIQUE column `d`. TiDB prints 1.00 via
    // `pseudoSelectivity`'s `1/RealtimeCount` shortcut; this tier reaches the
    // same 1.00 by COSTING the unique index and letting the range scan
    // consume the condition, so the printed number agrees while the row above
    // it does not.
    assert_eq!(
        est_rows(&mut session, &format!("{not_equal} and d = 7")),
        "1.00",
        "64 x a != k and d = 7"
    );

    // `sel(A or B) = sel(A) + sel(B) - sel(A)*sel(B)` (`selectivity.go:331`),
    // the recursive DNF estimate, now reached on the pseudo path too: the
    // `Disjunction` carries its own estimate into the leftover block and
    // covers itself instead of taking the 0.8 factor (which printed 8000.00).
    assert_eq!(est_rows(&mut session, "a = 1 or b = 2"), "19.99");

    // Every predicate on ONE column merges into ONE range set before it is
    // estimated -- Go's node loop walks the DEDUPLICATED columns and runs
    // `getMaskAndRanges` over the whole condition list per column
    // (`selectivity.go:98-113`). A per-conjunct product instead multiplies
    // two independent half-lines and says 1107.78 here.
    let per_column = [
        ("a >= 3 and a <= 7", "250.00"),
        // The intersection keeps only the tighter bound, so this is `a > 5`
        // alone; a product would print 1111.11.
        ("a > 3 and a > 5", "3333.33"),
        // `IN` and an ordering predicate on one column intersect too:
        // `[2,2], [3,3]`, not three points. A product prints 30.00.
        ("a in (1,2,3) and a > 1", "20.00"),
        // Two columns still multiply -- the independence assumption is
        // between COLUMNS, not between conjuncts.
        ("a > 3 and b < 5", "1107.78"),
        ("a >= 3 and a <= 7 and b >= 3 and b <= 7", "6.25"),
        // A prefix LIKE is a range on its own column, so it merges with the
        // other column's node exactly like any other predicate.
        ("a >= 3 and a <= 7 and c like 'q%'", "6.25"),
        ("a < 5 and (b > 8 or b < 2)", "2212.23"),
        ("a <> 3 and a <> 5", "6906.67"),
    ];
    for (where_clause, expected) in per_column {
        assert_eq!(
            est_rows(&mut session, where_clause),
            expected,
            "{where_clause}"
        );
    }
}

/// Go `findBestTask`'s empty-range short-circuit: a chosen path whose range
/// list is EMPTY is a `PhysicalTableDual` with `rows:0`, not a scan printed
/// with an empty `range:` cell.
///
/// The schema is `tests/integrationtest/t/util/ranger.test`'s own, and it is
/// the UNSIGNED key part that makes the case reachable: `a < -1` over a
/// SIGNED column is the ordinary range `[-inf,-1)`, so a fixture that dropped
/// `UNSIGNED` would pass this test for the wrong reason. Recorded by TiDB in
/// `tests/integrationtest/r/util/ranger.result`:
///
/// ```text
/// explain format = 'plan_tree' select * from t1 use index(a) where a < -1;
/// TableDual  root    rows:0
/// ```
///
/// DIVERGENCE (`tidb_executor::plan_trace::empty_range_table_dual`): Go
/// discards the operators above the read too, since the whole `DataSource`
/// task becomes the dual; this tier keeps the `Selection`/`Projection` over
/// a source that produces nothing.
#[test]
fn an_empty_index_range_is_a_table_dual_not_a_scan() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE t1 (a DECIMAL UNSIGNED, KEY(a))")
        .unwrap();
    session.run("INSERT INTO t1 VALUES (0), (NULL)").unwrap();

    for where_clause in ["a < -1", "a <= -1", "a = -1"] {
        let rows = row_text(session.run(&format!(
            "EXPLAIN FORMAT = 'brief' SELECT * FROM t1 USE INDEX(a) WHERE {where_clause}"
        )));
        let leaf = rows.last().expect("a plan has at least one row");
        assert_eq!(leaf[0], "  └─TableDual", "{where_clause}");
        assert_eq!(leaf[4], "rows:0", "{where_clause}");
        // The rows were already right and must stay right.
        assert!(row_text(session.run(&format!(
            "SELECT * FROM t1 USE INDEX(a) WHERE {where_clause}"
        )))
        .is_empty());
    }

    // The CONTROL: a bound the unsigned domain can satisfy still reads the
    // index over a real range. A change that promoted every `USE INDEX` path
    // to a dual would pass the loop above and fail here.
    let rows = row_text(
        session.run("EXPLAIN FORMAT = 'brief' SELECT * FROM t1 USE INDEX(a) WHERE a > -1"),
    );
    let leaf = rows.last().expect("a plan has at least one row");
    assert_eq!(leaf[0], "  └─IndexRangeScan");
    assert_eq!(leaf[4], "range:[0,+inf], keep order:false, stats:pseudo");
    assert_eq!(
        row_text(session.run("SELECT * FROM t1 USE INDEX(a) WHERE a > -1")),
        [["0"]]
    );
}

/// Open-interval ranges survive a NON-COVERING read, identically to a covering
/// one.
///
/// This is a MEASURED NEGATIVE, kept as a test because the claim it refutes --
/// "no open-interval range is built for a non-covering read" -- has been chased
/// twice. It is not true anywhere that could be found. Coveringness is not an
/// input to range building at all: the pairs below differ only in whether the
/// projection can be served from the index, and they produce byte-identical
/// `range:` cells.
///
/// The measurement, over `d (a, b, c)` with `KEY ia(a)` and `KEY iab(a,b)`:
///
/// ```text
/// SELECT * FROM d USE INDEX(ia)  WHERE a > 5          range:(5,+inf]      non-covering
/// SELECT a FROM d USE INDEX(ia)  WHERE a > 5          range:(5,+inf]      covering
/// SELECT * FROM d USE INDEX(ia)  WHERE a >= 5         range:[5,+inf]
/// SELECT * FROM d USE INDEX(ia)  WHERE a < 5          range:[-inf,5)
/// SELECT * FROM d USE INDEX(ia)  WHERE a > 5 AND a < 9  range:(5,9)
/// SELECT * FROM d USE INDEX(iab) WHERE a = 1 AND b > 5  range:(1 5,1 +inf]  non-covering
/// SELECT c FROM d USE INDEX(iab) WHERE a = 1 AND b > 5  range:(1 5,1 +inf]  non-covering
/// SELECT * FROM d USE INDEX(ia)  WHERE a != 5         range:[-inf,5), (5,+inf]
/// ```
///
/// What DOES still fall back to a full scan is a different mechanism, and
/// naming it here is the point of writing this down: `SELECT * FROM d WHERE
/// a > 5` with no hint picks a `TableFullScan`, because under pseudo stats the
/// double read costs more than the scan. That is access-path SELECTION, not
/// range building -- the range exists and is correct the moment the path is
/// chosen, as the `USE INDEX` line directly above it shows. The one remaining
/// `!=` divergence in `util/ranger` is the same kind: a `!=` inside a join
/// `ON` clause, where the path is never considered, not where the range comes
/// out empty. The last line above proves `!=` itself lowers to its two open
/// intervals.
#[test]
fn open_interval_ranges_do_not_depend_on_a_read_being_covering() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE d (a INT, b INT, c INT, KEY ia(a), KEY iab(a,b))")
        .unwrap();
    let range_of = |session: &mut Session, query: &str| {
        let rows = row_text(session.run(&format!("EXPLAIN FORMAT = 'brief' {query}")));
        let leaf = rows.last().expect("a plan has at least one row").clone();
        (leaf[0].clone(), leaf[4].clone())
    };
    for (query, expected) in [
        (
            "SELECT * FROM d USE INDEX(ia) WHERE a > 5",
            "range:(5,+inf]",
        ),
        (
            "SELECT a FROM d USE INDEX(ia) WHERE a > 5",
            "range:(5,+inf]",
        ),
        (
            "SELECT * FROM d USE INDEX(ia) WHERE a >= 5",
            "range:[5,+inf]",
        ),
        (
            "SELECT * FROM d USE INDEX(ia) WHERE a < 5",
            "range:[-inf,5)",
        ),
        (
            "SELECT * FROM d USE INDEX(ia) WHERE a > 5 AND a < 9",
            "range:(5,9)",
        ),
        (
            "SELECT * FROM d USE INDEX(iab) WHERE a = 1 AND b > 5",
            "range:(1 5,1 +inf]",
        ),
        (
            "SELECT c FROM d USE INDEX(iab) WHERE a = 1 AND b > 5",
            "range:(1 5,1 +inf]",
        ),
        (
            "SELECT * FROM d USE INDEX(ia) WHERE a != 5",
            "range:[-inf,5), (5,+inf]",
        ),
    ] {
        let (operator, info) = range_of(&mut session, query);
        assert_eq!(operator, "  \u{2514}\u{2500}IndexRangeScan", "{query}");
        assert!(
            info.starts_with(expected),
            "{query} gave {info}, expected it to start with {expected}",
        );
    }

    // The control, and the thing that is genuinely different: WITHOUT the
    // hint, the same predicate takes a full scan on cost grounds. If range
    // building ever did depend on coveringness, this line would be the only
    // one above that still passed.
    let (operator, _) = range_of(&mut session, "SELECT * FROM d WHERE a > 5");
    assert_eq!(operator, "  \u{2514}\u{2500}TableFullScan");
}

/// Go `cardinality.EstimateFullJoinRowCount`, which
/// `tidb_executor::plan_trace` now calls for the `HashJoin` row that used to
/// print `N/A`.
///
/// Each right-hand string is TiDB's own printed `estRows` for the same
/// statement on the same schema, captured with `gorun`. Two of the three
/// shapes agree exactly; the equi-join shapes are 0.1% high for ONE reason,
/// named on its assertion.
#[test]
fn explain_est_rows_for_a_join() {
    let mut session = Session::new();
    session.run("CREATE TABLE j1(a int, b int)").unwrap();
    session.run("CREATE TABLE j2(a int, b int)").unwrap();

    fn join_est(session: &mut Session, sql: &str) -> String {
        let rows = row_text(session.run(&format!("EXPLAIN {sql}")));
        let join = rows
            .iter()
            .find(|row| row[0].contains("HashJoin"))
            .unwrap_or_else(|| panic!("no HashJoin row for {sql}"));
        join[1].clone()
    }

    // EXACT. A Cartesian product is `leftRows * rightRows` and needs no
    // statistics at all -- and TiDB derives no null-rejecting filter under a
    // join with no join key, so both sides are the same 10000 here as there.
    assert_eq!(
        join_est(&mut session, "SELECT * FROM j1, j2"),
        "100000000.00"
    );

    // DIVERGENCE, 0.1%, and only this: TiDB rewrites an inner equi-join's
    // sides with `not(isnull(k))` before estimating, so it divides
    // 9990 * 9990 by 7992 (= 12487.50) where this divides 10000 * 10000 by
    // 8000. The arithmetic between those inputs is identical.
    for equi in [
        "SELECT * FROM j1 JOIN j2 ON j1.a = j2.a",
        "SELECT * FROM j1 LEFT JOIN j2 ON j1.a = j2.a",
        // TWO keys, same answer: `EstimateColsNDVWithMatchedLen`'s default
        // arm is the MAXIMUM over the keys' column NDVs, not their product,
        // and every column of a pseudo side has the same NDV. TiDB prints
        // 12475.01, again its own 9980.01-row inputs.
        "SELECT * FROM j1 JOIN j2 ON j1.a = j2.a AND j1.b = j2.b",
    ] {
        assert_eq!(join_est(&mut session, equi), "12500.00", "{equi}");
    }

    // A non-equality `ON` is a CARTESIAN join with an `other cond:`, so it
    // takes the product arm. TiDB prints 99800100.00 = 9990 * 9990: `gt`
    // rejects nulls, so its rewrite fires here too.
    assert_eq!(
        join_est(&mut session, "SELECT * FROM j1 JOIN j2 ON j1.a > j2.a"),
        "100000000.00"
    );

    // An ANALYZEd side has real per-column histogram NDVs, which this tier
    // does not carry through the trace, so the join keeps Go's `N/A` rather
    // than dividing by the pseudo 0.8 ratio (TiDB prints 3.00 here).
    session
        .run("INSERT INTO j1 VALUES (1,1),(2,2),(3,3)")
        .unwrap();
    session.run("ANALYZE TABLE j1").unwrap();
    assert_eq!(
        join_est(&mut session, "SELECT * FROM j1 JOIN j2 ON j1.a = j2.a"),
        "N/A"
    );
}

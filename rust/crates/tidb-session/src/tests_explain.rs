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

    // An indexed column's range scan. TiDB prints the same 3333.33 and
    // the same `table:t, index:ub(b)` access object; it wraps the scan in
    // a TableReader/cop task (divergence 1) and its Selection sits in the
    // cop task rather than above the scan.
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
                "  └─IndexRangeScan_1".to_owned(),
                "3333.33".to_owned(),
                "root".to_owned(),
                "table:t, index:ub(b)".to_owned(),
                "range:(\"x\",+inf], keep order:false, stats:pseudo".to_owned(),
            ],
        ]
    );

    // ORDER BY + LIMIT. DIVERGENCE (item 2): TiDB merges these into one
    // TopN_7 (10.00). This tier builds a Sort and a Limit, so both show.
    // The Limit's 10.00 and its `offset:0, count:10` match Go's.
    assert_eq!(
        row_text(session.run("EXPLAIN SELECT * FROM t ORDER BY c LIMIT 10")),
        vec![
            vec![
                "Limit_4".to_owned(),
                "10.00".to_owned(),
                "root".to_owned(),
                String::new(),
                "offset:0, count:10".to_owned(),
            ],
            vec![
                "└─Projection_3".to_owned(),
                "10000.00".to_owned(),
                "root".to_owned(),
                String::new(),
                "*".to_owned(),
            ],
            vec![
                "  └─Sort_2".to_owned(),
                "10000.00".to_owned(),
                "root".to_owned(),
                String::new(),
                "test.t.c".to_owned(),
            ],
            vec![
                "    └─TableFullScan_1".to_owned(),
                "10000.00".to_owned(),
                "root".to_owned(),
                "table:t".to_owned(),
                "keep order:false, stats:pseudo".to_owned(),
            ],
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
/// count) beneath it -- the write path always full-scans here (`explain`
/// module doc, divergence 8), never a `Point_Get`, even for a
/// primary-key equality.
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
    assert_eq!(rows[2][0], "  └─TableFullScan_1");
    assert_eq!(rows[2][2], "3");

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
/// find the target rows. Divergence 8 (`explain` module doc): those
/// drivers always scan the whole table and filter row-by-row, with no
/// point-get/index fast path, so the recorder always shows
/// `TableFullScan` + `Selection` -- even for a primary-key equality,
/// where Go's own planner would print `Point_Get`.
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
                "10.00".to_owned(),
                "root".to_owned(),
                String::new(),
                "eq(test.t.a, 1)".to_owned(),
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
                "10.00".to_owned(),
                "root".to_owned(),
                String::new(),
                "eq(test.t.a, 1)".to_owned(),
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
        Err(DriverError::Unsupported(
            "EXPLAIN ANALYZE of a set operation is not supported yet"
        ))
    ));
    assert!(matches!(
        session.run("EXPLAIN FORMAT = 'bogus' SELECT * FROM t"),
        Err(DriverError::Unsupported("unknown EXPLAIN format name"))
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

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

    // An exact handle predicate takes Go's fast-plan path and replaces the
    // ordinary projection/filter pipeline with the point-get itself.
    assert_eq!(
        row_text(session.run("EXPLAIN SELECT * FROM t WHERE a = 1")),
        vec![vec![
            "Point_Get_1".to_owned(),
            "1.00".to_owned(),
            "root".to_owned(),
            "table:t".to_owned(),
            "handle:1".to_owned(),
        ]]
    );

    // Go's batch fast plan consumes the exact key-only IN just like the
    // single-point path above. The Batch_Point_Get row itself matches the
    // capture byte for byte:
    //   Batch_Point_Get_1 | 3.00 | root | table:t |
    //     handle:[1 2 3], keep order:false, desc:false
    let plan = row_text(session.run("EXPLAIN SELECT * FROM t WHERE a IN (1,2,3)"));
    assert_eq!(
        plan.iter()
            .find(|row| row[0].contains("Batch_Point_Get"))
            .expect("batch point row"),
        &vec![
            "Batch_Point_Get_1".to_owned(),
            "3.00".to_owned(),
            "root".to_owned(),
            "table:t".to_owned(),
            "handle:[1 2 3], keep order:false, desc:false".to_owned(),
        ]
    );

    session
        .run(
            "CREATE TABLE composite_point (id BIGINT PRIMARY KEY, a BIGINT, b VARCHAR(8), \
             UNIQUE KEY ab (a, b))",
        )
        .unwrap();
    session
        .run("INSERT INTO composite_point VALUES (1, 10, 'x'), (2, 20, 'y')")
        .unwrap();
    let plan = row_text(session.run(
        "EXPLAIN SELECT id FROM composite_point \
                 WHERE (b, a) IN (('y', 20), ('x', 10), ('missing', 30))",
    ));
    let batch = plan
        .iter()
        .find(|row| row[0].contains("Batch_Point_Get"))
        .unwrap_or_else(|| panic!("the composite key must reach the batch-point path: {plan:?}"));
    assert_eq!(
        &batch[1..],
        [
            "3.00",
            "root",
            "table:composite_point, index:ab(a, b)",
            "keep order:false, desc:false"
        ]
    );
    let forced_table = row_text(session.run(
        "EXPLAIN SELECT id FROM composite_point USE INDEX () \
         WHERE (a, b) IN ((10, 'x'), (20, 'y'))",
    ));
    assert!(
        forced_table
            .iter()
            .all(|row| !row[0].contains("Batch_Point_Get")),
        "Go's index hint removes the composite index fast path: {forced_table:?}"
    );
    let mut forced_ids = row_text(session.run(
        "SELECT id FROM composite_point USE INDEX () \
         WHERE (a, b) IN ((10, 'x'), (20, 'y'))",
    ));
    forced_ids.sort();
    assert_eq!(forced_ids, [vec!["1".to_owned()], vec!["2".to_owned()]]);
    let mut ids =
        row_text(session.run(
            "SELECT id FROM composite_point WHERE (b, a) IN (('y', 20), ('x', 10), ('y', 20))",
        ));
    ids.sort();
    assert_eq!(ids, [vec!["1".to_owned()], vec!["2".to_owned()]]);
    session
        .run(
            "CREATE TABLE common_point (a BIGINT, b VARCHAR(8), v BIGINT, \
             PRIMARY KEY (a, b))",
        )
        .unwrap();
    session
        .run("INSERT INTO common_point VALUES (10, 'x', 1), (20, 'y', 2)")
        .unwrap();
    let plan = row_text(
        session.run("EXPLAIN SELECT v FROM common_point WHERE (b, a) IN (('y', 20), ('x', 10))"),
    );
    let batch = plan
        .iter()
        .find(|row| row[0].contains("Batch_Point_Get"))
        .unwrap_or_else(|| panic!("the common key must reach the batch-point path: {plan:?}"));
    assert_eq!(
        &batch[3..],
        [
            "table:common_point, clustered index:PRIMARY(a, b)",
            "keep order:false, desc:false"
        ]
    );
    let mut common_values = row_text(session.run(
        "SELECT v FROM common_point USE INDEX (PRIMARY) \
             WHERE (b, a) IN (('y', 20), ('x', 10), ('y', 20))",
    ));
    common_values.sort();
    assert_eq!(common_values, [vec!["1".to_owned()], vec!["2".to_owned()]]);
    let no_common_primary = row_text(session.run(
        "EXPLAIN SELECT v FROM common_point USE INDEX () \
         WHERE (b, a) IN (('y', 20), ('x', 10))",
    ));
    assert!(
        no_common_primary
            .iter()
            .all(|row| !row[0].contains("Batch_Point_Get")),
        "USE INDEX() must remove the clustered primary path: {no_common_primary:?}"
    );

    // `tryWhereIn2BatchPointGet` itself declines any generated column, but
    // Go's ordinary optimizer recovers the exact query as BatchPointGet. Its
    // checked-in generated_columns.result pins this final plan, so Rust's one
    // access-path decision must do the same rather than exposing the helper's
    // temporary refusal.
    session
        .run(
            "CREATE TABLE generated_point (a BIGINT, b BIGINT, \
             c BIGINT GENERATED ALWAYS AS (a + b) VIRTUAL, UNIQUE KEY uk(c))",
        )
        .unwrap();
    session
        .run("INSERT INTO generated_point (a, b) VALUES (1, 2), (4, 5)")
        .unwrap();
    let generated_plan =
        row_text(session.run("EXPLAIN SELECT a, c FROM generated_point WHERE c IN (3, 9, 3)"));
    let generated_batch = generated_plan
        .iter()
        .find(|row| row[0].contains("Batch_Point_Get"))
        .unwrap_or_else(|| {
            panic!("generated unique key must use batch point get: {generated_plan:?}")
        });
    assert_eq!(
        &generated_batch[3..],
        [
            "table:generated_point, index:uk(c)",
            "keep order:false, desc:false"
        ]
    );
    let mut generated_rows =
        row_text(session.run("SELECT a, c FROM generated_point WHERE c IN (3, 9, 3)"));
    generated_rows.sort();
    assert_eq!(
        generated_rows,
        [
            vec!["1".to_owned(), "3".to_owned()],
            vec!["4".to_owned(), "9".to_owned()]
        ]
    );

    // TiDB prints
    //   TableReader_5 | 10000.00 | root | | data:TableFullScan_4
    //   └─TableFullScan_4 | 10000.00 | cop[tikv] | table:t | keep order:false, stats:pseudo
    // and so does this tier now: `convertToTableScan` puts every base-table
    // read in a `CopTask` and `ConvertToRootTask` caps it with the reader
    // (`pkg/planner/core/find_best_task.go:2953`,
    // `pkg/planner/core/operator/physicalop/task_base.go:504`). Like Go, it
    // eliminates the identity projection over `SELECT *`. The only remaining
    // difference is the CHILD ID inside `data:` -- ids are build order here
    // and plan-construction order in Go, so this tier prints the child's
    // NAME alone (as it already does for `data:TopN`, `data:StreamAgg`).
    assert_eq!(
        row_text(session.run("EXPLAIN SELECT * FROM t")),
        vec![
            vec![
                "TableReader_2".to_owned(),
                "10000.00".to_owned(),
                "root".to_owned(),
                String::new(),
                "data:TableFullScan".to_owned(),
            ],
            vec![
                "└─TableFullScan_1".to_owned(),
                "10000.00".to_owned(),
                "cop[tikv]".to_owned(),
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
                "TableReader_3".to_owned(),
                "3333.33".to_owned(),
                "root".to_owned(),
                String::new(),
                "data:Selection".to_owned(),
            ],
            vec![
                "└─Selection_2".to_owned(),
                "3333.33".to_owned(),
                "cop[tikv]".to_owned(),
                String::new(),
                // Go's own function-call rendering, captured:
                // gt(test.t.b, "x").
                "gt(test.t.b, \"x\")".to_owned(),
            ],
            vec![
                "  └─TableFullScan_1".to_owned(),
                "10000.00".to_owned(),
                "cop[tikv]".to_owned(),
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
    // The root/cop TopN pair, reader boundary, row estimates, and operator
    // details below are the captured Go shape. The identity projection is
    // eliminated.
    assert_eq!(
        row_text(session.run("EXPLAIN SELECT * FROM t ORDER BY c LIMIT 10")),
        vec![
            vec![
                "TopN_4".to_owned(),
                "10.00".to_owned(),
                "root".to_owned(),
                String::new(),
                "test.t.c, offset:0, count:10".to_owned(),
            ],
            vec![
                "└─TableReader_3".to_owned(),
                "10.00".to_owned(),
                "root".to_owned(),
                String::new(),
                "data:TopN".to_owned(),
            ],
            vec![
                "  └─TopN_2".to_owned(),
                "10.00".to_owned(),
                "cop[tikv]".to_owned(),
                String::new(),
                "test.t.c, offset:0, count:10".to_owned(),
            ],
            vec![
                "    └─TableFullScan_1".to_owned(),
                "10000.00".to_owned(),
                "cop[tikv]".to_owned(),
                "table:t".to_owned(),
                "keep order:false, stats:pseudo".to_owned(),
            ],
        ]
    );

    // ORDER BY with no LIMIT above it still builds a plain Sort: there is
    // nothing for the rule to fuse, so nothing enters the cop task and the
    // reader caps a bare scan.
    assert_eq!(
        row_text(session.run("EXPLAIN SELECT * FROM t ORDER BY c"))
            .into_iter()
            .map(|row| row[0].clone())
            .collect::<Vec<_>>(),
        vec![
            "Sort_3".to_owned(),
            "└─TableReader_2".to_owned(),
            "  └─TableFullScan_1".to_owned(),
        ]
    );

    // GROUP BY. The 8000.00 is Go's stats-less distinctFactor result. The
    // projection, root/coprocessor aggregate pair, and reader boundary match
    // the current Go plan.
    assert_eq!(
        row_text(session.run("EXPLAIN SELECT c, COUNT(*) FROM t GROUP BY c")),
        vec![
            vec![
                "Projection_5".to_owned(),
                "8000.00".to_owned(),
                "root".to_owned(),
                String::new(),
                "test.t.c, Column#0".to_owned(),
            ],
            vec![
                "└─HashAgg_4".to_owned(),
                "8000.00".to_owned(),
                "root".to_owned(),
                String::new(),
                "group by:test.t.c, funcs:count(Column#0)->Column#0, \
                 funcs:firstrow(test.t.c)->test.t.c"
                    .to_owned(),
            ],
            vec![
                "  └─TableReader_3".to_owned(),
                "8000.00".to_owned(),
                "root".to_owned(),
                String::new(),
                "data:HashAgg".to_owned(),
            ],
            vec![
                "    └─HashAgg_2".to_owned(),
                "8000.00".to_owned(),
                "cop[tikv]".to_owned(),
                String::new(),
                "group by:test.t.c, funcs:count(1)->Column#0".to_owned(),
            ],
            vec![
                "      └─TableFullScan_1".to_owned(),
                "10000.00".to_owned(),
                "cop[tikv]".to_owned(),
                "table:t".to_owned(),
                "keep order:false, stats:pseudo".to_owned(),
            ],
        ]
    );
}

/// A pushed `WHERE` keeps its `Selection` BETWEEN the partial aggregate and
/// the scan, all three in the same coprocessor task.
///
/// TiDB's own recording of the same shape
/// (`tests/integrationtest/r/explain_easy.result:208-214`):
///
/// ```text
/// StreamAgg             root       funcs:count(1)->Column
/// └─StreamAgg           root       funcs:count(Column)->Column
///   └─TableReader       root       data:StreamAgg
///     └─StreamAgg       cop[tikv]  funcs:count(1)->Column
///       └─Selection     cop[tikv]  eq(explain_easy.t1.c3, 100)
///         └─TableFullScan cop[tikv] table:t1  keep order:false, stats:pseudo
/// ```
///
/// The partial aggregate goes to the top of the COP TASK, not directly onto
/// the scan. Requiring a bare scan under it left this shape unprintable.
///
/// The constant is the refined one for the reason
/// [`crate::tests_compare_refinement`] states: Go runs `refineArgs` before it
/// builds the comparison at all, so `int_col > '10ab'` is `gt(..., 10)`
/// everywhere -- in the plan text, and in what the scan is asked to evaluate.
#[test]
fn a_pushed_where_keeps_its_cop_selection_under_the_partial_aggregate() {
    let mut session = Session::new();
    session.run("CREATE TABLE t (a INT, b INT)").unwrap();
    session
        .run("INSERT INTO t VALUES (1,1),(2,1),(3,2),(10,4)")
        .unwrap();
    let plan = |session: &mut Session, sql: &str| -> Vec<String> {
        row_text(session.run(sql))
            .into_iter()
            .map(|row| row.join("|"))
            .collect()
    };
    assert_eq!(
        plan(
            &mut session,
            "EXPLAIN SELECT count(*) FROM t WHERE a > '10ab'"
        ),
        [
            "StreamAgg_5|1.00|root||funcs:count(Column#0)->Column#0",
            "└─TableReader_4|1.00|root||data:StreamAgg",
            "  └─StreamAgg_3|1.00|cop[tikv]||funcs:count(1)->Column#0",
            "    └─Selection_2|3333.33|cop[tikv]||gt(test.t.a, 10)",
            "      └─TableFullScan_1|10000.00|cop[tikv]|table:t|keep order:false, stats:pseudo",
        ]
    );
    // The answer the refined plan gives is the answer the string gave.
    assert_eq!(
        row_text(session.run("SELECT count(*) FROM t WHERE a > '10ab'")),
        [["0"]]
    );
    assert_eq!(
        row_text(session.run("SELECT count(*) FROM t WHERE a > 1")),
        [["3"]]
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
/// root (a pass-through). All three rows are printed here, with the same
/// three counts: the reader boundary is recorded now, so the pass-through
/// row exists and carries its child's real count.
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
    assert_eq!(rows[0][0], "TableReader_3");
    assert_eq!(rows[0][2], "2"); // the pass-through reader's own count.
    assert_eq!(rows[1][0], "└─Selection_2");
    assert_eq!(rows[1][2], "2"); // actRows: real, not the 3333.33 estimate.
    assert_eq!(rows[2][0], "  └─TableFullScan_1");
    assert_eq!(rows[2][2], "4");
    assert_eq!(rows[0][3], "root");
    assert_eq!(rows[1][3], "cop[tikv]");
    assert_eq!(rows[2][3], "cop[tikv]");
    // This tier collects no runtime timing/memory/disk counters at all.
    for row in &rows {
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
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0][0], "Delete_2");
    assert_eq!(rows[0][2], "0");
    assert_eq!(rows[1][0], "└─Point_Get_1");
    assert_eq!(rows[1][2], "1");

    assert_eq!(
        row_text(session.run("SELECT a FROM t ORDER BY a")),
        vec![vec!["1".to_owned()], vec!["3".to_owned()]]
    );
}

/// `EXPLAIN ANALYZE` of a `Point_Get`/`Batch_Point_Get`/`IndexRangeScan`
/// access path: real `actRows`, not `N/A`. Exact point and batch predicates
/// take Go's replacement fast-plan path; an index range may retain a reader
/// wrapper. A point hit is `1` and a miss `0`; BatchPointGet reports the
/// handles found; the index range reports the rows it covers.
#[test]
fn explain_analyze_fast_paths_real_act_rows() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE pg (a INT PRIMARY KEY, b INT, KEY idx_b(b))")
        .unwrap();
    session
        .run("INSERT INTO pg VALUES (1,10),(2,20),(3,30),(4,40)")
        .unwrap();

    let act_rows = |rows: Vec<Vec<String>>, operator: &str| {
        rows.into_iter()
            .find(|row| row[0].contains(operator))
            .unwrap_or_else(|| panic!("missing {operator}"))[2]
            .clone()
    };
    assert_eq!(
        act_rows(
            row_text(session.run("EXPLAIN ANALYZE SELECT * FROM pg WHERE a = 2")),
            "Point_Get",
        ),
        "1"
    );
    assert_eq!(
        act_rows(
            row_text(session.run("EXPLAIN ANALYZE SELECT * FROM pg WHERE a = 999")),
            "Point_Get",
        ),
        "0"
    );
    assert_eq!(
        act_rows(
            row_text(session.run("EXPLAIN ANALYZE SELECT * FROM pg WHERE a IN (1,2,3)")),
            "Batch_Point_Get",
        ),
        "3"
    );
    assert_eq!(
        act_rows(
            row_text(session.run("EXPLAIN ANALYZE SELECT * FROM pg WHERE b > 15 AND b < 35")),
            "IndexRangeScan",
        ),
        "2"
    );
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
    let grouped = rows
        .iter()
        .find(|row| row[0].contains("HashAgg"))
        .expect("grouped aggregate");
    assert_eq!(grouped[2], "3");

    let rows = row_text(session.run("EXPLAIN ANALYZE SELECT DISTINCT a FROM g"));
    assert_eq!(rows[0][2], "3");
}

/// `EXPLAIN ANALYZE INSERT ... SELECT`'s source gets the SAME real
/// `actRows` a plain `EXPLAIN ANALYZE SELECT` of that query would --
/// captured: `insert into dst select * from src where a > 1` on
/// `src = (1),(2),(3)` reports `2` for the `Selection` (the
/// `WHERE`-matching rows) over the real `3`-row `TableFullScan`, computed
/// before the insert writes anything. The pass-through reader Go prints
/// between the `Insert` and the cop `Selection` is recorded here too, with
/// its child's count.
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
    assert_eq!(rows[1][0], "└─TableReader_3");
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
/// The point plan replaces the ordinary Selection pipeline, matching Go's
/// `tryUpdatePointPlan` and `tryDeletePointPlan` shapes.
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
                "Update_2".to_owned(),
                "N/A".to_owned(),
                "root".to_owned(),
                String::new(),
                "N/A".to_owned(),
            ],
            vec![
                "└─Point_Get_1".to_owned(),
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
                "Delete_2".to_owned(),
                "N/A".to_owned(),
                "root".to_owned(),
                String::new(),
                "N/A".to_owned(),
            ],
            vec![
                "└─Point_Get_1".to_owned(),
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
/// operator's `_N` build-order suffix stripped. The exact handle predicate
/// takes the replacement fast plan, so both formats contain one point-get.
#[test]
fn explain_brief_format_strips_operator_ids() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE t (a BIGINT PRIMARY KEY)")
        .unwrap();
    session.run("INSERT INTO t VALUES (1)").unwrap();

    assert_eq!(
        row_text(session.run("EXPLAIN FORMAT = 'brief' SELECT * FROM t WHERE a = 1")),
        vec![vec![
            "Point_Get".to_owned(),
            "1.00".to_owned(),
            "root".to_owned(),
            "table:t".to_owned(),
            "handle:1".to_owned(),
        ]]
    );
    assert_eq!(
        row_text(session.run("EXPLAIN FORMAT = 'row' SELECT * FROM t WHERE a = 1"))[0],
        vec![
            "Point_Get_1".to_owned(),
            "1.00".to_owned(),
            "root".to_owned(),
            "table:t".to_owned(),
            "handle:1".to_owned(),
        ]
    );
}

/// `EXPLAIN ANALYZE` builds and executes the physical `Union` tree for a
/// `UNION ALL`: each branch retains its own source counters and the union
/// reports the rows it emits. Go's generic `buildExplain` builds the target
/// plan regardless of whether the query is a SELECT or a set operation.
#[test]
fn explain_analyze_union_all_executes_and_meters_each_term() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE t (a BIGINT PRIMARY KEY)")
        .unwrap();
    session.run("INSERT INTO t VALUES (1),(2),(3),(4)").unwrap();

    let rows = row_text(session.run(
        "EXPLAIN ANALYZE (SELECT a FROM t WHERE a <= 2) UNION ALL (SELECT a FROM t WHERE a >= 3)",
    ));
    assert_eq!(rows.len(), 7);
    assert!(rows[0][0].starts_with("Union_"));
    assert_eq!(rows[0][2], "4");
    assert_eq!(
        rows.iter()
            .skip(1)
            .map(|row| row[2].as_str())
            .collect::<Vec<_>>(),
        vec!["2", "2", "2", "2", "2", "2"]
    );
}

/// Plain `EXPLAIN` describes the same physical `Union` tree but must not run
/// either operand. The plan-only trace records the branches before their
/// executors are drained, which is Go's `ExplainExec` build-only path.
#[test]
fn explain_union_all_records_each_term_without_execution() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE t (a BIGINT PRIMARY KEY)")
        .unwrap();
    session.run("INSERT INTO t VALUES (1),(2),(3),(4)").unwrap();

    let rows = row_text(
        session
            .run("EXPLAIN (SELECT a FROM t WHERE a <= 2) UNION ALL (SELECT a FROM t WHERE a >= 3)"),
    );
    assert_eq!(rows.len(), 7);
    assert!(rows[0][0].starts_with("Union_"));
    assert_eq!(rows[0][1], "3335.33");
    assert_eq!(rows[0][2], "root");
    assert_eq!(
        rows.iter()
            .skip(1)
            .map(|row| row[2].as_str())
            .collect::<Vec<_>>(),
        vec![
            "root",
            "cop[tikv]",
            "cop[tikv]",
            "root",
            "cop[tikv]",
            "cop[tikv]"
        ]
    );
    assert_eq!(
        row_text(session.run("SELECT a FROM t ORDER BY a")),
        vec![
            vec!["1".to_owned()],
            vec!["2".to_owned()],
            vec!["3".to_owned()],
            vec!["4".to_owned()],
        ]
    );
}

/// Go's `buildUnion` (`pkg/planner/core/logical_plan_builder.go`) builds a
/// `Union` of the distinct operands and places `HashAgg` above it to remove
/// duplicates.  Plain EXPLAIN must record that physical shape without
/// draining either operand.
#[test]
fn explain_union_distinct_records_its_hash_aggregation() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE t (a BIGINT PRIMARY KEY)")
        .unwrap();
    session.run("INSERT INTO t VALUES (1),(2),(3),(4)").unwrap();

    let rows = row_text(
        session.run("EXPLAIN (SELECT a FROM t WHERE a <= 2) UNION (SELECT a FROM t WHERE a >= 2)"),
    );
    assert_eq!(rows.len(), 8);
    assert!(rows[0][0].starts_with("HashAgg_"));
    assert_eq!(rows[0][2], "root");
    assert!(rows[1][0].contains("Union_"));
    assert_eq!(rows[1][2], "root");
    assert_eq!(
        rows.iter()
            .skip(2)
            .map(|row| row[2].as_str())
            .collect::<Vec<_>>(),
        vec![
            "root",
            "cop[tikv]",
            "cop[tikv]",
            "root",
            "cop[tikv]",
            "cop[tikv]"
        ]
    );
    assert_eq!(
        row_text(session.run("SELECT a FROM t ORDER BY a")),
        vec![
            vec!["1".to_owned()],
            vec!["2".to_owned()],
            vec!["3".to_owned()],
            vec!["4".to_owned()],
        ]
    );
}

/// `EXPLAIN ANALYZE` meters the two physical stages separately: the `Union`
/// receives every branch row, while the `HashAgg` emits only unique rows.
#[test]
fn explain_analyze_union_distinct_separates_input_and_output_rows() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE t (a BIGINT PRIMARY KEY)")
        .unwrap();
    session.run("INSERT INTO t VALUES (1),(2),(3),(4)").unwrap();

    let rows = row_text(session.run(
        "EXPLAIN ANALYZE (SELECT a FROM t WHERE a <= 2) UNION (SELECT a FROM t WHERE a >= 2)",
    ));
    assert_eq!(rows.len(), 8);
    assert!(rows[0][0].starts_with("HashAgg_"));
    assert_eq!(rows[0][2], "4");
    assert!(rows[1][0].contains("Union_"));
    assert_eq!(rows[1][2], "5");
    assert_eq!(
        rows.iter()
            .skip(2)
            .map(|row| row[2].as_str())
            .collect::<Vec<_>>(),
        vec!["2", "2", "2", "3", "3", "3"]
    );
}

/// Go treats a DISTINCT union as a boundary: in `a UNION b UNION ALL c`, the
/// first two terms feed `HashAgg(Union(...))`, then a second `Union` appends
/// `c`. `buildUnion` deliberately preserves this order when it divides the
/// terms into a distinct prefix and an ALL suffix.
#[test]
fn explain_analyze_mixed_union_keeps_the_distinct_prefix_separate() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE t (a BIGINT PRIMARY KEY)")
        .unwrap();
    session.run("INSERT INTO t VALUES (1),(2),(3),(4)").unwrap();

    let rows = row_text(session.run(
        "EXPLAIN ANALYZE \
         (SELECT a FROM t WHERE a <= 2) UNION \
         (SELECT a FROM t WHERE a >= 2 AND a <= 3) UNION ALL \
         (SELECT a FROM t WHERE a = 4)",
    ));
    assert_eq!(rows.len(), 10);
    assert!(rows[0][0].starts_with("Union_"));
    assert_eq!(rows[0][2], "4");
    assert!(rows[1][0].contains("HashAgg_"));
    assert_eq!(rows[1][2], "3");
    assert!(rows[2][0].contains("Union_"));
    assert_eq!(rows[2][2], "4");
    assert_eq!(
        rows.iter()
            .skip(3)
            .map(|row| row[2].as_str())
            .collect::<Vec<_>>(),
        vec!["2", "2", "2", "2", "2", "2", "1"]
    );
}

/// EXPLAIN still refuses forms this tier cannot plan honestly and format names
/// Go itself does not recognize.
#[test]
fn explain_refuses_what_it_cannot_plan() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE t (a BIGINT PRIMARY KEY)")
        .unwrap();

    // INTERSECT/EXCEPT use join-like physical plans in Go and still need their
    // own trace constructors; only UNION DISTINCT is handled above.
    assert!(matches!(
        session.run("EXPLAIN ANALYZE (SELECT a FROM t) INTERSECT (SELECT a FROM t)"),
        Err(DriverError::Unsupported(reason)) if reason == "EXPLAIN ANALYZE of this set operation is not supported yet"
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
/// Strict projection elimination removes the identity projection here, as in
/// Go. The read is a cop task under its `TableReader`, so the shape matches
/// the capture for every conjunct this tier's push-down catalog admits.
///
/// The middle statement is the one place the two still differ, and the
/// difference is the CATALOG, not the boundary: Go's coprocessor evaluates
/// `plus(int, int)` and this tier's `tidb_expr::pushdown_catalog` does not,
/// so `lt(plus(b, 1), 10)` is `CopTask.RootTaskConds` here -- a root
/// `Selection` above the reader, which is exactly where Go puts a condition
/// `expression.PushDownExprs` refuses
/// (`pkg/planner/core/operator/physicalop/task.go:47`). The estimate is
/// unaffected: Go's captured `2666.67` is what the two halves compose to,
/// because Go prices its own two halves the same way.
#[test]
fn pushing_a_predicate_into_the_scan_keeps_the_captured_plan_shape() {
    let mut session = Session::new();
    session.run("CREATE TABLE t (a BIGINT, b BIGINT)").unwrap();

    for (sql, printed) in [
        ("SELECT a, b FROM t WHERE a > 5", "gt(test.t.a, 5)"),
        (
            "SELECT a, b FROM t WHERE a > 5 OR b < 10",
            "or(gt(test.t.a, 5), lt(test.t.b, 10))",
        ),
    ] {
        let rows = row_text(session.run(&format!("EXPLAIN {sql}")));
        assert_eq!(rows.len(), 3, "{sql}");
        assert_eq!(rows[0][0], "TableReader_3", "{sql}");
        assert_eq!(rows[0][2], "root", "{sql}");
        assert_eq!(rows[0][4], "data:Selection", "{sql}");
        assert_eq!(rows[1][0], "\u{2514}\u{2500}Selection_2", "{sql}");
        assert_eq!(rows[1][2], "cop[tikv]", "{sql}");
        assert_eq!(rows[1][4], printed, "{sql}");
        assert_eq!(rows[2][0], "  \u{2514}\u{2500}TableFullScan_1", "{sql}");
        assert_eq!(rows[2][1], "10000.00", "{sql}");
        assert_eq!(rows[2][2], "cop[tikv]", "{sql}");
    }

    // The conjunct the catalog cannot lower stays at root, over the same
    // reader; the conjunct it can keeps its cop `Selection`.
    let rows = row_text(session.run("EXPLAIN SELECT a, b FROM t WHERE a > 5 AND b + 1 < 10"));
    assert_eq!(rows.len(), 4);
    assert_eq!(rows[0][0], "Selection_4");
    assert_eq!(rows[0][2], "root");
    assert_eq!(rows[0][4], "lt(plus(test.t.b, 1), 10)");
    assert_eq!(rows[1][0], "\u{2514}\u{2500}TableReader_3");
    assert_eq!(rows[2][0], "  \u{2514}\u{2500}Selection_2");
    assert_eq!(rows[2][2], "cop[tikv]");
    assert_eq!(rows[2][4], "gt(test.t.a, 5)");
    // Go's captured estimates, both of them.
    assert_eq!(rows[0][1], "2666.67");
    assert_eq!(rows[2][1], "3333.33");

    // The single `>` keeps Go's captured 3333.33 estimate, which the split
    // must not disturb.
    let rows = row_text(session.run("EXPLAIN SELECT a, b FROM t WHERE a > 5"));
    assert_eq!(rows[0][1], "3333.33");
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
    assert_eq!(rows[0][0], "TableReader_3");
    assert_eq!(rows[1][0], "\u{2514}\u{2500}Selection_2");
    assert_eq!(rows[1][2], "2", "rows that passed the predicate");
    assert_eq!(rows[2][0], "  \u{2514}\u{2500}TableFullScan_1");
    assert_eq!(rows[2][2], "4", "rows the scan read, before filtering");
}

/// `EXPLAIN` of a hash join, against a `pkg/executor` mock-store capture on
/// the same statistics-free schema (`TestZZDumpHashJoin`).
///
/// Every assertion below pins the join row's own `operator info` cell plus the
/// `(Build)`/`(Probe)` labels. Like Go, the labels now sit on reader rows,
/// whose access-object cells are empty; the table names live on their scan
/// children.
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
            "(Build) ",
            "(Probe) ",
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
            "left outer join, left side:TableReader, equal:[eq(test.hj1.a, test.hj2.a)]",
            "(Build) ",
            "(Probe) ",
        ]
    );
    assert_eq!(
        join_shape(
            &mut session,
            "EXPLAIN FORMAT='brief' SELECT * FROM hj1 RIGHT JOIN hj2 ON hj1.a = hj2.a"
        ),
        vec![
            "right outer join, left side:TableReader, equal:[eq(test.hj1.a, test.hj2.a)]",
            "(Build) ",
            "(Probe) ",
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
        "left outer join, left side:TableReader, equal:[eq(test.hj1.s, test.hj2.s)]"
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
/// Go discards the operators above the read too, since the whole
/// `DataSource` task becomes the dual. The local fast path does the same.
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
        assert_eq!(leaf[0], "TableDual", "{where_clause}: {rows:?}");
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
    assert_eq!(leaf[0], "└─IndexRangeScan");
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
    let scan_of = |session: &mut Session, query: &str, operator: &str| {
        let rows = row_text(session.run(&format!("EXPLAIN FORMAT = 'brief' {query}")));
        rows.into_iter()
            .find(|row| row[0].contains(operator))
            .unwrap_or_else(|| panic!("{query} has no {operator}"))
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
        let range = scan_of(&mut session, query, "IndexRangeScan");
        assert!(
            range[4].starts_with(expected),
            "{query} gave {}, expected it to start with {expected}",
            range[4],
        );
    }

    // The control, and the thing that is genuinely different: WITHOUT the
    // hint, the same predicate takes a full scan on cost grounds. If range
    // building ever did depend on coveringness, this line would be the only
    // one above that still passed.
    scan_of(&mut session, "SELECT * FROM d WHERE a > 5", "TableFullScan");
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

    // TiDB rewrites an equi-join's nullable keys with `not(isnull(k))`
    // before estimating: one key leaves 9990 rows on each side.
    for equi in [
        "SELECT * FROM j1 JOIN j2 ON j1.a = j2.a",
        "SELECT * FROM j1 LEFT JOIN j2 ON j1.a = j2.a",
    ] {
        assert_eq!(join_est(&mut session, equi), "12487.50", "{equi}");
    }
    // TWO nullable keys leave 9980.01 rows per side; the join NDV remains the
    // maximum over the keys rather than their product.
    assert_eq!(
        join_est(
            &mut session,
            "SELECT * FROM j1 JOIN j2 ON j1.a = j2.a AND j1.b = j2.b"
        ),
        "12475.01"
    );

    // A non-equality `ON` is a CARTESIAN join with an `other cond:`, so it
    // takes the product arm. TiDB prints 99800100.00 = 9990 * 9990: `gt`
    // rejects nulls, so its rewrite fires here too.
    assert_eq!(
        join_est(&mut session, "SELECT * FROM j1 JOIN j2 ON j1.a > j2.a"),
        "99800100.00"
    );

    // An ANALYZEd side contributes its real row count and histogram NDV.
    session
        .run("INSERT INTO j1 VALUES (1,1),(2,2),(3,3)")
        .unwrap();
    session.run("ANALYZE TABLE j1").unwrap();
    assert_eq!(
        join_est(&mut session, "SELECT * FROM j1 JOIN j2 ON j1.a = j2.a"),
        "3.75"
    );
}

/// `id IS NULL` over an INTEGER PRIMARY KEY selects nothing, and the plan says
/// so instead of reading the whole table.
///
/// Go `points2TableRanges` (`pkg/util/ranger/ranger.go:466`) passes
/// `skipNull = true` into `convertPointsInPlace`, which DROPS any interval
/// whose END point is `KindNull` (`:102-104`) while converting a NULL START
/// point to the domain minimum, inclusive. A row handle is never NULL, so the
/// `[NULL, NULL]` pair `IS NULL` produces leaves zero ranges.
///
/// This tier mapped the NULL high bound to `i64::MAX` instead, so the pair
/// became `[MinInt64, MaxInt64]` -- the RIGHT rows (the `WHERE` above still
/// filters) read the MOST EXPENSIVE possible way. Captured:
///
/// ```text
/// explain select * from t where id is null      TableDual_6 | 0.00 | rows:0
/// explain select * from t where id <=> null     TableDual_5 | 1.00 | rows:0
/// explain select * from t where id is not null  TableReader -> TableFullScan
/// select * from t where id is null              (no rows)
/// ```
///
/// `IS NOT NULL` is the control: it must STAY a full scan, because dropping
/// every NULL-ended interval there would be dropping nothing.
#[test]
fn an_is_null_on_an_integer_handle_is_a_table_dual_not_a_full_scan() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .unwrap();
    session.run("INSERT INTO t VALUES (1,10),(2,20)").unwrap();

    for where_clause in ["id IS NULL", "id <=> NULL"] {
        let rows = row_text(session.run(&format!(
            "EXPLAIN FORMAT = 'brief' SELECT * FROM t WHERE {where_clause}"
        )));
        let leaf = rows.last().expect("a plan has at least one row");
        assert!(leaf[0].ends_with("TableDual"), "{where_clause}: {leaf:?}");
        assert_eq!(leaf[4], "rows:0", "{where_clause}");
        assert!(row_text(session.run(&format!("SELECT * FROM t WHERE {where_clause}"))).is_empty());
    }

    // The control: `IS NOT NULL` keeps its full scan and its rows.
    let rows =
        row_text(session.run("EXPLAIN FORMAT = 'brief' SELECT * FROM t WHERE id IS NOT NULL"));
    assert!(
        rows.iter().any(|r| r[0].ends_with("TableFullScan")),
        "{rows:?}"
    );
    assert_eq!(
        row_text(session.run("SELECT * FROM t WHERE id IS NOT NULL")),
        [["1", "10"], ["2", "20"]]
    );
}

/// `LIKE 'abc_%'` excludes its LOW bound on a NON-PAD-SPACE collation, and
/// keeps it on a PAD SPACE one.
///
/// Go `newBuildFromPatternLike` (`pkg/util/ranger/points.go:775-788`) sets
/// `exclude = true` for a `_` wildcard -- the prefix is strictly shorter than
/// anything that matches -- but ONLY when `!collate.IsPadSpaceCollation`.
/// Under a PAD SPACE collation the stored index key has its trailing spaces
/// trimmed, so `'abc'` and `'abc   '` share a key and excluding the bound
/// would MISS a matching row.
///
/// `IsPadSpaceCollation` (`pkg/util/collate/collate.go:363`) is a three-name
/// exception list -- `binary`, `utf8mb4_0900_ai_ci`, `utf8mb4_0900_bin` -- and
/// `binary` being one of them is what makes a `VARBINARY` key take the
/// exclusive bound. A comment here used to claim TiDB's own default
/// collations all pad, which is true of `utf8mb4_bin` and false of `binary`.
///
/// Captured:
///
/// ```text
/// create table b(a varbinary(20), key(a));
/// explain select * from b where a like 'abc_%'   range:("abc","abd")
/// explain select * from b where a like 'abc%'    range:["abc","abd")
/// create table c(a varchar(20), key(a));
/// explain select * from c where a like 'abc_%'   range:["abc","abd")
/// ```
#[test]
fn a_like_underscore_excludes_its_low_bound_only_on_a_non_pad_collation() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE b (a VARBINARY(20), KEY(a))")
        .unwrap();
    session
        .run("CREATE TABLE c (a VARCHAR(20), KEY(a))")
        .unwrap();
    session
        .run("INSERT INTO b VALUES ('abc'), ('abcd'), ('abd')")
        .unwrap();
    session
        .run("INSERT INTO c VALUES ('abc'), ('abcd'), ('abd')")
        .unwrap();

    let range_of = |session: &mut Session, sql: &str| -> String {
        row_text(session.run(sql))
            .into_iter()
            .find_map(|row| {
                row.iter()
                    .find(|cell| cell.starts_with("range:"))
                    .map(|cell| cell.split(',').take(2).collect::<Vec<_>>().join(","))
            })
            .unwrap_or_else(|| panic!("no range in the plan for `{sql}`"))
    };

    // `binary` is NOT a PAD SPACE collation: the low bound is EXCLUSIVE.
    assert_eq!(
        range_of(
            &mut session,
            "EXPLAIN FORMAT = 'brief' SELECT * FROM b USE INDEX(a) WHERE a LIKE 'abc_%'"
        ),
        "range:(\"abc\",\"abd\")"
    );
    // `%` alone never excludes, on either collation.
    assert_eq!(
        range_of(
            &mut session,
            "EXPLAIN FORMAT = 'brief' SELECT * FROM b USE INDEX(a) WHERE a LIKE 'abc%'"
        ),
        "range:[\"abc\",\"abd\")"
    );
    // `utf8mb4_bin` PADS: the bound stays inclusive.
    assert_eq!(
        range_of(
            &mut session,
            "EXPLAIN FORMAT = 'brief' SELECT * FROM c USE INDEX(a) WHERE a LIKE 'abc_%'"
        ),
        "range:[\"abc\",\"abd\")"
    );

    // The narrower range must not lose a row: `LIKE` still runs above the
    // scan, so both tables answer the same set.
    assert_eq!(
        row_text(session.run("SELECT a FROM b USE INDEX(a) WHERE a LIKE 'abc_%'")),
        [["abcd"]]
    );
    assert_eq!(
        row_text(session.run("SELECT a FROM c USE INDEX(a) WHERE a LIKE 'abc_%'")),
        [["abcd"]]
    );
}

/// `a IS NULL` over a `NOT NULL` INDEX column plans a `TableDual`, the index
/// sibling of the integer-handle case above.
///
/// Go `points2Ranges` (`pkg/util/ranger/ranger.go:129`) passes
/// `skipNull = mysql.HasNotNullFlag(newTp.GetFlag())` into
/// `convertPointsInPlace`, which then drops any interval ending at NULL. Only
/// the FIRST index column gets this: `appendPoints2Ranges` (`:295`) passes
/// `false`, because a NULL there is a real key byte inside a wider range.
///
/// This tier's `points_to_ranges` had no nullability input at all, so
/// `a IS NULL` on a `NOT NULL` key scanned `[NULL,NULL]` -- a range no row can
/// live in, read anyway. Captured:
///
/// ```text
/// create table nn(id int primary key, a int not null, key(a));
/// explain select * from nn use index(a) where a is null
///   TableDual_6 | 0.00 | root | | rows:0
/// ```
#[test]
fn an_is_null_on_a_not_null_index_column_is_a_table_dual() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE nn (id INT PRIMARY KEY, a INT NOT NULL, KEY(a))")
        .unwrap();
    session
        .run("CREATE TABLE nu (id INT PRIMARY KEY, a INT, KEY(a))")
        .unwrap();
    session.run("INSERT INTO nn VALUES (1,10)").unwrap();
    session
        .run("INSERT INTO nu VALUES (1,10),(2,NULL)")
        .unwrap();

    let rows = row_text(
        session.run("EXPLAIN FORMAT = 'brief' SELECT * FROM nn USE INDEX(a) WHERE a IS NULL"),
    );
    let leaf = rows.last().expect("a plan has at least one row");
    assert!(leaf[0].ends_with("TableDual"), "{leaf:?}");
    assert_eq!(leaf[4], "rows:0");
    assert!(row_text(session.run("SELECT * FROM nn USE INDEX(a) WHERE a IS NULL")).is_empty());

    // The control: a NULLABLE key keeps its `[NULL,NULL]` range and its row,
    // because there the interval really can hold one.
    let rows = row_text(
        session.run("EXPLAIN FORMAT = 'brief' SELECT * FROM nu USE INDEX(a) WHERE a IS NULL"),
    );
    let leaf = rows.last().expect("a plan has at least one row");
    assert!(leaf[0].ends_with("IndexRangeScan"), "{leaf:?}");
    assert!(leaf[4].starts_with("range:[NULL,NULL]"), "{leaf:?}");
    assert_eq!(
        row_text(session.run("SELECT id FROM nu USE INDEX(a) WHERE a IS NULL")),
        [["2"]]
    );
}

/// Go's table-scan penalty (`getTableScanPenalty`,
/// `pkg/planner/core/plan_cost_ver2.go`, ported in
/// `tidb_executor::access_cost::table_scan_penalty_rows`) charges a
/// full-range table scan a SECOND scan's worth of rows whenever the
/// statistics behind it cannot be trusted -- pseudo, stale, or outrun by
/// `modify_count`. Under pseudo statistics a covering index and the table it
/// covers cost within a few percent of each other, so this penalty is the
/// whole reason real TiDB reads the index.
///
/// Every row below was captured from a real TiDB session through
/// `rust/difftests/gorun`:
///
/// ```text
/// create table t(a bigint, b bigint, key idx(a, b));
/// explain format = 'plan_tree' select * from t;
///   IndexReader        root       index:IndexFullScan
///   └─IndexFullScan    cop[tikv]  table:t, index:idx(a, b)  keep order:false, stats:pseudo
///
/// create table t2(a bigint, b bigint, c bigint, key kb(b));
/// explain format = 'plan_tree' select * from t2;
///   TableReader        root       data:TableFullScan
///   └─TableFullScan    cop[tikv]  table:t2  keep order:false, stats:pseudo
/// explain format = 'plan_tree' select b from t2;
///   IndexReader        root       index:IndexFullScan
///   └─IndexFullScan    cop[tikv]  table:t2, index:kb(b)  keep order:false, stats:pseudo
///
/// create table t3(a bigint primary key, b bigint, c varchar(40));
/// explain format = 'plan_tree' select * from t3 where a > 5;
///   TableReader        root       data:TableRangeScan
///   └─TableRangeScan   cop[tikv]  table:t3  range:(5,+inf], keep order:false, stats:pseudo
/// ```
///
/// The three NEGATIVE rows are the acceptance criterion, not a footnote. The
/// recorded TiDB plans this workspace replays contain roughly nine full scans
/// for every index read, so a penalty that merely made indexes attractive
/// would trade a large body of correct agreements for divergences. `t2` reads
/// its table for `SELECT *` because `kb(b)` covers nothing of `c` and a full
/// index scan plus a row lookup can never beat the scan it would do anyway;
/// the same index wins the moment the statement reads only `b`. And a table
/// path the ranger NARROWED is exempt outright (Go's `hasFullRangeScan`),
/// because the range is the evidence the penalty exists to demand.
#[test]
fn a_full_table_scan_under_pseudo_stats_pays_gos_risk_penalty() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE t (a BIGINT, b BIGINT, KEY idx(a, b))")
        .unwrap();
    session
        .run("CREATE TABLE t2 (a BIGINT, b BIGINT, c BIGINT, KEY kb(b))")
        .unwrap();
    session
        .run("CREATE TABLE t3 (a BIGINT PRIMARY KEY, b BIGINT, c VARCHAR(40))")
        .unwrap();

    let leaf = |session: &mut Session, sql: &str| {
        let rows = row_text(session.run(sql));
        let last = rows.last().expect("a plan has at least one row").clone();
        (last[0].clone(), last[3].clone(), last[4].clone())
    };

    // The whole row is covered by `idx(a, b)`, so the penalty decides it.
    let (name, object, info) = leaf(&mut session, "EXPLAIN SELECT * FROM t");
    assert!(name.contains("IndexFullScan"), "{name}");
    assert_eq!(object, "table:t, index:idx(a, b)");
    assert_eq!(info, "keep order:false, stats:pseudo");

    // NEGATIVE: `kb(b)` does not cover `c`, so the double read loses and the
    // penalized scan is still the cheapest path there is.
    let (name, object, _) = leaf(&mut session, "EXPLAIN SELECT * FROM t2");
    assert!(name.contains("TableFullScan"), "{name}");
    assert_eq!(object, "table:t2");

    // The same index, the same penalty, a narrower read: now it covers.
    let (name, object, _) = leaf(&mut session, "EXPLAIN SELECT b FROM t2");
    assert!(name.contains("IndexFullScan"), "{name}");
    assert_eq!(object, "table:t2, index:kb(b)");

    // NEGATIVE: a NARROWED table path is exempt from the penalty, and a table
    // with no index has nothing to lose to anyway.
    let (name, _, info) = leaf(&mut session, "EXPLAIN SELECT * FROM t3 WHERE a > 5");
    assert!(name.contains("TableRangeScan"), "{name}");
    assert!(info.starts_with("range:(5,+inf]"), "{info}");
    let (name, object, _) = leaf(&mut session, "EXPLAIN SELECT * FROM t3");
    assert!(name.contains("TableFullScan"), "{name}");
    assert_eq!(object, "table:t3");

    // The exemption where it can actually be OBSERVED: a wide table whose
    // narrowed handle range still loses to a covering index the moment the
    // penalty is charged to it. Captured TiDB:
    //
    // ```text
    // create table t4(a bigint primary key, b bigint,
    //                 c varchar(255), d varchar(255), key kb(b));
    // explain format = 'plan_tree' select b from t4 where a > 5;
    //   TableReader        root       data:Projection
    //   └─Projection       cop[tikv]  test.t4.b
    //     └─TableRangeScan cop[tikv]  table:t4  range:(5,+inf], keep order:false, stats:pseudo
    // ```
    //
    // `kb(b)` covers `{b, a}` and would be read whole; the range reads a
    // third of a wide row. Go charges the range NOTHING, so the range wins.
    session
        .run(
            "CREATE TABLE t4 (a BIGINT PRIMARY KEY, b BIGINT, \
             c VARCHAR(255), d VARCHAR(255), KEY kb(b))",
        )
        .unwrap();
    let (name, object, info) = leaf(&mut session, "EXPLAIN SELECT b FROM t4 WHERE a > 5");
    assert!(name.contains("TableRangeScan"), "{name}");
    assert_eq!(object, "table:t4");
    assert!(info.starts_with("range:(5,+inf]"), "{info}");
}

/// The columns a covering test reads are the ones the statement STILL needs
/// after Go's `rule_column_pruning` -- and Go's pruner walks a correlated
/// subquery like any other expression, so a column named only inside one is
/// the DataSource's whole demand.
///
/// This tier's exact column pruner
/// (`tidb_executor::column_prune::prunable_columns`) cannot answer that: it
/// NARROWS the scan's output, so it must be exact in both directions and
/// refuses every shape it cannot prove, a subquery above all -- and a refusal
/// reads as "every column", which makes no index cover and hands the full
/// scan the win by construction. The cost model therefore reads the
/// over-approximating leaf walk instead
/// (`tidb_executor::driver::leaf_demand`), the same one a leaf of a
/// multi-table `FROM` already used.
///
/// Captured from a real TiDB session through `rust/difftests/gorun`:
///
/// ```text
/// create table t1 (c1 int primary key, c2 int, c3 int, index kc2 (c2));
/// create table t2 (c1 int, c2 int);
///
/// explain format = 'plan_tree' select c2 = (select c2 from t2 where t2.c1 = t1.c1) from t1;
///   Projection            root       eq(test.t1.c2, test.t2.c2)->Column
///   └─Apply               root       CARTESIAN left outer join, left side:IndexReader
///     ├─IndexReader(Build)  root     index:IndexFullScan
///     │ └─IndexFullScan   cop[tikv]  table:t1, index:kc2(c2)  keep order:false, stats:pseudo
///     ...
///
/// explain format = 'plan_tree' select c3 = (select c2 from t2 where t2.c1 = t1.c1) from t1;
///   ...
///     ├─TableReader(Build)  root     data:TableFullScan
///     │ └─TableFullScan   cop[tikv]  table:t1  keep order:false, stats:pseudo
///     ...
/// ```
///
/// The negative row is the acceptance criterion: reading `c3` instead of `c2`
/// leaves `kc2(c2)` short of the row by exactly one column, and the same
/// statement shape then reads the table. So the index is chosen because it
/// covers what is read, not because a subquery is present.
#[test]
fn a_correlated_subquerys_columns_decide_whether_an_index_covers() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE t1 (c1 INT PRIMARY KEY, c2 INT, c3 INT, INDEX kc2 (c2))")
        .unwrap();
    session.run("CREATE TABLE t2 (c1 INT, c2 INT)").unwrap();

    // The outer `t1` reads `c1` (through the correlated `t2.c1 = t1.c1`) and
    // `c2`; `kc2(c2)` stores both, because `c1` is the row handle.
    let (name, object) = scan_of(
        &mut session,
        "EXPLAIN SELECT c2 = (SELECT c2 FROM t2 WHERE t2.c1 = t1.c1) FROM t1",
        "table:t1",
    );
    assert!(name.contains("IndexFullScan"), "{name}");
    assert_eq!(object, "table:t1, index:kc2(c2)");

    // NEGATIVE: `c3` is not in the index and is not the handle.
    let (name, object) = scan_of(
        &mut session,
        "EXPLAIN SELECT c3 = (SELECT c2 FROM t2 WHERE t2.c1 = t1.c1) FROM t1",
        "table:t1",
    );
    assert!(name.contains("TableFullScan"), "{name}");
    assert_eq!(object, "table:t1");
}

/// Go's index-force penalty is STATEMENT-wide, not per table.
/// `getGeneralAttributesFromPaths` (`pkg/planner/core/stats.go`) raises
/// `StmtCtx.SetIndexForce()` the moment ANY `AccessPath` of the statement is
/// `path.Forced`, and `getTableScanPenalty` then charges a second scan's
/// worth of rows to EVERY full table scan of that statement -- including one
/// over a table no hint ever named. `StatementContext`'s own field comment
/// says it: "indexForce is set if any table in the query has a force or use
/// index applied".
///
/// The statement below is `tests/integrationtest/t/subquery.test`'s own, and
/// the positive row is TiDB's recording of it
/// (`tests/integrationtest/r/subquery.result`):
///
/// ```text
/// create table t(a int primary key, b int, c int, d int, index idx(b,c,d));
/// insert into t values(1,1,1,1),(2,2,2,2),(3,2,2,2),(4,2,2,2),(5,2,2,2);
/// analyze table t;
///
/// explain format = 'plan_tree' select t.c in (select count(*) from t s use index(idx),
///     t t1 where s.b = 1 and s.c = 1 and s.d = t.a and s.a = t1.a) from t;
///   ...
///   ├─IndexReader(Build)  root       index:IndexFullScan
///   │ └─IndexFullScan     cop[tikv]  table:t, index:idx(b, c, d)  keep order:false
///   ...
/// ```
///
/// The negative row is the same statement with `use index(idx)` DELETED, and
/// nothing else changed; captured through `rust/difftests/gorun`:
///
/// ```text
///   │ └─TableFullScan     cop[tikv]  table:t   keep order:false
/// ```
///
/// The table is ANALYZED, so nothing else in `getTableScanPenalty` fires: the
/// statistics are neither pseudo, nor stale, nor outrun by `modify_count`.
/// With five analyzed rows the two paths over the OUTER `t` cost the same to
/// the cent (`explain format='verbose'` prints `123.64` for both readers), so
/// the tie-break keeps the table path -- and the hint on `s`, a different
/// occurrence of the same table, is the entire reason the recorded plan reads
/// the index over `t` instead.
#[test]
fn a_use_index_on_one_table_penalizes_every_other_full_scan_of_the_statement() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE t (a INT PRIMARY KEY, b INT, c INT, d INT, INDEX idx(b, c, d))")
        .unwrap();
    session
        .run("INSERT INTO t VALUES(1,1,1,1),(2,2,2,2),(3,2,2,2),(4,2,2,2),(5,2,2,2)")
        .unwrap();
    session.run("ANALYZE TABLE t").unwrap();

    // The hint names the inner `s`; the outer `t` has none of its own.
    let (name, object) = scan_of(
        &mut session,
        "EXPLAIN SELECT t.c IN (SELECT COUNT(*) FROM t s USE INDEX(idx), t t1 \
         WHERE s.b = 1 AND s.c = 1 AND s.d = t.a AND s.a = t1.a) FROM t",
        "table:t",
    );
    assert!(name.contains("IndexFullScan"), "{name}");
    assert_eq!(object, "table:t, index:idx(b, c, d)");

    // Three NEGATIVES, each the same statement with one thing changed, and
    // each reading `TableFullScan  cop[tikv]  table:t  keep order:false` in
    // TiDB (captured through `rust/difftests/gorun`).
    for inner in [
        // No hint at all. No path of the statement is forced, the analyzed
        // statistics earn no penalty, and the tie holds the table.
        "t s",
        // `IGNORE INDEX` is the one scan hint Go does NOT turn into
        // `path.Forced`: `planbuilder.go` collects it into `ignored` and
        // leaves `hasUseOrForce` alone.
        "t s IGNORE INDEX(idx)",
        // A hint outside `ast.HintForScan` is skipped before its index names
        // are even looked up, so `FOR JOIN` forces nothing.
        "t s USE INDEX FOR JOIN(idx)",
    ] {
        let sql = format!(
            "EXPLAIN SELECT t.c IN (SELECT COUNT(*) FROM {inner}, t t1 \
             WHERE s.b = 1 AND s.c = 1 AND s.d = t.a AND s.a = t1.a) FROM t"
        );
        let (name, object) = scan_of(&mut session, &sql, "table:t");
        assert!(name.contains("TableFullScan"), "{inner}: {name}");
        assert_eq!(object, "table:t", "{inner}");
    }
}

/// The `(operator, access object)` of the one scan node reading `object`.
///
/// Named rather than positional because both tests above plan a JOIN, where
/// the leaf under test is not the last row and its neighbour reads the same
/// TABLE under a different alias.
fn scan_of(session: &mut Session, sql: &str, object: &str) -> (String, String) {
    let rows = row_text(session.run(sql));
    let names_it =
        |written: &str| written == object || written.starts_with(&format!("{object}, index:"));
    let mut found = rows
        .iter()
        .filter(|row| names_it(&row[3]))
        .map(|row| (row[0].clone(), row[3].clone()));
    let first = found
        .next()
        .unwrap_or_else(|| panic!("no scan over {object} in the plan of {sql}:\n{rows:#?}"));
    assert!(
        found.next().is_none(),
        "more than one scan over {object} in the plan of {sql}:\n{rows:#?}"
    );
    first
}

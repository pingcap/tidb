#![cfg(test)]

use crate::tests_support::*;
use crate::*;

/// Go `mockStatsTable`/`mockStatsHistogram`: each integer in [0, NDV)
/// occupies one bucket, and table StatsVer deliberately remains zero while
/// the individual columns and indexes use version 2.
fn install_cardinality_mock_statistics(session: &Session, row_count: i64, columns: &[(i64, i64)]) {
    install_cardinality_mock_statistics_for_table(session, "t", row_count, columns);
}

fn install_cardinality_mock_statistics_for_table(
    session: &Session,
    table_name: &str,
    row_count: i64,
    columns: &[(i64, i64)],
) {
    use tidb_planner::cardinality::row_count_estimator::{ColumnStats, IndexStats};
    use tidb_stats::histogram::Histogram;

    let shared = session.shared_catalog();
    let mut catalog = shared.lock().unwrap();
    let tidb_executor::TableEntry::Kv(table) = catalog.table_in("test", table_name).unwrap() else {
        panic!("{table_name} must be a KV table");
    };
    assert_eq!(columns.len(), table.columns.len());
    let table_id = table.table_id;
    let histogram = |id, ndv: i64, repeat: i64, index| {
        let mut histogram = Histogram::new(id, ndv, 0, 0, ndv as usize, 0);
        for value in 0..ndv {
            let datum = if index {
                Datum::Bytes(tidb_codec::encode_key(&[Datum::Int(value)]).unwrap())
            } else {
                Datum::Int(value)
            };
            histogram.append_bucket(datum.clone(), datum, (value + 1) * repeat, repeat);
        }
        histogram
    };
    let mut statistics = tidb_executor::access_cost::TableStatistics {
        row_count,
        ..Default::default()
    };
    for (column, &(ndv, repeat)) in table.columns.iter().zip(columns) {
        statistics.columns.insert(
            column.id,
            ColumnStats {
                histogram: histogram(column.id, ndv, repeat, false),
                topn: None,
                cms: None,
                stats_ver: 2,
                unsigned: false,
            },
        );
        statistics
            .column_load_status
            .insert(column.id, tidb_stats::StatsLoadedStatus::full_load());
        statistics.column_stats_existence.insert(column.id, true);
    }
    for index in table
        .indexes()
        .iter()
        .filter(|index| !index.clustered_primary)
    {
        // Go TestOrderingIdxSelectivityRatioForApply deliberately installs
        // one-component histogram keys even for its composite ibc index.
        let (ndv, repeat) = columns[index.column_offsets[0]];
        statistics.indexes.insert(
            index.id,
            IndexStats {
                histogram: histogram(index.id, ndv, repeat, true),
                topn: None,
                cms: None,
                stats_ver: 2,
                num_columns: index.column_offsets.len(),
                unique: index.unique,
            },
        );
        statistics
            .index_load_status
            .insert(index.id, tidb_stats::StatsLoadedStatus::full_load());
        statistics.index_stats_existence.insert(index.id, true);
    }
    catalog.set_table_statistics(table_id, std::sync::Arc::new(statistics));
}

fn check_cardinality_query_fixture(session: &mut Session, name: &str) {
    let fixture: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../pkg/planner/cardinality/testdata/cardinality_suite_out.json"
    ))
    .unwrap();
    let cases = fixture
        .as_array()
        .unwrap()
        .iter()
        .find(|section| section["Name"] == name)
        .unwrap()["Cases"]
        .as_array()
        .unwrap();
    let mut mismatches = Vec::new();
    for case in cases {
        let sql = case["Query"].as_str().unwrap();
        if case["Result"].is_null() {
            session.run(sql).unwrap();
            continue;
        }
        let actual = row_text(session.run(sql))
            .iter()
            .map(|row| row.join(" "))
            .collect::<Vec<_>>();
        let expected = case["Result"]
            .as_array()
            .unwrap()
            .iter()
            .map(|row| row.as_str().unwrap())
            .collect::<Vec<_>>();
        if actual != expected {
            mismatches.push(format!(
                "{sql}\nactual: {actual:#?}\nexpected: {expected:#?}"
            ));
        }
    }
    assert!(
        mismatches.is_empty(),
        "{name}: {} mismatches\n{}",
        mismatches.len(),
        mismatches.join("\n")
    );
}

/// Go `TestIndexJoinInnerRowCountUpperBound`: Fix44855 caps each probe's
/// scan estimate using loaded join-key NDV, separately from the access floor.
#[test]
fn index_join_inner_row_count_upper_bound_matches_go() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE t(a INT, b INT, INDEX idx(b))")
        .unwrap();
    install_cardinality_mock_statistics(&session, 500_000, &[(500, 1000), (500, 1000)]);
    check_cardinality_query_fixture(&mut session, "TestIndexJoinInnerRowCountUpperBound");
}

#[test]
fn ordering_index_selectivity_threshold_matches_go_fixture() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE t(a INT PRIMARY KEY, b INT, c INT, d INT, INDEX ib(b), INDEX ic(c))")
        .unwrap();
    install_cardinality_mock_statistics(
        &session,
        100_000,
        &[(100_000, 1), (10_000, 10), (10_000, 10), (10_000, 10)],
    );
    check_cardinality_query_fixture(&mut session, "TestOrderingIdxSelectivityThreshold");
}

#[test]
fn index_merge_disjuncts_use_their_own_ranges_and_union_overlap() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE t(a INT PRIMARY KEY,b INT,c INT,d INT,INDEX ib(b),INDEX ic(c))")
        .unwrap();
    install_cardinality_mock_statistics(
        &session,
        100_000,
        &[(100_000, 1), (10_000, 10), (10_000, 10), (10_000, 10)],
    );
    for hint in ["/*+ USE_INDEX_MERGE(t,ib,ic) */", ""] {
        let rows = row_text(session.run(&format!("EXPLAIN FORMAT='brief' SELECT {hint} * FROM t WHERE (b>=0 AND b<=50) OR (c>=0 AND c<=50)")));
        assert_eq!(rows[0][0], "IndexMerge", "{rows:?}");
        assert_eq!(rows[0][1], "1017.40", "{rows:?}");
        assert_eq!(rows[1][1], "510.00", "{rows:?}");
        assert_eq!(rows[2][1], "510.00", "{rows:?}");
        assert_eq!(rows[3][1], "1017.40", "{rows:?}");
    }
    session.run("INSERT INTO t VALUES (1,1,100,0),(2,2,90,0),(3,9,10,0),(4,9,20,0),(5,3,30,0),(6,9,200,0)").unwrap();
    for direction in ["ASC", "DESC"] {
        let suffix = format!("WHERE b<=3 OR c<=30 ORDER BY c {direction} LIMIT 2 OFFSET 1");
        let expected = row_text(session.run(&format!("SELECT * FROM t USE INDEX() {suffix}")));
        let actual = row_text(session.run(&format!(
            "SELECT /*+ USE_INDEX_MERGE(t,ib,ic) */ * FROM t {suffix}"
        )));
        assert_eq!(actual, expected, "merge advisory order {direction}");
    }
}

#[test]
fn ordering_index_selectivity_ratio_matches_go_fixture() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE t(a INT PRIMARY KEY, b INT, c INT, INDEX ib(b), INDEX ic(c))")
        .unwrap();
    install_cardinality_mock_statistics(&session, 1000, &[(1000, 1), (1000, 1), (1000, 1)]);
    check_cardinality_query_fixture(&mut session, "TestOrderingIdxSelectivityRatio");
}

/// Index lookup must apply table predicates before LIMIT and obtain table
/// columns before sorting; index readers must restore datasource column order.
#[test]
fn index_lookup_limit_and_topn_preserve_table_rows() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE t(a INT PRIMARY KEY, b INT, c INT, INDEX ib(b), INDEX ic(c))")
        .unwrap();
    session
        .run("INSERT INTO t VALUES (1,0,10),(2,900,40),(3,901,30),(4,902,20)")
        .unwrap();
    for (sql, expected) in [
        (
            "SELECT a,b,c FROM t FORCE INDEX(ic) WHERE b>=900 ORDER BY c LIMIT 1",
            vec![vec!["4", "902", "20"]],
        ),
        (
            "SELECT a,b,c FROM t FORCE INDEX(ic) WHERE b>=900 ORDER BY c LIMIT 1 OFFSET 1",
            vec![vec!["3", "901", "30"]],
        ),
        (
            "SELECT a,b,c FROM t FORCE INDEX(ib) WHERE b>=900 ORDER BY c LIMIT 1",
            vec![vec!["4", "902", "20"]],
        ),
        (
            "SELECT a,b FROM t FORCE INDEX(ib) WHERE b>=900 ORDER BY b LIMIT 1",
            vec![vec!["2", "900"]],
        ),
    ] {
        assert_eq!(row_text(session.run(sql)), expected, "{sql}");
    }
}

#[test]
fn ordinary_execution_publishes_the_brief_binary_plan() {
    let registry = process::ProcessRegistry::default();
    let mut session = Session::new();
    let guard = registry.register(
        41,
        "root".to_owned(),
        "127.0.0.1:4000".to_owned(),
        "test".to_owned(),
        None,
    );
    session.attach_process(41, guard);
    session
        .run("CREATE TABLE src (a BIGINT, INDEX ia(a))")
        .unwrap();
    session.run("CREATE TABLE dst (a BIGINT)").unwrap();
    session.run("INSERT INTO src VALUES (1)").unwrap();

    session.run("SELECT * FROM src USE INDEX ()").unwrap();
    let select_info = tidb_util::memoryusagealarm::SessionManager::get_process_info(&registry, 41)
        .expect("registered process");
    let select_plan = select_info.brief_binary_plan.clone();
    let select_rows =
        tidb_util::plancodec::decode_binary_plan_for_connection(select_plan, "row", true).unwrap();
    assert!(
        select_rows
            .iter()
            .any(|row| row.first().is_some_and(|id| id.contains("TableReader"))),
        "ordinary SELECT must publish its retained physical plan: {select_rows:?}"
    );
    assert_eq!(select_info.table_ids.len(), 1);
    assert_eq!(select_info.index_names, Vec::<String>::new());
    assert_eq!(select_info.stats_info.get("src"), Some(&0));

    session
        .run("SELECT a FROM src USE INDEX (ia) WHERE a > 0")
        .unwrap();
    let index_info = tidb_util::memoryusagealarm::SessionManager::get_process_info(&registry, 41)
        .expect("registered process");
    assert_eq!(index_info.index_names, ["src:ia"]);

    session.run("INSERT INTO dst SELECT a FROM src").unwrap();
    let insert_plan = tidb_util::memoryusagealarm::SessionManager::get_process_info(&registry, 41)
        .expect("registered process")
        .brief_binary_plan
        .clone();
    let insert_rows =
        tidb_util::plancodec::decode_binary_plan_for_connection(insert_plan, "row", true).unwrap();
    assert!(
        insert_rows
            .first()
            .and_then(|row| row.first())
            .is_some_and(|id| id.contains("Insert")),
        "ordinary INSERT must publish its DML physical root: {insert_rows:?}"
    );
}

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
    session.set_connection_id(41);
    // Go-TPC deliverySelectNewOrder, planned through EXPLAIN with a domain.
    session.run("CREATE TABLE explain_new_order(no_w_id INT, no_d_id INT, no_o_id INT, PRIMARY KEY(no_w_id, no_d_id, no_o_id))").unwrap();
    let plan = row_text(session.run("EXPLAIN SELECT no_o_id FROM explain_new_order WHERE no_w_id = 1 AND no_d_id = 1 ORDER BY no_o_id ASC LIMIT 1 FOR UPDATE"));
    assert!(!plan.is_empty());
    // Go TestPointGetWithSelectLock plans locking reads through the same
    // domain as ordinary execution, including the EXPLAIN wrapper.
    session
        .run("CREATE TABLE explain_lock(c INT UNIQUE, d INT)")
        .unwrap();
    session.run("BEGIN").unwrap();
    for sql in [
        "EXPLAIN SELECT c, d FROM explain_lock WHERE c = 1 FOR UPDATE",
        "EXPLAIN SELECT c, d FROM explain_lock WHERE (c = 1 OR c = 2) AND d = 1 FOR UPDATE",
    ] {
        let plan = row_text(session.run(sql));
        assert!(!plan.is_empty(), "{sql}");
    }
    session.run("ROLLBACK").unwrap();
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
    // Go's ordinary table path can recover clustered BatchPointGet even
    // after USE INDEX() makes the fast common-index path unavailable.
    let batch = no_common_primary
        .iter()
        .find(|row| row[0].contains("Batch_Point_Get"))
        .expect("Go retains clustered table point ranges");
    assert_eq!(batch[1], "2.00");
    for (values, operator, estimate) in [
        ("(10, 'x'), (20, 'y'), (10, 'x')", "Batch_Point_Get", "2.00"),
        ("(10, 'x')", "Point_Get", "1.00"),
    ] {
        let rows = row_text(session.run(&format!(
            "EXPLAIN SELECT v FROM common_point USE INDEX () WHERE (a,b) IN ({values})"
        )));
        let point = rows.iter().find(|row| row[0].contains(operator)).unwrap();
        assert_eq!(point[1], estimate, "{rows:?}");
    }

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
    // read in a `CopTask` and `ConvertToRootTask` caps it with a freshly
    // allocated reader (`pkg/planner/core/find_best_task.go:2953`,
    // `pkg/planner/core/operator/physicalop/task_base.go:504`). Go's physical
    // post-optimizer removes the identity projection over `SELECT *`.
    assert_eq!(
        row_text(session.run("EXPLAIN SELECT * FROM t")),
        vec![
            vec![
                "TableReader_5".to_owned(),
                "10000.00".to_owned(),
                "root".to_owned(),
                String::new(),
                "data:TableFullScan_4".to_owned(),
            ],
            vec![
                "└─TableFullScan_4".to_owned(),
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
                "TableReader_7".to_owned(),
                "3333.33".to_owned(),
                "root".to_owned(),
                String::new(),
                "data:Selection_6".to_owned(),
            ],
            vec![
                "└─Selection_6".to_owned(),
                "3333.33".to_owned(),
                "cop[tikv]".to_owned(),
                String::new(),
                // Go's own function-call rendering, captured:
                // gt(test.t.b, "x").
                "gt(test.t.b, \"x\")".to_owned(),
            ],
            vec![
                "  └─TableFullScan_5".to_owned(),
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
                "TopN_7".to_owned(),
                "10.00".to_owned(),
                "root".to_owned(),
                String::new(),
                "test.t.c, offset:0, count:10".to_owned(),
            ],
            vec![
                "└─TableReader_17".to_owned(),
                "10.00".to_owned(),
                "root".to_owned(),
                String::new(),
                "data:TopN_16".to_owned(),
            ],
            vec![
                "  └─TopN_16".to_owned(),
                "10.00".to_owned(),
                "cop[tikv]".to_owned(),
                String::new(),
                "test.t.c, offset:0, count:10".to_owned(),
            ],
            vec![
                "    └─TableFullScan_15".to_owned(),
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
            "Sort_4".to_owned(),
            "└─TableReader_8".to_owned(),
            "  └─TableFullScan_7".to_owned(),
        ]
    );

    // GROUP BY. The 8000.00 is Go's stats-less distinctFactor result. The
    // projection, root/coprocessor aggregate pair, and reader boundary match
    // the current Go plan.
    assert_eq!(
        row_text(session.run("EXPLAIN SELECT c, COUNT(*) FROM t GROUP BY c")),
        vec![
            vec![
                "Projection_4".to_owned(),
                "8000.00".to_owned(),
                "root".to_owned(),
                String::new(),
                "test.t.c, Column#5".to_owned(),
            ],
            vec![
                "└─HashAgg_9".to_owned(),
                "8000.00".to_owned(),
                "root".to_owned(),
                String::new(),
                "group by:test.t.c, funcs:count(Column#6)->Column#5, \
                 funcs:firstrow(test.t.c)->test.t.c"
                    .to_owned(),
            ],
            vec![
                "  └─TableReader_10".to_owned(),
                "8000.00".to_owned(),
                "root".to_owned(),
                String::new(),
                "data:HashAgg_5".to_owned(),
            ],
            vec![
                "    └─HashAgg_5".to_owned(),
                "8000.00".to_owned(),
                "cop[tikv]".to_owned(),
                String::new(),
                "group by:test.t.c, funcs:count(1)->Column#6".to_owned(),
            ],
            vec![
                "      └─TableFullScan_8".to_owned(),
                "10000.00".to_owned(),
                "cop[tikv]".to_owned(),
                "table:t".to_owned(),
                "keep order:false, stats:pseudo".to_owned(),
            ],
        ]
    );
}

/// Go `pkg/planner/core/casetest/rule/rule_common_handle_range_test.go`:
/// tuple comparisons over a secondary index include every appended common
/// handle dimension in their lexicographic ranges.
#[test]
fn common_handle_tuple_comparison_uses_appended_index_ranges() {
    let mut session = Session::new();
    session
        .run(
            "CREATE TABLE tuple_ranges (
                a BIGINT NOT NULL,
                b BIGINT NOT NULL,
                c BIGINT NOT NULL,
                PRIMARY KEY (b, c) CLUSTERED,
                KEY ia(a)
            )",
        )
        .unwrap();
    session
        .run("INSERT INTO tuple_ranges VALUES (1,2,3), (1,2,4), (1,3,1), (2,1,1)")
        .unwrap();

    let explain = row_text(session.run(
        "EXPLAIN SELECT * FROM tuple_ranges USE INDEX (ia) \
         WHERE (a, b, c) > (1, 2, 3)",
    ));
    assert!(
        explain.iter().any(|row| {
            row.iter()
                .any(|cell| cell.contains("range:(1 2 3,1 2 +inf], (1 2,1 +inf], (1,+inf]"))
        }),
        "tuple comparison must reach the appended common handle: {explain:?}"
    );
    assert_eq!(
        row_text(session.run(
            "SELECT a, b, c FROM tuple_ranges USE INDEX (ia) \
             WHERE (a, b, c) > (1, 2, 3) ORDER BY a, b, c",
        )),
        [
            vec!["1".to_owned(), "2".to_owned(), "4".to_owned()],
            vec!["1".to_owned(), "3".to_owned(), "1".to_owned()],
            vec!["2".to_owned(), "1".to_owned(), "1".to_owned()],
        ]
    );

    session
        .run(
            "CREATE TABLE tuple_ranges3 (
                a BIGINT NOT NULL,
                b BIGINT NOT NULL,
                c BIGINT NOT NULL,
                d BIGINT NOT NULL,
                PRIMARY KEY (b, c, d) CLUSTERED,
                KEY ia(a)
            )",
        )
        .unwrap();
    session
        .run("INSERT INTO tuple_ranges3 VALUES (1,2,3,4), (1,2,3,5), (1,2,4,1)")
        .unwrap();
    let explain = row_text(session.run(
        "EXPLAIN SELECT * FROM tuple_ranges3 USE INDEX (ia) \
         WHERE (a, b, c, d) > (1, 2, 3, 4)",
    ));
    assert!(
        explain.iter().any(|row| {
            row.iter().any(|cell| {
                cell.contains(
                    "range:(1 2 3 4,1 2 3 +inf], (1 2 3,1 2 +inf], (1 2,1 +inf], (1,+inf]",
                )
            })
        }),
        "three-column common handles must all be range dimensions: {explain:?}"
    );
}

/// Go `pkg/planner/cardinality.AdjustRowCountForAppendedHandleColumns`:
/// a range on an integer primary-key handle appended to a secondary index
/// lowers the index scan estimate after analyzed statistics are available.
#[test]
fn appended_integer_handle_range_lowers_index_scan_estimate() {
    let mut session = Session::new();
    session
        .run(
            "CREATE TABLE appended_handle_estimate (
                id BIGINT PRIMARY KEY,
                a BIGINT NOT NULL,
                payload BIGINT,
                KEY ia(a)
            )",
        )
        .unwrap();
    let values = (1..=100)
        .map(|id| format!("({id}, 1, {id})"))
        .collect::<Vec<_>>()
        .join(",");
    session
        .run(&format!(
            "INSERT INTO appended_handle_estimate VALUES {values}"
        ))
        .unwrap();
    session
        .run("ANALYZE TABLE appended_handle_estimate")
        .unwrap();

    let index_scan_estimate = |session: &mut Session, predicate: &str| {
        let rows = row_text(session.run(&format!(
            "EXPLAIN SELECT payload FROM appended_handle_estimate USE INDEX (ia) WHERE {predicate}"
        )));
        rows.iter()
            .find(|row| row[0].contains("IndexRangeScan"))
            .unwrap_or_else(|| panic!("missing index scan for {predicate}: {rows:?}"))[1]
            .parse::<f64>()
            .unwrap()
    };

    let prefix_estimate = index_scan_estimate(&mut session, "a = 1");
    let appended_handle_estimate =
        index_scan_estimate(&mut session, "a = 1 AND id BETWEEN 10 AND 19");
    assert!(
        appended_handle_estimate < prefix_estimate,
        "the appended integer-handle range should reduce the index estimate: \
         prefix={prefix_estimate}, with_handle={appended_handle_estimate}"
    );
}

/// Go `TestIndexRangeEstimationWithAppendedHandleColumn`: estimation must
/// remain aligned with the declared index columns when only column
/// histograms are available and a signed integer handle is appended to the
/// execution range.
#[test]
fn appended_handle_range_uses_partial_column_statistics() {
    let mut session = Session::new();
    session
        .run(
            "CREATE TABLE appended_partial_stats (
                id INT PRIMARY KEY, a INT, b INT, c INT, KEY idx_ab(a,b)
            )",
        )
        .unwrap();
    let values = (1..=100)
        .map(|id| format!("({id}, {id}, {id}, {id})"))
        .collect::<Vec<_>>()
        .join(",");
    session
        .run(&format!(
            "INSERT INTO appended_partial_stats VALUES {values}"
        ))
        .unwrap();
    session
        .run("ANALYZE TABLE appended_partial_stats ALL COLUMNS")
        .unwrap();

    let shared = session.shared_catalog();
    let (table_id, index_id) = {
        let catalog = shared.lock().unwrap();
        let table = match catalog.table_in("test", "appended_partial_stats").unwrap() {
            tidb_executor::TableEntry::Kv(table) => table,
            _ => panic!("appended_partial_stats is not a KV table"),
        };
        (
            table.table_id,
            table
                .indexes()
                .iter()
                .find(|index| index.name.eq_ignore_ascii_case("idx_ab"))
                .expect("idx_ab")
                .id,
        )
    };
    {
        let mut catalog = shared.lock().unwrap();
        let mut statistics = (*catalog.table_statistics(table_id).unwrap()).clone();
        statistics.indexes.remove(&index_id);
        statistics.index_load_status.remove(&index_id);
        statistics.index_stats_existence.remove(&index_id);
        catalog.set_table_statistics(table_id, std::sync::Arc::new(statistics));
    }

    let rows = row_text(session.run(
        "EXPLAIN SELECT * FROM appended_partial_stats USE INDEX (idx_ab) \
         WHERE a = 3 AND b = 3 AND id = 3",
    ));
    let scan = rows
        .iter()
        .find(|row| row[0].contains("IndexRangeScan"))
        .unwrap_or_else(|| panic!("missing index range scan: {rows:?}"));
    assert!(scan[4].contains("range:[3 3 3,3 3 3]"), "{scan:?}");
    assert_eq!(scan[1], "1.00", "{rows:?}");
    assert!(
        scan[4].contains("stats:partial[idx_ab:missing]"),
        "{scan:?}"
    );
}

/// Go `TestNewIndexWithColumnStats`: an index created after ANALYZE must use
/// existing column statistics when its own index histogram is absent.
#[test]
fn newly_created_index_estimates_from_existing_column_statistics() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE indexed_after_analyze (a INT)")
        .unwrap();
    session
        .run("CREATE TABLE indexed_without_stats (a INT, KEY idxa(a))")
        .unwrap();
    let values = (1..=500)
        .map(|value| format!("({})", value % 250))
        .collect::<Vec<_>>()
        .join(",");
    session
        .run(&format!(
            "INSERT INTO indexed_after_analyze VALUES {values}"
        ))
        .unwrap();
    session
        .run(&format!(
            "INSERT INTO indexed_without_stats VALUES {values}"
        ))
        .unwrap();
    session
        .run("ANALYZE TABLE indexed_after_analyze ALL COLUMNS")
        .unwrap();
    session
        .run("CREATE INDEX idxa ON indexed_after_analyze(a)")
        .unwrap();

    let with_column_stats = row_text(session.run(
        "EXPLAIN ANALYZE SELECT * FROM indexed_after_analyze USE INDEX (idxa) \
         WHERE a > 5 AND a < 25",
    ));
    let without_stats = row_text(session.run(
        "EXPLAIN SELECT * FROM indexed_without_stats USE INDEX (idxa) \
         WHERE a > 5 AND a < 25",
    ));
    let analyzed_scan = with_column_stats
        .iter()
        .find(|row| row[0].contains("IndexRangeScan"))
        .unwrap_or_else(|| panic!("missing analyzed index range scan: {with_column_stats:?}"));
    let pseudo_scan = without_stats
        .iter()
        .find(|row| row[0].contains("IndexRangeScan"))
        .unwrap_or_else(|| panic!("missing pseudo index range scan: {without_stats:?}"));
    assert_eq!(analyzed_scan[2], "38", "{with_column_stats:?}");
    assert!(
        (analyzed_scan[1].parse::<f64>().unwrap() - 38.0).abs() < 0.1,
        "column stats should estimate the actual 38 rows: {with_column_stats:?}"
    );
    assert_ne!(analyzed_scan[1], pseudo_scan[1], "{without_stats:?}");
}

/// Go `TestNewIndexWithoutStats`: skyline planning should favor analyzed
/// statistics when equality coverage ties, but prefer a newly-created index
/// when it covers more equality/range predicates.
#[test]
fn new_index_without_stats_skyline_choice_matches_go() {
    let mut session = Session::new();
    session
        .run("SET SESSION tidb_opt_table_full_scan_cost_factor = 1000")
        .unwrap();
    session
        .run("CREATE TABLE new_index_skyline (a INT, b INT, c INT, KEY idxa(a), KEY idxca(c,a))")
        .unwrap();
    let values = (1..=500)
        .map(|value| format!("({}, {}, {})", value % 250, value % 10, value % 100))
        .chain(std::iter::once("(1, 1, 1)".to_owned()))
        .collect::<Vec<_>>()
        .join(",");
    session
        .run(&format!("INSERT INTO new_index_skyline VALUES {values}"))
        .unwrap();
    session.run("ANALYZE TABLE new_index_skyline").unwrap();
    session
        .run("CREATE INDEX idxb ON new_index_skyline(b)")
        .unwrap();

    let assert_index = |session: &mut Session, predicate: &str, expected: &str| {
        let sql =
            format!("EXPLAIN FORMAT='brief' SELECT * FROM new_index_skyline WHERE {predicate}");
        let plan = row_text(session.run(&sql));
        let plan_text = plan
            .iter()
            .map(|row| row.join(" "))
            .collect::<Vec<_>>()
            .join("\n");
        assert!(
            plan_text.contains(expected),
            "expected {expected} for `{predicate}`, got:\n{plan_text}"
        );
    };

    // A fresh index without statistics loses when it ties idxa's equality
    // coverage. Once analyzed, the same tie still resolves to idxa.
    assert_index(&mut session, "a = 5 AND b = 5", "index:idxa(a)");
    session.run("ANALYZE TABLE new_index_skyline").unwrap();
    assert_index(&mut session, "a = 5 AND b = 5", "index:idxa(a)");

    // A fresh composite index wins when its leading columns cover more
    // predicates, even before that index has its own histogram.
    session
        .run("CREATE INDEX idxab ON new_index_skyline(a, b)")
        .unwrap();
    assert_index(&mut session, "a = 5 AND b = 5", "index:idxab(a, b)");
    assert_index(&mut session, "a > 5 AND b > 5", "index:idxab(a, b)");
    assert_index(
        &mut session,
        "a = 5 AND b > 5 AND c > 5",
        "index:idxab(a, b)",
    );
    assert_index(
        &mut session,
        "a = 5 AND b > 5 AND c = 5",
        "index:idxca(c, a)",
    );
}

/// Go `TestIssue57948`: a lone index created after ANALYZE is still a valid
/// access path using its column statistics and must not be pruned.
#[test]
fn single_new_index_with_column_stats_is_chosen() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE issue_57948 (a INT, b INT, c INT)")
        .unwrap();
    let values = (1..=500)
        .map(|value| format!("({}, {}, {})", value % 250, value % 10, value % 100))
        .chain(std::iter::once("(1, 1, 1)".to_owned()))
        .collect::<Vec<_>>()
        .join(",");
    session
        .run(&format!("INSERT INTO issue_57948 VALUES {values}"))
        .unwrap();
    session.run("ANALYZE TABLE issue_57948").unwrap();
    session.run("CREATE INDEX idxb ON issue_57948(b)").unwrap();

    let plan =
        row_text(session.run("EXPLAIN FORMAT='brief' SELECT * FROM issue_57948 WHERE b = 5"));
    let plan_text = plan
        .iter()
        .map(|row| row.join(" "))
        .collect::<Vec<_>>()
        .join("\n");
    let forced_plan = row_text(
        session
            .run("EXPLAIN FORMAT='brief' SELECT * FROM issue_57948 USE INDEX (idxb) WHERE b = 5"),
    );
    let forced_plan_text = forced_plan
        .iter()
        .map(|row| row.join(" "))
        .collect::<Vec<_>>()
        .join("\n");
    assert!(
        plan_text.contains("index:idxb(b)"),
        "the sole newly created index must remain a candidate using column stats:\n{plan_text}\nforced index plan:\n{forced_plan_text}"
    );
}

/// SQL-plan assertions from Go `TestIndexEstimationCrossValidate`: a composite
/// equality range should estimate one row, and a newly analyzed but empty
/// index histogram should not replace the valid table scan estimate. The
/// high-CMS-count comparison is exercised at the planner estimator boundary.
#[test]
fn composite_index_estimate_and_empty_index_stats_match_go() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE cross_validate (a INT, b INT, KEY idx_ab(a, b))")
        .unwrap();
    session
        .run("INSERT INTO cross_validate VALUES (1,1),(1,2),(1,3),(2,2)")
        .unwrap();
    session.run("ANALYZE TABLE cross_validate").unwrap();

    let plan = row_text(
        session.run("EXPLAIN FORMAT='brief' SELECT * FROM cross_validate WHERE a=1 AND b=2"),
    );
    let plan_text = plan
        .iter()
        .map(|row| row.join(" "))
        .collect::<Vec<_>>()
        .join("\n");
    assert!(
        plan_text.contains("IndexRangeScan") && plan_text.contains("1.00"),
        "composite equality estimate should use the index histogram:\n{plan_text}"
    );

    session
        .run("CREATE TABLE cross_validate_invalid (a INT, b INT, KEY idx_b(b))")
        .unwrap();
    session
        .run("INSERT INTO cross_validate_invalid VALUES (1,1),(2,2),(3,3),(4,4),(5,5)")
        .unwrap();
    // Go marks the column's stats as needed before ANALYZE creates an empty
    // index histogram, reproducing the stale-index-stats fallback.
    session
        .run("SELECT * FROM cross_validate_invalid WHERE b=2")
        .unwrap();
    session
        .run("ANALYZE TABLE cross_validate_invalid INDEX idx_b")
        .unwrap();
    let plan = row_text(
        session.run("EXPLAIN FORMAT='brief' SELECT * FROM cross_validate_invalid WHERE b=2"),
    );
    let plan_text = plan
        .iter()
        .map(|row| row.join(" "))
        .collect::<Vec<_>>()
        .join("\n");
    assert!(
        plan_text.contains("TableFullScan") && !plan_text.contains("IndexRangeScan"),
        "invalid index stats should leave the table scan estimate in place:\n{plan_text}"
    );
}

/// Go `TestCrossValidationSelectivity`: the clustered PK range keeps both
/// matching rows, then the out-of-range residual column predicate estimates
/// one of them away.
#[test]
fn cross_validation_on_clustered_pk_range_matches_go() {
    let mut session = Session::new();
    session.run("SET tidb_analyze_version = 2").unwrap();
    session
        .run("CREATE TABLE cross_validate_pk (a INT, b INT, c INT, PRIMARY KEY (a, b) CLUSTERED)")
        .unwrap();
    session
        .run("INSERT INTO cross_validate_pk VALUES (1,2,3),(1,4,5)")
        .unwrap();
    session.run("ANALYZE TABLE cross_validate_pk").unwrap();

    let rows = row_text(session.run(
        "EXPLAIN FORMAT='brief' SELECT * FROM cross_validate_pk \
         WHERE a = 1 AND b > 0 AND b < 1000 AND c > 1000",
    ));
    let plan_text = rows
        .iter()
        .map(|row| row.join(" "))
        .collect::<Vec<_>>()
        .join("\n");
    assert!(
        plan_text.contains("Selection 1.00")
            && plan_text.contains("TableRangeScan 2.00")
            && plan_text.contains("range:(1 0,1 1000)"),
        "clustered-PK range and residual selectivity should match Go:\n{plan_text}"
    );
}

/// Go `TestIgnoreRealtimeStats`: determinate objective uses pseudo stats
/// before ANALYZE and analyzed counts after later modifications.
#[test]
fn determinate_objective_uses_analyzed_row_count_after_inserts() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE ignore_realtime_stats (a INT, b INT)")
        .unwrap();
    session
        .run(
            "INSERT INTO ignore_realtime_stats VALUES \
             (1,1),(1,2),(1,3),(1,4),(1,5),(2,1),(2,2),(2,3),(2,4),(2,5),(3,1)",
        )
        .unwrap();
    session.run("FLUSH STATS_DELTA *.*").unwrap();

    let explain = |session: &mut Session| {
        row_text(session.run(
            "EXPLAIN FORMAT='brief' SELECT * FROM ignore_realtime_stats \
             WHERE a = 1 AND b > 2",
        ))
    };
    let estimate = |rows: &[Vec<String>], operator: &str| {
        rows.iter()
            .find(|row| row[0].contains(operator))
            .map(|row| row[1].clone())
            .unwrap_or_else(|| panic!("missing {operator} in {rows:?}"))
    };

    // Go's unANALYZEd stats_meta row retains its real count in moderate mode;
    // determinate resets it to zero and uses PseudoRowCount.
    let moderate_unanalyzed = explain(&mut session);
    session
        .run("SET tidb_opt_objective = 'determinate'")
        .unwrap();
    let determinate_unanalyzed = explain(&mut session);
    assert!(
        estimate(&moderate_unanalyzed, "TableFullScan") == "11.00"
            && estimate(&moderate_unanalyzed, "Selection") == "1.00",
        "{moderate_unanalyzed:?}"
    );
    assert!(
        estimate(&determinate_unanalyzed, "TableFullScan") == "10000.00"
            && estimate(&determinate_unanalyzed, "Selection") == "3.33",
        "{determinate_unanalyzed:?}"
    );

    session.run("SET tidb_opt_objective = 'moderate'").unwrap();
    session
        .run("ANALYZE TABLE ignore_realtime_stats ALL COLUMNS WITH 1 SAMPLERATE")
        .unwrap();
    let moderate_analyzed = explain(&mut session);
    session
        .run("SET tidb_opt_objective = 'determinate'")
        .unwrap();
    let determinate_analyzed = explain(&mut session);
    assert!(
        estimate(&moderate_analyzed, "TableFullScan") == "11.00"
            && estimate(&moderate_analyzed, "Selection") == "2.73"
            && estimate(&determinate_analyzed, "TableFullScan") == "11.00"
            && estimate(&determinate_analyzed, "Selection") == "2.73",
        "ANALYZE should make both objectives use 11 rows:\nmoderate={moderate_analyzed:?}\ndeterminate={determinate_analyzed:?}"
    );

    session.run("BEGIN").unwrap();
    session
        .run("INSERT INTO ignore_realtime_stats VALUES (3,2),(3,3)")
        .unwrap();
    session.run("FLUSH STATS_DELTA *.*").unwrap();
    let shared = session.shared_catalog();
    {
        let catalog = shared.lock().unwrap();
        let table_id = match catalog.table_in("test", "ignore_realtime_stats").unwrap() {
            tidb_executor::TableEntry::Kv(table) => table.table_id,
            _ => panic!("ignore_realtime_stats is not a KV table"),
        };
        let statistics = catalog.table_statistics(table_id).unwrap();
        assert_eq!(statistics.row_count, 11);
        assert_eq!(statistics.modify_count, 0);
    }
    session.run("ROLLBACK").unwrap();

    session
        .run("INSERT INTO ignore_realtime_stats VALUES (3,2),(3,3),(3,4),(3,5)")
        .unwrap();
    session.run("FLUSH STATS_DELTA *.*").unwrap();
    let shared = session.shared_catalog();
    {
        let catalog = shared.lock().unwrap();
        let table_id = match catalog.table_in("test", "ignore_realtime_stats").unwrap() {
            tidb_executor::TableEntry::Kv(table) => table.table_id,
            _ => panic!("ignore_realtime_stats is not a KV table"),
        };
        // Go's source test flushes these four committed inserts and reloads
        // stats_meta before comparing the objective modes.
        let statistics = catalog.table_statistics(table_id).unwrap();
        assert_eq!(statistics.row_count, 15);
        assert_eq!(statistics.modify_count, 4);
    }

    session.run("SET tidb_opt_objective = 'moderate'").unwrap();
    let moderate = explain(&mut session);
    session
        .run("SET tidb_opt_objective = 'determinate'")
        .unwrap();
    let determinate = explain(&mut session);
    assert!(
        estimate(&moderate, "TableFullScan") == "15.00"
            && estimate(&moderate, "Selection") == "3.72",
        "{moderate:?}"
    );
    assert!(
        estimate(&determinate, "TableFullScan") == "11.00"
            && estimate(&determinate, "Selection") == "2.73",
        "{determinate:?}"
    );
}

#[test]
fn flush_stats_delta_honors_table_and_database_scope() {
    let mut session = Session::new();
    session.run("CREATE TABLE flush_scope_a (a INT)").unwrap();
    session.run("CREATE TABLE flush_scope_b (a INT)").unwrap();
    session.run("INSERT INTO flush_scope_a VALUES (1)").unwrap();
    session
        .run("INSERT INTO flush_scope_b VALUES (1),(2)")
        .unwrap();

    session.run("FLUSH STATS_DELTA test.flush_scope_a").unwrap();
    let shared = session.shared_catalog();
    {
        let catalog = shared.lock().unwrap();
        let a = match catalog.table_in("test", "flush_scope_a").unwrap() {
            tidb_executor::TableEntry::Kv(table) => table.table_id,
            _ => panic!("flush_scope_a is not a KV table"),
        };
        let b = match catalog.table_in("test", "flush_scope_b").unwrap() {
            tidb_executor::TableEntry::Kv(table) => table.table_id,
            _ => panic!("flush_scope_b is not a KV table"),
        };
        assert_eq!(catalog.table_statistics(a).unwrap().row_count, 1);
        assert!(catalog.table_statistics(b).is_none());
    }

    session.run("FLUSH STATS_DELTA test.*").unwrap();
    let catalog = shared.lock().unwrap();
    let b = match catalog.table_in("test", "flush_scope_b").unwrap() {
        tidb_executor::TableEntry::Kv(table) => table.table_id,
        _ => panic!("flush_scope_b is not a KV table"),
    };
    assert_eq!(catalog.table_statistics(b).unwrap().row_count, 2);
}

#[test]
fn flush_stats_delta_counts_changed_updates_and_deletes() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE flush_dml_delta (a INT PRIMARY KEY, b INT)")
        .unwrap();
    session
        .run("INSERT INTO flush_dml_delta VALUES (1,1),(2,2)")
        .unwrap();
    session.run("ANALYZE TABLE flush_dml_delta").unwrap();

    session
        .run("UPDATE flush_dml_delta SET b = 3 WHERE a = 1")
        .unwrap();
    session
        .run("FLUSH STATS_DELTA test.flush_dml_delta")
        .unwrap();
    let shared = session.shared_catalog();
    let table_id = {
        let catalog = shared.lock().unwrap();
        let table_id = match catalog.table_in("test", "flush_dml_delta").unwrap() {
            tidb_executor::TableEntry::Kv(table) => table.table_id,
            _ => panic!("flush_dml_delta is not a KV table"),
        };
        let statistics = catalog.table_statistics(table_id).unwrap();
        assert_eq!(statistics.row_count, 2);
        assert_eq!(statistics.modify_count, 1);
        table_id
    };

    session
        .run("DELETE FROM flush_dml_delta WHERE a = 2")
        .unwrap();
    session
        .run("FLUSH STATS_DELTA test.flush_dml_delta")
        .unwrap();
    let catalog = shared.lock().unwrap();
    let statistics = catalog.table_statistics(table_id).unwrap();
    assert_eq!(statistics.row_count, 1);
    assert_eq!(statistics.modify_count, 2);
}

#[test]
fn flush_stats_delta_tracks_partition_physical_ids() {
    let mut session = Session::new();
    session
        .run(
            "CREATE TABLE flush_partition_delta (a INT) \
             PARTITION BY HASH(a) PARTITIONS 2",
        )
        .unwrap();
    session
        .run("INSERT INTO flush_partition_delta VALUES (1),(2)")
        .unwrap();
    session
        .run("INSERT INTO flush_partition_delta VALUES (3),(4)")
        .unwrap();
    session
        .run("FLUSH STATS_DELTA test.flush_partition_delta")
        .unwrap();

    let shared = session.shared_catalog();
    let catalog = shared.lock().unwrap();
    let table = match catalog.table_in("test", "flush_partition_delta").unwrap() {
        tidb_executor::TableEntry::Kv(table) => table,
        _ => panic!("flush_partition_delta is not a KV table"),
    };
    let partition_ids = table
        .partition()
        .unwrap()
        .definitions
        .iter()
        .map(|partition| partition.id)
        .collect::<Vec<_>>();
    let statistics = partition_ids
        .iter()
        .map(|id| catalog.table_statistics(*id).unwrap())
        .collect::<Vec<_>>();
    assert_eq!(
        statistics.iter().map(|stats| stats.row_count).sum::<i64>(),
        4
    );
    assert_eq!(
        statistics
            .iter()
            .map(|stats| stats.modify_count)
            .sum::<i64>(),
        4
    );
}

#[test]
fn flush_stats_delta_restores_statement_and_savepoint_deltas() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE flush_rollback_delta (a INT PRIMARY KEY)")
        .unwrap();
    session
        .run("INSERT INTO flush_rollback_delta VALUES (1)")
        .unwrap();
    session.run("ANALYZE TABLE flush_rollback_delta").unwrap();

    session.run("BEGIN").unwrap();
    session
        .run("INSERT INTO flush_rollback_delta VALUES (2)")
        .unwrap();
    session.run("SAVEPOINT delta_point").unwrap();
    session
        .run("INSERT INTO flush_rollback_delta VALUES (3)")
        .unwrap();
    session.run("ROLLBACK TO delta_point").unwrap();
    assert!(
        session
            .run("INSERT INTO flush_rollback_delta VALUES (4),(1)")
            .is_err()
    );
    session.run("COMMIT").unwrap();
    session
        .run("FLUSH STATS_DELTA test.flush_rollback_delta")
        .unwrap();

    let shared = session.shared_catalog();
    let catalog = shared.lock().unwrap();
    let table_id = match catalog.table_in("test", "flush_rollback_delta").unwrap() {
        tidb_executor::TableEntry::Kv(table) => table.table_id,
        _ => panic!("flush_rollback_delta is not a KV table"),
    };
    let statistics = catalog.table_statistics(table_id).unwrap();
    assert_eq!(statistics.row_count, 2);
    assert_eq!(statistics.modify_count, 1);
}

/// Go `TestIssue64137`: a high-count TopN is removed from the index histogram;
/// the small remaining NDV bounds an out-of-range estimate after inserts.
#[test]
fn small_ndv_out_of_range_index_reader_rows_match_go() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE issue_64137 (a INT, KEY idx_a(a))")
        .unwrap();
    session.run("SET cte_max_recursion_depth = 10000").unwrap();
    session
        .run("INSERT INTO issue_64137 SELECT a FROM (WITH RECURSIVE cte AS (SELECT 1 AS a, 1 AS num UNION ALL SELECT 1 AS a, num + 1 AS num FROM cte WHERE num < 10000) SELECT a FROM cte) AS source_rows")
        .unwrap();
    session.run("ANALYZE TABLE issue_64137").unwrap();
    session
        .run("INSERT INTO issue_64137 SELECT * FROM issue_64137 LIMIT 2000")
        .unwrap();
    let shared = session.shared_catalog();
    {
        let mut catalog = shared.lock().unwrap();
        catalog.flush_stats_delta();
        let table_id = match catalog.table_in("test", "issue_64137").unwrap() {
            tidb_executor::TableEntry::Kv(table) => table.table_id,
            _ => panic!("issue_64137 is not a KV table"),
        };
        // Go's source test flushes the session delta and calls
        // StatsHandle.Update, so its cached stats_meta count/modify_count
        // reflect the 2,000 committed inserts while the analyzed histogram
        // remains unchanged. Session::new has no domain stats worker; model
        // that refreshed metadata at the same boundary for this estimator
        // parity test.
        let mut statistics = (*catalog.table_statistics(table_id).unwrap()).clone();
        statistics.row_count = 12_000;
        statistics.modify_count = 2_000;
        catalog.set_table_statistics(table_id, std::sync::Arc::new(statistics));
    }

    for (predicate, expected_rows) in [("a=99999999", "24.00"), ("a=1", "12000.00")] {
        let rows = row_text(session.run(&format!(
            "EXPLAIN FORMAT='brief' SELECT * FROM issue_64137 WHERE {predicate}"
        )));
        let plan_text = rows
            .iter()
            .map(|row| row.join(" "))
            .collect::<Vec<_>>()
            .join("\n");
        assert!(
            plan_text.contains("IndexRangeScan") && plan_text.contains(expected_rows),
            "{predicate} should estimate {expected_rows} through the index range:\n{plan_text}"
        );
    }
}

#[test]
fn null_column_and_index_ranges_match_cardinality_goldens() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE null_estimation (a INT, b INT, c INT, KEY idx_b(b), KEY idx_c_a(c,a))")
        .unwrap();
    session
        .run("INSERT INTO null_estimation VALUES (1,NULL,1),(2,NULL,2),(3,3,3),(4,NULL,4),(NULL,NULL,NULL)")
        .unwrap();
    session.run("ANALYZE TABLE null_estimation").unwrap();

    let cases = [
        (
            "EXPLAIN SELECT b FROM null_estimation WHERE b IS NULL",
            "4.00",
            "IndexRangeScan",
            Some("range:[NULL,NULL]"),
        ),
        (
            "EXPLAIN SELECT b FROM null_estimation WHERE b IS NOT NULL",
            "1.00",
            "IndexFullScan",
            None,
        ),
        (
            "EXPLAIN SELECT b FROM null_estimation WHERE b IS NULL OR b > 3",
            "4.00",
            "IndexRangeScan",
            Some("range:[NULL,NULL], (3,+inf]"),
        ),
        (
            "EXPLAIN SELECT b FROM null_estimation USE INDEX (idx_b)",
            "5.00",
            "IndexFullScan",
            None,
        ),
        (
            "EXPLAIN SELECT b FROM null_estimation WHERE b < 4",
            "1.00",
            "IndexRangeScan",
            Some("range:[-inf,4)"),
        ),
        (
            "EXPLAIN SELECT * FROM null_estimation WHERE a IS NULL",
            "1.00",
            "TableFullScan",
            None,
        ),
        (
            "EXPLAIN SELECT * FROM null_estimation WHERE a IS NOT NULL",
            "4.00",
            "TableFullScan",
            None,
        ),
        (
            "EXPLAIN SELECT * FROM null_estimation WHERE a IS NULL OR a > 3",
            "2.00",
            "TableFullScan",
            None,
        ),
        (
            "EXPLAIN SELECT * FROM null_estimation",
            "5.00",
            "TableFullScan",
            None,
        ),
        (
            "EXPLAIN SELECT * FROM null_estimation WHERE a < 4",
            "3.00",
            "TableFullScan",
            None,
        ),
    ];
    for (sql, expected_rows, expected_scan, expected_range) in cases {
        let plan = row_text(session.run(sql));
        assert_eq!(plan[0][1], expected_rows, "{sql}: {plan:?}");
        let scan = plan
            .iter()
            .find(|row| row[0].contains(expected_scan))
            .unwrap_or_else(|| panic!("{sql}: no {expected_scan} in {plan:?}"));
        assert_eq!(
            scan[1],
            if expected_scan == "TableFullScan" {
                "5.00"
            } else {
                expected_rows
            }
        );
        if let Some(expected_range) = expected_range {
            assert!(scan[4].contains(expected_range), "{sql}: {scan:?}");
        }
    }
}

/// Go `TestUniqCompEqualEst`: complete equalities on a clustered composite
/// primary key resolve to one point-get row after ANALYZE.
#[test]
fn clustered_composite_primary_key_equality_matches_go_point_get() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE uniq_composite (a INT, b INT, PRIMARY KEY (a,b))")
        .unwrap();
    let values = (1..=10)
        .map(|value| format!("(1,{value})"))
        .collect::<Vec<_>>()
        .join(",");
    session
        .run(&format!("INSERT INTO uniq_composite VALUES {values}"))
        .unwrap();
    session.run("ANALYZE TABLE uniq_composite").unwrap();

    let plan = row_text(
        session.run("EXPLAIN SELECT * FROM uniq_composite WHERE a = 1 AND b = 5 AND 1 = 1"),
    );
    let point = plan
        .iter()
        .find(|row| row[0].contains("Point_Get"))
        .unwrap_or_else(|| panic!("expected clustered composite point get: {plan:?}"));
    assert_eq!(point[1], "1.00", "{plan:?}");
    assert!(
        point[3].contains("clustered index:PRIMARY(a, b)"),
        "{plan:?}"
    );
}

/// Go `TestRiskRangeSkewRatioWithinBucket`: session ratios widen an index
/// interval's estimate monotonically. Go resolves `SET SESSION ... = DEFAULT`
/// to the system variable's compiled-in default, independently of a later
/// `SET GLOBAL` value.
#[test]
fn within_bucket_range_skew_setting_changes_analyzed_index_estimate() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE skew_range (a INT, KEY idx_a(a))")
        .unwrap();
    session
        .run("INSERT INTO skew_range VALUES (1),(1),(1),(1),(2),(2),(3),(4),(5),(5)")
        .unwrap();
    session
        .run("ANALYZE TABLE skew_range WITH 0 TOPN, 1 BUCKETS")
        .unwrap();
    let estimate = |session: &mut Session| {
        let rows = row_text(
            session
                .run("EXPLAIN SELECT * FROM skew_range USE INDEX (idx_a) WHERE a >= 2 AND a <= 3"),
        );
        let scan = rows
            .iter()
            .find(|row| row[0].contains("IndexRangeScan"))
            .unwrap_or_else(|| panic!("missing index range scan: {rows:?}"));
        scan[1].parse::<f64>().unwrap()
    };

    session
        .run("SET SESSION tidb_opt_risk_range_skew_ratio = 0")
        .unwrap();
    let zero = estimate(&mut session);
    session
        .run("SET SESSION tidb_opt_risk_range_skew_ratio = 0.5")
        .unwrap();
    let half = estimate(&mut session);
    session
        .run("SET SESSION tidb_opt_risk_range_skew_ratio = 1")
        .unwrap();
    let one = estimate(&mut session);
    assert!(zero < half && half < one, "0={zero}, 0.5={half}, 1={one}");

    session
        .run("SET GLOBAL tidb_opt_risk_range_skew_ratio = 0.5")
        .unwrap();
    let retained_session_value = estimate(&mut session);
    assert!(half < retained_session_value);
    session
        .run("SET SESSION tidb_opt_risk_range_skew_ratio = DEFAULT")
        .unwrap();
    let default_value = estimate(&mut session);
    assert_eq!(default_value, zero);
    session
        .run("SET GLOBAL tidb_opt_risk_range_skew_ratio = DEFAULT")
        .unwrap();
}

/// Go `TestIndexRangeEstimationWithTruncatedHandleRange`: pruning estimate
/// ranges to declared index columns must retain valid bound inclusivity, and
/// complete point ranges may use appended-handle selectivity and the point cap.
#[test]
fn truncated_integer_handle_ranges_match_go_cardinality_estimates() {
    let mut session = Session::new();
    session
        .run(
            "CREATE TABLE truncated_handle_estimate (
                id BIGINT PRIMARY KEY CLUSTERED,
                a INT,
                KEY ia(a)
            )",
        )
        .unwrap();
    let values = (1..=100)
        .map(|id| format!("({id}, {})", id % 10))
        .collect::<Vec<_>>()
        .join(",");
    session
        .run(&format!(
            "INSERT INTO truncated_handle_estimate VALUES {values}"
        ))
        .unwrap();
    session
        .run("ANALYZE TABLE truncated_handle_estimate ALL COLUMNS")
        .unwrap();

    let index_scan = |session: &mut Session, predicate: &str| {
        let rows = row_text(session.run(&format!(
            "EXPLAIN SELECT * FROM truncated_handle_estimate USE INDEX (ia) WHERE {predicate}"
        )));
        rows.iter()
            .find(|row| row[0].contains("IndexRangeScan"))
            .unwrap_or_else(|| panic!("missing index scan for {predicate}: {rows:?}"))
            .clone()
    };

    let low_exclusive = index_scan(&mut session, "a = 5 AND id > 10");
    assert!(
        low_exclusive[4].contains("range:(5 10,5 +inf]"),
        "{low_exclusive:?}"
    );
    assert_eq!(low_exclusive[1], "10.00");

    let high_exclusive = index_scan(&mut session, "a = 5 AND id < 10");
    assert!(
        high_exclusive[4].contains("range:[5 -inf,5 10)"),
        "{high_exclusive:?}"
    );
    assert_eq!(high_exclusive[1], "10.00");

    let handle_points = index_scan(&mut session, "a = 5 AND id IN (11, 22)");
    assert!(
        handle_points[4].contains("range:[5 11,5 11], [5 22,5 22]"),
        "{handle_points:?}"
    );
    assert_eq!(handle_points[1], "2.00");

    let full_point = index_scan(&mut session, "a = 5 AND id = 7");
    assert!(full_point[4].contains("range:[5 7,5 7]"), "{full_point:?}");
    assert_eq!(full_point[1], "1.00");

    session
        .run(
            "CREATE TABLE unsigned_handle_estimate (
                id BIGINT UNSIGNED PRIMARY KEY CLUSTERED,
                a INT,
                KEY ia(a)
            )",
        )
        .unwrap();
    session
        .run(&format!(
            "INSERT INTO unsigned_handle_estimate VALUES {values}"
        ))
        .unwrap();
    session
        .run("ANALYZE TABLE unsigned_handle_estimate ALL COLUMNS")
        .unwrap();
    let unsigned_point = row_text(session.run(
        "EXPLAIN SELECT * FROM unsigned_handle_estimate USE INDEX (ia) \
         WHERE a = 5 AND id IN (11, 22)",
    ));
    let unsigned_scan = unsigned_point
        .iter()
        .find(|row| row[0].contains("IndexRangeScan"))
        .unwrap_or_else(|| panic!("missing unsigned index scan: {unsigned_point:?}"));
    assert!(
        unsigned_scan[4].contains("range:[5,5]"),
        "{unsigned_scan:?}"
    );
    assert!(!unsigned_scan[4].contains("5 11"), "{unsigned_scan:?}");
    assert_eq!(unsigned_scan[1], "10.00");
}

/// Go `cardinality.recordUsedItemStatsStatus` and scan `FormatForExplain`:
/// fully loaded items are omitted, while missing, uninitialized, and evicted
/// items are attached to the statement's scans without leaking to later SQL.
#[test]
fn explain_marks_missing_stats_for_selected_index() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE explain_partial_index (a INT, b INT, payload INT, KEY idx_ab(a,b))")
        .unwrap();
    session
        .run("INSERT INTO explain_partial_index VALUES (1,1,1),(2,2,2),(3,3,3),(4,4,4)")
        .unwrap();
    session.run("ANALYZE TABLE explain_partial_index").unwrap();

    let shared = session.shared_catalog();
    let (table_id, index_id, column_b_id, original_statistics) = {
        let catalog = shared.lock().unwrap();
        let table = match catalog.table_in("test", "explain_partial_index").unwrap() {
            tidb_executor::TableEntry::Kv(table) => table,
            _ => panic!("explain_partial_index is not a KV table"),
        };
        let statistics = catalog.table_statistics(table.table_id).unwrap().clone();
        (
            table.table_id,
            table
                .indexes()
                .iter()
                .find(|index| index.name.eq_ignore_ascii_case("idx_ab"))
                .expect("idx_ab")
                .id,
            table
                .columns
                .iter()
                .find(|column| column.name.eq_ignore_ascii_case("b"))
                .expect("column b")
                .id,
            statistics,
        )
    };
    let fully_loaded = row_text(session.run(
        "EXPLAIN SELECT payload FROM explain_partial_index USE INDEX (idx_ab) WHERE a = 2 AND b = 2",
    ));
    let fully_loaded_scan = fully_loaded
        .iter()
        .find(|row| row[0].contains("IndexRangeScan"))
        .unwrap_or_else(|| panic!("missing index range scan: {fully_loaded:?}"));
    assert!(
        !fully_loaded_scan[4].contains("stats:partial"),
        "fully loaded index statistics must not be marked partial: {fully_loaded:?}"
    );
    {
        let mut catalog = shared.lock().unwrap();
        let mut statistics = (*catalog.table_statistics(table_id).unwrap()).clone();
        statistics.indexes.remove(&index_id);
        statistics.index_load_status.remove(&index_id);
        statistics.index_stats_existence.remove(&index_id);
        catalog.set_table_statistics(table_id, std::sync::Arc::new(statistics));
    }

    let rows = row_text(session.run(
        "EXPLAIN SELECT payload FROM explain_partial_index USE INDEX (idx_ab) WHERE a = 2 AND b = 2",
    ));
    let scan = rows
        .iter()
        .find(|row| row[0].contains("IndexRangeScan"))
        .unwrap_or_else(|| panic!("missing index range scan: {rows:?}"));
    assert!(
        scan[4].contains("stats:partial[idx_ab:missing]"),
        "missing used-index status in EXPLAIN: {rows:?}"
    );

    let mut statistics = (*original_statistics).clone();
    statistics
        .index_load_status
        .insert(index_id, tidb_stats::StatsLoadedStatus::all_evicted());
    shared
        .lock()
        .unwrap()
        .set_table_statistics(table_id, std::sync::Arc::new(statistics));
    let rows = row_text(session.run(
        "EXPLAIN SELECT payload FROM explain_partial_index USE INDEX (idx_ab) WHERE a = 2 AND b = 2",
    ));
    let scan = rows
        .iter()
        .find(|row| row[0].contains("IndexRangeScan"))
        .unwrap_or_else(|| panic!("missing index range scan: {rows:?}"));
    assert!(
        scan[4].contains("stats:partial[idx_ab:allEvicted]"),
        "evicted used-index status in EXPLAIN: {rows:?}"
    );

    let mut statistics = (*original_statistics).clone();
    statistics.columns.remove(&column_b_id);
    statistics.column_load_status.remove(&column_b_id);
    statistics.column_stats_existence.remove(&column_b_id);
    shared
        .lock()
        .unwrap()
        .set_table_statistics(table_id, std::sync::Arc::new(statistics));
    let rows = row_text(session.run(
        "EXPLAIN SELECT payload FROM explain_partial_index IGNORE INDEX (idx_ab) WHERE b = 2",
    ));
    let scan = rows
        .iter()
        .find(|row| row[0].contains("TableFullScan"))
        .unwrap_or_else(|| panic!("missing table scan: {rows:?}"));
    assert!(
        scan[4].contains("stats:partial[b:missing]"),
        "missing used-column status in EXPLAIN: {rows:?}"
    );

    let mut statistics = (*original_statistics).clone();
    statistics.columns.remove(&column_b_id);
    statistics.column_load_status.remove(&column_b_id);
    statistics.column_stats_existence.insert(column_b_id, true);
    shared
        .lock()
        .unwrap()
        .set_table_statistics(table_id, std::sync::Arc::new(statistics));
    let rows = row_text(session.run(
        "EXPLAIN SELECT payload FROM explain_partial_index IGNORE INDEX (idx_ab) WHERE b = 2",
    ));
    let scan = rows
        .iter()
        .find(|row| row[0].contains("TableFullScan"))
        .unwrap_or_else(|| panic!("missing table scan: {rows:?}"));
    assert!(
        scan[4].contains("stats:partial[b:unInitialized]"),
        "analyzed but uninitialized column status missing from EXPLAIN: {rows:?}"
    );

    // A later statement must not inherit the preceding statement's statuses.
    shared
        .lock()
        .unwrap()
        .set_table_statistics(table_id, original_statistics);
    let rows = row_text(session.run(
        "EXPLAIN SELECT payload FROM explain_partial_index IGNORE INDEX (idx_ab) WHERE b = 2",
    ));
    let scan = rows
        .iter()
        .find(|row| row[0].contains("TableFullScan"))
        .unwrap_or_else(|| panic!("missing table scan: {rows:?}"));
    assert!(
        !scan[4].contains("stats:partial"),
        "fully loaded statistics in a later statement inherited old statuses: {rows:?}"
    );
}

/// Port of Go `TestPartialStatsInExplain`'s SQL matrix. The compact in-memory
/// session has no asynchronous histogram loader, so model its before/after
/// states by changing the cached item load statuses around the same queries.
#[test]
fn explain_partial_stats_match_go_plan_matrix() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE t(a INT, b INT, c INT, PRIMARY KEY(a), KEY idx(b))")
        .unwrap();
    session
        .run("INSERT INTO t VALUES (1,1,1),(2,2,2),(3,3,3)")
        .unwrap();
    session
        .run("CREATE TABLE t2(a INT, PRIMARY KEY(a))")
        .unwrap();
    session.run("INSERT INTO t2 VALUES (1),(2),(3)").unwrap();
    session
        .run(
            "CREATE TABLE tp(a INT, b INT, c INT, INDEX ic(c)) \
             PARTITION BY RANGE(a) (PARTITION p0 VALUES LESS THAN (10), \
             PARTITION p1 VALUES LESS THAN (20), PARTITION p2 VALUES LESS THAN MAXVALUE)",
        )
        .unwrap();
    session
        .run("INSERT INTO tp VALUES (1,1,1),(2,2,2),(13,13,13),(14,14,14),(25,25,25),(36,36,36)")
        .unwrap();
    session.run("ANALYZE TABLE t").unwrap();
    session.run("ANALYZE TABLE t2").unwrap();
    session.run("ANALYZE TABLE tp").unwrap();

    let shared = session.shared_catalog();
    let (tp_id, tp_b_id, tp_original, t_id, t_original) = {
        let catalog = shared.lock().unwrap();
        let table = match catalog.table_in("test", "tp").unwrap() {
            tidb_executor::TableEntry::Kv(table) => table,
            _ => panic!("tp is not a KV table"),
        };
        let column_b_id = table
            .columns
            .iter()
            .find(|column| column.name.eq_ignore_ascii_case("b"))
            .unwrap()
            .id;
        let t = match catalog.table_in("test", "t").unwrap() {
            tidb_executor::TableEntry::Kv(table) => table,
            _ => panic!("t is not a KV table"),
        };
        (
            table.table_id,
            column_b_id,
            catalog.table_statistics(table.table_id).unwrap().clone(),
            t.table_id,
            catalog.table_statistics(t.table_id).unwrap().clone(),
        )
    };

    let query_tp = "EXPLAIN FORMAT = BRIEF SELECT * FROM tp WHERE b = 10";
    let mut partial_tp = (*tp_original).clone();
    partial_tp.columns.remove(&tp_b_id);
    partial_tp.column_load_status.remove(&tp_b_id);
    partial_tp.column_stats_existence.remove(&tp_b_id);
    shared
        .lock()
        .unwrap()
        .set_table_statistics(tp_id, std::sync::Arc::new(partial_tp));
    let plan = row_text(session.run(query_tp));
    let plan_text = plan
        .iter()
        .map(|row| row.join("\t"))
        .collect::<Vec<_>>()
        .join("\n");
    assert!(
        plan_text.contains("stats:partial["),
        "missing partial marker: {plan_text}"
    );
    shared
        .lock()
        .unwrap()
        .set_table_statistics(tp_id, tp_original.clone());
    let plan = row_text(session.run(query_tp));
    let plan_text = plan
        .iter()
        .map(|row| row.join("\t"))
        .collect::<Vec<_>>()
        .join("\n");
    assert!(
        !plan_text.contains("stats:partial["),
        "unexpected partial marker: {plan_text}"
    );

    let query_join = "EXPLAIN FORMAT = BRIEF SELECT * FROM t JOIN tp \
                      WHERE tp.a = 10 AND t.b = tp.c";
    let plan = row_text(session.run(query_join));
    let plan_text = plan
        .iter()
        .map(|row| row.join("\t"))
        .collect::<Vec<_>>()
        .join("\n");
    assert!(
        !plan_text.contains("stats:partial["),
        "unexpected partial marker: {plan_text}"
    );

    let mut evicted_t = (*t_original).clone();
    for id in evicted_t.columns.keys().copied().collect::<Vec<_>>() {
        evicted_t
            .column_load_status
            .insert(id, tidb_stats::StatsLoadedStatus::all_evicted());
    }
    for id in evicted_t.indexes.keys().copied().collect::<Vec<_>>() {
        evicted_t
            .index_load_status
            .insert(id, tidb_stats::StatsLoadedStatus::all_evicted());
    }
    shared
        .lock()
        .unwrap()
        .set_table_statistics(t_id, std::sync::Arc::new(evicted_t.clone()));
    let plan = row_text(session.run(query_join));
    let plan_text = plan
        .iter()
        .map(|row| row.join("\t"))
        .collect::<Vec<_>>()
        .join("\n");
    assert!(
        plan_text.contains("stats:partial["),
        "missing partial marker: {plan_text}"
    );
    assert!(
        plan_text.contains("allEvicted"),
        "missing evicted status: {plan_text}"
    );
    shared
        .lock()
        .unwrap()
        .set_table_statistics(t_id, t_original.clone());
    let plan = row_text(session.run(query_join));
    let plan_text = plan
        .iter()
        .map(|row| row.join("\t"))
        .collect::<Vec<_>>()
        .join("\n");
    assert!(
        !plan_text.contains("stats:partial["),
        "unexpected partial marker: {plan_text}"
    );

    let query_partition_join = "EXPLAIN FORMAT = BRIEF SELECT * FROM t \
        JOIN tp PARTITION (p0) JOIN t2 WHERE t.a < 10 AND t.b = tp.c \
        AND t2.a > 10 AND t2.a = tp.c";
    let plan = row_text(session.run(query_partition_join));
    let plan_text = plan
        .iter()
        .map(|row| row.join("\t"))
        .collect::<Vec<_>>()
        .join("\n");
    assert!(
        plan_text.contains("IndexHashJoin"),
        "Go plan shape missing: {plan_text}"
    );
    shared
        .lock()
        .unwrap()
        .set_table_statistics(t_id, std::sync::Arc::new(evicted_t));
    let plan = row_text(session.run(query_partition_join));
    let plan_text = plan
        .iter()
        .map(|row| row.join("\t"))
        .collect::<Vec<_>>()
        .join("\n");
    assert!(
        plan_text.contains("IndexHashJoin"),
        "Go plan shape missing: {plan_text}"
    );
    assert!(
        plan_text.contains("stats:partial["),
        "missing partial marker: {plan_text}"
    );
    assert!(
        plan_text.contains("allEvicted"),
        "missing evicted status: {plan_text}"
    );
}

/// Go `cardinality.Selectivity`: every dimension of a clustered common
/// handle appended to a non-unique index participates in its estimate range.
#[test]
fn appended_common_handle_ranges_lower_index_scan_estimate() {
    let mut session = Session::new();
    session
        .run(
            "CREATE TABLE appended_common_handle_estimate (
                a BIGINT NOT NULL,
                b BIGINT NOT NULL,
                c BIGINT NOT NULL,
                payload BIGINT,
                PRIMARY KEY (b, c),
                KEY ia(a)
            )",
        )
        .unwrap();
    let values = (1..=100)
        .map(|id| format!("(1, {}, {id}, {id})", id % 10))
        .collect::<Vec<_>>()
        .join(",");
    session
        .run(&format!(
            "INSERT INTO appended_common_handle_estimate VALUES {values}"
        ))
        .unwrap();
    session
        .run("ANALYZE TABLE appended_common_handle_estimate")
        .unwrap();

    let index_scan_estimate = |session: &mut Session, predicate: &str| {
        let rows = row_text(session.run(&format!(
            "EXPLAIN SELECT payload FROM appended_common_handle_estimate USE INDEX (ia) WHERE {predicate}"
        )));
        rows.iter()
            .find(|row| row[0].contains("IndexRangeScan"))
            .unwrap_or_else(|| panic!("missing index scan for {predicate}: {rows:?}"))[1]
            .parse::<f64>()
            .unwrap()
    };

    let prefix_estimate = index_scan_estimate(&mut session, "a = 1");
    let appended_handle_estimate =
        index_scan_estimate(&mut session, "a = 1 AND b = 1 AND c BETWEEN 11 AND 19");
    assert!(
        appended_handle_estimate < prefix_estimate,
        "the complete common-handle range should reduce the index estimate: \
         prefix={prefix_estimate}, with_handle={appended_handle_estimate}"
    );
}

/// Go `TestIndexRangeEstimationWithPrefixedCommonHandle`: execution ranges
/// use the stored prefix, retain remaining common-handle columns, and recheck
/// untruncated predicates while statistics use full column values.
#[test]
fn prefixed_common_handle_ranges_match_go_cardinality_cases() {
    let mut session = Session::new();
    session
        .run(
            "CREATE TABLE prefixed_common_handle_estimate (
                p1 VARCHAR(64),
                p2 INT,
                c INT,
                PRIMARY KEY (p1(2), p2) CLUSTERED,
                KEY ic(c)
            )",
        )
        .unwrap();
    let values = (1..=100)
        .map(|id| format!("('pp_{id:03}', {id}, {})", id % 10))
        .collect::<Vec<_>>()
        .join(",");
    session
        .run(&format!(
            "INSERT INTO prefixed_common_handle_estimate VALUES {values}"
        ))
        .unwrap();
    session
        .run("ANALYZE TABLE prefixed_common_handle_estimate ALL COLUMNS")
        .unwrap();

    let explain = |session: &mut Session, sql: &str| row_text(session.run(sql));
    let prefix_query = "SELECT * FROM prefixed_common_handle_estimate USE INDEX (ic) \
                        WHERE c = 5 AND p1 = 'pp_055'";
    let prefix_plan = explain(&mut session, &format!("EXPLAIN {prefix_query}"));
    let prefix_scan = prefix_plan
        .iter()
        .find(|row| row[0].contains("IndexRangeScan"))
        .unwrap_or_else(|| panic!("missing prefix index scan: {prefix_plan:?}"));
    assert!(
        prefix_scan[4].contains("range:[5 \"pp\",5 \"pp\"]"),
        "{prefix_scan:?}"
    );
    let selection = prefix_plan
        .iter()
        .find(|row| row[0].contains("Selection"))
        .unwrap_or_else(|| panic!("missing prefix predicate recheck: {prefix_plan:?}"));
    assert!(
        selection[4].contains("eq(test.prefixed_common_handle_estimate.p1, \"pp_055\")"),
        "{selection:?}"
    );

    let second_handle_query = "EXPLAIN SELECT * FROM prefixed_common_handle_estimate \
        USE INDEX (ic) WHERE c = 5 AND p1 = 'pp_055' AND p2 = 55";
    let second_handle_plan = explain(&mut session, second_handle_query);
    let second_handle_scan = second_handle_plan
        .iter()
        .find(|row| row[0].contains("IndexRangeScan"))
        .unwrap_or_else(|| panic!("missing two-column handle range: {second_handle_plan:?}"));
    assert!(
        second_handle_scan[4].contains("range:[5 \"pp\" 55,5 \"pp\" 55]"),
        "{second_handle_scan:?}"
    );

    let tuple_query = "SELECT * FROM prefixed_common_handle_estimate \
                       WHERE (c, p1, p2) > (5, 'pp_055', 55)";
    let tuple_plan = explain(&mut session, &format!("EXPLAIN {tuple_query}"));
    let tuple_selection = tuple_plan
        .iter()
        .find(|row| row[0].contains("Selection"))
        .unwrap_or_else(|| panic!("missing tuple Selection: {tuple_plan:?}"));
    assert_eq!(tuple_selection[1], "40.00", "{tuple_plan:?}");
    assert_eq!(row_text(session.run(tuple_query)).len(), 44);

    let forced_tuple_query = "SELECT * FROM prefixed_common_handle_estimate USE INDEX (ic) \
                              WHERE (c, p1, p2) > (5, 'pp_055', 55)";
    let forced_tuple_plan = explain(&mut session, &format!("EXPLAIN {forced_tuple_query}"));
    let forced_tuple_scan = forced_tuple_plan
        .iter()
        .find(|row| row[0].contains("IndexRangeScan"))
        .unwrap_or_else(|| panic!("missing forced tuple scan: {forced_tuple_plan:?}"));
    assert!(
        forced_tuple_scan[4].contains("range:[5 \"pp\",5 +inf], (5,+inf]"),
        "{forced_tuple_scan:?}"
    );
    assert_eq!(forced_tuple_scan[1], "50.00");
    assert_eq!(row_text(session.run(forced_tuple_query)).len(), 44);
}

/// Go `TestDefaultStringMatchSelectivityZeroImprovesLikeEstimation`:
/// setting the default string-match selectivity to zero lets the analyzed
/// TopN estimate a selective infix LIKE more accurately.
#[test]
fn default_string_match_selectivity_zero_improves_like_estimates() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE default_string_match_estimate (a VARCHAR(64))")
        .unwrap();
    let values = (0..100)
        .map(|row| {
            if row < 5 {
                "('needle target')"
            } else {
                "('other value')"
            }
        })
        .collect::<Vec<_>>()
        .join(",");
    session
        .run(&format!(
            "INSERT INTO default_string_match_estimate VALUES {values}"
        ))
        .unwrap();
    session
        .run("ANALYZE TABLE default_string_match_estimate WITH 2 TOPN")
        .unwrap();

    let actual = row_text(
        session.run("SELECT COUNT(*) FROM default_string_match_estimate WHERE a LIKE '%needle%'"),
    );
    assert_eq!(actual, [["5"]]);

    let table_reader_estimate = |session: &mut Session| {
        let rows = row_text(
            session
                .run("EXPLAIN SELECT * FROM default_string_match_estimate WHERE a LIKE '%needle%'"),
        );
        rows.iter()
            .find(|row| row[0].contains("TableReader"))
            .unwrap_or_else(|| panic!("missing table reader for LIKE query: {rows:?}"))[1]
            .parse::<f64>()
            .unwrap()
    };

    session
        .run("SET @@tidb_default_string_match_selectivity = 0.8")
        .unwrap();
    let default_estimate = table_reader_estimate(&mut session);
    assert_eq!(default_estimate, 80.0);
    session
        .run("SET @@tidb_default_string_match_selectivity = 0")
        .unwrap();
    let topn_assisted_estimate = table_reader_estimate(&mut session);

    assert!(
        (topn_assisted_estimate - 5.0).abs() < (default_estimate - 5.0).abs(),
        "TopN-assisted LIKE estimate should be closer to 5 actual rows: \
         default={default_estimate}, topn={topn_assisted_estimate}"
    );
}

/// Go `TestDNFCondSelectivity` keeps these planner safety cases alongside the
/// numeric selectivity goldens: hidden-rowid predicates must plan, adding an
/// unanalyzed timestamp column must terminate DNF estimation, and mixed
/// BLOB/DECIMAL/TIMESTAMP NOT-BETWEEN ranges must survive EXPLAIN.
#[test]
fn dnf_selectivity_safety_cases_match_go_smoke_coverage() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE dnf_safety (a INT, b INT, c INT, d INT, INDEX idx(a,b,c,d))")
        .unwrap();

    // Go issue 19981: the hidden row id can appear in both OR arms.
    session
        .run("SELECT * FROM dnf_safety WHERE _tidb_rowid IS NULL OR _tidb_rowid > 7")
        .unwrap();

    // Go issue 22134: stats are not available for this new column yet. The
    // disjunction must use the missing-statistics guard instead of recursing.
    session
        .run("ALTER TABLE dnf_safety ADD COLUMN n TIMESTAMP")
        .unwrap();
    session
        .run("SELECT * FROM dnf_safety WHERE n = '2000-01-01' OR n = '2000-01-02'")
        .unwrap();

    // Go issue 27294: range construction over these mixed types used to fail
    // while planning this NOT-BETWEEN DNF expression.
    session
        .run("CREATE TABLE dnf_range_safety (COL1 BLOB DEFAULT NULL, COL2 DECIMAL(37,4) DEFAULT NULL, COL3 TIMESTAMP NULL DEFAULT NULL, COL4 INT(11) DEFAULT NULL, UNIQUE KEY U_M_COL4(COL1(10),COL2), UNIQUE KEY U_M_COL5(COL3,COL2))")
        .unwrap();
    let explain = row_text(session.run(
        "EXPLAIN FORMAT = 'brief' SELECT * FROM dnf_range_safety \
         WHERE col1 IS NOT NULL OR \
         col2 NOT BETWEEN 454623814170074.2771 AND -975540642273402.9269 AND \
         col3 NOT BETWEEN '2039-1-19 10:14:57' AND '2002-3-27 14:40:23'",
    ));
    assert!(!explain.is_empty(), "the source Go smoke query must plan");
}

/// Go `TestBuiltinInEstWithoutStats`: the pseudo distribution for eight
/// equality points over a ten-row table must keep the one-row selection
/// floor, for either column, without inventing analyzed column statistics.
#[test]
fn builtin_in_estimate_without_stats_keeps_selection_floor() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE builtin_in_pseudo (a INT, b INT)")
        .unwrap();
    session
        .run("INSERT INTO builtin_in_pseudo VALUES (1,1),(2,2),(3,3),(4,4),(5,5),(6,6),(7,7),(8,8),(9,9),(10,10)")
        .unwrap();
    session.shared_catalog().lock().unwrap().flush_stats_delta();
    let shared = session.shared_catalog();
    let catalog = shared.lock().unwrap();
    let table_id = match catalog.table_in("test", "builtin_in_pseudo").unwrap() {
        tidb_executor::TableEntry::Kv(table) => table.table_id,
        _ => panic!("builtin_in_pseudo is not a KV table"),
    };
    let statistics = catalog
        .table_statistics(table_id)
        .expect("flushed stats_meta produces a cached statistics table");
    assert!(statistics.pseudo);
    assert!(!statistics.cache_pseudo);
    assert_eq!(
        statistics.column_stats_existence,
        std::collections::BTreeMap::from([(1, false), (2, false)])
    );
    assert!(statistics.index_stats_existence.is_empty());
    drop(catalog);

    for column in ["a", "b"] {
        let rows = row_text(session.run(&format!(
            "EXPLAIN FORMAT = 'brief' SELECT * FROM builtin_in_pseudo WHERE {column} IN (1,2,3,4,5,6,7,8)"
        )));
        assert_eq!(rows.len(), 3, "{column}: {rows:?}");
        assert_eq!(rows[0][0], "TableReader", "{column}: {rows:?}");
        assert_eq!(rows[0][1], "1.00", "{column}: {rows:?}");
        assert_eq!(rows[1][0], "└─Selection", "{column}: {rows:?}");
        assert_eq!(rows[1][1], "1.00", "{column}: {rows:?}");
        assert_eq!(rows[2][0], "  └─TableFullScan", "{column}: {rows:?}");
        assert_eq!(rows[2][1], "10.00", "{column}: {rows:?}");
        assert_eq!(
            rows[2][4], "keep order:false, stats:pseudo",
            "{column}: {rows:?}"
        );
    }
}

/// Go `TestRangeStepOverflow`: after ANALYZE loads a DATETIME histogram whose
/// values are far below the query bounds, detaching the wide year range must
/// remain valid on both the stats-loading and steady-state execution paths.
#[test]
fn datetime_range_step_overflow_survives_stats_loading() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE datetime_range_overflow (col DATETIME)")
        .unwrap();
    session
        .run("INSERT INTO datetime_range_overflow VALUES ('3580-05-26 07:16:48'),('4055-03-06 22:27:16'),('4862-01-26 07:16:54')")
        .unwrap();
    session
        .run("ANALYZE TABLE datetime_range_overflow")
        .unwrap();

    let sql = "SELECT * FROM datetime_range_overflow \
               WHERE col BETWEEN '8499-1-23 2:14:38' AND '9961-7-23 18:35:26'";
    assert!(row_text(session.run(sql)).is_empty());
    assert!(row_text(session.run(sql)).is_empty());
}

/// An analyzed-table reference checked against Go master. The threshold
/// gates LIMIT adjustment; it does not force the selective index to win.
#[test]
fn analyzed_ordering_threshold_preserves_go_cost_choice() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE ordering_threshold (a INT PRIMARY KEY, b INT, c INT, INDEX ib(b), INDEX ic(c))")
        .unwrap();
    let values = (1..=1000)
        .map(|row| format!("({row}, {row}, {})", (row * 337) % 1000))
        .collect::<Vec<_>>()
        .join(",");
    session
        .run(&format!("INSERT INTO ordering_threshold VALUES {values}"))
        .unwrap();
    session
        .run("ANALYZE TABLE ordering_threshold WITH 8 BUCKETS, 0 TOPN")
        .unwrap();
    session
        .run("SET tidb_opt_ordering_index_selectivity_ratio = -1")
        .unwrap();

    let cases = [
        (
            "",
            vec![
                [
                    "TopN",
                    "1.00",
                    "root",
                    "",
                    "test.ordering_threshold.c, offset:0, count:1",
                ],
                ["└─TableReader", "1.00", "root", "", "data:TopN"],
                [
                    "  └─TopN",
                    "1.00",
                    "cop[tikv]",
                    "",
                    "test.ordering_threshold.c, offset:0, count:1",
                ],
                [
                    "    └─Selection",
                    "52.00",
                    "cop[tikv]",
                    "",
                    "ge(test.ordering_threshold.b, 950)",
                ],
                [
                    "      └─TableFullScan",
                    "1000.00",
                    "cop[tikv]",
                    "table:ordering_threshold",
                    "keep order:false",
                ],
            ],
        ),
        (
            "FORCE INDEX(ic)",
            vec![
                ["Limit", "1.00", "root", "", "offset:0, count:1"],
                ["└─IndexLookUp", "1.00", "root", "", ""],
                [
                    "  ├─IndexFullScan(Build)",
                    "19.23",
                    "cop[tikv]",
                    "table:ordering_threshold, index:ic(c)",
                    "keep order:true",
                ],
                [
                    "  └─Selection(Probe)",
                    "1.00",
                    "cop[tikv]",
                    "",
                    "ge(test.ordering_threshold.b, 950)",
                ],
                [
                    "    └─TableRowIDScan",
                    "19.23",
                    "cop[tikv]",
                    "table:ordering_threshold",
                    "keep order:false",
                ],
            ],
        ),
        (
            "FORCE INDEX(ib)",
            vec![
                [
                    "TopN",
                    "1.00",
                    "root",
                    "",
                    "test.ordering_threshold.c, offset:0, count:1",
                ],
                ["└─IndexLookUp", "1.00", "root", "", ""],
                [
                    "  ├─IndexRangeScan(Build)",
                    "52.00",
                    "cop[tikv]",
                    "table:ordering_threshold, index:ib(b)",
                    "range:[950,+inf], keep order:false",
                ],
                [
                    "  └─TopN(Probe)",
                    "1.00",
                    "cop[tikv]",
                    "",
                    "test.ordering_threshold.c, offset:0, count:1",
                ],
                [
                    "    └─TableRowIDScan",
                    "52.00",
                    "cop[tikv]",
                    "table:ordering_threshold",
                    "keep order:false",
                ],
            ],
        ),
    ];
    for threshold in ["0", "0.1"] {
        session
            .run(&format!(
                "SET tidb_opt_ordering_index_selectivity_threshold = {threshold}"
            ))
            .unwrap();
        for (hint, expected) in &cases {
            let actual = row_text(session.run(&format!(
                "EXPLAIN FORMAT='brief' SELECT * FROM ordering_threshold {hint} WHERE b >= 950 ORDER BY c LIMIT 1"
            )));
            assert_eq!(&actual, expected, "threshold={threshold}, hint={hint}");
        }
    }
}

/// Go cardinality.TestVirtualColumnIndexEstimation: a missing virtual-column
/// histogram must not remove the most selective dimension from the estimate.
#[test]
fn virtual_column_index_estimation_preserves_the_selective_suffix() {
    let mut session = Session::new();
    session.run("SET tidb_analyze_version = 2").unwrap();
    session.run("CREATE TABLE virtual_estimate(a INT, b INT, c INT, d INT AS (c + 1) VIRTUAL, INDEX iabd(a,b,d))").unwrap();
    let values = (1..=500)
        .map(|n| format!("({},{},{n})", n % 5, n % 5))
        .collect::<Vec<_>>()
        .join(",");
    session
        .run(&format!(
            "INSERT INTO virtual_estimate(a,b,c) VALUES {values}"
        ))
        .unwrap();
    session
        .run("ANALYZE TABLE virtual_estimate WITH 8 BUCKETS, 0 TOPN")
        .unwrap();
    session
        .with_catalog_mut(|catalog| {
            let Some(tidb_executor::TableEntry::Kv(table)) =
                catalog.table_in("test", "virtual_estimate")
            else {
                panic!("missing test table");
            };
            let stats = catalog.table_statistics(table.table_id).unwrap();
            let virtual_id = table
                .columns
                .iter()
                .find(|column| column.name == "d")
                .unwrap()
                .id;
            assert!(
                !stats.columns.contains_key(&virtual_id),
                "Go never publishes a virtual-column histogram"
            );
            assert!(
                !stats.indexes.is_empty(),
                "the composite index must retain statistics"
            );
            Ok(())
        })
        .unwrap();
    session
        .run("CREATE TABLE ordinary_estimate(a INT, b INT, c INT, d INT, INDEX iabd(a,b,d))")
        .unwrap();
    session
        .run("INSERT INTO ordinary_estimate SELECT a,b,c,d FROM virtual_estimate")
        .unwrap();
    session
        .run("ANALYZE TABLE ordinary_estimate COLUMNS a,b WITH 8 BUCKETS, 0 TOPN")
        .unwrap();
    for (table, virtual_column) in [("virtual_estimate", true), ("ordinary_estimate", false)] {
        let predicate = "a = 1 AND b = 1 AND d > 447";
        let count = row_text(session.run(&format!(
            "SELECT COUNT(*) FROM {table} USE INDEX(iabd) WHERE {predicate}"
        )));
        assert_eq!(count, vec![vec!["10".to_owned()]]);
        let rows = row_text(session.run(&format!(
            "EXPLAIN SELECT * FROM {table} USE INDEX(iabd) WHERE {predicate}"
        )));
        let estimate = rows[0][1].parse::<f64>().unwrap();
        if virtual_column {
            assert!(estimate < 25.0, "{table}: {rows:?}");
        } else {
            assert!(estimate > 10.0, "{table}: {rows:?}");
        }
    }
}

/// Small Web3Bench aggregates, pinned to Go master's live plans
/// (fdfadb96b2, unistore): COUNT(DISTINCT) stays a SINGLE-phase root
/// HashAgg over a plain IndexFullScan -- with
/// `tidb_opt_distinct_agg_push_down` OFF, `applyLogicalAggregationHint`
/// only prefers a root-task plan and `NewPartialAggregate` never splits a
/// DISTINCT aggregation. The UNION-derived COUNT is a root HashAgg over
/// the Union of two cop Projections. The tiny covering-index COUNT keeps
/// the two-phase shape: root StreamAgg over TableReader over cop
/// StreamAgg over IndexRangeScan.
#[test]
fn web3bench_small_aggregates_follow_go_cost_boundary() {
    let mut session = Session::new();
    session
        .run(
            "CREATE TABLE web3_agg (id BIGINT PRIMARY KEY, from_address VARCHAR(32), value BIGINT,
             KEY idx_from (from_address))",
        )
        .unwrap();
    session
        .run(
            "INSERT INTO web3_agg VALUES
             (1, 'a1', 10), (2, 'a2', 20), (3, 'a1', 30), (4, 'a3', 40)",
        )
        .unwrap();

    // Shape 1 -- COUNT(DISTINCT): the whole aggregation stays at root. Go's
    // own plan (captured live): `HashAgg_7 | 1.00 | root |
    // funcs:count(distinct ...)` over `IndexReader_17 | index:IndexFullScan_16`
    // over the plain index full scan.
    // Brief format compares this cost-boundary choice without coupling it to
    // IDs allocated for other candidates that the optimizer also considered.
    let distinct = row_text(
        session.run("EXPLAIN FORMAT = 'brief' SELECT COUNT(DISTINCT from_address) FROM web3_agg"),
    );
    assert_eq!(
        distinct.iter().map(|row| row.join("|")).collect::<Vec<_>>(),
        vec![
            "HashAgg|1.00|root||funcs:count(distinct test.web3_agg.from_address)->Column#5",
            "└─IndexReader|10000.00|root||index:IndexFullScan",
            "  └─IndexFullScan|10000.00|cop[tikv]|table:web3_agg, index:idx_from(from_address)|keep order:false, stats:pseudo",
        ]
    );

    // Shape 2 -- COUNT over a UNION ALL source: a root HashAgg over the
    // Union of two cop Projections (Go's live capture; the pin that
    // expected a StreamAgg predated the projection-retaining Union shape).
    let union = row_text(session.run(
        "EXPLAIN SELECT COUNT(*) FROM
         (SELECT from_address FROM web3_agg WHERE id <= 2
          UNION ALL
          SELECT from_address FROM web3_agg WHERE id >= 3) AS temp",
    ));
    assert!(
        union
            .iter()
            .any(|row| row[0].contains("HashAgg") && row[2] == "root"),
        "the UNION-derived COUNT keeps its root aggregation: {union:?}"
    );
    assert!(
        union
            .iter()
            .any(|row| row[0].contains("Union") && row[2] == "root"),
        "the derived source stays a Union: {union:?}"
    );

    // Shape 3 -- ANALYZE then a tiny covering-index COUNT: the two-phase
    // shape with a cop partial `count(1)`.
    session.run("ANALYZE TABLE web3_agg").unwrap();
    let indexed_count =
        row_text(session.run("EXPLAIN SELECT COUNT(*) FROM web3_agg WHERE from_address = 'a1'"));
    assert!(
        indexed_count.iter().any(|row| {
            row[2] == "root"
                && row[0]
                    .trim_start_matches('└')
                    .trim_start_matches(' ')
                    .starts_with("StreamAgg")
        }),
        "tiny covering-index COUNT should use Go's StreamAgg root: {indexed_count:?}"
    );
}

/// A pushed `WHERE` keeps its `Selection` BETWEEN the partial aggregate and
/// the scan, all three in the same coprocessor task.
///
/// Captured from Go master (`fdfadb96b2`, unistore): the two-phase HASH
/// aggregate wins the cost comparison for this ungrouped count over an
/// unordered pseudo scan --
///
/// ```text
/// HashAgg_13 1.00 root funcs:count(Column#6)->Column#5
/// └─TableReader_14 1.00 root data:HashAgg_6
///   └─HashAgg_6 1.00 cop[tikv] funcs:count(1)->Column#6
///     └─Selection_12 3333.33 cop[tikv] gt(test.t.a, 10)
///       └─TableFullScan_11 10000.00 cop[tikv] table:t keep order:false, stats:pseudo
/// ```
///
/// (Older recordings show a StreamAgg in these slots; the current cost
/// model's division of the cop hash work by the final concurrency made
/// hash cheaper.) The partial aggregate goes to the top of the COP TASK,
/// not directly onto the scan. The constant is the refined one for the
/// reason [`crate::tests_compare_refinement`] states: Go runs `refineArgs`
/// before it builds the comparison at all, so `int_col > '10ab'` is
/// `gt(..., 10)` everywhere -- in the plan text, and in what the scan is
/// asked to evaluate.
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
            "HashAgg_13|1.00|root||funcs:count(Column#6)->Column#5",
            "└─TableReader_14|1.00|root||data:HashAgg_6",
            "  └─HashAgg_6|1.00|cop[tikv]||funcs:count(1)->Column#6",
            "    └─Selection_12|3333.33|cop[tikv]||gt(test.t.a, 10)",
            "      └─TableFullScan_11|10000.00|cop[tikv]|table:t|keep order:false, stats:pseudo",
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
    assert!(rows[0][0].starts_with("TableReader_"), "{rows:?}");
    assert_eq!(rows[0][2], "2"); // the pass-through reader's own count.
    assert!(rows[1][0].starts_with("└─Selection_"), "{rows:?}");
    assert_eq!(rows[1][2], "2"); // actRows: real, not the 3333.33 estimate.
    assert!(rows[2][0].starts_with("  └─TableFullScan_"), "{rows:?}");
    assert_eq!(rows[2][2], "4");
    assert_eq!(rows[0][3], "root");
    assert_eq!(rows[1][3], "cop[tikv]");
    assert_eq!(rows[2][3], "cop[tikv]");
    // Go exec.Next counts the final empty call as well as the result batch.
    assert!(rows[0][5].starts_with("time:"), "{rows:?}");
    assert!(rows[0][5].contains("loops:2"), "{rows:?}");
    // TiKV timing and memory/disk counters are not collected at this seam.
    for row in &rows {
        assert_eq!(row[7], "N/A"); // memory
        assert_eq!(row[8], "N/A"); // disk
    }
    assert_eq!(rows[1][5], "N/A"); // no fabricated coprocessor timing
    assert_eq!(rows[2][5], "N/A");

    session.run("SET tidb_init_chunk_size = 32").unwrap();
    session.run("SET tidb_max_chunk_size = 32").unwrap();
    let values = (5..=65)
        .map(|id| format!("({id},{id})"))
        .collect::<Vec<_>>()
        .join(",");
    session
        .run(&format!("INSERT INTO t VALUES {values}"))
        .unwrap();
    for _ in 0..2 {
        let rows = row_text(session.run("EXPLAIN ANALYZE SELECT * FROM t"));
        assert_eq!(rows[0][2], "65");
        assert!(rows[0][5].contains("loops:4"), "{rows:?}");
    }
    let rows = row_text(session.run("EXPLAIN ANALYZE SELECT * FROM t WHERE v > 100"));
    assert_eq!(rows[0][2], "0");
    assert!(rows[0][5].contains("loops:1"), "{rows:?}");
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
/// root `TableReader` over a coprocessor `Selection` (`actRows` `1`, the real
/// number of `WHERE`-matching rows) and `TableFullScan` (`actRows` `4`, the
/// real pre-write row count). The scan is the right read for THIS `WHERE`:
/// `c` is an ordinary column that neither pins a key nor bounds the handle,
/// so both engines read the table. The key and handle shapes are
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
    assert_eq!(rows.len(), 4);
    assert_eq!(rows[0][0], "Update_1");
    assert_eq!(rows[0][2], "0");
    assert_eq!(rows[1][0], "└─TableReader_8");
    assert_eq!(rows[1][2], "1");
    assert_eq!(rows[2][0], "  └─Selection_7");
    assert_eq!(rows[2][2], "1");
    assert_eq!(rows[3][0], "    └─TableFullScan_6");
    assert_eq!(rows[3][2], "4");

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

/// The Go `TestAdaptiveLimitDirectIndexLookUpExecution` query keeps its
/// ordered double-read shape while adaptive admission changes only speculative
/// lookup work. Results, offsets, empty results, and the one-row early stop
/// stay identical with the setting disabled.
#[test]
fn adaptive_limit_direct_index_lookup_preserves_sql_results() {
    let mut session = Session::new();
    session
        .run(
            "CREATE TABLE adaptive_direct_lookup (\
             id INT PRIMARY KEY, order_key INT NOT NULL, filter_col INT NOT NULL, \
             payload VARCHAR(32), KEY idx_order_key(order_key))",
        )
        .unwrap();
    let values = (1..=128)
        .map(|id| {
            let filter_col = if matches!(id, 1 | 3 | 6 | 9 | 12) || id % 16 == 0 {
                1
            } else {
                0
            };
            format!("({id},{id},{filter_col},'payload_{id}')")
        })
        .collect::<Vec<_>>()
        .join(",");
    session
        .run(&format!(
            "INSERT INTO adaptive_direct_lookup VALUES {values}"
        ))
        .unwrap();
    for setting in [
        "SET tidb_index_lookup_size = 32",
        "SET tidb_index_lookup_concurrency = 2",
        "SET tidb_init_chunk_size = 32",
        "SET tidb_max_chunk_size = 32",
    ] {
        session.run(setting).unwrap();
    }

    let sql = "SELECT order_key, payload FROM adaptive_direct_lookup \
               USE INDEX(idx_order_key) \
               WHERE order_key BETWEEN 1 AND 128 AND filter_col = 1 \
               ORDER BY order_key LIMIT 4";
    let plan = row_text(session.run(&format!("EXPLAIN {sql}")))
        .into_iter()
        .flatten()
        .collect::<Vec<_>>()
        .join(" ");
    assert!(plan.contains("IndexLookUp"), "{plan}");
    assert!(plan.contains("Selection"), "{plan}");
    assert!(plan.contains("keep order:true"), "{plan}");

    session
        .run("SET tidb_enable_adaptive_limit_scan = ON")
        .unwrap();
    let on_rows = row_text(session.run(sql));
    assert_eq!(
        on_rows,
        [
            ["1", "payload_1"],
            ["3", "payload_3"],
            ["6", "payload_6"],
            ["9", "payload_9"],
        ]
        .map(|row| row.map(str::to_owned).to_vec())
    );
    let on_analyze = row_text(session.run(&format!("EXPLAIN ANALYZE {sql}")))
        .into_iter()
        .flatten()
        .collect::<Vec<_>>()
        .join(" ");
    assert!(on_analyze.contains("adaptive:{lookup:"), "{on_analyze}");
    assert!(!on_analyze.contains("outer:"), "{on_analyze}");

    let paging_sql = "SELECT order_key, payload FROM adaptive_direct_lookup \
                      USE INDEX(idx_order_key) \
                      WHERE order_key BETWEEN 1 AND 128 AND filter_col >= 0 \
                      ORDER BY order_key LIMIT 1";
    assert_eq!(
        row_text(session.run(paging_sql)),
        [vec!["1".to_owned(), "payload_1".to_owned()]]
    );
    let paging_analyze = row_text(session.run(&format!("EXPLAIN ANALYZE {paging_sql}")))
        .into_iter()
        .flatten()
        .collect::<Vec<_>>()
        .join(" ");
    assert!(
        paging_analyze.contains("adaptive:{lookup:1/1, outstanding:0,"),
        "{paging_analyze}"
    );

    let offset_sql = sql.replace("LIMIT 4", "LIMIT 2, 2");
    let offset_rows = row_text(session.run(&offset_sql));
    assert_eq!(
        offset_rows,
        [["6", "payload_6"], ["9", "payload_9"],].map(|row| row.map(str::to_owned).to_vec())
    );
    let empty_sql = sql.replace("filter_col = 1", "filter_col = 2");
    assert!(row_text(session.run(&empty_sql)).is_empty());
    let empty_analyze = row_text(session.run(&format!("EXPLAIN ANALYZE {empty_sql}")))
        .into_iter()
        .flatten()
        .collect::<Vec<_>>()
        .join(" ");
    assert!(
        empty_analyze.contains("adaptive:{lookup:"),
        "{empty_analyze}"
    );
    let large_limit_sql = sql.replace("LIMIT 4", "LIMIT 100");
    let large_limit_rows = row_text(session.run(&large_limit_sql));
    assert_eq!(large_limit_rows.len(), 13);

    session
        .run("SET tidb_enable_adaptive_limit_scan = OFF")
        .unwrap();
    let off_rows = row_text(session.run(sql));
    assert_eq!(off_rows, on_rows);
    assert_eq!(
        row_text(session.run(paging_sql)),
        [vec!["1".to_owned(), "payload_1".to_owned()]]
    );
    assert_eq!(row_text(session.run(&offset_sql)), offset_rows);
    assert!(row_text(session.run(&empty_sql)).is_empty());
    assert_eq!(row_text(session.run(&large_limit_sql)), large_limit_rows);
    let off_analyze = row_text(session.run(&format!("EXPLAIN ANALYZE {sql}")))
        .into_iter()
        .flatten()
        .collect::<Vec<_>>()
        .join(" ");
    assert!(!off_analyze.contains("adaptive:{"), "{off_analyze}");
}

fn adaptive_index_join_counts(analyze: &str) -> [u64; 6] {
    let fields = analyze
        .split_once("adaptive:{outer:")
        .expect("index-join adaptive stats")
        .1
        .split_once('}')
        .expect("closed adaptive stats")
        .0;
    let (outer, fields) = fields.split_once(", lookup:").expect("outer counters");
    let (lookup, fields) = fields
        .split_once(", outstanding:")
        .expect("lookup counters");
    let (outstanding, _) = fields
        .split_once(", blocked:outer=")
        .expect("outstanding counters");
    let pair = |value: &str| {
        let (left, right) = value.split_once('/').expect("counter pair");
        (
            left.parse::<u64>().expect("left counter"),
            right.parse::<u64>().expect("right counter"),
        )
    };
    let outer = pair(outer);
    let lookup = pair(lookup);
    let outstanding = pair(outstanding);
    [
        outer.0,
        outer.1,
        lookup.0,
        lookup.1,
        outstanding.0,
        outstanding.1,
    ]
}

/// The Go `TestAdaptiveLimitExecution` workload drives the other supported
/// shape: an early-stop ordered index join whose outer reader is an ordered
/// double read. Adaptive mode reports both admission stages and leaves query
/// rows unchanged when switched off.
#[test]
fn adaptive_limit_index_join_preserves_sql_results() {
    let mut session = Session::new();
    session
        .run(
            "CREATE TABLE adaptive_outer (\
             id INT PRIMARY KEY, order_key INT NOT NULL, join_key INT NOT NULL, \
             filter_col INT NOT NULL, payload VARCHAR(32), KEY idx_order_key(order_key))",
        )
        .unwrap();
    session
        .run(
            "CREATE TABLE adaptive_inner (\
             id INT PRIMARY KEY, join_key INT NOT NULL, v INT NOT NULL, \
             KEY idx_join_key(join_key))",
        )
        .unwrap();
    let outer_values = (1..=128)
        .map(|id| {
            let filter_col = if matches!(id, 1 | 3 | 6 | 9 | 12) || id % 16 == 0 {
                1
            } else {
                0
            };
            format!("({id},{id},{id},{filter_col},'payload_{id}')")
        })
        .collect::<Vec<_>>()
        .join(",");
    session
        .run(&format!("INSERT INTO adaptive_outer VALUES {outer_values}"))
        .unwrap();
    let mut inner_values = (1..=64)
        .map(|id| format!("({id},1,{id})"))
        .collect::<Vec<_>>();
    inner_values.extend(
        [(65, 3), (66, 6), (67, 9), (68, 12)].map(|(id, key)| format!("({id},{key},{key})")),
    );
    inner_values.extend((16..=128).step_by(16).map(|key| {
        let id = 1000 + key;
        format!("({id},{key},{key})")
    }));
    session
        .run(&format!(
            "INSERT INTO adaptive_inner VALUES {}",
            inner_values.join(",")
        ))
        .unwrap();
    for setting in [
        "SET tidb_enable_adaptive_limit_scan = ON",
        "SET tidb_index_join_batch_size = 32",
        "SET tidb_index_lookup_size = 32",
        "SET tidb_index_lookup_join_concurrency = 2",
        "SET tidb_index_lookup_concurrency = 2",
        "SET tidb_init_chunk_size = 32",
        "SET tidb_max_chunk_size = 32",
    ] {
        session.run(setting).unwrap();
    }

    let sql = "SELECT /*+ INL_JOIN(i) */ o.payload, i.v \
               FROM adaptive_outer o USE INDEX(idx_order_key) \
               JOIN adaptive_inner i USE INDEX(idx_join_key) \
                 ON o.join_key = i.join_key \
               WHERE o.order_key BETWEEN 1 AND 4 AND o.filter_col > 0 \
               ORDER BY o.order_key LIMIT 40";
    let text = |rows: Vec<Vec<String>>| rows.into_iter().flatten().collect::<Vec<_>>().join(" ");
    let plan = text(row_text(session.run(&format!("EXPLAIN {sql}"))));
    assert!(plan.contains("IndexJoin"), "{plan}");
    assert!(plan.contains("keep order:true"), "{plan}");
    let mut on_rows = row_text(session.run(sql));
    on_rows.sort();
    assert_eq!(on_rows.len(), 40);
    let on_analyze = text(row_text(session.run(&format!("EXPLAIN ANALYZE {sql}"))));
    assert!(on_analyze.contains("adaptive:{outer:"), "{on_analyze}");
    assert!(on_analyze.contains("lookup:"), "{on_analyze}");
    assert!(on_analyze.contains("outstanding:"), "{on_analyze}");
    assert!(on_analyze.contains("blocked:"), "{on_analyze}");

    let budget_sql = "SELECT /*+ INL_JOIN(i) */ o.payload, i.v \
               FROM adaptive_outer o USE INDEX(idx_order_key) \
               JOIN adaptive_inner i USE INDEX(idx_join_key) \
                 ON o.join_key = i.join_key \
               WHERE o.order_key BETWEEN 1 AND 128 AND o.filter_col >= 0 \
               ORDER BY o.order_key LIMIT 4";
    let mut budget_rows = row_text(session.run(budget_sql));
    budget_rows.sort();
    assert_eq!(budget_rows.len(), 4);
    let budget_analyze = text(row_text(
        session.run(&format!("EXPLAIN ANALYZE {budget_sql}")),
    ));
    let [
        outer_fetched,
        outer_consumed,
        lookup_handles,
        lookup_rows,
        outer_outstanding,
        lookup_outstanding,
    ] = adaptive_index_join_counts(&budget_analyze);
    assert!(outer_fetched <= 4, "{budget_analyze}");
    assert!(lookup_handles <= 4, "{budget_analyze}");
    assert!(outer_consumed <= outer_fetched, "{budget_analyze}");
    assert!(lookup_rows <= lookup_handles, "{budget_analyze}");
    assert!(outer_outstanding <= 4, "{budget_analyze}");
    assert!(lookup_outstanding <= 4, "{budget_analyze}");

    let low_selectivity_sql = "SELECT /*+ INL_JOIN(i) */ o.order_key, i.v \
               FROM adaptive_outer o USE INDEX(idx_order_key) \
               JOIN adaptive_inner i USE INDEX(idx_join_key) \
                 ON o.join_key = i.join_key \
               WHERE o.order_key BETWEEN 13 AND 128 AND o.filter_col = 1 \
               ORDER BY o.order_key LIMIT 4";
    let low_rows = row_text(session.run(low_selectivity_sql));
    assert_eq!(
        low_rows,
        [
            vec!["16".to_owned(), "16".to_owned()],
            vec!["32".to_owned(), "32".to_owned()],
            vec!["48".to_owned(), "48".to_owned()],
            vec!["64".to_owned(), "64".to_owned()],
        ]
    );
    let low_analyze = text(row_text(
        session.run(&format!("EXPLAIN ANALYZE {low_selectivity_sql}")),
    ));
    assert!(low_analyze.contains("adaptive:{outer:"), "{low_analyze}");
    let [
        outer_fetched,
        outer_consumed,
        lookup_handles,
        lookup_rows,
        _,
        _,
    ] = adaptive_index_join_counts(&low_analyze);
    assert!(lookup_handles > lookup_rows, "{low_analyze}");
    assert!(lookup_rows >= 4, "{low_analyze}");
    assert!(outer_consumed <= outer_fetched, "{low_analyze}");
    assert!(lookup_rows <= lookup_handles, "{low_analyze}");

    let small_limit_sql = low_selectivity_sql.replace("LIMIT 4", "LIMIT 1");
    let small_rows = row_text(session.run(&small_limit_sql));
    assert_eq!(small_rows, [vec!["16".to_owned(), "16".to_owned()]]);
    let ordered_sql = "SELECT /*+ INL_JOIN(i) */ o.order_key, i.v \
               FROM adaptive_outer o USE INDEX(idx_order_key) \
               JOIN adaptive_inner i USE INDEX(idx_join_key) \
                 ON o.join_key = i.join_key \
               WHERE o.order_key BETWEEN 2 AND 12 AND o.filter_col > 0 \
               ORDER BY o.order_key LIMIT 4";
    assert_eq!(
        row_text(session.run(ordered_sql)),
        [
            vec!["3".to_owned(), "3".to_owned()],
            vec!["6".to_owned(), "6".to_owned()],
            vec!["9".to_owned(), "9".to_owned()],
            vec!["12".to_owned(), "12".to_owned()],
        ]
    );

    session
        .run("SET tidb_enable_adaptive_limit_scan = OFF")
        .unwrap();
    let mut off_rows = row_text(session.run(sql));
    off_rows.sort();
    assert_eq!(off_rows, on_rows);
    let mut off_budget_rows = row_text(session.run(budget_sql));
    off_budget_rows.sort();
    assert_eq!(off_budget_rows, budget_rows);
    assert_eq!(row_text(session.run(low_selectivity_sql)), low_rows);
    assert_eq!(row_text(session.run(&small_limit_sql)), small_rows);
    assert_eq!(
        row_text(session.run(ordered_sql)),
        [
            vec!["3".to_owned(), "3".to_owned()],
            vec!["6".to_owned(), "6".to_owned()],
            vec!["9".to_owned(), "9".to_owned()],
            vec!["12".to_owned(), "12".to_owned()],
        ]
    );
    let off_analyze = text(row_text(session.run(&format!("EXPLAIN ANALYZE {sql}"))));
    assert!(!off_analyze.contains("adaptive:{"), "{off_analyze}");
    let off_budget_analyze = text(row_text(
        session.run(&format!("EXPLAIN ANALYZE {budget_sql}")),
    ));
    assert!(
        !off_budget_analyze.contains("adaptive:{"),
        "{off_budget_analyze}"
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
    assert_eq!(rows[0][0], "Insert_1");
    assert_eq!(rows[0][2], "0");
    assert_eq!(rows[1][0], "└─TableReader_8");
    assert_eq!(rows[1][2], "2");
    assert_eq!(rows[2][0], "  └─Selection_7");
    assert_eq!(rows[2][2], "2");
    assert_eq!(rows[3][0], "    └─TableFullScan_6");
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

    for (sql, root_name) in [
        ("EXPLAIN UPDATE t SET b = 100 WHERE a = 1", "Update_"),
        ("EXPLAIN DELETE FROM t WHERE a = 1", "Delete_"),
    ] {
        let rows = row_text(session.run(sql));
        assert_eq!(rows.len(), 2, "{sql}: {rows:?}");
        assert!(rows[0][0].starts_with(root_name), "{sql}: {rows:?}");
        assert_eq!(rows[0][1..], ["N/A", "root", "", "N/A"], "{sql}");
        assert!(rows[1][0].starts_with("└─Point_Get"), "{sql}: {rows:?}");
        assert_eq!(rows[1][1..4], ["1.00", "root", "table:t"], "{sql}");
        assert_eq!(rows[1][4], "handle:1, lock", "{sql}: {rows:?}");
    }
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

/// Go's `plan_tree` format is the four-column tree used by the planner's
/// TPC-DS source suite.  It accepts the format name, omits `estRows`, and
/// keeps the same operator tree as the ordinary row format.
#[test]
fn explain_plan_tree_format_uses_go_columns() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE t (a BIGINT PRIMARY KEY)")
        .unwrap();
    session.run("INSERT INTO t VALUES (1)").unwrap();

    assert_eq!(
        row_text(session.run("EXPLAIN FORMAT = 'plan_tree' SELECT * FROM t WHERE a = 1")),
        vec![vec![
            "Point_Get".to_owned(),
            "root".to_owned(),
            "table:t".to_owned(),
            "handle:1".to_owned(),
        ]]
    );
}

/// A materialized, multi-use CTE is the shape that previously failed before
/// the driver could build its actual read path.  The plan-only path must now
/// describe that consumer instead of returning the old blanket refusal.
#[test]
fn explain_plan_tree_materialized_cte_is_not_refused() {
    let mut session = Session::new();
    session.run("CREATE TABLE t (a BIGINT)").unwrap();
    session.run("INSERT INTO t VALUES (1)").unwrap();

    let rows = row_text(session.run(
        "EXPLAIN FORMAT = 'plan_tree' WITH x AS (SELECT a FROM t) \
         SELECT x1.a FROM x AS x1 JOIN x AS x2 ON x1.a = x2.a",
    ));
    assert!(rows.iter().any(|row| row[0] == "HashJoin"), "{rows:?}");
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
    assert_eq!(rows.len(), 5);
    assert!(rows[0][0].starts_with("Union_"));
    assert_eq!(rows[0][2], "4");
    assert_eq!(
        rows.iter()
            .skip(1)
            .map(|row| row[2].as_str())
            .collect::<Vec<_>>(),
        vec!["2", "2", "2", "2"]
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
    assert_eq!(rows.len(), 5);
    assert!(rows[0][0].starts_with("Union_"));
    // Go Selectivity uses the sentinel-bound column estimate for a <= 2.
    assert_eq!(rows[0][1], "3335.33");
    assert_eq!(rows[0][2], "root");
    assert_eq!(
        rows.iter()
            .skip(1)
            .map(|row| row[2].as_str())
            .collect::<Vec<_>>(),
        vec!["root", "cop[tikv]", "root", "cop[tikv]"]
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
    // Re-captured through gorun: HashAgg <- Union <- two
    // [TableReader <- TableRangeScan] sides = 6 operator rows.
    assert_eq!(rows.len(), 6);
    assert!(rows[0][0].starts_with("HashAgg_"));
    assert_eq!(rows[0][2], "root");
    assert!(rows[1][0].contains("Union_"));
    assert_eq!(rows[1][2], "root");
    // The two sides, in tree order: root reader over cop range scan, with
    // the pushed ranges [-inf,2] and [2,+inf].
    assert!(rows[2][0].contains("TableReader"));
    assert!(rows[3][0].contains("TableRangeScan"));
    assert!(rows[4][0].contains("TableReader"));
    assert!(rows[5][0].contains("TableRangeScan"));
    assert_eq!(
        rows.iter()
            .skip(2)
            .map(|row| row[2].as_str())
            .collect::<Vec<_>>(),
        vec!["root", "cop[tikv]", "root", "cop[tikv]"]
    );
    assert!(rows[3][4].starts_with("range:[-inf,2]"));
    assert!(rows[5][4].starts_with("range:[2,+inf]"));
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
    assert_eq!(rows.len(), 6);
    assert!(rows[0][0].starts_with("HashAgg_"));
    assert_eq!(rows[0][2], "4");
    assert!(rows[1][0].contains("Union_"));
    assert_eq!(rows[1][2], "5");
    assert_eq!(
        rows.iter()
            .skip(2)
            .map(|row| row[2].as_str())
            .collect::<Vec<_>>(),
        vec!["2", "2", "3", "3"]
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

    // Live Go master (fdfadb96b2, unistore) answers with EIGHT rows: the
    // distinct prefix (HashAgg over the two-sided Union) and the ALL
    // suffix (Point_Get) hang directly under the outer Union. The nine-row
    // shape in older recordings carried an extra operator that current Go
    // no longer builds.
    let rows = row_text(session.run(
        "EXPLAIN ANALYZE \
         (SELECT a FROM t WHERE a <= 2) UNION \
         (SELECT a FROM t WHERE a >= 2 AND a <= 3) UNION ALL \
         (SELECT a FROM t WHERE a = 4)",
    ));
    assert_eq!(rows.len(), 8);
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
        vec!["2", "2", "2", "2", "1"]
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

    // INTERSECT/EXCEPT use join-like physical plans in Go: live master
    // (fdfadb96b2, unistore) plans the INTERSECT ANALYZE as a semi-join
    // HashJoin over two TableReaders (the dedup IS the semi join), so the
    // statement plans and executes instead of refusing.
    assert!(
        row_text(session.run("EXPLAIN ANALYZE (SELECT a FROM t) INTERSECT (SELECT a FROM t)"))
            .iter()
            .any(|row| row[0].contains("HashJoin")
                && row.iter().any(|cell| cell.contains("semi join"))),
        "the INTERSECT analyze must plan through the common physical path"
    );
    assert!(matches!(
        session.run("EXPLAIN FORMAT = 'bogus' SELECT * FROM t"),
        Err(DriverError::Unsupported(reason)) if reason == "unknown EXPLAIN format name"
    ));
}

#[test]
fn explain_analyze_intersect_uses_the_common_physical_plan() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE explain_intersect (a BIGINT PRIMARY KEY)")
        .unwrap();
    session
        .run("INSERT INTO explain_intersect VALUES (1),(2),(3)")
        .unwrap();

    let rows = row_text(session.run(
        "EXPLAIN ANALYZE (SELECT a FROM explain_intersect WHERE a <= 2) \
         INTERSECT (SELECT a FROM explain_intersect WHERE a >= 2)",
    ));
    assert!(
        rows.iter().any(|row| row[0].contains("Join")),
        "Go lowers INTERSECT to a physical semi join: {rows:?}"
    );
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
        // Operator ids shift with the session's statement history; the
        // operator names are the pinned contract.
        assert!(rows[0][0].starts_with("TableReader"), "{sql}");
        assert_eq!(rows[0][2], "root", "{sql}");
        assert!(
            rows[0][4].starts_with("data:Selection"),
            "{sql}: {:?}",
            rows[0][4]
        );
        assert!(
            rows[1][0].ends_with("Selection_2") || rows[1][0].contains("Selection"),
            "{sql}"
        );
        assert_eq!(rows[1][2], "cop[tikv]", "{sql}");
        assert_eq!(rows[1][4], printed, "{sql}");
        assert!(rows[2][0].contains("TableFullScan"), "{sql}");
        assert_eq!(rows[2][1], "10000.00", "{sql}");
        assert_eq!(rows[2][2], "cop[tikv]", "{sql}");
    }

    // Re-captured through gorun against the current pinned tree: BOTH
    // conditions push into the cop Selection (ast.Plus is in the pushdown
    // sets, infer_pushdown.go:198/304), giving the 3-row shape.
    let rows = row_text(session.run("EXPLAIN SELECT a, b FROM t WHERE a > 5 AND b + 1 < 10"));
    assert_eq!(rows.len(), 3);
    assert!(rows[0][0].starts_with("TableReader"));
    assert_eq!(rows[0][2], "root");
    assert!(rows[0][4].starts_with("data:Selection"));
    assert!(rows[1][0].contains("Selection"));
    assert_eq!(rows[1][2], "cop[tikv]");
    assert!(rows[1][4].starts_with("gt(test.t.a, 5), lt(plus(test.t.b, 1), 10)"));
    assert!(rows[2][0].contains("TableFullScan"));
    assert_eq!(rows[2][1], "10000.00");
    assert_eq!(rows[2][2], "cop[tikv]");
    // Go's captured estimates (gorun re-verified): the reader/copy selection
    // carries 2666.67; the full scan's 10000.00 is unchanged.
    assert_eq!(rows[0][1], "2666.67");
    assert_eq!(rows[2][1], "10000.00");

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
    // The allocator-suffixed IDs depend on the session's statement history,
    // so the operator names are matched by prefix, not by exact ID.
    assert!(
        rows[0][0].starts_with("TableReader"),
        "root reader: {:?}",
        rows[0][0]
    );
    assert!(
        rows[1][0].contains("Selection"),
        "the pushed predicate stays a cop Selection: {:?}",
        rows[1][0]
    );
    assert_eq!(rows[1][2], "2", "rows that passed the predicate");
    assert!(
        rows[2][0].contains("TableFullScan"),
        "the leaf is the full scan: {:?}",
        rows[2][0]
    );
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
        assert!(
            row_text(session.run(&format!(
                "SELECT * FROM t1 USE INDEX(a) WHERE {where_clause}"
            )))
            .is_empty()
        );
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

/// Go `pkg/statistics/integration_test.go::TestOutdatedStatsCheck`: stale
/// analyzed statistics become pseudo only for a session that enables
/// `tidb_enable_pseudo_for_outdated_stats`. The denominator is the histogram's
/// analyzed row count (20), not the current `stats_meta.count` (35), so 15
/// modifications cross Go's strict `> 0.7` threshold.
#[test]
fn outdated_statistics_follow_the_session_pseudo_switch() {
    struct RestoreOutdatedRatio(f64);
    impl Drop for RestoreOutdatedRatio {
        fn drop(&mut self) {
            tidb_stats::RATIO_OF_PSEUDO_ESTIMATE.store(self.0);
        }
    }

    let _restore = RestoreOutdatedRatio(tidb_stats::RATIO_OF_PSEUDO_ESTIMATE.load());
    let mut session = Session::new();
    session.run("CREATE TABLE t (a INT)").unwrap();
    session
        .run(
            "INSERT INTO t VALUES (1),(1),(1),(1),(1),(1),(1),(1),(1),(1),\
             (1),(1),(1),(1),(1),(1),(1),(1),(1),(1)",
        )
        .unwrap();
    session.run("ANALYZE TABLE t").unwrap();

    let shared = session.shared_catalog();
    {
        let mut catalog = shared.lock().unwrap();
        let table_id = match catalog.table_mut_in("test", "t").unwrap() {
            tidb_executor::TableEntry::Kv(table) => table.table_id,
            _ => panic!("t is not a KV table"),
        };
        let mut statistics = (*catalog.table_statistics(table_id).unwrap()).clone();
        statistics.row_count = 35;
        statistics.modify_count = 15;
        catalog.set_table_statistics(table_id, std::sync::Arc::new(statistics));
    }

    let scan_info = |session: &mut Session| {
        row_text(session.run("EXPLAIN SELECT * FROM t WHERE a = 1"))
            .into_iter()
            .find(|row| row[0].contains("Scan"))
            .expect("scan row")[4]
            .clone()
    };
    tidb_stats::RATIO_OF_PSEUDO_ESTIMATE.store(10.0);
    assert!(!scan_info(&mut session).contains("stats:pseudo"));
    session
        .run("SET SESSION tidb_enable_pseudo_for_outdated_stats = ON")
        .unwrap();
    assert!(!scan_info(&mut session).contains("stats:pseudo"));

    tidb_stats::RATIO_OF_PSEUDO_ESTIMATE.store(0.7);
    assert!(scan_info(&mut session).contains("stats:pseudo"));

    session
        .run("SET SESSION tidb_enable_pseudo_for_outdated_stats = OFF")
        .unwrap();
    assert!(!scan_info(&mut session).contains("stats:pseudo"));
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

#[test]
fn binary_prepared_execution_keeps_plan_detail_out_of_the_process_list() {
    let registry = process::ProcessRegistry::default();
    let mut session = Session::new();
    let guard = registry.register(
        42,
        "root".to_owned(),
        "127.0.0.1:4000".to_owned(),
        "test".to_owned(),
        None,
    );
    session.attach_process(42, guard);
    session
        .run("CREATE TABLE src (a BIGINT, INDEX ia(a))")
        .unwrap();
    session.run("INSERT INTO src VALUES (1)").unwrap();

    // Go `executeStmtImpl`: a binary-protocol EXECUTE (`execStmt.Name == ""`)
    // clears `currentPlan`, so `SHOW PROCESSLIST` carries no plan detail for
    // it, while `StmtCtx.TableIDs`/`IndexNames` are still collected.
    session.set_binary_prepared_execution(true);
    session
        .run("SELECT a FROM src USE INDEX (ia) WHERE a > 0")
        .unwrap();
    let info = tidb_util::memoryusagealarm::SessionManager::get_process_info(&registry, 42)
        .expect("registered process");
    assert_eq!(info.brief_binary_plan, "");
    assert_eq!(info.index_names, ["src:ia"]);

    // The span ends with the EXECUTE; an ordinary statement publishes again.
    session.set_binary_prepared_execution(false);
    session.run("SELECT * FROM src USE INDEX ()").unwrap();
    let info = tidb_util::memoryusagealarm::SessionManager::get_process_info(&registry, 42)
        .expect("registered process");
    assert!(!info.brief_binary_plan.is_empty());
}

#[test]
fn prepared_explain_constants_follow_current_parameters() {
    let registry = process::ProcessRegistry::default();
    let mut session = Session::new();
    let guard = registry.register(
        43,
        "root".to_owned(),
        "localhost".to_owned(),
        "test".to_owned(),
        None,
    );
    session.attach_process(43, guard);
    session
        .run("CREATE TABLE explain_param(a INT, b VARCHAR(20))")
        .unwrap();
    session
        .run("INSERT INTO explain_param VALUES (1,'one'),(2,'two')")
        .unwrap();
    for (query, kind) in [
        ("SELECT a+?,b FROM explain_param WHERE b=?", 0),
        ("SELECT ?,a FROM explain_param WHERE a>?", 1),
        ("SELECT a,b FROM explain_param WHERE b IN (?,?)", 2),
    ] {
        session.run(&format!("PREPARE s FROM '{query}'")).unwrap();
        for (number, word, value, cache) in [(1, "one", "2", "0"), (2, "two", "4", "1")] {
            session
                .run(&format!("SET @x={number},@y='{word}'"))
                .unwrap();
            let actual = row_text(session.run("EXECUTE s USING @x,@y"));
            let number_text = number.to_string();
            let expected = match kind {
                0 => vec![vec![value, word]],
                1 => vec![
                    vec![number_text.as_str(), "1"],
                    vec![number_text.as_str(), "2"],
                ],
                _ => vec![vec![number_text.as_str(), word]],
            };
            assert_eq!(actual, expected, "{query}");
            let info = tidb_util::memoryusagealarm::SessionManager::get_process_info(&registry, 43)
                .unwrap();
            let rows = tidb_util::plancodec::decode_binary_plan_for_connection(
                info.brief_binary_plan.clone(),
                "row",
                false,
            )
            .unwrap();
            if kind < 2 {
                let projection = rows
                    .iter()
                    .find(|row| row[0].contains("Projection"))
                    .unwrap();
                let expected = if kind == 0 {
                    format!("plus(test.explain_param.a, {number})")
                } else {
                    number_text
                };
                assert_eq!(
                    projection[4].split("->").next().unwrap(),
                    expected,
                    "{query}"
                );
            }
            let selection = rows
                .iter()
                .find(|row| row[0].contains("Selection"))
                .unwrap();
            let expected = match kind {
                0 => format!("eq(test.explain_param.b, \"{word}\")"),
                1 => "gt(test.explain_param.a, 0)".to_owned(),
                _ => format!(
                    "or(eq(cast(test.explain_param.b, double BINARY), {number}), eq(test.explain_param.b, \"{word}\"))"
                ),
            };
            assert_eq!(selection[4], expected, "{query}");
            assert_eq!(
                row_text(session.run("SELECT @@last_plan_from_cache")),
                vec![vec![if kind == 1 { "0" } else { cache }]],
                "{query}"
            );
        }
        session.run("DEALLOCATE PREPARE s").unwrap();
    }
}

#[test]
fn prepared_explain_index_probe_strings_match_go() {
    let registry = process::ProcessRegistry::default();
    let mut session = Session::new();
    session.attach_process(
        44,
        registry.register(
            44,
            "root".to_owned(),
            "localhost".to_owned(),
            "test".to_owned(),
            None,
        ),
    );
    session
        .run("CREATE TABLE exp_outer(a INT NOT NULL)")
        .unwrap();
    session.run("CREATE TABLE exp_inner(a INT NOT NULL,b VARCHAR(20) NOT NULL,PRIMARY KEY(a,b) CLUSTERED)").unwrap();
    session.run("INSERT INTO exp_outer VALUES (1),(2)").unwrap();
    session
        .run("INSERT INTO exp_inner VALUES (1,'one'),(2,'two')")
        .unwrap();
    for hint in ["INL_JOIN", "INL_HASH_JOIN"] {
        session.run(&format!("PREPARE s FROM 'SELECT /*+ {hint}(p) */ p.b FROM exp_outer o JOIN exp_inner p ON p.a=o.a AND p.b=?'")).unwrap();
        for (word, cache) in [("one", "0"), ("two", "1")] {
            session.run(&format!("SET @x='{word}'")).unwrap();
            assert_eq!(
                row_text(session.run("EXECUTE s USING @x")),
                vec![vec![word]]
            );
            let info = tidb_util::memoryusagealarm::SessionManager::get_process_info(&registry, 44)
                .unwrap();
            let rows = tidb_util::plancodec::decode_binary_plan_for_connection(
                info.brief_binary_plan.clone(),
                "row",
                false,
            )
            .unwrap();
            let scan = rows
                .iter()
                .find(|row| row[0].contains("TableRangeScan"))
                .unwrap();
            assert_eq!(
                scan[4],
                format!(
                    "range: decided by [eq(test.exp_inner.a, test.exp_outer.a) eq(test.exp_inner.b, {word})], keep order:false, stats:pseudo"
                ),
                "{hint}"
            );
            let selection = rows
                .iter()
                .find(|row| row[0].contains("Selection"))
                .unwrap();
            assert_eq!(
                selection[4],
                format!("eq(test.exp_inner.b, \"{word}\")"),
                "{hint}"
            );
            assert_eq!(
                row_text(session.run("SELECT @@last_plan_from_cache")),
                vec![vec![cache]],
                "{hint}"
            );
        }
        session.run("DEALLOCATE PREPARE s").unwrap();
    }
}

/// Go `TestEstimationForUnknownValues`: exercise unknown-value estimation
/// around repeated ANALYZE, stats-delta publication, TRUNCATE, and table
/// recreation through the production local session path.
#[test]
fn unknown_value_estimates_follow_analyze_and_truncate_lifecycle() {
    let mut session = Session::new();
    session
        .run("SET GLOBAL tidb_analyze_column_options = 'PREDICATE'")
        .unwrap();
    session.run("SET tidb_analyze_version = 2").unwrap();
    session
        .run("CREATE TABLE unknown_value_lifecycle (a INT, b INT, KEY idx_ab(a, b))")
        .unwrap();
    session
        .run("ANALYZE TABLE unknown_value_lifecycle")
        .unwrap();
    session
        .run(
            "INSERT INTO unknown_value_lifecycle VALUES \
             (0,0),(1,1),(2,2),(3,3),(4,4),(5,5),(6,6),(7,7),(8,8),(9,9)",
        )
        .unwrap();
    session.run("FLUSH STATS_DELTA *.*").unwrap();
    session
        .run("ANALYZE TABLE unknown_value_lifecycle")
        .unwrap();
    session
        .run(
            "INSERT INTO unknown_value_lifecycle VALUES \
             (10,10),(11,11),(12,12),(13,13),(14,14),\
             (15,15),(16,16),(17,17),(18,18),(19,19)",
        )
        .unwrap();
    session.run("FLUSH STATS_DELTA *.*").unwrap();

    let estimate_column_range = |session: &Session, table: &str, low: i64, high: i64| {
        let catalog = session.shared_catalog();
        let catalog = catalog.lock().unwrap();
        let (table_id, column_id) = match catalog.table_in("test", table).unwrap() {
            tidb_executor::TableEntry::Kv(table) => (table.table_id, table.columns[0].id),
            _ => panic!("{table} is not a KV table"),
        };
        let stats = catalog.table_statistics(table_id).unwrap();
        tidb_planner::cardinality::row_count_estimator::get_row_count_by_column_ranges(
            stats.columns.get(&column_id),
            &[
                tidb_planner::cardinality::row_count_estimator::ColumnRange::new(
                    tidb_datatype::Datum::Int(low),
                    tidb_datatype::Datum::Int(high),
                    false,
                    false,
                ),
            ],
            tidb_datatype::Collation::Binary,
            stats.row_count,
            stats.modify_count,
            false,
            tidb_planner::cardinality::row_count_estimator::EstimatorOptions::default(),
        )
        .unwrap()
        .est
    };
    assert_eq!(
        estimate_column_range(&session, "unknown_value_lifecycle", 30, 30),
        2.0
    );
    assert_eq!(
        estimate_column_range(&session, "unknown_value_lifecycle", 9, 30),
        4.0
    );
    assert_eq!(
        estimate_column_range(&session, "unknown_value_lifecycle", 9, i64::MAX),
        4.0
    );

    session
        .run("TRUNCATE TABLE unknown_value_lifecycle")
        .unwrap();
    session
        .run("INSERT INTO unknown_value_lifecycle VALUES (NULL, NULL)")
        .unwrap();
    session
        .run("ANALYZE TABLE unknown_value_lifecycle")
        .unwrap();
    assert_eq!(
        estimate_column_range(&session, "unknown_value_lifecycle", 1, 30),
        1.0
    );

    session.run("DROP TABLE unknown_value_lifecycle").unwrap();
    session
        .run("CREATE TABLE unknown_value_lifecycle (a INT, b INT, KEY idx_b(b))")
        .unwrap();
    session
        .run("INSERT INTO unknown_value_lifecycle VALUES (1, 1)")
        .unwrap();
    session
        .run("ANALYZE TABLE unknown_value_lifecycle")
        .unwrap();
    let estimate = estimate_column_range(&session, "unknown_value_lifecycle", 2, 2);
    assert!((estimate - 0.001).abs() < 1e-12, "estimate was {estimate}");
    session
        .run("SET GLOBAL tidb_analyze_column_options = 'ALL'")
        .unwrap();
}

/// Go `TestEstimationForUnknownValuesAfterModify`: after ANALYZE, committed
/// writes update only the realtime/modify counts and change an unseen value's
/// estimate without replacing the analyzed histogram.
#[test]
fn unknown_value_estimates_follow_modify_delta_lifecycle() {
    let mut session = Session::new();
    let prior_auto_analyze =
        row_text(session.run("SELECT @@global.tidb_enable_auto_analyze"))[0][0].clone();
    session
        .run("SET GLOBAL tidb_enable_auto_analyze = 'OFF'")
        .unwrap();
    session.run("SET tidb_analyze_version = 2").unwrap();
    session
        .run("CREATE TABLE unknown_modify_lifecycle (a INT, KEY idx_a(a))")
        .unwrap();
    let values = (1..=10)
        .flat_map(|value| std::iter::repeat_n(format!("({value})"), 10))
        .collect::<Vec<_>>()
        .join(",");
    session
        .run(&format!(
            "INSERT INTO unknown_modify_lifecycle VALUES {values}"
        ))
        .unwrap();
    session
        .run("ANALYZE TABLE unknown_modify_lifecycle")
        .unwrap();

    let estimate_column_point = |session: &Session, value: i64| {
        let catalog = session.shared_catalog();
        let catalog = catalog.lock().unwrap();
        let (table_id, column_id) = match catalog
            .table_in("test", "unknown_modify_lifecycle")
            .unwrap()
        {
            tidb_executor::TableEntry::Kv(table) => (table.table_id, table.columns[0].id),
            _ => panic!("unknown_modify_lifecycle is not a KV table"),
        };
        let stats = catalog.table_statistics(table_id).unwrap();
        tidb_planner::cardinality::row_count_estimator::get_row_count_by_column_ranges(
            stats.columns.get(&column_id),
            &[
                tidb_planner::cardinality::row_count_estimator::ColumnRange::point(
                    tidb_datatype::Datum::Int(value),
                ),
            ],
            tidb_datatype::Collation::Binary,
            stats.row_count,
            stats.modify_count,
            false,
            tidb_planner::cardinality::row_count_estimator::EstimatorOptions::default(),
        )
        .unwrap()
        .est
    };
    assert_eq!(estimate_column_point(&session, 5), 10.0);
    assert_eq!(estimate_column_point(&session, 11), 1.0);

    session
        .run("INSERT INTO unknown_modify_lifecycle SELECT a + 10 FROM unknown_modify_lifecycle")
        .unwrap();
    session
        .run("INSERT INTO unknown_modify_lifecycle SELECT a + 10 FROM unknown_modify_lifecycle WHERE a <= 10")
        .unwrap();
    session.run("FLUSH STATS_DELTA *.*").unwrap();
    let estimate = estimate_column_point(&session, 15);
    assert!(estimate > 1.0 && estimate < 10.0, "estimate was {estimate}");

    session
        .run(&format!(
            "SET GLOBAL tidb_enable_auto_analyze = '{}'",
            prior_auto_analyze.to_ascii_uppercase()
        ))
        .unwrap();
}

/// Go `TestGlobalStatsOutOfRangeEstimationAfterDelete`: dynamic partition
/// statistics must account for committed deletes and stay stable when one
/// partition's statistics are analyzed again.
#[test]
fn global_partition_out_of_range_estimates_survive_delete_and_partition_analyze() {
    let mut session = Session::new();
    session
        .run("SET tidb_partition_prune_mode = 'dynamic'")
        .unwrap();
    session
        .run(
            "CREATE TABLE global_out_of_range (a INT UNSIGNED) PARTITION BY RANGE (a) (\
             PARTITION p0 VALUES LESS THAN (400), PARTITION p1 VALUES LESS THAN (600),\
             PARTITION p2 VALUES LESS THAN (800), PARTITION p3 VALUES LESS THAN (1000),\
             PARTITION p4 VALUES LESS THAN (1200))",
        )
        .unwrap();
    let values = (0..3000)
        .map(|row| format!("({})", row / 5 + 300))
        .collect::<Vec<_>>()
        .join(",");
    session
        .run(&format!("INSERT INTO global_out_of_range VALUES {values}"))
        .unwrap();
    session.run("FLUSH STATS_DELTA *.*").unwrap();
    session
        .run("ANALYZE TABLE global_out_of_range ALL COLUMNS WITH 1 SAMPLERATE, 0 TOPN")
        .unwrap();
    session
        .run("DELETE FROM global_out_of_range WHERE a < 500")
        .unwrap();
    session.run("FLUSH STATS_DELTA *.*").unwrap();
    let expected_input: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../pkg/planner/cardinality/testdata/cardinality_suite_in.json"
    ))
    .unwrap();
    let expected_output: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../pkg/planner/cardinality/testdata/cardinality_suite_out.json"
    ))
    .unwrap();
    let queries = expected_input
        .as_array()
        .unwrap()
        .iter()
        .find(|test| test["name"] == "TestGlobalStatsOutOfRangeEstimationAfterDelete")
        .unwrap()["cases"]
        .as_array()
        .unwrap();
    let goldens = expected_output
        .as_array()
        .unwrap()
        .iter()
        .find(|test| test["Name"] == "TestGlobalStatsOutOfRangeEstimationAfterDelete")
        .unwrap()["Cases"]
        .as_array()
        .unwrap();
    assert_eq!(queries.len(), 13);
    assert_eq!(goldens.len(), queries.len());

    let check_plans = |session: &mut Session| {
        for (query, golden) in queries.iter().zip(goldens) {
            let sql = query
                .as_str()
                .unwrap()
                .replace("from t ", "from global_out_of_range ");
            let rows = row_text(session.run(&sql));
            let expected = golden["Result"][0].as_str().unwrap();
            let expected_rows = expected
                .split_whitespace()
                .nth(1)
                .unwrap()
                .parse::<f64>()
                .unwrap();
            let reader = rows
                .first()
                .unwrap_or_else(|| panic!("no plan for {sql}: {rows:?}"));
            let actual_rows = reader[1].parse::<f64>().unwrap();
            assert!(
                (actual_rows - expected_rows).abs() < 0.01,
                "{sql}: expected {expected_rows}, got {actual_rows}: {rows:?}"
            );
            let expected_partition = expected
                .split_once("partition:")
                .map(|(_, suffix)| suffix.split_whitespace().next().unwrap())
                .unwrap();
            assert!(
                reader
                    .join(" ")
                    .contains(&format!("partition:{expected_partition}")),
                "{sql}: expected partition {expected_partition}: {rows:?}"
            );
            let scan = rows
                .iter()
                .find(|row| row[0].contains("TableFullScan"))
                .unwrap_or_else(|| panic!("missing full scan for {sql}: {rows:?}"));
            assert_eq!(scan[1], "2000.00", "{sql}: {rows:?}");
        }
    };
    check_plans(&mut session);

    session
        .run(
            "ANALYZE TABLE global_out_of_range PARTITION p4 ALL COLUMNS \
             WITH 1 SAMPLERATE, 0 TOPN",
        )
        .unwrap();
    check_plans(&mut session);
}


/// Go cardinality.TestOptScaleNDVSkewRatioSetVar.
#[test]
fn ndv_skew_hint_changes_distinct_estimates_after_analyze() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE t(a INT, b INT, KEY(a), KEY(b))")
        .unwrap();
    let values = (0..100)
        .map(|i| format!("({}, {i})", i % 20))
        .collect::<Vec<_>>()
        .join(",");
    session
        .run(&format!("INSERT INTO t VALUES {values}"))
        .unwrap();
    session.run("ANALYZE TABLE t").unwrap();
    session.run("SET tidb_stats_load_sync_wait=100").unwrap();
    for (ratio, expected) in [("0", "19.44"), ("\"0.5\"", "14.82"), ("1", "10.20")] {
        let rows = row_text(session.run(&format!(
            "EXPLAIN SELECT /*+ SET_VAR(tidb_opt_scale_ndv_skew_ratio={ratio}) */ DISTINCT(a) FROM t WHERE b<50"
        )));
        assert_eq!(rows[0][1], expected, "ratio={ratio}: {rows:?}");
    }
}

/// Go cardinality.TestIssue54812.
#[test]
fn ndv_skew_distinct_aggregation_preserves_selection_rows() {
    let mut session = Session::new();
    session.run("SET tidb_opt_scale_ndv_skew_ratio=0").unwrap();
    session
        .run("CREATE TABLE t(a INT, b INT, KEY(a), KEY(b))")
        .unwrap();
    let values = (0..100)
        .map(|i| format!("({i}, 1)"))
        .collect::<Vec<_>>()
        .join(",");
    session
        .run(&format!("INSERT INTO t VALUES {values}"))
        .unwrap();
    let repeated = vec!["(100, 2)"; 100].join(",");
    for _ in 0..10 {
        session
            .run(&format!("INSERT INTO t VALUES {repeated}"))
            .unwrap();
    }
    session.run("ANALYZE TABLE t").unwrap();
    session.run("SET tidb_stats_load_sync_wait=100").unwrap();
    let rows = row_text(session.run("EXPLAIN FORMAT='brief' SELECT DISTINCT(a) FROM t WHERE b=1"));
    let actual = rows.iter().map(|row| row.join(" ")).collect::<Vec<_>>();
    assert_eq!(
        actual,
        vec![
            "HashAgg 65.23 root  group by:test.t.a, funcs:firstrow(test.t.a)->test.t.a",
            "└─TableReader 65.23 root  data:HashAgg",
            "  └─HashAgg 65.23 cop[tikv]  group by:test.t.a, ",
            "    └─Selection 100.00 cop[tikv]  eq(test.t.b, 1)",
            "      └─TableFullScan 1100.00 cop[tikv] table:t keep order:false",
        ]
    );
}

#[test]
fn physical_null_filters_reuse_derived_datasource_statistics() {
    let mut session = Session::new();
    session.run("CREATE TABLE t(id INT PRIMARY KEY, a INT, b INT, KEY ia(a))").unwrap();
    let values = (1..=100)
        .map(|i| format!("({i},{},{})", i % 10, if i % 2 == 0 { "NULL" } else { "1" }))
        .collect::<Vec<_>>()
        .join(",");
    session.run(&format!("INSERT INTO t VALUES {values}")).unwrap();
    session.run("ANALYZE TABLE t ALL COLUMNS").unwrap();
    session.run("SET tidb_stats_load_sync_wait = 100").unwrap();
    for (query, access, expected, actual_rows) in [
        ("SELECT * FROM t USE INDEX(ia) WHERE a<5 AND b IS NULL", "IndexLookUp", "25.00", 30),
        ("SELECT * FROM t USE INDEX(ia) WHERE a<5 AND b IS NOT NULL", "IndexLookUp", "25.00", 20),
        ("SELECT * FROM t WHERE id IN (1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20) AND b IS NULL", "Batch_Point_Get", "10.00", 10),
        ("SELECT * FROM t WHERE id IN (1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20) AND b IS NOT NULL", "Batch_Point_Get", "10.00", 10),
    ] {
        let plan = row_text(session.run(&format!("EXPLAIN FORMAT='brief' {query}")));
        assert!(plan.iter().any(|row| row[0].contains(access)), "{query}: {plan:?}");
        let selection = plan.iter().find(|row| row[0].contains("Selection")).unwrap();
        assert_eq!(selection[1], expected, "{query}: {plan:?}");
        assert_eq!(row_text(session.run(query)).len(), actual_rows, "{query}");
    }
}

#[test]
fn verbose_explain_reports_statement_costs_and_runtime_columns() {
    let mut session = Session::new();
    session.run("CREATE TABLE verbose_cost(a INT)").unwrap();
    session.run("INSERT INTO verbose_cost VALUES (1),(2),(3)").unwrap();
    let query = "SELECT * FROM verbose_cost WHERE a > 1";
    let rows = row_text(session.run(&format!("EXPLAIN FORMAT=verbose {query}")));
    assert!(rows.iter().all(|row| row.len() == 6), "{rows:?}");
    let costs = rows.iter().map(|row| row[2].parse::<f64>().unwrap()).collect::<Vec<_>>();
    assert_eq!(costs, [318680.0, 4569000.0, 4070000.0], "Go verbose costs: {rows:?}");
    session.run("SET tidb_opt_table_full_scan_cost_factor = 1000").unwrap();
    let expensive = row_text(session.run(&format!("EXPLAIN FORMAT=verbose {query}")));
    assert_eq!(
        expensive.iter().map(|row| row[2].as_str()).collect::<Vec<_>>(),
        ["271380680.00", "4070499000.00", "4070000000.00"],
        "Go verbose costs with the statement scan factor: {expensive:?}",
    );
    let analyzed = row_text(session.run(&format!("EXPLAIN ANALYZE FORMAT=verbose {query}")));
    assert!(analyzed.iter().all(|row| row.len() == 10), "{analyzed:?}");
    assert_eq!(analyzed[0][3], "2", "{analyzed:?}");
    for (plain, runtime) in expensive.iter().zip(&analyzed) {
        assert_eq!(plain[2], runtime[2], "estimated cost must not use actual rows");
    }
    let update = row_text(session.run(
        "EXPLAIN FORMAT=verbose UPDATE verbose_cost SET a=a+1 WHERE a>1",
    ));
    assert_eq!(update[0][2], "N/A", "the Go DML root is not a physical cost node");
    assert!(update.iter().skip(1).all(|row| row[2].parse::<f64>().is_ok()), "{update:?}");
}

/// Go TestOrderingIdxSelectivityRatioForJoin: the ordering penalty must
/// reach the chosen index join through the ordinary child-cost lifecycle.
#[test]
fn ordering_ratio_increases_index_join_cost() {
    let mut session = Session::new();
    session.run("CREATE TABLE t(a INT, b INT, c INT, INDEX ibc(b,c))").unwrap();
    session.run("INSERT INTO t VALUES (1,1,1),(2,2,2),(3,3,3),(4,4,4),(5,5,5),(6,6,6),(7,7,7),(8,8,8),(9,9,9),(10,10,10)").unwrap();
    session.run("INSERT INTO t SELECT a,b,c FROM t").unwrap();
    session.run("ANALYZE TABLE t").unwrap();
    for name in ["tidb_opt_merge_join_cost_factor", "tidb_opt_hash_join_cost_factor", "tidb_opt_topn_cost_factor"] {
        session.run(&format!("SET {name} = 1000")).unwrap();
    }
    let query = "EXPLAIN FORMAT=verbose SELECT t1.* FROM t t1 USE INDEX(ibc) JOIN t t2 ON t1.b=t2.b WHERE t2.c=5 ORDER BY t1.b LIMIT 2";
    let mut costs = Vec::new();
    for ratio in [-1.0, 0.0, 0.5, 1.0] {
        session.run(&format!("SET tidb_opt_ordering_index_selectivity_ratio = {ratio}")).unwrap();
        let rows = row_text(session.run(query));
        costs.push(rows[0][2].parse::<f64>().unwrap());
    }
    assert_eq!(costs[0], costs[1], "{costs:?}");
    assert!(costs[1] < costs[2] && costs[2] < costs[3], "{costs:?}");
}

/// Go TestOrderingIdxSelectivityRatioForMergeJoin, including its 320-row
/// inputs and stable forced MergeJoin shape at every ratio.
#[test]
fn ordering_ratio_increases_merge_join_cost() {
    let mut session = Session::new();
    for table in ["t1", "t2"] {
        session.run(&format!("CREATE TABLE {table}(a INT, b INT, c INT, INDEX ib(b))")).unwrap();
        session.run(&format!("INSERT INTO {table} VALUES (1,1,1),(2,2,2),(3,3,3),(4,4,4),(5,5,5),(6,6,6),(7,7,7),(8,8,8),(9,9,9),(10,10,10)")).unwrap();
        for _ in 0..5 {
            session.run(&format!("INSERT INTO {table} SELECT a,b,c FROM {table}")).unwrap();
        }
        session.run(&format!("ANALYZE TABLE {table}")).unwrap();
    }
    let query = "EXPLAIN FORMAT=verbose SELECT /*+ MERGE_JOIN(t1,t2) */ t1.* FROM t1 USE INDEX(ib) JOIN t2 USE INDEX(ib) ON t1.b=t2.b WHERE t1.c<t2.c ORDER BY t1.b LIMIT 2";
    let mut costs = Vec::new();
    for ratio in [-1.0, 0.0, 0.5, 1.0] {
        session.run(&format!("SET tidb_opt_ordering_index_selectivity_ratio = {ratio}")).unwrap();
        let rows = row_text(session.run(query));
        assert!(rows.iter().any(|row| row[0].contains("MergeJoin")), "{rows:?}");
        costs.push(rows[0][2].parse::<f64>().unwrap());
    }
    assert_eq!(costs[0], costs[1], "{costs:?}");
    assert!(costs[1] < costs[2] && costs[2] < costs[3], "{costs:?}");
}

/// Go TestOrderingIdxSelectivityRatioForApply, including its prefix-only
/// mock index histogram and required correlated Apply shape.
#[test]
fn ordering_ratio_increases_apply_cost() {
    let mut session = Session::new();
    session.run("CREATE TABLE t1(a INT, b INT, c INT, INDEX ibc(b,c))").unwrap();
    session.run("CREATE TABLE t2(a INT, b INT, c INT)").unwrap();
    for table in ["t1", "t2"] {
        install_cardinality_mock_statistics_for_table(
            &session, table, 1000, &[(1000, 1), (1000, 1), (1000, 1)],
        );
    }
    for name in ["tidb_opt_merge_join_cost_factor", "tidb_opt_hash_join_cost_factor", "tidb_opt_topn_cost_factor"] {
        session.run(&format!("SET {name} = 1000")).unwrap();
    }
    let query = "EXPLAIN FORMAT=verbose SELECT * FROM t1 WHERE EXISTS (SELECT /*+ NO_DECORRELATE() */ 1 FROM t2 WHERE t2.b=t1.b AND t2.c>1) ORDER BY t1.b LIMIT 2";
    let mut costs = Vec::new();
    for ratio in [-1.0, 0.0, 0.5, 1.0] {
        session.run(&format!("SET tidb_opt_ordering_index_selectivity_ratio = {ratio}")).unwrap();
        let rows = row_text(session.run(query));
        assert!(rows.iter().any(|row| row[0].contains("Apply")), "{rows:?}");
        costs.push(rows[0][2].parse::<f64>().unwrap());
    }
    assert_eq!(costs[0], costs[1], "{costs:?}");
    assert!(costs[1] < costs[2] && costs[2] < costs[3], "{costs:?}");
}

/// Go TestApplyCacheEnabledByOuterRowCount, including an upstream join that
/// duplicates otherwise unique outer keys before a correlated LATERAL Apply.
#[test]
fn apply_cache_runtime_uses_repeated_outer_keys() {
    let mut session = Session::new();
    session.run("CREATE TABLE tac_inner(k1 INT NOT NULL, k2 INT NOT NULL, PRIMARY KEY(k1,k2) CLUSTERED)").unwrap();
    session.run("CREATE TABLE tac_uniq(id INT PRIMARY KEY, k1 INT NOT NULL)").unwrap();
    session.run("CREATE TABLE tac_rep(id INT PRIMARY KEY, k1 INT NOT NULL)").unwrap();
    session.run("CREATE TABLE tac_fan(id INT PRIMARY KEY, k1 INT NOT NULL, KEY ik(k1))").unwrap();
    let inner = (0..50).flat_map(|key| (0..40).map(move |i| format!("({key},{})", key * 1000 + i))).collect::<Vec<_>>().join(",");
    session.run(&format!("INSERT INTO tac_inner VALUES {inner}")).unwrap();
    for (table, count) in [("tac_uniq", 50), ("tac_rep", 500), ("tac_fan", 500)] {
        let values = (0..count).map(|i| format!("({i},{})", i % 50)).collect::<Vec<_>>().join(",");
        session.run(&format!("INSERT INTO {table} VALUES {values}")).unwrap();
    }
    for table in ["tac_inner", "tac_uniq", "tac_rep", "tac_fan"] {
        session.run(&format!("ANALYZE TABLE {table} ALL COLUMNS")).unwrap();
    }
    session.run("INSERT INTO mysql.opt_rule_blacklist VALUES ('decorrelate')").unwrap();
    session.run("ADMIN RELOAD OPT_RULE_BLACKLIST").unwrap();
    let lateral = "INNER JOIN LATERAL (SELECT t2.k2 FROM tac_inner t2 WHERE t2.k1=o.k1) f";
    for (outer, cache, actual) in [
        ("tac_uniq o", "cache:OFF", "2000"),
        ("tac_rep o", "cache:ON, cacheHitRatio:90.000%", "20000"),
        ("tac_uniq o JOIN tac_fan ON tac_fan.k1=o.k1", "cache:ON, cacheHitRatio:90.000%", "20000"),
    ] {
        let rows = row_text(session.run(&format!("EXPLAIN ANALYZE SELECT o.k1,f.k2 FROM {outer} {lateral}")));
        let apply = rows.iter().find(|row| row[0].contains("Apply")).unwrap_or_else(|| panic!("correlated Apply: {rows:?}"));
        assert_eq!(apply[2], actual, "{rows:?}");
        assert!(apply[5].contains(cache), "expected {cache}: {rows:?}");
    }
    session.run("DELETE FROM mysql.opt_rule_blacklist WHERE name='decorrelate'").unwrap();
    session.run("ADMIN RELOAD OPT_RULE_BLACKLIST").unwrap();
}

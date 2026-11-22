//! Aggregate queries: the aggregate functions, `GROUP BY`, `HAVING`,
//! aggregate `ORDER BY`, and `SELECT DISTINCT`.
//!
//! Mirrors Go `pkg/executor/aggregate`'s hash-aggregate surface, including
//! the distinct path a `DISTINCT` select takes through the same operator.

use crate::StatsStmtCtx as _;
use tidb_planner::stmt_ctx::PlannerStmtCtx as _;
use super::*;

#[test]
fn aggregation_hints_are_lowered_from_the_shared_physical_plan() {
    let store = crate::test_store::store();
    use crate::explain::explain_select_stmt;
    use tidb_sessionctx::explain_format::ExplainFormat;

    let mut catalog = tidb_infoschema_perfschema::mock_catalog();
    crate::run_create_table_on("CREATE TABLE hinted_agg (g BIGINT, v BIGINT)", &mut catalog)
        .unwrap();
    let ctx = crate::StmtContext::for_query().with_txn_store(Box::new(store.clone()));
    run_insert_on(
        "INSERT INTO hinted_agg VALUES (2,20),(1,10),(2,21),(3,30)",
        &mut catalog,
        &ctx,
    )
    .unwrap();

    let stream_sql = "SELECT /*+ STREAM_AGG() */ g, COUNT(v) FROM hinted_agg GROUP BY g";
    assert_eq!(
        run_select_on(stream_sql, &catalog, &ctx).unwrap(),
        vec![
            vec![Datum::Int(1), Datum::Int(1)],
            vec![Datum::Int(2), Datum::Int(2)],
            vec![Datum::Int(3), Datum::Int(1)],
        ]
    );

    for (sql, expected_root, expected_child) in [
        (stream_sql, "StreamAgg", Some("Sort")),
        (
            "SELECT /*+ HASH_AGG() */ g, COUNT(v) FROM hinted_agg GROUP BY g",
            "HashAgg",
            None,
        ),
    ] {
        let Stmt::Query(query) = tidb_parser::parse(sql).unwrap() else {
            panic!("a query");
        };
        let QueryStmt::Select(select) = &*query else {
            panic!("a SELECT");
        };
        let (_, rows) =
            explain_select_stmt(select, &catalog, "test", &ctx, ExplainFormat::Brief).unwrap();
        let names = rows
            .iter()
            .filter_map(|row| match &row[0] {
                Datum::Bytes(bytes) => Some(String::from_utf8_lossy(bytes).into_owned()),
                _ => None,
            })
            .collect::<Vec<_>>();
        let root = names
            .iter()
            .position(|name| name.contains(expected_root))
            .unwrap_or_else(|| panic!("{names:?}"));
        if let Some(expected_child) = expected_child {
            assert!(
                names
                    .get(root + 1)
                    .is_some_and(|name| name.contains(expected_child)),
                "{names:?}"
            );
        }
    }

    // Go's aggregation hint/property search must retain a forced HashAgg
    // even when an index can supply a cheaper ordered StreamAgg.
    crate::run_create_table_on(
        "CREATE TABLE hinted_order (g BIGINT, v BIGINT, INDEX g_idx(g))",
        &mut catalog,
    )
    .unwrap();
    run_insert_on(
        "INSERT INTO hinted_order VALUES (2,20),(1,10),(2,21),(3,30)",
        &mut catalog,
        &ctx,
    )
    .unwrap();
    for (hint, aggregate, direction, expected) in [
        ("HASH_AGG", "HashAgg", "ASC", vec![1, 2, 3]),
        ("HASH_AGG", "HashAgg", "DESC", vec![3, 2, 1]),
        ("STREAM_AGG", "StreamAgg", "ASC", vec![1, 2, 3]),
        ("STREAM_AGG", "StreamAgg", "DESC", vec![3, 2, 1]),
    ] {
        let sql = format!(
            "SELECT /*+ {hint}() */ g, COUNT(*) FROM hinted_order GROUP BY g ORDER BY g {direction}"
        );
        assert_eq!(
            run_select_on(&sql, &catalog, &ctx).unwrap(),
            expected
                .into_iter()
                .map(|g| vec![Datum::Int(g), Datum::Int(if g == 2 { 2 } else { 1 })])
                .collect::<Vec<_>>()
        );
        let Stmt::Query(query) = tidb_parser::parse(&sql).unwrap() else {
            panic!("a query");
        };
        let QueryStmt::Select(select) = &*query else {
            panic!("a SELECT");
        };
        let (_, rows) =
            explain_select_stmt(select, &catalog, "test", &ctx, ExplainFormat::Brief).unwrap();
        let names = rows
            .iter()
            .filter_map(|row| match &row[0] {
                Datum::Bytes(bytes) => Some(String::from_utf8_lossy(bytes).into_owned()),
                _ => None,
            })
            .collect::<Vec<_>>();
        let agg = names
            .iter()
            .position(|name| name.contains(aggregate))
            .unwrap_or_else(|| panic!("{sql}: {names:?}"));
        if hint == "HASH_AGG" {
            assert!(
                names[..agg].iter().any(|name| name.contains("Sort")),
                "{names:?}"
            );
            assert!(
                !names.iter().any(|name| name.contains("StreamAgg")),
                "{names:?}"
            );
        }
    }
}

#[test]
fn distinct_aggregation_family_is_lowered_from_the_shared_physical_plan() {
    let store = crate::test_store::store();
    use crate::explain::explain_select_stmt;
    use tidb_sessionctx::explain_format::ExplainFormat;

    let mut catalog = tidb_infoschema_perfschema::mock_catalog();
    crate::run_create_table_on("CREATE TABLE hinted_distinct (g BIGINT)", &mut catalog).unwrap();
    let ctx = crate::StmtContext::for_query().with_txn_store(Box::new(store.clone()));
    run_insert_on(
        "INSERT INTO hinted_distinct VALUES (2),(1),(2),(3)",
        &mut catalog,
        &ctx,
    )
    .unwrap();

    let sql = "SELECT /*+ STREAM_AGG() */ DISTINCT g FROM hinted_distinct";
    assert_eq!(
        run_select_on(sql, &catalog, &ctx).unwrap(),
        vec![
            vec![Datum::Int(1)],
            vec![Datum::Int(2)],
            vec![Datum::Int(3)],
        ]
    );
    let Stmt::Query(query) = tidb_parser::parse(sql).unwrap() else {
        panic!("a query");
    };
    let QueryStmt::Select(select) = &*query else {
        panic!("a SELECT");
    };
    let (_, rows) =
        explain_select_stmt(select, &catalog, "test", &ctx, ExplainFormat::Brief).unwrap();
    let names = rows
        .iter()
        .filter_map(|row| match &row[0] {
            Datum::Bytes(bytes) => Some(String::from_utf8_lossy(bytes).into_owned()),
            _ => None,
        })
        .collect::<Vec<_>>();
    let stream = names
        .iter()
        .position(|name| name.contains("StreamAgg"))
        .unwrap_or_else(|| panic!("the shared STREAM_AGG receipt was not lowered: {names:?}"));
    assert!(
        names
            .get(stream + 1)
            .is_some_and(|name| name.contains("Sort")),
        "the enforced StreamAgg child sort must be retained: {names:?}"
    );
}

/// The shared planner's aggregate child retains Go's 99-row handle range
/// instead of scanning the unrelated `k` index.
#[test]
fn a_shared_aggregate_access_receipt_keeps_the_table_range() {
    let store = crate::test_store::store();
    use crate::explain::explain_select_stmt;
    use tidb_sessionctx::explain_format::ExplainFormat;

    let mut catalog = tidb_infoschema_perfschema::mock_catalog();
    crate::run_create_table_on(
        "CREATE TABLE sbtest1 (id BIGINT PRIMARY KEY, k BIGINT, INDEX k_1(k))",
        &mut catalog,
    )
    .unwrap();
    let ctx = crate::StmtContext::for_query().with_txn_store(Box::new(store.clone()));
    let stmt =
        tidb_parser::parse("SELECT SUM(k) FROM sbtest1 WHERE id BETWEEN 100 AND 199").unwrap();
    let Stmt::Query(query) = &stmt else {
        panic!("a query");
    };
    let QueryStmt::Select(select) = &**query else {
        panic!("a SELECT");
    };

    let (_, rows) =
        explain_select_stmt(select, &catalog, "test", &ctx, ExplainFormat::Brief).unwrap();

    assert!(rows.iter().any(|row| {
        matches!(&row[0], Datum::Bytes(name) if String::from_utf8_lossy(name).contains("TableRangeScan"))
            && matches!(&row[4], Datum::Bytes(info) if String::from_utf8_lossy(info).contains("range:[100,199]"))
    }), "{rows:#?}");
}

/// TPC-H q14, planned exactly as Go records it in
/// `tests/integrationtest/r/tpch.result` (the `explain format = 'plan_tree'`
/// block under "Q14 Promotion Effect Query").
///
/// Two contracts meet in this one statement.
///
/// `InjectProjBelowAgg` extracts every scalar aggregate argument -- here
/// `SUM(CASE WHEN p_type LIKE 'PROMO%' ...)` -- into a physical Projection
/// below the HashAgg, which is the third row of the recorded tree.
///
/// The join BUILD SIDE is the other, and it is decided by cost, not by row
/// count. `getHashJoins` (`pkg/planner/core/exhaust_physical_plans.go:176`)
/// enumerates both orientations of an inner join, and
/// `getPlanCostVer24PhysicalHashJoin` (`pkg/planner/core/plan_cost_ver2.go:776`)
/// prices each as `build-hash + build-filter + (probe-filter + probe-hash) /
/// p.Concurrency`. `hashBuildCostVer2` (same file, line 1167) charges the
/// build side `buildRows * buildRowSize * memFactor` -- and ROW SIZE is what

/// Go `AggregationPushDownSolver` substitutes every aggregate argument and
/// group item through a child Projection before `InjectProjBelowAgg` runs.
/// The injected physical Projection must therefore evaluate the derived
/// expressions directly over base-table columns instead of reading another
/// materialized Projection.
#[test]
fn aggregate_push_down_substitutes_child_projection_before_injection() {
    let store = crate::test_store::store();
    use crate::explain::explain_select_stmt;
    use tidb_sessionctx::explain_format::ExplainFormat;

    let mut catalog = tidb_infoschema_perfschema::mock_catalog();
    crate::run_create_table_on(
        "CREATE TABLE orders (o_orderkey INT PRIMARY KEY, o_orderdate DATE)",
        &mut catalog,
    )
    .unwrap();
    crate::run_create_table_on(
        "CREATE TABLE lineitem (l_orderkey INT, l_extendedprice DECIMAL(15,2), \
            l_discount DECIMAL(15,2), nation VARCHAR(25), cust_nation VARCHAR(25))",
        &mut catalog,
    )
    .unwrap();

    let sql = "SELECT o_year, \
        SUM(CASE WHEN nation = 'INDIA' THEN volume ELSE 0 END) / SUM(volume) AS share \
        FROM (SELECT EXTRACT(YEAR FROM o_orderdate) AS o_year, \
                     l_extendedprice * (1 - l_discount) AS volume, nation \
              FROM orders, lineitem WHERE o_orderkey = l_orderkey) all_nations \
        GROUP BY o_year ORDER BY o_year";
    let stmt = tidb_parser::parse(sql).unwrap();
    let Stmt::Query(query) = &stmt else {
        panic!("not a query");
    };
    let QueryStmt::Select(select) = &**query else {
        panic!("not a SELECT");
    };
    let ctx = crate::StmtContext::for_query().with_txn_store(Box::new(store.clone())).with_only_full_group_by(true);
    let (_, rows) =
        explain_select_stmt(select, &catalog, "test", &ctx, ExplainFormat::Brief).unwrap();
    let operator = |row: usize| match &rows[row][0] {
        Datum::Bytes(bytes) => String::from_utf8_lossy(bytes)
            .trim_start_matches(&[' ', '│', '├', '└', '─'][..])
            .to_owned(),
        other => panic!("operator is not text: {other:?}"),
    };
    let info = |row: usize| match &rows[row][4] {
        Datum::Bytes(bytes) => String::from_utf8_lossy(bytes).into_owned(),
        other => panic!("operator info is not text: {other:?}"),
    };
    let operators = (0..rows.len()).map(operator).collect::<Vec<_>>();
    assert_eq!(
        &operators[..6],
        &[
            "Sort",
            "Projection",
            "HashAgg",
            "Projection",
            "Projection",
            "HashJoin",
        ],
        "the visible Projection remains above HashAgg and the derived \
         Projection is eliminated below it: {rows:#?}",
    );
    assert!(info(0).starts_with("Column#"), "{rows:#?}");
    assert!(
        info(1).starts_with("Column#") && info(1).contains("div(Column#"),
        "{rows:#?}",
    );
    assert_eq!(
        info(2).matches("funcs:firstrow(").count(),
        1,
        "the grouped year needs one carrier, not an extra ORDER BY carrier: {rows:#?}",
    );
    assert!(
        info(3).contains("test.lineitem.nation")
            && info(3).contains("test.lineitem.l_extendedprice")
            && info(3).contains("extract(YEAR, test.orders.o_orderdate)")
            && info(3).matches("test.orders.o_orderdate").count() == 1,
        "AggregationPushDownSolver must substitute the child Projection before \
         InjectProjBelowAgg: {rows:#?}",
    );
    assert!(
        info(4).contains("test.lineitem.l_extendedprice")
            && info(4).contains("test.lineitem.l_discount")
            && info(4).contains("test.orders.o_orderdate")
            && info(4).contains("test.lineitem.nation"),
        "join reorder restores the base columns below InjectProjBelowAgg: {rows:#?}",
    );

    // The semantic checks still bind the outer clauses against the derived
    // table's output scope. Q7 and q9 have more grouped derived columns than
    // q8, so pairing those clauses with the flattened base-table scope used
    // for physical aggregation used to fail with an unresolved reference.
    for (sql, direct_groups) in [
        (
            "SELECT supp_nation, cust_nation, l_year, SUM(volume) AS revenue \
         FROM (SELECT nation AS supp_nation, cust_nation, \
                      EXTRACT(YEAR FROM o_orderdate) AS l_year, \
                      l_extendedprice * (1 - l_discount) AS volume \
               FROM orders, lineitem WHERE o_orderkey = l_orderkey) shipping \
         GROUP BY supp_nation, cust_nation, l_year \
         ORDER BY supp_nation, cust_nation, l_year",
            &["test.lineitem.nation", "test.lineitem.cust_nation"][..],
        ),
        (
            "SELECT nation, o_year, SUM(amount) AS sum_profit \
         FROM (SELECT nation, EXTRACT(YEAR FROM o_orderdate) AS o_year, \
                      l_extendedprice * (1 - l_discount) AS amount \
               FROM orders, lineitem WHERE o_orderkey = l_orderkey) profit \
         GROUP BY nation, o_year ORDER BY nation, o_year DESC",
            &["test.lineitem.nation"][..],
        ),
    ] {
        let stmt = tidb_parser::parse(sql).unwrap();
        let Stmt::Query(query) = &stmt else {
            panic!("not a query");
        };
        let QueryStmt::Select(select) = &**query else {
            panic!("not a SELECT");
        };
        assert!(run_select_on(sql, &catalog, &ctx)
            .unwrap_or_else(|error| panic!("derived aggregation execution failed: {error}"))
            .is_empty());
        let (_, rows) = explain_select_stmt(select, &catalog, "test", &ctx, ExplainFormat::Brief)
            .unwrap_or_else(|error| {
                panic!("derived aggregation must keep its semantic scope: {error}")
            });
        let operators = rows
            .iter()
            .map(|row| match &row[0] {
                Datum::Bytes(bytes) => String::from_utf8_lossy(bytes)
                    .trim_start_matches(&[' ', '│', '├', '└', '─'][..])
                    .to_owned(),
                other => panic!("operator is not text: {other:?}"),
            })
            .collect::<Vec<_>>();
        let info = |row: usize| match &rows[row][4] {
            Datum::Bytes(bytes) => String::from_utf8_lossy(bytes).into_owned(),
            other => panic!("operator info is not text: {other:?}"),
        };
        assert_eq!(
            &operators[..4],
            ["Sort", "Projection", "HashAgg", "Projection"],
            "derived group outputs must be restored below the unbounded sort: {rows:#?}",
        );
        assert_eq!(
            info(2).matches("funcs:firstrow(").count(),
            direct_groups.len() + 1,
            "ORDER BY must reuse each direct/computed group carrier: {rows:#?}",
        );
        for direct_group in direct_groups {
            assert!(
                info(0).contains(direct_group) && info(1).contains(direct_group),
                "a direct group must keep its physical identity above aggregation: {rows:#?}",
            );
            assert!(
                info(2).contains(&format!(")->{direct_group}")),
                "FIRST_ROW must return a direct group's physical identity: {rows:#?}",
            );
        }
        assert!(
            info(0).contains("Column#")
                && info(1).contains("Column#")
                && info(2).contains(")->Column#"),
            "a computed group must keep its generated physical identity: {rows:#?}",
        );
    }
}

/// Go builds the visible SELECT projection before an unbounded ORDER BY.
/// TPC-H q12 depends on this boundary because its CASE expressions are
/// projected below HashAgg while the visible group/count columns are restored
/// between HashAgg and Sort.
#[test]
fn grouped_order_by_projects_visible_fields_below_sort() {
    let store = crate::test_store::store();
    use crate::explain::explain_select_stmt;
    use tidb_sessionctx::explain_format::ExplainFormat;

    let mut catalog = tidb_infoschema_perfschema::mock_catalog();
    crate::run_create_table_on(
        "CREATE TABLE orders (o_orderkey INT PRIMARY KEY, o_orderpriority VARCHAR(15))",
        &mut catalog,
    )
    .unwrap();
    crate::run_create_table_on(
        "CREATE TABLE lineitem (l_orderkey INT, l_shipmode VARCHAR(10), \
            l_commitdate DATE, l_receiptdate DATE, l_shipdate DATE)",
        &mut catalog,
    )
    .unwrap();
    let sql = "SELECT l_shipmode, \
        SUM(CASE WHEN o_orderpriority = '1-URGENT' OR o_orderpriority = '2-HIGH' \
            THEN 1 ELSE 0 END) AS high_line_count, \
        SUM(CASE WHEN o_orderpriority <> '1-URGENT' AND o_orderpriority <> '2-HIGH' \
            THEN 1 ELSE 0 END) AS low_line_count \
        FROM orders, lineitem WHERE o_orderkey = l_orderkey \
        AND l_shipmode IN ('RAIL', 'FOB') \
        AND l_commitdate < l_receiptdate AND l_shipdate < l_commitdate \
        AND l_receiptdate >= '1997-01-01' \
        AND l_receiptdate < DATE_ADD('1997-01-01', INTERVAL 1 YEAR) \
        GROUP BY l_shipmode ORDER BY l_shipmode";
    let stmt = tidb_parser::parse(sql).unwrap();
    let Stmt::Query(query) = &stmt else {
        panic!("not a query");
    };
    let QueryStmt::Select(select) = &**query else {
        panic!("not a SELECT");
    };
    let ctx = crate::StmtContext::for_query().with_txn_store(Box::new(store.clone()));
    let (_, rows) =
        explain_select_stmt(select, &catalog, "test", &ctx, ExplainFormat::Brief).unwrap();
    let operators = rows
        .iter()
        .map(|row| match &row[0] {
            Datum::Bytes(bytes) => String::from_utf8_lossy(bytes)
                .trim_start_matches(&[' ', '│', '├', '└', '─'][..])
                .to_owned(),
            other => panic!("operator is not text: {other:?}"),
        })
        .collect::<Vec<_>>();

    assert_eq!(
        &operators[..5],
        ["Sort", "Projection", "HashAgg", "Projection", "Projection"],
        "Go keeps the visible SELECT projection below Sort and restores the reordered join \
         schema below InjectProjBelowAgg: {rows:#?}",
    );
    let info = |row: usize| match &rows[row][4] {
        Datum::Bytes(bytes) => String::from_utf8_lossy(bytes).into_owned(),
        other => panic!("operator info is not text: {other:?}"),
    };
    assert_eq!(
        info(4),
        "test.orders.o_orderpriority, test.lineitem.l_shipmode",
        "restoreSchemaIfChanged must retain the original column identities: {rows:#?}",
    );
    assert!(
        info(3).contains("test.orders.o_orderpriority")
            && info(3).contains("test.lineitem.l_shipmode"),
        "InjectProjBelowAgg must resolve compact offsets against the restored schema: {rows:#?}",
    );
    // Go's contract is structural, not a fixed id: `InjectProjBelowAgg` gives
    // the HashAgg group column and the `firstrow` carrier it renders the SAME
    // fresh identity, and only the allocation order decides the number.
    // `pkg/planner/core/casetest/tpch/testdata/tpch_suite_out.json` shows the
    // shape for q1: `group by:Column#100, Column#101, ...
    // funcs:firstrow(Column#100)->test.lineitem.l_returnflag`.
    let hash_agg = info(2);
    let group = hash_agg
        .strip_prefix("group by:")
        .and_then(|rest| rest.split(',').next())
        .expect("HashAgg renders its group-by column")
        .trim();
    assert!(
        hash_agg.contains(&format!(
            "funcs:firstrow({group})->test.lineitem.l_shipmode"
        )),
        "HashAgg must carry the restored group-column identity in its firstrow carrier \
         (group {group}): {rows:#?}",
    );
}

/// Go re-derives the group-key NDV from the join tree produced by join
/// reorder. Joining a one-row filtered region to its dimension first clamps
/// the dimension name NDV before the fact table raises the row count again.
#[test]
fn grouped_rows_follow_the_reordered_join_tree() {
    let store = crate::test_store::store();
    use crate::explain::explain_select_stmt;
    use tidb_sessionctx::explain_format::ExplainFormat;

    let mut catalog = tidb_infoschema_perfschema::mock_catalog();
    crate::run_create_table_on(
        "CREATE TABLE fact (f_id INT PRIMARY KEY, f_dim_id INT, f_value INT)",
        &mut catalog,
    )
    .unwrap();
    crate::run_create_table_on(
        "CREATE TABLE dim (d_id INT PRIMARY KEY, d_region_id INT, d_name VARCHAR(20))",
        &mut catalog,
    )
    .unwrap();
    crate::run_create_table_on(
        "CREATE TABLE region (r_id INT PRIMARY KEY, r_name VARCHAR(20))",
        &mut catalog,
    )
    .unwrap();
    let ctx = crate::StmtContext::for_query().with_txn_store(Box::new(store.clone()));
    run_insert_on(
        "INSERT INTO fact VALUES (1,1,10),(2,2,20)",
        &mut catalog,
        &ctx,
    )
    .unwrap();
    run_insert_on(
        "INSERT INTO dim VALUES (1,1,'a'),(2,2,'b')",
        &mut catalog,
        &ctx,
    )
    .unwrap();
    run_insert_on(
        "INSERT INTO region VALUES (1,'MIDDLE'),(2,'OTHER')",
        &mut catalog,
        &ctx,
    )
    .unwrap();
    scale_analyzed_tpcc_table(
        &mut catalog,
        "fact",
        1_000,
        &[("f_id", 1_000), ("f_dim_id", 25), ("f_value", 1_000)],
        &ctx,
    );
    scale_analyzed_tpcc_table(
        &mut catalog,
        "dim",
        25,
        &[("d_id", 25), ("d_region_id", 5), ("d_name", 25)],
        &ctx,
    );
    scale_analyzed_tpcc_table(
        &mut catalog,
        "region",
        5,
        &[("r_id", 5), ("r_name", 5)],
        &ctx,
    );
    let ctx = ctx.with_fresh_staged_writes();

    let sql = "SELECT d_name, SUM(f_value) AS revenue FROM fact, dim, region \
        WHERE f_dim_id = d_id AND d_region_id = r_id AND r_name = 'MIDDLE' \
        GROUP BY d_name ORDER BY revenue DESC";
    let stmt = tidb_parser::parse(sql).unwrap();
    let Stmt::Query(query) = &stmt else {
        panic!("not a query");
    };
    let QueryStmt::Select(select) = &**query else {
        panic!("not a SELECT");
    };
    let (_, rows) =
        explain_select_stmt(select, &catalog, "test", &ctx, ExplainFormat::Brief).unwrap();
    let hash_agg = rows
        .iter()
        .find(|row| match &row[0] {
            Datum::Bytes(bytes) => {
                String::from_utf8_lossy(bytes).trim_start_matches(&[' ', '│', '├', '└', '─'][..])
                    == "HashAgg"
            }
            _ => false,
        })
        .expect("grouped query has a HashAgg");
    assert_eq!(
        hash_agg[1],
        Datum::Bytes(b"5.00".to_vec()),
        "the reordered region-dimension join must clamp d_name NDV before the fact join: {rows:#?}",
    );
    // The absolute `Column#N` follows the planner's allocation counter, which
    // does not yet reproduce Go's history (the recorded Go plan names this
    // column `Column#1`); pin the RELATIONSHIP: the Sort orders by the
    // aggregate's SUM output column.
    let aggregate = rows
        .iter()
        .find(|row| {
            matches!(&row[0], Datum::Bytes(bytes) if
                String::from_utf8_lossy(bytes)
                    .trim_start_matches(&[' ', '│', '├', '└', '─'][..]) == "HashAgg")
        })
        .expect("grouped query has a HashAgg");
    let aggregate = match &aggregate[4] {
        Datum::Bytes(bytes) => String::from_utf8_lossy(bytes).into_owned(),
        other => format!("{other:?}"),
    };
    let sum_out = aggregate
        .split("funcs:sum(")
        .nth(1)
        .and_then(|rest| rest.split(")->").nth(1))
        .and_then(|rest| rest.split(',').next())
        .expect("grouped aggregate SUM output");
    assert_eq!(
        rows[0][4],
        Datum::Bytes(format!("{sum_out}:desc").into_bytes()),
        "the Sort above the visible aggregate projection must read its generated column: {rows:#?}",
    );
    assert!(
        rows.iter().any(|row| match &row[4] {
            Datum::Bytes(bytes) =>
                String::from_utf8_lossy(bytes).contains("eq(test.dim.d_id, test.fact.f_dim_id)"),
            _ => false,
        }),
        "HashJoin EXPLAIN must align equality arguments with the logical children: {rows:#?}",
    );
}

#[test]
fn derived_aggregate_null_filter_refreshes_source_statistics() {
    let store = crate::test_store::store();
    use crate::explain::explain_select_stmt;
    use tidb_sessionctx::explain_format::ExplainFormat;

    let mut catalog = tidb_infoschema_perfschema::mock_catalog();
    crate::run_create_table_on(
        "CREATE TABLE district (d_id INT NOT NULL, d_w_id INT NOT NULL, \
         d_ytd DECIMAL(12,2), PRIMARY KEY(d_w_id,d_id) CLUSTERED)",
        &mut catalog,
    )
    .unwrap();
    let ctx = crate::StmtContext::for_query().with_txn_store(Box::new(store.clone()));
    run_insert_on(
        "INSERT INTO district VALUES (1,1,10),(2,1,NULL),(1,2,20)",
        &mut catalog,
        &ctx,
    )
    .unwrap();
    let sql = "SELECT d_id FROM (SELECT d_id,d_w_id,SUM(d_ytd) s \
               FROM district GROUP BY d_id,d_w_id) d WHERE d_w_id=1 AND s IS NOT NULL";
    assert_eq!(
        run_select_on(sql, &catalog, &ctx).unwrap(),
        vec![vec![Datum::Int(1)]]
    );
    let Stmt::Query(query) = tidb_parser::parse(sql).unwrap() else {
        panic!("query")
    };
    let QueryStmt::Select(select) = &*query else {
        panic!("select")
    };
    let (_, plan) =
        explain_select_stmt(select, &catalog, "test", &ctx, ExplainFormat::Brief).unwrap();
    let filter = plan
        .iter()
        .find(|row| {
            matches!(&row[4], Datum::Bytes(bytes) if String::from_utf8_lossy(bytes).contains("not(isnull(cast("))
        })
        .expect("optimizer-added cast NULL filter");
    assert_eq!(filter[1], Datum::Bytes(b"8.00".to_vec()), "{plan:#?}");
}

/// A NARROW clustered table still prefers its covering index range. Go's
/// `PhysicalIndexScan.InitSchema` (`physical_index_scan.go:363`) builds the
/// physical index schema as the index columns plus `CommonHandleCols`, and
/// only appends a separate handle column when that schema does not already
/// carry one. This port keeps `handle_cols` equal to `common_handle_cols` for
/// a common-handle table, so appending both priced three duplicate INT slots:
/// the covering index range then lost to the clustered table range on a table
/// without a wide payload column (the sibling test's 1000-byte payload masked
/// the same mistake).
#[test]
fn a_narrow_covering_index_range_prices_the_common_handle_once() {
    let store = crate::test_store::store();
    use crate::explain::explain_select_stmt;
    use tidb_sessionctx::explain_format::ExplainFormat;

    let mut catalog = tidb_infoschema_perfschema::mock_catalog();
    crate::run_create_table_on(
        "CREATE TABLE orders (o_id INT NOT NULL, o_d_id INT NOT NULL, o_w_id INT NOT NULL,             o_c_id INT, o_entry_d DATETIME, o_carrier_id INT, o_ol_cnt INT, o_all_local INT,             PRIMARY KEY (o_w_id,o_d_id,o_id) CLUSTERED,             KEY idx_order (o_w_id,o_d_id,o_c_id,o_id))",
        &mut catalog,
    )
    .unwrap();
    let ctx = crate::StmtContext::for_query().with_txn_store(Box::new(store.clone()));
    let sql = "SELECT o_w_id, o_d_id, count(*) FROM orders WHERE o_w_id = 1                GROUP BY o_w_id, o_d_id";
    let stmt = tidb_parser::parse(sql).unwrap();
    let Stmt::Query(query) = &stmt else {
        panic!("not a query");
    };
    let QueryStmt::Select(select) = &**query else {
        panic!("not a SELECT");
    };
    let (_, rows) =
        explain_select_stmt(select, &catalog, "test", &ctx, ExplainFormat::Brief).unwrap();
    let cell = |row: usize, column: usize| match &rows[row][column] {
        Datum::Bytes(bytes) => String::from_utf8_lossy(bytes).into_owned(),
        other => format!("{other:?}"),
    };
    let plan = (0..rows.len())
        .map(|row| (cell(row, 0), cell(row, 3), cell(row, 4)))
        .collect::<Vec<_>>();
    assert!(
        plan.iter().any(|(operator, access, info)| {
            operator.contains("IndexRangeScan")
                && access.contains("idx_order")
                && info.contains("range:[1,1]")
                && info.contains("keep order:true")
        }),
        "{plan:#?}"
    );
    assert!(
        plan.iter()
            .all(|(operator, _, _)| !operator.contains("TableRangeScan")),
        "{plan:#?}"
    );
}

/// Go keeps Sysbench's random-range COUNT on the covering secondary index and
/// places a partial StreamAgg over that scan below the IndexReader.
#[test]
fn global_count_over_index_ranges_uses_gos_stream_agg_and_index_reader() {
    let store = crate::test_store::store();
    use crate::explain::explain_select_stmt;
    use tidb_sessionctx::explain_format::ExplainFormat;

    let mut catalog = tidb_infoschema_perfschema::mock_catalog();
    crate::run_create_table_on(
        "CREATE TABLE count_ranges (id INT PRIMARY KEY, k INT NOT NULL, KEY k_1(k))",
        &mut catalog,
    )
    .unwrap();
    let ctx = crate::StmtContext::for_query().with_txn_store(Box::new(store.clone()));
    run_insert_on(
        "INSERT INTO count_ranges VALUES (1, 1), (2, 15), (3, 20)",
        &mut catalog,
        &ctx,
    )
    .unwrap();

    let sql = concat!(
        "SELECT COUNT(k) FROM count_ranges WHERE ",
        "(k BETWEEN 1 AND 6) OR (k BETWEEN 2 AND 7) OR ",
        "(k BETWEEN 3 AND 8) OR (k BETWEEN 4 AND 9) OR ",
        "(k BETWEEN 5 AND 10) OR (k BETWEEN 6 AND 11) OR ",
        "(k BETWEEN 7 AND 12) OR (k BETWEEN 8 AND 13) OR ",
        "(k BETWEEN 9 AND 14) OR (k BETWEEN 10 AND 15)"
    );
    let stmt = tidb_parser::parse(sql).unwrap();
    let Stmt::Query(query) = &stmt else {
        panic!("not a query");
    };
    let QueryStmt::Select(select) = &**query else {
        panic!("not a SELECT");
    };
    let (_, rows) =
        explain_select_stmt(select, &catalog, "test", &ctx, ExplainFormat::Brief).unwrap();
    let cell = |row: usize, column: usize| match &rows[row][column] {
        Datum::Bytes(bytes) => String::from_utf8_lossy(bytes).into_owned(),
        other => format!("{other:?}"),
    };
    assert_eq!(
        (0..rows.len()).map(|row| cell(row, 0)).collect::<Vec<_>>(),
        vec![
            "StreamAgg",
            "└─IndexReader",
            "  └─StreamAgg",
            "    └─IndexRangeScan"
        ]
    );
    assert_eq!(
        (0..rows.len()).map(|row| cell(row, 2)).collect::<Vec<_>>(),
        vec!["root", "root", "cop[tikv]", "cop[tikv]"]
    );
    assert!(cell(0, 4).starts_with("funcs:count(Column#"));
    assert_eq!(cell(1, 4), "index:StreamAgg");
    assert!(cell(2, 4).starts_with("funcs:count(test.count_ranges.k)->Column#"));
    assert!(cell(3, 3).contains("index:k_1(k)"));
    assert_eq!(
        run_select_on(sql, &catalog, &ctx).unwrap(),
        vec![vec![Datum::Int(2)]]
    );

    // A full covering scan has no residual predicate. COUNT(*) reaches the
    // aggregate descriptor as COUNT(1), whose cop input is a constant rather
    // than a scan-column offset, and Go's unordered global path uses HashAgg.
    let full_count_sql = "SELECT COUNT(*) FROM count_ranges";
    let full_count_stmt = tidb_parser::parse(full_count_sql).unwrap();
    let Stmt::Query(full_count_query) = &full_count_stmt else {
        panic!("not a query");
    };
    let QueryStmt::Select(full_count_select) = &**full_count_query else {
        panic!("not a SELECT");
    };
    let (_, full_count_rows) = explain_select_stmt(
        full_count_select,
        &catalog,
        "test",
        &ctx,
        ExplainFormat::Brief,
    )
    .unwrap();
    let full_count_cell = |row: usize, column: usize| match &full_count_rows[row][column] {
        Datum::Bytes(bytes) => String::from_utf8_lossy(bytes).into_owned(),
        other => format!("{other:?}"),
    };
    assert_eq!(
        (0..full_count_rows.len())
            .map(|row| full_count_cell(row, 0))
            .collect::<Vec<_>>(),
        vec![
            "HashAgg",
            "└─IndexReader",
            "  └─HashAgg",
            "    └─IndexFullScan"
        ]
    );
    assert_eq!(
        run_select_on(full_count_sql, &catalog, &ctx).unwrap(),
        vec![vec![Datum::Int(3)]]
    );

    // Decimal SUM is decomposed through the global cop HashAgg as well. This
    // matters for a covering index: without the index-source Global contract
    // the executor would fetch every index row and cast it at the root.
    let full_sum_sql = "SELECT SUM(k) FROM count_ranges";
    let full_sum_stmt = tidb_parser::parse(full_sum_sql).unwrap();
    let Stmt::Query(full_sum_query) = &full_sum_stmt else {
        panic!("not a query");
    };
    let QueryStmt::Select(full_sum_select) = &**full_sum_query else {
        panic!("not a SELECT");
    };
    let (_, full_sum_rows) = explain_select_stmt(
        full_sum_select,
        &catalog,
        "test",
        &ctx,
        ExplainFormat::Brief,
    )
    .unwrap();
    let full_sum_cell = |row: usize, column: usize| match &full_sum_rows[row][column] {
        Datum::Bytes(bytes) => String::from_utf8_lossy(bytes).into_owned(),
        other => format!("{other:?}"),
    };
    assert_eq!(
        (0..full_sum_rows.len())
            .map(|row| full_sum_cell(row, 0))
            .collect::<Vec<_>>(),
        vec![
            "HashAgg",
            "└─IndexReader",
            "  └─HashAgg",
            "    └─IndexFullScan"
        ]
    );
    assert_eq!(
        run_select_on(full_sum_sql, &catalog, &ctx).unwrap(),
        vec![vec![Datum::Decimal(tidb_datatype::Decimal::from_int(36))]]
    );

    // The loaded Sysbench fixture has most rows in TopN, with the queried
    // range below the histogram. Go builds both a column statistics node and
    // an index statistics node for the DNF, then its greedy cover prefers the
    // index node's 5.75 estimate. Falling back to the column node raises the
    // data-source estimate and `adjustCountAfterAccess` prints 8.96 instead.
    let (table_id, k_column_id, index_id) = {
        let TableEntry::Kv(table) = catalog.get_in("test", "count_ranges").unwrap() else {
            panic!("count_ranges is not a KV table");
        };
        let k_column_id = table
            .visible_columns()
            .into_iter()
            .map(|(_, column)| column)
            .find(|column| column.read().name.original() == "k")
            .map(|column| column.read().id)
            .expect("k column");
        let index_id = table
            .indexes()
            .iter()
            .find(|index| index.read().name.original() == "k_1")
            .map(|index| index.read().id)
            .expect("k_1 index");
        (table.table_id(), k_column_id, index_id)
    };
    let analyzed_stats = |column_id: i64, index_id: i64| {
        let mut topn = tidb_stats::cmsketch::TopN::new(100);
        for position in 0..100 {
            let value = 100 + position as i64;
            let encoded = tidb_codec::encode_key(&[Datum::Int(value)]).unwrap();
            topn.append(&encoded, if position < 91 { 77 } else { 76 });
        }
        topn.sort();
        let bounds = |value| tidb_codec::encode_key(&[Datum::Int(value)]).unwrap();
        (
            tidb_stats::column::Column {
                histogram: tidb_stats::Histogram {
                    id: column_id,
                    ndv: 1736,
                    last_update_version: 42,
                    buckets: vec![tidb_stats::Bucket {
                        count: 2309,
                        repeat: 1,
                        ndv: 0,
                        lower_bound: Datum::Int(2310),
                        upper_bound: Datum::Int(7574),
                    }],
                    ..tidb_stats::Histogram::default()
                },
                top_n: Some(topn.clone()),
                cmsketch: None,
                stats_version: 2,
                ..Default::default()
            },
            tidb_stats::index::Index {
                histogram: tidb_stats::Histogram {
                    id: index_id,
                    ndv: 1736,
                    last_update_version: 42,
                    buckets: vec![tidb_stats::Bucket {
                        count: 2309,
                        repeat: 1,
                        ndv: 0,
                        lower_bound: Datum::Bytes(bounds(2310)),
                        upper_bound: Datum::Bytes(bounds(7574)),
                    }],
                    ..tidb_stats::Histogram::default()
                },
                top_n: Some(topn),
                cmsketch: None,
                stats_version: 2,
                info: Some(tidb_model::GoShared::new(tidb_model::IndexInfo {
                    columns: (vec![String::new(); 1])
                        .into_iter()
                        .map(|name: String| tidb_model::IndexColumn {
                            name: tidb_ast::CiString::new(name),
                            ..Default::default()
                        })
                        .collect::<Vec<_>>()
                        .into(),
                    ..tidb_model::IndexInfo::default()
                })),
                ..Default::default()
            },
        )
    };
    let (column_stats, index_stats) = analyzed_stats(k_column_id, index_id);
    let statistics = tidb_stats::Table::from_analyzed_items(
        table_id,
        10_000,
        0,
        [(k_column_id, column_stats)],
        [(index_id, index_stats)],
    );
    let ctx = ctx.with_fresh_staged_writes();
    ctx.stats_handle().set_table_statistics(table_id, std::sync::Arc::new(statistics));
    let (_, analyzed) =
        explain_select_stmt(select, &catalog, "test", &ctx, ExplainFormat::Brief).unwrap();
    let analyzed_cell = |row: usize, column: usize| match &analyzed[row][column] {
        Datum::Bytes(bytes) => String::from_utf8_lossy(bytes).into_owned(),
        other => format!("{other:?}"),
    };
    assert_eq!(analyzed_cell(3, 1), "5.75", "{analyzed:#?}");
}

#[test]
fn explain_uses_the_common_physical_plan_without_legacy_ast_execution() {
    let store = crate::test_store::store();
    use crate::explain::explain_select_stmt;
    use tidb_sessionctx::explain_format::ExplainFormat;

    let mut catalog = tidb_infoschema_perfschema::mock_catalog();
    let ctx = crate::StmtContext::for_query().with_txn_store(Box::new(store.clone()));
    crate::run_create_table_on(
        "CREATE TABLE explain_common_plan (id INT PRIMARY KEY, k INT)",
        &mut catalog,
    )
    .unwrap();
    let statement =
        tidb_parser::parse("SELECT k, SUM(id) FROM explain_common_plan WHERE id > 0 GROUP BY k")
            .unwrap();
    let Stmt::Query(query) = statement else {
        panic!("not a query");
    };
    let QueryStmt::Select(select) = &*query else {
        panic!("not a SELECT");
    };

    let (_, rows) = explain_select_stmt(
        select,
        &catalog,
        DEFAULT_DATABASE,
        &ctx,
        ExplainFormat::Brief,
    )
    .unwrap();
    assert!(!rows.is_empty());
}

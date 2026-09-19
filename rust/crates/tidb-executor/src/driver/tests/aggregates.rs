//! Aggregate queries: the aggregate functions, `GROUP BY`, `HAVING`,
//! aggregate `ORDER BY`, and `SELECT DISTINCT`.
//!
//! Mirrors Go `pkg/executor/aggregate`'s hash-aggregate surface, including
//! the distinct path a `DISTINCT` select takes through the same operator.

use super::*;

#[test]
fn aggregation_hints_are_lowered_from_the_shared_physical_plan() {
    use crate::explain::{explain_select_stmt, ExplainFormat};

    let mut catalog = Catalog::default();
    crate::run_create_table_on("CREATE TABLE hinted_agg (g BIGINT, v BIGINT)", &mut catalog)
        .unwrap();
    let ctx = crate::StmtContext::for_query();
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
fn a_computed_projection_column_explains_as_column_not_its_alias() {
    use crate::explain::{explain_select_stmt, ExplainFormat};

    let mut catalog = Catalog::default();
    crate::run_create_table_on("CREATE TABLE alias_agg (g BIGINT, v BIGINT)", &mut catalog)
        .unwrap();
    let ctx = crate::StmtContext::for_query();
    run_insert_on("INSERT INTO alias_agg VALUES (1,10)", &mut catalog, &ctx).unwrap();

    // Go `buildProjectionField`: a computed field's fresh `Column` has NO
    // `OrigName`, so its alias never reaches the operator text.
    let sql = "SELECT v + 0 AS revenue FROM alias_agg";
    let stmt = tidb_parser::parse(sql).unwrap();
    let Stmt::Query(query) = &stmt else {
        panic!("not a query");
    };
    let QueryStmt::Select(select) = &**query else {
        panic!("not a SELECT");
    };
    let (_, rows) =
        explain_select_stmt(select, &catalog, "test", &ctx, ExplainFormat::Brief).unwrap();
    let text = rows
        .iter()
        .map(|row| {
            row.iter()
                .map(|datum| match datum {
                    Datum::Bytes(bytes) => String::from_utf8_lossy(bytes).into_owned(),
                    other => format!("{other:?}"),
                })
                .collect::<Vec<_>>()
                .join("\t")
        })
        .collect::<Vec<_>>();
    assert!(
        text.iter()
            .any(|line| line.contains("Projection") && line.contains("->Column#")),
        "the projection explains its output column: {text:#?}"
    );
    assert!(
        !text.iter().any(|line| line.contains("->revenue")),
        "the alias must not appear in the operator text: {text:#?}"
    );
}

#[test]
fn distinct_aggregation_family_is_lowered_from_the_shared_physical_plan() {
    use crate::explain::{explain_select_stmt, ExplainFormat};

    let mut catalog = Catalog::default();
    crate::run_create_table_on("CREATE TABLE hinted_distinct (g BIGINT)", &mut catalog).unwrap();
    let ctx = crate::StmtContext::for_query();
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
    use crate::explain::{explain_select_stmt, ExplainFormat};

    let mut catalog = Catalog::default();
    crate::run_create_table_on(
        "CREATE TABLE sbtest1 (id BIGINT PRIMARY KEY, k BIGINT, INDEX k_1(k))",
        &mut catalog,
    )
    .unwrap();
    let ctx = crate::StmtContext::for_query();
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

/// Go `expression.NewFunction` folds each builtin as it is constructed in the
/// live statement context (`foldConstant`), so a wholly-constant `DATE_ADD`
/// is a literal before predicate push-down sees it. The planner's single
/// deferred top-level fold could not reach it: the predicate's `AND` parent is
/// a lazy short-circuit and its `LT` parent has a column argument, so neither
/// descends. The recorded q14 plan carries
/// `lt(l_shipdate, 1997-01-01 00:00:00.000000)` for exactly this reason.
#[test]
fn a_constant_date_add_in_a_predicate_folds_before_push_down() {
    use crate::explain::{explain_select_stmt, ExplainFormat};

    let mut catalog = Catalog::default();
    crate::run_create_table_on("CREATE TABLE t (d DATE NOT NULL)", &mut catalog).unwrap();
    let ctx = crate::StmtContext::for_query();
    // The AND is load-bearing: it is a lazy short-circuit parent, which is
    // what the deferred top-level fold refuses to descend through.
    let sql = "SELECT * FROM t WHERE d >= '1996-01-01' \
        AND d < DATE_ADD('1996-12-01', INTERVAL 1 MONTH)";
    let stmt = tidb_parser::parse(sql).unwrap();
    let Stmt::Query(query) = &stmt else {
        panic!("not a query");
    };
    let QueryStmt::Select(select) = &**query else {
        panic!("not a SELECT");
    };
    let (_, rows) =
        explain_select_stmt(select, &catalog, "test", &ctx, ExplainFormat::Brief).unwrap();
    let info: Vec<String> = rows
        .iter()
        .map(|row| match &row[4] {
            Datum::Bytes(bytes) => String::from_utf8_lossy(bytes).into_owned(),
            other => format!("{other:?}"),
        })
        .collect();
    assert!(
        info.iter()
            .any(|line| line.contains("lt(test.t.d, 1997-01-01 00:00:00.000000)")),
        "the constant DATE_ADD must be a literal: {info:?}",
    );
    assert!(
        !info.iter().any(|line| line.contains("date_add")),
        "no DATE_ADD may survive planning: {info:?}",
    );
}

/// Go `AggregationPushDownSolver` substitutes every aggregate argument and
/// group item through a child Projection before `InjectProjBelowAgg` runs.
/// The injected physical Projection must therefore evaluate the derived
/// expressions directly over base-table columns instead of reading another
/// materialized Projection.
#[test]
fn aggregate_push_down_substitutes_child_projection_before_injection() {
    use crate::explain::{explain_select_stmt, ExplainFormat};

    let mut catalog = Catalog::default();
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
    let ctx = crate::StmtContext::for_query().with_only_full_group_by(true);
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
    use crate::explain::{explain_select_stmt, ExplainFormat};

    let mut catalog = Catalog::default();
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
    let ctx = crate::StmtContext::for_query();
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
    use crate::explain::{explain_select_stmt, ExplainFormat};

    let mut catalog = Catalog::default();
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
    let ctx = crate::StmtContext::for_query();
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

/// A selected group key is a root FIRST_ROW carrier, not an extra TiKV
/// function. The partial schema is `[count, group]`; the final projection
/// restores the written `[group, count]` order.
#[test]
fn grouped_partial_count_carries_the_group_key() {
    use crate::explain::{explain_select_stmt, ExplainFormat};

    let mut catalog = Catalog::default();
    crate::run_create_table_on(
        "CREATE TABLE order_line (ol_o_id INT NOT NULL, ol_d_id INT NOT NULL, \
            ol_w_id INT NOT NULL, ol_number INT NOT NULL, \
            PRIMARY KEY (ol_w_id,ol_d_id,ol_o_id,ol_number))",
        &mut catalog,
    )
    .unwrap();
    let TableEntry::Kv(table) = catalog.get_mut_in("test", "order_line").unwrap() else {
        panic!("order_line is not a KV table");
    };
    let table = std::sync::Arc::make_mut(table);
    table.add_index(
        crate::kv_table::KvIndex {
            id: 1,
            name: "PRIMARY".to_owned(),
            comment: String::new(),
            unique: true,
            prefix_lengths: vec![crate::ddl::index_prefix::UNSPECIFIED_LENGTH; 4],
            column_offsets: vec![2, 1, 0, 3],
            visible: true,
            global: false,
            global_index_version: 0,
            clustered_primary: false,
        },
        false,
    );
    let ctx = crate::StmtContext::for_query();
    run_insert_on(
        "INSERT INTO order_line VALUES (1,1,1,1),(1,1,1,2),(2,2,1,1),(1,1,2,1)",
        &mut catalog,
        &ctx,
    )
    .unwrap();

    let sql = "SELECT ol_d_id, COUNT(*) cn FROM order_line \
        WHERE ol_w_id=1 GROUP BY ol_d_id";
    assert_eq!(
        run_select_on(sql, &catalog, &ctx).unwrap(),
        vec![
            vec![Datum::Int(1), Datum::Int(2)],
            vec![Datum::Int(2), Datum::Int(1)],
        ],
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
            "Projection",
            "└─StreamAgg",
            "  └─TableReader",
            "    └─StreamAgg",
            "      └─TableRangeScan",
        ],
    );
    assert!(cell(0, 4).starts_with("test.order_line.ol_d_id, Column#"));
    assert!(cell(1, 4).contains("funcs:count(Column#"));
    assert!(cell(1, 4).contains("funcs:firstrow(test.order_line.ol_d_id)"));
    assert!(!cell(3, 4).contains("firstrow"));
}

/// Go `buildSelect` (`logical_plan_builder.go:4583`): a derived table's
/// `ORDER BY` is built only for the top-level query, when the query has a
/// `LIMIT`, or when `@@tidb_remove_orderby_in_subquery` is off. Dropping it is
/// what lets the aggregate above the join pick a HashAgg instead of exploiting
/// a meaningless input order (TPCC condition 06).
#[test]
fn derived_table_order_by_is_removed_unless_top_level_limit_or_disabled() {
    use crate::explain::{explain_select_stmt, ExplainFormat};

    let mut catalog = Catalog::default();
    crate::run_create_table_on(
        "CREATE TABLE s (a INT NOT NULL, b INT NOT NULL, PRIMARY KEY (a))",
        &mut catalog,
    )
    .unwrap();
    crate::run_create_table_on(
        "CREATE TABLE t (a INT NOT NULL, b INT NOT NULL, PRIMARY KEY (a))",
        &mut catalog,
    )
    .unwrap();
    let ctx = crate::StmtContext::for_query();

    let operators = |ctx: &crate::StmtContext, sql: &str| -> Vec<String> {
        let stmt = tidb_parser::parse(sql).unwrap();
        let Stmt::Query(query) = &stmt else {
            panic!("not a query");
        };
        let QueryStmt::Select(select) = &**query else {
            panic!("not a SELECT");
        };
        let (_, rows) =
            explain_select_stmt(select, &catalog, "test", ctx, ExplainFormat::Brief).unwrap();
        rows.iter()
            .map(|row| match &row[0] {
                Datum::Bytes(bytes) => String::from_utf8_lossy(bytes).into_owned(),
                other => format!("{other:?}"),
            })
            .collect()
    };

    let derived = "SELECT COUNT(*) FROM \
        (SELECT s.a, SUM(s.b) sm FROM s GROUP BY s.a ORDER BY s.a + 0) d, t \
        WHERE t.a = d.a";
    assert!(
        !operators(&ctx, derived)
            .iter()
            .any(|op| op.contains("Sort")),
        "a derived table's ORDER BY is dropped by default: {:?}",
        operators(&ctx, derived)
    );

    let with_limit = "SELECT COUNT(*) FROM \
        (SELECT s.a, SUM(s.b) sm FROM s GROUP BY s.a ORDER BY s.a + 0 LIMIT 3) d, t \
        WHERE t.a = d.a";
    assert!(
        operators(&ctx, with_limit)
            .iter()
            .any(|op| op.contains("Sort") || op.contains("TopN")),
        "a LIMIT keeps the derived ORDER BY: {:?}",
        operators(&ctx, with_limit)
    );

    let keep = ctx.clone().with_remove_orderby_in_subquery(false);
    assert!(
        operators(&keep, derived)
            .iter()
            .any(|op| op.contains("Sort")),
        "tidb_remove_orderby_in_subquery=OFF keeps the derived ORDER BY: {:?}",
        operators(&keep, derived)
    );

    let top_level = "SELECT s.a, SUM(s.b) sm FROM s GROUP BY s.a ORDER BY SUM(s.b)";
    assert!(
        operators(&ctx, top_level)
            .iter()
            .any(|op| op.contains("Sort")),
        "a top-level ORDER BY is always built: {:?}",
        operators(&ctx, top_level)
    );
}

/// Go's MaxMinEliminate endgame may only answer `max(col)` from a bounded
/// reverse walk when the ENTRIES rank by `col`. Over a clustered table whose
/// secondary index ranks by other columns, the argument arrives through the
/// common handle appended to the executor schema
/// (`PhysicalIndexScan.ToPB` appends `ds.CommonHandleCols`) -- so the cop
/// TopN lowers and the root TopN merges each region's local extreme. A
/// bounded reverse read offered without Go's `checkColCanUseIndex` proof
/// answered an arbitrary row as the extreme.
#[test]
fn max_min_over_a_clustered_column_the_index_does_not_rank_answers_the_true_extreme() {
    let mut catalog = Catalog::default();
    crate::run_create_table_on(
        "CREATE TABLE ox (a VARCHAR(3) NOT NULL, b VARCHAR(8) NOT NULL, c INT NOT NULL, \
         d VARCHAR(2), PRIMARY KEY(a,b,c) CLUSTERED, KEY k4(a,b,d))",
        &mut catalog,
    )
    .unwrap();
    let ctx = crate::StmtContext::for_query();
    run_insert_on(
        "INSERT INTO ox VALUES ('a01','20220101',3,'x'),\
         ('a02','20220102',42,'y'),\
         ('a03','20220103',7,'z')",
        &mut catalog,
        &ctx,
    )
    .unwrap();
    // The index ranks (a, b, d): the entry its reverse walk reaches FIRST is
    // a03, whose row carries c = 7 -- not the true maximum 42. An unproven
    // bounded read therefore answers 7; only the gated lowering answers 42.
    let (_, rows) =
        crate::run_select_meta_on("SELECT MAX(c), MIN(c) FROM ox", &catalog, &ctx).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0], Datum::Int(42), "{rows:#?}");
    assert_eq!(rows[0][1], Datum::Int(3), "{rows:#?}");
}

#[test]
fn max_over_a_derived_sum_materializes_coprocessor_topn_keys() {
    let mut catalog = Catalog::default();
    crate::run_create_table_on(
        "CREATE TABLE topn_cast (id BIGINT PRIMARY KEY, v BIGINT)",
        &mut catalog,
    )
    .unwrap();
    let ctx = crate::StmtContext::for_query();
    run_insert_on(
        "INSERT INTO topn_cast VALUES (1,7),(2,42),(3,9),(4,NULL)",
        &mut catalog,
        &ctx,
    )
    .unwrap();
    let rows = run_select_on(
        "SELECT id FROM (SELECT id,SUM(v) AS total FROM topn_cast GROUP BY id) d \
         WHERE total=(SELECT MAX(total) FROM \
         (SELECT id,SUM(v) AS total FROM topn_cast GROUP BY id) x)",
        &catalog,
        &ctx,
    )
    .unwrap();
    assert_eq!(rows, vec![vec![Datum::Int(2)]]);
}

#[test]
fn derived_aggregate_null_filter_refreshes_source_statistics() {
    use crate::explain::{explain_select_stmt, ExplainFormat};

    let mut catalog = Catalog::default();
    crate::run_create_table_on(
        "CREATE TABLE district (d_id INT NOT NULL, d_w_id INT NOT NULL, \
         d_ytd DECIMAL(12,2), PRIMARY KEY(d_w_id,d_id) CLUSTERED)",
        &mut catalog,
    )
    .unwrap();
    let ctx = crate::StmtContext::for_query();
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
    use crate::explain::{explain_select_stmt, ExplainFormat};

    let mut catalog = Catalog::default();
    crate::run_create_table_on(
        "CREATE TABLE orders (o_id INT NOT NULL, o_d_id INT NOT NULL, o_w_id INT NOT NULL,             o_c_id INT, o_entry_d DATETIME, o_carrier_id INT, o_ol_cnt INT, o_all_local INT,             PRIMARY KEY (o_w_id,o_d_id,o_id) CLUSTERED,             KEY idx_order (o_w_id,o_d_id,o_c_id,o_id))",
        &mut catalog,
    )
    .unwrap();
    let ctx = crate::StmtContext::for_query();
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

/// Go accepts a pushdown-safe expression as the input of a global SUM. The
/// cop StreamAgg evaluates that expression after its Selection, and the root
/// StreamAgg merges one partial result per region. Live Go with the same
/// pseudo-statistics fixture selects this pair (2026-09-07).
#[test]
fn global_sum_expression_uses_partial_and_final_stream_agg() {
    use crate::explain::{explain_select_stmt, ExplainFormat};

    let mut catalog = Catalog::default();
    crate::run_create_table_on(
        "CREATE TABLE revenue (\
            id INT PRIMARY KEY, price DECIMAL(10,2), discount DECIMAL(4,2), k INT)",
        &mut catalog,
    )
    .unwrap();
    let ctx = crate::StmtContext::for_query();
    run_insert_on(
        "INSERT INTO revenue VALUES \
            (1,100.00,0.05,1),(2,200.00,0.10,2),(3,300.00,0.20,4)",
        &mut catalog,
        &ctx,
    )
    .unwrap();

    let sql = "SELECT SUM(price * discount) FROM revenue WHERE k >= 1 AND k <= 3";
    let result = run_select_on(sql, &catalog, &ctx).unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].len(), 1);
    assert_eq!(result[0][0].sql_string().unwrap(), "25.0000");

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
            "└─TableReader",
            "  └─StreamAgg",
            "    └─Selection",
            "      └─TableFullScan"
        ]
    );
    assert_eq!(
        (0..rows.len()).map(|row| cell(row, 2)).collect::<Vec<_>>(),
        vec!["root", "root", "cop[tikv]", "cop[tikv]", "cop[tikv]"]
    );
    assert!(cell(0, 4).starts_with("funcs:sum(Column#"));
    assert!(cell(2, 4).contains("sum(mul(test.revenue.price, test.revenue.discount))"));
}

/// A single integer SUM above a joined source uses Go's serial root
/// StreamAgg, including the decimal input projection required by SUM.
#[test]
fn joined_integer_sum_uses_root_stream_agg() {
    use crate::explain::{explain_select_stmt, ExplainFormat};

    let mut catalog = Catalog::default();
    crate::run_create_table_on(
        "CREATE TABLE sum_orders (id INT PRIMARY KEY, customer_id INT, quantity INT)",
        &mut catalog,
    )
    .unwrap();
    crate::run_create_table_on(
        "CREATE TABLE sum_customers (id INT PRIMARY KEY)",
        &mut catalog,
    )
    .unwrap();
    // Go picks the serial root StreamAgg only when the HashAgg's divided
    // CPU cost loses; with the default five final workers it picks HashAgg.
    // The tpcds matrix this test was authored from ran every concurrency
    // variable at 1, so pin the same serial session.
    let ctx = crate::StmtContext::for_query().with_hashagg_concurrency(1, 1);
    run_insert_on(
        "INSERT INTO sum_orders VALUES (1,10,7),(2,20,11),(3,30,13)",
        &mut catalog,
        &ctx,
    )
    .unwrap();
    run_insert_on(
        "INSERT INTO sum_customers VALUES (10),(20),(30)",
        &mut catalog,
        &ctx,
    )
    .unwrap();

    let sql =
        "SELECT SUM(o.quantity) FROM sum_orders o JOIN sum_customers c ON o.customer_id = c.id";
    let result = run_select_on(sql, &catalog, &ctx).unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result[0][0].sql_string().unwrap(), "31");

    let stmt = tidb_parser::parse(sql).unwrap();
    let Stmt::Query(query) = &stmt else {
        panic!("not a query");
    };
    let QueryStmt::Select(select) = &**query else {
        panic!("not a SELECT");
    };
    let (_, rows) =
        explain_select_stmt(select, &catalog, "test", &ctx, ExplainFormat::Brief).unwrap();
    let operator = |row: &[Datum]| match &row[0] {
        Datum::Bytes(bytes) => String::from_utf8_lossy(bytes).into_owned(),
        other => format!("{other:?}"),
    };
    assert_eq!(operator(&rows[0]), "StreamAgg");
    assert!(rows.iter().any(|row| {
        operator(row).trim_start_matches(&[' ', '│', '├', '└', '─'][..]) == "Projection"
    }));
}

/// Go's `BasePhysicalAgg.NewPartialAggregate` expands a global AVG into a
/// cop COUNT/SUM pair and a root final AVG over those two partial columns.
/// Live Go selects StreamAgg at both stages for this pseudo-statistics fixture.
#[test]
fn global_avg_uses_count_sum_partial_and_final_stream_agg() {
    use crate::explain::{explain_select_stmt, ExplainFormat};

    let mut catalog = Catalog::default();
    crate::run_create_table_on(
        "CREATE TABLE avg_revenue (id INT PRIMARY KEY, price DECIMAL(10,2), k INT)",
        &mut catalog,
    )
    .unwrap();
    let ctx = crate::StmtContext::for_query();
    run_insert_on(
        "INSERT INTO avg_revenue VALUES (10,100.00,1),(20,200.00,2),(30,300.00,3)",
        &mut catalog,
        &ctx,
    )
    .unwrap();

    let sql = "SELECT AVG(price) FROM avg_revenue WHERE k >= 1 AND k <= 3";
    let result = run_select_on(sql, &catalog, &ctx).unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result[0][0].sql_string().unwrap(), "200.000000");

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
            "└─TableReader",
            "  └─StreamAgg",
            "    └─Selection",
            "      └─TableFullScan"
        ]
    );
    assert!(cell(0, 4).contains("funcs:avg(Column#"));
    assert!(cell(0, 4).matches("Column#").count() >= 3);
    assert!(cell(2, 4).contains("funcs:count(test.avg_revenue.price)"));
    assert!(cell(2, 4).contains("funcs:sum(test.avg_revenue.price)"));
}

/// Go splits a pseudo-statistics Sysbench SUM range into partial/final
/// StreamAgg stages. The partial result is already DECIMAL, so the root cast
/// projection used by the one-row plan is absent.
#[test]
fn global_integer_sum_uses_gos_stream_agg_and_cast_projection() {
    use crate::explain::{explain_select_stmt, ExplainFormat};

    let mut catalog = Catalog::default();
    crate::run_create_table_on(
        "CREATE TABLE sum_range (id INT PRIMARY KEY, k INT NOT NULL)",
        &mut catalog,
    )
    .unwrap();
    let ctx = crate::StmtContext::for_query();
    run_insert_on(
        "INSERT INTO sum_range VALUES (1, 10), (2, 20)",
        &mut catalog,
        &ctx,
    )
    .unwrap();

    let sql = "SELECT SUM(k) FROM sum_range WHERE id BETWEEN 1 AND 100";
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
            "└─TableReader",
            "  └─StreamAgg",
            "    └─TableRangeScan"
        ]
    );
    assert_eq!(
        (0..rows.len()).map(|row| cell(row, 2)).collect::<Vec<_>>(),
        vec!["root", "root", "cop[tikv]", "cop[tikv]"]
    );
    assert!(cell(0, 4).starts_with("funcs:sum(Column#"));
    assert!(cell(2, 4).contains("funcs:sum(test.sum_range.k)->Column#"));
    assert_eq!(
        run_select_on(sql, &catalog, &ctx).unwrap(),
        vec![vec![Datum::Decimal(tidb_datatype::Decimal::from_int(30))]]
    );
}

/// Go's ordinary optimizer converts the one-row table path to PointGet after
/// predicate pushdown, then costs the global COUNT as StreamAgg. The key
/// equality is an access condition, so no Selection survives above PointGet.
#[test]
fn global_count_over_point_get_uses_gos_stream_agg_without_selection() {
    use crate::explain::{explain_select_stmt, ExplainFormat};

    let mut catalog = Catalog::default();
    crate::run_create_table_on(
        "CREATE TABLE point_count (\
            id INT NOT NULL AUTO_INCREMENT, \
            k INT NOT NULL DEFAULT 0, \
            c CHAR(120) NOT NULL DEFAULT '', \
            pad CHAR(60) NOT NULL DEFAULT '', \
            PRIMARY KEY (id) CLUSTERED, \
            KEY k_1 (k))",
        &mut catalog,
    )
    .unwrap();
    let ctx = crate::StmtContext::for_query();
    run_insert_on(
        "INSERT INTO point_count VALUES \
            (1, 10, 'one', 'pad'), (2, 20, 'two', 'pad')",
        &mut catalog,
        &ctx,
    )
    .unwrap();

    let sql = "SELECT COUNT(*) FROM point_count WHERE id = 1";
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
        vec!["StreamAgg", "└─Point_Get"]
    );
    assert_eq!(
        run_select_on(sql, &catalog, &ctx).unwrap(),
        vec![vec![Datum::Int(1)]]
    );
}

/// Go keeps Sysbench's random-range COUNT on the covering secondary index and
/// places a partial StreamAgg over that scan below the IndexReader.
#[test]
fn global_count_over_index_ranges_uses_gos_stream_agg_and_index_reader() {
    use crate::explain::{explain_select_stmt, ExplainFormat};

    let mut catalog = Catalog::default();
    crate::run_create_table_on(
        "CREATE TABLE count_ranges (id INT PRIMARY KEY, k INT NOT NULL, KEY k_1(k))",
        &mut catalog,
    )
    .unwrap();
    let ctx = crate::StmtContext::for_query();
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
            .iter()
            .find(|column| column.name == "k")
            .map(|column| column.id)
            .expect("k column");
        let index_id = table
            .indexes()
            .iter()
            .find(|index| index.name == "k_1")
            .map(|index| index.id)
            .expect("k_1 index");
        (table.table_id, k_column_id, index_id)
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
            tidb_planner::cardinality::row_count_estimator::ColumnStats {
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
                topn: Some(topn.clone()),
                cms: None,
                stats_ver: 2,
                unsigned: false,
            },
            tidb_planner::cardinality::row_count_estimator::IndexStats {
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
                topn: Some(topn),
                cms: None,
                stats_ver: 2,
                num_columns: 1,
                unique: false,
            },
        )
    };
    let (column_stats, index_stats) = analyzed_stats(k_column_id, index_id);
    let statistics = crate::access_cost::TableStatistics::new(
        10_000,
        0,
        [(k_column_id, column_stats)].into_iter().collect(),
        [(index_id, index_stats)].into_iter().collect(),
    );
    let ctx = ctx.with_fresh_staged_writes();
    catalog.set_table_statistics(table_id, std::sync::Arc::new(statistics));
    let (_, analyzed) =
        explain_select_stmt(select, &catalog, "test", &ctx, ExplainFormat::Brief).unwrap();
    let analyzed_cell = |row: usize, column: usize| match &analyzed[row][column] {
        Datum::Bytes(bytes) => String::from_utf8_lossy(bytes).into_owned(),
        other => format!("{other:?}"),
    };
    assert_eq!(analyzed_cell(3, 1), "5.75", "{analyzed:#?}");
}

#[test]
fn aggregate_selects() {
    let catalog = test_catalog();
    // Global aggregates: rows (1,30),(2,20),(3,10).
    assert_eq!(
        run_select_on(
            "SELECT COUNT(*), SUM(a) FROM t",
            &catalog,
            &crate::StmtContext::for_query()
        )
        .unwrap(),
        // SUM is a DECIMAL in MySQL even over a BIGINT column.
        vec![vec![
            Datum::Int(3),
            Datum::Decimal(tidb_datatype::Decimal::from_int(6))
        ]]
    );
    // GROUP BY with a carried key column, WHERE below the agg.
    // As in Go's aggregate tests, normalize results without ORDER BY.
    let mut groups = run_select_on(
        "SELECT a, COUNT(*) FROM t WHERE b >= 20 GROUP BY a",
        &catalog,
        &crate::StmtContext::for_query(),
    )
    .unwrap();
    groups.sort_by_key(|row| match row[0] {
        Datum::Int(value) => value,
        _ => panic!("integer group key"),
    });
    assert_eq!(
        groups,
        vec![
            vec![Datum::Int(1), Datum::Int(1)],
            vec![Datum::Int(2), Datum::Int(1)],
        ]
    );
    // Empty-input rules through SQL: global agg over no rows -> one row.
    assert_eq!(
        run_select_on(
            "SELECT COUNT(a) FROM t WHERE a > 100",
            &catalog,
            &crate::StmtContext::for_query()
        )
        .unwrap(),
        vec![vec![Datum::Int(0)]]
    );
    assert_eq!(
        run_select_on(
            "SELECT a, COUNT(*) FROM t WHERE a > 100 GROUP BY a",
            &catalog,
            &crate::StmtContext::for_query()
        )
        .unwrap(),
        Vec::<Vec<Datum>>::new()
    );
    // MIN/MAX over the shared datum ordering.
    assert_eq!(
        run_select_on(
            "SELECT MIN(a), MAX(b) FROM t",
            &catalog,
            &crate::StmtContext::for_query()
        )
        .unwrap(),
        vec![vec![Datum::Int(1), Datum::Int(30)]]
    );
    // AVG over integers is DECIMAL, scaled by div_precision_increment.
    assert_eq!(
        run_select_on(
            "SELECT AVG(a) FROM t",
            &catalog,
            &crate::StmtContext::for_query()
        )
        .unwrap(),
        vec![vec![Datum::Decimal(tidb_datatype::Decimal::from_literal(
            "2.0000"
        ))]]
    );
    // DISTINCT folds repeated inputs once per group: a is 1,2,3 while the
    // constant 1 collapses to a single counted value.
    assert_eq!(
        run_select_on(
            "SELECT COUNT(DISTINCT a), COUNT(DISTINCT 1) FROM t",
            &catalog,
            &crate::StmtContext::for_query()
        )
        .unwrap(),
        vec![vec![Datum::Int(3), Datum::Int(1)]]
    );
    // An all-NULL / empty group is NULL for MIN/MAX and AVG, as in Go.
    assert_eq!(
        run_select_on(
            "SELECT MIN(a), MAX(a), AVG(a) FROM t WHERE a > 100",
            &catalog,
            &crate::StmtContext::for_query()
        )
        .unwrap(),
        vec![vec![Datum::Null, Datum::Null, Datum::Null]]
    );
}

#[test]
fn float_sum_and_avg_use_the_real_domain() {
    let mut catalog = Catalog::default();
    crate::run_create_table_on("CREATE TABLE f (v FLOAT)", &mut catalog).unwrap();
    run_insert_on(
        "INSERT INTO f VALUES (1.25), (2.5), (NULL)",
        &mut catalog,
        &crate::StmtContext::for_query(),
    )
    .unwrap();

    assert_eq!(
        run_select_on(
            "SELECT SUM(v), AVG(v) FROM f",
            &catalog,
            &crate::StmtContext::for_query(),
        )
        .unwrap(),
        vec![vec![Datum::Real(3.75), Datum::Real(1.875)]],
    );
}

/// HAVING filters aggregate output rows, ORDER BY sorts them, and an
/// aggregate that appears only in those clauses is computed as a hidden
/// column and trimmed from the result (Go's resolveHavingAndOrderBy plus
/// the final projection).
#[test]
fn aggregate_having_and_order_by() {
    // Go's SERIAL HashAgg emits its groups in `groupKeys` first-seen order;
    // the parallel pipeline shuffles them by final worker. The assertions
    // below that do not write an ORDER BY therefore pin the serial path,
    // which is what Go selects with both hashagg concurrencies at 1.
    let ctx = || crate::StmtContext::for_query().with_hashagg_concurrency(1, 1);
    let mut catalog = test_catalog();
    crate::run_create_table_on("CREATE TABLE g (a BIGINT, b BIGINT)", &mut catalog).unwrap();
    run_insert_on(
        "INSERT INTO g VALUES (1, 10), (1, 20), (2, 5), (3, 7), (3, 8)",
        &mut catalog,
        &ctx(),
    )
    .unwrap();

    // HAVING over an aggregate that IS in the select list.
    assert_eq!(
        run_select_on(
            "SELECT a, COUNT(*) FROM g GROUP BY a HAVING COUNT(*) > 1",
            &catalog,
            &ctx()
        )
        .unwrap(),
        vec![
            vec![Datum::Int(1), Datum::Int(2)],
            vec![Datum::Int(3), Datum::Int(2)],
        ]
    );
    // HAVING over an aggregate that is NOT selected: one output column.
    assert_eq!(
        run_select_on(
            "SELECT a FROM g GROUP BY a HAVING SUM(b) > 15",
            &catalog,
            &ctx()
        )
        .unwrap(),
        vec![vec![Datum::Int(1)]]
    );
    // ORDER BY an aggregate that is not selected, descending.
    assert_eq!(
        run_select_on(
            "SELECT a FROM g GROUP BY a ORDER BY SUM(b) DESC",
            &catalog,
            &ctx()
        )
        .unwrap(),
        vec![
            vec![Datum::Int(1)],
            vec![Datum::Int(3)],
            vec![Datum::Int(2)]
        ]
    );
    // HAVING and ORDER BY together, with LIMIT applied after both.
    assert_eq!(
        run_select_on(
            "SELECT a, SUM(b) FROM g GROUP BY a HAVING COUNT(*) > 1 ORDER BY SUM(b) LIMIT 1",
            &catalog,
            &ctx()
        )
        .unwrap(),
        vec![vec![
            Datum::Int(3),
            Datum::Decimal(tidb_datatype::Decimal::from_int(15))
        ]]
    );
    // ORDER BY a selected alias.
    assert_eq!(
        run_select_on(
            "SELECT a, SUM(b) AS total FROM g GROUP BY a ORDER BY total",
            &catalog,
            &ctx()
        )
        .unwrap(),
        vec![
            vec![
                Datum::Int(2),
                Datum::Decimal(tidb_datatype::Decimal::from_int(5))
            ],
            vec![
                Datum::Int(3),
                Datum::Decimal(tidb_datatype::Decimal::from_int(15))
            ],
            vec![
                Datum::Int(1),
                Datum::Decimal(tidb_datatype::Decimal::from_int(30))
            ],
        ]
    );
    // A grouped column that is not selected is still visible to HAVING
    // and ORDER BY (Go carries it as a hidden FIRST_ROW column).
    assert_eq!(
        run_select_on(
            "SELECT COUNT(*) FROM g GROUP BY a HAVING a > 1",
            &catalog,
            &ctx()
        )
        .unwrap(),
        vec![vec![Datum::Int(1)], vec![Datum::Int(2)]]
    );
    // A global aggregate's HAVING filters the single group.
    assert_eq!(
        run_select_on(
            "SELECT COUNT(*) FROM g HAVING COUNT(*) > 100",
            &catalog,
            &ctx()
        )
        .unwrap(),
        Vec::<Vec<Datum>>::new()
    );
}

/// SELECT DISTINCT deduplicates the projected rows, which Go builds as an
/// aggregation grouping by every projected column with FIRST_ROW
/// aggregates. The plain path silently returned duplicates before.
#[test]
fn select_distinct() {
    // Go's SERIAL HashAgg emits its groups in `groupKeys` first-seen order
    // (`unparallelExec` walks that slice), while the parallel pipeline -- the
    // default concurrency -- shuffles them by final worker. The order-sensitive
    // assertions below therefore pin the serial path, which is what Go selects
    // when both `tidb_hashagg_{partial,final}_concurrency` are 1.
    let ctx = || crate::StmtContext::for_query().with_hashagg_concurrency(1, 1);
    let mut catalog = Catalog::default();
    crate::run_create_table_on("CREATE TABLE d2 (a BIGINT, b BIGINT)", &mut catalog).unwrap();
    run_insert_on(
        "INSERT INTO d2 VALUES (1, 1), (1, 2), (1, 1), (2, 2)",
        &mut catalog,
        &ctx(),
    )
    .unwrap();

    assert_eq!(
        run_select_on("SELECT DISTINCT a FROM d2", &catalog, &ctx()).unwrap(),
        vec![vec![Datum::Int(1)], vec![Datum::Int(2)]]
    );
    // Every projected column takes part, so (1,1) collapses but (1,2)
    // stays.
    assert_eq!(
        run_select_on("SELECT DISTINCT a, b FROM d2", &catalog, &ctx()).unwrap(),
        vec![
            vec![Datum::Int(1), Datum::Int(1)],
            vec![Datum::Int(1), Datum::Int(2)],
            vec![Datum::Int(2), Datum::Int(2)],
        ]
    );
    // Without DISTINCT every row survives.
    assert_eq!(
        run_select_on("SELECT a FROM d2", &catalog, &ctx())
            .unwrap()
            .len(),
        4
    );

    // DISTINCT applies to the projected expression, not the source rows.
    assert_eq!(
        run_select_on("SELECT DISTINCT a + b FROM d2", &catalog, &ctx()).unwrap(),
        vec![
            vec![Datum::Int(2)],
            vec![Datum::Int(3)],
            vec![Datum::Int(4)]
        ]
    );

    // The dedup emits groups in first-seen order, so a sort below it still
    // orders the surviving rows.
    assert_eq!(
        run_select_on(
            "SELECT DISTINCT a FROM d2 ORDER BY a DESC",
            &catalog,
            &ctx()
        )
        .unwrap(),
        vec![vec![Datum::Int(2)], vec![Datum::Int(1)]]
    );
    // LIMIT applies after the dedup.
    assert_eq!(
        run_select_on("SELECT DISTINCT a FROM d2 LIMIT 1", &catalog, &ctx()).unwrap(),
        vec![vec![Datum::Int(1)]]
    );
    // A WHERE below it still filters.
    assert_eq!(
        run_select_on("SELECT DISTINCT a FROM d2 WHERE b = 2", &catalog, &ctx()).unwrap(),
        vec![vec![Datum::Int(1)], vec![Datum::Int(2)]]
    );

    // Over an aggregate result, DISTINCT deduplicates the output rows.
    crate::run_create_table_on("CREATE TABLE g3 (k BIGINT, v BIGINT)", &mut catalog).unwrap();
    run_insert_on(
        "INSERT INTO g3 VALUES (1, 5), (2, 5), (3, 9)",
        &mut catalog,
        &ctx(),
    )
    .unwrap();
    assert_eq!(
        run_select_on(
            "SELECT DISTINCT SUM(v) FROM g3 GROUP BY k",
            &catalog,
            &ctx()
        )
        .unwrap(),
        vec![
            vec![Datum::Decimal(tidb_datatype::Decimal::from_int(5))],
            vec![Datum::Decimal(tidb_datatype::Decimal::from_int(9))],
        ]
    );
}

/// A DISTINCT scalar subquery may accept a cop partial aggregate while its
/// residual predicate remains above the scan. EXPLAIN must fall back to the
/// ordinary root HashAgg shape instead of returning a trace-only error.
#[test]
fn explain_distinct_scalar_subquery_with_filter() {
    use crate::explain::{explain_select_stmt, ExplainFormat};

    let mut catalog = Catalog::default();
    crate::run_create_table_on(
        "CREATE TABLE date_dim (d_month_seq BIGINT, d_year BIGINT, d_moy BIGINT)",
        &mut catalog,
    )
    .unwrap();
    run_insert_on(
        "INSERT INTO date_dim VALUES (1201, 2000, 2), (1201, 2000, 2)",
        &mut catalog,
        &crate::StmtContext::for_query(),
    )
    .unwrap();

    let sql = "SELECT (SELECT DISTINCT d_month_seq FROM date_dim \
        WHERE d_year = 2000 AND d_moy = 2)";
    let stmt = tidb_parser::parse(sql).unwrap();
    let Stmt::Query(query) = &stmt else {
        panic!("not a query");
    };
    let QueryStmt::Select(select) = &**query else {
        panic!("not a SELECT");
    };
    let (_, rows) = explain_select_stmt(
        select,
        &catalog,
        "test",
        &crate::StmtContext::for_query(),
        ExplainFormat::Brief,
    )
    .unwrap();
    let operators = rows
        .iter()
        .filter_map(|row| match row.first() {
            Some(Datum::Bytes(bytes)) => Some(String::from_utf8_lossy(bytes).into_owned()),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert!(
        operators
            .iter()
            .any(|operator| operator.contains("HashAgg")),
        "{rows:#?}"
    );
    assert!(
        operators
            .iter()
            .any(|operator| operator.contains("Selection")),
        "{rows:#?}"
    );
    assert_eq!(
        run_select_on(sql, &catalog, &crate::StmtContext::for_query()).unwrap(),
        vec![vec![Datum::Int(1201)]]
    );
}

/// `BIT_AND`/`BIT_OR`/`BIT_XOR` return BIGINT **UNSIGNED**.
///
/// Go `aggregation/base_func.go`'s `typeInfer4BitFuncs` adds
/// `mysql.UnsignedFlag`, and `func_bitfuncs.go` appends the fold with
/// `AppendUint64`. Captured from TiDB over `v BIGINT`:
///
/// ```text
/// values (null)      -> 18446744073709551615 | 0                    | 0
/// values (-1),(-1)   -> 18446744073709551615 | 18446744073709551615 | 0
/// values (3),(5),(null) -> 1 | 7 | 6
/// desc of a view over them -> bigint(21) unsigned NO
/// ```
///
/// The all-NULL and all-`-1` rows are the ones that separate the signed
/// reading from Go's: as a signed BIGINT they would print `-1`.
#[test]
fn bit_aggregates_are_unsigned() {
    use tidb_datatype::FieldTypeCode;

    fn catalog_with(rows: Vec<Vec<Datum>>) -> Catalog {
        let mut catalog = Catalog::default();
        catalog.register(
            "b",
            MemTable {
                columns: vec![("v".to_owned(), FieldType::new(FieldTypeCode::LongLong))],
                rows,
            },
        );
        catalog
    }

    // `Datum`'s equality is NUMERIC across the integer kinds, so `Int(-1)`
    // and `UInt(u64::MAX)` compare equal. The whole finding is about which
    // KIND the fold lands in, so the assertion has to read the variant.
    fn unsigned(value: &Datum) -> u64 {
        match value {
            Datum::UInt(bits) => *bits,
            other => panic!("expected an unsigned datum, got {other:?}"),
        }
    }

    let query = "SELECT BIT_AND(v), BIT_OR(v), BIT_XOR(v) FROM b";
    let cases: Vec<(Vec<Vec<Datum>>, [u64; 3])> = vec![
        (vec![vec![Datum::Null]], [u64::MAX, 0, 0]),
        (
            vec![vec![Datum::Int(-1)], vec![Datum::Int(-1)]],
            [u64::MAX, u64::MAX, 0],
        ),
        (
            vec![vec![Datum::Int(3)], vec![Datum::Int(5)], vec![Datum::Null]],
            [1, 7, 6],
        ),
    ];
    for (rows, expected) in cases {
        let out =
            run_select_on(query, &catalog_with(rows), &crate::StmtContext::for_query()).unwrap();
        assert_eq!(out.len(), 1);
        let folds: Vec<u64> = out[0].iter().map(unsigned).collect();
        assert_eq!(folds, expected.to_vec());
    }
}

/// Aggregates read the COLLATION of what they aggregate, not raw bytes.
///
/// Go builds a `collate.GetCollator(RetTp.GetCollate())` per aggregate
/// (`aggfuncs/builder.go:460-468` for MIN/MAX, one per `byItem` for
/// `GROUP_CONCAT`'s own ORDER BY) and keys the DISTINCT value set with the
/// same collator. Captured from TiDB on a `utf8mb4_general_ci` column:
///
/// ```text
/// values ('a'),('B'),('A')
///   max(s), min(s)                                  -> B | a
///   count(distinct s), (distinct concat(s,'')),
///                      (distinct upper(s))          -> 2 | 2 | 2
/// values ('B'),('a')
///   group_concat(s order by s), (... order by s desc) -> a,B | B,a
/// values ('b'),('A'),('a'),('B')
///   group_concat(distinct s)                        -> b,A
/// ```
///
/// The ORDER BY probe deliberately uses a pair with NO collation tie:
/// `'B','a'` sorts `a,B` under the collation and `B,a` under bytes. Go
/// resolves ties with an unstable `sort.Sort` over its top-N heap
/// (`func_group_concat.go:470`), so the relative order of two
/// collation-equal values is not a behavior to pin.
///
/// The two COMPUTED-argument DISTINCT counts are the ones a bare column
/// cannot catch: a column datum carries its own collation, but a string
/// builtin mints its result with the default `utf8mb4_bin`, so the key has
/// to come from the argument EXPRESSION's derived collation.
#[test]
fn aggregates_read_the_arguments_collation() {
    fn catalog_with(values: &[&str]) -> Catalog {
        let mut catalog = Catalog::default();
        crate::run_create_table_on(
            "CREATE TABLE g (s VARCHAR(10) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci)",
            &mut catalog,
        )
        .unwrap();
        let list = values
            .iter()
            .map(|value| format!("('{value}')"))
            .collect::<Vec<_>>()
            .join(", ");
        run_insert_on(
            &format!("INSERT INTO g VALUES {list}"),
            &mut catalog,
            &crate::StmtContext::for_query(),
        )
        .unwrap();
        catalog
    }

    let catalog = catalog_with(&["a", "B", "A"]);
    let row = |sql: &str| {
        run_select_on(sql, &catalog, &crate::StmtContext::for_query())
            .unwrap()
            .remove(0)
    };

    // MIN/MAX under the case-insensitive collation: the binary answers would
    // be max='a', min='A'.
    assert_eq!(
        row("SELECT MAX(s), MIN(s) FROM g")
            .iter()
            .map(datum_text_for_test)
            .collect::<Vec<_>>(),
        vec!["B".to_owned(), "a".to_owned()]
    );
    // DISTINCT over a bare column AND over two computed string expressions.
    assert_eq!(
        row("SELECT COUNT(DISTINCT s), COUNT(DISTINCT CONCAT(s, '')), COUNT(DISTINCT UPPER(s)) FROM g"),
        vec![Datum::Int(2), Datum::Int(2), Datum::Int(2)]
    );

    // GROUP_CONCAT's own ORDER BY sorts under the byItem's collation: over
    // 'B','a' the byte order is the REVERSE of the collation order.
    let catalog = catalog_with(&["B", "a"]);
    let row = |sql: &str| {
        run_select_on(sql, &catalog, &crate::StmtContext::for_query())
            .unwrap()
            .remove(0)
    };
    assert_eq!(
        datum_text_for_test(&row("SELECT GROUP_CONCAT(s ORDER BY s) FROM g")[0]),
        "a,B"
    );
    assert_eq!(
        datum_text_for_test(&row("SELECT GROUP_CONCAT(s ORDER BY s DESC) FROM g")[0]),
        "B,a"
    );

    let catalog = catalog_with(&["b", "A", "a", "B"]);
    let row = |sql: &str| {
        run_select_on(sql, &catalog, &crate::StmtContext::for_query())
            .unwrap()
            .remove(0)
    };
    assert_eq!(
        datum_text_for_test(&row("SELECT GROUP_CONCAT(DISTINCT s) FROM g")[0]),
        "b,A"
    );
}

/// A DISTINCT argument that is a COMPUTED expression forces the cop partial
/// aggregation to emit its GROUP BY key out of the operator.
///
/// Go's `BuildFinalModeAggregation` (`base_physical_agg.go:681`) moves the
/// distinct argument into the partial's GROUP BY and, for a cop partial, drops
/// the redundant `firstrow()` ("group by items are outputted by group by
/// schema"). The partial therefore has NO aggregate functions at all and its
/// schema is exactly the group-by columns. A bare column key never reaches
/// this path because the scan's partial-aggregate pushdown deduplicates it
/// (`pushed_partial_aggregation`), which is why only a computed key exposes a
/// missing group-key emission.
#[test]
fn a_computed_distinct_argument_round_trips_through_the_cop_partial_aggregation() {
    let mut catalog = Catalog::default();
    crate::run_create_table_on(
        "CREATE TABLE g (s VARCHAR(10) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci)",
        &mut catalog,
    )
    .unwrap();
    run_insert_on(
        "INSERT INTO g VALUES ('a'), ('B'), ('A')",
        &mut catalog,
        &crate::StmtContext::for_query(),
    )
    .unwrap();

    // A function-only partial is not enough: the distinct argument shares the
    // partial with a real aggregate, so the group-by column TRAILS `count(*)`
    // in the same output schema.
    assert_eq!(
        run_select_on(
            "SELECT COUNT(DISTINCT CONCAT(s, '')), COUNT(*) FROM g",
            &catalog,
            &crate::StmtContext::for_query(),
        )
        .unwrap()
        .remove(0),
        vec![Datum::Int(2), Datum::Int(3)]
    );

    // A GROUP BY column AND a computed distinct argument: the partial groups
    // by `(s, concat(s, ''))` and emits both trailing columns.
    let rows = run_select_on(
        "SELECT s, COUNT(DISTINCT CONCAT(s, '')) FROM g GROUP BY s ORDER BY s",
        &catalog,
        &crate::StmtContext::for_query(),
    )
    .unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(datum_text_for_test(&rows[0][0]), "a");
    assert_eq!(rows[0][1], Datum::Int(1));
    assert_eq!(datum_text_for_test(&rows[1][0]), "B");
    assert_eq!(rows[1][1], Datum::Int(1));
}

/// Executing a grouped aggregate and planning its result metadata use the same
/// physical planner and agree on the output columns.
#[test]
fn a_delivered_grouped_aggregate_matches_the_planned_statement() {
    let mut catalog = Catalog::default();
    let ctx = crate::StmtContext::for_query();
    crate::run_create_table_on(
        "CREATE TABLE cost_receipt (id INT PRIMARY KEY, k INT, c VARCHAR(20))",
        &mut catalog,
    )
    .unwrap();
    run_insert_on(
        "INSERT INTO cost_receipt VALUES (1, 10, 'a'), (2, 10, 'b'), (3, 20, 'a')",
        &mut catalog,
        &ctx,
    )
    .unwrap();

    let sql = "SELECT k, SUM(id) FROM cost_receipt WHERE id BETWEEN 1 AND 100 GROUP BY k";
    let stmt = tidb_parser::parse(sql).unwrap();
    let Stmt::Query(query) = &stmt else {
        panic!("not a query");
    };
    let QueryStmt::Select(select) = &**query else {
        panic!("not a SELECT");
    };

    let (executed_columns, mut rows) = run_select_meta_on(sql, &catalog, &ctx).unwrap();
    let planned_columns = plan_select_meta_stmt(select, &catalog, "test", &ctx).unwrap();
    assert_eq!(
        executed_columns, planned_columns,
        "the delivered pipeline's columns must be the planned statement's, or \
         the cost receipt moved the plan"
    );

    rows.sort_by_key(|row| match row[0] {
        Datum::Int(value) => value,
        _ => panic!("the group key is the FIRST output column, not the sum"),
    });
    let keys: Vec<_> = rows.iter().map(|row| row[0].clone()).collect();
    assert_eq!(
        keys,
        vec![Datum::Int(10), Datum::Int(20)],
        "the group key must arrive in the SELECT list's position; TiKV returns \
         the partial aggregate functions first and the restoring Projection is \
         what puts them back, and that Projection is exactly what derived mode \
         drops"
    );
}

#[test]
fn grouped_aggregation_enumerates_families_over_one_planned_child() {
    let mut catalog = Catalog::default();
    let ctx = crate::StmtContext::for_query();
    crate::run_create_table_on(
        "CREATE TABLE shared_child (id INT PRIMARY KEY, k INT, INDEX idx_k(k))",
        &mut catalog,
    )
    .unwrap();
    run_insert_on(
        "INSERT INTO shared_child VALUES (1, 10), (2, 10), (3, 20)",
        &mut catalog,
        &ctx,
    )
    .unwrap();

    let (_, mut rows) = run_select_meta_on(
        "SELECT k, SUM(id) FROM shared_child GROUP BY k",
        &catalog,
        &ctx,
    )
    .unwrap();
    rows.sort_by_key(|row| match row[0] {
        Datum::Int(value) => value,
        _ => i64::MAX,
    });
    assert_eq!(rows.len(), 2);
}

#[test]
fn explain_uses_the_common_physical_plan_without_legacy_ast_execution() {
    use crate::explain::{explain_select_stmt, ExplainFormat};

    let mut catalog = Catalog::default();
    let ctx = crate::StmtContext::for_query();
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

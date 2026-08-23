//! Aggregate queries: the aggregate functions, `GROUP BY`, `HAVING`,
//! aggregate `ORDER BY`, and `SELECT DISTINCT`.
//!
//! Mirrors Go `pkg/executor/aggregate`'s hash-aggregate surface, including
//! the distinct path a `DISTINCT` select takes through the same operator.

use super::*;

/// TPC-H q1 is Go's complete grouped partial-aggregation contract: AVG is a
/// count/sum pair in TiKV, the root AVG merges both partial columns, and the
/// restoring projection stays below the final group-key sort.
#[test]
fn tpch_q1_splits_avg_and_sorts_the_restored_output() {
    use crate::explain::{explain_select_stmt, ExplainFormat};

    let mut catalog = Catalog::default();
    crate::run_create_table_on(
        "CREATE TABLE lineitem (\
            l_returnflag CHAR(1), l_linestatus CHAR(1), \
            l_quantity DECIMAL(15,2), l_extendedprice DECIMAL(15,2), \
            l_discount DECIMAL(15,2), l_tax DECIMAL(15,2), l_shipdate DATE)",
        &mut catalog,
    )
    .unwrap();
    let ctx = crate::StmtContext::for_query();
    run_insert_on(
        "INSERT INTO lineitem VALUES \
            ('A','F',10.00,100.00,0.10,0.05,'1998-01-01'), \
            ('A','F',20.00,200.00,0.20,0.10,'1998-02-01'), \
            ('B','O',30.00,300.00,0.05,0.02,'1998-03-01'), \
            ('Z','Z',99.00,999.00,0.90,0.90,'1999-01-01')",
        &mut catalog,
        &ctx,
    )
    .unwrap();

    let sql = "SELECT l_returnflag, l_linestatus, \
        SUM(l_quantity) AS sum_qty, SUM(l_extendedprice) AS sum_base_price, \
        SUM(l_extendedprice * (1 - l_discount)) AS sum_disc_price, \
        SUM(l_extendedprice * (1 - l_discount) * (1 + l_tax)) AS sum_charge, \
        AVG(l_quantity) AS avg_qty, AVG(l_extendedprice) AS avg_price, \
        AVG(l_discount) AS avg_disc, COUNT(*) AS count_order \
        FROM lineitem \
        WHERE l_shipdate <= DATE_SUB('1998-12-01', INTERVAL 108 DAY) \
        GROUP BY l_returnflag, l_linestatus \
        ORDER BY l_returnflag, l_linestatus";
    let result = run_select_on(sql, &catalog, &ctx).unwrap();
    assert_eq!(result.len(), 2);
    assert_eq!(result[0][0].sql_string().unwrap(), "A");
    assert_eq!(result[0][1].sql_string().unwrap(), "F");
    assert_eq!(result[0][6].sql_string().unwrap(), "15.000000");
    assert_eq!(result[0][7].sql_string().unwrap(), "150.000000");
    assert_eq!(result[0][8].sql_string().unwrap(), "0.150000");
    assert_eq!(result[0][9], Datum::Int(2));

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
            "Sort",
            "└─Projection",
            "  └─HashAgg",
            "    └─TableReader",
            "      └─HashAgg",
            "        └─Selection",
            "          └─TableFullScan",
        ],
        "{rows:#?}",
    );
    assert_eq!(
        cell(0, 4),
        "test.lineitem.l_returnflag, test.lineitem.l_linestatus"
    );
    assert!(
        cell(1, 4).starts_with("test.lineitem.l_returnflag, test.lineitem.l_linestatus, Column#")
    );
    assert!(cell(2, 4).contains("funcs:avg(Column#"));
    assert!(cell(2, 4).contains(", Column#"));
    assert!(cell(4, 4).contains("funcs:count(test.lineitem.l_quantity)->Column#"));
    assert!(cell(4, 4).contains("funcs:sum(test.lineitem.l_quantity)->Column#"));
    assert!(cell(4, 4).contains("funcs:count(test.lineitem.l_extendedprice)->Column#"));
    assert!(cell(4, 4).contains("funcs:sum(test.lineitem.l_extendedprice)->Column#"));
    assert!(cell(4, 4).contains("funcs:count(test.lineitem.l_discount)->Column#"));
    assert!(cell(4, 4).contains("funcs:sum(test.lineitem.l_discount)->Column#"));
}

/// TPC-H q3 exercises both physical projection rules around an aggregate:
/// the final SELECT projection stays above TopN, while the complex SUM input
/// is evaluated once by `InjectProjBelowAgg` immediately below HashAgg.
#[test]
fn tpch_q3_keeps_go_projections_around_grouped_topn() {
    use crate::explain::{explain_select_stmt, ExplainFormat};

    let mut catalog = Catalog::default();
    crate::run_create_table_on(
        "CREATE TABLE customer (c_custkey INT PRIMARY KEY, c_mktsegment VARCHAR(10))",
        &mut catalog,
    )
    .unwrap();
    crate::run_create_table_on(
        "CREATE TABLE orders (o_orderkey INT PRIMARY KEY, o_custkey INT, \
            o_orderdate DATE, o_shippriority INT)",
        &mut catalog,
    )
    .unwrap();
    crate::run_create_table_on(
        "CREATE TABLE lineitem (l_orderkey INT NOT NULL, l_linenumber INT NOT NULL, \
            l_extendedprice DECIMAL(15,2), l_discount DECIMAL(15,2), l_shipdate DATE, \
            PRIMARY KEY (l_orderkey, l_linenumber) CLUSTERED)",
        &mut catalog,
    )
    .unwrap();

    let sql = "SELECT l_orderkey, SUM(l_extendedprice * (1 - l_discount)) AS revenue, \
        o_orderdate, o_shippriority FROM customer, orders, lineitem \
        WHERE c_mktsegment = 'AUTOMOBILE' AND c_custkey = o_custkey \
        AND l_orderkey = o_orderkey AND o_orderdate < '1995-03-13' \
        AND l_shipdate > '1995-03-13' GROUP BY l_orderkey, o_orderdate, o_shippriority \
        ORDER BY revenue DESC, o_orderdate LIMIT 10";
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
    let cell = |row: usize, column: usize| match &rows[row][column] {
        Datum::Bytes(bytes) => String::from_utf8_lossy(bytes).into_owned(),
        other => panic!("EXPLAIN cell is not text: {other:?}"),
    };

    assert_eq!(
        &operators[..3],
        ["Projection", "TopN", "HashAgg"],
        "Go keeps the final SELECT projection above q3's TopN: {rows:#?}",
    );
    let aggregate = operators
        .iter()
        .position(|operator| operator == "HashAgg")
        .expect("q3 has a grouped HashAgg");
    assert_eq!(
        operators.get(aggregate + 1).map(String::as_str),
        Some("Projection"),
        "Go InjectProjBelowAgg evaluates q3's complex SUM argument: {rows:#?}",
    );
    assert_eq!(
        cell(0, 4),
        "test.lineitem.l_orderkey, Column#0, test.orders.o_orderdate, \
         test.orders.o_shippriority"
    );
    assert_eq!(
        cell(1, 4),
        "Column#0:desc, test.orders.o_orderdate, offset:0, count:10"
    );
    assert_eq!(
        cell(2, 4),
        "group by:Column#1, Column#2, Column#3, funcs:sum(Column#0)->Column#0, \
         funcs:firstrow(Column#1)->test.orders.o_orderdate, \
         funcs:firstrow(Column#2)->test.orders.o_shippriority, \
         funcs:firstrow(Column#3)->test.lineitem.l_orderkey"
    );
    assert_eq!(
        cell(3, 4),
        "mul(test.lineitem.l_extendedprice, minus(1, test.lineitem.l_discount))->Column#0, \
         test.orders.o_orderdate->Column#1, test.orders.o_shippriority->Column#2, \
         test.lineitem.l_orderkey->Column#3"
    );
    assert!(
        rows.iter().any(|row| match &row[4] {
            Datum::Bytes(bytes) => String::from_utf8_lossy(bytes)
                .contains("lt(test.orders.o_orderdate, 1995-03-13 00:00:00.000000)",),
            _ => false,
        }),
        "q3's pushed orders predicate must use Go's typed DATE constant: {rows:#?}",
    );
    assert!(
        rows.iter().any(|row| match &row[4] {
            Datum::Bytes(bytes) => String::from_utf8_lossy(bytes)
                .contains("gt(test.lineitem.l_shipdate, 1995-03-13 00:00:00.000000)",),
            _ => false,
        }),
        "q3's pushed lineitem predicate must use Go's typed DATE constant: {rows:#?}",
    );
}

/// TPC-H q13's outer aggregation groups by a column produced by a grouped
/// derived table. Go still lays out physical HashAgg states with COUNT before
/// the selected group-key FIRST_ROW carrier, then restores select-list order
/// with a Projection below Sort.
#[test]
fn tpch_q13_restores_grouped_derived_hash_agg_output() {
    use crate::explain::{explain_select_stmt, ExplainFormat};

    let mut catalog = Catalog::default();
    crate::run_create_table_on(
        "CREATE TABLE customer (c_custkey INT PRIMARY KEY)",
        &mut catalog,
    )
    .unwrap();
    crate::run_create_table_on(
        "CREATE TABLE orders (o_orderkey INT PRIMARY KEY, o_custkey INT, o_comment VARCHAR(79))",
        &mut catalog,
    )
    .unwrap();
    let sql = "SELECT c_count, COUNT(*) AS custdist FROM (\
        SELECT c_custkey, COUNT(o_orderkey) AS c_count FROM customer \
        LEFT JOIN orders ON c_custkey = o_custkey \
        AND o_comment NOT LIKE '%pending%deposits%' GROUP BY c_custkey\
        ) AS c_orders GROUP BY c_count ORDER BY custdist DESC, c_count DESC";
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
    let operator = |row: usize| match &rows[row][0] {
        Datum::Bytes(bytes) => String::from_utf8_lossy(bytes)
            .trim_start_matches(&[' ', '│', '├', '└', '─'][..])
            .to_owned(),
        other => panic!("operator is not text: {other:?}"),
    };
    let info = |row: usize| match &rows[row][4] {
        Datum::Bytes(bytes) => String::from_utf8_lossy(bytes).into_owned(),
        other => panic!("EXPLAIN cell is not text: {other:?}"),
    };

    assert_eq!(
        (0..3).map(operator).collect::<Vec<_>>(),
        ["Sort", "Projection", "HashAgg"],
        "q13 must restore the physical aggregate output before sorting: {rows:#?}",
    );
    assert_eq!(info(0), "Column#1:desc, Column#0:desc");
    assert_eq!(info(1), "Column#0, Column#1");
    let aggregate = info(2);
    assert!(
        !aggregate.contains("c_orders.c_count"),
        "a computed derived output has no base-column identity: {aggregate}",
    );
    let count = aggregate
        .find("funcs:count(1)->Column#")
        .expect("q13 outer COUNT");
    let carrier = aggregate
        .find("funcs:firstrow(Column#0)->Column#0")
        .expect("q13 group-key carrier");
    assert!(
        count < carrier,
        "Go places aggregate states before FIRST_ROW carriers: {aggregate}",
    );
}

/// Go `InjectProjBelowAgg` extracts every scalar aggregate argument, including
/// TPC-H q14's `SUM(CASE ...)`, into a physical Projection below HashAgg.
#[test]
fn scalar_case_aggregate_argument_is_projected_below_hash_agg() {
    use crate::explain::{explain_select_stmt, ExplainFormat};

    let mut catalog = Catalog::default();
    crate::run_create_table_on(
        "CREATE TABLE part (p_partkey INT PRIMARY KEY, p_type VARCHAR(25))",
        &mut catalog,
    )
    .unwrap();
    crate::run_create_table_on(
        "CREATE TABLE lineitem (l_partkey INT, l_extendedprice DECIMAL(15,2), \
            l_discount DECIMAL(15,2), l_shipdate DATE)",
        &mut catalog,
    )
    .unwrap();

    let sql = "SELECT 100.00 * \
        SUM(CASE WHEN p_type LIKE 'PROMO%' \
            THEN l_extendedprice * (1 - l_discount) ELSE 0 END) / \
        SUM(l_extendedprice * (1 - l_discount)) AS promo_revenue \
        FROM lineitem, part WHERE l_partkey = p_partkey \
        AND l_shipdate >= '1996-12-01' \
        AND l_shipdate < DATE_ADD('1996-12-01', INTERVAL 1 MONTH)";
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
    let cell = |row: usize, column: usize| match &rows[row][column] {
        Datum::Bytes(bytes) => String::from_utf8_lossy(bytes).into_owned(),
        other => panic!("EXPLAIN cell is not text: {other:?}"),
    };

    assert_eq!(
        &operators[..3],
        ["Projection", "HashAgg", "Projection"],
        "Go InjectProjBelowAgg must extract q14's scalar CASE argument: {rows:#?}",
    );
    assert!(
        cell(1, 4).contains("funcs:sum(Column#"),
        "HashAgg must consume the projected scalar columns: {rows:#?}",
    );
    assert!(
        cell(2, 4).contains("case("),
        "the injected Projection must evaluate the CASE expression: {rows:#?}",
    );
    let selection = rows
        .iter()
        .position(|row| match &row[4] {
            Datum::Bytes(bytes) => String::from_utf8_lossy(bytes).contains("l_shipdate"),
            _ => false,
        })
        .expect("q14 has a lineitem date Selection");
    assert!(
        !cell(selection, 4).contains("and("),
        "Go PhysicalSelection prints split CNF conditions: {rows:#?}",
    );
    let hash_join = operators
        .iter()
        .position(|operator| operator == "HashJoin")
        .expect("q14 has a HashJoin");
    assert!(
        operators[hash_join + 1] == "Selection(Build)"
            || cell(hash_join + 1, 4) == "data:Selection",
        "Go builds q14's hash table from the smaller filtered lineitem side: {rows:#?}",
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
    assert!(
        info(2).contains("funcs:firstrow(Column#2)->test.lineitem.l_shipmode"),
        "HashAgg must carry the restored group-column identity: {rows:#?}",
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
    catalog.clear_dirty_content();

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
    assert_eq!(
        rows[0][4],
        Datum::Bytes(b"Column#1:desc".to_vec()),
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

/// TPCC condition 01: join pruning renumbers the merge-key column, but the
/// committed MergeJoin still delivers that named order to the grouped
/// StreamAgg. Go projects the three aggregate inputs between the join and the
/// aggregation.
#[test]
fn tpcc_grouped_merge_join_keeps_order_through_pruning() {
    use crate::explain::{explain_select_stmt, ExplainFormat};

    let mut catalog = Catalog::default();
    crate::run_create_table_on(
        "CREATE TABLE district (d_id INT NOT NULL, d_w_id INT NOT NULL, d_ytd DECIMAL(12,2) NOT NULL, \
            PRIMARY KEY (d_w_id,d_id))",
        &mut catalog,
    )
    .unwrap();
    let TableEntry::Kv(district) = catalog.get_mut_in("test", "district").unwrap() else {
        panic!("district is not a KV table");
    };
    district.add_index(crate::kv_table::KvIndex {
        id: 1,
        name: "PRIMARY".to_owned(),
        comment: String::new(),
        unique: true,
        prefix_lengths: vec![crate::ddl::index_prefix::UNSPECIFIED_LENGTH; 2],
        column_offsets: vec![1, 0],
        visible: true,
        global: false,
    });
    crate::run_create_table_on(
        "CREATE TABLE warehouse (w_id INT PRIMARY KEY, w_ytd DECIMAL(12,2) NOT NULL)",
        &mut catalog,
    )
    .unwrap();
    let ctx = crate::StmtContext::for_query();
    run_insert_on(
        "INSERT INTO district VALUES (1,1,10),(2,1,20),(1,2,40)",
        &mut catalog,
        &ctx,
    )
    .unwrap();
    run_insert_on(
        "INSERT INTO warehouse VALUES (1,5),(2,7)",
        &mut catalog,
        &ctx,
    )
    .unwrap();

    let sql = "SELECT SUM(d_ytd)-MAX(w_ytd) diff FROM district, warehouse \
        WHERE d_w_id=w_id AND w_id=1 GROUP BY d_w_id";
    let result = run_select_on(sql, &catalog, &ctx).unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].len(), 1);
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
            "  └─Projection",
            "    └─MergeJoin",
            "      ├─TableReader(Build)",
            "      │ └─TableRangeScan",
            "      └─Point_Get(Probe)",
        ],
    );
    assert!(cell(1, 4).contains("group by:test.district.d_w_id"));
    assert!(cell(2, 4).contains("test.district.d_ytd"));
}

/// TPCC condition 03: a constant fixes the leading common-handle column, so
/// the remaining `(district, order)` record order supplies the grouped stream
/// directly. Go pushes the partial StreamAgg below a TableReader and keeps
/// only the scalar arithmetic in the root Projection.
#[test]
fn tpcc_grouped_common_handle_uses_partial_and_final_stream_agg() {
    use crate::explain::{explain_select_stmt, ExplainFormat};

    let mut catalog = Catalog::default();
    crate::run_create_table_on(
        "CREATE TABLE new_order (\
            no_o_id INT NOT NULL, no_d_id INT NOT NULL, no_w_id INT NOT NULL,\
            PRIMARY KEY (no_w_id,no_d_id,no_o_id))",
        &mut catalog,
    )
    .unwrap();
    let TableEntry::Kv(table) = catalog.get_mut_in("test", "new_order").unwrap() else {
        panic!("new_order is not a KV table");
    };
    table.add_index(crate::kv_table::KvIndex {
        id: 1,
        name: "PRIMARY".to_owned(),
        comment: String::new(),
        unique: true,
        prefix_lengths: vec![crate::ddl::index_prefix::UNSPECIFIED_LENGTH; 3],
        column_offsets: vec![2, 1, 0],
        visible: true,
        global: false,
    });
    let ctx = crate::StmtContext::for_query();
    run_insert_on(
        "INSERT INTO new_order VALUES (1,1,1),(2,1,1),(5,2,1),(7,2,1),(9,1,2)",
        &mut catalog,
        &ctx,
    )
    .unwrap();

    let sql = "SELECT MAX(no_o_id)-MIN(no_o_id)+1-COUNT(*) diff \
        FROM new_order WHERE no_w_id=1 GROUP BY no_d_id";
    assert_eq!(
        run_select_on(sql, &catalog, &ctx).unwrap(),
        vec![vec![Datum::Int(0)], vec![Datum::Int(1)]],
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
    assert_eq!(
        (0..rows.len()).map(|row| cell(row, 2)).collect::<Vec<_>>(),
        vec!["root", "root", "root", "cop[tikv]", "cop[tikv]"],
    );
    assert!(cell(0, 4).starts_with("minus(plus(minus(Column#"));
    assert!(cell(1, 4).contains("group by:test.new_order.no_d_id"));
    assert!(cell(3, 4).contains("funcs:max(test.new_order.no_o_id)->Column#"));
    assert!(cell(4, 4).contains("range:[1,1], keep order:true"));
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
    table.add_index(crate::kv_table::KvIndex {
        id: 1,
        name: "PRIMARY".to_owned(),
        comment: String::new(),
        unique: true,
        prefix_lengths: vec![crate::ddl::index_prefix::UNSPECIFIED_LENGTH; 4],
        column_offsets: vec![2, 1, 0, 3],
        visible: true,
        global: false,
    });
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

/// TPCC condition 04: the selected group key order survives materializing the
/// inner aggregate and lets both the join and the aggregate above it stream.
#[test]
fn tpcc_condition_four_streams_across_a_grouped_derived_table() {
    use crate::explain::{explain_select_stmt, ExplainFormat};

    let mut catalog = Catalog::default();
    crate::run_create_table_on(
        "CREATE TABLE orders (o_id INT NOT NULL, o_d_id INT NOT NULL, o_w_id INT NOT NULL, \
            o_ol_cnt INT NOT NULL, PRIMARY KEY (o_w_id,o_d_id,o_id) CLUSTERED)",
        &mut catalog,
    )
    .unwrap();
    let TableEntry::Kv(orders) = catalog.get_mut_in("test", "orders").unwrap() else {
        panic!("orders is not a KV table");
    };
    orders.add_index(crate::kv_table::KvIndex {
        id: 1,
        name: "PRIMARY".to_owned(),
        comment: String::new(),
        unique: true,
        prefix_lengths: vec![crate::ddl::index_prefix::UNSPECIFIED_LENGTH; 3],
        column_offsets: vec![2, 1, 0],
        visible: true,
        global: false,
    });
    crate::run_create_table_on(
        "CREATE TABLE order_line (ol_o_id INT NOT NULL, ol_d_id INT NOT NULL, \
            ol_w_id INT NOT NULL, ol_number INT NOT NULL, \
            PRIMARY KEY (ol_w_id,ol_d_id,ol_o_id,ol_number) CLUSTERED)",
        &mut catalog,
    )
    .unwrap();
    let TableEntry::Kv(order_line) = catalog.get_mut_in("test", "order_line").unwrap() else {
        panic!("order_line is not a KV table");
    };
    order_line.add_index(crate::kv_table::KvIndex {
        id: 2,
        name: "PRIMARY".to_owned(),
        comment: String::new(),
        unique: true,
        prefix_lengths: vec![crate::ddl::index_prefix::UNSPECIFIED_LENGTH; 4],
        column_offsets: vec![2, 1, 0, 3],
        visible: true,
        global: false,
    });
    let ctx = crate::StmtContext::for_query();
    run_insert_on(
        "INSERT INTO orders VALUES (1,1,1,2),(2,2,1,1)",
        &mut catalog,
        &ctx,
    )
    .unwrap();
    run_insert_on(
        "INSERT INTO order_line VALUES (1,1,1,1),(1,1,1,2),(2,2,1,1)",
        &mut catalog,
        &ctx,
    )
    .unwrap();

    let sql = "SELECT COUNT(*) FROM (SELECT o_d_id, SUM(o_ol_cnt) sm1, MAX(cn) cn \
        FROM orders, (SELECT ol_d_id, COUNT(*) cn FROM order_line \
        WHERE ol_w_id=1 GROUP BY ol_d_id) ol WHERE o_w_id=1 AND ol_d_id=o_d_id \
        GROUP BY o_d_id) t1 WHERE sm1<>cn";
    assert_eq!(
        run_select_on(sql, &catalog, &ctx).unwrap(),
        vec![vec![Datum::Int(0)]],
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
            "└─Selection",
            "  └─StreamAgg",
            "    └─Projection",
            "      └─MergeJoin",
            "        ├─StreamAgg(Build)",
            "        │ └─TableReader",
            "        │   └─StreamAgg",
            "        │     └─TableRangeScan",
            "        └─TableReader(Probe)",
            "          └─TableRangeScan",
        ],
    );
    assert!(
        cell(3, 4).starts_with("cast(test.orders.o_ol_cnt, decimal(10,0) BINARY)"),
        "{}",
        cell(3, 4)
    );
    assert!(cell(3, 4).ends_with("test.orders.o_d_id->Column#2"));
    assert_eq!(
        cell(1, 4),
        "ne(Column#0, cast(Column#1, decimal(20,0) BINARY))"
    );
    assert_eq!(
        cell(2, 4),
        "group by:Column#2, funcs:sum(Column#0)->Column#0, funcs:max(Column#1)->Column#1"
    );
    assert!(!cell(2, 4).contains("firstrow"));
    assert!(cell(4, 4).contains("right key:test.order_line.ol_d_id"));

    // With the ten-warehouse cardinalities Go switches the middle pipeline
    // to HashAgg + IndexHashJoin, but retains the injected SUM cast and the
    // comparison cast above it.
    scale_analyzed_tpcc_table(
        &mut catalog,
        "orders",
        300_000,
        &[
            ("o_id", 3_000),
            ("o_d_id", 10),
            ("o_w_id", 10),
            ("o_ol_cnt", 15),
        ],
        &ctx,
    );
    scale_analyzed_tpcc_table(
        &mut catalog,
        "order_line",
        4_075_321,
        &[
            ("ol_o_id", 3_000),
            ("ol_d_id", 10),
            ("ol_w_id", 10),
            ("ol_number", 15),
        ],
        &ctx,
    );
    catalog.clear_dirty_content();
    let (_, analyzed) =
        explain_select_stmt(select, &catalog, "test", &ctx, ExplainFormat::Brief).unwrap();
    let analyzed_cell = |row: usize, column: usize| match &analyzed[row][column] {
        Datum::Bytes(bytes) => String::from_utf8_lossy(bytes).into_owned(),
        other => format!("{other:?}"),
    };
    let analyzed_operators = (0..analyzed.len())
        .map(|row| analyzed_cell(row, 0))
        .collect::<Vec<_>>();
    assert_eq!(
        analyzed_operators,
        vec![
            "StreamAgg",
            "└─Selection",
            "  └─HashAgg",
            "    └─Projection",
            "      └─IndexHashJoin",
            "        ├─HashAgg(Build)",
            "        │ └─TableReader",
            "        │   └─HashAgg",
            "        │     └─TableRangeScan",
            "        └─TableReader(Probe)",
            "          └─Selection",
            "            └─TableRangeScan",
        ],
        "{analyzed:#?}",
    );
    assert!(
        analyzed_cell(3, 4).starts_with("cast(test.orders.o_ol_cnt, decimal(10,0) BINARY)"),
        "{analyzed:#?}"
    );
    assert!(
        analyzed_cell(4, 4).contains("equal cond:eq(test.order_line.ol_d_id, test.orders.o_d_id)"),
        "IndexJoin equality must be rendered outer-first: {analyzed:#?}"
    );
    assert_eq!(
        analyzed_cell(1, 4),
        "ne(Column#0, cast(Column#1, decimal(20,0) BINARY))"
    );
    assert!(
        analyzed_cell(11, 1).parse::<f64>().unwrap() < 300_000.0,
        "the dynamic orders probe must not keep the full-table estimate: {analyzed:#?}"
    );
}

/// TPCC condition 06: the predicate above a pass-through derived projection
/// rejects the LEFT JOIN's NULL row, so Go simplifies it to an inner merge
/// join. The fixed warehouse key then reaches both common-handle scans, while
/// the nullable left comparison operand becomes a coprocessor IS NOT NULL
/// filter.
#[test]
fn tpcc_condition_six_simplifies_and_pushes_through_derived_tables() {
    use crate::explain::{explain_select_stmt, ExplainFormat};

    let mut catalog = Catalog::default();
    crate::run_create_table_on(
        "CREATE TABLE orders (o_id INT NOT NULL, o_d_id INT NOT NULL, o_w_id INT NOT NULL, \
            o_ol_cnt INT, PRIMARY KEY (o_w_id,o_d_id,o_id))",
        &mut catalog,
    )
    .unwrap();
    let TableEntry::Kv(orders) = catalog.get_mut_in("test", "orders").unwrap() else {
        panic!("orders is not a KV table");
    };
    orders.add_index(crate::kv_table::KvIndex {
        id: 1,
        name: "PRIMARY".to_owned(),
        comment: String::new(),
        unique: true,
        prefix_lengths: vec![crate::ddl::index_prefix::UNSPECIFIED_LENGTH; 3],
        column_offsets: vec![2, 1, 0],
        visible: true,
        global: false,
    });
    crate::run_create_table_on(
        "CREATE TABLE order_line (ol_o_id INT NOT NULL, ol_d_id INT NOT NULL, \
            ol_w_id INT NOT NULL, ol_number INT NOT NULL, \
            PRIMARY KEY (ol_w_id,ol_d_id,ol_o_id,ol_number))",
        &mut catalog,
    )
    .unwrap();
    let TableEntry::Kv(order_line) = catalog.get_mut_in("test", "order_line").unwrap() else {
        panic!("order_line is not a KV table");
    };
    order_line.add_index(crate::kv_table::KvIndex {
        id: 2,
        name: "PRIMARY".to_owned(),
        comment: String::new(),
        unique: true,
        prefix_lengths: vec![crate::ddl::index_prefix::UNSPECIFIED_LENGTH; 4],
        column_offsets: vec![2, 1, 0, 3],
        visible: true,
        global: false,
    });

    let ctx = crate::StmtContext::for_query();
    run_insert_on(
        "INSERT INTO orders VALUES (1,1,1,2),(2,1,1,2),(3,2,1,NULL),(4,1,2,1)",
        &mut catalog,
        &ctx,
    )
    .unwrap();
    run_insert_on(
        "INSERT INTO order_line VALUES \
            (1,1,1,1),(1,1,1,2),(2,1,1,1),(3,2,1,1),(4,1,2,1)",
        &mut catalog,
        &ctx,
    )
    .unwrap();

    let sql = "SELECT COUNT(*) FROM \
        (SELECT o_ol_cnt, order_line_count FROM orders LEFT JOIN \
          (SELECT ol_w_id, ol_d_id, ol_o_id, count(*) order_line_count \
           FROM order_line GROUP BY ol_w_id, ol_d_id, ol_o_id \
           ORDER BY ol_w_id, ol_d_id, ol_o_id) AS order_line \
         ON orders.o_w_id=order_line.ol_w_id \
         AND orders.o_d_id=order_line.ol_d_id \
         AND orders.o_id=order_line.ol_o_id \
         WHERE orders.o_w_id=1) AS T \
        WHERE T.o_ol_cnt != T.order_line_count";
    assert_eq!(
        run_select_on(sql, &catalog, &ctx).unwrap(),
        vec![vec![Datum::Int(1)]],
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
            "└─MergeJoin",
            "  ├─StreamAgg(Build)",
            "  │ └─TableReader",
            "  │   └─StreamAgg",
            "  │     └─TableRangeScan",
            "  └─TableReader(Probe)",
            "    └─Selection",
            "      └─TableRangeScan",
        ],
    );
    assert_eq!(cell(1, 1), "9.99", "selection estimate={}", cell(7, 1));
    assert!(cell(1, 4).contains("inner join"), "{}", cell(1, 4));
    assert!(cell(1, 4).contains("other cond:ne("), "{}", cell(1, 4));
    assert!(cell(1, 4).contains(", Column#"), "{}", cell(1, 4));
    assert!(
        cell(2, 4).starts_with(
            "group by:test.order_line.ol_d_id, test.order_line.ol_o_id, \
             test.order_line.ol_w_id, funcs:count(Column#"
        ),
        "{}",
        cell(2, 4)
    );
    assert!(
        cell(2, 4).contains(
            "funcs:firstrow(test.order_line.ol_o_id)->test.order_line.ol_o_id, \
             funcs:firstrow(test.order_line.ol_d_id)->test.order_line.ol_d_id, \
             funcs:firstrow(test.order_line.ol_w_id)->test.order_line.ol_w_id"
        ),
        "{}",
        cell(2, 4)
    );
    assert!(
        cell(4, 4).starts_with(
            "group by:test.order_line.ol_d_id, test.order_line.ol_o_id, \
             test.order_line.ol_w_id, funcs:count(1)->Column#"
        ),
        "{}",
        cell(4, 4)
    );
    assert!(cell(5, 4).contains("range:[1,1]"), "{}", cell(5, 4));
    assert!(cell(5, 4).contains("keep order:true"), "{}", cell(5, 4));
    assert!(cell(7, 4).contains("not(isnull("), "{}", cell(7, 4));
    assert_eq!(cell(7, 1), "9.99");
    assert!(cell(8, 4).contains("range:[1,1]"), "{}", cell(8, 4));
    assert!(cell(8, 4).contains("keep order:true"), "{}", cell(8, 4));

    // The primary key has four columns, so Go cannot use its GroupNDV for
    // this three-column GROUP BY. It falls back to the largest analyzed
    // column NDV (ol_o_id), scaled by the propagated warehouse predicate.
    scale_analyzed_tpcc_table(
        &mut catalog,
        "orders",
        299_696,
        &[
            ("o_id", 3_000),
            ("o_d_id", 10),
            ("o_w_id", 10),
            ("o_ol_cnt", 15),
        ],
        &ctx,
    );
    scale_analyzed_tpcc_table(
        &mut catalog,
        "order_line",
        3_007_443,
        &[
            ("ol_o_id", 3_000),
            ("ol_d_id", 10),
            ("ol_w_id", 10),
            ("ol_number", 15),
        ],
        &ctx,
    );
    catalog.clear_dirty_content();

    let (_, analyzed) =
        explain_select_stmt(select, &catalog, "test", &ctx, ExplainFormat::Brief).unwrap();
    let analyzed_cell = |row: usize, column: usize| match &analyzed[row][column] {
        Datum::Bytes(bytes) => String::from_utf8_lossy(bytes).into_owned(),
        other => format!("{other:?}"),
    };
    assert_eq!(
        (0..analyzed.len())
            .map(|row| analyzed_cell(row, 0))
            .collect::<Vec<_>>(),
        vec![
            "HashAgg",
            "└─IndexJoin",
            "  ├─HashAgg(Build)",
            "  │ └─TableReader",
            "  │   └─HashAgg",
            "  │     └─TableRangeScan",
            "  └─TableReader(Probe)",
            "    └─Selection",
            "      └─TableRangeScan",
        ],
        "{analyzed:#?}",
    );
    assert!(
        analyzed_cell(2, 1).parse::<f64>().unwrap() < 1_000.0,
        "the three-column group must use analyzed column NDVs: {analyzed:#?}"
    );
    assert!(
        analyzed_cell(1, 4).contains(
            "outer key:test.order_line.ol_d_id, test.order_line.ol_o_id, \
             test.order_line.ol_w_id, inner key:test.orders.o_d_id, \
             test.orders.o_id, test.orders.o_w_id"
        ),
        "IndexJoin must print only access keys in logical equality order: {analyzed:#?}"
    );
    assert_eq!(analyzed_cell(2, 1), analyzed_cell(8, 1), "{analyzed:#?}");
    let probe_rows = analyzed_cell(8, 1).parse::<f64>().unwrap();
    let filtered_rows = analyzed_cell(7, 1).parse::<f64>().unwrap();
    assert!(filtered_rows <= probe_rows, "{analyzed:#?}");
    assert!(
        analyzed_cell(5, 4).contains("keep order:false"),
        "an unordered IndexJoin outer child must be replanned without order: {analyzed:#?}"
    );
}

/// TPCC condition 08: the fixed warehouse row is the ordered outer side of
/// an IndexHashJoin into history's secondary index. The grouped SUM carries
/// `w_ytd` through FIRST_ROW because the derived table's outer predicate
/// compares that value with the aggregate result.
#[test]
fn tpcc_condition_eight_uses_index_join_and_carries_warehouse_ytd() {
    use crate::explain::{explain_select_stmt, ExplainFormat};

    let mut catalog = Catalog::default();
    crate::run_create_table_on(
        "CREATE TABLE warehouse (w_id INT PRIMARY KEY, w_ytd DECIMAL(12,2) NOT NULL)",
        &mut catalog,
    )
    .unwrap();
    crate::run_create_table_on(
        "CREATE TABLE history (h_w_id INT NOT NULL, h_amount DECIMAL(6,2) NOT NULL, \
            KEY idx_h_w_id(h_w_id))",
        &mut catalog,
    )
    .unwrap();
    let ctx = crate::StmtContext::for_query();
    run_insert_on(
        "INSERT INTO warehouse VALUES (1,10.00),(2,20.00)",
        &mut catalog,
        &ctx,
    )
    .unwrap();
    run_insert_on(
        "INSERT INTO history VALUES (1,3.00),(1,4.00),(2,20.00)",
        &mut catalog,
        &ctx,
    )
    .unwrap();

    let sql = "SELECT count(*) cn FROM \
        (SELECT w_id,w_ytd,SUM(h_amount) sm FROM history,warehouse \
         WHERE h_w_id=w_id and w_id=1 GROUP BY w_id) t1 WHERE w_ytd<>sm";
    assert_eq!(
        run_select_on(sql, &catalog, &ctx).unwrap(),
        vec![vec![Datum::Int(1)]],
    );

    crate::driver::join_search::ANSWERS.with(|answers| answers.borrow_mut().clear());
    let stmt = tidb_parser::parse(sql).unwrap();
    let Stmt::Query(query) = &stmt else {
        panic!("not a query");
    };
    let QueryStmt::Select(select) = &**query else {
        panic!("not a SELECT");
    };
    let (_, rows) =
        explain_select_stmt(select, &catalog, "test", &ctx, ExplainFormat::Brief).unwrap();
    let answers = crate::driver::join_search::ANSWERS.with(|answers| answers.borrow().clone());
    assert!(!answers.is_empty(), "the join search was not consulted");
    assert!(
        answers.iter().all(|answer| {
            answer.chosen == crate::driver::join_search::Chosen::IndexForSingleOuterRow
                && answer == &answers[0]
        }),
        "rebuilt candidates must make the same join decision: {answers:#?}"
    );
    let cell = |row: usize, column: usize| match &rows[row][column] {
        Datum::Bytes(bytes) => String::from_utf8_lossy(bytes).into_owned(),
        other => format!("{other:?}"),
    };
    assert_eq!(
        (0..rows.len()).map(|row| cell(row, 0)).collect::<Vec<_>>(),
        vec![
            "StreamAgg",
            "└─Selection",
            "  └─StreamAgg",
            "    └─Projection",
            "      └─IndexHashJoin",
            "        ├─Point_Get(Build)",
            "        └─IndexLookUp(Probe)",
            "          ├─Selection(Build)",
            "          │ └─IndexRangeScan",
            "          └─TableRowIDScan(Probe)",
        ],
        "{rows:#?}",
    );
    assert_eq!(
        cell(1, 4),
        "ne(test.warehouse.w_ytd, Column#1)",
        "{rows:#?}"
    );
    for row in [3, 4, 6, 7, 9] {
        assert_eq!(cell(row, 1), "1.25", "{rows:#?}");
    }
    assert_eq!(cell(8, 1), "1250.00", "{rows:#?}");
    assert_eq!(
        cell(3, 4),
        "test.history.h_amount, test.warehouse.w_id, test.warehouse.w_ytd"
    );
    assert!(cell(2, 4).contains("funcs:sum(test.history.h_amount)->Column#"));
    assert!(
        cell(2, 4).contains("funcs:firstrow(test.warehouse.w_ytd)->test.warehouse.w_ytd"),
        "{}",
        cell(2, 4)
    );
    assert!(cell(4, 4).contains("outer key:test.warehouse.w_id"));
    assert!(cell(4, 4).contains("inner key:test.history.h_w_id"));
    assert!(cell(5, 3).contains("table:warehouse"));
    assert!(cell(8, 4).contains("range: decided by"));

    scale_analyzed_tpcc_table(
        &mut catalog,
        "warehouse",
        10,
        &[("w_id", 10), ("w_ytd", 10)],
        &ctx,
    );
    scale_analyzed_tpcc_table(
        &mut catalog,
        "history",
        300_000,
        &[("h_w_id", 10), ("h_amount", 30_000)],
        &ctx,
    );
    catalog.clear_dirty_content();
    let (_, analyzed) =
        explain_select_stmt(select, &catalog, "test", &ctx, ExplainFormat::Brief).unwrap();
    let analyzed_cell = |row: usize, column: usize| match &analyzed[row][column] {
        Datum::Bytes(bytes) => String::from_utf8_lossy(bytes).into_owned(),
        other => format!("{other:?}"),
    };
    assert_eq!(
        (0..analyzed.len())
            .map(|row| analyzed_cell(row, 0))
            .collect::<Vec<_>>(),
        vec![
            "StreamAgg",
            "└─Selection",
            "  └─HashAgg",
            "    └─Projection",
            "      └─IndexHashJoin",
            "        ├─Point_Get(Build)",
            "        └─IndexLookUp(Probe)",
            "          ├─Selection(Build)",
            "          │ └─IndexRangeScan",
            "          └─TableRowIDScan(Probe)",
        ],
        "{analyzed:#?}",
    );
    assert_eq!(
        analyzed_cell(3, 4),
        "test.history.h_amount, test.warehouse.w_id, test.warehouse.w_ytd"
    );
    let aggregate = analyzed_cell(2, 4);
    assert!(
        aggregate.starts_with("group by:test.warehouse.w_id"),
        "HashAgg must resolve group columns against its projection input: {analyzed:#?}"
    );
    assert!(
        aggregate.contains("funcs:sum(test.history.h_amount)->Column#0"),
        "HashAgg must resolve SUM against its projection input: {analyzed:#?}"
    );
    assert!(
        aggregate.contains("funcs:firstrow(test.warehouse.w_ytd)->test.warehouse.w_ytd"),
        "HashAgg must resolve FIRST_ROW against its projection input: {analyzed:#?}"
    );
    assert!(
        aggregate.find("funcs:sum(").unwrap() < aggregate.find("funcs:firstrow(").unwrap(),
        "Go orders real aggregate states before FIRST_ROW carriers: {aggregate}"
    );
}

/// TPCC condition 09 starts with a grouped district relation whose two group
/// keys are the complete clustered primary key. Every group therefore holds
/// at most one row, so Go eliminates the aggregation and widens the nullable
/// DECIMAL `SUM` argument with a real projection cast.
#[test]
fn tpcc_condition_nine_eliminates_the_unique_district_aggregation() {
    use crate::explain::{explain_select_stmt, ExplainFormat};

    let mut catalog = Catalog::default();
    crate::run_create_table_on(
        "CREATE TABLE district (d_id INT NOT NULL, d_w_id INT NOT NULL, \
            d_ytd DECIMAL(12,2), PRIMARY KEY (d_w_id,d_id))",
        &mut catalog,
    )
    .unwrap();
    let TableEntry::Kv(district) = catalog.get_mut_in("test", "district").unwrap() else {
        panic!("district is not a KV table");
    };
    district.add_index(crate::kv_table::KvIndex {
        id: 1,
        name: "PRIMARY".to_owned(),
        comment: String::new(),
        unique: true,
        prefix_lengths: vec![crate::ddl::index_prefix::UNSPECIFIED_LENGTH; 2],
        column_offsets: vec![1, 0],
        visible: true,
        global: false,
    });
    let ctx = crate::StmtContext::for_query();
    run_insert_on(
        "INSERT INTO district VALUES (1,1,10.25),(2,1,NULL),(1,2,20.50)",
        &mut catalog,
        &ctx,
    )
    .unwrap();

    let sql = "SELECT d_id,d_w_id,SUM(d_ytd) s1 FROM district \
        WHERE d_w_id=1 GROUP BY d_id,d_w_id";
    let (columns, rows) = crate::run_select_meta_on(sql, &catalog, &ctx).unwrap();
    assert_eq!(rows.len(), 2, "{rows:#?}");
    assert!(rows.iter().any(|row| matches!(row[2], Datum::Null)));
    assert_eq!(columns[2].1.code(), FieldTypeCode::NewDecimal);
    assert_eq!(columns[2].1.flen(), 34);
    assert_eq!(columns[2].1.decimal(), 2);

    let stmt = tidb_parser::parse(sql).unwrap();
    let Stmt::Query(query) = &stmt else {
        panic!("not a query");
    };
    let QueryStmt::Select(select) = &**query else {
        panic!("not a SELECT");
    };
    let (_, plan) =
        explain_select_stmt(select, &catalog, "test", &ctx, ExplainFormat::Brief).unwrap();
    let cell = |row: usize, column: usize| match &plan[row][column] {
        Datum::Bytes(bytes) => String::from_utf8_lossy(bytes).into_owned(),
        other => format!("{other:?}"),
    };
    assert_eq!(
        (0..plan.len()).map(|row| cell(row, 0)).collect::<Vec<_>>(),
        vec!["Projection", "└─TableReader", "  └─TableRangeScan"],
        "{plan:#?}",
    );
    assert_eq!(
        cell(0, 4),
        "test.district.d_id, test.district.d_w_id, \
         cast(test.district.d_ytd, decimal(34,2) BINARY)->Column#2"
    );
    assert_eq!(cell(1, 4), "data:TableRangeScan");
    assert!(cell(2, 4).contains("range:[1,1]"), "{}", cell(2, 4));
    assert!(cell(2, 4).contains("keep order:false"), "{}", cell(2, 4));
}

/// The other half of TPCC condition 09 is a grouped `history` derived table.
/// Go retains that aggregation while rebuilding its indexed base-table read
/// for each district probe.  The lookup path must therefore aggregate the
/// fetched base rows before matching the derived outputs, including NULL SUM
/// behavior, rather than joining raw history rows.
#[test]
fn tpcc_condition_nine_rebuilds_grouped_history_over_index_lookup() {
    use crate::explain::{explain_select_stmt, ExplainFormat};

    let mut catalog = Catalog::default();
    crate::run_create_table_on(
        "CREATE TABLE district (d_id INT NOT NULL, d_w_id INT NOT NULL, \
            d_name VARCHAR(10), d_street_1 VARCHAR(20), d_street_2 VARCHAR(20), \
            d_city VARCHAR(20), d_state CHAR(2), d_zip CHAR(9), d_tax DECIMAL(4,4), \
            d_ytd DECIMAL(12,2), d_next_o_id INT, \
            PRIMARY KEY (d_w_id,d_id) CLUSTERED)",
        &mut catalog,
    )
    .unwrap();
    let TableEntry::Kv(district) = catalog.get_mut_in("test", "district").unwrap() else {
        panic!("district is not a KV table");
    };
    district.add_index(crate::kv_table::KvIndex {
        id: 1,
        name: "PRIMARY".to_owned(),
        comment: String::new(),
        unique: true,
        prefix_lengths: vec![crate::ddl::index_prefix::UNSPECIFIED_LENGTH; 2],
        column_offsets: vec![1, 0],
        visible: true,
        global: false,
    });
    crate::run_create_table_on(
        "CREATE TABLE history (h_c_id INT NOT NULL, h_c_d_id INT NOT NULL, \
            h_c_w_id INT NOT NULL, h_d_id INT NOT NULL, h_w_id INT NOT NULL, \
            h_date DATETIME, h_amount DECIMAL(6,2), h_data VARCHAR(24), \
            KEY idx_h_w_id(h_w_id), KEY idx_h_c_w_id(h_c_w_id))",
        &mut catalog,
    )
    .unwrap();
    let ctx = crate::StmtContext::for_query();
    run_insert_on(
        "INSERT INTO district(d_id,d_w_id,d_ytd) VALUES \
            (1,1,10.00),(2,1,20.00),(1,2,30.00)",
        &mut catalog,
        &ctx,
    )
    .unwrap();
    run_insert_on(
        "INSERT INTO history(h_c_id,h_c_d_id,h_c_w_id,h_d_id,h_w_id,h_amount) VALUES \
            (1,1,1,1,1,4.00),(2,1,1,1,1,6.00),(3,1,1,1,1,NULL), \
            (4,2,1,2,1,8.00),(5,1,2,1,2,30.00)",
        &mut catalog,
        &ctx,
    )
    .unwrap();

    let sql = "SELECT COUNT(*) FROM \
        (SELECT d_id,d_w_id,SUM(d_ytd) s1 FROM district \
         GROUP BY d_id,d_w_id) d, \
        (SELECT h_d_id,h_w_id,SUM(h_amount) s2 FROM history \
         WHERE h_w_id=1 GROUP BY h_d_id,h_w_id) h \
        WHERE h_d_id=d_id AND d_w_id=h_w_id AND d_w_id=1 AND s1<>s2";
    assert_eq!(
        run_select_on(sql, &catalog, &ctx).unwrap(),
        vec![vec![Datum::Int(1)]],
    );

    let stmt = tidb_parser::parse(sql).unwrap();
    let Stmt::Query(query) = &stmt else {
        panic!("not a query");
    };
    let QueryStmt::Select(select) = &**query else {
        panic!("not a SELECT");
    };
    let (_, plan) =
        explain_select_stmt(select, &catalog, "test", &ctx, ExplainFormat::Brief).unwrap();
    let cell = |row: usize, column: usize| match &plan[row][column] {
        Datum::Bytes(bytes) => String::from_utf8_lossy(bytes).into_owned(),
        other => format!("{other:?}"),
    };
    let operators = plan
        .iter()
        .map(|row| match &row[0] {
            Datum::Bytes(bytes) => String::from_utf8_lossy(bytes).into_owned(),
            other => format!("{other:?}"),
        })
        .collect::<Vec<_>>();
    assert_eq!(
        operators,
        vec![
            "StreamAgg",
            "└─IndexJoin",
            "  ├─Projection(Build)",
            "  │ └─TableReader",
            "  │   └─Selection",
            "  │     └─TableRangeScan",
            "  └─Selection(Probe)",
            "    └─HashAgg",
            "      └─IndexLookUp",
            "        ├─Selection(Build)",
            "        │ └─IndexRangeScan",
            "        └─HashAgg(Probe)",
            "          └─TableRowIDScan",
        ],
        "{plan:#?}",
    );
    assert_eq!(
        cell(4, 4),
        "not(isnull(cast(test.district.d_ytd, decimal(34,2) BINARY)))"
    );
    let answers = crate::driver::join_search::ANSWERS.with(|answers| answers.borrow().clone());
    assert!(
        operators
            .iter()
            .any(|operator| operator.contains("IndexJoin")),
        "{plan:#?}\n{answers:#?}",
    );

    scale_analyzed_tpcc_table(
        &mut catalog,
        "district",
        100,
        &[("d_id", 10), ("d_w_id", 10), ("d_ytd", 100)],
        &ctx,
    );
    scale_analyzed_tpcc_table(
        &mut catalog,
        "history",
        300_000,
        &[("h_d_id", 10), ("h_w_id", 10), ("h_amount", 30_000)],
        &ctx,
    );
    // The live ten-warehouse load has five rows newer than the selected
    // index/column statistics. Go scales the single-column NDV by this
    // analyzed/realtime ratio before deriving the per-probe access floor.
    let (history_id, h_w_id, h_w_id_index) = {
        let TableEntry::Kv(history) = catalog.get_in("test", "history").unwrap() else {
            panic!("history is not a KV table");
        };
        (
            history.table_id,
            history
                .visible_columns()
                .iter()
                .find(|column| column.name == "h_w_id")
                .expect("history.h_w_id")
                .id,
            history
                .indexes()
                .iter()
                .find(|index| index.name == "idx_h_w_id")
                .expect("history.idx_h_w_id")
                .id,
        )
    };
    let mut history_stats = catalog
        .table_statistics(history_id)
        .expect("history statistics")
        .as_ref()
        .clone();
    let preserve_distribution_at_analyzed_count = |histogram: &mut tidb_stats::Histogram| {
        for bucket in &mut histogram.buckets {
            bucket.count = ((bucket.count as f64) * 299_995.0 / 300_000.0).round() as i64;
            bucket.repeat = ((bucket.repeat as f64) * 299_995.0 / 300_000.0).round() as i64;
        }
        histogram.null_count =
            ((histogram.null_count as f64) * 299_995.0 / 300_000.0).round() as i64;
    };
    preserve_distribution_at_analyzed_count(
        &mut history_stats
            .columns
            .get_mut(&h_w_id)
            .expect("history.h_w_id statistics")
            .histogram,
    );
    preserve_distribution_at_analyzed_count(
        &mut history_stats
            .indexes
            .get_mut(&h_w_id_index)
            .expect("history.idx_h_w_id statistics")
            .histogram,
    );
    assert_eq!(history_stats.columns[&h_w_id].total_row_count(), 299_995.0);
    assert_eq!(
        history_stats.indexes[&h_w_id_index].total_row_count(),
        299_995.0
    );
    catalog.set_table_statistics(history_id, std::sync::Arc::new(history_stats));
    catalog.clear_dirty_content();
    let (_, analyzed) =
        explain_select_stmt(select, &catalog, "test", &ctx, ExplainFormat::Brief).unwrap();
    let analyzed_cell = |row: usize, column: usize| match &analyzed[row][column] {
        Datum::Bytes(bytes) => String::from_utf8_lossy(bytes).into_owned(),
        other => format!("{other:?}"),
    };
    let analyzed_operators = (0..analyzed.len())
        .map(|row| analyzed_cell(row, 0))
        .collect::<Vec<_>>();
    assert_eq!(analyzed_operators[1], "└─IndexHashJoin", "{analyzed:#?}");
    let inner = analyzed_operators
        .iter()
        .position(|operator| operator.contains("Selection(Probe)"))
        .expect("grouped IndexJoin inner selection");
    assert_eq!(
        &analyzed_operators[inner..],
        &[
            "  └─Selection(Probe)",
            "    └─HashAgg",
            "      └─IndexLookUp",
            "        ├─Selection(Build)",
            "        │ └─IndexRangeScan",
            "        └─HashAgg(Probe)",
            "          └─TableRowIDScan",
        ],
        "{analyzed:#?}",
    );
    let grouped = analyzed_cell(inner + 1, 4);
    let sum = grouped.find("funcs:sum(").expect("history SUM state");
    let first_row = grouped
        .find("funcs:firstrow(")
        .expect("history FIRST_ROW state");
    assert!(
        grouped.contains("funcs:sum(Column#0)->Column#0"),
        "Go's root aggregate consumes the cop partial SUM: {grouped}"
    );
    assert!(
        sum < first_row,
        "Go places aggregate states before group carriers: {grouped}"
    );
    assert_eq!(analyzed_cell(1, 1), "0.80", "{analyzed:#?}");
    assert_eq!(analyzed_cell(2, 1), "8.00", "{analyzed:#?}");
    assert_eq!(analyzed_cell(4, 1), "8.00", "{analyzed:#?}");
    assert_eq!(analyzed_cell(inner, 1), "24000.00", "{analyzed:#?}");
    assert_eq!(analyzed_cell(inner + 1, 1), "24000.00", "{analyzed:#?}");
    assert_eq!(analyzed_cell(inner + 2, 1), "24000.00", "{analyzed:#?}");
    assert_eq!(analyzed_cell(inner + 3, 1), "24000.00", "{analyzed:#?}");
    assert_eq!(analyzed_cell(inner + 4, 1), "239996.00", "{analyzed:#?}");
    assert_eq!(analyzed_cell(inner + 5, 1), "24000.00", "{analyzed:#?}");
    assert_eq!(analyzed_cell(inner + 6, 1), "24000.00", "{analyzed:#?}");
    assert_eq!(
        analyzed_cell(inner + 5, 4),
        "group by:test.history.h_d_id, test.history.h_w_id, \
         funcs:sum(test.history.h_amount)->Column#0",
        "{analyzed:#?}",
    );
    assert!(
        analyzed_cell(1, 4)
            .contains("outer key:test.district.d_w_id, inner key:test.history.h_w_id"),
        "only the key admitted by the history index belongs in IndexJoin access keys: \
         {analyzed:#?}"
    );
}

/// TPCC condition 11 pushes its outer predicates through two levels of
/// pass-through derived projections. Go pins all three grouped leaves to the
/// requested warehouse, places the aggregate comparison on the inner join,
/// and preserves the grouped index orders through two merge joins.
#[test]
fn tpcc_condition_eleven_pushes_filters_through_nested_derived_joins() {
    use crate::explain::{explain_select_stmt, ExplainFormat};

    let mut catalog = Catalog::default();
    crate::run_create_table_on(
        "CREATE TABLE customer (c_id INT NOT NULL, c_d_id INT NOT NULL, c_w_id INT NOT NULL, \
         c_first VARCHAR(16), c_middle CHAR(2), c_last VARCHAR(16), \
         c_street_1 VARCHAR(20), c_street_2 VARCHAR(20), c_city VARCHAR(20), c_state CHAR(2), \
         c_zip CHAR(9), c_phone CHAR(16), c_since DATETIME, c_credit CHAR(2), \
         c_credit_lim DECIMAL(12,2), c_discount DECIMAL(4,4), c_balance DECIMAL(12,2), \
         c_ytd_payment DECIMAL(12,2), c_payment_cnt INT, c_delivery_cnt INT, \
         c_data VARCHAR(500), \
         PRIMARY KEY(c_w_id,c_d_id,c_id) CLUSTERED, \
         KEY idx_customer(c_w_id,c_d_id,c_last,c_first))",
        &mut catalog,
    )
    .unwrap();
    crate::run_create_table_on(
        "CREATE TABLE orders (o_id INT NOT NULL, o_d_id INT NOT NULL, o_w_id INT NOT NULL, \
         o_c_id INT, o_entry_d DATETIME, o_carrier_id INT, o_ol_cnt INT, o_all_local INT, \
         PRIMARY KEY(o_w_id,o_d_id,o_id) CLUSTERED, \
         KEY idx_order(o_w_id,o_d_id,o_c_id,o_id))",
        &mut catalog,
    )
    .unwrap();
    crate::run_create_table_on(
        "CREATE TABLE new_order (no_o_id INT NOT NULL, no_d_id INT NOT NULL, \
         no_w_id INT NOT NULL, PRIMARY KEY(no_w_id,no_d_id,no_o_id) CLUSTERED)",
        &mut catalog,
    )
    .unwrap();

    for (table_name, indexes) in [
        (
            "customer",
            vec![
                ("PRIMARY", true, vec![2, 1, 0]),
                ("idx_customer", false, vec![2, 1, 5, 3]),
            ],
        ),
        (
            "orders",
            vec![
                ("PRIMARY", true, vec![2, 1, 0]),
                ("idx_order", false, vec![2, 1, 3, 0]),
            ],
        ),
        ("new_order", vec![("PRIMARY", true, vec![2, 1, 0])]),
    ] {
        let TableEntry::Kv(table) = catalog.get_mut_in("test", table_name).unwrap() else {
            panic!("{table_name} is not a KV table");
        };
        for (position, (name, unique, column_offsets)) in indexes.into_iter().enumerate() {
            table.add_index(crate::kv_table::KvIndex {
                id: (position + 1) as i64,
                name: name.to_owned(),
                comment: String::new(),
                unique,
                prefix_lengths: vec![
                    crate::ddl::index_prefix::UNSPECIFIED_LENGTH;
                    column_offsets.len()
                ],
                column_offsets,
                visible: true,
                global: false,
            });
        }
    }

    // Keep one matching group and one filtered warehouse so predicate
    // placement is observable in both the answer and the physical plan.
    let ctx = crate::StmtContext::for_query();
    run_insert_on(
        "INSERT INTO customer VALUES \
         (1,1,1,'Alice','OE','Able','101 First St','Suite 1','Seattle','WA',\
          '981010001','2065550100000001','2026-01-01','GC',50000.00,0.1000,0,0,0,0,\
          'representative tpcc customer payload one'),\
         (2,1,1,'Bob','OE','Baker','202 Second St','Suite 2','Seattle','WA',\
          '981010002','2065550100000002','2026-01-01','GC',50000.00,0.1000,0,0,0,0,\
          'representative tpcc customer payload two'),\
         (1,1,2,'Carol','OE','Clark','303 Third St','Suite 3','Tacoma','WA',\
          '984010001','2535550100000001','2026-01-01','GC',50000.00,0.1000,0,0,0,0,\
          'representative tpcc customer payload three')",
        &mut catalog,
        &ctx,
    )
    .unwrap();
    run_insert_on(
        "INSERT INTO orders VALUES \
         (1,1,1,1,'2026-01-01',1,10,1),\
         (2,1,1,2,'2026-01-02',2,11,1),\
         (1,1,2,1,'2026-01-03',3,12,1)",
        &mut catalog,
        &ctx,
    )
    .unwrap();
    run_insert_on(
        "INSERT INTO new_order VALUES (1,1,1),(1,1,2)",
        &mut catalog,
        &ctx,
    )
    .unwrap();

    let sql = "SELECT count(*) FROM \
        (SELECT * FROM \
          (SELECT o_w_id, o_d_id, count(*) order_count FROM orders \
           GROUP BY o_w_id, o_d_id) orders \
          JOIN \
          (SELECT no_w_id, no_d_id, count(*) new_order_count FROM new_order \
           GROUP BY no_w_id, no_d_id) new_order \
          ON orders.o_w_id = new_order.no_w_id \
             AND orders.o_d_id = new_order.no_d_id) order_new_order \
        JOIN \
        (SELECT c_w_id, c_d_id, count(*) customer_count FROM customer \
         GROUP BY c_w_id, c_d_id) customer \
        ON order_new_order.no_w_id = customer.c_w_id \
           AND order_new_order.no_d_id = customer.c_d_id \
        WHERE c_w_id = 1 AND order_count - 2100 != new_order_count";
    assert_eq!(
        run_select_on(sql, &catalog, &ctx).unwrap(),
        vec![vec![Datum::Int(1)]]
    );

    let statement = tidb_parser::parse(sql).unwrap();
    let Stmt::Query(query) = &statement else {
        panic!("not a query");
    };
    let QueryStmt::Select(select) = &**query else {
        panic!("not a SELECT");
    };
    let (_, plan) =
        explain_select_stmt(select, &catalog, "test", &ctx, ExplainFormat::Brief).unwrap();
    let cell = |row: &[Datum], column: usize| match &row[column] {
        Datum::Bytes(bytes) => String::from_utf8_lossy(bytes).into_owned(),
        other => format!("{other:?}"),
    };
    let operators = plan.iter().map(|row| cell(row, 0)).collect::<Vec<_>>();
    let details = plan.iter().map(|row| cell(row, 4)).collect::<Vec<_>>();
    let access = plan.iter().map(|row| cell(row, 3)).collect::<Vec<_>>();

    assert_eq!(
        operators
            .iter()
            .filter(|operator| operator.contains("MergeJoin"))
            .count(),
        2,
        "{operators:#?}\n{details:#?}\n{access:#?}"
    );
    let merge_details = operators
        .iter()
        .zip(&details)
        .filter_map(|(operator, detail)| operator.contains("MergeJoin").then_some(detail))
        .collect::<Vec<_>>();
    assert_eq!(merge_details.len(), 2, "{operators:#?}\n{details:#?}");
    assert!(
        merge_details[0].contains("left key:test.new_order.no_w_id, test.new_order.no_d_id")
            && merge_details[0]
                .contains("right key:test.customer.c_w_id, test.customer.c_d_id"),
        "top join must keep Go TiDB's order_new_order-to-customer orientation:\n{operators:#?}\n{details:#?}"
    );
    assert!(
        merge_details[1].contains("left key:test.orders.o_w_id, test.orders.o_d_id")
            && merge_details[1]
                .contains("right key:test.new_order.no_w_id, test.new_order.no_d_id"),
        "nested join must keep Go TiDB's orders-to-new_order orientation:\n{operators:#?}\n{details:#?}"
    );
    assert!(
        operators.iter().all(|operator| {
            !operator.contains("HashJoin")
                && !operator.contains("Selection")
                && !operator.contains("Projection")
                && !operator.contains("FullScan")
        }),
        "{operators:#?}\n{details:#?}\n{access:#?}"
    );
    assert_eq!(
        operators
            .iter()
            .filter(|operator| operator.contains("RangeScan"))
            .count(),
        3,
        "{operators:#?}\n{details:#?}\n{access:#?}"
    );
    assert!(
        details.iter().any(|detail| {
            detail.contains("other cond:ne(minus(") && detail.contains(", 2100)")
        }),
        "{operators:#?}\n{details:#?}\n{access:#?}"
    );
    assert!(
        access
            .iter()
            .any(|object| object.contains("customer") && object.contains("idx_customer")),
        "{operators:#?}\n{details:#?}\n{access:#?}"
    );
    assert!(
        access
            .iter()
            .any(|object| object.contains("orders") && object.contains("idx_order")),
        "{operators:#?}\n{details:#?}\n{access:#?}"
    );
    let customer_aggregation = details
        .iter()
        .find(|detail| detail.starts_with("group by:test.customer.c_d_id"))
        .expect("customer grouped StreamAgg");
    let customer_district = customer_aggregation
        .find("funcs:firstrow(test.customer.c_d_id)->test.customer.c_d_id")
        .expect("customer district carrier");
    let customer_warehouse = customer_aggregation
        .find("funcs:firstrow(test.customer.c_w_id)->test.customer.c_w_id")
        .expect("customer warehouse carrier");
    let synthetic_count = customer_aggregation
        .find("funcs:count(Column#")
        .expect("synthetic customer row count");
    assert!(
        customer_district < customer_warehouse && customer_warehouse < synthetic_count,
        "Go appends the synthetic COUNT after surviving FIRST_ROW carriers: {customer_aggregation}"
    );

    scale_analyzed_tpcc_table(
        &mut catalog,
        "customer",
        300_000,
        &[
            ("c_id", 3_000),
            ("c_d_id", 10),
            ("c_w_id", 10),
            ("c_first", 3_000),
            ("c_last", 1_000),
        ],
        &ctx,
    );
    scale_analyzed_tpcc_table(
        &mut catalog,
        "orders",
        300_000,
        &[
            ("o_id", 3_000),
            ("o_d_id", 10),
            ("o_w_id", 10),
            ("o_c_id", 3_000),
        ],
        &ctx,
    );
    scale_analyzed_tpcc_table(
        &mut catalog,
        "new_order",
        90_000,
        &[("no_o_id", 900), ("no_d_id", 10), ("no_w_id", 10)],
        &ctx,
    );
    catalog.clear_dirty_content();
    let (_, analyzed) =
        explain_select_stmt(select, &catalog, "test", &ctx, ExplainFormat::Brief).unwrap();
    let analyzed_cell = |row: &[Datum], column: usize| match &row[column] {
        Datum::Bytes(bytes) => String::from_utf8_lossy(bytes).into_owned(),
        other => format!("{other:?}"),
    };
    let analyzed_operators = analyzed
        .iter()
        .map(|row| analyzed_cell(row, 0))
        .collect::<Vec<_>>();
    let analyzed_access = analyzed
        .iter()
        .map(|row| analyzed_cell(row, 3))
        .collect::<Vec<_>>();
    let analyzed_details = analyzed
        .iter()
        .map(|row| analyzed_cell(row, 4))
        .collect::<Vec<_>>();
    let analyzed_operator_names = analyzed_operators
        .iter()
        .map(|operator| operator.trim_start_matches(&[' ', '│', '├', '└', '─'][..]))
        .collect::<Vec<_>>();
    assert_eq!(
        analyzed_operator_names
            .iter()
            .filter(|operator| operator.starts_with("IndexJoin"))
            .count(),
        1,
        "{analyzed:#?}"
    );
    assert_eq!(
        analyzed_operator_names
            .iter()
            .filter(|operator| operator.starts_with("IndexHashJoin"))
            .count(),
        1,
        "{analyzed:#?}"
    );
    assert!(
        analyzed_operator_names
            .iter()
            .all(|operator| !operator.starts_with("HashJoin")),
        "{analyzed:#?}"
    );
    let index_hash_join = analyzed_operator_names
        .iter()
        .position(|operator| operator.starts_with("IndexHashJoin"))
        .expect("top IndexHashJoin");
    assert!(
        analyzed_details[index_hash_join].contains(
            "outer key:test.new_order.no_d_id, test.new_order.no_w_id, \
             inner key:test.customer.c_d_id, test.customer.c_w_id"
        ),
        "top access keys must retain logical equality order: {}",
        analyzed_details[index_hash_join]
    );
    let index_join = analyzed_operator_names
        .iter()
        .position(|operator| operator.starts_with("IndexJoin"))
        .expect("nested IndexJoin");
    assert!(
        analyzed_details[index_join].contains(
            "outer key:test.new_order.no_d_id, test.new_order.no_w_id, \
             inner key:test.orders.o_d_id, test.orders.o_w_id"
        ),
        "nested access keys must retain logical equality order: {}",
        analyzed_details[index_join]
    );
    let grouped_detail = |table: &str| {
        analyzed_details
            .iter()
            .find(|detail| detail.contains(&format!("group by:test.{table}.")))
            .unwrap_or_else(|| panic!("missing grouped {table} plan: {analyzed:#?}"))
    };
    for (table, district, warehouse) in [
        ("new_order", "no_d_id", "no_w_id"),
        ("orders", "o_d_id", "o_w_id"),
        ("customer", "c_d_id", "c_w_id"),
    ] {
        let detail = grouped_detail(table);
        assert!(
            detail.starts_with(&format!(
                "group by:test.{table}.{district}, test.{table}.{warehouse}"
            )),
            "Go sorts final physical group names: {detail}"
        );
        let district_carrier = detail
            .find(&format!(
                "funcs:firstrow(test.{table}.{district})->test.{table}.{district}"
            ))
            .unwrap_or_else(|| panic!("missing district carrier: {detail}"));
        let warehouse_carrier = detail
            .find(&format!(
                "funcs:firstrow(test.{table}.{warehouse})->test.{table}.{warehouse}"
            ))
            .unwrap_or_else(|| panic!("missing warehouse carrier: {detail}"));
        assert!(
            district_carrier < warehouse_carrier,
            "Go retains source-schema order for FIRST_ROW carriers: {detail}"
        );
    }
    assert_eq!(
        analyzed_operators
            .iter()
            .filter(|operator| operator.contains("StreamAgg"))
            .count(),
        3,
        "{analyzed:#?}"
    );
    assert!(
        analyzed.iter().any(|row| {
            analyzed_cell(row, 0).contains("IndexRangeScan")
                && analyzed_cell(row, 3).contains("idx_customer")
                && analyzed_cell(row, 4).contains("keep order:true")
        }),
        "{analyzed:#?}"
    );
    assert!(
        analyzed_operator_names
            .iter()
            .zip(&analyzed_access)
            .zip(&analyzed_details)
            .any(|((operator, access), detail)| {
                *operator == "TableRangeScan"
                    && access.contains("table:orders")
                    && detail.contains("keep order:true")
            }),
        "{analyzed:#?}"
    );
}

#[test]
fn tpcc_condition_two_orders_group_uses_the_covering_index_range() {
    use crate::explain::{explain_select_stmt, ExplainFormat};

    let mut catalog = Catalog::default();
    crate::run_create_table_on(
        "CREATE TABLE orders (o_id INT NOT NULL, o_d_id INT NOT NULL, o_w_id INT NOT NULL, \
            o_c_id INT, o_entry_d DATETIME, o_carrier_id INT, o_ol_cnt INT, o_all_local INT, \
            o_unread_payload VARCHAR(1000), \
            PRIMARY KEY (o_w_id,o_d_id,o_id) CLUSTERED, \
            KEY idx_order (o_w_id,o_d_id,o_c_id,o_id))",
        &mut catalog,
    )
    .unwrap();
    let TableEntry::Kv(orders) = catalog.get_mut_in("test", "orders").unwrap() else {
        panic!("orders is not a KV table");
    };
    orders.add_index(crate::kv_table::KvIndex {
        id: 1,
        name: "PRIMARY".to_owned(),
        comment: String::new(),
        unique: true,
        prefix_lengths: vec![crate::ddl::index_prefix::UNSPECIFIED_LENGTH; 3],
        column_offsets: vec![2, 1, 0],
        visible: true,
        global: false,
    });
    orders.add_index(crate::kv_table::KvIndex {
        id: 2,
        name: "idx_order".to_owned(),
        comment: String::new(),
        unique: false,
        prefix_lengths: vec![crate::ddl::index_prefix::UNSPECIFIED_LENGTH; 4],
        column_offsets: vec![2, 1, 3, 0],
        visible: true,
        global: false,
    });
    let ctx = crate::StmtContext::for_query();
    run_insert_on(
        "INSERT INTO orders VALUES \
            (1,1,1,10,'2026-01-01',1,10,1,REPEAT('x',1000)),\
            (2,1,1,20,'2026-01-02',2,11,1,REPEAT('y',1000)),\
            (3,2,1,30,'2026-01-03',3,12,1,REPEAT('z',1000)),\
            (4,1,2,40,'2026-01-04',4,13,1,REPEAT('w',1000))",
        &mut catalog,
        &ctx,
    )
    .unwrap();

    let sql = "SELECT o_d_id, MAX(o_id) mo FROM orders WHERE o_w_id=1 GROUP BY o_d_id";
    assert_eq!(
        run_select_on(sql, &catalog, &ctx).unwrap(),
        vec![
            vec![Datum::Int(1), Datum::Int(2)],
            vec![Datum::Int(2), Datum::Int(3)],
        ]
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
    let plan = (0..rows.len())
        .map(|row| (cell(row, 0), cell(row, 3), cell(row, 4)))
        .collect::<Vec<_>>();
    assert_eq!(
        plan.iter()
            .map(|(operator, _, _)| operator.as_str())
            .collect::<Vec<_>>(),
        vec![
            "Projection",
            "└─StreamAgg",
            "  └─IndexReader",
            "    └─StreamAgg",
            "      └─IndexRangeScan",
        ]
    );
    assert!(
        plan.iter().any(|(operator, access, info)| {
            operator.contains("IndexRangeScan")
                && access.contains("idx_order")
                && info.contains("range:[1,1]")
                && info.contains("keep order:true")
        }),
        "{plan:#?}"
    );

    crate::run_create_table_on(
        "CREATE TABLE district (d_id INT NOT NULL, d_w_id INT NOT NULL, \
            d_next_o_id INT NOT NULL, PRIMARY KEY (d_w_id,d_id) CLUSTERED)",
        &mut catalog,
    )
    .unwrap();
    let TableEntry::Kv(district) = catalog.get_mut_in("test", "district").unwrap() else {
        panic!("district is not a KV table");
    };
    district.add_index(crate::kv_table::KvIndex {
        id: 3,
        name: "PRIMARY".to_owned(),
        comment: String::new(),
        unique: true,
        prefix_lengths: vec![crate::ddl::index_prefix::UNSPECIFIED_LENGTH; 2],
        column_offsets: vec![1, 0],
        visible: true,
        global: false,
    });
    crate::run_create_table_on(
        "CREATE TABLE new_order (no_o_id INT NOT NULL, no_d_id INT NOT NULL, \
            no_w_id INT NOT NULL, PRIMARY KEY (no_w_id,no_d_id,no_o_id) CLUSTERED)",
        &mut catalog,
    )
    .unwrap();
    let TableEntry::Kv(new_order) = catalog.get_mut_in("test", "new_order").unwrap() else {
        panic!("new_order is not a KV table");
    };
    new_order.add_index(crate::kv_table::KvIndex {
        id: 4,
        name: "PRIMARY".to_owned(),
        comment: String::new(),
        unique: true,
        prefix_lengths: vec![crate::ddl::index_prefix::UNSPECIFIED_LENGTH; 3],
        column_offsets: vec![2, 1, 0],
        visible: true,
        global: false,
    });
    run_insert_on(
        "INSERT INTO district VALUES (1,1,5),(2,1,6),(1,2,10)",
        &mut catalog,
        &ctx,
    )
    .unwrap();
    run_insert_on(
        "INSERT INTO new_order VALUES (2,1,1),(3,2,1),(9,1,2)",
        &mut catalog,
        &ctx,
    )
    .unwrap();

    let condition = "SELECT POWER((d_next_o_id-1-mo),2) + \
        POWER((d_next_o_id-1-mno),2) diff FROM district dis, \
        (SELECT o_d_id,MAX(o_id) mo FROM orders WHERE o_w_id=1 GROUP BY o_d_id) q, \
        (SELECT no_d_id,MAX(no_o_id) mno FROM new_order WHERE no_w_id=1 \
        GROUP BY no_d_id) no WHERE d_w_id=1 AND q.o_d_id=dis.d_id \
        AND no.no_d_id=dis.d_id";
    assert_eq!(
        run_select_on(condition, &catalog, &ctx).unwrap(),
        vec![vec![Datum::Real(8.0)], vec![Datum::Real(8.0)]],
    );
    let stmt = tidb_parser::parse(condition).unwrap();
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
    assert_eq!(cell(0, 1), "10.00");
    assert!(
        cell(0, 4)
            .starts_with("plus(power(cast(minus(minus(test.district.d_next_o_id, 1), Column#"),
        "{}",
        cell(0, 4)
    );
    assert!(cell(0, 4).ends_with(")->Column#0"), "{}", cell(0, 4));
    assert_eq!(cell(1, 1), "10.00");
    assert!(
        cell(1, 4).contains("left key:test.district.d_id"),
        "{}",
        cell(1, 4)
    );
    assert!(
        cell(6, 4).contains("left key:test.district.d_id"),
        "{}",
        cell(6, 4)
    );

    scale_analyzed_tpcc_table(
        &mut catalog,
        "district",
        100,
        &[("d_id", 10), ("d_w_id", 10), ("d_next_o_id", 1)],
        &ctx,
    );
    scale_analyzed_tpcc_table(
        &mut catalog,
        "orders",
        300_000,
        &[
            ("o_id", 3_000),
            ("o_d_id", 10),
            ("o_w_id", 10),
            ("o_c_id", 3_000),
        ],
        &ctx,
    );
    scale_analyzed_tpcc_table(
        &mut catalog,
        "new_order",
        90_000,
        &[("no_o_id", 900), ("no_d_id", 10), ("no_w_id", 10)],
        &ctx,
    );
    catalog.clear_dirty_content();
    let (_, analyzed) =
        explain_select_stmt(select, &catalog, "test", &ctx, ExplainFormat::Brief).unwrap();
    let operator = |row: &[Datum]| match &row[0] {
        Datum::Bytes(bytes) => String::from_utf8_lossy(bytes).into_owned(),
        other => format!("{other:?}"),
    };
    let operators = analyzed.iter().map(|row| operator(row)).collect::<Vec<_>>();
    assert_eq!(
        operators
            .iter()
            .filter(|name| name.trim_start_matches(&[' ', '│', '├', '└', '─'][..]) == "Projection")
            .count(),
        2,
        "Go restoreSchemaIfChanged must retain the reordered join's schema projection: {analyzed:#?}"
    );
    assert_eq!(
        operators
            .iter()
            .filter(|name| name.contains("IndexJoin"))
            .count(),
        2,
        "{analyzed:#?}"
    );
    assert!(
        operators.iter().all(|name| !name.contains("HashJoin")),
        "{analyzed:#?}"
    );
    assert_eq!(
        operators
            .iter()
            .filter(|name| name.contains("StreamAgg"))
            .count(),
        2,
        "{analyzed:#?}"
    );
    assert!(
        analyzed.iter().any(|row| {
            operator(row).trim_start_matches(&[' ', '│', '├', '└', '─'][..]) == "TableRangeScan"
                && match &row[3] {
                    Datum::Bytes(bytes) => String::from_utf8_lossy(bytes).contains("table:orders"),
                    _ => false,
                }
                && match &row[4] {
                    Datum::Bytes(bytes) => {
                        String::from_utf8_lossy(bytes).contains("range: decided by")
                    }
                    _ => false,
                }
        }),
        "the analyzed IndexJoin probe must retain Go's common-handle table path: {analyzed:#?}"
    );
    assert!(
        operators.iter().any(
            |name| name.trim_start_matches(&[' ', '│', '├', '└', '─'][..]) == "TableReader(Build)"
        ),
        "the district IndexJoin outer scan must keep Go's root reader boundary: {analyzed:#?}"
    );
    assert!(
        analyzed.iter().all(|row| match &row[4] {
            Datum::Bytes(bytes) => !String::from_utf8_lossy(bytes).contains("test.dis.d_id"),
            _ => true,
        }),
        "dynamic ranges must use the physical district OrigName, not its SQL alias: {analyzed:#?}"
    );
    assert!(
        analyzed.iter().all(|row| match &row[3] {
            Datum::Bytes(bytes) => !String::from_utf8_lossy(bytes).contains("idx_order"),
            _ => true,
        }),
        "the covering secondary index must not displace Go's common-handle probe: {analyzed:#?}"
    );
}

/// Go accepts a pushdown-safe expression as the input of a global SUM. The
/// cop HashAgg evaluates that expression after its Selection, and the root
/// HashAgg merges one partial result per region.
#[test]
fn global_sum_expression_uses_partial_and_final_hash_agg() {
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
            "HashAgg",
            "└─TableReader",
            "  └─HashAgg",
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

/// Go's `BasePhysicalAgg.NewPartialAggregate` expands a global AVG into a
/// cop COUNT/SUM pair and a root final AVG over those two partial columns.
#[test]
fn global_avg_uses_count_sum_partial_and_final_hash_agg() {
    use crate::explain::{explain_select_stmt, ExplainFormat};

    let mut catalog = Catalog::default();
    crate::run_create_table_on(
        "CREATE TABLE avg_revenue (id INT PRIMARY KEY, price DECIMAL(10,2), k INT)",
        &mut catalog,
    )
    .unwrap();
    let ctx = crate::StmtContext::for_query();
    run_insert_on(
        "INSERT INTO avg_revenue VALUES (1,100.00,1),(2,200.00,2),(3,300.00,3)",
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
            "HashAgg",
            "└─TableReader",
            "  └─HashAgg",
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
    catalog.clear_dirty_content();
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
    assert_eq!(
        run_select_on(
            "SELECT a, COUNT(*) FROM t WHERE b >= 20 GROUP BY a",
            &catalog,
            &crate::StmtContext::for_query()
        )
        .unwrap(),
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
    let mut catalog = test_catalog();
    crate::run_create_table_on("CREATE TABLE g (a BIGINT, b BIGINT)", &mut catalog).unwrap();
    run_insert_on(
        "INSERT INTO g VALUES (1, 10), (1, 20), (2, 5), (3, 7), (3, 8)",
        &mut catalog,
        &crate::StmtContext::for_query(),
    )
    .unwrap();

    // HAVING over an aggregate that IS in the select list.
    assert_eq!(
        run_select_on(
            "SELECT a, COUNT(*) FROM g GROUP BY a HAVING COUNT(*) > 1",
            &catalog,
            &crate::StmtContext::for_query()
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
            &crate::StmtContext::for_query()
        )
        .unwrap(),
        vec![vec![Datum::Int(1)]]
    );
    // ORDER BY an aggregate that is not selected, descending.
    assert_eq!(
        run_select_on(
            "SELECT a FROM g GROUP BY a ORDER BY SUM(b) DESC",
            &catalog,
            &crate::StmtContext::for_query()
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
            &crate::StmtContext::for_query()
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
            &crate::StmtContext::for_query()
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
            &crate::StmtContext::for_query()
        )
        .unwrap(),
        vec![vec![Datum::Int(1)], vec![Datum::Int(2)]]
    );
    // A global aggregate's HAVING filters the single group.
    assert_eq!(
        run_select_on(
            "SELECT COUNT(*) FROM g HAVING COUNT(*) > 100",
            &catalog,
            &crate::StmtContext::for_query()
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
    let mut catalog = Catalog::default();
    crate::run_create_table_on("CREATE TABLE d2 (a BIGINT, b BIGINT)", &mut catalog).unwrap();
    run_insert_on(
        "INSERT INTO d2 VALUES (1, 1), (1, 2), (1, 1), (2, 2)",
        &mut catalog,
        &crate::StmtContext::for_query(),
    )
    .unwrap();

    assert_eq!(
        run_select_on(
            "SELECT DISTINCT a FROM d2",
            &catalog,
            &crate::StmtContext::for_query()
        )
        .unwrap(),
        vec![vec![Datum::Int(1)], vec![Datum::Int(2)]]
    );
    // Every projected column takes part, so (1,1) collapses but (1,2)
    // stays.
    assert_eq!(
        run_select_on(
            "SELECT DISTINCT a, b FROM d2",
            &catalog,
            &crate::StmtContext::for_query()
        )
        .unwrap(),
        vec![
            vec![Datum::Int(1), Datum::Int(1)],
            vec![Datum::Int(1), Datum::Int(2)],
            vec![Datum::Int(2), Datum::Int(2)],
        ]
    );
    // Without DISTINCT every row survives.
    assert_eq!(
        run_select_on(
            "SELECT a FROM d2",
            &catalog,
            &crate::StmtContext::for_query()
        )
        .unwrap()
        .len(),
        4
    );

    // DISTINCT applies to the projected expression, not the source rows.
    assert_eq!(
        run_select_on(
            "SELECT DISTINCT a + b FROM d2",
            &catalog,
            &crate::StmtContext::for_query()
        )
        .unwrap(),
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
            &crate::StmtContext::for_query()
        )
        .unwrap(),
        vec![vec![Datum::Int(2)], vec![Datum::Int(1)]]
    );
    // LIMIT applies after the dedup.
    assert_eq!(
        run_select_on(
            "SELECT DISTINCT a FROM d2 LIMIT 1",
            &catalog,
            &crate::StmtContext::for_query()
        )
        .unwrap(),
        vec![vec![Datum::Int(1)]]
    );
    // A WHERE below it still filters.
    assert_eq!(
        run_select_on(
            "SELECT DISTINCT a FROM d2 WHERE b = 2",
            &catalog,
            &crate::StmtContext::for_query()
        )
        .unwrap(),
        vec![vec![Datum::Int(1)], vec![Datum::Int(2)]]
    );

    // Over an aggregate result, DISTINCT deduplicates the output rows.
    crate::run_create_table_on("CREATE TABLE g3 (k BIGINT, v BIGINT)", &mut catalog).unwrap();
    run_insert_on(
        "INSERT INTO g3 VALUES (1, 5), (2, 5), (3, 9)",
        &mut catalog,
        &crate::StmtContext::for_query(),
    )
    .unwrap();
    assert_eq!(
        run_select_on(
            "SELECT DISTINCT SUM(v) FROM g3 GROUP BY k",
            &catalog,
            &crate::StmtContext::for_query()
        )
        .unwrap(),
        vec![
            vec![Datum::Decimal(tidb_datatype::Decimal::from_int(5))],
            vec![Datum::Decimal(tidb_datatype::Decimal::from_int(9))],
        ]
    );
}

/// A pseudo-statistics Sysbench DISTINCT range is physically `Sort -> final
/// HashAgg -> Reader -> partial HashAgg -> cop Scan`; the identity FIRST_ROW
/// output projection is absorbed by the final aggregate.
#[test]
fn distinct_range_orders_gos_hash_agg_over_reader_tree() {
    use crate::explain::{explain_select_stmt, ExplainFormat};

    let mut catalog = Catalog::default();
    crate::run_create_table_on(
        "CREATE TABLE distinct_range (id INT PRIMARY KEY, c CHAR(4))",
        &mut catalog,
    )
    .unwrap();
    let ctx = crate::StmtContext::for_query();
    run_insert_on(
        "INSERT INTO distinct_range VALUES (1, 'b'), (2, 'a'), (3, 'b')",
        &mut catalog,
        &ctx,
    )
    .unwrap();
    let sql = "SELECT DISTINCT c FROM distinct_range WHERE id BETWEEN 1 AND 100 ORDER BY c";
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
            "Sort",
            "└─HashAgg",
            "  └─TableReader",
            "    └─HashAgg",
            "      └─TableRangeScan"
        ]
    );
    assert_eq!(
        (0..rows.len()).map(|row| cell(row, 2)).collect::<Vec<_>>(),
        vec!["root", "root", "root", "cop[tikv]", "cop[tikv]"]
    );
    let values = run_select_on(sql, &catalog, &ctx)
        .unwrap()
        .into_iter()
        .map(|row| row[0].sql_string().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(values, vec!["a", "b"]);

    // Go collects `c` as metadata-only statistics for DISTINCT while the `id`
    // predicate is fully loaded. `EstimateColumnNDV(c)` borrows `id`'s
    // same-version analyzed row count, and the 100-row range clamps the group
    // NDV to exactly 100 instead of applying the pseudo 0.8 factor.
    let (table_id, id_column, c_column) = {
        let TableEntry::Kv(table) = catalog.get_in("test", "distinct_range").unwrap() else {
            panic!("distinct_range is not a KV table");
        };
        let column_id = |name: &str| {
            table
                .visible_columns()
                .iter()
                .find(|column| column.name == name)
                .map(|column| column.id)
                .unwrap_or_else(|| panic!("missing {name} column"))
        };
        (table.table_id, column_id("id"), column_id("c"))
    };
    let version = 42;
    let column = |id: i64, ndv: i64, low: Datum, high: Datum| {
        let mut histogram = tidb_stats::Histogram::new(id, ndv, 0, version, 1, 10_000);
        histogram.append_bucket(low, high, 10_000, 1);
        tidb_planner::cardinality::row_count_estimator::ColumnStats {
            histogram,
            topn: None,
            cms: None,
            stats_ver: 2,
            unsigned: false,
        }
    };
    let statistics = crate::access_cost::TableStatistics::new(
        10_000,
        0,
        [
            (
                id_column,
                column(id_column, 10_000, Datum::Int(1), Datum::Int(10_000)),
            ),
            (
                c_column,
                column(c_column, 10_000, Datum::Int(1), Datum::Int(10_000)),
            ),
        ]
        .into_iter()
        .collect(),
        Default::default(),
    );
    catalog.clear_dirty_content();
    catalog.set_table_statistics(table_id, std::sync::Arc::new(statistics));
    let (_, analyzed) =
        explain_select_stmt(select, &catalog, "test", &ctx, ExplainFormat::Brief).unwrap();
    let analyzed_cell = |row: usize, column: usize| match &analyzed[row][column] {
        Datum::Bytes(bytes) => String::from_utf8_lossy(bytes).into_owned(),
        other => format!("{other:?}"),
    };
    assert_eq!(analyzed_cell(0, 1), "100.00", "{analyzed:#?}");
    assert_eq!(analyzed_cell(1, 1), "100.00", "{analyzed:#?}");
    assert_eq!(analyzed_cell(2, 1), "100.00", "{analyzed:#?}");
    assert_eq!(analyzed_cell(3, 1), "100.00", "{analyzed:#?}");
    assert_eq!(analyzed_cell(4, 1), "100.00", "{analyzed:#?}");
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

    // The inferred column type carries `UnsignedFlag` too, which is what
    // makes a view over one describe as `bigint(21) unsigned NO`.
    for name in ["BIT_AND", "BIT_OR", "BIT_XOR"] {
        let (_, field_type) = crate::driver::agg_build::agg_kind_and_type(name, &[]).unwrap();
        assert_ne!(
            field_type.flags() & tidb_datatype::FieldTypeFlags::UNSIGNED,
            0,
            "{name} must infer an UNSIGNED column"
        );
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

/// TPC-H q17's `SUM(l_extendedprice) / 7.0`: Go's `buildAggregation` splits
/// every select field that CONTAINS an aggregate into the pure aggregate
/// function on the Aggregation operator plus a scalar wrapper evaluated by
/// the projection above it, so the physical HashAgg explains
/// `funcs:sum(...)->Column#N` -- never the written scalar expression as an
/// aggregate function. The wrapper lives on the Projection above, exactly as
/// Go prints for this query in `pkg/planner/core/casetest/tpch`.
#[test]
fn tpch_q17_scalar_wrapped_sum_explains_the_physical_aggregate_function() {
    use crate::explain::{explain_select_stmt, ExplainFormat};

    let mut catalog = Catalog::default();
    crate::run_create_table_on(
        "CREATE TABLE part (p_partkey BIGINT PRIMARY KEY, p_brand VARCHAR(16), \
         p_container VARCHAR(16))",
        &mut catalog,
    )
    .unwrap();
    crate::run_create_table_on(
        "CREATE TABLE lineitem (l_partkey BIGINT, l_quantity DECIMAL(15,2), \
         l_extendedprice DECIMAL(15,2))",
        &mut catalog,
    )
    .unwrap();

    let sql = "SELECT SUM(l_extendedprice) / 7.0 AS avg_yearly FROM lineitem, part \
         WHERE p_partkey = l_partkey AND p_brand = 'Brand#44' \
         AND p_container = 'WRAP PKG'";
    let stmt = tidb_parser::parse(sql).unwrap();
    let Stmt::Query(query) = &stmt else {
        panic!("not a query");
    };
    let QueryStmt::Select(select) = &**query else {
        panic!("not a SELECT");
    };
    let (_, plan) = explain_select_stmt(
        select,
        &catalog,
        "test",
        &crate::StmtContext::for_query(),
        ExplainFormat::Brief,
    )
    .unwrap();
    let text = |row: &[Datum], column: usize| match &row[column] {
        Datum::Bytes(bytes) => String::from_utf8_lossy(bytes).into_owned(),
        other => format!("{other:?}"),
    };

    let hash_agg_info = plan
        .iter()
        .find(|row| text(row, 0).contains("HashAgg"))
        .map(|row| text(row, 4))
        .expect("the plan has a root HashAgg");
    assert!(
        !hash_agg_info.contains("div("),
        "the physical HashAgg must not render the written scalar wrapper: {hash_agg_info}"
    );
    assert!(
        hash_agg_info.starts_with("funcs:"),
        "a non-grouped HashAgg lists its physical functions without group keys: {hash_agg_info}"
    );
    assert!(
        hash_agg_info.contains("funcs:sum(test.lineitem.l_extendedprice)"),
        "the hoisted SUM is the only physical aggregate state: {hash_agg_info}"
    );
}

#[test]
fn debug_q14_plan_dump() {
    use crate::explain::{explain_select_stmt, ExplainFormat};

    let mut catalog = Catalog::default();
    crate::run_create_table_on(
        "CREATE TABLE part (p_partkey INT PRIMARY KEY, p_type VARCHAR(25))",
        &mut catalog,
    )
    .unwrap();
    crate::run_create_table_on(
        "CREATE TABLE lineitem (l_partkey INT, l_extendedprice DECIMAL(15,2), \
            l_discount DECIMAL(15,2), l_shipdate DATE)",
        &mut catalog,
    )
    .unwrap();

    let sql = "SELECT 100.00 * \
        SUM(CASE WHEN p_type LIKE 'PROMO%' \
            THEN l_extendedprice * (1 - l_discount) ELSE 0 END) / \
        SUM(l_extendedprice * (1 - l_discount)) AS promo_revenue \
        FROM lineitem, part WHERE l_partkey = p_partkey \
        AND l_shipdate >= '1996-12-01' \
        AND l_shipdate < DATE_ADD('1996-12-01', INTERVAL 1 MONTH)";
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
    for (i, row) in rows.iter().enumerate() {
        let op = match &row[0] {
            Datum::Bytes(b) => String::from_utf8_lossy(b).into_owned(),
            o => format!("{o:?}"),
        };
        let info = match &row[4] {
            Datum::Bytes(b) => String::from_utf8_lossy(b).into_owned(),
            o => format!("{o:?}"),
        };
        println!("ROW{i}: {op} | {info}");
    }
}

//! Subqueries: uncorrelated, correlated, and correlated inside an aggregate.
//!
//! The correlated cases are the interesting ones -- the inner query is
//! re-evaluated per outer row, and the grouped case pushes that re-evaluation
//! under an aggregate. Mirrors Go `pkg/executor`'s apply and
//! `pkg/planner/core`'s correlated-column handling.

use super::*;

/// TPC-H q2's correlated `min(ps_supplycost)`, decorrelated and planned exactly
/// as Go records it in `tests/integrationtest/r/tpch.result`.
///
/// Go's greedy join reorder is what fixes this shape. With five inner-join
/// nodes and `DefTiDBOptJoinReorderThreshold = 0`
/// (`pkg/sessionctx/vardef/tidb_vars.go`), `rule_join_reorder.go`'s
/// `useGreedy := !allInnerJoin || joinGroupNum > threshold` selects the greedy
/// solver, which sorts the group by `cumCost` and starts from the cheapest node
/// (`rule_join_reorder_greedy.go`'s `constructConnectedJoinTree` takes
/// `s.curJoinGroup[0]`). `region` after `r_name = 'ASIA'` is that node, and the
/// join graph then forces `region -> nation -> supplier -> partsupp -> part`:
/// `part` connects only through `partsupp`, so it is attached last, to the
/// four-way join result rather than to the `partsupp` table. An index join
/// needs a `DataSource` on the inner side to accept
/// `property.IndexJoinRuntimeProp` (`exhaust_physical_plans.go`'s
/// `enumerateIndexJoinByOuterIdx`), so no `IndexHashJoin` and no `partsupp`
/// `TableRangeScan` is reachable here -- upstream's own comment above this
/// query in `tests/integrationtest/t/tpch.test` reads
/// "Planner enhancement: join reorder." The `part`-driven
/// `IndexHashJoin` into `partsupp` does exist in that recording, but for q16
/// (`tpch.result:939`), whose join graph makes the two adjacent.
///
/// The fixture keeps TPC-H's `NOT NULL` columns because
/// `logicalop.deriveNotNullExpr` only synthesises `not(isnull(col))` for a
/// nullable column (`logical_join.go`'s
/// `!mysql.HasNotNullFlag(childCol.RetType.GetFlag())`); a nullable fixture
/// would add cop `Selection`s the recording does not have.
#[test]
fn tpch_q2_correlated_min_matches_recorded_hash_join_plan() {
    let mut catalog = Catalog::default();
    for table in [
        "CREATE TABLE part (p_partkey BIGINT PRIMARY KEY CLUSTERED, \
         p_mfgr VARCHAR(32) NOT NULL, p_type VARCHAR(32) NOT NULL, p_size BIGINT NOT NULL)",
        "CREATE TABLE supplier (s_suppkey BIGINT PRIMARY KEY CLUSTERED, \
         s_name VARCHAR(32) NOT NULL, s_address VARCHAR(64) NOT NULL, \
         s_nationkey BIGINT NOT NULL, s_phone VARCHAR(32) NOT NULL, \
         s_acctbal DECIMAL(15,2) NOT NULL, s_comment VARCHAR(128) NOT NULL)",
        "CREATE TABLE partsupp (ps_partkey BIGINT NOT NULL, ps_suppkey BIGINT NOT NULL, \
         ps_supplycost DECIMAL(15,2) NOT NULL, PRIMARY KEY (ps_partkey, ps_suppkey) CLUSTERED)",
        "CREATE TABLE nation (n_nationkey BIGINT PRIMARY KEY CLUSTERED, \
         n_name VARCHAR(32) NOT NULL, n_regionkey BIGINT NOT NULL)",
        "CREATE TABLE region (r_regionkey BIGINT PRIMARY KEY CLUSTERED, \
         r_name VARCHAR(32) NOT NULL)",
    ] {
        crate::run_create_table_on(table, &mut catalog).unwrap();
    }
    let ctx = crate::StmtContext::for_query();
    for insert in [
        "INSERT INTO part VALUES (1, 'Manufacturer#1', 'ECONOMY ANODIZED STEEL', 30)",
        "INSERT INTO supplier VALUES \
         (1, 'Supplier#1', 'Address#1', 1, '10-000-000-0000', 100.00, 'Comment')",
        "INSERT INTO partsupp VALUES (1, 1, 10.00)",
        "INSERT INTO nation VALUES (1, 'INDIA', 1)",
        "INSERT INTO region VALUES (1, 'ASIA')",
    ] {
        run_insert_on(insert, &mut catalog, &ctx).unwrap();
    }
    for (table, rows, ndvs) in [
        (
            "part",
            200_000,
            vec![
                ("p_partkey", 196_960),
                ("p_mfgr", 5),
                ("p_type", 150),
                ("p_size", 50),
            ],
        ),
        (
            "supplier",
            10_000,
            vec![("s_suppkey", 10_000), ("s_nationkey", 25)],
        ),
        (
            "partsupp",
            800_000,
            vec![
                ("ps_partkey", 196_960),
                ("ps_suppkey", 10_000),
                ("ps_supplycost", 99_865),
            ],
        ),
        (
            "nation",
            25,
            vec![("n_nationkey", 25), ("n_name", 25), ("n_regionkey", 5)],
        ),
        ("region", 5, vec![("r_regionkey", 5), ("r_name", 5)]),
    ] {
        scale_analyzed_tpcc_table(&mut catalog, table, rows, &ndvs, &ctx);
    }

    let sql = "SELECT s_acctbal, s_name, n_name, p_partkey, p_mfgr, \
        s_address, s_phone, s_comment FROM part, supplier, partsupp, nation, region \
        WHERE p_partkey = ps_partkey AND s_suppkey = ps_suppkey \
        AND p_size = 30 AND p_type LIKE '%STEEL' \
        AND s_nationkey = n_nationkey AND n_regionkey = r_regionkey \
        AND r_name = 'ASIA' AND ps_supplycost = \
        (SELECT MIN(ps_supplycost) FROM partsupp, supplier, nation, region \
         WHERE p_partkey = ps_partkey AND s_suppkey = ps_suppkey \
         AND s_nationkey = n_nationkey AND n_regionkey = r_regionkey \
         AND r_name = 'ASIA') \
        ORDER BY s_acctbal DESC, n_name, s_name, p_partkey LIMIT 100";
    let statement = tidb_parser::parse(sql).unwrap();
    let Stmt::Query(query) = &statement else {
        panic!("not a query");
    };
    let QueryStmt::Select(select) = &**query else {
        panic!("not a SELECT");
    };
    let (_, plan) = crate::explain::explain_select_stmt(
        select,
        &catalog,
        "test",
        &ctx,
        crate::explain::ExplainFormat::Brief,
    )
    .expect("q2 must remain explainable after scalar-MIN decorrelation");
    // Go redacts column ids under `explain format = 'plan_tree'`
    // (`pkg/expression/column.go`: "show \"Column\" instead of
    // \"Column#<number>\""), so the produced ids are stripped the same way
    // before comparing. estRows is not compared: `plan_tree` does not print it,
    // and this fixture is scaled to SF1 while the recording is tpch50.
    fn strip_column_ids(info: &str) -> String {
        let mut stripped = String::with_capacity(info.len());
        let mut rest = info;
        while let Some(at) = rest.find("Column#") {
            stripped.push_str(&rest[..at]);
            stripped.push_str("Column");
            rest = &rest[at + "Column#".len()..];
            let digits = rest.len() - rest.trim_start_matches(|c: char| c.is_ascii_digit()).len();
            rest = &rest[digits..];
        }
        stripped.push_str(rest);
        stripped
    }
    let text = |row: &[Datum], column: usize| match &row[column] {
        Datum::Bytes(bytes) => String::from_utf8_lossy(bytes).into_owned(),
        other => format!("{other:?}"),
    };

    // Go's recorded q2 tree, `tests/integrationtest/r/tpch.result`, with
    // `tpch50.` rewritten to this fixture's `test.` schema. Columns are
    // id / task / access object / operator info.
    const RECORDED_Q2_PLAN_TREE: &[(&str, &str, &str, &str)] = &[
    ("Projection", "root", "", "test.supplier.s_acctbal, test.supplier.s_name, test.nation.n_name, test.part.p_partkey, test.part.p_mfgr, test.supplier.s_address, test.supplier.s_phone, test.supplier.s_comment"),
    ("└─TopN", "root", "", "test.supplier.s_acctbal:desc, test.nation.n_name, test.supplier.s_name, test.part.p_partkey, offset:0, count:100"),
    ("  └─Projection", "root", "", "test.part.p_partkey, test.part.p_mfgr, test.supplier.s_name, test.supplier.s_address, test.supplier.s_phone, test.supplier.s_acctbal, test.supplier.s_comment, test.nation.n_name"),
    ("    └─HashJoin", "root", "", "inner join, equal:[eq(test.part.p_partkey, test.partsupp.ps_partkey) eq(test.partsupp.ps_supplycost, Column)]"),
    ("      ├─HashJoin(Build)", "root", "", "inner join, equal:[eq(test.partsupp.ps_partkey, test.part.p_partkey)]"),
    ("      │ ├─TableReader(Build)", "root", "", "data:Selection"),
    ("      │ │ └─Selection", "cop[tikv]", "", "eq(test.part.p_size, 30), like(test.part.p_type, \"%STEEL\", 92)"),
    ("      │ │   └─TableFullScan", "cop[tikv]", "table:part", "keep order:false"),
    ("      │ └─HashJoin(Probe)", "root", "", "inner join, equal:[eq(test.supplier.s_suppkey, test.partsupp.ps_suppkey)]"),
    ("      │   ├─HashJoin(Build)", "root", "", "inner join, equal:[eq(test.nation.n_nationkey, test.supplier.s_nationkey)]"),
    ("      │   │ ├─HashJoin(Build)", "root", "", "inner join, equal:[eq(test.region.r_regionkey, test.nation.n_regionkey)]"),
    ("      │   │ │ ├─TableReader(Build)", "root", "", "data:Selection"),
    ("      │   │ │ │ └─Selection", "cop[tikv]", "", "eq(test.region.r_name, \"ASIA\")"),
    ("      │   │ │ │   └─TableFullScan", "cop[tikv]", "table:region", "keep order:false"),
    ("      │   │ │ └─TableReader(Probe)", "root", "", "data:TableFullScan"),
    ("      │   │ │   └─TableFullScan", "cop[tikv]", "table:nation", "keep order:false"),
    ("      │   │ └─TableReader(Probe)", "root", "", "data:TableFullScan"),
    ("      │   │   └─TableFullScan", "cop[tikv]", "table:supplier", "keep order:false"),
    ("      │   └─TableReader(Probe)", "root", "", "data:TableFullScan"),
    ("      │     └─TableFullScan", "cop[tikv]", "table:partsupp", "keep order:false"),
    ("      └─Selection(Probe)", "root", "", "not(isnull(Column))"),
    ("        └─HashAgg", "root", "", "group by:test.partsupp.ps_partkey, funcs:min(test.partsupp.ps_supplycost)->Column, funcs:firstrow(test.partsupp.ps_partkey)->test.partsupp.ps_partkey"),
    ("          └─HashJoin", "root", "", "inner join, equal:[eq(test.supplier.s_suppkey, test.partsupp.ps_suppkey)]"),
    ("            ├─HashJoin(Build)", "root", "", "inner join, equal:[eq(test.nation.n_nationkey, test.supplier.s_nationkey)]"),
    ("            │ ├─HashJoin(Build)", "root", "", "inner join, equal:[eq(test.region.r_regionkey, test.nation.n_regionkey)]"),
    ("            │ │ ├─TableReader(Build)", "root", "", "data:Selection"),
    ("            │ │ │ └─Selection", "cop[tikv]", "", "eq(test.region.r_name, \"ASIA\")"),
    ("            │ │ │   └─TableFullScan", "cop[tikv]", "table:region", "keep order:false"),
    ("            │ │ └─TableReader(Probe)", "root", "", "data:TableFullScan"),
    ("            │ │   └─TableFullScan", "cop[tikv]", "table:nation", "keep order:false"),
    ("            │ └─TableReader(Probe)", "root", "", "data:TableFullScan"),
    ("            │   └─TableFullScan", "cop[tikv]", "table:supplier", "keep order:false"),
    ("            └─TableReader(Probe)", "root", "", "data:TableFullScan"),
    ("              └─TableFullScan", "cop[tikv]", "table:partsupp", "keep order:false"),
    ];

    let produced = plan
        .iter()
        .map(|row| {
            (
                strip_column_ids(&text(row, 0)),
                text(row, 2),
                text(row, 3),
                strip_column_ids(&text(row, 4)),
            )
        })
        .collect::<Vec<_>>();
    let recorded = RECORDED_Q2_PLAN_TREE
        .iter()
        .map(|(id, task, access, info)| {
            (
                (*id).to_owned(),
                (*task).to_owned(),
                (*access).to_owned(),
                (*info).to_owned(),
            )
        })
        .collect::<Vec<_>>();
    assert_eq!(
        produced, recorded,
        "q2 must plan exactly as Go records it in tests/integrationtest/r/tpch.result",
    );

    let rows = run_select_on(sql, &catalog, &ctx)
        .expect("the composite dynamic lookup must execute, not only explain");
    assert_eq!(
        rows.len(),
        1,
        "the one matching q2 fixture row must survive"
    );
    assert_eq!(rows[0].len(), 8);
}

/// Go `handleInSubquery` lowers TPC-H q16's non-null `NOT IN` predicate to
/// an anti-semi join before physical join search. The grouped aggregate must
/// keep the DISTINCT bit on its supplier count after that rewrite.
#[test]
fn tpch_q16_non_null_not_in_is_an_anti_semi_join() {
    let mut catalog = Catalog::default();
    for table in [
        "CREATE TABLE part (P_PARTKEY BIGINT PRIMARY KEY CLUSTERED, \
         P_BRAND VARCHAR(16), P_TYPE VARCHAR(32), P_SIZE BIGINT)",
        "CREATE TABLE partsupp (PS_PARTKEY BIGINT NOT NULL, PS_SUPPKEY BIGINT NOT NULL, \
         PRIMARY KEY (PS_PARTKEY, PS_SUPPKEY) CLUSTERED)",
        "CREATE TABLE supplier (S_SUPPKEY BIGINT PRIMARY KEY CLUSTERED, S_COMMENT VARCHAR(128))",
    ] {
        crate::run_create_table_on(table, &mut catalog).unwrap();
    }
    let ctx = crate::StmtContext::for_query();
    for (table, rows, ndvs) in [
        (
            "part",
            200_000,
            vec![
                ("P_PARTKEY", 200_000),
                ("P_BRAND", 25),
                ("P_TYPE", 150),
                ("P_SIZE", 50),
            ],
        ),
        (
            "partsupp",
            800_000,
            vec![("PS_PARTKEY", 200_000), ("PS_SUPPKEY", 10_000)],
        ),
        (
            "supplier",
            10_000,
            vec![("S_SUPPKEY", 10_000), ("S_COMMENT", 10_000)],
        ),
    ] {
        scale_analyzed_tpcc_table(&mut catalog, table, rows, &ndvs, &ctx);
    }

    let sql = "SELECT p_brand, p_type, p_size, COUNT(DISTINCT ps_suppkey) AS supplier_cnt \
        FROM partsupp, part WHERE p_partkey = ps_partkey \
        AND p_brand <> 'Brand#34' AND p_type NOT LIKE 'LARGE BRUSHED%' \
        AND p_size IN (48, 19, 12, 4, 41, 7, 21, 39) \
        AND ps_suppkey NOT IN (SELECT s_suppkey FROM supplier \
          WHERE s_comment LIKE '%Customer%Complaints%') \
        GROUP BY p_brand, p_type, p_size \
        ORDER BY supplier_cnt DESC, p_brand, p_type, p_size";
    let statement = tidb_parser::parse(sql).unwrap();
    let Stmt::Query(query) = &statement else {
        panic!("not a query");
    };
    let QueryStmt::Select(select) = &**query else {
        panic!("not a SELECT");
    };
    let (_, plan) = crate::explain::explain_select_stmt(
        select,
        &catalog,
        "test",
        &ctx,
        crate::explain::ExplainFormat::Brief,
    )
    .expect("q16 must remain explainable after NOT IN decorrelation");
    let text = |row: &[Datum], column: usize| match &row[column] {
        Datum::Bytes(bytes) => String::from_utf8_lossy(bytes).into_owned(),
        other => format!("{other:?}"),
    };

    assert!(
        plan.iter().any(|row| {
            text(row, 0).contains("Join") && text(row, 4).starts_with("anti semi join")
        }),
        "the non-null NOT IN must become an anti-semi join: {plan:#?}",
    );
    assert!(
        plan.iter().any(|row| {
            text(row, 0).contains("Join")
                && text(row, 4).starts_with("anti semi join")
                && text(row, 4).contains("left side:Projection")
        }),
        "the anti-semi join must consume the pruned left schema Go builds before physical search: \
         {plan:#?}",
    );
    assert!(
        plan.iter().any(|row| {
            text(row, 0).contains("Projection(")
                && text(row, 4)
                    == "test.partsupp.ps_suppkey, test.part.p_brand, test.part.p_type, \
                        test.part.p_size"
        }),
        "the semi-join input must restore the written join schema before aggregation: {plan:#?}",
    );
    let anti_join_rows = plan
        .iter()
        .find(|row| text(row, 0).contains("Join") && text(row, 4).starts_with("anti semi join"))
        .and_then(|row| text(row, 1).parse::<f64>().ok())
        .expect("q16 anti-semi join estimate");
    let preserved_rows = plan
        .iter()
        .find(|row| text(row, 0).contains("Projection("))
        .and_then(|row| text(row, 1).parse::<f64>().ok())
        .expect("q16 preserved input estimate");
    assert!(
        (anti_join_rows - preserved_rows * crate::plan_trace::SELECTIVITY_FACTOR).abs() < 0.02,
        "Go LogicalJoin derives an anti-semi join from its already-filtered left child: \
         {plan:#?}",
    );
    assert!(
        plan.iter().any(|row| {
            text(row, 0).contains("Join")
                && text(row, 4).contains("part.p_partkey")
                && text(row, 4).contains("partsupp.ps_partkey")
        }),
        "the anti-semi join must stop the aggregate order from forcing a MergeJoin below it: \
         {plan:#?}",
    );
    assert!(
        plan.iter().all(|row| !text(row, 0).contains("MergeJoin")),
        "q16's unordered anti-semi join input must not retain a MergeJoin property: {plan:#?}",
    );
    assert!(
        plan.iter().any(|row| {
            text(row, 0).contains("HashAgg")
                && text(row, 4).contains("count(distinct test.partsupp.ps_suppkey)")
        }),
        "the physical aggregate must retain COUNT(DISTINCT): {plan:#?}",
    );
    assert!(
        plan.iter().any(|row| {
            text(row, 0).contains("Selection") && text(row, 4).contains("supplier.s_comment")
        }),
        "the subquery predicate must remain on the supplier input: {plan:#?}",
    );
}

/// Go `handleInSubquery` adds duplicate elimination above the inner query, but
/// `AggregationEliminator` removes it when that query already groups by its
/// complete one-column output. The surviving grouped relation remains
/// reorderable and can use the ordered partial/final StreamAgg selected for its
/// clustered-key prefix.
#[test]
fn grouped_in_subquery_reuses_its_unique_group_output() {
    let mut catalog = Catalog::default();
    for table in [
        "CREATE TABLE customer (c_custkey BIGINT PRIMARY KEY CLUSTERED, c_name VARCHAR(32))",
        "CREATE TABLE orders (o_orderkey BIGINT PRIMARY KEY CLUSTERED, o_custkey BIGINT, \
         o_orderdate DATE, o_totalprice DECIMAL(15,2))",
        "CREATE TABLE lineitem (l_orderkey BIGINT, l_linenumber BIGINT, l_quantity DECIMAL(15,2), \
         PRIMARY KEY (l_orderkey, l_linenumber) CLUSTERED)",
    ] {
        crate::run_create_table_on(table, &mut catalog).unwrap();
    }
    let ctx = crate::StmtContext::for_query();
    run_insert_on(
        "INSERT INTO customer VALUES (1, 'a'), (2, 'b')",
        &mut catalog,
        &ctx,
    )
    .unwrap();
    run_insert_on(
        "INSERT INTO orders VALUES \
         (1, 1, '1995-01-01', 100.00), (2, 2, '1995-01-02', 200.00)",
        &mut catalog,
        &ctx,
    )
    .unwrap();
    run_insert_on(
        "INSERT INTO lineitem VALUES (1, 1, 10.00), (1, 2, 20.00), (2, 1, 30.00)",
        &mut catalog,
        &ctx,
    )
    .unwrap();
    for (table, rows, ndvs) in [
        (
            "customer",
            150_000,
            vec![("c_custkey", 150_000), ("c_name", 150_000)],
        ),
        (
            "orders",
            1_500_000,
            vec![
                ("o_orderkey", 1_500_000),
                ("o_custkey", 100_000),
                ("o_orderdate", 2_406),
                ("o_totalprice", 1_400_000),
            ],
        ),
        (
            "lineitem",
            6_001_215,
            vec![
                ("l_orderkey", 1_487_616),
                ("l_linenumber", 7),
                ("l_quantity", 50),
            ],
        ),
    ] {
        scale_analyzed_tpcc_table(&mut catalog, table, rows, &ndvs, &ctx);
    }

    let statement = tidb_parser::parse(
        "SELECT c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice, SUM(l_quantity) \
         FROM customer, orders, lineitem \
         WHERE o_orderkey IN (SELECT l_orderkey FROM lineitem GROUP BY l_orderkey \
           HAVING SUM(l_quantity) > 314) \
         AND c_custkey = o_custkey AND o_orderkey = l_orderkey \
         GROUP BY c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice \
         ORDER BY o_totalprice DESC, o_orderdate LIMIT 100",
    )
    .unwrap();
    let Stmt::Query(query) = &statement else {
        panic!("not a query");
    };
    let QueryStmt::Select(select) = &**query else {
        panic!("not a SELECT");
    };
    let (_, plan) = crate::explain::explain_select_stmt(
        select,
        &catalog,
        "test",
        &ctx,
        crate::explain::ExplainFormat::Brief,
    )
    .expect("the grouped IN relation remains explainable after duplicate elimination");
    let text = |row: &[Datum], column: usize| match &row[column] {
        Datum::Bytes(bytes) => String::from_utf8_lossy(bytes).into_owned(),
        other => format!("{other:?}"),
    };

    assert!(
        plan.iter().all(|row| {
            !(text(row, 0).contains("Selection")
                && text(row, 4).contains("customer.c_custkey")
                && text(row, 4).contains("orders.o_orderkey"))
        }),
        "join equalities consumed by the rebuilt join group must not survive as a Selection: \
         {plan:#?}",
    );
    assert!(
        plan.iter()
            .filter(|row| text(row, 0).contains("Join"))
            .all(|row| text(row, 1) != "N/A"),
        "the grouped derived relation must participate in join cardinality derivation: {plan:#?}",
    );
    let stream_aggs = plan
        .iter()
        .filter(|row| text(row, 0).contains("StreamAgg"))
        .collect::<Vec<_>>();
    assert_eq!(
        stream_aggs.len(),
        2,
        "the grouped inner query must split into root and cop StreamAgg stages: {plan:#?}",
    );
    assert!(
        stream_aggs
            .iter()
            .all(|row| { text(row, 1) == "1487616.00" && text(row, 4).contains("funcs:sum(") }),
        "HAVING's SUM must survive in both Go partial/final StreamAgg stages: {plan:#?}",
    );
    assert!(
        stream_aggs.iter().any(|row| {
            text(row, 2) == "cop[tikv]" && text(row, 4).contains("sum(test.lineitem.l_quantity)")
        }),
        "the cop StreamAgg must read the SUM argument required only by HAVING: {plan:#?}",
    );
    assert!(
        plan.iter().any(|row| {
            text(row, 0).contains("TableReader") && text(row, 4) == "data:StreamAgg"
        }),
        "the partial StreamAgg must remain below a TableReader boundary: {plan:#?}",
    );
    assert!(
        plan.iter().any(|row| {
            text(row, 0).contains("TableFullScan")
                && text(row, 2) == "cop[tikv]"
                && text(row, 3) == "table:lineitem"
                && text(row, 4) == "keep order:true"
        }),
        "the clustered l_orderkey prefix must satisfy the StreamAgg order: {plan:#?}",
    );
}

/// Go evaluates an uncorrelated scalar subquery before decorrelating a sibling
/// NOT EXISTS. The resulting constant predicate is therefore pushed into the
/// preserved DataSource together with its ordinary local predicates.
#[test]
fn evaluated_scalar_predicate_is_pushed_below_a_sibling_anti_semi_join() {
    let mut catalog = Catalog::default();
    crate::run_create_table_on(
        "CREATE TABLE customer (c_custkey BIGINT PRIMARY KEY CLUSTERED, \
         c_phone VARCHAR(16), c_acctbal DECIMAL(15,2))",
        &mut catalog,
    )
    .unwrap();
    crate::run_create_table_on("CREATE TABLE orders (o_custkey BIGINT)", &mut catalog).unwrap();
    let ctx = crate::StmtContext::for_query();
    run_insert_on(
        "INSERT INTO customer VALUES (1, '20-1', 100.00), (2, '40-2', 10.00)",
        &mut catalog,
        &ctx,
    )
    .unwrap();
    run_insert_on("INSERT INTO orders VALUES (2)", &mut catalog, &ctx).unwrap();
    scale_analyzed_tpcc_table(
        &mut catalog,
        "customer",
        150_000,
        &[
            ("c_custkey", 150_000),
            ("c_phone", 150_000),
            ("c_acctbal", 140_000),
        ],
        &ctx,
    );
    scale_analyzed_tpcc_table(
        &mut catalog,
        "orders",
        1_500_000,
        &[("o_custkey", 100_000)],
        &ctx,
    );

    let statement = tidb_parser::parse(
        "SELECT cntrycode, COUNT(*), SUM(c_acctbal) FROM ( \
           SELECT SUBSTRING(c_phone, 1, 2) AS cntrycode, c_acctbal FROM customer \
           WHERE SUBSTRING(c_phone, 1, 2) IN ('20', '40') \
           AND c_acctbal > (SELECT AVG(c_acctbal) FROM customer \
             WHERE c_acctbal > 0.00 AND SUBSTRING(c_phone, 1, 2) IN ('20', '40')) \
           AND NOT EXISTS (SELECT 1 FROM orders WHERE o_custkey = c_custkey) \
         ) AS custsale GROUP BY cntrycode ORDER BY cntrycode",
    )
    .unwrap();
    let Stmt::Query(query) = &statement else {
        panic!("not a query");
    };
    let QueryStmt::Select(select) = &**query else {
        panic!("not a SELECT");
    };
    let (_, plan) = crate::explain::explain_select_stmt(
        select,
        &catalog,
        "test",
        &ctx,
        crate::explain::ExplainFormat::Brief,
    )
    .unwrap();
    let text = |row: &[Datum], column: usize| match &row[column] {
        Datum::Bytes(bytes) => String::from_utf8_lossy(bytes).into_owned(),
        other => format!("{other:?}"),
    };

    let anti_join = plan
        .iter()
        .position(|row| text(row, 0).contains("Join") && text(row, 4).starts_with("anti semi join"))
        .expect("q22 must contain an anti-semi join");
    let outer_agg = plan[..anti_join]
        .iter()
        .rposition(|row| text(row, 0).contains("HashAgg"))
        .expect("q22 must aggregate the anti-semi result");
    assert!(
        plan[outer_agg + 1..anti_join].iter().any(|row| {
            let info = text(row, 4);
            text(row, 0).contains("Projection")
                && info.contains("test.customer.c_acctbal")
                && info.contains("substring(test.customer.c_phone")
        }),
        "Go InjectProjBelowAgg must evaluate q22's scalar group item between the outer HashAgg \
         and anti-semi join: {plan:#?}",
    );
    let scalar_subquery = plan
        .iter()
        .position(|row| text(row, 0).contains("ScalarSubQuery"))
        .expect("q22 must retain the scalar child as a separate EXPLAIN root");
    assert_eq!(
        text(&plan[scalar_subquery], 4),
        "Output: ScalarQueryCol#14",
        "Go builds the derived source's scalar child before allocating the outer aggregate columns: \
         {plan:#?}",
    );
    let customer_selection = plan[anti_join + 1..scalar_subquery]
        .iter()
        .find(|row| {
            let info = text(row, 4);
            text(row, 0).contains("Selection")
                && info.contains("ScalarQueryCol#")
                && info.contains("substring(test.customer.c_phone")
        })
        .unwrap_or_else(|| {
            panic!(
                "the evaluated scalar and ordinary customer predicates must reach one DataSource \
                 Selection below the anti-semi join: {plan:#?}"
            )
        });
    assert_ne!(
        text(customer_selection, 1),
        "120000.00",
        "the evaluated scalar constant must participate in loaded-statistics selectivity instead \
         of charging the whole predicate Go's 0.8 fallback: {plan:#?}",
    );
    let filtered_left_rows = text(customer_selection, 1).parse::<f64>().unwrap();
    let anti_join_rows = text(&plan[anti_join], 1).parse::<f64>().unwrap();
    let outer_agg_rows = text(&plan[outer_agg], 1).parse::<f64>().unwrap();
    assert!(
        outer_agg_rows <= anti_join_rows + 0.02,
        "a grouped aggregation cannot produce more groups than its filtered anti-semi input: \
         {plan:#?}",
    );
    assert!(
        (anti_join_rows - filtered_left_rows * crate::plan_trace::SELECTIVITY_FACTOR).abs() < 0.02,
        "Go LogicalJoin derives an anti-semi join from its filtered left child: {plan:#?}",
    );
    assert!(
        plan[..anti_join].iter().all(|row| {
            let info = text(row, 4);
            !(text(row, 0).contains("Selection")
                && info.contains("ScalarQueryCol#")
                && info.contains("substring(test.customer.c_phone"))
        }),
        "a predicate accepted by the preserved DataSource must not remain as a duplicate root \
         Selection above the anti-semi join: {plan:#?}",
    );
    assert!(
        plan.iter().any(|row| {
            text(row, 0).contains("Join") && text(row, 4).starts_with("anti semi join")
        }),
        "NOT EXISTS must still decorrelate after scalar evaluation: {plan:#?}",
    );
    assert!(
        plan.iter().any(|row| {
            text(row, 0).contains("Join")
                && text(row, 4).starts_with("anti semi join")
                && text(row, 4).contains("left side:TableReader")
        }),
        "the smaller filtered customer side must build the anti-semi hash table: {plan:#?}",
    );
    assert!(
        plan[scalar_subquery + 1..].iter().any(|row| {
            text(row, 0).contains("HashAgg")
                && text(row, 2).contains("cop")
                && text(row, 4).contains("funcs:count(test.customer.c_acctbal)")
                && text(row, 4).contains("funcs:sum(test.customer.c_acctbal)")
        }),
        "the scalar AVG must split into TiKV COUNT/SUM and a root final AVG: {plan:#?}",
    );
}

/// Go's default plain-EXPLAIN path registers the scalar child plan, evaluates
/// it once, and leaves a `Constant` carrying `SubqueryRefID` in the outer
/// predicate. The non-evaluating behavior is an opt-in session variable.
#[test]
fn plain_explain_evaluates_and_labels_an_uncorrelated_scalar_subquery() {
    let mut catalog = Catalog::default();
    crate::run_create_table_on("CREATE TABLE balances (v DECIMAL(10,2))", &mut catalog).unwrap();
    let ctx = crate::StmtContext::for_query();
    run_insert_on(
        "INSERT INTO balances VALUES (1.00), (3.00)",
        &mut catalog,
        &ctx,
    )
    .unwrap();
    let statement =
        tidb_parser::parse("SELECT v FROM balances WHERE v > (SELECT AVG(v) FROM balances)")
            .unwrap();
    let Stmt::Query(query) = &statement else {
        panic!("not a query");
    };
    let QueryStmt::Select(select) = &**query else {
        panic!("not a SELECT");
    };
    let ((_, plan), operations) = crate::storage::capture_storage_ops(|| {
        crate::explain::explain_select_stmt(
            select,
            &catalog,
            "test",
            &ctx,
            crate::explain::ExplainFormat::Brief,
        )
        .unwrap()
    });
    assert_ne!(
        operations,
        crate::storage::StorageOps::default(),
        "the default Go branch evaluates the scalar child once",
    );
    let text = |row: &[Datum], column: usize| match &row[column] {
        Datum::Bytes(bytes) => String::from_utf8_lossy(bytes).into_owned(),
        other => format!("{other:?}"),
    };
    assert!(
        plan.iter()
            .any(|row| text(row, 0).contains("ScalarSubQuery")),
        "the evaluated child plan must remain a separate EXPLAIN root: {plan:#?}",
    );
    assert!(
        plan.iter().any(|row| {
            text(row, 0).contains("Selection")
                && text(row, 4).contains("ScalarQueryCol#")
                && text(row, 4).contains('(')
        }),
        "the outer predicate must display the evaluated subquery constant: {plan:#?}",
    );

    let statement = tidb_parser::parse(
        "SELECT v, SUM(v) FROM balances GROUP BY v \
         HAVING SUM(v) > (SELECT AVG(v) FROM balances)",
    )
    .unwrap();
    let Stmt::Query(query) = &statement else {
        panic!("not a query");
    };
    let QueryStmt::Select(select) = &**query else {
        panic!("not a SELECT");
    };
    let (_, grouped_plan) = crate::explain::explain_select_stmt(
        select,
        &catalog,
        "test",
        &ctx,
        crate::explain::ExplainFormat::Brief,
    )
    .unwrap();
    assert!(
        grouped_plan.iter().any(|row| {
            let info = text(row, 4);
            text(row, 0).contains("Selection")
                && info.contains("ScalarQueryCol#")
                && info.contains("(2.000000)")
        }),
        "HAVING must resolve the evaluated subquery through the aggregate output: \
         {grouped_plan:#?}",
    );

    crate::run_create_table_on(
        "CREATE TABLE inventory (k INT, price DECIMAL(10,2), qty INT)",
        &mut catalog,
    )
    .unwrap();
    crate::run_create_table_on("CREATE TABLE inventory_key (k INT)", &mut catalog).unwrap();
    run_insert_on(
        "INSERT INTO inventory VALUES (1, 2.00, 3), (2, 4.00, 5)",
        &mut catalog,
        &ctx,
    )
    .unwrap();
    run_insert_on(
        "INSERT INTO inventory_key VALUES (1), (2)",
        &mut catalog,
        &ctx,
    )
    .unwrap();
    let statement = tidb_parser::parse(
        "SELECT inventory.k, SUM(inventory.price * inventory.qty) AS value \
         FROM inventory, inventory_key WHERE inventory.k = inventory_key.k \
         GROUP BY inventory.k \
         HAVING SUM(inventory.price * inventory.qty) > \
             (SELECT SUM(price * qty) * 0.1 FROM inventory) \
         ORDER BY value DESC",
    )
    .unwrap();
    let Stmt::Query(query) = &statement else {
        panic!("not a query");
    };
    let QueryStmt::Select(select) = &**query else {
        panic!("not a SELECT");
    };
    let (_, q11_plan) = crate::explain::explain_select_stmt(
        select,
        &catalog,
        "test",
        &ctx,
        crate::explain::ExplainFormat::Brief,
    )
    .unwrap();
    let operators = q11_plan
        .iter()
        .map(|row| {
            text(row, 0)
                .trim_start_matches(&[' ', '│', '├', '└', '─'][..])
                .to_owned()
        })
        .collect::<Vec<_>>();
    assert_eq!(
        &operators[..5],
        ["Projection", "Sort", "Selection", "HashAgg", "Projection"],
        "the visible aggregate projection must stay above an unbounded sort: {q11_plan:#?}",
    );
    assert!(
        text(&q11_plan[0], 4).contains("Column#14->Column#27"),
        "the final projection must retain Go's input-to-output column identity: {q11_plan:#?}",
    );
    assert!(
        q11_plan.iter().any(|row| {
            text(row, 0).contains("ScalarSubQuery") && text(row, 4) == "Output: ScalarQueryCol#25"
        }),
        "the scalar placeholder must use Go's statement-wide plan-column allocator: \
         {q11_plan:#?}",
    );
    let hash_agg = text(&q11_plan[3], 4);
    assert_eq!(
        hash_agg.matches("funcs:sum(").count(),
        1,
        "HAVING must reuse the selected SUM: {q11_plan:#?}",
    );
    assert!(
        text(&q11_plan[4], 4).contains("cast(test.inventory.qty, decimal(10,0) BINARY)"),
        "DECIMAL arithmetic must cast its integer column like Go: {q11_plan:#?}",
    );
}

/// Plain EXPLAIN infers correlated-subquery and view output types from their
/// plans. It must not execute those children merely to discover a type; an
/// uncorrelated scalar subquery is different and is evaluated by the default
/// Go branch pinned above.
#[test]
fn explaining_a_correlated_scalar_type_reads_no_storage() {
    use crate::explain::{explain_select_stmt, ExplainFormat};

    fn explain_without_storage(
        sql: &str,
        catalog: &Catalog,
        ctx: &crate::StmtContext,
    ) -> Vec<Vec<Datum>> {
        let statement = tidb_parser::parse(sql).unwrap();
        let Stmt::Query(query) = &statement else {
            panic!("not a query");
        };
        let QueryStmt::Select(select) = &**query else {
            panic!("not a SELECT");
        };
        let (explained, operations) = crate::storage::capture_storage_ops(|| {
            explain_select_stmt(select, catalog, "test", ctx, ExplainFormat::Brief)
        });
        let (_, plan) = explained.unwrap_or_else(|error| panic!("{sql}: {error:?}"));
        assert_eq!(
            operations,
            crate::storage::StorageOps::default(),
            "plain EXPLAIN read storage for {sql}"
        );
        plan
    }

    fn operators(plan: &[Vec<Datum>]) -> Vec<String> {
        plan.iter()
            .map(|row| match &row[0] {
                Datum::Bytes(bytes) => String::from_utf8_lossy(bytes).into_owned(),
                other => format!("{other:?}"),
            })
            .collect()
    }

    let mut catalog = Catalog::default();
    crate::run_create_table_on(
        "CREATE TABLE outer_t (k BIGINT PRIMARY KEY CLUSTERED)",
        &mut catalog,
    )
    .unwrap();
    crate::run_create_table_on(
        "CREATE TABLE inner_t (k BIGINT NOT NULL, v DECIMAL(6,2))",
        &mut catalog,
    )
    .unwrap();
    crate::run_create_table_on("CREATE TABLE inner_u (k BIGINT PRIMARY KEY)", &mut catalog)
        .unwrap();
    let ctx = crate::StmtContext::for_query();
    run_insert_on("INSERT INTO outer_t VALUES (1)", &mut catalog, &ctx).unwrap();
    run_insert_on("INSERT INTO inner_t VALUES (1, 2.50)", &mut catalog, &ctx).unwrap();
    scale_analyzed_tpcc_table(&mut catalog, "outer_t", 10_000, &[("k", 10_000)], &ctx);
    scale_analyzed_tpcc_table(
        &mut catalog,
        "inner_t",
        10_000,
        &[("k", 500), ("v", 10_000)],
        &ctx,
    );
    catalog
        .register_view_in(
            "test",
            "inner_v",
            crate::driver::catalog::ViewDef {
                name: "inner_v".to_owned(),
                columns: vec![(
                    "v".to_owned(),
                    tidb_datatype::FieldType::new(tidb_datatype::FieldTypeCode::NewDecimal),
                )],
                select_sql: "SELECT `v` AS `v` FROM `test`.`inner_t`".to_owned(),
                definer_user: String::new(),
                definer_host: String::new(),
                character_set_client: "utf8mb4".to_owned(),
                collation_connection: "utf8mb4_bin".to_owned(),
                algorithm: "UNDEFINED".to_owned(),
                security: "DEFINER".to_owned(),
                check_option: "CASCADED".to_owned(),
            },
        )
        .unwrap();
    catalog
        .register_view_in(
            "test",
            "revenue_v",
            crate::driver::catalog::ViewDef {
                name: "revenue_v".to_owned(),
                columns: vec![
                    (
                        "supplier_no".to_owned(),
                        tidb_datatype::FieldType::new(tidb_datatype::FieldTypeCode::LongLong),
                    ),
                    (
                        "total_revenue".to_owned(),
                        tidb_datatype::FieldType::new(tidb_datatype::FieldTypeCode::NewDecimal),
                    ),
                ],
                select_sql: "SELECT k AS supplier_no, SUM(v) AS total_revenue \
                    FROM inner_t GROUP BY k"
                    .to_owned(),
                definer_user: String::new(),
                definer_host: String::new(),
                character_set_client: "utf8mb4".to_owned(),
                collation_connection: "utf8mb4_bin".to_owned(),
                algorithm: "UNDEFINED".to_owned(),
                security: "DEFINER".to_owned(),
                check_option: "CASCADED".to_owned(),
            },
        )
        .unwrap();

    for sql in [
        "SELECT (SELECT SUM(v) FROM inner_t WHERE inner_t.k=outer_t.k) FROM outer_t",
        "SELECT * FROM inner_v",
    ] {
        explain_without_storage(sql, &catalog, &ctx);
    }

    for (sql, expected) in [
        (
            "SELECT k FROM outer_t WHERE k=(SELECT MAX(v) FROM inner_v)",
            "Output: ScalarQueryCol#8",
        ),
        (
            "SELECT k FROM outer_t WHERE k=(SELECT MAX(total_revenue) FROM revenue_v)",
            "Output: ScalarQueryCol#10",
        ),
    ] {
        let statement = tidb_parser::parse(sql).unwrap();
        let Stmt::Query(query) = &statement else {
            panic!("not a query");
        };
        let QueryStmt::Select(select) = &**query else {
            panic!("not a SELECT");
        };
        let (_, plan) = explain_select_stmt(select, &catalog, "test", &ctx, ExplainFormat::Brief)
            .unwrap_or_else(|error| panic!("{sql}: {error:?}"));
        assert!(
            plan.iter().any(|row| {
                operators(std::slice::from_ref(row))[0].contains("ScalarSubQuery")
                    && match &row[4] {
                        Datum::Bytes(bytes) => String::from_utf8_lossy(bytes) == expected,
                        _ => false,
                    }
            }),
            "query-source allocation must match Go for {sql}: {plan:#?}",
        );
    }

    let q15 = tidb_parser::parse(
        "SELECT outer_t.k, total_revenue FROM outer_t, revenue_v \
         WHERE outer_t.k=supplier_no AND total_revenue=(SELECT MAX(total_revenue) FROM revenue_v) \
         ORDER BY outer_t.k",
    )
    .unwrap();
    let Stmt::Query(q15) = &q15 else {
        panic!("not a query");
    };
    let QueryStmt::Select(q15) = &**q15 else {
        panic!("not a SELECT");
    };
    let (_, q15_plan) = explain_select_stmt(q15, &catalog, "test", &ctx, ExplainFormat::Brief)
        .expect("the scalar output must remain resolvable while the outer join is built");
    let info = |row: &[Datum]| match &row[4] {
        Datum::Bytes(bytes) => String::from_utf8_lossy(bytes).into_owned(),
        other => format!("{other:?}"),
    };
    let estimate = |row: &[Datum]| match &row[1] {
        Datum::Bytes(bytes) => String::from_utf8_lossy(bytes).into_owned(),
        other => format!("{other:?}"),
    };
    let q15_operators = operators(&q15_plan)
        .into_iter()
        .map(|operator| {
            operator
                .trim_start_matches(&[' ', '│', '├', '└', '─'][..])
                .to_owned()
        })
        .collect::<Vec<_>>();
    assert_eq!(q15_operators[0], "Sort", "{q15_plan:#?}");
    assert!(
        q15_operators[1].starts_with("Index") && q15_operators[1].ends_with("Join"),
        "the filtered grouped view must drive a dynamic index lookup: {q15_plan:#?}",
    );
    assert_eq!(
        &q15_operators[2..4],
        ["Selection(Build)", "HashAgg"],
        "the view-output filter must stay directly above its aggregate: {q15_plan:#?}",
    );
    assert_eq!(
        estimate(&q15_plan[1]),
        "400.00",
        "the view-output Selection must use Go's fixed SelectionFactor: {q15_plan:#?}",
    );
    assert!(
        info(&q15_plan[2]).contains("ScalarQueryCol#"),
        "{q15_plan:#?}"
    );
    assert!(
        info(&q15_plan[1]).contains("test.inner_t.k")
            && !info(&q15_plan[1]).contains("test.revenue_v.supplier_no"),
        "an eliminated view projection must retain the grouped base-column identity: {q15_plan:#?}",
    );
    assert!(
        info(&q15_plan[2]).contains("eq(Column#")
            && !info(&q15_plan[2]).contains("test.revenue_v")
            && !info(&q15_plan[2]).contains("not(isnull("),
        "the computed view output is internal and its non-null group key needs no filter: {q15_plan:#?}",
    );
    let scalar = q15_operators
        .iter()
        .position(|operator| operator == "ScalarSubQuery")
        .expect("plain EXPLAIN must retain the scalar subquery root");
    assert_eq!(
        info(&q15_plan[scalar]),
        "Output: ScalarQueryCol#15",
        "a view body must consume Go's plan-column IDs before the outer scalar subquery: \
         {q15_plan:#?}",
    );
    let dynamic_range = q15_operators[..scalar]
        .iter()
        .position(|operator| operator == "TableRangeScan")
        .expect("the index join must retain its dynamic range source");
    assert!(
        info(&q15_plan[dynamic_range]).contains("test.inner_t.k"),
        "the dynamic range must name the grouped base column: {q15_plan:#?}",
    );
    assert_eq!(
        &q15_operators[scalar..scalar + 6],
        [
            "ScalarSubQuery",
            "MaxOneRow",
            "StreamAgg",
            "TopN",
            "Selection",
            "HashAgg",
        ],
        "a single global MAX must follow Go's max/min elimination: {q15_plan:#?}",
    );
    assert_eq!(estimate(&q15_plan[scalar + 4]), "400.00", "{q15_plan:#?}",);
    assert!(
        info(&q15_plan[scalar + 4]).contains("not(isnull("),
        "{q15_plan:#?}",
    );
    assert!(
        !info(&q15_plan[scalar + 5]).contains("firstrow("),
        "column pruning must remove the unused grouped output below MAX: {q15_plan:#?}",
    );

    let non_unique = explain_without_storage(
        "SELECT k FROM outer_t WHERE k IN (SELECT k FROM inner_t)",
        &catalog,
        &ctx,
    );
    let non_unique_operators = operators(&non_unique);
    assert!(
        non_unique_operators
            .iter()
            .any(|operator| operator.contains("HashJoin")),
        "{non_unique:#?}"
    );
    assert!(
        non_unique_operators
            .iter()
            .any(|operator| operator.contains("HashAgg")),
        "{non_unique:#?}"
    );

    let unique = explain_without_storage(
        "SELECT k FROM outer_t WHERE k IN (SELECT k FROM inner_u)",
        &catalog,
        &ctx,
    );
    let unique_operators = operators(&unique);
    assert!(
        unique_operators
            .iter()
            .any(|operator| operator.contains("HashJoin")),
        "{unique:#?}"
    );
    assert!(
        unique_operators
            .iter()
            .all(|operator| !operator.contains("HashAgg")),
        "{unique:#?}"
    );
}

/// Go's `DecorrelateSolver` pulls equality predicates below a scalar
/// aggregation into join keys and appends those keys to the inner grouping.
/// The scalar value then participates in the outer predicate as an ordinary
/// aggregate column instead of remaining an opaque per-row subquery.
#[test]
fn correlated_avg_predicate_decorrelates_to_grouped_join() {
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

    let statement = tidb_parser::parse(
        "SELECT SUM(l_extendedprice) / 7.0 FROM lineitem, part \
         WHERE p_partkey = l_partkey AND p_brand = 'Brand#44' \
         AND p_container = 'WRAP PKG' AND l_quantity < \
         (SELECT 0.2 * AVG(l_quantity) FROM lineitem \
          WHERE l_partkey = p_partkey)",
    )
    .unwrap();
    let Stmt::Query(query) = &statement else {
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

    assert!(
        plan.iter().any(|row| {
            text(row, 0).contains("HashAgg")
                && text(row, 4).contains("group by:test.lineitem.l_partkey")
                && text(row, 4).contains("avg(")
        }),
        "the correlated AVG must become a grouped inner aggregation: {plan:#?}"
    );
    assert!(
        plan.iter().any(|row| {
            text(row, 0).contains("HashJoin")
                && text(row, 4).contains("test.part.p_partkey")
                && text(row, 4).contains("test.lineitem.l_partkey")
        }),
        "the correlation equality must become a join key: {plan:#?}"
    );
    assert!(
        plan.iter()
            .all(|row| !text(row, 4).contains("SELECT 0.2*AVG")),
        "the outer predicate must not retain an opaque scalar subquery: {plan:#?}"
    );
}

#[test]
fn correlated_sum_predicate_pulls_above_unique_outer_join() {
    use crate::explain::{explain_select_stmt, ExplainFormat};

    let mut catalog = Catalog::default();
    crate::run_create_table_on(
        "CREATE TABLE part (p_partkey BIGINT PRIMARY KEY, p_name VARCHAR(32))",
        &mut catalog,
    )
    .unwrap();
    crate::run_create_table_on(
        "CREATE TABLE partsupp (ps_partkey BIGINT NOT NULL, ps_suppkey BIGINT NOT NULL, \
         ps_availqty BIGINT, PRIMARY KEY(ps_partkey, ps_suppkey) CLUSTERED)",
        &mut catalog,
    )
    .unwrap();
    crate::run_create_table_on(
        "CREATE TABLE lineitem (l_partkey BIGINT, l_suppkey BIGINT, \
         l_quantity DECIMAL(15,2), l_shipdate DATE)",
        &mut catalog,
    )
    .unwrap();
    crate::run_create_table_on(
        "CREATE TABLE supplier (s_suppkey BIGINT PRIMARY KEY, s_name VARCHAR(32), \
         s_address VARCHAR(64), s_nationkey BIGINT)",
        &mut catalog,
    )
    .unwrap();
    crate::run_create_table_on(
        "CREATE TABLE nation (n_nationkey BIGINT PRIMARY KEY, n_name VARCHAR(32))",
        &mut catalog,
    )
    .unwrap();
    let ctx = crate::StmtContext::for_query();
    let part_values = (1..=128)
        .map(|key| {
            let name = if key == 1 {
                "green alpha".to_owned()
            } else {
                format!("red {key:03}")
            };
            format!("({key}, '{name}')")
        })
        .collect::<Vec<_>>()
        .join(", ");
    crate::run_insert_on(
        &format!("INSERT INTO part VALUES {part_values}"),
        &mut catalog,
        &ctx,
    )
    .unwrap();
    crate::run_insert_on("INSERT INTO partsupp VALUES (1, 1, 10)", &mut catalog, &ctx).unwrap();
    crate::run_insert_on(
        "INSERT INTO lineitem VALUES \
         (1, 1, 1.00, '1992-01-01'), \
         (1, 1, 2.00, '1993-06-01'), \
         (2, 2, 3.00, '1994-06-01')",
        &mut catalog,
        &ctx,
    )
    .unwrap();
    crate::run_insert_on(
        "INSERT INTO supplier VALUES (1, 'Supplier#1', 'Address#1', 1)",
        &mut catalog,
        &ctx,
    )
    .unwrap();
    crate::run_insert_on(
        "INSERT INTO nation VALUES (1, 'ALGERIA')",
        &mut catalog,
        &ctx,
    )
    .unwrap();
    scale_analyzed_tpcc_table(
        &mut catalog,
        "part",
        200_000,
        &[("p_partkey", 196_960), ("p_name", 198_848)],
        &ctx,
    );
    scale_analyzed_tpcc_table(
        &mut catalog,
        "partsupp",
        800_000,
        &[
            ("ps_partkey", 196_960),
            ("ps_suppkey", 10_000),
            ("ps_availqty", 9_999),
        ],
        &ctx,
    );
    scale_analyzed_tpcc_table(
        &mut catalog,
        "lineitem",
        6_001_215,
        &[
            ("l_partkey", 200_000),
            ("l_suppkey", 10_000),
            ("l_quantity", 50),
            ("l_shipdate", 2_526),
        ],
        &ctx,
    );
    scale_analyzed_tpcc_table(
        &mut catalog,
        "supplier",
        10_000,
        &[("s_suppkey", 10_000), ("s_nationkey", 25)],
        &ctx,
    );
    scale_analyzed_tpcc_table(
        &mut catalog,
        "nation",
        25,
        &[("n_nationkey", 25), ("n_name", 25)],
        &ctx,
    );

    let statement = tidb_parser::parse(
        "SELECT ps_suppkey FROM partsupp WHERE ps_partkey IN \
         (SELECT p_partkey FROM part WHERE p_name LIKE 'green%') \
         AND ps_availqty > (SELECT 0.5 * SUM(l_quantity) FROM lineitem \
         WHERE l_partkey = ps_partkey AND l_suppkey = ps_suppkey \
         AND l_shipdate >= '1993-01-01' \
         AND l_shipdate < DATE_ADD('1993-01-01', INTERVAL '1' YEAR))",
    )
    .unwrap();
    let Stmt::Query(query) = &statement else {
        panic!("not a query");
    };
    let QueryStmt::Select(select) = &**query else {
        panic!("not a SELECT");
    };
    let planned_select = (**select).clone();
    let planned_catalog = catalog.clone();
    let (_, plan) = std::thread::Builder::new()
        .name("correlated-sum-plan".to_owned())
        .stack_size(8 * 1024 * 1024)
        .spawn(move || {
            explain_select_stmt(
                &planned_select,
                &planned_catalog,
                "test",
                &crate::StmtContext::for_query(),
                ExplainFormat::Brief,
            )
        })
        .unwrap()
        .join()
        .unwrap()
        .unwrap();
    let text = |row: &[Datum], column: usize| match &row[column] {
        Datum::Bytes(bytes) => String::from_utf8_lossy(bytes).into_owned(),
        other => format!("{other:?}"),
    };

    assert!(
        plan.iter().any(|row| {
            text(row, 0).contains("HashJoin")
                && text(row, 4).contains("left outer join")
                && text(row, 4).contains("partsupp.ps_partkey")
                && text(row, 4).contains("lineitem.l_partkey")
        }),
        "the scalar SUM input must be left-joined before aggregation: {plan:#?}"
    );
    assert!(
        plan.iter().any(|row| {
            text(row, 0).contains("HashAgg")
                && text(row, 4).contains("group by:test.partsupp.ps_partkey")
                && text(row, 4).contains("test.partsupp.ps_suppkey")
                && text(row, 4).contains("sum(")
        }),
        "the complete unique key must group the pulled aggregate: {plan:#?}"
    );
    assert!(
        plan.iter().any(|row| {
            text(row, 0).contains("Selection")
                && text(row, 4).contains("gt(")
                && text(row, 4).contains("mul(0.5")
        }),
        "the scalar comparison must remain above the pulled aggregate: {plan:#?}"
    );

    let statement = tidb_parser::parse(
        "SELECT s_name, s_address FROM supplier, nation \
         WHERE s_nationkey = n_nationkey AND s_suppkey IN \
         (SELECT ps_suppkey FROM partsupp WHERE ps_partkey IN \
          (SELECT p_partkey FROM part WHERE p_name LIKE 'green%') \
          AND ps_availqty > (SELECT 0.5 * SUM(l_quantity) FROM lineitem \
           WHERE l_partkey = ps_partkey AND l_suppkey = ps_suppkey \
           AND l_shipdate >= '1993-01-01' \
           AND l_shipdate < DATE_ADD('1993-01-01', INTERVAL '1' YEAR))) \
         AND n_name = 'ALGERIA' ORDER BY s_name",
    )
    .unwrap();
    let Stmt::Query(query) = &statement else {
        panic!("not a query");
    };
    let QueryStmt::Select(select) = &**query else {
        panic!("not a SELECT");
    };
    let reorder_select = (**select).clone();
    let reorder_catalog = catalog.clone();
    let reordered = std::thread::Builder::new()
        .name("nested-correlated-sum-reorder".to_owned())
        .stack_size(8 * 1024 * 1024)
        .spawn(move || {
            let reorder_ctx = crate::StmtContext::for_query();
            let rewritten = crate::driver::subquery::rewrite_filter_in_subqueries(
                &reorder_select,
                &reorder_catalog,
                "test",
                &reorder_ctx,
            )
            .unwrap()
            .expect("the outer q20 IN predicate must become a distinct join leaf");
            crate::driver::join_reorder::reorder(
                rewritten.from.as_ref().unwrap(),
                &rewritten,
                rewritten.where_clause.as_ref(),
                &reorder_catalog,
                "test",
                &reorder_ctx,
            )
            .expect("the rewritten q20 inner join group must remain reorderable")
        })
        .unwrap()
        .join()
        .unwrap();
    assert_eq!(
        reordered.written_order,
        vec![1, 0, 2],
        "the filtered nation input must seed the rewritten q20 join group",
    );
    let nested_select = (**select).clone();
    let nested_catalog = catalog.clone();
    let (_, nested_plan) = std::thread::Builder::new()
        .name("nested-correlated-sum-plan".to_owned())
        .stack_size(8 * 1024 * 1024)
        .spawn(move || {
            explain_select_stmt(
                &nested_select,
                &nested_catalog,
                "test",
                &crate::StmtContext::for_query(),
                ExplainFormat::Brief,
            )
        })
        .unwrap()
        .join()
        .unwrap()
        .unwrap();

    assert!(
        nested_plan.iter().any(|row| {
            text(row, 0).contains("HashJoin")
                && text(row, 4).contains("left outer join")
                && text(row, 4).contains("partsupp.ps_partkey")
                && text(row, 4).contains("lineitem.l_partkey")
        }),
        "the scalar SUM must be pulled up before the outer IN rewrite: {nested_plan:#?}"
    );
    let pulled_outer_join = nested_plan
        .iter()
        .position(|row| {
            text(row, 0).contains("HashJoin")
                && text(row, 4).contains("left outer join")
                && text(row, 4).contains("partsupp.ps_partkey")
                && text(row, 4).contains("lineitem.l_partkey")
        })
        .expect("pulled scalar-SUM left outer join");
    let pulled_outer_join_rows = nested_plan
        .iter()
        .skip(pulled_outer_join)
        .take(7)
        .map(|row| {
            (0..row.len())
                .map(|column| text(row, column))
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    assert!(
        !text(&nested_plan[0], 0).contains("Projection"),
        "a direct output containing every ORDER BY column must be pruned into the join: \
         {nested_plan:#?}"
    );
    let supplier_membership = nested_plan
        .iter()
        .find(|row| {
            text(row, 0).contains("HashJoin") && text(row, 4).contains("supplier.s_suppkey")
        })
        .expect("supplier membership join");
    assert!(
        text(supplier_membership, 4).contains("partsupp.ps_suppkey"),
        "the DISTINCT relation key must retain its base-column identity: {nested_plan:#?}"
    );
    assert!(
        nested_plan.iter().any(|row| {
            text(row, 0).contains("HashJoin")
                && text(row, 4)
                    .contains("equal:[eq(test.nation.n_nationkey, test.supplier.s_nationkey)]")
        }),
        "the filtered nation seed must remain the logical left side, independently of the \
         physical build side: {nested_plan:#?}"
    );
    let scalar_selection = nested_plan
        .iter()
        .position(|row| text(row, 0).contains("Selection") && text(row, 4).contains("mul(0.5"))
        .expect("scalar aggregate predicate Selection");
    let selection_rows = text(&nested_plan[scalar_selection], 1)
        .parse::<f64>()
        .unwrap();
    let aggregate_rows = text(&nested_plan[scalar_selection + 1], 1)
        .parse::<f64>()
        .unwrap();
    assert!(
        (selection_rows - aggregate_rows * crate::plan_trace::SELECTIVITY_FACTOR).abs() < 0.02,
        "LogicalSelection.DeriveStats scales its aggregate child by SelectivityFactor: \
         selection={selection_rows}, aggregate={aggregate_rows}, expected={}; \
         {nested_plan:#?}",
        aggregate_rows * crate::plan_trace::SELECTIVITY_FACTOR
    );
    let selection_info = text(&nested_plan[scalar_selection], 4);
    assert!(
        selection_info.contains("cast(test.partsupp.ps_availqty, decimal(20,0) BINARY)")
            && selection_info.contains("mul(0.5, Column#"),
        "decorrelation carriers must print as a base column and an aggregate result: \
         {nested_plan:#?}"
    );
    assert!(
        text(&nested_plan[pulled_outer_join + 1], 0).contains("IndexHashJoin(Build)"),
        "HashJoin v2 must cost the preserved side as a build candidate: \
         {pulled_outer_join_rows:#?}"
    );
    let leaked_aliases = nested_plan
        .iter()
        .map(|row| text(row, 4))
        .filter(|info| info.contains("__decorrelated_"))
        .collect::<Vec<_>>();
    assert!(
        leaked_aliases.is_empty(),
        "internal decorrelation aliases must not escape their query block: {leaked_aliases:#?}"
    );
    assert!(
        nested_plan.iter().any(|row| {
            text(row, 0).contains("HashJoin")
                && text(row, 4).contains("supplier.s_suppkey")
                && text(row, 1) != "N/A"
        }),
        "the DISTINCT IN relation must retain a modeled row count: {nested_plan:#?}"
    );
    let part_partsupp_join = nested_plan
        .iter()
        .find(|row| {
            text(row, 0).contains("Join")
                && text(row, 4).contains("part.p_partkey")
                && text(row, 4).contains("partsupp.ps_partkey")
        })
        .unwrap_or_else(|| {
            panic!(
                "the selective part input must drive partsupp's clustered-key prefix lookup: \
                 {nested_plan:#?}"
            )
        });
    let part_selection_rows = nested_plan
        .iter()
        .find(|row| text(row, 0).contains("Selection") && text(row, 4).contains("part.p_name"))
        .and_then(|row| text(row, 1).parse::<f64>().ok())
        .expect("part LIKE Selection estimate");
    let part_scan_rows = nested_plan
        .iter()
        .find(|row| text(row, 0).contains("TableFullScan") && text(row, 3) == "table:part")
        .and_then(|row| text(row, 1).parse::<f64>().ok())
        .expect("part full-scan estimate");
    assert!(
        part_selection_rows < part_scan_rows,
        "a sibling outer join must not erase the derived child's loaded-statistics selectivity: \
         {nested_plan:#?}"
    );
    let part_partsupp_rows = text(part_partsupp_join, 1).parse::<f64>().unwrap();
    let expected_part_partsupp_rows = part_selection_rows * 800_000.0 / 196_960.0;
    assert!(
        (part_partsupp_rows - expected_part_partsupp_rows).abs() < 0.02,
        "Go LogicalJoin derives the filtered join from the analyzed ps_partkey NDV: \
         expected {expected_part_partsupp_rows}, got {part_partsupp_rows}; {nested_plan:#?}"
    );
}

/// Go decorrelates a scalar SUM over equality-correlated keys by pulling the
/// aggregation above a left join. TPCC condition 12 depends on both halves:
/// an unmatched customer still produces one outer row with a NULL SUM, while
/// EXPLAIN contains the real HashAgg/Join pipeline rather than an opaque
/// per-row scalar-subquery projection.
#[test]
fn tpcc_conditions_ten_and_twelve_decorrelate_scalar_sums() {
    use crate::explain::{explain_select_stmt, ExplainFormat};

    let mut catalog = Catalog::default();
    crate::run_create_table_on(
        "CREATE TABLE customer (c_w_id INT NOT NULL, c_d_id INT NOT NULL, \
         c_id INT NOT NULL, c_balance DECIMAL(12,2), c_ytd_payment DECIMAL(12,2), \
         PRIMARY KEY(c_w_id,c_d_id,c_id) CLUSTERED)",
        &mut catalog,
    )
    .unwrap();
    crate::run_create_table_on(
        "CREATE TABLE orders (o_w_id INT NOT NULL, o_d_id INT NOT NULL, \
         o_id INT NOT NULL, o_c_id INT, PRIMARY KEY(o_w_id,o_d_id,o_id) CLUSTERED)",
        &mut catalog,
    )
    .unwrap();
    crate::run_create_table_on(
        "CREATE TABLE order_line (ol_w_id INT NOT NULL, ol_d_id INT NOT NULL, \
         ol_o_id INT NOT NULL, ol_number INT NOT NULL, ol_amount DECIMAL(6,2), \
         ol_delivery_d BIGINT, PRIMARY KEY(ol_w_id,ol_d_id,ol_o_id,ol_number) CLUSTERED)",
        &mut catalog,
    )
    .unwrap();
    crate::run_create_table_on(
        "CREATE TABLE history (h_c_id INT NOT NULL, h_c_d_id INT NOT NULL, \
         h_c_w_id INT NOT NULL, \
         h_amount DECIMAL(6,2), INDEX idx_h_c_w_id(h_c_w_id))",
        &mut catalog,
    )
    .unwrap();
    // Cluster-loaded metadata exposes the common-handle PRIMARY as a table
    // access path. The in-memory DDL catalog stores only the handle, so add
    // the corresponding planner index explicitly, as the join tests do.
    for (table_name, column_offsets) in [
        ("customer", vec![0, 1, 2]),
        ("orders", vec![0, 1, 2]),
        ("order_line", vec![0, 1, 2, 3]),
    ] {
        let TableEntry::Kv(table) = catalog.get_mut_in("test", table_name).unwrap() else {
            panic!("{table_name} is not a KV table");
        };
        table.add_index(crate::kv_table::KvIndex {
            id: 1,
            name: "PRIMARY".to_owned(),
            comment: String::new(),
            unique: true,
            prefix_lengths: vec![
                crate::ddl::index_prefix::UNSPECIFIED_LENGTH;
                column_offsets.len()
            ],
            column_offsets,
            visible: true,
            global: false,
            clustered_primary: false,
        }, false);
    }
    let ctx = crate::StmtContext::for_query();
    run_insert_on(
        "INSERT INTO customer VALUES \
         (1,1,1,5.00,5.00),(1,1,2,0.00,0.00),(1,1,3,7.00,1.00)",
        &mut catalog,
        &ctx,
    )
    .unwrap();
    run_insert_on(
        "INSERT INTO orders VALUES (1,1,11,1),(1,1,12,2)",
        &mut catalog,
        &ctx,
    )
    .unwrap();
    run_insert_on(
        "INSERT INTO order_line VALUES \
         (1,1,11,1,10.00,1),(1,1,11,2,100.00,NULL),(1,1,12,1,5.00,1)",
        &mut catalog,
        &ctx,
    )
    .unwrap();
    run_insert_on(
        "INSERT INTO history VALUES (1,1,1,5.00),(2,1,1,5.00),(3,1,1,1.00)",
        &mut catalog,
        &ctx,
    )
    .unwrap();

    let sql = "SELECT count(*) FROM (SELECT c.c_id,c.c_d_id,c.c_balance c1, \
        c_ytd_payment,(SELECT sum(ol_amount) FROM orders,order_line \
        WHERE ol_w_id=o_w_id AND ol_d_id=o_d_id AND ol_o_id=o_id \
        AND ol_delivery_d IS NOT NULL AND o_w_id=1 AND o_d_id=c.c_d_id \
        AND o_c_id=c.c_id) sm FROM customer c WHERE c.c_w_id=1) t1 \
        WHERE c1+c_ytd_payment<>sm";
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
    let operators = plan
        .iter()
        .map(|row| match &row[0] {
            Datum::Bytes(bytes) => String::from_utf8_lossy(bytes).into_owned(),
            other => format!("{other:?}"),
        })
        .collect::<Vec<_>>();
    let details = plan
        .iter()
        .map(|row| match &row[4] {
            Datum::Bytes(bytes) => String::from_utf8_lossy(bytes).into_owned(),
            other => format!("{other:?}"),
        })
        .collect::<Vec<_>>();
    let estimates = plan
        .iter()
        .map(|row| match &row[1] {
            Datum::Bytes(bytes) => String::from_utf8_lossy(bytes).into_owned(),
            other => format!("{other:?}"),
        })
        .collect::<Vec<_>>();
    assert!(
        operators
            .iter()
            .any(|operator| operator.contains("HashAgg")),
        "{plan:#?}"
    );
    assert!(
        operators
            .iter()
            .any(|operator| operator.contains("HashJoin")),
        "{plan:#?}"
    );
    assert!(
        operators
            .iter()
            .all(|operator| !operator.contains("Projection")),
        "{plan:#?}"
    );
    assert_eq!(
        operators
            .iter()
            .filter(|operator| operator.contains("TableRangeScan"))
            .count(),
        3,
        "{plan:#?}"
    );
    assert_eq!(
        operators
            .iter()
            .filter(|operator| operator.contains("Selection"))
            .count(),
        3,
        "{plan:#?}"
    );
    assert!(
        details.iter().all(|detail| !detail.contains("other cond")),
        "{plan:#?}"
    );
    assert!(
        details
            .iter()
            .any(|detail| detail.contains("test.customer.c_balance")),
        "{plan:#?}"
    );
    assert!(
        details.iter().all(|detail| !detail.contains("test.c.")),
        "{plan:#?}"
    );
    assert!(
        details.iter().any(|detail| {
            detail.contains(
                "equal:[eq(test.customer.c_d_id, test.orders.o_d_id) \
                 eq(test.customer.c_id, test.orders.o_c_id)]",
            )
        }),
        "{plan:#?}"
    );
    assert_eq!(
        details
            .iter()
            .filter(|detail| detail.contains("range:[1,1], keep order:false"))
            .count(),
        1,
        "{plan:#?}"
    );
    assert_eq!(&estimates[1..4], ["6.40", "8.00", "15.61"], "{plan:#?}");

    let condition_ten = "SELECT count(*) FROM (\
        SELECT c.c_id,c.c_d_id,c.c_w_id,c.c_balance c1,\
        (SELECT sum(ol_amount) FROM orders,order_line \
         WHERE ol_w_id=o_w_id AND ol_d_id=o_d_id AND ol_o_id=o_id \
         AND ol_delivery_d IS NOT NULL AND o_w_id=1 AND o_d_id=c.c_d_id \
         AND o_c_id=c.c_id) sm,\
        (SELECT sum(h_amount) FROM history WHERE h_c_w_id=1 \
         AND h_c_d_id=c.c_d_id AND h_c_id=c.c_id) smh \
        FROM customer c WHERE c.c_w_id=1) t WHERE c1<>sm-smh";
    assert_eq!(
        run_select_on(condition_ten, &catalog, &ctx).unwrap(),
        vec![vec![Datum::Int(0)]]
    );
    let statement = tidb_parser::parse(condition_ten).unwrap();
    let Stmt::Query(query) = &statement else {
        panic!("not a query");
    };
    let QueryStmt::Select(select) = &**query else {
        panic!("not a SELECT");
    };
    let (_, plan) =
        explain_select_stmt(select, &catalog, "test", &ctx, ExplainFormat::Brief).unwrap();
    let text = |row: &[Datum], column: usize| match &row[column] {
        Datum::Bytes(bytes) => String::from_utf8_lossy(bytes).into_owned(),
        other => format!("{other:?}"),
    };
    assert!(text(&plan[1], 0).contains("HashJoin"), "{plan:#?}");
    assert_ne!(text(&plan[1], 1), "N/A", "{plan:#?}");
    assert!(
        plan.iter().any(|row| text(row, 0).contains("IndexLookUp")),
        "{plan:#?}"
    );
    assert!(
        plan.iter().any(|row| {
            text(row, 0).contains("HashAgg")
                && text(row, 2) == "cop[tikv]"
                && text(row, 4).contains("sum(test.history.h_amount)")
        }),
        "{plan:#?}"
    );
    assert!(
        plan.iter()
            .all(|row| { !text(row, 4).contains("funcs:firstrow(test.customer.c_w_id)") }),
        "{plan:#?}"
    );

    // The production TPCC schema also carries the ordered covering index that
    // lets the decorrelated outer join preserve (warehouse, district,
    // customer) order. Keep the smaller fixture above on its original
    // primary-only access contract, then add the production path before the
    // ten-warehouse assertions.
    let TableEntry::Kv(orders) = catalog.get_mut_in("test", "orders").unwrap() else {
        panic!("orders is not a KV table");
    };
    orders.add_index(crate::kv_table::KvIndex {
        id: 2,
        name: "idx_order".to_owned(),
        comment: String::new(),
        unique: false,
        prefix_lengths: vec![crate::ddl::index_prefix::UNSPECIFIED_LENGTH; 4],
        column_offsets: vec![0, 1, 3, 2],
        visible: true,
        global: false,
        clustered_primary: false,
    }, false);
    scale_analyzed_tpcc_table(
        &mut catalog,
        "customer",
        300_000,
        &[("c_w_id", 10), ("c_d_id", 10), ("c_id", 3_000)],
        &ctx,
    );
    scale_analyzed_tpcc_table(
        &mut catalog,
        "orders",
        300_000,
        &[
            ("o_w_id", 10),
            ("o_d_id", 10),
            ("o_id", 3_000),
            ("o_c_id", 3_000),
        ],
        &ctx,
    );
    scale_analyzed_tpcc_table(
        &mut catalog,
        "order_line",
        4_075_321,
        &[
            ("ol_w_id", 10),
            ("ol_d_id", 10),
            ("ol_o_id", 3_000),
            ("ol_number", 15),
        ],
        &ctx,
    );
    scale_analyzed_tpcc_table(
        &mut catalog,
        "history",
        300_000,
        &[("h_c_id", 3_000), ("h_c_d_id", 10), ("h_c_w_id", 10)],
        &ctx,
    );
    // The captured ten-warehouse ANALYZE sampled 299,995 rows for the
    // h_c_w_id column/index while h_c_id's persisted histogram totals 300,000.
    // Go fully loads the predicate column and its index, leaves h_c_id evicted,
    // and makes EstimateColumnNDV borrow that 299,995-row denominator.
    let (history_id, h_c_w_id, h_c_w_index) = {
        let TableEntry::Kv(history) = catalog.get_in("test", "history").unwrap() else {
            panic!("history is not a KV table");
        };
        (
            history.table_id,
            history
                .visible_columns()
                .iter()
                .find(|column| column.name == "h_c_w_id")
                .unwrap()
                .id,
            history
                .indexes()
                .iter()
                .find(|index| index.name == "idx_h_c_w_id")
                .unwrap()
                .id,
        )
    };
    let mut history_stats = catalog
        .table_statistics(history_id)
        .map(|stats| (**stats).clone())
        .unwrap();
    for histogram in [
        &mut history_stats.columns.get_mut(&h_c_w_id).unwrap().histogram,
        &mut history_stats
            .indexes
            .get_mut(&h_c_w_index)
            .unwrap()
            .histogram,
    ] {
        let bucket = histogram.buckets.last_mut().unwrap();
        bucket.count = 299_995;
        bucket.repeat = 29_702;
    }
    catalog.set_table_statistics(history_id, std::sync::Arc::new(history_stats));
    catalog.clear_dirty_content();

    let statement = tidb_parser::parse(sql).unwrap();
    let Stmt::Query(query) = &statement else {
        panic!("not a query");
    };
    let QueryStmt::Select(select) = &**query else {
        panic!("not a SELECT");
    };
    let (_, analyzed_twelve) =
        explain_select_stmt(select, &catalog, "test", &ctx, ExplainFormat::Brief).unwrap();
    let analyzed_text = |row: &[Datum], column: usize| match &row[column] {
        Datum::Bytes(bytes) => String::from_utf8_lossy(bytes).into_owned(),
        other => format!("{other:?}"),
    };
    assert_eq!(
        analyzed_text(&analyzed_twelve[0], 0),
        "HashAgg",
        "{analyzed_twelve:#?}"
    );
    assert_eq!(
        analyzed_text(&analyzed_twelve[0], 4),
        "funcs:count(1)->Column#0",
        "{analyzed_twelve:#?}"
    );
    assert!(
        analyzed_twelve
            .iter()
            .any(|row| analyzed_text(row, 0).contains("MergeJoin")),
        "{analyzed_twelve:#?}"
    );
    assert!(
        analyzed_twelve.iter().any(|row| {
            analyzed_text(row, 0).contains("IndexReader")
                && analyzed_text(row, 4).contains("Selection")
        }),
        "orders must use Go's ordered covering index: {analyzed_twelve:#?}"
    );
    let twelve_index_join = analyzed_twelve
        .iter()
        .find(|row| analyzed_text(row, 0).contains("IndexHashJoin"))
        .expect("condition 12 IndexHashJoin");
    let twelve_index_join_detail = analyzed_text(twelve_index_join, 4);
    assert!(
        twelve_index_join_detail.contains(
            "outer key:test.orders.o_d_id, test.orders.o_id, test.orders.o_w_id, \
             inner key:test.order_line.ol_d_id, test.order_line.ol_o_id, \
             test.order_line.ol_w_id"
        ),
        "condition 12 access keys must retain logical equality order: \
         {twelve_index_join_detail}"
    );

    let statement = tidb_parser::parse(condition_ten).unwrap();
    let Stmt::Query(query) = &statement else {
        panic!("not a query");
    };
    let QueryStmt::Select(select) = &**query else {
        panic!("not a SELECT");
    };
    let (_, analyzed_ten) =
        explain_select_stmt(select, &catalog, "test", &ctx, ExplainFormat::Brief).unwrap();
    assert_eq!(
        analyzed_text(&analyzed_ten[0], 0),
        "HashAgg",
        "{analyzed_ten:#?}"
    );
    assert_eq!(
        analyzed_text(&analyzed_ten[0], 4),
        "funcs:count(1)->Column#0",
        "{analyzed_ten:#?}"
    );
    assert!(
        analyzed_ten
            .iter()
            .any(|row| analyzed_text(row, 0).contains("MergeJoin")),
        "{analyzed_ten:#?}"
    );
    let history_hash = analyzed_ten
        .iter()
        .position(|row| {
            analyzed_text(row, 0).contains("HashAgg")
                && analyzed_text(row, 2) == "cop[tikv]"
                && analyzed_text(row, 4).contains("sum(test.history.h_amount)")
        })
        .expect("history cop HashAgg");
    assert!(
        history_hash + 1 < analyzed_ten.len()
            && analyzed_text(&analyzed_ten[history_hash + 1], 0).contains("Selection")
            && analyzed_text(&analyzed_ten[history_hash + 1], 2) == "cop[tikv]",
        "history warehouse filter must stay below the cop HashAgg: {analyzed_ten:#?}"
    );
    let root_hash_join = analyzed_ten
        .iter()
        .position(|row| analyzed_text(row, 0).contains("HashJoin"))
        .expect("condition 10 root HashJoin");
    assert_eq!(
        analyzed_text(&analyzed_ten[root_hash_join], 1),
        "297.03",
        "condition 10 must use Go's evicted-column NDV denominator: {analyzed_ten:#?}"
    );
    assert_eq!(
        analyzed_text(&analyzed_ten[root_hash_join], 1),
        analyzed_text(&analyzed_ten[root_hash_join + 1], 1),
        "Go does not apply SelectionFactor to the grouped history relation: {analyzed_ten:#?}"
    );
    assert!(
        analyzed_text(&analyzed_ten[root_hash_join + 1], 0).contains("HashAgg(Build)")
            && analyzed_text(&analyzed_ten[root_hash_join + 1], 4)
                .starts_with("group by:test.history.h_c_d_id"),
        "Go builds condition 10's root HashJoin from grouped history: {analyzed_ten:#?}"
    );
    let history_info = analyzed_text(&analyzed_ten[root_hash_join + 1], 4);
    let district_first_row = history_info
        .find("funcs:firstrow(test.history.h_c_d_id)")
        .expect("history district FIRST_ROW carrier");
    let customer_first_row = history_info
        .find("funcs:firstrow(test.history.h_c_id)")
        .expect("history customer FIRST_ROW carrier");
    assert!(
        district_first_row < customer_first_row,
        "decorrelation must retain Go's correlation-condition carrier order: {analyzed_ten:#?}"
    );
    let ten_index_join = analyzed_ten
        .iter()
        .find(|row| analyzed_text(row, 0).contains("IndexHashJoin"))
        .expect("condition 10 nested IndexHashJoin");
    assert!(
        analyzed_text(ten_index_join, 4).contains(
            "outer key:test.orders.o_d_id, test.orders.o_id, test.orders.o_w_id, \
             inner key:test.order_line.ol_d_id, test.order_line.ol_o_id, \
             test.order_line.ol_w_id"
        ),
        "condition 10 access keys must retain logical equality order: {analyzed_ten:#?}"
    );
}

/// Uncorrelated subqueries are evaluated and folded into literals, the way
/// Go's handleScalarSubquery does for the non-Apply case.
#[test]
fn subqueries() {
    let mut catalog = Catalog::default();
    crate::run_create_table_on("CREATE TABLE s (a BIGINT, b BIGINT)", &mut catalog).unwrap();
    crate::run_create_table_on("CREATE TABLE u (a BIGINT)", &mut catalog).unwrap();
    run_insert_on(
        "INSERT INTO s VALUES (1, 10), (2, 20), (3, 30)",
        &mut catalog,
        &crate::StmtContext::for_query(),
    )
    .unwrap();
    run_insert_on(
        "INSERT INTO u VALUES (2), (3)",
        &mut catalog,
        &crate::StmtContext::for_query(),
    )
    .unwrap();

    // A scalar subquery in the select list and in a predicate.
    assert_eq!(
        run_select_on(
            "SELECT (SELECT MAX(b) FROM s)",
            &catalog,
            &crate::StmtContext::for_query()
        )
        .unwrap(),
        vec![vec![Datum::Int(30)]]
    );
    assert_eq!(
        run_select_on(
            "SELECT a FROM s WHERE b = (SELECT MAX(b) FROM s)",
            &catalog,
            &crate::StmtContext::for_query()
        )
        .unwrap(),
        vec![vec![Datum::Int(3)]]
    );

    // No rows is NULL, as Go's buildMaxOneRow leaves it.
    assert_eq!(
        run_select_on(
            "SELECT (SELECT a FROM s WHERE a > 100)",
            &catalog,
            &crate::StmtContext::for_query()
        )
        .unwrap(),
        vec![vec![Datum::Null]]
    );
    // More than one row is Go's ER_SUBQUERY_NO_1_ROW.
    assert!(matches!(
        run_select_on(
            "SELECT (SELECT a FROM s)",
            &catalog,
            &crate::StmtContext::for_query()
        ),
        Err(DriverError::SubqueryReturnsMoreThanOneRow)
    ));

    // IN / NOT IN over a subquery.
    assert_eq!(
        run_select_on(
            "SELECT a FROM s WHERE a IN (SELECT a FROM u)",
            &catalog,
            &crate::StmtContext::for_query()
        )
        .unwrap(),
        vec![vec![Datum::Int(2)], vec![Datum::Int(3)]]
    );
    assert_eq!(
        run_select_on(
            "SELECT a FROM s WHERE a NOT IN (SELECT a FROM u)",
            &catalog,
            &crate::StmtContext::for_query()
        )
        .unwrap(),
        vec![vec![Datum::Int(1)]]
    );
    // An empty IN subquery matches nothing, and NOT IN over it matches all.
    assert_eq!(
        run_select_on(
            "SELECT a FROM s WHERE a IN (SELECT a FROM u WHERE a > 100)",
            &catalog,
            &crate::StmtContext::for_query()
        )
        .unwrap(),
        Vec::<Vec<Datum>>::new()
    );
    assert_eq!(
        run_select_on(
            "SELECT a FROM s WHERE a NOT IN (SELECT a FROM u WHERE a > 100)",
            &catalog,
            &crate::StmtContext::for_query()
        )
        .unwrap()
        .len(),
        3
    );

    // EXISTS / NOT EXISTS fold to 1 / 0.
    assert_eq!(
        run_select_on(
            "SELECT a FROM s WHERE EXISTS (SELECT 1 FROM u)",
            &catalog,
            &crate::StmtContext::for_query()
        )
        .unwrap()
        .len(),
        3
    );
    assert_eq!(
        run_select_on(
            "SELECT a FROM s WHERE NOT EXISTS (SELECT 1 FROM u)",
            &catalog,
            &crate::StmtContext::for_query()
        )
        .unwrap(),
        Vec::<Vec<Datum>>::new()
    );

    // A subquery in HAVING, over the aggregate path.
    assert_eq!(
        run_select_on(
            "SELECT a FROM s GROUP BY a HAVING SUM(b) > (SELECT MIN(b) FROM s)",
            &catalog,
            &crate::StmtContext::for_query()
        )
        .unwrap(),
        vec![vec![Datum::Int(2)], vec![Datum::Int(3)]]
    );

    // ANY is the OR chain over the folded values, ALL the AND chain:
    // `a > ANY (2, 3)` holds only for 3, and `a > ALL (2, 3)` for nothing.
    assert_eq!(
        run_select_on(
            "SELECT a FROM s WHERE a > ANY (SELECT a FROM u)",
            &catalog,
            &crate::StmtContext::for_query()
        )
        .unwrap(),
        vec![vec![Datum::Int(3)]]
    );
    assert_eq!(
        run_select_on(
            "SELECT a FROM s WHERE a > ALL (SELECT a FROM u)",
            &catalog,
            &crate::StmtContext::for_query()
        )
        .unwrap(),
        Vec::<Vec<Datum>>::new()
    );
    // An empty inner result: ALL is vacuously true, ANY is false.
    assert_eq!(
        run_select_on(
            "SELECT a FROM s WHERE a > ALL (SELECT a FROM u WHERE a > 100)",
            &catalog,
            &crate::StmtContext::for_query()
        )
        .unwrap(),
        vec![
            vec![Datum::Int(1)],
            vec![Datum::Int(2)],
            vec![Datum::Int(3)]
        ]
    );
    assert_eq!(
        run_select_on(
            "SELECT a FROM s WHERE a > ANY (SELECT a FROM u WHERE a > 100)",
            &catalog,
            &crate::StmtContext::for_query()
        )
        .unwrap(),
        Vec::<Vec<Datum>>::new()
    );

    // Go rewrites a correlated scalar subquery before join reorder. The row
    // source must therefore leave this predicate above the outer join for the
    // existing Apply/decorrelation path, rather than treating columns inside
    // the subquery as a multi-table `OtherCondition` of that join.
    for table in [
        "CREATE TABLE outer_a (id BIGINT)",
        "CREATE TABLE outer_b (id BIGINT, v BIGINT)",
        "CREATE TABLE outer_c (id BIGINT)",
    ] {
        crate::run_create_table_on(table, &mut catalog).unwrap();
    }
    let sql = "SELECT outer_a.id FROM outer_a, outer_b, outer_c \
        WHERE outer_a.id = outer_b.id AND outer_b.id = outer_c.id \
        AND outer_b.v = (SELECT MIN(v) FROM outer_b \
        WHERE outer_a.id = outer_b.id AND outer_b.id = outer_c.id)";
    let statement = tidb_parser::parse(sql).unwrap();
    let Stmt::Query(query) = &statement else {
        panic!("not a query");
    };
    let QueryStmt::Select(select) = &**query else {
        panic!("not a SELECT");
    };
    crate::explain::explain_select_stmt(
        select,
        &catalog,
        "test",
        &crate::StmtContext::for_query(),
        crate::explain::ExplainFormat::Brief,
    )
    .expect("the correlated scalar subquery remains a residual until Apply rewriting");

    for table in [
        "CREATE TABLE tpch_orders (o_orderkey BIGINT PRIMARY KEY CLUSTERED, \
         o_custkey BIGINT, o_orderstatus VARCHAR(1), o_totalprice DECIMAL(15,2), \
         o_orderdate DATE, o_orderpriority VARCHAR(16), o_clerk VARCHAR(16), \
         o_shippriority BIGINT, o_comment VARCHAR(128))",
        "CREATE TABLE tpch_lineitem (l_orderkey BIGINT, l_linenumber BIGINT, \
         l_commitdate DATE, l_receiptdate DATE, \
         PRIMARY KEY (l_orderkey, l_linenumber) CLUSTERED)",
    ] {
        crate::run_create_table_on(table, &mut catalog).unwrap();
    }
    scale_analyzed_tpcc_table(
        &mut catalog,
        "tpch_orders",
        1_500_000,
        &[
            ("o_orderkey", 1_500_000),
            ("o_orderdate", 2_406),
            ("o_orderpriority", 5),
        ],
        &crate::StmtContext::for_query(),
    );
    scale_analyzed_tpcc_table(
        &mut catalog,
        "tpch_lineitem",
        6_001_215,
        &[
            ("l_orderkey", 1_500_000),
            ("l_linenumber", 7),
            ("l_commitdate", 2_466),
            ("l_receiptdate", 2_554),
        ],
        &crate::StmtContext::for_query(),
    );
    let sql = "SELECT o_orderpriority, COUNT(*) FROM tpch_orders \
        WHERE o_orderdate >= '1995-01-01' \
        AND o_orderdate < DATE_ADD('1995-01-01', INTERVAL '3' MONTH) \
        AND EXISTS (SELECT * FROM tpch_lineitem \
        WHERE tpch_lineitem.l_orderkey = tpch_orders.o_orderkey \
        AND l_commitdate < l_receiptdate) GROUP BY o_orderpriority ORDER BY o_orderpriority";
    let statement = tidb_parser::parse(sql).unwrap();
    let Stmt::Query(query) = &statement else {
        panic!("not a query");
    };
    let QueryStmt::Select(select) = &**query else {
        panic!("not a SELECT");
    };
    let (_, plan) = crate::explain::explain_select_stmt(
        select,
        &catalog,
        "test",
        &crate::StmtContext::for_query(),
        crate::explain::ExplainFormat::Brief,
    )
    .expect("the correlated EXISTS reaches semi-join decorrelation before scalar rewriting");
    let text = |row: &[Datum], column: usize| match &row[column] {
        Datum::Bytes(bytes) => String::from_utf8_lossy(bytes).into_owned(),
        other => format!("{other:?}"),
    };
    let operators = plan.iter().map(|row| text(row, 0)).collect::<Vec<_>>();
    assert_eq!(
        &operators[..3],
        ["Sort", "└─Projection", "  └─HashAgg"],
        "Go restores SELECT-field order between the grouped HashAgg and Sort: {plan:#?}",
    );
    let aggregate_info = plan
        .iter()
        .find(|row| text(row, 0).contains("HashAgg"))
        .map(|row| text(row, 4))
        .expect("q4 has a grouped HashAgg");
    let count_position = aggregate_info
        .find("funcs:count(1)")
        .expect("q4 HashAgg contains COUNT(1)");
    let first_row_position = aggregate_info
        .find("funcs:firstrow(test.tpch_orders.o_orderpriority)")
        .expect("q4 HashAgg carries its group column with FIRST_ROW");
    assert!(
        count_position < first_row_position,
        "physical HashAgg states precede FIRST_ROW group carriers: {plan:#?}",
    );
    assert!(
        plan.iter().any(|row| {
            text(row, 0).contains("Join")
                && text(row, 4)
                    .starts_with("semi join, inner:TableReader, left side:TableReader, outer key:")
                && text(row, 4).contains("tpch_orders.o_orderkey")
                && text(row, 4).contains("tpch_lineitem.l_orderkey")
        }),
        "Go decorrelates EXISTS into a semi join over the compact orders schema: {plan:#?}"
    );
    let semi_join = plan
        .iter()
        .find(|row| text(row, 0).contains("Join") && text(row, 4).starts_with("semi join"))
        .expect("q4 has a physical semi join");
    let semi_index = plan
        .iter()
        .position(|row| std::ptr::eq(row, semi_join))
        .expect("q4 semi join row is part of the plan");
    let build_rows = plan[semi_index + 1..]
        .iter()
        .find(|row| text(row, 0).contains("TableReader(Build)"))
        .map(|row| text(row, 1).parse::<f64>().unwrap())
        .expect("q4 has a preserved build reader");
    let semi_rows = text(semi_join, 1).parse::<f64>().unwrap();
    assert!(
        (semi_rows - build_rows * crate::plan_trace::SELECTIVITY_FACTOR).abs() < 0.02,
        "the semi selectivity must be applied once to q4's filtered orders: {plan:#?}",
    );
    assert!(
        plan.iter().any(|row| {
            text(row, 0).contains("Selection") && text(row, 4).contains("tpch_orders.o_orderdate")
        }),
        "the outer date range must stay below the semi join: {plan:#?}"
    );
    assert!(
        plan.iter().any(|row| {
            text(row, 0).contains("Selection")
                && text(row, 4).contains("tpch_lineitem.l_commitdate")
                && text(row, 4).contains("tpch_lineitem.l_receiptdate")
        }),
        "the inner residual predicate must stay below the semi join: {plan:#?}"
    );
    assert!(
        plan.iter().any(|row| {
            text(row, 0).contains("TableRangeScan")
                && text(row, 3).contains("table:tpch_lineitem")
                && text(row, 4).contains("range: decided by")
        }),
        "the inner clustered key must be rebuilt from each outer order key: {plan:#?}"
    );

    for table in [
        "CREATE TABLE wait_supplier (s_suppkey BIGINT PRIMARY KEY CLUSTERED, \
         s_name VARCHAR(32), s_nationkey BIGINT)",
        "CREATE TABLE wait_lineitem (l_orderkey BIGINT, l_linenumber BIGINT, \
         l_suppkey BIGINT, l_commitdate DATE, l_receiptdate DATE, \
         PRIMARY KEY (l_orderkey, l_linenumber) CLUSTERED)",
        "CREATE TABLE wait_orders (o_orderkey BIGINT PRIMARY KEY CLUSTERED, \
         o_orderstatus VARCHAR(1))",
        "CREATE TABLE wait_nation (n_nationkey BIGINT PRIMARY KEY CLUSTERED, \
         n_name VARCHAR(32))",
    ] {
        crate::run_create_table_on(table, &mut catalog).unwrap();
    }
    for (table, rows, ndvs) in [
        (
            "wait_supplier",
            10_000,
            vec![
                ("s_suppkey", 10_000),
                ("s_name", 10_000),
                ("s_nationkey", 25),
            ],
        ),
        (
            "wait_lineitem",
            6_001_215,
            vec![
                ("l_orderkey", 1_500_000),
                ("l_linenumber", 7),
                ("l_suppkey", 10_000),
                ("l_commitdate", 2_466),
                ("l_receiptdate", 2_554),
            ],
        ),
        (
            "wait_orders",
            1_500_000,
            vec![("o_orderkey", 1_500_000), ("o_orderstatus", 3)],
        ),
        ("wait_nation", 25, vec![("n_nationkey", 25), ("n_name", 25)]),
    ] {
        scale_analyzed_tpcc_table(
            &mut catalog,
            table,
            rows,
            &ndvs,
            &crate::StmtContext::for_query(),
        );
    }
    let statement = tidb_parser::parse(
        "SELECT o_orderkey, COUNT(*) AS numwait FROM wait_orders \
         WHERE o_orderstatus = 'F' AND o_orderkey < 100000 \
         AND EXISTS (SELECT * FROM wait_lineitem l2 \
          WHERE l2.l_orderkey = wait_orders.o_orderkey) \
         AND NOT EXISTS (SELECT * FROM wait_lineitem l3 \
          WHERE l3.l_orderkey = wait_orders.o_orderkey \
          AND l3.l_receiptdate > l3.l_commitdate) \
         GROUP BY o_orderkey ORDER BY numwait DESC, o_orderkey LIMIT 100",
    )
    .unwrap();
    let Stmt::Query(query) = &statement else {
        panic!("not a query");
    };
    let QueryStmt::Select(select) = &**query else {
        panic!("not a SELECT");
    };
    let (_, plan) = crate::explain::explain_select_stmt(
        select,
        &catalog,
        "test",
        &crate::StmtContext::for_query(),
        crate::explain::ExplainFormat::Brief,
    )
    .expect("both q21 EXISTS predicates decorrelate into physical semi joins");
    let semi_joins = plan
        .iter()
        .filter(|row| text(row, 0).contains("IndexHashJoin") && text(row, 4).contains("semi join"))
        .collect::<Vec<_>>();
    assert_eq!(
        semi_joins.len(),
        2,
        "each decorrelated predicate must use ordinary physical join search: {plan:#?}",
    );
    assert!(
        semi_joins
            .iter()
            .any(|row| text(row, 4).starts_with("semi join, inner:TableReader")),
        "the positive EXISTS must become an index semi join: {plan:#?}",
    );
    assert!(
        semi_joins
            .iter()
            .any(|row| text(row, 4).starts_with("anti semi join, inner:TableReader")),
        "the NOT EXISTS must become an index anti-semi join: {plan:#?}",
    );
    for alias in ["l2", "l3"] {
        assert!(
            plan.iter().any(|row| {
                text(row, 0).contains("TableRangeScan")
                    && text(row, 3) == format!("table:{alias}")
                    && text(row, 4).contains("range: decided by")
            }),
            "the {alias} clustered key must be rebuilt for each outer order: {plan:#?}",
        );
    }
    let semi_rows = semi_joins
        .iter()
        .map(|row| text(row, 1).parse::<f64>().unwrap())
        .collect::<Vec<_>>();
    assert!(
        (semi_rows[0] - semi_rows[1] * crate::plan_trace::SELECTIVITY_FACTOR).abs() < 0.02,
        "the outer anti-semi join must derive from the inner semi join: {plan:#?}",
    );
    let statement = tidb_parser::parse(
        "SELECT s_name, COUNT(*) AS numwait \
         FROM wait_supplier, wait_lineitem l1, wait_orders, wait_nation \
         WHERE s_suppkey = l1.l_suppkey AND o_orderkey = l1.l_orderkey \
         AND o_orderstatus = 'F' AND l1.l_receiptdate > l1.l_commitdate \
         AND EXISTS (SELECT * FROM wait_lineitem l2 \
          WHERE l2.l_orderkey = l1.l_orderkey AND l2.l_suppkey <> l1.l_suppkey) \
         AND NOT EXISTS (SELECT * FROM wait_lineitem l3 \
          WHERE l3.l_orderkey = l1.l_orderkey AND l3.l_suppkey <> l1.l_suppkey \
          AND l3.l_receiptdate > l3.l_commitdate) \
         AND s_nationkey = n_nationkey AND n_name = 'EGYPT' \
         GROUP BY s_name ORDER BY numwait DESC, s_name LIMIT 100",
    )
    .unwrap();
    let Stmt::Query(query) = &statement else {
        panic!("not a query");
    };
    let QueryStmt::Select(select) = &**query else {
        panic!("not a SELECT");
    };
    let reordered = crate::driver::join_reorder::reorder(
        select.from.as_ref().unwrap(),
        select,
        select.where_clause.as_ref(),
        &catalog,
        "test",
        &crate::StmtContext::for_query(),
    )
    .expect("subquery conjuncts must not block reordering their outer join group");
    assert_eq!(
        reordered.written_order,
        vec![1, 2, 3, 0],
        "Go reorders q21's outer group before physicalizing its two semi joins",
    );
    let (_, q21_plan) = crate::explain::explain_select_stmt(
        select,
        &catalog,
        "test",
        &crate::StmtContext::for_query(),
        crate::explain::ExplainFormat::Brief,
    )
    .expect("q21's reordered outer group remains explainable");
    assert!(
        q21_plan.iter().any(|row| {
            text(row, 0).contains("Selection")
                && text(row, 4).contains(
                    "gt(test.wait_lineitem.l_receiptdate, test.wait_lineitem.l_commitdate)",
                )
        }),
        "projection elimination must preserve q21's base-table identity: {q21_plan:#?}",
    );
    assert!(
        q21_plan
            .iter()
            .all(|row| !text(row, 4).contains("test.l1.")),
        "q21's SQL alias must not replace its physical source identity: {q21_plan:#?}",
    );

    for table in [
        "CREATE TABLE mix_customer (id BIGINT, balance DECIMAL(12,2))",
        "CREATE TABLE mix_orders (customer_id BIGINT)",
        "CREATE TABLE mix_part (id BIGINT)",
        "CREATE TABLE mix_supply (part_id BIGINT, supplier_id BIGINT, avail_qty BIGINT)",
        "CREATE TABLE mix_line (part_id BIGINT, supplier_id BIGINT, qty BIGINT)",
    ] {
        crate::run_create_table_on(table, &mut catalog).unwrap();
    }
    // Go rewrites each subquery node in expression order. An uncorrelated
    // scalar/IN sibling is therefore folded even when another sibling in the
    // same WHERE is correlated and must remain for Apply decorrelation.
    for sql in [
        "SELECT prefix, COUNT(*) FROM (SELECT id AS prefix, balance \
         FROM mix_customer WHERE balance > (SELECT AVG(balance) FROM mix_customer) \
         AND NOT EXISTS (SELECT 1 FROM mix_orders \
         WHERE mix_orders.customer_id = mix_customer.id)) AS eligible \
         GROUP BY prefix ORDER BY prefix",
        "SELECT supplier_id FROM mix_supply \
         WHERE part_id IN (SELECT id FROM mix_part) \
         AND avail_qty > (SELECT 0.5 * SUM(qty) FROM mix_line \
         WHERE mix_line.part_id = mix_supply.part_id \
         AND mix_line.supplier_id = mix_supply.supplier_id)",
    ] {
        let statement = tidb_parser::parse(sql).unwrap();
        let Stmt::Query(query) = &statement else {
            panic!("not a query");
        };
        let QueryStmt::Select(select) = &**query else {
            panic!("not a SELECT");
        };
        crate::explain::explain_select_stmt(
            select,
            &catalog,
            "test",
            &crate::StmtContext::for_query(),
            crate::explain::ExplainFormat::Brief,
        )
        .unwrap_or_else(|error| {
            panic!(
                "mixed correlated and uncorrelated subqueries are rewritten independently: \
                 {sql}: {error:?}"
            )
        });
    }
}

/// A correlated subquery becomes an Apply: the inner query re-runs once
/// per outer row with the outer row's values bound, which is Go's
/// NestedLoopApplyExec loop.
#[test]
fn correlated_subqueries() {
    let mut catalog = Catalog::default();
    crate::run_create_table_on("CREATE TABLE o (id BIGINT, v BIGINT)", &mut catalog).unwrap();
    crate::run_create_table_on("CREATE TABLE i (id BIGINT, w BIGINT)", &mut catalog).unwrap();
    run_insert_on(
        "INSERT INTO o VALUES (1, 10), (2, 20), (3, 30)",
        &mut catalog,
        &crate::StmtContext::for_query(),
    )
    .unwrap();
    run_insert_on(
        "INSERT INTO i VALUES (1, 10), (2, 5), (2, 25), (4, 40)",
        &mut catalog,
        &crate::StmtContext::for_query(),
    )
    .unwrap();

    // Scalar: each outer row compares against its own inner maximum.
    assert_eq!(
        run_select_on(
            "SELECT id FROM o WHERE v = (SELECT MAX(w) FROM i WHERE i.id = o.id)",
            &catalog,
            &crate::StmtContext::for_query()
        )
        .unwrap(),
        vec![vec![Datum::Int(1)]]
    );
    // id 2's inner rows are 5 and 25, so its max is 25 and 20 < 25 holds;
    // id 1 compares 10 < 10, which does not.
    assert_eq!(
        run_select_on(
            "SELECT id FROM o WHERE v < (SELECT MAX(w) FROM i WHERE i.id = o.id)",
            &catalog,
            &crate::StmtContext::for_query()
        )
        .unwrap(),
        vec![vec![Datum::Int(2)]]
    );
    // An outer row whose inner query returns nothing compares against
    // NULL, so the predicate is unknown and the row drops -- id 3 has no
    // matching inner rows.
    assert_eq!(
        run_select_on(
            "SELECT id FROM o WHERE (SELECT MAX(w) FROM i WHERE i.id = o.id) IS NULL",
            &catalog,
            &crate::StmtContext::for_query()
        )
        .unwrap(),
        vec![vec![Datum::Int(3)]]
    );

    // Correlated EXISTS / NOT EXISTS, the semi- and anti-join shapes.
    assert_eq!(
        run_select_on(
            "SELECT id FROM o WHERE EXISTS (SELECT 1 FROM i WHERE i.id = o.id)",
            &catalog,
            &crate::StmtContext::for_query()
        )
        .unwrap(),
        vec![vec![Datum::Int(1)], vec![Datum::Int(2)]]
    );
    assert_eq!(
        run_select_on(
            "SELECT id FROM o WHERE NOT EXISTS (SELECT 1 FROM i WHERE i.id = o.id)",
            &catalog,
            &crate::StmtContext::for_query()
        )
        .unwrap(),
        vec![vec![Datum::Int(3)]]
    );
    assert_eq!(
        run_select_on(
            "SELECT id FROM o WHERE EXISTS (SELECT 1 FROM i WHERE i.id = o.id) \
             AND NOT EXISTS (SELECT 1 FROM i WHERE i.id = o.id AND i.w > 20) \
             ORDER BY id",
            &catalog,
            &crate::StmtContext::for_query()
        )
        .unwrap(),
        vec![vec![Datum::Int(1)]]
    );

    // An unqualified inner reference to an outer column still correlates.
    assert_eq!(
        run_select_on(
            "SELECT id FROM o WHERE EXISTS (SELECT 1 FROM i WHERE i.w = v)",
            &catalog,
            &crate::StmtContext::for_query()
        )
        .unwrap(),
        vec![vec![Datum::Int(1)]]
    );

    // A correlated subquery returning several rows is still the 1242 case,
    // raised from inside the apply loop and reported as the same error the
    // folded path reports.
    assert!(matches!(
        run_select_on(
            "SELECT id FROM o WHERE v = (SELECT w FROM i WHERE i.id = o.id)",
            &catalog,
            &crate::StmtContext::for_query()
        ),
        Err(DriverError::SubqueryReturnsMoreThanOneRow)
    ));

    // Correlated IN / NOT IN and ANY / ALL: the same Apply, folding this
    // outer row's inner result into the three-valued answer. id 3's inner
    // result is EMPTY, which is why NOT IN and ALL keep it.
    assert_eq!(
        run_select_on(
            "SELECT id FROM o WHERE v IN (SELECT w FROM i WHERE i.id = o.id)",
            &catalog,
            &crate::StmtContext::for_query()
        )
        .unwrap(),
        vec![vec![Datum::Int(1)]]
    );
    assert_eq!(
        run_select_on(
            "SELECT id FROM o WHERE v NOT IN (SELECT w FROM i WHERE i.id = o.id)",
            &catalog,
            &crate::StmtContext::for_query()
        )
        .unwrap(),
        vec![vec![Datum::Int(2)], vec![Datum::Int(3)]]
    );
    assert_eq!(
        run_select_on(
            "SELECT id FROM o WHERE v > ANY (SELECT w FROM i WHERE i.id = o.id)",
            &catalog,
            &crate::StmtContext::for_query()
        )
        .unwrap(),
        vec![vec![Datum::Int(2)]]
    );
    assert_eq!(
        run_select_on(
            "SELECT id FROM o WHERE v > ALL (SELECT w FROM i WHERE i.id = o.id)",
            &catalog,
            &crate::StmtContext::for_query()
        )
        .unwrap(),
        vec![vec![Datum::Int(3)]]
    );
}

/// A correlated subquery nested inside a larger aggregate-path
/// expression: arithmetic over an aggregate in the select list, and a
/// comparison against an aggregate in `HAVING`. The Apply sits above the
/// aggregation (Go's plan shape), so the subquery sees the GROUPED value
/// and runs once per group.
///
/// Every result here was cross-checked against a
/// `testkit.CreateMockStore` capture of real TiDB on the same schema.
#[test]
fn grouped_correlated_subqueries() {
    let mut catalog = Catalog::default();
    crate::run_create_table_on("CREATE TABLE t (g BIGINT, v BIGINT)", &mut catalog).unwrap();
    crate::run_create_table_on("CREATE TABLE s (k BIGINT, x BIGINT)", &mut catalog).unwrap();
    run_insert_on(
        "INSERT INTO t VALUES (1, 10), (1, 20), (2, 5), (3, 100), (NULL, 7)",
        &mut catalog,
        &crate::StmtContext::for_query(),
    )
    .unwrap();
    run_insert_on(
        "INSERT INTO s VALUES (1, 1), (1, 2), (2, 3)",
        &mut catalog,
        &crate::StmtContext::for_query(),
    )
    .unwrap();

    // A correlated scalar subquery combined with an aggregate by
    // arithmetic in the select list: SUM(v) is the group's own total,
    // (SELECT COUNT...) reads how many `s` rows share the group's key.
    assert_eq!(
        run_select_on(
            "SELECT g, SUM(v) + (SELECT COUNT(*) FROM s WHERE s.k = t.g) \
             FROM t GROUP BY g ORDER BY g",
            &catalog,
            &crate::StmtContext::for_query()
        )
        .unwrap(),
        vec![
            vec![
                Datum::Null,
                Datum::Decimal(tidb_datatype::Decimal::from_int(7))
            ],
            vec![
                Datum::Int(1),
                Datum::Decimal(tidb_datatype::Decimal::from_int(32))
            ],
            vec![
                Datum::Int(2),
                Datum::Decimal(tidb_datatype::Decimal::from_int(6))
            ],
            vec![
                Datum::Int(3),
                Datum::Decimal(tidb_datatype::Decimal::from_int(100))
            ],
        ]
    );

    // A correlated scalar subquery compared against an aggregate in
    // HAVING: only groups whose SUM(v) beats the correlated average
    // survive.
    assert_eq!(
        run_select_on(
            "SELECT g FROM t GROUP BY g \
             HAVING SUM(v) > (SELECT AVG(x) FROM s WHERE s.k = t.g) \
             ORDER BY g",
            &catalog,
            &crate::StmtContext::for_query()
        )
        .unwrap(),
        vec![vec![Datum::Int(1)], vec![Datum::Int(2)]]
    );

    // The same HAVING subquery, ANDed with a plain grouped-column
    // predicate -- both conjuncts must be readable off the same
    // post-Apply row.
    assert_eq!(
        run_select_on(
            "SELECT g, SUM(v) FROM t GROUP BY g \
             HAVING SUM(v) > (SELECT COUNT(*) FROM s WHERE s.k = t.g) AND g IS NOT NULL \
             ORDER BY g",
            &catalog,
            &crate::StmtContext::for_query()
        )
        .unwrap(),
        vec![
            vec![
                Datum::Int(1),
                Datum::Decimal(tidb_datatype::Decimal::from_int(30))
            ],
            vec![
                Datum::Int(2),
                Datum::Decimal(tidb_datatype::Decimal::from_int(5))
            ],
            vec![
                Datum::Int(3),
                Datum::Decimal(tidb_datatype::Decimal::from_int(100))
            ],
        ]
    );

    // HAVING a correlated subquery against a bare (unaggregated) GROUP
    // BY column, with a NULL group in the mix.
    assert_eq!(
        run_select_on(
            "SELECT g, COUNT(*) FROM t GROUP BY g \
             HAVING (SELECT COUNT(*) FROM s WHERE s.k = g) >= 0 \
             ORDER BY g",
            &catalog,
            &crate::StmtContext::for_query()
        )
        .unwrap(),
        vec![
            vec![Datum::Null, Datum::Int(1)],
            vec![Datum::Int(1), Datum::Int(2)],
            vec![Datum::Int(2), Datum::Int(1)],
            vec![Datum::Int(3), Datum::Int(1)],
        ]
    );

    // A correlated subquery inside an AGGREGATE'S OWN ARGUMENT: the Apply runs
    // per SOURCE row BELOW the aggregation (stage 5b), so group `1`'s two rows
    // each contribute the 2 matching `s` rows and the group sums to 4 -- a
    // per-GROUP Apply would have given 2. The NULL group's `s.k = NULL`
    // matches nothing, so `COUNT(*)` is 0 there, not NULL. Captured from Go:
    // `<nil>|0;1|4;2|1;3|0`.
    assert_eq!(
        run_select_on(
            "SELECT g, SUM((SELECT COUNT(*) FROM s WHERE s.k = g)) FROM t GROUP BY g ORDER BY g",
            &catalog,
            &crate::StmtContext::for_query()
        )
        .unwrap(),
        vec![
            vec![
                Datum::Null,
                Datum::Decimal(tidb_datatype::Decimal::from_int(0))
            ],
            vec![
                Datum::Int(1),
                Datum::Decimal(tidb_datatype::Decimal::from_int(4))
            ],
            vec![
                Datum::Int(2),
                Datum::Decimal(tidb_datatype::Decimal::from_int(1))
            ],
            vec![
                Datum::Int(3),
                Datum::Decimal(tidb_datatype::Decimal::from_int(0))
            ],
        ]
    );
    // The correlated EXISTS is evaluated for every source row before SUM,
    // matching Go's Apply below the aggregation.
    assert_eq!(
        run_select_on(
            "SELECT g, SUM(CASE WHEN EXISTS(SELECT 1 FROM s WHERE s.k = t.g) THEN v ELSE 0 END) \
             FROM t GROUP BY g ORDER BY g",
            &catalog,
            &crate::StmtContext::for_query()
        )
        .unwrap(),
        vec![
            vec![
                Datum::Null,
                Datum::Decimal(tidb_datatype::Decimal::from_int(0).with_declared_shape(41, 0),),
            ],
            vec![
                Datum::Int(1),
                Datum::Decimal(tidb_datatype::Decimal::from_int(30).with_declared_shape(41, 0),),
            ],
            vec![
                Datum::Int(2),
                Datum::Decimal(tidb_datatype::Decimal::from_int(5).with_declared_shape(41, 0),),
            ],
            vec![
                Datum::Int(3),
                Datum::Decimal(tidb_datatype::Decimal::from_int(0).with_declared_shape(41, 0),),
            ],
        ]
    );

    // A HAVING clause referencing a non-grouped, non-aggregated column
    // stays refused even with a correlated subquery alongside it -- the
    // subquery does not launder the column reference. Captured from
    // TiDB, this is `ErrUnknownColumn` naming the `having clause` (HAVING
    // resolves against the aggregation's output), in every sql_mode.
    assert!(matches!(
        run_select_on(
            "SELECT g, SUM(v) FROM t GROUP BY g \
             HAVING v > (SELECT AVG(x) FROM s WHERE s.k = t.g)",
            &catalog,
            &crate::StmtContext::for_query()
        ),
        Err(DriverError::UnknownColumnInClause { .. })
    ));

    // Go decorrelates both levels: the AVG groups the innermost `s2` by
    // `s.k`, and the COUNT then groups its surviving rows by `t.g`.
    assert_eq!(
        run_select_on(
            "SELECT g, (SELECT COUNT(*) FROM s WHERE s.k = t.g \
             AND s.x > (SELECT AVG(x) FROM s s2 WHERE s2.k = s.k)) \
             FROM t GROUP BY g ORDER BY g",
            &catalog,
            &crate::StmtContext::for_query(),
        )
        .unwrap(),
        vec![
            vec![Datum::Null, Datum::Int(0)],
            vec![Datum::Int(1), Datum::Int(1)],
            vec![Datum::Int(2), Datum::Int(0)],
            vec![Datum::Int(3), Datum::Int(0)],
        ]
    );
}

/// A subquery does not launder a `HAVING` column reference: the name it
/// CORRELATES to answers to the same scope rule as one written in the clause
/// directly, and TiDB reports it under the same `having clause`.
///
/// Go reaches this without a second pass --
/// `havingWindowAndOrderbyExprResolver.Enter` returns `skipChildren` for a
/// subquery, so the correlated name is bound later against the outer plan,
/// which at `HAVING` time is the aggregation's output.
///
/// Captured from real TiDB on `ht(a, b)` = (1,1),(2,2) and `hs(x, y)` = (1,5):
///
/// ```text
/// select a from ht group by a having (select y from hs where hs.x = ht.b) > 0;
///   [planner:1054]Unknown column 'ht.b' in 'having clause'
/// select a from ht group by a having exists (select 1 from hs where hs.x = ht.b);
///   [planner:1054]Unknown column 'ht.b' in 'having clause'
/// select a from ht group by a having a in (select x from hs where hs.y = ht.b);
///   [planner:1054]Unknown column 'ht.b' in 'having clause'
/// select max(b) from ht having (select y from hs where hs.x = ht.b) > 0;
///   [planner:1054]Unknown column 'ht.b' in 'having clause'
/// select a from ht group by a having (select y from hs where hs.x = ht.a) > 0;  -- 1
/// select a from ht group by a having (select count(*) from hs) > 0;             -- 1;2
/// select a, b from ht having (select y from hs where hs.x = ht.b) > 0;          -- 1|1
/// ```
#[test]
fn a_having_subquery_may_only_correlate_to_the_aggregations_output() {
    let mut catalog = Catalog::default();
    crate::run_create_table_on("CREATE TABLE ht (a INT, b INT)", &mut catalog).unwrap();
    crate::run_create_table_on("CREATE TABLE hs (x INT, y INT)", &mut catalog).unwrap();
    run_insert_on(
        "INSERT INTO ht VALUES (1, 1), (2, 2)",
        &mut catalog,
        &crate::StmtContext::for_query(),
    )
    .unwrap();
    run_insert_on(
        "INSERT INTO hs VALUES (1, 5)",
        &mut catalog,
        &crate::StmtContext::for_query(),
    )
    .unwrap();

    // `b` is neither grouped nor in the select list, so no spelling of the
    // subquery reaches it -- and the name is reported AS WRITTEN.
    for sql in [
        "SELECT a FROM ht GROUP BY a HAVING (SELECT y FROM hs WHERE hs.x = ht.b) > 0",
        "SELECT a FROM ht GROUP BY a HAVING EXISTS (SELECT 1 FROM hs WHERE hs.x = ht.b)",
        "SELECT a FROM ht GROUP BY a HAVING a IN (SELECT x FROM hs WHERE hs.y = ht.b)",
        "SELECT max(b) FROM ht HAVING (SELECT y FROM hs WHERE hs.x = ht.b) > 0",
    ] {
        match run_select_on(sql, &catalog, &crate::StmtContext::for_query()) {
            Err(DriverError::UnknownColumnInClause { column, clause }) => {
                assert_eq!(
                    (column.as_str(), clause.as_str()),
                    ("ht.b", "having clause"),
                    "{sql}"
                );
            }
            other => panic!("expected 1054 for `{sql}`, got {other:?}"),
        }
    }

    // A grouped column, an UNcorrelated subquery, and a column the select list
    // carries are all still reachable -- the refusal is about the scope, not
    // about subqueries in `HAVING`.
    assert!(run_select_on(
        "SELECT a FROM ht GROUP BY a HAVING (SELECT y FROM hs WHERE hs.x = ht.a) > 0",
        &catalog,
        &crate::StmtContext::for_query()
    )
    .is_ok());
    assert!(run_select_on(
        "SELECT a FROM ht GROUP BY a HAVING (SELECT count(*) FROM hs) > 0",
        &catalog,
        &crate::StmtContext::for_query()
    )
    .is_ok());
    assert!(run_select_on(
        "SELECT a, b FROM ht HAVING (SELECT y FROM hs WHERE hs.x = ht.b) > 0",
        &catalog,
        &crate::StmtContext::for_query()
    )
    .is_ok());
}

/// Go's `rule_decorrelate` turns a correlated `EXISTS` in the WHERE into
/// a SEMI JOIN, and it does so under an `Aggregation` exactly as under a
/// plain SELECT. This port ran that rule only on the plain path, so an
/// aggregate over a correlated EXISTS reached the expression rewriter
/// with the subquery still in the tree and failed as an unsupported form
/// (1105) -- while the same predicate without the aggregate, and the same
/// aggregate over a NON-correlated EXISTS, both worked.
///
/// A semi join emits left rows only, so the schema the aggregate resolves
/// against is unchanged -- which is what makes running the rule here safe
/// for the group keys and the aggregate arguments alike.
#[test]
fn an_aggregate_over_a_correlated_exists_decorrelates() {
    let mut catalog = Catalog::default();
    let ctx = crate::StmtContext::for_query();
    crate::run_create_table_on("CREATE TABLE w (g VARCHAR(5), v INT)", &mut catalog).unwrap();
    run_insert_on(
        "INSERT INTO w VALUES ('a',1),('a',2),('b',3),('b',4),('c',NULL)",
        &mut catalog,
        &ctx,
    )
    .unwrap();

    // The NULL row matches no `b.v = a.v`, so four of the five survive.
    assert_eq!(
        run_select_on(
            "SELECT count(*) FROM w a WHERE EXISTS (SELECT 1 FROM w b WHERE b.v = a.v)",
            &catalog,
            &ctx
        )
        .unwrap(),
        vec![vec![Datum::Int(4)]],
    );

    // NOT EXISTS keeps exactly the row the semi join dropped.
    assert_eq!(
        run_select_on(
            "SELECT count(*) FROM w a WHERE NOT EXISTS (SELECT 1 FROM w b WHERE b.v = a.v)",
            &catalog,
            &ctx
        )
        .unwrap(),
        vec![vec![Datum::Int(1)]],
    );

    // The grouped form resolves its group key against the same schema.
    let grouped: Vec<Vec<String>> = run_select_on(
        "SELECT g, sum(v) FROM w a WHERE EXISTS (SELECT 1 FROM w b WHERE b.v = a.v) \
         GROUP BY g ORDER BY g",
        &catalog,
        &ctx,
    )
    .unwrap()
    .into_iter()
    .map(|row| {
        row.into_iter()
            .map(|datum| match datum {
                Datum::String(text) => String::from_utf8_lossy(text.bytes()).into_owned(),
                Datum::Bytes(bytes) => String::from_utf8_lossy(&bytes).into_owned(),
                Datum::Decimal(value) => value.to_string(),
                other => format!("{other:?}"),
            })
            .collect()
    })
    .collect();
    assert_eq!(grouped, [["a", "3"], ["b", "7"]]);
}

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
/// Go `handleScalarSubQuery` optimizes an uncorrelated subquery with the
/// builder's own optimizer flags and the same statistics as the enclosing
/// statement (`DoOptimize(ctx, planCtx.builder.ctx, planCtx.builder.optFlag,
/// np)`, `expression_rewriter.go`), so a join inside it is planned exactly
/// as it would be standalone: partsupp keeps its 800k rows and the filtered
/// supplier side is the hash-join build. TPC-H Q11's HAVING subquery was
/// planned with estimates that belonged to other nodes (partsupp 64000) and
/// chose an IndexJoin driving 800k lookups.
#[test]
fn an_uncorrelated_join_subquery_with_a_filter_keeps_its_table_statistics() {
    let mut catalog = Catalog::default();
    for table in [
        "CREATE TABLE supplier (s_suppkey BIGINT PRIMARY KEY CLUSTERED, \
         s_nationkey BIGINT NOT NULL, s_acctbal DECIMAL(15,2) NOT NULL)",
        "CREATE TABLE partsupp (ps_partkey BIGINT NOT NULL, ps_suppkey BIGINT NOT NULL, \
         ps_supplycost DECIMAL(15,2) NOT NULL, PRIMARY KEY (ps_partkey, ps_suppkey) CLUSTERED)",
    ] {
        crate::run_create_table_on(table, &mut catalog).unwrap();
    }
    let ctx = crate::StmtContext::for_query();
    for insert in [
        "INSERT INTO supplier VALUES (1, 3, 100.00)",
        "INSERT INTO partsupp VALUES (1, 1, 10.00)",
    ] {
        run_insert_on(insert, &mut catalog, &ctx).unwrap();
    }
    scale_analyzed_tpcc_table(
        &mut catalog,
        "supplier",
        10_000,
        &[("s_suppkey", 10_000), ("s_nationkey", 25)],
        &ctx,
    );
    scale_analyzed_tpcc_table(
        &mut catalog,
        "partsupp",
        800_000,
        &[
            ("ps_partkey", 196_960),
            ("ps_suppkey", 10_000),
            ("ps_supplycost", 99_865),
        ],
        &ctx,
    );
    let text = |row: &[Datum], column: usize| match &row[column] {
        Datum::Bytes(bytes) => String::from_utf8_lossy(bytes).into_owned(),
        other => format!("{other:?}"),
    };
    let explain = |sql: &str| {
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
        .unwrap();
        plan.iter()
            .map(|row| (text(row, 0), text(row, 1), text(row, 3)))
            .collect::<Vec<_>>()
    };
    let inner = "SELECT SUM(ps_supplycost) * 0.0001 FROM partsupp, supplier \
                 WHERE ps_suppkey = s_suppkey AND s_nationkey = 3";
    let standalone = explain(inner);
    let nested = explain(&format!(
        "SELECT ps_partkey FROM partsupp WHERE ps_supplycost > ({inner})"
    ));
    let subquery_part = nested
        .iter()
        .skip_while(|(id, _, _)| !id.starts_with("ScalarSubQuery"))
        .collect::<Vec<_>>();
    assert!(
        !subquery_part.is_empty(),
        "nested plan lists the subquery: {nested:?}"
    );
    let rows_of = |rows: &[&(String, String, String)], table: &str| {
        rows.iter()
            .find(|(id, _, object)| id.contains("TableFullScan") && object == table)
            .map(|(_, rows, _)| rows.clone())
            .unwrap_or_else(|| panic!("no full scan of {table} in {rows:?}"))
    };
    let standalone_rows = standalone.iter().collect::<Vec<_>>();
    assert_eq!(
        rows_of(&subquery_part, "table:partsupp"),
        rows_of(&standalone_rows, "table:partsupp"),
        "the nested subquery scans partsupp with the table's own statistics\n{nested:#?}"
    );
    assert_eq!(rows_of(&subquery_part, "table:partsupp"), "800000.00");
    assert!(
        subquery_part
            .iter()
            .any(|(id, _, _)| id.contains("HashJoin"))
            && !subquery_part
                .iter()
                .any(|(id, _, _)| id.contains("IndexJoin") || id.contains("IndexHashJoin")),
        "the nested subquery keeps the standalone hash join\n{nested:#?}"
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
        (anti_join_rows - filtered_left_rows * tidb_planner::cost_factors::SELECTION_FACTOR).abs()
            < 0.02,
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
    // Pinned Go probe on this ANALYZED fixture (`outer_t` 10000 rows / k NDV
    // 10000, `inner_t` 10000 rows / k NDV 500, empty `inner_u`):
    // `IndexJoin -> HashAgg(Build) -> ... -> TableFullScan` plus a
    // `TableRangeScan(Probe)`. The correlate suite's recorded `HashJoin`
    // came from a PSEUDO-statistics fixture whose dedup aggregate is 7992
    // rows; at this fixture's 500-row aggregate Go's own cost model prefers
    // the index join. The assertion pins the dedup rewrite, not the join
    // family.
    assert!(
        non_unique_operators
            .iter()
            .any(|operator| operator.starts_with("IndexJoin")),
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
    // The same Go probe picks a `MergeJoin` over the unique key's full scans;
    // the IN rewrite still drops the deduplication aggregate.
    assert!(
        unique_operators
            .iter()
            .any(|operator| operator.starts_with("MergeJoin")),
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
        (selection_rows - aggregate_rows * tidb_planner::cost_factors::SELECTION_FACTOR).abs()
            < 0.02,
        "LogicalSelection.DeriveStats scales its aggregate child by SelectivityFactor: \
         selection={selection_rows}, aggregate={aggregate_rows}, expected={}; \
         {nested_plan:#?}",
        aggregate_rows * tidb_planner::cost_factors::SELECTION_FACTOR
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

/// A correlated `EXISTS` in a SELECT field plans as a left-outer-semi Apply, so
/// it answers exactly one row per OUTER row no matter how many inner rows
/// match. The apply executor feeds the joiner one inner row per call so a
/// single output chunk can be filled incrementally; it must therefore stop at
/// the settling row exactly as Go's `inners.ReachEnd()` does. Without that,
/// every extra matching inner row appended the outer row again, so `t`'s two
/// `g = 1` rows produced four rows here and the grouped `SUM` in
/// [`grouped_correlated_subqueries`] doubled.
#[test]
fn correlated_exists_apply_answers_once_per_outer_row() {
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
    // `g = 1` matches TWO `s` rows and `g = 2` matches one, yet the row count
    // stays `t`'s own: the semi apply settles each outer row on its first
    // match.
    assert_eq!(
        run_select_on(
            "SELECT g, EXISTS(SELECT 1 FROM s WHERE s.k = t.g) FROM t ORDER BY g",
            &catalog,
            &crate::StmtContext::for_query()
        )
        .unwrap(),
        vec![
            vec![Datum::Null, Datum::Int(0)],
            vec![Datum::Int(1), Datum::Int(1)],
            vec![Datum::Int(1), Datum::Int(1)],
            vec![Datum::Int(2), Datum::Int(1)],
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

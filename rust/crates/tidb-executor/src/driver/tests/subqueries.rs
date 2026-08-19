//! Subqueries: uncorrelated, correlated, and correlated inside an aggregate.
//!
//! The correlated cases are the interesting ones -- the inner query is
//! re-evaluated per outer row, and the grouped case pushes that re-evaluation
//! under an aggregate. Mirrors Go `pkg/executor`'s apply and
//! `pkg/planner/core`'s correlated-column handling.

use super::*;

/// EXPLAIN infers a correlated scalar subquery's output type from its plan.
/// It must not execute the inner query merely to discover that type: on TPCC
/// condition 10 that planning-time scan alone exceeds the protocol timeout.
#[test]
fn explaining_a_correlated_scalar_type_reads_no_storage() {
    use crate::explain::{explain_select_stmt, ExplainFormat};

    let mut catalog = Catalog::default();
    crate::run_create_table_on("CREATE TABLE outer_t (k BIGINT)", &mut catalog).unwrap();
    crate::run_create_table_on(
        "CREATE TABLE inner_t (k BIGINT, v DECIMAL(6,2))",
        &mut catalog,
    )
    .unwrap();
    let ctx = crate::StmtContext::for_query();
    run_insert_on("INSERT INTO inner_t VALUES (1, 2.50)", &mut catalog, &ctx).unwrap();

    let statement = tidb_parser::parse(
        "SELECT (SELECT SUM(v) FROM inner_t WHERE inner_t.k=outer_t.k) FROM outer_t",
    )
    .unwrap();
    let Stmt::Query(query) = &statement else {
        panic!("not a query");
    };
    let QueryStmt::Select(select) = &**query else {
        panic!("not a SELECT");
    };
    let (explained, operations) = crate::storage::capture_storage_ops(|| {
        explain_select_stmt(select, &catalog, "test", &ctx, ExplainFormat::Brief)
    });
    explained.unwrap();
    assert_eq!(operations, crate::storage::StorageOps::default());
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
        });
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
    });
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

    // DEFERRED: two-level nesting (a correlated subquery whose own body
    // contains a subquery correlated to ITS outer scope) is refused
    // rather than mis-evaluated.
    assert!(run_select_on(
        "SELECT g, (SELECT COUNT(*) FROM s WHERE s.k = t.g \
         AND s.x > (SELECT AVG(x) FROM s s2 WHERE s2.k = s.k)) FROM t GROUP BY g",
        &catalog,
        &crate::StmtContext::for_query()
    )
    .is_err());
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

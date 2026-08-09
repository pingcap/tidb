//! Joins between stored tables: the join types, the ON/USING forms, and the
//! row order the result comes back in.
//!
//! Mirrors Go `pkg/executor/join`.

use super::*;

/// Two-table joins: inner, left/right outer with NULL padding, the
/// ON-vs-WHERE distinction, qualified and ambiguous column references,
/// wildcard expansion, and a three-table left-deep chain.
#[test]
fn joins() {
    let mut catalog = Catalog::default();
    crate::run_create_table_on("CREATE TABLE l (id BIGINT, v BIGINT)", &mut catalog).unwrap();
    crate::run_create_table_on("CREATE TABLE r (id BIGINT, w BIGINT)", &mut catalog).unwrap();
    run_insert_on(
        "INSERT INTO l VALUES (1, 10), (2, 20), (3, 30)",
        &mut catalog,
        &crate::StmtContext::for_query(),
    )
    .unwrap();
    run_insert_on(
        "INSERT INTO r VALUES (1, 100), (3, 300), (3, 301)",
        &mut catalog,
        &crate::StmtContext::for_query(),
    )
    .unwrap();

    // INNER JOIN: only matches, and a left row matching twice emits twice.
    assert_eq!(
        run_select_on(
            "SELECT l.id, l.v, r.w FROM l JOIN r ON l.id = r.id",
            &catalog,
            &crate::StmtContext::for_query()
        )
        .unwrap(),
        vec![
            vec![Datum::Int(1), Datum::Int(10), Datum::Int(100)],
            vec![Datum::Int(3), Datum::Int(30), Datum::Int(300)],
            vec![Datum::Int(3), Datum::Int(30), Datum::Int(301)],
        ]
    );

    // LEFT JOIN pads the unmatched left row with NULLs.
    assert_eq!(
        run_select_on(
            "SELECT l.id, r.w FROM l LEFT JOIN r ON l.id = r.id",
            &catalog,
            &crate::StmtContext::for_query()
        )
        .unwrap(),
        vec![
            vec![Datum::Int(1), Datum::Int(100)],
            vec![Datum::Int(2), Datum::Null],
            vec![Datum::Int(3), Datum::Int(300)],
            vec![Datum::Int(3), Datum::Int(301)],
        ]
    );

    // The ON/WHERE distinction: filtering the padded rows is an anti-join.
    assert_eq!(
        run_select_on(
            "SELECT l.id FROM l LEFT JOIN r ON l.id = r.id WHERE r.id IS NULL",
            &catalog,
            &crate::StmtContext::for_query()
        )
        .unwrap(),
        vec![vec![Datum::Int(2)]]
    );
    // A condition in ON does NOT drop the left row; it only stops matching.
    assert_eq!(
        run_select_on(
            "SELECT l.id, r.w FROM l LEFT JOIN r ON l.id = r.id AND r.w > 200",
            &catalog,
            &crate::StmtContext::for_query()
        )
        .unwrap(),
        vec![
            vec![Datum::Int(1), Datum::Null],
            vec![Datum::Int(2), Datum::Null],
            vec![Datum::Int(3), Datum::Int(300)],
            vec![Datum::Int(3), Datum::Int(301)],
        ]
    );

    // RIGHT JOIN keeps every right row, padding the left side.
    assert_eq!(
        run_select_on(
            "SELECT l.v, r.id FROM l RIGHT JOIN r ON l.id = r.id AND l.v > 100",
            &catalog,
            &crate::StmtContext::for_query()
        )
        .unwrap(),
        vec![
            vec![Datum::Null, Datum::Int(1)],
            vec![Datum::Null, Datum::Int(3)],
            vec![Datum::Null, Datum::Int(3)],
        ]
    );

    // A comma join with no ON is a Cartesian product.
    assert_eq!(
        run_select_on(
            "SELECT l.id FROM l, r",
            &catalog,
            &crate::StmtContext::for_query()
        )
        .unwrap()
        .len(),
        9
    );

    // `*` expands across both tables in FROM order; `t.*` over one.
    assert_eq!(
        run_select_on(
            "SELECT * FROM l JOIN r ON l.id = r.id",
            &catalog,
            &crate::StmtContext::for_query()
        )
        .unwrap()
        .first()
        .unwrap()
        .len(),
        4
    );
    assert_eq!(
        run_select_on(
            "SELECT r.* FROM l JOIN r ON l.id = r.id",
            &catalog,
            &crate::StmtContext::for_query()
        )
        .unwrap()
        .first()
        .unwrap()
        .len(),
        2
    );

    // An unqualified column present in both tables is ambiguous, as in
    // MySQL; one present in only one table resolves.
    assert!(run_select_on(
        "SELECT id FROM l JOIN r ON l.id = r.id",
        &catalog,
        &crate::StmtContext::for_query()
    )
    .is_err());
    assert_eq!(
        run_select_on(
            "SELECT v, w FROM l JOIN r ON l.id = r.id",
            &catalog,
            &crate::StmtContext::for_query()
        )
        .unwrap()
        .len(),
        3
    );

    // An alias replaces the table name for qualification.
    assert_eq!(
        run_select_on(
            "SELECT a.id FROM l AS a JOIN r AS b ON a.id = b.id",
            &catalog,
            &crate::StmtContext::for_query()
        )
        .unwrap()
        .len(),
        3
    );

    // A three-table left-deep chain, and an aggregate over a join.
    crate::run_create_table_on("CREATE TABLE m (id BIGINT)", &mut catalog).unwrap();
    run_insert_on(
        "INSERT INTO m VALUES (3)",
        &mut catalog,
        &crate::StmtContext::for_query(),
    )
    .unwrap();
    assert_eq!(
        run_select_on(
            "SELECT COUNT(*) FROM l JOIN r ON l.id = r.id JOIN m ON m.id = r.id",
            &catalog,
            &crate::StmtContext::for_query()
        )
        .unwrap(),
        vec![vec![Datum::Int(2)]]
    );

    // A coalesced join reports `id` ONCE, so `*` is one column narrower
    // than the same join written with an `ON`, and the unqualified `id`
    // that is ambiguous above resolves here. See
    // `tidb_session`'s `tests_coalesced_joins` for the full rule set.
    for sql in [
        "SELECT * FROM l NATURAL JOIN r",
        "SELECT * FROM l JOIN r USING (id)",
    ] {
        assert_eq!(
            run_select_on(sql, &catalog, &crate::StmtContext::for_query())
                .unwrap()
                .first()
                .unwrap()
                .len(),
            3,
            "{sql}"
        );
    }
    assert_eq!(
        run_select_on(
            "SELECT id FROM l JOIN r USING (id)",
            &catalog,
            &crate::StmtContext::for_query()
        )
        .unwrap()
        .len(),
        3
    );
}

#[test]
fn tpcc_check_five_keeps_only_the_cross_leaf_residual() {
    use crate::explain::{explain_select_stmt, ExplainFormat};

    let mut catalog = Catalog::default();
    crate::run_create_table_on(
        "CREATE TABLE orders (o_id INT NOT NULL, o_d_id INT NOT NULL, o_w_id INT NOT NULL, o_c_id INT, o_entry_d DATETIME, o_carrier_id INT, o_ol_cnt INT, o_all_local INT, PRIMARY KEY (o_w_id,o_d_id,o_id), KEY idx_order(o_w_id,o_d_id,o_c_id,o_id))",
        &mut catalog,
    )
    .unwrap();
    crate::run_create_table_on(
        "CREATE TABLE new_order (no_o_id INT NOT NULL, no_d_id INT NOT NULL, no_w_id INT NOT NULL, PRIMARY KEY (no_w_id,no_d_id,no_o_id))",
        &mut catalog,
    )
    .unwrap();
    let sql = "SELECT count(*) FROM orders LEFT JOIN new_order ON no_w_id=o_w_id AND o_d_id=no_d_id AND o_id=no_o_id WHERE o_w_id=1 AND ((o_carrier_id IS NULL and no_o_id IS NULL) OR (o_carrier_id IS NOT NULL and no_o_id IS NOT NULL))";
    let stmt = tidb_parser::parse(sql).unwrap();
    let Stmt::Query(query) = &stmt else {
        panic!("not query")
    };
    let QueryStmt::Select(select) = &**query else {
        panic!("not select")
    };
    let (_, rows) = explain_select_stmt(
        select,
        &catalog,
        "test",
        &crate::StmtContext::for_query(),
        ExplainFormat::Row,
    )
    .unwrap();
    let plan = rows
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
        plan.iter().any(|line| line.contains("MergeJoin")),
        "{plan:?}"
    );
    assert_eq!(
        plan.iter()
            .filter(|line| line
                .split('\t')
                .next()
                .is_some_and(|id| id.contains("TableReader")))
            .count(),
        2,
        "both ordered scans are TiKV tasks behind root readers: {plan:?}",
    );
    assert!(
        plan.iter().any(|line| {
            line.contains("or(and(isnull(test.orders.o_carrier_id)")
                && !line.contains("eq(test.orders.o_w_id, 1)")
        }),
        "the root Selection retains only the cross-leaf residue: {plan:?}",
    );
    assert!(
        plan.iter().any(|line| {
            line.contains(
                "or(isnull(test.orders.o_carrier_id), not(isnull(test.orders.o_carrier_id)))",
            )
        }),
        "Go's DNF weakening is evaluated at the preserved orders leaf: {plan:?}",
    );
}

#[test]
fn tpcc_check_seven_propagates_the_warehouse_range_to_both_leaves() {
    let mut catalog = Catalog::default();
    crate::run_create_table_on(
        "CREATE TABLE orders (o_id INT NOT NULL, o_d_id INT NOT NULL, o_w_id INT NOT NULL, o_carrier_id INT, PRIMARY KEY (o_w_id,o_d_id,o_id))",
        &mut catalog,
    )
    .unwrap();
    crate::run_create_table_on(
        "CREATE TABLE order_line (ol_o_id INT NOT NULL, ol_d_id INT NOT NULL, ol_w_id INT NOT NULL, ol_number INT NOT NULL, ol_delivery_d DATETIME, PRIMARY KEY (ol_w_id,ol_d_id,ol_o_id,ol_number))",
        &mut catalog,
    )
    .unwrap();
    // The cluster catalog carries a clustered PRIMARY as both the common
    // handle and a plan access path. The in-memory DDL catalog stores only
    // the former, so mirror the metadata the parity fixture exposes.
    for (table_name, column_offsets) in
        [("orders", vec![2, 1, 0]), ("order_line", vec![2, 1, 0, 3])]
    {
        let TableEntry::Kv(table) = catalog.get_mut_in("test", table_name).unwrap() else {
            panic!("{table_name} is not a KV table");
        };
        table.add_index(crate::kv_table::KvIndex {
            id: 1,
            name: "PRIMARY".to_owned(),
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
        "INSERT INTO orders VALUES (1,1,1,7),(2,1,1,NULL)",
        &mut catalog,
        &ctx,
    )
    .unwrap();
    run_insert_on(
        "INSERT INTO order_line VALUES \
            (1,1,1,1,NULL),(1,1,1,2,'2026-01-01 00:00:00'),\
            (2,1,1,1,'2026-01-01 00:00:00'),(2,1,1,2,NULL)",
        &mut catalog,
        &ctx,
    )
    .unwrap();
    let sql = "SELECT count(*) FROM orders, order_line WHERE o_id=ol_o_id AND o_d_id=ol_d_id AND ol_w_id=o_w_id AND o_w_id=1 AND ((ol_delivery_d IS NULL and o_carrier_id IS NOT NULL) or (o_carrier_id IS NULL and ol_delivery_d IS NOT NULL))";
    let stmt = tidb_parser::parse(sql).unwrap();
    let Stmt::Query(query) = &stmt else {
        panic!("not query")
    };
    let QueryStmt::Select(select) = &**query else {
        panic!("not select")
    };
    let rows = crate::driver::join_reorder::row_source(
        select.from.as_ref().expect("FROM"),
        select.where_clause.as_ref(),
        &catalog,
        "test",
        &crate::StmtContext::for_query(),
    )
    .expect("the TPCC join group is modelled");
    let order_line = rows.filters_for("order_line").expect("order_line leaf");
    assert!(
        order_line.iter().any(|filter| {
            let restored = filter.restore();
            restored.contains("`order_line`.`ol_w_id`=1") || restored.contains("`ol_w_id`=1")
        }),
        "o_w_id=1 must propagate through the equality edge: {order_line:?}",
    );
    assert_eq!(
        run_select_on(sql, &catalog, &ctx).unwrap(),
        vec![vec![Datum::Int(2)]],
        "the cross-leaf disjunction must be evaluated exactly once by the join",
    );

    let (_, rows) = crate::explain::explain_select_stmt(
        select,
        &catalog,
        "test",
        &ctx,
        crate::explain::ExplainFormat::Brief,
    )
    .unwrap();
    let plan: Vec<String> = rows
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
        .collect();
    assert!(
        plan.iter().any(|line| {
            line.contains("MergeJoin")
                && line.contains("other cond:or(and(isnull(test.order_line.ol_delivery_d)")
        }),
        "the cross-leaf disjunction must be reported as the merge join's other condition: {plan:?}",
    );
    for table in ["orders", "order_line"] {
        assert!(
            plan.iter().any(|line| {
                line.contains("TableRangeScan") && line.contains(&format!("table:{table}"))
            }),
            "the propagated warehouse key must bound {table}'s common-handle scan: {plan:?}",
        );
        assert!(
            !plan.iter().any(|line| {
                line.contains("TableFullScan") && line.contains(&format!("table:{table}"))
            }),
            "the bounded {table} leaf must not fall back to a full scan: {plan:?}",
        );
    }
}

/// Every leaf of a join is costed, and a leaf whose parents read only the
/// columns an index covers reads that index instead of the table -- Go's
/// `findBestTask` recursing into each `DataSource` below a `LogicalJoin`.
///
/// The second half is the safety argument this seam rests on, asserted rather
/// than argued: the whole-index read is over the SAME rows the table scan
/// reads, so the join's answer -- values and multiplicity, including the
/// unmatched row a `LEFT JOIN` pads -- is byte-for-byte what it was before
/// any leaf had a choice. The `SELECT *` control is the other side of it: a
/// wildcard needs the columns no index carries, so that leaf keeps its scan.
#[test]
fn a_join_leaf_reads_a_covering_index_without_moving_a_row() {
    use crate::explain::{explain_select_stmt, ExplainFormat};

    let mut catalog = Catalog::default();
    crate::run_create_table_on(
        "CREATE TABLE jl (a INT PRIMARY KEY, b INT, c VARCHAR(32), KEY(b))",
        &mut catalog,
    )
    .unwrap();
    crate::run_create_table_on(
        "CREATE TABLE jr (a INT PRIMARY KEY, b INT, c VARCHAR(32), KEY(b))",
        &mut catalog,
    )
    .unwrap();
    let ctx = crate::StmtContext::for_query();
    run_insert_on(
        "INSERT INTO jl VALUES (1, 10, 'x'), (2, 20, 'y'), (3, 30, 'z')",
        &mut catalog,
        &ctx,
    )
    .unwrap();
    run_insert_on(
        "INSERT INTO jr VALUES (7, 10, 'p'), (8, 30, 'q')",
        &mut catalog,
        &ctx,
    )
    .unwrap();

    let plan_of = |sql: &str| {
        let stmt = tidb_parser::parse(sql).unwrap();
        let Stmt::Query(query) = &stmt else {
            panic!("not a query");
        };
        let QueryStmt::Select(select) = &**query else {
            panic!("not a SELECT");
        };
        let (_, rows) =
            explain_select_stmt(select, &catalog, "test", &ctx, ExplainFormat::Row).unwrap();
        rows.iter()
            .map(|row| {
                row.iter()
                    .map(|datum| match datum {
                        Datum::Bytes(bytes) => String::from_utf8_lossy(bytes).into_owned(),
                        other => format!("{other:?}"),
                    })
                    .collect::<Vec<_>>()
                    .join("\t")
            })
            .collect::<Vec<_>>()
    };

    // `jl.b` and `jl.a` are exactly what the index on `b` carries (the
    // clustered handle rides along), so the leaf costs a single scan of it.
    let covering = "SELECT jl.a FROM jl LEFT JOIN jr ON jl.b = jr.b";
    let plan = plan_of(covering);
    assert!(
        plan.iter()
            .any(|line| line.contains("IndexFullScan") && line.contains("table:jl")),
        "the jl leaf reads its covering index, got {plan:?}"
    );

    // The control: `jl.c` is in no index, so the same leaf keeps its scan.
    let plan = plan_of("SELECT * FROM jl LEFT JOIN jr ON jl.b = jr.b");
    assert!(
        plan.iter()
            .any(|line| line.contains("TableFullScan") && line.contains("table:jl")),
        "a wildcard needs the whole row, so the leaf still scans, got {plan:?}"
    );

    // And the rows are untouched: the padded `jl.a = 2` row is still there
    // exactly once, and `jl.a = 1`/`3` still match exactly once each.
    assert_eq!(
        run_select_on(covering, &catalog, &ctx).unwrap(),
        vec![
            vec![Datum::Int(1)],
            vec![Datum::Int(2)],
            vec![Datum::Int(3)],
        ]
    );
}

/// A predicate that references only one inner-join leaf can narrow that leaf
/// before the join runs. The original WHERE remains above the join, so this
/// checks both the access-path shape and the result-preservation contract.
#[test]
fn a_join_leaf_uses_a_single_table_constant_for_access() {
    use crate::explain::{explain_select_stmt, ExplainFormat};

    let mut catalog = Catalog::default();
    crate::run_create_table_on(
        "CREATE TABLE lp (id INT PRIMARY KEY, value INT)",
        &mut catalog,
    )
    .unwrap();
    crate::run_create_table_on(
        "CREATE TABLE rp (id INT PRIMARY KEY, value INT)",
        &mut catalog,
    )
    .unwrap();
    let ctx = crate::StmtContext::for_query();
    run_insert_on(
        "INSERT INTO lp VALUES (1, 10), (2, 20), (3, 30)",
        &mut catalog,
        &ctx,
    )
    .unwrap();
    run_insert_on(
        "INSERT INTO rp VALUES (7, 20), (8, 20), (9, 30)",
        &mut catalog,
        &ctx,
    )
    .unwrap();

    let sql = "SELECT lp.id, rp.id FROM lp JOIN rp ON lp.value = rp.value WHERE lp.id = 2";
    let stmt = tidb_parser::parse(sql).unwrap();
    let Stmt::Query(query) = &stmt else {
        panic!("not a query");
    };
    let QueryStmt::Select(select) = &**query else {
        panic!("not a SELECT");
    };
    let (_, rows) =
        explain_select_stmt(select, &catalog, "test", &ctx, ExplainFormat::Row).unwrap();
    let plan: Vec<String> = rows
        .iter()
        .map(|row| {
            row.iter()
                .map(|datum| match datum {
                    Datum::Bytes(bytes) => String::from_utf8_lossy(bytes).into_owned(),
                    other => format!("{other:?}"),
                })
                .collect::<Vec<_>>()
                .join("\\t")
        })
        .collect();

    assert!(
        plan.iter()
            .any(|line| line.contains("Point_Get") && line.contains("table:lp")),
        "the lp leaf should use its constant primary-key predicate, got {plan:?}"
    );
    assert_eq!(
        run_select_on(sql, &catalog, &ctx).unwrap(),
        vec![
            vec![Datum::Int(2), Datum::Int(7)],
            vec![Datum::Int(2), Datum::Int(8)],
        ]
    );
}

/// TPCC StockLevel must bound both clustered-primary-key leaves before the
/// join. Scanning either complete table turns this low-frequency transaction
/// into a worker-consuming minute-long query at benchmark scale.
#[test]
fn tpcc_stock_level_bounds_both_join_leaves() {
    use crate::explain::{explain_select_stmt, ExplainFormat};

    let mut catalog = Catalog::default();
    crate::run_create_table_on(
        "CREATE TABLE order_line (\
            ol_o_id INT NOT NULL, ol_d_id INT NOT NULL, ol_w_id INT NOT NULL, \
            ol_number INT NOT NULL, ol_i_id INT NOT NULL, \
            PRIMARY KEY (ol_w_id, ol_d_id, ol_o_id, ol_number) CLUSTERED)",
        &mut catalog,
    )
    .unwrap();
    crate::run_create_table_on(
        "CREATE TABLE stock (\
            s_i_id INT NOT NULL, s_w_id INT NOT NULL, s_quantity INT, \
            PRIMARY KEY (s_w_id, s_i_id) CLUSTERED)",
        &mut catalog,
    )
    .unwrap();
    let ctx = crate::StmtContext::for_query();
    run_insert_on(
        "INSERT INTO order_line VALUES \
            (3627, 7, 1, 1, 100), (3630, 7, 1, 1, 101), \
            (3647, 7, 1, 1, 102), (3630, 8, 1, 1, 101)",
        &mut catalog,
        &ctx,
    )
    .unwrap();
    run_insert_on(
        "INSERT INTO stock VALUES \
            (100, 1, 10), (101, 1, 20), (102, 1, 5), (100, 2, 5)",
        &mut catalog,
        &ctx,
    )
    .unwrap();
    // The cluster catalog exposes TiDB's clustered common-handle PRIMARY as a
    // table access path. The in-memory DDL catalog stores only the handle, so
    // mirror the loaded metadata shape explicitly after loading rows. There
    // are deliberately no separate PRIMARY index entries to fall back to.
    for (table_name, column_offsets) in [("order_line", vec![2, 1, 0, 3]), ("stock", vec![1, 0])] {
        let TableEntry::Kv(table) = catalog.get_mut_in("test", table_name).unwrap() else {
            panic!("{table_name} is not a KV table");
        };
        table.add_index(crate::kv_table::KvIndex {
            id: 1,
            name: "PRIMARY".to_owned(),
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
    let sql = "SELECT /*+ TIDB_INLJ(`order_line`, `stock`)*/ \
        COUNT(DISTINCT (`s_i_id`)) AS `stock_count` \
        FROM (`order_line`) JOIN `stock` \
        WHERE `ol_w_id`=1 AND `ol_d_id`=7 \
        AND `ol_o_id`<3647 AND `ol_o_id`>=3647-20 \
        AND `s_w_id`=1 AND `s_i_id`=`ol_i_id` AND `s_quantity`<18";
    let stmt = tidb_parser::parse(sql).unwrap();
    let Stmt::Query(query) = &stmt else {
        panic!("not a query");
    };
    let QueryStmt::Select(select) = &**query else {
        panic!("not a SELECT");
    };
    let (_, rows) =
        explain_select_stmt(select, &catalog, "test", &ctx, ExplainFormat::Row).unwrap();
    let plan: Vec<String> = rows
        .iter()
        .map(|row| {
            row.iter()
                .map(|datum| match datum {
                    Datum::Bytes(bytes) => String::from_utf8_lossy(bytes).into_owned(),
                    other => format!("{other:?}"),
                })
                .collect::<Vec<_>>()
                .join("\\t")
        })
        .collect();

    assert!(
        plan.iter().any(|line| line.contains("StreamAgg")),
        "COUNT(DISTINCT) over the forced lookup join must stream, got {plan:?}"
    );
    assert!(
        plan.iter().any(|line| line.contains("IndexJoin")),
        "TIDB_INLJ must force the viable clustered-handle lookup, got {plan:?}"
    );

    for table in ["order_line", "stock"] {
        assert!(
            plan.iter().any(|line| {
                line.contains("TableRangeScan") && line.contains(&format!("table:{table}"))
            }),
            "the {table} common handle must be read as a bounded table path, got {plan:?}"
        );
        assert!(
            !plan.iter().any(|line| {
                (line.contains("FullScan") || line.contains("IndexRangeScan"))
                    && line.contains(&format!("table:{table}"))
            }),
            "the {table} leaf must neither scan the complete table nor double-read PRIMARY, got {plan:?}"
        );
    }
    let (result, ops) =
        crate::storage::capture_storage_ops(|| run_select_on(sql, &catalog, &ctx).unwrap());
    assert_eq!(result, vec![vec![Datum::Int(1)]]);
    assert_eq!(ops.scans, 2);
    assert_eq!(
        ops.gets, 2,
        "the two distinct outer item keys must become common-handle point probes"
    );
}

/// The TPCC NewOrder customer/warehouse lookup: a constant on warehouse's
/// join key propagates to the customer's clustered composite primary key, so
/// both leaves are direct lookups rather than full scans.
#[test]
fn tpcc_customer_warehouse_join_uses_two_point_gets() {
    use crate::explain::{explain_select_stmt, ExplainFormat};

    let mut catalog = Catalog::default();
    crate::run_create_table_on(
        "CREATE TABLE customer (\
            c_id INT NOT NULL, c_d_id INT NOT NULL, c_w_id INT NOT NULL, \
            c_discount INT, c_last VARCHAR(16), c_credit VARCHAR(2), \
            PRIMARY KEY (c_w_id, c_d_id, c_id))",
        &mut catalog,
    )
    .unwrap();
    crate::run_create_table_on(
        "CREATE TABLE warehouse (w_id INT PRIMARY KEY, w_tax INT)",
        &mut catalog,
    )
    .unwrap();
    let ctx = crate::StmtContext::for_query();
    run_insert_on(
        "INSERT INTO customer VALUES (629, 6, 1, 5, 'Smith', 'GC')",
        &mut catalog,
        &ctx,
    )
    .unwrap();
    run_insert_on("INSERT INTO warehouse VALUES (1, 7)", &mut catalog, &ctx).unwrap();

    let sql = "SELECT c_discount, c_last, c_credit, w_tax \
        FROM customer, warehouse \
        WHERE w_id = 1 AND c_w_id = w_id AND c_d_id = 6 AND c_id = 629";
    let stmt = tidb_parser::parse(sql).unwrap();
    let Stmt::Query(query) = &stmt else {
        panic!("not a query");
    };
    let QueryStmt::Select(select) = &**query else {
        panic!("not a SELECT");
    };
    let (_, rows) =
        explain_select_stmt(select, &catalog, "test", &ctx, ExplainFormat::Row).unwrap();
    let plan: Vec<String> = rows
        .iter()
        .map(|row| {
            row.iter()
                .map(|datum| match datum {
                    Datum::Bytes(bytes) => String::from_utf8_lossy(bytes).into_owned(),
                    other => format!("{other:?}"),
                })
                .collect::<Vec<_>>()
                .join("\\t")
        })
        .collect();
    for table in ["customer", "warehouse"] {
        assert!(
            plan.iter().any(|line| {
                line.contains("Point_Get") && line.contains(&format!("table:{table}"))
            }),
            "the {table} leaf should be a direct lookup, got {plan:?}"
        );
    }
    assert!(
        !plan.iter().any(|line| line.contains("Selection")),
        "the point keys and join equality consume the complete WHERE, got {plan:?}"
    );
    assert!(
        plan.iter().any(|line| {
            (line.contains("MergeJoin") || line.contains("HashJoin"))
                && line.split("\\t").nth(1) == Some("1.00")
        }),
        "two one-row point leaves produce a one-row join estimate, got {plan:?}"
    );
    assert_eq!(run_select_on(sql, &catalog, &ctx).unwrap().len(), 1);

    let residual_sql = format!("{sql} AND c_credit = 'BC'");
    let stmt = tidb_parser::parse(&residual_sql).unwrap();
    let Stmt::Query(query) = &stmt else {
        panic!("not a query");
    };
    let QueryStmt::Select(select) = &**query else {
        panic!("not a SELECT");
    };
    let (_, rows) =
        explain_select_stmt(select, &catalog, "test", &ctx, ExplainFormat::Row).unwrap();
    assert!(
        rows.iter().any(|row| match &row[0] {
            Datum::Bytes(bytes) => String::from_utf8_lossy(bytes).contains("Selection"),
            _ => false,
        }),
        "a non-key residual predicate must remain above the point join"
    );
    assert!(run_select_on(&residual_sql, &catalog, &ctx)
        .unwrap()
        .is_empty());
}

/// THE ROW-DROP PIN: a parent merge join over a child join that HASHES.
///
/// This is the exact hazard `crate::driver::merge_decision`'s narrowing once
/// existed to prevent, and the reason the narrowing could be removed. The
/// shape:
///
/// ```text
///   mid JOIN top ON mid.a = top.a         -- promise says mid.a is ordered
///   └─ bot JOIN mid ON bot.k = mid.k      -- hashes: k is no table's handle
/// ```
///
/// `PreparePossibleProperties` UNIONS its children's orders, so the bottom
/// join PROMISES `bot.a` and `mid.a` -- both are clustered integer handles.
/// The parent reads that promise and forms a merge join on `mid.a`. But the
/// bottom join hashes (its own key `k` is on no provided order), and a hash
/// join emits its rows in its PROBE side's order, which here is `bot`'s. A
/// merge join over that stream would advance past groups the input never
/// separated and silently DROP rows.
///
/// The VERIFY step is what stops it: both children are built first, each
/// reports what it actually delivers, and the bottom join reports nothing --
/// so the parent falls back to hashing. The assertion below is the full,
/// correct row set. Delete the `merge.filter(...)` in
/// `crate::driver::from::build_join` and this test loses rows.
#[test]
fn a_promise_the_child_cannot_deliver_falls_back_instead_of_dropping_rows() {
    let mut catalog = Catalog::default();
    for ddl in [
        "CREATE TABLE bot (a BIGINT PRIMARY KEY, k BIGINT)",
        "CREATE TABLE mid (a BIGINT PRIMARY KEY, k BIGINT)",
        "CREATE TABLE top (a BIGINT PRIMARY KEY)",
    ] {
        crate::run_create_table_on(ddl, &mut catalog).unwrap();
    }
    let ctx = crate::StmtContext::for_query();
    // `mid` is written so that joining on `k` emits its rows in an order that
    // is NOT `mid.a` ascending -- which is the whole point: the promise says
    // `mid.a`, the hash join delivers `bot.a`.
    for insert in [
        "INSERT INTO bot VALUES (1, 7), (2, 7), (3, 8)",
        "INSERT INTO mid VALUES (10, 8), (20, 7), (30, 7)",
        "INSERT INTO top VALUES (10), (20), (30)",
    ] {
        run_insert_on(insert, &mut catalog, &ctx).unwrap();
    }
    let sql = "SELECT bot.a, mid.a, top.a FROM bot \
        JOIN mid ON bot.k = mid.k \
        JOIN top ON mid.a = top.a";
    let mut rows = run_select_on(sql, &catalog, &ctx).unwrap();
    rows.sort_by_key(|row| format!("{row:?}"));
    // Every `(bot, mid)` pair that shares a `k`, each matched to its one
    // `top` row: `k=7` pairs bot 1,2 with mid 20,30 (four rows) and `k=8`
    // pairs bot 3 with mid 10 (one row).
    assert_eq!(
        rows,
        vec![
            vec![Datum::Int(1), Datum::Int(20), Datum::Int(20)],
            vec![Datum::Int(1), Datum::Int(30), Datum::Int(30)],
            vec![Datum::Int(2), Datum::Int(20), Datum::Int(20)],
            vec![Datum::Int(2), Datum::Int(30), Datum::Int(30)],
            vec![Datum::Int(3), Datum::Int(10), Datum::Int(10)],
        ],
    );
}

/// THE PROMISE-GROWTH PIN: a leaf reports the order its scan WALKS IN, not
/// the orders the catalog says some access path of that table could produce.
///
/// `crate::driver::merge_decision`'s promise/verify contract rests on the
/// verify side reading the BUILD. It once read
/// `merge_decision::table_orders` -- the same function the PROMISE is made
/// from -- which agreed with itself by construction and verified nothing. It
/// happened to be right only because `merge_join_plan::provided_orders`
/// reported exactly the one order a `TableFullScan` walks in.
///
/// MEASURED: growing `provided_orders` into Go's index branch of
/// `PreparePossibleProperties` (`logical_datasource.go:343`) while that
/// coincidence held made this query return TWO rows instead of three -- a
/// merge join formed on `s` over two leaves that were still walking in `h`
/// order, so it advanced past groups its input never separated. Not a worse
/// plan: a wrong answer.
///
/// The tables below are written so handle order and `s` order DISAGREE on
/// both sides, which is what makes the drop observable at all.
///
/// That increment HAS now landed, and this test is what pins the reason it is
/// safe. The leaf asked for `s` order takes the `ks` index -- Go's
/// `convertToIndexScan` under a non-empty property -- and delivers `s` order
/// because it was BUILT with `keep order:true`; the merge join above it then
/// merges a stream that really is grouped by `s`. Both readings, the fallback
/// and the index walk, have to answer three rows, and the assertion below does
/// not care which one ran: a bijection is a bijection. Mutating either half --
/// the order filter in `driver::access::leaf_index_path`, or the
/// `keep_order`-gated `answer_in_index_order` that stops the source reordering its
/// handle batches -- is what makes it fail.
#[test]
fn a_leaf_delivers_only_the_order_its_scan_walks_in() {
    let mut catalog = Catalog::default();
    for ddl in [
        "CREATE TABLE il (h BIGINT PRIMARY KEY, s BIGINT NOT NULL, KEY ks (s))",
        "CREATE TABLE ir (h BIGINT PRIMARY KEY, s BIGINT NOT NULL, KEY ks (s))",
    ] {
        crate::run_create_table_on(ddl, &mut catalog).unwrap();
    }
    let ctx = crate::StmtContext::for_query();
    for insert in [
        "INSERT INTO il VALUES (1, 30), (2, 10), (3, 20)",
        "INSERT INTO ir VALUES (1, 20), (2, 30), (3, 10)",
    ] {
        run_insert_on(insert, &mut catalog, &ctx).unwrap();
    }
    let mut rows = run_select_on(
        "SELECT il.h, ir.h FROM il JOIN ir ON il.s = ir.s",
        &catalog,
        &ctx,
    )
    .unwrap();
    rows.sort_by_key(|row| format!("{row:?}"));
    // Each `s` value appears once on each side, so the join is a bijection:
    // three rows, whatever algorithm runs.
    assert_eq!(
        rows,
        vec![
            vec![Datum::Int(1), Datum::Int(2)],
            vec![Datum::Int(2), Datum::Int(3)],
            vec![Datum::Int(3), Datum::Int(1)],
        ],
    );
}

/// THE ORDERED-LEAF PIN: a leaf asked for an order it cannot walk in handle
/// order answers with the INDEX that walks in it, and says `keep order:true`.
///
/// This is Go's `convertToIndexScan` under a NON-EMPTY property. The leaf used
/// to DELETE its index candidates the moment a parent required an order
/// (`demand.columns.filter(|_| !keep_order)`), so a merge join over an index
/// column could never form and TiDB's own recording of exactly that shape --
/// `tests/integrationtest/r/topn_push_down.result:237`, a `MergeJoin` over two
/// `IndexFullScan ... keep order:true` -- was unreachable here.
///
/// The shape below is that recording's, reduced to one table: `t(a int not
/// null, index idx(a))` self-joined on `a`, whose only order is the index's.
///
/// Two assertions, and the second is the one that makes the first SAFE:
///
///  * the two leaves read `idx` and print `keep order:true`, so the plan is
///    TiDB's;
///  * the rows are the full product, which is what a merge join over a stream
///    that is really grouped by `a` returns. A leaf that printed `keep
///    order:true` while its source reordered handle batches would drop rows
///    here instead -- that is the failure
///    [`a_leaf_delivers_only_the_order_its_scan_walks_in`] measured.
#[test]
fn a_leaf_asked_for_an_index_order_walks_the_index_and_says_so() {
    use crate::explain::{explain_select_stmt, ExplainFormat};

    let mut catalog = Catalog::default();
    crate::run_create_table_on(
        "CREATE TABLE oi (a INT NOT NULL, KEY idx (a))",
        &mut catalog,
    )
    .unwrap();
    let ctx = crate::StmtContext::for_query();
    run_insert_on(
        "INSERT INTO oi VALUES (1), (2), (2), (3)",
        &mut catalog,
        &ctx,
    )
    .unwrap();

    let sql = "SELECT t1.a, t2.a FROM oi t1 JOIN oi t2 ON t1.a = t2.a";
    let stmt = tidb_parser::parse(sql).unwrap();
    let Stmt::Query(query) = &stmt else {
        panic!("not a query");
    };
    let QueryStmt::Select(select) = &**query else {
        panic!("not a SELECT");
    };
    let (_, rows) =
        explain_select_stmt(select, &catalog, "test", &ctx, ExplainFormat::Row).unwrap();
    let plan: Vec<String> = rows
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
        .collect();
    assert!(
        plan.iter().any(|line| line.contains("MergeJoin")),
        "the two index orders make a merge join available, got {plan:?}"
    );
    for side in ["table:t1", "table:t2"] {
        assert!(
            plan.iter().any(|line| {
                line.contains("IndexFullScan")
                    && line.contains(side)
                    && line.contains("keep order:true")
            }),
            "{side} walks idx in order, got {plan:?}"
        );
    }

    // `a = 2` appears twice on each side, so the join is 1 + 4 + 1 = 6 rows.
    let mut got = run_select_on(sql, &catalog, &ctx).unwrap();
    got.sort_by_key(|row| format!("{row:?}"));
    assert_eq!(
        got,
        vec![
            vec![Datum::Int(1), Datum::Int(1)],
            vec![Datum::Int(2), Datum::Int(2)],
            vec![Datum::Int(2), Datum::Int(2)],
            vec![Datum::Int(2), Datum::Int(2)],
            vec![Datum::Int(2), Datum::Int(2)],
            vec![Datum::Int(3), Datum::Int(3)],
        ],
    );
}

/// THE HINT PIN: the same three statements, separated by nothing but their
/// join hint, plan three different joins -- which is Go's
/// `exhaustPhysicalPlans4LogicalJoin` reading `PreferJoinType` BEFORE it costs
/// anything.
///
/// Reduced from `tests/integrationtest/t/topn_push_down.test`, where TiDB
/// records a `MergeJoin` for `TIDB_SMJ`, a `HashJoin` for `TIDB_HJ` and an
/// `IndexJoin` for `TIDB_INLJ` over the very same `t t1 join t t2 on t1.a =
/// t2.a`. Without the gate in [`crate::driver::join_method_hints`] all three
/// merge here, which the `join_shape` casetest counts as EXTRA merge pairs.
#[test]
fn a_join_hint_decides_the_family_before_any_cost_is_compared() {
    use crate::explain::{explain_select_stmt, ExplainFormat};

    let mut catalog = Catalog::default();
    crate::run_create_table_on(
        "CREATE TABLE hj (a INT NOT NULL, KEY idx (a))",
        &mut catalog,
    )
    .unwrap();
    let ctx = crate::StmtContext::for_query();
    run_insert_on("INSERT INTO hj VALUES (1), (2), (3)", &mut catalog, &ctx).unwrap();

    let joins_of = |sql: &str| {
        let stmt = tidb_parser::parse(sql).unwrap();
        let Stmt::Query(query) = &stmt else {
            panic!("not a query");
        };
        let QueryStmt::Select(select) = &**query else {
            panic!("not a SELECT");
        };
        let (_, rows) =
            explain_select_stmt(select, &catalog, "test", &ctx, ExplainFormat::Row).unwrap();
        rows.iter()
            .map(|row| {
                row.iter()
                    .map(|datum| match datum {
                        Datum::Bytes(bytes) => String::from_utf8_lossy(bytes).into_owned(),
                        other => format!("{other:?}"),
                    })
                    .collect::<Vec<_>>()
                    .join("\t")
            })
            .collect::<Vec<_>>()
    };
    let body = "* FROM hj t1 JOIN hj t2 ON t1.a = t2.a";

    let merged = joins_of(&format!("SELECT /*+ TIDB_SMJ(t1, t2) */ {body}"));
    assert!(
        merged.iter().any(|line| line.contains("MergeJoin")),
        "TIDB_SMJ keeps the merge candidate, got {merged:?}"
    );
    for (hint, why) in [
        (
            "TIDB_HJ(t1, t2)",
            "getHashJoins returns forced and the merge is never built",
        ),
        (
            "TIDB_INLJ(t2)",
            "handleForceIndexJoinHints returns the index candidates alone",
        ),
    ] {
        let plan = joins_of(&format!("SELECT /*+ {hint} */ {body}"));
        assert!(
            !plan.iter().any(|line| line.contains("MergeJoin")),
            "{hint}: {why}, got {plan:?}"
        );
    }

    // Whichever family runs, the rows are the same three pairs.
    for hint in ["TIDB_SMJ(t1, t2)", "TIDB_HJ(t1, t2)", "TIDB_INLJ(t2)"] {
        let mut got = run_select_on(
            &format!("SELECT /*+ {hint} */ t1.a, t2.a FROM hj t1 JOIN hj t2 ON t1.a = t2.a"),
            &catalog,
            &ctx,
        )
        .unwrap();
        got.sort_by_key(|row| format!("{row:?}"));
        assert_eq!(
            got,
            vec![
                vec![Datum::Int(1), Datum::Int(1)],
                vec![Datum::Int(2), Datum::Int(2)],
                vec![Datum::Int(3), Datum::Int(3)],
            ],
            "{hint} changed the row set",
        );
    }
}

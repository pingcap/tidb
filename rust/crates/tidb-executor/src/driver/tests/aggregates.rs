//! Aggregate queries: the aggregate functions, `GROUP BY`, `HAVING`,
//! aggregate `ORDER BY`, and `SELECT DISTINCT`.
//!
//! Mirrors Go `pkg/executor/aggregate`'s hash-aggregate surface, including
//! the distinct path a `DISTINCT` select takes through the same operator.

use super::*;

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
            o_ol_cnt INT NOT NULL, PRIMARY KEY (o_w_id,o_d_id,o_id))",
        &mut catalog,
    )
    .unwrap();
    let TableEntry::Kv(orders) = catalog.get_mut_in("test", "orders").unwrap() else {
        panic!("orders is not a KV table");
    };
    orders.add_index(crate::kv_table::KvIndex {
        id: 1,
        name: "PRIMARY".to_owned(),
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
    assert_eq!(answers.len(), 1, "{answers:#?}");
    assert_eq!(
        answers[0].chosen,
        crate::driver::join_search::Chosen::IndexForSingleOuterRow,
        "{answers:#?}"
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
}

#[test]
fn tpcc_condition_two_orders_group_uses_the_covering_index_range() {
    use crate::explain::{explain_select_stmt, ExplainFormat};

    let mut catalog = Catalog::default();
    crate::run_create_table_on(
        "CREATE TABLE orders (o_id INT NOT NULL, o_d_id INT NOT NULL, o_w_id INT NOT NULL, \
            o_c_id INT, o_entry_d DATETIME, o_carrier_id INT, o_ol_cnt INT, o_all_local INT, \
            PRIMARY KEY (o_w_id,o_d_id,o_id), \
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
        unique: true,
        prefix_lengths: vec![crate::ddl::index_prefix::UNSPECIFIED_LENGTH; 3],
        column_offsets: vec![2, 1, 0],
        visible: true,
        global: false,
    });
    orders.add_index(crate::kv_table::KvIndex {
        id: 2,
        name: "idx_order".to_owned(),
        unique: false,
        prefix_lengths: vec![crate::ddl::index_prefix::UNSPECIFIED_LENGTH; 4],
        column_offsets: vec![2, 1, 3, 0],
        visible: true,
        global: false,
    });
    let ctx = crate::StmtContext::for_query();
    run_insert_on(
        "INSERT INTO orders VALUES \
            (1,1,1,10,NULL,NULL,1,1),(2,1,1,20,NULL,NULL,1,1),\
            (3,2,1,30,NULL,NULL,1,1),(4,1,2,40,NULL,NULL,1,1)",
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
            d_next_o_id INT NOT NULL, PRIMARY KEY (d_w_id,d_id))",
        &mut catalog,
    )
    .unwrap();
    let TableEntry::Kv(district) = catalog.get_mut_in("test", "district").unwrap() else {
        panic!("district is not a KV table");
    };
    district.add_index(crate::kv_table::KvIndex {
        id: 3,
        name: "PRIMARY".to_owned(),
        unique: true,
        prefix_lengths: vec![crate::ddl::index_prefix::UNSPECIFIED_LENGTH; 2],
        column_offsets: vec![1, 0],
        visible: true,
        global: false,
    });
    crate::run_create_table_on(
        "CREATE TABLE new_order (no_o_id INT NOT NULL, no_d_id INT NOT NULL, \
            no_w_id INT NOT NULL, PRIMARY KEY (no_w_id,no_d_id,no_o_id))",
        &mut catalog,
    )
    .unwrap();
    let TableEntry::Kv(new_order) = catalog.get_mut_in("test", "new_order").unwrap() else {
        panic!("new_order is not a KV table");
    };
    new_order.add_index(crate::kv_table::KvIndex {
        id: 4,
        name: "PRIMARY".to_owned(),
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

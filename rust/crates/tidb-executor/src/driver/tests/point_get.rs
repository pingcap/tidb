//! The single-row and batched point-get plans: when they are chosen, and
//! what they read.
//!
//! Half of each pair is a NEGATIVE test -- the query shapes Go refuses to
//! serve with a point get -- because choosing the plan too eagerly is the
//! failure mode. Mirrors Go `pkg/executor`'s `PointGetExec` /
//! `BatchPointGetExec` and the planner conditions that pick them.

use super::*;

/// Go's TryFastPlan: a single-table SELECT whose WHERE pins the handle or
/// a whole unique index reads one row instead of scanning. The results
/// must be identical to the scan in every case, including the cases that
/// do NOT qualify and fall back.
#[test]
fn point_get_plans() {
    let mut catalog = Catalog::default();
    crate::run_create_table_on(
        "CREATE TABLE g (id BIGINT PRIMARY KEY, code VARCHAR(8) UNIQUE, v BIGINT)",
        &mut catalog,
    )
    .unwrap();
    run_insert_on(
        "INSERT INTO g VALUES (1, 'a', 10), (2, 'b', 20), (3, 'c', 30)",
        &mut catalog,
        &crate::StmtContext::for_query(),
    )
    .unwrap();

    // Handle point get.
    assert_eq!(
        run_select_on(
            "SELECT v FROM g WHERE id = 2",
            &catalog,
            &crate::StmtContext::for_query()
        )
        .unwrap(),
        vec![vec![Datum::Int(20)]]
    );
    // A handle that does not exist reads nothing.
    assert_eq!(
        run_select_on(
            "SELECT v FROM g WHERE id = 99",
            &catalog,
            &crate::StmtContext::for_query()
        )
        .unwrap(),
        Vec::<Vec<Datum>>::new()
    );
    // Unique-index point get, through the entry's stored handle.
    assert_eq!(
        run_select_on(
            "SELECT id FROM g WHERE code = 'c'",
            &catalog,
            &crate::StmtContext::for_query()
        )
        .unwrap(),
        vec![vec![Datum::Int(3)]]
    );
    assert_eq!(
        run_select_on(
            "SELECT id FROM g WHERE code = 'zz'",
            &catalog,
            &crate::StmtContext::for_query()
        )
        .unwrap(),
        Vec::<Vec<Datum>>::new()
    );

    // The WHERE stays in the pipeline, so an extra condition still
    // filters: the point get narrows the source, it does not replace the
    // filter.
    assert_eq!(
        run_select_on(
            "SELECT id FROM g WHERE id = 2 AND v = 20",
            &catalog,
            &crate::StmtContext::for_query()
        )
        .unwrap(),
        vec![vec![Datum::Int(2)]]
    );
    assert_eq!(
        run_select_on(
            "SELECT id FROM g WHERE id = 2 AND v = 999",
            &catalog,
            &crate::StmtContext::for_query()
        )
        .unwrap(),
        Vec::<Vec<Datum>>::new()
    );

    // Shapes that do not qualify fall back to the scan and stay correct.
    assert_eq!(
        run_select_on(
            "SELECT id FROM g WHERE v = 30",
            &catalog,
            &crate::StmtContext::for_query()
        )
        .unwrap(),
        vec![vec![Datum::Int(3)]],
        "a non-key column is not a point get"
    );
    assert_eq!(
        run_select_on(
            "SELECT id FROM g WHERE id > 1 ORDER BY id",
            &catalog,
            &crate::StmtContext::for_query()
        )
        .unwrap(),
        vec![vec![Datum::Int(2)], vec![Datum::Int(3)]],
        "a range is not a point get"
    );
    assert_eq!(
        run_select_on(
            "SELECT id FROM g WHERE id = 1 OR id = 3",
            &catalog,
            &crate::StmtContext::for_query()
        )
        .unwrap(),
        vec![vec![Datum::Int(1)], vec![Datum::Int(3)]],
        "Go recurses only through AND, so OR is not a point get"
    );
    // Go rejects the fast plan when ORDER BY or HAVING is present, or when
    // LIMIT could remove the row; the answers stay right either way.
    assert_eq!(
        run_select_on(
            "SELECT id FROM g WHERE id = 2 LIMIT 1 OFFSET 1",
            &catalog,
            &crate::StmtContext::for_query()
        )
        .unwrap(),
        Vec::<Vec<Datum>>::new()
    );
    assert_eq!(
        run_select_on(
            "SELECT id FROM g WHERE id = 2 ORDER BY id",
            &catalog,
            &crate::StmtContext::for_query()
        )
        .unwrap(),
        vec![vec![Datum::Int(2)]]
    );

    // A non-integer constant cannot name an integer handle: no row, not a
    // wrong row.
    assert_eq!(
        run_select_on(
            "SELECT id FROM g WHERE id = 'x'",
            &catalog,
            &crate::StmtContext::for_query()
        )
        .unwrap(),
        Vec::<Vec<Datum>>::new()
    );

    // A point get sees writes, including the row a DELETE removed.
    run_update_on(
        "UPDATE g SET v = 99 WHERE id = 2",
        &mut catalog,
        &crate::StmtContext::for_query(),
    )
    .unwrap();
    assert_eq!(
        run_select_on(
            "SELECT v FROM g WHERE id = 2",
            &catalog,
            &crate::StmtContext::for_query()
        )
        .unwrap(),
        vec![vec![Datum::Int(99)]]
    );
    run_delete_on(
        "DELETE FROM g WHERE id = 2",
        &mut catalog,
        &crate::StmtContext::for_query(),
    )
    .unwrap();
    assert_eq!(
        run_select_on(
            "SELECT v FROM g WHERE id = 2",
            &catalog,
            &crate::StmtContext::for_query()
        )
        .unwrap(),
        Vec::<Vec<Datum>>::new()
    );
    assert_eq!(
        run_select_on(
            "SELECT id FROM g WHERE code = 'b'",
            &catalog,
            &crate::StmtContext::for_query()
        )
        .unwrap(),
        Vec::<Vec<Datum>>::new(),
        "the deleted row's index entry is gone too"
    );
}

/// The results above would be right even if the fast plan never fired, so
/// this asserts the DECISION: which shapes Go's tryPointGetPlan accepts
/// and which it rejects.
#[test]
fn point_get_is_chosen_only_for_the_shapes_go_accepts() {
    let mut catalog = Catalog::default();
    crate::run_create_table_on(
        "CREATE TABLE d (id BIGINT PRIMARY KEY, code VARCHAR(8) UNIQUE, v BIGINT)",
        &mut catalog,
    )
    .unwrap();
    run_insert_on(
        "INSERT INTO d VALUES (1, 'a', 10)",
        &mut catalog,
        &crate::StmtContext::for_query(),
    )
    .unwrap();
    let Some(TableEntry::Kv(table)) = catalog.get_table_for_test("d") else {
        panic!("expected a kv table");
    };
    let columns = table
        .columns
        .iter()
        .map(|c| (c.name.clone(), c.field_type.clone()))
        .collect::<Vec<_>>();

    let decides = |sql: &str| {
        let stmt = tidb_parser::parse(sql).unwrap();
        let Stmt::Query(query) = &stmt else {
            panic!("not a query")
        };
        let QueryStmt::Select(select) = &**query else {
            panic!("not a select")
        };
        try_point_get(
            &crate::driver::access::PointPlanStmt::of_select(select),
            table,
            &columns,
            &tidb_datatype::SessionTimeZone::utc(),
        )
        .unwrap()
        // The tests below assert WHICH handle was pinned; the pin's index
        // half has its own coverage through the recorded plans.
        .map(|pin| pin.handle)
    };

    // Accepted: the handle, and a whole unique index.
    assert_eq!(
        decides("SELECT v FROM d WHERE id = 1"),
        Some(Some(TableHandle::Int(1)))
    );
    assert_eq!(
        decides("SELECT v FROM d WHERE 1 = id"),
        Some(Some(TableHandle::Int(1)))
    );
    assert_eq!(
        decides("SELECT v FROM d WHERE code = 'a'"),
        Some(Some(TableHandle::Int(1)))
    );
    // The handle path does not probe: it hands the plan the handle the
    // constant names, and the row read finds nothing. The index path does
    // probe, because the handle only exists in an index entry.
    assert_eq!(
        decides("SELECT v FROM d WHERE id = 7"),
        Some(Some(TableHandle::Int(7)))
    );
    assert_eq!(decides("SELECT v FROM d WHERE code = 'z'"), Some(None));
    // The index path allows extra pairs beyond the key.
    assert_eq!(
        decides("SELECT v FROM d WHERE code = 'a' AND v = 10"),
        Some(Some(TableHandle::Int(1)))
    );

    // Rejected, so the scan runs: Go requires the handle pair to be the
    // ONLY pair, a conjunction of equalities, no ORDER BY or HAVING, and
    // a LIMIT that cannot drop the row.
    assert_eq!(decides("SELECT v FROM d WHERE id = 1 AND v = 10"), None);
    assert_eq!(decides("SELECT v FROM d WHERE v = 10"), None);
    assert_eq!(decides("SELECT v FROM d WHERE id > 1"), None);
    assert_eq!(decides("SELECT v FROM d WHERE id = 1 OR id = 2"), None);
    assert_eq!(decides("SELECT v FROM d WHERE id = 1 ORDER BY v"), None);
    assert_eq!(decides("SELECT v FROM d WHERE id = 1 LIMIT 0"), None);
    assert_eq!(
        decides("SELECT v FROM d WHERE id = 1 LIMIT 1 OFFSET 1"),
        None
    );
    assert_eq!(decides("SELECT v FROM d"), None);

    crate::run_create_table_on(
        "CREATE TABLE generated_point (\
            id BIGINT PRIMARY KEY, \
            base BIGINT, \
            projected BIGINT AS (base + 1))",
        &mut catalog,
    )
    .unwrap();
    let stored_projection =
        tidb_parser::parse("SELECT base FROM generated_point WHERE id = ?").unwrap();
    assert!(
        build_prepared_point_get_plan(&stored_projection, 1, &catalog, DEFAULT_DATABASE,).is_some()
    );
    let generated_projection =
        tidb_parser::parse("SELECT projected FROM generated_point WHERE id = ?").unwrap();
    assert!(
        build_prepared_point_get_plan(&generated_projection, 1, &catalog, DEFAULT_DATABASE,)
            .is_none(),
        "a generated-column projection needs the full statement context"
    );
}

#[test]
fn prepared_fast_point_get_binds_common_handle_without_cloning_template() {
    let mut catalog = Catalog::default();
    crate::run_create_table_on(
        "CREATE TABLE prepared_y (id VARCHAR(64) PRIMARY KEY, v VARCHAR(32))",
        &mut catalog,
    )
    .unwrap();
    run_insert_on(
        "INSERT INTO prepared_y VALUES ('user-0001', 'value-1')",
        &mut catalog,
        &crate::StmtContext::for_query(),
    )
    .unwrap();
    let stmt = tidb_parser::parse("SELECT * FROM prepared_y WHERE id = ?").unwrap();
    let select = match &stmt {
        Stmt::Query(query) => match &**query {
            QueryStmt::Select(select) => select,
            QueryStmt::SetOpr(_) => panic!("expected a select"),
        },
        _ => panic!("expected a query"),
    };
    let fast = run_fast_prepared_point_get(
        select,
        &[Datum::Bytes(b"user-0001".to_vec())],
        &mut catalog,
        "test",
        &crate::StmtContext::for_query(),
    )
    .unwrap()
    .expect("prepared common-handle point read should use the fast path");
    assert_eq!(fast.1.len(), 1);
    assert_eq!(datum_text_for_test(&fast.1[0][0]), "user-0001");
    assert_eq!(datum_text_for_test(&fast.1[0][1]), "value-1");
}

/// The YCSB E scan fast path reads one clustered-handle range row and refuses
/// a wider limit, leaving every non-admitted shape on the general planner.
#[test]
fn fast_single_row_scan_reads_the_first_clustered_handle() {
    let mut catalog = Catalog::default();
    crate::run_create_table_on(
        "CREATE TABLE ycsb_scan (id VARCHAR(32) PRIMARY KEY CLUSTERED, v VARCHAR(32))",
        &mut catalog,
    )
    .unwrap();
    crate::run_insert_on(
        "INSERT INTO ycsb_scan VALUES ('user-0001','value-1'),('user-0002','value-2'),('user-0003','value-3')",
        &mut catalog,
        &crate::StmtContext::for_query(),
    )
    .unwrap();
    catalog.clear_dirty_content();
    let stmt =
        tidb_parser::parse("SELECT * FROM ycsb_scan WHERE id >= 'user-0002' LIMIT 1").unwrap();
    let select = match &stmt {
        Stmt::Query(query) => match &**query {
            QueryStmt::Select(select) => select,
            QueryStmt::SetOpr(_) => panic!("expected a select"),
        },
        _ => panic!("expected a query"),
    };
    let fast = run_fast_single_row_scan(select, &catalog, "test", &crate::StmtContext::for_query())
        .unwrap()
        .expect("bounded clustered-handle scan should use the fast path");
    assert_eq!(datum_text_for_test(&fast.1[0][0]), "user-0002");
    assert_eq!(datum_text_for_test(&fast.1[0][1]), "value-2");
    let scan_select = select;

    let wider =
        tidb_parser::parse("SELECT * FROM ycsb_scan WHERE id >= 'user-0002' LIMIT 2").unwrap();
    let Stmt::Query(query) = &wider else {
        panic!("expected a query");
    };
    let QueryStmt::Select(select) = &**query else {
        panic!("expected a select");
    };
    assert!(
        run_fast_single_row_scan(select, &catalog, "test", &crate::StmtContext::for_query(),)
            .unwrap()
            .is_none()
    );

    let cell = |datum: &Datum| match datum {
        Datum::Bytes(bytes) => String::from_utf8_lossy(bytes).into_owned(),
        other => format!("{other:?}"),
    };
    let point = tidb_parser::parse("SELECT * FROM ycsb_scan WHERE id = 'user-0002'").unwrap();
    let Stmt::Query(query) = &point else {
        panic!("expected a query");
    };
    let QueryStmt::Select(point) = &**query else {
        panic!("expected a select");
    };
    let (_, point_rows) = crate::explain::explain_select_stmt(
        point,
        &catalog,
        "test",
        &crate::StmtContext::for_query(),
        crate::explain::ExplainFormat::Row,
    )
    .unwrap();
    assert_eq!(point_rows.len(), 1, "point plan has no root wrappers");
    assert!(cell(&point_rows[0][0]).starts_with("Point_Get_"));
    assert!(cell(&point_rows[0][3]).contains("clustered index:PRIMARY(id)"));

    let (_, scan_rows) = crate::explain::explain_select_stmt(
        scan_select,
        &catalog,
        "test",
        &crate::StmtContext::for_query(),
        crate::explain::ExplainFormat::Row,
    )
    .unwrap();
    let scan_names = scan_rows
        .iter()
        .map(|row| cell(&row[0]))
        .collect::<Vec<_>>();
    assert!(scan_names[0].starts_with("Limit_"), "{scan_names:?}");
    assert!(scan_names[1].contains("TableReader_"), "{scan_names:?}");
    assert!(scan_names[2].contains("Limit_"), "{scan_names:?}");
    assert!(scan_names[3].contains("TableRangeScan_"), "{scan_names:?}");

    let update =
        tidb_parser::parse("UPDATE ycsb_scan SET v = 'updated' WHERE id = 'user-0002'").unwrap();
    let Stmt::Dml(update) = &update else {
        panic!("expected DML");
    };
    let tidb_ast::DmlStmt::Update(update) = &**update else {
        panic!("expected UPDATE");
    };
    let (_, update_rows) = crate::explain::explain_update_stmt(
        update,
        &mut catalog,
        "test",
        &crate::StmtContext::for_query(),
        crate::explain::ExplainFormat::Row,
    )
    .unwrap();
    let update_names = update_rows
        .iter()
        .map(|row| cell(&row[0]))
        .collect::<Vec<_>>();
    assert!(update_names[0].starts_with("Update_"), "{update_names:?}");
    assert!(update_names[1].contains("Point_Get_"), "{update_names:?}");
    assert_eq!(update_names.len(), 2, "{update_names:?}");
}

/// Go's tryWhereIn2BatchPointGet: `col IN (constants)` over the handle or
/// a single-column unique index reads those rows directly. Results must
/// match the scan in every case, including the shapes Go rejects.
#[test]
fn batch_point_get() {
    let mut catalog = Catalog::default();
    crate::run_create_table_on(
        "CREATE TABLE b (id BIGINT PRIMARY KEY, code VARCHAR(8) UNIQUE, v BIGINT)",
        &mut catalog,
    )
    .unwrap();
    run_insert_on(
        "INSERT INTO b VALUES (1, 'a', 10), (2, 'b', 20), (3, 'c', 30)",
        &mut catalog,
        &crate::StmtContext::for_query(),
    )
    .unwrap();

    let ids = |sql: &str, catalog: &Catalog| {
        let mut got: Vec<i64> = run_select_on(sql, catalog, &crate::StmtContext::for_query())
            .unwrap()
            .into_iter()
            .map(|row| match row[0] {
                Datum::Int(value) => value,
                ref other => panic!("expected an int, got {other:?}"),
            })
            .collect();
        got.sort_unstable();
        got
    };

    // Handle path, including a value that matches nothing.
    assert_eq!(
        ids("SELECT id FROM b WHERE id IN (1, 3)", &catalog),
        vec![1, 3]
    );
    assert_eq!(
        ids("SELECT id FROM b WHERE id IN (3, 99)", &catalog),
        vec![3]
    );
    assert_eq!(
        ids("SELECT id FROM b WHERE id IN (99)", &catalog),
        Vec::<i64>::new()
    );
    // Unique-index path.
    assert_eq!(
        ids("SELECT id FROM b WHERE code IN ('a', 'c')", &catalog),
        vec![1, 3]
    );

    // Shapes Go rejects fall back to the scan and stay correct: NOT IN,
    // a non-key column, and an IN with anything else in the WHERE.
    assert_eq!(
        ids("SELECT id FROM b WHERE id NOT IN (1, 3)", &catalog),
        vec![2]
    );
    assert_eq!(
        ids("SELECT id FROM b WHERE v IN (20, 30)", &catalog),
        vec![2, 3]
    );
    assert_eq!(
        ids("SELECT id FROM b WHERE id IN (1, 3) AND v = 30", &catalog),
        vec![3]
    );
    // Go also rejects it with ORDER BY, LIMIT or DISTINCT present.
    assert_eq!(
        ids("SELECT id FROM b WHERE id IN (3, 1) ORDER BY id", &catalog),
        vec![1, 3]
    );
    assert_eq!(
        run_select_on(
            "SELECT id FROM b WHERE id IN (1, 2, 3) LIMIT 2",
            &catalog,
            &crate::StmtContext::for_query()
        )
        .unwrap()
        .len(),
        2
    );
}

/// Go's `tryWhereIn2BatchPointGet` also accepts a row-valued `IN` when the
/// tuples pin every column of a composite primary/unique key.
#[test]
fn batch_point_get_accepts_row_in_on_a_composite_key() {
    let mut catalog = Catalog::default();
    crate::run_create_table_on(
        "CREATE TABLE c (a INT, b INT, v INT, PRIMARY KEY (a, b))",
        &mut catalog,
    )
    .unwrap();
    let ctx = crate::StmtContext::for_query();
    run_insert_on(
        "INSERT INTO c VALUES (1, 1, 11), (1, 2, 12), (2, 1, 21)",
        &mut catalog,
        &ctx,
    )
    .unwrap();
    let Some(TableEntry::Kv(table)) = catalog.get_table_for_test("c") else {
        panic!("expected a kv table");
    };
    let columns = table
        .columns
        .iter()
        .map(|column| (column.name.clone(), column.field_type.clone()))
        .collect::<Vec<_>>();
    let stmt = tidb_parser::parse("SELECT v FROM c WHERE (a, b) IN ((1, 2), (2, 1))").unwrap();
    let Stmt::Query(query) = &stmt else {
        panic!("not a query");
    };
    let QueryStmt::Select(select) = &**query else {
        panic!("not a SELECT");
    };
    assert!(
        try_batch_point_get(
            select,
            table,
            &columns,
            &tidb_datatype::SessionTimeZone::utc()
        )
        .unwrap()
        .is_some(),
        "a composite row IN should use Batch_Point_Get"
    );
    assert_eq!(
        run_select_on(
            "SELECT v FROM c WHERE (a, b) IN ((1, 2), (2, 1))",
            &catalog,
            &ctx
        )
        .unwrap(),
        vec![vec![Datum::Int(12)], vec![Datum::Int(21)]],
    );
}

/// The answers above would be right from a scan too, so this asserts the
/// DECISION: which shapes Go's batch point get claims.
#[test]
fn batch_point_get_is_chosen_only_for_the_shapes_go_accepts() {
    let mut catalog = Catalog::default();
    crate::run_create_table_on(
        "CREATE TABLE bd (id BIGINT PRIMARY KEY, code VARCHAR(8) UNIQUE, v BIGINT)",
        &mut catalog,
    )
    .unwrap();
    run_insert_on(
        "INSERT INTO bd VALUES (1, 'a', 10)",
        &mut catalog,
        &crate::StmtContext::for_query(),
    )
    .unwrap();
    let Some(TableEntry::Kv(table)) = catalog.get_table_for_test("bd") else {
        panic!("expected a kv table");
    };
    let columns = table
        .columns
        .iter()
        .map(|c| (c.name.clone(), c.field_type.clone()))
        .collect::<Vec<_>>();
    let decides = |sql: &str| {
        let stmt = tidb_parser::parse(sql).unwrap();
        let Stmt::Query(query) = &stmt else {
            panic!("not a query")
        };
        let QueryStmt::Select(select) = &**query else {
            panic!("not a select")
        };
        try_batch_point_get(
            select,
            table,
            &columns,
            &tidb_datatype::SessionTimeZone::utc(),
        )
        .unwrap()
        .map(BatchPointLookup::into_handles)
    };

    assert_eq!(
        decides("SELECT v FROM bd WHERE id IN (1, 2)"),
        Some(vec![TableHandle::Int(1), TableHandle::Int(2)]),
        "the handle path does not probe, as the single point get does not"
    );
    assert_eq!(
        decides("SELECT v FROM bd WHERE code IN ('a', 'zz')"),
        Some(vec![TableHandle::Int(1)]),
        "the index path probes, so a missing key yields no handle"
    );
    // Rejected shapes.
    assert_eq!(decides("SELECT v FROM bd WHERE id NOT IN (1)"), None);
    assert_eq!(decides("SELECT v FROM bd WHERE v IN (1)"), None);
    assert_eq!(decides("SELECT v FROM bd WHERE id IN (1) AND v = 1"), None);
    assert_eq!(decides("SELECT v FROM bd WHERE id IN (1) ORDER BY v"), None);
    assert_eq!(decides("SELECT v FROM bd WHERE id IN (1) LIMIT 1"), None);
    assert_eq!(decides("SELECT DISTINCT v FROM bd WHERE id IN (1)"), None);
    assert_eq!(decides("SELECT v FROM bd WHERE id = 1"), None);
}

/// A constant a point plan keys by must first be moved into the COLUMN's
/// domain -- Go `getPointGetValue` in `pkg/planner/core/point_get_plan.go`.
///
/// The regression: every non-integer constant was treated as "names no
/// integer handle", so the point plan was still chosen and returned ZERO
/// rows. `WHERE int_pk = 1.0` silently lost the row, while the same
/// predicate on an unindexed or merely-indexed column returned it.
///
/// Every expectation below is TiDB's own answer, captured with
/// `rust/difftests/gorun` against a mock-backed session.
#[test]
fn a_point_plan_keys_by_the_constant_in_the_columns_domain() {
    let mut catalog = Catalog::default();
    crate::run_create_table_on(
        "CREATE TABLE pk1 (pk BIGINT PRIMARY KEY, v BIGINT)",
        &mut catalog,
    )
    .unwrap();
    run_insert_on(
        "INSERT INTO pk1 VALUES (1, 10)",
        &mut catalog,
        &crate::StmtContext::for_query(),
    )
    .unwrap();
    let rows = |sql: &str| run_select_on(sql, &catalog, &crate::StmtContext::for_query()).unwrap();
    let one = vec![vec![Datum::Int(1)]];
    let none = Vec::<Vec<Datum>>::new();

    // Exactly representable: the point plan keys handle 1 and finds the row.
    assert_eq!(rows("SELECT pk FROM pk1 WHERE pk = 1.0"), one);
    assert_eq!(rows("SELECT pk FROM pk1 WHERE pk = 1.00"), one);
    assert_eq!(rows("SELECT pk FROM pk1 WHERE pk = 1e0"), one);
    assert_eq!(rows("SELECT pk FROM pk1 WHERE pk = '1'"), one);
    assert_eq!(rows("SELECT pk FROM pk1 WHERE pk = '1.0'"), one);
    assert_eq!(rows("SELECT pk FROM pk1 WHERE pk IN (1.0, 2.0)"), one);

    // Not representable: the point plan is abandoned and the SCAN answers,
    // which is the same empty result -- the fix must not make these match.
    assert_eq!(rows("SELECT pk FROM pk1 WHERE pk = 1.5"), none);
    assert_eq!(rows("SELECT pk FROM pk1 WHERE pk = 0.5"), none);
    assert_eq!(rows("SELECT pk FROM pk1 WHERE pk = '1.5'"), none);
    assert_eq!(rows("SELECT pk FROM pk1 WHERE pk IN (1.5, 2.5)"), none);

    // The inequalities never took the point path and must not move.
    assert_eq!(rows("SELECT pk FROM pk1 WHERE pk <> 1.0"), none);
    assert_eq!(rows("SELECT pk FROM pk1 WHERE pk <> 1.5"), one);
    assert_eq!(rows("SELECT pk FROM pk1 WHERE pk < 1.5"), one);
    assert_eq!(rows("SELECT pk FROM pk1 WHERE pk <= 1.0"), one);
    assert_eq!(rows("SELECT pk FROM pk1 WHERE pk > 0.5"), one);
    assert_eq!(rows("SELECT pk FROM pk1 WHERE pk >= 1.0"), one);
    assert_eq!(rows("SELECT pk FROM pk1 WHERE pk BETWEEN 0.5 AND 1.5"), one);

    // A unique index is the same rule through the other arm.
    let mut catalog = Catalog::default();
    crate::run_create_table_on(
        "CREATE TABLE uq (id BIGINT PRIMARY KEY, u BIGINT UNIQUE)",
        &mut catalog,
    )
    .unwrap();
    run_insert_on(
        "INSERT INTO uq VALUES (7, 1)",
        &mut catalog,
        &crate::StmtContext::for_query(),
    )
    .unwrap();
    let rows = |sql: &str| run_select_on(sql, &catalog, &crate::StmtContext::for_query()).unwrap();
    assert_eq!(
        rows("SELECT id FROM uq WHERE u = 1.0"),
        vec![vec![Datum::Int(7)]]
    );
    assert_eq!(
        rows("SELECT id FROM uq WHERE u IN (1.0, 2.0)"),
        vec![vec![Datum::Int(7)]]
    );
    assert_eq!(
        rows("SELECT id FROM uq WHERE u = 1.5"),
        Vec::<Vec<Datum>>::new()
    );
}

/// Go `TryFastPlan` replaces the complete query plan when one primary-key
/// equality identifies the row and every selected field is a source column.
/// The `PointGetPlan` itself owns the output schema, so no residual
/// `Selection` or `Projection` remains.
#[test]
fn fast_point_get_replaces_selection_and_projection_like_go() {
    use crate::explain::{explain_select_stmt, ExplainFormat};

    let mut catalog = Catalog::default();
    crate::run_create_table_on(
        "CREATE TABLE fast_point (id INT PRIMARY KEY, c CHAR(8) NOT NULL)",
        &mut catalog,
    )
    .unwrap();
    let ctx = crate::StmtContext::for_query();
    run_insert_on(
        "INSERT INTO fast_point VALUES (1, 'one')",
        &mut catalog,
        &ctx,
    )
    .unwrap();

    let stmt = tidb_parser::parse("SELECT c FROM fast_point WHERE id = 1").unwrap();
    let Stmt::Query(query) = &stmt else {
        panic!("not a query");
    };
    let QueryStmt::Select(select) = &**query else {
        panic!("not a SELECT");
    };
    let (_, rows) =
        explain_select_stmt(select, &catalog, "test", &ctx, ExplainFormat::Brief).unwrap();
    let cell = |datum: &Datum| match datum {
        Datum::Bytes(bytes) => String::from_utf8_lossy(bytes).into_owned(),
        other => format!("{other:?}"),
    };
    let plan: Vec<String> = rows
        .iter()
        .map(|row| row.iter().map(cell).collect::<Vec<_>>().join("\t"))
        .collect();

    assert_eq!(
        plan,
        vec!["Point_Get\t1.00\troot\ttable:fast_point\thandle:1"]
    );
    assert_eq!(
        run_select_on("SELECT c FROM fast_point WHERE id = 1", &catalog, &ctx).unwrap(),
        vec![vec![Datum::new_string("one")]]
    );
}

/// Go calls `TryFastPlan` before `PlanBuilder` constructs a `DataSource` or
/// enumerates its ordinary access paths. A qualifying primary-key point read
/// must therefore finish without entering Rust's ordinary single-table path.
#[test]
fn fast_point_get_precedes_ordinary_access_path_planning_like_go() {
    let mut catalog = Catalog::default();
    crate::run_create_table_on(
        "CREATE TABLE fast_order (id INT PRIMARY KEY, c CHAR(8) NOT NULL)",
        &mut catalog,
    )
    .unwrap();
    let ctx = crate::StmtContext::for_query();
    run_insert_on(
        "INSERT INTO fast_order VALUES (1, 'one')",
        &mut catalog,
        &ctx,
    )
    .unwrap();

    reset_ordinary_access_path_entries();
    assert_eq!(
        run_select_on("SELECT c FROM fast_order WHERE id = 1", &catalog, &ctx).unwrap(),
        vec![vec![Datum::new_string("one")]]
    );
    assert_eq!(
        ordinary_access_path_entries(),
        0,
        "Go TryFastPlan returns before ordinary DataSource access planning"
    );
}

/// Go's point UPDATE/DELETE plans consume the primary-key predicate exactly
/// as the SELECT fast plan does. An additional equality remains a real
/// Selection and must still be evaluated before the write.
#[test]
fn fast_point_writes_remove_only_the_consumed_selection_like_go() {
    use crate::explain::{explain_delete_stmt, explain_update_stmt, ExplainFormat};

    fn explain_write(sql: &str, catalog: &mut Catalog, ctx: &crate::StmtContext) -> Vec<String> {
        let stmt = tidb_parser::parse(sql).unwrap();
        let Stmt::Dml(dml) = &stmt else {
            panic!("not DML");
        };
        let (_, rows) = match &**dml {
            tidb_ast::DmlStmt::Update(update) => {
                explain_update_stmt(update, catalog, "test", ctx, ExplainFormat::Brief).unwrap()
            }
            tidb_ast::DmlStmt::Delete(delete) => {
                explain_delete_stmt(delete, catalog, "test", ctx, ExplainFormat::Brief).unwrap()
            }
            _ => panic!("not an UPDATE or DELETE"),
        };
        let cell = |datum: &Datum| match datum {
            Datum::Bytes(bytes) => String::from_utf8_lossy(bytes).into_owned(),
            other => format!("{other:?}"),
        };
        rows.iter()
            .map(|row| row.iter().map(cell).collect::<Vec<_>>().join("\t"))
            .collect()
    }

    let mut catalog = Catalog::default();
    crate::run_create_table_on(
        "CREATE TABLE fast_write (id INT PRIMARY KEY, c CHAR(8) NOT NULL)",
        &mut catalog,
    )
    .unwrap();
    let ctx = crate::StmtContext::for_query();
    run_insert_on(
        "INSERT INTO fast_write VALUES (1, 'one')",
        &mut catalog,
        &ctx,
    )
    .unwrap();

    assert_eq!(
        explain_write(
            "UPDATE fast_write SET c = 'two' WHERE id = 1",
            &mut catalog,
            &ctx,
        ),
        vec![
            "Update\tN/A\troot\t\tN/A",
            "└─Point_Get\t1.00\troot\ttable:fast_write\thandle:1",
        ]
    );
    let guarded = explain_write(
        "UPDATE fast_write SET c = 'bad' WHERE id = 1 AND c = 'missing'",
        &mut catalog,
        &ctx,
    );
    assert!(guarded.iter().any(|row| row.contains("Selection")));
    assert_eq!(
        run_update_on(
            "UPDATE fast_write SET c = 'bad' WHERE id = 1 AND c = 'missing'",
            &mut catalog,
            &ctx,
        )
        .unwrap(),
        0
    );
    assert_eq!(
        run_update_on(
            "UPDATE fast_write SET c = 'two' WHERE id = 1",
            &mut catalog,
            &ctx,
        )
        .unwrap(),
        1
    );
    assert_eq!(
        run_select_on("SELECT c FROM fast_write WHERE id = 1", &catalog, &ctx).unwrap(),
        vec![vec![Datum::new_string("two")]]
    );

    assert_eq!(
        explain_write("DELETE FROM fast_write WHERE id = 1", &mut catalog, &ctx,),
        vec![
            "Delete\tN/A\troot\t\tN/A",
            "└─Point_Get\t1.00\troot\ttable:fast_write\thandle:1",
        ]
    );
    assert_eq!(
        run_delete_on("DELETE FROM fast_write WHERE id = 1", &mut catalog, &ctx,).unwrap(),
        1
    );
    assert!(
        run_select_on("SELECT c FROM fast_write WHERE id = 1", &catalog, &ctx)
            .unwrap()
            .is_empty()
    );
}

/// A handle point BESIDE an extra conjunct reads the bare handle plan, and
/// the unique index that also matches is never chosen: Go's
/// `derivePathStatsAndTryHeuristics` selects the FIRST only-point-range path
/// that is (the table path or a unique index) and a single scan, walking the
/// table path first -- so `[1,1]` on the int handle wins outright and the
/// unique `(i, j)` point is never examined, let alone costed. The recorded
/// capture (`tests/integrationtest/r/explain_easy.result`, in-transaction so
/// its point get also carries `, lock`):
///
/// ```text
/// Update
/// └─Selection      eq(explain_easy.t.j, 1)
///   └─Point_Get    table:t    handle:1, lock
/// ```
///
/// The fast plan REFUSES this statement (a handle pair plus an extra
/// conjunct, `tryPointGetPlan`'s `else if handlePair.value.Kind() !=
/// KindNull` -- ported in `try_point_get`), so reaching the same tree PROVES
/// the ordinary chooser picked the table path: before
/// `access_cost::heuristic_point_path` this statement read the unique index
/// (`IndexRangeScan index:i(i, j) range:[1 1,1 1]`).
#[test]
fn a_handle_point_with_an_extra_conjunct_wins_over_the_unique_index_like_go() {
    use crate::explain::{explain_update_stmt, ExplainFormat};

    let mut catalog = Catalog::default();
    crate::run_create_table_on(
        "CREATE TABLE heuristic_pin (i INT PRIMARY KEY, j INT, UNIQUE KEY ij (i, j))",
        &mut catalog,
    )
    .unwrap();
    let ctx = crate::StmtContext::for_query();
    run_insert_on(
        "INSERT INTO heuristic_pin VALUES (1, 1)",
        &mut catalog,
        &ctx,
    )
    .unwrap();

    let stmt = tidb_parser::parse("UPDATE heuristic_pin SET j = -j WHERE i = 1 AND j = 1").unwrap();
    let Stmt::Dml(dml) = &stmt else {
        panic!("not DML");
    };
    let tidb_ast::DmlStmt::Update(update) = &**dml else {
        panic!("not an UPDATE");
    };
    let (_, rows) =
        explain_update_stmt(update, &mut catalog, "test", &ctx, ExplainFormat::Brief).unwrap();
    let cell = |datum: &Datum| match datum {
        Datum::Bytes(bytes) => String::from_utf8_lossy(bytes).into_owned(),
        other => format!("{other:?}"),
    };
    let plan: Vec<Vec<String>> = rows
        .iter()
        .map(|row| row.iter().map(cell).collect())
        .collect();
    // The recorded tree, operator by operator: no reader, no index, and the
    // unconsumed `j` conjunct as the root filter above the point read. The
    // `, lock` marker is the recording's explicit transaction, which this
    // autocommit statement does not carry.
    assert_eq!(plan.len(), 3, "{plan:?}");
    assert_eq!(plan[0][0], "Update");
    assert!(plan[1][0].contains("Selection"), "{:?}", plan[1]);
    assert!(plan[1][4].contains("eq("), "{:?}", plan[1]);
    assert_eq!(
        &plan[2][..5],
        [
            "  └─Point_Get".to_owned(),
            "1.00".to_owned(),
            "root".to_owned(),
            "table:heuristic_pin".to_owned(),
            "handle:1".to_owned(),
        ],
        "{plan:?}"
    );

    // The filter above the point read still decides the write: the pinned
    // row's `j` is 1, so the guarded miss writes nothing and the match
    // negates it.
    assert_eq!(
        run_update_on(
            "UPDATE heuristic_pin SET j = -j WHERE i = 1 AND j = 2",
            &mut catalog,
            &ctx,
        )
        .unwrap(),
        0
    );
    assert_eq!(
        run_update_on(
            "UPDATE heuristic_pin SET j = -j WHERE i = 1 AND j = 1",
            &mut catalog,
            &ctx,
        )
        .unwrap(),
        1
    );
    assert_eq!(
        run_select_on("SELECT j FROM heuristic_pin WHERE i = 1", &catalog, &ctx).unwrap(),
        vec![vec![Datum::Int(-1)]]
    );
}

/// A handle range that represents the complete predicate is an access
/// condition, not a residual Selection. Go pushes a simple column projection
/// into TiKV and returns it through a TableReader.
#[test]
fn exact_handle_range_uses_the_go_cop_projection_tree() {
    use crate::explain::{explain_select_stmt, ExplainFormat};

    let mut catalog = Catalog::default();
    crate::run_create_table_on(
        "CREATE TABLE fast_range (id INT PRIMARY KEY, c CHAR(8) NOT NULL)",
        &mut catalog,
    )
    .unwrap();
    let ctx = crate::StmtContext::for_query();
    run_insert_on(
        "INSERT INTO fast_range VALUES (1, 'one'), (2, 'two')",
        &mut catalog,
        &ctx,
    )
    .unwrap();

    let stmt = tidb_parser::parse("SELECT c FROM fast_range WHERE id BETWEEN 1 AND 100").unwrap();
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
        vec!["TableReader", "└─Projection", "  └─TableRangeScan"]
    );
    assert_eq!(
        (0..rows.len()).map(|row| cell(row, 2)).collect::<Vec<_>>(),
        vec!["root", "cop[tikv]", "cop[tikv]"]
    );
    assert_eq!(
        run_select_on(
            "SELECT c FROM fast_range WHERE id BETWEEN 1 AND 100",
            &catalog,
            &ctx,
        )
        .unwrap(),
        vec![
            vec![Datum::new_string("one")],
            vec![Datum::new_string("two")]
        ]
    );
}

/// Go keeps the DataSource output estimate separate from the rows covered by
/// its chosen access path. The complete predicate can use composite-index
/// statistics even when the physical read is a common-handle prefix range.
#[test]
fn residual_selection_uses_logical_rows_over_access_rows() {
    use crate::explain::{explain_select_stmt, ExplainFormat};

    let mut catalog = Catalog::default();
    crate::run_create_table_on(
        "CREATE TABLE customer (\
         c_id INT NOT NULL, c_d_id INT NOT NULL, c_w_id INT NOT NULL, \
         c_first VARCHAR(16), c_middle CHAR(2), c_last VARCHAR(16), \
         c_balance DECIMAL(12,2), PRIMARY KEY(c_w_id,c_d_id,c_id) CLUSTERED, \
         KEY idx_customer(c_w_id,c_d_id,c_last,c_first))",
        &mut catalog,
    )
    .unwrap();
    let TableEntry::Kv(customer) = catalog.get_mut_in("test", "customer").unwrap() else {
        panic!("customer is not a KV table");
    };
    customer.set_common_handle_offsets(vec![2, 1, 0]);
    customer.add_index(crate::kv_table::KvIndex {
        id: 2,
        name: "PRIMARY".to_owned(),
        comment: String::new(),
        unique: true,
        prefix_lengths: vec![crate::ddl::index_prefix::UNSPECIFIED_LENGTH; 3],
        column_offsets: vec![2, 1, 0],
        visible: true,
        global: false,
        clustered_primary: false,
    }, false);
    let ctx = crate::StmtContext::for_query();
    run_insert_on(
        "INSERT INTO customer VALUES \
         (1,1,1,'Alice','OE','Able',10.00), \
         (2,1,1,'Bob','OE','Able',20.00), \
         (3,1,1,'Carol','OE','Baker',30.00), \
         (1,2,1,'Dan','OE','Clark',40.00), \
         (1,1,2,'Eve','OE','Davis',50.00)",
        &mut catalog,
        &ctx,
    )
    .unwrap();
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
    catalog.clear_dirty_content();

    let sql = "SELECT c_balance, c_first, c_middle, c_id FROM customer \
        IGNORE INDEX(idx_customer) \
        WHERE c_w_id=1 AND c_d_id=1 AND c_last='Able' ORDER BY c_first";
    let stmt = tidb_parser::parse(sql).unwrap();
    let Stmt::Query(query) = &stmt else {
        panic!("not a query");
    };
    let QueryStmt::Select(select) = &**query else {
        panic!("not a SELECT");
    };
    let customer = match catalog.get_in("test", "customer").unwrap() {
        TableEntry::Kv(customer) => customer,
        _ => panic!("customer is not a KV table"),
    };
    let scope = PlanTrace::single_table_scope(
        "customer",
        Some("test".to_owned()),
        catalog.get_in("test", "customer").unwrap().column_list(),
    );
    let logical_rows = crate::access_cost::realtime_row_count(
        catalog
            .table_statistics(customer.table_id)
            .map(AsRef::as_ref),
    ) * crate::driver::access::stats_selectivity(
        &catalog,
        customer,
        &scope,
        select.where_clause.as_ref(),
    )
    .unwrap();

    let (_, rows) =
        explain_select_stmt(select, &catalog, "test", &ctx, ExplainFormat::Brief).unwrap();
    let cell = |row: usize, column: usize| match &rows[row][column] {
        Datum::Bytes(bytes) => String::from_utf8_lossy(bytes).into_owned(),
        other => format!("{other:?}"),
    };
    let selection = (0..rows.len())
        .find(|row| cell(*row, 0).contains("Selection"))
        .unwrap_or_else(|| {
            panic!("common-handle access retains the last-name Selection: {rows:#?}")
        });
    let scan = (0..rows.len())
        .find(|row| cell(*row, 0).contains("TableRangeScan"))
        .expect("customer is read through its common-handle prefix");
    let selection_rows = cell(selection, 1).parse::<f64>().unwrap();
    let scan_rows = cell(scan, 1).parse::<f64>().unwrap();

    assert_eq!(selection_rows, (logical_rows * 100.0).round() / 100.0);
    assert_ne!(selection_rows, scan_rows);
}

/// An ORDER BY stays in the root task, while the exact handle range still
/// pushes its simple projection into TiKV. This is the Sysbench
/// `oltp_common.lua` simple ordered-range shape.
#[test]
fn ordered_handle_range_keeps_the_go_cop_projection_below_sort() {
    use crate::explain::{explain_select_stmt, ExplainFormat};

    let mut catalog = Catalog::default();
    crate::run_create_table_on(
        "CREATE TABLE ordered_range (id INT PRIMARY KEY, c CHAR(8) NOT NULL)",
        &mut catalog,
    )
    .unwrap();
    let ctx = crate::StmtContext::for_query();
    run_insert_on(
        "INSERT INTO ordered_range VALUES (1, 'z'), (2, 'a')",
        &mut catalog,
        &ctx,
    )
    .unwrap();

    let sql = "SELECT c FROM ordered_range WHERE id BETWEEN 1 AND 100 ORDER BY c";
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
            "└─TableReader",
            "  └─Projection",
            "    └─TableRangeScan"
        ]
    );
    assert_eq!(
        (0..rows.len()).map(|row| cell(row, 2)).collect::<Vec<_>>(),
        vec!["root", "root", "cop[tikv]", "cop[tikv]"]
    );
    assert_eq!(
        run_select_on(sql, &catalog, &ctx).unwrap(),
        vec![vec![Datum::new_string("a")], vec![Datum::new_string("z")]]
    );
}

/// Go's `isPointGetPath`/`convertToPointGet`: a table path whose one range is
/// a single non-null point on the clustered integer handle becomes a
/// `Point_Get` even when a further conjunct stays a filter above it. So
/// `c1 = 1 AND c2 > 1` reads `Point_Get`, not a `TableRangeScan` over `[1,1]`.
#[test]
fn a_single_point_handle_range_with_a_filter_is_a_point_get() {
    use crate::explain::{explain_select_stmt, ExplainFormat};

    let mut catalog = Catalog::default();
    crate::run_create_table_on(
        "CREATE TABLE t1 (c1 INT PRIMARY KEY, c2 INT, c3 INT, INDEX c2 (c2))",
        &mut catalog,
    )
    .unwrap();
    let ctx = crate::StmtContext::for_query();
    run_insert_on(
        "INSERT INTO t1 VALUES (1, 5, 50), (2, 20, 200)",
        &mut catalog,
        &ctx,
    )
    .unwrap();

    let stmt = tidb_parser::parse("SELECT * FROM t1 WHERE c1 = 1 AND c2 > 1").unwrap();
    let Stmt::Query(query) = &stmt else {
        panic!("not a query");
    };
    let QueryStmt::Select(select) = &**query else {
        panic!("not a SELECT");
    };
    let (_, rows) =
        explain_select_stmt(select, &catalog, "test", &ctx, ExplainFormat::Row).unwrap();
    let cell = |datum: &Datum| match datum {
        Datum::Bytes(bytes) => String::from_utf8_lossy(bytes).into_owned(),
        other => format!("{other:?}"),
    };
    let plan: Vec<String> = rows
        .iter()
        .map(|row| row.iter().map(cell).collect::<Vec<_>>().join("\t"))
        .collect();
    assert!(
        plan.iter().any(|line| line.contains("Point_Get")),
        "the single-point handle range is a Point_Get, got {plan:?}"
    );
    assert!(
        !plan.iter().any(|line| line.contains("TableRangeScan")),
        "no range scan remains, got {plan:?}"
    );

    // The row it reads is the c1 = 1 row, and c2 > 1 still filters it in.
    assert_eq!(
        run_select_on("SELECT c1 FROM t1 WHERE c1 = 1 AND c2 > 1", &catalog, &ctx).unwrap(),
        vec![vec![Datum::Int(1)]]
    );
}

/// A `LIMIT 1` whose WHERE never detaches into clustered-handle ranges (an
/// `IS NOT NULL`, or a predicate naming no primary column) is the general
/// planner's statement: Go plans a table reader over it and answers. The fast
/// path must FALL BACK (`None`), never refuse the statement -- captured
/// against TiDB, where `select prdaccno from dpm_prd_acc where prdaccno is
/// not null limit 1` returns a row while an erroring fast path rejected it.
#[test]
fn fast_single_row_scan_falls_back_when_the_where_detaches_nothing() {
    let mut catalog = Catalog::default();
    crate::run_create_table_on(
        "CREATE TABLE ycsb_fb (id VARCHAR(32) PRIMARY KEY CLUSTERED, v VARCHAR(32))",
        &mut catalog,
    )
    .unwrap();
    crate::run_insert_on(
        "INSERT INTO ycsb_fb VALUES ('user-0001','value-1'),('user-0002','value-2')",
        &mut catalog,
        &crate::StmtContext::for_query(),
    )
    .unwrap();
    catalog.clear_dirty_content();
    for sql in [
        "SELECT id FROM ycsb_fb WHERE v IS NOT NULL LIMIT 1",
        "SELECT id FROM ycsb_fb WHERE v = 'value-2' LIMIT 1",
    ] {
        let stmt = tidb_parser::parse(sql).unwrap();
        let Stmt::Query(query) = &stmt else {
            panic!("expected a query");
        };
        let QueryStmt::Select(select) = &**query else {
            panic!("expected a select");
        };
        let ctx = crate::StmtContext::for_query();
        assert!(
            crate::driver::plan_fast_single_row_scan(select, &catalog, "test", &ctx)
                .unwrap()
                .is_none(),
            "{sql} should fall back to the general planner"
        );
        // The general planner answers it, as Go's does.
        let rows =
            crate::run_select_meta_stmt(select, &catalog, "test", &ctx).expect("{sql} should run");
        let (_, values) = rows;
        assert_eq!(values.len(), 1, "{sql} should return one row");
    }
}

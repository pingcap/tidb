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
            &crate::index_hints::AvailablePaths::unrestricted(),
        )
        .unwrap()
        .map(|decision| decision.handles)
    };

    assert_eq!(
        decides("SELECT v FROM bd WHERE id IN (1, 2)"),
        Some(vec![TableHandle::Int(1), TableHandle::Int(2)]),
        "the handle path does not probe, as the single point get does not"
    );
    assert_eq!(
        decides("SELECT v FROM bd WHERE id IN (2, 1, 2)"),
        Some(vec![TableHandle::Int(2), TableHandle::Int(1)]),
        "Go deduplicates repeated handles while retaining first-seen order"
    );
    assert_eq!(
        decides("SELECT v FROM bd WHERE code IN ('a', 'zz')"),
        Some(vec![TableHandle::Int(1)]),
        "the index path probes, so a missing key yields no handle"
    );
    assert_eq!(
        decides("SELECT v FROM bd WHERE code IN ('a', 'a')"),
        Some(vec![TableHandle::Int(1)]),
        "Go deduplicates repeated unique-index keys"
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

/// Go `tryWhereIn2BatchPointGet` accepts a row constructor when its columns
/// cover one whole unique index.  The WHERE's column order need not be the
/// index order; `newBatchPointGetPlan` records a permutation and converts each
/// literal in the domain of the column it was written against.
#[test]
fn row_batch_point_get_covers_a_composite_unique_key() {
    let mut catalog = Catalog::default();
    crate::run_create_table_on(
        "CREATE TABLE br (id BIGINT PRIMARY KEY, a BIGINT, b VARCHAR(8), \
         UNIQUE KEY ab (a, b))",
        &mut catalog,
    )
    .unwrap();
    run_insert_on(
        "INSERT INTO br VALUES (1, 10, 'x'), (2, 20, 'y'), (3, 30, 'z')",
        &mut catalog,
        &crate::StmtContext::for_query(),
    )
    .unwrap();
    let Some(TableEntry::Kv(table)) = catalog.get_table_for_test("br") else {
        panic!("expected a kv table");
    };
    let columns = table
        .columns
        .iter()
        .map(|column| (column.name.clone(), column.field_type.clone()))
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
            &crate::index_hints::AvailablePaths::unrestricted(),
        )
        .unwrap()
        .map(|decision| decision.handles)
    };

    assert_eq!(
        decides("SELECT id FROM br WHERE (b, a) IN (('y', 20), ('x', 10), ('y', 20))"),
        Some(vec![TableHandle::Int(2), TableHandle::Int(1)]),
        "the row is reordered into index order and duplicate keys are read once"
    );
    assert_eq!(
        decides("SELECT id FROM br WHERE (a, b) IN ((10, 'x'), (NULL, 'y'))"),
        Some(vec![TableHandle::Int(1)]),
        "a NULL index tuple is filtered without declining the whole fast plan"
    );
    assert_eq!(
        decides("SELECT id FROM br WHERE (a, b) IN ((10, 'x'), 20)"),
        None,
        "every right-hand row must have the left-hand arity"
    );
    assert_eq!(
        decides("SELECT id FROM br WHERE (a, id) IN ((10, 1))"),
        None,
        "a row that does not cover one unique index is not a batch point get"
    );
    assert_eq!(
        decides("SELECT id FROM br WHERE (a, b) IN ((10, 'x')) WINDOW w AS ()"),
        None,
        "Go refuses the fast path when the SELECT declares a window"
    );

    crate::run_create_table_on(
        "CREATE TABLE ch (a BIGINT, b VARCHAR(8), v BIGINT, PRIMARY KEY (a, b))",
        &mut catalog,
    )
    .unwrap();
    run_insert_on(
        "INSERT INTO ch VALUES (1, 'x', 10), (2, 'y', 20)",
        &mut catalog,
        &crate::StmtContext::for_query(),
    )
    .unwrap();
    let Some(TableEntry::Kv(common)) = catalog.get_table_for_test("ch") else {
        panic!("expected a common-handle table");
    };
    let common_columns = common
        .columns
        .iter()
        .map(|column| (column.name.clone(), column.field_type.clone()))
        .collect::<Vec<_>>();
    let stmt =
        tidb_parser::parse("SELECT v FROM ch WHERE (b, a) IN (('y', 2), ('x', 1), ('y', 2))")
            .unwrap();
    let Stmt::Query(query) = &stmt else {
        panic!("not a query")
    };
    let QueryStmt::Select(select) = &**query else {
        panic!("not a select")
    };
    let handles = try_batch_point_get(
        select,
        common,
        &common_columns,
        &tidb_datatype::SessionTimeZone::utc(),
        &crate::index_hints::AvailablePaths::unrestricted(),
    )
    .unwrap()
    .expect("the common primary key is the unique access path")
    .handles;
    assert_eq!(handles.len(), 2, "repeated common handles are deduplicated");
    let mut table = common.clone();
    assert_eq!(
        handles
            .iter()
            .map(|handle| {
                table
                    .get_row_by_handle(handle, &tidb_datatype::SessionTimeZone::utc())
                    .unwrap()
                    .unwrap()[2]
                    .clone()
            })
            .collect::<Vec<_>>(),
        vec![Datum::Int(20), Datum::Int(10)]
    );
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

//! Aggregate queries: the aggregate functions, `GROUP BY`, `HAVING`,
//! aggregate `ORDER BY`, and `SELECT DISTINCT`.
//!
//! Mirrors Go `pkg/executor/aggregate`'s hash-aggregate surface, including
//! the distinct path a `DISTINCT` select takes through the same operator.

use super::*;

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

//! `UNION`/`INTERSECT`/`EXCEPT` and common table expressions.
//!
//! Both are query-composition surfaces: a set operation combines two result
//! sets, and a CTE names one for reuse. Mirrors Go `pkg/executor`'s
//! `UnionExec`/`SetOprExec` and CTE executors.

use super::*;

/// Non-recursive CTEs: each is materialized in written order and then
/// resolves like an ordinary table, which is the shape Go's buildWith
/// plans. The previous behavior was an "unknown table" error.
#[test]
fn common_table_expressions() {
    let mut catalog = Catalog::default();
    crate::run_create_table_on("CREATE TABLE c1 (a BIGINT, b BIGINT)", &mut catalog).unwrap();
    run_insert_on(
        "INSERT INTO c1 VALUES (1, 10), (2, 20), (3, 30)",
        &mut catalog,
        &crate::StmtContext::for_query(),
    )
    .unwrap();

    assert_eq!(
        run_select_on(
            "WITH c AS (SELECT a FROM c1 WHERE a > 1) SELECT a FROM c",
            &catalog,
            &crate::StmtContext::for_query()
        )
        .unwrap(),
        vec![vec![Datum::Int(2)], vec![Datum::Int(3)]]
    );
    // The outer query filters, orders and aggregates the CTE like a table.
    assert_eq!(
        run_select_on(
            "WITH c AS (SELECT a, b FROM c1) SELECT SUM(b) FROM c WHERE a >= 2",
            &catalog,
            &crate::StmtContext::for_query()
        )
        .unwrap(),
        vec![vec![Datum::Decimal(tidb_datatype::Decimal::from_int(50))]]
    );
    // A column list renames the CTE's columns.
    assert_eq!(
        run_select_on(
            "WITH c (x) AS (SELECT a FROM c1 WHERE a = 3) SELECT x FROM c",
            &catalog,
            &crate::StmtContext::for_query()
        )
        .unwrap(),
        vec![vec![Datum::Int(3)]]
    );
    // A later CTE may read an earlier one, which is why they are
    // materialized in written order.
    assert_eq!(
        run_select_on(
            "WITH c AS (SELECT a FROM c1 WHERE a > 1), d AS (SELECT a FROM c WHERE a > 2) \
             SELECT a FROM d",
            &catalog,
            &crate::StmtContext::for_query()
        )
        .unwrap(),
        vec![vec![Datum::Int(3)]]
    );
    // A CTE and a real table join.
    assert_eq!(
        run_select_on(
            "WITH c AS (SELECT a FROM c1 WHERE a = 2) SELECT c1.b FROM c JOIN c1 ON c.a = c1.a",
            &catalog,
            &crate::StmtContext::for_query()
        )
        .unwrap(),
        vec![vec![Datum::Int(20)]]
    );
    // A CTE shadows a real table of the same name, as in SQL.
    assert_eq!(
        run_select_on(
            "WITH c1 AS (SELECT 9 AS a) SELECT a FROM c1",
            &catalog,
            &crate::StmtContext::for_query()
        )
        .unwrap(),
        vec![vec![Datum::Int(9)]]
    );

    // WITH RECURSIVE runs the fixpoint rather than returning only the seed
    // row; see `driver::recursive_cte`.
    assert_eq!(
        run_select_on(
            "WITH RECURSIVE c (n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM c WHERE n < 3) \
             SELECT n FROM c",
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
    // A mismatched column list is an error, not a silent rename of some.
    assert!(run_select_on(
        "WITH c (x, y) AS (SELECT a FROM c1) SELECT x FROM c",
        &catalog,
        &crate::StmtContext::for_query()
    )
    .is_err());
}

/// Set operations, checked against results captured from a running TiDB
/// for the same data: u1 = 1,2,2,3 and u2 = 2,3,4.
#[test]
fn set_operations() {
    let mut catalog = Catalog::default();
    crate::run_create_table_on("CREATE TABLE u1 (a BIGINT)", &mut catalog).unwrap();
    crate::run_create_table_on("CREATE TABLE u2 (a BIGINT)", &mut catalog).unwrap();
    run_insert_on(
        "INSERT INTO u1 VALUES (1), (2), (2), (3)",
        &mut catalog,
        &crate::StmtContext::for_query(),
    )
    .unwrap();
    run_insert_on(
        "INSERT INTO u2 VALUES (2), (3), (4)",
        &mut catalog,
        &crate::StmtContext::for_query(),
    )
    .unwrap();

    let sorted = |sql: &str, catalog: &Catalog| {
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
    let listed = |sql: &str, catalog: &Catalog| {
        run_select_on(sql, catalog, &crate::StmtContext::for_query())
            .unwrap()
            .into_iter()
            .map(|row| match row[0] {
                Datum::Int(value) => value,
                ref other => panic!("expected an int, got {other:?}"),
            })
            .collect::<Vec<_>>()
    };

    // Captured: UNION dedups (TiDB returned 4,1,2,3 in hash order, so the
    // comparison sorts); UNION ALL concatenates in term order.
    assert_eq!(
        sorted("SELECT a FROM u1 UNION SELECT a FROM u2", &catalog),
        vec![1, 2, 3, 4]
    );
    assert_eq!(
        listed("SELECT a FROM u1 UNION ALL SELECT a FROM u2", &catalog),
        vec![1, 2, 2, 3, 2, 3, 4],
        "captured: UNION ALL keeps duplicates and term order"
    );
    // Captured: EXCEPT -> [1], INTERSECT -> [2, 3] (hash order).
    assert_eq!(
        listed("SELECT a FROM u1 EXCEPT SELECT a FROM u2", &catalog),
        vec![1]
    );
    assert_eq!(
        sorted("SELECT a FROM u1 INTERSECT SELECT a FROM u2", &catalog),
        vec![2, 3]
    );
    // The ALL forms keep multiplicity: u1 has 2 twice, u2 once.
    assert_eq!(
        listed("SELECT a FROM u1 INTERSECT ALL SELECT a FROM u2", &catalog),
        vec![2, 3]
    );
    assert_eq!(
        listed("SELECT a FROM u1 EXCEPT ALL SELECT a FROM u2", &catalog),
        vec![1, 2],
        "one of the two 2s survives EXCEPT ALL"
    );

    // A statement-level ORDER BY and LIMIT apply to the folded result.
    // Captured: ... ORDER BY a DESC -> 4,3,2,1.
    assert_eq!(
        listed(
            "SELECT a FROM u1 UNION SELECT a FROM u2 ORDER BY a DESC",
            &catalog
        ),
        vec![4, 3, 2, 1]
    );
    assert_eq!(
        listed(
            "SELECT a FROM u1 UNION SELECT a FROM u2 ORDER BY a LIMIT 2",
            &catalog
        ),
        vec![1, 2]
    );

    // Three terms fold left to right.
    assert_eq!(
        sorted(
            "SELECT a FROM u1 UNION SELECT a FROM u2 UNION SELECT 9",
            &catalog
        ),
        vec![1, 2, 3, 4, 9]
    );
    // A CTE prefix belongs to the whole statement.
    assert_eq!(
        sorted(
            "WITH c AS (SELECT a FROM u1 WHERE a = 3) \
             SELECT a FROM c UNION SELECT a FROM u2",
            &catalog
        ),
        vec![2, 3, 4]
    );

    // Captured: a term of a different width is 1222.
    assert!(matches!(
        run_select_on(
            "SELECT a FROM u1 UNION SELECT a, a FROM u2",
            &catalog,
            &crate::StmtContext::for_query()
        ),
        Err(DriverError::WrongNumberOfColumnsInSelect)
    ));
}

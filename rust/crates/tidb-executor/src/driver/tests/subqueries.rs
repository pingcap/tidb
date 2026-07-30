//! Subqueries: uncorrelated, correlated, and correlated inside an aggregate.
//!
//! The correlated cases are the interesting ones -- the inner query is
//! re-evaluated per outer row, and the grouped case pushes that re-evaluation
//! under an aggregate. Mirrors Go `pkg/executor`'s apply and
//! `pkg/planner/core`'s correlated-column handling.

use super::*;

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
    // Still REFUSED (documented, not silently wrong), and it is the SHAPE that
    // refuses, not the placement: `extract_correlated_subquery` does not walk
    // into a `CASE` arm, so the subquery stays in the aggregate's argument and
    // `build_agg_func` reports it by name instead of falling through to the
    // expression rewriter's generic message. Go answers
    // `<nil>|0;1|30;2|5;3|0`.
    assert!(matches!(
        run_select_on(
            "SELECT g, SUM(CASE WHEN EXISTS(SELECT 1 FROM s WHERE s.k = t.g) THEN v ELSE 0 END) \
             FROM t GROUP BY g",
            &catalog,
            &crate::StmtContext::for_query()
        ),
        Err(DriverError::Unsupported(_))
    ));

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

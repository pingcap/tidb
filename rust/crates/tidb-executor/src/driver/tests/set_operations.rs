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
    // Go's planner deliberately has no ALL implementation for these two
    // set operations (`buildIntersect`/`buildExcept` refuse them before
    // planning), so execution must not silently invent multiset semantics.
    for (sql, message) in [
        (
            "SELECT a FROM u1 INTERSECT ALL SELECT a FROM u2",
            "TiDB do not support intersect all",
        ),
        (
            "SELECT a FROM u1 EXCEPT ALL SELECT a FROM u2",
            "TiDB do not support except all",
        ),
    ] {
        assert!(matches!(
            run_select_on(sql, &catalog, &crate::StmtContext::for_query()),
            Err(crate::driver::DriverError::Unsupported(reason)) if reason == message
        ));
    }

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

/// Go's outer-join eliminator receives the duplicate-agnostic columns from a
/// UNION DISTINCT's HashAgg, so an unread non-unique inner side disappears
/// from that operand. UNION ALL must retain it because its duplicate rows are
/// observable.
#[test]
fn union_distinct_propagates_duplicate_agnostic_outer_join_elimination() {
    use crate::explain::{explain_set_opr_stmt, ExplainFormat};

    let mut catalog = Catalog::default();
    crate::run_create_table_on("CREATE TABLE ul (id BIGINT PRIMARY KEY)", &mut catalog).unwrap();
    crate::run_create_table_on("CREATE TABLE ur (k BIGINT)", &mut catalog).unwrap();
    let ctx = crate::StmtContext::for_query();
    run_insert_on("INSERT INTO ul VALUES (1),(2)", &mut catalog, &ctx).unwrap();
    run_insert_on("INSERT INTO ur VALUES (1),(1)", &mut catalog, &ctx).unwrap();

    let plan_mentions_inner = |sql: &str| {
        let Stmt::Query(query) = tidb_parser::parse(sql).unwrap() else {
            panic!("not a query")
        };
        let QueryStmt::SetOpr(set_opr) = &*query else {
            panic!("not a set operation")
        };
        let (_, rows) =
            explain_set_opr_stmt(set_opr, &catalog, "test", &ctx, ExplainFormat::Row).unwrap();
        rows.iter().flatten().any(|value| match value {
            Datum::Bytes(bytes) => String::from_utf8_lossy(bytes).contains("table:ur"),
            _ => false,
        })
    };

    assert!(!plan_mentions_inner(
        "SELECT id FROM ul UNION SELECT ul.id FROM ul LEFT JOIN ur ON ul.id = ur.k"
    ));
    assert!(plan_mentions_inner(
        "SELECT id FROM ul UNION ALL SELECT ul.id FROM ul LEFT JOIN ur ON ul.id = ur.k"
    ));
}

/// A `UNION DISTINCT` fixpoint deduplicates against ONE accumulating hash
/// table (Go `cteProducer.hashTbl`), not by re-deduplicating the whole result
/// every round. Re-deduplicating is quadratic, and this recursion -- straight
/// out of `tests/integrationtest/t/executor/admin.test` -- is the case that
/// makes the difference visible: TiDB answers it in seconds.
///
/// Captured from real TiDB with `difftests/gorun` (3.6s wall):
///
/// ```text
/// set @@cte_max_recursion_depth = 200000;
/// with recursive cte(a,b) as (select 1,1 union select a+1,b+1 from cte
///   where cte.a < 100000) select count(*), max(a), min(a) from cte;
///     -> 100000|100000|1
/// ```
///
/// The row count is the assertion; the wall clock is the point. A quadratic
/// fold does not finish this test at all.
#[test]
fn a_distinct_fixpoint_dedups_incrementally() {
    let catalog = Catalog::default();
    let ctx = crate::StmtContext::for_query().with_cte_max_recursion_depth(200_000);
    let rows = run_select_on(
        "WITH RECURSIVE cte(a,b) AS (SELECT 1,1 UNION SELECT a+1,b+1 FROM cte \
         WHERE cte.a < 100000) SELECT count(*), max(a), min(a) FROM cte",
        &catalog,
        &ctx,
    )
    .unwrap();
    assert_eq!(
        rows,
        vec![vec![
            Datum::Int(100_000),
            Datum::Int(100_000),
            Datum::Int(1)
        ]]
    );
}

/// A recursive CTE's schema is the SEED's, and every recursive block is CAST
/// into it -- Go `buildProjection4CTEUnion` +
/// `expression.BuildCastFunction4Union`. A recursive block that produces a
/// different kind does not widen the CTE; it is cast.
///
/// Captured from real TiDB with `difftests/gorun`:
///
/// ```text
/// with recursive t (a,b,c,d) AS ( select 1,2,3,4 UNION
///   select a+1,b+1,c+1,concat(d,1) from t where a < 5 ) select * from t;
///     -> 1|2|3|4; 2|3|4|41; 3|4|5|411; 4|5|6|4111; 5|6|7|41111
/// with recursive t (a,b) AS ( select 1,'2' UNION
///   select a+1, concat(b,'x') from t where a < 3 ) select * from t;
///     -> 1|2; 2|2; 3|2         (a divergence here; see the body)
/// with recursive t (a,b) AS ( select 1, cast(2 as char(20)) UNION ALL
///   select a+1, concat(b,'x') from t where a < 3 ) select * from t;
///     -> 1|2; 2|2x; 3|2xx
/// with recursive t (a,b) AS ( select 1, 2 UNION ALL
///   select a+1, 'zz' from t where a < 3 ) select * from t;
///     -> 1|2; 2|0; 3|0
/// ```
#[test]
fn a_recursive_block_is_cast_into_the_seed_schema() {
    let catalog = Catalog::default();
    let ctx = crate::StmtContext::for_query();
    let run = |sql: &str| run_select_on(sql, &catalog, &ctx).unwrap();

    // `d` is the seed's bigint, so `concat(d, 1)` is cast BACK to a bigint --
    // this is the case that panicked, appending bytes to a fixed-length
    // column.
    assert_eq!(
        run("WITH RECURSIVE t (a,b,c,d) AS ( SELECT 1, 2, 3, 4 UNION \
             SELECT a + 1, b + 1, c + 1, concat(d, 1) FROM t WHERE a < 5 ) SELECT * FROM t"),
        vec![
            vec![Datum::Int(1), Datum::Int(2), Datum::Int(3), Datum::Int(4)],
            vec![Datum::Int(2), Datum::Int(3), Datum::Int(4), Datum::Int(41)],
            vec![Datum::Int(3), Datum::Int(4), Datum::Int(5), Datum::Int(411)],
            vec![
                Datum::Int(4),
                Datum::Int(5),
                Datum::Int(6),
                Datum::Int(4111)
            ],
            vec![
                Datum::Int(5),
                Datum::Int(6),
                Datum::Int(7),
                Datum::Int(41111)
            ],
        ]
    );

    // DIVERGENCE, named rather than asserted: TiDB answers `2; 2; 2` for the
    // `'2'` seed, because a string LITERAL's field type carries flen = 1 and
    // the cast truncates "2x" back to it. This tier infers an unspecified
    // flen for a select-list literal, so the same query grows to `2; 2x; 2xx`
    // here. That gap is in literal type inference, not in the CTE cast -- the
    // `cast(2 as char(20))` case below proves the cast honours a flen it is
    // given.

    // `cast(2 as char(20))` leaves room, so the recursion grows.
    let rows = run(
        "WITH RECURSIVE t (a,b) AS ( SELECT 1, cast(2 AS CHAR(20)) UNION ALL \
         SELECT a + 1, concat(b, 'x') FROM t WHERE a < 3 ) SELECT * FROM t",
    );
    assert_eq!(
        rows.iter()
            .map(|row| datum_text_for_test(&row[1]))
            .collect::<Vec<_>>(),
        vec!["2".to_owned(), "2x".to_owned(), "2xx".to_owned()]
    );

    // A cast that cannot parse a number is SILENT and gives 0, exactly as
    // `select cast('zz' as signed)` does -- it is an expression cast, not the
    // INSERT path's strict conversion.
    assert_eq!(
        run("WITH RECURSIVE t (a,b) AS ( SELECT 1, 2 UNION ALL \
             SELECT a + 1, 'zz' FROM t WHERE a < 3 ) SELECT * FROM t"),
        vec![
            vec![Datum::Int(1), Datum::Int(2)],
            vec![Datum::Int(2), Datum::Int(0)],
            vec![Datum::Int(3), Datum::Int(0)],
        ]
    );
}

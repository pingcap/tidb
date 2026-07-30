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

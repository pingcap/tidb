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

#![cfg(test)]

//! `WITH RECURSIVE` semantics. Every assertion here is a capture of real TiDB
//! taken with `rust/difftests/gorun` on the same schema, including the errnos
//! -- not inferred from the fixpoint's shape.

use crate::tests_support::*;
use crate::*;

/// The one column of every row, as text.
fn column(result: Result<StmtResult, DriverError>) -> Vec<String> {
    row_text(result)
        .into_iter()
        .map(|mut row| row.remove(0))
        .collect()
}

/// The `(errno, message)` a failing statement reports on the wire.
fn wire_error(session: &mut Session, sql: &str) -> (u16, String) {
    let error = session.run(sql).expect_err("statement must fail");
    let rendered = error.to_mysql_error();
    (rendered.code, rendered.message)
}

/// The classic counter, and the delta rule that makes it come out right: each
/// round sees ONLY the previous round's new rows. A whole-table rescan would
/// re-derive `2` from `1` once `2` itself joined the table.
#[test]
fn counter_iterates_over_the_previous_rounds_delta() {
    let mut session = Session::new();
    assert_eq!(
        column(session.run(
            "WITH RECURSIVE t(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM t WHERE n < 5) \
             SELECT * FROM t ORDER BY n"
        )),
        vec!["1", "2", "3", "4", "5"]
    );
}

/// `UNION` deduplicates against the WHOLE accumulated result every round,
/// which is what lets a cyclic graph terminate at all; `UNION ALL` does not,
/// and the same cycle then runs until the depth bound refuses it.
#[test]
fn union_dedups_a_cycle_where_union_all_diverges() {
    let mut session = Session::new();
    session.run("CREATE TABLE g (a INT, b INT)").unwrap();
    session
        .run("INSERT INTO g VALUES (1,2),(2,3),(3,1)")
        .unwrap();

    assert_eq!(
        column(session.run(
            "WITH RECURSIVE r(x) AS (SELECT 1 UNION SELECT g.b FROM g, r WHERE g.a = r.x) \
             SELECT * FROM r ORDER BY x"
        )),
        vec!["1", "2", "3"]
    );

    let (code, _) = wire_error(
        &mut session,
        "WITH RECURSIVE r(x) AS (SELECT 1 UNION ALL SELECT g.b FROM g, r WHERE g.a = r.x) \
         SELECT * FROM r",
    );
    assert_eq!(code, 3636);
}

/// The depth bound is `@@cte_max_recursion_depth` ROUNDS, and the round it
/// refuses is the round it reports -- one past the limit. A limit of 3 lets a
/// recursion that needs 3 rounds through and aborts "after 4 iterations" on
/// one that needs 4.
#[test]
fn depth_bound_reports_the_round_it_refused() {
    let mut session = Session::new();

    assert_eq!(
        wire_error(
            &mut session,
            "WITH RECURSIVE t(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM t) SELECT * FROM t"
        ),
        (
            3636,
            "Recursive query aborted after 1001 iterations. Try increasing \
             @@cte_max_recursion_depth to a larger value"
                .to_owned()
        )
    );

    session.run("SET @@cte_max_recursion_depth = 3").unwrap();
    assert_eq!(
        column(session.run(
            "WITH RECURSIVE t(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM t WHERE n<3) \
             SELECT * FROM t ORDER BY n"
        )),
        vec!["1", "2", "3"]
    );
    assert_eq!(
        wire_error(
            &mut session,
            "WITH RECURSIVE t(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM t WHERE n<4) \
             SELECT * FROM t"
        ),
        (
            3636,
            "Recursive query aborted after 4 iterations. Try increasing \
             @@cte_max_recursion_depth to a larger value"
                .to_owned()
        )
    );
}

/// The default is exactly 1000 rounds: a counter needing 1000 succeeds and one
/// needing 1001 aborts.
#[test]
fn default_depth_is_one_thousand_rounds() {
    let mut session = Session::new();
    assert_eq!(
        column(session.run(
            "WITH RECURSIVE t(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM t WHERE n<1000) \
             SELECT COUNT(*) FROM t"
        )),
        vec!["1000"]
    );
    assert_eq!(
        wire_error(
            &mut session,
            "WITH RECURSIVE t(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM t WHERE n<1001) \
             SELECT COUNT(*) FROM t"
        )
        .0,
        3636
    );
}

/// A `LIMIT` on the CTE's own definition caps the TOTAL accumulated rows
/// across every round and stops the fixpoint -- so it terminates a recursion
/// the depth bound would otherwise refuse. `OFFSET` widens the target and is
/// applied as an ordinary window at the end; `LIMIT 0` short-circuits before
/// any round runs; a limit past the natural fixpoint is a no-op.
#[test]
fn definition_limit_caps_total_rows_and_ends_the_recursion() {
    let mut session = Session::new();
    let counter = |bound: &str| {
        format!(
            "WITH RECURSIVE t(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM t WHERE n<1000000 \
             {bound}) SELECT n FROM t ORDER BY n"
        )
    };
    assert_eq!(
        column(session.run(&counter("LIMIT 5"))),
        vec!["1", "2", "3", "4", "5"]
    );
    assert_eq!(
        column(session.run(&counter("LIMIT 3 OFFSET 2"))),
        vec!["3", "4", "5"]
    );
    assert_eq!(
        column(session.run(&counter("LIMIT 0"))),
        Vec::<String>::new()
    );
    assert_eq!(
        column(session.run(
            "WITH RECURSIVE t(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM t WHERE n<3 LIMIT 100) \
             SELECT n FROM t ORDER BY n"
        )),
        vec!["1", "2", "3"]
    );

    // A round may overshoot: two recursive blocks fire together, and the
    // surplus is dropped in the order the blocks produced it.
    assert_eq!(
        column(session.run(
            "WITH RECURSIVE t(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM t \
             UNION ALL SELECT n+100 FROM t WHERE n<3 LIMIT 4) SELECT n FROM t ORDER BY n"
        )),
        vec!["1", "2", "3", "101"]
    );
}

/// `RECURSIVE` is a CLAUSE-level flag, not a per-CTE one: a CTE that never
/// names itself is simply evaluated once, whether or not the keyword is there,
/// and it may sit beside a genuinely recursive sibling. Conversely, a
/// self-reference WITHOUT the keyword names no table at all.
#[test]
fn recursive_is_a_clause_flag_not_a_per_cte_one() {
    let mut session = Session::new();

    assert_eq!(
        column(session.run("WITH RECURSIVE t AS (SELECT 1 AS n) SELECT * FROM t")),
        vec!["1"]
    );
    assert_eq!(
        column(session.run(
            "WITH RECURSIVE a AS (SELECT 1 AS n UNION ALL SELECT n+1 FROM a WHERE n<2), \
             b AS (SELECT * FROM a) SELECT n FROM b ORDER BY n"
        )),
        vec!["1", "2"]
    );
    // A UNION-bodied CTE needs no RECURSIVE at all when nothing self-names.
    assert_eq!(
        column(session.run("WITH c AS (SELECT 1 AS n UNION SELECT 2) SELECT n FROM c ORDER BY n")),
        vec!["1", "2"]
    );
    assert!(matches!(
        session.run(
            "WITH c AS (SELECT 1 AS n UNION ALL SELECT n+1 FROM c WHERE n<5) \
                     SELECT * FROM c"
        ),
        Err(DriverError::Schema(SchemaErrorKind::UnknownTable(_)))
    ));
}

/// The blocks split into a leading run of seeds and a trailing run of
/// recursive ones; both runs may hold more than one block.
#[test]
fn multiple_seed_and_recursive_blocks() {
    let mut session = Session::new();
    assert_eq!(
        column(session.run(
            "WITH RECURSIVE t(n) AS (SELECT 1 UNION ALL SELECT 2 UNION ALL \
             SELECT n+1 FROM t WHERE n<4) SELECT * FROM t ORDER BY n"
        )),
        vec!["1", "2", "2", "3", "3", "4", "4"]
    );
    assert_eq!(
        column(session.run(
            "WITH RECURSIVE t(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM t WHERE n<4 \
             UNION ALL SELECT n+10 FROM t WHERE n<3) SELECT * FROM t ORDER BY n"
        )),
        vec!["1", "2", "3", "4", "11", "12"]
    );
}

/// The materialized CTE may be referenced more than once at the OUTER level --
/// which is a self-JOIN error only inside a recursive block.
#[test]
fn outer_query_may_reference_the_cte_twice() {
    let mut session = Session::new();
    assert_eq!(
        row_text(session.run(
            "WITH RECURSIVE t(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM t WHERE n<2) \
             SELECT c1.n, c2.n FROM t c1 JOIN t c2 ON c1.n = c2.n ORDER BY c1.n"
        )),
        vec![
            vec!["1".to_owned(), "1".to_owned()],
            vec!["2".to_owned(), "2".to_owned()],
        ]
    );
}

/// The `t(n)` column list binds the names the RECURSIVE block itself reads, so
/// it has to be applied to the seed before any round runs. A width mismatch is
/// Go's `ErrViewWrongList`.
#[test]
fn column_list_binds_before_the_first_round() {
    let mut session = Session::new();
    assert_eq!(
        column(session.run(
            "WITH RECURSIVE t(n) AS (SELECT 1 AS other UNION ALL SELECT n+1 FROM t WHERE n<3) \
             SELECT n FROM t ORDER BY n"
        )),
        vec!["1", "2", "3"]
    );
    assert_eq!(
        wire_error(
            &mut session,
            "WITH RECURSIVE t(n,m) AS (SELECT 1 UNION ALL SELECT n+1 FROM t WHERE n<3) \
             SELECT * FROM t"
        )
        .0,
        1353
    );
}

/// Every restriction on a recursive block, with the errno Go reports. These
/// are genuine errors, not this tier's own scope boundaries.
#[test]
fn recursive_block_restrictions_report_gos_errnos() {
    let mut session = Session::new();
    let cases: &[(&str, u16)] = &[
        // A bare SELECT body that names itself has no seed to start from.
        (
            "WITH RECURSIVE t(n) AS (SELECT n FROM t) SELECT * FROM t",
            3573,
        ),
        // A recursive block first, with no seed ahead of it.
        (
            "WITH RECURSIVE t(n) AS (SELECT n+1 FROM t WHERE n<3 UNION ALL SELECT 1) \
             SELECT * FROM t",
            3574,
        ),
        // A non-recursive block AFTER a recursive one.
        (
            "WITH RECURSIVE t(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM t WHERE n<4 \
             UNION ALL SELECT 99) SELECT * FROM t",
            3574,
        ),
        (
            "WITH RECURSIVE t(n) AS (SELECT 1 UNION ALL SELECT MAX(n)+1 FROM t WHERE n<3) \
             SELECT * FROM t",
            3575,
        ),
        (
            "WITH RECURSIVE t(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM t WHERE n<3 GROUP BY n) \
             SELECT * FROM t",
            3575,
        ),
        // Referenced twice: a self-join within the block.
        (
            "WITH RECURSIVE t(n) AS (SELECT 1 UNION ALL SELECT a.n+1 FROM t a, t b WHERE a.n<3) \
             SELECT * FROM t",
            3577,
        ),
        // Referenced from a scalar subquery.
        (
            "WITH RECURSIVE t(n) AS (SELECT 1 UNION ALL SELECT (SELECT MAX(n) FROM t)+1 FROM t \
             WHERE n<3) SELECT * FROM t",
            3577,
        ),
        // Referenced from a derived table rather than plainly in FROM.
        (
            "WITH RECURSIVE t(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM (SELECT * FROM t) x \
             WHERE n<3) SELECT * FROM t",
            3577,
        ),
        (
            "WITH RECURSIVE t(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM t WHERE n<3 ORDER BY n) \
             SELECT * FROM t",
            1235,
        ),
        (
            "WITH RECURSIVE t(n) AS (SELECT 1 UNION ALL SELECT DISTINCT n+1 FROM t WHERE n<3) \
             SELECT * FROM t",
            1235,
        ),
        (
            "WITH RECURSIVE t(n) AS (SELECT 1 EXCEPT SELECT n FROM t) SELECT * FROM t",
            1235,
        ),
        (
            "WITH RECURSIVE t(n) AS (SELECT 1 INTERSECT SELECT n FROM t) SELECT * FROM t",
            1235,
        ),
    ];
    for (sql, want) in cases {
        assert_eq!(wire_error(&mut session, sql).0, *want, "for {sql}");
    }
}

#![cfg(test)]

//! `WITH RECURSIVE` semantics. Every assertion here is a capture of real TiDB
//! taken with `rust/difftests/gorun` on the same schema, including the errnos
//! -- not inferred from the fixpoint's shape.

use crate::tests_support::*;
use crate::*;
use std::sync::Arc;

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

/// The CTE producer is a real spill-backed operator: with temporary storage
/// enabled the quota spills and the query completes; with the same quota and
/// temporary storage disabled it is TiDB's 8175. Both paths release their
/// statement resources before the next command.
#[test]
fn cte_storage_obeys_the_session_spill_policy_and_quota() {
    let path =
        std::env::temp_dir().join(format!("tidb-session-cte-storage-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&path);
    let storage = Arc::new(
        tidb_util::spill_storage::SpillStorage::open(tidb_util::spill_storage::SpillStorageSpec {
            path: path.clone(),
            quota_bytes: -1,
            encryption: tidb_util::spill_storage::SpillEncryptionMethod::Plaintext,
        })
        .unwrap(),
    );
    let mut session = Session::new();
    session.set_spill_storage(Arc::clone(&storage));
    // Keep the quota below even one stored chunk. Go's own CTE spill test
    // forces the spill action rather than depending on allocator-size
    // accidents; a one-byte quota gives this integration boundary the same
    // deterministic trigger without a failpoint.
    session.run("SET @@tidb_mem_quota_query = 1").unwrap();
    session
        .run("SET @@global.tidb_enable_tmp_storage_on_oom = 1")
        .unwrap();

    let sql = "WITH RECURSIVE t(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM t WHERE n<128) \
               SELECT COUNT(*) FROM t";
    assert_eq!(column(session.run(sql)), vec!["128"]);
    assert!(storage.global_tracker().max_consumed() > 0);
    assert_eq!(storage.global_tracker().bytes_consumed(), 0);

    session
        .run("SET @@global.tidb_enable_tmp_storage_on_oom = 0")
        .unwrap();
    assert_eq!(wire_error(&mut session, sql).0, 8175);
    assert_eq!(storage.global_tracker().bytes_consumed(), 0);

    session
        .run("SET @@global.tidb_enable_tmp_storage_on_oom = 1")
        .unwrap();
    drop(session);
    drop(storage);
    std::fs::remove_dir_all(path).unwrap();
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

    session.run("ANALYZE TABLE g").unwrap();
    assert_eq!(
        column(session.run(
            "WITH RECURSIVE r(x) AS (SELECT 1 UNION SELECT g.b FROM g, r WHERE g.a = r.x) \
             SELECT * FROM r ORDER BY x"
        )),
        vec!["1", "2", "3"]
    );
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

/// Go checks the next recursive round against the depth bound before the
/// accumulated LIMIT short-circuit. `LIMIT 0` leaves the delta empty and runs
/// no round; `LIMIT 1` retains the seed and therefore attempts round one.
#[test]
fn depth_check_precedes_the_definition_limit_short_circuit() {
    let mut session = Session::new();
    session.run("SET @@cte_max_recursion_depth = 0").unwrap();

    assert_eq!(
        column(session.run(
            "WITH RECURSIVE t(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM t LIMIT 0) \
             SELECT * FROM t"
        )),
        Vec::<String>::new()
    );
    assert_eq!(
        wire_error(
            &mut session,
            "WITH RECURSIVE t(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM t LIMIT 1) \
             SELECT * FROM t"
        ),
        (
            3636,
            "Recursive query aborted after 1 iterations. Try increasing \
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

/// Go's set-operation preprocessor rejects LIMIT on an unparenthesized term
/// before the last UNION operand. Parentheses give the LIMIT its own scope;
/// a trailing LIMIT belongs to the whole set operation.
#[test]
fn union_limit_requires_parentheses_around_a_non_final_term() {
    let mut session = Session::new();
    assert_eq!(
        wire_error(&mut session, "SELECT 1 LIMIT 1 UNION SELECT 2"),
        (1221, "Incorrect usage of UNION and LIMIT".to_owned())
    );
    assert_eq!(
        wire_error(&mut session, "SELECT 1 ORDER BY 1 UNION SELECT 2"),
        (1221, "Incorrect usage of UNION and ORDER BY".to_owned())
    );
    assert_eq!(
        row_text(session.run("(SELECT 1 LIMIT 1) UNION SELECT 2 ORDER BY 1")),
        [["1"], ["2"]]
    );
    assert_eq!(
        row_text(session.run("SELECT 1 UNION SELECT 2 ORDER BY 1 LIMIT 1")),
        [["1"]]
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
    crate::tests_support::assert_err(
        session.run(
            "WITH c AS (SELECT 1 AS n UNION ALL SELECT n+1 FROM c WHERE n<5) \
                     SELECT * FROM c",
        ),
        1146,
        "Table 'test.c' doesn't exist",
    );
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
        let got = wire_error(&mut session, sql);
        assert_eq!(got.0, *want, "for {sql}: {}", got.1);
    }
}

/// Go `LogicalCTE.DeriveStats` (`logical_cte.go:172`) optimizes a CTE class
/// lazily, the first time a reference's statistics are derived: the seed is
/// optimized with the DNF of every reference's recorded predicates, its
/// statistics are published to `SeedStat`, and only THEN is the recursive
/// part optimized, so its `CTETable` reads the seed's row count. Expected
/// rows are Go master 8936d7bdcb's `explain format='brief'` output.
#[test]
fn cte_classes_are_optimized_from_derive_stats_like_go() {
    let mut session = Session::new();
    for sql in [
        "CREATE TABLE uj1 (id INT PRIMARY KEY, left_v INT)",
        "INSERT INTO uj1 VALUES (1,10),(2,20),(3,30)",
    ] {
        session.run(sql).unwrap();
    }
    let explain = |session: &mut Session, sql: &str| -> Vec<String> {
        row_text(session.run(sql))
            .into_iter()
            .map(|row| format!("{}|{}|{}", row[0], row[1], row[4]))
            .collect()
    };
    assert_eq!(
        explain(
            &mut session,
            "explain format='brief' WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL \
             SELECT n+1 FROM r WHERE n < 3) SELECT * FROM r a JOIN r b ON a.n = b.n \
             WHERE a.n > 1"
        ),
        [
            "HashJoin|1.30|inner join, equal:[eq(Column#6, Column#7)]",
            "├─Selection(Build)|1.44|gt(Column#7, 1), not(isnull(Column#7))",
            "│ └─CTEFullScan|1.80|data:CTE_0",
            "└─Selection(Probe)|1.44|gt(Column#6, 1), not(isnull(Column#6))",
            "  └─CTEFullScan|1.80|data:CTE_0",
            "CTE_0|1.80|Recursive CTE",
            "├─Projection(Seed Part)|1.00|1->Column#2",
            "│ └─TableDual|1.00|rows:1",
            "└─Projection(Recursive Part)|0.80|cast(plus(Column#3, 1), bigint BINARY)->Column#5",
            "  └─Selection|0.80|lt(Column#3, 3)",
            "    └─CTETable|1.00|Scan on CTE_0",
        ]
    );
    // Both references' predicates reach the seed as one DNF.
    assert_eq!(
        explain(
            &mut session,
            "explain format='brief' WITH c AS (SELECT id, left_v FROM uj1) SELECT * \
             FROM c c1 JOIN c c2 ON c1.id = c2.id WHERE c1.left_v = 10 AND c2.left_v > 5"
        )[5..],
        [
            "CTE_0|3333.33|Non-Recursive CTE",
            "└─TableReader(Seed Part)|3333.33|data:Selection",
            "  └─Selection|3333.33|or(eq(test.uj1.left_v, 10), gt(test.uj1.left_v, 5))",
            "    └─TableFullScan|10000.00|keep order:false, stats:pseudo",
        ]
    );
    assert_eq!(
        row_text(session.run(
            "WITH c AS (SELECT id, left_v FROM uj1) SELECT * FROM c c1, c c2 \
             WHERE c1.id = 1 AND c2.id = 2"
        ))
        .into_iter()
        .map(|row| row.join("|"))
        .collect::<Vec<_>>(),
        ["1|10|2|20"]
    );
}

/// Go `UpdateStmt.With` / `DeleteStmt.With` (expression/misc
/// TestCTEWithDML): the statement's CTEs resolve in its WHERE subqueries and
/// FROM, and a CTE named as the write target is 1288, even when a table of
/// that name exists.
#[test]
fn with_clause_scopes_the_whole_update_and_delete() {
    let mut session = Session::new();
    session.run("create table t1(a int)").unwrap();
    session.run("insert into t1 values(2),(3)").unwrap();
    session
        .run("insert into t1 with t1 as (select 36 as col from t1) select * from t1")
        .unwrap();
    session
        .run("with cte1(a) as (select 36) update t1 set a = 1 where a in (select a from cte1)")
        .unwrap();
    assert_eq!(
        column(session.run("select * from t1 order by a")),
        vec!["1", "1", "2", "3"]
    );
    session
        .run(
            "with recursive cte(a) as (select 1 union select a + 1 from cte where a < 10) \
             update cte, t1 set t1.a=1",
        )
        .unwrap();
    assert_eq!(
        column(session.run("select * from t1")),
        vec!["1", "1", "1", "1"]
    );
    for (sql, statement) in [
        (
            "with recursive cte(a) as (select 1 union select a + 1 from cte where a < 10) \
             update cte set a=1",
            "UPDATE",
        ),
        (
            "with recursive cte(a) as (select 1 union select a + 1 from cte where a < 10) \
             delete from cte",
            "DELETE",
        ),
        (
            "with cte(a) as (select a from t1) delete from cte",
            "DELETE",
        ),
        (
            "with cte(a) as (select a from t1) update cte set a=1",
            "UPDATE",
        ),
    ] {
        assert_eq!(
            wire_error(&mut session, sql),
            (
                1288,
                format!("The target table cte of the {statement} is not updatable")
            ),
            "{sql}"
        );
    }
    assert_eq!(
        wire_error(
            &mut session,
            "with t1 as (select 5 as a) update t1 set a = 2"
        ),
        (
            1288,
            "The target table t1 of the UPDATE is not updatable".to_owned()
        )
    );
    session
        .run("with c(a) as (select 1) delete from t1 where a in (select a from c) limit 3")
        .unwrap();
    assert_eq!(column(session.run("select * from t1")), vec!["1"]);
}

/// Three recursive-part rules Go applies (oracle-captured, `cte`,
/// `planner/core/lateral_join`, `physical_plan`):
/// - ORDER BY / LIMIT is allowed inside a LATERAL body
///   (`buildingLateralSubquery`);
/// - a CTE whose body holds a LIMIT, ORDER BY, DISTINCT, aggregation or window
///   is not inlined into a recursive part (`containRecursiveForbiddenOperator`),
///   so its own LIMIT is not judged as the recursive part's;
/// - the recursive self-reference takes its alias (`FROM cte M ... M.col`).
#[test]
fn recursive_parts_follow_gos_lateral_inline_and_alias_rules() {
    let mut session = Session::new();
    for sql in [
        "create table c (id int, parent_id int, sort_order int)",
        "insert into c values (1, null, 1), (2, 1, 1), (3, 1, 2), (4, 1, 3), (5, 2, 1)",
    ] {
        session.run(sql).unwrap();
    }
    let mut rows = row_text(session.run(
        "with recursive tree as (select id, 1 as depth from c where parent_id is null \
         union all select l.id, tree.depth + 1 from tree cross join lateral \
         (select id from c where parent_id = tree.id order by sort_order limit 2) as l \
         where tree.depth < 3) select id, depth from tree",
    ));
    rows.sort();
    assert_eq!(rows, [["1", "1"], ["2", "2"], ["3", "2"], ["5", "3"]]);

    let rows = row_text(session.run(
        "with recursive top1 (x) as (select id from c order by id limit 1), \
         walk (x) as (select id from c where id = 1 union all \
         select top1.x from top1 join walk on top1.x = walk.x + 100) select * from walk",
    ));
    assert_eq!(rows, [["1"]]);

    let mut rows = row_text(session.run(
        "with recursive up as (select id, parent_id from c where id = 5 \
         union all select p.id, p.parent_id from up m join c p on m.parent_id = p.id) \
         select id from up",
    ));
    rows.sort();
    assert_eq!(rows, [["1"], ["2"], ["5"]]);
}

/// Go `preprocessor.handleTableName` leaves an unqualified name that names a
/// usable CTE unresolved: it needs no current database, and it shadows a
/// stored table of the same name -- in the single-table shapes Go's fast
/// point plan inspects too (`tryPointGetPlan` finds no `TableNameW` for a CTE
/// and declines). Any other unqualified name is `ErrNoDB` wherever it sits,
/// never an unknown database ''. Captured from Go's mock-store oracle.
#[test]
fn cte_names_resolve_without_a_current_database() {
    let mut session = Session::new();
    session
        .run("create table t (id int not null primary key, v int, k int, key ik(k))")
        .unwrap();
    session
        .run("insert into t values (0, 0, 0), (1, 101, 1), (2, 2, 2), (12, 12, 2)")
        .unwrap();
    assert_eq!(
        row_text(session.run("WITH t AS (SELECT 5 id, 66 v, 77 k) SELECT * FROM t WHERE id = 5")),
        [["5", "66", "77"]]
    );
    assert_eq!(
        row_text(
            session.run("WITH t AS (SELECT 5 id, 66 v, 77 k) SELECT * FROM t WHERE id IN (5, 6)")
        ),
        [["5", "66", "77"]]
    );

    session.deselect_database();
    assert_eq!(
        column(session.run(
            "WITH RECURSIVE c(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM c WHERE n < 5) \
             SELECT SUM(n) FROM c"
        )),
        vec!["15"]
    );
    assert_eq!(
        column(session.run("WITH c AS (SELECT 1 a) SELECT * FROM c WHERE a = 1")),
        vec!["1"]
    );
    assert_eq!(
        column(session.run("WITH c AS (SELECT 1 a), d AS (SELECT a + 1 b FROM c) SELECT * FROM d")),
        vec!["2"]
    );
    assert_eq!(
        column(session.run("WITH c AS (SELECT 1 a) SELECT * FROM c WHERE a IN (1, 2)")),
        vec!["1"]
    );
    for sql in [
        "SELECT * FROM (SELECT * FROM nosuch) d",
        "SELECT * FROM nosuch x JOIN nosuch y",
        "WITH c AS (SELECT * FROM nosuch) SELECT * FROM c",
        "SELECT * FROM nosuch",
    ] {
        assert_eq!(
            wire_error(&mut session, sql),
            (1046, "No database selected".to_owned()),
            "{sql}"
        );
    }
}

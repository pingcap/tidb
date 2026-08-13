//! Multi-table `UPDATE`/`DELETE` and `DELETE IGNORE`, checked against a real
//! TiDB session.
//!
//! Every expectation below was captured from `mockstore`-backed
//! `session.Execute`, reading the affected-row count off
//! `StmtCtx.AffectedRows()` and the error code off the returned
//! `*errors.Error`. `tidb_executor::driver::multi_dml`'s module doc states
//! the rules; this file is the evidence that they hold.

#![cfg(test)]

use crate::tests_support::row_text;
use crate::{Session, StmtResult};

/// Runs `sql` and returns its affected-row count.
fn affected(session: &mut Session, sql: &str) -> u64 {
    match session.run(sql).unwrap() {
        StmtResult::Affected(count) => count,
        other => panic!("expected an affected count from `{sql}`, got {other:?}"),
    }
}

/// One column of a query, as text, so a comparison does not depend on which
/// datum kind the codec hands back.
fn column(session: &mut Session, sql: &str) -> Vec<String> {
    row_text(session.run(sql))
        .into_iter()
        .map(|row| row.join("|"))
        .collect()
}

/// THE rule most likely to be got wrong: a target row reachable through
/// several join paths is updated ONCE.
///
/// Go: `UPDATE a JOIN b ON a.id = b.aid SET a.x = a.x + 1` over
/// `a(1,10),(2,20)` and `b(1,1,100),(2,1,200),(3,2,300)` -- the join matches
/// `a.id = 1` twice -- reports 2 and leaves `a` at `1|11`, `2|21`. A second
/// increment on the doubly-matched row would silently make it 12.
#[test]
fn multi_table_update_writes_a_doubly_matched_row_once() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE a (id INT PRIMARY KEY, x INT)")
        .unwrap();
    session
        .run("CREATE TABLE b (id INT PRIMARY KEY, aid INT, y INT)")
        .unwrap();
    session
        .run("INSERT INTO a VALUES (1, 10), (2, 20)")
        .unwrap();
    session
        .run("INSERT INTO b VALUES (1, 1, 100), (2, 1, 200), (3, 2, 300)")
        .unwrap();

    assert_eq!(
        affected(
            &mut session,
            "UPDATE a JOIN b ON a.id = b.aid SET a.x = a.x + 1"
        ),
        2
    );
    assert_eq!(
        column(&mut session, "SELECT id, x FROM a ORDER BY id"),
        ["1|11", "2|21"]
    );
}

/// The same rule for `DELETE`: a target row reachable twice is removed once
/// and counted once. Go reports 2 for a two-row `h1` matched by three `h2`
/// rows.
#[test]
fn multi_table_delete_removes_a_doubly_matched_row_once() {
    let mut session = Session::new();
    session.run("CREATE TABLE h1 (id INT PRIMARY KEY)").unwrap();
    session
        .run("CREATE TABLE h2 (id INT PRIMARY KEY, hid INT)")
        .unwrap();
    session.run("INSERT INTO h1 VALUES (1), (2)").unwrap();
    session
        .run("INSERT INTO h2 VALUES (1,1), (2,1), (3,2)")
        .unwrap();

    assert_eq!(
        affected(&mut session, "DELETE h1 FROM h1 JOIN h2 ON h1.id = h2.hid"),
        2
    );
    assert!(column(&mut session, "SELECT id FROM h1 ORDER BY id").is_empty());
}

/// Every `SET` right-hand side reads the joined row as the statement found
/// it, ACROSS tables -- so the two assignments swap rather than both landing
/// on one value. Go reports 2 and leaves `s1.x = 9`, `s2.y = 7`.
#[test]
fn multi_table_update_reads_original_values_across_tables() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE s1 (id INT PRIMARY KEY, x INT)")
        .unwrap();
    session
        .run("CREATE TABLE s2 (id INT PRIMARY KEY, y INT)")
        .unwrap();
    session.run("INSERT INTO s1 VALUES (1, 7)").unwrap();
    session.run("INSERT INTO s2 VALUES (1, 9)").unwrap();

    assert_eq!(
        affected(
            &mut session,
            "UPDATE s1, s2 SET s1.x = s2.y, s2.y = s1.x WHERE s1.id = s2.id"
        ),
        2
    );
    assert_eq!(column(&mut session, "SELECT x FROM s1"), ["9"]);
    assert_eq!(column(&mut session, "SELECT y FROM s2"), ["7"]);
}

/// The affected count is CHANGED rows summed over the target tables, not
/// matched rows: Go reports 0 for a join that matches nothing, and 1 when
/// one of two assignments is a no-op.
#[test]
fn multi_table_update_counts_changed_rows_only() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE n1 (id INT PRIMARY KEY, v INT)")
        .unwrap();
    session
        .run("CREATE TABLE n2 (id INT PRIMARY KEY, v INT)")
        .unwrap();
    session.run("INSERT INTO n1 VALUES (1, 1)").unwrap();
    session.run("INSERT INTO n2 VALUES (2, 2)").unwrap();
    assert_eq!(
        affected(
            &mut session,
            "UPDATE n1 JOIN n2 ON n1.id = n2.id SET n1.v = 99, n2.v = 99"
        ),
        0
    );

    session
        .run("CREATE TABLE q1 (id INT PRIMARY KEY, v INT)")
        .unwrap();
    session
        .run("CREATE TABLE q2 (id INT PRIMARY KEY, v INT)")
        .unwrap();
    session.run("INSERT INTO q1 VALUES (1,1),(2,2)").unwrap();
    session.run("INSERT INTO q2 VALUES (1,1),(2,2)").unwrap();
    assert_eq!(
        affected(
            &mut session,
            "UPDATE q1 JOIN q2 ON q1.id = q2.id SET q1.v = q1.v, q2.v = 5 WHERE q1.id = 1"
        ),
        1
    );
    assert_eq!(
        column(&mut session, "SELECT id, v FROM q1 ORDER BY id"),
        ["1|1", "2|2"]
    );
    assert_eq!(
        column(&mut session, "SELECT id, v FROM q2 ORDER BY id"),
        ["1|5", "2|2"]
    );
}

/// The update-once key is the target POSITION, so one physical table joined
/// under two aliases is two targets: Go reports 4 for a two-row table and
/// stores the LATER assignment's value.
#[test]
fn multi_table_update_treats_two_aliases_of_one_table_as_two_targets() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE z1 (id INT PRIMARY KEY, v INT)")
        .unwrap();
    session.run("INSERT INTO z1 VALUES (1, 1), (2, 2)").unwrap();

    assert_eq!(
        affected(
            &mut session,
            "UPDATE z1 AS p JOIN z1 AS q ON p.id = q.id SET p.v = p.v + 1, q.v = q.v + 10"
        ),
        4
    );
    assert_eq!(
        column(&mut session, "SELECT id, v FROM z1 ORDER BY id"),
        ["1|11", "2|12"]
    );
}

/// An outer join's NULL-padded side has no row to write, so it is skipped
/// (Go's `unmatchedOuterRow`): 1 for `y2` alone, then 3 once `y1` is a
/// target too.
#[test]
fn multi_table_update_skips_the_null_padded_side_of_an_outer_join() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE y1 (id INT PRIMARY KEY, v INT)")
        .unwrap();
    session
        .run("CREATE TABLE y2 (id INT PRIMARY KEY, yid INT, v INT)")
        .unwrap();
    session.run("INSERT INTO y1 VALUES (1,1),(2,2)").unwrap();
    session.run("INSERT INTO y2 VALUES (1,1,1)").unwrap();

    assert_eq!(
        affected(
            &mut session,
            "UPDATE y1 LEFT JOIN y2 ON y1.id = y2.yid SET y2.v = 9"
        ),
        1
    );
    assert_eq!(column(&mut session, "SELECT id, yid, v FROM y2"), ["1|1|9"]);
    assert_eq!(
        affected(
            &mut session,
            "UPDATE y1 LEFT JOIN y2 ON y1.id = y2.yid SET y1.v = 9, y2.v = 8"
        ),
        3
    );
    assert_eq!(
        column(&mut session, "SELECT id, v FROM y1 ORDER BY id"),
        ["1|9", "2|9"]
    );
    assert_eq!(column(&mut session, "SELECT id, yid, v FROM y2"), ["1|1|8"]);
}

/// An explicitly `JOIN`ed `UPDATE` accepts `ORDER BY`/`LIMIT`, and the
/// `LIMIT` caps the JOINED ROWS reached -- so an ordered `LIMIT 2` writes
/// the two largest keys, not the first two scanned.
#[test]
fn multi_table_update_limit_caps_the_joined_rows_reached() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE g1 (id INT PRIMARY KEY, v INT)")
        .unwrap();
    session
        .run("CREATE TABLE g2 (id INT PRIMARY KEY, v INT)")
        .unwrap();
    session
        .run("INSERT INTO g1 VALUES (1,1),(2,2),(3,3)")
        .unwrap();
    session
        .run("INSERT INTO g2 VALUES (1,1),(2,2),(3,3)")
        .unwrap();

    assert_eq!(
        affected(
            &mut session,
            "UPDATE g1 JOIN g2 ON g1.id = g2.id SET g1.v = g1.v + 10 LIMIT 1"
        ),
        1
    );
    assert_eq!(
        column(&mut session, "SELECT id, v FROM g1 ORDER BY id"),
        ["1|11", "2|2", "3|3"]
    );
    assert_eq!(
        affected(
            &mut session,
            "UPDATE g1 JOIN g2 ON g1.id = g2.id SET g1.v = g1.v + 100 ORDER BY g1.id DESC LIMIT 2"
        ),
        2
    );
    assert_eq!(
        column(&mut session, "SELECT id, v FROM g1 ORDER BY id"),
        ["1|11", "2|102", "3|103"]
    );
}

/// Only the tables in the DELETE list lose rows; the join's other tables are
/// a row source. `DELETE a FROM ...` reports 1, `DELETE a, b FROM ...` 2.
#[test]
fn multi_table_delete_removes_only_the_listed_targets() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE d1 (id INT PRIMARY KEY, v INT)")
        .unwrap();
    session
        .run("CREATE TABLE d2 (id INT PRIMARY KEY, v INT)")
        .unwrap();
    session
        .run("INSERT INTO d1 VALUES (1,1),(2,2),(3,3)")
        .unwrap();
    session.run("INSERT INTO d2 VALUES (1,1),(2,2)").unwrap();

    assert_eq!(
        affected(
            &mut session,
            "DELETE d1 FROM d1 JOIN d2 ON d1.id = d2.id WHERE d1.id = 1"
        ),
        1
    );
    assert_eq!(
        column(&mut session, "SELECT id FROM d1 ORDER BY id"),
        ["2", "3"]
    );
    assert_eq!(
        column(&mut session, "SELECT id FROM d2 ORDER BY id"),
        ["1", "2"]
    );

    assert_eq!(
        affected(
            &mut session,
            "DELETE d1, d2 FROM d1 JOIN d2 ON d1.id = d2.id WHERE d1.id = 2"
        ),
        2
    );
    assert_eq!(column(&mut session, "SELECT id FROM d1 ORDER BY id"), ["3"]);
    assert_eq!(column(&mut session, "SELECT id FROM d2 ORDER BY id"), ["1"]);
}

/// `DELETE FROM a USING ...` is the same statement as `DELETE a FROM ...`,
/// and a duplicated target still removes each row once (Go dedups by TABLE,
/// unlike UPDATE's per-position key).
#[test]
fn multi_table_delete_using_spelling_and_duplicate_targets() {
    let mut session = Session::new();
    session.run("CREATE TABLE e1 (id INT PRIMARY KEY)").unwrap();
    session.run("CREATE TABLE e2 (id INT PRIMARY KEY)").unwrap();
    session.run("INSERT INTO e1 VALUES (1),(2)").unwrap();
    session.run("INSERT INTO e2 VALUES (1),(2)").unwrap();

    assert_eq!(
        affected(
            &mut session,
            "DELETE FROM e1 USING e1 JOIN e2 ON e1.id = e2.id WHERE e1.id = 1"
        ),
        1
    );
    assert_eq!(column(&mut session, "SELECT id FROM e1 ORDER BY id"), ["2"]);
    assert_eq!(
        column(&mut session, "SELECT id FROM e2 ORDER BY id"),
        ["1", "2"]
    );

    assert_eq!(
        affected(
            &mut session,
            "DELETE e1, e1 FROM e1 JOIN e2 ON e1.id = e2.id WHERE e1.id = 2"
        ),
        1
    );
    assert!(column(&mut session, "SELECT id FROM e1 ORDER BY id").is_empty());
}

/// An alias REPLACES the target's name: `DELETE x FROM f1 AS x` deletes,
/// while `DELETE f1 FROM f1 AS x` -- and any target the `FROM` never
/// mentions -- is Go's `ERROR 1109 Unknown table '<t>' in MULTI DELETE`. A
/// schema-qualified target resolves against an unaliased source.
#[test]
fn multi_table_delete_target_is_named_by_its_alias() {
    let mut session = Session::new();
    session.run("CREATE TABLE f1 (id INT PRIMARY KEY)").unwrap();
    session.run("CREATE TABLE f2 (id INT PRIMARY KEY)").unwrap();
    session.run("INSERT INTO f1 VALUES (1),(2),(3)").unwrap();
    session.run("INSERT INTO f2 VALUES (1),(2),(3)").unwrap();

    assert_eq!(
        affected(
            &mut session,
            "DELETE x FROM f1 AS x JOIN f2 ON x.id = f2.id WHERE x.id = 1"
        ),
        1
    );
    assert_eq!(
        column(&mut session, "SELECT id FROM f1 ORDER BY id"),
        ["2", "3"]
    );

    for sql in [
        "DELETE f1 FROM f1 AS x JOIN f2 ON x.id = f2.id WHERE x.id = 2",
        "DELETE f3 FROM f1 JOIN f2 ON f1.id = f2.id",
        "DELETE f2 FROM f1 WHERE f1.id = 2",
    ] {
        let error = session.run(sql).unwrap_err();
        let wire = error.to_mysql_error();
        assert_eq!(wire.code, 1109, "`{sql}` should be ErrUnknownTable");
        assert!(
            wire.message.ends_with("in MULTI DELETE"),
            "`{sql}` gave {:?}",
            wire.message
        );
    }
    // The rejected statements changed nothing.
    assert_eq!(
        column(&mut session, "SELECT id FROM f1 ORDER BY id"),
        ["2", "3"]
    );

    assert_eq!(
        affected(
            &mut session,
            "DELETE test.f1 FROM f1 JOIN f2 ON f1.id = f2.id WHERE f1.id = 2"
        ),
        1
    );
    assert_eq!(column(&mut session, "SELECT id FROM f1 ORDER BY id"), ["3"]);
}

/// A `SET` column the join cannot bind -- including one qualified by a table
/// the `FROM` never mentions -- is Go's `ERROR 1054 Unknown column`, not an
/// unknown-table error. An unqualified `SET` column binds to the one table
/// that has it.
#[test]
fn multi_table_update_set_column_resolution() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE c1 (id INT PRIMARY KEY, v INT)")
        .unwrap();
    session.run("CREATE TABLE c2 (id INT PRIMARY KEY)").unwrap();
    session.run("INSERT INTO c1 VALUES (1,1),(2,2)").unwrap();
    session.run("INSERT INTO c2 VALUES (1),(2)").unwrap();

    for sql in [
        "UPDATE c1 JOIN c2 SET c3.v = 1",
        "UPDATE c1 JOIN c2 SET c2.v = 1",
    ] {
        let wire = session.run(sql).unwrap_err().to_mysql_error();
        assert_eq!(wire.code, 1054, "`{sql}` should be ErrUnknownColumn");
        assert_eq!(wire.message, "Unknown column 'v' in 'field list'");
    }

    assert_eq!(
        affected(
            &mut session,
            "UPDATE c1 JOIN c2 ON c1.id = c2.id SET v = 99"
        ),
        2
    );
    assert_eq!(
        column(&mut session, "SELECT id, v FROM c1 ORDER BY id"),
        ["1|99", "2|99"]
    );
}

/// A comma join is a cross join: every `c1` row is reached (through two
/// `c2` rows each) and still written once.
#[test]
fn multi_table_update_over_a_comma_cross_join() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE x1 (id INT PRIMARY KEY, v INT)")
        .unwrap();
    session.run("CREATE TABLE x2 (id INT PRIMARY KEY)").unwrap();
    session.run("INSERT INTO x1 VALUES (1,1),(2,2)").unwrap();
    session.run("INSERT INTO x2 VALUES (1),(2)").unwrap();

    assert_eq!(
        affected(&mut session, "UPDATE x1, x2 SET x1.v = x1.v * 2"),
        2
    );
    assert_eq!(
        column(&mut session, "SELECT id, v FROM x1 ORDER BY id"),
        ["1|2", "2|4"]
    );
}

/// `DELETE ... LEFT JOIN ... WHERE <right> IS NULL`, the anti-join idiom.
#[test]
fn multi_table_delete_over_a_left_join() {
    let mut session = Session::new();
    session.run("CREATE TABLE l1 (id INT PRIMARY KEY)").unwrap();
    session
        .run("CREATE TABLE l2 (id INT PRIMARY KEY, lid INT)")
        .unwrap();
    session.run("INSERT INTO l1 VALUES (1),(2),(3)").unwrap();
    session.run("INSERT INTO l2 VALUES (1,1)").unwrap();

    assert_eq!(
        affected(
            &mut session,
            "DELETE l1 FROM l1 LEFT JOIN l2 ON l1.id = l2.lid WHERE l2.lid IS NULL"
        ),
        2
    );
    assert_eq!(column(&mut session, "SELECT id FROM l1 ORDER BY id"), ["1"]);
}

/// The parser carries the `ORDER BY`/`LIMIT` rules, so they hold end to end:
/// a multi-table `DELETE` rejects both (Go errno 1064), and so does a
/// COMMA-joined `UPDATE` (Go errno 1221).
#[test]
fn multi_table_dml_rejects_order_by_and_limit_where_mysql_does() {
    let mut session = Session::new();
    session.run("CREATE TABLE o1 (id INT PRIMARY KEY)").unwrap();
    session.run("CREATE TABLE o2 (id INT PRIMARY KEY)").unwrap();
    session.run("INSERT INTO o1 VALUES (1),(2)").unwrap();
    session.run("INSERT INTO o2 VALUES (1),(2)").unwrap();

    for sql in [
        "DELETE o1 FROM o1 JOIN o2 ON o1.id = o2.id ORDER BY o1.id LIMIT 1",
        "DELETE o1 FROM o1 JOIN o2 ON o1.id = o2.id LIMIT 1",
        "DELETE o1 FROM o1 JOIN o2 ON o1.id = o2.id ORDER BY o1.id",
        "DELETE FROM o1 USING o1 JOIN o2 LIMIT 1",
        "UPDATE o1, o2 SET o1.id = o1.id LIMIT 1",
    ] {
        assert!(session.run(sql).is_err(), "`{sql}` should be rejected");
    }
    // The explicit-JOIN UPDATE spelling accepts both.
    session
        .run("UPDATE o1 JOIN o2 ON o1.id = o2.id SET o1.id = o1.id ORDER BY o1.id LIMIT 1")
        .unwrap();
    assert_eq!(
        column(&mut session, "SELECT id FROM o1 ORDER BY id"),
        ["1", "2"]
    );
}

/// `DELETE IGNORE` removes the same rows and reports the same count as a
/// plain `DELETE` when no per-row failure is available to downgrade -- which
/// is every row here, since this engine models no foreign keys. Its
/// single-table `ORDER BY`/`LIMIT` tail keeps working.
#[test]
fn delete_ignore_matches_plain_delete_without_a_failure_to_downgrade() {
    let mut session = Session::new();
    session.run("CREATE TABLE j1 (id INT PRIMARY KEY)").unwrap();
    session.run("INSERT INTO j1 VALUES (1),(2),(3)").unwrap();

    assert_eq!(
        affected(&mut session, "DELETE IGNORE FROM j1 WHERE id = 1"),
        1
    );
    assert_eq!(
        column(&mut session, "SELECT id FROM j1 ORDER BY id"),
        ["2", "3"]
    );
    assert_eq!(
        affected(&mut session, "DELETE IGNORE FROM j1 WHERE id = 99"),
        0
    );
    assert_eq!(
        affected(&mut session, "DELETE IGNORE FROM j1 ORDER BY id LIMIT 1"),
        1
    );
    assert_eq!(column(&mut session, "SELECT id FROM j1 ORDER BY id"), ["3"]);

    session.run("CREATE TABLE i1 (id INT PRIMARY KEY)").unwrap();
    session.run("CREATE TABLE i2 (id INT PRIMARY KEY)").unwrap();
    session.run("INSERT INTO i1 VALUES (1),(2)").unwrap();
    session.run("INSERT INTO i2 VALUES (1),(2)").unwrap();
    assert_eq!(
        affected(
            &mut session,
            "DELETE IGNORE i1 FROM i1 JOIN i2 ON i1.id = i2.id WHERE i1.id = 1"
        ),
        1
    );
    assert_eq!(column(&mut session, "SELECT id FROM i1 ORDER BY id"), ["2"]);
}

/// A view is a readable join source but never a write target: only a source
/// with an underlying base-row identity can be updated or deleted.
#[test]
fn multi_table_dml_keeps_view_sources_read_only() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE r1 (id INT PRIMARY KEY, v INT)")
        .unwrap();
    session.run("CREATE TABLE r2 (id INT PRIMARY KEY)").unwrap();
    session.run("INSERT INTO r1 VALUES (1,1)").unwrap();
    session.run("INSERT INTO r2 VALUES (1)").unwrap();
    session.run("CREATE VIEW rv AS SELECT * FROM r2").unwrap();
    assert_eq!(column(&mut session, "SELECT id FROM rv"), ["1"]);

    assert_eq!(
        affected(
            &mut session,
            "UPDATE IGNORE r1 JOIN r2 ON r1.id = r2.id SET r1.v = 2"
        ),
        1
    );
    assert_eq!(
        affected(
            &mut session,
            "UPDATE r1 JOIN rv ON r1.id = rv.id SET r1.v = 3"
        ),
        1
    );
    assert!(
        session
            .run("UPDATE rv JOIN r1 ON rv.id = r1.id SET rv.id = 2")
            .is_err(),
        "a view has no base-row identity to update"
    );
    assert_eq!(
        affected(&mut session, "DELETE r1 FROM r1 JOIN rv ON r1.id = rv.id"),
        1
    );

    assert!(column(&mut session, "SELECT id, v FROM r1").is_empty());
}

/// Go's `PlanBuilder.buildUsingClause` and `buildNaturalJoin` construct the
/// common-column equality, then restore the full child schema when the
/// statement is an `UPDATE` or `DELETE`. The common name is consequently
/// usable without a qualifier, while both base rows retain write identity.
#[test]
fn multi_table_dml_keeps_using_and_natural_join_rows_writable() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE uj1 (id INT PRIMARY KEY, left_v INT)")
        .unwrap();
    session
        .run("CREATE TABLE uj2 (id INT PRIMARY KEY, right_v INT)")
        .unwrap();
    session
        .run("INSERT INTO uj1 VALUES (1,10),(2,20),(3,30)")
        .unwrap();
    session.run("INSERT INTO uj2 VALUES (1,7),(2,8)").unwrap();

    assert_eq!(
        affected(
            &mut session,
            "UPDATE uj1 JOIN uj2 USING (id) SET uj1.left_v = uj1.left_v + uj2.right_v WHERE id = 1"
        ),
        1
    );
    assert_eq!(
        column(&mut session, "SELECT id, left_v FROM uj1 ORDER BY id"),
        ["1|17", "2|20", "3|30"]
    );

    assert_eq!(
        affected(
            &mut session,
            "DELETE uj1 FROM uj1 NATURAL JOIN uj2 WHERE id = 2"
        ),
        1
    );
    assert_eq!(
        column(&mut session, "SELECT id, left_v FROM uj1 ORDER BY id"),
        ["1|17", "3|30"]
    );

    // A RIGHT join keeps the right copy as the unqualified common column.
    // The unmatched row is still a valid target on that preserved side.
    session
        .run("CREATE TABLE ur1 (id INT PRIMARY KEY, left_v INT)")
        .unwrap();
    session
        .run("CREATE TABLE ur2 (id INT PRIMARY KEY, right_v INT)")
        .unwrap();
    session.run("INSERT INTO ur1 VALUES (1,10)").unwrap();
    session.run("INSERT INTO ur2 VALUES (1,7),(2,8)").unwrap();
    assert_eq!(
        affected(
            &mut session,
            "UPDATE ur1 RIGHT JOIN ur2 USING (id) SET ur2.right_v = ur2.right_v + 1 WHERE id = 2"
        ),
        1
    );
    assert_eq!(
        column(&mut session, "SELECT id, right_v FROM ur2 ORDER BY id"),
        ["1|7", "2|9"]
    );
}

/// Go turns a multi-table DML's `JOIN LATERAL` into an Apply: the derived
/// query is evaluated against each outer row, but only the outer base table
/// has a row identity that UPDATE or DELETE may target.
#[test]
fn multi_table_dml_rebinds_lateral_sources_for_every_outer_row() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE la (id INT PRIMARY KEY, v INT)")
        .unwrap();
    session.run("CREATE TABLE lb (id INT PRIMARY KEY)").unwrap();
    session.run("INSERT INTO la VALUES (1,10),(2,20)").unwrap();
    session.run("INSERT INTO lb VALUES (1)").unwrap();

    assert_eq!(
        affected(
            &mut session,
            "UPDATE la t1 JOIN LATERAL (SELECT id FROM lb WHERE id = t1.id) d ON 1=1 SET t1.v = t1.v + 1"
        ),
        1
    );
    assert_eq!(
        column(&mut session, "SELECT id, v FROM la ORDER BY id"),
        ["1|11", "2|20"]
    );
    assert_eq!(
        affected(
            &mut session,
            "DELETE t1 FROM la t1 JOIN LATERAL (SELECT id FROM lb WHERE id = t1.id) d ON 1=1"
        ),
        1
    );
    assert_eq!(column(&mut session, "SELECT id FROM la"), ["2"]);
}

/// A derived table is a READ source of a multi-table write, and the base
/// table joined against it IS written.
///
/// This is the statement `executor/union_scan` carries -- Go, `mockstore`:
/// `update t t1, (select a, b from t) t2 set t1.b = t2.b where t1.a = t2.a +
/// 10` over `(1,1),(2,2),(3,3),(11,11),(12,12),(13,13)` reports
/// `OK affected=3` and leaves `1|1 ; 2|2 ; 3|3 ; 11|1 ; 12|2 ; 13|3`.
///
/// The assertion is the ORDERED POST-UPDATE CONTENTS, not the affected-row
/// count: a count of 3 also comes back from a write that changed the WRONG
/// three rows, or that wrote the right rows with the wrong values, so a
/// count alone cannot tell a correct write from either. Rows `1`, `2` and
/// `3` are here precisely because nothing may write them -- `t2.a + 10` is
/// never `1`, `2` or `3` -- so an over-applying rule fails this test on the
/// first three entries of the vector.
#[test]
fn multi_table_update_reads_a_derived_table_and_writes_the_base_table() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE t (a INT PRIMARY KEY, b INT)")
        .unwrap();
    session
        .run("INSERT INTO t VALUES (1,1),(2,2),(3,3),(11,11),(12,12),(13,13)")
        .unwrap();

    assert_eq!(
        affected(
            &mut session,
            "UPDATE /*+ INL_JOIN(t1) */ t t1, (SELECT a, b FROM t) t2 \
             SET t1.b = t2.b WHERE t1.a = t2.a + 10"
        ),
        3
    );
    assert_eq!(
        column(&mut session, "SELECT a, b FROM t ORDER BY a"),
        ["1|1", "2|2", "3|3", "11|1", "12|2", "13|3"]
    );
}

/// The derived table is a SNAPSHOT the whole statement reads, so a write is
/// never visible to a later row of the same statement.
///
/// Go, `mockstore`, over the same six rows: `... WHERE t1.a = t2.a + 1`
/// reports `OK affected=4` and leaves `1|1 ; 2|1 ; 3|2 ; 11|11 ; 12|11 ;
/// 13|12`. Row `3` takes `2`, which is row `2`'s value BEFORE this statement
/// changed it to `1` -- a re-reading derived table would leave `3|1`, and the
/// affected count would be 4 either way.
#[test]
fn a_derived_table_source_is_read_once_before_any_row_is_written() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE t (a INT PRIMARY KEY, b INT)")
        .unwrap();
    session
        .run("INSERT INTO t VALUES (1,1),(2,2),(3,3),(11,11),(12,12),(13,13)")
        .unwrap();

    assert_eq!(
        affected(
            &mut session,
            "UPDATE t t1, (SELECT a, b FROM t) t2 SET t1.b = t2.b WHERE t1.a = t2.a + 1"
        ),
        4
    );
    assert_eq!(
        column(&mut session, "SELECT a, b FROM t ORDER BY a"),
        ["1|1", "2|1", "3|2", "11|11", "12|11", "13|12"]
    );
}

/// A derived table that matches NOTHING writes nothing, and one that matches
/// a base row TWICE writes it once.
///
/// Both halves are aimed at over-application, which an affected-row count
/// hides: Go reports `OK affected=0` for the empty join and leaves
/// `1|1 ; 2|2 ; 3|3`, and reports `OK affected=1` for the doubled join,
/// leaving `1|11 ; 2|20` -- not `1|12`.
#[test]
fn a_derived_table_source_neither_over_applies_nor_repeats() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE u (a INT PRIMARY KEY, b INT)")
        .unwrap();
    session
        .run("INSERT INTO u VALUES (1,1),(2,2),(3,3)")
        .unwrap();
    assert_eq!(
        affected(
            &mut session,
            "UPDATE u t1, (SELECT a, b FROM u WHERE a < 0) t2 SET t1.b = 99 WHERE t1.a = t2.a"
        ),
        0
    );
    // The same empty derived table with NO `WHERE` at all, so nothing but the
    // emptiness itself can stop the write. A source that stood an empty
    // subquery up as one NULL-padded row would write all three rows here and
    // still report 0 on the statement above, where `t1.a = NULL` filters it
    // back out. Go: `OK affected=0`, `1|1 ; 2|2 ; 3|3` for both, and for the
    // matching `DELETE t1 FROM u t1, (SELECT ...) t2` as well.
    assert_eq!(
        affected(
            &mut session,
            "UPDATE u t1, (SELECT a, b FROM u WHERE a < 0) t2 SET t1.b = 99"
        ),
        0
    );
    assert_eq!(
        affected(
            &mut session,
            "DELETE t1 FROM u t1, (SELECT a, b FROM u WHERE a < 0) t2"
        ),
        0
    );
    assert_eq!(
        column(&mut session, "SELECT a, b FROM u ORDER BY a"),
        ["1|1", "2|2", "3|3"]
    );
    // And the shape that DOES write through an empty derived table, so the
    // three assertions above cannot be passed by a source that dropped the
    // derived table's rows entirely: Go reports 3 and leaves `1|77 ; 2|77 ;
    // 3|77`.
    assert_eq!(
        affected(
            &mut session,
            "UPDATE u t1 LEFT JOIN (SELECT a, b FROM u WHERE a < 0) t2 ON t1.a = t2.a \
             SET t1.b = 77 WHERE t2.a IS NULL"
        ),
        3
    );
    assert_eq!(
        column(&mut session, "SELECT a, b FROM u ORDER BY a"),
        ["1|77", "2|77", "3|77"]
    );

    session
        .run("CREATE TABLE y (id INT PRIMARY KEY, v INT)")
        .unwrap();
    session.run("INSERT INTO y VALUES (1,10),(2,20)").unwrap();
    assert_eq!(
        affected(
            &mut session,
            "UPDATE y JOIN (SELECT 1 AS k UNION ALL SELECT 1) d ON y.id = d.k SET y.v = y.v + 1"
        ),
        1
    );
    assert_eq!(
        column(&mut session, "SELECT id, v FROM y ORDER BY id"),
        ["1|11", "2|20"]
    );

    session.run("CREATE TABLE z (id INT PRIMARY KEY)").unwrap();
    session.run("INSERT INTO z VALUES (1),(2)").unwrap();
    assert_eq!(
        affected(
            &mut session,
            "DELETE z FROM z JOIN (SELECT 1 AS k UNION ALL SELECT 1) d ON z.id = d.k"
        ),
        1
    );
    assert_eq!(column(&mut session, "SELECT id FROM z ORDER BY id"), ["2"]);
}

/// An outer join's NULL-padded side is still not written when one of the two
/// sides is a derived table, and the derived table's own NULL padding is
/// simply values.
///
/// Go, `mockstore`, over `u(1,1),(2,2),(3,3)`:
/// `UPDATE u t1 LEFT JOIN (SELECT a, b FROM u WHERE a > 100) t2 ON t1.a =
/// t2.a SET t1.b = IFNULL(t2.b, -1)` reports 3 and leaves `1|-1 ; 2|-1 ;
/// 3|-1` -- the derived side padded, the base side written.
/// `UPDATE (SELECT 1 AS a UNION ALL SELECT 9) d LEFT JOIN v t2 ON d.a = t2.a
/// SET t2.b = 55` reports 1 over `v(1,1),(2,2),(3,3)` and leaves
/// `1|55 ; 2|2 ; 3|3`: the `9` row padded `t2`, so nothing was written for
/// it, and `2`/`3` were never joined at all.
#[test]
fn an_outer_join_against_a_derived_table_writes_only_the_rows_that_matched() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE u (a INT PRIMARY KEY, b INT)")
        .unwrap();
    session
        .run("INSERT INTO u VALUES (1,1),(2,2),(3,3)")
        .unwrap();
    assert_eq!(
        affected(
            &mut session,
            "UPDATE u t1 LEFT JOIN (SELECT a, b FROM u WHERE a > 100) t2 ON t1.a = t2.a \
             SET t1.b = IFNULL(t2.b, -1)"
        ),
        3
    );
    assert_eq!(
        column(&mut session, "SELECT a, b FROM u ORDER BY a"),
        ["1|-1", "2|-1", "3|-1"]
    );

    session
        .run("CREATE TABLE v (a INT PRIMARY KEY, b INT)")
        .unwrap();
    session
        .run("INSERT INTO v VALUES (1,1),(2,2),(3,3)")
        .unwrap();
    assert_eq!(
        affected(
            &mut session,
            "UPDATE (SELECT 1 AS a UNION ALL SELECT 9) d LEFT JOIN v t2 ON d.a = t2.a \
             SET t2.b = 55"
        ),
        1
    );
    assert_eq!(
        column(&mut session, "SELECT a, b FROM v ORDER BY a"),
        ["1|55", "2|2", "3|3"]
    );
}

/// A derived table is not a WRITE target, and saying so is a different error
/// from naming a table the `FROM` never provides.
///
/// Go, `mockstore`, error codes read off the returned `*terror.Error`:
///
/// * `update t t1, (select a, b from t) t2 set t2.b = 5 ...` ->
///   `[planner:1288]The target table t2 of the UPDATE is not updatable`
/// * `update (select a, b from t) t1 set t1.b = 1` -> the same 1288, which is
///   `buildUpdateLists`'s own first documented case ("no updatable table
///   here").
/// * `delete t2 from t t1, (select a, b from t) t2 ...` ->
///   `[planner:1288]The target table t2 of the DELETE is not updatable`
/// * `delete zz from t t1, (select a, b from t) t2 ...` ->
///   `[planner:1109]Unknown table 'zz' in MULTI DELETE` -- a name the `FROM`
///   does not carry at all is still 1109, so the two refusals cannot be
///   collapsed into one.
/// * `update x t1, (select a, a from x) t2 ...` -> `[planner:1060]Duplicate
///   column name 'a'` and `update x t1, (select a from x) set ...` ->
///   `[ddl:1248]Every derived table must have its own alias`: the derived
///   table's own shape rules apply here exactly as in a `SELECT`.
#[test]
fn a_derived_table_is_not_a_write_target() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE t (a INT PRIMARY KEY, b INT)")
        .unwrap();
    session
        .run("INSERT INTO t VALUES (1,1),(2,2),(3,3)")
        .unwrap();

    for (sql, code, message) in [
        (
            "UPDATE t t1, (SELECT a, b FROM t) t2 SET t2.b = 5 WHERE t1.a = t2.a",
            1288,
            "The target table t2 of the UPDATE is not updatable",
        ),
        (
            "UPDATE (SELECT a, b FROM t) t1 SET t1.b = 1",
            1288,
            "The target table t1 of the UPDATE is not updatable",
        ),
        (
            "DELETE t2 FROM t t1, (SELECT a, b FROM t) t2 WHERE t1.a = t2.a",
            1288,
            "The target table t2 of the DELETE is not updatable",
        ),
        (
            "DELETE zz FROM t t1, (SELECT a, b FROM t) t2 WHERE t1.a = t2.a",
            1109,
            "Unknown table 'zz' in MULTI DELETE",
        ),
        (
            "UPDATE t t1, (SELECT a, a FROM t) t2 SET t1.b = 1",
            1060,
            "Duplicate column name 'a'",
        ),
        (
            "UPDATE t t1, (SELECT a FROM t) SET t1.b = 1",
            1248,
            "Every derived table must have its own alias",
        ),
    ] {
        let error = session.run(sql).unwrap_err().to_mysql_error();
        assert_eq!(
            (error.code, error.message.as_str()),
            (code, message),
            "{sql}"
        );
    }
    // Not one of the six refusals wrote anything.
    assert_eq!(
        column(&mut session, "SELECT a, b FROM t ORDER BY a"),
        ["1|1", "2|2", "3|3"]
    );

    // A derived table standing FIRST in the `FROM` still lets the base table
    // beside it be written: Go reports 3 and leaves `1|7 ; 2|7 ; 3|7`.
    assert_eq!(
        affected(
            &mut session,
            "UPDATE (SELECT a, b FROM t) t1, t t2 SET t2.b = 7 WHERE t1.a = t2.a"
        ),
        3
    );
    assert_eq!(
        column(&mut session, "SELECT a, b FROM t ORDER BY a"),
        ["1|7", "2|7", "3|7"]
    );
}

/// A derived table's own expressions are computed once and joined as values:
/// an aggregate one is a single row that every base row matches, and the base
/// row whose value ALREADY equals it is not counted.
///
/// Go, `mockstore`, over `x(1,1),(2,2),(3,3)`:
/// `UPDATE x t1, (SELECT MAX(a) AS m FROM x) t2 SET t1.b = t2.m` reports
/// `OK affected=2` -- not 3 -- and leaves `1|3 ; 2|3 ; 3|3`. Row `3` was
/// already `3`, so it is written but not COUNTED, which is the multi-table
/// path's "affected rows are CHANGED rows" rule reaching a derived source.
#[test]
fn an_aggregate_derived_table_joins_as_one_row_and_counts_only_changes() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE x (a INT PRIMARY KEY, b INT)")
        .unwrap();
    session
        .run("INSERT INTO x VALUES (1,1),(2,2),(3,3)")
        .unwrap();

    assert_eq!(
        affected(
            &mut session,
            "UPDATE x t1, (SELECT MAX(a) AS m FROM x) t2 SET t1.b = t2.m"
        ),
        2
    );
    assert_eq!(
        column(&mut session, "SELECT a, b FROM x ORDER BY a"),
        ["1|3", "2|3", "3|3"]
    );
}

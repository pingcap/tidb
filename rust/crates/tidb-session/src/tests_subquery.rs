#![cfg(test)]

use crate::tests_support::*;
use crate::*;

/// A CORRELATED scalar subquery in the SELECT list: an Apply above the
/// filter, one inner run per outer row.
///
/// Every assertion is a capture of real TiDB on the same schema
/// (`testkit.CreateMockStore`, `pkg/executor`).
#[test]
fn correlated_subquery_in_select_list() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE t1 (id INT, name VARCHAR(20))")
        .unwrap();
    session
        .run("CREATE TABLE t2 (id INT, t1_id INT, v INT)")
        .unwrap();
    session
        .run("INSERT INTO t1 VALUES (1,'a'),(2,'b'),(3,'c')")
        .unwrap();
    session
        .run("INSERT INTO t2 VALUES (10,1,100),(11,1,200),(12,2,300)")
        .unwrap();

    // COUNT answers 0 for the outer row with no match -- the inner
    // aggregate over an empty group, NOT the "no rows" NULL.
    assert_eq!(
        row_text(session.run(
            "SELECT id, (SELECT COUNT(*) FROM t2 WHERE t2.t1_id = t1.id) FROM t1 ORDER BY id"
        )),
        vec![
            vec!["1".to_owned(), "2".to_owned()],
            vec!["2".to_owned(), "1".to_owned()],
            vec!["3".to_owned(), "0".to_owned()],
        ]
    );

    // MAX over an empty group is NULL, so the unmatched outer row is NULL.
    assert_eq!(
        row_text(
            session.run(
                "SELECT id, (SELECT MAX(v) FROM t2 WHERE t2.t1_id = t1.id) FROM t1 ORDER BY id"
            )
        ),
        vec![
            vec!["1".to_owned(), "200".to_owned()],
            vec!["2".to_owned(), "300".to_owned()],
            vec!["3".to_owned(), "NULL".to_owned()],
        ]
    );

    // ORDER BY the subquery's alias sorts on the Apply's column, and NULL
    // sorts first ascending.
    assert_eq!(
        row_text(session.run(
            "SELECT id, (SELECT SUM(v) FROM t2 WHERE t2.t1_id = t1.id) AS s FROM t1 ORDER BY s"
        )),
        vec![
            vec!["3".to_owned(), "NULL".to_owned()],
            vec!["1".to_owned(), "300".to_owned()],
            vec!["2".to_owned(), "300".to_owned()],
        ]
    );

    // Go's max-one-row check: 1242, raised per outer row.
    assert!(matches!(
        session.run("SELECT id, (SELECT v FROM t2 WHERE t2.t1_id = t1.id) FROM t1 ORDER BY id"),
        Err(DriverError::SubqueryReturnsMoreThanOneRow)
    ));

    // An UNcorrelated subquery beside a correlated one still folds to a
    // constant, so both fields answer in the same row.
    assert_eq!(
        row_text(session.run(
            "SELECT id, (SELECT COUNT(*) FROM t2) AS u, \
                 (SELECT COUNT(*) FROM t2 WHERE t2.t1_id = t1.id) AS c FROM t1 ORDER BY id"
        )),
        vec![
            vec!["1".to_owned(), "3".to_owned(), "2".to_owned()],
            vec!["2".to_owned(), "3".to_owned(), "1".to_owned()],
            vec!["3".to_owned(), "3".to_owned(), "0".to_owned()],
        ]
    );

    // Inside an expression: the Apply column is an ordinary operand.
    assert_eq!(
        row_text(session.run(
            "SELECT id, (SELECT COUNT(*) FROM t2 WHERE t2.t1_id = t1.id) + 1 FROM t1 ORDER BY id"
        )),
        vec![
            vec!["1".to_owned(), "3".to_owned()],
            vec!["2".to_owned(), "2".to_owned()],
            vec!["3".to_owned(), "1".to_owned()],
        ]
    );
    // NULL + 1 is NULL, so the unmatched row stays NULL through the
    // arithmetic.
    assert_eq!(
        row_text(session.run(
            "SELECT id, (SELECT MAX(v) FROM t2 WHERE t2.t1_id = t1.id) + 1 FROM t1 ORDER BY id"
        )),
        vec![
            vec!["1".to_owned(), "201".to_owned()],
            vec!["2".to_owned(), "301".to_owned()],
            vec!["3".to_owned(), "NULL".to_owned()],
        ]
    );

    // The outer column the inner query reads need not be the projected
    // one, and ORDER BY over it is unaffected.
    assert_eq!(
        row_text(session.run(
            "SELECT name, (SELECT COUNT(*) FROM t2 WHERE t2.t1_id = t1.id) FROM t1 \
                 ORDER BY name"
        )),
        vec![
            vec!["a".to_owned(), "2".to_owned()],
            vec!["b".to_owned(), "1".to_owned()],
            vec!["c".to_owned(), "0".to_owned()],
        ]
    );
}

/// The CORRELATED semi-join shapes: `[NOT] IN` and `<op> ANY|ALL` over a
/// subquery that reads the outer row.
///
/// Every assertion is a capture of real TiDB on this schema
/// (`testkit.CreateMockStore`, `pkg/executor`). The rows are chosen for
/// the three traps: an inner set holding NULL (id 2), an EMPTY inner set
/// (id 4), and a NULL left operand (id 4 again).
#[test]
fn correlated_semi_join_subqueries() {
    let mut session = semi_join_session();

    // IN: matched is 1; an unmatched left operand against a set holding
    // NULL is NULL, not 0; an EMPTY set is 0 even for a NULL operand.
    assert_eq!(
        row_text(
            session.run(
                "SELECT id, v IN (SELECT w FROM t2 WHERE t2.t1_id = t1.id) FROM t1 ORDER BY id"
            )
        ),
        vec![
            vec!["1".to_owned(), "1".to_owned()],
            vec!["2".to_owned(), "NULL".to_owned()],
            vec!["3".to_owned(), "1".to_owned()],
            vec!["4".to_owned(), "0".to_owned()],
        ]
    );

    // NOT IN is the negation, NULL included: the row whose inner set holds
    // a NULL stays NULL and is therefore filtered out by a WHERE.
    assert_eq!(
        row_text(session.run(
            "SELECT id, v NOT IN (SELECT w FROM t2 WHERE t2.t1_id = t1.id) FROM t1 ORDER BY id"
        )),
        vec![
            vec!["1".to_owned(), "0".to_owned()],
            vec!["2".to_owned(), "NULL".to_owned()],
            vec!["3".to_owned(), "0".to_owned()],
            vec!["4".to_owned(), "1".to_owned()],
        ]
    );
    assert_eq!(
        row_text(session.run(
            "SELECT id FROM t1 WHERE v IN (SELECT w FROM t2 WHERE t2.t1_id = t1.id) \
                 ORDER BY id"
        )),
        vec![vec!["1".to_owned()], vec!["3".to_owned()]]
    );
    // The NULL trap in a WHERE: only the EMPTY-set row survives NOT IN --
    // the id-2 row is NULL (its set holds a NULL), and NULL is not true.
    assert_eq!(
        row_text(session.run(
            "SELECT id FROM t1 WHERE v NOT IN (SELECT w FROM t2 WHERE t2.t1_id = t1.id) \
                 ORDER BY id"
        )),
        vec![vec!["4".to_owned()]]
    );

    // `> ANY` is the OR chain: false OR NULL is NULL (id 2), and an empty
    // set is FALSE (id 4, whose left operand is NULL too).
    assert_eq!(
        row_text(session.run(
            "SELECT id, v > ANY (SELECT w FROM t2 WHERE t2.t1_id = t1.id) FROM t1 ORDER BY id"
        )),
        vec![
            vec!["1".to_owned(), "1".to_owned()],
            vec!["2".to_owned(), "NULL".to_owned()],
            vec!["3".to_owned(), "0".to_owned()],
            vec!["4".to_owned(), "0".to_owned()],
        ]
    );
    // `> ALL` is the AND chain: false AND NULL is FALSE, so id 2 answers 0
    // rather than NULL -- and the EMPTY set is vacuously TRUE (id 4).
    assert_eq!(
        row_text(session.run(
            "SELECT id, v > ALL (SELECT w FROM t2 WHERE t2.t1_id = t1.id) FROM t1 ORDER BY id"
        )),
        vec![
            vec!["1".to_owned(), "0".to_owned()],
            vec!["2".to_owned(), "0".to_owned()],
            vec!["3".to_owned(), "0".to_owned()],
            vec!["4".to_owned(), "1".to_owned()],
        ]
    );
    // `< ALL` keeps the NULL, because every comparison is true or NULL.
    assert_eq!(
        row_text(session.run(
            "SELECT id, v < ALL (SELECT w FROM t2 WHERE t2.t1_id = t1.id) FROM t1 ORDER BY id"
        )),
        vec![
            vec!["1".to_owned(), "0".to_owned()],
            vec!["2".to_owned(), "NULL".to_owned()],
            vec!["3".to_owned(), "0".to_owned()],
            vec!["4".to_owned(), "1".to_owned()],
        ]
    );
    // `= ANY` answers exactly as IN does, empty set included.
    assert_eq!(
        row_text(session.run(
            "SELECT id, v = ANY (SELECT w FROM t2 WHERE t2.t1_id = t1.id) FROM t1 ORDER BY id"
        )),
        vec![
            vec!["1".to_owned(), "1".to_owned()],
            vec!["2".to_owned(), "NULL".to_owned()],
            vec!["3".to_owned(), "1".to_owned()],
            vec!["4".to_owned(), "0".to_owned()],
        ]
    );
}

/// A CORRELATED scalar subquery in a GROUPED select list: the Apply sits
/// ABOVE the aggregation, so the subquery is bound to the GROUP's value
/// and runs once per output row.
///
/// Captured from real TiDB on the same schema.
#[test]
fn correlated_subquery_in_aggregate_select() {
    let mut session = semi_join_session();

    assert_eq!(
        row_text(session.run(
            "SELECT id, (SELECT MAX(w) FROM t2 WHERE t2.t1_id = id) FROM t1 \
                 GROUP BY id ORDER BY id"
        )),
        vec![
            vec!["1".to_owned(), "10".to_owned()],
            vec!["2".to_owned(), "25".to_owned()],
            vec!["3".to_owned(), "30".to_owned()],
            vec!["4".to_owned(), "NULL".to_owned()],
        ]
    );

    // Beside an ordinary aggregate, and in any field position.
    assert_eq!(
        row_text(session.run(
            "SELECT id, COUNT(*), (SELECT MAX(w) FROM t2 WHERE t2.t1_id = id) FROM t1 \
                 GROUP BY id ORDER BY id"
        )),
        vec![
            vec!["1".to_owned(), "1".to_owned(), "10".to_owned()],
            vec!["2".to_owned(), "1".to_owned(), "25".to_owned()],
            vec!["3".to_owned(), "1".to_owned(), "30".to_owned()],
            vec!["4".to_owned(), "1".to_owned(), "NULL".to_owned()],
        ]
    );

    // The NULL group binds a NULL into the inner comparison, which matches
    // nothing -- COUNT answers 0 rather than NULL.
    assert_eq!(
        row_text(session.run(
            "SELECT v, (SELECT COUNT(*) FROM t2 WHERE t2.w = v) FROM t1 \
                 GROUP BY v ORDER BY v"
        )),
        vec![
            vec!["NULL".to_owned(), "0".to_owned()],
            vec!["10".to_owned(), "1".to_owned()],
            vec!["20".to_owned(), "0".to_owned()],
            vec!["30".to_owned(), "1".to_owned()],
        ]
    );

    // The grouped column the subquery reads need not be projected: it
    // rides a hidden carrier out of the aggregation and is trimmed again.
    assert_eq!(
        row_text(session.run(
            "SELECT (SELECT MAX(w) FROM t2 WHERE t2.t1_id = id) FROM t1 \
                 GROUP BY id ORDER BY id"
        )),
        vec![
            vec!["10".to_owned()],
            vec!["25".to_owned()],
            vec!["30".to_owned()],
            vec!["NULL".to_owned()],
        ]
    );
}

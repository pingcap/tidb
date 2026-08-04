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

/// An UNCORRELATED scalar subquery is a constant, and folds wherever it
/// appears -- including places the fold pass used to walk past: inside a
/// `CASE`, inside a function call, and inside an AGGREGATE's own argument.
///
/// The bug this pins was not where its symptom pointed: the aggregate cases
/// reported `driver::agg_build`'s "subquery inside an aggregate function's
/// argument" refusal and the `CASE` case reported the expression rewriter's
/// generic refusal, but both came from `select_has_uncorrelated_subquery` --
/// the GATE deciding whether the fold pass runs at all -- not recognising a
/// subquery in those positions, so nothing was ever folded.
///
/// Captured from Go (`rust/difftests/gorun`), over
/// `d1(id,name) = (1,eng),(2,sales),(9,ops)` and `t1(id,v) = (1,10),(2,20)`:
///
/// ```text
/// select sum((select max(id) from d1)) from t1                  RS:18
/// select count((select max(id) from d1)) from t1                RS:2
/// select max((select 3)) from t1                                RS:3
/// select sum(v + (select count(*) from d1)) from t1             RS:36
/// select id, case when (select count(*) from d1) > 1
///        then 'multi' else 'single' end from t1 where id = 1    RS:1|multi
/// select case (select 2) when 2 then 'two' else 'other' end     RS:two
/// select concat('n=', (select count(*) from d1))                RS:n=3
/// ```
#[test]
fn an_uncorrelated_subquery_folds_inside_case_function_and_aggregate_arguments() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE d1 (id INT PRIMARY KEY, name VARCHAR(8))")
        .unwrap();
    session
        .run("INSERT INTO d1 VALUES (1,'eng'),(2,'sales'),(9,'ops')")
        .unwrap();
    session
        .run("CREATE TABLE t1 (id INT PRIMARY KEY, v INT)")
        .unwrap();
    session.run("INSERT INTO t1 VALUES (1,10),(2,20)").unwrap();

    // An aggregate's own argument: the subquery is the same constant for
    // every source row, so SUM adds it once per row and COUNT counts the rows.
    assert_eq!(
        row_text(session.run("SELECT SUM((SELECT MAX(id) FROM d1)) FROM t1")),
        [vec!["18".to_owned()]]
    );
    assert_eq!(
        row_text(session.run("SELECT COUNT((SELECT MAX(id) FROM d1)) FROM t1")),
        [vec!["2".to_owned()]]
    );
    assert_eq!(
        row_text(session.run("SELECT MAX((SELECT 3)) FROM t1")),
        [vec!["3".to_owned()]]
    );
    // ... and nested deeper inside that argument.
    assert_eq!(
        row_text(session.run("SELECT SUM(v + (SELECT COUNT(*) FROM d1)) FROM t1")),
        [vec!["36".to_owned()]]
    );

    // A CASE condition, and the simple CASE's compare value.
    assert_eq!(
        row_text(session.run(
            "SELECT id, CASE WHEN (SELECT COUNT(*) FROM d1) > 1 THEN 'multi' \
                 ELSE 'single' END FROM t1 WHERE id = 1"
        )),
        [vec!["1".to_owned(), "multi".to_owned()]]
    );
    assert_eq!(
        row_text(session.run("SELECT CASE (SELECT 2) WHEN 2 THEN 'two' ELSE 'other' END")),
        [vec!["two".to_owned()]]
    );

    // An ordinary function argument.
    assert_eq!(
        row_text(session.run("SELECT CONCAT('n=', (SELECT COUNT(*) FROM d1))")),
        [vec!["n=3".to_owned()]]
    );
}

/// The neighbouring CORRELATED case, which used to be refused by name and now
/// RUNS: a subquery in an aggregate's argument that reads the outer row runs
/// once per SOURCE row, below the aggregation (`hoist_pre_agg_subqueries` +
/// `build_pre_agg_applies`). Go answers
/// `select count((select id from d2 where d2.id = d1.id)) from d1` with 3, and
/// so does this tier now.
///
/// The shapes still refused are the ones whose subquery sits somewhere the
/// extraction cannot reach -- inside a `CASE` arm, or a function call's
/// argument -- which stay named rather than answered wrongly.
#[test]
fn a_correlated_subquery_in_an_aggregate_argument_runs_below_the_aggregation() {
    let mut session = Session::new();
    session.run("CREATE TABLE dc (id INT PRIMARY KEY)").unwrap();
    session.run("INSERT INTO dc VALUES (1),(2),(9)").unwrap();
    assert_eq!(
        row_text(session.run("SELECT COUNT((SELECT id FROM dc d2 WHERE d2.id = dc.id)) FROM dc")),
        [["3"]]
    );
    // Still refused, by name: a `CASE` arm is not a shape
    // `extract_correlated_subquery` walks into, so the aggregate's argument
    // keeps its subquery and `build_agg_func` says so.
    let error = session
        .run(
            "SELECT SUM(CASE WHEN EXISTS(SELECT 1 FROM dc d2 WHERE d2.id = dc.id) \
             THEN dc.id ELSE 0 END) FROM dc",
        )
        .unwrap_err();
    assert!(
        matches!(
            &error,
            DriverError::Unsupported(message)
                if message.contains("subquery inside an aggregate function's argument")
        ),
        "unexpected error: {error:?}"
    );
}

/// Go's answers for the shape, captured via `rust/difftests/gorun` on
/// `corpus/table/foundations`' own `dept`/`emp`:
///
/// | statement | Go |
/// | --- | --- |
/// | `SELECT dept.name, SUM((SELECT COUNT(*) FROM emp WHERE emp.dept_id = dept.id)) FROM dept GROUP BY dept.name` | `eng|2`, `ops|0`, `sales|1` |
/// | `SELECT COUNT((SELECT id FROM dept d2 WHERE d2.id = dept.id)) FROM dept` | `3` |
/// | `SELECT SUM((SELECT COUNT(*) FROM emp WHERE emp.dept_id = dept.id)) FROM dept` | `3` |
/// | `SELECT MAX((SELECT COUNT(*) FROM emp WHERE emp.dept_id = dept.id)) FROM dept` | `2` |
/// | `SELECT COUNT((SELECT id FROM emp WHERE emp.dept_id = dept.id AND emp.id = 10)) FROM dept` | `1` |
///
/// The last row is the discriminator, and the reason a shortcut cannot fake
/// this: the inner query returns NULL for two of the three `dept` rows, and
/// `COUNT` skips exactly those, so the answer only comes out right if the
/// subquery really ran ONCE PER SOURCE ROW and its NULLs reached the
/// accumulator. Any scheme that evaluates the subquery once, or above the
/// grouping, gets a different number.
///
/// This is what `driver::agg_select`'s stage 1a
/// (`hoist_pre_agg_subqueries`) and stage 5b (`build_pre_agg_applies`) were
/// built for: the Apply chain the pipeline already had (stage 6) sits ABOVE
/// the aggregation and binds from the grouped output row, which cannot answer
/// any of these. The new stages take the subquery out of the aggregate's
/// argument, run it per SOURCE row below the aggregation, and leave the
/// argument reading the appended column.
///
/// `EXPLAIN` in Go prints exactly that shape --
/// `HashAgg <- Projection <- Apply(dept, MaxOneRow(inner))` -- which is also
/// where its `1242` comes from: the `MaxOneRow` is per outer row, so a
/// subquery returning two rows for SOME source row errors at ROW time while
/// `EXPLAIN` of the same statement succeeds.
#[test]
fn a_correlated_subquery_in_an_aggregate_argument_matches_go() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE dept (id INT, name VARCHAR(9))")
        .unwrap();
    session
        .run("INSERT INTO dept VALUES (1,'eng'),(2,'sales'),(3,'ops')")
        .unwrap();
    session
        .run("CREATE TABLE emp (id INT, dept_id INT, name VARCHAR(9))")
        .unwrap();
    session
        .run("INSERT INTO emp VALUES (10,1,'ann'),(11,1,'bob'),(12,2,'cid'),(13,9,'dan')")
        .unwrap();

    assert_eq!(
        row_text(session.run(
            "SELECT dept.name, SUM((SELECT COUNT(*) FROM emp WHERE emp.dept_id = dept.id)) \
             FROM dept GROUP BY dept.name ORDER BY dept.name"
        )),
        [["eng", "2"], ["ops", "0"], ["sales", "1"]]
    );
    assert_eq!(
        row_text(
            session.run("SELECT COUNT((SELECT id FROM dept d2 WHERE d2.id = dept.id)) FROM dept")
        ),
        [["3"]]
    );
    assert_eq!(
        row_text(
            session.run(
                "SELECT SUM((SELECT COUNT(*) FROM emp WHERE emp.dept_id = dept.id)) FROM dept"
            )
        ),
        [["3"]]
    );
    assert_eq!(
        row_text(
            session.run(
                "SELECT MAX((SELECT COUNT(*) FROM emp WHERE emp.dept_id = dept.id)) FROM dept"
            )
        ),
        [["2"]]
    );
    // The discriminator: two of three inner runs return NULL, and COUNT skips
    // exactly those.
    assert_eq!(
        row_text(session.run(
            "SELECT COUNT((SELECT id FROM emp WHERE emp.dept_id = dept.id AND emp.id = 10)) \
             FROM dept"
        )),
        [["1"]]
    );

    // Captured from Go alongside the rows above.
    //
    // | statement | Go |
    // | --- | --- |
    // | `AVG((SELECT COUNT(*) ...))` | `1.0000` |
    // | `MIN((SELECT COUNT(*) ...))` | `0` |
    // | `SUM(sub + dept.id)` | `9` |
    // | `COUNT(sub + dept.id)` (sub NULL twice) | `1` |
    // | `COUNT(DISTINCT sub)` | `3` |
    // | `SUM((SELECT COUNT(*) ...) + (SELECT COUNT(*) ...))` | `3` |
    // | `SUM((SELECT id ... LIMIT 1))` | `22` |
    // | `... WHERE dept.id = 3` (no inner row) | `NULL` |
    // | `... WHERE dept.id > 100` (no SOURCE row) | `0` for COUNT |
    assert_eq!(
        row_text(
            session.run(
                "SELECT AVG((SELECT COUNT(*) FROM emp WHERE emp.dept_id = dept.id)) FROM dept"
            )
        ),
        [["1.0000"]]
    );
    assert_eq!(
        row_text(
            session.run(
                "SELECT MIN((SELECT COUNT(*) FROM emp WHERE emp.dept_id = dept.id)) FROM dept"
            )
        ),
        [["0"]]
    );
    // A correlated subquery AND a plain source column in the same aggregate
    // call: both read the same source row, so the appended column simply joins
    // the argument expression.
    assert_eq!(
        row_text(session.run(
            "SELECT SUM((SELECT COUNT(*) FROM emp WHERE emp.dept_id = dept.id) + dept.id) \
             FROM dept"
        )),
        [["9"]]
    );
    assert_eq!(
        row_text(session.run(
            "SELECT COUNT((SELECT id FROM emp WHERE emp.dept_id = dept.id AND emp.id = 10) \
             + dept.id) FROM dept"
        )),
        [["1"]]
    );
    assert_eq!(
        row_text(session.run(
            "SELECT COUNT(DISTINCT (SELECT COUNT(*) FROM emp WHERE emp.dept_id = dept.id)) \
             FROM dept"
        )),
        [["3"]]
    );
    // TWO correlated subqueries in ONE argument (Go: `3`) is still refused,
    // and by the extraction's own pre-existing rule rather than anything this
    // stage added: `extract_correlated_subquery` rejects a second correlated
    // subquery in the same expression outright, so it cannot be walked one at
    // a time the way the stage's hoist loop would otherwise take them. Named
    // here so the next unit inherits the exact obstacle.
    let error = session
        .run(
            "SELECT SUM((SELECT COUNT(*) FROM emp WHERE emp.dept_id = dept.id) \
             + (SELECT COUNT(*) FROM emp WHERE emp.id = dept.id)) FROM dept",
        )
        .unwrap_err();
    assert!(
        matches!(
            &error,
            DriverError::Unsupported(message)
                if message.contains("more than one correlated subquery")
        ),
        "unexpected error: {error:?}"
    );
    assert_eq!(
        row_text(
            session.run(
                "SELECT SUM((SELECT id FROM emp WHERE emp.dept_id = dept.id LIMIT 1)) FROM dept"
            )
        ),
        [["22"]]
    );
    assert_eq!(
        row_text(session.run(
            "SELECT SUM((SELECT id FROM emp WHERE emp.dept_id = dept.id)) FROM dept \
             WHERE dept.id = 3"
        )),
        [["NULL"]]
    );
    assert_eq!(
        row_text(session.run(
            "SELECT COUNT((SELECT id FROM emp WHERE emp.dept_id = dept.id)) FROM dept \
             WHERE dept.id > 100"
        )),
        [["0"]]
    );

    // `1242` is a ROW-time error, not a plan-time one: the same statement
    // succeeds when no source row's subquery returns two rows, and Go's
    // `EXPLAIN` of the failing statement plans fine (its `MaxOneRow` sits
    // inside the Apply's probe side).
    assert_eq!(
        row_text(session.run(
            "SELECT SUM((SELECT id FROM emp WHERE emp.dept_id = dept.id)) FROM dept \
             WHERE dept.id = 2"
        )),
        [["12"]]
    );
    let error = session
        .run("SELECT SUM((SELECT id FROM emp WHERE emp.dept_id = dept.id)) FROM dept")
        .unwrap_err();
    assert!(
        matches!(&error, DriverError::SubqueryReturnsMoreThanOneRow),
        "unexpected error: {error:?}"
    );

    // GROUP BY runs the Apply per SOURCE row, not per group. A second `eng`
    // department (`id = 9`, whose one employee is `dan`) takes the grouped SUM
    // from `eng|2` to `eng|3`: both source rows ran the subquery and the
    // aggregate summed both results. A per-GROUP Apply would have run once for
    // `eng` and could not produce 3.
    session.run("INSERT INTO dept VALUES (9,'eng')").unwrap();
    assert_eq!(
        row_text(session.run(
            "SELECT dept.name, SUM((SELECT COUNT(*) FROM emp WHERE emp.dept_id = dept.id)) \
             FROM dept GROUP BY dept.name ORDER BY dept.name"
        )),
        [["eng", "3"], ["ops", "0"], ["sales", "1"]]
    );
    // ... and the row-time `1242` fires from inside a group, too: `dept.id = 1`
    // has two employees.
    let error = session
        .run(
            "SELECT dept.name, COUNT((SELECT id FROM emp WHERE emp.dept_id = dept.id)) \
             FROM dept GROUP BY dept.name",
        )
        .unwrap_err();
    assert!(
        matches!(&error, DriverError::SubqueryReturnsMoreThanOneRow),
        "unexpected error: {error:?}"
    );
}

/// The aggregation's DEFAULT row is deselected by the Apply above it, so a
/// correlated SCALAR subquery beside an aggregate over an EMPTY outer answers
/// NULL -- not what its inner would compute from that all-NULL row.
///
/// Go `NestedLoopApplyExec.fetchSelectedOuterRow` names this case in its own
/// comment: the outer `count(1)` produces one row `<0, null>` over the empty
/// input, and that row is "specially mark[ed] ... as not selected, to trigger
/// the mismatch join procedure". Every assertion below is a capture of real
/// TiDB on `t1(a)`/`t2(a)` holding `1,2` and `1,2,3`.
#[test]
fn a_scalar_subquery_beside_an_aggregate_over_an_empty_outer_is_null() {
    let mut session = Session::new();
    session.run("CREATE TABLE e1 (a INT)").unwrap();
    session.run("CREATE TABLE e2 (a INT)").unwrap();
    session.run("INSERT INTO e1 VALUES (1),(2)").unwrap();
    session.run("INSERT INTO e2 VALUES (1),(2),(3)").unwrap();

    // THE case: `0, NULL`, where running the inner over the default row's
    // NULL would have answered `0, 0`.
    assert_eq!(
        row_text(session.run(
            "SELECT COUNT(1), (SELECT COUNT(1) FROM e2 WHERE e2.a > e1.a) AS field \
             FROM e1 WHERE e1.a = 100"
        )),
        [["0", "NULL"]]
    );
    // The inner really is skipped rather than merely answering NULL by
    // accident: a subquery that would return a value for ANY binding still
    // answers NULL here.
    assert_eq!(
        row_text(session.run(
            "SELECT COUNT(1), (SELECT COUNT(1) FROM e2 WHERE e1.a IS NULL) \
             FROM e1 WHERE e1.a = 100"
        )),
        [["0", "NULL"]]
    );
    // Only the FIRST Apply deselects: Go's `aggExecutorTreeInputEmpty` stops
    // at a two-child operator, and the second Apply's outer IS the first one.
    // Captured from TiDB, which answers `0, NULL, 0`.
    assert_eq!(
        row_text(session.run(
            "SELECT COUNT(1), (SELECT COUNT(1) FROM e2 WHERE e2.a > e1.a), \
             (SELECT COUNT(1) FROM e2 WHERE e2.a < e1.a) FROM e1 WHERE e1.a = 100"
        )),
        [["0", "NULL", "0"]]
    );
    // Only a SCALAR subquery's apply is the left outer join whose mismatch
    // pads NULL. Go decorrelates `EXISTS` into a semi HASH JOIN, which has no
    // deselect rule at all, so its inner really does run over the default
    // row: captured from TiDB, this answers `0, 1` -- an `EXISTS` that is
    // TRUE precisely because the default row's grouped `a` is NULL.
    assert_eq!(
        row_text(session.run(
            "SELECT COUNT(1), EXISTS(SELECT 1 FROM e2 WHERE e2.a > e1.a OR e1.a IS NULL) \
             FROM e1 WHERE e1.a = 100"
        )),
        [["0", "1"]]
    );

    // The boundary: a NON-empty outer runs the inner as always, even when the
    // grouped value it binds is NULL. One row whose `a` IS NULL answers `1, 0`
    // -- the same binding the default row carries, and a different answer.
    session.run("INSERT INTO e1 VALUES (NULL)").unwrap();
    assert_eq!(
        row_text(session.run(
            "SELECT COUNT(1), (SELECT COUNT(1) FROM e2 WHERE e2.a > e1.a) \
             FROM e1 WHERE e1.a IS NULL"
        )),
        [["1", "0"]]
    );
    // ... and an aggregate whose own empty-input value is NULL keeps it.
    assert_eq!(
        row_text(session.run(
            "SELECT MAX(a), (SELECT COUNT(1) FROM e2 WHERE e2.a > e1.a) \
             FROM e1 WHERE e1.a = 100"
        )),
        [["NULL", "NULL"]]
    );
    // A GROUP BY has no default row at all, so there is nothing to deselect
    // and the statement answers no rows.
    assert!(row_text(session.run(
        "SELECT COUNT(1), (SELECT COUNT(1) FROM e2 WHERE e2.a > e1.a) \
             FROM e1 WHERE e1.a = 100 GROUP BY e1.a"
    ))
    .is_empty());
    // And without an aggregate there is no default row either.
    assert!(row_text(
        session.run("SELECT (SELECT COUNT(1) FROM e2 WHERE e2.a > e1.a) FROM e1 WHERE e1.a = 100")
    )
    .is_empty());
}

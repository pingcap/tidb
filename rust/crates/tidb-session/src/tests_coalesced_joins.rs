//! `NATURAL JOIN`, `JOIN ... USING`, and `GROUP BY <select-list alias>`.
//!
//! The three share one question -- which names are visible where, and in what
//! order a name resolves -- so they are checked together, one test per RULE
//! captured from a real `mockstore`-backed TiDB session (`session.Execute`,
//! reading the result set's `Fields()` for the column order and the returned
//! `*errors.Error` for the code). `tidb_executor::driver::from`'s
//! `coalesce_common_columns` and `driver::resolve_group_by_item` state the
//! rules; this file is the evidence that they hold.

#![cfg(test)]

use crate::tests_support::row_text;
use crate::{Session, StmtOutput};

/// A query's column names, which is the whole point of coalescing.
fn columns(session: &mut Session, sql: &str) -> Vec<String> {
    match session.run_with_columns(sql).unwrap() {
        StmtOutput::Rows { columns, .. } => columns.into_iter().map(|(name, _)| name).collect(),
        other => panic!("expected rows from `{sql}`, got {other:?}"),
    }
}

/// A query's rows, sorted, so an assertion does not depend on join order.
fn rows(session: &mut Session, sql: &str) -> Vec<String> {
    let mut out: Vec<String> = row_text(session.run(sql))
        .into_iter()
        .map(|row| row.join("|"))
        .collect();
    out.sort();
    out
}

/// `n1(a, b)` and `n2(a, c)`: one common column, one matched key, and one
/// unmatched row on each side so outer joins have something to NULL-pad.
fn natural_session() -> Session {
    let mut session = Session::new();
    session.run("CREATE TABLE n1 (a INT, b INT)").unwrap();
    session.run("CREATE TABLE n2 (a INT, c INT)").unwrap();
    session
        .run("INSERT INTO n1 VALUES (1, 10), (2, 20)")
        .unwrap();
    session
        .run("INSERT INTO n2 VALUES (1, 100), (3, 300)")
        .unwrap();
    session
}

/// `m1(k, a, b, z)` and `m2(b, a, w)`: two common columns, DECLARED IN
/// OPPOSITE ORDERS on the two sides, which is what makes the ordering rule
/// observable at all.
fn common_pair_session() -> Session {
    let mut session = Session::new();
    session
        .run("CREATE TABLE m1 (k INT, a INT, b INT, z INT)")
        .unwrap();
    session
        .run("CREATE TABLE m2 (b INT, a INT, w INT)")
        .unwrap();
    session.run("INSERT INTO m1 VALUES (7, 1, 2, 9)").unwrap();
    session.run("INSERT INTO m2 VALUES (2, 1, 5)").unwrap();
    session
}

/// The display order: common columns first, then the left side's remaining
/// columns, then the right side's.
///
/// Go: `SELECT * FROM n1 NATURAL JOIN n2` reports `a | b | c`, one `a`.
#[test]
fn natural_join_reports_the_common_column_once_and_first() {
    let mut session = natural_session();
    assert_eq!(
        columns(&mut session, "SELECT * FROM n1 NATURAL JOIN n2"),
        ["a", "b", "c"]
    );
    assert_eq!(
        rows(&mut session, "SELECT * FROM n1 NATURAL JOIN n2"),
        ["1|10|100"]
    );
}

/// THE ordering rule, and the one a `USING` list makes easy to get wrong:
/// the common columns take the LEFT side's own declaration order, NOT the
/// order the `USING` clause writes them in.
///
/// Go: `m1` declares `k, a, b, z`, so both `USING (b, a)` and `USING (a, b)`
/// report `a | b | k | z | w`.
#[test]
fn common_columns_take_the_left_sides_order_not_the_using_lists() {
    let mut session = common_pair_session();
    for sql in [
        "SELECT * FROM m1 JOIN m2 USING (b, a)",
        "SELECT * FROM m1 JOIN m2 USING (a, b)",
        "SELECT * FROM m1 NATURAL JOIN m2",
    ] {
        assert_eq!(
            columns(&mut session, sql),
            ["a", "b", "k", "z", "w"],
            "{sql}"
        );
        assert_eq!(rows(&mut session, sql), ["1|2|7|9|5"], "{sql}");
    }
}

/// A RIGHT join mirrors EVERYTHING: the two sides swap in the display order,
/// and the common columns take the RIGHT side's declaration order.
///
/// Go: `m2` declares `b, a, w`, so `m1 RIGHT JOIN m2 USING (b, a)` reports
/// `b | a | w | k | z` -- while the LEFT join keeps `m1`'s order.
#[test]
fn a_right_join_mirrors_the_display_order() {
    let mut session = common_pair_session();
    assert_eq!(
        columns(&mut session, "SELECT * FROM m1 RIGHT JOIN m2 USING (b, a)"),
        ["b", "a", "w", "k", "z"]
    );
    assert_eq!(
        rows(&mut session, "SELECT * FROM m1 RIGHT JOIN m2 USING (b, a)"),
        ["2|1|5|7|9"]
    );
    assert_eq!(
        columns(&mut session, "SELECT * FROM m1 LEFT JOIN m2 USING (b, a)"),
        ["a", "b", "k", "z", "w"]
    );
}

/// The surviving copy is the OUTER (row-preserving) side's column, so it is
/// never the NULL-padded one -- which is why no `COALESCE` is needed and the
/// coalesced column can be a pure naming decision.
///
/// Go: `n1 NATURAL LEFT JOIN n2` reports `a` = `n1.a` (`2` where `n2` has no
/// match), and `n1 NATURAL RIGHT JOIN n2` reports `a` = `n2.a` (`3` where
/// `n1` has none).
#[test]
fn the_surviving_common_column_is_the_row_preserving_sides() {
    let mut session = natural_session();
    assert_eq!(
        rows(
            &mut session,
            "SELECT a, n1.a, n2.a FROM n1 NATURAL LEFT JOIN n2"
        ),
        ["1|1|1", "2|2|NULL"]
    );
    assert_eq!(
        rows(
            &mut session,
            "SELECT a, n1.a, n2.a FROM n1 NATURAL RIGHT JOIN n2"
        ),
        ["1|1|1", "3|NULL|3"]
    );
    assert_eq!(
        columns(&mut session, "SELECT * FROM n1 NATURAL RIGHT JOIN n2"),
        ["a", "c", "b"]
    );
}

/// The coalesced-away column is hidden from `*` and from an unqualified name
/// ONLY: both sides stay reachable through their own qualifier, and Go does
/// NOT reject the qualified reference.
///
/// Go: `SELECT n1.a, n2.a FROM n1 NATURAL JOIN n2` reports two columns.
#[test]
fn a_coalesced_column_is_still_reachable_through_either_qualifier() {
    let mut session = natural_session();
    assert_eq!(
        rows(&mut session, "SELECT n1.a, n2.a FROM n1 NATURAL JOIN n2"),
        ["1|1"]
    );
    // `t.*` is untouched by coalescing: it is still the table's own columns
    // in declaration order, hidden copy included.
    assert_eq!(
        columns(&mut session, "SELECT n2.* FROM n1 NATURAL JOIN n2"),
        ["a", "c"]
    );
    assert_eq!(
        rows(&mut session, "SELECT n2.* FROM n1 NATURAL LEFT JOIN n2"),
        ["1|100", "NULL|NULL"]
    );
}

/// Zero common columns degenerates to a plain cross join -- the full
/// cartesian product, not an empty result.
///
/// Go: `x1(p)` with 2 rows NATURAL JOIN `x2(q)` with 2 rows is 4 rows.
#[test]
fn a_natural_join_with_no_common_column_is_a_cross_join() {
    let mut session = Session::new();
    session.run("CREATE TABLE x1 (p INT)").unwrap();
    session.run("CREATE TABLE x2 (q INT)").unwrap();
    session.run("INSERT INTO x1 VALUES (1), (2)").unwrap();
    session.run("INSERT INTO x2 VALUES (3), (4)").unwrap();
    assert_eq!(
        rows(&mut session, "SELECT * FROM x1 NATURAL JOIN x2"),
        ["1|3", "1|4", "2|3", "2|4"]
    );
    // The RIGHT-join mirror still applies with nothing to coalesce.
    assert_eq!(
        columns(&mut session, "SELECT * FROM x1 NATURAL RIGHT JOIN x2"),
        ["q", "p"]
    );
}

/// A nested coalesced join matches on its child's DISPLAY columns, so the
/// child's already-hidden duplicate never re-enters the match.
///
/// Go: `n1 NATURAL JOIN n2 NATURAL JOIN t3(a, d)` reports `a | b | c | d`.
#[test]
fn a_nested_natural_join_matches_the_childs_visible_columns() {
    let mut session = natural_session();
    session.run("CREATE TABLE t3 (a INT, d INT)").unwrap();
    session.run("INSERT INTO t3 VALUES (1, 77)").unwrap();
    assert_eq!(
        columns(
            &mut session,
            "SELECT * FROM n1 NATURAL JOIN n2 NATURAL JOIN t3"
        ),
        ["a", "b", "c", "d"]
    );
    assert_eq!(
        rows(
            &mut session,
            "SELECT * FROM n1 JOIN n2 USING (a) JOIN t3 USING (a)"
        ),
        ["1|10|100|77"]
    );
}

/// A PLAIN join above a coalesced one keeps BOTH children's display columns.
///
/// Go's `buildJoin` gives a plain join its two CHILDREN's output names
/// concatenated, and a coalesced child's names are already the coalesced
/// ones; only the join that COALESCES drops a column. Captured from TiDB:
/// `select * from t1 join t2 using (a) right join t3 on (t2.a = t3.a)` heads
/// `a c d a` -- four columns, the inner join's three plus t3's own -- and the
/// two-coalesced-sides form heads `a b c b d a`.
#[test]
fn a_plain_join_above_a_coalesced_one_keeps_the_right_sides_columns() {
    let mut session = natural_session();
    session.run("CREATE TABLE t3 (a INT, d INT)").unwrap();
    session.run("INSERT INTO t3 VALUES (1, 77)").unwrap();
    assert_eq!(
        columns(
            &mut session,
            "SELECT * FROM n1 JOIN n2 USING (a) RIGHT JOIN t3 ON (n2.a = t3.a)"
        ),
        ["a", "b", "c", "a", "d"]
    );
    assert_eq!(
        columns(
            &mut session,
            "SELECT * FROM n1 JOIN n2 USING (a) JOIN t3 ON (n2.a = t3.a)"
        ),
        ["a", "b", "c", "a", "d"]
    );
    assert_eq!(
        rows(
            &mut session,
            "SELECT * FROM n1 JOIN n2 USING (a) JOIN t3 ON (n2.a = t3.a)"
        ),
        ["1|10|100|1|77"]
    );
}

/// A comma binds LOOSER than `NATURAL JOIN`, so `FROM a, b NATURAL JOIN c`
/// coalesces b with c and leaves a alone.
///
/// Captured from TiDB: over three one-column tables `t1(i)`, `t2(i)`, `t3(i)`
/// with rows 1, 2, 3, `select * from t1, t2 natural left join t3` heads `i i`
/// and answers `1|2` -- t1's own column plus the natural join's single
/// coalesced one. The RIGHT form keeps t3's copy instead (`1|3`), which is
/// the same outer-side rule the two-table case follows. Written as an
/// explicit parenthesized right operand as well, because that spelling must
/// agree: it is the same tree.
#[test]
fn a_comma_binds_looser_than_a_natural_join() {
    let mut session = Session::new();
    for table in ["c1", "c2", "c3"] {
        session
            .run(&format!("CREATE TABLE {table} (i INT)"))
            .unwrap();
    }
    session.run("INSERT INTO c1 VALUES (1)").unwrap();
    session.run("INSERT INTO c2 VALUES (2)").unwrap();
    session.run("INSERT INTO c3 VALUES (3)").unwrap();
    assert_eq!(
        columns(&mut session, "SELECT * FROM c1, c2 NATURAL LEFT JOIN c3"),
        ["i", "i"]
    );
    assert_eq!(
        rows(&mut session, "SELECT * FROM c1, c2 NATURAL LEFT JOIN c3"),
        ["1|2"]
    );
    assert_eq!(
        rows(&mut session, "SELECT * FROM c1, c2 NATURAL RIGHT JOIN c3"),
        ["1|3"]
    );
    assert_eq!(
        rows(&mut session, "SELECT * FROM c1 NATURAL LEFT JOIN c2, c3"),
        ["1|3"]
    );
}

/// A `USING` name neither side offers is Go's `ErrUnknownColumn` (1054)
/// against the `from clause`, not a silently empty join.
#[test]
fn an_unknown_using_column_is_rejected() {
    let mut session = natural_session();
    let error = session
        .run("SELECT * FROM n1 JOIN n2 USING (q)")
        .unwrap_err();
    let mysql = error.clone().to_mysql_error();
    assert_eq!(mysql.code, 1054);
    assert!(
        mysql.message.contains("'q'") && mysql.message.contains("from clause"),
        "{}",
        mysql.message
    );
}

/// A `USING` list is a SET: repeating a name coalesces it once.
#[test]
fn a_repeated_using_column_coalesces_once() {
    let mut session = natural_session();
    assert_eq!(
        columns(&mut session, "SELECT * FROM n1 JOIN n2 USING (a, a)"),
        ["a", "b", "c"]
    );
}

/// A `USING` name is matched case-insensitively, as every SQL identifier is.
#[test]
fn a_using_column_matches_case_insensitively() {
    let mut session = natural_session();
    assert_eq!(
        rows(&mut session, "SELECT * FROM n1 JOIN n2 USING (A)"),
        ["1|10|100"]
    );
}

/// The equality a coalesced join synthesizes is a plain `=`, so a NULL
/// common value matches nothing -- it does not behave like `IS NOT
/// DISTINCT FROM`.
#[test]
fn a_null_common_value_matches_nothing() {
    let mut session = Session::new();
    session.run("CREATE TABLE nn1 (a INT, b INT)").unwrap();
    session.run("CREATE TABLE nn2 (a INT, c INT)").unwrap();
    session
        .run("INSERT INTO nn1 VALUES (1,1),(2,2),(NULL,9)")
        .unwrap();
    session
        .run("INSERT INTO nn2 VALUES (NULL,7),(2,8)")
        .unwrap();
    assert_eq!(
        rows(&mut session, "SELECT * FROM nn1 NATURAL JOIN nn2"),
        ["2|2|8"]
    );
    assert_eq!(
        rows(&mut session, "SELECT * FROM nn1 NATURAL LEFT JOIN nn2"),
        ["1|1|NULL", "2|2|8", "NULL|9|NULL"]
    );
}

/// THE `GROUP BY` alias rule, and the one that is the opposite of `ORDER
/// BY`'s: a real column of the `FROM` scope WINS over a select-list alias of
/// the same name.
///
/// Go: over `sh(x, y)`, `SELECT y AS x, count(*) FROM sh GROUP BY x` groups
/// by `sh.x` -- and therefore fails `ONLY_FULL_GROUP_BY` (1055) because `y`
/// is not determined by it. An `ORDER BY x` on the same statement would have
/// sorted by `y`.
#[test]
fn a_real_column_beats_a_select_alias_in_group_by() {
    let mut session = Session::new();
    session.run("CREATE TABLE sh (x INT, y INT)").unwrap();
    session.run("INSERT INTO sh VALUES (1,10),(2,20)").unwrap();
    let error = session
        .run("SELECT y AS x, count(*) FROM sh GROUP BY x")
        .unwrap_err();
    assert_eq!(error.clone().to_mysql_error().code, 1055);
    // The mirror image: the alias `y` does not divert the grouping from the
    // real `sh.y` either.
    let error = session
        .run("SELECT x AS y, count(*) FROM sh GROUP BY y")
        .unwrap_err();
    assert_eq!(error.clone().to_mysql_error().code, 1055);
}

/// A name the `FROM` scope does NOT have resolves to the select-list alias,
/// and the group key is that field's own COMPUTED expression.
#[test]
fn group_by_resolves_an_alias_the_scope_lacks() {
    let mut session = alias_session();
    assert_eq!(
        rows(
            &mut session,
            "SELECT dept AS x, count(*) FROM gt GROUP BY x"
        ),
        ["a|2", "b|2", "c|1"]
    );
    assert_eq!(
        rows(
            &mut session,
            "SELECT concat(dept, '_s') AS x, count(*) FROM gt GROUP BY x"
        ),
        ["a_s|2", "b_s|2", "c_s|1"]
    );
    // An alias inside a LARGER group-by expression resolves too, which is
    // what makes the substitution a rewrite rather than a whole-item lookup.
    // Go reports 1055 here, not 1054: the item DID resolve, to `dept + 0`,
    // and it is `dept` in the select list that `dept + 0` fails to justify.
    let error = session
        .run("SELECT dept AS x, count(*) FROM gt GROUP BY x + 0")
        .unwrap_err();
    assert_eq!(error.clone().to_mysql_error().code, 1055);
}

/// Grouping by an alias satisfies `ONLY_FULL_GROUP_BY` exactly as grouping by
/// the underlying expression does, because the check runs on the RESOLVED
/// item: `GROUP BY x` pins `gt.dept`, so `SELECT dept AS x` is justified --
/// while a second, ungrouped column in the same select list is still not.
#[test]
fn a_group_by_alias_satisfies_only_full_group_by_through_what_it_resolves_to() {
    let mut session = alias_session();
    assert_eq!(
        rows(
            &mut session,
            "SELECT dept AS x, count(*) FROM gt GROUP BY x"
        ),
        ["a|2", "b|2", "c|1"]
    );
    let error = session
        .run("SELECT dept AS x, id FROM gt GROUP BY x")
        .unwrap_err();
    assert_eq!(error.clone().to_mysql_error().code, 1055);
    // Clearing the mode restores the permissive FIRST_ROW behavior, which is
    // the same switch every other ONLY_FULL_GROUP_BY case answers to.
    session.run("SET sql_mode = ''").unwrap();
    assert_eq!(
        rows(&mut session, "SELECT dept AS x, id FROM gt GROUP BY x").len(),
        3
    );
}

/// Once `GROUP BY` has established a column through an alias, `HAVING` and
/// `ORDER BY` may name EITHER the alias or the underlying column.
#[test]
fn having_and_order_by_see_both_names_after_a_group_by_alias() {
    let mut session = alias_session();
    assert_eq!(
        rows(
            &mut session,
            "SELECT dept AS x, count(*) FROM gt GROUP BY x HAVING dept = 'a'"
        ),
        ["a|2"]
    );
    assert_eq!(
        rows(
            &mut session,
            "SELECT dept AS x, count(*) c FROM gt GROUP BY x HAVING x = 'a'"
        ),
        ["a|2"]
    );
    assert_eq!(
        rows(
            &mut session,
            "SELECT dept AS x, count(*) c FROM gt GROUP BY x HAVING c > 1"
        ),
        ["a|2", "b|2"]
    );
    assert_eq!(
        row_text(session.run("SELECT dept AS x, count(*) FROM gt GROUP BY x ORDER BY dept"))
            .into_iter()
            .map(|row| row.join("|"))
            .collect::<Vec<_>>(),
        ["a|2", "b|2", "c|1"]
    );
}

/// An alias naming an AGGREGATE has no value at grouping time, so Go reports
/// `ErrIllegalReference` (1247) rather than grouping by it.
#[test]
fn group_by_an_aggregate_alias_is_illegal() {
    let mut session = alias_session();
    let error = session
        .run("SELECT dept, count(*) AS c FROM gt GROUP BY c")
        .unwrap_err();
    let mysql = error.clone().to_mysql_error();
    assert_eq!(mysql.code, 1247);
    assert!(
        mysql.message.contains("group function"),
        "{}",
        mysql.message
    );
}

/// The fixture the `GROUP BY` alias cases run against: three departments
/// with different group sizes, so a wrong group key changes the row count.
fn alias_session() -> Session {
    let mut session = Session::new();
    session
        .run("CREATE TABLE gt (id INT, dept VARCHAR(10), salary INT)")
        .unwrap();
    session
        .run("INSERT INTO gt VALUES (1,'a',100),(2,'a',200),(3,'b',150),(4,'b',300),(5,'c',50)")
        .unwrap();
    session
}

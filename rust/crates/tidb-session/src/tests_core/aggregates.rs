//! Aggregation: the aggregate functions, how `ORDER BY`/`GROUP BY`/`HAVING`
//! resolve against the select list, what makes a query an aggregate query,
//! and `WITH ROLLUP` -- Go `pkg/executor/aggregate` and
//! `pkg/planner/core`'s aggregate resolution.

use crate::tests_support::*;
use crate::*;

/// `GROUP_CONCAT`, checked against captured TiDB output.
#[test]
fn group_concat() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE t (g BIGINT, v VARCHAR(10), n BIGINT)")
        .unwrap();
    session
        .run("INSERT INTO t VALUES (1,'b',2),(1,'a',1),(2,'c',3),(2,NULL,4),(1,'a',5)")
        .unwrap();

    // Captured: every non-NULL value joined by a comma, in row order.
    assert_eq!(
        row_text(session.run("SELECT GROUP_CONCAT(v) FROM t")),
        [["b,a,c,a"]]
    );
    // Captured: per group, with the NULL contributing nothing.
    assert_eq!(
        row_text(session.run("SELECT g, GROUP_CONCAT(v) FROM t GROUP BY g ORDER BY g")),
        [["1", "b,a,a"], ["2", "c"]]
    );
    // Captured: an explicit separator.
    assert_eq!(
        row_text(
            session.run("SELECT g, GROUP_CONCAT(v SEPARATOR '-') FROM t GROUP BY g ORDER BY g")
        ),
        [["1", "b-a-a"], ["2", "c"]]
    );
    // Captured: DISTINCT folds the repeat. TiDB's own output for this
    // group is `a,b`; MySQL documents the order of a GROUP_CONCAT
    // without ORDER BY as undefined, so only the membership is asserted.
    let distinct =
        row_text(session.run("SELECT g, GROUP_CONCAT(DISTINCT v) FROM t GROUP BY g ORDER BY g"));
    let mut first: Vec<&str> = distinct[0][1].split(',').collect();
    first.sort_unstable();
    assert_eq!(first, ["a", "b"]);
    assert_eq!(distinct[1][1], "c");
    // Captured: numbers are stringified.
    assert_eq!(
        row_text(session.run("SELECT GROUP_CONCAT(n) FROM t")),
        [["2,1,3,4,5"]]
    );
    // Captured: an empty group is NULL, not an empty string.
    assert_eq!(
        row_text(session.run("SELECT GROUP_CONCAT(v) FROM t WHERE g = 99")),
        [["NULL"]]
    );

    // Captured: the aggregate's own ORDER BY orders the rows WITHIN the
    // concatenation -- a separate scope from the query's ORDER BY.
    assert_eq!(
        row_text(session.run("SELECT g, GROUP_CONCAT(v ORDER BY v) FROM t GROUP BY g ORDER BY g")),
        [["1", "a,a,b"], ["2", "c"]]
    );
    // Captured: it may order by a column the concatenation does not
    // contain, descending, with its own separator.
    assert_eq!(
        row_text(session.run(
            "SELECT g, GROUP_CONCAT(v ORDER BY n DESC SEPARATOR '|') FROM t \
                 GROUP BY g ORDER BY g"
        )),
        [["1", "a|b|a"], ["2", "c"]]
    );

    // The multi-argument form: captured from TiDB, the arguments are
    // concatenated PER ROW (like CONCAT) before the rows are joined, and
    // a row is dropped as soon as ANY of its arguments is NULL -- not
    // only when all of them are.
    session.run("INSERT INTO t VALUES (2,'d',NULL)").unwrap();
    session.run("INSERT INTO t VALUES (1,'a',1)").unwrap();
    // (2,NULL,4) and (2,'d',NULL) each have one NULL argument: both drop.
    assert_eq!(
        row_text(session.run("SELECT g, GROUP_CONCAT(v, n) FROM t GROUP BY g ORDER BY g")),
        [["1", "b2,a1,a5,a1"], ["2", "c3"]]
    );
    // ...while the one-argument form still keeps 'd' (its v is not NULL).
    assert_eq!(
        row_text(session.run("SELECT GROUP_CONCAT(v) FROM t WHERE g = 2")),
        [["c,d"]]
    );
    // Captured: DISTINCT dedupes over the CONCATENATED per-row value, so
    // the repeated ('a',1) folds while ('a',5) survives. Row order
    // without ORDER BY is undefined; assert membership only.
    let multi =
        row_text(session.run("SELECT g, GROUP_CONCAT(DISTINCT v, n) FROM t GROUP BY g ORDER BY g"));
    let mut first: Vec<&str> = multi[0][1].split(',').collect();
    first.sort_unstable();
    assert_eq!(first, ["a1", "a5", "b2"]);
    assert_eq!(multi[1][1], "c3");
    // Captured: a literal argument concatenates like any other.
    assert_eq!(
        row_text(session.run("SELECT g, GROUP_CONCAT(v, '-', n) FROM t GROUP BY g ORDER BY g")),
        [["1", "b-2,a-1,a-5,a-1"], ["2", "c-3"]]
    );
    // Captured: multi-arg with the aggregate's own ORDER BY and separator.
    assert_eq!(
        row_text(session.run(
            "SELECT g, GROUP_CONCAT(v, n ORDER BY n DESC SEPARATOR '|') FROM t \
                 GROUP BY g ORDER BY g"
        )),
        [["1", "a5|b2|a1|a1"], ["2", "c3"]]
    );
}

/// The aggregates over each numeric domain, checked against captured
/// TiDB output.
///
/// The type is the load-bearing part: `SUM` over a BIGINT column is a
/// DECIMAL in MySQL (captured type 246), not a BIGINT, so it sums in the
/// decimal domain the way Go's `sum4Decimal` does. Only a real argument
/// makes it a DOUBLE.
#[test]
fn aggregates_over_numeric_domains() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE t (a BIGINT, d DECIMAL(10,2), r DOUBLE)")
        .unwrap();
    session
        .run("INSERT INTO t VALUES (1,1.5,1.5),(2,2.25,2.5),(3,3.25,3.5)")
        .unwrap();

    // Captured: SUM over each domain, with the decimal column keeping
    // its own scale.
    assert_eq!(
        row_text(session.run("SELECT SUM(a), SUM(d), SUM(r) FROM t")),
        [["6", "7.00", "7.5"]]
    );
    // Captured: an empty SUM is NULL, not zero.
    assert_eq!(
        row_text(session.run("SELECT SUM(a) FROM t WHERE a > 100")),
        [["NULL"]]
    );
    // Captured: AVG and MIN/MAX over a decimal column.
    assert_eq!(
        row_text(session.run("SELECT MIN(d), MAX(d) FROM t")),
        [["1.50", "3.25"]]
    );
    assert_eq!(
        row_text(session.run("SELECT COUNT(DISTINCT a), COUNT(*) FROM t")),
        [["3", "3"]]
    );
    // Captured: grouped SUM over a decimal column.
    assert_eq!(
        row_text(session.run("SELECT a, SUM(d) FROM t GROUP BY a ORDER BY a")),
        [["1", "1.50"], ["2", "2.25"], ["3", "3.25"]]
    );
}

/// `COUNT(a, b, ...)` / `COUNT(DISTINCT a, b, ...)`, checked against
/// captured TiDB output. Only the `DISTINCT` form is valid SQL for more
/// than one argument (`pkg/parser` rejects a bare `COUNT(a, b)` at parse
/// time, matched by `tidb_parser`'s own `parse_aggregate`), so this test
/// only has `COUNT(DISTINCT ...)` to exercise: a row counts only when
/// EVERY argument is non-NULL, and DISTINCT dedupes over the whole
/// argument tuple rather than a single column.
#[test]
fn multi_argument_count() {
    let mut session = Session::new();
    session.run("CREATE TABLE t (g INT, a INT, b INT)").unwrap();
    session
        .run(
            "INSERT INTO t VALUES \
                 (1, 1, 1), (1, 1, 1), (1, 1, NULL), (1, NULL, 1), (1, NULL, NULL), \
                 (2, 2, 2), (2, 2, 2), (2, 3, 3)",
        )
        .unwrap();

    // Captured: `count(distinct a, b)` over the whole table sees three
    // distinct non-NULL pairs -- (1,1), (2,2), (3,3) -- with every row
    // that has a NULL in either column excluded entirely.
    assert_eq!(
        row_text(session.run("SELECT COUNT(DISTINCT a, b) FROM t")),
        [["3"]]
    );
    // Captured: grouped, group 1 has one distinct non-NULL pair (1,1)
    // (its NULL-containing rows don't count), group 2 has two: (2,2) and
    // (3,3).
    assert_eq!(
        row_text(session.run("SELECT g, COUNT(DISTINCT a, b) FROM t GROUP BY g ORDER BY g")),
        [["1", "1"], ["2", "2"]]
    );
}

/// `ORDER BY` resolved against the SELECT list, checked against captured
/// TiDB output.
///
/// A positional `ORDER BY 1` used to rewrite as a constant here, which
/// silently produced UNSORTED rows -- the worst kind of divergence, and
/// the reason this unit was picked.
#[test]
fn order_by_resolves_against_the_select_list() {
    let mut session = Session::new();
    session.run("CREATE TABLE t (a BIGINT, b BIGINT)").unwrap();
    session
        .run("INSERT INTO t VALUES (1,30),(2,20),(3,10)")
        .unwrap();

    // Captured: an alias names a projected expression.
    assert_eq!(
        row_text(session.run("SELECT a, a*2 AS twice FROM t ORDER BY twice DESC")),
        [["3", "6"], ["2", "4"], ["1", "2"]]
    );
    assert_eq!(
        row_text(session.run("SELECT a AS z FROM t ORDER BY z DESC")),
        [["3"], ["2"], ["1"]]
    );
    // Captured: an expression BUILT on an alias resolves too.
    assert_eq!(
        row_text(session.run("SELECT a*2 AS twice FROM t ORDER BY twice+0 DESC")),
        [["6"], ["4"], ["2"]]
    );
    // Captured: a bare integer is a 1-based output position.
    assert_eq!(
        row_text(session.run("SELECT a FROM t ORDER BY 1 DESC")),
        [["3"], ["2"], ["1"]]
    );
    assert_eq!(
        row_text(session.run("SELECT a, b FROM t ORDER BY 2")),
        [["3", "10"], ["2", "20"], ["1", "30"]]
    );
    // Captured: an alias SHADOWS a real column of the same name.
    assert_eq!(
        row_text(session.run("SELECT b AS a FROM t ORDER BY a")),
        [["10"], ["20"], ["30"]]
    );
    assert_eq!(
        row_text(session.run("SELECT a+0 AS a FROM t ORDER BY a DESC")),
        [["3"], ["2"], ["1"]]
    );
    // Captured: a source column that is not projected still sorts.
    assert_eq!(
        row_text(session.run("SELECT a FROM t ORDER BY b DESC")),
        [["1"], ["2"], ["3"]]
    );

    // Captured: an unknown name and an out-of-range position are both
    // 1054 naming the order clause.
    for sql in [
        "SELECT a FROM t ORDER BY nosuch",
        "SELECT a FROM t ORDER BY 5",
    ] {
        match session.run(sql) {
            Err(error) => {
                let reported = error.to_mysql_error();
                assert_eq!(reported.code, 1054, "{sql}");
                assert!(
                    reported.message.ends_with("in 'order clause'"),
                    "{sql}: {}",
                    reported.message
                );
            }
            Ok(other) => panic!("expected 1054 from {sql}, got {other:?}"),
        }
    }
}

/// `GROUP BY` resolved against the SELECT list, checked against captured
/// TiDB output.
///
/// A positional `GROUP BY 1` used to rewrite as a constant here too --
/// the same silent-wrong-rows bug `ORDER BY 1` once had, but for
/// grouping: every row collapsed into one group instead of grouping by
/// the first select field.
#[test]
fn group_by_resolves_against_the_select_list() {
    let mut session = Session::new();
    session.run("CREATE TABLE t (a BIGINT, b BIGINT)").unwrap();
    session
        .run("INSERT INTO t VALUES (1,30),(1,31),(2,20),(3,10)")
        .unwrap();

    // Captured: a bare integer is a 1-based output position, grouping by
    // the first select field (`a`) -- three groups, not one.
    assert_eq!(
        row_text(session.run("SELECT a, COUNT(*) FROM t GROUP BY 1")),
        [["1", "2"], ["2", "1"], ["3", "1"]]
    );

    // Captured: a position landing on an aggregate select field is
    // ErrWrongGroupField (1056), whether or not it carries an alias.
    for sql in [
        "SELECT a, COUNT(*) FROM t GROUP BY 2",
        "SELECT a, COUNT(*) AS c FROM t GROUP BY 2",
    ] {
        match session.run(sql) {
            Err(error) => {
                let reported = error.to_mysql_error();
                assert_eq!(reported.code, 1056, "{sql}");
                assert!(
                    reported.message.starts_with("Can't group on"),
                    "{sql}: {}",
                    reported.message
                );
            }
            Ok(other) => panic!("expected 1056 from {sql}, got {other:?}"),
        }
    }

    // A positional ORDER BY on the AGGREGATE path was the same silent
    // drop: the bare integer fell through as a constant and the sort
    // never happened. `ORDER BY 2 DESC` sorts by the count.
    assert_eq!(
        row_text(session.run("SELECT a, COUNT(*) FROM t GROUP BY a ORDER BY 2 DESC, a")),
        [["1", "2"], ["2", "1"], ["3", "1"]]
    );
    assert_eq!(
        row_text(session.run("SELECT a, COUNT(*) FROM t GROUP BY a ORDER BY 1 DESC")),
        [["3", "1"], ["2", "1"], ["1", "2"]]
    );

    // Captured: an out-of-range position (including zero) is 1054 naming
    // the group statement.
    for sql in [
        "SELECT a, COUNT(*) FROM t GROUP BY 0",
        "SELECT a, COUNT(*) FROM t GROUP BY 3",
    ] {
        match session.run(sql) {
            Err(error) => {
                let reported = error.to_mysql_error();
                assert_eq!(reported.code, 1054, "{sql}");
                assert!(
                    reported.message.ends_with("in 'group statement'"),
                    "{sql}: {}",
                    reported.message
                );
            }
            Ok(other) => panic!("expected 1054 from {sql}, got {other:?}"),
        }
    }

    // An expression BUILT on a position (`1+1`) is arithmetic, not a
    // position: it groups every row into one bucket by the constant 2.
    assert_eq!(
        row_text(session.run("SELECT COUNT(*) FROM t GROUP BY 1+1")),
        [["4"]]
    );
}

/// Go `havingWindowAndOrderbyExprResolver` walks the WHOLE expression tree,
/// so an aggregate in `HAVING`/`ORDER BY` is hoisted to an aggregation output
/// column no matter which form encloses it -- `BETWEEN`, `IN`, `IS NULL`,
/// `LIKE`, `IF()`, `CASE`, or a subquery comparison. A subquery's own SELECT
/// is NOT descended into; the operand beside it still is.
///
/// Captured from TiDB (`testkit`, `pkg/executor`) over
/// `ha (id, v) = (1,10),(1,20),(2,30),(2,40),(3,50)`:
///
/// ```text
/// having count(*) between 1 and 5     [[1 2] [2 2] [3 1]]
/// having count(*) in (2)              [[1 2] [2 2]]
/// having count(*) is not null         [[1 2] [2 2] [3 1]]
/// having count(*) in (select 2)       [[1 2] [2 2]]
/// having count(*) like '1'            [[3 1]]
/// having if(count(*) = 2, 1, 0)       [[1] [2]]
/// having case count(*) when 2 then 1 else 0 end   [[1] [2]]
/// having count(*) not between 2 and 9 [[3]]
/// having sum(v) > 20                  [[1] [2] [3]]
/// order by count(*) desc, id          [[1] [2] [3]]
/// having count(*) in (select count(*) from ha)    []
/// having exists (select 1 from ha)    [[1] [2] [3]]
/// having count(*) = any (select 2)    [[1] [2]]
/// ```
#[test]
fn having_hoists_an_aggregate_out_of_any_enclosing_form() {
    let mut session = Session::new();
    session.run("CREATE TABLE ha (id INT, v INT)").unwrap();
    session
        .run("INSERT INTO ha VALUES (1,10),(1,20),(2,30),(2,40),(3,50)")
        .unwrap();
    let cases: &[(&str, &[&[&str]])] = &[
        (
            "SELECT id, COUNT(*) FROM ha GROUP BY id HAVING COUNT(*) BETWEEN 1 AND 5 ORDER BY id",
            &[&["1", "2"], &["2", "2"], &["3", "1"]],
        ),
        (
            "SELECT id, COUNT(*) FROM ha GROUP BY id HAVING COUNT(*) IN (2) ORDER BY id",
            &[&["1", "2"], &["2", "2"]],
        ),
        (
            "SELECT id, COUNT(*) FROM ha GROUP BY id HAVING COUNT(*) IS NOT NULL ORDER BY id",
            &[&["1", "2"], &["2", "2"], &["3", "1"]],
        ),
        (
            "SELECT id, COUNT(*) FROM ha GROUP BY id HAVING COUNT(*) IN (SELECT 2) ORDER BY id",
            &[&["1", "2"], &["2", "2"]],
        ),
        (
            "SELECT id, COUNT(*) FROM ha GROUP BY id HAVING COUNT(*) LIKE '1' ORDER BY id",
            &[&["3", "1"]],
        ),
        (
            "SELECT id FROM ha GROUP BY id HAVING IF(COUNT(*) = 2, 1, 0) ORDER BY id",
            &[&["1"], &["2"]],
        ),
        (
            "SELECT id FROM ha GROUP BY id HAVING CASE COUNT(*) WHEN 2 THEN 1 ELSE 0 END \
             ORDER BY id",
            &[&["1"], &["2"]],
        ),
        (
            "SELECT id FROM ha GROUP BY id HAVING COUNT(*) NOT BETWEEN 2 AND 9 ORDER BY id",
            &[&["3"]],
        ),
        (
            "SELECT id FROM ha GROUP BY id HAVING SUM(v) > 20 ORDER BY id",
            &[&["1"], &["2"], &["3"]],
        ),
        (
            "SELECT id FROM ha GROUP BY id ORDER BY COUNT(*) DESC, id",
            &[&["1"], &["2"], &["3"]],
        ),
        (
            "SELECT id FROM ha GROUP BY id HAVING COUNT(*) IN (SELECT COUNT(*) FROM ha) \
             ORDER BY id",
            &[],
        ),
        (
            "SELECT id FROM ha GROUP BY id HAVING EXISTS (SELECT 1 FROM ha) ORDER BY id",
            &[&["1"], &["2"], &["3"]],
        ),
        (
            "SELECT id FROM ha GROUP BY id HAVING COUNT(*) = ANY (SELECT 2) ORDER BY id",
            &[&["1"], &["2"]],
        ),
    ];
    for (sql, want) in cases {
        let got = row_text(session.run(sql));
        let want: Vec<Vec<String>> = want
            .iter()
            .map(|row| row.iter().map(|v| (*v).to_owned()).collect())
            .collect();
        assert_eq!(got, want, "{sql}");
    }
}

/// Go `PlanBuilder.detectSelectAgg` + `buildProjection`: a query is an
/// aggregate query when any select field, `HAVING` or `ORDER BY` expression
/// CONTAINS an aggregate, and an aggregate inside a larger expression is
/// evaluated by a projection ABOVE the aggregation.
///
/// `checkOnlyFullGroupByWithOutGroupClause` guards the other side of that
/// widening: an `ORDER BY` aggregate over a select list that reads a bare
/// column is 3029, so the widening does not turn an error into a wrong answer.
///
/// Captured from TiDB (`testkit`, `pkg/executor`) over
/// `ha (id, v) = (1,10),(1,20),(2,30),(2,40),(3,50)`:
///
/// ```text
/// select if(1=1, count(*), 0) from ha                 [[5]]
/// select case when count(*) > 2 then 'many' else 'few' end from ha  [[many]]
/// select avg(v) / 2, avg(v/id) from ha    [[15.00000000 16.33333333]]
/// select count(*) + 1 from ha                         [[6]]
/// select id, count(*) * 2 from ha group by id         [[1 4] [2 4] [3 2]]
/// select id, if(count(*) = 2, 'pair', 'other') from ha group by id
///                                          [[1 pair] [2 pair] [3 other]]
/// select concat(count(*), '-', sum(v)) from ha        [[5-150]]
/// select -count(*) from ha                            [[-5]]
/// select sum(v) + count(*) from ha                    [[155]]
/// select coalesce(max(v), 0) from ha where id = 99    [[0]]
/// select 1 from ha having count(*) > 1                [[1]]
/// select count(*) from ha order by count(*)           [[5]]
/// select id from ha group by id order by count(*)     [[3] [2] [1]]
///     -- not asserted: ids 1 and 2 tie on count 2, so their order is
///     -- unspecified; the ordered form is asserted in the HAVING test
///
/// select id from ha order by count(*) desc
///     ERR errno=3029 "[planner:3029]Expression #1 of ORDER BY contains
///         aggregate function and applies to the result of a non-aggregated
///         query"
/// select id, count(*) from ha order by count(*)   ERR errno=3029, #1
/// select id from ha order by id, count(*)         ERR errno=3029, #2
/// select id from ha where id = 1 order by count(v)  ERR errno=3029, #1
/// set sql_mode = ''
/// select id from ha order by count(*) desc        [[1]]
/// select id, count(*) from ha order by count(*)   [[1 5]]
/// ```
#[test]
fn a_select_field_containing_an_aggregate_is_an_aggregate_query() {
    let mut session = Session::new();
    session.run("CREATE TABLE ha (id INT, v INT)").unwrap();
    session
        .run("INSERT INTO ha VALUES (1,10),(1,20),(2,30),(2,40),(3,50)")
        .unwrap();
    let cases: &[(&str, &[&[&str]])] = &[
        ("SELECT IF(1=1, COUNT(*), 0) FROM ha", &[&["5"]]),
        (
            "SELECT CASE WHEN COUNT(*) > 2 THEN 'many' ELSE 'few' END FROM ha",
            &[&["many"]],
        ),
        (
            "SELECT AVG(v) / 2, AVG(v/id) FROM ha",
            &[&["15.00000000", "16.33333333"]],
        ),
        ("SELECT COUNT(*) + 1 FROM ha", &[&["6"]]),
        (
            "SELECT id, COUNT(*) * 2 FROM ha GROUP BY id ORDER BY id",
            &[&["1", "4"], &["2", "4"], &["3", "2"]],
        ),
        (
            "SELECT id, IF(COUNT(*) = 2, 'pair', 'other') FROM ha GROUP BY id ORDER BY id",
            &[&["1", "pair"], &["2", "pair"], &["3", "other"]],
        ),
        (
            "SELECT CONCAT(COUNT(*), '-', SUM(v)) FROM ha",
            &[&["5-150"]],
        ),
        ("SELECT -COUNT(*) FROM ha", &[&["-5"]]),
        ("SELECT SUM(v) + COUNT(*) FROM ha", &[&["155"]]),
        (
            "SELECT COALESCE(MAX(v), 0) FROM ha WHERE id = 99",
            &[&["0"]],
        ),
        ("SELECT 1 FROM ha HAVING COUNT(*) > 1", &[&["1"]]),
        ("SELECT COUNT(*) FROM ha ORDER BY COUNT(*)", &[&["5"]]),
    ];
    for (sql, want) in cases {
        let got = row_text(session.run(sql));
        let want: Vec<Vec<String>> = want
            .iter()
            .map(|row| row.iter().map(|v| (*v).to_owned()).collect())
            .collect();
        assert_eq!(got, want, "{sql}");
    }

    // The widening does not turn Go's 3029 into a wrong answer: an ORDER BY
    // aggregate over a select list that reads a bare column is illegal, and
    // the WHERE-clause pinning that exempts a column from 8123 does NOT exempt
    // it here.
    for (sql, position) in [
        ("SELECT id FROM ha ORDER BY COUNT(*) DESC", 1),
        ("SELECT id, COUNT(*) FROM ha ORDER BY COUNT(*)", 1),
        ("SELECT id FROM ha ORDER BY id, COUNT(*)", 2),
        ("SELECT id FROM ha WHERE id = 1 ORDER BY COUNT(v)", 1),
    ] {
        let error = session.run(sql).unwrap_err();
        assert!(
            matches!(&error, DriverError::AggregateOrderNonAggQuery { position: got }
                if *got == position),
            "expected 3029 #{position} from {sql}, got {error:?}"
        );
        let rendered = error.to_mysql_error();
        assert_eq!(rendered.code, 3029);
        assert_eq!(
            rendered.message,
            format!(
                "Expression #{position} of ORDER BY contains aggregate function and applies to \
                 the result of a non-aggregated query"
            )
        );
    }

    // Off ONLY_FULL_GROUP_BY the same statements answer rows, which is what
    // the widened aggregate path computes.
    session.apply_set("SET sql_mode = ''").unwrap();
    assert_eq!(
        row_text(session.run("SELECT id FROM ha ORDER BY COUNT(*) DESC")),
        [["1"]]
    );
    assert_eq!(
        row_text(session.run("SELECT id, COUNT(*) FROM ha ORDER BY COUNT(*)")),
        [["1", "5"]]
    );
}

/// `GROUP BY ... WITH ROLLUP`, checked against captured TiDB output.
///
/// Go's hash aggregation over Expand emits rollup rows in a
/// NONDETERMINISTIC order (verified: the captured order changed across
/// runs of the same query), so without `ORDER BY` only the row MULTISET
/// is contractual. This tier's deterministic order is: full groups in
/// first-seen order, then each shorter prefix's subtotals, then the
/// grand total. The `ORDER BY` cases below match captured TiDB output
/// row for row.
#[test]
fn with_rollup() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE t (a BIGINT, b BIGINT, c BIGINT)")
        .unwrap();
    session
        .run("INSERT INTO t VALUES (1,1,10),(1,2,20),(2,1,30),(2,2,40),(1,1,5)")
        .unwrap();

    // Two-column rollup: every prefix (a,b), (a), () gets aggregate rows,
    // with the rolled-up columns NULL. Multiset captured from TiDB.
    assert_eq!(
        row_text(session.run("SELECT a, b, SUM(c) FROM t GROUP BY a, b WITH ROLLUP")),
        [
            ["1", "1", "15"],
            ["1", "2", "20"],
            ["2", "1", "30"],
            ["2", "2", "40"],
            ["1", "NULL", "35"],
            ["2", "NULL", "70"],
            ["NULL", "NULL", "105"],
        ]
    );
    // Single-column rollup.
    assert_eq!(
        row_text(session.run("SELECT a, SUM(c) FROM t GROUP BY a WITH ROLLUP")),
        [["1", "35"], ["2", "70"], ["NULL", "105"]]
    );
    // COUNT(*) counts the replicated rows per grouping set.
    assert_eq!(
        row_text(session.run("SELECT a, b, COUNT(*) FROM t GROUP BY a, b WITH ROLLUP")),
        [
            ["1", "1", "2"],
            ["1", "2", "1"],
            ["2", "1", "1"],
            ["2", "2", "1"],
            ["1", "NULL", "3"],
            ["2", "NULL", "2"],
            ["NULL", "NULL", "5"],
        ]
    );
    // AVG: captured scale is 4 (decimal AVG over BIGINT).
    assert_eq!(
        row_text(session.run("SELECT a, b, AVG(c) FROM t GROUP BY a, b WITH ROLLUP")),
        [
            ["1", "1", "7.5000"],
            ["1", "2", "20.0000"],
            ["2", "1", "30.0000"],
            ["2", "2", "40.0000"],
            ["1", "NULL", "11.6667"],
            ["2", "NULL", "35.0000"],
            ["NULL", "NULL", "21.0000"],
        ]
    );
    // Captured row for row: ORDER BY sorts NULL first, so the grand
    // total leads and each subtotal precedes its group's rows.
    assert_eq!(
        row_text(session.run("SELECT a, b, SUM(c) FROM t GROUP BY a, b WITH ROLLUP ORDER BY a, b")),
        [
            ["NULL", "NULL", "105"],
            ["1", "NULL", "35"],
            ["1", "1", "15"],
            ["1", "2", "20"],
            ["2", "NULL", "70"],
            ["2", "1", "30"],
            ["2", "2", "40"],
        ]
    );
    assert_eq!(
        row_text(session.run("SELECT a, SUM(c) FROM t GROUP BY a WITH ROLLUP ORDER BY a")),
        [["NULL", "105"], ["1", "35"], ["2", "70"]]
    );

    // A genuinely-NULL data value is indistinguishable from a rollup
    // NULL in the output, exactly as in TiDB: a=1 has rows (b=1,c=10)
    // and (b=NULL,c=20), so both the data group [1 NULL 20] and the
    // subtotal [1 NULL 30] appear (captured). Only GROUPING() tells them
    // apart -- see `grouping_with_rollup`.
    session
        .run("CREATE TABLE tn (a BIGINT, b BIGINT, c BIGINT)")
        .unwrap();
    session
        .run("INSERT INTO tn VALUES (1,1,10),(1,NULL,20),(NULL,1,30),(2,2,40)")
        .unwrap();
    assert_eq!(
        row_text(session.run("SELECT a, b, SUM(c) FROM tn GROUP BY a, b WITH ROLLUP")),
        [
            ["1", "1", "10"],
            ["1", "NULL", "20"],
            ["NULL", "1", "30"],
            ["2", "2", "40"],
            ["1", "NULL", "30"],
            ["NULL", "NULL", "30"],
            ["2", "NULL", "40"],
            ["NULL", "NULL", "100"],
        ]
    );

    // Deferred: a non-column grouping expression cannot be NULLed at the
    // source, so it is refused rather than answered wrongly.
    assert!(matches!(
        session.run("SELECT a+1, SUM(c) FROM t GROUP BY a+1 WITH ROLLUP"),
        Err(DriverError::Unsupported(_))
    ));

    // An empty source yields no rows at all -- not even the grand total
    // -- because Expand replicates zero rows (unlike a scalar aggregate).
    session.run("DELETE FROM t").unwrap();
    assert!(row_text(session.run("SELECT a, SUM(c) FROM t GROUP BY a WITH ROLLUP")).is_empty());
}

/// `GROUPING()` under `WITH ROLLUP`, checked against captured TiDB output.
///
/// `GROUPING(c)` is 1 when `c` is rolled up in the grouping set that
/// produced the row and 0 otherwise, which is the ONLY way to tell a
/// subtotal's NULL from a data NULL. With several arguments it returns a
/// bitmask whose LEFTMOST argument owns the HIGHEST bit (captured:
/// `GROUPING(a,b) = 1` and `GROUPING(b,a) = 2` on the `b`-rolled-up row).
///
/// Rows whose whole `ORDER BY` key ties -- a data-NULL row and the
/// subtotal that also reports `b = NULL` -- keep this tier's stable
/// emission order (data rows first, then subtotals); Go's order for such
/// ties is nondeterministic, so only the multiset is contractual there.
#[test]
fn grouping_with_rollup() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE t (a BIGINT, b BIGINT, c BIGINT)")
        .unwrap();
    session
        .run("INSERT INTO t VALUES (1,1,10),(1,NULL,20),(1,2,30),(2,1,40)")
        .unwrap();

    // Captured row for row. The two `1 NULL` rows are the point: the
    // first is a DATA NULL (grouping(b) = 0, sum 20), the second the
    // rollup subtotal over a=1 (grouping(b) = 1, sum 60).
    assert_eq!(
        row_text(session.run(
            "SELECT a, b, GROUPING(a), GROUPING(b), SUM(c) FROM t \
                 GROUP BY a, b WITH ROLLUP ORDER BY a, b"
        )),
        [
            ["NULL", "NULL", "1", "1", "100"],
            ["1", "NULL", "0", "0", "20"],
            ["1", "NULL", "0", "1", "60"],
            ["1", "1", "0", "0", "10"],
            ["1", "2", "0", "0", "30"],
            ["2", "NULL", "0", "1", "40"],
            ["2", "1", "0", "0", "40"],
        ]
    );

    // Multi-argument bitmask, captured row for row.
    assert_eq!(
        row_text(session.run(
            "SELECT a, b, GROUPING(a,b), GROUPING(b,a), SUM(c) FROM t \
                 GROUP BY a, b WITH ROLLUP ORDER BY a, b"
        )),
        [
            ["NULL", "NULL", "3", "3", "100"],
            ["1", "NULL", "0", "0", "20"],
            ["1", "NULL", "1", "2", "60"],
            ["1", "1", "0", "0", "10"],
            ["1", "2", "0", "0", "30"],
            ["2", "NULL", "1", "2", "40"],
            ["2", "1", "0", "0", "40"],
        ]
    );

    // HAVING reads a GROUPING() the select list does not project: the
    // column is computed, filtered on, and trimmed away. Captured.
    assert_eq!(
        row_text(session.run(
            "SELECT a, b, SUM(c) FROM t GROUP BY a, b WITH ROLLUP \
                 HAVING GROUPING(b) = 0 ORDER BY a, b"
        )),
        [
            ["1", "NULL", "20"],
            ["1", "1", "10"],
            ["1", "2", "30"],
            ["2", "1", "40"],
        ]
    );

    // ORDER BY reads one the same way.
    assert_eq!(
        row_text(session.run(
            "SELECT a, b, GROUPING(a), SUM(c) FROM t GROUP BY a, b WITH ROLLUP \
                 ORDER BY GROUPING(a), a, b"
        )),
        [
            ["1", "NULL", "0", "20"],
            ["1", "NULL", "0", "60"],
            ["1", "1", "0", "10"],
            ["1", "2", "0", "30"],
            ["2", "NULL", "0", "40"],
            ["2", "1", "0", "40"],
            ["NULL", "NULL", "1", "100"],
        ]
    );

    // Captured result type: BIGINT UNSIGNED, flen 20, binary flag.
    match session
        .run_with_columns("SELECT GROUPING(a) FROM t GROUP BY a WITH ROLLUP")
        .unwrap()
    {
        StmtOutput::Rows { columns, .. } => {
            let (name, ftype) = &columns[0];
            // Go names the column with the ORIGINAL written text,
            // `GROUPING(a)` -- no backticks, since none were written.
            assert_eq!(name, "GROUPING(a)");
            assert_eq!(ftype.code(), tidb_datatype::FieldTypeCode::LongLong);
            assert!(ftype.is_unsigned());
            assert_eq!(ftype.flen(), 20);
        }
        other => panic!("expected rows, got {other:?}"),
    }

    // Captured: GROUPING() without WITH ROLLUP is
    // "[planner:1111]Invalid use of group function", whether the query
    // groups or not.
    assert!(matches!(
        session.run("SELECT a, GROUPING(a) FROM t GROUP BY a"),
        Err(DriverError::InvalidGroupFuncUse)
    ));
    assert!(matches!(
        session.run("SELECT a, GROUPING(a) FROM t"),
        Err(DriverError::InvalidGroupFuncUse)
    ));

    // Captured: an argument that is not grouped is
    // "[planner:3602]Argument #0 of GROUPING function is not in GROUP BY".
    assert!(matches!(
        session.run("SELECT a, GROUPING(c) FROM t GROUP BY a, b WITH ROLLUP"),
        Err(DriverError::FieldInGroupingNotGroupBy(0))
    ));

    // Deferred: Go evaluates `GROUPING(a) + 1` in the projection above
    // the aggregation, which this tier does not build for select fields.
    assert!(matches!(
        session.run("SELECT GROUPING(a) + 1 FROM t GROUP BY a, b WITH ROLLUP"),
        Err(DriverError::Unsupported(_))
    ));
}

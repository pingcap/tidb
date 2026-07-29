//! Column pruning over a single-table scan: what the scan is asked to
//! decode, and that every shape outside the narrow slice keeps the unchanged
//! full-width path.
//!
//! The decode set is read straight from the row codec's own input through
//! [`tidb_executor::kv_table::capture_decoded_column_ids`], so "column `d` is
//! never decoded" is measured rather than argued.
#![cfg(test)]

use std::collections::BTreeSet;

use tidb_executor::kv_table::capture_decoded_column_ids;

use crate::tests_support::row_text;
use crate::Session;

/// `t(a,b,c,d)` with ids 1..4 and three rows, plus `s` for the join shapes.
fn prune_session() -> Session {
    let mut session = Session::new();
    session
        .run("CREATE TABLE t (a BIGINT, b BIGINT, c BIGINT, d BIGINT)")
        .unwrap();
    session
        .run("INSERT INTO t VALUES (1,10,100,1000),(2,20,200,2000),(3,30,300,3000)")
        .unwrap();
    session.run("CREATE TABLE s (k BIGINT, v BIGINT)").unwrap();
    session.run("INSERT INTO s VALUES (1,7),(2,8)").unwrap();
    session
}

/// The rows a query returns, and the column ids its scans asked the row
/// codec to decode.
fn rows_and_decoded(session: &mut Session, sql: &str) -> (Vec<Vec<String>>, BTreeSet<i64>) {
    let (result, ids) = capture_decoded_column_ids(|| session.run(sql));
    (row_text(result), ids)
}

/// The ids of `t`'s four columns, in declaration order.
const A: i64 = 1;
const B: i64 = 2;
const C: i64 = 3;
const D: i64 = 4;

/// The whole point: a column no clause mentions is never read out of the
/// stored row bytes, and the ones that are mentioned all are -- including the
/// `WHERE`'s, which the predicate push-down evaluates against the pruned row.
#[test]
fn a_pruned_scan_decodes_exactly_the_columns_the_statement_names() {
    let mut session = prune_session();
    let (rows, decoded) = rows_and_decoded(&mut session, "SELECT c FROM t WHERE a > 1 ORDER BY b");
    assert_eq!(rows, vec![vec!["200"], vec!["300"]]);
    assert_eq!(decoded, BTreeSet::from([A, B, C]));

    // One column, no other clause: three of the four are never touched.
    let (rows, decoded) = rows_and_decoded(&mut session, "SELECT d FROM t");
    assert_eq!(rows, vec![vec!["1000"], vec!["2000"], vec!["3000"]]);
    assert_eq!(decoded, BTreeSet::from([D]));

    // An expression over columns keeps every column it reads.
    let (rows, decoded) = rows_and_decoded(&mut session, "SELECT a + d FROM t WHERE b < 30");
    assert_eq!(rows, vec![vec!["1001"], vec!["2002"]]);
    assert_eq!(decoded, BTreeSet::from([A, B, D]));

    // GROUP BY / HAVING are collected like any other clause.
    let (rows, decoded) = rows_and_decoded(
        &mut session,
        "SELECT b, SUM(c) FROM t GROUP BY b HAVING b > 10",
    );
    assert_eq!(rows, vec![vec!["20", "200"], vec!["30", "300"]]);
    assert_eq!(decoded, BTreeSet::from([B, C]));
}

/// A pruned statement must answer exactly what the full-width path answers.
/// `SELECT *` is not eligible, so the same values read through it are the
/// full-width control.
#[test]
fn pruning_does_not_change_any_value() {
    let mut session = prune_session();
    let wide = row_text(session.run("SELECT * FROM t ORDER BY a"));
    for (offset, name) in ["a", "b", "c", "d"].iter().enumerate() {
        let narrow = row_text(session.run(&format!("SELECT {name} FROM t ORDER BY a")));
        let expected: Vec<Vec<String>> = wide.iter().map(|row| vec![row[offset].clone()]).collect();
        assert_eq!(
            narrow, expected,
            "column {name} read differently when pruned"
        );
    }
    // Reordered and repeated references still line up with the wide read.
    let narrow = row_text(session.run("SELECT d, a, d FROM t ORDER BY a"));
    let expected: Vec<Vec<String>> = wide
        .iter()
        .map(|row| vec![row[3].clone(), row[0].clone(), row[3].clone()])
        .collect();
    assert_eq!(narrow, expected);
}

/// The gate refuses every wide shape, and each still returns the correct
/// full-width answer. A refusal is visible as "all four columns decoded".
#[test]
fn the_gate_refuses_every_wide_shape_and_they_still_answer_correctly() {
    let mut session = prune_session();
    let all = BTreeSet::from([A, B, C, D]);

    // A wildcard names every column.
    let (rows, decoded) = rows_and_decoded(&mut session, "SELECT * FROM t WHERE a = 1");
    assert_eq!(rows, vec![vec!["1", "10", "100", "1000"]]);
    assert_eq!(decoded, all);

    // A join: two tables in the scope, so the offsets the driver indexes by
    // are the concatenated ones and nothing may be renumbered.
    let (rows, decoded) = rows_and_decoded(
        &mut session,
        "SELECT t.c FROM t, s WHERE t.a = s.k ORDER BY t.a",
    );
    assert_eq!(rows, vec![vec!["100"], vec!["200"]]);
    assert!(decoded.is_superset(&all), "join decoded {decoded:?}");

    // A correlated subquery -- which is how a correlated reference to this
    // scope is refused, since one can only occur inside a subquery. (An
    // UNCORRELATED subquery is not in this list: the driver folds it into a
    // literal before pruning is offered, so what the gate sees is a genuine
    // plain single-table scan and pruning it is correct -- covered by
    // `an_uncorrelated_subquery_is_folded_away_before_the_gate_sees_it`.)
    let (rows, decoded) = rows_and_decoded(
        &mut session,
        "SELECT c FROM t WHERE EXISTS (SELECT 1 FROM s WHERE s.k = t.a) ORDER BY a",
    );
    assert_eq!(rows, vec![vec!["100"], vec!["200"]]);
    assert!(
        decoded.is_superset(&all),
        "correlated subquery decoded {decoded:?}"
    );

    // A window function.
    let (rows, decoded) =
        rows_and_decoded(&mut session, "SELECT ROW_NUMBER() OVER (ORDER BY b) FROM t");
    assert_eq!(rows, vec![vec!["1"], vec!["2"], vec!["3"]]);
    assert_eq!(decoded, all);

    // A `WITH` clause: the outer query reads a materialized CTE, not a base
    // table, so the gate refuses it. The CTE's own body is a separate,
    // ordinary single-table `SELECT c FROM t`, which is why `c` alone is
    // decoded -- pruning the body is exactly right, and nothing about the
    // outer query was renumbered.
    let (rows, decoded) =
        rows_and_decoded(&mut session, "WITH w AS (SELECT c FROM t) SELECT c FROM w");
    assert_eq!(rows, vec![vec!["100"], vec!["200"], vec!["300"]]);
    assert_eq!(decoded, BTreeSet::from([C]));

    // A derived table: the outer query's FROM is not a base table, and the
    // inner one's wildcard names every column.
    let (rows, decoded) = rows_and_decoded(&mut session, "SELECT c FROM (SELECT * FROM t) x");
    assert_eq!(rows, vec![vec!["100"], vec!["200"], vec!["300"]]);
    assert_eq!(decoded, all);
}

/// The Go differential for the pruned scan's operator info.
///
/// Captured from real TiDB (`pkg/executor/zz_dump_prune_test.go`, mock store,
/// `-tags=intest`): the `TableFullScan`'s access object and operator info are
/// **byte-identical** whether the query reads one column or all four --
///
/// ```text
/// explain select c from t where a > 1
///   TableFullScan_9 10000.00 cop[tikv] table:t  keep order:false, stats:pseudo
/// explain select * from t where a > 1
///   TableFullScan_5 10000.00 cop[tikv] table:t  keep order:false, stats:pseudo
/// ```
///
/// Pruning is a change to the scan's *schema*, which `EXPLAIN` never prints
/// for a `TableFullScan`; Go's only visible pruning artefact is a separate
/// coprocessor `Projection`, which this tier has no `cop[tikv]` task to
/// carry. So the correct behaviour here is that the plan text does not move
/// at all -- which is why pruning is applied after the trace records the
/// scan, and why this test pins both spellings to the same rows.
#[test]
fn pruning_leaves_the_plan_text_exactly_where_it_was() {
    let mut session = prune_session();
    let narrow = row_text(session.run("EXPLAIN SELECT c FROM t WHERE a > 1"));
    let wide = row_text(session.run("EXPLAIN SELECT * FROM t WHERE a > 1"));
    // The `Projection` prints the select list, which genuinely differs (Go's
    // does too). Every row below it -- the `Selection` and the scan -- must
    // be identical.
    assert_eq!(narrow[1..], wide[1..]);
    assert_eq!(
        narrow.last().unwrap(),
        &vec![
            "  └─TableFullScan_1".to_owned(),
            "10000.00".to_owned(),
            "root".to_owned(),
            "table:t".to_owned(),
            "keep order:false, stats:pseudo".to_owned(),
        ]
    );
}

/// An uncorrelated subquery is evaluated and folded into a literal *before*
/// the gate runs, so the statement the gate reads has no subquery left in it
/// and prunes like any other single-table scan. Recorded here because it is a
/// real interaction between two passes, not an accident of this test.
#[test]
fn an_uncorrelated_subquery_is_folded_away_before_the_gate_sees_it() {
    let mut session = prune_session();
    let (rows, decoded) = rows_and_decoded(
        &mut session,
        "SELECT c FROM t WHERE a IN (SELECT k FROM s) ORDER BY a",
    );
    assert_eq!(rows, vec![vec!["100"], vec!["200"]]);
    // `s`'s own scan is unpruned and shares the id space, so only `t`'s
    // untouched columns are asserted: `b` and `d` are never decoded.
    assert!(!decoded.contains(&D), "d decoded: {decoded:?}");
}

/// A statement whose reference set is every column prunes to a no-op: the
/// driver only offers the prune when it is strictly narrower, so nothing in
/// the scan or the scope changes.
#[test]
fn a_statement_that_reads_every_column_is_left_alone() {
    let mut session = prune_session();
    let (rows, decoded) = rows_and_decoded(&mut session, "SELECT a, b, c, d FROM t WHERE a = 2");
    assert_eq!(rows, vec![vec!["2", "20", "200", "2000"]]);
    assert_eq!(decoded, BTreeSet::from([A, B, C, D]));
}

/// `UPDATE`/`DELETE` address rows by handle and rewrite every column, so they
/// stay on the unchanged full-width scan -- and a pruned `SELECT` before and
/// after one still reads the current values.
#[test]
fn writes_are_unaffected_by_pruning() {
    let mut session = prune_session();
    assert_eq!(
        row_text(session.run("SELECT d FROM t WHERE a = 2")),
        vec![vec!["2000"]]
    );
    session.run("UPDATE t SET d = 42 WHERE a = 2").unwrap();
    assert_eq!(
        row_text(session.run("SELECT d FROM t WHERE a = 2")),
        vec![vec!["42"]]
    );
    assert_eq!(
        row_text(session.run("SELECT * FROM t WHERE a = 2")),
        vec![vec!["2", "20", "200", "42"]]
    );
    session.run("DELETE FROM t WHERE b = 20").unwrap();
    assert_eq!(
        row_text(session.run("SELECT c FROM t ORDER BY a")),
        vec![vec!["100"], vec!["300"]]
    );
}

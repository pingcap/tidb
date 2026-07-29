//! Column pruning over a single-table scan and over a two-base-table join:
//! what each scan is asked to decode, and that every shape outside the slice
//! keeps the unchanged full-width path.
//!
//! The decode set is read straight from the row codec's own input through
//! [`tidb_executor::kv_table::capture_decoded_column_ids`], so "column `d` is
//! never decoded" is measured rather than argued.
#![cfg(test)]

use std::collections::BTreeSet;

use tidb_executor::kv_table::capture_decoded_column_ids;

use crate::tests_support::row_text;
use crate::Session;

/// `t(a,b,c,d)` with ids 1..4 and three rows, `s` for the subquery shapes,
/// and `j(k,m,n,p)` -- also ids 1..4 -- for the join shapes.
///
/// `j` deliberately matches `t`'s arity: the decode probe reports column
/// *ids*, which every table numbers from 1, so a join's captured set is the
/// union of the two tables' ids. Two tables of the same width make an ABSENT
/// id mean "decoded by neither side", which is what the join assertions
/// below rely on.
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
        .run("CREATE TABLE j (k BIGINT, m BIGINT, n BIGINT, p BIGINT)")
        .unwrap();
    session
        .run("INSERT INTO j VALUES (1,11,111,1111),(2,22,222,2222)")
        .unwrap();
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

/// The join widening: each side of a two-base-table join decodes only the
/// columns the statement -- its `ON` and `WHERE` included -- reads from it.
///
/// `t` and `j` are both four columns wide, so an id missing from the captured
/// set was decoded by neither table (see [`prune_session`]).
#[test]
fn each_side_of_a_two_table_join_decodes_only_what_it_contributes() {
    let mut session = prune_session();

    // `ON` form: `t` keeps a (the ON) and d (the field), `j` keeps k (the ON)
    // and p (the field). Ids 2 and 3 -- b, c, m, n -- are never read.
    let (rows, decoded) = rows_and_decoded(
        &mut session,
        "SELECT t.d, j.p FROM t JOIN j ON t.a = j.k ORDER BY t.a",
    );
    assert_eq!(rows, vec![vec!["1000", "1111"], vec!["2000", "2222"]]);
    assert_eq!(decoded, BTreeSet::from([A, D]));

    // Comma form with the join predicate in the `WHERE`: the parser wraps the
    // left relation, and the gate peels the wrapper, so this prunes exactly
    // like the `ON` spelling above. `t` keeps a and b, `j` keeps k alone.
    let (rows, decoded) = rows_and_decoded(
        &mut session,
        "SELECT t.b FROM t, j WHERE t.a = j.k ORDER BY t.a",
    );
    assert_eq!(rows, vec![vec!["10"], vec!["20"]]);
    assert_eq!(decoded, BTreeSet::from([A, B]));

    // An outer join prunes the same way: the null-extended side is narrowed,
    // not dropped, so the unmatched row still produces its NULLs.
    let (rows, decoded) = rows_and_decoded(
        &mut session,
        "SELECT t.c, j.n FROM t LEFT JOIN j ON t.a = j.k ORDER BY t.a",
    );
    assert_eq!(
        rows,
        vec![vec!["100", "111"], vec!["200", "222"], vec!["300", "NULL"]]
    );
    assert_eq!(decoded, BTreeSet::from([A, C]));

    // Every clause feeds the kept set on both sides at once: `ON` (a, k),
    // `WHERE` (m), `ORDER BY` (b), select list (c).
    let (rows, decoded) = rows_and_decoded(
        &mut session,
        "SELECT t.c FROM t JOIN j ON t.a = j.k WHERE j.m > 11 ORDER BY t.b",
    );
    assert_eq!(rows, vec![vec!["200"]]);
    assert_eq!(decoded, BTreeSet::from([A, B, C]));

    // A join under an aggregate prunes too -- the aggregate path reads the
    // same already-narrowed scope.
    let (rows, decoded) = rows_and_decoded(
        &mut session,
        "SELECT t.b, SUM(j.p) FROM t JOIN j ON t.a = j.k GROUP BY t.b ORDER BY t.b",
    );
    assert_eq!(rows, vec![vec!["10", "1111"], vec!["20", "2222"]]);
    assert_eq!(decoded, BTreeSet::from([A, B, D]));
}

/// The pruned join must answer exactly what the full-width join answers.
/// `SELECT *` is refused by the gate, so it is the wide control.
#[test]
fn pruning_a_join_does_not_change_any_value() {
    let mut session = prune_session();
    let wide = row_text(session.run("SELECT * FROM t JOIN j ON t.a = j.k ORDER BY t.a"));
    // Offsets into the concatenated row: t's a,b,c,d then j's k,m,n,p.
    for (offset, name) in ["t.a", "t.b", "t.c", "t.d", "j.k", "j.m", "j.n", "j.p"]
        .iter()
        .enumerate()
    {
        let narrow = row_text(session.run(&format!(
            "SELECT {name} FROM t JOIN j ON t.a = j.k ORDER BY t.a"
        )));
        let expected: Vec<Vec<String>> = wide.iter().map(|row| vec![row[offset].clone()]).collect();
        assert_eq!(
            narrow, expected,
            "column {name} read differently when pruned"
        );
    }
    // A field from each side, reordered and repeated, still lines up.
    let narrow =
        row_text(session.run("SELECT j.p, t.b, j.p FROM t JOIN j ON t.a = j.k ORDER BY t.a"));
    let expected: Vec<Vec<String>> = wide
        .iter()
        .map(|row| vec![row[7].clone(), row[1].clone(), row[7].clone()])
        .collect();
    assert_eq!(narrow, expected);
    // The outer join's null-extended rows too.
    let wide = row_text(session.run("SELECT * FROM t LEFT JOIN j ON t.a = j.k ORDER BY t.a"));
    let narrow =
        row_text(session.run("SELECT t.d, j.n FROM t LEFT JOIN j ON t.a = j.k ORDER BY t.a"));
    let expected: Vec<Vec<String>> = wide
        .iter()
        .map(|row| vec![row[3].clone(), row[6].clone()])
        .collect();
    assert_eq!(narrow, expected);
}

/// The join shapes the widened gate still refuses, and why each one has to
/// stay refused. Every one of them still answers correctly.
#[test]
fn the_join_gate_refuses_the_shapes_it_cannot_see_all_the_references_of() {
    let mut session = prune_session();
    let all = BTreeSet::from([A, B, C, D]);

    // THREE tables. `build_join` recurses, so the inner `t JOIN j` would be
    // offered a prune with the OUTER join's `ON` -- which names `t.b` here --
    // nowhere in view. Dropping `t.b` there would be a silently wrong answer,
    // so the whole shape is refused.
    let (rows, decoded) = rows_and_decoded(
        &mut session,
        "SELECT t.c FROM t JOIN j ON t.a = j.k JOIN s ON s.v = t.b ORDER BY t.a",
    );
    assert_eq!(rows, Vec::<Vec<String>>::new());
    assert!(
        decoded.is_superset(&all),
        "three-way join decoded {decoded:?}"
    );

    // Three tables in the COMMA spelling, which nests differently: here the
    // inner `t, j` wears no `ON` at all, so nothing about the node shape
    // marks it as a join to peel past. The scope width is what refuses it --
    // `tables[0]`/`tables[1]` would name `t` and `j` while the executor on
    // the left is the whole `t, j` join, and the split point would be applied
    // to `s`.
    let (rows, decoded) = rows_and_decoded(
        &mut session,
        "SELECT t.c FROM t, j, s WHERE t.a = j.k AND t.b = 10",
    );
    assert_eq!(rows, vec![vec!["100"], vec!["100"]]);
    assert!(
        decoded.is_superset(&all),
        "three-way comma join decoded {decoded:?}"
    );

    // A derived table on one side: not a base table, so the side has no
    // column list this gate may narrow. The subquery names only `k` and `p`,
    // so `t` staying full width is what puts ids 2 and 3 in the set.
    let (rows, decoded) = rows_and_decoded(
        &mut session,
        "SELECT t.c FROM t JOIN (SELECT k, p FROM j) x ON t.a = x.k ORDER BY t.a",
    );
    assert_eq!(rows, vec![vec!["100"], vec!["200"]]);
    assert_eq!(decoded, all, "derived side decoded {decoded:?}");

    // A wildcard names every column of both sides.
    let (rows, decoded) = rows_and_decoded(&mut session, "SELECT * FROM t JOIN j ON t.a = j.k");
    assert_eq!(rows.len(), 2);
    assert_eq!(decoded, all);

    // A one-sided wildcard is still a wildcard.
    let (rows, decoded) = rows_and_decoded(&mut session, "SELECT j.* FROM t JOIN j ON t.a = j.k");
    assert_eq!(rows.len(), 2);
    assert_eq!(decoded, all);

    // A correlated subquery inside a join: the correlated reference lives in
    // a scope this walk never enters, so the whole statement is refused --
    // the same rule as the single-table case.
    let (rows, decoded) = rows_and_decoded(
        &mut session,
        "SELECT t.c FROM t JOIN j ON t.a = j.k WHERE EXISTS (SELECT 1 FROM s WHERE s.k = t.b) ORDER BY t.a",
    );
    assert_eq!(rows, Vec::<Vec<String>>::new());
    assert!(
        decoded.is_superset(&all),
        "correlated join decoded {decoded:?}"
    );

    // A window function over the join.
    let (rows, decoded) = rows_and_decoded(
        &mut session,
        "SELECT ROW_NUMBER() OVER (ORDER BY t.b) FROM t JOIN j ON t.a = j.k",
    );
    assert_eq!(rows, vec![vec!["1"], vec!["2"]]);
    assert_eq!(decoded, all);

    // A side no clause mentions keeps its full width: the scan refuses an
    // empty prune, because a zero-column row is not a shape any source here
    // emits. The two sides answer independently, so the cross join's `j`
    // stays wide while `t` still narrows to `c` -- which is why the union is
    // the full set here even though half the join really was pruned.
    let (rows, decoded) = rows_and_decoded(&mut session, "SELECT t.c FROM t, j ORDER BY t.c");
    assert_eq!(rows.len(), 6);
    assert_eq!(decoded, all);
}

/// The Go differential for the join's plan text.
///
/// Captured from real TiDB (`pkg/executor/zz_dump_prunewide_test.go`, mock
/// store, `-tags=intest`): the narrow and wide spellings produce plans that
/// are byte-identical **down to the operator ids** --
///
/// ```text
/// explain select t.d, j.p from t join j on t.a = j.k
/// explain select * from t join j on t.a = j.k
///   both:
///   HashJoin_9 12487.50 root  inner join, equal:[eq(test.t.a, test.j.k)]
///   ├─TableReader_28(Build) 9990.00 root  data:Selection_27
///   │ └─Selection_27 9990.00 cop[tikv]  not(isnull(test.j.k))
///   │   └─TableFullScan_26 10000.00 cop[tikv] table:j keep order:false, stats:pseudo
///   └─TableReader_25(Probe) 9990.00 root  data:Selection_24
///     └─Selection_24 9990.00 cop[tikv]  not(isnull(test.t.a))
///       └─TableFullScan_23 10000.00 cop[tikv] table:t keep order:false, stats:pseudo
/// ```
///
/// (`explain select t.b from t, j where t.a = j.k` yields that same plan
/// again, which is the Go-side confirmation that the comma spelling and the
/// `ON` spelling are one shape.)
///
/// Same reason as the single-table case: pruning changes a scan's SCHEMA,
/// which `EXPLAIN` never prints for a `TableFullScan`, and `equal:[...]`
/// names columns rather than offsets.
///
/// Pinned here because the join's operator info is rendered from the (now
/// narrowed) scope, so a qualification that shifted under narrowing would
/// show up in exactly this string.
#[test]
fn pruning_a_join_leaves_the_plan_text_exactly_where_it_was() {
    let mut session = prune_session();
    let narrow = row_text(session.run("EXPLAIN SELECT t.d, j.p FROM t JOIN j ON t.a = j.k"));
    let wide = row_text(session.run("EXPLAIN SELECT * FROM t JOIN j ON t.a = j.k"));
    // Only the `Projection`'s own select list differs, as in Go.
    assert_eq!(narrow[1..], wide[1..]);
    assert!(
        narrow[1][4].contains("equal:[eq(test.t.a, test.j.k)]"),
        "join operator info moved: {:?}",
        narrow[1]
    );
}

/// Aggregates were already inside the narrow slice -- the gate walks
/// `GROUP BY` and `HAVING`, and `Expr::Aggregate` recurses into its arguments
/// like any other call -- so this pins the shapes rather than widening
/// anything.
#[test]
fn aggregate_shapes_prune_through_their_arguments() {
    let mut session = prune_session();

    // No GROUP BY at all: the aggregate's argument is the only reference.
    let (rows, decoded) = rows_and_decoded(&mut session, "SELECT SUM(c) FROM t");
    assert_eq!(rows, vec![vec!["600"]]);
    assert_eq!(decoded, BTreeSet::from([C]));

    // DISTINCT inside the aggregate, and a second aggregate over another
    // column.
    let (rows, decoded) = rows_and_decoded(&mut session, "SELECT COUNT(DISTINCT b), MAX(d) FROM t");
    assert_eq!(rows, vec![vec!["3", "3000"]]);
    assert_eq!(decoded, BTreeSet::from([B, D]));

    // GROUP_CONCAT's own ORDER BY is walked alongside its arguments.
    let (rows, decoded) =
        rows_and_decoded(&mut session, "SELECT GROUP_CONCAT(a ORDER BY d) FROM t");
    assert_eq!(rows, vec![vec!["1,2,3"]]);
    assert_eq!(decoded, BTreeSet::from([A, D]));

    // `COUNT(*)` names no column, so the kept set is empty and the scan
    // refuses the prune -- the full-width path answers it.
    let (rows, decoded) = rows_and_decoded(&mut session, "SELECT COUNT(*) FROM t");
    assert_eq!(rows, vec![vec!["3"]]);
    assert_eq!(decoded, BTreeSet::from([A, B, C, D]));
}

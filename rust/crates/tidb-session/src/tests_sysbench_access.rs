#![cfg(test)]

//! The four query shapes one sysbench `oltp_read_only` transaction runs, as
//! `EXPLAIN` plans, against sysbench's own `sbtest1` schema.
//!
//! Every "Go prints" line below is a verbatim row from a
//! `testkit.CreateMockStore` capture of real TiDB's `EXPLAIN` on this exact
//! schema with no analyzed statistics -- the same oracle
//! `tidb_executor::explain`'s module doc names. The capture ran
//! `explain select ...` for each shape below; its output is quoted at each
//! assertion so a plan that regresses is compared against Go rather than
//! against whatever this tier last printed.
//!
//! Generated operator IDs are not part of the contract. What IS compared is
//! the ACCESS PATH: which source the plan reads through, the range it reads,
//! and the row estimate that choice was made on. Any remaining difference
//! from the captured Go plan is called out at the assertion that retains it.
//!
//! # What makes the three range shapes work
//!
//! `tidb_executor::handle_range` builds the table path's ranges, and the
//! driver offers them to the whole-table source already installed rather than
//! replacing it -- Go's `TableRangeScan` is the same `PhysicalTableScan` with
//! ranges, so there is one scan executor and not two. The builder is the
//! index detacher run over the primary key as a one-column index, because
//! Go's `ranger.BuildTableRange` IS `buildColumnRange` with `tableRange=true`
//! over one column: no second range algebra.
//!
//! The row ESTIMATE decides whether the range beats the index full scan. It
//! is `path.CountAfterAccess`, estimated over the ranges AFTER
//! `points2TableRanges` has replaced their open endpoints -- which is what
//! makes Go's estimator take its signed arm for a table path.
//! `tidb_executor::handle_range`'s doc has the instrumented capture of Go's
//! own dispatch, and names the one further step (`adjustCountAfterAccess`)
//! that is not ported and the two corpus shapes it moves.
//!
//! # The captured Go corpus
//!
//! Every row below is from the same capture, as
//! `estRows` then `range:`. It is recorded here because it is the acceptance
//! set for the range builder, and because a capture is cheap to read and
//! expensive to re-take.
//!
//! ```text
//! id between 100 and 199        99.00     [100,199]
//! id > 199                    3333.33     (199,+inf]
//! id >= 199                   3333.33     [199,+inf]
//! id < 5                      3333.33     [-inf,5)
//! id < 0                      3333.33     [-inf,0)
//! id < -1                    10000.00     [-inf,-1)
//! id <= -1                   10000.00     [-inf,-1]
//! id < -100000               3333.33     [-inf,-100000)
//! id > -5 and id < 5            10.00     (-5,5)
//! id > 2 and id < 99            97.00     (2,99)
//! id <> 0 and id < 3          3336.33     [-inf,0), (0,3)
//! id not between 0 and 200    6666.67     [-inf,0), (200,+inf]
//! id between 100 and 199 and c = 'c150'
//!                               99.00     [100,199]   + cop Selection
//! id > 0 and k = 4              33.33     IndexLookUp over k_1 + row probe
//! id in (-1, 2, 150)                      Batch_Point_Get handle:[-1 2 150]
//! id = 150 or id = 1                      Batch_Point_Get handle:[1 150]
//! id > 100 and id < 100                   TableDual rows:0
//! k > 5                      10000.00     TableFullScan (no handle bound)
//! ```
//!
//! Note what the corpus settles: Go PRESERVES endpoint exclusivity in the
//! printed range (`(199,+inf]`, `[-inf,5)`), so a builder that normalizes an
//! integer range to inclusive bounds would print text Go never prints.

use crate::tests_support::*;
use crate::*;

/// sysbench's `sbtest1`: a clustered signed-`BIGINT` handle plus a secondary
/// index on `k` that none of the `WHERE` clauses below mentions.
fn sbtest1() -> Session {
    let mut session = Session::new();
    session
        .run(
            "CREATE TABLE sbtest1 (id BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL DEFAULT 0, \
             c CHAR(120) NOT NULL DEFAULT '', pad CHAR(60) NOT NULL DEFAULT '', INDEX k_1(k))",
        )
        .unwrap();
    session
}

/// The access row (`id`, `estRows`, `access object`, `operator info`) of a
/// plan's deepest node -- the source the whole shape reads through.
fn source_row(session: &mut Session, sql: &str) -> Vec<String> {
    let rows = row_text(session.run(sql));
    let last = rows.last().expect("a plan has at least one row").clone();
    // The tree-drawing prefix is display, not identity.
    let name = last[0]
        .rsplit(['─', ' '])
        .next()
        .expect("a drawn name is nonempty")
        .to_owned();
    vec![name, last[1].clone(), last[3].clone(), last[4].clone()]
}

/// Shape 1 of 4: the point read. Go prints one `Point_Get_1 | 1.00 | root |
/// table:sbtest1 | handle:100`, and so does this tier's source row.
#[test]
fn point_select_reads_one_handle() {
    let mut session = sbtest1();
    assert_eq!(
        source_row(&mut session, "EXPLAIN SELECT c FROM sbtest1 WHERE id = 100"),
        vec![
            "Point_Get_1".to_owned(),
            "1.00".to_owned(),
            "table:sbtest1".to_owned(),
            "handle:100".to_owned(),
        ]
    );
}

/// Shape 2 of 4: a range over the clustered handle.
///
/// Go prints
/// `TableRangeScan_8 | 99.00 | cop[tikv] | table:sbtest1 | range:[100,199],
/// keep order:false, stats:pseudo`.
///
/// The 99.00 is `getPseudoRowCountBySignedIntRanges`
/// (`pkg/planner/cardinality/pseudo.go`): a bounded non-point range estimates
/// `10000 / pseudoBetweenRate` = 250, then clamps to the range's own width
/// `high - low` = 99.
#[test]
fn range_select_reads_only_the_handle_range() {
    let mut session = sbtest1();
    assert_eq!(
        source_row(
            &mut session,
            "EXPLAIN SELECT c FROM sbtest1 WHERE id BETWEEN 100 AND 199"
        ),
        vec![
            "TableRangeScan_1".to_owned(),
            "99.00".to_owned(),
            "table:sbtest1".to_owned(),
            "range:[100,199], keep order:false, stats:pseudo".to_owned(),
        ]
    );
}

/// Shape 3 of 4: the aggregate over the same range.
///
/// The `WHERE` never mentions `k`, so reading the `k_1` index end to end is a
/// full scan of an index the predicate cannot narrow. Go reads the handle
/// range instead: `TableRangeScan_16 | 99.00 | cop[tikv] | table:sbtest1 |
/// range:[100,199], keep order:false, stats:pseudo`.
#[test]
fn aggregate_over_a_range_does_not_scan_an_unrelated_index() {
    let mut session = sbtest1();
    assert_eq!(
        source_row(
            &mut session,
            "EXPLAIN SELECT SUM(k) FROM sbtest1 WHERE id BETWEEN 100 AND 199"
        ),
        vec![
            "TableRangeScan_1".to_owned(),
            "99.00".to_owned(),
            "table:sbtest1".to_owned(),
            "range:[100,199], keep order:false, stats:pseudo".to_owned(),
        ]
    );
}

/// Shape 4 of 4, and its `DISTINCT` sibling: an `ORDER BY` above the same
/// range. Go's source row is the same `TableRangeScan ... range:[100,199]`
/// for both; the ordering and dedup are stages above the read and do not
/// change which rows the source has to produce.
#[test]
fn ordered_and_distinct_selects_read_the_same_range() {
    let mut session = sbtest1();
    let expected = vec![
        "TableRangeScan_1".to_owned(),
        "99.00".to_owned(),
        "table:sbtest1".to_owned(),
        "range:[100,199], keep order:false, stats:pseudo".to_owned(),
    ];
    assert_eq!(
        source_row(
            &mut session,
            "EXPLAIN SELECT c FROM sbtest1 WHERE id BETWEEN 100 AND 199 ORDER BY c"
        ),
        expected
    );
    assert_eq!(
        source_row(
            &mut session,
            "EXPLAIN SELECT DISTINCT c FROM sbtest1 WHERE id BETWEEN 100 AND 199 ORDER BY c"
        ),
        expected
    );
}

/// The rows a handle range returns are the rows the full scan returned.
///
/// A range that reads fewer rows than the `WHERE` admits is a silent wrong
/// answer, which is worse than the full scan it replaced. Every shape the
/// ranger accepts is checked here against the same statement run over a table
/// whose rows are known.
#[test]
fn narrowed_ranges_return_the_same_rows_as_a_full_scan() {
    let mut session = sbtest1();
    for id in [-3i64, -1, 0, 1, 2, 3, 98, 99, 100, 150, 199, 200, 201] {
        session
            .run(&format!(
                "INSERT INTO sbtest1 (id, k, c, pad) VALUES ({id}, {}, 'c{id}', 'p')",
                id * 2
            ))
            .unwrap();
    }
    for (predicate, expected) in [
        ("id BETWEEN 100 AND 199", vec!["c100", "c150", "c199"]),
        ("id NOT BETWEEN 0 AND 200", vec!["c-3", "c-1", "c201"]),
        ("id > 199", vec!["c200", "c201"]),
        ("id >= 199", vec!["c199", "c200", "c201"]),
        ("id < -1", vec!["c-3"]),
        ("id <= -1", vec!["c-3", "c-1"]),
        ("id IN (-1, 2, 150)", vec!["c-1", "c2", "c150"]),
        ("id <> 0 AND id < 3", vec!["c-3", "c-1", "c1", "c2"]),
        ("id > 2 AND id < 99", vec!["c3", "c98"]),
        ("id > 100 AND id < 100", vec![]),
        ("id = 150 OR id = 1", vec!["c1", "c150"]),
        ("id > 0 AND k = 4", vec!["c2"]),
        ("id BETWEEN 100 AND 199 AND c = 'c150'", vec!["c150"]),
    ] {
        let sql = format!("SELECT c FROM sbtest1 WHERE {predicate} ORDER BY id");
        let rows: Vec<String> = row_text(session.run(&sql))
            .into_iter()
            .map(|row| row[0].clone())
            .collect();
        assert_eq!(rows, expected, "rows changed for `{predicate}`");
    }
}

/// The RECORDS a handle range actually reads, from `EXPLAIN ANALYZE`'s
/// `actRows` -- the receipt no plan assertion can give.
///
/// A plan may say `TableRangeScan range:[100,199]` and still walk the whole
/// table underneath: the `WHERE` above the source would filter the surplus,
/// the answer would be right, and every assertion about the printed tree
/// would pass while the read that the range exists to avoid still happened.
/// That is exactly the shape of a performance fix that is not one, so it is
/// asserted separately here, against a table whose rows are known.
///
/// The scan's `actRows` is rows READ before any filter, so a narrowed scan
/// reports the records inside its ranges and a full scan reports all
/// thirteen.
#[test]
fn a_handle_range_reads_only_the_records_inside_it() {
    let mut session = sbtest1();
    for id in [-3i64, -1, 0, 1, 2, 3, 98, 99, 100, 150, 199, 200, 201] {
        session
            .run(&format!(
                "INSERT INTO sbtest1 (id, k, c, pad) VALUES ({id}, {}, 'c{id}', 'p')",
                id * 2
            ))
            .unwrap();
    }
    // (predicate, records the scan must read, rows the statement returns)
    for (predicate, read, returned) in [
        // Three of the thirteen lie in [100,199]; a full scan would say 13.
        ("id BETWEEN 100 AND 199", "3", "3"),
        // Two disjoint ranges, so the two-range cursor is exercised: a scan
        // that collapsed them to their SPAN would read all thirteen.
        ("id NOT BETWEEN 0 AND 200", "3", "3"),
        ("id > 199", "2", "2"),
        ("id <= -1", "2", "2"),
        // Three point ranges out of thirteen records.
        ("id IN (-1, 2, 150)", "3", "3"),
        // The contradictory `WHERE`: reads NOTHING, which is the one
        // direction an empty range list must never be read as "everything".
        ("id > 100 AND id < 100", "0", "0"),
        // A residual condition the range did not consume still reads the
        // whole range and filters above it.
        ("id BETWEEN 100 AND 199 AND c = 'c150'", "3", "1"),
        // No handle bound: the whole table, unchanged. `k` is `id * 2`, so
        // `k > 5` keeps the eight records with `id >= 3`.
        ("k > 5", "13", "8"),
    ] {
        let rows = row_text(session.run(&format!(
            "EXPLAIN ANALYZE SELECT c FROM sbtest1 WHERE {predicate}"
        )));
        let scan = rows.last().expect("a plan has a source row");
        assert_eq!(
            scan[2], read,
            "records read changed for `{predicate}` (source: {})",
            scan[0]
        );
        assert_eq!(
            rows[0][2], returned,
            "rows returned changed for `{predicate}`"
        );
    }
}

/// The whole eighteen-shape corpus above, as `operator | estRows | range`.
///
/// The four shapes sysbench itself runs are asserted individually above; this
/// is the acceptance set for the RANGE BUILDER and the estimator behind it,
/// which the four alone do not exercise -- the open bounds, the multi-range
/// `OR`/`<>` walks, the two `-1` shapes that separate Go's two estimator arms,
/// and the shapes that must NOT become a range scan at all.
#[test]
fn the_handle_range_corpus_matches_go() {
    let mut session = sbtest1();
    for (predicate, operator, est_rows, info) in [
        (
            "id BETWEEN 100 AND 199",
            "TableRangeScan",
            "99.00",
            "range:[100,199]",
        ),
        ("id > 199", "TableRangeScan", "3333.33", "range:(199,+inf]"),
        ("id >= 199", "TableRangeScan", "3333.33", "range:[199,+inf]"),
        ("id < 5", "TableRangeScan", "3333.33", "range:[-inf,5)"),
        ("id < 0", "TableRangeScan", "3333.33", "range:[-inf,0)"),
        // Go's `CountAfterAccess` here starts at 3333.33; what lifts it is
        // `adjustCountAfterAccess`,
        // which raises a path's estimate to `ds.StatsInfo().RowCount /
        // SelectionFactor` -- and `RowCount` reaches 10000 only because
        // `Selectivity` estimates the UNCONVERTED range, whose `-inf` low
        // makes the estimator read `-1`'s bits as `u64::MAX` and call
        // `[-inf,-1)` the whole domain. The access-path estimator now applies
        // that same lower bound, so both values match Go's recorded 10000.00.
        ("id < -1", "TableRangeScan", "10000.00", "range:[-inf,-1)"),
        ("id <= -1", "TableRangeScan", "10000.00", "range:[-inf,-1]"),
        (
            "id < -100000",
            "TableRangeScan",
            "3333.33",
            "range:[-inf,-100000)",
        ),
        (
            "id > -5 AND id < 5",
            "TableRangeScan",
            "10.00",
            "range:(-5,5)",
        ),
        (
            "id > 2 AND id < 99",
            "TableRangeScan",
            "97.00",
            "range:(2,99)",
        ),
        (
            "id <> 0 AND id < 3",
            "TableRangeScan",
            "3336.33",
            "range:[-inf,0), (0,3)",
        ),
        (
            "id NOT BETWEEN 0 AND 200",
            "TableRangeScan",
            "6666.67",
            "range:[-inf,0), (200,+inf]",
        ),
        // A residual condition on a non-handle column narrows nothing here:
        // Go keeps the same range and puts a cop `Selection` above it.
        (
            "id BETWEEN 100 AND 199 AND c = 'c150'",
            "TableRangeScan",
            "99.00",
            "range:[100,199]",
        ),
        // CLASSIFIED DIVERGENCE, in the direction of Go rather than away from
        // it. Go settles a `WHERE` that PINS handles before costing and
        // prints `Batch_Point_Get ... handle:[-1 2 150]`; this tier's
        // `try_batch_point_get` does not claim these two shapes, so before
        // the handle range they fell all the way through to a
        // `TableFullScan` over 10000 rows. They now read the three (and two)
        // point ranges the handles name -- the same records Go's batch point
        // get reads, through a scan node instead of a point node. Closing the
        // rest is `try_batch_point_get`'s gate, which this unit did not
        // touch.
        (
            "id IN (-1, 2, 150)",
            "TableRangeScan",
            "3.00",
            "range:[-1,-1], [2,2], [150,150]",
        ),
        (
            "id = 150 OR id = 1",
            "TableRangeScan",
            "2.00",
            "range:[1,1], [150,150]",
        ),
        // A `WHERE` no handle satisfies reads NOTHING, which is the one
        // direction a range must never get wrong -- and Go names that a
        // `TableDual`, not a scan over an empty range list
        // (`find_best_task.go`: `if len(path.Ranges) == 0`). Captured:
        // `explain select * from t where id > 100 and id < 100` ->
        // `TableDual_5 | 1.00 | rows:0`. The OPERATOR now matches; the estRows
        // does not, because Go reaches ITS dual here through an earlier
        // always-false predicate rule (whose dual prints 1.00) rather than
        // through the empty-range short-circuit (whose dual prints 0.00, as it
        // does for `id is null`). This tier has only the latter, so it lands
        // on 0.00 for both -- one rule short, not one rule wrong.
        ("id > 100 AND id < 100", "TableDual", "0.00", "rows:0"),
        // No handle bound at all: still the whole table.
        ("k > 5", "TableFullScan", "10000.00", "keep order:false"),
    ] {
        let row = source_row(
            &mut session,
            &format!("EXPLAIN SELECT c FROM sbtest1 WHERE {predicate}"),
        );
        assert_eq!(
            (row[0].as_str(), row[1].as_str()),
            (format!("{operator}_1").as_str(), est_rows),
            "operator or estRows changed for `{predicate}` (info: {})",
            row[3]
        );
        assert!(
            row[3].starts_with(info),
            "range changed for `{predicate}`: expected a prefix of `{info}`, got `{}`",
            row[3]
        );
    }

    // Go chooses the same complete non-covering-index path for the mixed
    // predicate. A fresh pseudo-statistics capture from the Go server is:
    //
    // ```text
    // IndexLookUp                 33.33  root
    // |-IndexRangeScan(Build)     33.33  range:(4 0,4 +inf]
    // `-TableRowIDScan(Probe)     33.33
    // ```
    //
    // This tier retains a classified estimate/residual-selection difference
    // and prints 10.00, but the former source-only assertion was obsolete: a
    // lookup has two physical source children, and its deepest node is the
    // row probe rather than the index range. Pin the complete lookup shape so
    // the access choice and compound range remain visible.
    let rows = row_text(session.run("EXPLAIN SELECT c FROM sbtest1 WHERE id > 0 AND k = 4"));
    let lookup = rows
        .iter()
        .position(|row| row[0].contains("IndexLookUp_"))
        .expect("the mixed predicate uses a non-covering index lookup");
    let lookup_rows = &rows[lookup..lookup + 3];
    assert!(lookup_rows[0][0].contains("IndexLookUp_"));
    assert_eq!(lookup_rows[0][1], "10.00");
    assert!(lookup_rows[1][0].contains("IndexRangeScan_"));
    assert!(lookup_rows[1][0].ends_with("(Build)"));
    assert_eq!(lookup_rows[1][1], "10.00");
    assert!(lookup_rows[1][4].starts_with("range:(4 0,4 +inf]"));
    assert!(lookup_rows[2][0].contains("TableRowIDScan_"));
    assert!(lookup_rows[2][0].ends_with("(Probe)"));
    assert_eq!(lookup_rows[2][1], "10.00");
}

/// The sysbench WRITE shapes, as the source row of their plan.
///
/// Go plans a write's read from the same predicate, with the same FUNCTIONS,
/// as a read's: `tryUpdatePointPlan`/`tryDeletePointPlan` hand
/// `tryPointGetPlan` an `ast.SelectStmt` synthesized from the write's own
/// `TableRefs`/`Where`/`Order`/`Limit`, and only when that declines does the
/// ordinary path plan a `DataSource` whose table path gets its ranges from
/// `deriveTablePathStats` exactly as a `SELECT`'s does. This tier reuses BOTH
/// halves, and in that order: `tidb_executor::driver::access::try_point_get`
/// -- the same function a `SELECT` reaches -- then
/// `tidb_executor::handle_range`, the crate's single range algebra. So a
/// write whose `WHERE` pins a whole key reads `Point_Get`, a write the ranger
/// bounds reads `TableRangeScan`, and anything else still reads the table.
///
/// The remaining divergence from Go is that no INDEX path is offered to a
/// write, so a `WHERE` on a secondary index still scans; see
/// `tidb_executor::explain`'s divergence 8.
#[test]
fn the_sysbench_write_shapes_read_a_handle_range() {
    let mut session = sbtest1();
    for (sql, operator, est_rows, info) in [
        (
            "UPDATE sbtest1 SET k = k + 1 WHERE id = 500",
            "Point_Get",
            "1.00",
            "handle:500",
        ),
        (
            "UPDATE sbtest1 SET c = 'x' WHERE id = 500",
            "Point_Get",
            "1.00",
            "handle:500",
        ),
        (
            "DELETE FROM sbtest1 WHERE id = 500",
            "Point_Get",
            "1.00",
            "handle:500",
        ),
        // The FAST plan is decided from equalities alone (Go's
        // `getNameValuePairs` accepts `AND` of `column = constant` and
        // nothing else), so this spelling refuses it -- but the ordinary
        // path it falls back to converts a single-point handle range to a
        // `Point_Get` anyway (`find_best_task.go`'s `convertToPointGet`,
        // whose own comment names exactly this over-optimized family:
        // "`a>=1(?) and a<=1(?)` --> `a=1` --> PointGet(a=1)", which is why
        // such plans are barred from the plan cache there).
        (
            "UPDATE sbtest1 SET c = 'x' WHERE id BETWEEN 500 AND 500",
            "Point_Get",
            "1.00",
            "handle:500",
        ),
        // `ORDER BY` is `tryPointGetPlan`'s own refusal, and the ordinary
        // path again reads the point: Go builds the `Sort` above the read
        // (`buildUpdate`: `if update.Order != nil`), and a `Sort` demands no
        // order OF ITS CHILD, so `convertToPointGet`'s property check passes
        // and the SOURCE row is the point read either way.
        (
            "UPDATE sbtest1 SET c = 'x' WHERE id = 500 ORDER BY k",
            "Point_Get",
            "1.00",
            "handle:500",
        ),
        // `LIMIT 0` never reaches path selection at all: Go's `buildLimit`
        // (`logical_plan_builder.go`, `if offset+count == 0`) replaces the
        // whole read subtree with `LogicalTableDual{RowCount: 0}` at logical
        // build, so the write's child is a dual that reads nothing. (A
        // write's `LIMIT` is a row count with no offset -- the grammar
        // admits nothing else -- so `count == 0` is the whole of that half.)
        (
            "UPDATE sbtest1 SET c = 'x' WHERE id = 500 LIMIT 0",
            "TableDual",
            "0.00",
            "rows:0",
        ),
        // A write whose `WHERE` bounds no handle but names a secondary index
        // reads through that index, exactly as the `SELECT` form does: Go plans
        // a write's read from the same cost chooser, so `WHERE k = 500` takes
        // `k_1` (captured for the read side; the recorded corpus shows the same
        // for `delete from t1 where c2 = 1`).
        (
            "UPDATE sbtest1 SET c = 'x' WHERE k = 500",
            "IndexRangeScan",
            "10.00",
            "range:[500,500]",
        ),
        // No `WHERE` at all: also the whole table, which is every row the
        // statement names.
        (
            "UPDATE sbtest1 SET c = 'x'",
            "TableFullScan",
            "10000.00",
            "keep order:false",
        ),
    ] {
        let row = source_row(&mut session, &format!("EXPLAIN {sql}"));
        assert_eq!(
            (row[0].as_str(), row[1].as_str()),
            (format!("{operator}_1").as_str(), est_rows),
            "operator or estRows changed for `{sql}` (info: {})",
            row[3]
        );
        assert!(
            row[3].starts_with(info),
            "range changed for `{sql}`: expected a prefix of `{info}`, got `{}`",
            row[3]
        );
    }
}

/// A table of thirteen known records, the fixture the two write receipts use.
fn sbtest1_with_rows() -> Session {
    let mut session = sbtest1();
    for id in SEEDED_IDS {
        session
            .run(&format!(
                "INSERT INTO sbtest1 (id, k, c, pad) VALUES ({id}, {}, 'c{id}', 'p')",
                id * 2
            ))
            .unwrap();
    }
    session
}

/// The handles `sbtest1_with_rows` seeds. Chosen to straddle every bound the
/// predicates below name, so a range that is off by one record is visible.
const SEEDED_IDS: [i64; 13] = [-3, -1, 0, 1, 2, 3, 98, 99, 100, 150, 199, 200, 201];

/// The rows a narrowed write CHANGES are the rows the full scan changed.
///
/// This is `narrowed_ranges_return_the_same_rows_as_a_full_scan` for the write
/// path, and it is the load-bearing evidence: a range that reads fewer records
/// than the `WHERE` admits does not merely return a wrong answer here, it
/// MODIFIES THE WRONG ROWS and leaves the damage behind. The whole table is
/// compared afterwards, so a row the write should have left alone is caught as
/// loudly as a row it should have touched -- including the handles adjacent to
/// each range's own bounds.
#[test]
fn narrowed_writes_change_exactly_the_rows_a_full_scan_changed() {
    // (`WHERE`, the ids the statement must touch and no others)
    for (predicate, affected) in [
        ("id = 150", vec![150i64]),
        // A point range over a handle no record carries.
        ("id = 151", vec![]),
        ("id BETWEEN 100 AND 199", vec![100, 150, 199]),
        ("id NOT BETWEEN 0 AND 200", vec![-3, -1, 201]),
        ("id > 199", vec![200, 201]),
        ("id >= 199", vec![199, 200, 201]),
        ("id <= -1", vec![-3, -1]),
        ("id IN (-1, 2, 150)", vec![-1, 2, 150]),
        ("id <> 0 AND id < 3", vec![-3, -1, 1, 2]),
        ("id > 2 AND id < 99", vec![3, 98]),
        // The contradictory `WHERE`: the one direction an empty range list
        // must never be read as "every row".
        ("id > 100 AND id < 100", vec![]),
        ("id = 150 OR id = 1", vec![1, 150]),
        // A residual condition the range did not consume still filters.
        ("id BETWEEN 100 AND 199 AND c = 'c150'", vec![150]),
        // No handle bound: the whole table, unchanged in behaviour.
        ("k > 5", vec![3, 98, 99, 100, 150, 199, 200, 201]),
    ] {
        let mut session = sbtest1_with_rows();
        session
            .run(&format!("UPDATE sbtest1 SET pad = 'W' WHERE {predicate}"))
            .unwrap();
        let rows: Vec<(String, String)> =
            row_text(session.run("SELECT id, pad FROM sbtest1 ORDER BY id"))
                .into_iter()
                .map(|row| (row[0].clone(), row[1].clone()))
                .collect();
        let expected: Vec<(String, String)> = SEEDED_IDS
            .iter()
            .map(|id| {
                let pad = if affected.contains(id) { "W" } else { "p" };
                (id.to_string(), pad.to_owned())
            })
            .collect();
        assert_eq!(
            rows, expected,
            "UPDATE touched the wrong rows for `{predicate}`"
        );

        // The same predicate as a DELETE: the survivors are exactly the
        // complement of the same affected set.
        let mut session = sbtest1_with_rows();
        session
            .run(&format!("DELETE FROM sbtest1 WHERE {predicate}"))
            .unwrap();
        let survivors: Vec<String> = row_text(session.run("SELECT id FROM sbtest1 ORDER BY id"))
            .into_iter()
            .map(|row| row[0].clone())
            .collect();
        let expected: Vec<String> = SEEDED_IDS
            .iter()
            .filter(|id| !affected.contains(id))
            .map(i64::to_string)
            .collect();
        assert_eq!(
            survivors, expected,
            "DELETE removed the wrong rows for `{predicate}`"
        );
    }
}

/// Write shapes whose `WHERE` mentions the handle but which the ranger must
/// still get exactly right, or must decline entirely.
///
/// These are the interactions where "which feature runs first" decides the
/// answer: an alias renames the table the `WHERE` qualifies, `ORDER BY` +
/// `LIMIT` picks a SUBSET of the matched rows, and a subquery bound is not a
/// constant the ranger may fold. Each is checked by its EFFECT on the table,
/// so a range that admits the wrong records fails here regardless of what the
/// plan printed.
#[test]
fn narrowing_survives_aliases_ordering_limits_and_subqueries() {
    // An alias: the `WHERE` names `y.id`, and the ranger matches the handle
    // column by name ignoring the qualifier, as the read side does.
    let mut session = sbtest1_with_rows();
    session
        .run("UPDATE sbtest1 AS y SET pad = 'W' WHERE y.id BETWEEN 100 AND 199")
        .unwrap();
    assert_eq!(
        row_text(session.run("SELECT id FROM sbtest1 WHERE pad = 'W' ORDER BY id")),
        vec![
            vec!["100".to_owned()],
            vec!["150".to_owned()],
            vec!["199".to_owned()]
        ]
    );

    // `ORDER BY` + `LIMIT` over a narrowed read: the LIMIT picks from the
    // rows the `WHERE` matched, so the range must still deliver ALL of them
    // before the ordering chooses. Reading a prefix of the range would take
    // the wrong two rows here rather than fewer.
    let mut session = sbtest1_with_rows();
    session
        .run("UPDATE sbtest1 SET pad = 'W' WHERE id BETWEEN 100 AND 199 ORDER BY id DESC LIMIT 2")
        .unwrap();
    assert_eq!(
        row_text(session.run("SELECT id FROM sbtest1 WHERE pad = 'W' ORDER BY id")),
        vec![vec!["150".to_owned()], vec!["199".to_owned()]]
    );

    // A subquery bound is not a constant the ranger may fold. The DML source
    // plan evaluates it as an Apply over the immutable statement snapshot,
    // then updates exactly the selected maximum handle.
    let mut session = sbtest1_with_rows();
    session
        .run("UPDATE sbtest1 SET pad = 'W' WHERE id = (SELECT MAX(id) FROM sbtest1)")
        .unwrap();
    assert_eq!(
        row_text(session.run("SELECT id FROM sbtest1 WHERE pad = 'W' ORDER BY id")),
        vec![vec!["201".to_owned()]]
    );

    // A `WHERE` on the handle that is not a bound at all: the ranger declines
    // and the statement still reaches every row it names.
    let mut session = sbtest1_with_rows();
    session
        .run("UPDATE sbtest1 SET pad = 'W' WHERE id + 1 > 200")
        .unwrap();
    assert_eq!(
        row_text(session.run("SELECT id FROM sbtest1 WHERE pad = 'W' ORDER BY id")),
        vec![vec!["200".to_owned()], vec!["201".to_owned()]]
    );
}

/// The REQUEST KIND a write reads its records with, from the storage seam.
///
/// This is the receipt `actRows` cannot give, and the reason this unit
/// exists. A scan over the one-key range `[150,150]` and a lookup of key 150
/// both print `1` row read; they are not the same request. Go's seam names
/// the two kinds (`kv.Retriever.Get` -> `kv_get`,
/// `kv.Retriever.Iter` -> `kv_scan`), a real cluster's gRPC counters count
/// them separately, and the measured gap against Go was one `kv_scan` per
/// write where Go sends a `kv_get`. So the assertion is on the count of
/// ITERATORS OPENED, and for a point plan that count is zero: not a narrower
/// scan, no scan.
///
/// The range shapes are the control. They must keep opening iterators -- a
/// point plan that swallowed them would be reading one record where the
/// statement names several, which is the silent-wrong-answer direction.
#[test]
fn a_point_write_opens_no_scan_at_all() {
    // (`WHERE`, iterators the statement may open)
    for (predicate, scans) in [
        // The unit: a pinned key, read by key.
        ("id = 150", 0),
        // A key no record carries is still a lookup, not a scan.
        ("id = 151", 0),
        // The SAME single record, named as a range instead. Go plans a
        // `TableRangeScan` here too (`getNameValuePairs` takes equalities
        // only), and a range is scanned -- which is exactly what the point
        // plan avoids, spelled as the same one record.
        ("id BETWEEN 150 AND 150", 1),
        ("id BETWEEN 100 AND 199", 1),
        // Two disjoint ranges, one iterator each.
        ("id NOT BETWEEN 0 AND 200", 2),
        // No handle bound at all: the whole table, one iterator.
        ("k > 5", 1),
    ] {
        for statement in [
            format!("UPDATE sbtest1 SET pad = 'W' WHERE {predicate}"),
            format!("DELETE FROM sbtest1 WHERE {predicate}"),
        ] {
            let mut session = sbtest1_with_rows();
            let (result, ops) =
                tidb_executor::storage::capture_storage_ops(|| session.run(&statement));
            result.unwrap();
            assert_eq!(
                ops.scans, scans,
                "iterators opened changed for `{statement}` (gets: {})",
                ops.gets
            );
        }
    }
}

/// The keys a point WRITE makes newly reachable, each checked by its EFFECT
/// on the table.
///
/// A write that takes the wrong path does not return a wrong answer, it
/// modifies the wrong rows. These are the shapes where "reads by key" and
/// "scans and filters" could disagree, and they are the shapes the write path
/// had never reached before, because a scan reached the row no matter what
/// the key logic thought.
#[test]
fn a_point_write_keys_the_row_a_scan_would_have_filtered_to() {
    // A NULL bound pins no key: Go's `getNameValuePairs` returns nil for a
    // NULL datum, so this is not a point plan at all -- and `id = NULL` is
    // UNKNOWN for every row, so the write must touch NOTHING. Answering it
    // by "look up the NULL key, find no record" would be right by accident;
    // answering it by keying NULL as zero would delete row 0.
    let mut session = sbtest1_with_rows();
    session.run("DELETE FROM sbtest1 WHERE id = NULL").unwrap();
    assert_eq!(
        row_text(session.run("SELECT COUNT(*) FROM sbtest1")),
        vec![vec!["13".to_owned()]]
    );

    // A constant that is not written as an integer. The key is the constant
    // IN THE COLUMN'S DOMAIN or there is no point plan
    // (`driver::point_get_key`), so `150.0` and `'150'` key row 150 and
    // `150.5` keys nothing and reaches the row through a scan that matches
    // nothing either.
    for (predicate, operator, survivors) in [
        ("id = 150.0", "Point_Get_1", 12),
        ("id = '150'", "Point_Get_1", 12),
        ("id = 150.5", "TableRangeScan_1", 13),
    ] {
        let mut session = sbtest1_with_rows();
        assert_eq!(
            source_row(
                &mut session,
                &format!("EXPLAIN DELETE FROM sbtest1 WHERE {predicate}")
            )[0],
            operator,
            "`{predicate}` took the wrong access path"
        );
        session
            .run(&format!("DELETE FROM sbtest1 WHERE {predicate}"))
            .unwrap();
        assert_eq!(
            row_text(session.run("SELECT COUNT(*) FROM sbtest1")),
            vec![vec![survivors.to_string()]],
            "`{predicate}` removed the wrong number of rows"
        );
    }

    // An UNSIGNED handle above `i64::MAX`. `handle_range` REFUSES to range
    // over an unsigned handle (its record keys are not the interval their
    // bounds read like), so before this unit such a write always scanned.
    // The point plan does not refuse it: it keys the row by the same
    // reinterpretation the read side keys a `SELECT` by, and the row was
    // STORED under that same key. The adjacent handle is seeded so keying
    // the wrong one is visible.
    let mut session = Session::new();
    session
        .run("CREATE TABLE u (id BIGINT UNSIGNED PRIMARY KEY, v INT)")
        .unwrap();
    session
        .run("INSERT INTO u VALUES (18446744073709551615, 1), (18446744073709551614, 2), (7, 3)")
        .unwrap();
    assert_eq!(
        source_row(
            &mut session,
            "EXPLAIN UPDATE u SET v = 99 WHERE id = 18446744073709551615"
        )[0],
        "Point_Get_1",
        "the unsigned handle must reach the point plan, not fall back to a scan"
    );
    session
        .run("UPDATE u SET v = 99 WHERE id = 18446744073709551615")
        .unwrap();
    assert_eq!(
        row_text(session.run("SELECT id, v FROM u ORDER BY id")),
        vec![
            vec!["7".to_owned(), "3".to_owned()],
            vec!["18446744073709551614".to_owned(), "2".to_owned()],
            vec!["18446744073709551615".to_owned(), "99".to_owned()],
        ]
    );

    // A whole UNIQUE INDEX pinned instead of the handle: Go's point plan
    // covers this too, and it is the one arm that reads an index entry to
    // find the key. The extra conjunct is the guard that the `WHERE` is
    // still evaluated above the fetch -- the key did not pin `v`, so a
    // point plan that dropped the filter would change the row anyway.
    let mut session = Session::new();
    session
        .run("CREATE TABLE q (id INT PRIMARY KEY, uk INT UNIQUE, v INT)")
        .unwrap();
    session
        .run("INSERT INTO q VALUES (1, 10, 100), (2, 20, 200), (3, 30, 300)")
        .unwrap();
    assert_eq!(
        source_row(&mut session, "EXPLAIN UPDATE q SET v = 0 WHERE uk = 20")[0],
        "Point_Get_1",
        "a pinned unique index must reach the point plan"
    );
    session.run("UPDATE q SET v = 0 WHERE uk = 20").unwrap();
    session
        .run("UPDATE q SET v = 7 WHERE uk = 30 AND v = 999")
        .unwrap();
    // A unique key no row carries.
    session.run("DELETE FROM q WHERE uk = 40").unwrap();
    assert_eq!(
        row_text(session.run("SELECT id, v FROM q ORDER BY id")),
        vec![
            vec!["1".to_owned(), "100".to_owned()],
            vec!["2".to_owned(), "0".to_owned()],
            vec!["3".to_owned(), "300".to_owned()],
        ]
    );
}

/// The RECORDS a narrowed write actually reads, from `EXPLAIN ANALYZE`'s
/// `actRows` -- the receipt no plan assertion can give.
///
/// A write may print `TableRangeScan range:[150,150]` and still walk all
/// thirteen records underneath: the `WHERE` above the scan would filter the
/// surplus, exactly one row would change, and every assertion in
/// `the_sysbench_write_shapes_read_a_handle_range` and
/// `narrowed_writes_change_exactly_the_rows_a_full_scan_changed` would pass
/// while the read the range exists to avoid still happened. That read IS the
/// measured gap against Go, so it is asserted directly.
///
/// `actRows` cannot tell a one-key scan from a key lookup, though -- both
/// read one record. `a_point_write_opens_no_scan_at_all` is the receipt for
/// that half.
#[test]
fn writes_read_only_the_records_inside_their_handle_range() {
    // (`WHERE`, records the write's scan must read)
    for (predicate, read) in [
        // One of thirteen records. Before the narrowing this was 13, which is
        // the `kv_scan`-per-statement gap measured against Go.
        ("id = 150", "1"),
        // A handle no record carries: a point plan that finds nothing.
        ("id = 151", "0"),
        ("id BETWEEN 100 AND 199", "3"),
        // Two disjoint ranges, so the multi-range cursor is exercised: a scan
        // that collapsed them to their SPAN would read all thirteen.
        ("id NOT BETWEEN 0 AND 200", "3"),
        ("id > 199", "2"),
        ("id <= -1", "2"),
        ("id IN (-1, 2, 150)", "3"),
        ("id > 100 AND id < 100", "0"),
        // A residual condition still reads the whole range and filters above.
        ("id BETWEEN 100 AND 199 AND c = 'c150'", "3"),
        // No handle bound: the whole table, unchanged.
        ("k > 5", "13"),
    ] {
        for statement in [
            format!("UPDATE sbtest1 SET pad = 'W' WHERE {predicate}"),
            format!("DELETE FROM sbtest1 WHERE {predicate}"),
        ] {
            let mut session = sbtest1_with_rows();
            let rows = row_text(session.run(&format!("EXPLAIN ANALYZE {statement}")));
            let scan = rows.last().expect("a write plan has a source row");
            assert_eq!(
                scan[2], read,
                "records read changed for `{statement}` (source: {})",
                scan[0]
            );
        }
    }
}

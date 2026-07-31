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
//! Two of Go's rows can never appear here and are not defects: Go's
//! `cop[tikv]`/`TableReader` task split, which this tier does not print
//! (`tidb_executor::explain` divergence 1), and its always-present
//! `Projection` (divergence 3). What IS compared is the ACCESS PATH: which
//! source the plan reads through, the range it reads, and the row estimate
//! that choice was made on.
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
//! id > 0 and k = 4            3333.33     (0,+inf]    + cop Selection
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
        // CLASSIFIED DIVERGENCE, the only two in this corpus. Go prints
        // 10000.00 for both. Its `CountAfterAccess` here is the same 3333.33
        // this tier computes; what lifts it is `adjustCountAfterAccess`,
        // which raises a path's estimate to `ds.StatsInfo().RowCount /
        // SelectionFactor` -- and `RowCount` reaches 10000 only because
        // `Selectivity` estimates the UNCONVERTED range, whose `-inf` low
        // makes the estimator read `-1`'s bits as `u64::MAX` and call
        // `[-inf,-1)` the whole domain. That adjustment is not ported while
        // this tier's `RowCount` is not yet Go's under pseudo statistics
        // (`tidb_executor::access_cost::source_row_count` holds the reason
        // and the live differential that forced it). The direction is
        // conservative: an UNDER-estimate of a range this tier reads in full
        // anyway, so it can only lose an optimization, never a row.
        ("id < -1", "TableRangeScan", "3333.33", "range:[-inf,-1)"),
        ("id <= -1", "TableRangeScan", "3333.33", "range:[-inf,-1]"),
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
        // CLASSIFIED DIVERGENCE, and NOT one this range work introduced: Go
        // reads `TableRangeScan (0,+inf] 3333.33` with a cop `Selection` for
        // `k = 4`, where this tier takes `k_1` and looks the rows up. The
        // handle range only made the table path CHEAPER than it was before
        // (3333.33 rows instead of 10000), so the index path had already won
        // this comparison and still does. What separates them is the pseudo
        // estimate of an equality on a non-covering index, which is the same
        // pseudo-selectivity gap `source_row_count` records; the row set is
        // identical either way and is pinned by
        // `narrowed_ranges_return_the_same_rows_as_a_full_scan`, which runs
        // this very predicate.
        ("id > 0 AND k = 4", "IndexRangeScan", "10.00", "range:[4,4]"),
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
        // direction a range must never get wrong.
        ("id > 100 AND id < 100", "TableRangeScan", "0.00", "range:"),
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
}

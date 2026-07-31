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
//! # Why three of these are `#[ignore]`
//!
//! This tier has no clustered-handle range access path at all. Its source
//! choice (`tidb_executor::driver::access::commit_fast_path_source`) offers a
//! batch point get, an index range, and a point get -- Go's `TryFastPlan`
//! order -- and nothing between "one handle" and "every handle". A `WHERE`
//! that bounds the handle without pinning it therefore falls through to
//! `TableFullScan`, and `SUM(k)`'s reaches an `IndexFullScan` on `k_1`
//! because a full index scan and a full table scan are costed as the same
//! 10000 rows and the index wins the tie.
//!
//! The ranger that would build these ranges is already here and already
//! complete: `tidb_executor::index_range` ports `pkg/util/ranger`'s point
//! algebra including `BETWEEN`, `IN`, `NOT BETWEEN` and the DNF walk. Go
//! builds a table range with that SAME algebra over one column
//! (`ranger.BuildTableRange` -> `buildColumnRange`), so
//! `detach_cond_and_build_range_for_index` over the primary key as a
//! one-column index is the range builder -- no second ranger is needed.
//!
//! The row ESTIMATE the range is costed on is pinned too, and is what makes
//! these implementable rather than guesswork: it decides whether the range
//! beats the index full scan. It is not
//! `getPseudoRowCountBySignedIntRanges` alone -- Go dispatches on the FIRST
//! range's low bound and its `else` arm is the UNSIGNED estimator, which is
//! why `id < 0` estimates 3333.33 while `id < -1` estimates 10000.00 from
//! ranges of identical shape. Both arms are ported in
//! `tidb_planner::cardinality::pseudo`, and
//! `row_count_estimator::pseudo_row_count` now performs Go's dispatch
//! between them.
//!
//! So what remains for these three is the access path itself: build the
//! handle ranges, give the table candidate in
//! `tidb_executor::access_cost::enumerate_paths` a range instead of the
//! hard-coded full one (its own comment says "this tier builds no
//! primary-key range ... its range is therefore always the full one"), teach
//! `KvTable`'s `record_key_range` and `pushdown_row_cursor` to read a
//! narrowed span, and print the node as `TableRangeScan`.
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
#[ignore = "no clustered-handle range access path yet; see this module's doc"]
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
#[ignore = "no clustered-handle range access path yet; see this module's doc"]
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
#[ignore = "no clustered-handle range access path yet; see this module's doc"]
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

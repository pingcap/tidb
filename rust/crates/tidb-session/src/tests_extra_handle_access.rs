#![cfg(test)]

//! `_tidb_rowid` as a COVERED column and as an ORDER the table walk supplies.
//!
//! Three Go mechanisms meet on the extra handle, and each test pins one:
//!
//! * `handleCoveringColumn` (`pkg/planner/core/operator/logicalop/
//!   logical_datasource.go:759`): a column with `model.ExtraHandleID` is
//!   ALWAYS `stateCoveredByIntHandle` -- every index entry stores the int
//!   handle as its value -- so an index that covers the named columns covers
//!   `_tidb_rowid` too and the narrow index read wins.
//! * `findBestTask`'s two legs under a required sort
//!   (`pkg/planner/core/find_best_task.go`): a path that does not walk in
//!   the required order is priced UNDER the Sort enforcer
//!   (`EnforceProperty`), so a covering index that would have to sort loses
//!   to the table walk that delivers `_tidb_rowid` order for free.
//! * `MaxMinEliminator` (`pkg/planner/core/rule/rule_max_min_eliminate.go`):
//!   an ungrouped single `MAX(col)`/`MIN(col)` becomes `ORDER BY col [DESC]
//!   LIMIT 1` before physical optimization, which is what makes the ordered
//!   one-row table walk near-free and decisively cheaper than any covering
//!   `IndexFullScan`.
//!
//! The recorded plans live in `tests/integrationtest/r/executor/
//! autoid.result` (issue58631) and `tests/integrationtest/r/
//! access_path_selection.result`.

use crate::tests_support::*;
use crate::*;

/// `executor/autoid`'s issue58631 read: on a heap table whose PRIMARY(id) is
/// a plain unique index, `select _tidb_rowid, id from t` is answered from the
/// index alone -- TiDB reads `IndexFullScan index:PRIMARY(id)` in UNSIGNED
/// key order, so the row `(124, 123)` precedes `(-2, 18446744073709551613)`
/// even though the SIGNED handle order is the reverse. Before the
/// `ExtraHandleID` covering arm this tier full-scanned in handle order and
/// answered the rows backwards.
#[test]
fn extra_handle_is_covered_and_reads_in_index_order() {
    let mut session = Session::new();
    session.run("set tidb_enable_clustered_index=off").unwrap();
    session
        .run("create table t(id bigint unsigned auto_increment primary key)")
        .unwrap();
    session.run("insert into t values(123)").unwrap();
    session
        .run("insert into t values(18446744073709551613)")
        .unwrap();
    let plan = row_text(session.run("explain select _tidb_rowid, id from t"));
    let scan = plan.last().expect("a plan has a source row");
    assert!(
        scan[0].contains("IndexFullScan"),
        "the PRIMARY index covers `_tidb_rowid, id`, got {}",
        scan[0]
    );
    assert_eq!(scan[3], "table:t, index:PRIMARY(id)");
    assert_eq!(
        row_text(session.run("select _tidb_rowid, id from t")),
        vec![
            vec!["124".to_owned(), "123".to_owned()],
            vec!["-2".to_owned(), "18446744073709551613".to_owned()],
        ],
        "unsigned index order: 123 before 18446744073709551613"
    );
}

/// `access_path_selection`'s ORDER BY `_tidb_rowid`: the required order is
/// satisfied FREE by the table walk (Go `matchProperty`'s
/// `path.IsIntHandlePath` arm through `NewExtraHandleSchemaCol`), while the
/// covering `IDX_ab` path would have to add a Sort enforcer over its 3333
/// estimated rows -- so TiDB keeps `TableFullScan keep order:true`, and so
/// must the two-leg comparison here. This is the statement the covering arm
/// alone was measured to REGRESS to an `IndexRangeScan`.
#[test]
fn handle_order_keeps_the_table_walk_over_a_covering_index() {
    let mut session = Session::new();
    session
        .run(
            "CREATE TABLE access_path_selection (`a` int, `b` int, KEY `IDX_a` (`a`), \
             KEY `IDX_b` (`b`), KEY `IDX_ab` (`a`, `b`))",
        )
        .unwrap();
    let plan =
        row_text(session.run(
            "explain select a, b from access_path_selection where a > 10 order by _tidb_rowid",
        ));
    let scan = plan.last().expect("a plan has a source row");
    assert!(
        scan[0].contains("TableFullScan"),
        "the handle walk satisfies the order for free, got {}",
        scan[0]
    );
    assert_eq!(scan[3], "table:access_path_selection");
    assert_eq!(
        scan[4], "keep order:true, stats:pseudo",
        "the scan itself delivers the order; no Sort is planned"
    );
    assert!(
        !plan.iter().any(|row| row[0].contains("Sort")),
        "no enforcer above a walk that already delivers the order"
    );
    session
        .run("insert into access_path_selection (a,b) values (1,2),(3,4),(5,6)")
        .unwrap();
    assert_eq!(
        row_text(
            session.run("select a, b from access_path_selection where a > 2 order by _tidb_rowid")
        ),
        vec![
            vec!["3".to_owned(), "4".to_owned()],
            vec!["5".to_owned(), "6".to_owned()],
        ]
    );
}

/// `select max(_tidb_rowid)` / `select min(_tidb_rowid)`: Go's
/// `MaxMinEliminator.eliminateSingleMaxMin` rewrites the ungrouped aggregate
/// to `Agg -> Limit 1 -> Sort col [desc] -> DataSource`, and `findBestTask`
/// under that one-row ordered property picks the table walk. Refreshed
/// against the LIVE oracle (SELECT tidb_version() => fdfadb96b2...): Go
/// prints `StreamAgg -> Limit(root, offset:0, count:1) -> TableReader ->
/// Limit(cop) -> TableFullScan keep order:true[, desc], stats:pseudo` -- the
/// Sort collapses into the ordered walk's `desc` marker, and this tier now
/// prints the SAME shape (ids included, from the shared allocator walk).
/// The access decision underneath -- the table path, not a covering
/// `IndexFullScan` -- is what this test pins, with the answers over live
/// rows.
#[test]
fn max_min_over_the_handle_is_eliminated_to_a_one_row_ordered_walk() {
    let mut session = Session::new();
    session
        .run(
            "CREATE TABLE access_path_selection (`a` int, `b` int, KEY `IDX_a` (`a`), \
             KEY `IDX_b` (`b`), KEY `IDX_ab` (`a`, `b`))",
        )
        .unwrap();
    for (aggregate, order_suffix) in [
        ("max", "keep order:true, desc, stats:pseudo"),
        ("min", "keep order:true, stats:pseudo"),
    ] {
        let plan = row_text(session.run(&format!(
            "explain select {aggregate}(_tidb_rowid) from access_path_selection"
        )));
        assert!(
            plan[0][0].contains("StreamAgg"),
            "the aggregate stays above the one-row read, got {}",
            plan[0][0]
        );
        assert!(
            plan[1][0].contains("Limit") && plan[1][4] == "offset:0, count:1",
            "{aggregate} eliminates to a one-row ordered read, got {} / {}",
            plan[1][0],
            plan[1][4]
        );
        let scan = plan.last().expect("a plan has a source row");
        assert!(
            scan[0].contains("TableFullScan"),
            "the ordered one-row table walk beats every covering index, got {}",
            scan[0]
        );
        assert_eq!(scan[3], "table:access_path_selection");
        assert_eq!(
            scan[4], order_suffix,
            "{aggregate} orders the walk {order_suffix}"
        );
    }
    session
        .run("insert into access_path_selection (a,b) values (1,2),(3,4),(5,6)")
        .unwrap();
    assert_eq!(
        row_text(session.run("select max(_tidb_rowid) from access_path_selection")),
        vec![vec!["3".to_owned()]]
    );
    assert_eq!(
        row_text(session.run("select min(_tidb_rowid) from access_path_selection")),
        vec![vec!["1".to_owned()]]
    );
    // The empty table answers NULL: the aggregate above the eliminated
    // Limit is what supplies it (Go keeps the Aggregation for exactly this).
    session.run("delete from access_path_selection").unwrap();
    assert_eq!(
        row_text(session.run("select max(_tidb_rowid) from access_path_selection")),
        vec![vec!["NULL".to_owned()]]
    );
}

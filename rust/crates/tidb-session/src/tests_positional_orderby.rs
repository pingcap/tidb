#![cfg(test)]

//! Positional `ORDER BY` (`ORDER BY 1, 2`) plans and sorts like the
//! equivalent named-column `ORDER BY`.
//!
//! Go never shows a literal sort key to its optimizer rules: the parser builds
//! a bare-integer item as an `ast.PositionExpr` and `positionToScalarFunc`
//! rewrites it onto the plan's schema before any rule runs. When the driver
//! kept the written literal instead, the grouped partial-aggregate plans'
//! order/group match refused both the TiKV HashAgg split and -- with it --
//! every pushdown below the aggregate, leaving the whole plan at `root`.
//! These assertions pin the Go shapes captured in
//! `rust/TPCH_HBX_GO_PLAN_PARITY_EXECPLAN.md`'s q1-style aggregate query.

use crate::tests_support::*;
use crate::*;

/// The lineitem shape, minus the columns no clause here reads.
fn lineitem_session() -> Session {
    let mut session = Session::new();
    session
        .run(
            "CREATE TABLE lineitem (l_orderkey BIGINT, l_shipdate DATE, \
             l_returnflag CHAR(1), l_linestatus CHAR(1), l_quantity DECIMAL(15,2))",
        )
        .unwrap();
    session
}

const Q: &str = "FROM lineitem WHERE l_shipdate <= date_sub('1998-12-01', interval 108 day) \
                 GROUP BY l_returnflag, l_linestatus";

/// The operator names of one EXPLAIN, top down: the plan's SHAPE, free of
/// per-statement ids.
fn shape(session: &mut Session, order_by: &str) -> Vec<String> {
    let sql = format!(
        "EXPLAIN SELECT l_returnflag, l_linestatus, sum(l_quantity), count(*) {Q} {order_by}"
    );
    row_text(session.run(&sql))
        .into_iter()
        .map(|row| {
            // `Sort_7` -> `Sort`; a tree-drawing prefix rides the child rows.
            let id = row[0].rsplit('─').next().expect("a printed cell");
            id.split('_').next().expect("an operator name").to_owned()
        })
        .collect()
}

#[test]
fn positional_order_by_pushes_selection_and_hashagg_into_the_cop_task() {
    let mut session = lineitem_session();
    let rows = row_text(session.run(&format!(
        "EXPLAIN SELECT l_returnflag, l_linestatus, sum(l_quantity), count(*) {Q} ORDER BY 1, 2"
    )));

    // The cop half carries the Selection, the partial HashAgg and the scan;
    // the root keeps only Sort -> Projection -> final HashAgg -> TableReader.
    let task = |name: &str| {
        rows.iter()
            .find(|row| {
                row[0]
                    .rsplit('─')
                    .next()
                    .is_some_and(|id| id.starts_with(name))
            })
            .unwrap_or_else(|| panic!("no {name} row in {:?}", rows))
    };
    assert_eq!(task("Selection")[2], "cop[tikv]", "{rows:?}");
    assert_eq!(task("TableFullScan")[2], "cop[tikv]", "{rows:?}");
    assert_eq!(task("TableReader")[2], "root", "{rows:?}");
    let cop_agg = rows
        .iter()
        .find(|row| {
            row[0].ends_with("HashAgg_3") || row[0].contains("HashAgg") && row[2] == "cop[tikv]"
        })
        .expect("the split puts a HashAgg into the cop task");
    assert_eq!(cop_agg[2], "cop[tikv]");
    // No filter or aggregation remains at the root: nothing serially scans.
    assert!(
        rows.iter().all(|row| !(row[2] == "root"
            && (row[0].contains("Selection") || row[0].contains("TableFullScan")))),
        "{rows:?}"
    );

    // Go resolves positional items before its rules run, so the sort prints
    // the projected columns -- not the written literals.
    let sort = task("Sort");
    assert!(
        sort[4].contains("test.lineitem.l_returnflag")
            && sort[4].contains("test.lineitem.l_linestatus"),
        "sort by-items must be resolved columns: {:?}",
        sort
    );
}

#[test]
fn positional_order_by_plans_like_the_named_column_form() {
    let mut session = lineitem_session();
    assert_eq!(
        shape(&mut session, "ORDER BY 1, 2"),
        shape(&mut session, "ORDER BY l_returnflag, l_linestatus"),
    );
}

/// The whole point of resolving positions rather than dropping them: each
/// position still orders by ITS field, and the pushed-down plan returns the
/// rows Go does. Captured from real TiDB on these four rows.
#[test]
fn positional_order_by_sorts_by_the_field_it_names() {
    let mut session = lineitem_session();
    session
        .run(
            "INSERT INTO lineitem VALUES \
             (1,'1998-08-01','B','O',20.0),\
             (2,'1998-07-01','A','O',10.0),\
             (3,'1998-06-01','B','F',40.0),\
             (4,'1998-05-01','A','F',30.0)",
        )
        .unwrap();
    let select = |order_by: &str| {
        format!("SELECT l_returnflag, l_linestatus, sum(l_quantity) {Q} {order_by}")
    };
    // Captured from Go: `ORDER BY 1, 2` is `ORDER BY l_returnflag, l_linestatus`.
    assert_eq!(
        row_text(session.run(&select("ORDER BY 1, 2"))),
        [
            ["A", "F", "30.00"],
            ["A", "O", "10.00"],
            ["B", "F", "40.00"],
            ["B", "O", "20.00"]
        ]
    );
    assert_eq!(
        row_text(session.run(&select("ORDER BY l_returnflag, l_linestatus"))),
        row_text(session.run(&select("ORDER BY 1, 2"))),
    );
    // Reversed positions reverse the key order: linestatus first, flag second.
    assert_eq!(
        row_text(session.run(&select("ORDER BY 2, 1"))),
        [
            ["A", "F", "30.00"],
            ["B", "F", "40.00"],
            ["A", "O", "10.00"],
            ["B", "O", "20.00"]
        ]
    );
}

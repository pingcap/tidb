//! The clauses a `SELECT` carries, over constants and over a table: the
//! expression list, `WHERE`, `ORDER BY` and `LIMIT`.
//!
//! These hold the plumbing still -- that each clause reaches its executor and
//! composes with the others -- rather than any one operator's semantics. Go's
//! counterpart surface is `pkg/executor`'s `LimitExec`/`SortExec` wiring.

use super::*;

#[test]
fn select_constant_arithmetic() {
    assert_eq!(
        run_select("SELECT 1 + 1").unwrap(),
        vec![vec![Datum::Int(2)]]
    );
    assert_eq!(
        run_select("SELECT 1 + 1, 2 * 3").unwrap(),
        vec![vec![Datum::Int(2), Datum::Int(6)]]
    );
    assert_eq!(
        run_select("SELECT 2 * 3 - 1").unwrap(),
        vec![vec![Datum::Int(5)]]
    );
}

#[test]
fn select_with_where() {
    // A true predicate keeps the row.
    assert_eq!(
        run_select("SELECT 42 WHERE 1 = 1").unwrap(),
        vec![vec![Datum::Int(42)]]
    );
    // A false predicate yields no rows.
    assert_eq!(
        run_select("SELECT 42 WHERE 1 = 0").unwrap(),
        Vec::<Vec<Datum>>::new()
    );
}

#[test]
fn limit_and_order_by_wire_up() {
    // LIMIT truncates / zeroes the single row.
    assert_eq!(
        run_select("SELECT 42 LIMIT 1").unwrap(),
        vec![vec![Datum::Int(42)]]
    );
    assert_eq!(
        run_select("SELECT 42 LIMIT 0").unwrap(),
        Vec::<Vec<Datum>>::new()
    );
    assert_eq!(
        run_select("SELECT 42 LIMIT 1, 1").unwrap(),
        Vec::<Vec<Datum>>::new()
    );
    // ORDER BY over the single dual row passes through the sort.
    assert_eq!(
        run_select("SELECT 42 ORDER BY 1 DESC").unwrap(),
        vec![vec![Datum::Int(42)]]
    );
}

#[test]
fn select_from_table_order_limit() {
    let catalog = test_catalog();
    // ORDER BY a column that is not projected (sort runs below projection).
    assert_eq!(
        run_select_on(
            "SELECT a FROM t ORDER BY b",
            &catalog,
            &crate::StmtContext::for_query()
        )
        .unwrap(),
        vec![
            vec![Datum::Int(3)],
            vec![Datum::Int(2)],
            vec![Datum::Int(1)]
        ]
    );
    assert_eq!(
        run_select_on(
            "SELECT a FROM t ORDER BY b DESC LIMIT 2",
            &catalog,
            &crate::StmtContext::for_query()
        )
        .unwrap(),
        vec![vec![Datum::Int(1)], vec![Datum::Int(2)]]
    );
}

//! `UPDATE` and `DELETE` against stored rows, and the positional `ORDER BY`
//! a DML statement resolves.
//!
//! These check that a mutation rewrites the row bytes AND its index entries,
//! which is the part a naive implementation gets wrong. Mirrors Go
//! `pkg/executor`'s `UpdateExec`/`DeleteExec` over `pkg/table/tables`.

use super::*;

/// Go plans an `UPDATE`/`DELETE`'s read from the same cost chooser a `SELECT`
/// reaches, so a `WHERE` on a secondary index reads through that index rather
/// than scanning the whole table. `EXPLAIN` prints the `IndexRangeScan`, and
/// the write still removes exactly the rows the `WHERE` admits.
#[test]
fn a_delete_on_a_secondary_index_reads_through_it() {
    use crate::explain::{explain_delete_stmt, ExplainFormat};

    let mut catalog = Catalog::default();
    crate::run_create_table_on(
        "CREATE TABLE t1 (c1 INT PRIMARY KEY, c2 INT, c3 INT, INDEX c2 (c2))",
        &mut catalog,
    )
    .unwrap();
    let ctx = crate::StmtContext::for_query();
    run_insert_on(
        "INSERT INTO t1 VALUES (1, 10, 100), (2, 20, 200), (3, 10, 300)",
        &mut catalog,
        &ctx,
    )
    .unwrap();

    // EXPLAIN reads the index, not the table.
    let stmt = tidb_parser::parse("DELETE FROM t1 WHERE c2 = 10").unwrap();
    let Stmt::Dml(dml) = &stmt else {
        panic!("not a DML statement");
    };
    let tidb_ast::DmlStmt::Delete(delete) = &**dml else {
        panic!("not a DELETE");
    };
    let (_, rows) =
        explain_delete_stmt(delete, &mut catalog, "test", &ctx, ExplainFormat::Row).unwrap();
    let cell = |datum: &Datum| match datum {
        Datum::Bytes(bytes) => String::from_utf8_lossy(bytes).into_owned(),
        other => format!("{other:?}"),
    };
    let plan: Vec<String> = rows
        .iter()
        .map(|row| row.iter().map(cell).collect::<Vec<_>>().join("\t"))
        .collect();
    assert!(
        plan.iter()
            .any(|line| line.contains("IndexRangeScan") && line.contains("index:c2(c2)")),
        "the delete reads through index c2, got {plan:?}"
    );
    assert!(
        !plan.iter().any(|line| line.contains("TableFullScan")),
        "no full scan remains, got {plan:?}"
    );

    // Executing it removes exactly the two c2 = 10 rows, index entries and all,
    // proving the index-range read fetched the right records.
    assert_eq!(
        run_delete_on("DELETE FROM t1 WHERE c2 = 10", &mut catalog, &ctx).unwrap(),
        2
    );
    assert_eq!(
        run_select_on("SELECT c1 FROM t1 ORDER BY c1", &catalog, &ctx).unwrap(),
        vec![vec![Datum::Int(2)]]
    );
}

/// A bare integer in `UPDATE`/`DELETE ... ORDER BY` is a POSITIONAL
/// reference to the table's own column at that 1-based position, not a
/// constant. Captured via `zz_dump_parity_test.go`
/// (`TestZZDumpParityDMLPositionalOrderBy`, run with
/// `go test -tags=intest -run TestZZDumpParityDMLPositionalOrderBy
/// ./pkg/executor/ -v`): on `t(a, b)` seeded with
/// `(1,30),(2,20),(3,10)`, `UPDATE t SET a = a + 100 ORDER BY 2 LIMIT 1`
/// updated the row with the SMALLEST `b` (`(3,10)` -> `(103,10)`), and
/// `DELETE FROM t ORDER BY 2 LIMIT 1` removed that same smallest-`b`
/// row. `2` resolves to column `b`, exactly like `SELECT`'s positional
/// `ORDER BY`/`GROUP BY` against the select list -- there is no select
/// list in a single-table `UPDATE`/`DELETE`, so it indexes the table's
/// declared columns instead. Do not "fix" this back to a constant.
#[test]
fn dml_positional_order_by_resolves_to_column() {
    let mut catalog = Catalog::default();
    crate::run_create_table_on("CREATE TABLE t (a BIGINT, b BIGINT)", &mut catalog).unwrap();
    run_insert_on(
        "INSERT INTO t VALUES (1, 30), (2, 20), (3, 10)",
        &mut catalog,
        &crate::StmtContext::for_query(),
    )
    .unwrap();

    assert_eq!(
        run_update_on(
            "UPDATE t SET a = a + 100 ORDER BY 2 LIMIT 1",
            &mut catalog,
            &crate::StmtContext::for_query()
        )
        .unwrap(),
        1
    );
    assert_eq!(
        run_select_on(
            "SELECT a, b FROM t ORDER BY b",
            &catalog,
            &crate::StmtContext::for_query()
        )
        .unwrap(),
        vec![
            vec![Datum::Int(103), Datum::Int(10)],
            vec![Datum::Int(2), Datum::Int(20)],
            vec![Datum::Int(1), Datum::Int(30)],
        ]
    );

    crate::run_create_table_on("CREATE TABLE t2 (a BIGINT, b BIGINT)", &mut catalog).unwrap();
    run_insert_on(
        "INSERT INTO t2 VALUES (1, 30), (2, 20), (3, 10)",
        &mut catalog,
        &crate::StmtContext::for_query(),
    )
    .unwrap();
    assert_eq!(
        run_delete_on(
            "DELETE FROM t2 ORDER BY 2 LIMIT 1",
            &mut catalog,
            &crate::StmtContext::for_query()
        )
        .unwrap(),
        1
    );
    assert_eq!(
        run_select_on(
            "SELECT a, b FROM t2 ORDER BY b",
            &catalog,
            &crate::StmtContext::for_query()
        )
        .unwrap(),
        vec![
            vec![Datum::Int(2), Datum::Int(20)],
            vec![Datum::Int(1), Datum::Int(30)],
        ]
    );
}

/// UPDATE and DELETE over both table backings, including MySQL's
/// affected-row rule: an UPDATE counts CHANGED rows, so a row whose new
/// values equal its old ones is touched but not affected.
#[test]
fn update_and_delete_rows() {
    for kv in [false, true] {
        let mut catalog = Catalog::default();
        if kv {
            crate::run_create_table_on("CREATE TABLE w (a BIGINT, b BIGINT)", &mut catalog)
                .unwrap();
        } else {
            catalog.register(
                "w",
                MemTable {
                    columns: vec![
                        ("a".to_owned(), FieldType::new(FieldTypeCode::LongLong)),
                        ("b".to_owned(), FieldType::new(FieldTypeCode::LongLong)),
                    ],
                    rows: vec![],
                },
            );
        }
        run_insert_on(
            "INSERT INTO w VALUES (1, 10), (2, 20), (3, 30)",
            &mut catalog,
            &crate::StmtContext::for_query(),
        )
        .unwrap();

        // WHERE-selected update, counting only changed rows.
        assert_eq!(
            run_update_on(
                "UPDATE w SET b = b + 1 WHERE a >= 2",
                &mut catalog,
                &crate::StmtContext::for_query()
            )
            .unwrap(),
            2,
            "kv={kv}"
        );
        assert_eq!(
            run_select_on(
                "SELECT a, b FROM w",
                &catalog,
                &crate::StmtContext::for_query()
            )
            .unwrap(),
            vec![
                vec![Datum::Int(1), Datum::Int(10)],
                vec![Datum::Int(2), Datum::Int(21)],
                vec![Datum::Int(3), Datum::Int(31)],
            ],
            "kv={kv}"
        );

        // A no-op update matches rows but changes none: MySQL reports 0.
        assert_eq!(
            run_update_on(
                "UPDATE w SET b = b WHERE a = 1",
                &mut catalog,
                &crate::StmtContext::for_query()
            )
            .unwrap(),
            0,
            "kv={kv}"
        );

        // Every assignment reads the row as the statement found it, so
        // `b` takes the ORIGINAL `a` (1), not the just-assigned 7.
        assert_eq!(
            run_update_on(
                "UPDATE w SET a = 7, b = a WHERE a = 1",
                &mut catalog,
                &crate::StmtContext::for_query()
            )
            .unwrap(),
            1,
            "kv={kv}"
        );
        assert_eq!(
            run_select_on(
                "SELECT a, b FROM w WHERE a = 7",
                &catalog,
                &crate::StmtContext::for_query()
            )
            .unwrap(),
            vec![vec![Datum::Int(7), Datum::Int(1)]],
            "kv={kv}"
        );

        // A WHERE-less UPDATE touches every row.
        assert_eq!(
            run_update_on(
                "UPDATE w SET b = 0",
                &mut catalog,
                &crate::StmtContext::for_query()
            )
            .unwrap(),
            3,
            "kv={kv}"
        );

        // DELETE removes the selected rows and reports their count.
        assert_eq!(
            run_delete_on(
                "DELETE FROM w WHERE a >= 3",
                &mut catalog,
                &crate::StmtContext::for_query()
            )
            .unwrap(),
            2,
            "kv={kv}"
        );
        assert_eq!(
            run_select_on(
                "SELECT a FROM w",
                &catalog,
                &crate::StmtContext::for_query()
            )
            .unwrap(),
            vec![vec![Datum::Int(2)]],
            "kv={kv}"
        );

        // A WHERE-less DELETE empties the table, and re-inserting works
        // after it (the store is genuinely empty, not just filtered).
        assert_eq!(
            run_delete_on(
                "DELETE FROM w",
                &mut catalog,
                &crate::StmtContext::for_query()
            )
            .unwrap(),
            1,
            "kv={kv}"
        );
        assert_eq!(
            run_select_on(
                "SELECT a FROM w",
                &catalog,
                &crate::StmtContext::for_query()
            )
            .unwrap(),
            Vec::<Vec<Datum>>::new(),
            "kv={kv}"
        );
        run_insert_on(
            "INSERT INTO w VALUES (9, 9)",
            &mut catalog,
            &crate::StmtContext::for_query(),
        )
        .unwrap();
        assert_eq!(
            run_select_on(
                "SELECT a FROM w",
                &catalog,
                &crate::StmtContext::for_query()
            )
            .unwrap(),
            vec![vec![Datum::Int(9)]],
            "kv={kv}"
        );

        // ORDER BY and LIMIT are supported now (see the session's
        // `insert_select_and_ordered_dml`); an unknown SET column and
        // the IGNORE form still fail closed.
        assert!(run_update_on(
            "UPDATE w SET zzz = 1",
            &mut catalog,
            &crate::StmtContext::for_query()
        )
        .is_err());
        assert!(run_update_on(
            "UPDATE IGNORE w SET a = 1",
            &mut catalog,
            &crate::StmtContext::for_query()
        )
        .is_err());
    }
}

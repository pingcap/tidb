//! `UPDATE` and `DELETE` against stored rows, and the positional `ORDER BY`
//! a DML statement resolves.
//!
//! These check that a mutation rewrites the row bytes AND its index entries,
//! which is the part a naive implementation gets wrong. Mirrors Go
//! `pkg/executor`'s `UpdateExec`/`DeleteExec` over `pkg/table/tables`.

use super::*;

#[test]
fn retryable_storage_errors_keep_their_transaction_identity() {
    let retryable = || {
        crate::kv_table::KvTableError::Storage(
            "Retryable(\"region response no longer matches observed route\")".to_owned(),
        )
    };

    for error in [
        kv_read_error("row read failed", retryable()),
        kv_write_error(retryable()),
    ] {
        let DriverError::Txn(TxnErrorKind::RegionUnavailable) = error else {
            panic!("retryable storage failure lost its transaction identity")
        };
    }

    let ordinary = kv_read_error(
        "row read failed",
        crate::kv_table::KvTableError::Storage("Backend(\"disk error\")".to_owned()),
    );
    // A storage failure is a runtime 1105, never a 1064: the client's SQL
    // text was fine.
    assert!(matches!(
        ordinary,
        DriverError::Exec(crate::ExecError::Internal(_))
    ));
}

/// A normal clustered INSERT may batch its record-key absence probe, but it
/// must still reject both a key committed before the statement and a repeated
/// key within one VALUES list. The latter guards the local de-duplication
/// branch in `all_clustered_insert_keys_absent`.
#[test]
fn clustered_batch_insert_keeps_primary_duplicate_errors() {
    let mut catalog = Catalog::default();
    crate::run_create_table_on(
        "CREATE TABLE batch_insert (id BIGINT PRIMARY KEY, value INT)",
        &mut catalog,
    )
    .unwrap();
    let ctx = crate::StmtContext::for_query();
    run_insert_on(
        "INSERT INTO batch_insert VALUES (1, 10), (2, 20), (3, 30)",
        &mut catalog,
        &ctx,
    )
    .unwrap();
    let existing = run_insert_on(
        "INSERT INTO batch_insert VALUES (2, 200)",
        &mut catalog,
        &ctx,
    )
    .expect_err("an existing clustered key must not be overwritten");
    assert!(matches!(existing, DriverError::DuplicateEntry { .. }));
    let mut repeated_catalog = Catalog::default();
    crate::run_create_table_on(
        "CREATE TABLE batch_insert (id BIGINT PRIMARY KEY, value INT)",
        &mut repeated_catalog,
    )
    .unwrap();
    let repeated = run_insert_on(
        "INSERT INTO batch_insert VALUES (5, 50), (5, 500)",
        &mut repeated_catalog,
        &ctx,
    )
    .expect_err("duplicate keys in one VALUES list must be rejected");
    assert!(matches!(repeated, DriverError::DuplicateEntry { .. }));
}

/// go-tpc's delivery transaction deletes several `new_order` rows with one
/// row-valued `IN` predicate. The SQL remains one DELETE and must report the
/// number of matching composite keys exactly as Go TiDB does.
#[test]
fn delete_accepts_tpcc_three_column_row_in() {
    let mut catalog = Catalog::default();
    crate::run_create_table_on(
        "CREATE TABLE new_order (\
         no_w_id INT, no_d_id INT, no_o_id INT, \
         PRIMARY KEY (no_w_id, no_d_id, no_o_id))",
        &mut catalog,
    )
    .unwrap();
    let ctx = crate::StmtContext::for_query();
    run_insert_on(
        "INSERT INTO new_order VALUES \
         (1, 1, 100), (1, 1, 101), (1, 2, 100), (2, 1, 100)",
        &mut catalog,
        &ctx,
    )
    .unwrap();

    let stmt = tidb_parser::parse(
        "DELETE FROM new_order WHERE (no_w_id, no_d_id, no_o_id) IN ((1, 1, 100), (2, 1, 100))",
    )
    .unwrap();
    let Stmt::Dml(dml) = &stmt else {
        panic!("not a DML statement");
    };
    let tidb_ast::DmlStmt::Delete(delete) = &**dml else {
        panic!("not a DELETE");
    };
    let (_, plan_rows) = crate::explain::explain_delete_stmt(
        delete,
        &mut catalog,
        "test",
        &ctx,
        crate::explain::ExplainFormat::Row,
    )
    .unwrap();
    let plan_text: String = plan_rows
        .iter()
        .flat_map(|row| row.iter())
        .filter_map(|datum| match datum {
            Datum::Bytes(bytes) => Some(String::from_utf8_lossy(bytes).into_owned()),
            _ => None,
        })
        .collect::<Vec<_>>()
        .join("\n");
    assert!(plan_text.contains("Batch_Point_Get"), "plan={plan_text}");
    assert_eq!(
        run_delete_on(
            "DELETE FROM new_order \
             WHERE (no_w_id, no_d_id, no_o_id) \
             IN ((1, 1, 100), (2, 1, 100))",
            &mut catalog,
            &ctx,
        )
        .unwrap(),
        2,
    );
    assert_eq!(
        run_select_on(
            "SELECT no_w_id, no_d_id, no_o_id FROM new_order \
             ORDER BY no_w_id, no_d_id, no_o_id",
            &catalog,
            &ctx,
        )
        .unwrap(),
        vec![
            vec![Datum::Int(1), Datum::Int(1), Datum::Int(101)],
            vec![Datum::Int(1), Datum::Int(2), Datum::Int(100)],
        ],
    );
}

/// Go finishes a table-range cop task with a root TableReader before feeding
/// its rows to Update. A consumed handle range is still a table scan, not a
/// root fast plan.
#[test]
fn a_consumed_handle_range_update_keeps_its_table_reader() {
    use crate::explain::{explain_update_stmt, ExplainFormat};

    let mut catalog = Catalog::default();
    crate::run_create_table_on(
        "CREATE TABLE target (id INT PRIMARY KEY, value INT)",
        &mut catalog,
    )
    .unwrap();
    let ctx = crate::StmtContext::for_query();
    let stmt =
        tidb_parser::parse("UPDATE target SET value = 1 WHERE id BETWEEN 10 AND 20").unwrap();
    let Stmt::Dml(dml) = &stmt else {
        panic!("not a DML statement");
    };
    let tidb_ast::DmlStmt::Update(update) = &**dml else {
        panic!("not an UPDATE");
    };

    let (_, rows) =
        explain_update_stmt(update, &mut catalog, "test", &ctx, ExplainFormat::Brief).unwrap();
    let cell = |row: usize, column: usize| match &rows[row][column] {
        Datum::Bytes(bytes) => String::from_utf8_lossy(bytes).into_owned(),
        other => format!("{other:?}"),
    };

    assert_eq!(rows.len(), 3, "{rows:#?}");
    assert_eq!(cell(0, 0), "Update");
    assert_eq!(cell(1, 0), "└─TableReader");
    assert_eq!(cell(1, 2), "root");
    assert_eq!(cell(1, 4), "data:TableRangeScan");
    assert_eq!(cell(2, 0), "  └─TableRangeScan");
    assert_eq!(cell(2, 2), "cop[tikv]");
    assert_eq!(cell(2, 3), "table:target");
    assert!(cell(2, 4).contains("range:[10,20]"));
}

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
/// Go resolves the whole plan before `buildLimit`'s zero short-circuit
/// (`pkg/planner/core/logical_plan_builder.go`), so `LIMIT 0` still reports a
/// bad `ORDER BY` column or a partition selection on an unpartitioned table
/// instead of silently touching nothing.
#[test]
fn limit_zero_dml_still_validates_the_statement() {
    let mut catalog = Catalog::default();
    crate::run_create_table_on("CREATE TABLE lz (a BIGINT, b BIGINT)", &mut catalog).unwrap();
    let ctx = crate::StmtContext::for_query();
    run_insert_on("INSERT INTO lz VALUES (1, 10), (2, 20)", &mut catalog, &ctx).unwrap();

    assert_eq!(
        run_update_on("UPDATE lz SET b = 1 LIMIT 0", &mut catalog, &ctx).unwrap(),
        0
    );
    assert_eq!(
        run_delete_on("DELETE FROM lz LIMIT 0", &mut catalog, &ctx).unwrap(),
        0
    );

    assert!(
        run_update_on(
            "UPDATE lz SET b = 1 ORDER BY nope LIMIT 0",
            &mut catalog,
            &ctx
        )
        .is_err(),
        "an unresolvable ORDER BY column must error under LIMIT 0"
    );
    assert!(
        run_delete_on("DELETE FROM lz ORDER BY nope LIMIT 0", &mut catalog, &ctx).is_err(),
        "an unresolvable ORDER BY column must error under LIMIT 0"
    );

    assert!(
        run_update_on(
            "UPDATE lz PARTITION (p0) SET b = 1 LIMIT 0",
            &mut catalog,
            &ctx
        )
        .is_err(),
        "a partition selection on an unpartitioned table must error under LIMIT 0"
    );
    assert!(
        run_delete_on("DELETE FROM lz PARTITION (p0) LIMIT 0", &mut catalog, &ctx).is_err(),
        "a partition selection on an unpartitioned table must error under LIMIT 0"
    );

    // Nothing above modified anything.
    assert_eq!(
        run_select_on("SELECT a, b FROM lz", &catalog, &ctx).unwrap(),
        vec![
            vec![Datum::Int(1), Datum::Int(10)],
            vec![Datum::Int(2), Datum::Int(20)],
        ],
    );
}

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

        // ORDER BY, LIMIT, and IGNORE are supported now (see the session's
        // `insert_select_and_ordered_dml`); an unknown SET column still fails
        // closed. With no duplicate-key conflict, Go applies UPDATE IGNORE as
        // an ordinary update.
        assert!(run_update_on(
            "UPDATE w SET zzz = 1",
            &mut catalog,
            &crate::StmtContext::for_query()
        )
        .is_err());
        assert_eq!(
            run_update_on(
                "UPDATE IGNORE w SET a = 1",
                &mut catalog,
                &crate::StmtContext::for_query()
            )
            .unwrap(),
            1,
            "kv={kv}"
        );
        assert_eq!(
            run_select_on(
                "SELECT a, b FROM w",
                &catalog,
                &crate::StmtContext::for_query()
            )
            .unwrap(),
            vec![vec![Datum::Int(1), Datum::Int(9)]],
            "kv={kv}"
        );
    }
}

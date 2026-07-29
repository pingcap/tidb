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
fn unknown_table_is_rejected() {
    assert!(matches!(
        run_select("SELECT a FROM missing"),
        Err(DriverError::Unsupported(_))
    ));
}

/// The split rule itself, at the shape boundary: a column-versus-constant
/// comparison moves into the scan and everything else stays above it.
#[test]
fn the_scan_takes_comparisons_against_constants_and_nothing_else() {
    use tidb_datatype::FieldTypeCode;
    let scope = FromScope {
        tables: vec![FromTable {
            name: "t".to_owned(),
            database: None,
            columns: vec![
                ("a".to_owned(), FieldType::new(FieldTypeCode::LongLong)),
                ("b".to_owned(), FieldType::new(FieldTypeCode::LongLong)),
            ],
            offset: 0,
            determinants: Vec::new(),
        }],
        ..FromScope::default()
    };
    let split = |sql: &str| {
        let stmt = tidb_parser::parse(sql).expect("a select");
        let Stmt::Query(query) = &stmt else {
            panic!("a select");
        };
        let QueryStmt::Select(statement) = &**query else {
            panic!("a select");
        };
        let where_clause = statement.where_clause.clone().expect("a where clause");
        let (pushed, residual) =
            split_scan_predicates(&where_clause, &ScopeResolver { scope: &scope });
        (
            pushed
                .comparisons()
                .iter()
                .map(|c| (c.column_offset, c.op, c.literal.clone(), c.column_on_left))
                .collect::<Vec<_>>(),
            residual.map(|expr| expr.restore()),
        )
    };

    // Either operand order pushes, and the order is preserved.
    assert_eq!(
        split("SELECT 1 FROM t WHERE a > 5"),
        (vec![(0, ScanComparisonOp::Gt, Datum::Int(5), true)], None)
    );
    assert_eq!(
        split("SELECT 1 FROM t WHERE 5 < a"),
        (vec![(0, ScanComparisonOp::Lt, Datum::Int(5), false)], None)
    );
    // A qualified name resolves to the same column.
    assert_eq!(
        split("SELECT 1 FROM t WHERE t.b = 1").0,
        vec![(1, ScanComparisonOp::Eq, Datum::Int(1), true)]
    );
    // Mixed: the comparison pushes, the arithmetic does not.
    let (pushed, residual) = split("SELECT 1 FROM t WHERE a > 5 AND b + 1 < 10");
    assert_eq!(pushed, vec![(0, ScanComparisonOp::Gt, Datum::Int(5), true)]);
    assert!(residual.is_some(), "the arithmetic conjunct stays above");
    // Shapes that push nothing: a disjunction, a column-to-column
    // comparison, a NULL constant, an operator outside the accepted set.
    for sql in [
        "SELECT 1 FROM t WHERE a > 5 OR b < 10",
        "SELECT 1 FROM t WHERE a > b",
        "SELECT 1 FROM t WHERE a = NULL",
        "SELECT 1 FROM t WHERE a IS NULL",
        "SELECT 1 FROM t WHERE a <=> 5",
    ] {
        let (pushed, residual) = split(sql);
        assert!(pushed.is_empty(), "{sql} must not push");
        assert!(residual.is_some(), "{sql} keeps its whole predicate");
    }
}

fn test_catalog() -> Catalog {
    use tidb_datatype::FieldTypeCode;
    let mut catalog = Catalog::default();
    catalog.register(
        "t",
        MemTable {
            columns: vec![
                ("a".to_owned(), FieldType::new(FieldTypeCode::LongLong)),
                ("b".to_owned(), FieldType::new(FieldTypeCode::LongLong)),
            ],
            rows: vec![
                vec![Datum::Int(1), Datum::Int(30)],
                vec![Datum::Int(2), Datum::Int(20)],
                vec![Datum::Int(3), Datum::Int(10)],
            ],
        },
    );
    catalog
}

#[test]
fn select_from_table() {
    let catalog = test_catalog();
    // Column projection.
    assert_eq!(
        run_select_on(
            "SELECT a FROM t",
            &catalog,
            &crate::StmtContext::for_query()
        )
        .unwrap(),
        vec![
            vec![Datum::Int(1)],
            vec![Datum::Int(2)],
            vec![Datum::Int(3)]
        ]
    );
    // Wildcard, qualified column, and an expression over columns.
    assert_eq!(
        run_select_on(
            "SELECT * FROM t WHERE t.a > 1",
            &catalog,
            &crate::StmtContext::for_query()
        )
        .unwrap(),
        vec![
            vec![Datum::Int(2), Datum::Int(20)],
            vec![Datum::Int(3), Datum::Int(10)],
        ]
    );
    assert_eq!(
        run_select_on(
            "SELECT a + b FROM t WHERE a = 2",
            &catalog,
            &crate::StmtContext::for_query()
        )
        .unwrap(),
        vec![vec![Datum::Int(22)]]
    );
}

/// Regression for the label a `COUNT` field gets when no `AS` is
/// written: MySQL/Go name the output column after the SQL as WRITTEN
/// (`SelectField.Text`), not after the AST's normal form -- `COUNT(*)`
/// restores as `COUNT(1)` (both here and in Go's own hand-written
/// parser, which lowers a bare `*` argument to the literal `1`), but the
/// column label must stay `count(*)`. Before the fix this asserted
/// `field_name(0) == "COUNT(1)"`; after, it is the written text.
#[test]
fn count_star_field_keeps_its_written_label() {
    let catalog = test_catalog();
    let field_name = |sql: &str| {
        run_select_meta_on(sql, &catalog, &crate::StmtContext::for_query())
            .unwrap()
            .0
            .into_iter()
            .map(|(name, _)| name)
            .collect::<Vec<_>>()
    };

    assert_eq!(field_name("SELECT count(*) FROM t"), vec!["count(*)"]);
    assert_eq!(field_name("SELECT count(1) FROM t"), vec!["count(1)"]);
    assert_eq!(field_name("SELECT count(a) FROM t"), vec!["count(a)"]);
    assert_eq!(
        field_name("SELECT count(DISTINCT a) FROM t"),
        vec!["count(DISTINCT a)"]
    );
    assert_eq!(field_name("SELECT count(*) AS n FROM t"), vec!["n"]);
    // Same rule inside a derived table: the label becomes the derived
    // column name.
    assert_eq!(
        field_name("SELECT * FROM (SELECT count(*) FROM t) d"),
        vec!["count(*)"]
    );

    // The same root cause (the AST losing the original written text)
    // also surfaces in `ErrWrongGroupField`'s message, which Go quotes
    // with the field's written text too.
    let group_by_err = |sql: &str| {
        run_select_meta_on(sql, &catalog, &crate::StmtContext::for_query())
            .unwrap_err()
            .to_mysql_error()
            .message
    };
    assert_eq!(
        group_by_err("SELECT count(*) FROM t GROUP BY 1"),
        "Can't group on 'count(*)'"
    );
    assert_eq!(
        group_by_err("SELECT count(1) FROM t GROUP BY 1"),
        "Can't group on 'count(1)'"
    );
}

#[test]
fn insert_then_select_round_trip() {
    let mut catalog = test_catalog();
    // Full-row insert.
    assert_eq!(
        run_insert_on(
            "INSERT INTO t VALUES (4, 40), (5, 50)",
            &mut catalog,
            &crate::StmtContext::for_query()
        )
        .unwrap(),
        2
    );
    // Column-list insert: unspecified column fills with NULL.
    assert_eq!(
        run_insert_on(
            "INSERT INTO t (a) VALUES (6)",
            &mut catalog,
            &crate::StmtContext::for_query()
        )
        .unwrap(),
        1
    );
    assert_eq!(
        run_select_on(
            "SELECT a, b FROM t WHERE a > 3 ORDER BY a",
            &catalog,
            &crate::StmtContext::for_query()
        )
        .unwrap(),
        vec![
            vec![Datum::Int(4), Datum::Int(40)],
            vec![Datum::Int(5), Datum::Int(50)],
            vec![Datum::Int(6), Datum::Null],
        ]
    );
    // Arity mismatch and unknown table are rejected.
    assert!(run_insert_on(
        "INSERT INTO t (a) VALUES (1, 2)",
        &mut catalog,
        &crate::StmtContext::for_query()
    )
    .is_err());
    assert!(run_insert_on(
        "INSERT INTO missing VALUES (1)",
        &mut catalog,
        &crate::StmtContext::for_query()
    )
    .is_err());
}

/// The deployment-ladder proof: INSERT and SELECT round-trip through a
/// table whose rows are genuine TiKV-format bytes (record keys + v2 row
/// values), not a value matrix.
#[test]
fn sql_round_trips_through_real_tikv_bytes() {
    use crate::kv_table::{KvColumn, KvTable};
    use tidb_datatype::FieldTypeCode;
    let mut catalog = Catalog::default();
    catalog.register_kv(
        "kt",
        KvTable::new(
            77,
            vec![
                KvColumn {
                    name: "a".to_owned(),
                    id: 1,
                    field_type: FieldType::new(FieldTypeCode::LongLong),
                    default_value: None,
                    // A column present at CREATE TABLE has no pre-existing rows.
                    origin_default: None,
                },
                KvColumn {
                    name: "b".to_owned(),
                    id: 2,
                    field_type: FieldType::new(FieldTypeCode::LongLong),
                    default_value: None,
                    // A column present at CREATE TABLE has no pre-existing rows.
                    origin_default: None,
                },
            ],
        ),
    );

    assert_eq!(
        run_insert_on(
            "INSERT INTO kt VALUES (1, 10), (2, 20), (3, 30)",
            &mut catalog,
            &crate::StmtContext::for_query()
        )
        .unwrap(),
        3
    );
    assert_eq!(
        run_select_on(
            "SELECT a, b FROM kt WHERE a > 1 ORDER BY b DESC",
            &catalog,
            &crate::StmtContext::for_query()
        )
        .unwrap(),
        vec![
            vec![Datum::Int(3), Datum::Int(30)],
            vec![Datum::Int(2), Datum::Int(20)],
        ]
    );
    assert_eq!(
        run_select_on(
            "SELECT a + b FROM kt WHERE a = 1",
            &catalog,
            &crate::StmtContext::for_query()
        )
        .unwrap(),
        vec![vec![Datum::Int(11)]]
    );
}

#[test]
fn aggregate_selects() {
    let catalog = test_catalog();
    // Global aggregates: rows (1,30),(2,20),(3,10).
    assert_eq!(
        run_select_on(
            "SELECT COUNT(*), SUM(a) FROM t",
            &catalog,
            &crate::StmtContext::for_query()
        )
        .unwrap(),
        // SUM is a DECIMAL in MySQL even over a BIGINT column.
        vec![vec![
            Datum::Int(3),
            Datum::Decimal(tidb_datatype::Decimal::from_int(6))
        ]]
    );
    // GROUP BY with a carried key column, WHERE below the agg.
    assert_eq!(
        run_select_on(
            "SELECT a, COUNT(*) FROM t WHERE b >= 20 GROUP BY a",
            &catalog,
            &crate::StmtContext::for_query()
        )
        .unwrap(),
        vec![
            vec![Datum::Int(1), Datum::Int(1)],
            vec![Datum::Int(2), Datum::Int(1)],
        ]
    );
    // Empty-input rules through SQL: global agg over no rows -> one row.
    assert_eq!(
        run_select_on(
            "SELECT COUNT(a) FROM t WHERE a > 100",
            &catalog,
            &crate::StmtContext::for_query()
        )
        .unwrap(),
        vec![vec![Datum::Int(0)]]
    );
    assert_eq!(
        run_select_on(
            "SELECT a, COUNT(*) FROM t WHERE a > 100 GROUP BY a",
            &catalog,
            &crate::StmtContext::for_query()
        )
        .unwrap(),
        Vec::<Vec<Datum>>::new()
    );
    // MIN/MAX over the shared datum ordering.
    assert_eq!(
        run_select_on(
            "SELECT MIN(a), MAX(b) FROM t",
            &catalog,
            &crate::StmtContext::for_query()
        )
        .unwrap(),
        vec![vec![Datum::Int(1), Datum::Int(30)]]
    );
    // AVG over integers is DECIMAL, scaled by div_precision_increment.
    assert_eq!(
        run_select_on(
            "SELECT AVG(a) FROM t",
            &catalog,
            &crate::StmtContext::for_query()
        )
        .unwrap(),
        vec![vec![Datum::Decimal(tidb_datatype::Decimal::from_literal(
            "2.0000"
        ))]]
    );
    // DISTINCT folds repeated inputs once per group: a is 1,2,3 while the
    // constant 1 collapses to a single counted value.
    assert_eq!(
        run_select_on(
            "SELECT COUNT(DISTINCT a), COUNT(DISTINCT 1) FROM t",
            &catalog,
            &crate::StmtContext::for_query()
        )
        .unwrap(),
        vec![vec![Datum::Int(3), Datum::Int(1)]]
    );
    // An all-NULL / empty group is NULL for MIN/MAX and AVG, as in Go.
    assert_eq!(
        run_select_on(
            "SELECT MIN(a), MAX(a), AVG(a) FROM t WHERE a > 100",
            &catalog,
            &crate::StmtContext::for_query()
        )
        .unwrap(),
        vec![vec![Datum::Null, Datum::Null, Datum::Null]]
    );
}

/// HAVING filters aggregate output rows, ORDER BY sorts them, and an
/// aggregate that appears only in those clauses is computed as a hidden
/// column and trimmed from the result (Go's resolveHavingAndOrderBy plus
/// the final projection).
#[test]
fn aggregate_having_and_order_by() {
    let mut catalog = test_catalog();
    crate::run_create_table_on("CREATE TABLE g (a BIGINT, b BIGINT)", &mut catalog).unwrap();
    run_insert_on(
        "INSERT INTO g VALUES (1, 10), (1, 20), (2, 5), (3, 7), (3, 8)",
        &mut catalog,
        &crate::StmtContext::for_query(),
    )
    .unwrap();

    // HAVING over an aggregate that IS in the select list.
    assert_eq!(
        run_select_on(
            "SELECT a, COUNT(*) FROM g GROUP BY a HAVING COUNT(*) > 1",
            &catalog,
            &crate::StmtContext::for_query()
        )
        .unwrap(),
        vec![
            vec![Datum::Int(1), Datum::Int(2)],
            vec![Datum::Int(3), Datum::Int(2)],
        ]
    );
    // HAVING over an aggregate that is NOT selected: one output column.
    assert_eq!(
        run_select_on(
            "SELECT a FROM g GROUP BY a HAVING SUM(b) > 15",
            &catalog,
            &crate::StmtContext::for_query()
        )
        .unwrap(),
        vec![vec![Datum::Int(1)]]
    );
    // ORDER BY an aggregate that is not selected, descending.
    assert_eq!(
        run_select_on(
            "SELECT a FROM g GROUP BY a ORDER BY SUM(b) DESC",
            &catalog,
            &crate::StmtContext::for_query()
        )
        .unwrap(),
        vec![
            vec![Datum::Int(1)],
            vec![Datum::Int(3)],
            vec![Datum::Int(2)]
        ]
    );
    // HAVING and ORDER BY together, with LIMIT applied after both.
    assert_eq!(
        run_select_on(
            "SELECT a, SUM(b) FROM g GROUP BY a HAVING COUNT(*) > 1 ORDER BY SUM(b) LIMIT 1",
            &catalog,
            &crate::StmtContext::for_query()
        )
        .unwrap(),
        vec![vec![
            Datum::Int(3),
            Datum::Decimal(tidb_datatype::Decimal::from_int(15))
        ]]
    );
    // ORDER BY a selected alias.
    assert_eq!(
        run_select_on(
            "SELECT a, SUM(b) AS total FROM g GROUP BY a ORDER BY total",
            &catalog,
            &crate::StmtContext::for_query()
        )
        .unwrap(),
        vec![
            vec![
                Datum::Int(2),
                Datum::Decimal(tidb_datatype::Decimal::from_int(5))
            ],
            vec![
                Datum::Int(3),
                Datum::Decimal(tidb_datatype::Decimal::from_int(15))
            ],
            vec![
                Datum::Int(1),
                Datum::Decimal(tidb_datatype::Decimal::from_int(30))
            ],
        ]
    );
    // A grouped column that is not selected is still visible to HAVING
    // and ORDER BY (Go carries it as a hidden FIRST_ROW column).
    assert_eq!(
        run_select_on(
            "SELECT COUNT(*) FROM g GROUP BY a HAVING a > 1",
            &catalog,
            &crate::StmtContext::for_query()
        )
        .unwrap(),
        vec![vec![Datum::Int(1)], vec![Datum::Int(2)]]
    );
    // A global aggregate's HAVING filters the single group.
    assert_eq!(
        run_select_on(
            "SELECT COUNT(*) FROM g HAVING COUNT(*) > 100",
            &catalog,
            &crate::StmtContext::for_query()
        )
        .unwrap(),
        Vec::<Vec<Datum>>::new()
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

/// Two-table joins: inner, left/right outer with NULL padding, the
/// ON-vs-WHERE distinction, qualified and ambiguous column references,
/// wildcard expansion, and a three-table left-deep chain.
#[test]
fn joins() {
    let mut catalog = Catalog::default();
    crate::run_create_table_on("CREATE TABLE l (id BIGINT, v BIGINT)", &mut catalog).unwrap();
    crate::run_create_table_on("CREATE TABLE r (id BIGINT, w BIGINT)", &mut catalog).unwrap();
    run_insert_on(
        "INSERT INTO l VALUES (1, 10), (2, 20), (3, 30)",
        &mut catalog,
        &crate::StmtContext::for_query(),
    )
    .unwrap();
    run_insert_on(
        "INSERT INTO r VALUES (1, 100), (3, 300), (3, 301)",
        &mut catalog,
        &crate::StmtContext::for_query(),
    )
    .unwrap();

    // INNER JOIN: only matches, and a left row matching twice emits twice.
    assert_eq!(
        run_select_on(
            "SELECT l.id, l.v, r.w FROM l JOIN r ON l.id = r.id",
            &catalog,
            &crate::StmtContext::for_query()
        )
        .unwrap(),
        vec![
            vec![Datum::Int(1), Datum::Int(10), Datum::Int(100)],
            vec![Datum::Int(3), Datum::Int(30), Datum::Int(300)],
            vec![Datum::Int(3), Datum::Int(30), Datum::Int(301)],
        ]
    );

    // LEFT JOIN pads the unmatched left row with NULLs.
    assert_eq!(
        run_select_on(
            "SELECT l.id, r.w FROM l LEFT JOIN r ON l.id = r.id",
            &catalog,
            &crate::StmtContext::for_query()
        )
        .unwrap(),
        vec![
            vec![Datum::Int(1), Datum::Int(100)],
            vec![Datum::Int(2), Datum::Null],
            vec![Datum::Int(3), Datum::Int(300)],
            vec![Datum::Int(3), Datum::Int(301)],
        ]
    );

    // The ON/WHERE distinction: filtering the padded rows is an anti-join.
    assert_eq!(
        run_select_on(
            "SELECT l.id FROM l LEFT JOIN r ON l.id = r.id WHERE r.id IS NULL",
            &catalog,
            &crate::StmtContext::for_query()
        )
        .unwrap(),
        vec![vec![Datum::Int(2)]]
    );
    // A condition in ON does NOT drop the left row; it only stops matching.
    assert_eq!(
        run_select_on(
            "SELECT l.id, r.w FROM l LEFT JOIN r ON l.id = r.id AND r.w > 200",
            &catalog,
            &crate::StmtContext::for_query()
        )
        .unwrap(),
        vec![
            vec![Datum::Int(1), Datum::Null],
            vec![Datum::Int(2), Datum::Null],
            vec![Datum::Int(3), Datum::Int(300)],
            vec![Datum::Int(3), Datum::Int(301)],
        ]
    );

    // RIGHT JOIN keeps every right row, padding the left side.
    assert_eq!(
        run_select_on(
            "SELECT l.v, r.id FROM l RIGHT JOIN r ON l.id = r.id AND l.v > 100",
            &catalog,
            &crate::StmtContext::for_query()
        )
        .unwrap(),
        vec![
            vec![Datum::Null, Datum::Int(1)],
            vec![Datum::Null, Datum::Int(3)],
            vec![Datum::Null, Datum::Int(3)],
        ]
    );

    // A comma join with no ON is a Cartesian product.
    assert_eq!(
        run_select_on(
            "SELECT l.id FROM l, r",
            &catalog,
            &crate::StmtContext::for_query()
        )
        .unwrap()
        .len(),
        9
    );

    // `*` expands across both tables in FROM order; `t.*` over one.
    assert_eq!(
        run_select_on(
            "SELECT * FROM l JOIN r ON l.id = r.id",
            &catalog,
            &crate::StmtContext::for_query()
        )
        .unwrap()
        .first()
        .unwrap()
        .len(),
        4
    );
    assert_eq!(
        run_select_on(
            "SELECT r.* FROM l JOIN r ON l.id = r.id",
            &catalog,
            &crate::StmtContext::for_query()
        )
        .unwrap()
        .first()
        .unwrap()
        .len(),
        2
    );

    // An unqualified column present in both tables is ambiguous, as in
    // MySQL; one present in only one table resolves.
    assert!(run_select_on(
        "SELECT id FROM l JOIN r ON l.id = r.id",
        &catalog,
        &crate::StmtContext::for_query()
    )
    .is_err());
    assert_eq!(
        run_select_on(
            "SELECT v, w FROM l JOIN r ON l.id = r.id",
            &catalog,
            &crate::StmtContext::for_query()
        )
        .unwrap()
        .len(),
        3
    );

    // An alias replaces the table name for qualification.
    assert_eq!(
        run_select_on(
            "SELECT a.id FROM l AS a JOIN r AS b ON a.id = b.id",
            &catalog,
            &crate::StmtContext::for_query()
        )
        .unwrap()
        .len(),
        3
    );

    // A three-table left-deep chain, and an aggregate over a join.
    crate::run_create_table_on("CREATE TABLE m (id BIGINT)", &mut catalog).unwrap();
    run_insert_on(
        "INSERT INTO m VALUES (3)",
        &mut catalog,
        &crate::StmtContext::for_query(),
    )
    .unwrap();
    assert_eq!(
        run_select_on(
            "SELECT COUNT(*) FROM l JOIN r ON l.id = r.id JOIN m ON m.id = r.id",
            &catalog,
            &crate::StmtContext::for_query()
        )
        .unwrap(),
        vec![vec![Datum::Int(2)]]
    );

    // A coalesced join reports `id` ONCE, so `*` is one column narrower
    // than the same join written with an `ON`, and the unqualified `id`
    // that is ambiguous above resolves here. See
    // `tidb_session`'s `tests_coalesced_joins` for the full rule set.
    for sql in [
        "SELECT * FROM l NATURAL JOIN r",
        "SELECT * FROM l JOIN r USING (id)",
    ] {
        assert_eq!(
            run_select_on(sql, &catalog, &crate::StmtContext::for_query())
                .unwrap()
                .first()
                .unwrap()
                .len(),
            3,
            "{sql}"
        );
    }
    assert_eq!(
        run_select_on(
            "SELECT id FROM l JOIN r USING (id)",
            &catalog,
            &crate::StmtContext::for_query()
        )
        .unwrap()
        .len(),
        3
    );
}

/// Uncorrelated subqueries are evaluated and folded into literals, the way
/// Go's handleScalarSubquery does for the non-Apply case.
#[test]
fn subqueries() {
    let mut catalog = Catalog::default();
    crate::run_create_table_on("CREATE TABLE s (a BIGINT, b BIGINT)", &mut catalog).unwrap();
    crate::run_create_table_on("CREATE TABLE u (a BIGINT)", &mut catalog).unwrap();
    run_insert_on(
        "INSERT INTO s VALUES (1, 10), (2, 20), (3, 30)",
        &mut catalog,
        &crate::StmtContext::for_query(),
    )
    .unwrap();
    run_insert_on(
        "INSERT INTO u VALUES (2), (3)",
        &mut catalog,
        &crate::StmtContext::for_query(),
    )
    .unwrap();

    // A scalar subquery in the select list and in a predicate.
    assert_eq!(
        run_select_on(
            "SELECT (SELECT MAX(b) FROM s)",
            &catalog,
            &crate::StmtContext::for_query()
        )
        .unwrap(),
        vec![vec![Datum::Int(30)]]
    );
    assert_eq!(
        run_select_on(
            "SELECT a FROM s WHERE b = (SELECT MAX(b) FROM s)",
            &catalog,
            &crate::StmtContext::for_query()
        )
        .unwrap(),
        vec![vec![Datum::Int(3)]]
    );

    // No rows is NULL, as Go's buildMaxOneRow leaves it.
    assert_eq!(
        run_select_on(
            "SELECT (SELECT a FROM s WHERE a > 100)",
            &catalog,
            &crate::StmtContext::for_query()
        )
        .unwrap(),
        vec![vec![Datum::Null]]
    );
    // More than one row is Go's ER_SUBQUERY_NO_1_ROW.
    assert!(matches!(
        run_select_on(
            "SELECT (SELECT a FROM s)",
            &catalog,
            &crate::StmtContext::for_query()
        ),
        Err(DriverError::SubqueryReturnsMoreThanOneRow)
    ));

    // IN / NOT IN over a subquery.
    assert_eq!(
        run_select_on(
            "SELECT a FROM s WHERE a IN (SELECT a FROM u)",
            &catalog,
            &crate::StmtContext::for_query()
        )
        .unwrap(),
        vec![vec![Datum::Int(2)], vec![Datum::Int(3)]]
    );
    assert_eq!(
        run_select_on(
            "SELECT a FROM s WHERE a NOT IN (SELECT a FROM u)",
            &catalog,
            &crate::StmtContext::for_query()
        )
        .unwrap(),
        vec![vec![Datum::Int(1)]]
    );
    // An empty IN subquery matches nothing, and NOT IN over it matches all.
    assert_eq!(
        run_select_on(
            "SELECT a FROM s WHERE a IN (SELECT a FROM u WHERE a > 100)",
            &catalog,
            &crate::StmtContext::for_query()
        )
        .unwrap(),
        Vec::<Vec<Datum>>::new()
    );
    assert_eq!(
        run_select_on(
            "SELECT a FROM s WHERE a NOT IN (SELECT a FROM u WHERE a > 100)",
            &catalog,
            &crate::StmtContext::for_query()
        )
        .unwrap()
        .len(),
        3
    );

    // EXISTS / NOT EXISTS fold to 1 / 0.
    assert_eq!(
        run_select_on(
            "SELECT a FROM s WHERE EXISTS (SELECT 1 FROM u)",
            &catalog,
            &crate::StmtContext::for_query()
        )
        .unwrap()
        .len(),
        3
    );
    assert_eq!(
        run_select_on(
            "SELECT a FROM s WHERE NOT EXISTS (SELECT 1 FROM u)",
            &catalog,
            &crate::StmtContext::for_query()
        )
        .unwrap(),
        Vec::<Vec<Datum>>::new()
    );

    // A subquery in HAVING, over the aggregate path.
    assert_eq!(
        run_select_on(
            "SELECT a FROM s GROUP BY a HAVING SUM(b) > (SELECT MIN(b) FROM s)",
            &catalog,
            &crate::StmtContext::for_query()
        )
        .unwrap(),
        vec![vec![Datum::Int(2)], vec![Datum::Int(3)]]
    );

    // ANY is the OR chain over the folded values, ALL the AND chain:
    // `a > ANY (2, 3)` holds only for 3, and `a > ALL (2, 3)` for nothing.
    assert_eq!(
        run_select_on(
            "SELECT a FROM s WHERE a > ANY (SELECT a FROM u)",
            &catalog,
            &crate::StmtContext::for_query()
        )
        .unwrap(),
        vec![vec![Datum::Int(3)]]
    );
    assert_eq!(
        run_select_on(
            "SELECT a FROM s WHERE a > ALL (SELECT a FROM u)",
            &catalog,
            &crate::StmtContext::for_query()
        )
        .unwrap(),
        Vec::<Vec<Datum>>::new()
    );
    // An empty inner result: ALL is vacuously true, ANY is false.
    assert_eq!(
        run_select_on(
            "SELECT a FROM s WHERE a > ALL (SELECT a FROM u WHERE a > 100)",
            &catalog,
            &crate::StmtContext::for_query()
        )
        .unwrap(),
        vec![
            vec![Datum::Int(1)],
            vec![Datum::Int(2)],
            vec![Datum::Int(3)]
        ]
    );
    assert_eq!(
        run_select_on(
            "SELECT a FROM s WHERE a > ANY (SELECT a FROM u WHERE a > 100)",
            &catalog,
            &crate::StmtContext::for_query()
        )
        .unwrap(),
        Vec::<Vec<Datum>>::new()
    );
}

/// A correlated subquery becomes an Apply: the inner query re-runs once
/// per outer row with the outer row's values bound, which is Go's
/// NestedLoopApplyExec loop.
#[test]
fn correlated_subqueries() {
    let mut catalog = Catalog::default();
    crate::run_create_table_on("CREATE TABLE o (id BIGINT, v BIGINT)", &mut catalog).unwrap();
    crate::run_create_table_on("CREATE TABLE i (id BIGINT, w BIGINT)", &mut catalog).unwrap();
    run_insert_on(
        "INSERT INTO o VALUES (1, 10), (2, 20), (3, 30)",
        &mut catalog,
        &crate::StmtContext::for_query(),
    )
    .unwrap();
    run_insert_on(
        "INSERT INTO i VALUES (1, 10), (2, 5), (2, 25), (4, 40)",
        &mut catalog,
        &crate::StmtContext::for_query(),
    )
    .unwrap();

    // Scalar: each outer row compares against its own inner maximum.
    assert_eq!(
        run_select_on(
            "SELECT id FROM o WHERE v = (SELECT MAX(w) FROM i WHERE i.id = o.id)",
            &catalog,
            &crate::StmtContext::for_query()
        )
        .unwrap(),
        vec![vec![Datum::Int(1)]]
    );
    // id 2's inner rows are 5 and 25, so its max is 25 and 20 < 25 holds;
    // id 1 compares 10 < 10, which does not.
    assert_eq!(
        run_select_on(
            "SELECT id FROM o WHERE v < (SELECT MAX(w) FROM i WHERE i.id = o.id)",
            &catalog,
            &crate::StmtContext::for_query()
        )
        .unwrap(),
        vec![vec![Datum::Int(2)]]
    );
    // An outer row whose inner query returns nothing compares against
    // NULL, so the predicate is unknown and the row drops -- id 3 has no
    // matching inner rows.
    assert_eq!(
        run_select_on(
            "SELECT id FROM o WHERE (SELECT MAX(w) FROM i WHERE i.id = o.id) IS NULL",
            &catalog,
            &crate::StmtContext::for_query()
        )
        .unwrap(),
        vec![vec![Datum::Int(3)]]
    );

    // Correlated EXISTS / NOT EXISTS, the semi- and anti-join shapes.
    assert_eq!(
        run_select_on(
            "SELECT id FROM o WHERE EXISTS (SELECT 1 FROM i WHERE i.id = o.id)",
            &catalog,
            &crate::StmtContext::for_query()
        )
        .unwrap(),
        vec![vec![Datum::Int(1)], vec![Datum::Int(2)]]
    );
    assert_eq!(
        run_select_on(
            "SELECT id FROM o WHERE NOT EXISTS (SELECT 1 FROM i WHERE i.id = o.id)",
            &catalog,
            &crate::StmtContext::for_query()
        )
        .unwrap(),
        vec![vec![Datum::Int(3)]]
    );

    // An unqualified inner reference to an outer column still correlates.
    assert_eq!(
        run_select_on(
            "SELECT id FROM o WHERE EXISTS (SELECT 1 FROM i WHERE i.w = v)",
            &catalog,
            &crate::StmtContext::for_query()
        )
        .unwrap(),
        vec![vec![Datum::Int(1)]]
    );

    // A correlated subquery returning several rows is still the 1242 case,
    // raised from inside the apply loop and reported as the same error the
    // folded path reports.
    assert!(matches!(
        run_select_on(
            "SELECT id FROM o WHERE v = (SELECT w FROM i WHERE i.id = o.id)",
            &catalog,
            &crate::StmtContext::for_query()
        ),
        Err(DriverError::SubqueryReturnsMoreThanOneRow)
    ));

    // Correlated IN / NOT IN and ANY / ALL: the same Apply, folding this
    // outer row's inner result into the three-valued answer. id 3's inner
    // result is EMPTY, which is why NOT IN and ALL keep it.
    assert_eq!(
        run_select_on(
            "SELECT id FROM o WHERE v IN (SELECT w FROM i WHERE i.id = o.id)",
            &catalog,
            &crate::StmtContext::for_query()
        )
        .unwrap(),
        vec![vec![Datum::Int(1)]]
    );
    assert_eq!(
        run_select_on(
            "SELECT id FROM o WHERE v NOT IN (SELECT w FROM i WHERE i.id = o.id)",
            &catalog,
            &crate::StmtContext::for_query()
        )
        .unwrap(),
        vec![vec![Datum::Int(2)], vec![Datum::Int(3)]]
    );
    assert_eq!(
        run_select_on(
            "SELECT id FROM o WHERE v > ANY (SELECT w FROM i WHERE i.id = o.id)",
            &catalog,
            &crate::StmtContext::for_query()
        )
        .unwrap(),
        vec![vec![Datum::Int(2)]]
    );
    assert_eq!(
        run_select_on(
            "SELECT id FROM o WHERE v > ALL (SELECT w FROM i WHERE i.id = o.id)",
            &catalog,
            &crate::StmtContext::for_query()
        )
        .unwrap(),
        vec![vec![Datum::Int(3)]]
    );
}

/// A correlated subquery nested inside a larger aggregate-path
/// expression: arithmetic over an aggregate in the select list, and a
/// comparison against an aggregate in `HAVING`. The Apply sits above the
/// aggregation (Go's plan shape), so the subquery sees the GROUPED value
/// and runs once per group.
///
/// Every result here was cross-checked against a
/// `testkit.CreateMockStore` capture of real TiDB on the same schema.
#[test]
fn grouped_correlated_subqueries() {
    let mut catalog = Catalog::default();
    crate::run_create_table_on("CREATE TABLE t (g BIGINT, v BIGINT)", &mut catalog).unwrap();
    crate::run_create_table_on("CREATE TABLE s (k BIGINT, x BIGINT)", &mut catalog).unwrap();
    run_insert_on(
        "INSERT INTO t VALUES (1, 10), (1, 20), (2, 5), (3, 100), (NULL, 7)",
        &mut catalog,
        &crate::StmtContext::for_query(),
    )
    .unwrap();
    run_insert_on(
        "INSERT INTO s VALUES (1, 1), (1, 2), (2, 3)",
        &mut catalog,
        &crate::StmtContext::for_query(),
    )
    .unwrap();

    // A correlated scalar subquery combined with an aggregate by
    // arithmetic in the select list: SUM(v) is the group's own total,
    // (SELECT COUNT...) reads how many `s` rows share the group's key.
    assert_eq!(
        run_select_on(
            "SELECT g, SUM(v) + (SELECT COUNT(*) FROM s WHERE s.k = t.g) \
             FROM t GROUP BY g ORDER BY g",
            &catalog,
            &crate::StmtContext::for_query()
        )
        .unwrap(),
        vec![
            vec![
                Datum::Null,
                Datum::Decimal(tidb_datatype::Decimal::from_int(7))
            ],
            vec![
                Datum::Int(1),
                Datum::Decimal(tidb_datatype::Decimal::from_int(32))
            ],
            vec![
                Datum::Int(2),
                Datum::Decimal(tidb_datatype::Decimal::from_int(6))
            ],
            vec![
                Datum::Int(3),
                Datum::Decimal(tidb_datatype::Decimal::from_int(100))
            ],
        ]
    );

    // A correlated scalar subquery compared against an aggregate in
    // HAVING: only groups whose SUM(v) beats the correlated average
    // survive.
    assert_eq!(
        run_select_on(
            "SELECT g FROM t GROUP BY g \
             HAVING SUM(v) > (SELECT AVG(x) FROM s WHERE s.k = t.g) \
             ORDER BY g",
            &catalog,
            &crate::StmtContext::for_query()
        )
        .unwrap(),
        vec![vec![Datum::Int(1)], vec![Datum::Int(2)]]
    );

    // The same HAVING subquery, ANDed with a plain grouped-column
    // predicate -- both conjuncts must be readable off the same
    // post-Apply row.
    assert_eq!(
        run_select_on(
            "SELECT g, SUM(v) FROM t GROUP BY g \
             HAVING SUM(v) > (SELECT COUNT(*) FROM s WHERE s.k = t.g) AND g IS NOT NULL \
             ORDER BY g",
            &catalog,
            &crate::StmtContext::for_query()
        )
        .unwrap(),
        vec![
            vec![
                Datum::Int(1),
                Datum::Decimal(tidb_datatype::Decimal::from_int(30))
            ],
            vec![
                Datum::Int(2),
                Datum::Decimal(tidb_datatype::Decimal::from_int(5))
            ],
            vec![
                Datum::Int(3),
                Datum::Decimal(tidb_datatype::Decimal::from_int(100))
            ],
        ]
    );

    // HAVING a correlated subquery against a bare (unaggregated) GROUP
    // BY column, with a NULL group in the mix.
    assert_eq!(
        run_select_on(
            "SELECT g, COUNT(*) FROM t GROUP BY g \
             HAVING (SELECT COUNT(*) FROM s WHERE s.k = g) >= 0 \
             ORDER BY g",
            &catalog,
            &crate::StmtContext::for_query()
        )
        .unwrap(),
        vec![
            vec![Datum::Null, Datum::Int(1)],
            vec![Datum::Int(1), Datum::Int(2)],
            vec![Datum::Int(2), Datum::Int(1)],
            vec![Datum::Int(3), Datum::Int(1)],
        ]
    );

    // DEFERRED (documented, not silently wrong): a correlated subquery
    // inside an AGGREGATE'S OWN ARGUMENT needs a per-SOURCE-ROW Apply
    // below the aggregation, not the per-GROUP Apply above it this
    // driver builds -- refused precisely rather than mis-evaluated.
    assert!(matches!(
        run_select_on(
            "SELECT g, SUM((SELECT COUNT(*) FROM s WHERE s.k = g)) FROM t GROUP BY g",
            &catalog,
            &crate::StmtContext::for_query()
        ),
        Err(DriverError::Unsupported(_))
    ));
    assert!(matches!(
        run_select_on(
            "SELECT g, SUM(CASE WHEN EXISTS(SELECT 1 FROM s WHERE s.k = t.g) THEN v ELSE 0 END) \
             FROM t GROUP BY g",
            &catalog,
            &crate::StmtContext::for_query()
        ),
        Err(DriverError::Exec(_))
    ));

    // A HAVING clause referencing a non-grouped, non-aggregated column
    // stays refused even with a correlated subquery alongside it -- the
    // subquery does not launder the column reference. Captured from
    // TiDB, this is `ErrUnknownColumn` naming the `having clause` (HAVING
    // resolves against the aggregation's output), in every sql_mode.
    assert!(matches!(
        run_select_on(
            "SELECT g, SUM(v) FROM t GROUP BY g \
             HAVING v > (SELECT AVG(x) FROM s WHERE s.k = t.g)",
            &catalog,
            &crate::StmtContext::for_query()
        ),
        Err(DriverError::UnknownColumnInClause { .. })
    ));

    // DEFERRED: two-level nesting (a correlated subquery whose own body
    // contains a subquery correlated to ITS outer scope) is refused
    // rather than mis-evaluated.
    assert!(run_select_on(
        "SELECT g, (SELECT COUNT(*) FROM s WHERE s.k = t.g \
         AND s.x > (SELECT AVG(x) FROM s s2 WHERE s2.k = s.k)) FROM t GROUP BY g",
        &catalog,
        &crate::StmtContext::for_query()
    )
    .is_err());
}

/// A single-column integer PRIMARY KEY becomes the row handle (Go's
/// TableInfo.PKIsHandle), so the key value addresses the row and a repeat
/// is ErrDupEntry. Transcreated from Go's own duplicate-key behavior in
/// pkg/table/tables `AddRecord`.
#[test]
fn integer_primary_key_is_the_row_handle() {
    for ddl in [
        "CREATE TABLE p (id BIGINT PRIMARY KEY, v BIGINT)",
        "CREATE TABLE p (id BIGINT, v BIGINT, PRIMARY KEY (id))",
    ] {
        let mut catalog = Catalog::default();
        crate::run_create_table_on(ddl, &mut catalog).unwrap();
        run_insert_on(
            "INSERT INTO p VALUES (10, 100), (20, 200)",
            &mut catalog,
            &crate::StmtContext::for_query(),
        )
        .unwrap();

        // The rows come back in handle order, which is the key's order --
        // not insertion order, because the handle IS the primary key.
        run_insert_on(
            "INSERT INTO p VALUES (5, 50)",
            &mut catalog,
            &crate::StmtContext::for_query(),
        )
        .unwrap();
        assert_eq!(
            run_select_on(
                "SELECT id FROM p",
                &catalog,
                &crate::StmtContext::for_query()
            )
            .unwrap(),
            vec![
                vec![Datum::Int(5)],
                vec![Datum::Int(10)],
                vec![Datum::Int(20)],
            ],
            "{ddl}"
        );

        // A repeated key is Go's ErrDupEntry.
        assert!(
            matches!(
                run_insert_on(
                    "INSERT INTO p VALUES (10, 999)",
                    &mut catalog,
                    &crate::StmtContext::for_query()
                ),
                Err(DriverError::DuplicateEntry { .. })
            ),
            "{ddl}"
        );
        // The failed insert left the original row untouched.
        assert_eq!(
            run_select_on(
                "SELECT v FROM p WHERE id = 10",
                &catalog,
                &crate::StmtContext::for_query()
            )
            .unwrap(),
            vec![vec![Datum::Int(100)]],
            "{ddl}"
        );
        // A negative key works too: the key codec sign-flips handles.
        run_insert_on(
            "INSERT INTO p VALUES (-1, 1)",
            &mut catalog,
            &crate::StmtContext::for_query(),
        )
        .unwrap();
        assert_eq!(
            run_select_on(
                "SELECT id FROM p",
                &catalog,
                &crate::StmtContext::for_query()
            )
            .unwrap()
            .len(),
            4,
            "{ddl}"
        );
    }
}

/// Without a primary key the handle is the allocated row id, so repeated
/// values are fine -- the table is a heap, as in Go with _tidb_rowid.
#[test]
fn without_a_primary_key_rows_repeat_freely() {
    let mut catalog = Catalog::default();
    crate::run_create_table_on("CREATE TABLE h (a BIGINT)", &mut catalog).unwrap();
    run_insert_on(
        "INSERT INTO h VALUES (1), (1), (1)",
        &mut catalog,
        &crate::StmtContext::for_query(),
    )
    .unwrap();
    assert_eq!(
        run_select_on(
            "SELECT a FROM h",
            &catalog,
            &crate::StmtContext::for_query()
        )
        .unwrap()
        .len(),
        3
    );
}

/// Constraint shapes that need tiers this seed lacks are rejected rather
/// than silently dropped, so a table never claims what it cannot enforce.
#[test]
fn unsupported_constraints_are_rejected() {
    let mut catalog = Catalog::default();
    for ddl in [
        // Two primary keys is not a table.
        "CREATE TABLE c (a BIGINT PRIMARY KEY, b BIGINT PRIMARY KEY)",
        // A prefix-length primary key needs prefix index support.
        "CREATE TABLE c (a VARCHAR(10), PRIMARY KEY (a(3)))",
    ] {
        assert!(
            crate::run_create_table_on(ddl, &mut catalog).is_err(),
            "{ddl} should be rejected"
        );
    }
}

/// A non-integer primary key is not a handle -- Go only sets PKIsHandle
/// for a single integer column -- so the table keeps allocating row ids
/// and enforces the key through a unique index instead.
#[test]
fn a_non_integer_primary_key_is_enforced_by_its_index() {
    let mut catalog = Catalog::default();
    crate::run_create_table_on("CREATE TABLE s (k VARCHAR(10) PRIMARY KEY)", &mut catalog).unwrap();
    run_insert_on(
        "INSERT INTO s VALUES ('a'), ('b')",
        &mut catalog,
        &crate::StmtContext::for_query(),
    )
    .unwrap();
    assert_eq!(
        run_select_on(
            "SELECT k FROM s",
            &catalog,
            &crate::StmtContext::for_query()
        )
        .unwrap()
        .len(),
        2
    );
    // The duplicate is now caught by the index, as in real TiDB.
    assert!(matches!(
        run_insert_on(
            "INSERT INTO s VALUES ('a')",
            &mut catalog,
            &crate::StmtContext::for_query()
        ),
        Err(DriverError::DuplicateEntry { .. })
    ));
}

/// The text of a string datum, however the codec chose to represent it.
fn datum_text_for_test(value: &Datum) -> String {
    match value {
        Datum::Bytes(bytes) => String::from_utf8_lossy(bytes).into_owned(),
        Datum::String(text) => String::from_utf8_lossy(text.bytes()).into_owned(),
        other => panic!("expected a string datum, got {other:?}"),
    }
}

/// UNIQUE indexes are enforced on every write path, and MySQL's rule that
/// a unique index permits any number of NULLs is Go's `distinct` flag:
/// an entry with a NULL indexed value is stored the non-distinct way and
/// never collides.
#[test]
fn unique_indexes() {
    let mut catalog = Catalog::default();
    crate::run_create_table_on(
        "CREATE TABLE u (id BIGINT PRIMARY KEY, email VARCHAR(32) UNIQUE, v BIGINT)",
        &mut catalog,
    )
    .unwrap();
    run_insert_on(
        "INSERT INTO u VALUES (1, 'a@x', 10), (2, 'b@x', 20)",
        &mut catalog,
        &crate::StmtContext::for_query(),
    )
    .unwrap();

    // A repeated unique value is rejected, naming the index.
    match run_insert_on(
        "INSERT INTO u VALUES (3, 'a@x', 30)",
        &mut catalog,
        &crate::StmtContext::for_query(),
    ) {
        Err(DriverError::DuplicateEntry { value, key }) => {
            assert_eq!(value, "a@x");
            // Captured from TiDB: the key is qualified table.index, as in
            // "Duplicate entry 'a' for key 'm.code'".
            assert_eq!(key, "u.email");
        }
        other => panic!("expected a duplicate-entry error, got {other:?}"),
    }
    // The rejected insert wrote nothing.
    assert_eq!(
        run_select_on(
            "SELECT id FROM u",
            &catalog,
            &crate::StmtContext::for_query()
        )
        .unwrap()
        .len(),
        2
    );

    // UPDATE is checked too, and a rejected update leaves the row alone.
    assert!(matches!(
        run_update_on(
            "UPDATE u SET email = 'a@x' WHERE id = 2",
            &mut catalog,
            &crate::StmtContext::for_query()
        ),
        Err(DriverError::DuplicateEntry { .. })
    ));
    assert_eq!(
        datum_text_for_test(
            &run_select_on(
                "SELECT email FROM u WHERE id = 2",
                &catalog,
                &crate::StmtContext::for_query()
            )
            .unwrap()[0][0]
        ),
        "b@x"
    );
    // An update that frees a value lets another row take it.
    run_update_on(
        "UPDATE u SET email = 'c@x' WHERE id = 1",
        &mut catalog,
        &crate::StmtContext::for_query(),
    )
    .unwrap();
    run_insert_on(
        "INSERT INTO u VALUES (4, 'a@x', 40)",
        &mut catalog,
        &crate::StmtContext::for_query(),
    )
    .unwrap();

    // DELETE frees the value as well.
    run_delete_on(
        "DELETE FROM u WHERE id = 4",
        &mut catalog,
        &crate::StmtContext::for_query(),
    )
    .unwrap();
    run_insert_on(
        "INSERT INTO u VALUES (5, 'a@x', 50)",
        &mut catalog,
        &crate::StmtContext::for_query(),
    )
    .unwrap();

    // MySQL permits many NULLs in a unique index.
    run_insert_on(
        "INSERT INTO u VALUES (6, NULL, 60)",
        &mut catalog,
        &crate::StmtContext::for_query(),
    )
    .unwrap();
    run_insert_on(
        "INSERT INTO u VALUES (7, NULL, 70)",
        &mut catalog,
        &crate::StmtContext::for_query(),
    )
    .unwrap();
    assert_eq!(
        run_select_on(
            "SELECT id FROM u",
            &catalog,
            &crate::StmtContext::for_query()
        )
        .unwrap()
        .len(),
        5
    );
}

/// A non-unique index accepts repeats: its key carries the handle, so two
/// rows with the same value are two entries (Go's non-distinct path).
#[test]
fn a_non_unique_index_accepts_repeats() {
    let mut catalog = Catalog::default();
    crate::run_create_table_on(
        "CREATE TABLE n (id BIGINT PRIMARY KEY, tag VARCHAR(8), KEY tag_idx (tag))",
        &mut catalog,
    )
    .unwrap();
    run_insert_on(
        "INSERT INTO n VALUES (1, 'x'), (2, 'x'), (3, 'y')",
        &mut catalog,
        &crate::StmtContext::for_query(),
    )
    .unwrap();
    assert_eq!(
        run_select_on(
            "SELECT id FROM n",
            &catalog,
            &crate::StmtContext::for_query()
        )
        .unwrap()
        .len(),
        3
    );
}

/// A unique index stores the handle as its value, which is what makes a
/// unique-key lookup a point read (Go's PointGetPlan on a unique key).
#[test]
fn a_unique_index_entry_points_at_its_row() {
    let mut catalog = Catalog::default();
    crate::run_create_table_on(
        "CREATE TABLE k (id BIGINT PRIMARY KEY, code VARCHAR(8) UNIQUE)",
        &mut catalog,
    )
    .unwrap();
    run_insert_on(
        "INSERT INTO k VALUES (7, 'abc'), (8, 'def')",
        &mut catalog,
        &crate::StmtContext::for_query(),
    )
    .unwrap();

    let Some(TableEntry::Kv(table)) = catalog.get_table_for_test("k") else {
        panic!("expected a kv table");
    };
    let mut table = table.clone();
    let index_id = table
        .indexes()
        .iter()
        .find(|index| index.name == "code")
        .expect("the unique index exists")
        .id;
    assert_eq!(
        table
            .lookup_unique(index_id, &[Datum::Bytes(b"abc".to_vec())])
            .unwrap(),
        Some(TableHandle::Int(7)),
        "the entry carries the row's handle"
    );
    assert_eq!(
        table
            .lookup_unique(index_id, &[Datum::Bytes(b"nope".to_vec())])
            .unwrap(),
        None
    );
}

/// Go's TryFastPlan: a single-table SELECT whose WHERE pins the handle or
/// a whole unique index reads one row instead of scanning. The results
/// must be identical to the scan in every case, including the cases that
/// do NOT qualify and fall back.
#[test]
fn point_get_plans() {
    let mut catalog = Catalog::default();
    crate::run_create_table_on(
        "CREATE TABLE g (id BIGINT PRIMARY KEY, code VARCHAR(8) UNIQUE, v BIGINT)",
        &mut catalog,
    )
    .unwrap();
    run_insert_on(
        "INSERT INTO g VALUES (1, 'a', 10), (2, 'b', 20), (3, 'c', 30)",
        &mut catalog,
        &crate::StmtContext::for_query(),
    )
    .unwrap();

    // Handle point get.
    assert_eq!(
        run_select_on(
            "SELECT v FROM g WHERE id = 2",
            &catalog,
            &crate::StmtContext::for_query()
        )
        .unwrap(),
        vec![vec![Datum::Int(20)]]
    );
    // A handle that does not exist reads nothing.
    assert_eq!(
        run_select_on(
            "SELECT v FROM g WHERE id = 99",
            &catalog,
            &crate::StmtContext::for_query()
        )
        .unwrap(),
        Vec::<Vec<Datum>>::new()
    );
    // Unique-index point get, through the entry's stored handle.
    assert_eq!(
        run_select_on(
            "SELECT id FROM g WHERE code = 'c'",
            &catalog,
            &crate::StmtContext::for_query()
        )
        .unwrap(),
        vec![vec![Datum::Int(3)]]
    );
    assert_eq!(
        run_select_on(
            "SELECT id FROM g WHERE code = 'zz'",
            &catalog,
            &crate::StmtContext::for_query()
        )
        .unwrap(),
        Vec::<Vec<Datum>>::new()
    );

    // The WHERE stays in the pipeline, so an extra condition still
    // filters: the point get narrows the source, it does not replace the
    // filter.
    assert_eq!(
        run_select_on(
            "SELECT id FROM g WHERE id = 2 AND v = 20",
            &catalog,
            &crate::StmtContext::for_query()
        )
        .unwrap(),
        vec![vec![Datum::Int(2)]]
    );
    assert_eq!(
        run_select_on(
            "SELECT id FROM g WHERE id = 2 AND v = 999",
            &catalog,
            &crate::StmtContext::for_query()
        )
        .unwrap(),
        Vec::<Vec<Datum>>::new()
    );

    // Shapes that do not qualify fall back to the scan and stay correct.
    assert_eq!(
        run_select_on(
            "SELECT id FROM g WHERE v = 30",
            &catalog,
            &crate::StmtContext::for_query()
        )
        .unwrap(),
        vec![vec![Datum::Int(3)]],
        "a non-key column is not a point get"
    );
    assert_eq!(
        run_select_on(
            "SELECT id FROM g WHERE id > 1 ORDER BY id",
            &catalog,
            &crate::StmtContext::for_query()
        )
        .unwrap(),
        vec![vec![Datum::Int(2)], vec![Datum::Int(3)]],
        "a range is not a point get"
    );
    assert_eq!(
        run_select_on(
            "SELECT id FROM g WHERE id = 1 OR id = 3",
            &catalog,
            &crate::StmtContext::for_query()
        )
        .unwrap(),
        vec![vec![Datum::Int(1)], vec![Datum::Int(3)]],
        "Go recurses only through AND, so OR is not a point get"
    );
    // Go rejects the fast plan when ORDER BY or HAVING is present, or when
    // LIMIT could remove the row; the answers stay right either way.
    assert_eq!(
        run_select_on(
            "SELECT id FROM g WHERE id = 2 LIMIT 1 OFFSET 1",
            &catalog,
            &crate::StmtContext::for_query()
        )
        .unwrap(),
        Vec::<Vec<Datum>>::new()
    );
    assert_eq!(
        run_select_on(
            "SELECT id FROM g WHERE id = 2 ORDER BY id",
            &catalog,
            &crate::StmtContext::for_query()
        )
        .unwrap(),
        vec![vec![Datum::Int(2)]]
    );

    // A non-integer constant cannot name an integer handle: no row, not a
    // wrong row.
    assert_eq!(
        run_select_on(
            "SELECT id FROM g WHERE id = 'x'",
            &catalog,
            &crate::StmtContext::for_query()
        )
        .unwrap(),
        Vec::<Vec<Datum>>::new()
    );

    // A point get sees writes, including the row a DELETE removed.
    run_update_on(
        "UPDATE g SET v = 99 WHERE id = 2",
        &mut catalog,
        &crate::StmtContext::for_query(),
    )
    .unwrap();
    assert_eq!(
        run_select_on(
            "SELECT v FROM g WHERE id = 2",
            &catalog,
            &crate::StmtContext::for_query()
        )
        .unwrap(),
        vec![vec![Datum::Int(99)]]
    );
    run_delete_on(
        "DELETE FROM g WHERE id = 2",
        &mut catalog,
        &crate::StmtContext::for_query(),
    )
    .unwrap();
    assert_eq!(
        run_select_on(
            "SELECT v FROM g WHERE id = 2",
            &catalog,
            &crate::StmtContext::for_query()
        )
        .unwrap(),
        Vec::<Vec<Datum>>::new()
    );
    assert_eq!(
        run_select_on(
            "SELECT id FROM g WHERE code = 'b'",
            &catalog,
            &crate::StmtContext::for_query()
        )
        .unwrap(),
        Vec::<Vec<Datum>>::new(),
        "the deleted row's index entry is gone too"
    );
}

/// The results above would be right even if the fast plan never fired, so
/// this asserts the DECISION: which shapes Go's tryPointGetPlan accepts
/// and which it rejects.
#[test]
fn point_get_is_chosen_only_for_the_shapes_go_accepts() {
    let mut catalog = Catalog::default();
    crate::run_create_table_on(
        "CREATE TABLE d (id BIGINT PRIMARY KEY, code VARCHAR(8) UNIQUE, v BIGINT)",
        &mut catalog,
    )
    .unwrap();
    run_insert_on(
        "INSERT INTO d VALUES (1, 'a', 10)",
        &mut catalog,
        &crate::StmtContext::for_query(),
    )
    .unwrap();
    let Some(TableEntry::Kv(table)) = catalog.get_table_for_test("d") else {
        panic!("expected a kv table");
    };
    let columns = table
        .columns
        .iter()
        .map(|c| (c.name.clone(), c.field_type.clone()))
        .collect::<Vec<_>>();

    let decides = |sql: &str| {
        let stmt = tidb_parser::parse(sql).unwrap();
        let Stmt::Query(query) = &stmt else {
            panic!("not a query")
        };
        let QueryStmt::Select(select) = &**query else {
            panic!("not a select")
        };
        try_point_get(select, table, &columns).unwrap()
    };

    // Accepted: the handle, and a whole unique index.
    assert_eq!(
        decides("SELECT v FROM d WHERE id = 1"),
        Some(Some(TableHandle::Int(1)))
    );
    assert_eq!(
        decides("SELECT v FROM d WHERE 1 = id"),
        Some(Some(TableHandle::Int(1)))
    );
    assert_eq!(
        decides("SELECT v FROM d WHERE code = 'a'"),
        Some(Some(TableHandle::Int(1)))
    );
    // The handle path does not probe: it hands the plan the handle the
    // constant names, and the row read finds nothing. The index path does
    // probe, because the handle only exists in an index entry.
    assert_eq!(
        decides("SELECT v FROM d WHERE id = 7"),
        Some(Some(TableHandle::Int(7)))
    );
    assert_eq!(decides("SELECT v FROM d WHERE code = 'z'"), Some(None));
    // The index path allows extra pairs beyond the key.
    assert_eq!(
        decides("SELECT v FROM d WHERE code = 'a' AND v = 10"),
        Some(Some(TableHandle::Int(1)))
    );

    // Rejected, so the scan runs: Go requires the handle pair to be the
    // ONLY pair, a conjunction of equalities, no ORDER BY or HAVING, and
    // a LIMIT that cannot drop the row.
    assert_eq!(decides("SELECT v FROM d WHERE id = 1 AND v = 10"), None);
    assert_eq!(decides("SELECT v FROM d WHERE v = 10"), None);
    assert_eq!(decides("SELECT v FROM d WHERE id > 1"), None);
    assert_eq!(decides("SELECT v FROM d WHERE id = 1 OR id = 2"), None);
    assert_eq!(decides("SELECT v FROM d WHERE id = 1 ORDER BY v"), None);
    assert_eq!(decides("SELECT v FROM d WHERE id = 1 LIMIT 0"), None);
    assert_eq!(
        decides("SELECT v FROM d WHERE id = 1 LIMIT 1 OFFSET 1"),
        None
    );
    assert_eq!(decides("SELECT v FROM d"), None);
}

/// A composite-index range spans several datums per bound, an IN list
/// produces several ranges, and an OR unions them. The answers must be
/// the same rows a full scan would return -- a range that reads too few
/// rows is invisible to the range text alone.
#[test]
fn multi_column_and_multi_range_scans_read_the_same_rows_as_a_full_scan() {
    let mut catalog = Catalog::default();
    crate::run_create_table_on(
        "CREATE TABLE m (id BIGINT PRIMARY KEY, a BIGINT, b BIGINT, KEY ab (a, b))",
        &mut catalog,
    )
    .unwrap();
    run_insert_on(
        "INSERT INTO m VALUES (1, 1, 1), (2, 1, 5), (3, 1, 9), (4, 2, 5), \
         (5, 3, 5), (6, NULL, 1), (7, 2, NULL)",
        &mut catalog,
        &crate::StmtContext::for_query(),
    )
    .unwrap();
    let ids = |sql: &str| {
        let mut ids: Vec<i64> = run_select_on(sql, &catalog, &crate::StmtContext::for_query())
            .unwrap()
            .into_iter()
            .map(|row| match row[0] {
                Datum::Int(v) => v,
                ref other => panic!("expected an int, got {other:?}"),
            })
            .collect();
        ids.sort_unstable();
        ids
    };

    // Equality on the leading column plus a range on the next.
    assert_eq!(ids("SELECT id FROM m WHERE a = 1 AND b > 1"), vec![2, 3]);
    assert_eq!(
        ids("SELECT id FROM m WHERE a = 1 AND b BETWEEN 1 AND 5"),
        vec![1, 2]
    );
    // An IN list on the leading column: several point ranges, each
    // extended by the equality on the next column.
    assert_eq!(
        ids("SELECT id FROM m WHERE a IN (1, 3) AND b = 5"),
        vec![2, 5]
    );
    // A disjunction: the branches' ranges are unioned.
    assert_eq!(
        ids("SELECT id FROM m WHERE (a = 1 AND b = 5) OR (a = 3 AND b = 5)"),
        vec![2, 5]
    );
    // A NULL in the indexed columns is reachable only through IS NULL,
    // never through a comparison.
    assert_eq!(ids("SELECT id FROM m WHERE a IS NULL"), vec![6]);
    assert_eq!(ids("SELECT id FROM m WHERE a = 2 AND b IS NULL"), vec![7]);
    // The residual half still filters: `id` is not in the index, so the
    // range cannot express it and the Selection above must.
    assert_eq!(ids("SELECT id FROM m WHERE a = 1 AND id > 1"), vec![2, 3]);
}

/// Index range scans: a comparison on an indexed column reads the rows the
/// index covers instead of scanning the table, with Go's range semantics.
#[test]
fn index_range_scans() {
    let mut catalog = Catalog::default();
    crate::run_create_table_on(
        "CREATE TABLE r (id BIGINT PRIMARY KEY, score BIGINT, KEY score_idx (score))",
        &mut catalog,
    )
    .unwrap();
    run_insert_on(
        "INSERT INTO r VALUES (1, 10), (2, 20), (3, 30), (4, 20), (5, NULL)",
        &mut catalog,
        &crate::StmtContext::for_query(),
    )
    .unwrap();

    let ids = |sql: &str, catalog: &Catalog| {
        let mut got: Vec<i64> = run_select_on(sql, catalog, &crate::StmtContext::for_query())
            .unwrap()
            .into_iter()
            .map(|row| match row[0] {
                Datum::Int(value) => value,
                ref other => panic!("expected an int, got {other:?}"),
            })
            .collect();
        got.sort_unstable();
        got
    };

    assert_eq!(
        ids("SELECT id FROM r WHERE score > 15", &catalog),
        vec![2, 3, 4]
    );
    assert_eq!(
        ids("SELECT id FROM r WHERE score >= 20", &catalog),
        vec![2, 3, 4]
    );
    assert_eq!(
        ids("SELECT id FROM r WHERE score < 30", &catalog),
        vec![1, 2, 4]
    );
    assert_eq!(ids("SELECT id FROM r WHERE score <= 10", &catalog), vec![1]);
    assert_eq!(
        ids("SELECT id FROM r WHERE score = 20", &catalog),
        vec![2, 4]
    );
    // The constant may sit on the left, with the operator flipped.
    assert_eq!(
        ids("SELECT id FROM r WHERE 15 < score", &catalog),
        vec![2, 3, 4]
    );

    // Several conditions on the column intersect into one range.
    assert_eq!(
        ids("SELECT id FROM r WHERE score > 10 AND score < 30", &catalog),
        vec![2, 4]
    );
    assert_eq!(
        ids(
            "SELECT id FROM r WHERE score >= 20 AND score <= 20",
            &catalog
        ),
        vec![2, 4]
    );

    // Go's ranges start at MinNotNull, so a NULL satisfies no comparison
    // -- row 5 never appears, and IS NULL still finds it through the scan.
    assert_eq!(
        ids("SELECT id FROM r WHERE score > -100", &catalog),
        vec![1, 2, 3, 4]
    );
    assert_eq!(
        ids("SELECT id FROM r WHERE score IS NULL", &catalog),
        vec![5]
    );

    // A condition the ranges do not consume still filters, because the
    // WHERE stays above the read.
    assert_eq!(
        ids("SELECT id FROM r WHERE score > 15 AND id = 3", &catalog),
        vec![3]
    );

    // Writes are visible to a later range scan, including through the
    // index entries a DELETE removed.
    run_update_on(
        "UPDATE r SET score = 99 WHERE id = 1",
        &mut catalog,
        &crate::StmtContext::for_query(),
    )
    .unwrap();
    assert_eq!(ids("SELECT id FROM r WHERE score > 50", &catalog), vec![1]);
    run_delete_on(
        "DELETE FROM r WHERE id = 1",
        &mut catalog,
        &crate::StmtContext::for_query(),
    )
    .unwrap();
    assert_eq!(
        ids("SELECT id FROM r WHERE score > 50", &catalog),
        Vec::<i64>::new()
    );
}

/// A range scan over a UNIQUE index reads its handles out of the entry
/// VALUES, not the key, so this covers the other half of the entry format
/// -- including the NULL entries a unique index stores non-distinctly.
#[test]
fn index_range_scan_over_a_unique_index() {
    let mut catalog = Catalog::default();
    crate::run_create_table_on(
        "CREATE TABLE u2 (id BIGINT PRIMARY KEY, code BIGINT UNIQUE)",
        &mut catalog,
    )
    .unwrap();
    run_insert_on(
        "INSERT INTO u2 VALUES (1, 100), (2, 200), (3, 300), (4, NULL)",
        &mut catalog,
        &crate::StmtContext::for_query(),
    )
    .unwrap();
    let mut ids: Vec<Datum> = run_select_on(
        "SELECT id FROM u2 WHERE code >= 200",
        &catalog,
        &crate::StmtContext::for_query(),
    )
    .unwrap()
    .into_iter()
    .map(|row| row[0].clone())
    .collect();
    ids.sort_by_key(|value| match value {
        Datum::Int(v) => *v,
        other => panic!("expected an int, got {other:?}"),
    });
    assert_eq!(ids, vec![Datum::Int(2), Datum::Int(3)]);
    // The NULL row is reachable, just never through a comparison.
    assert_eq!(
        run_select_on(
            "SELECT id FROM u2 WHERE code IS NULL",
            &catalog,
            &crate::StmtContext::for_query()
        )
        .unwrap(),
        vec![vec![Datum::Int(4)]]
    );
}

/// The answers above would be right even from a full scan, so this asserts
/// the DECISION and the ranges themselves.
#[test]
fn index_ranges_are_built_the_way_go_builds_them() {
    let mut catalog = Catalog::default();
    crate::run_create_table_on(
        "CREATE TABLE q (id BIGINT PRIMARY KEY, score BIGINT, note VARCHAR(8), KEY s (score))",
        &mut catalog,
    )
    .unwrap();
    let Some(TableEntry::Kv(table)) = catalog.get_table_for_test("q") else {
        panic!("expected a kv table");
    };
    let columns = table
        .columns
        .iter()
        .map(|c| (c.name.clone(), c.field_type.clone()))
        .collect::<Vec<_>>();
    let ranges = |sql: &str| {
        let stmt = tidb_parser::parse(sql).unwrap();
        let Stmt::Query(query) = &stmt else {
            panic!("not a query")
        };
        let QueryStmt::Select(select) = &**query else {
            panic!("not a select")
        };
        let scope = crate::plan_trace::PlanTrace::single_table_scope("q", None, columns.clone());
        choose_index_range_path(select, &catalog, &scope, table, &columns)
            .map(|(id, ranges, _)| (id, ranges))
    };

    // Go: GT is (v, MaxValue], LT is [MinNotNull, v).
    assert_eq!(
        ranges("SELECT id FROM q WHERE score > 5"),
        Some((
            1,
            vec![IndexRange {
                low: vec![Datum::Int(5)],
                high: vec![Datum::MaxValue],
                low_exclusive: true,
                high_exclusive: false,
            }]
        ))
    );
    assert_eq!(
        ranges("SELECT id FROM q WHERE score < 5"),
        Some((
            1,
            vec![IndexRange {
                low: vec![Datum::MinNotNull],
                high: vec![Datum::Int(5)],
                low_exclusive: false,
                high_exclusive: true,
            }]
        ))
    );
    // An intersection keeps the tighter end of each side.
    assert_eq!(
        ranges("SELECT id FROM q WHERE score > 5 AND score <= 9"),
        Some((
            1,
            vec![IndexRange {
                low: vec![Datum::Int(5)],
                high: vec![Datum::Int(9)],
                low_exclusive: true,
                high_exclusive: false,
            }]
        ))
    );
    // A NULL constant matches nothing, which Go represents as no ranges.
    assert_eq!(
        ranges("SELECT id FROM q WHERE score > NULL"),
        Some((1, vec![]))
    );

    // An OR is detached branch by branch and the branches' ranges are
    // unioned (Go `detachDNFCondAndBuildRangeForIndex` + `UnionRanges`).
    assert_eq!(
        ranges("SELECT id FROM q WHERE score > 1 OR score < 0"),
        Some((
            1,
            vec![
                IndexRange {
                    low: vec![Datum::MinNotNull],
                    high: vec![Datum::Int(0)],
                    low_exclusive: false,
                    high_exclusive: true,
                },
                IndexRange {
                    low: vec![Datum::Int(1)],
                    high: vec![Datum::MaxValue],
                    low_exclusive: true,
                    high_exclusive: false,
                }
            ]
        ))
    );

    // No usable index: an unindexed column, or no WHERE at all.
    assert_eq!(ranges("SELECT id FROM q WHERE note = 'x'"), None);
    assert_eq!(ranges("SELECT id FROM q"), None);
}

/// Column defaults and the NOT NULL rules, following Go's fillColValue
/// and CheckNotNull: an omitted column takes its DEFAULT, an omitted NOT
/// NULL column with no DEFAULT is ErrNoDefaultForField, and an explicit
/// NULL into a NOT NULL column is the different ErrColumnCantNull.
#[test]
fn column_defaults_and_not_null() {
    let mut catalog = Catalog::default();
    crate::run_create_table_on(
        "CREATE TABLE d (id BIGINT PRIMARY KEY, n BIGINT NOT NULL, \
         w BIGINT DEFAULT 7, s VARCHAR(4) DEFAULT 'zz', plain BIGINT)",
        &mut catalog,
    )
    .unwrap();

    // Omitted columns take their defaults; a nullable one with no DEFAULT
    // is NULL.
    run_insert_on(
        "INSERT INTO d (id, n) VALUES (1, 5)",
        &mut catalog,
        &crate::StmtContext::for_query(),
    )
    .unwrap();
    let row = &run_select_on(
        "SELECT w, s, plain FROM d WHERE id = 1",
        &catalog,
        &crate::StmtContext::for_query(),
    )
    .unwrap()[0];
    assert_eq!(row[0], Datum::Int(7));
    assert_eq!(datum_text_for_test(&row[1]), "zz");
    assert_eq!(row[2], Datum::Null);

    // An explicit value overrides the default.
    run_insert_on(
        "INSERT INTO d (id, n, w) VALUES (2, 5, 100)",
        &mut catalog,
        &crate::StmtContext::for_query(),
    )
    .unwrap();
    assert_eq!(
        run_select_on(
            "SELECT w FROM d WHERE id = 2",
            &catalog,
            &crate::StmtContext::for_query()
        )
        .unwrap(),
        vec![vec![Datum::Int(100)]]
    );

    // An omitted NOT NULL column with no default is 1364.
    assert!(matches!(
        run_insert_on("INSERT INTO d (id) VALUES (3)", &mut catalog, &crate::StmtContext::for_query()),
        Err(DriverError::NoDefaultForField(name)) if name == "n"
    ));
    // An explicit NULL into that column is the other error, 1048.
    assert!(matches!(
        run_insert_on("INSERT INTO d (id, n) VALUES (3, NULL)", &mut catalog, &crate::StmtContext::for_query()),
        Err(DriverError::ColumnCannotBeNull(name)) if name == "n"
    ));
    // A NULL into a nullable column is fine.
    run_insert_on(
        "INSERT INTO d (id, n, plain) VALUES (3, 5, NULL)",
        &mut catalog,
        &crate::StmtContext::for_query(),
    )
    .unwrap();

    // A DEFAULT NULL column is not the same as no DEFAULT: it is
    // omittable even when the column is otherwise unconstrained.
    crate::run_create_table_on(
        "CREATE TABLE e (id BIGINT PRIMARY KEY, v BIGINT DEFAULT NULL)",
        &mut catalog,
    )
    .unwrap();
    run_insert_on(
        "INSERT INTO e (id) VALUES (1)",
        &mut catalog,
        &crate::StmtContext::for_query(),
    )
    .unwrap();
    assert_eq!(
        run_select_on(
            "SELECT v FROM e",
            &catalog,
            &crate::StmtContext::for_query()
        )
        .unwrap(),
        vec![vec![Datum::Null]]
    );

    // A primary key is NOT NULL, so omitting it is 1364 as well.
    assert!(matches!(
        run_insert_on(
            "INSERT INTO e (v) VALUES (1)",
            &mut catalog,
            &crate::StmtContext::for_query()
        ),
        Err(DriverError::NoDefaultForField(_))
    ));

    // An AUTO_INCREMENT column supplies its own value, so omitting it is
    // never the missing-default case (see the auto_increment test).
    crate::run_create_table_on("CREATE TABLE f (a BIGINT AUTO_INCREMENT)", &mut catalog).unwrap();
    run_insert_on(
        "INSERT INTO f () VALUES ()",
        &mut catalog,
        &crate::StmtContext::for_query(),
    )
    .or_else(|_| {
        run_insert_on(
            "INSERT INTO f VALUES (NULL)",
            &mut catalog,
            &crate::StmtContext::for_query(),
        )
    })
    .unwrap();
    assert_eq!(
        run_select_on(
            "SELECT a FROM f",
            &catalog,
            &crate::StmtContext::for_query()
        )
        .unwrap(),
        vec![vec![Datum::Int(1)]]
    );
    // A generated column is still rejected rather than ignored.
    assert!(crate::run_create_table_on(
        "CREATE TABLE g2 (a BIGINT, b BIGINT GENERATED ALWAYS AS (a+1) VIRTUAL)",
        &mut catalog
    )
    .is_err());
}

/// A primary key that is not a single integer column becomes a clustered
/// COMMON handle: its encoding IS the row key, so rows scan in key order,
/// the columns live in the key rather than the value, and a repeat is a
/// duplicate (Go's IsCommonHandle path in addRecord).
#[test]
fn clustered_common_handle() {
    let mut catalog = Catalog::default();
    crate::run_create_table_on(
        "CREATE TABLE c (k VARCHAR(8) PRIMARY KEY, v BIGINT)",
        &mut catalog,
    )
    .unwrap();
    run_insert_on(
        "INSERT INTO c VALUES ('b', 2), ('a', 1), ('c', 3)",
        &mut catalog,
        &crate::StmtContext::for_query(),
    )
    .unwrap();

    // Key order, not insertion order -- the key IS the primary key.
    assert_eq!(
        run_select_on(
            "SELECT k, v FROM c",
            &catalog,
            &crate::StmtContext::for_query()
        )
        .unwrap()
        .into_iter()
        .map(|row| datum_text_for_test(&row[0]))
        .collect::<Vec<_>>(),
        vec!["a".to_owned(), "b".to_owned(), "c".to_owned()]
    );
    // The key column round-trips even though the value omits it.
    assert_eq!(
        run_select_on(
            "SELECT v FROM c WHERE k = 'b'",
            &catalog,
            &crate::StmtContext::for_query()
        )
        .unwrap(),
        vec![vec![Datum::Int(2)]]
    );
    // A repeated key is a duplicate.
    assert!(matches!(
        run_insert_on(
            "INSERT INTO c VALUES ('a', 9)",
            &mut catalog,
            &crate::StmtContext::for_query()
        ),
        Err(DriverError::DuplicateEntry { .. })
    ));

    // Writes address the row through its clustered key.
    run_update_on(
        "UPDATE c SET v = 20 WHERE k = 'b'",
        &mut catalog,
        &crate::StmtContext::for_query(),
    )
    .unwrap();
    assert_eq!(
        run_select_on(
            "SELECT v FROM c WHERE k = 'b'",
            &catalog,
            &crate::StmtContext::for_query()
        )
        .unwrap(),
        vec![vec![Datum::Int(20)]]
    );
    run_delete_on(
        "DELETE FROM c WHERE k = 'a'",
        &mut catalog,
        &crate::StmtContext::for_query(),
    )
    .unwrap();
    assert_eq!(
        run_select_on(
            "SELECT k FROM c",
            &catalog,
            &crate::StmtContext::for_query()
        )
        .unwrap()
        .len(),
        2
    );
    // The freed key can be inserted again.
    run_insert_on(
        "INSERT INTO c VALUES ('a', 1)",
        &mut catalog,
        &crate::StmtContext::for_query(),
    )
    .unwrap();

    // A multi-column primary key is a clustered common handle too.
    crate::run_create_table_on(
        "CREATE TABLE m (a BIGINT, b VARCHAR(4), v BIGINT, PRIMARY KEY (a, b))",
        &mut catalog,
    )
    .unwrap();
    run_insert_on(
        "INSERT INTO m VALUES (1, 'y', 10), (1, 'x', 20), (2, 'a', 30)",
        &mut catalog,
        &crate::StmtContext::for_query(),
    )
    .unwrap();
    assert_eq!(
        run_select_on(
            "SELECT a, b FROM m",
            &catalog,
            &crate::StmtContext::for_query()
        )
        .unwrap()
        .into_iter()
        .map(|row| format!("{:?}/{}", row[0], datum_text_for_test(&row[1])))
        .collect::<Vec<_>>(),
        vec![
            "Int(1)/x".to_owned(),
            "Int(1)/y".to_owned(),
            "Int(2)/a".to_owned()
        ]
    );
    // Only the whole key must be unique; a repeated leading column is fine.
    assert!(matches!(
        run_insert_on(
            "INSERT INTO m VALUES (1, 'x', 99)",
            &mut catalog,
            &crate::StmtContext::for_query()
        ),
        Err(DriverError::DuplicateEntry { .. })
    ));
    run_insert_on(
        "INSERT INTO m VALUES (1, 'z', 40)",
        &mut catalog,
        &crate::StmtContext::for_query(),
    )
    .unwrap();

    // A secondary index over a clustered table stores the common handle
    // and still resolves to its row.
    crate::run_create_table_on(
        "CREATE TABLE s (k VARCHAR(4) PRIMARY KEY, tag BIGINT, KEY tag_idx (tag))",
        &mut catalog,
    )
    .unwrap();
    run_insert_on(
        "INSERT INTO s VALUES ('p', 1), ('q', 2)",
        &mut catalog,
        &crate::StmtContext::for_query(),
    )
    .unwrap();
    assert_eq!(
        run_select_on(
            "SELECT k FROM s WHERE tag >= 2",
            &catalog,
            &crate::StmtContext::for_query()
        )
        .unwrap()
        .into_iter()
        .map(|row| datum_text_for_test(&row[0]))
        .collect::<Vec<_>>(),
        vec!["q".to_owned()]
    );
}

/// AUTO_INCREMENT, checked against behavior captured from real TiDB:
/// inserting 1,2 then an explicit 100 rebases the allocator, so the next
/// rows are 101, 102, 103 -- NULL and 0 both allocate.
#[test]
fn auto_increment() {
    let mut catalog = Catalog::default();
    crate::run_create_table_on(
        "CREATE TABLE a1 (id BIGINT AUTO_INCREMENT PRIMARY KEY, v BIGINT)",
        &mut catalog,
    )
    .unwrap();
    run_insert_on(
        "INSERT INTO a1 (v) VALUES (10), (20)",
        &mut catalog,
        &crate::StmtContext::for_query(),
    )
    .unwrap();
    run_insert_on(
        "INSERT INTO a1 VALUES (100, 30)",
        &mut catalog,
        &crate::StmtContext::for_query(),
    )
    .unwrap();
    run_insert_on(
        "INSERT INTO a1 (v) VALUES (40)",
        &mut catalog,
        &crate::StmtContext::for_query(),
    )
    .unwrap();
    run_insert_on(
        "INSERT INTO a1 VALUES (NULL, 50), (0, 60)",
        &mut catalog,
        &crate::StmtContext::for_query(),
    )
    .unwrap();

    // Captured from TiDB: [[1 10] [2 20] [100 30] [101 40] [102 50] [103 60]]
    assert_eq!(
        run_select_on(
            "SELECT id, v FROM a1",
            &catalog,
            &crate::StmtContext::for_query()
        )
        .unwrap(),
        vec![
            vec![Datum::Int(1), Datum::Int(10)],
            vec![Datum::Int(2), Datum::Int(20)],
            vec![Datum::Int(100), Datum::Int(30)],
            vec![Datum::Int(101), Datum::Int(40)],
            vec![Datum::Int(102), Datum::Int(50)],
            vec![Datum::Int(103), Datum::Int(60)],
        ]
    );

    // TiDB does NOT require the auto column to be a key -- captured, and
    // unlike MySQL, which raises 1075 for it.
    crate::run_create_table_on(
        "CREATE TABLE bad (a BIGINT AUTO_INCREMENT, b BIGINT)",
        &mut catalog,
    )
    .unwrap();
    run_insert_on(
        "INSERT INTO bad (b) VALUES (1), (2)",
        &mut catalog,
        &crate::StmtContext::for_query(),
    )
    .unwrap();
    assert_eq!(
        run_select_on(
            "SELECT a FROM bad",
            &catalog,
            &crate::StmtContext::for_query()
        )
        .unwrap(),
        vec![vec![Datum::Int(1)], vec![Datum::Int(2)]]
    );

    // A second auto column is Go's 1075, and a non-integer one is its
    // "Incorrect column specifier" -- both captured from TiDB.
    assert!(matches!(
        crate::run_create_table_on(
            "CREATE TABLE two (a BIGINT AUTO_INCREMENT PRIMARY KEY, b BIGINT AUTO_INCREMENT)",
            &mut catalog
        ),
        Err(DriverError::WrongAutoKey)
    ));
    assert!(matches!(
        crate::run_create_table_on(
            "CREATE TABLE strk (a VARCHAR(4) AUTO_INCREMENT PRIMARY KEY)",
            &mut catalog
        ),
        Err(DriverError::WrongColumnSpecifier(_))
    ));
}

/// Go's tryWhereIn2BatchPointGet: `col IN (constants)` over the handle or
/// a single-column unique index reads those rows directly. Results must
/// match the scan in every case, including the shapes Go rejects.
#[test]
fn batch_point_get() {
    let mut catalog = Catalog::default();
    crate::run_create_table_on(
        "CREATE TABLE b (id BIGINT PRIMARY KEY, code VARCHAR(8) UNIQUE, v BIGINT)",
        &mut catalog,
    )
    .unwrap();
    run_insert_on(
        "INSERT INTO b VALUES (1, 'a', 10), (2, 'b', 20), (3, 'c', 30)",
        &mut catalog,
        &crate::StmtContext::for_query(),
    )
    .unwrap();

    let ids = |sql: &str, catalog: &Catalog| {
        let mut got: Vec<i64> = run_select_on(sql, catalog, &crate::StmtContext::for_query())
            .unwrap()
            .into_iter()
            .map(|row| match row[0] {
                Datum::Int(value) => value,
                ref other => panic!("expected an int, got {other:?}"),
            })
            .collect();
        got.sort_unstable();
        got
    };

    // Handle path, including a value that matches nothing.
    assert_eq!(
        ids("SELECT id FROM b WHERE id IN (1, 3)", &catalog),
        vec![1, 3]
    );
    assert_eq!(
        ids("SELECT id FROM b WHERE id IN (3, 99)", &catalog),
        vec![3]
    );
    assert_eq!(
        ids("SELECT id FROM b WHERE id IN (99)", &catalog),
        Vec::<i64>::new()
    );
    // Unique-index path.
    assert_eq!(
        ids("SELECT id FROM b WHERE code IN ('a', 'c')", &catalog),
        vec![1, 3]
    );

    // Shapes Go rejects fall back to the scan and stay correct: NOT IN,
    // a non-key column, and an IN with anything else in the WHERE.
    assert_eq!(
        ids("SELECT id FROM b WHERE id NOT IN (1, 3)", &catalog),
        vec![2]
    );
    assert_eq!(
        ids("SELECT id FROM b WHERE v IN (20, 30)", &catalog),
        vec![2, 3]
    );
    assert_eq!(
        ids("SELECT id FROM b WHERE id IN (1, 3) AND v = 30", &catalog),
        vec![3]
    );
    // Go also rejects it with ORDER BY, LIMIT or DISTINCT present.
    assert_eq!(
        ids("SELECT id FROM b WHERE id IN (3, 1) ORDER BY id", &catalog),
        vec![1, 3]
    );
    assert_eq!(
        run_select_on(
            "SELECT id FROM b WHERE id IN (1, 2, 3) LIMIT 2",
            &catalog,
            &crate::StmtContext::for_query()
        )
        .unwrap()
        .len(),
        2
    );
}

/// The answers above would be right from a scan too, so this asserts the
/// DECISION: which shapes Go's batch point get claims.
#[test]
fn batch_point_get_is_chosen_only_for_the_shapes_go_accepts() {
    let mut catalog = Catalog::default();
    crate::run_create_table_on(
        "CREATE TABLE bd (id BIGINT PRIMARY KEY, code VARCHAR(8) UNIQUE, v BIGINT)",
        &mut catalog,
    )
    .unwrap();
    run_insert_on(
        "INSERT INTO bd VALUES (1, 'a', 10)",
        &mut catalog,
        &crate::StmtContext::for_query(),
    )
    .unwrap();
    let Some(TableEntry::Kv(table)) = catalog.get_table_for_test("bd") else {
        panic!("expected a kv table");
    };
    let columns = table
        .columns
        .iter()
        .map(|c| (c.name.clone(), c.field_type.clone()))
        .collect::<Vec<_>>();
    let decides = |sql: &str| {
        let stmt = tidb_parser::parse(sql).unwrap();
        let Stmt::Query(query) = &stmt else {
            panic!("not a query")
        };
        let QueryStmt::Select(select) = &**query else {
            panic!("not a select")
        };
        try_batch_point_get(select, table, &columns).unwrap()
    };

    assert_eq!(
        decides("SELECT v FROM bd WHERE id IN (1, 2)"),
        Some(vec![TableHandle::Int(1), TableHandle::Int(2)]),
        "the handle path does not probe, as the single point get does not"
    );
    assert_eq!(
        decides("SELECT v FROM bd WHERE code IN ('a', 'zz')"),
        Some(vec![TableHandle::Int(1)]),
        "the index path probes, so a missing key yields no handle"
    );
    // Rejected shapes.
    assert_eq!(decides("SELECT v FROM bd WHERE id NOT IN (1)"), None);
    assert_eq!(decides("SELECT v FROM bd WHERE v IN (1)"), None);
    assert_eq!(decides("SELECT v FROM bd WHERE id IN (1) AND v = 1"), None);
    assert_eq!(decides("SELECT v FROM bd WHERE id IN (1) ORDER BY v"), None);
    assert_eq!(decides("SELECT v FROM bd WHERE id IN (1) LIMIT 1"), None);
    assert_eq!(decides("SELECT DISTINCT v FROM bd WHERE id IN (1)"), None);
    assert_eq!(decides("SELECT v FROM bd WHERE id = 1"), None);
}

/// SELECT DISTINCT deduplicates the projected rows, which Go builds as an
/// aggregation grouping by every projected column with FIRST_ROW
/// aggregates. The plain path silently returned duplicates before.
#[test]
fn select_distinct() {
    let mut catalog = Catalog::default();
    crate::run_create_table_on("CREATE TABLE d2 (a BIGINT, b BIGINT)", &mut catalog).unwrap();
    run_insert_on(
        "INSERT INTO d2 VALUES (1, 1), (1, 2), (1, 1), (2, 2)",
        &mut catalog,
        &crate::StmtContext::for_query(),
    )
    .unwrap();

    assert_eq!(
        run_select_on(
            "SELECT DISTINCT a FROM d2",
            &catalog,
            &crate::StmtContext::for_query()
        )
        .unwrap(),
        vec![vec![Datum::Int(1)], vec![Datum::Int(2)]]
    );
    // Every projected column takes part, so (1,1) collapses but (1,2)
    // stays.
    assert_eq!(
        run_select_on(
            "SELECT DISTINCT a, b FROM d2",
            &catalog,
            &crate::StmtContext::for_query()
        )
        .unwrap(),
        vec![
            vec![Datum::Int(1), Datum::Int(1)],
            vec![Datum::Int(1), Datum::Int(2)],
            vec![Datum::Int(2), Datum::Int(2)],
        ]
    );
    // Without DISTINCT every row survives.
    assert_eq!(
        run_select_on(
            "SELECT a FROM d2",
            &catalog,
            &crate::StmtContext::for_query()
        )
        .unwrap()
        .len(),
        4
    );

    // DISTINCT applies to the projected expression, not the source rows.
    assert_eq!(
        run_select_on(
            "SELECT DISTINCT a + b FROM d2",
            &catalog,
            &crate::StmtContext::for_query()
        )
        .unwrap(),
        vec![
            vec![Datum::Int(2)],
            vec![Datum::Int(3)],
            vec![Datum::Int(4)]
        ]
    );

    // The dedup emits groups in first-seen order, so a sort below it still
    // orders the surviving rows.
    assert_eq!(
        run_select_on(
            "SELECT DISTINCT a FROM d2 ORDER BY a DESC",
            &catalog,
            &crate::StmtContext::for_query()
        )
        .unwrap(),
        vec![vec![Datum::Int(2)], vec![Datum::Int(1)]]
    );
    // LIMIT applies after the dedup.
    assert_eq!(
        run_select_on(
            "SELECT DISTINCT a FROM d2 LIMIT 1",
            &catalog,
            &crate::StmtContext::for_query()
        )
        .unwrap(),
        vec![vec![Datum::Int(1)]]
    );
    // A WHERE below it still filters.
    assert_eq!(
        run_select_on(
            "SELECT DISTINCT a FROM d2 WHERE b = 2",
            &catalog,
            &crate::StmtContext::for_query()
        )
        .unwrap(),
        vec![vec![Datum::Int(1)], vec![Datum::Int(2)]]
    );

    // Over an aggregate result, DISTINCT deduplicates the output rows.
    crate::run_create_table_on("CREATE TABLE g3 (k BIGINT, v BIGINT)", &mut catalog).unwrap();
    run_insert_on(
        "INSERT INTO g3 VALUES (1, 5), (2, 5), (3, 9)",
        &mut catalog,
        &crate::StmtContext::for_query(),
    )
    .unwrap();
    assert_eq!(
        run_select_on(
            "SELECT DISTINCT SUM(v) FROM g3 GROUP BY k",
            &catalog,
            &crate::StmtContext::for_query()
        )
        .unwrap(),
        vec![
            vec![Datum::Decimal(tidb_datatype::Decimal::from_int(5))],
            vec![Datum::Decimal(tidb_datatype::Decimal::from_int(9))],
        ]
    );
}

/// Non-recursive CTEs: each is materialized in written order and then
/// resolves like an ordinary table, which is the shape Go's buildWith
/// plans. The previous behavior was an "unknown table" error.
#[test]
fn common_table_expressions() {
    let mut catalog = Catalog::default();
    crate::run_create_table_on("CREATE TABLE c1 (a BIGINT, b BIGINT)", &mut catalog).unwrap();
    run_insert_on(
        "INSERT INTO c1 VALUES (1, 10), (2, 20), (3, 30)",
        &mut catalog,
        &crate::StmtContext::for_query(),
    )
    .unwrap();

    assert_eq!(
        run_select_on(
            "WITH c AS (SELECT a FROM c1 WHERE a > 1) SELECT a FROM c",
            &catalog,
            &crate::StmtContext::for_query()
        )
        .unwrap(),
        vec![vec![Datum::Int(2)], vec![Datum::Int(3)]]
    );
    // The outer query filters, orders and aggregates the CTE like a table.
    assert_eq!(
        run_select_on(
            "WITH c AS (SELECT a, b FROM c1) SELECT SUM(b) FROM c WHERE a >= 2",
            &catalog,
            &crate::StmtContext::for_query()
        )
        .unwrap(),
        vec![vec![Datum::Decimal(tidb_datatype::Decimal::from_int(50))]]
    );
    // A column list renames the CTE's columns.
    assert_eq!(
        run_select_on(
            "WITH c (x) AS (SELECT a FROM c1 WHERE a = 3) SELECT x FROM c",
            &catalog,
            &crate::StmtContext::for_query()
        )
        .unwrap(),
        vec![vec![Datum::Int(3)]]
    );
    // A later CTE may read an earlier one, which is why they are
    // materialized in written order.
    assert_eq!(
        run_select_on(
            "WITH c AS (SELECT a FROM c1 WHERE a > 1), d AS (SELECT a FROM c WHERE a > 2) \
             SELECT a FROM d",
            &catalog,
            &crate::StmtContext::for_query()
        )
        .unwrap(),
        vec![vec![Datum::Int(3)]]
    );
    // A CTE and a real table join.
    assert_eq!(
        run_select_on(
            "WITH c AS (SELECT a FROM c1 WHERE a = 2) SELECT c1.b FROM c JOIN c1 ON c.a = c1.a",
            &catalog,
            &crate::StmtContext::for_query()
        )
        .unwrap(),
        vec![vec![Datum::Int(20)]]
    );
    // A CTE shadows a real table of the same name, as in SQL.
    assert_eq!(
        run_select_on(
            "WITH c1 AS (SELECT 9 AS a) SELECT a FROM c1",
            &catalog,
            &crate::StmtContext::for_query()
        )
        .unwrap(),
        vec![vec![Datum::Int(9)]]
    );

    // WITH RECURSIVE runs the fixpoint rather than returning only the seed
    // row; see `driver::recursive_cte`.
    assert_eq!(
        run_select_on(
            "WITH RECURSIVE c (n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM c WHERE n < 3) \
             SELECT n FROM c",
            &catalog,
            &crate::StmtContext::for_query()
        )
        .unwrap(),
        vec![
            vec![Datum::Int(1)],
            vec![Datum::Int(2)],
            vec![Datum::Int(3)]
        ]
    );
    // A mismatched column list is an error, not a silent rename of some.
    assert!(run_select_on(
        "WITH c (x, y) AS (SELECT a FROM c1) SELECT x FROM c",
        &catalog,
        &crate::StmtContext::for_query()
    )
    .is_err());
}

/// Set operations, checked against results captured from a running TiDB
/// for the same data: u1 = 1,2,2,3 and u2 = 2,3,4.
#[test]
fn set_operations() {
    let mut catalog = Catalog::default();
    crate::run_create_table_on("CREATE TABLE u1 (a BIGINT)", &mut catalog).unwrap();
    crate::run_create_table_on("CREATE TABLE u2 (a BIGINT)", &mut catalog).unwrap();
    run_insert_on(
        "INSERT INTO u1 VALUES (1), (2), (2), (3)",
        &mut catalog,
        &crate::StmtContext::for_query(),
    )
    .unwrap();
    run_insert_on(
        "INSERT INTO u2 VALUES (2), (3), (4)",
        &mut catalog,
        &crate::StmtContext::for_query(),
    )
    .unwrap();

    let sorted = |sql: &str, catalog: &Catalog| {
        let mut got: Vec<i64> = run_select_on(sql, catalog, &crate::StmtContext::for_query())
            .unwrap()
            .into_iter()
            .map(|row| match row[0] {
                Datum::Int(value) => value,
                ref other => panic!("expected an int, got {other:?}"),
            })
            .collect();
        got.sort_unstable();
        got
    };
    let listed = |sql: &str, catalog: &Catalog| {
        run_select_on(sql, catalog, &crate::StmtContext::for_query())
            .unwrap()
            .into_iter()
            .map(|row| match row[0] {
                Datum::Int(value) => value,
                ref other => panic!("expected an int, got {other:?}"),
            })
            .collect::<Vec<_>>()
    };

    // Captured: UNION dedups (TiDB returned 4,1,2,3 in hash order, so the
    // comparison sorts); UNION ALL concatenates in term order.
    assert_eq!(
        sorted("SELECT a FROM u1 UNION SELECT a FROM u2", &catalog),
        vec![1, 2, 3, 4]
    );
    assert_eq!(
        listed("SELECT a FROM u1 UNION ALL SELECT a FROM u2", &catalog),
        vec![1, 2, 2, 3, 2, 3, 4],
        "captured: UNION ALL keeps duplicates and term order"
    );
    // Captured: EXCEPT -> [1], INTERSECT -> [2, 3] (hash order).
    assert_eq!(
        listed("SELECT a FROM u1 EXCEPT SELECT a FROM u2", &catalog),
        vec![1]
    );
    assert_eq!(
        sorted("SELECT a FROM u1 INTERSECT SELECT a FROM u2", &catalog),
        vec![2, 3]
    );
    // The ALL forms keep multiplicity: u1 has 2 twice, u2 once.
    assert_eq!(
        listed("SELECT a FROM u1 INTERSECT ALL SELECT a FROM u2", &catalog),
        vec![2, 3]
    );
    assert_eq!(
        listed("SELECT a FROM u1 EXCEPT ALL SELECT a FROM u2", &catalog),
        vec![1, 2],
        "one of the two 2s survives EXCEPT ALL"
    );

    // A statement-level ORDER BY and LIMIT apply to the folded result.
    // Captured: ... ORDER BY a DESC -> 4,3,2,1.
    assert_eq!(
        listed(
            "SELECT a FROM u1 UNION SELECT a FROM u2 ORDER BY a DESC",
            &catalog
        ),
        vec![4, 3, 2, 1]
    );
    assert_eq!(
        listed(
            "SELECT a FROM u1 UNION SELECT a FROM u2 ORDER BY a LIMIT 2",
            &catalog
        ),
        vec![1, 2]
    );

    // Three terms fold left to right.
    assert_eq!(
        sorted(
            "SELECT a FROM u1 UNION SELECT a FROM u2 UNION SELECT 9",
            &catalog
        ),
        vec![1, 2, 3, 4, 9]
    );
    // A CTE prefix belongs to the whole statement.
    assert_eq!(
        sorted(
            "WITH c AS (SELECT a FROM u1 WHERE a = 3) \
             SELECT a FROM c UNION SELECT a FROM u2",
            &catalog
        ),
        vec![2, 3, 4]
    );

    // Captured: a term of a different width is 1222.
    assert!(matches!(
        run_select_on(
            "SELECT a FROM u1 UNION SELECT a, a FROM u2",
            &catalog,
            &crate::StmtContext::for_query()
        ),
        Err(DriverError::WrongNumberOfColumnsInSelect)
    ));
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

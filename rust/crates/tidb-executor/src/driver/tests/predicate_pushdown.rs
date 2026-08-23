//! Which predicates a table scan hands to the storage layer, and which it
//! keeps for itself.
//!
//! The point of these tests is the BOUNDARY: a conjunct TiKV can evaluate is
//! pushed, anything else stays above the scan, and the answer is identical
//! either way. Also covers the unknown-table rejection that happens before
//! any scan is built. Mirrors Go `pkg/planner/core`'s predicate pushdown into
//! `pkg/executor`'s table reader.

use super::*;

/// The rejection is Go's `infoschema.ErrTableNotExists` (1146), not an
/// untyped refusal: a client tells a typo'd table from a fatal server error
/// by the CODE, so the shape here is load-bearing.
#[test]
fn unknown_table_is_rejected() {
    let error = run_select("SELECT a FROM missing").expect_err("a missing table is an error");
    assert!(
        matches!(
            &error,
            DriverError::Schema(crate::SchemaErrorKind::UnknownTable(_))
        ),
        "{error:?}"
    );
    let wire = error.to_mysql_error();
    assert_eq!(wire.code, 1146);
    assert!(
        wire.message.ends_with(".missing' doesn't exist"),
        "{wire:?}"
    );
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
            func_deps: Default::default(),
            physical: None,
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
        let (pushed, residual) = split_scan_predicates(
            &where_clause,
            &ScopeResolver { scope: &scope },
            &crate::StmtContext::default(),
        );
        (
            pushed
                .predicates()
                .iter()
                .map(|predicate| match predicate {
                    ScanPredicate::Compare(c) => {
                        (c.column_offset, c.op, c.literal.clone(), c.column_on_left)
                    }
                    other => panic!("only comparisons are described here: {other:?}"),
                })
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
    // Shapes that push nothing: a NULL constant, `IS TRUE` (a different Go
    // function with its own NULL rule),
    // NULL-safe equality (a different signature), an empty or NULL-carrying
    // `IN` list, and a disjunction one branch of which is not describable.
    for sql in [
        "SELECT 1 FROM t WHERE a = NULL",
        "SELECT 1 FROM t WHERE a IS TRUE",
        "SELECT 1 FROM t WHERE a <=> 5",
        "SELECT 1 FROM t WHERE a IN (1, NULL)",
        "SELECT 1 FROM t WHERE a > 5 OR b + 1 < 10",
    ] {
        let (pushed, residual) = split(sql);
        assert!(pushed.is_empty(), "{sql} must not push");
        assert!(residual.is_some(), "{sql} keeps its whole predicate");
    }

    let stmt = tidb_parser::parse("SELECT 1 FROM t WHERE a > b").unwrap();
    let Stmt::Query(query) = &stmt else {
        panic!("a select");
    };
    let QueryStmt::Select(statement) = &**query else {
        panic!("a select");
    };
    let (pushed, residual) = split_scan_predicates(
        statement.where_clause.as_ref().unwrap(),
        &ScopeResolver { scope: &scope },
        &crate::StmtContext::default(),
    );
    assert!(residual.is_none(), "a typed column comparison is pushed");
    assert!(matches!(
        pushed.predicates(),
        [ScanPredicate::ColumnCompare(comparison)]
            if comparison.left_offset == 0
                && comparison.right_offset == 1
                && comparison.op == ScanComparisonOp::Gt
    ));
}

/// TPC-H q6's four source conjuncts become the five typed Selection
/// conditions Go builds: `BETWEEN` splits, decimal arithmetic folds exactly,
/// and both date strings are cast to DATETIME(26,6) before TiPB encoding.
#[test]
fn tpch_q6_scan_descriptions_keep_go_comparison_types() {
    let scope = FromScope {
        tables: vec![FromTable {
            name: "lineitem".to_owned(),
            database: None,
            columns: vec![
                (
                    "l_shipdate".to_owned(),
                    FieldType::new(FieldTypeCode::Date)
                        .with_flen(10)
                        .with_decimal(0),
                ),
                (
                    "l_discount".to_owned(),
                    FieldType::new(FieldTypeCode::NewDecimal)
                        .with_flen(4)
                        .with_decimal(2),
                ),
                (
                    "l_quantity".to_owned(),
                    FieldType::new(FieldTypeCode::NewDecimal)
                        .with_flen(15)
                        .with_decimal(2),
                ),
            ],
            offset: 0,
            func_deps: Default::default(),
            physical: None,
        }],
        ..FromScope::default()
    };
    let sql = "SELECT 1 FROM lineitem WHERE \
        l_shipdate >= '1994-01-01' AND \
        l_shipdate < DATE_ADD('1994-01-01', INTERVAL '1' YEAR) AND \
        l_discount BETWEEN 0.06 - 0.01 AND 0.06 + 0.01 AND \
        l_quantity < 24";
    let stmt = tidb_parser::parse(sql).unwrap();
    let Stmt::Query(query) = &stmt else {
        panic!("a select");
    };
    let QueryStmt::Select(statement) = &**query else {
        panic!("a select");
    };
    let (pushed, residual) = split_scan_predicates(
        statement.where_clause.as_ref().unwrap(),
        &ScopeResolver { scope: &scope },
        &crate::StmtContext::default(),
    );
    assert!(residual.is_none());
    assert_eq!(pushed.predicates().len(), 4);

    let temporal = |predicate: &ScanPredicate| {
        let ScanPredicate::Compare(comparison) = predicate else {
            panic!("a temporal comparison: {predicate:?}");
        };
        assert!(matches!(comparison.literal, Datum::Time(_)));
        assert_eq!(comparison.literal_type.code(), FieldTypeCode::Datetime);
        assert_eq!(comparison.literal_type.flen(), 26);
        assert_eq!(comparison.literal_type.decimal(), 6);
    };
    temporal(&pushed.predicates()[0]);
    temporal(&pushed.predicates()[1]);

    let ScanPredicate::And(bounds) = &pushed.predicates()[2] else {
        panic!("BETWEEN expands to two bounds");
    };
    assert_eq!(bounds.len(), 2);
    let decimals = bounds
        .iter()
        .map(|predicate| match predicate {
            ScanPredicate::Compare(comparison) => comparison.literal.sql_string().unwrap(),
            other => panic!("a decimal comparison: {other:?}"),
        })
        .collect::<Vec<_>>();
    assert_eq!(decimals, ["0.05", "0.07"]);

    let ScanPredicate::Compare(quantity) = &pushed.predicates()[3] else {
        panic!("a quantity comparison");
    };
    assert_eq!(
        quantity.literal,
        Datum::Decimal(tidb_datatype::Decimal::from_int(24))
    );
    assert_eq!(quantity.literal_type.code(), FieldTypeCode::NewDecimal);
    assert_eq!(
        (
            quantity.literal_type.flen(),
            quantity.literal_type.decimal()
        ),
        (2, 0)
    );
}

/// Go's expression rewriter turns q6's BETWEEN into two CNF conditions,
/// folds the decimal/date constants, and estimates those conditions against
/// the base table's column identities after column pruning. The leading
/// `unused` column makes compact executor offsets differ from physical table
/// offsets, which must not disconnect the predicates from their statistics.
#[test]
fn tpch_q6_selection_keeps_go_conditions_and_cardinality_after_pruning() {
    let mut catalog = Catalog::default();
    crate::run_create_table_on(
        "CREATE TABLE q6_lineitem (\
           unused BIGINT, \
           l_shipdate DATE, \
           l_discount DECIMAL(4,2), \
           l_quantity DECIMAL(15,2), \
           l_extendedprice DECIMAL(15,2))",
        &mut catalog,
    )
    .unwrap();
    let statement = tidb_parser::parse(
        "SELECT SUM(l_extendedprice * l_discount) FROM q6_lineitem WHERE \
           l_shipdate >= '1994-01-01' AND \
           l_shipdate < DATE_ADD('1994-01-01', INTERVAL '1' YEAR) AND \
           l_discount BETWEEN 0.06 - 0.01 AND 0.06 + 0.01 AND \
           l_quantity < 24",
    )
    .unwrap();
    let Stmt::Query(query) = &statement else {
        panic!("a query");
    };
    let QueryStmt::Select(select) = &**query else {
        panic!("a SELECT");
    };
    let (_, rows) = crate::explain::explain_select_stmt(
        select,
        &catalog,
        "test",
        &crate::StmtContext::for_query(),
        crate::explain::ExplainFormat::Brief,
    )
    .unwrap();
    let selection = rows
        .iter()
        .find(|row| datum_text_for_test(&row[0]).contains("Selection"))
        .expect("q6 has a Selection");

    assert_eq!(datum_text_for_test(&selection[1]), "2.08", "{rows:?}");
    assert_eq!(
        datum_text_for_test(&selection[4]),
        "ge(test.q6_lineitem.l_discount, 0.05), \
         ge(test.q6_lineitem.l_shipdate, 1994-01-01 00:00:00.000000), \
         le(test.q6_lineitem.l_discount, 0.07), \
         lt(test.q6_lineitem.l_quantity, 24), \
         lt(test.q6_lineitem.l_shipdate, 1995-01-01 00:00:00.000000)"
    );
}

/// Go `expression.ExtractFiltersFromDNFs` lifts a CNF item repeated by every
/// OR branch before `LogicalJoin.PredicatePushDown` classifies join keys.
#[test]
fn a_common_dnf_equality_is_a_hash_join_key() {
    let mut catalog = Catalog::default();
    crate::run_create_table_on(
        "CREATE TABLE dnf_left (id BIGINT, kind BIGINT, size BIGINT)",
        &mut catalog,
    )
    .unwrap();
    crate::run_create_table_on(
        "CREATE TABLE dnf_right (\
            id BIGINT, flag BIGINT, quantity BIGINT, mode VARCHAR(16), instruction VARCHAR(32))",
        &mut catalog,
    )
    .unwrap();
    run_insert_on(
        "INSERT INTO dnf_left VALUES (1, 1, 3), (2, 2, 8), (3, 1, 3)",
        &mut catalog,
        &crate::StmtContext::for_query(),
    )
    .unwrap();
    run_insert_on(
        "INSERT INTO dnf_right VALUES \
            (1, 1, 4, 'AIR', 'DELIVER IN PERSON'), \
            (2, 2, 18, 'AIR REG', 'DELIVER IN PERSON'), \
            (3, 2, 4, 'SHIP', 'DELIVER IN PERSON')",
        &mut catalog,
        &crate::StmtContext::for_query(),
    )
    .unwrap();
    let sql = "SELECT /*+ HASH_AGG() */ SUM(dnf_right.quantity) \
        FROM dnf_left, dnf_right WHERE \
        (dnf_left.id = dnf_right.id AND dnf_left.kind = 1 AND dnf_left.size BETWEEN 1 AND 5 \
         AND dnf_right.flag = 1 AND dnf_right.quantity <= 4 + 10 \
         AND dnf_right.mode IN ('AIR', 'AIR REG') \
         AND dnf_right.instruction = 'DELIVER IN PERSON') OR \
        (dnf_left.id = dnf_right.id AND dnf_left.kind = 2 AND dnf_left.size BETWEEN 1 AND 10 \
         AND dnf_right.flag = 2 AND dnf_right.quantity <= 18 + 10 \
         AND dnf_right.mode IN ('AIR', 'AIR REG') \
         AND dnf_right.instruction = 'DELIVER IN PERSON')";

    assert_eq!(
        run_select_on(sql, &catalog, &crate::StmtContext::for_query()).unwrap(),
        vec![vec![Datum::Decimal(tidb_datatype::Decimal::from_int(22))]],
    );

    let statement = tidb_parser::parse(sql).unwrap();
    let Stmt::Query(query) = &statement else {
        panic!("a query");
    };
    let QueryStmt::Select(select) = &**query else {
        panic!("a SELECT");
    };
    let row_source = crate::driver::join_reorder::row_source(
        select.from.as_ref().expect("a joined FROM"),
        select.where_clause.as_ref(),
        &catalog,
        "test",
        &crate::StmtContext::for_query(),
    )
    .expect("a row source");
    for visible in ["dnf_left", "dnf_right"] {
        let filters = row_source.filters_for(visible).expect("leaf filters");
        for traced in row_source
            .trace_filters_for(visible)
            .expect("trace filters")
        {
            assert!(
                filters.contains(traced),
                "the statistics and physical leaf must share Go's balanced DNF shape: {traced:?} not in {filters:?}"
            );
        }
    }
    let (_, rows) = crate::explain::explain_select_stmt(
        select,
        &catalog,
        "test",
        &crate::StmtContext::for_query(),
        crate::explain::ExplainFormat::Brief,
    )
    .unwrap();
    assert!(
        datum_text_for_test(&rows[0][0]).contains("HashAgg"),
        "{rows:?}"
    );
    assert_eq!(datum_text_for_test(&rows[0][1]), "1.00", "{rows:?}");
    assert!(
        !datum_text_for_test(&rows[0][4]).contains("group by:"),
        "{rows:?}"
    );
    assert_eq!(
        datum_text_for_test(&rows[0][4]),
        "funcs:sum(Column#0)->Column#0",
        "{rows:?}"
    );
    let join = rows
        .iter()
        .find(|row| datum_text_for_test(&row[0]).contains("HashJoin"))
        .expect("a hash join");
    let join_at = rows.iter().position(|row| std::ptr::eq(row, join)).unwrap();
    assert!(
        rows[..join_at]
            .iter()
            .all(|row| !datum_text_for_test(&row[0]).contains("Selection")),
        "common leaf filters stay below the join: {rows:?}"
    );
    let info = datum_text_for_test(&join[4]);
    assert!(!info.contains("CARTESIAN"), "{rows:?}");
    assert!(
        info.contains("equal:[eq(test.dnf_right.id, test.dnf_left.id)]"),
        "{rows:?}"
    );
    assert!(!info.contains(" BETWEEN "), "{info}");
    assert!(!info.contains(" IN ("), "{info}");
    assert!(!info.contains("plus("), "{info}");
    assert!(info.contains("le(test.dnf_right.quantity, 14)"), "{info}");
}

/// Go `pkg/planner/cardinality.TestDNFCondSelectivity` estimates
/// `b > 7 OR c < 4` as `0.34375` on these eight analyzed rows. Appending
/// `d < 5` through the same recursive independence rule gives `0.671875`, or
/// 5.38 rows. A join leaf must carry that loaded-statistics answer into its
/// physical Selection instead of falling back to pseudo selectivity.
#[test]
fn a_join_leaf_dnf_selection_uses_loaded_column_statistics() {
    let mut catalog = Catalog::default();
    crate::run_create_table_on(
        "CREATE TABLE dnf_stats (a BIGINT, b BIGINT, c BIGINT, d BIGINT)",
        &mut catalog,
    )
    .unwrap();
    crate::run_create_table_on(
        "CREATE TABLE dnf_keys (a BIGINT, branch BIGINT)",
        &mut catalog,
    )
    .unwrap();
    let rows = "(1,5,4,4),(3,4,1,8),(4,2,6,10),(6,7,2,5),\
                (7,1,4,9),(8,9,8,3),(9,1,9,1),(10,6,6,2)";
    let ctx = crate::StmtContext::for_query();
    run_insert_on(
        &format!("INSERT INTO dnf_stats VALUES {rows}"),
        &mut catalog,
        &ctx,
    )
    .unwrap();
    run_insert_on(
        "INSERT INTO dnf_keys VALUES (1,1),(3,2),(4,3),(6,1),(7,2),(8,3),(9,1),(10,2)",
        &mut catalog,
        &ctx,
    )
    .unwrap();
    for table_name in ["dnf_stats", "dnf_keys"] {
        let (table_id, statistics) = {
            let TableEntry::Kv(table) = catalog
                .table_mut_in(crate::driver::DEFAULT_DATABASE, table_name)
                .expect("table exists")
            else {
                panic!("{table_name} is not a KV table");
            };
            (
                table.table_id,
                crate::analyze::kv::analyze_kv_table(
                    table,
                    &crate::analyze::AnalyzeOptions::default(),
                    None,
                    &ctx,
                )
                .unwrap(),
            )
        };
        catalog.set_table_statistics(table_id, std::sync::Arc::new(statistics));
    }

    let statement = tidb_parser::parse(
        "SELECT /*+ HASH_AGG() */ SUM(dnf_stats.a) FROM dnf_stats, dnf_keys WHERE \
         (dnf_stats.a = dnf_keys.a AND dnf_stats.d >= 1 AND dnf_stats.b > 7 \
          AND dnf_keys.branch >= 0 AND dnf_keys.branch = 1) OR \
         (dnf_stats.a = dnf_keys.a AND dnf_stats.d >= 1 AND dnf_stats.c < 4 \
          AND dnf_keys.branch >= 0 AND dnf_keys.branch = 2) OR \
         (dnf_stats.a = dnf_keys.a AND dnf_stats.d >= 1 AND dnf_stats.d < 5 \
          AND dnf_keys.branch >= 0 AND dnf_keys.branch = 3)",
    )
    .unwrap();
    let Stmt::Query(query) = &statement else {
        panic!("a query");
    };
    let QueryStmt::Select(select) = &**query else {
        panic!("a SELECT");
    };
    let (_, plan) = crate::explain::explain_select_stmt(
        select,
        &catalog,
        "test",
        &ctx,
        crate::explain::ExplainFormat::Brief,
    )
    .unwrap();
    let selection = plan
        .iter()
        .find(|row| {
            datum_text_for_test(&row[0]).contains("Selection")
                && datum_text_for_test(&row[4]).contains("dnf_stats.b")
        })
        .expect("the DNF is pushed into a leaf Selection");
    assert_eq!(datum_text_for_test(&selection[1]), "5.38", "{plan:?}");
    assert_eq!(datum_text_for_test(&selection[3]), "", "{plan:?}");
}

#[test]
fn common_dnf_filter_extraction_matches_go_expression_cases() {
    fn predicate(sql: &str) -> tidb_ast::Expr {
        let statement = tidb_parser::parse(&format!("SELECT 1 FROM t WHERE {sql}")).unwrap();
        let Stmt::Query(query) = &statement else {
            panic!("a query");
        };
        let QueryStmt::Select(select) = &**query else {
            panic!("a SELECT");
        };
        select.where_clause.clone().expect("a WHERE predicate")
    }
    let extracted =
        |sql: &str| crate::driver::predicate_push_down::extracted_conjuncts(&predicate(sql));

    let a = predicate("a = 1");
    let b = predicate("b = 1");
    for sql in [
        "a = 1 OR a = 1 OR a = 1",
        "a = 1 OR a = 1 OR (a = 1 AND b = 1)",
    ] {
        assert_eq!(extracted(sql), vec![a.clone()], "{sql}");
    }

    let unchanged = "(a = 1 AND a = 1) OR a = 1 OR b = 1";
    assert_eq!(extracted(unchanged), vec![predicate(unchanged)]);

    let partial = extracted("(a = 1 AND b = 2) OR (a = 1 AND b = 3) OR (a = 1 AND b = 4)");
    assert_eq!(partial.len(), 2, "{partial:?}");
    assert!(partial.contains(&a), "{partial:?}");
    let residual = partial.iter().find(|condition| **condition != a).unwrap();
    let residual = residual.restore();
    assert!(!residual.contains("`a`=1"), "{residual}");
    for expected in ["`b`=2", "`b`=3", "`b`=4"] {
        assert!(residual.contains(expected), "{residual}");
    }

    let multiple = extracted(
        "(a = 1 AND b = 1 AND c = 1) OR \
         (a = 1 AND b = 1) OR \
         (a = 1 AND b = 1 AND c > 2 AND c < 3)",
    );
    assert_eq!(multiple.len(), 2, "{multiple:?}");
    assert!(multiple.contains(&a), "{multiple:?}");
    assert!(multiple.contains(&b), "{multiple:?}");

    let duplicate = extracted("(a = 1 AND b = 2 AND a = 1) OR (a = 1 AND b = 3)");
    assert_eq!(
        duplicate
            .iter()
            .filter(|condition| **condition == a)
            .count(),
        1,
        "{duplicate:?}"
    );

    let between = extracted(
        "(a BETWEEN 1 AND 5 AND b = 1) OR \
         (a BETWEEN 1 AND 10 AND b = 2) OR \
         (a BETWEEN 1 AND 15 AND b = 3)",
    );
    let lower_bound = predicate("a >= 1");
    assert!(between.contains(&lower_bound), "{between:?}");
    let residual = between
        .iter()
        .find(|condition| **condition != lower_bound)
        .expect("branch-specific DNF residue")
        .restore();
    assert!(!residual.contains("BETWEEN"), "{residual}");
    assert!(!residual.contains("`a`>=1"), "{residual}");
}

#[test]
fn the_scan_keeps_the_refined_expression_it_executes() {
    let scope = FromScope {
        tables: vec![FromTable {
            name: "t".to_owned(),
            database: Some("test".to_owned()),
            columns: vec![("a".to_owned(), FieldType::new(FieldTypeCode::Long))],
            offset: 0,
            func_deps: Default::default(),
            physical: None,
        }],
        ..FromScope::default()
    };
    let statement = tidb_parser::parse("SELECT * FROM t WHERE a > '10ab'").unwrap();
    let Stmt::Query(query) = &statement else {
        panic!("a select");
    };
    let QueryStmt::Select(statement) = &**query else {
        panic!("a select");
    };
    let (pushed, residual) = split_scan_predicates(
        statement.where_clause.as_ref().unwrap(),
        &ScopeResolver { scope: &scope },
        &crate::StmtContext::default(),
    );
    assert!(residual.is_none());
    let [Expression::ScalarFunction(comparison)] = pushed.filters() else {
        panic!("one built comparison: {pushed:?}");
    };
    let [Expression::Column(_), Expression::Constant(constant)] = comparison.args.as_slice() else {
        panic!("column-to-constant comparison: {comparison:?}");
    };
    assert_eq!(constant.value, Datum::Int(10));
}

/// The composed shapes the coprocessor's whitelist admits: `OR`, `NOT`,
/// `IS [NOT] NULL` and `[NOT] IN`. Each becomes one described conjunct and
/// leaves no residual, because the whole conjunct moved into the scan.
#[test]
fn the_scan_takes_the_composed_predicates_tikv_evaluates() {
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
            func_deps: Default::default(),
            physical: None,
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
        let (pushed, residual) = split_scan_predicates(
            &where_clause,
            &ScopeResolver { scope: &scope },
            &crate::StmtContext::default(),
        );
        (pushed.predicates().to_vec(), residual)
    };

    let (pushed, residual) = split("SELECT 1 FROM t WHERE a IS NULL");
    assert!(residual.is_none(), "the whole conjunct moved into the scan");
    assert!(matches!(
        pushed.as_slice(),
        [ScanPredicate::IsNull {
            column_offset: 0,
            negated: false,
            ..
        }]
    ));
    let (pushed, _) = split("SELECT 1 FROM t WHERE b IS NOT NULL");
    assert!(matches!(
        pushed.as_slice(),
        [ScanPredicate::IsNull {
            column_offset: 1,
            negated: true,
            ..
        }]
    ));

    let (pushed, residual) = split("SELECT 1 FROM t WHERE a IN (1, 3, 5)");
    assert!(residual.is_none());
    let [ScanPredicate::In {
        column_offset,
        literals,
        negated,
        ..
    }] = pushed.as_slice()
    else {
        panic!("one `IN` description: {pushed:?}");
    };
    assert_eq!((*column_offset, *negated), (0, false));
    assert_eq!(
        literals,
        &[Datum::Int(1), Datum::Int(3), Datum::Int(5)],
        "the list keeps its source order and values"
    );
    let (pushed, _) = split("SELECT 1 FROM t WHERE a NOT IN (2)");
    assert!(matches!(
        pushed.as_slice(),
        [ScanPredicate::In { negated: true, .. }]
    ));

    // An `OR` chain flattens: `a = 1 OR a = 2 OR b IS NULL` is one conjunct
    // with three branches, not a nest of two-branch disjunctions.
    let (pushed, residual) = split("SELECT 1 FROM t WHERE a = 1 OR a = 2 OR b IS NULL");
    assert!(residual.is_none());
    let [ScanPredicate::Or(branches)] = pushed.as_slice() else {
        panic!("one `OR` description: {pushed:?}");
    };
    assert_eq!(branches.len(), 3);
    assert!(matches!(branches[2], ScanPredicate::IsNull { .. }));

    // `NOT` wraps whatever it negates. A conjunction inside an `OR` branch is
    // also a TiKV Selection shape, which is needed for TPC-H q19's DNF.
    let (pushed, _) = split("SELECT 1 FROM t WHERE NOT a = 1");
    assert!(matches!(pushed.as_slice(), [ScanPredicate::Not(_)]));
    let (pushed, residual) = split("SELECT 1 FROM t WHERE (a = 1 AND b = 2) OR a = 3");
    let [ScanPredicate::Or(branches)] = pushed.as_slice() else {
        panic!("one DNF disjunction: {pushed:?}");
    };
    assert!(residual.is_none());
    assert_eq!(branches.len(), 2);
    assert!(matches!(branches[0], ScanPredicate::And(_)));
    assert!(matches!(branches[1], ScanPredicate::Compare(_)));

    // Mixed with a residual: the describable conjunct moves, the rest stays.
    let (pushed, residual) = split("SELECT 1 FROM t WHERE a IN (1, 2) AND b + 1 < 10");
    assert_eq!(pushed.len(), 1);
    assert!(residual.is_some());
}

/// Go builds `IN` from the tested expression's evaluation type, so a
/// pushable string call is as eligible as a bare string column. This is the
/// TPC-H q22 phone-prefix shape, without coupling the rule to that query.
#[test]
fn the_scan_takes_string_expression_in() {
    let scope = FromScope {
        tables: vec![FromTable {
            name: "customer".to_owned(),
            database: None,
            columns: vec![(
                "phone".to_owned(),
                FieldType::new(FieldTypeCode::Varchar)
                    .with_charset_name("utf8mb4")
                    .with_collation_name("utf8mb4_bin"),
            )],
            offset: 0,
            func_deps: Default::default(),
            physical: None,
        }],
        ..FromScope::default()
    };
    let statement = tidb_parser::parse(
        "SELECT 1 FROM customer WHERE SUBSTRING(phone, 1, 2) IN ('20', '40', '20')",
    )
    .unwrap();
    let Stmt::Query(query) = &statement else {
        panic!("a select");
    };
    let QueryStmt::Select(statement) = &**query else {
        panic!("a select");
    };
    let (pushed, residual) = split_scan_predicates(
        statement.where_clause.as_ref().unwrap(),
        &ScopeResolver { scope: &scope },
        &crate::StmtContext::default(),
    );

    assert!(residual.is_none(), "the whole string IN reaches the scan");
    let [ScanPredicate::ScalarIn {
        tested,
        literals,
        negated,
        ..
    }] = pushed.predicates()
    else {
        panic!("one scalar IN description: {:?}", pushed.predicates());
    };
    let tidb_expr::pushdown_catalog::PbScalar::Call { signature, .. } = tested else {
        panic!("SUBSTRING remains the tested scalar: {tested:?}");
    };
    assert_eq!(
        signature.sig,
        tidb_expr::pushdown_catalog::ScalarFuncSig::Substring3ArgsUtf8
    );
    assert_eq!(
        literals.len(),
        3,
        "deduplication belongs to Go's IN builder"
    );
    assert!(!negated);
}

/// WHERE THE SPLIT IS PRINTED: the conjuncts the read evaluates go INSIDE the
/// coprocessor task, under the reader that finishes it.
///
/// Go builds one `CopTask` per base-table read (`convertToTableScan`,
/// `pkg/planner/core/find_best_task.go:2829`), hangs the coprocessor half of
/// the filter conditions off the scan as a `PhysicalSelection` inside it
/// (`addPushedDownSelection4PhysicalTableScan`, `:3198`), and caps it with
/// the reader (`ConvertToRootTask`,
/// `pkg/planner/core/operator/physicalop/task_base.go:504`). The recorded
/// spelling is quoted verbatim by `r/explain_easy.result:477` for a filtered
/// read --
///
/// ```text
/// TableReader        root                 data:Selection
/// └─Selection        cop[tikv]            eq(...)
///   └─TableFullScan  cop[tikv]  table:ta  keep order:false, stats:pseudo
/// ```
///
/// -- and by `r/explain_easy.result:26`, `select * from t1`, for an
/// unfiltered one:
///
/// ```text
/// TableReader        root                 data:TableFullScan
/// └─TableFullScan    cop[tikv]  table:t1  keep order:false, stats:pseudo
/// ```
///
/// The boundary is a CLAIM about where work happens, so each case below pairs
/// the printed task with the reason the scan may be said to do it:
/// [`negotiate_scan_filter`] handed the conjunct to the source, which
/// [`crate::predicate_pushdown`] obliges to apply it to every row it emits.
#[test]
fn a_single_table_read_ends_in_the_cop_task_go_prints() {
    let mut catalog = Catalog::default();
    crate::run_create_table_on(
        "CREATE TABLE reader_t (a BIGINT, b BIGINT, c VARCHAR(20))",
        &mut catalog,
    )
    .unwrap();
    let plan = |sql: &str| {
        let statement = tidb_parser::parse(sql).expect("a select");
        let Stmt::Query(query) = &statement else {
            panic!("a select");
        };
        let QueryStmt::Select(select) = &**query else {
            panic!("a select");
        };
        let (_, rows) = crate::explain::explain_select_stmt(
            select,
            &catalog,
            "test",
            &crate::StmtContext::for_query(),
            crate::explain::ExplainFormat::Row,
        )
        .unwrap();
        rows.iter()
            .map(|row| {
                (
                    datum_text_for_test(&row[0]),
                    datum_text_for_test(&row[2]),
                    datum_text_for_test(&row[4]),
                )
            })
            .collect::<Vec<_>>()
    };

    // A conjunct the scan takes whole: it is the cop `Selection`, and the
    // reader names it.
    assert_eq!(
        plan("SELECT * FROM reader_t WHERE a > 5"),
        vec![
            (
                "TableReader_3".to_owned(),
                "root".to_owned(),
                "data:Selection".to_owned(),
            ),
            (
                "└─Selection_2".to_owned(),
                "cop[tikv]".to_owned(),
                "gt(test.reader_t.a, 5)".to_owned(),
            ),
            (
                "  └─TableFullScan_1".to_owned(),
                "cop[tikv]".to_owned(),
                "keep order:false, stats:pseudo".to_owned(),
            ),
        ],
    );

    // No `WHERE` at all: the task holds the scan alone, and the reader names
    // the scan.
    assert_eq!(
        plan("SELECT * FROM reader_t"),
        vec![
            (
                "TableReader_2".to_owned(),
                "root".to_owned(),
                "data:TableFullScan".to_owned(),
            ),
            (
                "└─TableFullScan_1".to_owned(),
                "cop[tikv]".to_owned(),
                "keep order:false, stats:pseudo".to_owned(),
            ),
        ],
    );

    // A conjunct no source here can promise -- `plus(int, int)` is not in
    // `tidb_expr::pushdown_catalog`, so the scan never saw it -- is Go's
    // `CopTask.RootTaskConds`: a root `Selection` ABOVE the reader
    // (`pkg/planner/core/operator/physicalop/task.go:47`). The task below it
    // still closes, and the conjunct that DID push keeps its cop
    // `Selection`, so each half is printed where it runs.
    assert_eq!(
        plan("SELECT * FROM reader_t WHERE a > 5 AND b + 1 < 10"),
        vec![
            (
                "Selection_4".to_owned(),
                "root".to_owned(),
                "lt(plus(test.reader_t.b, 1), 10)".to_owned(),
            ),
            (
                "└─TableReader_3".to_owned(),
                "root".to_owned(),
                "data:Selection".to_owned(),
            ),
            (
                "  └─Selection_2".to_owned(),
                "cop[tikv]".to_owned(),
                "gt(test.reader_t.a, 5)".to_owned(),
            ),
            (
                "    └─TableFullScan_1".to_owned(),
                "cop[tikv]".to_owned(),
                "keep order:false, stats:pseudo".to_owned(),
            ),
        ],
    );

    // Nothing pushed at all: the whole `WHERE` is root conditions, and the
    // task below is the bare scan.
    assert_eq!(
        plan("SELECT * FROM reader_t WHERE b + 1 < 10"),
        vec![
            (
                "Selection_3".to_owned(),
                "root".to_owned(),
                "lt(plus(test.reader_t.b, 1), 10)".to_owned(),
            ),
            (
                "└─TableReader_2".to_owned(),
                "root".to_owned(),
                "data:TableFullScan".to_owned(),
            ),
            (
                "  └─TableFullScan_1".to_owned(),
                "cop[tikv]".to_owned(),
                "keep order:false, stats:pseudo".to_owned(),
            ),
        ],
    );
}

/// THE ANSWER DOES NOT MOVE WITH THE BOUNDARY. Printing a conjunct as
/// `cop[tikv]` says the scan applies it; this checks that the scan really
/// does, for each split above, by comparing the rows against the same
/// predicate over a derived table -- whose computed column no scan can filter
/// on, so nothing is pushed and the whole `WHERE` runs at root.
#[test]
fn the_cop_boundary_does_not_change_a_single_row() {
    let mut catalog = Catalog::default();
    crate::run_create_table_on("CREATE TABLE split_t (a BIGINT, b BIGINT)", &mut catalog).unwrap();
    let ctx = crate::StmtContext::for_query();
    crate::run_insert_on(
        "INSERT INTO split_t VALUES (1,1),(6,1),(7,20),(NULL,3),(8,NULL)",
        &mut catalog,
        &ctx,
    )
    .unwrap();
    for (pushed, at_root) in [
        (
            "SELECT a, b FROM split_t WHERE a > 5 ORDER BY a",
            "SELECT a, b FROM (SELECT a + 0 AS a, b FROM split_t) s WHERE a > 5 ORDER BY a",
        ),
        (
            "SELECT a, b FROM split_t WHERE a > 5 AND b + 1 < 10 ORDER BY a",
            "SELECT a, b FROM (SELECT a + 0 AS a, b FROM split_t) s \
             WHERE a > 5 AND b + 1 < 10 ORDER BY a",
        ),
        (
            "SELECT a, b FROM split_t WHERE a IS NULL OR b IS NULL ORDER BY a",
            "SELECT a, b FROM (SELECT a + 0 AS a, b FROM split_t) s \
             WHERE a IS NULL OR b IS NULL ORDER BY a",
        ),
    ] {
        assert_eq!(
            crate::run_select_on(pushed, &catalog, &ctx).unwrap(),
            crate::run_select_on(at_root, &catalog, &ctx).unwrap(),
            "{pushed}",
        );
    }
}
